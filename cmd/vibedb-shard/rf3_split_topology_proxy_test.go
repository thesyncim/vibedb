package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func TestRF3ProxiedSplitTopologyRequiresStorageIdentityAndCommittedStep(t *testing.T) {
	group := rf3CommandGroup()
	domain := rafttransport.TrustDomain{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation}
	storage, gatewayNode := rafttransport.NodeID{1}, rafttransport.NodeID{2}
	credentials, roots, err := rf3testfixture.WriteCredentials(t.TempDir(), rf3CommandIdentityOID, domain,
		[]rafttransport.NodeID{storage, gatewayNode})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := servicetls.LoadProfile(credentials[0].Certificate, credentials[0].Key, roots,
		rf3CommandIdentityOID.String(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	seeds := []nodecontrol.BootstrapGatewaySeed{{NodeID: gatewayNode, Incarnation: 1,
		ControlAddress: "127.0.0.1:1", SPKIPinDigest: [32]byte{1}}}
	identity := raftmember.RuntimeIdentity{Group: group, MemberID: 3, StoreID: [16]byte{4}, NodeIncarnation: 5}
	authority := serviceauthz.Authority{Node: storage, Generation: 1}
	lease, apply := new(splitcontroller.RuntimeStoreLease), new(sqldriver.ReplicatedApply)
	actions, err := newRF3ProxiedSplitTopologyActions(profile, authority, seeds, identity, lease, apply)
	if err != nil {
		t.Fatal(err)
	}
	factory := actions.(*rf3RetainedPruneFactory)
	// No context can invent a local committed-step witness. In its absence
	// the transport must reject before opening any gateway or native stream.
	transport, executor, err := openRF3SplitTopologyTransport(t.Context(), profile, factory.transport)
	if !errors.Is(err, errRF3Serving) || transport != nil || executor != nil {
		t.Fatalf("uncommitted topology step acquired transport: %v", err)
	}
	foreign := authority
	foreign.Node = gatewayNode
	if _, err := newRF3ProxiedSplitTopologyActions(profile, foreign, seeds, identity, lease, apply); !errors.Is(err, errRF3Serving) {
		t.Fatalf("storage TLS borrowed gateway authority: %v", err)
	}
	wrongGroup := identity
	wrongGroup.Group.ClusterIncarnation[0]++
	if _, err := newRF3ProxiedSplitTopologyActions(profile, authority, seeds, wrongGroup, lease, apply); !errors.Is(err, errRF3Serving) {
		t.Fatalf("source from another cluster accepted: %v", err)
	}
	seeds[0].SPKIPinDigest = [32]byte{}
	if _, err := newRF3ProxiedSplitTopologyActions(profile, authority, seeds, identity, lease, apply); !errors.Is(err, errRF3Serving) {
		t.Fatalf("unpinned gateway accepted: %v", err)
	}
}

func TestRF3SplitTopologyTransportNeverFallsBackAfterProxyRefusal(t *testing.T) {
	want := errors.New("committed topology action was withdrawn")
	calls := 0
	transport, executor, err := openRF3SplitTopologyTransport(t.Context(), nil,
		func(context.Context) (rf3SplitTopologyTransport, error) {
			calls++
			return nil, want
		})
	if !errors.Is(err, want) || calls != 1 || transport != nil || executor != nil {
		t.Fatalf("proxy refusal fell back to another authority: calls=%d err=%v", calls, err)
	}
}
