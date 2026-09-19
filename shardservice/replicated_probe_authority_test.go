package shardservice

import (
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func TestReplicatedLocalCatalogProbeUsesExactDrainingOwnerFence(t *testing.T) {
	state := testReplicatedServingState()
	owner := &fakeReplicatedOwner{state: state}
	fixture := bindSemanticServer(t, owner, time.Second)

	digest := state.Command.RelationManifestDigest
	var relation [16]byte
	copy(relation[:], digest[:])
	binding := serviceauthz.ServiceBinding{
		Principal:           fixture.gateway.LocalIdentity().Node,
		PhysicalNode:        fixture.storage.LocalIdentity().Node,
		PhysicalIncarnation: 1,
		KeyDigest:           fixture.gateway.LocalPeerKeyDigest(),
		Roles:               serviceauthz.ServiceRoleGateway,
		Lifecycle:           serviceauthz.ServiceDraining,
		GatewayIncarnation:  1,
		SessionID:           [16]byte{2},
		SessionRevision:     3,
		ParticipantDigest:   [32]byte{4},
		DrainFenceDigest:    digest,
		DrainFence: serviceauthz.ServiceFence{
			Action:          serviceauthz.ServiceActionGatewayCatalogRead,
			Operation:       serviceauthz.ServiceOperationCatalogRead,
			Group:            state.Identity.Group,
			Relation:        relation,
			SessionID:       [16]byte{2},
			SessionRevision: 3,
			IntentID:        digest,
			FenceDigest:     digest,
		},
	}
	binding.InternalFences = []serviceauthz.ServiceFence{binding.DrainFence}
	directory, err := serviceauthz.NewServiceDirectoryGate(serviceauthz.ServiceDirectoryCut{
		Revision:        1,
		TrustDomain:     fixture.gateway.LocalIdentity().TrustDomain,
		PolicyGeneration: fixture.gate.Generation(),
		Bindings:        []serviceauthz.ServiceBinding{binding},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.server.BindServiceDirectoryGate(directory); err != nil {
		t.Fatal(err)
	}

	probe := fixture.probe()
	probe.Request.Authority = serviceauthz.Authority{
		Node: fixture.gateway.LocalIdentity().Node, Generation: fixture.gate.Generation(),
	}
	probe.Request.Capability = serviceauthz.CapabilityTopology
	reply := dispatchProbeForAuthority(t, fixture, probe)
	if reply.Response.Kind != ReplicatedHandshake || !reply.Response.HasState {
		t.Fatalf("exact draining owner probe=%+v", reply.Response)
	}
	if got := owner.probeCalls.Load(); got != 1 {
		t.Fatalf("owner probes=%d, want 1", got)
	}

	for _, test := range []struct {
		name   string
		mutate func(*ReplicatedCall)
	}{
		{name: "forwarded authority", mutate: func(call *ReplicatedCall) {
			call.Request.Authority.Node = rafttransport.NodeID{93}
		}},
		{name: "wrong manifest", mutate: func(call *ReplicatedCall) {
			call.Request.Fence.Command.RelationManifestDigest[0]++
		}},
		{name: "wrong group", mutate: func(call *ReplicatedCall) {
			call.Request.Fence.Group.GroupID[0]++
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := probe
			test.mutate(&candidate)
			response := dispatchProbeForAuthority(t, fixture, candidate)
			if response.Response.Kind != ReplicatedRefusal || response.Response.Refusal != ReplicatedRefusalUnauthorized {
				t.Fatalf("invalid probe response=%+v", response.Response)
			}
		})
	}
	wrongPeer := serviceauthz.AuthenticatedPeer{
		Identity:  fixture.gateway.LocalIdentity(),
		KeyDigest: [32]byte{9},
	}
	if fixture.server.authorizeReplicatedPeerWithDirectory(directory, wrongPeer, &probe.Request) {
		t.Fatal("wrong verified leaf key authorized draining owner probe")
	}

	retired := binding
	retired.Lifecycle = serviceauthz.ServiceDecommissioned
	retired.DrainFenceDigest = [32]byte{}
	retired.DrainFence = serviceauthz.ServiceFence{}
	retired.InternalFences = nil
	if err := directory.ApplyCommittedCut(serviceauthz.ServiceDirectoryCut{
		Revision:        2,
		TrustDomain:     fixture.gateway.LocalIdentity().TrustDomain,
		PolicyGeneration: fixture.gate.Generation(),
		Bindings:        []serviceauthz.ServiceBinding{retired},
	}); err != nil {
		t.Fatal(err)
	}
	response := dispatchProbeForAuthority(t, fixture, probe)
	if response.Response.Kind != ReplicatedRefusal || response.Response.Refusal != ReplicatedRefusalUnauthorized {
		t.Fatalf("retired owner probe response=%+v", response.Response)
	}
}

func dispatchProbeForAuthority(t *testing.T, fixture semanticServerFixture, call ReplicatedCall) *ReplicatedReply {
	t.Helper()
	lease, err := fixture.server.DispatchReplicated(t.Context(), call)
	if err != nil {
		if errors.Is(err, ErrReplicatedAuthentication) {
			t.Fatalf("probe authentication failed: %v", err)
		}
		t.Fatal(err)
	}
	reply, err := DetachReplicatedReply(lease)
	if err != nil {
		t.Fatal(err)
	}
	return reply
}
