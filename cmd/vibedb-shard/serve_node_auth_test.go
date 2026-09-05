package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/shardservice"
)

type rf3PreparedGatewayOwner struct {
	*raftservice.Owner
	state raftservice.ServingState
}

func (owner *rf3PreparedGatewayOwner) Probe(context.Context, raftmember.GroupKey) (raftservice.ServingState, error) {
	return owner.state, nil
}

func TestRF3EmbeddedGatewayBindsBeforeNativeAdmissionWithIndependentRights(t *testing.T) {
	manifest, err := parseRF3Manifest([]byte(multiGroupRF3Manifest(t)))
	if err != nil {
		t.Fatal(err)
	}
	group := manifest.Groups[0]
	storageNode, frontendNode := group.Members[0].NodeID, rafttransport.NodeID{0xf1}
	root := t.TempDir()
	domain := rafttransport.TrustDomain{ClusterID: group.Route.Group.ClusterID, ClusterIncarnation: group.Route.Group.ClusterIncarnation}
	credentials, roots, err := rf3testfixture.WriteCredentials(root, rf3CommandIdentityOID, domain, []rafttransport.NodeID{storageNode, frontendNode})
	if err != nil {
		t.Fatal(err)
	}
	policyPath := filepath.Join(root, "policy.vibejson")
	policyRaw := []byte(fmt.Sprintf(`{"generation":1,"principals":[{"node":"%x","capabilities":["data_read","data_write"]},{"node":"%x","capabilities":["data_read","data_write","delegate","topology"]}]}`, storageNode, frontendNode))
	if err := os.WriteFile(policyPath, policyRaw, 0600); err != nil {
		t.Fatal(err)
	}
	policy, err := serviceauthz.Load(policyRaw)
	if err != nil {
		t.Fatal(err)
	}
	storage, err := servicetls.LoadProfile(credentials[0].Certificate, credentials[0].Key, roots, rf3CommandIdentityOID.String(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	manifest.Listeners.Native = listener.Addr().String()
	manifest.Gateway = &rf3ManifestGateway{
		CatalogPath: filepath.Join(root, "absent-catalog"), CatalogRouteSeedPath: filepath.Join(root, "absent-route-seed"),
		CatalogSessionJournal: filepath.Join(root, "absent-session"), DurableAckKeyPath: filepath.Join(root, "absent-ack-key"),
		CatalogClientID: strings.Repeat("a", 32), CatalogRetryHome: strings.Repeat("b", 16),
		TLS:                 rf3ManifestTLS{Certificate: credentials[1].Certificate, Key: credentials[1].Key, Roots: roots, IdentityOID: rf3CommandIdentityOID.String()},
		AuthorizationPolicy: policyPath,
	}
	for index, member := range group.Members {
		address := fmt.Sprintf("127.0.0.1:%d", 28000+index)
		if index == 0 {
			address = manifest.Listeners.Native
		}
		manifest.Gateway.ShardPeers = append(manifest.Gateway.ShardPeers, rf3ManifestGatewayPeer{Address: address, NodeID: fmt.Sprintf("%x", member.NodeID)})
	}
	frontend, frontendPolicy, err := loadRF3EmbeddedGatewayCredentials(manifest, storage, policy)
	if err != nil {
		t.Fatal(err)
	}
	identity := raftmember.RuntimeIdentity{Group: group.Route.Group, AllocationGeneration: group.Route.AllocationGeneration,
		MemberID: group.Route.MemberID, StoreID: group.Route.StoreID, NodeIncarnation: 1, RelationManifestDigest: [32]byte{1}}
	owner := &rf3PreparedGatewayOwner{state: raftservice.ServingState{Identity: identity,
		Command: commandFenceFromPublication(rf3CommandAuthority(), identity, 1),
		Status:  raftmember.RuntimeStatus{MemberID: identity.MemberID, LeaderID: identity.MemberID, Term: 1, Applied: 1, Commit: 1}}}
	server, err := shardservice.NewReplicatedServer(owner, shardservice.DefaultReplicatedInFlightFrameBytes, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	gate, _ := serviceauthz.NewGate(policy)
	if err := server.BindAuthorization(gate, nil); err != nil {
		t.Fatal(err)
	}
	nativeTLS, err := shardservice.NewReplicatedServerTLS(storage, []rafttransport.NodeID{frontendNode})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := prepareRF3EmbeddedGateway(manifest, frontend, frontendPolicy, nativeTLS, server)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.remote.Close()
	if prepared.runtime != nil {
		t.Fatal("credential preparation opened the absent catalog before services started")
	}
	admission := newRF3AcceptReadyListener(listener)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- server.ServeAuthenticated(ctx, admission, nativeTLS, servicetls.FixedDeadline(time.Second), 4, 2)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err := componentShutdownError(err); err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("native service did not drain")
		}
	})
	readyCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	if err := waitRF3ServiceAdmission(readyCtx, admission); err != nil {
		t.Fatal(err)
	}
	request := shardservice.ReplicatedRequest{Operation: shardservice.ReplicatedProbe,
		Authority: serviceauthz.Authority{Node: frontendNode, Generation: 1}, Capability: serviceauthz.CapabilityDataRead,
		Fence: shardservice.ReplicatedFence{Group: group.Route.Group, AllocationGeneration: identity.AllocationGeneration}}
	endpoint := gateway.ReplicatedEndpoint{Address: manifest.Listeners.Native, Node: storageNode, Member: identity.MemberID, StoreID: identity.StoreID, NodeIncarnation: identity.NodeIncarnation}
	reply, err := prepared.client.DoReplicated(ctx, endpoint, &request)
	if err != nil || reply == nil || reply.Kind != shardservice.ReplicatedHandshake {
		t.Fatalf("prepared local probe=%+v err=%v", reply, err)
	}
	if prepared.client.Stats().LocalCalls != 1 || server.Stats().Accepted != 0 ||
		policy.Check(storageNode, serviceauthz.CapabilityDelegate) == serviceauthz.DecisionAllow {
		t.Fatal("local frontend substituted storage rights or used a native socket")
	}
	manifest.Gateway.TLS.Certificate, manifest.Gateway.TLS.Key = credentials[0].Certificate, credentials[0].Key
	if _, _, err := loadRF3EmbeddedGatewayCredentials(manifest, storage, policy); err == nil {
		t.Fatal("shared storage/frontend principal accepted")
	}
}
