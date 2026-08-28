//go:build darwin || linux

package main

import (
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/shardservice"
)

func TestRF3CompositionUsesDistinctAuthenticatedActors(t *testing.T) {
	client, gatewayNode := rf3CompositionClientNodes()
	nodes, group, target := rf3CommandNodes(), rf3CommandGroup(), rafttransport.NodeID{0x71}
	actors := append(append([]rafttransport.NodeID(nil), nodes[:]...), target, client, gatewayNode)
	credentials, roots, err := rf3testfixture.WriteCredentials(t.TempDir(), rf3CommandIdentityOID,
		rafttransport.TrustDomain{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation}, actors)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := serviceauthz.Load(rf3CommandPolicyWithTarget(nodes, target, client, gatewayNode))
	if err != nil {
		t.Fatal(err)
	}
	profiles := make([]*rafttransport.PeerTLS, len(actors))
	for i := range actors {
		profiles[i], err = servicetls.LoadProfile(credentials[i].Certificate, credentials[i].Key, roots,
			"1.3.6.1.4.1.32473.1.1", time.Now)
		if err != nil || profiles[i].LocalIdentity().Node != actors[i] {
			t.Fatalf("actor %d profile: %v", i, err)
		}
	}
	for _, actor := range []rafttransport.NodeID{client, gatewayNode} {
		for _, capability := range []serviceauthz.Capability{serviceauthz.CapabilityMembership, serviceauthz.CapabilityTopology, serviceauthz.CapabilityDelegate, serviceauthz.CapabilityDataRead} {
			if policy.Check(actor, capability) != serviceauthz.DecisionAllow {
				t.Fatalf("actor %x lacks explicit capability %d", actor, capability)
			}
		}
	}
	for _, link := range []struct {
		from, to int
		class    rafttransport.TrafficClass
		refused  bool
	}{{4, 3, rafttransport.TrafficShardControl, false}, {5, 3, rafttransport.TrafficShardNative, false}, {4, 5, rafttransport.TrafficGatewayClient, false}, {3, 3, rafttransport.TrafficShardControl, true}} {
		if !link.refused && actors[link.from] == actors[link.to] {
			t.Fatal("fixture reused serving identity for its client")
		}
		left, right := net.Pipe()
		deadline := func() time.Time { return time.Now().Add(3 * time.Second) }
		result := make(chan error, 1)
		go func() {
			connection, err := profiles[link.to].Server(t.Context(), right, link.class, deadline)
			if err == nil && connection.PeerIdentity().Node != actors[link.from] {
				err = fmt.Errorf("wrong authenticated caller")
			}
			result <- err
		}()
		connection, clientErr := profiles[link.from].Client(t.Context(), left, actors[link.to], link.class, deadline)
		serverErr := <-result
		_ = left.Close()
		_ = right.Close()
		if link.refused {
			if !errors.Is(clientErr, rafttransport.ErrPeerAuthentication) && !errors.Is(serverErr, rafttransport.ErrPeerAuthentication) {
				t.Fatalf("production self-peer guard bypassed: client=%v server=%v", clientErr, serverErr)
			}
			continue
		}
		if clientErr != nil || serverErr != nil || connection.PeerIdentity().Node != actors[link.to] {
			t.Fatalf("authenticated actor link %d→%d: client=%v server=%v", link.from, link.to, clientErr, serverErr)
		}
	}
}

func TestRF3CompositionCatalogKeepsTransportRolesDistinct(t *testing.T) {
	fixture := gatewayHotShardLiveFixture{nodes: rf3CommandNodes(), group: rf3CommandGroup(),
		targetNode: rafttransport.NodeID{0x71}, targetStore: [16]byte{0x81}, targetIncarnation: 1,
		targetListeners: rf3ManifestListeners{Peer: "127.0.0.1:19001", Native: "127.0.0.1:19002"}}
	states := make([]shardservice.ReplicatedMemberState, rf3CommandMembers)
	links := make([]*gatewayHotShardNetworkLink, rf3CommandMembers+1)
	command := raftservice.CommandFence{ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1,
		OwnershipEpoch: 1, SchemaGeneration: 1, RoutingVersion: 1, RouteGeneration: 1, RelationManifestDigest: [32]byte{1}}
	for i := range states {
		fixture.peerAddresses[i], fixture.nativeAddresses[i] = fmt.Sprintf("127.0.0.1:%d", 19100+i), fmt.Sprintf("127.0.0.1:%d", 19200+i)
		states[i].Fence.Command, states[i].Fence.StoreID, states[i].Fence.NodeIncarnation = command, [16]byte{byte(i + 1)}, 1
	}
	for i := range links {
		links[i] = newGatewayHotShardNetworkLink(t, "127.0.0.1:1")
		defer links[i].close()
	}
	snapshot := gatewayHotShardSnapshotForLogical(t, fixture, states, links, [32]byte{2})
	var endpoints [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	route, ok := snapshot.ResolveReplicatedRoute(gateway.ReplicatedCatalogDistribution, gateway.ReplicatedCatalogShard, endpoints[:0])
	if !ok {
		t.Fatal("fixture catalog route missing")
	}
	for i, endpoint := range route.Replicas {
		if endpoint.DataAddress != fixture.peerAddresses[i] || endpoint.Address != fixture.nativeAddresses[i] || endpoint.ControlAddress != links[i].address() {
			t.Fatalf("replica %d conflated authenticated transport roles: %+v", i, endpoint)
		}
	}
}
