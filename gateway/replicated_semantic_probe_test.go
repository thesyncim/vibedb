package gateway

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

var _ interface {
	ProbeReplicated(context.Context, ReplicatedRoute, ReplicatedEndpoint, serviceauthz.Capability) (*shardservice.ReplicatedResponse, error)
	probeCatalog(context.Context, ReplicatedRoute, ReplicatedEndpoint) (*shardservice.ReplicatedResponse, error)
} = (*ReplicatedNodeClient)(nil)

type semanticProbeOwner struct {
	*raftservice.Owner
	mu       sync.Mutex
	response shardservice.ReplicatedResponse
	err      error
}

func (owner *semanticProbeOwner) Probe(context.Context, raftmember.GroupKey) (raftservice.ServingState, error) {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	wire := owner.response.State
	return raftservice.ServingState{Identity: raftmember.RuntimeIdentity{Group: wire.Fence.Group,
		AllocationGeneration: wire.Fence.AllocationGeneration, MemberID: wire.Fence.MemberID,
		StoreID: wire.Fence.StoreID, NodeIncarnation: wire.Fence.NodeIncarnation, RelationManifestDigest: wire.Fence.Command.RelationManifestDigest},
		Command: wire.Fence.Command, Status: raftmember.RuntimeStatus{MemberID: wire.Fence.MemberID,
			LeaderID: wire.LeaderID, Term: wire.Fence.Term, Applied: wire.Applied, Commit: wire.Commit, CheckpointApplied: wire.CheckpointApplied}}, owner.err
}

func newSemanticProbeClient(t *testing.T, local bool, route ReplicatedRoute, endpoint ReplicatedEndpoint,
	state shardservice.ReplicatedMemberState, actor serviceauthz.Authority,
) (*ReplicatedNodeClient, *semanticProbeOwner) {
	t.Helper()
	owner := &semanticProbeOwner{response: shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedHandshake, HasState: true, State: state}}
	if !local {
		remote, _ := testPooledClient(t, endpoint, func(connection net.Conn) {
			defer connection.Close()
			for {
				request, err := shardservice.DecodeReplicatedRequest(connection)
				if err != nil {
					return
				}
				owner.mu.Lock()
				response, unavailable := owner.response, owner.err != nil
				owner.mu.Unlock()
				if unavailable {
					response = shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedRefusal, Refusal: shardservice.ReplicatedRefusalUnavailable}
				}
				if request.Authority != actor {
					response = shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedRefusal, Refusal: shardservice.ReplicatedRefusalUnauthorized}
				}
				if err := shardservice.EncodeReplicatedResponse(connection, &response); err != nil {
					return
				}
			}
		})
		t.Cleanup(func() { _ = remote.Close() })
		return &ReplicatedNodeClient{localNode: rafttransport.NodeID{0xff}, remote: remote}, owner
	}
	server, err := shardservice.NewReplicatedServer(owner, shardservice.DefaultReplicatedInFlightFrameBytes, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	credentials := newGatewayTLSAuthority(t)
	domain := rafttransport.TrustDomain{ClusterID: route.Group.ClusterID, ClusterIncarnation: route.Group.ClusterIncarnation}
	storage := credentials.profile(t, rafttransport.PeerIdentity{TrustDomain: domain, Node: endpoint.Node})
	principal := credentials.profile(t, rafttransport.PeerIdentity{TrustDomain: domain, Node: rafttransport.NodeID{92}})
	native, err := shardservice.NewReplicatedServerTLS(storage, []rafttransport.NodeID{principal.LocalIdentity().Node})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := serviceauthz.NewPolicy(actor.Generation, []serviceauthz.Entry{
		{Node: principal.LocalIdentity().Node, Capabilities: serviceauthz.CapabilityDelegate},
		{Node: actor.Node, Capabilities: serviceauthz.CapabilityDataRead | serviceauthz.CapabilityTopology},
	})
	if err != nil {
		t.Fatal(err)
	}
	gate, _ := serviceauthz.NewGate(policy)
	if err := server.BindAuthorization(gate, nil); err != nil {
		t.Fatal(err)
	}
	client, err := NewReplicatedNodeClient(native, principal, server, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if got := server.Stats().InFlightFrameBytes; got != 0 {
			t.Errorf("observer retained %d admission bytes", got)
		}
	})
	return client, owner
}

func TestReplicatedNodeDiscoveryRebindsIncarnationAndKeepsExactOperations(t *testing.T) {
	for _, local := range []bool{true, false} {
		name := "remote"
		if local {
			name = "local"
		}
		t.Run(name, func(t *testing.T) {
			route, _, states := testReplicatedRouteCommand(t)
			endpoint := route.Replicas[1]
			state := states[endpoint.Address]
			state.Fence.NodeIncarnation++
			actor := serviceauthz.Authority{Node: rafttransport.NodeID{93}, Generation: 5}
			ctx, _ := serviceauthz.WithAuthority(t.Context(), actor)
			client, owner := newSemanticProbeClient(t, local, route, endpoint, state, actor)
			executor, err := NewReplicatedExecutor(client, 1, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if !executor.parallelDiscovery() {
				t.Fatal("supported local/authenticated remote concurrency was lost")
			}
			observed, got, err := executor.discoverLeaderFresh(ctx, route, endpoint.Member, serviceauthz.CapabilityDataRead)
			if err != nil || got != state || observed.NodeIncarnation != state.Fence.NodeIncarnation {
				t.Fatalf("restart observation=%+v state=%+v err=%v", observed, got, err)
			}
			if route.Replicas[1] != endpoint {
				t.Fatal("probe rewrote catalog authority")
			}
			stats := client.Stats()
			if stats.LegacyCalls != 1 || stats.LocalCalls+stats.RemoteCalls != 1 || stats.SQLRequestEncodings != 0 || (local && stats.LocalCalls != 1) || (!local && stats.RemoteCalls != 1) {
				t.Fatalf("probe dispatch counters=%+v", stats)
			}
			request := &shardservice.ReplicatedRequest{Operation: shardservice.ReplicatedProbe, Authority: actor, Capability: serviceauthz.CapabilityDataRead,
				Fence: shardservice.ReplicatedFence{Group: route.Group, AllocationGeneration: route.AllocationGeneration}}
			if _, err := client.DoReplicated(ctx, endpoint, request); !errors.Is(err, ErrReplicatedRoute) {
				t.Fatalf("old exact incarnation accepted: %v", err)
			}
			if _, err := client.DoReplicated(ctx, observed, request); err != nil {
				t.Fatalf("observed exact incarnation refused: %v", err)
			}
			for _, mutation := range []string{"store", "group", "command", "rollback", "physical"} {
				wrongRoute, wrongEndpoint := route, endpoint
				switch mutation {
				case "store":
					wrongEndpoint.StoreID[0]++
				case "group":
					wrongRoute.Group.GroupID[0]++
				case "command":
					wrongRoute.Command.SchemaGeneration++
				case "rollback":
					wrongEndpoint.NodeIncarnation = state.Fence.NodeIncarnation + 1
				case "physical":
					wrongEndpoint.Node[0]++
				}
				if _, err := client.ProbeReplicated(ctx, wrongRoute, wrongEndpoint, serviceauthz.CapabilityDataRead); err == nil {
					t.Fatalf("%s substitution accepted", mutation)
				}
			}
			stale := actor
			stale.Generation--
			staleCtx, _ := serviceauthz.WithAuthority(t.Context(), stale)
			refused, err := client.ProbeReplicated(staleCtx, route, endpoint, serviceauthz.CapabilityDataRead)
			if err != nil || !validReplicatedUnauthorizedWithoutState(refused) {
				t.Fatalf("stale actor response=%+v err=%v", refused, err)
			}
			owner.mu.Lock()
			owner.err = errors.New("group not installed yet")
			owner.mu.Unlock()
			if _, err := client.ProbeReplicated(ctx, route, endpoint, serviceauthz.CapabilityDataRead); !isSemanticProbeUnavailable(err) {
				t.Fatalf("pre-load refusal lost its classification: %v", err)
			}
			owner.mu.Lock()
			owner.err = nil
			owner.mu.Unlock()
			if _, err := client.ProbeReplicated(ctx, route, endpoint, serviceauthz.CapabilityDataRead); err != nil {
				t.Fatalf("loaded group could not be observed: %v", err)
			}
		})
	}
}

func TestReplicatedNodeCatalogProbePreservesRestrictedProgression(t *testing.T) {
	for _, local := range []bool{true, false} {
		route, _, states := testReplicatedRouteCommand(t)
		route.Distribution, route.Shard = ReplicatedCatalogDistribution, ReplicatedCatalogShard
		endpoint := route.Replicas[1]
		state := states[endpoint.Address]
		state.Fence.NodeIncarnation++
		state.Fence.Command.OwnershipEpoch++
		state.Fence.Command.RoutingVersion++
		state.Fence.Command.RouteGeneration++
		actor := serviceauthz.Authority{Node: rafttransport.NodeID{93}, Generation: 5}
		ctx, _ := serviceauthz.WithAuthority(t.Context(), actor)
		client, owner := newSemanticProbeClient(t, local, route, endpoint, state, actor)
		if response, err := client.probeCatalog(ctx, route, endpoint); err != nil || response.State != state {
			t.Fatalf("catalog observation local=%t response=%+v err=%v", local, response, err)
		}
		if _, err := client.ProbeReplicated(ctx, route, endpoint, serviceauthz.CapabilityTopology); !errors.Is(err, ErrReplicatedRoute) {
			t.Fatalf("ordinary observation accepted catalog progression: %v", err)
		}
		owner.mu.Lock()
		owner.response.State.Fence.Command.ActivePolicyGeneration++
		owner.mu.Unlock()
		if _, err := client.probeCatalog(ctx, route, endpoint); !errors.Is(err, ErrReplicatedRoute) {
			t.Fatalf("catalog observation accepted policy substitution: %v", err)
		}
	}
	client := &ReplicatedNodeClient{remote: &semanticRecordingRemote{}}
	executor, _ := NewReplicatedExecutor(client, 1, time.Second)
	if executor.parallelDiscovery() {
		t.Fatal("arbitrary remote gained concurrent discovery")
	}
	var absent *ReplicatedNodeClient
	if absent.parallelReplicatedDiscoveryEnabled() {
		t.Fatal("nil client advertises discovery")
	}
}

func isSemanticProbeUnavailable(err error) bool {
	var refusal *ReplicatedRefusalError
	return errors.As(err, &refusal) && refusal.Code == shardservice.ReplicatedRefusalUnavailable
}

func TestReplicatedUnavailableProbeStillValidatesAttachedFence(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	endpoint := route.Replicas[1]
	response := shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedRefusal, Refusal: shardservice.ReplicatedRefusalUnavailable,
		HasState: true, State: states[endpoint.Address]}
	if _, err := bindReplicatedObservation(route, endpoint, &response); !isSemanticProbeUnavailable(err) {
		t.Fatalf("matching unavailable fence=%v", err)
	}
	response.State.Fence.StoreID[0]++
	if _, err := bindReplicatedObservation(route, endpoint, &response); !errors.Is(err, ErrReplicatedRoute) {
		t.Fatalf("wrong unavailable fence=%v", err)
	}
	response.HasState = false
	if _, err := bindReplicatedObservation(route, endpoint, &response); !errors.Is(err, ErrReplicatedRoute) {
		t.Fatalf("malformed unavailable state=%v", err)
	}
}
