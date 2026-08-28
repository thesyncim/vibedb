package gateway

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

func TestAuthenticatedReplicatedDiscoveryRebindsRestartButOperationsRemainExact(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	endpoint := route.Replicas[1]
	state := states[endpoint.Address]
	state.Fence.NodeIncarnation++ // Same durable store after BeginIncarnation.
	client, _ := testPooledClient(t, endpoint, func(server net.Conn) {
		defer server.Close()
		for {
			request, err := shardservice.DecodeReplicatedRequest(server)
			if err != nil {
				return
			}
			if request.Authority != (serviceauthz.Authority{Node: [16]byte{7}, Generation: 5}) {
				t.Error("probe lost authenticated observer authority")
				return
			}
			if err := shardservice.EncodeReplicatedResponse(server, &shardservice.ReplicatedResponse{
				Kind: shardservice.ReplicatedHandshake, HasState: true, State: state,
			}); err != nil {
				return
			}
		}
	})
	defer client.Close()
	ctx, err := serviceauthz.WithAuthority(t.Context(), serviceauthz.Authority{Node: [16]byte{7}, Generation: 5})
	if err != nil {
		t.Fatal(err)
	}
	request := &shardservice.ReplicatedRequest{Operation: shardservice.ReplicatedProbe,
		Authority: serviceauthz.Authority{Node: [16]byte{7}, Generation: 5}, Capability: serviceauthz.CapabilityDataRead,
		Fence: shardservice.ReplicatedFence{Group: route.Group, AllocationGeneration: route.AllocationGeneration}}
	if _, err := client.DoReplicated(ctx, endpoint, request); !errors.Is(err, ErrReplicatedRoute) {
		t.Fatalf("strict old incarnation accepted: %v", err)
	}
	executor, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	observed, got, err := executor.discoverLeaderFresh(ctx, route, endpoint.Member, serviceauthz.CapabilityDataRead)
	if err != nil || got != state || observed.NodeIncarnation != state.Fence.NodeIncarnation {
		t.Fatalf("restart discovery: endpoint=%+v state=%+v err=%v", observed, got, err)
	}
	if route.Replicas[1] != endpoint {
		t.Fatal("discovery mutated catalog authority")
	}
	if cached, _, ok := executor.leaderHints.lookup(route); !ok || cached != observed {
		t.Fatal("restarted leader was not retained as an exact bounded hint")
	}
	if _, err := executor.doReplicated(ctx, observed, request); err != nil {
		t.Fatalf("exact observed incarnation rejected: %v", err)
	}
	if _, err := executor.doReplicated(ctx, endpoint, request); !errors.Is(err, ErrReplicatedRoute) {
		t.Fatalf("old incarnation bypassed exact operation check: %v", err)
	}
}

func TestMembershipStableObservationRejectsOtherFenceChanges(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	endpoint := route.Replicas[1]
	response := shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedHandshake,
		HasState: true, State: states[endpoint.Address]}
	response.State.Fence.Command.ReplicaSetVersion++
	if _, err := bindReplicatedObservation(route, endpoint, &response); !errors.Is(err, ErrReplicatedRoute) {
		t.Fatalf("legacy route accepted changed membership: %v", err)
	}
	route.membershipStable = true
	if _, err := bindReplicatedObservation(route, endpoint, &response); err != nil {
		t.Fatalf("stable route rejected membership advancement: %v", err)
	}
	for name, change := range map[string]func(*shardservice.ReplicatedResponse){
		"group":       func(r *shardservice.ReplicatedResponse) { r.State.Fence.Group.GroupID[0]++ },
		"allocation":  func(r *shardservice.ReplicatedResponse) { r.State.Fence.AllocationGeneration++ },
		"member":      func(r *shardservice.ReplicatedResponse) { r.State.Fence.MemberID++ },
		"store":       func(r *shardservice.ReplicatedResponse) { r.State.Fence.StoreID[0]++ },
		"incarnation": func(r *shardservice.ReplicatedResponse) { r.State.Fence.NodeIncarnation = 0 },
		"membership-rollback": func(r *shardservice.ReplicatedResponse) {
			r.State.Fence.Command.ReplicaSetVersion = route.Command.ReplicaSetVersion - 1
		},
		"schema":     func(r *shardservice.ReplicatedResponse) { r.State.Fence.Command.SchemaGeneration++ },
		"manifest":   func(r *shardservice.ReplicatedResponse) { r.State.Fence.Command.RelationManifestDigest[0]++ },
		"policy":     func(r *shardservice.ReplicatedResponse) { r.State.Fence.Command.ActivePolicyGeneration++ },
		"protection": func(r *shardservice.ReplicatedResponse) { r.State.Fence.Command.ProtectionEpoch++ },
		"ownership":  func(r *shardservice.ReplicatedResponse) { r.State.Fence.Command.OwnershipEpoch++ },
		"routing":    func(r *shardservice.ReplicatedResponse) { r.State.Fence.Command.RoutingVersion++ },
		"generation": func(r *shardservice.ReplicatedResponse) { r.State.Fence.Command.RouteGeneration++ },
	} {
		t.Run(name, func(t *testing.T) {
			wrong := response
			change(&wrong)
			if _, err := bindReplicatedObservation(route, endpoint, &wrong); !errors.Is(err, ErrReplicatedRoute) {
				t.Fatalf("accepted substituted observation: %v", err)
			}
		})
	}
}

func TestReplicatedIncarnationObservationRejectsSubstitutionAndRollback(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	endpoint := route.Replicas[1]
	response := shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedHandshake,
		HasState: true, State: states[endpoint.Address]}
	response.State.Fence.NodeIncarnation++
	for _, test := range []struct {
		name   string
		mutate func(*shardservice.ReplicatedResponse)
	}{
		{"group", func(r *shardservice.ReplicatedResponse) { r.State.Fence.Group.GroupID[0]++ }},
		{"allocation", func(r *shardservice.ReplicatedResponse) { r.State.Fence.AllocationGeneration++ }},
		{"member", func(r *shardservice.ReplicatedResponse) { r.State.Fence.MemberID++ }},
		{"store", func(r *shardservice.ReplicatedResponse) { r.State.Fence.StoreID[0]++ }},
		{"command", func(r *shardservice.ReplicatedResponse) { r.State.Fence.Command.SchemaGeneration++ }},
		{"rollback", func(r *shardservice.ReplicatedResponse) { r.State.Fence.NodeIncarnation = endpoint.NodeIncarnation - 1 }},
		{"term", func(r *shardservice.ReplicatedResponse) { r.State.Fence.Term = 0 }},
		{"refusal", func(r *shardservice.ReplicatedResponse) { r.Kind = shardservice.ReplicatedRefusal }},
		{"missing", func(r *shardservice.ReplicatedResponse) { r.HasState = false }},
		{"payload", func(r *shardservice.ReplicatedResponse) { r.Value = []byte{1} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := response
			test.mutate(&changed)
			if _, err := bindReplicatedObservation(route, endpoint, &changed); !errors.Is(err, ErrReplicatedRoute) {
				t.Fatalf("accepted substituted observation: %v", err)
			}
		})
	}
	response.State.LeaderID = 0
	if _, err := bindReplicatedObservation(route, endpoint, &response); err != nil {
		t.Fatalf("valid isolated member status refused: %v", err)
	}
	client := new(AuthenticatedReplicatedClient)
	for _, ctx := range []context.Context{nil, t.Context()} {
		if _, err := client.ProbeReplicated(ctx, route, endpoint, serviceauthz.CapabilityTopology); !errors.Is(err, ErrReplicatedUnauthorized) {
			t.Fatalf("unauthorized observation reached transport: %v", err)
		}
	}
}

func TestReplicatedLeaderHintRetainsIncarnationMonotonicity(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	oldEndpoint := route.Replicas[1]
	oldState := states[oldEndpoint.Address]
	newEndpoint, newState := oldEndpoint, oldState
	newEndpoint.NodeIncarnation++
	newState.Fence.NodeIncarnation++
	cache := newReplicatedLeaderHintCache(4)
	cache.publish(route, newEndpoint, newState)
	cache.publish(route, oldEndpoint, oldState)
	cache.invalidate(route, oldEndpoint, oldState)
	if endpoint, state, ok := cache.lookup(route); !ok || endpoint != newEndpoint || state != newState {
		t.Fatalf("delayed pre-restart response replaced new incarnation: %+v %+v %t", endpoint, state, ok)
	}
}
