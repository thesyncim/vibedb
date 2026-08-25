package gateway

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

func TestReplicatedLeaderHintCacheExactFenceIdentityAndSafeInvalidation(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	endpoint := route.Replicas[1]
	old := states[endpoint.Address]
	cache := newReplicatedLeaderHintCache(4)
	cache.publish(route, endpoint, old)
	if gotEndpoint, gotState, ok := cache.lookup(route); !ok ||
		!sameReplicatedEndpoint(gotEndpoint, endpoint) || gotState != old {
		t.Fatalf("initial lookup = %+v %+v %t", gotEndpoint, gotState, ok)
	}

	changedStore := route
	changedStore.Replicas = append([]ReplicatedEndpoint(nil), route.Replicas...)
	changedStore.Replicas[1].StoreID[0]++
	if _, _, ok := cache.lookup(changedStore); ok {
		t.Fatal("store-mismatched route reused a leader hint")
	}
	changedIncarnation := route
	changedIncarnation.Replicas = append([]ReplicatedEndpoint(nil), route.Replicas...)
	changedIncarnation.Replicas[1].NodeIncarnation++
	if _, _, ok := cache.lookup(changedIncarnation); ok {
		t.Fatal("incarnation-mismatched route reused a leader hint")
	}
	changedFence := route
	changedFence.Command.RouteGeneration++
	if _, _, ok := cache.lookup(changedFence); ok {
		t.Fatal("command-fence-mismatched route reused a leader hint")
	}

	newer := old
	newer.Fence.Term++
	newer.Commit++
	cache.publish(route, endpoint, newer)
	cache.publish(route, endpoint, old)
	if _, got, ok := cache.lookup(route); !ok || got != newer {
		t.Fatalf("delayed publication replaced newer hint: %+v %t", got, ok)
	}
	cache.invalidate(route, endpoint, old)
	if _, got, ok := cache.lookup(route); !ok || got != newer {
		t.Fatalf("delayed invalidation removed newer hint: %+v %t", got, ok)
	}
	cache.invalidate(route, endpoint, newer)
	if _, _, ok := cache.lookup(route); ok {
		t.Fatal("exact invalidation retained consumed hint")
	}
}

func TestReplicatedLeaderHintCacheWarmOperationsAllocateNothing(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	endpoint := route.Replicas[1]
	state := states[endpoint.Address]
	cache := newReplicatedLeaderHintCache(4)
	cache.publish(route, endpoint, state)
	if allocations := testing.AllocsPerRun(1000, func() {
		cache.publish(route, endpoint, state)
		if _, _, ok := cache.lookup(route); !ok {
			panic("missing warm hint")
		}
	}); allocations != 0 {
		t.Fatalf("warm leader cache allocations = %v", allocations)
	}
}

func testReplicatedLeaderHintRoute(
	base ReplicatedRoute,
	baseState shardservice.ReplicatedMemberState,
	generation uint64,
) (ReplicatedRoute, shardservice.ReplicatedMemberState) {
	route := base
	route.Command.RouteGeneration = generation
	state := baseState
	state.Fence.Command = route.Command
	return route, state
}

func TestReplicatedLeaderHintCacheFourWayCollisionRetentionAndBoundedEviction(t *testing.T) {
	base, _, states := testReplicatedRouteCommand(t)
	endpoint := base.Replicas[1]
	baseState := states[endpoint.Address]
	cache := newReplicatedLeaderHintCache(4)
	var routes [5]ReplicatedRoute
	for index := range routes {
		state := shardservice.ReplicatedMemberState{}
		routes[index], state = testReplicatedLeaderHintRoute(
			base, baseState, base.Command.RouteGeneration+uint64(index),
		)
		cache.publish(routes[index], endpoint, state)
		if _, got, ok := cache.lookup(routes[index]); !ok || got != state {
			t.Fatalf("route %d missing immediately after publication: %+v %t", index, got, ok)
		}
	}
	if _, _, ok := cache.lookup(routes[0]); ok {
		t.Fatal("fifth colliding route did not evict the oldest of four ways")
	}
	for index := 1; index < len(routes); index++ {
		if _, _, ok := cache.lookup(routes[index]); !ok {
			t.Fatalf("colliding route %d was needlessly evicted", index)
		}
	}
}

func TestReplicatedLeaderHintCacheConcurrentOldResponsesCannotEraseNewTerm(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	endpoint := route.Replicas[1]
	old := states[endpoint.Address]
	newer := old
	newer.Fence.Term++
	newer.Commit++
	cache := newReplicatedLeaderHintCache(4)
	cache.publish(route, endpoint, old)

	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for iteration := 0; iteration < 1000; iteration++ {
				cache.publish(route, endpoint, newer)
				cache.publish(route, endpoint, old)
				cache.invalidate(route, endpoint, old)
			}
		}()
	}
	workers.Wait()
	if _, got, ok := cache.lookup(route); !ok || got != newer {
		t.Fatalf("concurrent stale response erased newer term: %+v %t", got, ok)
	}
}

func TestReplicatedExecutorLeaderHintCapacityOptions(t *testing.T) {
	client := &bypassLeaderHintClient{}
	defaultExecutor, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(defaultExecutor.leaderHints.sets); got != DefaultReplicatedLeaderHintCapacity/4 {
		t.Fatalf("default leader hint sets = %d", got)
	}

	for _, capacity := range []int{1, 3, 4, 5, 4097, AbsoluteMaxReplicatedLeaderHints} {
		executor, constructErr := NewReplicatedExecutorWithOptions(client, ReplicatedExecutorOptions{
			MaxAttempts: 1, AttemptTimeout: time.Second, LeaderHintCapacity: capacity,
		})
		if constructErr != nil {
			t.Fatalf("capacity %d: %v", capacity, constructErr)
		}
		entries := 0
		for index := range executor.leaderHints.sets {
			entries += int(executor.leaderHints.sets[index].ways)
		}
		if entries != capacity {
			t.Fatalf("capacity %d constructed %d entries", capacity, entries)
		}
	}
	for _, capacity := range []int{-1, AbsoluteMaxReplicatedLeaderHints + 1} {
		if _, err := NewReplicatedExecutorWithOptions(client, ReplicatedExecutorOptions{
			MaxAttempts: 1, AttemptTimeout: time.Second, LeaderHintCapacity: capacity,
		}); !errors.Is(err, ErrReplicatedRoute) {
			t.Fatalf("invalid capacity %d = %v", capacity, err)
		}
	}
}

type bypassLeaderHintClient struct {
	probes    int
	proposals int
}

func (client *bypassLeaderHintClient) DoReplicated(
	_ context.Context,
	_ ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	if request.Operation == shardservice.ReplicatedProbe {
		client.probes++
		return nil, errors.New("fresh probe required")
	}
	client.proposals++
	return nil, errors.New("cached proposal was unsafe")
}

func TestReplicatedExecutorUnknownRetryBypassesWarmLeaderHint(t *testing.T) {
	route, command, states := testReplicatedRouteCommand(t)
	client := &bypassLeaderHintClient{}
	executor, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	executor.leaderHints.publish(route, route.Replicas[1], states["m2"])
	_, err = executor.RetryUnknown(context.Background(), route, command)
	if err == nil || client.probes != len(route.Replicas) || client.proposals != 0 {
		t.Fatalf("error=%v probes=%d proposals=%d", err, client.probes, client.proposals)
	}
}

func TestReplicatedExecutorNormalDiscoveryConsumesWarmLeaderHint(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	client := &bypassLeaderHintClient{}
	executor, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	wantEndpoint, wantState := route.Replicas[1], states["m2"]
	executor.leaderHints.publish(route, wantEndpoint, wantState)
	endpoint, state, err := executor.discoverLeader(
		context.Background(), route, route.Replicas[0].Member, serviceauthz.CapabilityDataWrite,
	)
	if err != nil || !sameReplicatedEndpoint(endpoint, wantEndpoint) || state != wantState ||
		client.probes != 0 || client.proposals != 0 {
		t.Fatalf("endpoint=%+v state=%+v error=%v probes=%d proposals=%d",
			endpoint, state, err, client.probes, client.proposals)
	}
}

func BenchmarkReplicatedLeaderHintCacheLookup(b *testing.B) {
	route, _, states := testReplicatedRouteCommand(b)
	endpoint := route.Replicas[1]
	state := states[endpoint.Address]
	cache := newReplicatedLeaderHintCache(DefaultReplicatedLeaderHintCapacity)
	cache.publish(route, endpoint, state)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _, _ = cache.lookup(route)
	}
}
