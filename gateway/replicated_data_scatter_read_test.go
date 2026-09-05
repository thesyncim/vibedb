package gateway

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/shardservice"
)

type scatterReadClient struct {
	mu             sync.Mutex
	states         map[raftmember.GroupKey]map[uint64]shardservice.ReplicatedMemberState
	values         map[raftmember.GroupKey][][]byte
	reads          map[raftmember.GroupKey]int
	intentGroup    raftmember.GroupKey
	staleGroup     raftmember.GroupKey
	staleCommand   raftservice.CommandFence
	staleRemaining bool
	delay          time.Duration
	active         atomic.Int64
	maxActive      atomic.Int64
}

func (client *scatterReadClient) DoReplicated(
	ctx context.Context,
	endpoint ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	client.mu.Lock()
	state := client.states[request.Fence.Group][endpoint.Member]
	client.mu.Unlock()
	if request.Operation == shardservice.ReplicatedProbe {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedHandshake,
			HasState: true, State: state}, nil
	}
	active := client.active.Add(1)
	for maximum := client.maxActive.Load(); active > maximum &&
		!client.maxActive.CompareAndSwap(maximum, active); maximum = client.maxActive.Load() {
	}
	defer client.active.Add(-1)
	if client.delay != 0 {
		timer := time.NewTimer(client.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-timer.C:
		}
	}
	batch, err := replicatedstate.OpenPointReadBatch(request.BatchRead)
	if err != nil || request.Operation != shardservice.ReplicatedReadBatchLeader {
		return nil, ErrReplicatedRoute
	}
	client.mu.Lock()
	client.reads[request.Fence.Group]++
	if request.Fence.Group == client.staleGroup && client.staleRemaining {
		client.staleRemaining = false
		for member, next := range client.states[request.Fence.Group] {
			next.Fence.Command = client.staleCommand
			client.states[request.Fence.Group][member] = next
		}
		state.Fence.Command = client.staleCommand
		client.mu.Unlock()
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedRefusal,
			Refusal:  shardservice.ReplicatedRefusalStaleFence,
			HasState: true, State: state}, nil
	}
	values := client.values[request.Fence.Group]
	intent := request.Fence.Group == client.intentGroup
	client.mu.Unlock()
	if intent {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedRefusal,
			Refusal:  shardservice.ReplicatedRefusalReadIntentActive,
			HasState: true, State: state}, nil
	}
	if batch.Count() != len(values) {
		return nil, ErrReplicatedRoute
	}
	packed := appendScatterValues(nil, values)
	return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedReadBatchResult,
		HasState: true, State: state, ReadApplied: state.Applied, Value: packed}, nil
}

func appendScatterValues(dst []byte, values [][]byte) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(values)))
	bitmapStart := len(dst)
	dst = append(dst, make([]byte, (len(values)+7)/8+len(values)*4)...)
	lengthStart := bitmapStart + (len(values)+7)/8
	for index := range values {
		dst[bitmapStart+index/8] |= 1 << uint(index&7)
		binary.LittleEndian.PutUint32(dst[lengthStart+index*4:], uint32(len(values[index])))
		dst = append(dst, values[index]...)
	}
	return dst
}

type scatterCatalogFixture struct {
	snapshot    *Snapshot
	request     ReplicatedTableBatchReadRequest
	routes      []ResolvedReplicatedTableKey
	descriptors []ReplicatedShardDescriptor
	config      distribution.ClusterConfig
	endpoints   map[distribution.EndpointID]string
	profiles    []ReplicatedTableProfile
}

func newScatterCatalogFixture(t testing.TB, groupCount int, generation uint64) scatterCatalogFixture {
	t.Helper()
	_, endpoints, baseDescriptor, baseProfile := testReplicatedTableInput(t)
	fixture := scatterCatalogFixture{endpoints: endpoints}
	fixture.request.MaxResultBytes = 1 << 20
	for index := range groupCount {
		distributionName := distribution.DistributionName(fmt.Sprintf("scatter-%03d", index))
		table := fmt.Sprintf("table_%03d", index)
		manifest, err := distribution.NewManifest(distributionName, 3, []distribution.Shard{{
			ID: "all", AllocationGeneration: 1,
			Range: distribution.KeyRange{Start: distribution.KeyspacePoint{},
				End: distribution.KeyspaceEnd{Max: true}},
			Leaders: []distribution.EndpointID{"peer-a", "peer-b", "peer-c"}, Epoch: 7,
		}})
		if err != nil {
			t.Fatal(err)
		}
		fixture.config.Distributions = append(fixture.config.Distributions,
			distribution.DistributionSpec{Name: distributionName, Arity: 1,
				MapperVersion: distribution.NativeMapperVersion})
		fixture.config.Placements = append(fixture.config.Placements,
			distribution.TablePlacement{Table: table, Distribution: distributionName,
				Columns: []string{"/id"}})
		fixture.config.Manifests = append(fixture.config.Manifests, manifest)
		descriptor := baseDescriptor
		descriptor.Distribution, descriptor.Shard = distributionName, "all"
		descriptor.Group.ShardIncarnation[14] = byte(index >> 8)
		descriptor.Group.ShardIncarnation[15] = byte(index + 1)
		descriptor.Group.GroupID[14] = byte(index >> 8)
		descriptor.Group.GroupID[15] = byte(index + 1)
		fixture.descriptors = append(fixture.descriptors, descriptor)
		profile := baseProfile
		profile.Table = table
		fixture.profiles = append(fixture.profiles, profile)
		key, ok := orderedkey.AppendString(nil, []byte(fmt.Sprintf("key-%03d", index)), orderedkey.Ascending)
		if !ok {
			t.Fatal("ordered key")
		}
		fixture.request.Points = append(fixture.request.Points,
			ReplicatedTableBatchPoint{Table: []byte(table), Key: key})
	}
	snapshot, err := NewSnapshotWithReplicatedTableMetadata(
		fixture.config, fixture.endpoints, generation, nil, nil,
		fixture.descriptors, fixture.profiles,
	)
	if err != nil {
		t.Fatal(err)
	}
	fixture.snapshot = snapshot
	fixture.routes = make([]ResolvedReplicatedTableKey, len(fixture.request.Points))
	for index := range fixture.request.Points {
		var replicas [ServingReplicaCount]ReplicatedEndpoint
		var scratch [replication.MaxMutationKeyBytes + 16]byte
		resolved, ok := snapshot.ResolveReplicatedTableKey(
			fixture.request.Points[index].Table, fixture.request.Points[index].Key,
			scratch[:0], replicas[:0],
		)
		if !ok {
			t.Fatalf("resolve %d", index)
		}
		fixture.routes[index] = resolved
	}
	return fixture
}

func newScatterReader(
	t testing.TB,
	fixture scatterCatalogFixture,
	client *scatterReadClient,
	refresh RefreshFunc,
	concurrency int,
) *ReplicatedDataReader {
	t.Helper()
	client.states = make(map[raftmember.GroupKey]map[uint64]shardservice.ReplicatedMemberState)
	client.values = make(map[raftmember.GroupKey][][]byte)
	client.reads = make(map[raftmember.GroupKey]int)
	for index, resolved := range fixture.routes {
		if client.states[resolved.Route.Group] == nil {
			client.states[resolved.Route.Group] = make(map[uint64]shardservice.ReplicatedMemberState)
			for _, endpoint := range resolved.Route.Replicas {
				client.states[resolved.Route.Group][endpoint.Member] = shardservice.ReplicatedMemberState{
					Fence: shardservice.ReplicatedFence{Group: resolved.Route.Group,
						AllocationGeneration: resolved.Route.AllocationGeneration,
						Command:              resolved.Route.Command, MemberID: endpoint.Member,
						StoreID: endpoint.StoreID, NodeIncarnation: endpoint.NodeIncarnation, Term: 7},
					LeaderID: 2, Commit: uint64(20 + index), Applied: uint64(20 + index),
					CheckpointApplied: 19,
				}
			}
		}
		client.values[resolved.Route.Group] = append(client.values[resolved.Route.Group],
			[]byte(fmt.Sprintf("value-%03d", index)))
	}
	executor, err := NewReplicatedExecutor(client, 3, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := NewReplicatedDataReaderWithOptions(ReplicatedDataReaderOptions{
		Catalog: NewCatalogHolder(fixture.snapshot), Executor: executor, Refresh: refresh,
		MaxScatterConcurrency: concurrency,
	})
	if err != nil {
		t.Fatal(err)
	}
	return reader
}

func TestReplicatedScatterReadMergesOriginalOrderAndReturnsObservationVector(t *testing.T) {
	fixture := newScatterCatalogFixture(t, 2, 5)
	// Alternate the two groups so group-sorted execution cannot accidentally
	// become result order.
	fixture.request.Points = []ReplicatedTableBatchPoint{
		fixture.request.Points[1], fixture.request.Points[0],
		fixture.request.Points[1], fixture.request.Points[0],
	}
	fixture.routes = []ResolvedReplicatedTableKey{
		fixture.routes[1], fixture.routes[0], fixture.routes[1], fixture.routes[0],
	}
	client := &scatterReadClient{}
	reader := newScatterReader(t, fixture, client, nil, 2)
	client.values[fixture.routes[0].Route.Group] = [][]byte{[]byte("right-0"), []byte("right-1")}
	client.values[fixture.routes[1].Route.Group] = [][]byte{[]byte("left-0"), []byte("left-1")}
	result, err := reader.ReadScatterBatch(context.Background(), fixture.request)
	if err != nil || result.Count() != 4 || len(result.Observations) != 2 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	for index, want := range []string{"right-0", "left-0", "right-1", "left-1"} {
		raw, found, ok := result.Lookup(index)
		if !ok || !found || string(raw) != want {
			t.Fatalf("position %d raw=%q found=%v ok=%v", index, raw, found, ok)
		}
	}
	if bytes.Compare(result.Observations[0].RouteID[:], result.Observations[1].RouteID[:]) >= 0 ||
		result.Observations[0].Applied == 0 || result.Observations[1].Applied == 0 {
		t.Fatalf("observations=%+v", result.Observations)
	}
	result.Release()
	if reader.readBytes.Load() != 0 {
		t.Fatalf("read bytes retained after release=%d", reader.readBytes.Load())
	}
}

func TestReplicatedScatterReadIntentFailureReturnsNoPartialResult(t *testing.T) {
	fixture := newScatterCatalogFixture(t, 2, 5)
	intentIndex := 0
	if bytes.Compare(fixture.routes[1].RouteID[:], fixture.routes[0].RouteID[:]) > 0 {
		intentIndex = 1
	}
	client := &scatterReadClient{intentGroup: fixture.routes[intentIndex].Route.Group}
	reader := newScatterReader(t, fixture, client, nil, 1)
	result, err := reader.ReadScatterBatch(context.Background(), fixture.request)
	if !errors.Is(err, ErrReplicatedReadIntentActive) || result.Packed != nil ||
		result.Observations != nil || result.Count() != 0 {
		t.Fatalf("partial result=%+v err=%v", result, err)
	}
	if reader.readBytes.Load() != 0 {
		t.Fatalf("read bytes retained=%d", reader.readBytes.Load())
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	for _, resolved := range fixture.routes {
		if client.reads[resolved.Route.Group] != 1 {
			t.Fatalf("group %x reads=%d, expected prior success then intent refusal",
				resolved.Route.Group.GroupID, client.reads[resolved.Route.Group])
		}
	}
}

func TestReplicatedScatterReadRefreshesOneStaleRouteAndReplaysWholeBatch(t *testing.T) {
	oldFixture := newScatterCatalogFixture(t, 2, 5)
	newFixture := newScatterCatalogFixture(t, 2, 6)
	staleIndex := 0
	if bytes.Compare(oldFixture.routes[1].RouteID[:], oldFixture.routes[0].RouteID[:]) > 0 {
		staleIndex = 1
	}
	newFixture.descriptors[staleIndex].Command.RouteGeneration++
	newSnapshot, err := NewSnapshotWithReplicatedTableMetadata(
		newFixture.config, newFixture.endpoints, 6, nil, nil,
		newFixture.descriptors, newFixture.profiles,
	)
	if err != nil {
		t.Fatal(err)
	}
	newFixture.snapshot = newSnapshot
	client := &scatterReadClient{staleGroup: oldFixture.routes[staleIndex].Route.Group,
		staleCommand: newFixture.descriptors[staleIndex].Command, staleRemaining: true}
	refreshes := 0
	reader := newScatterReader(t, oldFixture, client,
		func(context.Context, uint64) (*Snapshot, error) {
			refreshes++
			return newSnapshot, nil
		}, 1)
	result, err := reader.ReadScatterBatch(context.Background(), oldFixture.request)
	defer result.Release()
	if err != nil || result.Count() != 2 || refreshes != 1 || len(result.Observations) != 2 {
		t.Fatalf("result=%+v refreshes=%d err=%v", result, refreshes, err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	for _, resolved := range oldFixture.routes {
		if client.reads[resolved.Route.Group] != 2 {
			t.Fatalf("group %x reads=%d, want full replay",
				resolved.Route.Group.GroupID, client.reads[resolved.Route.Group])
		}
	}
}

func TestReplicatedScatterReadDrainsSixtyFiveGroupsThroughBoundedWorkers(t *testing.T) {
	fixture := newScatterCatalogFixture(t, 65, 5)
	client := &scatterReadClient{delay: 200 * time.Microsecond}
	reader := newScatterReader(t, fixture, client, nil, 3)
	result, err := reader.ReadScatterBatch(context.Background(), fixture.request)
	defer result.Release()
	if err != nil || result.Count() != 65 || len(result.Observations) != 65 {
		t.Fatalf("count=%d observations=%d err=%v",
			result.Count(), len(result.Observations), err)
	}
	if maximum := client.maxActive.Load(); maximum > 3 || maximum == 0 {
		t.Fatalf("active shard calls=%d, want 1..3", maximum)
	}
}

func TestReplicatedScatterReadRejectsWorkingSetBeforeShardIO(t *testing.T) {
	fixture := newScatterCatalogFixture(t, 3, 5)
	client := &scatterReadClient{}
	reader := newScatterReader(t, fixture, client, nil, 2)
	reader.maxReadBytes = uint64(fixture.request.MaxResultBytes)
	result, err := reader.ReadScatterBatch(context.Background(), fixture.request)
	if !errors.Is(err, ErrReplicatedReadAdmission) || result.Packed != nil ||
		result.Observations != nil {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	for group, calls := range client.reads {
		if calls != 0 {
			t.Fatalf("group %x calls=%d before admission", group.GroupID, calls)
		}
	}
}

func TestReplicatedScatterReadRejectsAggregateResultWithoutPartialReturn(t *testing.T) {
	fixture := newScatterCatalogFixture(t, 2, 5)
	fixture.request.MaxResultBytes = 64
	client := &scatterReadClient{}
	reader := newScatterReader(t, fixture, client, nil, 1)
	for _, resolved := range fixture.routes {
		client.values[resolved.Route.Group] = [][]byte{bytes.Repeat([]byte{'x'}, 40)}
	}
	result, err := reader.ReadScatterBatch(context.Background(), fixture.request)
	if !errors.Is(err, ErrReplicatedReadAdmission) || result.Packed != nil ||
		result.Observations != nil || result.Count() != 0 {
		t.Fatalf("partial result=%+v err=%v", result, err)
	}
	if reader.readBytes.Load() != 0 {
		t.Fatalf("read bytes retained=%d", reader.readBytes.Load())
	}
}

func BenchmarkReplicatedScatterReadEightGroups(b *testing.B) {
	fixture := newScatterCatalogFixture(b, 8, 5)
	fixture.request.MaxResultBytes = 64 << 10
	client := &scatterReadClient{}
	reader := newScatterReader(b, fixture, client, nil, 8)
	b.ReportAllocs()
	for b.Loop() {
		result, err := reader.ReadScatterBatch(context.Background(), fixture.request)
		if err != nil || result.Count() != 8 {
			b.Fatalf("count=%d err=%v", result.Count(), err)
		}
		result.Release()
	}
}

func BenchmarkReplicatedScatterReadSixteenPointsFourGroups(b *testing.B) {
	fixture := newScatterCatalogFixture(b, 4, 5)
	fixture.request.MaxResultBytes = 64 << 10
	rounds := fixture.request.Points
	roundRoutes := fixture.routes
	fixture.request.Points = make([]ReplicatedTableBatchPoint, 0, 16)
	fixture.routes = make([]ResolvedReplicatedTableKey, 0, 16)
	for range 4 {
		fixture.request.Points = append(fixture.request.Points, rounds...)
		fixture.routes = append(fixture.routes, roundRoutes...)
	}
	client := &scatterReadClient{}
	reader := newScatterReader(b, fixture, client, nil, 4)
	b.ReportAllocs()
	for b.Loop() {
		result, err := reader.ReadScatterBatch(context.Background(), fixture.request)
		if err != nil || result.Count() != 16 {
			b.Fatalf("count=%d err=%v", result.Count(), err)
		}
		result.Release()
	}
}
