package gateway

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

type publicPointReadClient struct {
	states        map[string]shardservice.ReplicatedMemberState
	value         []byte
	probes        []string
	reads         []string
	operations    []shardservice.ReplicatedOperation
	requests      int
	notLeaderOnce bool
	staleFence    bool
	wantRelation  replication.RelationID
	wantMaxValue  uint32
	wantMinimum   uint64
}

type publicBatchReadClient struct {
	states   map[string]shardservice.ReplicatedMemberState
	response []byte
	probes   int
	reads    int
	wantMax  uint32
}

func (client *publicBatchReadClient) DoReplicated(
	_ context.Context,
	endpoint ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	state := client.states[endpoint.Address]
	if request.Operation == shardservice.ReplicatedProbe {
		client.probes++
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedHandshake,
			HasState: true, State: state}, nil
	}
	client.reads++
	batch, err := replicatedstate.OpenPointReadBatch(request.BatchRead)
	if err != nil || batch.Count() != 2 ||
		request.Operation != shardservice.ReplicatedReadBatchLeader ||
		request.Capability != serviceauthz.CapabilityDataRead ||
		request.MinimumApplied != 1 || request.MaxValueBytes != client.wantMax {
		return nil, ErrReplicatedRoute
	}
	return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedReadBatchResult,
		HasState: true, State: state, ReadApplied: state.Applied,
		Value: client.response}, nil
}

type signalingReadContext struct {
	context.Context
	entered chan struct{}
	never   chan struct{}
	once    sync.Once
}

func (ctx *signalingReadContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.entered) })
	return ctx.never
}

func (client *publicPointReadClient) DoReplicated(
	_ context.Context,
	endpoint ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	client.requests++
	state := client.states[endpoint.Address]
	if request.Operation == shardservice.ReplicatedProbe {
		client.probes = append(client.probes, endpoint.Address)
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedHandshake,
			HasState: true, State: state}, nil
	}
	client.reads = append(client.reads, endpoint.Address)
	client.operations = append(client.operations, request.Operation)
	if request.Capability != serviceauthz.CapabilityDataRead ||
		request.Relation != client.wantRelation ||
		request.MaxValueBytes != client.wantMaxValue ||
		request.MinimumApplied != client.wantMinimum {
		return nil, ErrReplicatedRoute
	}
	if client.notLeaderOnce {
		client.notLeaderOnce = false
		state.LeaderID = 3
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedNotLeader,
			HasState: true, State: state}, nil
	}
	if client.staleFence {
		state.Fence.Command.RouteGeneration++
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedRefusal,
			Refusal:  shardservice.ReplicatedRefusalStaleFence,
			HasState: true, State: state}, nil
	}
	return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedReadFound,
		HasState: true, State: state, ReadApplied: state.Applied, Value: client.value}, nil
}

func testReplicatedDataReader(
	t testing.TB,
	client *publicPointReadClient,
) (*ReplicatedDataReader, *CatalogHolder, []byte, ResolvedReplicatedTableKey) {
	t.Helper()
	config, endpoints, descriptor, profile := testReplicatedTableInput(t)
	snapshot, err := NewSnapshotWithReplicatedTableMetadata(
		config, endpoints, 5, nil, nil,
		[]ReplicatedShardDescriptor{descriptor}, []ReplicatedTableProfile{profile},
	)
	if err != nil {
		t.Fatal(err)
	}
	key, ok := orderedkey.AppendString(nil, []byte("customer-17"), orderedkey.Ascending)
	if !ok {
		t.Fatal("ordered key")
	}
	var replicas [ServingReplicaCount]ReplicatedEndpoint
	var scalarScratch [replication.MaxMutationKeyBytes + 16]byte
	resolved, ok := snapshot.ResolveReplicatedTableKey(
		[]byte(profile.Table), key, scalarScratch[:0], replicas[:0],
	)
	if !ok {
		t.Fatal("resolve replicated table")
	}
	client.states = make(map[string]shardservice.ReplicatedMemberState, len(resolved.Route.Replicas))
	for _, endpoint := range resolved.Route.Replicas {
		client.states[endpoint.Address] = shardservice.ReplicatedMemberState{
			Fence: shardservice.ReplicatedFence{
				Group: resolved.Route.Group, AllocationGeneration: resolved.Route.AllocationGeneration,
				Command: resolved.Route.Command, MemberID: endpoint.Member,
				StoreID: endpoint.StoreID, NodeIncarnation: endpoint.NodeIncarnation, Term: 7,
			},
			LeaderID: 2, Commit: 12, Applied: 12, CheckpointApplied: 11,
		}
	}
	executor, err := NewReplicatedExecutor(client, 3, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	holder := NewCatalogHolder(snapshot)
	reader, err := NewReplicatedDataReader(holder, executor)
	if err != nil {
		t.Fatal(err)
	}
	return reader, holder, key, resolved
}

func TestReplicatedDataReaderLinearizableRefreshesNotLeader(t *testing.T) {
	client := &publicPointReadClient{
		value: []byte(`{"id":"customer-17"}`), notLeaderOnce: true,
		wantRelation: 1, wantMaxValue: 4 << 20, wantMinimum: 1,
	}
	reader, _, key, resolved := testReplicatedDataReader(t, client)
	// The replacement member's handshake must describe itself as leader in a
	// newer term. The first read refusal points discovery at that member.
	for address, state := range client.states {
		if state.Fence.MemberID == 3 {
			state.LeaderID, state.Fence.Term = 3, 8
			client.states[address] = state
		}
	}
	result, err := reader.Read(context.Background(), ReplicatedTableReadRequest{
		Table: []byte("messages"), Key: key, Consistency: ReplicatedDataReadLinearizable,
	})
	defer result.Release()
	if err != nil || !result.Found || result.Position.RouteID != resolved.RouteID ||
		result.Position.Applied != 12 || result.Retries != 1 ||
		!bytes.Equal(result.Value, client.value) {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if len(client.reads) != 2 || len(client.operations) != 2 ||
		client.operations[0] != shardservice.ReplicatedReadLeader ||
		client.operations[1] != shardservice.ReplicatedReadLeader ||
		client.reads[0] == client.reads[1] ||
		len(client.probes) < 3 || client.probes[len(client.probes)-1] != client.reads[1] {
		t.Fatalf("probes=%v reads=%v operations=%v", client.probes, client.reads, client.operations)
	}
}

type sameGroupBatchReadClient struct {
	states   map[string]shardservice.ReplicatedMemberState
	response []byte
	wantMax  uint32
	reads    int
}

func (client *sameGroupBatchReadClient) DoReplicated(
	_ context.Context,
	endpoint ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	state := client.states[endpoint.Address]
	if request.Operation == shardservice.ReplicatedProbe {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedHandshake,
			HasState: true, State: state}, nil
	}
	client.reads++
	batch, err := replicatedstate.OpenPointReadBatch(request.BatchRead)
	if err != nil || request.Operation != shardservice.ReplicatedReadBatchLeader ||
		request.Capability != serviceauthz.CapabilityDataRead ||
		request.MinimumApplied != 1 || request.MaxValueBytes != client.wantMax {
		return nil, ErrReplicatedRoute
	}
	_ = batch
	return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedReadBatchResult,
		HasState: true, State: state, ReadApplied: state.Applied,
		Value: client.response}, nil
}

// BenchmarkReplicatedDataReaderBatchSameGroupEightPoints measures one
// same-group eight-point batch end to end: per-point resolution against the
// pinned catalog, grouping, one RPC, and the ordered merge.
func BenchmarkReplicatedDataReaderBatchSameGroupEightPoints(b *testing.B) {
	config, endpoints, descriptor, profile := testReplicatedTableInput(b)
	snapshot, err := NewSnapshotWithReplicatedTableMetadata(
		config, endpoints, 5, nil, nil, []ReplicatedShardDescriptor{descriptor},
		[]ReplicatedTableProfile{profile},
	)
	if err != nil {
		b.Fatal(err)
	}
	const pointCount = 8
	packed := binary.LittleEndian.AppendUint32(nil, pointCount)
	packed = append(packed, 0xFF)
	points := make([]ReplicatedTableBatchPoint, 0, pointCount)
	values := make([]byte, 0, pointCount*2)
	for index := range pointCount {
		key, ok := orderedkey.AppendString(nil, []byte{'k', byte('0' + index)}, orderedkey.Ascending)
		if !ok {
			b.Fatal("key")
		}
		value := []byte{'v', byte('0' + index)}
		packed = binary.LittleEndian.AppendUint32(packed, uint32(len(value)))
		values = append(values, value...)
		points = append(points, ReplicatedTableBatchPoint{Table: []byte("messages"), Key: key})
	}
	packed = append(packed, values...)
	var replicas [ServingReplicaCount]ReplicatedEndpoint
	var scratch [replication.MaxMutationKeyBytes + 16]byte
	resolved, ok := snapshot.ResolveReplicatedTableKey(
		[]byte("messages"), points[0].Key, scratch[:0], replicas[:0],
	)
	if !ok {
		b.Fatal("resolve")
	}
	client := &sameGroupBatchReadClient{wantMax: 1 << 20, response: packed}
	client.states = make(map[string]shardservice.ReplicatedMemberState)
	for _, endpoint := range resolved.Route.Replicas {
		client.states[endpoint.Address] = shardservice.ReplicatedMemberState{
			Fence: shardservice.ReplicatedFence{Group: resolved.Route.Group,
				AllocationGeneration: resolved.Route.AllocationGeneration,
				Command:              resolved.Route.Command, MemberID: endpoint.Member,
				StoreID: endpoint.StoreID, NodeIncarnation: endpoint.NodeIncarnation, Term: 7},
			LeaderID: 2, Commit: 12, Applied: 12, CheckpointApplied: 11,
		}
	}
	executor, err := NewReplicatedExecutor(client, 3, time.Second)
	if err != nil {
		b.Fatal(err)
	}
	reader, err := NewReplicatedDataReader(NewCatalogHolder(snapshot), executor)
	if err != nil {
		b.Fatal(err)
	}
	request := ReplicatedTableBatchReadRequest{MaxResultBytes: 1 << 20, Points: points}
	b.ReportAllocs()
	for b.Loop() {
		result, err := reader.ReadBatch(context.Background(), request)
		if err != nil || result.Count() != pointCount {
			b.Fatalf("count=%d err=%v", result.Count(), err)
		}
		result.Release()
	}
	if client.reads != b.N {
		b.Fatalf("reads=%d for %d batches, want one RPC per batch", client.reads, b.N)
	}
}

func TestReplicatedDataReaderBatchUsesOneReadForTwoTables(t *testing.T) {
	config, endpoints, descriptor, profile := testReplicatedTableInput(t)
	config.Placements = append(config.Placements, config.Placements[0])
	config.Placements[1].Table = "profiles"
	second := profile
	second.Table, second.Relation = "profiles", 2
	snapshot, err := NewSnapshotWithReplicatedTableMetadata(
		config, endpoints, 5, nil, nil, []ReplicatedShardDescriptor{descriptor},
		[]ReplicatedTableProfile{profile, second},
	)
	if err != nil {
		t.Fatal(err)
	}
	keyA, ok := orderedkey.AppendString(nil, []byte("a"), orderedkey.Ascending)
	if !ok {
		t.Fatal("first key")
	}
	keyB, ok := orderedkey.AppendString(nil, []byte("b"), orderedkey.Ascending)
	if !ok {
		t.Fatal("second key")
	}
	// count=2, both found (the second is intentionally empty), lengths 5,0.
	packed := binary.LittleEndian.AppendUint32(nil, 2)
	packed = append(packed, 0b00000011)
	packed = binary.LittleEndian.AppendUint32(packed, 5)
	packed = binary.LittleEndian.AppendUint32(packed, 0)
	packed = append(packed, "alpha"...)
	client := &publicBatchReadClient{response: packed, wantMax: 1 << 20}
	client.states = make(map[string]shardservice.ReplicatedMemberState)
	var replicas [ServingReplicaCount]ReplicatedEndpoint
	var scratch [replication.MaxMutationKeyBytes + 16]byte
	resolved, ok := snapshot.ResolveReplicatedTableKey(
		[]byte("messages"), keyA, scratch[:0], replicas[:0],
	)
	if !ok {
		t.Fatal("resolve")
	}
	for _, endpoint := range resolved.Route.Replicas {
		client.states[endpoint.Address] = shardservice.ReplicatedMemberState{
			Fence: shardservice.ReplicatedFence{Group: resolved.Route.Group,
				AllocationGeneration: resolved.Route.AllocationGeneration,
				Command:              resolved.Route.Command, MemberID: endpoint.Member,
				StoreID: endpoint.StoreID, NodeIncarnation: endpoint.NodeIncarnation, Term: 7},
			LeaderID: 2, Commit: 12, Applied: 12, CheckpointApplied: 11,
		}
	}
	executor, err := NewReplicatedExecutor(client, 3, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := NewReplicatedDataReader(NewCatalogHolder(snapshot), executor)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.ReadBatch(context.Background(), ReplicatedTableBatchReadRequest{
		MaxResultBytes: 1 << 20,
		Points: []ReplicatedTableBatchPoint{
			{Table: []byte("messages"), Key: keyA},
			{Table: []byte("profiles"), Key: keyB},
		},
	})
	defer result.Release()
	if err != nil || result.Count() != 2 || result.Position.RouteID != resolved.RouteID ||
		result.Position.Applied != 12 || client.reads != 1 || client.probes == 0 {
		t.Fatalf("result=%+v probes=%d reads=%d err=%v", result, client.probes, client.reads, err)
	}
	if raw, found, ok := result.Lookup(0); !ok || !found || string(raw) != "alpha" {
		t.Fatalf("first raw=%q found=%v ok=%v", raw, found, ok)
	}
	if raw, found, ok := result.Lookup(1); !ok || !found || len(raw) != 0 {
		t.Fatalf("second raw=%q found=%v ok=%v", raw, found, ok)
	}
}

func TestReplicatedDataReaderAtLeastAppliedPrefersQualifiedFollower(t *testing.T) {
	client := &publicPointReadClient{
		value: []byte(`{"id":"customer-17"}`), wantRelation: 1,
		wantMaxValue: 4 << 20, wantMinimum: 10,
	}
	reader, _, key, resolved := testReplicatedDataReader(t, client)
	result, err := reader.Read(context.Background(), ReplicatedTableReadRequest{
		Table: []byte("messages"), Key: key,
		Consistency: ReplicatedDataReadAtLeastApplied,
		Position:    ReplicatedReadPosition{RouteID: resolved.RouteID, Applied: 10},
	})
	defer result.Release()
	if err != nil || result.Position.RouteID != resolved.RouteID ||
		result.Position.Applied != 12 || len(client.reads) != 1 ||
		client.operations[0] != shardservice.ReplicatedReadFollower {
		t.Fatalf("result=%+v probes=%v reads=%v operations=%v err=%v",
			result, client.probes, client.reads, client.operations, err)
	}
	readState := client.states[client.reads[0]]
	if readState.Fence.MemberID == readState.LeaderID {
		t.Fatalf("bounded read used leader: %+v", readState)
	}
}

func TestReplicatedDataReaderRejectsForeignPositionBeforeIO(t *testing.T) {
	client := &publicPointReadClient{wantRelation: 1, wantMaxValue: 4 << 20, wantMinimum: 10}
	reader, _, key, resolved := testReplicatedDataReader(t, client)
	foreign := resolved.RouteID
	foreign[0]++
	_, err := reader.Read(context.Background(), ReplicatedTableReadRequest{
		Table: []byte("messages"), Key: key,
		Consistency: ReplicatedDataReadAtLeastApplied,
		Position:    ReplicatedReadPosition{RouteID: foreign, Applied: 10},
	})
	if !errors.Is(err, ErrReplicatedReadPositionMismatch) || client.requests != 0 {
		t.Fatalf("error=%v network requests=%d", err, client.requests)
	}
}

func TestReplicatedDataReaderCatalogMissRefreshesBeforeIO(t *testing.T) {
	client := &publicPointReadClient{
		value: []byte(`{"id":"customer-17"}`), wantRelation: 2,
		wantMaxValue: 4 << 20, wantMinimum: 1,
	}
	reader, holder, key, _ := testReplicatedDataReader(t, client)
	config, endpoints, descriptor, profile := testReplicatedTableInput(t)
	config.Placements = append(config.Placements, config.Placements[0])
	config.Placements[1].Table = "fresh"
	fresh := profile
	fresh.Table, fresh.Relation = "fresh", 2
	newer, err := NewSnapshotWithReplicatedTableMetadata(
		config, endpoints, 6, nil, nil,
		[]ReplicatedShardDescriptor{descriptor}, []ReplicatedTableProfile{profile, fresh},
	)
	if err != nil {
		t.Fatal(err)
	}
	var refreshes atomic.Int32
	reader.refresh = func(_ context.Context, stale uint64) (*Snapshot, error) {
		refreshes.Add(1)
		if stale != 5 {
			t.Fatalf("stale generation = %d, want 5", stale)
		}
		return newer, nil
	}
	result, err := reader.Read(context.Background(), ReplicatedTableReadRequest{
		Table: []byte("fresh"), Key: key, Consistency: ReplicatedDataReadLinearizable,
	})
	defer result.Release()
	if err != nil || !result.Found || !bytes.Equal(result.Value, client.value) {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if refreshes.Load() != 1 || client.requests == 0 || holder.Current().Generation() != 6 {
		t.Fatalf("refreshes=%d requests=%d generation=%d", refreshes.Load(), client.requests, holder.Current().Generation())
	}
}

func TestReplicatedDataReaderCatalogMissRejectsMalformedKeysWithoutRefresh(t *testing.T) {
	client := &publicPointReadClient{wantRelation: 1, wantMaxValue: 4 << 20, wantMinimum: 1}
	reader, _, key, _ := testReplicatedDataReader(t, client)
	var refreshes atomic.Uint32
	reader.refresh = func(context.Context, uint64) (*Snapshot, error) {
		refreshes.Add(1)
		return nil, ErrReplicatedDataRead
	}
	malformed := append(append([]byte(nil), key...), 0)
	if _, err := reader.Read(context.Background(), ReplicatedTableReadRequest{
		Table: []byte("fresh"), Key: malformed,
		Consistency: ReplicatedDataReadLinearizable,
	}); !errors.Is(err, ErrReplicatedTableRoute) {
		t.Fatalf("single malformed miss error=%v", err)
	}
	if refreshes.Load() != 0 || client.requests != 0 {
		t.Fatalf("single malformed miss refreshed=%d requests=%d", refreshes.Load(), client.requests)
	}

	validFresh := append([]byte(nil), key...)
	if _, err := reader.ReadBatch(context.Background(), ReplicatedTableBatchReadRequest{
		MaxResultBytes: 1 << 20,
		Points: []ReplicatedTableBatchPoint{
			{Table: []byte("fresh"), Key: validFresh},
			{Table: []byte("messages"), Key: malformed},
		},
	}); !errors.Is(err, ErrReplicatedTableRoute) {
		t.Fatalf("batch malformed miss error=%v", err)
	}
	if refreshes.Load() != 0 || client.requests != 0 {
		t.Fatalf("batch malformed miss refreshed=%d requests=%d", refreshes.Load(), client.requests)
	}
}

func TestReplicatedDataReaderFailsClosedOnCatalogAndServingFence(t *testing.T) {
	client := &publicPointReadClient{wantRelation: 1, wantMaxValue: 4 << 20, wantMinimum: 1}
	reader, _, key, _ := testReplicatedDataReader(t, client)
	client.staleFence = true
	_, err := reader.Read(context.Background(), ReplicatedTableReadRequest{
		Table: []byte("messages"), Key: key, Consistency: ReplicatedDataReadLinearizable,
	})
	if !errors.Is(err, raftservice.ErrServingFence) {
		t.Fatalf("serving-fence error=%T %v", err, err)
	}

	config, endpoints, _, _ := testReplicatedTableInput(t)
	snapshot, err := NewSnapshot(config, endpoints, 6)
	if err != nil {
		t.Fatal(err)
	}
	before := client.requests
	reader.catalog = NewCatalogHolder(snapshot)
	_, err = reader.Read(context.Background(), ReplicatedTableReadRequest{
		Table: []byte("messages"), Key: key, Consistency: ReplicatedDataReadLinearizable,
	})
	if !errors.Is(err, ErrReplicatedTableRoute) || client.requests != before {
		t.Fatalf("catalog error=%v network requests=%d want=%d", err, client.requests, before)
	}
}

func TestReplicatedDataReaderEnforcesResourceBoundsWithoutIO(t *testing.T) {
	client := &publicPointReadClient{wantRelation: 1, wantMaxValue: 4 << 20, wantMinimum: 1}
	reader, _, key, _ := testReplicatedDataReader(t, client)
	tests := []ReplicatedTableReadRequest{
		{Table: bytes.Repeat([]byte{'t'}, replication.MaxIdentityBytes+1), Key: key,
			Consistency: ReplicatedDataReadLinearizable},
		{Table: []byte("messages"), Key: bytes.Repeat([]byte{'k'}, replication.MaxMutationKeyBytes+1),
			Consistency: ReplicatedDataReadLinearizable},
		{Table: []byte("messages"), Key: key},
		{Table: []byte("messages"), Key: key, Consistency: ReplicatedDataReadLinearizable,
			Position: ReplicatedReadPosition{RouteID: replication.Digest{1}, Applied: 1}},
		{Table: []byte("messages"), Key: key, Consistency: ReplicatedDataReadAtLeastApplied},
	}
	for index, request := range tests {
		before := client.requests
		if _, err := reader.Read(context.Background(), request); !errors.Is(err, ErrReplicatedDataRead) {
			t.Fatalf("case %d error=%v", index, err)
		}
		if client.requests != before {
			t.Fatalf("case %d performed I/O", index)
		}
		if allocations := testing.AllocsPerRun(1000, func() {
			_, _ = reader.Read(context.Background(), request)
		}); allocations != 0 {
			t.Fatalf("case %d rejected allocations=%f", index, allocations)
		}
	}
}

func TestReplicatedDataReaderRefreshesDefiniteServingFenceOnce(t *testing.T) {
	client := &publicPointReadClient{
		value: []byte(`{"id":"customer-17"}`), staleFence: true,
		wantRelation: 1, wantMaxValue: 4 << 20, wantMinimum: 1,
	}
	reader, holder, key, _ := testReplicatedDataReader(t, client)
	config, endpoints, descriptor, profile := testReplicatedTableInput(t)
	descriptor.Command.RouteGeneration++
	advanced, err := NewSnapshotWithReplicatedTableMetadata(
		config, endpoints, 6, nil, nil,
		[]ReplicatedShardDescriptor{descriptor}, []ReplicatedTableProfile{profile},
	)
	if err != nil {
		t.Fatal(err)
	}
	var replicas [ServingReplicaCount]ReplicatedEndpoint
	var scalarScratch [replication.MaxMutationKeyBytes + 16]byte
	resolved, ok := advanced.ResolveReplicatedTableKey(
		[]byte(profile.Table), key, scalarScratch[:0], replicas[:0],
	)
	if !ok {
		t.Fatal("resolve advanced route")
	}
	var refreshes atomic.Uint32
	reader.refresh = func(_ context.Context, stale uint64) (*Snapshot, error) {
		if stale != holder.Current().Generation() || refreshes.Add(1) != 1 {
			t.Fatalf("refresh stale=%d count=%d", stale, refreshes.Load())
		}
		client.staleFence = false
		for _, endpoint := range resolved.Route.Replicas {
			state := client.states[endpoint.Address]
			state.Fence.Command = resolved.Route.Command
			client.states[endpoint.Address] = state
		}
		return advanced, nil
	}
	result, err := reader.Read(context.Background(), ReplicatedTableReadRequest{
		Table: []byte(profile.Table), Key: key, Consistency: ReplicatedDataReadLinearizable,
	})
	defer result.Release()
	if err != nil || !result.Found || result.Position.RouteID != resolved.RouteID ||
		refreshes.Load() != 1 || holder.Current().Generation() != 6 {
		t.Fatalf("result=%+v refreshes=%d generation=%d err=%v",
			result, refreshes.Load(), holder.Current().Generation(), err)
	}
}

func TestReplicatedDataReaderCoalescesCatalogRefresh(t *testing.T) {
	client := &publicPointReadClient{wantRelation: 1, wantMaxValue: 4 << 20, wantMinimum: 1}
	reader, holder, _, _ := testReplicatedDataReader(t, client)
	config, endpoints, descriptor, profile := testReplicatedTableInput(t)
	advanced, err := NewSnapshotWithReplicatedTableMetadata(
		config, endpoints, 6, nil, nil,
		[]ReplicatedShardDescriptor{descriptor}, []ReplicatedTableProfile{profile},
	)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var refreshes atomic.Uint32
	reader.refresh = func(_ context.Context, stale uint64) (*Snapshot, error) {
		if stale != holder.Current().Generation() || refreshes.Add(1) != 1 {
			return nil, ErrReplicatedDataRead
		}
		close(entered)
		<-release
		return advanced, nil
	}
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { first <- reader.refreshAfterFence(context.Background(), 5) }()
	<-entered
	go func() { second <- reader.refreshAfterFence(context.Background(), 5) }()
	select {
	case err := <-second:
		t.Fatalf("coalesced waiter returned before refresh: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
	if refreshes.Load() != 1 || holder.Current().Generation() != 6 {
		t.Fatalf("refreshes=%d generation=%d", refreshes.Load(), holder.Current().Generation())
	}
}

func TestReplicatedDataReaderCanceledRefreshOwnerDoesNotPoisonWaiter(t *testing.T) {
	client := &publicPointReadClient{wantRelation: 1, wantMaxValue: 4 << 20, wantMinimum: 1}
	reader, holder, _, _ := testReplicatedDataReader(t, client)
	config, endpoints, descriptor, profile := testReplicatedTableInput(t)
	advanced, err := NewSnapshotWithReplicatedTableMetadata(
		config, endpoints, 6, nil, nil,
		[]ReplicatedShardDescriptor{descriptor}, []ReplicatedTableProfile{profile},
	)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	var refreshes atomic.Uint32
	reader.refresh = func(ctx context.Context, _ uint64) (*Snapshot, error) {
		if refreshes.Add(1) == 1 {
			close(entered)
			<-ctx.Done()
			return nil, context.Cause(ctx)
		}
		return advanced, nil
	}
	ownerContext, cancelOwner := context.WithCancel(context.Background())
	owner := make(chan error, 1)
	waiter := make(chan error, 1)
	go func() { owner <- reader.refreshAfterFence(ownerContext, 5) }()
	<-entered
	go func() { waiter <- reader.refreshAfterFence(context.Background(), 5) }()
	cancelOwner()
	if err := <-owner; !errors.Is(err, context.Canceled) {
		t.Fatalf("owner refresh = %v", err)
	}
	if err := <-waiter; err != nil {
		t.Fatalf("waiter refresh = %v", err)
	}
	if refreshes.Load() != 2 || holder.Current().Generation() != 6 {
		t.Fatalf("refreshes=%d generation=%d", refreshes.Load(), holder.Current().Generation())
	}
}

func TestReplicatedDataReaderLiveOwnerTimeoutIsShared(t *testing.T) {
	client := &publicPointReadClient{wantRelation: 1, wantMaxValue: 4 << 20, wantMinimum: 1}
	reader, _, _, _ := testReplicatedDataReader(t, client)
	entered := make(chan struct{})
	release := make(chan struct{})
	var refreshes atomic.Uint32
	reader.refresh = func(context.Context, uint64) (*Snapshot, error) {
		if refreshes.Add(1) == 1 {
			close(entered)
		}
		<-release
		return nil, errors.Join(context.DeadlineExceeded, ErrReplicatedCatalog)
	}
	owner := make(chan error, 1)
	waiter := make(chan error, 1)
	go func() { owner <- reader.refreshAfterFence(context.Background(), 5) }()
	<-entered
	waiterContext := &signalingReadContext{
		Context: context.Background(), entered: make(chan struct{}), never: make(chan struct{}),
	}
	go func() { waiter <- reader.refreshAfterFence(waiterContext, 5) }()
	<-waiterContext.entered
	close(release)
	for name, result := range map[string]<-chan error{"owner": owner, "waiter": waiter} {
		if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("%s refresh = %v", name, err)
		}
	}
	if refreshes.Load() != 1 {
		t.Fatalf("refreshes = %d, want 1", refreshes.Load())
	}
}

func TestReplicatedDataReaderBoundsConcurrentResponseBytes(t *testing.T) {
	client := &publicPointReadClient{wantRelation: 1, wantMaxValue: 4 << 20, wantMinimum: 1}
	base, _, _, _ := testReplicatedDataReader(t, client)
	reader, err := NewReplicatedDataReaderWithOptions(ReplicatedDataReaderOptions{
		Catalog: base.catalog, Executor: base.executor,
		MaxConcurrentReads: 2, MaxInFlightReadBytes: replication.MaxMutationValueBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	bytes := uint64(replication.MaxMutationValueBytes)
	slot, epoch, err := reader.admitRead(context.Background(), bytes)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := reader.admitRead(context.Background(), bytes); !errors.Is(err, ErrReplicatedReadAdmission) {
		t.Fatalf("second reservation = %v", err)
	}
	if reader.readBytes.Load() != bytes || len(reader.readSlots) != 1 {
		t.Fatalf("reserved bytes=%d slots=%d", reader.readBytes.Load(), len(reader.readSlots))
	}
	reader.releaseRead(slot, epoch, bytes)
	reader.releaseRead(slot, epoch, bytes)
	if reader.readBytes.Load() != 0 || len(reader.readSlots) != 2 {
		t.Fatalf("released bytes=%d slots=%d", reader.readBytes.Load(), len(reader.readSlots))
	}

	for _, options := range []ReplicatedDataReaderOptions{
		{Catalog: base.catalog, Executor: base.executor, MaxConcurrentReads: -1},
		{Catalog: base.catalog, Executor: base.executor, MaxConcurrentReads: AbsoluteMaxReplicatedReadConcurrency + 1},
		{Catalog: base.catalog, Executor: base.executor, MaxInFlightReadBytes: AbsoluteMaxReplicatedReadInFlight + 1},
	} {
		if _, err := NewReplicatedDataReaderWithOptions(options); !errors.Is(err, ErrReplicatedDataRead) {
			t.Fatalf("options %+v error=%v", options, err)
		}
	}
}

func TestReplicatedDataReaderHoldsResponseReservationUntilRelease(t *testing.T) {
	client := &publicPointReadClient{
		value: []byte(`{"id":"customer-17"}`), wantRelation: 1,
		wantMaxValue: 4 << 20, wantMinimum: 1,
	}
	base, _, key, _ := testReplicatedDataReader(t, client)
	reader, err := NewReplicatedDataReaderWithOptions(ReplicatedDataReaderOptions{
		Catalog: base.catalog, Executor: base.executor,
		MaxConcurrentReads: 2, MaxInFlightReadBytes: 4 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := ReplicatedTableReadRequest{
		Table: []byte("messages"), Key: key, Consistency: ReplicatedDataReadLinearizable,
	}
	result, err := reader.Read(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if reader.readBytes.Load() != 4<<20 || len(reader.readSlots) != 1 {
		result.Release()
		t.Fatalf("live response reserved bytes=%d slots=%d", reader.readBytes.Load(), len(reader.readSlots))
	}
	copyOfResult := result
	if _, err := reader.Read(context.Background(), request); !errors.Is(err, ErrReplicatedReadAdmission) {
		result.Release()
		t.Fatalf("read while response retained = %v", err)
	}
	result.Release()
	copyOfResult.Release()
	result.Release()
	if reader.readBytes.Load() != 0 || len(reader.readSlots) != 2 || result.Value != nil {
		t.Fatalf("released response bytes=%d slots=%d value=%q",
			reader.readBytes.Load(), len(reader.readSlots), result.Value)
	}
	result, err = reader.Read(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	result.Release()
}

func TestReplicatedBatchLiteResolveMatchesFullResolve(t *testing.T) {
	client := &publicPointReadClient{wantRelation: 1, wantMaxValue: 4 << 20, wantMinimum: 10}
	reader, _, key, full := testReplicatedDataReader(t, client)
	lease := reader.catalog.pinCurrent()
	if lease.snapshot == nil {
		t.Fatal("no catalog snapshot")
	}
	defer lease.release()
	var replicas [ServingReplicaCount]ReplicatedEndpoint
	var scalar [replication.MaxMutationKeyBytes + 16]byte
	lite, ok := lease.snapshot.resolveReplicatedTableKeyWithoutRouteID(
		[]byte("messages"), key, scalar[:0], replicas[:0],
	)
	if !ok {
		t.Fatal("lite resolve failed for a resolvable key")
	}
	if lite.RouteID != (replication.Digest{}) {
		t.Fatal("lite resolve must not compute a route digest")
	}
	if replicatedRouteAuthority(lite.Route) != replicatedRouteAuthority(full.Route) {
		t.Fatal("lite resolve authority differs from full resolve")
	}
	if lite.Profile != full.Profile || lite.Point != full.Point {
		t.Fatal("lite resolve profile/point differs from full resolve")
	}
	prep, ok := lease.snapshot.replicatedTableResolvePrepFor([]byte("messages"))
	if !ok {
		t.Fatal("resolve prep failed for a resolvable table")
	}
	reuse := lite.Route
	reuse.Replicas = nil
	var replicasAgain [ServingReplicaCount]ReplicatedEndpoint
	var scalarAgain [replication.MaxMutationKeyBytes + 16]byte
	again, ok := lease.snapshot.resolveReplicatedTableKeyPrepared(
		&prep, key, scalarAgain[:0], replicasAgain[:0], false, reuse, true,
	)
	if !ok {
		t.Fatal("prepared reuse resolve failed for a resolvable key")
	}
	if replicatedRouteAuthority(again.Route) != replicatedRouteAuthority(full.Route) ||
		again.Profile != full.Profile || again.Point != full.Point {
		t.Fatal("prepared reuse resolve differs from full resolve")
	}
	prepMiss, ok := lease.snapshot.replicatedTableResolvePrepFor([]byte("no-such-table"))
	if ok || prepMiss.mapper != nil {
		t.Fatal("resolve prep must fail for an unknown table")
	}
	// The authority comparison must discriminate every coordinate it
	// claims to cover: flipping any one of them has to break equality,
	// and the digest has to agree.
	flip := func(route ReplicatedRoute) []ReplicatedRoute {
		out := make([]ReplicatedRoute, 0, 8)
		next := route
		next.AllocationGeneration++
		out = append(out, next)
		next = route
		next.Command.OwnershipEpoch++
		out = append(out, next)
		next = route
		next.Command.SchemaGeneration++
		out = append(out, next)
		next = route
		next.Command.RouteGeneration++
		out = append(out, next)
		next = route
		next.Command.RoutingVersion++
		out = append(out, next)
		next = route
		next.Group.GroupID[0]++
		out = append(out, next)
		return out
	}
	want := replicatedRouteAuthority(full.Route)
	for index, tampered := range flip(full.Route) {
		if replicatedRouteAuthority(tampered) == want {
			t.Fatalf("authority comparison blind to coordinate %d", index)
		}
		if replicatedRouteAuthorityDigest(tampered) == replicatedRouteAuthorityDigest(full.Route) {
			t.Fatalf("route digest blind to coordinate %d", index)
		}
	}
}
