package gateway

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftservice"
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
	if err := reader.admitRead(context.Background(), bytes); err != nil {
		t.Fatal(err)
	}
	if err := reader.admitRead(context.Background(), bytes); !errors.Is(err, ErrReplicatedReadAdmission) {
		t.Fatalf("second reservation = %v", err)
	}
	if reader.readBytes.Load() != bytes || len(reader.readSlots) != 1 {
		t.Fatalf("reserved bytes=%d slots=%d", reader.readBytes.Load(), len(reader.readSlots))
	}
	reader.releaseRead(bytes)
	if reader.readBytes.Load() != 0 || len(reader.readSlots) != 0 {
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
	if _, err := reader.Read(context.Background(), request); !errors.Is(err, ErrReplicatedReadAdmission) {
		result.Release()
		t.Fatalf("read while response retained = %v", err)
	}
	result.Release()
	result.Release()
	if reader.readBytes.Load() != 0 || len(reader.readSlots) != 0 || result.Value != nil {
		t.Fatalf("released response bytes=%d slots=%d value=%q",
			reader.readBytes.Load(), len(reader.readSlots), result.Value)
	}
	result, err = reader.Read(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	result.Release()
}
