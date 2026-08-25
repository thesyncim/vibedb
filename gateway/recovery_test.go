package gateway

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

func TestRecoveryContextUsesOnlyExplicitInternalAuthority(t *testing.T) {
	var node rafttransport.NodeID
	node[0] = 41
	authority := serviceauthz.Authority{Node: node, Generation: 8}
	executor := &Executor{internalAuthority: authority}
	ctx := executor.recoveryContext(context.Background())
	if got, ok := serviceauthz.FromContext(ctx); !ok || got != authority {
		t.Fatalf("recovery authority=%+v present=%t", got, ok)
	}
	var externalNode rafttransport.NodeID
	externalNode[0] = 42
	external := serviceauthz.Authority{Node: externalNode, Generation: 7}
	externalCtx, _ := serviceauthz.WithAuthority(context.Background(), external)
	if got, ok := serviceauthz.FromContext(executor.recoveryContext(externalCtx)); !ok || got != external {
		t.Fatalf("caller authority was replaced: %+v present=%t", got, ok)
	}
	if _, ok := serviceauthz.FromContext((&Executor{}).recoveryContext(context.Background())); ok {
		t.Fatal("zero internal authority was synthesized")
	}
}

func recoveryTestID(seed byte) distributedtxn.ID {
	var id distributedtxn.ID
	for i := range id {
		id[i] = seed + byte(i)
	}
	return id
}

func stageRecoveryTransaction(
	t *testing.T,
	executor *Executor,
	snapshot *Snapshot,
	queries []Query,
	id distributedtxn.ID,
	participantCount int,
	commit bool,
) []transactionParticipant {
	t.Helper()
	profile := executor.profileFor(ClassInteractive)
	participants, err := executor.planTransaction(t.Context(), snapshot, queries, profile)
	if err != nil {
		t.Fatalf("planTransaction: %v", err)
	}
	refs := make([]distributedtxn.ParticipantRef, len(participants))
	for i := range participants {
		participant := &participants[i]
		participant.mutation, err = shardservice.AppendMutationBatch(nil, participant.statements)
		if err != nil {
			t.Fatal(err)
		}
		participant.digest = distributedtxn.ParticipantDigest(
			participant.bucketBits, participant.scopes, participant.mutation,
		)
		request := participant.call.req
		refs[i] = distributedtxn.ParticipantRef{
			Distribution: []byte(request.Distribution), Shard: []byte(request.Shard),
			RoutingVersion:       uint64(request.RoutingVersion),
			AllocationGeneration: uint64(request.AllocationGeneration),
			OwnershipEpoch:       uint64(request.OwnershipEpoch), MutationDigest: participant.digest,
			State: distributedtxn.ParticipantStaged,
		}
	}
	coordinator := &participants[0]
	coordinatorRecord, err := distributedtxn.AppendCoordinator(nil, distributedtxn.CoordinatorRecord{
		ID: id, State: distributedtxn.CoordinatorStaging, Revision: 1,
		CatalogGeneration: snapshot.Generation(),
		RecoveryDeadline:  time.Now().Add(-time.Second).UnixNano(), Participants: refs,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := transactionRequest(
		coordinator.call.req, profile, shardservice.TransactionStageCoordinator,
		id, 0, coordinatorRecord,
	)
	if _, err := executor.transactionRoundTrip(t.Context(), coordinator.call.address, request, profile); err != nil {
		t.Fatalf("stage coordinator: %v", err)
	}
	for i := range participants {
		participant := &participants[i]
		participant.record, err = distributedtxn.AppendParticipant(nil, distributedtxn.ParticipantRecord{
			ID: id, State: distributedtxn.ParticipantStaged, Revision: 1,
			RoutingVersion:            uint64(participant.call.req.RoutingVersion),
			AllocationGeneration:      uint64(participant.call.req.AllocationGeneration),
			OwnershipEpoch:            uint64(participant.call.req.OwnershipEpoch),
			CoordinatorDistribution:   []byte(coordinator.call.req.Distribution),
			CoordinatorShard:          []byte(coordinator.call.req.Shard),
			CoordinatorAllocation:     uint64(coordinator.call.req.AllocationGeneration),
			CoordinatorRoutingVersion: uint64(coordinator.call.req.RoutingVersion),
			CoordinatorOwnershipEpoch: uint64(coordinator.call.req.OwnershipEpoch),
			BucketBits:                participant.bucketBits, IntentScopes: participant.scopes,
			MutationDigest: participant.digest, Mutation: participant.mutation,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if participantCount > len(participants) {
		participantCount = len(participants)
	}
	if _, err := executor.participantPhase(
		t.Context(), id, participants[:participantCount], profile,
		shardservice.TransactionStageParticipant, 0,
	); err != nil {
		t.Fatalf("stage participants: %v", err)
	}
	if !commit {
		return participants
	}
	if participantCount != len(participants) {
		t.Fatal("cannot commit an incomplete recovery fixture")
	}
	if _, err := executor.participantPhase(
		t.Context(), id, participants, profile,
		shardservice.TransactionPrepareParticipant, 1,
	); err != nil {
		t.Fatalf("prepare participants: %v", err)
	}
	if err := executor.commitCoordinator(t.Context(), id, coordinator, profile); err != nil {
		t.Fatalf("commit coordinator: %v", err)
	}
	return participants
}

func TestRecoverTransactionRedrivesCommittedParticipants(t *testing.T) {
	cluster := newE2ECluster(t)
	snapshot := cluster.snapshot(t, 11)
	executor := NewExecutor(cluster.client, NewCatalogHolder(snapshot), Options{})
	key0 := cluster.freshKeysForShard(t, cluster.shards[0].id, 1)[0]
	key2 := cluster.freshKeysForShard(t, cluster.shards[2].id, 1)[0]
	id := recoveryTestID(41)
	stageRecoveryTransaction(t, executor, snapshot, []Query{
		{SQL: `INSERT INTO messages (tenant_id, n) VALUES (?, ?)`, Params: []shardservice.Param{
			shardservice.StringParam(key0), shardservice.NumberParam("501"),
		}, Class: ClassInteractive},
		{SQL: `INSERT INTO messages (tenant_id, n) VALUES (?, ?)`, Params: []shardservice.Param{
			shardservice.StringParam(key2), shardservice.NumberParam("502"),
		}, Class: ClassInteractive},
	}, id, 2, true)

	freshExecutor := NewExecutor(cluster.client, NewCatalogHolder(snapshot), Options{})
	result, err := freshExecutor.RecoverTransaction(context.Background(), id)
	if err != nil {
		t.Fatalf("RecoverTransaction: %v", err)
	}
	if result.State != distributedtxn.CoordinatorRetired || result.Participants != 2 ||
		result.RowsAffected != 2 {
		t.Fatalf("recovery result = %+v", result)
	}
	cluster.verifyInserted(t, key0, 501)
	cluster.verifyInserted(t, key2, 502)
}

func TestRecoverAllAbortsExpiredIncompleteTransaction(t *testing.T) {
	cluster := newE2ECluster(t)
	snapshot := cluster.snapshot(t, 12)
	executor := NewExecutor(cluster.client, NewCatalogHolder(snapshot), Options{})
	key1 := cluster.freshKeysForShard(t, cluster.shards[1].id, 1)[0]
	key3 := cluster.freshKeysForShard(t, cluster.shards[3].id, 1)[0]
	id := recoveryTestID(71)
	stageRecoveryTransaction(t, executor, snapshot, []Query{
		{SQL: `INSERT INTO messages (tenant_id, n) VALUES (?, ?)`, Params: []shardservice.Param{
			shardservice.StringParam(key1), shardservice.NumberParam("601"),
		}, Class: ClassInteractive},
		{SQL: `INSERT INTO messages (tenant_id, n) VALUES (?, ?)`, Params: []shardservice.Param{
			shardservice.StringParam(key3), shardservice.NumberParam("602"),
		}, Class: ClassInteractive},
	}, id, 1, false)

	results, err := executor.RecoverAll(context.Background())
	if err != nil {
		t.Fatalf("RecoverAll: %v", err)
	}
	if len(results) != 1 || results[0].ID != id ||
		results[0].State != distributedtxn.CoordinatorRetired {
		t.Fatalf("recovery results = %+v", results)
	}
	cluster.verifyDeleted(t, key1)
	cluster.verifyDeleted(t, key3)
}

type manifestRecoveryFixture struct {
	id          distributedtxn.ID
	record      []byte
	descriptor  distributedtxn.ManifestDescriptor
	pages       [][]byte
	refs        map[string]distributedtxn.ParticipantRef
	snapshot    *Snapshot
	coordinator recoveryCoordinator
}

func newManifestRecoveryFixture(
	t *testing.T,
	count int,
	state distributedtxn.CoordinatorState,
	deadline int64,
) manifestRecoveryFixture {
	t.Helper()
	const (
		dist    = distribution.DistributionName("tenant_data")
		version = distribution.RoutingVersion(7)
	)
	id := recoveryTestID(91)
	refs := make([]distributedtxn.ParticipantRef, count)
	byShard := make(map[string]distributedtxn.ParticipantRef, count)
	shards := make([]distribution.Shard, count)
	for index := range count {
		shardID := distribution.ShardID(fmt.Sprintf("s%08d", index))
		start := distribution.KeyspacePoint{}
		binary.BigEndian.PutUint64(start[:], uint64(index))
		end := distribution.KeyspaceEnd{Max: index == count-1}
		if !end.Max {
			binary.BigEndian.PutUint64(end.Point[:], uint64(index+1))
		}
		generation := distribution.ShardAllocationGeneration(index + 1)
		epoch := distribution.OwnershipEpoch(index + 1)
		shards[index] = distribution.Shard{
			ID: shardID, AllocationGeneration: generation,
			Range:   distribution.KeyRange{Start: start, End: end},
			Leaders: []distribution.EndpointID{"ep"}, Epoch: epoch,
		}
		var digest distributedtxn.Digest
		binary.LittleEndian.PutUint64(digest[:8], uint64(index+1))
		ref := distributedtxn.ParticipantRef{
			Distribution: []byte(dist), Shard: []byte(shardID),
			RoutingVersion: uint64(version), AllocationGeneration: uint64(generation),
			OwnershipEpoch: uint64(epoch), MutationDigest: digest,
			State: distributedtxn.ParticipantStaged,
		}
		refs[index] = ref
		byShard[string(shardID)] = ref
	}
	manifest, err := distribution.NewManifest(dist, version, shards)
	if err != nil {
		t.Fatalf("NewManifest: %v", err)
	}
	snapshot, err := NewSnapshot(distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: dist, Arity: 1, MapperVersion: 1}},
		Placements: []distribution.TablePlacement{{
			Table: "messages", Distribution: dist, Columns: []string{"/tenant_id"},
		}},
		Manifests: []*distribution.Manifest{manifest},
	}, map[distribution.EndpointID]string{"ep": "manifest-script"}, 11)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	pageArena := make([]byte, distributedtxn.ManifestSegmentBytes)
	pages := make([][]byte, 0, 8)
	builder, err := distributedtxn.NewManifestBuilder(
		pageArena,
		func(segment distributedtxn.ManifestSegment) error {
			pages = append(pages, append([]byte(nil), segment.Raw...))
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	for index := range refs {
		if err := builder.Append(refs[index]); err != nil {
			t.Fatal(err)
		}
	}
	descriptor, err := builder.Seal()
	if err != nil {
		t.Fatal(err)
	}
	record, err := distributedtxn.AppendManifestCoordinator(nil, distributedtxn.ManifestCoordinatorRecord{
		ID: id, State: distributedtxn.CoordinatorStaging, Revision: 1,
		CatalogGeneration: snapshot.Generation(), RecoveryDeadline: deadline,
		Manifest: descriptor,
	})
	if err != nil {
		t.Fatal(err)
	}
	profile := DefaultProfiles()[ClassAdmin].withDefaults()
	routes, err := buildRecoveryRouteIndex(snapshot, profile)
	if err != nil {
		t.Fatal(err)
	}
	first := refs[0]
	call := routes[recoveryRouteKey{
		distribution: string(first.Distribution), shard: string(first.Shard),
	}]
	revision := uint64(1)
	if state != distributedtxn.CoordinatorStaging {
		revision = 2
	}
	response := shardservice.CompletionResponse(0)
	response.Transaction = shardservice.TransactionReply{
		Role: shardservice.TransactionRoleCoordinator, ID: id, Revision: revision,
		CoordinatorState: state, RecordKind: shardservice.TransactionRecordManifestCoordinator,
		Record: record,
	}
	return manifestRecoveryFixture{
		id: id, record: record, descriptor: descriptor, pages: pages,
		refs: byShard, snapshot: snapshot,
		coordinator: recoveryCoordinator{call: call, response: response},
	}
}

type manifestRecoveryScript struct {
	mu sync.Mutex

	fixture        manifestRecoveryFixture
	state          distributedtxn.CoordinatorState
	revision       uint64
	missingPage    int
	reorderFirst   bool
	dropCommitOnce bool
	dropLookupOnce bool
	dropRetireOnce bool
	operations     map[shardservice.TransactionOperation]int
	serveErr       error
}

func newManifestRecoveryScript(fixture manifestRecoveryFixture, state distributedtxn.CoordinatorState) *manifestRecoveryScript {
	revision := uint64(1)
	if state != distributedtxn.CoordinatorStaging {
		revision = 2
	}
	return &manifestRecoveryScript{
		fixture: fixture, state: state, revision: revision, missingPage: -1,
		operations: make(map[shardservice.TransactionOperation]int),
	}
}

func (s *manifestRecoveryScript) dial(_ context.Context, _ string) (net.Conn, error) {
	client, server := net.Pipe()
	go func() {
		defer server.Close()
		request, err := shardservice.DecodeRequest(server)
		if err != nil {
			s.setServeErr(err)
			return
		}
		response := s.respond(request)
		if response == nil {
			return
		}
		if err := shardservice.EncodeResponse(server, response); err != nil {
			s.setServeErr(err)
		}
	}()
	return client, nil
}

func (s *manifestRecoveryScript) setServeErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.serveErr == nil {
		s.serveErr = err
	}
}

func (s *manifestRecoveryScript) respond(request *shardservice.ShardRequest) *shardservice.ShardResponse {
	tx := request.Transaction
	s.mu.Lock()
	s.operations[tx.Operation]++
	state, revision := s.state, s.revision
	if tx.Operation == shardservice.TransactionCommitCoordinator && s.dropCommitOnce {
		s.dropCommitOnce = false
		s.state, s.revision = distributedtxn.CoordinatorCommitted, 2
		s.mu.Unlock()
		return nil
	}
	if tx.Operation == shardservice.TransactionLookupCoordinator && s.dropLookupOnce {
		s.dropLookupOnce = false
		s.mu.Unlock()
		return nil
	}
	if tx.Operation == shardservice.TransactionRetireCoordinator && s.dropRetireOnce {
		s.dropRetireOnce = false
		s.mu.Unlock()
		return nil
	}
	missingPage, reorderFirst := s.missingPage, s.reorderFirst
	s.mu.Unlock()

	switch tx.Operation {
	case shardservice.TransactionReadManifestSegment:
		if int(tx.SegmentIndex) == missingPage {
			return shardservice.NewErrorResponse(shardservice.ErrorTransactionNotFound, "missing manifest page")
		}
		index := tx.SegmentIndex
		if reorderFirst && index == 0 && len(s.fixture.pages) > 1 {
			index = 1
		}
		response := shardservice.CompletionResponse(0)
		response.Transaction = shardservice.TransactionReply{
			Role: shardservice.TransactionRoleCoordinator, ID: s.fixture.id,
			Revision: revision, CoordinatorState: state,
			RecordKind:   shardservice.TransactionRecordManifestSegment,
			SegmentIndex: index, Record: s.fixture.pages[index],
		}
		return response
	case shardservice.TransactionReadParticipant:
		ref, ok := s.fixture.refs[string(request.Shard)]
		if !ok {
			return shardservice.NewErrorResponse(shardservice.ErrorTransactionNotFound, "missing participant")
		}
		record, err := s.participantRecord(ref)
		if err != nil {
			s.setServeErr(err)
			return nil
		}
		response := shardservice.CompletionResponse(0)
		response.Transaction = shardservice.TransactionReply{
			Role: shardservice.TransactionRoleParticipant, ID: s.fixture.id, Revision: 1,
			ParticipantState: distributedtxn.ParticipantStaged,
			RecordKind:       shardservice.TransactionRecordParticipant, Record: record,
		}
		return response
	case shardservice.TransactionPrepareParticipant:
		return manifestParticipantStatus(s.fixture.id, distributedtxn.ParticipantPrepared, 2, 0)
	case shardservice.TransactionApplyParticipant:
		return manifestParticipantStatus(s.fixture.id, distributedtxn.ParticipantApplied, 3, 1)
	case shardservice.TransactionReleaseParticipant:
		return manifestParticipantStatus(s.fixture.id, distributedtxn.ParticipantReleased, 4, 0)
	case shardservice.TransactionAbortParticipant:
		return manifestParticipantStatus(s.fixture.id, distributedtxn.ParticipantAborted, 2, 0)
	case shardservice.TransactionLookupParticipant:
		return manifestParticipantStatus(s.fixture.id, distributedtxn.ParticipantStaged, 1, 0)
	case shardservice.TransactionCommitCoordinator:
		s.mu.Lock()
		s.state, s.revision = distributedtxn.CoordinatorCommitted, 2
		s.mu.Unlock()
		return manifestCoordinatorStatus(s.fixture, distributedtxn.CoordinatorCommitted, 2, false)
	case shardservice.TransactionLookupCoordinator:
		s.mu.Lock()
		state, revision = s.state, s.revision
		s.mu.Unlock()
		return manifestCoordinatorStatus(s.fixture, state, revision, true)
	case shardservice.TransactionAbortCoordinator:
		s.mu.Lock()
		s.state, s.revision = distributedtxn.CoordinatorAborted, 2
		s.mu.Unlock()
		return manifestCoordinatorStatus(s.fixture, distributedtxn.CoordinatorAborted, 2, false)
	case shardservice.TransactionRetireCoordinator:
		s.mu.Lock()
		s.state, s.revision = distributedtxn.CoordinatorRetired, tx.Revision+1
		s.mu.Unlock()
		return manifestCoordinatorStatus(s.fixture, distributedtxn.CoordinatorRetired, tx.Revision+1, false)
	default:
		return shardservice.NewErrorResponse(shardservice.ErrorMalformedRequest, "unexpected transaction operation")
	}
}

func (s *manifestRecoveryScript) participantRecord(ref distributedtxn.ParticipantRef) ([]byte, error) {
	first := s.fixture.refs["s00000000"]
	return distributedtxn.AppendParticipant(nil, distributedtxn.ParticipantRecord{
		ID: s.fixture.id, State: distributedtxn.ParticipantStaged, Revision: 1,
		RoutingVersion: ref.RoutingVersion, AllocationGeneration: ref.AllocationGeneration,
		OwnershipEpoch:          ref.OwnershipEpoch,
		CoordinatorDistribution: first.Distribution, CoordinatorShard: first.Shard,
		CoordinatorAllocation:     first.AllocationGeneration,
		CoordinatorRoutingVersion: first.RoutingVersion,
		CoordinatorOwnershipEpoch: first.OwnershipEpoch,
		BucketBits:                8, IntentScopes: []distributedtxn.IntentScope{{Start: 0, End: 1}},
		MutationDigest: ref.MutationDigest, Mutation: []byte{1},
	})
}

func manifestParticipantStatus(
	id distributedtxn.ID,
	state distributedtxn.ParticipantState,
	revision uint64,
	rows int64,
) *shardservice.ShardResponse {
	response := shardservice.CompletionResponse(rows)
	response.Transaction = shardservice.TransactionReply{
		Role: shardservice.TransactionRoleParticipant, ID: id, Revision: revision,
		ParticipantState: state,
	}
	return response
}

func manifestCoordinatorStatus(
	fixture manifestRecoveryFixture,
	state distributedtxn.CoordinatorState,
	revision uint64,
	withRecord bool,
) *shardservice.ShardResponse {
	response := shardservice.CompletionResponse(0)
	response.Transaction = shardservice.TransactionReply{
		Role: shardservice.TransactionRoleCoordinator, ID: fixture.id, Revision: revision,
		CoordinatorState: state,
	}
	if withRecord {
		response.Transaction.RecordKind = shardservice.TransactionRecordManifestCoordinator
		response.Transaction.Record = fixture.record
	}
	return response
}

func (s *manifestRecoveryScript) executor() *Executor {
	client := NewClientWithOptions(s.dial, ClientOptions{DisableConnectionReuse: true})
	return NewExecutor(client, NewCatalogHolder(s.fixture.snapshot), Options{})
}

func (s *manifestRecoveryScript) coordinatorResponse() recoveryCoordinator {
	s.mu.Lock()
	defer s.mu.Unlock()
	coordinator := s.fixture.coordinator
	coordinator.response = manifestCoordinatorStatus(s.fixture, s.state, s.revision, true)
	return coordinator
}

func (s *manifestRecoveryScript) operationCount(operation shardservice.TransactionOperation) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.operations[operation]
}

func (s *manifestRecoveryScript) check(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.serveErr != nil {
		t.Fatalf("script server: %v", s.serveErr)
	}
}

func TestRecoveryManifestWalk4097UsesBoundedPagedArena(t *testing.T) {
	fixture := newManifestRecoveryFixture(
		t, 4097, distributedtxn.CoordinatorCommitted, time.Now().Add(-time.Second).UnixNano(),
	)
	if fixture.descriptor.SegmentCount <= 1 {
		t.Fatalf("segment count=%d", fixture.descriptor.SegmentCount)
	}
	script := newManifestRecoveryScript(fixture, distributedtxn.CoordinatorCommitted)
	executor := script.executor()
	profile := executor.profileFor(ClassAdmin)
	routes, err := buildRecoveryRouteIndex(fixture.snapshot, profile)
	if err != nil {
		t.Fatal(err)
	}
	arena := newManifestRecoveryArena(profile)
	seen, largestPage := 0, 0
	err = executor.walkRecoveryManifest(
		t.Context(), routes, fixture.coordinator, fixture.id, fixture.descriptor,
		profile, arena,
		func(refs []distributedtxn.ParticipantRef, participants []transactionParticipant) error {
			if len(refs) != len(participants) {
				return ErrTransactionConflict
			}
			seen += len(refs)
			largestPage = max(largestPage, len(refs))
			return nil
		},
	)
	if err != nil {
		t.Fatalf("walkRecoveryManifest: %v", err)
	}
	if seen != 4097 || largestPage > distributedtxn.MaxManifestPageParticipants ||
		len(arena.windowResults) != profile.MaxConcurrency {
		t.Fatalf("seen=%d largest=%d window=%d", seen, largestPage, len(arena.windowResults))
	}
	if got := script.operationCount(shardservice.TransactionReadManifestSegment); got != len(fixture.pages) {
		t.Fatalf("page reads=%d want=%d", got, len(fixture.pages))
	}
	script.check(t)
}

func TestRecoveryManifestMissingPageWaitsThenAbortsAfterRestart(t *testing.T) {
	deadline := time.Now().Add(100 * time.Millisecond)
	fixture := newManifestRecoveryFixture(t, 4097, distributedtxn.CoordinatorStaging, deadline.UnixNano())
	script := newManifestRecoveryScript(fixture, distributedtxn.CoordinatorStaging)
	script.missingPage = 1
	executor := script.executor()
	result, err := executor.recoverManifestCoordinator(
		t.Context(), fixture.snapshot, script.coordinatorResponse(), executor.profileFor(ClassAdmin),
	)
	if !errors.Is(err, ErrRecoveryNotReady) || result.ID != fixture.id {
		t.Fatalf("before deadline result=%+v err=%v", result, err)
	}
	if got := script.operationCount(shardservice.TransactionReadParticipant); got != 0 {
		t.Fatalf("incomplete manifest fanned out %d participant reads", got)
	}
	time.Sleep(time.Until(deadline) + 20*time.Millisecond)
	script.mu.Lock()
	script.dropRetireOnce = true
	script.mu.Unlock()
	fresh := script.executor()
	result, err = fresh.recoverManifestCoordinator(
		t.Context(), fixture.snapshot, script.coordinatorResponse(), fresh.profileFor(ClassAdmin),
	)
	if err == nil {
		t.Fatalf("crash before retire unexpectedly completed: %+v", result)
	}
	restarted := script.executor()
	result, err = restarted.recoverManifestCoordinator(
		t.Context(), fixture.snapshot, script.coordinatorResponse(), restarted.profileFor(ClassAdmin),
	)
	if err != nil || result.State != distributedtxn.CoordinatorRetired || result.Participants != 4097 {
		t.Fatalf("after restart result=%+v err=%v", result, err)
	}
	if script.operationCount(shardservice.TransactionAbortCoordinator) != 1 ||
		script.operationCount(shardservice.TransactionRetireCoordinator) != 2 {
		t.Fatal("incomplete coordinator did not preserve one abort across the retire retry")
	}
	script.check(t)
}

func TestRecoveryManifestCommittedMissingAndReorderedPagesFailClosed(t *testing.T) {
	fixture := newManifestRecoveryFixture(
		t, 4097, distributedtxn.CoordinatorCommitted, time.Now().Add(-time.Second).UnixNano(),
	)
	for _, test := range []struct {
		name      string
		configure func(*manifestRecoveryScript)
	}{
		{"missing", func(script *manifestRecoveryScript) { script.missingPage = 1 }},
		{"reordered", func(script *manifestRecoveryScript) { script.reorderFirst = true }},
	} {
		t.Run(test.name, func(t *testing.T) {
			script := newManifestRecoveryScript(fixture, distributedtxn.CoordinatorCommitted)
			test.configure(script)
			executor := script.executor()
			_, err := executor.recoverManifestCoordinator(
				t.Context(), fixture.snapshot, script.coordinatorResponse(), executor.profileFor(ClassAdmin),
			)
			if !errors.Is(err, ErrTransactionConflict) {
				t.Fatalf("error=%v", err)
			}
			if script.operationCount(shardservice.TransactionApplyParticipant) != 0 {
				t.Fatal("unverified manifest applied a participant")
			}
			script.check(t)
		})
	}
}

func TestRecoveryManifest65CommittedParticipants(t *testing.T) {
	fixture := newManifestRecoveryFixture(
		t, 65, distributedtxn.CoordinatorCommitted, time.Now().Add(-time.Second).UnixNano(),
	)
	script := newManifestRecoveryScript(fixture, distributedtxn.CoordinatorCommitted)
	executor := script.executor()
	result, err := executor.recoverManifestCoordinator(
		t.Context(), fixture.snapshot, script.coordinatorResponse(), executor.profileFor(ClassAdmin),
	)
	if err != nil || result.State != distributedtxn.CoordinatorRetired ||
		result.Participants != 65 || result.RowsAffected != 65 {
		script.check(t)
		t.Fatalf("result=%+v err=%v", result, err)
	}
	for _, operation := range []shardservice.TransactionOperation{
		shardservice.TransactionReadParticipant,
		shardservice.TransactionApplyParticipant,
		shardservice.TransactionReleaseParticipant,
	} {
		if got := script.operationCount(operation); got != 65 {
			t.Fatalf("operation %d count=%d", operation, got)
		}
	}
	script.check(t)
}

func TestRecoveryManifestOutcomeUnknownResumesCommittedDecision(t *testing.T) {
	fixture := newManifestRecoveryFixture(
		t, 65, distributedtxn.CoordinatorStaging, time.Now().Add(-time.Second).UnixNano(),
	)
	script := newManifestRecoveryScript(fixture, distributedtxn.CoordinatorStaging)
	script.dropCommitOnce, script.dropLookupOnce = true, true
	executor := script.executor()
	_, err := executor.recoverManifestCoordinator(
		t.Context(), fixture.snapshot, script.coordinatorResponse(), executor.profileFor(ClassAdmin),
	)
	if !errors.Is(err, ErrCommitOutcomeUnknown) {
		script.check(t)
		t.Fatalf("first recovery error=%v", err)
	}
	fresh := script.executor()
	result, err := fresh.recoverManifestCoordinator(
		t.Context(), fixture.snapshot, script.coordinatorResponse(), fresh.profileFor(ClassAdmin),
	)
	if err != nil || result.State != distributedtxn.CoordinatorRetired || result.RowsAffected != 65 {
		t.Fatalf("resumed result=%+v err=%v", result, err)
	}
	if got := script.operationCount(shardservice.TransactionPrepareParticipant); got != 65 {
		t.Fatalf("prepare retries=%d", got)
	}
	script.check(t)
}

func TestRecoveryManifestRouteIndexFencesCatalogCoordinates(t *testing.T) {
	fixture := newManifestRecoveryFixture(
		t, 4097, distributedtxn.CoordinatorCommitted, time.Now().Add(-time.Second).UnixNano(),
	)
	profile := DefaultProfiles()[ClassAdmin].withDefaults()
	routes, err := buildRecoveryRouteIndex(fixture.snapshot, profile)
	if err != nil {
		t.Fatal(err)
	}
	ref := fixture.refs["s00004096"]
	participants := make([]transactionParticipant, 1)
	for _, mutate := range []func(*distributedtxn.ParticipantRef){
		func(value *distributedtxn.ParticipantRef) { value.RoutingVersion++ },
		func(value *distributedtxn.ParticipantRef) { value.AllocationGeneration++ },
		func(value *distributedtxn.ParticipantRef) { value.OwnershipEpoch++ },
	} {
		changed := ref
		mutate(&changed)
		if err := resolveRecoveryParticipantsFromIndex(
			routes, []distributedtxn.ParticipantRef{changed}, participants,
		); !errors.Is(err, ErrTransactionConflict) {
			t.Fatalf("coordinate mismatch error=%v", err)
		}
	}
}
