package gateway

import (
	"context"
	"testing"
	"time"

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
