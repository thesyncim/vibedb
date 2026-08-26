package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replication"
)

func transactionCommandWithFingerprint(
	t testing.TB,
	fixture relationBundleFixture,
	control distributedtxn.ReplicatedCommand,
	batches []replication.RelationMutationBatch,
	fingerprint replication.Digest,
) []byte {
	t.Helper()
	transaction, err := distributedtxn.AppendReplicatedCommand(nil, control)
	if err != nil {
		t.Fatal(err)
	}
	sequence, err := replication.TransactionClientSequence(transaction)
	if err != nil {
		t.Fatal(err)
	}
	command := replication.Command{
		Kind:      replication.CommandTransaction,
		ClusterID: fixture.binding.ClusterID, ClusterIncarnation: fixture.binding.ClusterIncarnation,
		TopologyRecoveryEpoch: fixture.binding.TopologyRecoveryEpoch,
		Distribution:          fixture.binding.Distribution, Shard: fixture.binding.Shard,
		AllocationGeneration: fixture.binding.AllocationGeneration,
		ShardIncarnation:     fixture.binding.ShardIncarnation, GroupID: fixture.binding.GroupID,
		ReplicaSetVersion: 1, ActivePolicyGeneration: fixture.binding.ActivePolicyGeneration,
		ProtectionEpoch: fixture.binding.ProtectionEpoch, OwnershipEpoch: fixture.binding.OwnershipEpoch,
		SchemaGeneration: fixture.binding.SchemaGeneration, RoutingVersion: fixture.binding.RoutingVersion,
		RouteGeneration: fixture.binding.RouteGeneration,
		Tenant:          []byte("tenant"), ClientID: replication.ID128(control.ID),
		ClientEpoch: uint64(control.Role), ClientSequence: sequence,
		Fingerprint: fingerprint, Transaction: transaction, Batches: batches,
	}
	return encodeCommand(t, command)
}

func transactionParticipantStageCommand(
	t testing.TB,
	fixture relationBundleFixture,
	id distributedtxn.ID,
	batches []replication.RelationMutationBatch,
) []byte {
	t.Helper()
	digest, err := replication.TransactionMutationDigest(batches)
	if err != nil {
		t.Fatal(err)
	}
	return transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
		Role:        distributedtxn.ReplicatedRoleParticipant,
		Operation:   distributedtxn.ReplicatedStageParticipant,
		ID:          id,
		PayloadKind: distributedtxn.ReplicatedPayloadParticipantStage,
		Participant: distributedtxn.ParticipantStage{
			CoordinatorGroup:            distributedtxn.ID(fixture.binding.GroupID),
			CoordinatorShardIncarnation: distributedtxn.ID(fixture.binding.ShardIncarnation),
			CoordinatorAllocation:       fixture.binding.AllocationGeneration,
			BucketBits:                  8,
			IntentScopes:                []distributedtxn.IntentScope{{Start: 0, End: 256}},
			MutationDigest:              digest,
		},
	}, batches)
}

func transactionParticipantTransitionCommand(
	t testing.TB,
	fixture relationBundleFixture,
	id distributedtxn.ID,
	operation distributedtxn.ReplicatedOperation,
	expected uint64,
) []byte {
	t.Helper()
	return transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
		Role:             distributedtxn.ReplicatedRoleParticipant,
		Operation:        operation,
		ID:               id,
		ExpectedRevision: expected,
		PayloadKind:      distributedtxn.ReplicatedPayloadNone,
	}, nil)
}

func applyTransactionCommand(
	t testing.TB,
	machine *Machine,
	index uint64,
	command []byte,
) TransactionCompletionResult {
	t.Helper()
	if err := machine.AdmitCommand(command); err != nil {
		t.Fatalf("admit transaction at %d: %v", index, err)
	}
	if _, err := machine.ApplyNormal(normalMeta(index), command); err != nil {
		t.Fatalf("apply transaction at %d: %v", index, err)
	}
	completion, result := openTransactionCompletion(t, machine, command)
	if completion.ResultCode != ResultApplied {
		t.Fatalf("transaction result at %d = %d, want %d", index, completion.ResultCode, ResultApplied)
	}
	return result
}

func TestTransactionParticipantIntentBarrierPrepareApplyAndRelease(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	id := transactionCodecID(231)
	documentKey := []byte("txn-document")
	document := []byte(`{"email":"txn@example.com","n":1}`)
	globalKey := []byte{0x91, 0x01, 't'}
	globalValue := []byte(`["txn-document"]`)
	batches := []replication.RelationMutationBatch{
		{Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: documentKey, Value: document,
		}}},
		{Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: globalKey, Value: globalValue,
		}}},
	}
	stage := transactionParticipantStageCommand(t, fixture, id, batches)
	applyTransactionCommand(t, fixture.machine, 3, stage)
	if fixture.machine.state.TransactionControlCount != 1 ||
		fixture.machine.state.ActiveTransactionCount != 1 ||
		fixture.machine.state.TransactionPayloadRows != 2 ||
		fixture.machine.state.TransactionIntentRows != 2 ||
		fixture.machine.state.TransactionResidentBytes == 0 {
		t.Fatalf("packed stage accounting = %+v", fixture.machine.state)
	}

	if _, err := fixture.machine.PointReadInto(
		1, documentKey, 3, fixture.base.Limits.MaxDocumentBytes, nil,
	); !errors.Is(err, ErrTransactionIntentActive) {
		t.Fatalf("intersecting point read error = %v, want active-intent fence", err)
	}

	conflicting := fixture.command(t, 1, replication.RelationMutationBatch{
		Relation: 1,
		Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: documentKey,
			Value: []byte(`{"email":"other@example.com"}`),
		}},
	})
	if err := fixture.machine.AdmitCommand(conflicting); err != nil {
		t.Fatalf("deterministic intent conflict was not admissible: %v", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(4), conflicting); err != nil {
		t.Fatal(err)
	}
	if got := bundleCompletionResult(t, fixture.machine, conflicting); got != ResultIntentBusy {
		t.Fatalf("intersecting ordinary write result = %d, want %d", got, ResultIntentBusy)
	}

	disjointKey := []byte("disjoint-document")
	disjointValue := []byte(`{"email":"disjoint@example.com"}`)
	disjoint := fixture.command(t, 2, replication.RelationMutationBatch{
		Relation: 1,
		Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: disjointKey, Value: disjointValue,
		}},
	})
	if err := fixture.machine.AdmitCommand(disjoint); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(5), disjoint); err != nil {
		t.Fatal(err)
	}
	if got := bundleCompletionResult(t, fixture.machine, disjoint); got != ResultApplied {
		t.Fatalf("disjoint ordinary write result = %d, want %d", got, ResultApplied)
	}

	prepare := transactionParticipantTransitionCommand(
		t, fixture, id, distributedtxn.ReplicatedPrepareParticipant, 1,
	)
	applyTransactionCommand(t, fixture.machine, 6, prepare)
	apply := transactionParticipantTransitionCommand(
		t, fixture, id, distributedtxn.ReplicatedApplyParticipant, 2,
	)
	result := applyTransactionCommand(t, fixture.machine, 7, apply)
	if !result.AffectedRowsValid || result.AffectedRows != 1 {
		t.Fatalf("participant apply result = %+v, want one base logical row", result)
	}
	for _, check := range []struct {
		collection interface {
			AppendRaw([]byte, []byte) ([]byte, bool, error)
		}
		key, value []byte
	}{
		{fixture.base.Collection, documentKey, document},
		{fixture.global.Collection, globalKey, globalValue},
	} {
		got, found, err := check.collection.AppendRaw(nil, check.key)
		if err != nil || !found || !bytes.Equal(got, check.value) {
			t.Fatalf("committed value %q = %q found=%v err=%v", check.key, got, found, err)
		}
	}

	release := transactionParticipantTransitionCommand(
		t, fixture, id, distributedtxn.ReplicatedReleaseParticipant, 3,
	)
	applyTransactionCommand(t, fixture.machine, 8, release)
	read, err := fixture.machine.PointReadInto(
		1, documentKey, 8, fixture.base.Limits.MaxDocumentBytes, nil,
	)
	if err != nil || !read.Found || !bytes.Equal(read.Value, document) {
		t.Fatalf("released point read = %+v err=%v", read, err)
	}
	state := fixture.machine.Published()
	if state.Applied != 8 || fixture.machine.state.ActiveTransactionCount != 0 ||
		fixture.machine.state.TransactionPayloadRows != 0 ||
		fixture.machine.state.TransactionIntentRows != 0 {
		t.Fatalf("released transaction state = %+v", fixture.machine.state)
	}
}

func TestTransactionPrepareConflictStaysStagedAndCanAbort(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	key := []byte("prepare-conflict")
	seed := fixture.command(t, 1, replication.RelationMutationBatch{
		Relation: 1,
		Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: key,
			Value: []byte(`{"email":"existing@example.com"}`),
		}},
	})
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), seed); err != nil {
		t.Fatal(err)
	}
	id := transactionCodecID(232)
	stage := transactionParticipantStageCommand(t, fixture, id, []replication.RelationMutationBatch{{
		Relation: 1,
		Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: key,
			Value: []byte(`{"email":"competing@example.com"}`),
		}},
	}})
	applyTransactionCommand(t, fixture.machine, 4, stage)
	prepare := transactionParticipantTransitionCommand(
		t, fixture, id, distributedtxn.ReplicatedPrepareParticipant, 1,
	)
	if err := fixture.machine.AdmitCommand(prepare); err != nil {
		t.Fatalf("admit deterministic prepare conflict: %v", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(5), prepare); err != nil {
		t.Fatal(err)
	}
	completion, result := openTransactionCompletion(t, fixture.machine, prepare)
	if completion.ResultCode != ResultIndexConflict || result.AffectedRowsValid {
		t.Fatalf("prepare conflict completion=%+v result=%+v", completion, result)
	}
	controlKey, _ := TransactionControlStorageKey(
		distributedtxn.ReplicatedRoleParticipant, id,
	)
	raw, found, err := fixture.system.Collection.AppendRaw(nil, controlKey[:])
	if err != nil || !found {
		t.Fatalf("read participant control: found=%v err=%v", found, err)
	}
	control, err := OpenTransactionControl(raw)
	if err != nil || control.State != uint8(distributedtxn.ParticipantStaged) || control.Revision != 1 ||
		control.LastResultCode != ResultIndexConflict {
		t.Fatalf("failed prepare control=%+v err=%v", control.TransactionControl, err)
	}
	competingControl := distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleParticipant,
		Operation: distributedtxn.ReplicatedPrepareParticipant, ID: id,
		ExpectedRevision: 1, PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}
	competing := transactionCommandWithFingerprint(
		t, fixture, competingControl, nil, sha256.Sum256([]byte("competing-prepare")),
	)
	if err := fixture.machine.AdmitCommand(competing); err != nil {
		t.Fatalf("admit deterministic competing prepare: %v", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(6), competing); err != nil {
		t.Fatal(err)
	}
	competingCompletion, _ := openTransactionCompletion(t, fixture.machine, competing)
	if competingCompletion.ResultCode != ResultTransactionConflict {
		t.Fatalf("competing prepare result=%d, want %d",
			competingCompletion.ResultCode, ResultTransactionConflict)
	}
	abort := transactionParticipantTransitionCommand(
		t, fixture, id, distributedtxn.ReplicatedAbortParticipant, 1,
	)
	applyTransactionCommand(t, fixture.machine, 7, abort)
	release := transactionParticipantTransitionCommand(
		t, fixture, id, distributedtxn.ReplicatedReleaseParticipant, 2,
	)
	applyTransactionCommand(t, fixture.machine, 8, release)
}

func TestTransactionCommandIsAnExplicitNormalBatchBoundary(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	id := transactionCodecID(233)
	stage := transactionParticipantStageCommand(t, fixture, id, []replication.RelationMutationBatch{{
		Relation: 1,
		Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: []byte("batch-boundary"),
			Value: []byte(`{"email":"boundary@example.com"}`),
		}},
	}})
	entries := []raftmodel.NormalApply{{Meta: normalMeta(3), Data: stage}}
	applied, publication, err := fixture.machine.ApplyNormalBatch(
		entries, normalBatchWitnesses(entries),
	)
	if err != nil || applied != 0 || publication.Applied != 0 {
		t.Fatalf("transaction batch boundary = applied %d publication %+v err=%v", applied, publication, err)
	}
	applyTransactionCommand(t, fixture.machine, 3, stage)
}
