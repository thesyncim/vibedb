package replicatedstate

import (
	"bytes"
	"testing"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replication"
)

func openMutationCompletion(
	t testing.TB, machine *Machine, command []byte,
) (replication.CompletionView, int64, []byte) {
	t.Helper()
	lookup, err := machine.LookupCompletion(command)
	if err != nil {
		t.Fatal(err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := OpenMutationCompletionResult(completion.ResultCode, completion.InlineResult)
	if err != nil {
		t.Fatal(err)
	}
	return completion, rows, bytes.Clone(lookup.Bytes)
}

func multiJSONMutationBatches(prefix byte, count int) []replication.RelationMutationBatch {
	batches := make([]replication.RelationMutationBatch, 2)
	for relation := range batches {
		mutations := make([]replication.Mutation, count)
		for ordinal := range mutations {
			mutations[ordinal] = replication.Mutation{
				Kind:  replication.MutationPutAbsent,
				Key:   []byte{prefix, byte(relation + 1), byte(ordinal)},
				Value: []byte(`{"present":true}`),
			}
		}
		batches[relation] = replication.RelationMutationBatch{
			Relation: replication.RelationID(relation + 1), Mutations: mutations,
		}
	}
	return batches
}

func reopenRelationBundleFixture(t testing.TB, fixture relationBundleFixture) *Machine {
	t.Helper()
	if fixture.group == nil {
		t.Fatal("reopen fixture requires a checkpoint group")
	}
	if err := fixture.group.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenBundle(
		fixture.binding, testBootstrap(), fixture.system,
		relationBundleCollections(fixture.base, fixture.global, fixture.index, fixture.second),
		fixture.log, fixture.options,
	)
	if err != nil {
		t.Fatal(err)
	}
	return reopened
}

func TestMultiJSONNormalAffectedRowsExactRetryAndReopen(t *testing.T) {
	fixture := newMultiJSONRelationBundleFixture(t, true)
	command := fixture.command(t, 1, multiJSONMutationBatches('n', MaxDistinctMutations)...)
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), command); err != nil {
		t.Fatal(err)
	}
	completion, rows, firstWitness := openMutationCompletion(t, fixture.machine, command)
	if completion.ResultCode != ResultApplied || rows != 2*MaxDistinctMutations {
		t.Fatalf("multi-JSON completion=%+v rows=%d", completion, rows)
	}

	reopened := reopenRelationBundleFixture(t, fixture)
	_, reopenedRows, reopenedWitness := openMutationCompletion(t, reopened, command)
	if reopenedRows != rows || !bytes.Equal(reopenedWitness, firstWitness) {
		t.Fatalf("reopened completion rows=%d exact=%t", reopenedRows,
			bytes.Equal(reopenedWitness, firstWitness))
	}
	if _, err := reopened.ApplyNormal(normalMeta(4), command); err != nil {
		t.Fatal(err)
	}
	_, retryRows, retryWitness := openMutationCompletion(t, reopened, command)
	if retryRows != rows || !bytes.Equal(retryWitness, firstWitness) {
		t.Fatalf("retried completion rows=%d exact=%t", retryRows,
			bytes.Equal(retryWitness, firstWitness))
	}
	if fixture.base.Collection.Len() != MaxDistinctMutations ||
		fixture.global.Collection.Len() != MaxDistinctMutations {
		t.Fatalf("multi-JSON cardinality base=%d second=%d",
			fixture.base.Collection.Len(), fixture.global.Collection.Len())
	}
}

func TestMultiJSONTransactionAffectedRowsExactRetryAndReopen(t *testing.T) {
	fixture := newMultiJSONRelationBundleFixture(t, true)
	const mutationsPerRelation = 8
	id := transactionCodecID(0xc1)
	stage := transactionParticipantStageCommand(
		t, fixture, id, multiJSONMutationBatches('t', mutationsPerRelation),
	)
	applyTransactionCommand(t, fixture.machine, 3, stage)
	prepare := transactionParticipantTransitionCommand(
		t, fixture, id, distributedtxn.ReplicatedPrepareParticipant, 1,
	)
	applyTransactionCommand(t, fixture.machine, 4, prepare)
	apply := transactionParticipantTransitionCommand(
		t, fixture, id, distributedtxn.ReplicatedApplyParticipant, 2,
	)
	result := applyTransactionCommand(t, fixture.machine, 5, apply)
	if !result.AffectedRowsValid || result.AffectedRows != 2*mutationsPerRelation {
		t.Fatalf("multi-JSON transaction result=%+v", result)
	}
	firstWitness, err := fixture.machine.LookupCompletion(apply)
	if err != nil {
		t.Fatal(err)
	}

	reopened := reopenRelationBundleFixture(t, fixture)
	reopenedWitness, err := reopened.LookupCompletion(apply)
	if err != nil || !bytes.Equal(reopenedWitness.Bytes, firstWitness.Bytes) {
		t.Fatalf("reopened transaction completion exact=%t err=%v",
			bytes.Equal(reopenedWitness.Bytes, firstWitness.Bytes), err)
	}
	retry := applyTransactionCommand(t, reopened, 6, apply)
	if !retry.AffectedRowsValid || retry.AffectedRows != result.AffectedRows {
		t.Fatalf("retried multi-JSON transaction result=%+v", retry)
	}
	retryWitness, err := reopened.LookupCompletion(apply)
	if err != nil || !bytes.Equal(retryWitness.Bytes, firstWitness.Bytes) {
		t.Fatalf("retried transaction completion exact=%t err=%v",
			bytes.Equal(retryWitness.Bytes, firstWitness.Bytes), err)
	}
	if fixture.base.Collection.Len() != mutationsPerRelation ||
		fixture.global.Collection.Len() != mutationsPerRelation {
		t.Fatalf("multi-JSON transaction cardinality base=%d second=%d",
			fixture.base.Collection.Len(), fixture.global.Collection.Len())
	}
	release := transactionParticipantTransitionCommand(
		t, fixture, id, distributedtxn.ReplicatedReleaseParticipant, 3,
	)
	applyTransactionCommand(t, reopened, 7, release)
}

func TestStrictInsertAndConditionalUpdateNormalApplyAndExactRetry(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	key := []byte("conditional-row")
	first := []byte(`{"n":1}`)
	second := []byte(`{"n":2}`)

	insert := fixture.command(t, 1, replication.RelationMutationBatch{
		Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsent, Key: key, Value: first,
		}},
	})
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), insert); err != nil {
		t.Fatal(err)
	}
	completion, rows, _ := openMutationCompletion(t, fixture.machine, insert)
	if completion.ResultCode != ResultApplied || rows != 1 {
		t.Fatalf("strict insert completion=%+v rows=%d", completion, rows)
	}

	conflict := fixture.command(t, 2, replication.RelationMutationBatch{
		Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsent, Key: key, Value: first,
		}},
	})
	if _, err := fixture.machine.ApplyNormal(normalMeta(4), conflict); err != nil {
		t.Fatal(err)
	}
	completion, rows, _ = openMutationCompletion(t, fixture.machine, conflict)
	if completion.ResultCode != ResultIndexConflict || rows != 0 ||
		completion.ResultLength != 0 || len(completion.InlineResult) != 0 {
		t.Fatalf("strict insert conflict completion=%+v rows=%d", completion, rows)
	}

	missing := fixture.command(t, 3, replication.RelationMutationBatch{
		Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutPresent, Key: []byte("missing"), Value: second,
		}},
	})
	if _, err := fixture.machine.ApplyNormal(normalMeta(5), missing); err != nil {
		t.Fatal(err)
	}
	completion, rows, _ = openMutationCompletion(t, fixture.machine, missing)
	if completion.ResultCode != ResultApplied || rows != 0 ||
		completion.ResultLength != MutationCompletionResultBytes {
		t.Fatalf("missing update completion=%+v rows=%d", completion, rows)
	}
	if _, found, err := fixture.base.Collection.AppendRaw(nil, []byte("missing")); err != nil || found {
		t.Fatalf("missing update wrote row: found=%v err=%v", found, err)
	}

	update := fixture.command(t, 4, replication.RelationMutationBatch{
		Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutPresent, Key: key, Value: second,
		}},
	})
	if _, err := fixture.machine.ApplyNormal(normalMeta(6), update); err != nil {
		t.Fatal(err)
	}
	completion, rows, firstWitness := openMutationCompletion(t, fixture.machine, update)
	if completion.ResultCode != ResultApplied || rows != 1 {
		t.Fatalf("present update completion=%+v rows=%d", completion, rows)
	}
	if got, found, err := fixture.base.Collection.AppendRaw(nil, key); err != nil ||
		!found || !bytes.Equal(got, second) {
		t.Fatalf("updated row=%q found=%v err=%v", got, found, err)
	}

	beforeDigest := fixture.machine.state.DataChainDigest
	equal := fixture.command(t, 5, replication.RelationMutationBatch{
		Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutPresent, Key: key, Value: second,
		}},
	})
	if _, err := fixture.machine.ApplyNormal(normalMeta(7), equal); err != nil {
		t.Fatal(err)
	}
	_, rows, _ = openMutationCompletion(t, fixture.machine, equal)
	if rows != 1 || fixture.machine.state.DataChainDigest != beforeDigest {
		t.Fatalf("equal update rows=%d changed data chain", rows)
	}

	if _, err := fixture.machine.ApplyNormal(normalMeta(8), update); err != nil {
		t.Fatal(err)
	}
	_, rows, retried := openMutationCompletion(t, fixture.machine, update)
	if rows != 1 || !bytes.Equal(retried, firstWitness) {
		t.Fatal("exact retry did not reproduce the original affected-row witness")
	}
}

func TestConditionalUpdateBundleCountsOnlyBaseRelation(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	key := []byte("base-row")
	seed := fixture.command(t, 1, replication.RelationMutationBatch{
		Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: key, Value: []byte(`{"n":1}`),
		}},
	})
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), seed); err != nil {
		t.Fatal(err)
	}
	bundle := fixture.command(t, 2,
		replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutPresent, Key: key, Value: []byte(`{"n":2}`),
		}}},
		replication.RelationMutationBatch{Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual,
			Key:  []byte{0x91, 0x01, 'c'}, Value: []byte(`["base-row"]`),
		}}},
	)
	if _, err := fixture.machine.ApplyNormal(normalMeta(4), bundle); err != nil {
		t.Fatal(err)
	}
	completion, rows, _ := openMutationCompletion(t, fixture.machine, bundle)
	if completion.ResultCode != ResultApplied || rows != 1 {
		t.Fatalf("bundle completion=%+v rows=%d, want one base row", completion, rows)
	}
	conflict := fixture.command(t, 3,
		replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutPresent, Key: key, Value: []byte(`{"n":3}`),
		}}},
		replication.RelationMutationBatch{Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual,
			Key:  []byte{0x91, 0x01, 'c'}, Value: []byte(`["other-row"]`),
		}}},
	)
	if _, err := fixture.machine.ApplyNormal(normalMeta(5), conflict); err != nil {
		t.Fatal(err)
	}
	completion, rows, _ = openMutationCompletion(t, fixture.machine, conflict)
	if completion.ResultCode != ResultIndexConflict || rows != 0 ||
		completion.ResultLength != 0 || len(completion.InlineResult) != 0 {
		t.Fatalf("late conflict completion=%+v rows=%d", completion, rows)
	}
	if value, found, err := fixture.base.Collection.AppendRaw(nil, key); err != nil ||
		!found || !bytes.Equal(value, []byte(`{"n":2}`)) {
		t.Fatalf("late conflict leaked base write: value=%q found=%v err=%v", value, found, err)
	}
}

func TestConditionalPutTransactionAffectedRowsAndConflict(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	missingKey := []byte("transaction-missing")
	missingID := transactionCodecID(0xb1)
	missingBatches := []replication.RelationMutationBatch{{
		Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutPresent, Key: missingKey, Value: []byte(`{"n":1}`),
		}},
	}}
	stage := transactionParticipantStageCommand(t, fixture, missingID, missingBatches)
	applyTransactionCommand(t, fixture.machine, 3, stage)
	prepare := transactionParticipantTransitionCommand(
		t, fixture, missingID, distributedtxn.ReplicatedPrepareParticipant, 1,
	)
	applyTransactionCommand(t, fixture.machine, 4, prepare)
	apply := transactionParticipantTransitionCommand(
		t, fixture, missingID, distributedtxn.ReplicatedApplyParticipant, 2,
	)
	result := applyTransactionCommand(t, fixture.machine, 5, apply)
	if !result.AffectedRowsValid || result.AffectedRows != 0 {
		t.Fatalf("missing conditional update result=%+v", result)
	}
	release := transactionParticipantTransitionCommand(
		t, fixture, missingID, distributedtxn.ReplicatedReleaseParticipant, 3,
	)
	applyTransactionCommand(t, fixture.machine, 6, release)

	seed := fixture.command(t, 1, replication.RelationMutationBatch{
		Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: missingKey, Value: []byte(`{"n":1}`),
		}},
	})
	if _, err := fixture.machine.ApplyNormal(normalMeta(7), seed); err != nil {
		t.Fatal(err)
	}
	presentID := transactionCodecID(0xb2)
	presentBatches := []replication.RelationMutationBatch{
		{Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutPresent, Key: missingKey, Value: []byte(`{"n":2}`),
		}}},
		{Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual,
			Key:  []byte{0x91, 0x01, 't'}, Value: []byte(`["transaction-missing"]`),
		}}},
	}
	stage = transactionParticipantStageCommand(t, fixture, presentID, presentBatches)
	applyTransactionCommand(t, fixture.machine, 8, stage)
	prepare = transactionParticipantTransitionCommand(
		t, fixture, presentID, distributedtxn.ReplicatedPrepareParticipant, 1,
	)
	applyTransactionCommand(t, fixture.machine, 9, prepare)
	apply = transactionParticipantTransitionCommand(
		t, fixture, presentID, distributedtxn.ReplicatedApplyParticipant, 2,
	)
	result = applyTransactionCommand(t, fixture.machine, 10, apply)
	if !result.AffectedRowsValid || result.AffectedRows != 1 {
		t.Fatalf("present conditional update result=%+v", result)
	}
	firstRetry, err := fixture.machine.LookupCompletion(apply)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(11), apply); err != nil {
		t.Fatal(err)
	}
	secondRetry, err := fixture.machine.LookupCompletion(apply)
	if err != nil || !bytes.Equal(firstRetry.Bytes, secondRetry.Bytes) {
		t.Fatalf("transaction affected-row retry changed: err=%v", err)
	}
	release = transactionParticipantTransitionCommand(
		t, fixture, presentID, distributedtxn.ReplicatedReleaseParticipant, 3,
	)
	applyTransactionCommand(t, fixture.machine, 12, release)

	conflictID := transactionCodecID(0xb3)
	conflictBatches := []replication.RelationMutationBatch{{
		Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsent, Key: missingKey, Value: []byte(`{"n":3}`),
		}},
	}}
	stage = transactionParticipantStageCommand(t, fixture, conflictID, conflictBatches)
	applyTransactionCommand(t, fixture.machine, 13, stage)
	prepare = transactionParticipantTransitionCommand(
		t, fixture, conflictID, distributedtxn.ReplicatedPrepareParticipant, 1,
	)
	if _, err := fixture.machine.ApplyNormal(normalMeta(14), prepare); err != nil {
		t.Fatal(err)
	}
	completion, prepareResult := openTransactionCompletion(t, fixture.machine, prepare)
	if completion.ResultCode != ResultIndexConflict || prepareResult.AffectedRowsValid {
		t.Fatalf("strict insert prepare completion=%+v result=%+v", completion, prepareResult)
	}
}
