package replicatedstate

import (
	"bytes"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibejson"
)

// This fixture program is a canonical replacement. Driver tests exercise the
// actual column program; these tests isolate the machine's branch, atomicity,
// participant and retained-completion contracts.
type conflictReplacementValidator struct {
	MutationValidator
	calls int
}

func (v *conflictReplacementValidator) MaterializeConflict(key, candidate, program, current []byte, found bool) ([]byte, bool, MutationValidation) {
	v.calls++
	if !vibejson.Valid(program) {
		return nil, false, MutationValidationInvalid
	}
	if !found {
		return candidate, true, MutationValidationAccept
	}
	if bytes.Equal(program, []byte("null")) {
		return current, false, MutationValidationAccept
	}
	return program, true, MutationValidationAccept
}

func TestConflictMutationAppliesAtSnapshotAndRetainsResult(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	validator := &conflictReplacementValidator{MutationValidator: fixture.machine.relations[0].target.Validator}
	fixture.machine.relations[0].target.Validator = validator
	key := []byte("a")
	index := uint64(3)
	for i, tc := range []struct {
		candidate, program, want string
		code                     uint32
		rows                     int64
	}{
		{`{"n":1}`, `{"n":9}`, `{"n":1}`, ResultApplied, 1},
		{`{"n":2}`, `{"n":3}`, `{"n":3}`, ResultApplied, 1},
		{`{"n":4}`, `null`, `{"n":3}`, ResultApplied, 0},
		{`invalid`, `{"n":4}`, `{"n":3}`, ResultInvalidDocument, 0},
		{`{"n":4}`, `invalid`, `{"n":3}`, ResultInvalidDocument, 0},
	} {
		value, err := replication.AppendConflictValue(nil, []byte(tc.candidate), []byte(tc.program))
		if err != nil {
			t.Fatal(err)
		}
		command := fixture.command(t, uint64(i+1), replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{{Kind: replication.MutationPutConflict, Key: key, Value: value}}})
		if _, err := fixture.machine.ApplyNormal(normalMeta(index), command); err != nil {
			t.Fatal(err)
		}
		index++
		completion, rows, witness := openMutationCompletion(t, fixture.machine, command)
		if completion.ResultCode != tc.code || rows != tc.rows {
			t.Fatalf("completion=%+v rows=%d", completion, rows)
		}
		calls := validator.calls
		if _, err := fixture.machine.ApplyNormal(normalMeta(index), command); err != nil {
			t.Fatal(err)
		}
		index++
		_, retryRows, retry := openMutationCompletion(t, fixture.machine, command)
		if retryRows != rows || !bytes.Equal(retry, witness) || validator.calls != calls {
			t.Fatal("retry reevaluated the conflict action")
		}
		raw, found, err := fixture.base.Collection.AppendRaw(nil, key)
		if err != nil || !found || string(raw) != tc.want {
			t.Fatalf("raw=%s found=%v err=%v", raw, found, err)
		}
	}
	value, _ := replication.AppendConflictValue(nil, []byte(`{"n":5}`), []byte(`{"n":7}`))
	id := transactionCodecID(0xd3)
	batches := []replication.RelationMutationBatch{{Relation: 1, Mutations: []replication.Mutation{{Kind: replication.MutationPutConflict, Key: key, Value: value}}}}
	applyTransactionCommand(t, fixture.machine, index, transactionTargetStageCommand(t, fixture, id, batches))
	index++
	applyTransactionCommand(t, fixture.machine, index, transactionTargetTransitionCommand(t, fixture, id, distributedtxn.ReplicatedPrepareTarget, 1))
	index++
	result := applyTransactionCommand(t, fixture.machine, index, transactionTargetTransitionCommand(t, fixture, id, distributedtxn.ReplicatedApplyTarget, 2))
	if !result.AffectedRowsValid || result.AffectedRows != 1 {
		t.Fatalf("participant result=%+v", result)
	}
	raw, found, err := fixture.base.Collection.AppendRaw(nil, key)
	if err != nil || !found || string(raw) != `{"n":7}` {
		t.Fatalf("participant raw=%s found=%v err=%v", raw, found, err)
	}
}

type ownedConflictValidator struct{ *conflictReplacementValidator }

func (v ownedConflictValidator) ValidatePutOwnership(key, value []byte, owned distribution.KeyRange) MutationValidation {
	return validateTestMutationPoint(key, owned)
}

func (v ownedConflictValidator) ValidateDeleteOwnership(key, current []byte, found bool, owned distribution.KeyRange) MutationValidation {
	return validateTestMutationPoint(key, owned)
}

func TestConflictConditionNoopRetainsParticipantResultAndCurrentOwnershipFence(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	validator := ownedConflictValidator{&conflictReplacementValidator{MutationValidator: fixture.machine.relations[0].target.Validator}}
	fixture.machine.relations[0].target.Validator = validator
	key := []byte{0x90}
	value, _ := replication.AppendConflictValue(nil, []byte(`{"n":1}`), []byte("null"))
	batch := replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{{Kind: replication.MutationPutConflict, Key: key, Value: value}}}
	seed := fixture.command(t, 1, batch)
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), seed); err != nil {
		t.Fatal(err)
	}
	if code := bundleCompletionResult(t, fixture.machine, seed); code != ResultApplied {
		t.Fatalf("seed code=%d", code)
	}
	id := transactionCodecID(0xd5)
	applyTransactionCommand(t, fixture.machine, 4, transactionTargetStageCommand(t, fixture, id, []replication.RelationMutationBatch{batch}))
	applyTransactionCommand(t, fixture.machine, 5, transactionTargetTransitionCommand(t, fixture, id, distributedtxn.ReplicatedPrepareTarget, 1))
	apply := transactionTargetTransitionCommand(t, fixture, id, distributedtxn.ReplicatedApplyTarget, 2)
	result := applyTransactionCommand(t, fixture.machine, 6, apply)
	if !result.AffectedRowsValid || result.AffectedRows != 0 {
		t.Fatalf("participant no-op=%+v", result)
	}
	calls := validator.calls
	retry := applyTransactionCommand(t, fixture.machine, 7, apply)
	if !retry.AffectedRowsValid || retry.AffectedRows != 0 || validator.calls != calls {
		t.Fatalf("participant retry=%+v calls=%d/%d", retry, validator.calls, calls)
	}
	applyTransactionCommand(t, fixture.machine, 8, transactionTargetTransitionCommand(t, fixture, id, distributedtxn.ReplicatedReleaseTarget, 3))
	fixture.machine.state.Binding.OwnedRange = distribution.KeyRange{End: distribution.KeyspaceEnd{Point: distribution.KeyspacePoint{0x80}}}
	fixture.machine.binding.OwnedRange = fixture.machine.state.Binding.OwnedRange
	outside := fixture.command(t, 2, batch)
	if _, err := fixture.machine.ApplyNormal(normalMeta(9), outside); err != nil {
		t.Fatal(err)
	}
	if code := bundleCompletionResult(t, fixture.machine, outside); code != ResultWrongShard {
		t.Fatalf("no-op bypassed new ownership: %d", code)
	}
	if raw, found, err := fixture.base.Collection.AppendRaw(nil, key); err != nil || !found || string(raw) != `{"n":1}` {
		t.Fatalf("no-op mutated row: %s %v %v", raw, found, err)
	}
}

func TestConflictMutationRejectsBatchWithoutPartialPublication(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	fixture.machine.relations[0].target.Validator = &conflictReplacementValidator{MutationValidator: fixture.machine.relations[0].target.Validator}
	good, _ := replication.AppendConflictValue(nil, []byte(`{"n":1}`), []byte(`{"n":2}`))
	bad, _ := replication.AppendConflictValue(nil, []byte(`{"n":1}`), []byte(`invalid`))
	for i, mutations := range [][]replication.Mutation{
		{{Kind: replication.MutationPutConflict, Key: []byte("a"), Value: good}, {Kind: replication.MutationPutConflict, Key: []byte("b"), Value: bad}},
		{{Kind: replication.MutationPutConflict, Key: []byte("a"), Value: good}, {Kind: replication.MutationPutConflict, Key: []byte("a"), Value: good}},
	} {
		command := fixture.command(t, uint64(i+1), replication.RelationMutationBatch{Relation: 1, Mutations: mutations})
		if _, err := fixture.machine.ApplyNormal(normalMeta(uint64(i+3)), command); err != nil {
			t.Fatal(err)
		}
		completion, rows, _ := openMutationCompletion(t, fixture.machine, command)
		if completion.ResultCode != ResultInvalidDocument || rows != 0 {
			t.Fatalf("result=%+v rows=%d", completion, rows)
		}
		if _, found, err := fixture.base.Collection.AppendRaw(nil, []byte("a")); err != nil || found {
			t.Fatalf("partial publication: found=%v err=%v", found, err)
		}
	}
}
