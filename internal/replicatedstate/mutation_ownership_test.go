package replicatedstate

import (
	"bytes"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replication"
)

type mutationOwnershipValidator struct{ MutationValidator }

func (mutationOwnershipValidator) ValidatePutOwnership(
	key, _ []byte,
	owned distribution.KeyRange,
) MutationValidation {
	return validateTestMutationPoint(key, owned)
}

func (mutationOwnershipValidator) ValidateDeleteOwnership(
	key, _ []byte,
	_ bool,
	owned distribution.KeyRange,
) MutationValidation {
	return validateTestMutationPoint(key, owned)
}

func validateTestMutationPoint(
	key []byte,
	owned distribution.KeyRange,
) MutationValidation {
	if len(key) == 0 {
		return MutationValidationInvalid
	}
	if !owned.Contains(distribution.KeyspacePoint{key[0]}) {
		return MutationValidationWrongShard
	}
	return MutationValidationAccept
}

func TestMutationBundleFailsClosedAtomicallyForUnprovableNarrowedRelation(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	fixture.machine.state.Binding.OwnedRange = distribution.KeyRange{
		End: distribution.KeyspaceEnd{Point: distribution.KeyspacePoint{0x80}},
	}
	fixture.machine.binding.OwnedRange = fixture.machine.state.Binding.OwnedRange
	fixture.machine.relations[0].target.Validator = mutationOwnershipValidator{
		MutationValidator: fixture.machine.relations[0].target.Validator,
	}
	if _, ok := fixture.machine.relations[0].target.Validator.(OwnershipMutationValidator); !ok {
		t.Fatal("test ownership validator does not implement the ownership contract")
	}
	baseKey := []byte{0x20, 'b'}
	globalKey := []byte{0x20, 'i'}
	command := fixture.command(t, 1,
		replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: baseKey,
			Value: []byte(`{"email":"inside@example.com"}`),
		}}},
		replication.RelationMutationBatch{Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: globalKey,
			Value: []byte(`["inside"]`),
		}}},
	)
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), command); err != nil {
		t.Fatal(err)
	}
	if code := bundleCompletionResult(t, fixture.machine, command); code != ResultWrongShard {
		t.Fatalf("completion result=%d, want ResultWrongShard", code)
	}
	if raw, found, err := fixture.base.Collection.AppendRaw(nil, baseKey); err != nil || found || raw != nil {
		t.Fatalf("base changed: raw=%q found=%v err=%v", raw, found, err)
	}
	if raw, found, err := fixture.global.Collection.AppendRaw(nil, globalKey); err != nil || found || raw != nil {
		t.Fatalf("global changed: raw=%q found=%v err=%v", raw, found, err)
	}
}

func TestMutationOwnershipProofAcceptsInsideAndRejectsOutsideNarrowedRange(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	fixture.machine.state.Binding.OwnedRange = distribution.KeyRange{
		End: distribution.KeyspaceEnd{Point: distribution.KeyspacePoint{0x80}},
	}
	fixture.machine.binding.OwnedRange = fixture.machine.state.Binding.OwnedRange
	fixture.machine.relations[0].target.Validator = mutationOwnershipValidator{
		MutationValidator: fixture.machine.relations[0].target.Validator,
	}
	insideKey := []byte{0x20}
	insideValue := []byte(`{"email":"inside@example.com"}`)
	inside := fixture.command(t, 1, replication.RelationMutationBatch{
		Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: insideKey, Value: insideValue,
		}},
	})
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), inside); err != nil {
		t.Fatal(err)
	}
	if code := bundleCompletionResult(t, fixture.machine, inside); code != ResultApplied {
		t.Fatalf("inside result=%d", code)
	}
	outsideKey := []byte{0x90}
	if got := validateRelationMutationOwnership(
		fixture.machine.relations[0].target.Validator,
		finalMutation{key: outsideKey, value: []byte(`{"email":"outside@example.com"}`)},
		nil, false, fixture.machine.state.Binding.OwnedRange,
	); got != MutationValidationWrongShard {
		t.Fatalf("direct outside validation=%d", got)
	}
	outside := fixture.command(t, 2, replication.RelationMutationBatch{
		Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: outsideKey,
			Value: []byte(`{"email":"outside@example.com"}`),
		}},
	})
	if _, err := fixture.machine.ApplyNormal(normalMeta(4), outside); err != nil {
		t.Fatal(err)
	}
	if code := bundleCompletionResult(t, fixture.machine, outside); code != ResultWrongShard {
		t.Fatalf("outside result=%d", code)
	}
	raw, found, err := fixture.base.Collection.AppendRaw(nil, insideKey)
	if err != nil || !found || !bytes.Equal(raw, insideValue) {
		t.Fatalf("inside row=%q found=%v err=%v", raw, found, err)
	}
	if raw, found, err = fixture.base.Collection.AppendRaw(nil, outsideKey); err != nil || found || raw != nil {
		t.Fatalf("outside row=%q found=%v err=%v", raw, found, err)
	}
}

func TestUnprovableMutationRelationRemainsCompatibleForCompleteRange(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	key := []byte{0x91, 0x01, 'g'}
	value := []byte(`["locator"]`)
	command := fixture.command(t, 1, replication.RelationMutationBatch{
		Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: key, Value: value,
		}},
	})
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), command); err != nil {
		t.Fatal(err)
	}
	if code := bundleCompletionResult(t, fixture.machine, command); code != ResultApplied {
		t.Fatalf("full-range result=%d", code)
	}
	raw, found, err := fixture.global.Collection.AppendRaw(nil, key)
	if err != nil || !found || !bytes.Equal(raw, value) {
		t.Fatalf("global row=%q found=%v err=%v", raw, found, err)
	}
}

func TestFusedTransactionPersistsExactWrongShardVoteForUnprovableRelation(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	fixture.machine.state.Binding.OwnedRange = distribution.KeyRange{
		End: distribution.KeyspaceEnd{Point: distribution.KeyspacePoint{0x80}},
	}
	fixture.machine.binding.OwnedRange = fixture.machine.state.Binding.OwnedRange
	batches := []replication.RelationMutationBatch{{
		Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: []byte{0x20, 'i'},
			Value: []byte(`["inside"]`),
		}},
	}}
	id := transactionCodecID(247)
	control := fusedParticipantControl(
		t, fixture, id, distributedtxn.ReplicatedStagePrepareParticipant, 0, batches,
	)
	command := transactionCompletionCommand(t, fixture.binding, control, batches)
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), command); err != nil {
		t.Fatalf("wrong-shard vote poisoned apply: %v", err)
	}
	completion, result := openTransactionCompletion(t, fixture.machine, command)
	if completion.ResultCode != ResultWrongShard || result.Revision != 3 ||
		result.AffectedRowsValid {
		t.Fatalf("wrong-shard completion=%+v result=%+v", completion, result)
	}
	controlKey, _ := TransactionControlStorageKey(
		distributedtxn.ReplicatedRoleParticipant, id,
	)
	raw, found, err := fixture.system.Collection.AppendRaw(nil, controlKey[:])
	if err != nil || !found {
		t.Fatalf("wrong-shard vote found=%v err=%v", found, err)
	}
	vote, err := OpenTransactionControl(raw)
	if err != nil || vote.State != uint8(distributedtxn.ParticipantReleased) ||
		vote.PrepareResultCode != ResultWrongShard || vote.LastResultCode != ResultWrongShard ||
		vote.ResidentMutationBytes != 0 || vote.ResidentIntentBytes != 0 {
		t.Fatalf("wrong-shard vote=%+v err=%v", vote.TransactionControl, err)
	}
	if fixture.machine.state.ActiveTransactionCount != 0 ||
		fixture.machine.state.TransactionPayloadRows != 0 ||
		fixture.machine.state.TransactionIntentRows != 0 {
		t.Fatalf("wrong-shard vote retained transaction state=%+v", fixture.machine.state)
	}
	retryCompletion, retryResult := openTransactionCompletion(t, fixture.machine, command)
	if retryCompletion.ResultCode != ResultWrongShard || retryResult.Revision != 3 {
		t.Fatalf("wrong-shard retry completion=%+v result=%+v", retryCompletion, retryResult)
	}
}
