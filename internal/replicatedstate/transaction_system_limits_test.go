package replicatedstate

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestTransactionStageFitsShippedSystemProfile(t *testing.T) {
	limits, ok := RequiredTransactionSystemCollectionLimits(8, false, 132)
	if !ok {
		t.Fatal("missing system profile")
	}
	fixture := newRelationBundleFixtureWithSystemOptions(t, true, false,
		durable.Options{}, durable.Options{}, RelationGlobalIndex, durable.Options{
			OpaqueValues: true, MaxKeyBytes: limits.MaxKeyBytes,
			MaxDocumentBytes:  limits.MaxDocumentBytes,
			MaxBatchDocuments: limits.MaxDistinctMutations, MaxBatchBytes: limits.MaxBatchBytes,
		})
	batches := []replication.RelationMutationBatch{{Relation: 1}, {Relation: 2}}
	for i := 0; i < 12; i++ {
		key := []byte(fmt.Sprintf("document-%02d", i))
		value := []byte(fmt.Sprintf(`{"email":"%02d@example.test","padding":"%s"}`, i,
			bytes.Repeat([]byte{'x'}, 256)))
		batches[0].Mutations = append(batches[0].Mutations, replication.Mutation{
			Kind: replication.MutationPutAbsentOrEqual, Key: key, Value: value,
		})
		batches[1].Mutations = append(batches[1].Mutations, replication.Mutation{
			Kind: replication.MutationPutAbsentOrEqual, Key: []byte{0x91, byte(i + 1)},
			Value: []byte(fmt.Sprintf(`["%s"]`, key)),
		})
	}
	id := transactionCodecID(221)
	control := fusedParticipantControl(t, fixture, id,
		distributedtxn.ReplicatedStagePrepareParticipant, 0, batches)
	command := transactionCompletionCommand(t, fixture.binding, control, batches)
	applyTransactionCommand(t, fixture.machine, 3, command)
	if fixture.machine.state.TransactionIntentRows != 24 || fixture.machine.state.TransactionPayloadRows != 2 {
		t.Fatalf("stage did not retain the full multi-relation image: %+v", fixture.machine.state)
	}
	finish := transactionCompletionCommand(t, fixture.binding,
		fusedParticipantControl(t, fixture, id, distributedtxn.ReplicatedApplyReleaseParticipant, 2, nil), nil)
	applyTransactionCommand(t, fixture.machine, 4, finish)
	if fixture.machine.state.TransactionIntentRows != 0 || fixture.machine.state.TransactionPayloadRows != 0 ||
		fixture.base.Collection.Len() != 12 || fixture.global.Collection.Len() != 12 {
		t.Fatal("atomic finish did not publish and release the complete image")
	}
}

func TestTransactionPrepareReservesAtomicFinishCapacity(t *testing.T) {
	limits, ok := RequiredTransactionSystemCollectionLimits(8, false, 132)
	if !ok {
		t.Fatal("missing system profile")
	}
	fixture := newRelationBundleFixtureWithSystemOptions(t, true, false,
		durable.Options{}, durable.Options{}, RelationGlobalIndex, durable.Options{
			OpaqueValues: true, MaxKeyBytes: limits.MaxKeyBytes,
			MaxDocumentBytes:  limits.MaxDocumentBytes,
			MaxBatchDocuments: limits.MaxDistinctMutations, MaxBatchBytes: limits.MaxBatchBytes,
		})
	// Two relations with 32 mutations each require 64 user writes plus 64
	// intent deletions, two payload deletions, the control and the state row.
	const finishDocuments = 132
	if fixture.machine.options.TxnLimits.MaxDocuments != finishDocuments {
		t.Fatalf("fixture transaction budget=%d", fixture.machine.options.TxnLimits.MaxDocuments)
	}
	batches := []replication.RelationMutationBatch{{Relation: 1}, {Relation: 2}}
	for i := 0; i < 32; i++ {
		batches[0].Mutations = append(batches[0].Mutations, replication.Mutation{
			Kind: replication.MutationPutAbsentOrEqual, Key: []byte(fmt.Sprintf("doc-%02d", i)),
			Value: []byte(fmt.Sprintf(`{"email":"%02d@example.test"}`, i)),
		})
		batches[1].Mutations = append(batches[1].Mutations, replication.Mutation{
			Kind: replication.MutationPutAbsentOrEqual, Key: []byte{0x91, byte(i + 1)}, Value: []byte(fmt.Sprintf(`["doc-%02d"]`, i)),
		})
	}
	id := transactionCodecID(222)
	control := fusedParticipantControl(t, fixture, id, distributedtxn.ReplicatedStagePrepareParticipant, 0, batches)
	command := transactionCompletionCommand(t, fixture.binding, control, batches)
	fixture.machine.options.TxnLimits.MaxDocuments = finishDocuments - 1
	if err := fixture.machine.AdmitCommand(command); !errors.Is(err, ErrAdmissionBound) {
		t.Fatalf("prepare accepted without room for atomic finish: %v", err)
	}
	if fixture.machine.state.TransactionIntentRows != 0 || fixture.machine.state.TransactionPayloadRows != 0 {
		t.Fatal("refused prepare retained transaction work")
	}
	fixture.machine.options.TxnLimits.MaxDocuments = finishDocuments
	if err := fixture.machine.AdmitCommand(command); err != nil {
		t.Fatalf("exact finish capacity refused: %v", err)
	}
	applyTransactionCommand(t, fixture.machine, 3, command)
	finish := transactionCompletionCommand(t, fixture.binding,
		fusedParticipantControl(t, fixture, id, distributedtxn.ReplicatedApplyReleaseParticipant, 2, nil), nil)
	applyTransactionCommand(t, fixture.machine, 4, finish)
	if fixture.machine.state.TransactionIntentRows != 0 || fixture.machine.state.TransactionPayloadRows != 0 ||
		fixture.base.Collection.Len() != 32 || fixture.global.Collection.Len() != 32 {
		t.Fatal("exact-capacity transaction failed to finish")
	}
}
