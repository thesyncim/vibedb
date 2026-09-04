package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestTransactionImageReopenAccountingIntentOwnershipAndTopologyFence(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	id := transactionCodecID(41)
	mutation := transactionCodecMutation()
	batches := []replication.RelationMutationBatch{{Relation: 1, Mutations: []replication.Mutation{mutation}}}
	mutationDigest, err := replication.TransactionMutationDigest(batches)
	if err != nil {
		t.Fatal(err)
	}
	command, err := replication.OpenCommand(testCommand(fixture.binding, 1, mutation))
	if err != nil {
		t.Fatal(err)
	}
	relations := command.RelationBatches()
	if !relations.Next() || relations.Batch().Relation != 1 {
		t.Fatal("transaction relation fixture did not expose relation 1")
	}
	relationBatch := relations.Batch()
	controlBytes, _ := TransactionControlResidentBytes(1)
	mutationBytes, _ := TransactionRelationPayloadResidentBytes(len(relationBatch.MutationBytes()))
	intentBytes, _ := TransactionIntentResidentBytes(len(mutation.Key))
	control := TransactionControl{
		ID: id, Role: distributedtxn.ReplicatedRoleTarget,
		State: uint8(distributedtxn.TargetPrepared), Revision: 2,
		PayloadKind:   distributedtxn.ReplicatedPayloadTargetStage,
		PayloadDigest: transactionCodecDigest(20), PayloadBytes: 4096, PayloadCount: 1,
		PayloadRelationCount:        1,
		CoordinatorGroup:            transactionCodecReplicationID(30),
		CoordinatorShardIncarnation: transactionCodecReplicationID(50),
		CoordinatorAllocation:       71, MutationDigest: mutationDigest,
		BucketBits: 8, IntentScopes: []distributedtxn.IntentScope{{Start: 1, End: 3}},
		ResidentControlBytes: controlBytes, ResidentMutationBytes: mutationBytes,
		ResidentIntentBytes:  intentBytes,
		LastOperation:        distributedtxn.ReplicatedPrepareTarget,
		LastExpectedRevision: 1, LastCommandDigest: transactionCodecCommandDigest(110),
		LastResultCode: ResultApplied, LastAppliedIndex: 2,
	}
	controlValue, err := AppendTransactionControl(nil, fencedTransactionTestControl(control))
	if err != nil {
		t.Fatal(err)
	}
	controlKey, _ := TransactionControlStorageKey(control.Role, id)
	mutationValue, err := AppendTransactionRelationPayload(nil, id, relationBatch)
	if err != nil {
		t.Fatal(err)
	}
	mutationKey, _ := TransactionRelationPayloadStorageKey(id, 1)
	intentValue, err := AppendTransactionIntent(nil, id, 1, mutation.Key)
	if err != nil {
		t.Fatal(err)
	}
	intentKey, _ := TransactionIntentStorageKey(1, mutation.Key)
	state := cloneState(fixture.machine.state)
	state.Applied = 2
	state.LastTerm = 1
	state.LastKind = RecordNormal
	state.LastEntryDigest = sha256.Sum256([]byte("transaction-image"))
	state.TransactionControlCount = 1
	state.ActiveTransactionCount = 1
	state.TransactionPayloadRows = 1
	state.TransactionIntentRows = 1
	state.TransactionResidentBytes = controlBytes + mutationBytes + intentBytes
	stateValue, err := AppendState(nil, state)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.system.Collection.Update(func(batch *durable.WriteBatch) error {
		for _, row := range []struct{ key, value []byte }{
			{stateKey, stateValue}, {controlKey[:], controlValue},
			{mutationKey[:], mutationValue}, {intentKey[:], intentValue},
		} {
			if putErr := batch.Put(row.key, row.value); putErr != nil {
				return putErr
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	reopen := func() (*Machine, error) {
		return Open(
			fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log,
			Options{TxnLimits: durable.TxnLimits{
				MaxCollections: 2, MaxDocuments: fixture.user.Limits.MaxDistinctMutations + 4,
				MaxBytes: 64 << 20,
			}, MaxSessions: 128, RetryWindow: 8},
		)
	}
	reopened, err := reopen()
	if err != nil {
		t.Fatal(err)
	}
	identity := transactionIntentIdentity{relation: 1, digest: sha256.Sum256(mutation.Key)}
	owner, ok := reopened.transactionIntents[identity]
	start, end := int(owner.keyOffset), int(owner.keyOffset+owner.keyBytes)
	if !ok || owner.id != id || end > len(reopened.transactionIntentKeys) ||
		!bytes.Equal(reopened.transactionIntentKeys[start:end], mutation.Key) {
		t.Fatalf("rebuilt intent owner=%+v found=%v", owner, ok)
	}
	ownership, err := AppendOwnershipTransition(nil, testOwnershipTransition(fixture.binding, 1))
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.AdmitCommand(ownership); !errors.Is(err, ErrOwnershipTransition) {
		t.Fatalf("active transaction ownership fence error=%v", err)
	}

	foreignID := transactionCodecID(99)
	foreignIntent, err := AppendTransactionIntent(nil, foreignID, 1, mutation.Key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.system.Collection.Put(intentKey[:], foreignIntent); err != nil {
		t.Fatal(err)
	}
	if _, err := reopen(); !errors.Is(err, ErrTransactionStateCorrupt) {
		t.Fatalf("orphan intent reopen error=%v", err)
	}
}

func TestFusedCoordinatorZeroVoteCannotReopenOrCommit(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	id, payload := transactionCodecCoordinatorPayload(t)
	payloadDigest := distributedtxn.Digest(sha256.Sum256(payload))
	controlBytes, _ := TransactionControlResidentBytes(0)
	payloadBytes, _ := TransactionCoordinatorPayloadResidentBytes(len(payload))
	control := TransactionControl{
		ID: id, Role: distributedtxn.ReplicatedRoleCoordinator,
		State: uint8(distributedtxn.CoordinatorStaging), Revision: 1,
		PayloadKind:   distributedtxn.ReplicatedPayloadCoordinator,
		PayloadDigest: payloadDigest, PayloadBytes: uint64(len(payload)), PayloadCount: 1,
		CoordinatorGroup:            fixture.binding.GroupID,
		CoordinatorShardIncarnation: fixture.binding.ShardIncarnation,
		CoordinatorAllocation:       fixture.binding.AllocationGeneration,
		MutationDigest:              payloadDigest,
		CoordinatorTargetOrdinal:    0,
		PrepareResultCode:           ResultApplied,
		FusedPath:                   true,
		ResidentControlBytes:        controlBytes,
		ResidentPayloadBytes:        payloadBytes,
		LastOperation:               distributedtxn.ReplicatedBeginPrepareCoordinator,
		LastCommandDigest:           transactionCodecCommandDigest(111),
		LastResultCode:              ResultApplied,
		LastAppliedIndex:            2,
	}
	controlValue, err := AppendTransactionControl(nil, fencedTransactionTestControl(control))
	if err != nil {
		t.Fatal(err)
	}
	// Simulate an authenticated-but-semantically-impossible durable image. The
	// checksum is deliberately renewed after removing the immutable vote, so
	// reopen must reject semantics rather than merely notice torn bytes.
	controlValue[15] &^= 3 << 4
	sealRecord(controlValue, transactionControlChecksumDomain)
	if _, err := OpenTransactionControl(controlValue); !errors.Is(err, ErrTransactionStateCorrupt) {
		t.Fatalf("zero-vote fused control open error=%v", err)
	}
	controlKey, _ := TransactionControlStorageKey(control.Role, id)
	payloadValue, err := AppendTransactionCoordinatorPayload(
		nil, id, distributedtxn.ReplicatedPayloadCoordinator, payload,
	)
	if err != nil {
		t.Fatal(err)
	}
	payloadKey, _ := TransactionCoordinatorPayloadStorageKey(id)
	state := cloneState(fixture.machine.state)
	state.Applied = 2
	state.LastTerm = 1
	state.LastKind = RecordNormal
	state.LastEntryDigest = sha256.Sum256([]byte("zero-vote-fused-coordinator"))
	state.TransactionControlCount = 1
	state.ActiveTransactionCount = 1
	state.TransactionPayloadRows = 1
	state.TransactionResidentBytes = controlBytes + payloadBytes
	stateValue, err := AppendState(nil, state)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.system.Collection.Update(func(batch *durable.WriteBatch) error {
		for _, row := range []struct{ key, value []byte }{
			{stateKey, stateValue}, {controlKey[:], controlValue}, {payloadKey[:], payloadValue},
		} {
			if putErr := batch.Put(row.key, row.value); putErr != nil {
				return putErr
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	_, err = Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log,
		Options{TxnLimits: durable.TxnLimits{
			MaxCollections: 2, MaxDocuments: fixture.user.Limits.MaxDistinctMutations + 4,
			MaxBytes: 64 << 20,
		}, MaxSessions: 128, RetryWindow: 8},
	)
	if !errors.Is(err, ErrTransactionStateCorrupt) {
		t.Fatalf("zero-vote fused coordinator reopen error=%v", err)
	}
}

func TestTargetAbortFenceReopensAndSettlesExactRetry(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	id := transactionCodecID(247)
	mutationDigest := distributedtxn.Digest(sha256.Sum256([]byte("abort-fence-mutation")))
	commandControl := distributedtxn.ReplicatedCommand{
		Role:        distributedtxn.ReplicatedRoleTarget,
		Operation:   distributedtxn.ReplicatedAbortReleaseTarget,
		ID:          id,
		PayloadKind: distributedtxn.ReplicatedPayloadTargetStage,
		Target: distributedtxn.TransactionTargetStage{
			CoordinatorGroup:            distributedtxn.ID(fixture.binding.GroupID),
			CoordinatorShardIncarnation: distributedtxn.ID(fixture.binding.ShardIncarnation),
			CoordinatorAllocation:       fixture.binding.AllocationGeneration,
			MutationDigest:              mutationDigest,
			TargetOrdinal:               4096,
		},
	}
	command := transactionCompletionCommand(t, fixture.binding, commandControl, nil)
	openedCommand, err := replication.OpenCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	controlBytes, _ := TransactionControlResidentBytes(0)
	control := TransactionControl{
		ID: id, Role: distributedtxn.ReplicatedRoleTarget,
		State: uint8(distributedtxn.TargetReleased), Revision: 1,
		PayloadKind:                 distributedtxn.ReplicatedPayloadTargetStage,
		PayloadDigest:               mutationDigest,
		CoordinatorGroup:            fixture.binding.GroupID,
		CoordinatorShardIncarnation: fixture.binding.ShardIncarnation,
		CoordinatorAllocation:       fixture.binding.AllocationGeneration,
		MutationDigest:              mutationDigest,
		TargetOrdinal:               4096,
		FusedPath:                   true, CancellationWitness: true,
		ResidentControlBytes: controlBytes,
		LastOperation:        distributedtxn.ReplicatedAbortReleaseTarget,
		LastExpectedRevision: 0,
		LastCommandDigest:    LogicalCommandDigest(openedCommand),
		LastResultCode:       ResultApplied,
		LastAppliedIndex:     2,
	}
	controlValue, err := AppendTransactionControl(nil, fencedTransactionTestControl(control))
	if err != nil {
		t.Fatal(err)
	}
	controlKey, _ := TransactionControlStorageKey(control.Role, id)
	state := cloneState(fixture.machine.state)
	state.Applied = 2
	state.LastTerm = 1
	state.LastKind = RecordNormal
	state.LastEntryDigest = sha256.Sum256([]byte("abort-fence-image"))
	state.TransactionControlCount = 1
	state.TransactionResidentBytes = controlBytes
	stateValue, err := AppendState(nil, state)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.system.Collection.Update(func(batch *durable.WriteBatch) error {
		if putErr := batch.Put(stateKey, stateValue); putErr != nil {
			return putErr
		}
		return batch.Put(controlKey[:], controlValue)
	}); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log,
		Options{TxnLimits: durable.TxnLimits{
			MaxCollections: 2, MaxDocuments: fixture.user.Limits.MaxDistinctMutations + 4,
			MaxBytes: 64 << 20,
		}, MaxSessions: 128, RetryWindow: 8},
	)
	if err != nil {
		t.Fatalf("reopen abort fence: %v", err)
	}
	completion, result := openTransactionCompletion(t, reopened, command)
	if completion.ResultCode != ResultApplied || result.Revision != 1 ||
		result.AffectedRowsValid {
		t.Fatalf("reopened abort fence completion=%+v result=%+v", completion, result)
	}
	records := make([]TransactionRecoveryRecord, 0, 1)
	recovery, err := reopened.TransactionRecoveryReadInto(TransactionRecoveryReadRequest{
		Kind: TransactionRecoveryLookupTarget, ID: id, MinimumApplied: 2,
		MaxRows: 1, MaxBytes: TransactionRecoverySummaryBytes,
	}, records, nil)
	if err != nil || len(recovery.Records) != 1 ||
		!recovery.Records[0].CancellationWitness ||
		recovery.Records[0].TargetOrdinal != 4096 ||
		recovery.Records[0].PayloadCount != 0 {
		t.Fatalf("reopened abort fence recovery=%+v err=%v", recovery, err)
	}
}
