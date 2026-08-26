package replicatedstate

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replication"
)

func TestTransactionRecoveryReadRequestGrammarAndBounds(t *testing.T) {
	id := transactionCodecID(211)
	valid := []TransactionRecoveryReadRequest{
		{Kind: TransactionRecoveryLookupCoordinator, ID: id, MinimumApplied: 1, MaxRows: 1, MaxBytes: TransactionRecoverySummaryBytes},
		{Kind: TransactionRecoveryLookupParticipant, ID: id, MinimumApplied: 1, MaxRows: 1, MaxBytes: TransactionRecoverySummaryBytes},
		{Kind: TransactionRecoveryReadManifestPage, ID: id, ManifestPage: 7, MinimumApplied: 1, MaxRows: 1, MaxBytes: MaxTransactionRecoveryReadBytes},
		{Kind: TransactionRecoveryScanCoordinator, MinimumApplied: 1, MaxRows: MaxTransactionRecoveryScanRows, MaxBytes: MaxTransactionRecoveryScanBytes},
	}
	for index, request := range valid {
		if err := ValidateTransactionRecoveryReadRequest(request); err != nil {
			t.Fatalf("valid request %d: %v", index, err)
		}
	}
	invalid := []TransactionRecoveryReadRequest{
		{},
		{Kind: TransactionRecoveryLookupCoordinator, ID: id, MaxRows: 1, MaxBytes: TransactionRecoverySummaryBytes},
		{Kind: TransactionRecoveryLookupCoordinator, MinimumApplied: 1, MaxRows: 1, MaxBytes: TransactionRecoverySummaryBytes},
		{Kind: TransactionRecoveryLookupParticipant, ID: id, MinimumApplied: 1, MaxRows: 1, MaxBytes: TransactionRecoverySummaryBytes + 1},
		{Kind: TransactionRecoveryReadManifestPage, ID: id, MinimumApplied: 1, MaxRows: 1, MaxBytes: MaxTransactionRecoveryReadBytes + 1},
		{Kind: TransactionRecoveryScanCoordinator, MinimumApplied: 1, MaxRows: MaxTransactionRecoveryScanRows + 1, MaxBytes: MaxTransactionRecoveryScanBytes},
		{Kind: TransactionRecoveryScanCoordinator, MinimumApplied: 1, MaxRows: 1, MaxBytes: MaxTransactionRecoveryScanBytes + 1},
	}
	for index, request := range invalid {
		if err := ValidateTransactionRecoveryReadRequest(request); !errors.Is(err, ErrTransactionRecoveryRead) {
			t.Fatalf("invalid request %d error=%v", index, err)
		}
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if err := ValidateTransactionRecoveryReadRequest(valid[3]); err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("request validation allocations=%v", allocations)
	}
	if TransactionRecoverySummaryBytes != 121 || MaxTransactionRecoveryScanBytes != 30976 ||
		MaxTransactionRecoveryReadBytes != 65657 {
		t.Fatalf("recovery byte bounds summary=%d scan=%d read=%d",
			TransactionRecoverySummaryBytes, MaxTransactionRecoveryScanBytes,
			MaxTransactionRecoveryReadBytes)
	}
}

func TestTransactionRecoveryReadInlineParticipantAndExclusiveScan(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	coordinatorID, coordinatorPayload := transactionCodecCoordinatorPayload(t)
	coordinatorCommand := transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
		Role: distributedtxn.ReplicatedRoleCoordinator, Operation: distributedtxn.ReplicatedStageCoordinator,
		ID: coordinatorID, PayloadKind: distributedtxn.ReplicatedPayloadCoordinator, Payload: coordinatorPayload,
	}, nil)
	applyTransactionCommand(t, fixture.machine, 2, coordinatorCommand)

	participantID := transactionCodecID(212)
	mutation := replication.Mutation{Kind: replication.MutationPut, Key: []byte{1, 2}, Value: []byte{3}}
	batches := []replication.RelationMutationBatch{{Relation: 1, Mutations: []replication.Mutation{mutation}}}
	mutationDigest, err := replication.TransactionMutationDigest(batches)
	if err != nil {
		t.Fatal(err)
	}
	participantCommand := transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
		Role: distributedtxn.ReplicatedRoleParticipant, Operation: distributedtxn.ReplicatedStageParticipant,
		ID: participantID, PayloadKind: distributedtxn.ReplicatedPayloadParticipantStage,
		Participant: distributedtxn.ParticipantStage{
			CoordinatorGroup:            distributedtxn.ID(fixture.binding.GroupID),
			CoordinatorShardIncarnation: distributedtxn.ID(fixture.binding.ShardIncarnation),
			CoordinatorAllocation:       fixture.binding.AllocationGeneration,
			BucketBits:                  8, IntentScopes: []distributedtxn.IntentScope{{Start: 0, End: 256}},
			MutationDigest: mutationDigest,
		},
	}, batches)
	applyTransactionCommand(t, fixture.machine, 3, participantCommand)

	records := make([]TransactionRecoveryRecord, 0, MaxTransactionRecoveryScanRows)
	payload := make([]byte, 0, MaxTransactionRecoveryPayloadArenaBytes)
	coordinator, err := fixture.machine.TransactionRecoveryReadInto(TransactionRecoveryReadRequest{
		Kind: TransactionRecoveryLookupCoordinator, ID: coordinatorID, MinimumApplied: 3,
		MaxRows: 1, MaxBytes: uint32(TransactionRecoverySummaryBytes + len(coordinatorPayload)),
	}, records, payload)
	if err != nil || !coordinator.Complete || len(coordinator.Records) != 1 ||
		coordinator.Records[0].ID != coordinatorID ||
		!bytes.Equal(coordinator.Records[0].Payload, coordinatorPayload) || coordinator.Fence.Applied < 3 {
		t.Fatalf("coordinator recovery=%+v err=%v", coordinator, err)
	}
	if _, err := fixture.machine.TransactionRecoveryReadInto(TransactionRecoveryReadRequest{
		Kind: TransactionRecoveryReadManifestPage, ID: coordinatorID, MinimumApplied: 3,
		MaxRows: 1, MaxBytes: MaxTransactionRecoveryReadBytes,
	}, records, payload); !errors.Is(err, ErrTransactionRecoveryRead) {
		t.Fatalf("inline manifest-page error=%v", err)
	}
	participant, err := fixture.machine.TransactionRecoveryReadInto(TransactionRecoveryReadRequest{
		Kind: TransactionRecoveryLookupParticipant, ID: participantID, MinimumApplied: 3,
		MaxRows: 1, MaxBytes: TransactionRecoverySummaryBytes,
	}, records, nil)
	if err != nil || len(participant.Records) != 1 || participant.Records[0].ID != participantID ||
		participant.Records[0].Role != distributedtxn.ReplicatedRoleParticipant ||
		len(participant.Records[0].Payload) != 0 {
		t.Fatalf("participant recovery=%+v err=%v", participant, err)
	}
	if allocations := testing.AllocsPerRun(100, func() {
		result, readErr := fixture.machine.TransactionRecoveryReadInto(TransactionRecoveryReadRequest{
			Kind: TransactionRecoveryLookupParticipant, ID: participantID, MinimumApplied: 3,
			MaxRows: 1, MaxBytes: TransactionRecoverySummaryBytes,
		}, records, nil)
		if readErr != nil || len(result.Records) != 1 {
			panic(readErr)
		}
	}); allocations > 1 && !(raceDetectorEnabled && allocations <= 3) {
		t.Fatalf("participant recovery allocations=%v", allocations)
	}
	first, err := fixture.machine.TransactionRecoveryReadInto(TransactionRecoveryReadRequest{
		Kind: TransactionRecoveryScanCoordinator, MinimumApplied: 3,
		MaxRows: 1, MaxBytes: TransactionRecoverySummaryBytes,
	}, records, nil)
	if err != nil || first.Complete || len(first.Records) != 1 || first.Records[0].ID != coordinatorID {
		t.Fatalf("first scan=%+v err=%v", first, err)
	}
	second, err := fixture.machine.TransactionRecoveryReadInto(TransactionRecoveryReadRequest{
		Kind: TransactionRecoveryScanCoordinator, ID: first.Records[0].ID, MinimumApplied: 3,
		MaxRows: 1, MaxBytes: TransactionRecoverySummaryBytes,
	}, records, nil)
	if err != nil || !second.Complete || len(second.Records) != 0 {
		t.Fatalf("continued scan=%+v err=%v", second, err)
	}
	if _, err := fixture.machine.TransactionRecoveryReadInto(TransactionRecoveryReadRequest{
		Kind: TransactionRecoveryLookupCoordinator, ID: coordinatorID, MinimumApplied: 5,
		MaxRows: 1, MaxBytes: TransactionRecoverySummaryBytes,
	}, records, payload); !errors.Is(err, ErrReadBehind) {
		t.Fatalf("minimum-applied error=%v", err)
	}
	if _, err := fixture.machine.TransactionRecoveryReadInto(TransactionRecoveryReadRequest{
		Kind: TransactionRecoveryLookupCoordinator, ID: coordinatorID, MinimumApplied: 3,
		MaxRows: 1, MaxBytes: TransactionRecoverySummaryBytes,
	}, records, payload); !errors.Is(err, ErrReadBufferBound) {
		t.Fatalf("response byte bound error=%v", err)
	}
	if _, err := fixture.machine.TransactionRecoveryReadInto(TransactionRecoveryReadRequest{
		Kind: TransactionRecoveryLookupParticipant, ID: transactionCodecID(250), MinimumApplied: 3,
		MaxRows: 1, MaxBytes: TransactionRecoverySummaryBytes,
	}, records, nil); err != nil {
		t.Fatalf("missing participant lookup error=%v", err)
	}
}

func TestTransactionRecoveryReadManifestBeyondInlineParticipantLimit(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	id := transactionCodecID(213)
	pageScratch := make([]byte, distributedtxn.ManifestSegmentBytes)
	var pageZero []byte
	builder, err := distributedtxn.NewManifestBuilder(pageScratch, func(segment distributedtxn.ManifestSegment) error {
		if segment.Index == 0 {
			pageZero = bytes.Clone(segment.Raw)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < distributedtxn.MaxInlineParticipants+1; index++ {
		var digest distributedtxn.Digest
		digest[0] = byte(index + 1)
		if err := builder.Append(distributedtxn.ParticipantRef{
			Distribution: []byte{1}, Shard: []byte{byte(index + 1)},
			RoutingVersion: 1, AllocationGeneration: 1, OwnershipEpoch: 1,
			MutationDigest: digest, State: distributedtxn.ParticipantStaged,
		}); err != nil {
			t.Fatal(err)
		}
	}
	descriptor, err := builder.Seal()
	if err != nil || descriptor.ParticipantCount != distributedtxn.MaxInlineParticipants+1 || len(pageZero) == 0 {
		t.Fatalf("manifest descriptor=%+v page=%d err=%v", descriptor, len(pageZero), err)
	}
	coordinator, err := distributedtxn.AppendManifestCoordinator(nil, distributedtxn.ManifestCoordinatorRecord{
		ID: id, State: distributedtxn.CoordinatorStaging, Revision: 1,
		CatalogGeneration: 1, RecoveryDeadline: 2, Manifest: descriptor,
	})
	if err != nil {
		t.Fatal(err)
	}
	start := append(coordinator, pageZero...)
	command := transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
		Role: distributedtxn.ReplicatedRoleCoordinator, Operation: distributedtxn.ReplicatedStageManifestCoordinator,
		ID: id, PayloadKind: distributedtxn.ReplicatedPayloadManifestCoordinator, Payload: start,
	}, nil)
	applyTransactionCommand(t, fixture.machine, 2, command)
	records := make([]TransactionRecoveryRecord, 0, 1)
	payload := make([]byte, 0, MaxTransactionRecoveryPayloadArenaBytes)
	result, err := fixture.machine.TransactionRecoveryReadInto(TransactionRecoveryReadRequest{
		Kind: TransactionRecoveryReadManifestPage, ID: id, ManifestPage: 0, MinimumApplied: 2,
		MaxRows: 1, MaxBytes: MaxTransactionRecoveryReadBytes,
	}, records, payload)
	if err != nil || len(result.Records) != 1 ||
		result.Records[0].PayloadCount != distributedtxn.MaxInlineParticipants+1 ||
		!bytes.Equal(result.Records[0].Payload, pageZero) {
		t.Fatalf("manifest page recovery=%+v err=%v", result, err)
	}
}

func TestTransactionRecoveryReadFailsClosedOnCorruptPayload(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	id, coordinatorPayload := transactionCodecCoordinatorPayload(t)
	command := transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
		Role: distributedtxn.ReplicatedRoleCoordinator, Operation: distributedtxn.ReplicatedStageCoordinator,
		ID: id, PayloadKind: distributedtxn.ReplicatedPayloadCoordinator, Payload: coordinatorPayload,
	}, nil)
	applyTransactionCommand(t, fixture.machine, 2, command)
	key, _ := TransactionCoordinatorPayloadStorageKey(id)
	row, found, err := fixture.system.Collection.AppendRaw(nil, key[:])
	if err != nil || !found {
		t.Fatalf("payload row found=%v err=%v", found, err)
	}
	row[len(row)-1] ^= 1
	if _, err := fixture.system.Collection.Put(key[:], row); err != nil {
		t.Fatal(err)
	}
	records := make([]TransactionRecoveryRecord, 0, 1)
	payload := make([]byte, 0, MaxTransactionRecoveryPayloadArenaBytes)
	_, err = fixture.machine.TransactionRecoveryReadInto(TransactionRecoveryReadRequest{
		Kind: TransactionRecoveryLookupCoordinator, ID: id, MinimumApplied: 2,
		MaxRows: 1, MaxBytes: uint32(TransactionRecoverySummaryBytes + len(coordinatorPayload)),
	}, records, payload)
	if !errors.Is(err, ErrTransactionStateCorrupt) {
		t.Fatalf("corrupt coordinator payload error=%v", err)
	}
	if _, nextErr := fixture.machine.TransactionRecoveryReadInto(TransactionRecoveryReadRequest{
		Kind: TransactionRecoveryLookupParticipant, ID: transactionCodecID(251), MinimumApplied: 2,
		MaxRows: 1, MaxBytes: TransactionRecoverySummaryBytes,
	}, records, nil); nextErr == nil {
		t.Fatal("corrupt recovery read did not poison machine")
	}
}
