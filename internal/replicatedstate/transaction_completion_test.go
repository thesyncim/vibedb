package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replication"
)

func transactionCompletionCommand(
	t testing.TB,
	binding Binding,
	control distributedtxn.ReplicatedCommand,
	batches []replication.RelationMutationBatch,
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
	fingerprint := sha256.Sum256(transaction)
	command := replication.Command{
		Kind:      replication.CommandTransaction,
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		Distribution:          binding.Distribution, Shard: binding.Shard,
		AllocationGeneration: binding.AllocationGeneration,
		ShardIncarnation:     binding.ShardIncarnation, GroupID: binding.GroupID,
		ReplicaSetVersion: 1, ActivePolicyGeneration: binding.ActivePolicyGeneration,
		ProtectionEpoch: binding.ProtectionEpoch, OwnershipEpoch: binding.OwnershipEpoch,
		SchemaGeneration: binding.SchemaGeneration, RoutingVersion: binding.RoutingVersion,
		RouteGeneration: binding.RouteGeneration,
		Tenant:          []byte("tenant"), ClientID: replication.ID128(control.ID),
		ClientEpoch: uint64(control.Role), ClientSequence: sequence,
		Fingerprint: fingerprint, Transaction: transaction, Batches: batches,
	}
	return encodeCommand(t, command)
}

func putTransactionCompletionControl(
	t testing.TB,
	fixture machineFixture,
	control TransactionControl,
) {
	t.Helper()
	key, err := TransactionControlStorageKey(control.Role, control.ID)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := AppendTransactionControl(nil, control)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.system.Collection.Put(key[:], raw); err != nil {
		t.Fatal(err)
	}
}

func openTransactionCompletion(
	t testing.TB,
	machine *Machine,
	command []byte,
) (replication.CompletionView, TransactionCompletionResult) {
	t.Helper()
	lookup, err := machine.LookupCompletion(command)
	if err != nil {
		t.Fatal(err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	result, err := OpenTransactionCompletionResult(completion.ResultCode, completion.InlineResult)
	if err != nil {
		t.Fatal(err)
	}
	if completion.ResultFormat != ResultFormatTransaction ||
		lookup.AppliedSequence != completion.AppliedSequence {
		t.Fatalf("completion=%+v lookup=%+v", completion, lookup)
	}
	return completion, result
}

func transactionCompletionParticipantControl(
	t testing.TB,
	id distributedtxn.ID,
	state distributedtxn.ParticipantState,
	revision uint64,
	affected bool,
) TransactionControl {
	t.Helper()
	control := transactionCodecControl(t)
	control.ID = id
	control.State = uint8(state)
	control.Revision = revision
	control.AffectedRowsValid = affected
	if affected {
		control.AffectedRows = 7
	}
	control.LastResultCode = ResultApplied
	control.LastAppliedIndex = 50
	if state == distributedtxn.ParticipantReleased {
		control.ResidentMutationBytes = 0
		control.ResidentIntentBytes = 0
		control.LastOperation = distributedtxn.ReplicatedReleaseParticipant
		control.LastExpectedRevision = revision - 1
	} else if state == distributedtxn.ParticipantApplied {
		control.LastOperation = distributedtxn.ReplicatedApplyParticipant
		control.LastExpectedRevision = 2
	} else if state == distributedtxn.ParticipantAborted {
		control.LastOperation = distributedtxn.ReplicatedAbortParticipant
		control.LastExpectedRevision = revision - 1
	}
	return control
}

func TestTransactionCompletionExactHistoricalConflictAndUnknown(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	id := transactionCodecID(140)
	apply := transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleParticipant,
		Operation: distributedtxn.ReplicatedApplyParticipant, ID: id,
		ExpectedRevision: 2, PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}, nil)
	control := transactionCompletionParticipantControl(
		t, id, distributedtxn.ParticipantApplied, 3, true,
	)
	view, err := replication.OpenCommand(apply)
	if err != nil {
		t.Fatal(err)
	}
	control.LastCommandDigest = LogicalCommandDigest(view)
	putTransactionCompletionControl(t, fixture, control)

	completion, result := openTransactionCompletion(t, fixture.machine, apply)
	if completion.ResultCode != ResultApplied || !result.AffectedRowsValid ||
		result.AffectedRows != 7 || result.Revision != 3 ||
		result.Operation != distributedtxn.ReplicatedApplyParticipant {
		t.Fatalf("exact completion=%+v result=%+v", completion, result)
	}
	prepare := transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleParticipant,
		Operation: distributedtxn.ReplicatedPrepareParticipant, ID: id,
		ExpectedRevision: 1, PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}, nil)
	completion, result = openTransactionCompletion(t, fixture.machine, prepare)
	if completion.ResultCode != ResultApplied || result.AffectedRowsValid ||
		completion.AppliedSequence != control.LastAppliedIndex {
		t.Fatalf("historical completion=%+v result=%+v", completion, result)
	}
	abort := transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleParticipant,
		Operation: distributedtxn.ReplicatedAbortParticipant, ID: id,
		ExpectedRevision: 2, PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}, nil)
	completion, result = openTransactionCompletion(t, fixture.machine, abort)
	if completion.ResultCode != ResultTransactionConflict || result.AffectedRowsValid {
		t.Fatalf("conflict completion=%+v result=%+v", completion, result)
	}
	release := transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleParticipant,
		Operation: distributedtxn.ReplicatedReleaseParticipant, ID: id,
		ExpectedRevision: 3, PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}, nil)
	if _, err := fixture.machine.LookupCompletion(release); !errors.Is(err, ErrCompletionNotFound) {
		t.Fatalf("future completion err=%v", err)
	}
	missing := transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleParticipant,
		Operation: distributedtxn.ReplicatedPrepareParticipant, ID: transactionCodecID(180),
		ExpectedRevision: 1, PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}, nil)
	if _, err := fixture.machine.LookupCompletion(missing); !errors.Is(err, ErrCompletionNotFound) {
		t.Fatalf("missing completion err=%v", err)
	}

	staleBinding := fixture.binding
	staleBinding.RouteGeneration++
	staleMissing := transactionCompletionCommand(t, staleBinding, distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleParticipant,
		Operation: distributedtxn.ReplicatedPrepareParticipant, ID: transactionCodecID(181),
		ExpectedRevision: 1, PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}, nil)
	completion, result = openTransactionCompletion(t, fixture.machine, staleMissing)
	if completion.ResultCode != ResultStaleFence || result.RevisionValid ||
		completion.AppliedSequence != fixture.machine.Published().Applied {
		t.Fatalf("missing stale completion=%+v result=%+v", completion, result)
	}
	staleUnknown := transactionCompletionCommand(t, staleBinding, distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleParticipant,
		Operation: distributedtxn.ReplicatedReleaseParticipant, ID: id,
		ExpectedRevision: 3, PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}, nil)
	completion, result = openTransactionCompletion(t, fixture.machine, staleUnknown)
	if completion.ResultCode != ResultStaleFence || !result.RevisionValid ||
		result.Revision != control.Revision ||
		completion.AppliedSequence != fixture.machine.Published().Applied {
		t.Fatalf("known stale completion=%+v result=%+v", completion, result)
	}
	staleExact := transactionCompletionCommand(t, staleBinding, distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleParticipant,
		Operation: distributedtxn.ReplicatedPrepareParticipant, ID: id,
		ExpectedRevision: 1, PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}, nil)
	completion, _ = openTransactionCompletion(t, fixture.machine, staleExact)
	if completion.ResultCode != ResultApplied {
		t.Fatalf("historical exact overridden by stale fence: %d", completion.ResultCode)
	}
}

func TestTransactionCompletionReleasedOutcomeAndCreationRetention(t *testing.T) {
	for _, test := range []struct {
		name     string
		revision uint64
		affected bool
		exact    distributedtxn.ReplicatedCommand
		conflict distributedtxn.ReplicatedCommand
	}{
		{
			name: "applied", revision: 4, affected: true,
			exact:    distributedtxn.ReplicatedCommand{Operation: distributedtxn.ReplicatedApplyParticipant, ExpectedRevision: 2},
			conflict: distributedtxn.ReplicatedCommand{Operation: distributedtxn.ReplicatedAbortParticipant, ExpectedRevision: 2},
		},
		{
			name: "direct_abort", revision: 3,
			exact:    distributedtxn.ReplicatedCommand{Operation: distributedtxn.ReplicatedAbortParticipant, ExpectedRevision: 1},
			conflict: distributedtxn.ReplicatedCommand{Operation: distributedtxn.ReplicatedPrepareParticipant, ExpectedRevision: 1},
		},
		{
			name: "prepared_abort", revision: 4,
			exact:    distributedtxn.ReplicatedCommand{Operation: distributedtxn.ReplicatedPrepareParticipant, ExpectedRevision: 1},
			conflict: distributedtxn.ReplicatedCommand{Operation: distributedtxn.ReplicatedApplyParticipant, ExpectedRevision: 2},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newMachineFixture(t)
			if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
				t.Fatal(err)
			}
			id := transactionCodecID(byte(190 + test.revision))
			control := transactionCompletionParticipantControl(
				t, id, distributedtxn.ParticipantReleased, test.revision, test.affected,
			)
			putTransactionCompletionControl(t, fixture, control)
			for _, candidate := range []*distributedtxn.ReplicatedCommand{&test.exact, &test.conflict} {
				candidate.Role = distributedtxn.ReplicatedRoleParticipant
				candidate.ID = id
				candidate.PayloadKind = distributedtxn.ReplicatedPayloadNone
			}
			exact := transactionCompletionCommand(t, fixture.binding, test.exact, nil)
			completion, _ := openTransactionCompletion(t, fixture.machine, exact)
			if completion.ResultCode != ResultApplied {
				t.Fatalf("exact result=%d", completion.ResultCode)
			}
			conflict := transactionCompletionCommand(t, fixture.binding, test.conflict, nil)
			completion, _ = openTransactionCompletion(t, fixture.machine, conflict)
			if completion.ResultCode != ResultTransactionConflict {
				t.Fatalf("conflict result=%d", completion.ResultCode)
			}
		})
	}
}

func TestTransactionCompletionParticipantStageSurvivesRelease(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	id := transactionCodecID(205)
	batches := []replication.RelationMutationBatch{{
		Relation: 1,
		Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: []byte("stage-key"), Value: []byte("stage-value"),
		}},
	}}
	mutationDigest, err := replication.TransactionMutationDigest(batches)
	if err != nil {
		t.Fatal(err)
	}
	control := transactionCompletionParticipantControl(
		t, id, distributedtxn.ParticipantReleased, 4, true,
	)
	stage := distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleParticipant,
		Operation: distributedtxn.ReplicatedStageParticipant, ID: id,
		PayloadKind: distributedtxn.ReplicatedPayloadParticipantStage,
		Participant: distributedtxn.ParticipantStage{
			CoordinatorGroup:            distributedtxn.ID(control.CoordinatorGroup),
			CoordinatorShardIncarnation: distributedtxn.ID(control.CoordinatorShardIncarnation),
			CoordinatorAllocation:       control.CoordinatorAllocation,
			BucketBits:                  control.BucketBits,
			IntentScopes:                control.IntentScopes,
			MutationDigest:              mutationDigest,
		},
	}
	control.PayloadDigest = mutationDigest
	control.MutationDigest = mutationDigest
	control.PayloadBytes = 512
	control.PayloadCount = 1
	control.PayloadRelationCount = 1
	putTransactionCompletionControl(t, fixture, control)
	exact := transactionCompletionCommand(t, fixture.binding, stage, batches)
	completion, _ := openTransactionCompletion(t, fixture.machine, exact)
	if completion.ResultCode != ResultApplied {
		t.Fatalf("released stage result=%d", completion.ResultCode)
	}

	otherBatches := []replication.RelationMutationBatch{{
		Relation: 1,
		Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: []byte("stage-key"), Value: []byte("different"),
		}},
	}}
	otherDigest, err := replication.TransactionMutationDigest(otherBatches)
	if err != nil {
		t.Fatal(err)
	}
	stage.Participant.MutationDigest = otherDigest
	conflict := transactionCompletionCommand(t, fixture.binding, stage, otherBatches)
	completion, _ = openTransactionCompletion(t, fixture.machine, conflict)
	if completion.ResultCode != ResultTransactionConflict {
		t.Fatalf("competing stage result=%d", completion.ResultCode)
	}
}

func TestTransactionCompletionCoordinatorDecisionAndStageSurviveRetire(t *testing.T) {
	for _, decision := range []distributedtxn.CoordinatorState{
		distributedtxn.CoordinatorCommitted, distributedtxn.CoordinatorAborted,
	} {
		t.Run(string(rune('0'+decision)), func(t *testing.T) {
			fixture := newMachineFixture(t)
			if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
				t.Fatal(err)
			}
			id, payload := transactionCodecCoordinatorPayload(t)
			stageControl := distributedtxn.ReplicatedCommand{
				Role:      distributedtxn.ReplicatedRoleCoordinator,
				Operation: distributedtxn.ReplicatedStageCoordinator, ID: id,
				PayloadKind: distributedtxn.ReplicatedPayloadCoordinator, Payload: payload,
			}
			stage := transactionCompletionCommand(t, fixture.binding, stageControl, nil)
			controlBytes, _ := TransactionControlResidentBytes(0)
			control := TransactionControl{
				ID: id, Role: distributedtxn.ReplicatedRoleCoordinator,
				State: uint8(distributedtxn.CoordinatorRetired), Revision: 3,
				PayloadKind:   distributedtxn.ReplicatedPayloadCoordinator,
				PayloadDigest: distributedtxn.Digest(sha256.Sum256(payload)),
				PayloadBytes:  uint64(len(payload)), PayloadCount: 1,
				CoordinatorGroup:            transactionCodecReplicationID(33),
				CoordinatorShardIncarnation: transactionCodecReplicationID(49),
				CoordinatorAllocation:       65,
				MutationDigest:              transactionCodecDigest(66),
				CoordinatorDecision:         decision,
				ResidentControlBytes:        controlBytes,
				LastOperation:               distributedtxn.ReplicatedRetireCoordinator,
				LastExpectedRevision:        2,
				LastCommandDigest:           transactionCodecCommandDigest(99),
				LastResultCode:              ResultApplied,
				LastAppliedIndex:            70,
			}
			putTransactionCompletionControl(t, fixture, control)
			completion, _ := openTransactionCompletion(t, fixture.machine, stage)
			if completion.ResultCode != ResultApplied {
				t.Fatalf("stage result=%d", completion.ResultCode)
			}
			for _, operation := range []distributedtxn.ReplicatedOperation{
				distributedtxn.ReplicatedCommitCoordinator,
				distributedtxn.ReplicatedAbortCoordinator,
			} {
				command := transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
					Role: distributedtxn.ReplicatedRoleCoordinator, Operation: operation, ID: id,
					ExpectedRevision: 1, PayloadKind: distributedtxn.ReplicatedPayloadNone,
				}, nil)
				completion, _ = openTransactionCompletion(t, fixture.machine, command)
				want := ResultTransactionConflict
				if operation == distributedtxn.ReplicatedCommitCoordinator && decision == distributedtxn.CoordinatorCommitted ||
					operation == distributedtxn.ReplicatedAbortCoordinator && decision == distributedtxn.CoordinatorAborted {
					want = ResultApplied
				}
				if completion.ResultCode != want {
					t.Fatalf("operation=%d result=%d want=%d", operation, completion.ResultCode, want)
				}
			}
		})
	}
}

func transactionCompletionManifestPage(t testing.TB) distributedtxn.ManifestSegment {
	t.Helper()
	_, segment, _ := transactionCodecManifestSegment(t)
	raw := bytes.Clone(segment.Raw)
	binary.LittleEndian.PutUint32(raw[8:12], 1)
	binary.LittleEndian.PutUint64(raw[16:24], 1)
	binary.LittleEndian.PutUint32(
		raw[len(raw)-4:], crc32.Checksum(raw[:len(raw)-4], transactionManifestCRC),
	)
	meta, ok := openTransactionManifestSegmentMeta(raw)
	if !ok {
		t.Fatal("manifest page transform is not canonical")
	}
	return distributedtxn.ManifestSegment{
		Index: meta.Index, FirstParticipant: meta.FirstParticipant,
		ParticipantCount: meta.ParticipantCount, Digest: meta.Digest, Raw: raw,
	}
}

func TestTransactionCompletionManifestPageWitnessAndCorruption(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	id := transactionCodecID(220)
	page := transactionCompletionManifestPage(t)
	command := transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleCoordinator,
		Operation: distributedtxn.ReplicatedStageManifestSegment, ID: id,
		ExpectedRevision: 1, PayloadKind: distributedtxn.ReplicatedPayloadManifestSegment,
		Payload: page.Raw,
	}, nil)
	controlBytes, _ := TransactionControlResidentBytes(0)
	payloadBytes, _ := TransactionCoordinatorPayloadResidentBytes(128)
	manifestBytes, _ := TransactionManifestPageResidentBytes(len(page.Raw))
	control := TransactionControl{
		ID: id, Role: distributedtxn.ReplicatedRoleCoordinator,
		State: uint8(distributedtxn.CoordinatorStaging), Revision: 2,
		PayloadKind:   distributedtxn.ReplicatedPayloadManifestCoordinator,
		PayloadDigest: transactionCodecDigest(21), PayloadBytes: uint64(len(page.Raw) * 2), PayloadCount: 2,
		CoordinatorGroup:            transactionCodecReplicationID(22),
		CoordinatorShardIncarnation: transactionCodecReplicationID(38),
		CoordinatorAllocation:       54,
		MutationDigest:              transactionCodecDigest(55),
		ResidentControlBytes:        controlBytes,
		ResidentPayloadBytes:        payloadBytes,
		ResidentManifestBytes:       manifestBytes,
		LastOperation:               distributedtxn.ReplicatedStageManifestSegment,
		LastExpectedRevision:        1,
		LastCommandDigest:           transactionCodecCommandDigest(87),
		LastResultCode:              ResultApplied,
		LastAppliedIndex:            80,
		ManifestNextPage:            2,
		ManifestNextParticipant:     2,
		ManifestEncodedBytes:        uint64(len(page.Raw) * 2),
		ManifestChainDigest:         transactionCodecDigest(88),
	}
	putTransactionCompletionControl(t, fixture, control)
	pageKey, _ := TransactionManifestPageStorageKey(id, page.Index)
	pageRow, err := AppendTransactionManifestPage(nil, id, page)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.system.Collection.Put(pageKey[:], pageRow); err != nil {
		t.Fatal(err)
	}
	completion, _ := openTransactionCompletion(t, fixture.machine, command)
	if completion.ResultCode != ResultApplied {
		t.Fatalf("page result=%d", completion.ResultCode)
	}

	other := bytes.Clone(page.Raw)
	other[40]++
	binary.LittleEndian.PutUint32(
		other[len(other)-4:], crc32.Checksum(other[:len(other)-4], transactionManifestCRC),
	)
	conflict := transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleCoordinator,
		Operation: distributedtxn.ReplicatedStageManifestSegment, ID: id,
		ExpectedRevision: 1, PayloadKind: distributedtxn.ReplicatedPayloadManifestSegment,
		Payload: other,
	}, nil)
	completion, _ = openTransactionCompletion(t, fixture.machine, conflict)
	if completion.ResultCode != ResultTransactionConflict {
		t.Fatalf("changed page result=%d", completion.ResultCode)
	}

	pageRow[56] ^= 1
	if _, err := fixture.system.Collection.Put(pageKey[:], pageRow); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.LookupCompletion(command); err == nil ||
		!errors.Is(err, ErrTransactionStateCorrupt) {
		t.Fatalf("corrupt page err=%v", err)
	}
}

func TestTransactionCompletionWorkspaceIsAllocationFreeAndBounded(t *testing.T) {
	if MaxCompletionEnvelopeBytes != replication.MaxEmptyResultCompletionEnvelopeBytes+
		transactionCompletionResultBytes {
		t.Fatalf("completion envelope bound = %d, want empty bound + %d",
			MaxCompletionEnvelopeBytes, transactionCompletionResultBytes)
	}
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	id := transactionCodecID(230)
	command := transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleParticipant,
		Operation: distributedtxn.ReplicatedApplyParticipant, ID: id,
		ExpectedRevision: 2, PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}, nil)
	control := transactionCompletionParticipantControl(
		t, id, distributedtxn.ParticipantApplied, 3, true,
	)
	putTransactionCompletionControl(t, fixture, control)
	short := make([]byte, 0, replication.MaxEmptyResultCompletionEnvelopeBytes)
	if _, err := fixture.machine.LookupCompletionInto(command, short); !errors.Is(err, ErrCompletionBufferSmall) {
		t.Fatalf("short buffer err=%v", err)
	}
	var workspace CompletionLookupWorkspace
	if err := fixture.machine.BeginCompletionLookupBatch(&workspace, fixture.machine.Published()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := fixture.machine.EndCompletionLookupBatch(&workspace); err != nil {
			t.Fatal(err)
		}
	}()
	scratch := make([]byte, 0, MaxCompletionEnvelopeBytes)
	lookup, err := fixture.machine.LookupCompletionIntoWorkspace(&workspace, command, scratch[:0])
	opened, openErr := replication.OpenCompletion(lookup.Bytes)
	if err != nil || openErr != nil || len(lookup.Bytes) > MaxCompletionEnvelopeBytes ||
		len(opened.InlineResult) != transactionCompletionResultBytes {
		t.Fatalf("transaction completion envelope = %dB, bound=%d err=%v",
			len(lookup.Bytes), MaxCompletionEnvelopeBytes, errors.Join(err, openErr))
	}
	if allocations := testing.AllocsPerRun(100, func() {
		lookup, err := fixture.machine.LookupCompletionIntoWorkspace(&workspace, command, scratch[:0])
		if err != nil || len(lookup.Bytes) == 0 {
			panic(err)
		}
	}); allocations != 0 && !(raceDetectorEnabled && allocations <= 2) {
		t.Fatalf("transaction completion allocations=%v", allocations)
	}
}

func TestTransactionCompletionRetainsFailedPrepareWitness(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	id := transactionCodecID(235)
	command := transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleParticipant,
		Operation: distributedtxn.ReplicatedPrepareParticipant, ID: id,
		ExpectedRevision: 1, PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}, nil)
	view, err := replication.OpenCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	control := transactionCompletionParticipantControl(
		t, id, distributedtxn.ParticipantStaged, 1, false,
	)
	control.LastOperation = distributedtxn.ReplicatedPrepareParticipant
	control.LastExpectedRevision = 1
	control.LastCommandDigest = LogicalCommandDigest(view)
	control.LastResultCode = ResultIndexConflict
	putTransactionCompletionControl(t, fixture, control)

	completion, result := openTransactionCompletion(t, fixture.machine, command)
	if completion.ResultCode != ResultIndexConflict || result.Revision != 1 ||
		result.AffectedRowsValid ||
		result.Operation != distributedtxn.ReplicatedPrepareParticipant {
		t.Fatalf("completion=%+v result=%+v", completion, result)
	}
	control.LastCommandDigest = transactionCodecCommandDigest(241)
	putTransactionCompletionControl(t, fixture, control)
	completion, _ = openTransactionCompletion(t, fixture.machine, command)
	if completion.ResultCode != ResultTransactionConflict {
		t.Fatalf("competing prepare result=%d", completion.ResultCode)
	}
}

func TestTransactionCompletionResultRejectsDamage(t *testing.T) {
	raw := make([]byte, transactionCompletionResultBytes)
	raw[0] = byte(distributedtxn.ReplicatedRoleParticipant)
	raw[1] = byte(distributedtxn.ReplicatedApplyParticipant)
	raw[2] = transactionCompletionAffectedRows | transactionCompletionControlRevision
	binary.LittleEndian.PutUint64(raw[8:16], 3)
	binary.LittleEndian.PutUint64(raw[16:24], 7)
	if _, err := OpenTransactionCompletionResult(ResultApplied, raw); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func([]byte){
		func(candidate []byte) { candidate[3] = 1 },
		func(candidate []byte) { candidate[4] = 1 },
		func(candidate []byte) { candidate[2] = 2 },
		func(candidate []byte) { candidate[1] = byte(distributedtxn.ReplicatedPrepareParticipant) },
		func(candidate []byte) { binary.LittleEndian.PutUint64(candidate[8:16], 0) },
	} {
		candidate := bytes.Clone(raw)
		mutate(candidate)
		if _, err := OpenTransactionCompletionResult(ResultApplied, candidate); !errors.Is(err, ErrCompletionCorrupt) {
			t.Fatalf("damaged result err=%v", err)
		}
	}
	prepare := make([]byte, transactionCompletionResultBytes)
	prepare[0] = byte(distributedtxn.ReplicatedRoleParticipant)
	prepare[1] = byte(distributedtxn.ReplicatedPrepareParticipant)
	prepare[2] = transactionCompletionControlRevision
	binary.LittleEndian.PutUint64(prepare[8:16], 1)
	if _, err := OpenTransactionCompletionResult(ResultIndexConflict, prepare); err != nil {
		t.Fatal(err)
	}
	prepare[1] = byte(distributedtxn.ReplicatedAbortParticipant)
	if _, err := OpenTransactionCompletionResult(ResultIndexConflict, prepare); !errors.Is(err, ErrCompletionCorrupt) {
		t.Fatalf("non-prepare index conflict err=%v", err)
	}
}
