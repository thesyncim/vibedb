package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replication"
)

var ErrTransactionIntentActive = errors.New("replicatedstate: transaction intent is active")

type transactionRowMutation struct {
	key    []byte
	value  []byte
	delete bool
}

type transactionStateDelta struct {
	controls     int64
	active       int64
	payloadRows  int64
	intentRows   int64
	residentByte int64
}

type transactionCommandPlan struct {
	command commandPlan
	delta   transactionStateDelta
	rows    []transactionRowMutation
}

func (m *Machine) planTransactionCommand(
	command replication.CommandView,
	applied uint64,
	state State,
	systemSnapshot pointSnapshot,
	relationSnapshots relationPointSnapshots,
) (transactionCommandPlan, error) {
	plan := transactionCommandPlan{command: commandPlan{
		command: command, dataChainDigest: state.DataChainDigest,
	}}
	if command.Kind() != replication.CommandTransaction {
		return transactionCommandPlan{}, ErrStateCorrupt
	}
	if !m.mutableBindingMatchesState(command, state) {
		plan.command.resultCode = ResultStaleFence
		return plan, nil
	}
	controlView, err := distributedtxn.OpenReplicatedCommand(command.TransactionBytes())
	if err != nil {
		return transactionCommandPlan{}, ErrTransactionStateCorrupt
	}
	control := controlView.Command()
	commandDigest := LogicalCommandDigest(command)
	if control.Operation == distributedtxn.ReplicatedBeginPrepareCoordinator ||
		control.Operation == distributedtxn.ReplicatedBeginPrepareManifestCoordinator {
		return m.planCoordinatorBeginPrepare(
			plan, command, control, applied, state, commandDigest,
			systemSnapshot, relationSnapshots,
		)
	}
	storageKey, err := TransactionControlStorageKey(control.Role, control.ID)
	if err != nil {
		return transactionCommandPlan{}, err
	}
	existing, found, err := transactionControlAt(systemSnapshot, storageKey)
	if err != nil {
		return transactionCommandPlan{}, err
	}
	if found && (control.ExecutionPinDigest != existing.ExecutionPinDigest ||
		control.ControllerEpoch < existing.ControllerEpoch) {
		plan.command.resultCode = ResultTransactionConflict
		plan.command.conflict = true
		return plan, nil
	}
	if found && control.ControllerEpoch > existing.ControllerEpoch {
		existing.ControllerEpoch = control.ControllerEpoch
	}
	if found && existing.LastOperation == control.Operation &&
		existing.LastExpectedRevision == control.ExpectedRevision &&
		existing.LastCommandDigest == commandDigest {
		plan.command.resultCode = existing.LastResultCode
		plan.command.exactDuplicate = true
		return plan, nil
	}
	if found && existing.LastOperation == control.Operation &&
		existing.LastExpectedRevision == control.ExpectedRevision {
		plan.command.resultCode = ResultTransactionConflict
		plan.command.conflict = true
		return plan, nil
	}
	creation := control.Operation == distributedtxn.ReplicatedStageCoordinator ||
		control.Operation == distributedtxn.ReplicatedStageManifestCoordinator ||
		control.Operation == distributedtxn.ReplicatedStageParticipant ||
		control.Operation == distributedtxn.ReplicatedStagePrepareParticipant ||
		control.Operation == distributedtxn.ReplicatedAbortReleaseParticipant &&
			control.ExpectedRevision == 0
	if creation != !found {
		plan.command.resultCode = ResultTransactionConflict
		plan.command.conflict = true
		return plan, nil
	}
	if !creation && (existing.ID != control.ID || existing.Role != control.Role ||
		existing.Revision != control.ExpectedRevision) {
		plan.command.resultCode = ResultTransactionConflict
		plan.command.conflict = true
		return plan, nil
	}
	if !creation && operationHasExclusiveTransactionPath(control.Operation) &&
		existing.FusedPath != operationUsesFusedTransactionPath(control.Operation) {
		return transactionConflict(plan), nil
	}
	if state.TransactionControlCount >= MaxRetainedTransactions && creation {
		plan.command.refusal = ErrAdmissionBound
		return plan, nil
	}

	switch control.Operation {
	case distributedtxn.ReplicatedStageCoordinator:
		return m.planInlineCoordinatorStage(
			plan, command, control, applied, commandDigest, storageKey, 0, 0,
		)
	case distributedtxn.ReplicatedStageManifestCoordinator:
		return m.planManifestCoordinatorStage(
			plan, command, control, applied, commandDigest, storageKey, 0, 0,
		)
	case distributedtxn.ReplicatedStageManifestSegment:
		return m.planManifestPageStage(
			plan, command, control, existing.TransactionControl, applied, commandDigest,
			storageKey, systemSnapshot,
		)
	case distributedtxn.ReplicatedAppendManifestSegments:
		return m.planManifestSegmentsStage(
			plan, command, control, existing.TransactionControl, applied, commandDigest,
			storageKey, systemSnapshot,
		)
	case distributedtxn.ReplicatedCommitCoordinator,
		distributedtxn.ReplicatedAbortCoordinator,
		distributedtxn.ReplicatedRetireCoordinator:
		return m.planCoordinatorTransition(
			plan, control, existing.TransactionControl, applied, commandDigest,
			storageKey, systemSnapshot,
		)
	case distributedtxn.ReplicatedPulseCoordinator:
		return m.planCoordinatorRecoveryPulse(
			plan, control, existing.TransactionControl, applied, commandDigest, storageKey,
		)
	case distributedtxn.ReplicatedStageParticipant:
		return m.planParticipantStage(
			plan, command, control, applied, commandDigest, storageKey, systemSnapshot,
		)
	case distributedtxn.ReplicatedStagePrepareParticipant:
		return m.planParticipantStagePrepared(
			plan, command, control, applied, commandDigest, storageKey,
			systemSnapshot, relationSnapshots,
		)
	case distributedtxn.ReplicatedAbortReleaseParticipant:
		if control.ExpectedRevision == 0 {
			return m.planParticipantAbortFence(
				plan, control, applied, commandDigest, storageKey,
			)
		}
		return m.planParticipantTransition(
			plan, command, control, existing.TransactionControl, applied,
			commandDigest, storageKey, systemSnapshot, relationSnapshots,
		)
	case distributedtxn.ReplicatedPrepareParticipant,
		distributedtxn.ReplicatedApplyParticipant,
		distributedtxn.ReplicatedAbortParticipant,
		distributedtxn.ReplicatedReleaseParticipant,
		distributedtxn.ReplicatedApplyReleaseParticipant:
		return m.planParticipantTransition(
			plan, command, control, existing.TransactionControl, applied,
			commandDigest, storageKey, systemSnapshot, relationSnapshots,
		)
	default:
		return transactionCommandPlan{}, ErrTransactionStateCorrupt
	}
}

func (m *Machine) planCoordinatorRecoveryPulse(
	plan transactionCommandPlan,
	control distributedtxn.ReplicatedCommand,
	existing TransactionControl,
	applied uint64,
	commandDigest replication.Digest,
	controlKey [transactionControlStorageKeyBytes]byte,
) (transactionCommandPlan, error) {
	if distributedtxn.CoordinatorState(existing.State) != distributedtxn.CoordinatorStaging ||
		existing.RecoveryPulse == math.MaxUint8 ||
		control.RecoveryPulse != existing.RecoveryPulse+1 {
		return transactionConflict(plan), nil
	}
	existing.RecoveryPulse = control.RecoveryPulse
	stampTransactionWitness(&existing, control, commandDigest, applied, ResultApplied)
	encoded, err := AppendTransactionControl(nil, existing)
	if err != nil {
		return transactionCommandPlan{}, err
	}
	plan.rows = append(plan.rows, newTransactionPut(controlKey[:], encoded))
	plan.command.resultCode = ResultApplied
	return plan, nil
}

func (m *Machine) planParticipantAbortFence(
	plan transactionCommandPlan,
	control distributedtxn.ReplicatedCommand,
	applied uint64,
	commandDigest replication.Digest,
	controlKey [transactionControlStorageKeyBytes]byte,
) (transactionCommandPlan, error) {
	controlBytes, err := TransactionControlResidentBytes(0)
	if err != nil {
		return transactionCommandPlan{}, err
	}
	durableControl := TransactionControl{
		ID: control.ID, Role: distributedtxn.ReplicatedRoleParticipant,
		State: uint8(distributedtxn.ParticipantReleased), Revision: 1,
		ControllerEpoch: control.ControllerEpoch, ExecutionPinDigest: control.ExecutionPinDigest,
		PayloadKind:      control.PayloadKind,
		PayloadDigest:    control.Participant.MutationDigest,
		CoordinatorGroup: replication.ID128(control.Participant.CoordinatorGroup),
		CoordinatorShardIncarnation: replication.ID128(
			control.Participant.CoordinatorShardIncarnation,
		),
		CoordinatorAllocation: control.Participant.CoordinatorAllocation,
		MutationDigest:        control.Participant.MutationDigest,
		ParticipantOrdinal:    control.Participant.ParticipantOrdinal,
		FusedPath:             true, CancellationWitness: true,
		ResidentControlBytes: controlBytes,
		LastOperation:        control.Operation,
		LastExpectedRevision: 0,
		LastCommandDigest:    commandDigest,
		LastResultCode:       ResultApplied,
		LastAppliedIndex:     applied,
	}
	encoded, err := AppendTransactionControl(nil, durableControl)
	if err != nil {
		return transactionCommandPlan{}, err
	}
	plan.rows = append(plan.rows, newTransactionPut(controlKey[:], encoded))
	plan.delta = transactionStateDelta{controls: 1, residentByte: int64(controlBytes)}
	plan.command.resultCode = ResultApplied
	return plan, nil
}

func transactionControlAt(
	snapshot pointSnapshot,
	key [transactionControlStorageKeyBytes]byte,
) (TransactionControlView, bool, error) {
	raw, found, err := snapshot.appendRaw(nil, key[:])
	if err != nil || !found {
		return TransactionControlView{}, found, err
	}
	view, err := OpenTransactionControl(raw)
	if err != nil {
		return TransactionControlView{}, false, err
	}
	want, err := view.StorageKey()
	if err != nil || want != key {
		return TransactionControlView{}, false, ErrTransactionStateCorrupt
	}
	return view, true, nil
}

func (m *Machine) planCoordinatorBeginPrepare(
	plan transactionCommandPlan,
	command replication.CommandView,
	control distributedtxn.ReplicatedCommand,
	applied uint64,
	state State,
	commandDigest replication.Digest,
	systemSnapshot pointSnapshot,
	relationSnapshots relationPointSnapshots,
) (transactionCommandPlan, error) {
	if replication.ID128(control.Participant.CoordinatorGroup) != command.GroupID ||
		replication.ID128(control.Participant.CoordinatorShardIncarnation) != command.ShardIncarnation ||
		control.Participant.CoordinatorAllocation != command.AllocationGeneration {
		plan.command.resultCode = ResultStaleFence
		return plan, nil
	}
	if err := distributedtxn.ValidateReplicatedCoordinatorAuthorityWitnesses(
		control.Payload,
	); err != nil {
		plan.command.refusal = ErrAdmissionBound
		return plan, nil
	}
	authorityWitness := m.transactionRouteAuthorityWitness(command)
	if authorityWitness == (distributedtxn.AuthorityWitness{}) {
		return transactionCommandPlan{}, ErrTransactionStateCorrupt
	}
	coordinatorKey, _ := TransactionControlStorageKey(
		distributedtxn.ReplicatedRoleCoordinator, control.ID,
	)
	participantKey, _ := TransactionControlStorageKey(
		distributedtxn.ReplicatedRoleParticipant, control.ID,
	)
	coordinator, coordinatorFound, err := transactionControlAt(systemSnapshot, coordinatorKey)
	if err != nil {
		return transactionCommandPlan{}, err
	}
	participant, participantFound, err := transactionControlAt(systemSnapshot, participantKey)
	if err != nil {
		return transactionCommandPlan{}, err
	}
	if coordinatorFound || participantFound {
		if coordinatorFound && participantFound &&
			coordinator.LastOperation == control.Operation &&
			coordinator.LastExpectedRevision == 0 &&
			coordinator.LastCommandDigest == commandDigest &&
			participant.LastOperation == distributedtxn.ReplicatedStagePrepareParticipant &&
			participant.LastExpectedRevision == 0 &&
			participant.LastCommandDigest == commandDigest &&
			coordinator.PrepareResultCode == participant.PrepareResultCode {
			plan.command.resultCode = coordinator.PrepareResultCode
			plan.command.exactDuplicate = true
			return plan, nil
		}
		return transactionConflict(plan), nil
	}
	if state.TransactionControlCount > MaxRetainedTransactions-2 {
		plan.command.refusal = ErrAdmissionBound
		return plan, nil
	}
	wantParticipant := distributedtxn.ParticipantRef{
		Distribution:         command.Distribution,
		Shard:                command.Shard,
		RoutingVersion:       command.RoutingVersion,
		AllocationGeneration: command.AllocationGeneration,
		OwnershipEpoch:       command.OwnershipEpoch,
		AuthorityWitness:     authorityWitness,
		MutationDigest:       control.Participant.MutationDigest,
		State:                distributedtxn.ParticipantStaged,
	}
	present, matches, err := distributedtxn.ReplicatedCoordinatorBindsParticipant(
		control.Payload, uint64(control.Participant.ParticipantOrdinal), wantParticipant,
	)
	if err != nil {
		return transactionCommandPlan{}, ErrTransactionStateCorrupt
	}
	if present && !matches {
		plan.command.refusal = ErrAdmissionBound
		return plan, nil
	}

	participantControl := distributedtxn.ReplicatedCommand{
		Role:               distributedtxn.ReplicatedRoleParticipant,
		Operation:          distributedtxn.ReplicatedStagePrepareParticipant,
		ID:                 control.ID,
		PayloadKind:        distributedtxn.ReplicatedPayloadParticipantStage,
		Participant:        control.Participant,
		ControllerEpoch:    control.ControllerEpoch,
		ExecutionPinDigest: control.ExecutionPinDigest,
	}
	participantControl.Participant.ParticipantOrdinal = 0
	participantPlan, err := m.planParticipantStagePrepared(
		transactionCommandPlan{command: plan.command}, command,
		participantControl, applied, commandDigest, participantKey,
		systemSnapshot, relationSnapshots,
	)
	if err != nil {
		return transactionCommandPlan{}, err
	}
	if participantPlan.command.resultCode == ResultTransactionConflict {
		return transactionConflict(plan), nil
	}
	if participantPlan.command.resultCode != ResultApplied &&
		participantPlan.command.resultCode != ResultIndexConflict &&
		participantPlan.command.resultCode != ResultWrongShard {
		return transactionCommandPlan{}, ErrTransactionStateCorrupt
	}
	participantOrdinal := uint64(control.Participant.ParticipantOrdinal)
	prepareResult := participantPlan.command.resultCode
	var coordinatorPlan transactionCommandPlan
	switch control.Operation {
	case distributedtxn.ReplicatedBeginPrepareCoordinator:
		coordinatorPlan, err = m.planInlineCoordinatorStage(
			plan, command, control, applied, commandDigest, coordinatorKey,
			participantOrdinal, prepareResult,
		)
	case distributedtxn.ReplicatedBeginPrepareManifestCoordinator:
		coordinatorPlan, err = m.planManifestCoordinatorStage(
			plan, command, control, applied, commandDigest, coordinatorKey,
			participantOrdinal, prepareResult,
		)
	default:
		return transactionCommandPlan{}, ErrTransactionStateCorrupt
	}
	if err != nil {
		return transactionCommandPlan{}, err
	}
	coordinatorPlan.rows = append(coordinatorPlan.rows, participantPlan.rows...)
	coordinatorPlan.delta.controls += participantPlan.delta.controls
	coordinatorPlan.delta.active += participantPlan.delta.active
	coordinatorPlan.delta.payloadRows += participantPlan.delta.payloadRows
	coordinatorPlan.delta.intentRows += participantPlan.delta.intentRows
	coordinatorPlan.delta.residentByte += participantPlan.delta.residentByte
	wantActive := int64(1)
	if participantPlan.command.resultCode == ResultApplied {
		wantActive = 2
	}
	if coordinatorPlan.delta.controls != 2 || coordinatorPlan.delta.active != wantActive {
		return transactionCommandPlan{}, ErrTransactionStateCorrupt
	}
	coordinatorPlan.command.resultCode = prepareResult
	return coordinatorPlan, nil
}

func (m *Machine) planInlineCoordinatorStage(
	plan transactionCommandPlan,
	command replication.CommandView,
	control distributedtxn.ReplicatedCommand,
	applied uint64,
	commandDigest replication.Digest,
	controlKey [transactionControlStorageKeyBytes]byte,
	participantOrdinal uint64,
	prepareResult uint32,
) (transactionCommandPlan, error) {
	var participants [distributedtxn.MaxInlineParticipants]distributedtxn.ParticipantRef
	record, err := distributedtxn.OpenCoordinatorInto(control.Payload, participants[:])
	if err != nil || record.ID != control.ID || record.Revision != 1 {
		return transactionCommandPlan{}, ErrTransactionStateCorrupt
	}
	payloadRecord, err := AppendTransactionCoordinatorPayload(
		nil, control.ID, control.PayloadKind, control.Payload,
	)
	if err != nil {
		return transactionCommandPlan{}, err
	}
	payloadKey, _ := TransactionCoordinatorPayloadStorageKey(control.ID)
	controlBytes, _ := TransactionControlResidentBytes(0)
	payloadBytes, _ := TransactionCoordinatorPayloadResidentBytes(len(control.Payload))
	payloadDigest := distributedtxn.Digest(sha256.Sum256(control.Payload))
	durableControl := TransactionControl{
		ID: control.ID, Role: control.Role, State: uint8(distributedtxn.CoordinatorStaging),
		Revision: 1, PayloadKind: control.PayloadKind,
		ControllerEpoch: control.ControllerEpoch, ExecutionPinDigest: control.ExecutionPinDigest,
		PayloadDigest: payloadDigest, PayloadBytes: uint64(len(control.Payload)),
		PayloadCount:     uint64(len(record.Participants)),
		CoordinatorGroup: command.GroupID, CoordinatorShardIncarnation: command.ShardIncarnation,
		CoordinatorAllocation: command.AllocationGeneration, MutationDigest: payloadDigest,
		CoordinatorParticipantOrdinal: participantOrdinal,
		PrepareResultCode:             prepareResult,
		FusedPath:                     control.Operation == distributedtxn.ReplicatedBeginPrepareCoordinator,
		ResidentControlBytes:          controlBytes, ResidentPayloadBytes: payloadBytes,
		LastOperation: control.Operation, LastExpectedRevision: 0,
		LastCommandDigest: commandDigest, LastResultCode: transactionPrepareResultOrApplied(prepareResult),
		LastAppliedIndex: applied,
	}
	encodedControl, err := AppendTransactionControl(nil, durableControl)
	if err != nil {
		return transactionCommandPlan{}, err
	}
	plan.rows = append(plan.rows,
		newTransactionPut(controlKey[:], encodedControl),
		newTransactionPut(payloadKey[:], payloadRecord),
	)
	plan.delta = transactionStateDelta{controls: 1, active: 1, payloadRows: 1,
		residentByte: int64(controlBytes + payloadBytes)}
	plan.command.resultCode = ResultApplied
	return plan, nil
}

func (m *Machine) planManifestCoordinatorStage(
	plan transactionCommandPlan,
	command replication.CommandView,
	control distributedtxn.ReplicatedCommand,
	applied uint64,
	commandDigest replication.Digest,
	controlKey [transactionControlStorageKeyBytes]byte,
	participantOrdinal uint64,
	prepareResult uint32,
) (transactionCommandPlan, error) {
	coordinatorRaw, pages, err := distributedtxn.OpenReplicatedManifestStart(control.Payload)
	if err != nil {
		return transactionCommandPlan{}, ErrTransactionStateCorrupt
	}
	record, err := distributedtxn.OpenManifestCoordinator(coordinatorRaw)
	if err != nil || record.ID != control.ID || record.Revision != 1 {
		return transactionCommandPlan{}, ErrTransactionStateCorrupt
	}
	payloadRecord, err := AppendTransactionCoordinatorPayload(
		nil, control.ID, control.PayloadKind, coordinatorRaw,
	)
	if err != nil {
		return transactionCommandPlan{}, err
	}
	payloadKey, _ := TransactionCoordinatorPayloadStorageKey(control.ID)
	controlBytes, _ := TransactionControlResidentBytes(0)
	payloadBytes, _ := TransactionCoordinatorPayloadResidentBytes(len(coordinatorRaw))
	manifestBytes := uint64(0)
	manifestRows := int64(0)
	manifestChain := distributedtxn.Digest{}
	iterator := pages.Iterator()
	var firstPage []byte
	for iterator.Next() {
		segment := iterator.Segment()
		if firstPage == nil {
			firstPage = segment.Raw
		}
		pageRecord, appendErr := AppendTransactionManifestPage(nil, control.ID, segment)
		if appendErr != nil {
			return transactionCommandPlan{}, appendErr
		}
		pageKey, _ := TransactionManifestPageStorageKey(control.ID, segment.Index)
		plan.rows = append(plan.rows, newTransactionPut(pageKey[:], pageRecord))
		pageBytes, _ := TransactionManifestPageResidentBytes(len(segment.Raw))
		manifestBytes += pageBytes
		manifestRows++
		manifestChain = advanceTransactionManifestChain(
			manifestChain, segment.Index, segment.Digest,
		)
	}
	if manifestRows == 0 || firstPage == nil {
		return transactionCommandPlan{}, ErrTransactionStateCorrupt
	}
	payloadDigest := distributedtxn.Digest(sha256.Sum256(control.Payload))
	durableControl := TransactionControl{
		ID: control.ID, Role: control.Role, State: uint8(distributedtxn.CoordinatorStaging),
		Revision: uint64(pages.Count()), PayloadKind: control.PayloadKind,
		ControllerEpoch: control.ControllerEpoch, ExecutionPinDigest: control.ExecutionPinDigest,
		PayloadDigest: payloadDigest, PayloadBytes: record.Manifest.EncodedBytes,
		PayloadCount:     record.Manifest.ParticipantCount,
		CoordinatorGroup: command.GroupID, CoordinatorShardIncarnation: command.ShardIncarnation,
		CoordinatorAllocation:         command.AllocationGeneration,
		MutationDigest:                transactionManifestStartDigest(coordinatorRaw, firstPage),
		CoordinatorParticipantOrdinal: participantOrdinal,
		PrepareResultCode:             prepareResult,
		FusedPath:                     control.Operation == distributedtxn.ReplicatedBeginPrepareManifestCoordinator,
		ResidentControlBytes:          controlBytes, ResidentPayloadBytes: payloadBytes,
		ResidentManifestBytes: manifestBytes,
		LastOperation:         control.Operation, LastExpectedRevision: 0,
		LastCommandDigest: commandDigest, LastResultCode: transactionPrepareResultOrApplied(prepareResult),
		LastAppliedIndex:        applied,
		ManifestNextPage:        uint32(pages.Count()),
		ManifestNextParticipant: pages.ParticipantCount(),
		ManifestEncodedBytes:    pages.EncodedBytes(),
		ManifestChainDigest:     manifestChain,
	}
	encodedControl, err := AppendTransactionControl(nil, durableControl)
	if err != nil {
		return transactionCommandPlan{}, err
	}
	plan.rows = append(plan.rows,
		newTransactionPut(controlKey[:], encodedControl),
		newTransactionPut(payloadKey[:], payloadRecord),
	)
	plan.delta = transactionStateDelta{controls: 1, active: 1, payloadRows: manifestRows + 1,
		residentByte: int64(controlBytes + payloadBytes + manifestBytes)}
	plan.command.resultCode = ResultApplied
	return plan, nil
}

func (m *Machine) planManifestPageStage(
	plan transactionCommandPlan,
	command replication.CommandView,
	control distributedtxn.ReplicatedCommand,
	existing TransactionControl,
	applied uint64,
	commandDigest replication.Digest,
	controlKey [transactionControlStorageKeyBytes]byte,
	snapshot pointSnapshot,
) (transactionCommandPlan, error) {
	if existing.Role != distributedtxn.ReplicatedRoleCoordinator ||
		existing.PayloadKind != distributedtxn.ReplicatedPayloadManifestCoordinator ||
		distributedtxn.CoordinatorState(existing.State) != distributedtxn.CoordinatorStaging ||
		existing.FusedPath {
		return transactionConflict(plan), nil
	}
	meta, ok := openTransactionManifestSegmentMeta(control.Payload)
	if !ok || meta.Index != existing.ManifestNextPage ||
		meta.FirstParticipant != existing.ManifestNextParticipant ||
		existing.ManifestNextPage == math.MaxUint32 ||
		uint64(meta.ParticipantCount) > existing.PayloadCount-existing.ManifestNextParticipant ||
		uint64(len(control.Payload)) > existing.PayloadBytes-existing.ManifestEncodedBytes {
		return transactionConflict(plan), nil
	}
	sequence, sequenceErr := distributedtxn.OpenManifestSegmentSequence(control.Payload)
	follows, followErr := transactionManifestSequenceFollowsRetained(
		snapshot, control.ID, existing.ManifestNextPage, sequence,
	)
	if followErr != nil {
		return transactionCommandPlan{}, followErr
	}
	if sequenceErr != nil || !follows {
		return transactionConflict(plan), nil
	}
	if existing.PrepareResultCode != 0 &&
		existing.CoordinatorParticipantOrdinal >= meta.FirstParticipant &&
		existing.CoordinatorParticipantOrdinal < meta.FirstParticipant+uint64(meta.ParticipantCount) {
		participantKey, _ := TransactionControlStorageKey(
			distributedtxn.ReplicatedRoleParticipant, control.ID,
		)
		participant, found, participantErr := transactionControlAt(snapshot, participantKey)
		if participantErr != nil || !found || participant.PrepareResultCode != existing.PrepareResultCode {
			return transactionCommandPlan{}, errors.Join(participantErr, ErrTransactionStateCorrupt)
		}
		want := distributedtxn.ParticipantRef{
			Distribution: command.Distribution, Shard: command.Shard,
			RoutingVersion:       command.RoutingVersion,
			AllocationGeneration: command.AllocationGeneration,
			OwnershipEpoch:       command.OwnershipEpoch,
			AuthorityWitness:     m.transactionRouteAuthorityWitness(command),
			MutationDigest:       participant.MutationDigest,
			State:                distributedtxn.ParticipantStaged,
		}
		present, matches, matchErr := distributedtxn.ManifestSegmentMatchesParticipant(
			control.Payload, existing.CoordinatorParticipantOrdinal, want,
		)
		if matchErr != nil || !present {
			return transactionCommandPlan{}, errors.Join(matchErr, ErrTransactionStateCorrupt)
		}
		if !matches {
			plan.command.refusal = ErrAdmissionBound
			return plan, nil
		}
	}
	pageRecord, err := AppendTransactionManifestPage(nil, control.ID, distributedtxn.ManifestSegment{
		Index: meta.Index, FirstParticipant: meta.FirstParticipant,
		ParticipantCount: meta.ParticipantCount, Digest: meta.Digest, Raw: control.Payload,
	})
	if err != nil {
		return transactionCommandPlan{}, err
	}
	pageKey, _ := TransactionManifestPageStorageKey(control.ID, meta.Index)
	pageBytes, _ := TransactionManifestPageResidentBytes(len(control.Payload))
	existing.Revision++
	existing.ResidentManifestBytes += pageBytes
	existing.ManifestNextPage++
	existing.ManifestNextParticipant += uint64(meta.ParticipantCount)
	existing.ManifestEncodedBytes += uint64(len(control.Payload))
	existing.ManifestChainDigest = advanceTransactionManifestChain(
		existing.ManifestChainDigest, meta.Index, meta.Digest,
	)
	stampTransactionWitness(&existing, control, commandDigest, applied, ResultApplied)
	encodedControl, err := AppendTransactionControl(nil, existing)
	if err != nil {
		return transactionCommandPlan{}, err
	}
	plan.rows = append(plan.rows,
		newTransactionPut(controlKey[:], encodedControl),
		newTransactionPut(pageKey[:], pageRecord),
	)
	plan.delta.payloadRows = 1
	plan.delta.residentByte = int64(pageBytes)
	plan.command.resultCode = ResultApplied
	return plan, nil
}

func (m *Machine) planManifestSegmentsStage(
	plan transactionCommandPlan,
	command replication.CommandView,
	control distributedtxn.ReplicatedCommand,
	existing TransactionControl,
	applied uint64,
	commandDigest replication.Digest,
	controlKey [transactionControlStorageKeyBytes]byte,
	snapshot pointSnapshot,
) (transactionCommandPlan, error) {
	if existing.Role != distributedtxn.ReplicatedRoleCoordinator ||
		existing.PayloadKind != distributedtxn.ReplicatedPayloadManifestCoordinator ||
		distributedtxn.CoordinatorState(existing.State) != distributedtxn.CoordinatorStaging ||
		!existing.FusedPath {
		return transactionConflict(plan), nil
	}
	segments, err := distributedtxn.OpenManifestSegmentSequence(control.Payload)
	if err != nil || segments.FirstIndex() != existing.ManifestNextPage ||
		segments.FirstParticipant() != existing.ManifestNextParticipant ||
		uint64(segments.Count()) > uint64(math.MaxUint32-existing.ManifestNextPage) ||
		segments.ParticipantCount() > existing.PayloadCount-existing.ManifestNextParticipant ||
		segments.EncodedBytes() > existing.PayloadBytes-existing.ManifestEncodedBytes {
		return transactionConflict(plan), nil
	}
	if err = segments.ValidateAuthorityWitnesses(); err != nil {
		plan.command.refusal = ErrAdmissionBound
		return plan, nil
	}
	descriptor, descriptorErr := transactionManifestDescriptorAt(snapshot, control.ID)
	if descriptorErr != nil {
		return transactionCommandPlan{}, descriptorErr
	}
	remainingPages := uint32(0)
	if descriptorErr == nil && existing.ManifestNextPage <= descriptor.SegmentCount {
		remainingPages = descriptor.SegmentCount - existing.ManifestNextPage
	}
	wantPages := uint32(distributedtxn.MaxManifestSegmentsPerCommand)
	if remainingPages < wantPages {
		wantPages = remainingPages
	}
	follows, followErr := transactionManifestSequenceFollowsRetained(
		snapshot, control.ID, existing.ManifestNextPage, segments,
	)
	if followErr != nil {
		return transactionCommandPlan{}, followErr
	}
	if descriptor.ParticipantCount != existing.PayloadCount ||
		descriptor.EncodedBytes != existing.PayloadBytes || wantPages == 0 ||
		uint32(segments.Count()) != wantPages || !follows {
		return transactionConflict(plan), nil
	}
	var participant TransactionControlView
	if existing.PrepareResultCode != 0 {
		participantKey, _ := TransactionControlStorageKey(
			distributedtxn.ReplicatedRoleParticipant, control.ID,
		)
		var found bool
		participant, found, err = transactionControlAt(snapshot, participantKey)
		if err != nil || !found || participant.PrepareResultCode != existing.PrepareResultCode {
			return transactionCommandPlan{}, errors.Join(err, ErrTransactionStateCorrupt)
		}
	}
	resident := uint64(0)
	rows := int64(0)
	iterator := segments.Iterator()
	for iterator.Next() {
		segment := iterator.Segment()
		if existing.PrepareResultCode != 0 &&
			existing.CoordinatorParticipantOrdinal >= segment.FirstParticipant &&
			existing.CoordinatorParticipantOrdinal <
				segment.FirstParticipant+uint64(segment.ParticipantCount) {
			want := distributedtxn.ParticipantRef{
				Distribution: command.Distribution, Shard: command.Shard,
				RoutingVersion:       command.RoutingVersion,
				AllocationGeneration: command.AllocationGeneration,
				OwnershipEpoch:       command.OwnershipEpoch,
				AuthorityWitness:     m.transactionRouteAuthorityWitness(command),
				MutationDigest:       participant.MutationDigest,
				State:                distributedtxn.ParticipantStaged,
			}
			present, matches, matchErr := distributedtxn.ManifestSegmentMatchesParticipant(
				segment.Raw, existing.CoordinatorParticipantOrdinal, want,
			)
			if matchErr != nil || !present {
				return transactionCommandPlan{}, errors.Join(matchErr, ErrTransactionStateCorrupt)
			}
			if !matches {
				plan.command.refusal = ErrAdmissionBound
				return plan, nil
			}
		}
		pageRecord, appendErr := AppendTransactionManifestPage(nil, control.ID, segment)
		if appendErr != nil {
			return transactionCommandPlan{}, appendErr
		}
		pageKey, _ := TransactionManifestPageStorageKey(control.ID, segment.Index)
		plan.rows = append(plan.rows, newTransactionPut(pageKey[:], pageRecord))
		pageBytes, _ := TransactionManifestPageResidentBytes(len(segment.Raw))
		resident += pageBytes
		rows++
		existing.ManifestChainDigest = advanceTransactionManifestChain(
			existing.ManifestChainDigest, segment.Index, segment.Digest,
		)
	}
	existing.Revision += uint64(segments.Count())
	existing.ResidentManifestBytes += resident
	existing.ManifestNextPage += uint32(segments.Count())
	existing.ManifestNextParticipant += segments.ParticipantCount()
	existing.ManifestEncodedBytes += segments.EncodedBytes()
	stampTransactionWitness(&existing, control, commandDigest, applied, ResultApplied)
	encoded, err := AppendTransactionControl(nil, existing)
	if err != nil {
		return transactionCommandPlan{}, err
	}
	plan.rows = append(plan.rows, newTransactionPut(controlKey[:], encoded))
	plan.delta.payloadRows = rows
	plan.delta.residentByte = int64(resident)
	plan.command.resultCode = ResultApplied
	return plan, nil
}

func (m *Machine) transactionRouteAuthorityWitness(
	command replication.CommandView,
) distributedtxn.AuthorityWitness {
	membership := command.ReplicaSetVersion
	if command.AuthorityClass == replication.CommandAuthorityMembershipStableData {
		membership = 0
	}
	digest := replication.RouteAuthorityDigest(replication.RouteAuthority{
		ClusterID: command.ClusterID, ClusterIncarnation: command.ClusterIncarnation,
		TopologyRecoveryEpoch: command.TopologyRecoveryEpoch,
		ShardIncarnation:      command.ShardIncarnation, GroupID: command.GroupID,
		AllocationGeneration:   command.AllocationGeneration,
		ReplicaSetVersion:      membership,
		ActivePolicyGeneration: command.ActivePolicyGeneration,
		ProtectionEpoch:        command.ProtectionEpoch, OwnershipEpoch: command.OwnershipEpoch,
		SchemaGeneration:       command.SchemaGeneration,
		RelationManifestDigest: replication.Digest(m.manifestDigest),
		RoutingVersion:         command.RoutingVersion, RouteGeneration: command.RouteGeneration,
	})
	if command.AuthorityClass == replication.CommandAuthorityMembershipStableData {
		digest = replication.MembershipStableRouteAuthorityDigest(digest)
	}
	var witness distributedtxn.AuthorityWitness
	copy(witness[:], digest[:len(witness)])
	return witness
}

func transactionManifestDescriptorAt(
	snapshot pointSnapshot,
	id distributedtxn.ID,
) (distributedtxn.ManifestDescriptor, error) {
	payloadKey, _ := TransactionCoordinatorPayloadStorageKey(id)
	raw, found, err := snapshot.appendRaw(nil, payloadKey[:])
	if err != nil || !found {
		return distributedtxn.ManifestDescriptor{}, errors.Join(err, ErrTransactionStateCorrupt)
	}
	payload, err := OpenTransactionCoordinatorPayload(raw)
	if err != nil || payload.ID != id ||
		payload.Kind != distributedtxn.ReplicatedPayloadManifestCoordinator {
		return distributedtxn.ManifestDescriptor{}, errors.Join(err, ErrTransactionStateCorrupt)
	}
	record, err := distributedtxn.OpenManifestCoordinator(payload.Payload)
	if err != nil || record.ID != id {
		return distributedtxn.ManifestDescriptor{}, errors.Join(err, ErrTransactionStateCorrupt)
	}
	return record.Manifest, nil
}

func transactionManifestSequenceFollowsRetained(
	snapshot pointSnapshot,
	id distributedtxn.ID,
	nextPage uint32,
	segments distributedtxn.ManifestSegmentSequence,
) (bool, error) {
	if nextPage == 0 {
		return false, ErrTransactionStateCorrupt
	}
	previousKey, _ := TransactionManifestPageStorageKey(id, nextPage-1)
	raw, found, err := snapshot.appendRaw(nil, previousKey[:])
	if err != nil || !found {
		return false, errors.Join(err, ErrTransactionStateCorrupt)
	}
	storedID, meta, previous, err := openTransactionManifestPageWitness(raw)
	if err != nil || storedID != id || meta.Index != nextPage-1 {
		return false, errors.Join(err, ErrTransactionStateCorrupt)
	}
	return distributedtxn.ManifestSegmentSequenceFollows(previous, segments) == nil, nil
}

func (m *Machine) planCoordinatorTransition(
	plan transactionCommandPlan,
	control distributedtxn.ReplicatedCommand,
	existing TransactionControl,
	applied uint64,
	commandDigest replication.Digest,
	controlKey [transactionControlStorageKeyBytes]byte,
	snapshot pointSnapshot,
) (transactionCommandPlan, error) {
	state := distributedtxn.CoordinatorState(existing.State)
	var next distributedtxn.CoordinatorState
	switch control.Operation {
	case distributedtxn.ReplicatedCommitCoordinator:
		next = distributedtxn.CoordinatorCommitted
	case distributedtxn.ReplicatedAbortCoordinator:
		next = distributedtxn.CoordinatorAborted
	case distributedtxn.ReplicatedRetireCoordinator:
		next = distributedtxn.CoordinatorRetired
	}
	if !state.CanTransitionTo(next) || state == next {
		return transactionConflict(plan), nil
	}
	if next == distributedtxn.CoordinatorCommitted &&
		(existing.PrepareResultCode == ResultIndexConflict ||
			existing.PrepareResultCode == ResultWrongShard) {
		return transactionConflict(plan), nil
	}
	if next == distributedtxn.CoordinatorCommitted &&
		existing.PayloadKind == distributedtxn.ReplicatedPayloadManifestCoordinator {
		payloadKey, _ := TransactionCoordinatorPayloadStorageKey(control.ID)
		raw, found, err := snapshot.appendRaw(nil, payloadKey[:])
		if err != nil || !found {
			return transactionCommandPlan{}, errors.Join(err, ErrTransactionStateCorrupt)
		}
		payload, err := OpenTransactionCoordinatorPayload(raw)
		if err != nil || payload.ID != control.ID {
			return transactionCommandPlan{}, errors.Join(err, ErrTransactionStateCorrupt)
		}
		pageKey, _ := TransactionManifestPageStorageKey(control.ID, 0)
		pageRaw, pageFound, pageErr := snapshot.appendRaw(nil, pageKey[:])
		if pageErr != nil || !pageFound {
			return transactionCommandPlan{}, errors.Join(pageErr, ErrTransactionStateCorrupt)
		}
		pageID, pageMeta, nestedPage, pageErr := openTransactionManifestPageWitness(pageRaw)
		if pageErr != nil || pageID != control.ID || pageMeta.Index != 0 ||
			transactionManifestStartDigest(payload.Payload, nestedPage) != existing.MutationDigest {
			return transactionCommandPlan{}, errors.Join(pageErr, ErrTransactionStateCorrupt)
		}
		descriptor, err := distributedtxn.OpenManifestCoordinator(payload.Payload)
		if err != nil || existing.ManifestNextPage != descriptor.Manifest.SegmentCount ||
			existing.ManifestNextParticipant != descriptor.Manifest.ParticipantCount ||
			existing.ManifestEncodedBytes != descriptor.Manifest.EncodedBytes ||
			finishTransactionManifestRoot(
				existing.ManifestChainDigest, existing.ManifestNextParticipant,
				existing.ManifestEncodedBytes, existing.ManifestNextPage,
			) != descriptor.Manifest.Root {
			return transactionConflict(plan), nil
		}
	}
	existing.State = uint8(next)
	existing.Revision++
	if next == distributedtxn.CoordinatorCommitted || next == distributedtxn.CoordinatorAborted {
		existing.CoordinatorDecision = next
	}
	if next == distributedtxn.CoordinatorRetired {
		summary, err := distributedtxn.OpenReplicatedRetirementSummary(control.Payload)
		if err != nil {
			return transactionCommandPlan{}, errors.Join(err, ErrTransactionStateCorrupt)
		}
		committed := state == distributedtxn.CoordinatorCommitted
		if summary.AffectedRowsValid != committed ||
			!committed && summary.AffectedRows != 0 {
			return transactionConflict(plan), nil
		}
		existing.AffectedRows = summary.AffectedRows
		existing.AffectedRowsValid = summary.AffectedRowsValid
		payloadKey, _ := TransactionCoordinatorPayloadStorageKey(control.ID)
		plan.rows = append(
			plan.rows, newTransactionDelete(payloadKey[:]),
		)
		rows := int64(1)
		for page := uint32(0); page < existing.ManifestNextPage; page++ {
			pageKey, _ := TransactionManifestPageStorageKey(control.ID, page)
			plan.rows = append(
				plan.rows, newTransactionDelete(pageKey[:]),
			)
			rows++
		}
		removed := existing.ResidentPayloadBytes + existing.ResidentManifestBytes
		existing.ResidentPayloadBytes, existing.ResidentManifestBytes = 0, 0
		plan.delta.active = -1
		plan.delta.payloadRows = -rows
		plan.delta.residentByte = -int64(removed)
	}
	stampTransactionWitness(&existing, control, commandDigest, applied, ResultApplied)
	encoded, err := AppendTransactionControl(nil, existing)
	if err != nil {
		return transactionCommandPlan{}, err
	}
	plan.rows = append(
		plan.rows, newTransactionPut(controlKey[:], encoded),
	)
	plan.command.resultCode = ResultApplied
	return plan, nil
}

func (m *Machine) planParticipantStage(
	plan transactionCommandPlan,
	command replication.CommandView,
	control distributedtxn.ReplicatedCommand,
	applied uint64,
	commandDigest replication.Digest,
	controlKey [transactionControlStorageKeyBytes]byte,
	snapshot pointSnapshot,
) (transactionCommandPlan, error) {
	return m.planParticipantStageWithVote(
		plan, command, control, applied, commandDigest, controlKey,
		snapshot, relationPointSnapshots{}, false,
	)
}

func (m *Machine) planParticipantStagePrepared(
	plan transactionCommandPlan,
	command replication.CommandView,
	control distributedtxn.ReplicatedCommand,
	applied uint64,
	commandDigest replication.Digest,
	controlKey [transactionControlStorageKeyBytes]byte,
	snapshot pointSnapshot,
	relationSnapshots relationPointSnapshots,
) (transactionCommandPlan, error) {
	return m.planParticipantStageWithVote(
		plan, command, control, applied, commandDigest, controlKey,
		snapshot, relationSnapshots, true,
	)
}

func (m *Machine) planParticipantStageWithVote(
	plan transactionCommandPlan,
	command replication.CommandView,
	control distributedtxn.ReplicatedCommand,
	applied uint64,
	commandDigest replication.Digest,
	controlKey [transactionControlStorageKeyBytes]byte,
	snapshot pointSnapshot,
	relationSnapshots relationPointSnapshots,
	prepare bool,
) (transactionCommandPlan, error) {
	controlBytes, _ := TransactionControlResidentBytes(len(control.Participant.IntentScopes))
	residentMutation, residentIntent := uint64(0), uint64(0)
	relationRows, intentRows := int64(0), int64(0)
	var payloadArena [replication.MaxRelationsPerBundle]TransactionRelationPayloadView
	var payloads []TransactionRelationPayloadView
	if prepare {
		payloads = payloadArena[:0]
	}
	seenIntents := make(map[[transactionIntentKeyBytes]byte][]byte, command.MutationCount())
	relations := command.RelationBatches()
	for relations.Next() {
		batch := relations.Batch()
		relationRow, err := AppendTransactionRelationPayload(nil, control.ID, batch)
		if err != nil {
			return transactionCommandPlan{}, err
		}
		relationKey, _ := TransactionRelationPayloadStorageKey(control.ID, batch.Relation)
		plan.rows = append(
			plan.rows, newTransactionPut(relationKey[:], relationRow),
		)
		if prepare {
			payload, openErr := OpenTransactionRelationPayload(relationRow)
			if openErr != nil {
				return transactionCommandPlan{}, openErr
			}
			payloads = append(payloads, payload)
		}
		relationResident, _ := TransactionRelationPayloadResidentBytes(len(batch.MutationBytes()))
		if residentMutation > MaxTransactionResidentBytes-relationResident {
			return transactionCommandPlan{}, ErrAdmissionBound
		}
		residentMutation += relationResident
		relationRows++
		mutations := batch.Mutations()
		for mutations.Next() {
			view := mutations.Mutation()
			intentKey, _ := TransactionIntentStorageKey(batch.Relation, view.Key)
			if prior, ok := seenIntents[intentKey]; ok {
				if !bytes.Equal(prior, view.Key) {
					return transactionCommandPlan{}, ErrTransactionStateCorrupt
				}
				continue
			}
			if raw, found, err := snapshot.appendRaw(nil, intentKey[:]); err != nil {
				return transactionCommandPlan{}, err
			} else if found {
				intent, openErr := OpenTransactionIntentForKey(raw, batch.Relation, view.Key)
				if openErr != nil {
					return transactionCommandPlan{}, openErr
				}
				if intent.ID != control.ID {
					plan.rows = plan.rows[:0]
					return transactionConflict(plan), nil
				}
				return transactionCommandPlan{}, ErrTransactionStateCorrupt
			}
			seenIntents[intentKey] = bytes.Clone(view.Key)
			intentRow, err := AppendTransactionIntent(nil, control.ID, batch.Relation, view.Key)
			if err != nil {
				return transactionCommandPlan{}, err
			}
			plan.rows = append(
				plan.rows, newTransactionPut(intentKey[:], intentRow),
			)
			intentBytes, _ := TransactionIntentResidentBytes(len(view.Key))
			if residentIntent > MaxTransactionResidentBytes-intentBytes {
				return transactionCommandPlan{}, ErrAdmissionBound
			}
			residentIntent += intentBytes
			intentRows++
		}
	}
	if uint64(controlBytes)+residentMutation+residentIntent > MaxTransactionResidentBytes {
		return transactionCommandPlan{}, ErrAdmissionBound
	}
	resultCode := uint32(ResultApplied)
	state, revision := distributedtxn.ParticipantStaged, uint64(1)
	var finishPlan commandPlan
	if prepare {
		changes, spans, digest, _, code, err := m.planStoredTransactionMutations(
			command, payloads, plan.command.dataChainDigest, relationSnapshots,
		)
		if err != nil {
			return transactionCommandPlan{}, err
		}
		if code != ResultApplied && code != ResultIndexConflict && code != ResultWrongShard {
			return transactionCommandPlan{}, fmt.Errorf(
				"%w: fused participant prepare result %d", ErrTransactionStateCorrupt, code,
			)
		}
		resultCode = code
		if code == ResultApplied {
			state, revision = distributedtxn.ParticipantPrepared, 2
			finishPlan = commandPlan{changes: changes, relations: spans, dataChainDigest: digest}
		} else {
			// A rejected vote retains only the compact control witness. No intent
			// or mutation row becomes durable, so cleanup needs no proposal.
			state, revision = distributedtxn.ParticipantReleased, 3
			plan.rows = plan.rows[:0]
			residentMutation, residentIntent = 0, 0
			relationRows, intentRows = 0, 0
		}
	}
	durableControl := TransactionControl{
		ID: control.ID, Role: control.Role, State: uint8(state),
		Revision: revision, PayloadKind: control.PayloadKind,
		ControllerEpoch: control.ControllerEpoch, ExecutionPinDigest: control.ExecutionPinDigest,
		PayloadDigest: control.Participant.MutationDigest,
		PayloadBytes:  transactionCanonicalRelationBytes(command), PayloadCount: uint64(command.MutationCount()),
		PayloadRelationCount:        uint16(command.RelationCount()),
		CoordinatorGroup:            replication.ID128(control.Participant.CoordinatorGroup),
		CoordinatorShardIncarnation: replication.ID128(control.Participant.CoordinatorShardIncarnation),
		CoordinatorAllocation:       control.Participant.CoordinatorAllocation,
		MutationDigest:              control.Participant.MutationDigest,
		BucketBits:                  control.Participant.BucketBits, IntentScopes: control.Participant.IntentScopes,
		ResidentControlBytes: controlBytes, ResidentMutationBytes: residentMutation,
		ResidentIntentBytes: residentIntent,
		PrepareResultCode: func() uint32 {
			if prepare {
				return resultCode
			}
			return 0
		}(),
		PrepareCommandDigest: func() replication.Digest {
			if prepare {
				return commandDigest
			}
			return replication.Digest{}
		}(),
		FusedPath:     prepare,
		LastOperation: control.Operation, LastExpectedRevision: 0,
		LastCommandDigest: commandDigest, LastResultCode: resultCode,
		LastAppliedIndex: applied,
	}
	encoded, err := AppendTransactionControl(nil, durableControl)
	if err != nil {
		return transactionCommandPlan{}, err
	}
	plan.rows = append(
		plan.rows, newTransactionPut(controlKey[:], encoded),
	)
	active := int64(1)
	if state == distributedtxn.ParticipantReleased {
		active = 0
	}
	plan.delta = transactionStateDelta{
		controls: 1, active: active, payloadRows: relationRows, intentRows: intentRows,
		residentByte: int64(uint64(controlBytes) + residentMutation + residentIntent),
	}
	plan.command.resultCode = resultCode
	if prepare && resultCode == ResultApplied {
		// Finishing publishes the user mutations and deletes their durable
		// intents/payloads atomically. A prepare that fits by itself can still
		// exceed the frozen transaction budget at finish. Refuse before storing
		// any prepared intent, while retaining the same exact apply limits.
		finishPlan.systemRows = make([]transactionRowMutation, len(plan.rows))
		for index, row := range plan.rows[:len(plan.rows)-1] {
			finishPlan.systemRows[index] = newTransactionDelete(row.key)
		}
		// Control encoding is fixed-width apart from the unchanged scope list.
		finishPlan.systemRows[len(plan.rows)-1] = plan.rows[len(plan.rows)-1]
		next, stateErr := m.hypotheticalTransactionState(command, plan)
		if stateErr != nil {
			return transactionCommandPlan{}, stateErr
		}
		next.DataChainDigest = finishPlan.dataChainDigest
		if err := m.checkTransitionCapacity(next, finishPlan.changes, finishPlan); err != nil {
			return transactionCommandPlan{}, err
		}
	}
	return plan, nil
}

func (m *Machine) planParticipantTransition(
	plan transactionCommandPlan,
	command replication.CommandView,
	control distributedtxn.ReplicatedCommand,
	existing TransactionControl,
	applied uint64,
	commandDigest replication.Digest,
	controlKey [transactionControlStorageKeyBytes]byte,
	snapshot pointSnapshot,
	relationSnapshots relationPointSnapshots,
) (transactionCommandPlan, error) {
	state := distributedtxn.ParticipantState(existing.State)
	var next distributedtxn.ParticipantState
	applyRelease := control.Operation == distributedtxn.ReplicatedApplyReleaseParticipant
	abortRelease := control.Operation == distributedtxn.ReplicatedAbortReleaseParticipant
	switch control.Operation {
	case distributedtxn.ReplicatedPrepareParticipant:
		next = distributedtxn.ParticipantPrepared
	case distributedtxn.ReplicatedApplyParticipant:
		next = distributedtxn.ParticipantApplied
	case distributedtxn.ReplicatedAbortParticipant:
		next = distributedtxn.ParticipantAborted
	case distributedtxn.ReplicatedReleaseParticipant:
		next = distributedtxn.ParticipantReleased
	case distributedtxn.ReplicatedApplyReleaseParticipant,
		distributedtxn.ReplicatedAbortReleaseParticipant:
		next = distributedtxn.ParticipantReleased
	}
	validFusedFinish := (applyRelease || abortRelease) &&
		state == distributedtxn.ParticipantPrepared && existing.PrepareResultCode == ResultApplied
	if (!validFusedFinish && !state.CanTransitionTo(next)) || state == next {
		return transactionConflict(plan), nil
	}
	var finishBatches []TransactionRelationPayloadView
	if next == distributedtxn.ParticipantApplied || applyRelease {
		batches, err := loadTransactionRelationPayloads(
			snapshot, control.ID, existing.PayloadCount, existing.PayloadRelationCount,
		)
		if err != nil {
			return transactionCommandPlan{}, err
		}
		changes, spans, digest, affectedRows, code, err := m.planStoredTransactionMutations(
			command, batches, plan.command.dataChainDigest, relationSnapshots,
		)
		if err != nil {
			return transactionCommandPlan{}, err
		}
		if code != ResultApplied {
			return transactionCommandPlan{}, fmt.Errorf(
				"%w: prepared transaction apply result %d", ErrTransactionStateCorrupt, code,
			)
		}
		plan.command.changes, plan.command.relations = changes, spans
		plan.command.dataChainDigest = digest
		existing.AffectedRows, existing.AffectedRowsValid = affectedRows, true
		finishBatches = batches
	}
	if next == distributedtxn.ParticipantPrepared {
		batches, err := loadTransactionRelationPayloads(
			snapshot, control.ID, existing.PayloadCount, existing.PayloadRelationCount,
		)
		if err != nil {
			return transactionCommandPlan{}, err
		}
		if err := validateTransactionIntentImage(snapshot, control.ID, batches); err != nil {
			return transactionCommandPlan{}, err
		}
		_, _, _, _, code, err := m.planStoredTransactionMutations(
			command, batches, plan.command.dataChainDigest, relationSnapshots,
		)
		if err != nil {
			return transactionCommandPlan{}, err
		}
		if code != ResultApplied {
			existing.PrepareResultCode = code
			existing.PrepareCommandDigest = commandDigest
			stampTransactionWitness(&existing, control, commandDigest, applied, code)
			encoded, encodeErr := AppendTransactionControl(nil, existing)
			if encodeErr != nil {
				return transactionCommandPlan{}, encodeErr
			}
			plan.rows = append(plan.rows, newTransactionPut(controlKey[:], encoded))
			plan.command.resultCode = code
			return plan, nil
		}
		existing.PrepareResultCode = ResultApplied
		existing.PrepareCommandDigest = commandDigest
	}
	existing.State = uint8(next)
	if applyRelease || abortRelease {
		existing.Revision += 2
	} else {
		existing.Revision++
	}
	if next == distributedtxn.ParticipantReleased {
		batches := finishBatches
		if batches == nil {
			var err error
			batches, err = loadTransactionRelationPayloads(
				snapshot, control.ID, existing.PayloadCount, existing.PayloadRelationCount,
			)
			if err != nil {
				return transactionCommandPlan{}, err
			}
		}
		intentKeys := make(map[[transactionIntentKeyBytes]byte]struct{}, int(existing.PayloadCount))
		var relationRows int64
		for _, payload := range batches {
			key, _ := payload.StorageKey()
			plan.rows = append(plan.rows, newTransactionDelete(key[:]))
			relationRows++
			mutations := payload.Batch.Mutations()
			for mutations.Next() {
				intentKey, _ := TransactionIntentStorageKey(payload.Relation, mutations.Mutation().Key)
				intentKeys[intentKey] = struct{}{}
			}
		}
		for key := range intentKeys {
			plan.rows = append(plan.rows, newTransactionDelete(key[:]))
		}
		removed := existing.ResidentMutationBytes + existing.ResidentIntentBytes
		existing.ResidentMutationBytes, existing.ResidentIntentBytes = 0, 0
		plan.delta.active = -1
		plan.delta.payloadRows = -relationRows
		plan.delta.intentRows = -int64(len(intentKeys))
		plan.delta.residentByte = -int64(removed)
	}
	stampTransactionWitness(&existing, control, commandDigest, applied, ResultApplied)
	encoded, err := AppendTransactionControl(nil, existing)
	if err != nil {
		return transactionCommandPlan{}, err
	}
	plan.rows = append(
		plan.rows, newTransactionPut(controlKey[:], encoded),
	)
	plan.command.resultCode = ResultApplied
	return plan, nil
}

func loadTransactionRelationPayloads(
	snapshot pointSnapshot,
	id distributedtxn.ID,
	wantMutations uint64,
	wantRelations uint16,
) ([]TransactionRelationPayloadView, error) {
	if snapshot.value == nil || snapshot.overlay != nil || wantMutations == 0 ||
		wantMutations > replication.MaxMutations || wantRelations == 0 ||
		wantRelations > replication.MaxRelationsPerBundle {
		return nil, ErrTransactionStateCorrupt
	}
	var prefix [17]byte
	prefix[0] = transactionMutationPrefix
	copy(prefix[1:], id[:])
	rows := make([]TransactionRelationPayloadView, 0, wantRelations)
	seen := uint64(0)
	err := snapshot.value.RangePrefixRaw(prefix[:], func(key, value []byte) error {
		owned := bytes.Clone(value)
		view, err := OpenTransactionRelationPayload(owned)
		if err != nil || view.ID != id {
			return errors.Join(err, ErrTransactionStateCorrupt)
		}
		wantKey, _ := view.StorageKey()
		if !bytes.Equal(key, wantKey[:]) {
			return ErrTransactionStateCorrupt
		}
		if len(rows) != 0 && rows[len(rows)-1].Relation >= view.Relation ||
			len(rows) == int(wantRelations) {
			return ErrTransactionStateCorrupt
		}
		rows = append(rows, view)
		seen += uint64(view.Count)
		return nil
	})
	if err != nil || seen != wantMutations || len(rows) != int(wantRelations) {
		return nil, errors.Join(err, ErrTransactionStateCorrupt)
	}
	return rows, nil
}

func (m *Machine) planStoredTransactionMutations(
	command replication.CommandView,
	rows []TransactionRelationPayloadView,
	dataChain [sha256.Size]byte,
	snapshots relationPointSnapshots,
) ([]finalMutation, []plannedRelationChanges, [sha256.Size]byte, int64, uint32, error) {
	m.canonicalMutations.begin(command, rows)
	if snapshots.count != uint16(len(m.relations)) {
		return nil, nil, dataChain, 0, 0, ErrInconsistentSnapshot
	}
	clear(m.bundlePlan)
	m.bundlePlan = m.bundlePlan[:0]
	clear(m.bundleRelations)
	m.bundleRelations = m.bundleRelations[:0]
	var affectedRows int64
	for _, row := range rows {
		batch := row.Batch
		ordinal := int(batch.Relation) - 1
		if ordinal < 0 || ordinal >= len(m.relations) {
			return nil, nil, dataChain, 0, ResultUnknownRelation, nil
		}
		changes, batchAffectedRows, code, err := m.planMutations(
			&m.relations[ordinal], batch, snapshots.values[ordinal], nil, false,
		)
		if err != nil {
			return nil, nil, dataChain, 0, 0, err
		}
		if code == ResultInvalidDocument || code == ResultTargetBound {
			// The replicated transaction control format intentionally has one
			// deterministic abort vote. Invalid or relation-bounded mutation bytes
			// are caller-data conflicts, not corrupt transaction state; normalize
			// them before either fused or split prepare persists its vote.
			code = ResultIndexConflict
		}
		if code != ResultApplied {
			return nil, nil, dataChain, 0, code, nil
		}
		if m.relations[ordinal].kind == RelationJSON {
			affectedRows, err = addMutationAffectedRows(affectedRows, batchAffectedRows)
			if err != nil {
				return nil, nil, dataChain, 0, 0, err
			}
		}
		if len(changes) == 0 {
			continue
		}
		start := len(m.bundlePlan)
		m.bundlePlan = append(m.bundlePlan, changes...)
		m.bundleRelations = append(m.bundleRelations, plannedRelationChanges{
			ordinal: uint16(ordinal), start: uint32(start), end: uint32(len(m.bundlePlan)),
		})
		dataChain, err = dataChainTransitionDigest(
			m.dataChainHash, dataChain, m.relations[ordinal].contract,
			m.bundlePlan[start:], nil,
		)
		if err != nil {
			return nil, nil, dataChain, 0, 0, err
		}
	}
	return m.bundlePlan, m.bundleRelations, dataChain, affectedRows, ResultApplied, nil
}

func validateTransactionIntentImage(
	snapshot pointSnapshot,
	id distributedtxn.ID,
	rows []TransactionRelationPayloadView,
) error {
	seen := make(map[[transactionIntentKeyBytes]byte]struct{})
	for _, row := range rows {
		mutations := row.Batch.Mutations()
		for mutations.Next() {
			mutation := mutations.Mutation()
			key, err := TransactionIntentStorageKey(row.Relation, mutation.Key)
			if err != nil {
				return err
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			raw, found, err := snapshot.appendRaw(nil, key[:])
			if err != nil || !found {
				return errors.Join(err, ErrTransactionStateCorrupt)
			}
			intent, err := OpenTransactionIntentForKey(raw, row.Relation, mutation.Key)
			if err != nil || intent.ID != id {
				return errors.Join(err, ErrTransactionStateCorrupt)
			}
		}
	}
	return nil
}

func lookupTransactionIntentOwner(
	snapshot pointSnapshot,
	relation replication.RelationID,
	key []byte,
) (distributedtxn.ID, bool, error) {
	storageKey, err := TransactionIntentStorageKey(relation, key)
	if err != nil {
		return distributedtxn.ID{}, false, err
	}
	raw, found, err := snapshot.appendRaw(nil, storageKey[:])
	if err != nil || !found {
		return distributedtxn.ID{}, found, err
	}
	intent, err := OpenTransactionIntentForKey(raw, relation, key)
	if err != nil {
		return distributedtxn.ID{}, false, err
	}
	return intent.ID, true, nil
}

func transactionBatchBlocked(
	snapshot pointSnapshot,
	batch replication.RelationBatchView,
) (bool, error) {
	mutations := batch.Mutations()
	for mutations.Next() {
		_, found, err := lookupTransactionIntentOwner(snapshot, batch.Relation, mutations.Mutation().Key)
		if err != nil || found {
			return found, err
		}
	}
	return false, nil
}

func stampTransactionWitness(
	control *TransactionControl,
	command distributedtxn.ReplicatedCommand,
	digest replication.Digest,
	applied uint64,
	result uint32,
) {
	control.LastOperation = command.Operation
	control.LastExpectedRevision = command.ExpectedRevision
	control.LastCommandDigest = digest
	control.LastResultCode = result
	control.LastAppliedIndex = applied
}

func transactionManifestStartDigest(
	coordinator []byte,
	pageZero []byte,
) distributedtxn.Digest {
	h := sha256.New()
	_, _ = h.Write(coordinator)
	_, _ = h.Write(pageZero)
	var digest distributedtxn.Digest
	h.Sum(digest[:0])
	return digest
}

func transactionCanonicalRelationBytes(command replication.CommandView) uint64 {
	total := uint64(0)
	relations := command.RelationBatches()
	for relations.Next() {
		batch := relations.Batch()
		if command.RelationCount() > 1 {
			total += 8
		}
		total += uint64(len(batch.MutationBytes()))
	}
	return total
}

func transactionConflict(plan transactionCommandPlan) transactionCommandPlan {
	plan.command.resultCode = ResultTransactionConflict
	plan.command.conflict = true
	return plan
}

func transactionPrepareResultOrApplied(result uint32) uint32 {
	if result == 0 {
		return ResultApplied
	}
	return result
}

func newTransactionPut(key, value []byte) transactionRowMutation {
	return transactionRowMutation{key: bytes.Clone(key), value: value}
}

func newTransactionDelete(key []byte) transactionRowMutation {
	return transactionRowMutation{key: bytes.Clone(key), delete: true}
}

func applyTransactionStateDelta(next *State, delta transactionStateDelta) error {
	if next == nil {
		return ErrStateCorrupt
	}
	apply := func(value *uint64, change int64) bool {
		if change < 0 {
			amount := uint64(-change)
			if *value < amount {
				return false
			}
			*value -= amount
			return true
		}
		amount := uint64(change)
		if *value > math.MaxUint64-amount {
			return false
		}
		*value += amount
		return true
	}
	if !apply(&next.TransactionControlCount, delta.controls) ||
		!apply(&next.ActiveTransactionCount, delta.active) ||
		!apply(&next.TransactionPayloadRows, delta.payloadRows) ||
		!apply(&next.TransactionIntentRows, delta.intentRows) ||
		!apply(&next.TransactionResidentBytes, delta.residentByte) {
		return fmt.Errorf("%w: transaction accounting", ErrStateCorrupt)
	}
	return nil
}
