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
	storageKey, err := TransactionControlStorageKey(control.Role, control.ID)
	if err != nil {
		return transactionCommandPlan{}, err
	}
	existing, found, err := transactionControlAt(systemSnapshot, storageKey)
	if err != nil {
		return transactionCommandPlan{}, err
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
		control.Operation == distributedtxn.ReplicatedStageParticipant
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
	if state.TransactionControlCount >= MaxRetainedTransactions && creation {
		plan.command.refusal = ErrAdmissionBound
		return plan, nil
	}

	switch control.Operation {
	case distributedtxn.ReplicatedStageCoordinator:
		return m.planInlineCoordinatorStage(
			plan, command, control, applied, commandDigest, storageKey,
		)
	case distributedtxn.ReplicatedStageManifestCoordinator:
		return m.planManifestCoordinatorStage(
			plan, command, control, applied, commandDigest, storageKey,
		)
	case distributedtxn.ReplicatedStageManifestSegment:
		return m.planManifestPageStage(
			plan, control, existing.TransactionControl, applied, commandDigest,
			storageKey,
		)
	case distributedtxn.ReplicatedCommitCoordinator,
		distributedtxn.ReplicatedAbortCoordinator,
		distributedtxn.ReplicatedRetireCoordinator:
		return m.planCoordinatorTransition(
			plan, control, existing.TransactionControl, applied, commandDigest,
			storageKey, systemSnapshot,
		)
	case distributedtxn.ReplicatedStageParticipant:
		return m.planParticipantStage(
			plan, command, control, applied, commandDigest, storageKey, systemSnapshot,
		)
	case distributedtxn.ReplicatedPrepareParticipant,
		distributedtxn.ReplicatedApplyParticipant,
		distributedtxn.ReplicatedAbortParticipant,
		distributedtxn.ReplicatedReleaseParticipant:
		return m.planParticipantTransition(
			plan, command, control, existing.TransactionControl, applied,
			commandDigest, storageKey, systemSnapshot, relationSnapshots,
		)
	default:
		return transactionCommandPlan{}, ErrTransactionStateCorrupt
	}
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

func (m *Machine) planInlineCoordinatorStage(
	plan transactionCommandPlan,
	command replication.CommandView,
	control distributedtxn.ReplicatedCommand,
	applied uint64,
	commandDigest replication.Digest,
	controlKey [transactionControlStorageKeyBytes]byte,
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
		PayloadDigest: payloadDigest, PayloadBytes: uint64(len(control.Payload)),
		PayloadCount:     uint64(len(record.Participants)),
		CoordinatorGroup: command.GroupID, CoordinatorShardIncarnation: command.ShardIncarnation,
		CoordinatorAllocation: command.AllocationGeneration, MutationDigest: payloadDigest,
		ResidentControlBytes: controlBytes, ResidentPayloadBytes: payloadBytes,
		LastOperation: control.Operation, LastExpectedRevision: 0,
		LastCommandDigest: commandDigest, LastResultCode: ResultApplied,
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
) (transactionCommandPlan, error) {
	coordinatorRaw, pageRaw, err := distributedtxn.OpenReplicatedManifestStart(control.Payload)
	if err != nil {
		return transactionCommandPlan{}, ErrTransactionStateCorrupt
	}
	record, err := distributedtxn.OpenManifestCoordinator(coordinatorRaw)
	if err != nil || record.ID != control.ID || record.Revision != 1 {
		return transactionCommandPlan{}, ErrTransactionStateCorrupt
	}
	meta, ok := openTransactionManifestSegmentMeta(pageRaw)
	if !ok || meta.Index != 0 || meta.FirstParticipant != 0 {
		return transactionCommandPlan{}, ErrTransactionStateCorrupt
	}
	payloadRecord, err := AppendTransactionCoordinatorPayload(
		nil, control.ID, control.PayloadKind, coordinatorRaw,
	)
	if err != nil {
		return transactionCommandPlan{}, err
	}
	pageRecord, err := AppendTransactionManifestPage(nil, control.ID, distributedtxn.ManifestSegment{
		Index: meta.Index, FirstParticipant: meta.FirstParticipant,
		ParticipantCount: meta.ParticipantCount, Digest: meta.Digest, Raw: pageRaw,
	})
	if err != nil {
		return transactionCommandPlan{}, err
	}
	payloadKey, _ := TransactionCoordinatorPayloadStorageKey(control.ID)
	pageKey, _ := TransactionManifestPageStorageKey(control.ID, 0)
	controlBytes, _ := TransactionControlResidentBytes(0)
	payloadBytes, _ := TransactionCoordinatorPayloadResidentBytes(len(coordinatorRaw))
	pageBytes, _ := TransactionManifestPageResidentBytes(len(pageRaw))
	payloadDigest := distributedtxn.Digest(sha256.Sum256(control.Payload))
	durableControl := TransactionControl{
		ID: control.ID, Role: control.Role, State: uint8(distributedtxn.CoordinatorStaging),
		Revision: 1, PayloadKind: control.PayloadKind,
		PayloadDigest: payloadDigest, PayloadBytes: record.Manifest.EncodedBytes,
		PayloadCount:     record.Manifest.ParticipantCount,
		CoordinatorGroup: command.GroupID, CoordinatorShardIncarnation: command.ShardIncarnation,
		CoordinatorAllocation: command.AllocationGeneration, MutationDigest: payloadDigest,
		ResidentControlBytes: controlBytes, ResidentPayloadBytes: payloadBytes,
		ResidentManifestBytes: pageBytes,
		LastOperation:         control.Operation, LastExpectedRevision: 0,
		LastCommandDigest: commandDigest, LastResultCode: ResultApplied,
		LastAppliedIndex:        applied,
		ManifestNextPage:        1,
		ManifestNextParticipant: uint64(meta.ParticipantCount),
		ManifestEncodedBytes:    uint64(len(pageRaw)),
		ManifestChainDigest: advanceTransactionManifestChain(
			distributedtxn.Digest{}, 0, meta.Digest,
		),
	}
	encodedControl, err := AppendTransactionControl(nil, durableControl)
	if err != nil {
		return transactionCommandPlan{}, err
	}
	plan.rows = append(plan.rows,
		newTransactionPut(controlKey[:], encodedControl),
		newTransactionPut(payloadKey[:], payloadRecord),
		newTransactionPut(pageKey[:], pageRecord),
	)
	plan.delta = transactionStateDelta{controls: 1, active: 1, payloadRows: 2,
		residentByte: int64(controlBytes + payloadBytes + pageBytes)}
	plan.command.resultCode = ResultApplied
	return plan, nil
}

func (m *Machine) planManifestPageStage(
	plan transactionCommandPlan,
	control distributedtxn.ReplicatedCommand,
	existing TransactionControl,
	applied uint64,
	commandDigest replication.Digest,
	controlKey [transactionControlStorageKeyBytes]byte,
) (transactionCommandPlan, error) {
	if existing.Role != distributedtxn.ReplicatedRoleCoordinator ||
		existing.PayloadKind != distributedtxn.ReplicatedPayloadManifestCoordinator ||
		distributedtxn.CoordinatorState(existing.State) != distributedtxn.CoordinatorStaging {
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
			transactionManifestStartDigest(payload.Payload, nestedPage) != existing.PayloadDigest {
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
	controlBytes, _ := TransactionControlResidentBytes(len(control.Participant.IntentScopes))
	residentMutation, residentIntent := uint64(0), uint64(0)
	relationRows, intentRows := int64(0), int64(0)
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
	durableControl := TransactionControl{
		ID: control.ID, Role: control.Role, State: uint8(distributedtxn.ParticipantStaged),
		Revision: 1, PayloadKind: control.PayloadKind,
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
		LastOperation:       control.Operation, LastExpectedRevision: 0,
		LastCommandDigest: commandDigest, LastResultCode: ResultApplied,
		LastAppliedIndex: applied,
	}
	encoded, err := AppendTransactionControl(nil, durableControl)
	if err != nil {
		return transactionCommandPlan{}, err
	}
	plan.rows = append(
		plan.rows, newTransactionPut(controlKey[:], encoded),
	)
	plan.delta = transactionStateDelta{
		controls: 1, active: 1, payloadRows: relationRows, intentRows: intentRows,
		residentByte: int64(uint64(controlBytes) + residentMutation + residentIntent),
	}
	plan.command.resultCode = ResultApplied
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
	switch control.Operation {
	case distributedtxn.ReplicatedPrepareParticipant:
		next = distributedtxn.ParticipantPrepared
	case distributedtxn.ReplicatedApplyParticipant:
		next = distributedtxn.ParticipantApplied
	case distributedtxn.ReplicatedAbortParticipant:
		next = distributedtxn.ParticipantAborted
	case distributedtxn.ReplicatedReleaseParticipant:
		next = distributedtxn.ParticipantReleased
	}
	if !state.CanTransitionTo(next) || state == next {
		return transactionConflict(plan), nil
	}
	if next == distributedtxn.ParticipantApplied {
		batches, err := loadTransactionRelationPayloads(
			snapshot, control.ID, existing.PayloadCount, existing.PayloadRelationCount,
		)
		if err != nil {
			return transactionCommandPlan{}, err
		}
		changes, spans, digest, code, err := m.planStoredTransactionMutations(
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
		existing.AffectedRows, existing.AffectedRowsValid = transactionBaseAffectedRows(batches), true
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
		_, _, _, code, err := m.planStoredTransactionMutations(
			command, batches, plan.command.dataChainDigest, relationSnapshots,
		)
		if err != nil {
			return transactionCommandPlan{}, err
		}
		if code != ResultApplied {
			stampTransactionWitness(&existing, control, commandDigest, applied, code)
			encoded, encodeErr := AppendTransactionControl(nil, existing)
			if encodeErr != nil {
				return transactionCommandPlan{}, encodeErr
			}
			plan.rows = append(plan.rows, newTransactionPut(controlKey[:], encoded))
			plan.command.resultCode = code
			return plan, nil
		}
	}
	existing.State = uint8(next)
	existing.Revision++
	if next == distributedtxn.ParticipantReleased {
		batches, err := loadTransactionRelationPayloads(
			snapshot, control.ID, existing.PayloadCount, existing.PayloadRelationCount,
		)
		if err != nil {
			return transactionCommandPlan{}, err
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
) ([]finalMutation, []plannedRelationChanges, [sha256.Size]byte, uint32, error) {
	if snapshots.count != uint16(len(m.relations)) {
		return nil, nil, dataChain, 0, ErrInconsistentSnapshot
	}
	clear(m.bundlePlan)
	m.bundlePlan = m.bundlePlan[:0]
	clear(m.bundleRelations)
	m.bundleRelations = m.bundleRelations[:0]
	for _, row := range rows {
		batch := row.Batch
		ordinal := int(batch.Relation) - 1
		if ordinal < 0 || ordinal >= len(m.relations) {
			return nil, nil, dataChain, ResultUnknownRelation, nil
		}
		changes, code, err := m.planMutations(&m.relations[ordinal], batch, snapshots.values[ordinal], nil)
		if err != nil {
			return nil, nil, dataChain, 0, err
		}
		if code != ResultApplied {
			return nil, nil, dataChain, code, nil
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
			return nil, nil, dataChain, 0, err
		}
	}
	return m.bundlePlan, m.bundleRelations, dataChain, ResultApplied, nil
}

func transactionBaseAffectedRows(rows []TransactionRelationPayloadView) int64 {
	for i := range rows {
		if rows[i].Relation == 1 {
			return int64(rows[i].Count)
		}
		if rows[i].Relation > 1 {
			break
		}
	}
	return 0
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
