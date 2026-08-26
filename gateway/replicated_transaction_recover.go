package gateway

import (
	"bytes"
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibejson/x/byteview"
)

// Recover resumes one exact RF3 transaction without consulting the legacy
// transaction journal. Unknown proposals are settled byte-identically first;
// all subsequent decisions and terminal proofs are leader ReadIndex reads of
// replicated transaction controls.
func (orchestrator *ReplicatedTransactionOrchestrator) Recover(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
) (ReplicatedTransactionResult, error) {
	if orchestrator == nil || orchestrator.executor == nil || ctx == nil {
		return ReplicatedTransactionResult{}, ErrReplicatedTransaction
	}
	validationBytes := replicatedTransactionRecoveryValidationBytes
	adopted, adoptionErr := orchestrator.adoptExternalRecoveryHandle(handle)
	if adoptionErr != nil {
		return ReplicatedTransactionResult{}, adoptionErr
	}
	if !adopted {
		if err := orchestrator.activeByteBudget.acquireReserved(ctx, validationBytes); err != nil {
			return ReplicatedTransactionResult{}, err
		}
	}
	validationHeld := true
	defer func() {
		if validationHeld {
			orchestrator.activeByteBudget.release(validationBytes)
		}
	}()
	validHandle := orchestrator.validReplicatedTransactionRecoveryHandle(handle)
	if !validHandle {
		if adopted {
			rollbackExternalRecoveryAdoption(handle)
		}
		return ReplicatedTransactionResult{}, ErrReplicatedTransaction
	}
	if adopted {
		for index := range handle.Pending {
			ordinal := handle.Pending[index].Ordinal
			handle.Pending[index].Route = handle.Participants[ordinal].Route
		}
	}
	if handle.CoordinatorMinimumApplied == 0 {
		if len(handle.Pending) != 1 ||
			!pendingReplicatedTransactionOperation(handle.Pending[0],
				distributedtxn.ReplicatedBeginPrepareCoordinator,
				distributedtxn.ReplicatedBeginPrepareManifestCoordinator) {
			return ReplicatedTransactionResult{}, ErrReplicatedTransaction
		}
		// Before any durable coordinator cut, the sole safe network action is the
		// byte-identical begin whose admission outcome is unknown.
		if err := orchestrator.retryPendingMatching(ctx, handle,
			func(control distributedtxn.ReplicatedCommand) bool {
				return control.Operation == distributedtxn.ReplicatedBeginPrepareCoordinator ||
					control.Operation == distributedtxn.ReplicatedBeginPrepareManifestCoordinator
			}); err != nil {
			return ReplicatedTransactionResult{}, orchestrator.executionError(
				handle, false, err,
			)
		}
		if handle.CoordinatorMinimumApplied == 0 || len(handle.Pending) != 0 {
			return ReplicatedTransactionResult{}, orchestrator.executionError(
				handle, false, ErrReplicatedTransactionUnknown,
			)
		}
	}
	coordinator := &handle.Participants[handle.CoordinatorOrdinal]
	record, found, err := orchestrator.readCoordinator(ctx, handle, coordinator.Route)
	if err != nil || !found {
		return ReplicatedTransactionResult{}, orchestrator.executionError(
			handle, false, errors.Join(err, ErrReplicatedTransactionUnknown),
		)
	}
	if err = orchestrator.validateCoordinatorWitnesses(ctx, handle, record); err != nil {
		return ReplicatedTransactionResult{}, orchestrator.executionError(
			handle, false, err,
		)
	}
	orchestrator.activeByteBudget.release(validationBytes)
	validationHeld = false

	state := distributedtxn.CoordinatorState(record.State)
	if state == distributedtxn.CoordinatorRetired {
		// Retirement is written only after leader ReadIndex proved every exact
		// participant terminal. Participant controls are independently GC-able,
		// so a later recovery must trust that replicated terminal witness rather
		// than require already-cleaned child records to remain readable.
		handle.Phase = ReplicatedTransactionPhaseTerminal
		releaseReplicatedTransactionTerminalOwnership(handle)
		return ReplicatedTransactionResult{
			ID:           handle.ID,
			Committed:    record.CoordinatorDecision == distributedtxn.CoordinatorCommitted,
			AffectedRows: record.AffectedRows,
		}, nil
	}
	switch state {
	case distributedtxn.CoordinatorStaging:
		// This leader ReadIndex is ordered after every coordinator proposal known
		// to the quorum. A still-staging record proves no commit crossed admission,
		// so recovery chooses abort without replaying mutable-handle decision bytes.
		handle.DecisionRevision = record.Revision
		orchestrator.discardPendingExcept(handle,
			distributedtxn.ReplicatedAbortCoordinator)
		if err = orchestrator.retryPendingMatching(ctx, handle,
			func(control distributedtxn.ReplicatedCommand) bool {
				return control.Operation == distributedtxn.ReplicatedAbortCoordinator
			}); err != nil {
			return ReplicatedTransactionResult{}, orchestrator.executionError(handle, false, err)
		}
		if handle.Phase != ReplicatedTransactionPhaseAborted {
			abort := distributedtxn.ReplicatedCommand{
				Role:      distributedtxn.ReplicatedRoleCoordinator,
				Operation: distributedtxn.ReplicatedAbortCoordinator,
				ID:        handle.ID, ExpectedRevision: record.Revision,
				PayloadKind: distributedtxn.ReplicatedPayloadNone,
			}
			proposal := orchestrator.propose(
				ctx, coordinator.Route, abort, nil, handle.CoordinatorOrdinal,
				replicatedTransactionWorkerScratch{},
			)
			orchestrator.capturePending(handle, proposal)
			if proposal.err != nil || proposal.code != replicatedstate.ResultApplied {
				return ReplicatedTransactionResult{}, orchestrator.executionError(
					handle, false, errors.Join(proposal.err, ErrReplicatedTransactionUnknown),
				)
			}
			handle.CoordinatorMinimumApplied = proposal.result.Outcome.AppliedIndex
		}
		handle.Phase = ReplicatedTransactionPhaseAborted
	case distributedtxn.CoordinatorCommitted:
		handle.DecisionRevision = record.Revision - 1
		handle.Phase = ReplicatedTransactionPhaseCommitted
	case distributedtxn.CoordinatorAborted:
		handle.DecisionRevision = record.Revision - 1
		handle.Phase = ReplicatedTransactionPhaseAborted
	default:
		return ReplicatedTransactionResult{}, orchestrator.executionError(
			handle, false, ErrReplicatedTransaction,
		)
	}

	committed := record.CoordinatorDecision == distributedtxn.CoordinatorCommitted ||
		state == distributedtxn.CoordinatorCommitted
	retirementProvedThisCall := false
	if state != distributedtxn.CoordinatorRetired {
		// Release decision-incompatible commands and settle compatible exact
		// retries before starting participant recovery. Recovery walks the
		// canonical witnesses directly, so a near-limit handle needs no second
		// O(P) plan and funds forward progress from the bytes it just released.
		orchestrator.discardPendingAfterDecision(handle, committed)
		if err = orchestrator.retryPendingMatching(ctx, handle,
			func(control distributedtxn.ReplicatedCommand) bool {
				if committed {
					return control.Operation == distributedtxn.ReplicatedCommitCoordinator ||
						control.Operation == distributedtxn.ReplicatedApplyReleaseParticipant
				}
				return control.Operation == distributedtxn.ReplicatedAbortCoordinator ||
					control.Operation == distributedtxn.ReplicatedAbortReleaseParticipant
			}); err != nil {
			return ReplicatedTransactionResult{ID: handle.ID, Committed: committed, Recovery: handle},
				orchestrator.executionError(handle, committed, err)
		}
	}
	if state != distributedtxn.CoordinatorRetired {
		if committed {
			_, err = orchestrator.finish(ctx, handle, true)
		} else {
			err = orchestrator.abortFenceAndRelease(ctx, handle)
		}
		if err != nil {
			return ReplicatedTransactionResult{ID: handle.ID, Committed: committed, Recovery: handle},
				orchestrator.executionError(handle, committed, err)
		}
	}
	affected, err := orchestrator.proveTerminalParticipants(ctx, handle, committed)
	if err != nil {
		return ReplicatedTransactionResult{ID: handle.ID, Committed: committed, Recovery: handle},
			orchestrator.executionError(handle, committed, err)
	}
	if state != distributedtxn.CoordinatorRetired {
		retirementMatches := true
		if err = orchestrator.retryPendingMatchingObserved(ctx, handle,
			func(control distributedtxn.ReplicatedCommand) bool {
				if control.Operation != distributedtxn.ReplicatedRetireCoordinator {
					return false
				}
				matches := replicatedRetirementSummaryMatches(
					control.Payload, committed, affected,
				)
				retirementMatches = retirementMatches && matches
				return matches
			}, func(control distributedtxn.ReplicatedCommand) {
				if control.Operation == distributedtxn.ReplicatedRetireCoordinator &&
					replicatedRetirementSummaryMatches(control.Payload, committed, affected) {
					retirementProvedThisCall = true
				}
			}); err != nil {
			return ReplicatedTransactionResult{
				ID: handle.ID, Committed: committed, AffectedRows: affected, Recovery: handle,
			}, orchestrator.executionError(handle, committed, err)
		}
		if !retirementMatches {
			return ReplicatedTransactionResult{
				ID: handle.ID, Committed: committed, AffectedRows: affected, Recovery: handle,
			}, orchestrator.executionError(handle, committed, ErrReplicatedTransaction)
		}
		summary := distributedtxn.ReplicatedRetirementSummary{
			AffectedRows: affected, AffectedRowsValid: committed,
		}
		if !retirementProvedThisCall {
			err = orchestrator.retire(ctx, handle, coordinator.Route, summary)
		}
		if err != nil {
			return ReplicatedTransactionResult{
				ID: handle.ID, Committed: committed, AffectedRows: affected, Recovery: handle,
			}, orchestrator.executionError(handle, committed, err)
		}
	}
	result := ReplicatedTransactionResult{
		ID: handle.ID, Committed: committed, AffectedRows: affected,
	}
	releaseReplicatedTransactionTerminalOwnership(handle)
	return result, nil
}

func replicatedRetirementSummaryMatches(raw []byte, committed bool, affected int64) bool {
	summary, err := distributedtxn.OpenReplicatedRetirementSummary(raw)
	return err == nil && summary.AffectedRowsValid == committed &&
		summary.AffectedRows == affected
}

func (orchestrator *ReplicatedTransactionOrchestrator) validReplicatedTransactionRecoveryHandle(
	handle *ReplicatedTransactionRecoveryHandle,
) bool {
	if handle == nil || handle.ID.IsZero() || handle.CatalogGeneration == 0 ||
		handle.RecoveryDeadline <= 0 ||
		len(handle.Participants) < 2 ||
		uint64(len(handle.Participants)) > maxReplicatedTransactionOrdinal ||
		int(handle.CoordinatorOrdinal) >= len(handle.Participants) ||
		handle.DecisionRevision == 0 {
		return false
	}
	handleBytes, sizeErr := replicatedTransactionRecoveryHandleLogicalBytes(handle)
	if sizeErr != nil || handle.ownership == nil ||
		handle.ownership.handle == nil ||
		handle.ownership.handle.budget != &orchestrator.byteBudget ||
		handle.ownership.handle.budgetBytes != handleBytes ||
		handle.ownership.handle.spillBudget != nil ||
		handle.ownership.handle.spillBytes != 0 ||
		handle.ownership.handle.released.Load() ||
		handle.ownership.handle.bytes != handleBytes ||
		len(handle.ownership.pending) != len(handle.Pending) {
		return false
	}
	for ordinal := range handle.Participants {
		witness := &handle.Participants[ordinal]
		if int(witness.Ordinal) != ordinal || !validReplicatedRoute(witness.Route) ||
			witness.MutationDigest == (distributedtxn.Digest{}) ||
			witness.AuthorityWitness == (distributedtxn.AuthorityWitness{}) ||
			witness.AuthorityWitness != replicatedRouteAuthorityWitness(witness.Route) {
			return false
		}
	}
	var retainedPendingBytes, activePendingBytes uint64
	for index := range handle.Pending {
		pending := &handle.Pending[index]
		if int(pending.Ordinal) >= len(handle.Participants) ||
			len(pending.Command) == 0 || len(pending.Command) > replication.MaxCommandBytes ||
			cap(pending.Command) > replication.MaxCommandBytes ||
			!commandMatchesRoute(pending.Command, pending.Route) ||
			!equalReplicatedTransactionRoute(
				pending.Route, handle.Participants[pending.Ordinal].Route,
			) || pending.reservation == nil || pending.reservation.released.Load() ||
			pending.reservation.bytes != uint64(cap(pending.Command))+
				replicatedTransactionPendingLogicalBytes ||
			!orchestrator.validPendingTransactionCommand(handle, *pending) {
			return false
		}
		for prior := 0; prior < index; prior++ {
			if handle.Pending[prior].reservation == pending.reservation {
				return false
			}
		}
		registryMatches := 0
		for registryIndex := range handle.ownership.pending {
			if handle.ownership.pending[registryIndex] == pending.reservation {
				registryMatches++
			}
		}
		if registryMatches != 1 {
			return false
		}
		reservation := pending.reservation
		if reservation.budgetBytes == 0 ||
			reservation.spillBudget == nil != (reservation.spillBytes == 0) ||
			reservation.spillBudget == reservation.budget ||
			reservation.budgetBytes > math.MaxUint64-reservation.spillBytes ||
			reservation.bytes != reservation.budgetBytes+reservation.spillBytes {
			return false
		}
		for _, part := range []struct {
			budget *replicatedTransactionByteBudget
			bytes  uint64
		}{
			{reservation.budget, reservation.budgetBytes},
			{reservation.spillBudget, reservation.spillBytes},
		} {
			if part.bytes == 0 {
				continue
			}
			switch part.budget {
			case &orchestrator.byteBudget:
				if part.bytes > math.MaxUint64-retainedPendingBytes {
					return false
				}
				retainedPendingBytes += part.bytes
			case &orchestrator.activeByteBudget:
				if part.bytes > math.MaxUint64-activePendingBytes {
					return false
				}
				activePendingBytes += part.bytes
			default:
				return false
			}
		}
	}
	if retainedPendingBytes > orchestrator.byteBudget.limit ||
		handleBytes > orchestrator.byteBudget.limit-retainedPendingBytes ||
		replicatedTransactionRecoveryValidationBytes > orchestrator.activeByteBudget.limit ||
		activePendingBytes > orchestrator.activeByteBudget.limit-
			replicatedTransactionRecoveryValidationBytes {
		return false
	}
	orchestrator.byteBudget.mu.Lock()
	retainedUsed := orchestrator.byteBudget.used
	orchestrator.byteBudget.mu.Unlock()
	orchestrator.activeByteBudget.mu.Lock()
	activeUsed := orchestrator.activeByteBudget.used
	orchestrator.activeByteBudget.mu.Unlock()
	if handleBytes+retainedPendingBytes > retainedUsed || activePendingBytes+
		replicatedTransactionRecoveryValidationBytes > activeUsed {
		return false
	}
	return true
}

func (orchestrator *ReplicatedTransactionOrchestrator) validPendingTransactionCommand(
	handle *ReplicatedTransactionRecoveryHandle,
	pending ReplicatedTransactionPendingCommand,
) bool {
	command, err := replication.OpenCommand(pending.Command)
	if err != nil || command.Kind() != replication.CommandTransaction ||
		command.AuthorityClass != replication.CommandAuthorityData ||
		!bytes.Equal(command.Tenant, orchestrator.tenant) ||
		command.RetryHome != orchestrator.retryHome ||
		command.ClientID != replication.ID128(handle.ID) ||
		command.Fingerprint != nativeCommandViewFingerprint(command) {
		return false
	}
	control, err := distributedtxn.OpenReplicatedCommand(command.TransactionBytes())
	if err != nil || control.ID != handle.ID {
		return false
	}
	ordinal := int(pending.Ordinal)
	witness := handle.Participants[ordinal]
	coordinator := handle.Participants[handle.CoordinatorOrdinal].Route
	coordinatorOperation := control.Role == distributedtxn.ReplicatedRoleCoordinator
	// Coordinator-role controls are pinned to the coordinator shard. The same
	// shard also owns its fused participant, so participant-role stage/release
	// commands are valid at the coordinator ordinal too.
	if coordinatorOperation && pending.Ordinal != handle.CoordinatorOrdinal {
		return false
	}
	if control.Role == distributedtxn.ReplicatedRoleParticipant &&
		(control.Operation == distributedtxn.ReplicatedStagePrepareParticipant ||
			control.Operation == distributedtxn.ReplicatedAbortReleaseParticipant &&
				control.ExpectedRevision == 0) {
		stage := control.Participant
		if stage.ParticipantOrdinal != pending.Ordinal ||
			stage.MutationDigest != witness.MutationDigest ||
			stage.CoordinatorGroup != distributedtxn.ID(coordinator.Group.GroupID) ||
			stage.CoordinatorShardIncarnation != distributedtxn.ID(coordinator.Group.ShardIncarnation) ||
			stage.CoordinatorAllocation != coordinator.AllocationGeneration {
			return false
		}
	}
	if control.Operation == distributedtxn.ReplicatedBeginPrepareCoordinator ||
		control.Operation == distributedtxn.ReplicatedBeginPrepareManifestCoordinator {
		stage := control.Participant
		if stage.ParticipantOrdinal != handle.CoordinatorOrdinal ||
			stage.MutationDigest != handle.Participants[handle.CoordinatorOrdinal].MutationDigest ||
			stage.CoordinatorGroup != distributedtxn.ID(coordinator.Group.GroupID) ||
			stage.CoordinatorShardIncarnation != distributedtxn.ID(coordinator.Group.ShardIncarnation) ||
			stage.CoordinatorAllocation != coordinator.AllocationGeneration {
			return false
		}
		if !replicatedTransactionBeginMatchesHandle(handle, control.Command()) {
			return false
		}
	}
	switch control.Operation {
	case distributedtxn.ReplicatedBeginPrepareCoordinator,
		distributedtxn.ReplicatedBeginPrepareManifestCoordinator,
		distributedtxn.ReplicatedAppendManifestSegments,
		distributedtxn.ReplicatedStagePrepareParticipant,
		distributedtxn.ReplicatedCommitCoordinator,
		distributedtxn.ReplicatedAbortCoordinator,
		distributedtxn.ReplicatedApplyReleaseParticipant,
		distributedtxn.ReplicatedAbortReleaseParticipant,
		distributedtxn.ReplicatedRetireCoordinator:
		return true
	default:
		return false
	}
}

func replicatedTransactionBeginMatchesHandle(
	handle *ReplicatedTransactionRecoveryHandle,
	control distributedtxn.ReplicatedCommand,
) bool {
	switch control.Operation {
	case distributedtxn.ReplicatedBeginPrepareCoordinator:
		var scratch [distributedtxn.MaxInlineParticipants]distributedtxn.ParticipantRef
		record, err := distributedtxn.OpenCoordinatorInto(control.Payload, scratch[:])
		if err != nil || record.ID != handle.ID ||
			record.State != distributedtxn.CoordinatorStaging || record.Revision != 1 ||
			record.CatalogGeneration != handle.CatalogGeneration ||
			record.RecoveryDeadline != handle.RecoveryDeadline ||
			len(record.Participants) != len(handle.Participants) {
			return false
		}
		for ordinal := range record.Participants {
			if !equalReplicatedTransactionParticipantRef(
				record.Participants[ordinal],
				replicatedTransactionHandleParticipantRef(&handle.Participants[ordinal]),
			) {
				return false
			}
		}
		return true
	case distributedtxn.ReplicatedBeginPrepareManifestCoordinator:
		coordinator, initial, err := distributedtxn.OpenReplicatedManifestStart(control.Payload)
		if err != nil {
			return false
		}
		record, err := distributedtxn.OpenManifestCoordinator(coordinator)
		if err != nil || record.ID != handle.ID ||
			record.State != distributedtxn.CoordinatorStaging || record.Revision != 1 ||
			record.CatalogGeneration != handle.CatalogGeneration ||
			record.RecoveryDeadline != handle.RecoveryDeadline ||
			record.Manifest.ParticipantCount != uint64(len(handle.Participants)) {
			return false
		}
		pageScratch := make([]byte, distributedtxn.ManifestSegmentBytes)
		initialIterator := initial.Iterator()
		matchedInitial := 0
		builder, err := distributedtxn.NewManifestBuilder(pageScratch,
			func(segment distributedtxn.ManifestSegment) error {
				if matchedInitial >= initial.Count() {
					return nil
				}
				if !initialIterator.Next() ||
					!bytes.Equal(segment.Raw, initialIterator.Segment().Raw) {
					return ErrReplicatedTransaction
				}
				matchedInitial++
				return nil
			})
		if err != nil {
			return false
		}
		for ordinal := range handle.Participants {
			if err = builder.Append(replicatedTransactionHandleParticipantRef(
				&handle.Participants[ordinal],
			)); err != nil {
				return false
			}
		}
		descriptor, err := builder.Seal()
		return err == nil && descriptor == record.Manifest &&
			matchedInitial == initial.Count() && !initialIterator.Next()
	default:
		return false
	}
}

func replicatedTransactionHandleParticipantRef(
	witness *ReplicatedTransactionRouteWitness,
) distributedtxn.ParticipantRef {
	return distributedtxn.ParticipantRef{
		Distribution:         byteview.Bytes(string(witness.Route.Distribution)),
		Shard:                byteview.Bytes(string(witness.Route.Shard)),
		RoutingVersion:       witness.Route.Command.RoutingVersion,
		AllocationGeneration: witness.Route.AllocationGeneration,
		OwnershipEpoch:       witness.Route.Command.OwnershipEpoch,
		AuthorityWitness:     witness.AuthorityWitness,
		MutationDigest:       witness.MutationDigest,
		State:                distributedtxn.ParticipantStaged,
	}
}

func equalReplicatedTransactionParticipantRef(
	left, right distributedtxn.ParticipantRef,
) bool {
	return bytes.Equal(left.Distribution, right.Distribution) &&
		bytes.Equal(left.Shard, right.Shard) &&
		left.RoutingVersion == right.RoutingVersion &&
		left.AllocationGeneration == right.AllocationGeneration &&
		left.OwnershipEpoch == right.OwnershipEpoch &&
		left.AuthorityWitness == right.AuthorityWitness &&
		left.MutationDigest == right.MutationDigest && left.State == right.State
}

func pendingReplicatedTransactionOperation(
	pending ReplicatedTransactionPendingCommand,
	operations ...distributedtxn.ReplicatedOperation,
) bool {
	command, err := replication.OpenCommand(pending.Command)
	if err != nil {
		return false
	}
	control, err := distributedtxn.OpenReplicatedCommand(command.TransactionBytes())
	if err != nil {
		return false
	}
	for _, operation := range operations {
		if control.Operation == operation {
			return true
		}
	}
	return false
}

func (orchestrator *ReplicatedTransactionOrchestrator) discardPendingAfterDecision(
	handle *ReplicatedTransactionRecoveryHandle,
	committed bool,
) {
	retained := handle.Pending[:0]
	for index := range handle.Pending {
		pending := handle.Pending[index]
		keep := pendingReplicatedTransactionOperation(pending,
			distributedtxn.ReplicatedRetireCoordinator)
		if committed {
			keep = keep || pendingReplicatedTransactionOperation(pending,
				distributedtxn.ReplicatedCommitCoordinator,
				distributedtxn.ReplicatedApplyReleaseParticipant)
		} else {
			keep = keep || pendingReplicatedTransactionOperation(pending,
				distributedtxn.ReplicatedAbortCoordinator,
				distributedtxn.ReplicatedAbortReleaseParticipant)
		}
		if keep {
			retained = append(retained, pending)
		} else {
			if !handle.ownership.releasePending(pending.reservation) {
				panic("gateway: missing replicated transaction pending ownership")
			}
		}
	}
	clear(handle.Pending[len(retained):])
	handle.Pending = retained
}

func (orchestrator *ReplicatedTransactionOrchestrator) discardPendingExcept(
	handle *ReplicatedTransactionRecoveryHandle,
	operations ...distributedtxn.ReplicatedOperation,
) {
	retained := handle.Pending[:0]
	for index := range handle.Pending {
		pending := handle.Pending[index]
		if pendingReplicatedTransactionOperation(pending, operations...) {
			retained = append(retained, pending)
			continue
		}
		if !handle.ownership.releasePending(pending.reservation) {
			panic("gateway: missing replicated transaction pending ownership")
		}
	}
	clear(handle.Pending[len(retained):])
	handle.Pending = retained
}

func equalReplicatedTransactionRoute(left, right ReplicatedRoute) bool {
	if left.Distribution != right.Distribution || left.Shard != right.Shard ||
		left.Group != right.Group || left.AllocationGeneration != right.AllocationGeneration ||
		left.Command != right.Command || len(left.Replicas) != len(right.Replicas) {
		return false
	}
	for index := range left.Replicas {
		if left.Replicas[index] != right.Replicas[index] {
			return false
		}
	}
	return true
}

func (orchestrator *ReplicatedTransactionOrchestrator) readCoordinator(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
	route ReplicatedRoute,
) (replicatedstate.TransactionRecoveryRecord, bool, error) {
	result, err := orchestrator.executor.ReadTransactionRecovery(
		ctx, route, replicatedstate.TransactionRecoveryReadRequest{
			Kind: replicatedstate.TransactionRecoveryLookupCoordinator,
			ID:   handle.ID, MinimumApplied: handle.CoordinatorMinimumApplied,
			MaxRows: 1,
			MaxBytes: replicatedstate.TransactionRecoverySummaryBytes +
				distributedtxn.MaxCoordinatorRecordBytes,
		},
	)
	if err != nil || len(result.Records) == 0 {
		return replicatedstate.TransactionRecoveryRecord{}, false, err
	}
	if len(result.Records) != 1 || result.Records[0].ID != handle.ID ||
		result.Records[0].Role != distributedtxn.ReplicatedRoleCoordinator {
		return replicatedstate.TransactionRecoveryRecord{}, false, ErrReplicatedTransaction
	}
	handle.CoordinatorMinimumApplied = max(handle.CoordinatorMinimumApplied, result.Applied)
	return result.Records[0], true, nil
}

func (orchestrator *ReplicatedTransactionOrchestrator) validateCoordinatorWitnesses(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
	record replicatedstate.TransactionRecoveryRecord,
) error {
	coordinator := handle.Participants[handle.CoordinatorOrdinal]
	if record.CoordinatorGroup != replication.ID128(coordinator.Route.Group.GroupID) ||
		record.CoordinatorShardIncarnation != replication.ID128(coordinator.Route.Group.ShardIncarnation) ||
		record.CoordinatorAllocation != coordinator.Route.AllocationGeneration ||
		record.PayloadCount != uint64(len(handle.Participants)) {
		return ErrReplicatedTransaction
	}
	state := distributedtxn.CoordinatorState(record.State)
	if state == distributedtxn.CoordinatorRetired {
		if record.CoordinatorDecision == distributedtxn.CoordinatorCommitted {
			if !record.AffectedRowsValid || record.AffectedRows < 0 {
				return ErrReplicatedTransaction
			}
		} else if record.CoordinatorDecision != distributedtxn.CoordinatorAborted ||
			record.AffectedRowsValid || record.AffectedRows != 0 {
			return ErrReplicatedTransaction
		}
		return nil
	}
	if record.AffectedRowsValid || record.AffectedRows != 0 {
		return ErrReplicatedTransaction
	}
	switch record.PayloadKind {
	case distributedtxn.ReplicatedPayloadCoordinator:
		var scratch [distributedtxn.MaxInlineParticipants]distributedtxn.ParticipantRef
		coordinatorRecord, err := distributedtxn.OpenCoordinatorInto(record.Payload, scratch[:])
		if err != nil || coordinatorRecord.ID != handle.ID ||
			coordinatorRecord.CatalogGeneration != handle.CatalogGeneration ||
			coordinatorRecord.RecoveryDeadline != handle.RecoveryDeadline ||
			len(coordinatorRecord.Participants) != len(handle.Participants) {
			return errors.Join(err, ErrReplicatedTransaction)
		}
		for ordinal := range coordinatorRecord.Participants {
			if !replicatedTransactionRefMatchesWitness(
				coordinatorRecord.Participants[ordinal], handle.Participants[ordinal],
			) {
				return ErrReplicatedTransaction
			}
		}
		return nil
	case distributedtxn.ReplicatedPayloadManifestCoordinator:
		manifest, err := distributedtxn.OpenManifestCoordinator(record.Payload)
		if err != nil || manifest.ID != handle.ID ||
			manifest.CatalogGeneration != handle.CatalogGeneration ||
			manifest.RecoveryDeadline != handle.RecoveryDeadline ||
			manifest.Manifest.ParticipantCount != uint64(len(handle.Participants)) {
			return errors.Join(err, ErrReplicatedTransaction)
		}
		want, descriptorErr := replicatedTransactionWitnessManifest(handle.Participants)
		if descriptorErr != nil || manifest.Manifest != want {
			return errors.Join(descriptorErr, ErrReplicatedTransaction)
		}
		pages, pageErr := replicatedCoordinatorManifestRecoveryPages(
			state, record.Revision, manifest.Manifest.SegmentCount,
		)
		if pageErr != nil {
			return pageErr
		}
		return orchestrator.validateManifestWitnessPrefix(ctx, handle, manifest.Manifest, pages)
	default:
		return ErrReplicatedTransaction
	}
}

func replicatedCoordinatorManifestRecoveryPages(
	state distributedtxn.CoordinatorState,
	revision uint64,
	segmentCount uint32,
) (uint32, error) {
	var pages uint64
	switch state {
	case distributedtxn.CoordinatorStaging:
		pages = revision
	case distributedtxn.CoordinatorAborted:
		if revision == 0 {
			return 0, ErrReplicatedTransaction
		}
		pages = revision - 1
	case distributedtxn.CoordinatorCommitted:
		pages = uint64(segmentCount)
	default:
		return 0, ErrReplicatedTransaction
	}
	if pages > uint64(segmentCount) {
		return 0, ErrReplicatedTransaction
	}
	return uint32(pages), nil
}

func replicatedTransactionWitnessManifest(
	witnesses []ReplicatedTransactionRouteWitness,
) (distributedtxn.ManifestDescriptor, error) {
	scratch := make([]byte, distributedtxn.ManifestSegmentBytes)
	builder, err := distributedtxn.NewManifestBuilder(
		scratch, func(distributedtxn.ManifestSegment) error { return nil },
	)
	if err != nil {
		return distributedtxn.ManifestDescriptor{}, err
	}
	for index := range witnesses {
		witness := witnesses[index]
		ref := distributedtxn.ParticipantRef{
			Distribution:         byteview.Bytes(string(witness.Route.Distribution)),
			Shard:                byteview.Bytes(string(witness.Route.Shard)),
			RoutingVersion:       witness.Route.Command.RoutingVersion,
			AllocationGeneration: witness.Route.AllocationGeneration,
			OwnershipEpoch:       witness.Route.Command.OwnershipEpoch,
			AuthorityWitness:     witness.AuthorityWitness,
			MutationDigest:       witness.MutationDigest,
			State:                distributedtxn.ParticipantStaged,
		}
		if err = builder.Append(ref); err != nil {
			return distributedtxn.ManifestDescriptor{}, err
		}
	}
	return builder.Seal()
}

func (orchestrator *ReplicatedTransactionOrchestrator) validateManifestWitnessPrefix(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
	descriptor distributedtxn.ManifestDescriptor,
	pageCount uint32,
) error {
	coordinator := handle.Participants[handle.CoordinatorOrdinal]
	participantScratch := make([]distributedtxn.ParticipantRef,
		distributedtxn.MaxManifestPageParticipants)
	identityScratch := make([]byte,
		2*distributedtxn.MaxShardIdentityBytes*distributedtxn.MaxManifestPageParticipants)
	nextOrdinal := uint64(0)
	var reader *distributedtxn.ManifestReader
	var err error
	if pageCount == descriptor.SegmentCount {
		reader, err = distributedtxn.NewManifestReader(descriptor)
		if err != nil {
			return err
		}
	}
	for pageIndex := uint32(0); pageIndex < pageCount; pageIndex++ {
		result, readErr := orchestrator.executor.ReadTransactionRecovery(
			ctx, coordinator.Route, replicatedstate.TransactionRecoveryReadRequest{
				Kind: replicatedstate.TransactionRecoveryReadManifestPage,
				ID:   handle.ID, ManifestPage: pageIndex,
				MinimumApplied: handle.CoordinatorMinimumApplied,
				MaxRows:        1,
				MaxBytes: replicatedstate.TransactionRecoverySummaryBytes +
					distributedtxn.ManifestSegmentBytes,
			},
		)
		if readErr != nil || len(result.Records) != 1 ||
			result.Records[0].ManifestPage != pageIndex {
			return errors.Join(readErr, ErrReplicatedTransactionUnknown)
		}
		handle.CoordinatorMinimumApplied = max(handle.CoordinatorMinimumApplied, result.Applied)
		var page distributedtxn.ManifestPage
		if reader != nil {
			page, err = reader.OpenNext(
				result.Records[0].Payload, participantScratch, identityScratch,
			)
		} else {
			page, err = distributedtxn.OpenManifestSegment(
				result.Records[0].Payload, participantScratch, identityScratch,
			)
		}
		if err != nil || page.Segment.FirstParticipant != nextOrdinal {
			return errors.Join(err, ErrReplicatedTransaction)
		}
		for index := range page.Participants {
			ordinal := nextOrdinal + uint64(index)
			if ordinal >= uint64(len(handle.Participants)) ||
				!replicatedTransactionRefMatchesWitness(
					page.Participants[index], handle.Participants[ordinal],
				) {
				return ErrReplicatedTransaction
			}
		}
		nextOrdinal += uint64(len(page.Participants))
	}
	if reader != nil {
		if err = reader.Seal(); err != nil || nextOrdinal != uint64(len(handle.Participants)) {
			return errors.Join(err, ErrReplicatedTransaction)
		}
	}
	return nil
}

func replicatedTransactionRefMatchesWitness(
	ref distributedtxn.ParticipantRef,
	witness ReplicatedTransactionRouteWitness,
) bool {
	return bytes.Equal(ref.Distribution, byteview.Bytes(string(witness.Route.Distribution))) &&
		bytes.Equal(ref.Shard, byteview.Bytes(string(witness.Route.Shard))) &&
		ref.RoutingVersion == witness.Route.Command.RoutingVersion &&
		ref.AllocationGeneration == witness.Route.AllocationGeneration &&
		ref.OwnershipEpoch == witness.Route.Command.OwnershipEpoch &&
		ref.AuthorityWitness == witness.AuthorityWitness &&
		ref.MutationDigest == witness.MutationDigest &&
		ref.State == distributedtxn.ParticipantStaged
}

type replicatedTransactionTerminalProof struct {
	ordinal uint32
	record  replicatedstate.TransactionRecoveryRecord
	found   bool
	applied uint64
	err     error
}

func (orchestrator *ReplicatedTransactionOrchestrator) proveTerminalParticipants(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
	committed bool,
) (int64, error) {
	// Terminal is exported recovery material and therefore only a hint. Derive
	// the retirement proof exclusively from this call's leader-ordered reads.
	for index := range handle.Participants {
		handle.Participants[index].Terminal = false
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	workers := min(orchestrator.maxConcurrency, len(handle.Participants))
	results := make(chan replicatedTransactionTerminalProof, workers)
	var next atomic.Uint64
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for workCtx.Err() == nil {
				ordinal := int(next.Add(1) - 1)
				if ordinal >= len(handle.Participants) {
					return
				}
				witness := handle.Participants[ordinal]
				minimum := witness.MinimumApplied
				if minimum == 0 {
					minimum = 1
				}
				result, err := orchestrator.executor.ReadTransactionRecovery(
					workCtx, witness.Route, replicatedstate.TransactionRecoveryReadRequest{
						Kind: replicatedstate.TransactionRecoveryLookupParticipant,
						ID:   handle.ID, MinimumApplied: minimum, MaxRows: 1,
						MaxBytes: replicatedstate.TransactionRecoverySummaryBytes,
					},
				)
				proof := replicatedTransactionTerminalProof{ordinal: uint32(ordinal), err: err}
				if err == nil {
					proof.applied = result.Applied
					proof.found = len(result.Records) == 1
					if len(result.Records) > 1 {
						proof.err = ErrReplicatedTransaction
					} else if proof.found {
						proof.record = result.Records[0]
					}
				}
				select {
				case results <- proof:
				case <-workCtx.Done():
					return
				}
				if proof.err != nil {
					cancel()
					return
				}
			}
		}()
	}
	go func() {
		wait.Wait()
		close(results)
	}()
	var affected int64
	var joined error
	freshResults := 0
	for proof := range results {
		freshResults++
		witness := &handle.Participants[proof.ordinal]
		if proof.err != nil {
			joined = errors.Join(joined, proof.err)
			cancel()
			continue
		}
		witness.MinimumApplied = max(witness.MinimumApplied, proof.applied)
		if !proof.found {
			// Absence is never terminal: the aborted path must first install the
			// durable rev0 cancellation witness on this exact participant route.
			joined = errors.Join(joined, ErrReplicatedTransactionUnknown)
			cancel()
			continue
		}
		record := proof.record
		coordinator := handle.Participants[handle.CoordinatorOrdinal].Route
		if record.ID != handle.ID || record.Role != distributedtxn.ReplicatedRoleParticipant ||
			distributedtxn.ParticipantState(record.State) != distributedtxn.ParticipantReleased ||
			record.PayloadKind != distributedtxn.ReplicatedPayloadParticipantStage ||
			record.CoordinatorGroup != replication.ID128(coordinator.Group.GroupID) ||
			record.CoordinatorShardIncarnation != replication.ID128(coordinator.Group.ShardIncarnation) ||
			record.CoordinatorAllocation != coordinator.AllocationGeneration ||
			record.MutationDigest != witness.MutationDigest {
			joined = errors.Join(joined, ErrReplicatedTransactionUnknown)
			cancel()
			continue
		}
		if record.CancellationWitness {
			if committed || record.ParticipantOrdinal != witness.Ordinal {
				joined = errors.Join(joined, ErrReplicatedTransaction)
				cancel()
				continue
			}
		} else if record.ParticipantOrdinal != 0 {
			joined = errors.Join(joined, ErrReplicatedTransaction)
			cancel()
			continue
		}
		if committed {
			if !record.AffectedRowsValid || record.AffectedRows < 0 ||
				record.AffectedRows > math.MaxInt64-affected {
				joined = errors.Join(joined, ErrReplicatedTransaction)
				cancel()
				continue
			}
			affected += record.AffectedRows
		} else if record.AffectedRowsValid {
			joined = errors.Join(joined, ErrReplicatedTransaction)
			cancel()
			continue
		}
		witness.Terminal = true
	}
	if freshResults != len(handle.Participants) {
		joined = errors.Join(joined, ErrReplicatedTransactionUnknown)
	}
	for index := range handle.Participants {
		if !handle.Participants[index].Terminal {
			joined = errors.Join(joined, ErrReplicatedTransactionUnknown)
		}
	}
	return affected, joined
}
