package gateway

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibejson/x/byteview"
)

type DurableRequestTerminalAuthorityProvider interface {
	TerminalAuthority(context.Context, DurableRequestTypedExecutionContext) (DurableRequestTerminalAuthority, error)
}

// DurableRequestDistributedRunner executes the actual replicated transaction
// protocol sequentially over a replayable participant stream. It retains one
// participant, one manifest page pack, and one exact requestledger wave.
type DurableRequestDistributedRunner struct {
	ledger    DurableRequestLedger
	resolver  DurableRequestRouteResolver
	waves     durableDistributedWaveRunner
	payloads  durableDistributedPayloadStore
	terminal  durableDistributedTerminal
	authority DurableRequestTerminalAuthorityProvider
	pins      DurableRequestExecutionPinAuthority
	recovery  durableDistributedRecoveryReader
}

type durableDistributedWaveRunner interface {
	RunStagedWave(context.Context, DurableRequestWave) (DurableRequestWaveResult, error)
}
type durableDistributedPayloadStore interface {
	Cleanup(context.Context, DurableRequestLedgerHome, requestledger.RequestKey) (uint64, error)
}
type durableDistributedTerminal interface {
	Complete(context.Context, DurableRequestTerminalPlan) (DurableRequestTerminalResult, error)
}
type durableDistributedRecoveryReader interface {
	ReadTransactionRecovery(context.Context, ReplicatedRoute, replicatedstate.TransactionRecoveryReadRequest) (ReplicatedTransactionRecoveryResult, error)
}

// Hidden transaction state belongs to the gateway recovery service, not the
// forwarded SQL caller. Keep this identity override local to recovery reads;
// ordinary proposals must retain the caller's data-write authorization.
type authorizedDurableRecoveryReader struct {
	executor *ReplicatedExecutor
	service  serviceauthz.Authority
}

func (reader *authorizedDurableRecoveryReader) ReadTransactionRecovery(ctx context.Context, route ReplicatedRoute, request replicatedstate.TransactionRecoveryReadRequest) (ReplicatedTransactionRecoveryResult, error) {
	ctx, err := serviceauthz.WithAuthority(ctx, reader.service)
	if err != nil {
		return ReplicatedTransactionRecoveryResult{}, err
	}
	return reader.executor.ReadTransactionRecovery(ctx, route, request)
}

func NewDurableRequestDistributedRunner(
	ledger DurableRequestLedger,
	resolver DurableRequestRouteResolver,
	waves *DurableRequestLifecycleRunner,
	payloads *DurableRequestDynamicPayloadStore,
	terminal *DurableRequestTerminalCoordinator,
	authority DurableRequestTerminalAuthorityProvider,
	pins DurableRequestExecutionPinAuthority,
) (*DurableRequestDistributedRunner, error) {
	if ledger == nil || resolver == nil || waves == nil || !waves.pinAuthority.Valid() || payloads == nil || terminal == nil || authority == nil || pins == nil {
		return nil, ErrDurableRequest
	}
	runner := &DurableRequestDistributedRunner{
		ledger: ledger, resolver: resolver, waves: waves, payloads: payloads,
		terminal: terminal, authority: authority, pins: pins,
	}
	if executor, ok := waves.proposer.(*ReplicatedExecutor); ok {
		runner.recovery = &authorizedDurableRecoveryReader{executor: executor, service: waves.pinAuthority}
	}
	return runner, nil
}

func newDurableRequestDistributedRunner(
	ledger DurableRequestLedger,
	resolver DurableRequestRouteResolver,
	waves durableDistributedWaveRunner,
	payloads durableDistributedPayloadStore,
	terminal durableDistributedTerminal,
	authority DurableRequestTerminalAuthorityProvider,
) (*DurableRequestDistributedRunner, error) {
	if ledger == nil || resolver == nil || waves == nil || payloads == nil || terminal == nil || authority == nil {
		return nil, ErrDurableRequest
	}
	runner := &DurableRequestDistributedRunner{ledger: ledger, resolver: resolver, waves: waves,
		payloads: payloads, terminal: terminal, authority: authority}
	if recovery, ok := waves.(durableDistributedRecoveryReader); ok {
		runner.recovery = recovery
	}
	return runner, nil
}

type durableDistributedState struct {
	branch   uint8
	conflict bool
	affected int64
}

const (
	durableDistributedUndecided uint8 = iota
	durableDistributedCommitted
	durableDistributedAborted
	durableDistributedCursorBytes = 32
)

var durableDistributedCursorMagic = [8]byte{'V', 'D', 'R', 'T', 'X', 'N', 0, 1}

func (runner *DurableRequestDistributedRunner) RunTyped(
	ctx context.Context,
	execution DurableRequestTypedExecutionContext,
) (_ DurableRequestTerminalResult, failure error) {
	stage := "validate"
	defer func() {
		if failure != nil {
			failure = fmt.Errorf("gateway: distributed request %s: %w", stage, failure)
		}
	}()
	if runner == nil || ctx == nil || execution.Participants == nil {
		return DurableRequestTerminalResult{}, ErrDurableRequest
	}
	if !validDurableRequestProtocolProgram(execution.Recipe.Contract) {
		return DurableRequestTerminalResult{}, ErrDurableRequestConflict
	}
	stage = "terminal authority"
	authority, err := runner.authority.TerminalAuthority(ctx, execution)
	if err != nil {
		return DurableRequestTerminalResult{}, err
	}
	stage = "progress read"
	head, continuation, err := runner.openProgress(ctx, execution)
	if err != nil {
		return DurableRequestTerminalResult{}, err
	}
	// OperationAdvance moves the cursor before releasing its physical route.
	// Recover that retained release first; neither payload cleanup nor skipping
	// to the next ordinal is legal while the old route is still outstanding.
	// Only Advance installs CleanupBuildDigest and OutstandingRoutePinDigest.
	// Require its retained payload witness explicitly; an acquired/pending route
	// without that witness resumes through the current ordinal's staged path.
	if head.CleanupBuildDigest != (requestledger.Digest{}) &&
		(head.OutstandingRoutePinDigest != (requestledger.Digest{}) || head.CleanupNextChunk == 0) {
		stage = "advanced wave recovery"
		execution, err = runner.refreshExecutionPin(ctx, execution)
		if err != nil {
			return DurableRequestTerminalResult{}, err
		}
		recovery, ok := runner.waves.(interface {
			ResumeAdvancedWave(context.Context, DurableRequestTypedExecutionContext) error
		})
		if !ok {
			return DurableRequestTerminalResult{}, ErrDurableRequestUnresolved
		}
		if err = recovery.ResumeAdvancedWave(ctx, execution); err != nil {
			return DurableRequestTerminalResult{}, err
		}
		head, continuation, err = runner.openProgress(ctx, execution)
		if err != nil {
			return DurableRequestTerminalResult{}, err
		}
	}
	if head.CleanupBuildDigest != (requestledger.Digest{}) {
		stage = "payload cleanup"
		if _, err = runner.payloads.Cleanup(ctx, execution.Home, execution.Key.RequestKey); err != nil {
			return DurableRequestTerminalResult{}, err
		}
		head, continuation, err = runner.openProgress(ctx, execution)
		if err != nil {
			return DurableRequestTerminalResult{}, err
		}
	}
	stage = "continuation"
	state, err := openDurableDistributedState(continuation.Cursor)
	if err != nil && continuation.Revision != 0 {
		if bytes.Equal(continuation.Cursor, authority.CommitCursor) {
			state.branch = durableDistributedCommitted
		} else if bytes.Equal(continuation.Cursor, authority.AbortCursor) {
			state.branch = durableDistributedAborted
		} else {
			return DurableRequestTerminalResult{}, ErrDurableRequestConflict
		}
	}
	finalWaves := execution.Recipe.Contract.CommitFinalWaveCount
	if state.branch == durableDistributedAborted {
		finalWaves = execution.Recipe.Contract.AbortFinalWaveCount
	}
	// Decision cursors can have the same bytes as the terminal cursor (zero
	// affected rows). Only the certified final ordinal carries retirement.
	if continuation.Revision != 0 && head.NextStepOrdinal == finalWaves && state.branch != durableDistributedUndecided &&
		(bytes.Equal(continuation.Cursor, authority.CommitCursor) || bytes.Equal(continuation.Cursor, authority.AbortCursor)) {
		completion, openErr := replication.OpenCompletion(continuation.Observation)
		value, valueErr := replicatedstate.OpenTransactionCompletionResult(completion.ResultCode, completion.InlineResult)
		if openErr != nil || valueErr != nil || value.Role != distributedtxn.ReplicatedRoleCoordinator ||
			value.Operation != distributedtxn.ReplicatedRetireCoordinator ||
			(state.branch == durableDistributedCommitted) != value.AffectedRowsValid {
			return DurableRequestTerminalResult{}, errors.Join(openErr, valueErr, ErrDurableRequestConflict)
		}
		state.affected = value.AffectedRows
	}
	if state.branch != durableDistributedUndecided && head.NextStepOrdinal == finalWaves {
		stage = "terminal completion"
		return runner.completeTerminal(ctx, execution, authority, state)
	}
	progress := &durableDistributedProgress{
		runner: runner, execution: execution, authority: authority,
		next: head.NextStepOrdinal, state: state,
		encoder: replicatedTransactionCommandEncoder{tenant: bytes.Clone(execution.Recipe.Tenant),
			membershipStable: durableRequestMembershipStableProgram(execution.Recipe.Contract)},
	}
	stage = "manifest measurement"
	descriptor, coordinator, coordinatorRoute, err := progress.measureManifest(ctx)
	if err != nil {
		return DurableRequestTerminalResult{}, err
	}
	if head.NextStepOrdinal != 0 {
		stage = "manifest recovery"
		descriptor, err = runner.recoverManifestDescriptor(ctx, execution, coordinatorRoute)
		if err != nil {
			return DurableRequestTerminalResult{}, err
		}
	}
	manifestCommands := uint64(1)
	if descriptor.SegmentCount > distributedtxn.MaxManifestSegmentsPerCommand {
		manifestCommands += uint64(descriptor.SegmentCount-distributedtxn.MaxManifestSegmentsPerCommand+
			distributedtxn.MaxManifestSegmentsPerCommand-1) / uint64(distributedtxn.MaxManifestSegmentsPerCommand)
	}
	commitWaves := manifestCommands + 2*execution.Recipe.ParticipantCount + 1
	abortWaves := manifestCommands + 3*execution.Recipe.ParticipantCount + 1
	if execution.Recipe.Contract.CommitFinalWaveCount != commitWaves ||
		execution.Recipe.Contract.AbortFinalWaveCount != abortWaves {
		return DurableRequestTerminalResult{}, ErrDurableRequestConflict
	}
	// Before a decision, recover prepares to retain any conflict. After the
	// authenticated continuation records the decision, participants may already
	// be applied/released: requiring their old prepare state strands recovery.
	if state.branch == durableDistributedUndecided && head.NextStepOrdinal > manifestCommands {
		stage = "prepared prefix recovery"
		if err = progress.recoverPreparedPrefix(ctx, manifestCommands); err != nil {
			return DurableRequestTerminalResult{}, err
		}
	}
	stage = "manifest begin"
	if err = progress.beginManifest(ctx, descriptor, coordinator, coordinatorRoute); err != nil {
		return DurableRequestTerminalResult{}, err
	}
	stage = "participant prepare"
	if err = progress.prepare(ctx, coordinator, coordinatorRoute); err != nil {
		return DurableRequestTerminalResult{}, err
	}
	stage = "coordinator decision"
	if err = progress.decide(ctx, coordinator, coordinatorRoute, uint64(descriptor.SegmentCount)); err != nil {
		return DurableRequestTerminalResult{}, err
	}
	stage = "participant release"
	if err = progress.finish(ctx, coordinator, coordinatorRoute); err != nil {
		return DurableRequestTerminalResult{}, err
	}
	stage = "coordinator retirement"
	if err = progress.retire(ctx, coordinator, coordinatorRoute, uint64(descriptor.SegmentCount)); err != nil {
		return DurableRequestTerminalResult{}, err
	}
	stage = "terminal completion"
	return runner.completeTerminal(ctx, progress.execution, authority, progress.state)
}

func (runner *DurableRequestDistributedRunner) completeTerminal(
	ctx context.Context,
	execution DurableRequestTypedExecutionContext,
	authority DurableRequestTerminalAuthority,
	state durableDistributedState,
) (DurableRequestTerminalResult, error) {
	for attempt := 0; ; attempt++ {
		result, err := runner.completeTerminalAttempt(ctx, execution, authority, state)
		var advanced *durableExecutionPinAdvancedError
		if !errors.As(err, &advanced) || attempt == 3 || ctx.Err() != nil {
			return result, err
		}
		// Re-read the durable terminal cut and refresh its exact authority.
		// This marker proves the terminal side-effect fence admitted nothing.
	}
}

func (runner *DurableRequestDistributedRunner) completeTerminalAttempt(
	ctx context.Context,
	execution DurableRequestTypedExecutionContext,
	authority DurableRequestTerminalAuthority,
	state durableDistributedState,
) (DurableRequestTerminalResult, error) {
	// Terminal recovery owns an immutable prepared ACK and, when present, an
	// exact release intent. Read that cut before any attempted lease takeover.
	if runner.pins != nil {
		reader, ok := runner.ledger.(durableRequestTerminalCutReader)
		if !ok {
			return DurableRequestTerminalResult{}, ErrDurableRequestUnresolved
		}
		cut, err := reader.ReadTerminalCut(ctx, execution.Home, execution.Key.RequestKey)
		if err != nil {
			return DurableRequestTerminalResult{}, err
		}
		execution.terminalCut = nil
		if cut.Head.Phase == requestledger.PhasePrepared {
			if err = validateDurableRequestPreparedCut(execution, cut); err != nil {
				return DurableRequestTerminalResult{}, err
			}
			execution.terminalCut = &cut
		}
		execution, err = runner.refreshExecutionPin(ctx, execution)
		if err != nil {
			return DurableRequestTerminalResult{}, err
		}
		authority, err = runner.authority.TerminalAuthority(ctx, execution)
		if err != nil {
			return DurableRequestTerminalResult{}, err
		}
	}
	progress := durableDistributedProgress{execution: execution, state: state}
	result, err := progress.result()
	if err != nil {
		return DurableRequestTerminalResult{}, err
	}
	outcome := requestledger.OutcomeCommitted
	if state.branch == durableDistributedAborted {
		outcome = requestledger.OutcomeAborted
	}
	return runner.terminal.Complete(ctx, DurableRequestTerminalPlan{
		Execution: execution,
		Home:      execution.Home, Key: execution.Key.RequestKey, Outcome: outcome,
		AffectedRows:      state.affected,
		AffectedRowsValid: outcome == requestledger.OutcomeCommitted,
		Result:            result,
		RetirementWitness: requestledger.Digest(execution.Recipe.Contract.RetirementWitnessDigest),
		AckToken:          authority.AckToken, Release: authority.Release,
		Lease: execution.ExecutionPinLease,
	})
}

func (runner *DurableRequestDistributedRunner) refreshExecutionPin(ctx context.Context, execution DurableRequestTypedExecutionContext) (DurableRequestTypedExecutionContext, error) {
	// The private fixture constructor can omit the native pin driver. The
	// exported production constructor requires it, before accepting traffic.
	if runner.pins == nil {
		return execution, nil
	}
	route, acquire, lease, err := runner.pins.AcquireOrRecover(ctx, execution)
	if err != nil {
		return DurableRequestTypedExecutionContext{}, err
	}
	return BindDurableRequestExecutionPin(execution, route, acquire, lease)
}

func (runner *DurableRequestDistributedRunner) recoverManifestDescriptor(
	ctx context.Context,
	execution DurableRequestTypedExecutionContext,
	route ReplicatedRoute,
) (distributedtxn.ManifestDescriptor, error) {
	if runner.recovery == nil {
		return distributedtxn.ManifestDescriptor{}, ErrDurableRequestUnresolved
	}
	result, err := runner.recovery.ReadTransactionRecovery(ctx, route,
		replicatedstate.TransactionRecoveryReadRequest{
			Kind: replicatedstate.TransactionRecoveryLookupCoordinator,
			ID:   execution.Recipe.Identity.ID, MinimumApplied: 1, MaxRows: 1,
			MaxBytes: uint32(replicatedstate.TransactionRecoverySummaryBytes + distributedtxn.MaxCoordinatorRecordBytes),
		})
	if err != nil || len(result.Records) != 1 {
		return distributedtxn.ManifestDescriptor{}, errors.Join(err, ErrDurableRequestUnresolved)
	}
	record := result.Records[0]
	manifest, openErr := distributedtxn.OpenManifestCoordinator(record.Payload)
	if openErr != nil || record.ID != execution.Recipe.Identity.ID ||
		record.PayloadKind != distributedtxn.ReplicatedPayloadManifestCoordinator ||
		manifest.Manifest.ParticipantCount != execution.Recipe.ParticipantCount {
		return distributedtxn.ManifestDescriptor{}, errors.Join(openErr, ErrDurableRequestConflict)
	}
	return manifest.Manifest, nil
}

func (progress *durableDistributedProgress) prepare(
	ctx context.Context,
	coordinator DurableRequestLogicalParticipant,
	coordinatorRoute ReplicatedRoute,
) error {
	if err := progress.execution.Participants.Reset(); err != nil {
		return err
	}
	var ordinal uint64
	for progress.execution.Participants.Next() {
		logical := progress.execution.Participants.Current()
		if ordinal == uint64(progress.execution.Recipe.Identity.CoordinatorOrdinal) {
			ordinal++
			continue
		}
		route, _, err := progress.resolve(ctx, logical)
		if err != nil {
			return err
		}
		control := distributedtxn.ReplicatedCommand{
			Role:        distributedtxn.ReplicatedRoleParticipant,
			Operation:   distributedtxn.ReplicatedStagePrepareParticipant,
			ID:          progress.execution.Recipe.Identity.ID,
			PayloadKind: distributedtxn.ReplicatedPayloadParticipantStage,
			Participant: progress.participantStage(logical, coordinatorRoute, uint32(ordinal)),
		}
		_, err = progress.command(ctx, logical, route, control, logical.Batches,
			func(code uint32, _ replicatedstate.TransactionCompletionResult) error {
				switch code {
				case replicatedstate.ResultApplied:
					return nil
				case replicatedstate.ResultIndexConflict:
					progress.state.conflict = true
					return nil
				default:
					return ErrReplicatedTransaction
				}
			}, false)
		if err != nil {
			return err
		}
		ordinal++
	}
	return errors.Join(progress.execution.Participants.Err())
}

func (progress *durableDistributedProgress) recoverPreparedPrefix(
	ctx context.Context,
	manifestCommands uint64,
) error {
	if progress.runner.recovery == nil {
		return ErrDurableRequestUnresolved
	}
	completed := min(progress.next-manifestCommands, progress.execution.Recipe.ParticipantCount-1)
	if completed == 0 {
		return nil
	}
	if err := progress.execution.Participants.Reset(); err != nil {
		return err
	}
	var ordinal, checked uint64
	for progress.execution.Participants.Next() && checked < completed {
		logical := progress.execution.Participants.Current()
		if ordinal == uint64(progress.execution.Recipe.Identity.CoordinatorOrdinal) {
			ordinal++
			continue
		}
		route, _, err := progress.resolve(ctx, logical)
		if err != nil {
			return err
		}
		result, readErr := progress.runner.recovery.ReadTransactionRecovery(ctx, route,
			replicatedstate.TransactionRecoveryReadRequest{
				Kind: replicatedstate.TransactionRecoveryLookupParticipant,
				ID:   progress.execution.Recipe.Identity.ID, MinimumApplied: 1,
				MaxRows: 1, MaxBytes: replicatedstate.TransactionRecoverySummaryBytes,
			})
		if readErr != nil {
			return readErr
		}
		if len(result.Records) == 0 {
			progress.state.conflict = true
		} else if len(result.Records) != 1 || result.Records[0].ID != progress.execution.Recipe.Identity.ID ||
			result.Records[0].Role != distributedtxn.ReplicatedRoleParticipant ||
			distributedtxn.ParticipantState(result.Records[0].State) != distributedtxn.ParticipantPrepared {
			return ErrDurableRequestConflict
		}
		checked++
		ordinal++
	}
	if err := progress.execution.Participants.Err(); err != nil {
		return err
	}
	if checked != completed {
		return ErrDurableRequestConflict
	}
	return nil
}

func (progress *durableDistributedProgress) decide(
	ctx context.Context,
	coordinator DurableRequestLogicalParticipant,
	route ReplicatedRoute,
	decisionRevision uint64,
) error {
	operation := distributedtxn.ReplicatedCommitCoordinator
	branch := durableDistributedCommitted
	if progress.state.conflict {
		operation, branch = distributedtxn.ReplicatedAbortCoordinator, durableDistributedAborted
	}
	control := distributedtxn.ReplicatedCommand{
		Role: distributedtxn.ReplicatedRoleCoordinator, Operation: operation,
		ID: progress.execution.Recipe.Identity.ID, ExpectedRevision: decisionRevision,
		PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}
	_, err := progress.command(ctx, coordinator, route, control, nil,
		func(code uint32, _ replicatedstate.TransactionCompletionResult) error {
			if code != replicatedstate.ResultApplied {
				return ErrReplicatedTransaction
			}
			progress.state.branch = branch
			return nil
		}, false)
	return err
}

func (progress *durableDistributedProgress) finish(
	ctx context.Context,
	_ DurableRequestLogicalParticipant,
	coordinatorRoute ReplicatedRoute,
) error {
	if progress.state.branch == durableDistributedUndecided {
		return ErrDurableRequestConflict
	}
	if err := progress.execution.Participants.Reset(); err != nil {
		return err
	}
	var ordinal uint32
	for progress.execution.Participants.Next() {
		logical := progress.execution.Participants.Current()
		route, _, err := progress.resolve(ctx, logical)
		if err != nil {
			return err
		}
		if progress.state.branch == durableDistributedAborted {
			fence := distributedtxn.ReplicatedCommand{
				Role:      distributedtxn.ReplicatedRoleParticipant,
				Operation: distributedtxn.ReplicatedAbortReleaseParticipant,
				ID:        progress.execution.Recipe.Identity.ID, ExpectedRevision: 0,
				PayloadKind: distributedtxn.ReplicatedPayloadParticipantStage,
				Participant: distributedtxn.ParticipantStage{
					CoordinatorGroup:            distributedtxn.ID(coordinatorRoute.Group.GroupID),
					CoordinatorShardIncarnation: distributedtxn.ID(coordinatorRoute.Group.ShardIncarnation),
					CoordinatorAllocation:       coordinatorRoute.AllocationGeneration,
					MutationDigest:              logical.MutationDigest, ParticipantOrdinal: ordinal,
				},
			}
			if _, err = progress.command(ctx, logical, route, fence, nil,
				func(code uint32, _ replicatedstate.TransactionCompletionResult) error {
					if code != replicatedstate.ResultApplied && code != replicatedstate.ResultTransactionConflict {
						return ErrReplicatedTransaction
					}
					return nil
				}, false); err != nil {
				return err
			}
		}
		operation := distributedtxn.ReplicatedApplyReleaseParticipant
		if progress.state.branch == durableDistributedAborted {
			operation = distributedtxn.ReplicatedAbortReleaseParticipant
		}
		release := distributedtxn.ReplicatedCommand{
			Role: distributedtxn.ReplicatedRoleParticipant, Operation: operation,
			ID: progress.execution.Recipe.Identity.ID, ExpectedRevision: 2,
			PayloadKind: distributedtxn.ReplicatedPayloadNone,
		}
		if _, err = progress.command(ctx, logical, route, release, nil,
			func(code uint32, value replicatedstate.TransactionCompletionResult) error {
				accepted := code == replicatedstate.ResultApplied ||
					progress.state.branch == durableDistributedAborted && code == replicatedstate.ResultTransactionConflict
				if !accepted {
					return ErrReplicatedTransaction
				}
				if progress.state.branch == durableDistributedCommitted {
					if !value.AffectedRowsValid || value.AffectedRows > math.MaxInt64-progress.state.affected {
						return ErrReplicatedTransaction
					}
					progress.state.affected += value.AffectedRows
				}
				return nil
			}, false); err != nil {
			return err
		}
		ordinal++
	}
	return errors.Join(progress.execution.Participants.Err())
}

func (progress *durableDistributedProgress) retire(
	ctx context.Context,
	coordinator DurableRequestLogicalParticipant,
	route ReplicatedRoute,
	decisionRevision uint64,
) error {
	var storage [distributedtxn.ReplicatedRetirementSummaryBytes]byte
	summary := distributedtxn.ReplicatedRetirementSummary{}
	if progress.state.branch == durableDistributedCommitted {
		summary.AffectedRows, summary.AffectedRowsValid = progress.state.affected, true
	}
	payload, err := distributedtxn.AppendReplicatedRetirementSummary(storage[:0], summary)
	if err != nil {
		return err
	}
	control := distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleCoordinator,
		Operation: distributedtxn.ReplicatedRetireCoordinator,
		ID:        progress.execution.Recipe.Identity.ID, ExpectedRevision: decisionRevision + 1,
		PayloadKind: distributedtxn.ReplicatedPayloadRetirement, Payload: payload,
	}
	_, err = progress.command(ctx, coordinator, route, control, nil,
		func(code uint32, value replicatedstate.TransactionCompletionResult) error {
			if code != replicatedstate.ResultApplied || value.AffectedRowsValid != summary.AffectedRowsValid ||
				value.AffectedRows != summary.AffectedRows {
				return ErrReplicatedTransaction
			}
			return nil
		}, true)
	return err
}

func (progress *durableDistributedProgress) result() ([]byte, error) {
	committed := progress.state.branch == durableDistributedCommitted
	transition := progress.execution.Recipe.Contract.AbortTransitionTag
	stateDigest := progress.execution.Recipe.Contract.AbortTerminalStateDigest
	if committed {
		transition = progress.execution.Recipe.Contract.CommitTransitionTag
		stateDigest = progress.execution.Recipe.Contract.CommitTerminalStateDigest
	}
	return AppendDurableRequestResult(nil, DurableRequestResult{
		Committed: committed, AffectedRows: progress.state.affected,
		Transaction:       progress.execution.Recipe.Identity.ID,
		CatalogGeneration: progress.execution.Recipe.CatalogGeneration,
		ShardsFanned:      progress.execution.Recipe.ParticipantCount,
		TransitionTag:     transition, TerminalStateDigest: stateDigest,
		TerminalContractDigest:  progress.execution.Recipe.Contract.TerminalContractDigest,
		RetirementWitnessDigest: progress.execution.Recipe.Contract.RetirementWitnessDigest,
	})
}

type durableDistributedProgress struct {
	runner    *DurableRequestDistributedRunner
	execution DurableRequestTypedExecutionContext
	authority DurableRequestTerminalAuthority
	next      uint64
	ordinal   uint64
	state     durableDistributedState
	encoder   replicatedTransactionCommandEncoder
}

func (progress *durableDistributedProgress) measureManifest(
	ctx context.Context,
) (distributedtxn.ManifestDescriptor, DurableRequestLogicalParticipant, ReplicatedRoute, error) {
	if err := progress.execution.Participants.Reset(); err != nil {
		return distributedtxn.ManifestDescriptor{}, DurableRequestLogicalParticipant{}, ReplicatedRoute{}, err
	}
	scratch := make([]byte, distributedtxn.ManifestSegmentBytes)
	builder, err := distributedtxn.NewManifestBuilder(scratch, func(distributedtxn.ManifestSegment) error { return nil })
	if err != nil {
		return distributedtxn.ManifestDescriptor{}, DurableRequestLogicalParticipant{}, ReplicatedRoute{}, err
	}
	var coordinator DurableRequestLogicalParticipant
	var coordinatorRoute ReplicatedRoute
	var ordinal uint64
	for progress.execution.Participants.Next() {
		logical := progress.execution.Participants.Current()
		route, ref, resolveErr := progress.resolve(ctx, logical)
		if resolveErr != nil {
			return distributedtxn.ManifestDescriptor{}, DurableRequestLogicalParticipant{}, ReplicatedRoute{}, resolveErr
		}
		if ordinal == uint64(progress.execution.Recipe.Identity.CoordinatorOrdinal) {
			coordinator = cloneDurableLogicalParticipant(logical)
			coordinatorRoute = cloneDurableRequestRoute(route)
		}
		if err = builder.Append(ref); err != nil {
			return distributedtxn.ManifestDescriptor{}, DurableRequestLogicalParticipant{}, ReplicatedRoute{}, err
		}
		ordinal++
	}
	if err = progress.execution.Participants.Err(); err != nil ||
		!progress.execution.Participants.Complete() || ordinal != progress.execution.Recipe.ParticipantCount ||
		coordinatorRoute.Group == (raftmember.GroupKey{}) {
		return distributedtxn.ManifestDescriptor{}, DurableRequestLogicalParticipant{}, ReplicatedRoute{},
			errors.Join(err, ErrDurableRequestConflict)
	}
	descriptor, err := builder.Seal()
	return descriptor, coordinator, coordinatorRoute, err
}

func (progress *durableDistributedProgress) beginManifest(
	ctx context.Context,
	descriptor distributedtxn.ManifestDescriptor,
	coordinator DurableRequestLogicalParticipant,
	coordinatorRoute ReplicatedRoute,
) error {
	if err := progress.execution.Participants.Reset(); err != nil {
		return err
	}
	scratch := make([]byte, distributedtxn.ManifestSegmentBytes)
	var initial, pack []byte
	var started bool
	flushStart := func() error {
		if started {
			return nil
		}
		record, err := distributedtxn.AppendManifestCoordinator(nil, distributedtxn.ManifestCoordinatorRecord{
			ID: progress.execution.Recipe.Identity.ID, State: distributedtxn.CoordinatorStaging,
			Revision: 1, CatalogGeneration: progress.execution.Recipe.CatalogGeneration,
			RecoveryDeadline: progress.execution.Recipe.Identity.RecoveryDeadline, Manifest: descriptor,
		})
		if err != nil {
			return err
		}
		control := distributedtxn.ReplicatedCommand{
			Role:        distributedtxn.ReplicatedRoleCoordinator,
			Operation:   distributedtxn.ReplicatedBeginPrepareManifestCoordinator,
			ID:          progress.execution.Recipe.Identity.ID,
			PayloadKind: distributedtxn.ReplicatedPayloadManifestCoordinator,
			Payload:     append(record, initial...),
			Participant: progress.participantStage(coordinator, coordinatorRoute,
				progress.execution.Recipe.Identity.CoordinatorOrdinal),
		}
		_, err = progress.command(ctx, coordinator, coordinatorRoute, control, coordinator.Batches,
			func(code uint32, value replicatedstate.TransactionCompletionResult) error {
				if code == replicatedstate.ResultIndexConflict {
					progress.state.conflict = true
					return nil
				}
				if code != replicatedstate.ResultApplied {
					return ErrReplicatedTransaction
				}
				return nil
			}, false)
		started = err == nil
		return err
	}
	flushPack := func() error {
		if len(pack) == 0 {
			return nil
		}
		sequence, err := distributedtxn.OpenManifestSegmentSequence(pack)
		if err != nil {
			return err
		}
		control := distributedtxn.ReplicatedCommand{
			Role:             distributedtxn.ReplicatedRoleCoordinator,
			Operation:        distributedtxn.ReplicatedAppendManifestSegments,
			ID:               progress.execution.Recipe.Identity.ID,
			ExpectedRevision: uint64(sequence.FirstIndex()),
			PayloadKind:      distributedtxn.ReplicatedPayloadManifestSegments, Payload: pack,
		}
		_, err = progress.command(ctx, coordinator, coordinatorRoute, control, nil,
			func(code uint32, _ replicatedstate.TransactionCompletionResult) error {
				if code != replicatedstate.ResultApplied {
					return ErrReplicatedTransaction
				}
				return nil
			}, false)
		pack = pack[:0]
		return err
	}
	builder, err := distributedtxn.NewManifestBuilder(scratch, func(segment distributedtxn.ManifestSegment) error {
		if segment.Index < distributedtxn.MaxManifestSegmentsPerCommand {
			initial = append(initial, segment.Raw...)
			if segment.Index+1 == min(descriptor.SegmentCount, uint32(distributedtxn.MaxManifestSegmentsPerCommand)) {
				return flushStart()
			}
			return nil
		}
		pack = append(pack, segment.Raw...)
		if (segment.Index-uint32(distributedtxn.MaxManifestSegmentsPerCommand)+1)%
			uint32(distributedtxn.MaxManifestSegmentsPerCommand) == 0 {
			return flushPack()
		}
		return nil
	})
	if err != nil {
		return err
	}
	for progress.execution.Participants.Next() {
		_, ref, resolveErr := progress.resolve(ctx, progress.execution.Participants.Current())
		if resolveErr != nil {
			return resolveErr
		}
		if err = builder.Append(ref); err != nil {
			return err
		}
	}
	if err = progress.execution.Participants.Err(); err != nil {
		return err
	}
	got, err := builder.Seal()
	if err != nil || got != descriptor {
		return errors.Join(err, ErrDurableRequestConflict)
	}
	if err = flushStart(); err != nil {
		return err
	}
	return flushPack()
}

func (progress *durableDistributedProgress) command(
	ctx context.Context,
	logical DurableRequestLogicalParticipant,
	route ReplicatedRoute,
	control distributedtxn.ReplicatedCommand,
	batches []replication.RelationMutationBatch,
	settle func(uint32, replicatedstate.TransactionCompletionResult) error,
	final bool,
) (_ bool, failure error) {
	ordinal := progress.ordinal
	defer func() {
		if failure != nil {
			failure = fmt.Errorf("gateway: transaction wave %d role %d operation %d: %w", ordinal, control.Role, control.Operation, failure)
		}
	}()
	progress.ordinal++
	if ordinal < progress.next {
		return false, nil
	}
	if ordinal != progress.next {
		return false, ErrDurableRequestConflict
	}
	if progress.execution.terminalCut != nil {
		return false, ErrDurableRequestConflict
	}
	var err error
	progress.execution, err = progress.runner.refreshExecutionPin(ctx, progress.execution)
	if err != nil {
		return false, err
	}
	// Every participant and coordinator transition carries the same aggregate
	// execution authority and the controller epoch proven at wave admission.
	// Replicas persist this pair and reject stale or mismatched controllers
	// locally, avoiding a ledger-home ReadIndex on each shard proposal.
	lease := progress.execution.ExecutionPinLease
	if !lease.Valid() || lease.ControllerEpoch == 0 ||
		progress.execution.Recipe.Contract.PinDigest == (replication.Digest{}) {
		return false, ErrDurableRequestConflict
	}
	commandEpoch := lease.ControllerEpoch
	if store, ok := progress.runner.payloads.(interface {
		ExistingCommandEpoch(context.Context, DurableRequestLedgerHome, requestledger.RequestKey, uint64) (uint64, error)
	}); ok {
		retained, readErr := store.ExistingCommandEpoch(ctx, progress.execution.Home, progress.execution.Key.RequestKey, ordinal)
		if readErr != nil || retained > commandEpoch {
			return false, errors.Join(readErr, ErrDurableRequestConflict)
		}
		if retained != 0 {
			commandEpoch = retained
		}
	}
	control.ControllerEpoch = commandEpoch
	control.ExecutionPinDigest = distributedtxn.Digest(progress.execution.Recipe.Contract.PinDigest)
	exact, err := progress.encoder.appendExact(
		nil, progress.execution.Recipe.Identity.RetryHome, route, control, batches,
	)
	if err != nil {
		return false, err
	}
	target := bytes.Clone(route.Group.GroupID[:])
	wave := DurableRequestWave{
		Home: progress.execution.Home, Key: progress.execution.Key.RequestKey,
		Participant: logical, Identity: progress.execution.Recipe.Identity,
		Tenant:            progress.execution.Recipe.Tenant,
		PinID:             progress.execution.Recipe.Contract.PinID,
		GateEpoch:         lease.ControllerEpoch,
		Binding:           requestledger.Digest(progress.execution.Recipe.Contract.PinDigest),
		ExecutionPinRoute: progress.execution.ExecutionPinRoute,
		ExecutionPinLease: progress.execution.ExecutionPinLease,
		CommandEpoch:      commandEpoch,
		Ordinal:           ordinal, Target: target, Command: exact,
	}
	wave.Settle = func(observation []byte) (uint32, []byte, error) {
		code, value, decodeErr := durableDistributedCompletion(exact, control, observation)
		if decodeErr != nil {
			return 0, nil, decodeErr
		}
		if settleErr := settle(code, value); settleErr != nil {
			return 0, nil, settleErr
		}
		if final {
			if progress.state.branch == durableDistributedCommitted {
				return progress.execution.Recipe.Contract.CommitTransitionTag,
					progress.authority.CommitCursor, nil
			}
			return progress.execution.Recipe.Contract.AbortTransitionTag,
				progress.authority.AbortCursor, nil
		}
		cursor := appendDurableDistributedState(nil, progress.state)
		return progress.execution.Recipe.Contract.CommitTransitionTag, cursor, nil
	}
	for attempt := 0; ; attempt++ {
		_, err = progress.runner.waves.RunStagedWave(ctx, wave)
		var advanced *durableExecutionPinAdvancedError
		if !errors.As(err, &advanced) || attempt == 3 || ctx.Err() != nil {
			if err != nil {
				return false, err
			}
			break
		}
		// A one-successor pin may expire while another catalog command is
		// applying. No wave side effect ran: reacquire the exact execution pin
		// and repeat admission with the unchanged participant command bytes.
		progress.execution, err = progress.runner.refreshExecutionPin(ctx, progress.execution)
		if err != nil {
			return false, err
		}
		wave.ExecutionPinRoute, wave.ExecutionPinLease = progress.execution.ExecutionPinRoute, progress.execution.ExecutionPinLease
		wave.GateEpoch = wave.ExecutionPinLease.ControllerEpoch
	}
	if _, err = progress.runner.payloads.Cleanup(ctx, progress.execution.Home, progress.execution.Key.RequestKey); err != nil {
		return false, err
	}
	progress.next++
	return true, nil
}

func (progress *durableDistributedProgress) resolve(
	ctx context.Context,
	logical DurableRequestLogicalParticipant,
) (ReplicatedRoute, distributedtxn.ParticipantRef, error) {
	route, err := progress.runner.resolver.ResolveDurableRequestParticipant(ctx, logical)
	if err != nil {
		return ReplicatedRoute{}, distributedtxn.ParticipantRef{}, err
	}
	if !durableRequestRouteMatchesParticipant(route, logical) ||
		!distributedtxn.ValidateIntentScopes(logical.IntentScopes, logical.BucketBits) ||
		logical.MutationDigest == (distributedtxn.Digest{}) {
		return ReplicatedRoute{}, distributedtxn.ParticipantRef{}, ErrDurableRequestConflict
	}
	route.membershipStable = progress.encoder.membershipStable
	return route, distributedtxn.ParticipantRef{
		Distribution:         byteview.Bytes(string(route.Distribution)),
		Shard:                byteview.Bytes(string(route.Shard)),
		RoutingVersion:       route.Command.RoutingVersion,
		AllocationGeneration: route.AllocationGeneration,
		OwnershipEpoch:       route.Command.OwnershipEpoch,
		AuthorityWitness:     replicatedTransactionRouteAuthorityWitness(route, progress.encoder.membershipStable),
		MutationDigest:       logical.MutationDigest,
		State:                distributedtxn.ParticipantStaged,
	}, nil
}

func (progress *durableDistributedProgress) participantStage(
	logical DurableRequestLogicalParticipant,
	coordinator ReplicatedRoute,
	ordinal uint32,
) distributedtxn.ParticipantStage {
	return distributedtxn.ParticipantStage{
		CoordinatorGroup:            distributedtxn.ID(coordinator.Group.GroupID),
		CoordinatorShardIncarnation: distributedtxn.ID(coordinator.Group.ShardIncarnation),
		CoordinatorAllocation:       coordinator.AllocationGeneration,
		BucketBits:                  logical.BucketBits, IntentScopes: logical.IntentScopes,
		MutationDigest: logical.MutationDigest, ParticipantOrdinal: ordinal,
	}
}

func (runner *DurableRequestDistributedRunner) openProgress(
	ctx context.Context,
	execution DurableRequestTypedExecutionContext,
) (requestledger.HeadRecord, requestledger.ContinuationRecord, error) {
	if reader, ok := runner.ledger.(durableRequestProgressCutReader); ok {
		cut, err := reader.ReadProgressCut(ctx, execution.Home, execution.Key.RequestKey)
		if err != nil {
			return requestledger.HeadRecord{}, requestledger.ContinuationRecord{}, err
		}
		return cut.Head, cut.Continuation, nil
	}
	headRow, err := runner.ledger.ReadRow(ctx, execution.Home, DurableRequestLifecycleRead{
		Key: execution.Key.RequestKey, Kind: replicatedstate.RequestLedgerReadHead, MinimumApplied: 1,
	})
	if err != nil || !headRow.Found || headRow.Kind != replicatedstate.RequestLedgerReadHead {
		return requestledger.HeadRecord{}, requestledger.ContinuationRecord{}, errors.Join(err, ErrDurableRequestConflict)
	}
	row, err := runner.ledger.ReadRow(ctx, execution.Home, DurableRequestLifecycleRead{
		Key: execution.Key.RequestKey, Kind: replicatedstate.RequestLedgerReadContinuation,
		MinimumApplied: headRow.Applied,
	})
	if err != nil {
		return requestledger.HeadRecord{}, requestledger.ContinuationRecord{}, err
	}
	if !row.Found {
		return headRow.Head, requestledger.ContinuationRecord{}, nil
	}
	if row.Kind != replicatedstate.RequestLedgerReadContinuation {
		return requestledger.HeadRecord{}, requestledger.ContinuationRecord{}, ErrDurableRequestConflict
	}
	return headRow.Head, row.Continuation, nil
}

func durableDistributedCompletion(
	exact []byte,
	control distributedtxn.ReplicatedCommand,
	observation []byte,
) (uint32, replicatedstate.TransactionCompletionResult, error) {
	command, err := replication.OpenCommand(exact)
	completion, openErr := replication.OpenCompletion(observation)
	if err != nil || openErr != nil || !nativeCompletionMatches(command, completion) {
		return 0, replicatedstate.TransactionCompletionResult{}, errors.Join(err, openErr, ErrReplicatedTransaction)
	}
	value, err := replicatedstate.OpenTransactionCompletionResult(completion.ResultCode, completion.InlineResult)
	if err != nil || value.Role != control.Role || value.Operation != control.Operation {
		return 0, replicatedstate.TransactionCompletionResult{}, errors.Join(err, ErrReplicatedTransaction)
	}
	return completion.ResultCode, value, nil
}

func appendDurableDistributedState(dst []byte, state durableDistributedState) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, durableDistributedCursorBytes)...)
	out := dst[start:]
	copy(out[:8], durableDistributedCursorMagic[:])
	out[8] = state.branch
	if state.conflict {
		out[9] = 1
	}
	binary.LittleEndian.PutUint64(out[16:24], uint64(state.affected))
	return dst
}

func openDurableDistributedState(raw []byte) (durableDistributedState, error) {
	if len(raw) == 0 {
		return durableDistributedState{}, nil
	}
	if len(raw) != durableDistributedCursorBytes || !bytes.Equal(raw[:8], durableDistributedCursorMagic[:]) ||
		raw[8] > durableDistributedAborted || raw[9] > 1 || !allZero(raw[10:16]) || !allZero(raw[24:]) {
		return durableDistributedState{}, ErrDurableRequestConflict
	}
	state := durableDistributedState{
		branch: raw[8], conflict: raw[9] == 1,
		affected: int64(binary.LittleEndian.Uint64(raw[16:24])),
	}
	if state.affected < 0 || state.branch == durableDistributedAborted && state.affected != 0 {
		return durableDistributedState{}, ErrDurableRequestConflict
	}
	return state, nil
}

func cloneDurableLogicalParticipant(value DurableRequestLogicalParticipant) DurableRequestLogicalParticipant {
	cloned := value
	// The recipe reader lends names from its reusable participant frame too.
	cloned.Distribution = distribution.DistributionName(strings.Clone(string(value.Distribution)))
	cloned.Shard = distribution.ShardID(strings.Clone(string(value.Shard)))
	cloned.IntentScopes = slices.Clone(value.IntentScopes)
	cloned.Batches = slices.Clone(value.Batches)
	for batchIndex := range cloned.Batches {
		cloned.Batches[batchIndex].Mutations = slices.Clone(value.Batches[batchIndex].Mutations)
		for mutationIndex := range cloned.Batches[batchIndex].Mutations {
			mutation := &cloned.Batches[batchIndex].Mutations[mutationIndex]
			mutation.Key = bytes.Clone(mutation.Key)
			mutation.Value = bytes.Clone(mutation.Value)
		}
	}
	return cloned
}

var _ DurableRequestTypedRunner = (*DurableRequestDistributedRunner)(nil)
