package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/routegate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

// DurableRequestWave is one already-sealed physical work item. Target and
// Command are the exact bytes named by Step; they must remain immutable for
// RunWave. The referenced bytes already live in the sealed plan or sealed
// dynamic payload, so a crash never makes an outcome-unknown proposal depend
// on process memory.
type DurableRequestWave struct {
	Home              DurableRequestLedgerHome
	Key               requestledger.RequestKey
	LogicalTarget     DurableRequestLogicalTarget
	Identity          ReplicatedTransactionIdentity
	Tenant            []byte
	PinID             requestledger.PinID
	GateEpoch         uint64
	Binding           requestledger.Digest
	ExecutionPinRoute ReplicatedRoute
	ExecutionPinLease executionpin.LeaseCertificate
	// CommandEpoch is immutable once payload-build admission wins; a retry
	// may carry a newer ExecutionPinLease without rewriting this command.
	CommandEpoch uint64
	Build        requestledger.PayloadBuildRecord
	Step         requestledger.StepRef
	Ordinal      uint64
	Target       []byte
	Command      []byte
	Transition   uint32
	Cursor       []byte
	// Settle derives result-dependent protocol state only after Command has an
	// authenticated completion. Exactly one of Settle or the fixed
	// Transition/Cursor pair is accepted.
	Settle DurableRequestWaveSettlement
}

type DurableRequestWaveSettlement func(observation []byte) (transition uint32, cursor []byte, err error)

// DurableRequestWaveResult is the exact authenticated shard observation and
// the final ledger revision after the route release proof was installed.
type DurableRequestWaveResult struct {
	Observation []byte
	Revision    uint64
}

type durableRequestWaveProposer interface {
	Propose(context.Context, ReplicatedRoute, []byte) (ReplicatedResult, error)
}

type durableRequestExecutionPinFencer interface {
	ValidateExecutionPinFence(
		context.Context,
		ReplicatedRoute,
		executionpin.LeaseCertificate,
		uint64,
	) (ReplicatedExecutionPinReadResult, error)
}

type durableRequestWaveStager interface {
	Stage(context.Context, DurableRequestLedgerHome, requestledger.RequestKey, uint64, []byte, []byte, uint64) (DurableRequestDynamicPayload, error)
}

// DurableRequestLifecycleRunner drives one target at a time. Width is
// therefore bounded by bytes and the persisted uint64 wave ordinal, not by an
// aggregate target slice or a policy target cap.
type DurableRequestLifecycleRunner struct {
	ledger       DurableRequestLedger
	resolver     DurableRequestRouteResolver
	proposer     durableRequestWaveProposer
	pinFencer    durableRequestExecutionPinFencer
	pinAuthority serviceauthz.Authority
	payloads     durableRequestWaveStager
	gateSessions durableRequestRouteGateSessions
}

func NewDurableRequestLifecycleRunner(
	ledger DurableRequestLedger,
	resolver DurableRequestRouteResolver,
	executor *ReplicatedExecutor,
	pinAuthority serviceauthz.Authority,
) (*DurableRequestLifecycleRunner, error) {
	if ledger == nil || resolver == nil || executor == nil || !pinAuthority.Valid() {
		return nil, ErrDurableRequest
	}
	return &DurableRequestLifecycleRunner{
		ledger: ledger, resolver: resolver, proposer: executor, pinFencer: executor,
		pinAuthority: pinAuthority,
		payloads:     &DurableRequestDynamicPayloadStore{ledger: ledger},
		gateSessions: &nativeDurableRequestRouteGateSessions{executor: executor},
	}, nil
}

func newDurableRequestLifecycleRunner(
	ledger DurableRequestLedger,
	resolver DurableRequestRouteResolver,
	proposer durableRequestWaveProposer,
	fencers ...durableRequestExecutionPinFencer,
) (*DurableRequestLifecycleRunner, error) {
	if ledger == nil || resolver == nil || proposer == nil {
		return nil, ErrDurableRequest
	}
	var fencer durableRequestExecutionPinFencer
	if len(fencers) > 1 {
		return nil, ErrDurableRequest
	}
	if len(fencers) == 1 {
		fencer = fencers[0]
	} else {
		fencer, _ = proposer.(durableRequestExecutionPinFencer)
	}
	if fencer == nil {
		return nil, ErrDurableRequest
	}
	gateSessions, ok := proposer.(durableRequestRouteGateSessions)
	if !ok {
		return nil, ErrDurableRequest
	}
	return &DurableRequestLifecycleRunner{
		ledger: ledger, resolver: resolver, proposer: proposer, pinFencer: fencer,
		payloads:     &DurableRequestDynamicPayloadStore{ledger: ledger},
		gateSessions: gateSessions,
	}, nil
}

// RunWave executes the complete durable lifetime of exactly one logical
// target wave:
//
//	route intent -> route proposal -> route proof -> pending work -> work
//	settlement -> continuation -> release intent -> release proposal -> proof
//
// It first reopens all lifecycle rows, so invoking it after any ambiguous
// return resumes the exact persisted phase. Once Pending exists, neither route
// resolution nor command construction may replace its bytes.
func (runner *DurableRequestLifecycleRunner) RunWave(
	ctx context.Context,
	wave DurableRequestWave,
) (DurableRequestWaveResult, error) {
	if runner == nil || runner.ledger == nil || runner.resolver == nil ||
		runner.proposer == nil || runner.pinFencer == nil || runner.gateSessions == nil || ctx == nil {
		return DurableRequestWaveResult{}, ErrDurableRequest
	}
	keyDigest, err := validateDurableRequestWave(wave)
	if err != nil {
		return DurableRequestWaveResult{}, err
	}
	// One leader ReadIndex proof admits the complete persisted wave. Every
	// physical command additionally carries the pin digest/controller epoch and
	// is rejected locally by participant state if a takeover superseded it.
	if err = runner.fenceWaveSideEffect(ctx, wave); err != nil {
		return DurableRequestWaveResult{}, err
	}
	return runner.runAdmittedWave(ctx, wave, keyDigest)
}

// RunStagedWave admits one invocation before its payload staging writes. The
// admission is not a reusable token: every return/restart requires a new fence.
// No service authority is forwarded to staging or target proposals.
func (runner *DurableRequestLifecycleRunner) RunStagedWave(ctx context.Context, wave DurableRequestWave) (_ DurableRequestWaveResult, failure error) {
	stage := "validate"
	defer func() {
		if failure != nil {
			failure = fmt.Errorf("gateway: wave %d staging %s: %w", wave.Ordinal, stage, failure)
		}
	}()
	if runner == nil || runner.ledger == nil || runner.resolver == nil || runner.proposer == nil ||
		runner.pinFencer == nil || runner.gateSessions == nil || runner.payloads == nil || ctx == nil ||
		wave.Step != (requestledger.StepRef{}) || wave.Build != (requestledger.PayloadBuildRecord{}) ||
		len(wave.Target) == 0 || len(wave.Target) > requestledger.MaxTargetBytes ||
		len(wave.Command) == 0 || len(wave.Command) > replication.MaxCommandBytes {
		return DurableRequestWaveResult{}, ErrDurableRequest
	}
	// Validate immutable identity and bounded bytes before replicated writes.
	wave.Step.TargetLength, wave.Step.CommandLength = uint64(len(wave.Target)), uint64(len(wave.Command))
	wave.Step.TargetDigest = requestledger.Digest(sha256.Sum256(wave.Target))
	wave.Step.CommandDigest = requestledger.Digest(sha256.Sum256(wave.Command))
	keyDigest, err := validateDurableRequestWave(wave)
	if err != nil {
		return DurableRequestWaveResult{}, err
	}
	stage = "resolve"
	if _, err = runner.resolveWave(ctx, wave); err != nil {
		return DurableRequestWaveResult{}, err
	}
	stage = "execution fence"
	if err = runner.fenceWaveSideEffect(ctx, wave); err != nil {
		return DurableRequestWaveResult{}, err
	}
	stage = "head"
	headRow, err := runner.ledger.ReadRow(ctx, wave.Home, DurableRequestLifecycleRead{
		Key: wave.Key, Kind: replicatedstate.RequestLedgerReadHead, MinimumApplied: 1,
	})
	if err != nil || !headRow.Found || headRow.Kind != replicatedstate.RequestLedgerReadHead {
		return DurableRequestWaveResult{}, errors.Join(err, ErrDurableRequestConflict)
	}
	if headRow.Head.NextStepOrdinal != wave.Ordinal {
		stage = "advanced recovery"
		if wave.Ordinal == ^uint64(0) || headRow.Head.NextStepOrdinal != wave.Ordinal+1 {
			return DurableRequestWaveResult{}, ErrDurableRequestConflict
		}
		cut, openErr := runner.advancedWaveCut(ctx, wave)
		if openErr != nil || cut.route.WaveOrdinal != wave.Ordinal {
			return DurableRequestWaveResult{}, errors.Join(openErr, ErrDurableRequestConflict)
		}
		retained, openErr := runner.openAdvancedWave(ctx, wave, cut, uint64(len(wave.Target)))
		if openErr != nil || !bytes.Equal(retained.Target, wave.Target) || !bytes.Equal(retained.Command, wave.Command) {
			return DurableRequestWaveResult{}, errors.Join(openErr, ErrDurableRequestConflict)
		}
		return runner.runAdmittedWave(ctx, retained, keyDigest)
	}
	stage = "payload"
	payload, err := runner.payloads.Stage(ctx, wave.Home, wave.Key, wave.Ordinal, wave.Target, wave.Command, wave.CommandEpoch)
	if err != nil {
		return DurableRequestWaveResult{}, err
	}
	if !bytes.Equal(payload.Target, wave.Target) || !bytes.Equal(payload.Command, wave.Command) {
		return DurableRequestWaveResult{}, ErrDurableRequestConflict
	}
	wave.Build, wave.Step, wave.Target, wave.Command = payload.Build, payload.Step, payload.Target, payload.Command
	if _, err = validateDurableRequestWave(wave); err != nil {
		return DurableRequestWaveResult{}, err
	}
	stage = "admitted work"
	return runner.runAdmittedWave(ctx, wave, keyDigest)
}

func (runner *DurableRequestLifecycleRunner) runAdmittedWave(ctx context.Context, wave DurableRequestWave, keyDigest requestledger.Digest) (_ DurableRequestWaveResult, failure error) {
	stage := "open rows"
	defer func() {
		if failure != nil {
			failure = fmt.Errorf("gateway: wave %d %s: %w", wave.Ordinal, stage, failure)
		}
	}()
	head, routePin, pending, readApplied, err := runner.openWaveRows(ctx, wave, keyDigest)
	if err != nil {
		return DurableRequestWaveResult{}, err
	}
	var route ReplicatedRoute
	var beforeAdvance, afterAdvance bool
	retiredOpenRecoveryUsed := false
	for {
		if head.PinID != wave.PinID || head.RequestDigest == (requestledger.Digest{}) ||
			head.PlanRoot == (requestledger.Digest{}) {
			return DurableRequestWaveResult{}, ErrDurableRequestConflict
		}
		completed := routePin.Phase == requestledger.RoutePinReleased &&
			routePin.WaveOrdinal == wave.Ordinal && head.NextStepOrdinal == wave.Ordinal+1 &&
			head.OutstandingRoutePinDigest == (requestledger.Digest{}) && pending.Revision == 0
		if completed {
			stage = "completed session cleanup"
			if err := runner.cleanupRouteGateSession(ctx, wave, routePin); err != nil {
				return DurableRequestWaveResult{}, err
			}
			observation, readErr := runner.readWaveObservation(ctx, wave, routePin, readApplied)
			return DurableRequestWaveResult{Observation: observation, Revision: head.Revision}, readErr
		}
		beforeAdvance = head.NextStepOrdinal == wave.Ordinal
		afterAdvance = wave.Ordinal != ^uint64(0) && head.NextStepOrdinal == wave.Ordinal+1 &&
			head.OutstandingRoutePinDigest != (requestledger.Digest{})
		if !beforeAdvance && !afterAdvance {
			return DurableRequestWaveResult{}, ErrDurableRequestConflict
		}

		// A released record from the preceding wave is intentionally replaceable.
		if routePin.Phase == requestledger.RoutePinReleased &&
			routePin.WaveOrdinal+1 == wave.Ordinal && head.NextStepOrdinal == wave.Ordinal &&
			head.OutstandingRoutePinDigest == (requestledger.Digest{}) {
			stage = "prior session cleanup"
			// The released row remains the exact session-cleanup witness. Never
			// replace it until a replayable retirement/release has settled.
			if err := runner.cleanupRouteGateSession(ctx, wave, routePin); err != nil {
				return DurableRequestWaveResult{}, err
			}
			routePin = requestledger.RoutePinRecord{}
		}
		if routePin.Phase != requestledger.RoutePinInvalid && routePin.WaveOrdinal != wave.Ordinal {
			return DurableRequestWaveResult{}, ErrDurableRequestConflict
		}

		if routePin.Phase == requestledger.RoutePinInvalid {
			stage = "acquire intent"
			route, err = runner.resolveWave(ctx, wave)
			if err != nil {
				return DurableRequestWaveResult{}, err
			}
			priorHead := head
			acquire, physical, buildErr := runner.gateSessions.prepareAcquire(ctx, route, wave, head)
			if buildErr != nil {
				retired, isRetiredOpen := buildErr.(*durableRequestRetiredOpenError)
				if retiredOpenRecoveryUsed || !isRetiredOpen {
					return DurableRequestWaveResult{}, buildErr
				}
				retiredOpenRecoveryUsed = true
				refreshed, refreshErr := runner.refreshRetiredOpenCut(
					ctx, wave, keyDigest, priorHead, route, retired,
				)
				if refreshErr != nil {
					return DurableRequestWaveResult{}, errors.Join(buildErr, refreshErr)
				}
				head, routePin, pending, readApplied = refreshed.head, refreshed.route, refreshed.pending, refreshed.applied
				continue
			}
			routePin, err = requestledger.NewRoutePinAcquiring(
				head, wave.PinID, wave.Binding, physical, acquire,
			)
			if err != nil {
				return DurableRequestWaveResult{}, errors.Join(err, ErrDurableRequestConflict)
			}
			head, err = runner.applyRoutePin(ctx, wave, head, requestledger.RoutePinRecord{}, routePin,
				requestledger.OperationBeginRoutePinAcquire)
			if err != nil {
				return DurableRequestWaveResult{}, err
			}
		}
		break
	}

	if routePin.Phase == requestledger.RoutePinAcquiring {
		stage = "acquire proposal and proof"
		route, err = runner.resolvePersistedRoute(ctx, wave, routePin.Command)
		if err != nil {
			return DurableRequestWaveResult{}, err
		}
		settled, proposeErr := runner.proposer.Propose(ctx, route, routePin.Command)
		if proposeErr != nil {
			return DurableRequestWaveResult{}, proposeErr
		}
		if !validDurableRequestSettlement(routePin.Command, settled) {
			return DurableRequestWaveResult{}, ErrDurableRequestConflict
		}
		next, recordErr := requestledger.RecordVerifiedRoutePinAcquired(
			routePin, routePin.Revision+1, settled.Completion,
		)
		if recordErr != nil {
			return DurableRequestWaveResult{}, errors.Join(recordErr, ErrDurableRequestConflict)
		}
		acquiredHead, transitionErr := requestledger.AdvanceHeadRoutePin(
			head, routePin, next, head.Revision+1,
		)
		if transitionErr != nil {
			return DurableRequestWaveResult{}, errors.Join(transitionErr, ErrDurableRequestConflict)
		}
		nextPending, pendingErr := requestledger.NewPendingWaveWithRoutePin(
			acquiredHead, wave.Build, acquiredHead.Revision+1, next,
			[]requestledger.StepRef{wave.Step},
		)
		if pendingErr != nil {
			return DurableRequestWaveResult{}, errors.Join(pendingErr, ErrDurableRequestConflict)
		}
		result, applyErr := runner.ledger.ApplyCAS(ctx, wave.Home, wave.Key,
			DurableRequestLifecycleCAS{
				Operation:        requestledger.OperationRecordRoutePinAcquiredPutPending,
				ExpectedRevision: head.Revision, Revision: nextPending.Revision,
				RoutePin: next, Pending: nextPending,
			})
		if applyErr != nil {
			return DurableRequestWaveResult{}, applyErr
		}
		if result.Ledger.ResultCode != replicatedstate.ResultApplied {
			return DurableRequestWaveResult{}, ErrDurableRequestConflict
		}
		head, err = requestledger.InstallPendingWave(
			acquiredHead, nextPending, wave.Build, next,
		)
		if err != nil {
			return DurableRequestWaveResult{}, errors.Join(err, ErrDurableRequestConflict)
		}
		routePin = next
		pending = nextPending
	}

	var observation []byte
	if afterAdvance {
		stage = "retained observation"
		observation, err = runner.readWaveObservation(ctx, wave, routePin, readApplied)
		if err != nil {
			return DurableRequestWaveResult{}, err
		}
	}
	if pending.Revision == 0 &&
		routePin.Phase == requestledger.RoutePinAcquired &&
		head.OutstandingRoutePinDigest == (requestledger.Digest{}) {
		stage = "pending work"
		pending, err = requestledger.NewPendingWaveWithRoutePin(
			head, wave.Build, head.Revision+1, routePin,
			[]requestledger.StepRef{wave.Step},
		)
		if err != nil {
			return DurableRequestWaveResult{}, errors.Join(err, ErrDurableRequestConflict)
		}
		result, applyErr := runner.ledger.ApplyCAS(ctx, wave.Home, wave.Key,
			DurableRequestLifecycleCAS{
				Operation:        requestledger.OperationPutPending,
				ExpectedRevision: head.Revision, Revision: pending.Revision,
				Pending: pending,
			})
		if applyErr != nil {
			return DurableRequestWaveResult{}, applyErr
		}
		if result.Ledger.ResultCode != replicatedstate.ResultApplied {
			return DurableRequestWaveResult{}, ErrDurableRequestConflict
		}
		head, err = requestledger.InstallPendingWave(head, pending, wave.Build, routePin)
		if err != nil {
			return DurableRequestWaveResult{}, errors.Join(err, ErrDurableRequestConflict)
		}
	}

	if pending.Revision != 0 {
		stage = "participant work, continuation, and release intent"
		route, err = runner.resolvePersistedRoute(ctx, wave, wave.Command)
		if err != nil {
			return DurableRequestWaveResult{}, err
		}
		settled, proposeErr := runner.proposer.Propose(ctx, route, wave.Command)
		if proposeErr != nil {
			return DurableRequestWaveResult{}, proposeErr
		}
		if !validDurableRequestSettlement(wave.Command, settled) {
			return DurableRequestWaveResult{}, ErrDurableRequestConflict
		}
		observation = bytes.Clone(settled.Completion)
		transition, cursor := wave.Transition, wave.Cursor
		if wave.Settle != nil {
			transition, cursor, err = wave.Settle(observation)
			if err != nil || transition == 0 || len(cursor) == 0 ||
				len(cursor) > requestledger.MaxContinuationCursorBytes {
				return DurableRequestWaveResult{}, errors.Join(err, ErrDurableRequestConflict)
			}
		}
		continuation, continuationErr := requestledger.NewContinuation(
			head, pending, routePin, head.Revision+1, transition,
			cursor, observation,
		)
		if continuationErr != nil {
			return DurableRequestWaveResult{}, errors.Join(continuationErr, ErrDurableRequestConflict)
		}
		route, err = runner.resolvePersistedRoute(ctx, wave, routePin.Command)
		if err != nil {
			return DurableRequestWaveResult{}, err
		}
		release, buildErr := runner.gateSessions.prepareRelease(ctx, route, wave, routePin)
		if buildErr != nil {
			return DurableRequestWaveResult{}, buildErr
		}
		releasing, beginErr := requestledger.BeginRoutePinRelease(
			routePin, routePin.Revision+1, release,
		)
		if beginErr != nil {
			return DurableRequestWaveResult{}, errors.Join(beginErr, ErrDurableRequestConflict)
		}
		result, applyErr := runner.ledger.ApplyCAS(ctx, wave.Home, wave.Key,
			DurableRequestLifecycleCAS{
				Operation:        requestledger.OperationAdvanceBeginRoutePinRelease,
				ExpectedRevision: head.Revision, Revision: head.Revision + 2,
				Continuation: continuation, RoutePin: releasing,
			})
		if applyErr != nil {
			return DurableRequestWaveResult{}, applyErr
		}
		if result.Ledger.ResultCode != replicatedstate.ResultApplied {
			return DurableRequestWaveResult{}, ErrDurableRequestConflict
		}
		if wave.Build != (requestledger.PayloadBuildRecord{}) {
			head, err = requestledger.AdvancePendingWithBuild(head, pending, continuation, wave.Build)
		} else {
			head, err = requestledger.AdvancePending(head, pending, continuation)
		}
		if err != nil {
			return DurableRequestWaveResult{}, errors.Join(err, ErrDurableRequestConflict)
		}
		head, err = requestledger.AdvanceHeadRoutePin(
			head, routePin, releasing, head.Revision+1,
		)
		if err != nil {
			return DurableRequestWaveResult{}, errors.Join(err, ErrDurableRequestConflict)
		}
		routePin = releasing
		pending = requestledger.PendingWaveRecord{}
	}

	if routePin.Phase == requestledger.RoutePinAcquired &&
		head.OutstandingRoutePinDigest == routePin.AcquiredEvidenceDigest {
		stage = "release intent"
		route, err = runner.resolvePersistedRoute(ctx, wave, routePin.Command)
		if err != nil {
			return DurableRequestWaveResult{}, err
		}
		release, buildErr := runner.gateSessions.prepareRelease(ctx, route, wave, routePin)
		if buildErr != nil {
			return DurableRequestWaveResult{}, buildErr
		}
		next, beginErr := requestledger.BeginRoutePinRelease(
			routePin, routePin.Revision+1, release,
		)
		if beginErr != nil {
			return DurableRequestWaveResult{}, errors.Join(beginErr, ErrDurableRequestConflict)
		}
		head, err = runner.applyRoutePin(ctx, wave, head, routePin, next,
			requestledger.OperationBeginRoutePinRelease)
		if err != nil {
			return DurableRequestWaveResult{}, err
		}
		routePin = next
	}

	if routePin.Phase == requestledger.RoutePinReleasing {
		stage = "release proposal and proof"
		route, err = runner.resolvePersistedRoute(ctx, wave, routePin.Command)
		if err != nil {
			return DurableRequestWaveResult{}, err
		}
		settled, proposeErr := runner.proposer.Propose(ctx, route, routePin.Command)
		if proposeErr != nil {
			return DurableRequestWaveResult{}, proposeErr
		}
		if !validDurableRequestSettlement(routePin.Command, settled) {
			return DurableRequestWaveResult{}, ErrDurableRequestConflict
		}
		next, recordErr := requestledger.RecordVerifiedRoutePinReleased(
			routePin, routePin.Revision+1, settled.Completion,
		)
		if recordErr != nil {
			return DurableRequestWaveResult{}, errors.Join(recordErr, ErrDurableRequestConflict)
		}
		head, err = runner.applyRoutePin(ctx, wave, head, routePin, next,
			requestledger.OperationRecordRoutePinReleased)
		if err != nil {
			return DurableRequestWaveResult{}, err
		}
		routePin = next
	}

	if routePin.Phase != requestledger.RoutePinReleased ||
		head.OutstandingRoutePinDigest != (requestledger.Digest{}) || pending.Revision != 0 {
		return DurableRequestWaveResult{}, ErrDurableRequestUnresolved
	}
	stage = "session cleanup"
	if err := runner.cleanupRouteGateSession(ctx, wave, routePin); err != nil {
		return DurableRequestWaveResult{}, err
	}
	return DurableRequestWaveResult{
		Observation: observation, Revision: head.Revision,
	}, nil
}

func (runner *DurableRequestLifecycleRunner) openWaveRows(
	ctx context.Context,
	wave DurableRequestWave,
	keyDigest requestledger.Digest,
) (requestledger.HeadRecord, requestledger.RoutePinRecord, requestledger.PendingWaveRecord, uint64, error) {
	if reader, ok := runner.ledger.(durableRequestWaveCutReader); ok {
		var scratch [requestledger.MaxPendingWaveSteps]requestledger.StepRef
		cut, err := reader.ReadWaveCut(ctx, wave.Home, wave.Key, scratch[:])
		if err != nil || cut.Head.KeyDigest != keyDigest ||
			(cut.Route.Revision != 0 && cut.Route.KeyDigest != keyDigest) ||
			(cut.Pending.Revision != 0 && (cut.Pending.KeyDigest != keyDigest ||
				len(cut.Pending.Steps) != 1 || cut.Pending.Steps[0] != wave.Step)) {
			return requestledger.HeadRecord{}, requestledger.RoutePinRecord{},
				requestledger.PendingWaveRecord{}, 0, errors.Join(err, ErrDurableRequestConflict)
		}
		if cut.Pending.Revision != 0 {
			cut.Pending.Steps = append([]requestledger.StepRef(nil), cut.Pending.Steps...)
		}
		return cut.Head, cut.Route, cut.Pending, cut.Applied, nil
	}
	headRow, err := runner.ledger.ReadRow(ctx, wave.Home, DurableRequestLifecycleRead{
		Key: wave.Key, Kind: replicatedstate.RequestLedgerReadHead, MinimumApplied: 1,
	})
	if err != nil || !headRow.Found || headRow.Kind != replicatedstate.RequestLedgerReadHead ||
		headRow.Head.KeyDigest != keyDigest {
		return requestledger.HeadRecord{}, requestledger.RoutePinRecord{}, requestledger.PendingWaveRecord{}, 0,
			errors.Join(err, ErrDurableRequestConflict)
	}
	routeRow, err := runner.ledger.ReadRow(ctx, wave.Home, DurableRequestLifecycleRead{
		Key: wave.Key, Kind: replicatedstate.RequestLedgerReadRoutePin,
		MinimumApplied: headRow.Applied,
	})
	if err != nil {
		return requestledger.HeadRecord{}, requestledger.RoutePinRecord{}, requestledger.PendingWaveRecord{}, 0, err
	}
	var route requestledger.RoutePinRecord
	if routeRow.Found {
		if routeRow.Kind != replicatedstate.RequestLedgerReadRoutePin {
			return requestledger.HeadRecord{}, requestledger.RoutePinRecord{}, requestledger.PendingWaveRecord{}, 0, ErrDurableRequestConflict
		}
		route = routeRow.RoutePin
	}
	var scratch [requestledger.MaxPendingWaveSteps]requestledger.StepRef
	pendingRow, err := runner.ledger.ReadRow(ctx, wave.Home, DurableRequestLifecycleRead{
		Key: wave.Key, Kind: replicatedstate.RequestLedgerReadPending,
		MinimumApplied: headRow.Applied, PendingSteps: scratch[:],
	})
	if err != nil {
		return requestledger.HeadRecord{}, requestledger.RoutePinRecord{}, requestledger.PendingWaveRecord{}, 0, err
	}
	var pending requestledger.PendingWaveRecord
	if pendingRow.Found {
		if pendingRow.Kind != replicatedstate.RequestLedgerReadPending || len(pendingRow.Pending.Steps) != 1 ||
			pendingRow.Pending.Steps[0] != wave.Step {
			return requestledger.HeadRecord{}, requestledger.RoutePinRecord{}, requestledger.PendingWaveRecord{}, 0, ErrDurableRequestConflict
		}
		pending = pendingRow.Pending
		pending.Steps = append([]requestledger.StepRef(nil), pending.Steps...)
	}
	return headRow.Head, route, pending, headRow.Applied, nil
}

// durableRequestRetiredOpenCut is the one coherent lifecycle read permitted
// after a deterministic SessionOpen retires before its acquire intent is
// stored. The caller must dispatch the returned phase through the ordinary
// state machine; it must never call Open again.
type durableRequestRetiredOpenCut struct {
	head    requestledger.HeadRecord
	route   requestledger.RoutePinRecord
	pending requestledger.PendingWaveRecord
	applied uint64
}

func (runner *DurableRequestLifecycleRunner) refreshRetiredOpenCut(
	ctx context.Context,
	wave DurableRequestWave,
	keyDigest requestledger.Digest,
	priorHead requestledger.HeadRecord,
	route ReplicatedRoute,
	retired *durableRequestRetiredOpenError,
) (durableRequestRetiredOpenCut, error) {
	if ctx == nil || retired == nil || len(retired.openCommand) == 0 {
		return durableRequestRetiredOpenCut{}, ErrDurableRequestConflict
	}
	if err := ctx.Err(); err != nil {
		return durableRequestRetiredOpenCut{}, errors.Join(err, ErrDurableRequestConflict)
	}
	// The compatibility ReadRow path cannot establish the atomic evidence
	// needed here. A retired Open is recoverable only through the production
	// coherent wave-cut reader.
	if _, ok := runner.ledger.(durableRequestWaveCutReader); !ok {
		return durableRequestRetiredOpenCut{}, ErrDurableRequestConflict
	}
	head, routePin, pending, applied, err := runner.openWaveRows(ctx, wave, keyDigest)
	if err != nil || ctx.Err() != nil {
		return durableRequestRetiredOpenCut{}, errors.Join(err, ctx.Err(), ErrDurableRequestConflict)
	}
	cut := durableRequestRetiredOpenCut{head: head, route: routePin, pending: pending, applied: applied}
	if head.Revision <= priorHead.Revision {
		return durableRequestRetiredOpenCut{}, ErrDurableRequestConflict
	}
	if err := validateRetiredOpenCut(wave, priorHead, route, cut, retired); err != nil {
		return durableRequestRetiredOpenCut{}, err
	}
	return cut, nil
}

func validateRetiredOpenCut(
	wave DurableRequestWave,
	priorHead requestledger.HeadRecord,
	route ReplicatedRoute,
	cut durableRequestRetiredOpenCut,
	retired *durableRequestRetiredOpenError,
) error {
	if retired == nil || len(retired.openCommand) == 0 ||
		priorHead.NextStepOrdinal != wave.Ordinal || cut.head.Revision <= priorHead.Revision ||
		priorHead.Phase != requestledger.PhaseSealed || cut.head.Phase != requestledger.PhaseSealed ||
		cut.head.Key != wave.Key || cut.head.KeyDigest != priorHead.KeyDigest ||
		cut.head.RequestDigest != priorHead.RequestDigest || cut.head.PlanRoot != priorHead.PlanRoot ||
		cut.head.PinID != wave.PinID || cut.route.Revision == 0 || cut.route.WaveOrdinal != wave.Ordinal ||
		cut.route.KeyDigest != cut.head.KeyDigest || cut.route.RequestDigest != cut.head.RequestDigest ||
		cut.route.PlanRoot != cut.head.PlanRoot || cut.route.PriorContinuationDigest != priorHead.ContinuationDigest ||
		cut.route.PinID != wave.PinID || cut.route.BindingDigest != wave.Binding {
		return ErrDurableRequestConflict
	}
	if _, err := requestledger.AppendHead(nil, cut.head); err != nil {
		return ErrDurableRequestConflict
	}
	if _, err := requestledger.AppendRoutePin(nil, cut.route); err != nil {
		return ErrDurableRequestConflict
	}
	if cut.pending.Revision != 0 {
		if cut.route.Phase != requestledger.RoutePinAcquired ||
			cut.pending.WaveOrdinal != wave.Ordinal || cut.pending.KeyDigest != cut.head.KeyDigest ||
			cut.pending.RequestDigest != cut.head.RequestDigest || cut.pending.PlanRoot != cut.head.PlanRoot ||
			cut.pending.PriorContinuationDigest != priorHead.ContinuationDigest ||
			cut.pending.RoutePinDigest != cut.route.AcquiredEvidenceDigest ||
			cut.pending.ForwardingWitnessDigest != cut.route.PhysicalWitnessDigest ||
			cut.pending.PayloadBuildDigest != wave.Build.BuildDigest ||
			len(cut.pending.Steps) != 1 || cut.pending.Steps[0] != wave.Step {
			return ErrDurableRequestConflict
		}
		if _, err := requestledger.AppendPendingWave(nil, cut.pending); err != nil {
			return ErrDurableRequestConflict
		}
	}

	open, err := replication.OpenCommand(retired.openCommand)
	if err != nil || open.Kind() != replication.CommandSessionOpen ||
		open.AuthorityClass != replication.CommandAuthorityMembershipStableRouteSession ||
		open.ClientEpoch != 0 || open.ClientSequence != 1 || open.AckThrough != 0 ||
		open.NextDeadlineUnixNano != math.MaxInt64 || open.ExpectedDeadlineUnixNano != 0 ||
		!bytes.Equal(open.Tenant, wave.Tenant) || open.RetryHome != wave.Identity.RetryHome ||
		!commandViewMatchesRoute(open, route) {
		return ErrDurableRequestConflict
	}
	identity, err := requestledger.DeriveRouteGateIdentity(
		cut.head.KeyDigest, cut.head.RequestDigest, cut.head.PlanRoot,
		cut.route.PriorContinuationDigest, cut.route.PinID, cut.route.WaveOrdinal,
	)
	if err != nil {
		return ErrDurableRequestConflict
	}
	expectedClientID, err := durableRouteSessionIdentity(identity, route, wave.Tenant)
	if err != nil || open.ClientID != expectedClientID {
		return ErrDurableRequestConflict
	}
	retained, err := replication.OpenCommand(cut.route.Command)
	if err != nil || retained.Kind() != replication.CommandRouteGate ||
		retained.AuthorityClass != open.AuthorityClass ||
		!sameDurableRequestSessionEnvelope(open, retained) ||
		!commandViewMatchesRoute(retained, route) || retained.ClientEpoch == 0 ||
		retained.ClientID != open.ClientID || retained.RetryHome != wave.Identity.RetryHome ||
		!bytes.Equal(retained.Tenant, wave.Tenant) {
		return ErrDurableRequestConflict
	}
	gate, err := retained.OpenRouteGate()
	if err != nil || gate.Identity != routegate.Identity(identity) || gate.Epoch == 0 {
		return ErrDurableRequestConflict
	}
	physical, ok := replication.RouteGatePhysicalWitness(retained)
	if !ok || requestledger.Digest(physical) != cut.route.PhysicalWitnessDigest {
		return ErrDurableRequestConflict
	}
	binding, err := requestledger.DeriveRouteGateBinding(
		identity, wave.Binding, requestledger.Digest(physical), gate.Epoch,
	)
	if err != nil || gate.Binding != routegate.Binding(binding) {
		return ErrDurableRequestConflict
	}
	wantOperation, wantSequence, wantAck := routegate.OperationAcquireShared, uint64(2), uint64(1)
	if cut.route.Phase == requestledger.RoutePinReleasing || cut.route.Phase == requestledger.RoutePinReleased {
		wantOperation, wantSequence, wantAck = routegate.OperationReleaseShared, 3, 2
	}
	if gate.Operation != wantOperation || retained.ClientSequence != wantSequence || retained.AckThrough != wantAck {
		return ErrDurableRequestConflict
	}
	if cut.route.Phase == requestledger.RoutePinAcquiring || cut.route.Phase == requestledger.RoutePinAcquired {
		if cut.head.NextStepOrdinal != wave.Ordinal || cut.head.OutstandingRoutePinDigest != (requestledger.Digest{}) {
			return ErrDurableRequestConflict
		}
	} else if cut.route.Phase == requestledger.RoutePinReleasing {
		if cut.head.NextStepOrdinal != wave.Ordinal+1 ||
			cut.head.OutstandingRoutePinDigest != cut.route.AcquiredEvidenceDigest || cut.pending.Revision != 0 {
			return ErrDurableRequestConflict
		}
	} else if cut.route.Phase == requestledger.RoutePinReleased {
		if cut.head.NextStepOrdinal != wave.Ordinal+1 ||
			cut.head.OutstandingRoutePinDigest != (requestledger.Digest{}) || cut.pending.Revision != 0 {
			return ErrDurableRequestConflict
		}
	} else {
		return ErrDurableRequestConflict
	}
	if cut.route.Phase == requestledger.RoutePinAcquiring && cut.pending.Revision != 0 {
		return ErrDurableRequestConflict
	}
	if (cut.route.Phase == requestledger.RoutePinReleasing || cut.route.Phase == requestledger.RoutePinReleased) &&
		wave.Ordinal == ^uint64(0) {
		return ErrDurableRequestConflict
	}
	if (cut.route.Phase == requestledger.RoutePinAcquiring || cut.route.Phase == requestledger.RoutePinAcquired) &&
		cut.head.ContinuationDigest != priorHead.ContinuationDigest {
		return ErrDurableRequestConflict
	}
	if cut.route.Phase == requestledger.RoutePinAcquired || cut.route.Phase == requestledger.RoutePinReleased {
		var result ReplicatedResult
		result.Completion = cut.route.Completion
		completion, completionErr := replication.OpenCompletion(cut.route.Completion)
		if completionErr != nil {
			return ErrDurableRequestConflict
		}
		result.Outcome.AppliedIndex = completion.AppliedSequence
		if !validDurableRequestSettlement(cut.route.Command, result) {
			return ErrDurableRequestConflict
		}
	}
	return nil
}

func sameDurableRequestSessionEnvelope(open, retained replication.CommandView) bool {
	return open.ClusterID == retained.ClusterID &&
		open.ClusterIncarnation == retained.ClusterIncarnation &&
		open.TopologyRecoveryEpoch == retained.TopologyRecoveryEpoch &&
		bytes.Equal(open.Distribution, retained.Distribution) && bytes.Equal(open.Shard, retained.Shard) &&
		open.AllocationGeneration == retained.AllocationGeneration && open.ShardIncarnation == retained.ShardIncarnation &&
		open.GroupID == retained.GroupID && open.ReplicaSetVersion == retained.ReplicaSetVersion &&
		open.ActivePolicyGeneration == retained.ActivePolicyGeneration && open.ProtectionEpoch == retained.ProtectionEpoch &&
		open.OwnershipEpoch == retained.OwnershipEpoch && open.SchemaGeneration == retained.SchemaGeneration &&
		open.RoutingVersion == retained.RoutingVersion && open.RouteGeneration == retained.RouteGeneration &&
		bytes.Equal(open.Tenant, retained.Tenant) && open.ClientID == retained.ClientID &&
		open.RetryHome == retained.RetryHome
}

func (runner *DurableRequestLifecycleRunner) readWaveObservation(
	ctx context.Context,
	wave DurableRequestWave,
	route requestledger.RoutePinRecord,
	minimumApplied uint64,
) ([]byte, error) {
	row, err := runner.ledger.ReadRow(ctx, wave.Home, DurableRequestLifecycleRead{
		Key: wave.Key, Kind: replicatedstate.RequestLedgerReadContinuation,
		MinimumApplied: minimumApplied,
	})
	if err != nil || !row.Found || row.Kind != replicatedstate.RequestLedgerReadContinuation ||
		row.Continuation.SettledOrdinal != wave.Ordinal ||
		row.Continuation.RoutePinDigest != route.AcquiredEvidenceDigest {
		return nil, errors.Join(err, ErrDurableRequestConflict)
	}
	return bytes.Clone(row.Continuation.Observation), nil
}

func (runner *DurableRequestLifecycleRunner) applyRoutePin(
	ctx context.Context,
	wave DurableRequestWave,
	head requestledger.HeadRecord,
	prior requestledger.RoutePinRecord,
	next requestledger.RoutePinRecord,
	operation requestledger.Operation,
) (requestledger.HeadRecord, error) {
	result, err := runner.ledger.ApplyCAS(ctx, wave.Home, wave.Key,
		DurableRequestLifecycleCAS{
			Operation: operation, ExpectedRevision: head.Revision,
			Revision: head.Revision + 1, RoutePin: next,
		})
	if err != nil {
		return requestledger.HeadRecord{}, err
	}
	if result.Ledger.ResultCode != replicatedstate.ResultApplied {
		return requestledger.HeadRecord{}, fmt.Errorf("gateway: ledger route transition %d result %d (expected revision %d, observed %d, phase %d): %w", operation, result.Ledger.ResultCode, head.Revision, result.Ledger.Revision, result.Ledger.Phase, ErrDurableRequestConflict)
	}
	if operation == requestledger.OperationRecordRoutePinReleased {
		return requestledger.MarkRoutePinReleased(head, next, head.Revision+1)
	}
	return requestledger.AdvanceHeadRoutePin(head, prior, next, head.Revision+1)
}

func (runner *DurableRequestLifecycleRunner) resolveWave(
	ctx context.Context,
	wave DurableRequestWave,
) (ReplicatedRoute, error) {
	route, err := runner.resolver.ResolveDurableRequestTarget(ctx, wave.LogicalTarget)
	if err != nil {
		return ReplicatedRoute{}, err
	}
	if !durableRequestRouteMatchesTarget(route, wave.LogicalTarget) ||
		!commandMatchesRoute(wave.Command, route) {
		return ReplicatedRoute{}, ErrDurableRequestConflict
	}
	return route, nil
}

func (runner *DurableRequestLifecycleRunner) resolvePersistedRoute(
	ctx context.Context,
	wave DurableRequestWave,
	exact []byte,
) (ReplicatedRoute, error) {
	route, err := runner.resolver.ResolveDurableRequestTarget(ctx, wave.LogicalTarget)
	if err != nil {
		return ReplicatedRoute{}, err
	}
	if !durableRequestRouteMatchesTarget(route, wave.LogicalTarget) ||
		!commandMatchesRoute(exact, route) {
		return ReplicatedRoute{}, ErrDurableRequestConflict
	}
	return route, nil
}

func durableRequestRouteMatchesTarget(
	route ReplicatedRoute,
	target DurableRequestLogicalTarget,
) bool {
	return validReplicatedRoute(route) && route.Distribution == target.Distribution &&
		route.Shard == target.Shard && route.Group == target.Group &&
		route.RangeIdentity == target.RangeIdentity &&
		route.LineageDigest == target.LineageDigest &&
		route.ForwardingRuleDigest == target.ForwardingRuleDigest &&
		route.Command.SchemaGeneration == target.SchemaGeneration &&
		route.Command.RelationManifestDigest == target.RelationManifestDigest
}

func (runner *DurableRequestLifecycleRunner) fenceWaveSideEffect(
	ctx context.Context,
	wave DurableRequestWave,
) error {
	// The hidden pin belongs to the gateway service, not the data principal.
	// Scope delegation to this read: participant proposals retain the original
	// caller context, and no authority is derived from the supplied lease.
	if runner.pinAuthority.Valid() {
		var err error
		ctx, err = serviceauthz.WithAuthority(ctx, runner.pinAuthority)
		if err != nil {
			return err
		}
	}
	_, err := runner.pinFencer.ValidateExecutionPinFence(
		ctx, wave.ExecutionPinRoute, wave.ExecutionPinLease, wave.ExecutionPinLease.Applied,
	)
	if err != nil {
		return errors.Join(err, ErrDurableRequestConflict)
	}
	return nil
}

func validateDurableRequestWave(wave DurableRequestWave) (requestledger.Digest, error) {
	if !wave.Key.Valid() || wave.Home.Identity == (replication.Digest{}) ||
		wave.Identity.ID == ([16]byte{}) ||
		wave.Identity.RetryHome == (replication.RetryHome{}) || len(wave.Tenant) == 0 ||
		len(wave.Tenant) > replication.MaxIdentityBytes || wave.PinID == (requestledger.PinID{}) ||
		wave.GateEpoch == 0 || wave.CommandEpoch == 0 || wave.CommandEpoch > wave.ExecutionPinLease.ControllerEpoch || wave.Binding == (requestledger.Digest{}) ||
		!validReplicatedRoute(wave.ExecutionPinRoute) || !wave.ExecutionPinLease.Valid() ||
		(wave.Settle == nil && (wave.Transition == 0 || len(wave.Cursor) == 0)) ||
		(wave.Settle != nil && (wave.Transition != 0 || len(wave.Cursor) != 0)) ||
		len(wave.Target) == 0 || len(wave.Command) == 0 ||
		len(wave.Command) > replication.MaxCommandBytes ||
		uint64(len(wave.Target)) != wave.Step.TargetLength ||
		uint64(len(wave.Command)) != wave.Step.CommandLength ||
		requestledger.Digest(sha256.Sum256(wave.Target)) != wave.Step.TargetDigest ||
		requestledger.Digest(sha256.Sum256(wave.Command)) != wave.Step.CommandDigest {
		return requestledger.Digest{}, ErrDurableRequest
	}
	keyDigest, err := requestledger.KeyDigest(wave.Key)
	if err != nil {
		return requestledger.Digest{}, errors.Join(err, ErrDurableRequest)
	}
	home, err := requestledger.Home(wave.Key)
	if err != nil || home != wave.Home.Point {
		return requestledger.Digest{}, errors.Join(err, ErrDurableRequestConflict)
	}
	return keyDigest, nil
}

func appendDurableRequestRouteGateCommand(
	dst []byte,
	route ReplicatedRoute,
	wave DurableRequestWave,
	keyDigest requestledger.Digest,
	requestDigest requestledger.Digest,
	planRoot requestledger.Digest,
	priorContinuation requestledger.Digest,
	waveOrdinal uint64,
	operation routegate.Operation,
	session *NativeSession,
) ([]byte, requestledger.Digest, error) {
	identity, err := requestledger.DeriveRouteGateIdentity(
		keyDigest, requestDigest, planRoot, priorContinuation,
		wave.PinID, waveOrdinal,
	)
	if err != nil {
		return dst, requestledger.Digest{}, errors.Join(err, ErrDurableRequestConflict)
	}
	gate := routegate.Command{
		Operation: operation, Epoch: wave.GateEpoch,
		Identity: routegate.Identity(identity), Binding: routegate.Binding{1},
	}
	gateBytes, err := routegate.AppendCommand(nil, gate)
	if err != nil {
		return dst, requestledger.Digest{}, err
	}
	outer := session.commandHeader(replication.CommandRouteGate, session.epoch, session.nextSequence, session.ackThrough)
	outer.Kind, outer.RouteGate = replication.CommandRouteGate, gateBytes
	outer.Fingerprint = nativeCommandFingerprint(outer)
	provisional, err := replication.AppendCommand(nil, outer)
	if err != nil {
		return dst, requestledger.Digest{}, err
	}
	view, err := replication.OpenCommand(provisional)
	if err != nil {
		return dst, requestledger.Digest{}, err
	}
	physical, ok := replication.RouteGatePhysicalWitness(view)
	if !ok {
		return dst, requestledger.Digest{}, ErrDurableRequestConflict
	}
	binding, err := requestledger.DeriveRouteGateBinding(
		identity, wave.Binding, requestledger.Digest(physical), wave.GateEpoch,
	)
	if err != nil {
		return dst, requestledger.Digest{}, errors.Join(err, ErrDurableRequestConflict)
	}
	gate.Binding = routegate.Binding(binding)
	outer.RouteGate, err = routegate.AppendCommand(gateBytes[:0], gate)
	if err != nil {
		return dst, requestledger.Digest{}, err
	}
	outer.Fingerprint = nativeCommandFingerprint(outer)
	dst, err = replication.AppendCommand(dst, outer)
	if err != nil {
		return dst, requestledger.Digest{}, err
	}
	return dst, requestledger.Digest(physical), nil
}

func validDurableRequestSettlement(command []byte, result ReplicatedResult) bool {
	view, err := replication.OpenCommand(command)
	if err != nil || result.Outcome.AppliedIndex == 0 || len(result.Completion) == 0 {
		return false
	}
	completion, err := replication.OpenCompletion(result.Completion)
	if err != nil {
		return false
	}
	if view.Kind() != replication.CommandRouteGate {
		return nativeCompletionMatches(view, completion)
	}
	if completion.Storage != replication.CompletionInline ||
		completion.ResultCode != replicatedstate.ResultRouteGate ||
		completion.ResultFormat != replicatedstate.ResultFormatRouteGate ||
		completion.ResultLength != routegate.OutcomeBytes ||
		len(completion.InlineResult) != routegate.OutcomeBytes ||
		completion.AppliedSequence == 0 ||
		completion.ClusterID != view.ClusterID ||
		completion.ClusterIncarnation != view.ClusterIncarnation ||
		completion.TopologyRecoveryEpoch != view.TopologyRecoveryEpoch ||
		!bytes.Equal(completion.Distribution, view.Distribution) ||
		!bytes.Equal(completion.Shard, view.Shard) ||
		completion.AllocationGeneration != view.AllocationGeneration ||
		completion.ShardIncarnation != view.ShardIncarnation ||
		completion.GroupID != view.GroupID ||
		completion.ReplicaSetVersion != view.ReplicaSetVersion ||
		completion.ActivePolicyGeneration != view.ActivePolicyGeneration ||
		completion.ProtectionEpoch != view.ProtectionEpoch ||
		completion.RoutingVersion != view.RoutingVersion ||
		completion.RouteGeneration != view.RouteGeneration ||
		!bytes.Equal(completion.Tenant, view.Tenant) ||
		completion.ClientID != view.ClientID || completion.ClientEpoch != view.ClientEpoch ||
		completion.ClientSequence != view.ClientSequence ||
		completion.Fingerprint != view.Fingerprint || completion.RetryHome != view.RetryHome {
		return false
	}
	gate, gateErr := view.OpenRouteGate()
	outcome, outcomeErr := routegate.OpenOutcome(completion.InlineResult)
	if gateErr != nil || outcomeErr != nil {
		return false
	}
	switch gate.Operation {
	case routegate.OperationAcquireShared:
		return (outcome.Reason == routegate.ReasonAcquired ||
			outcome.Reason == routegate.ReasonIdempotent) && outcome.Status.ActivePins != 0
	case routegate.OperationReleaseShared:
		return (outcome.Reason == routegate.ReasonReleased ||
			outcome.Reason == routegate.ReasonAlreadyReleased) && outcome.Status.ReleasedPins != 0
	default:
		return false
	}
}
