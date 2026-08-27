package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

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
	Participant       DurableRequestLogicalParticipant
	Identity          ReplicatedTransactionIdentity
	Tenant            []byte
	PinID             requestledger.PinID
	GateEpoch         uint64
	Binding           requestledger.Digest
	ExecutionPinRoute ReplicatedRoute
	ExecutionPinLease executionpin.LeaseCertificate
	Build             requestledger.PayloadBuildRecord
	Step              requestledger.StepRef
	Ordinal           uint64
	Target            []byte
	Command           []byte
	Transition        uint32
	Cursor            []byte
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

// DurableRequestLifecycleRunner drives one participant at a time. Width is
// therefore bounded by bytes and the persisted uint64 wave ordinal, not by an
// aggregate participant slice or a policy participant cap.
type DurableRequestLifecycleRunner struct {
	ledger       DurableRequestLedger
	resolver     DurableRequestRouteResolver
	proposer     durableRequestWaveProposer
	pinFencer    durableRequestExecutionPinFencer
	pinAuthority serviceauthz.Authority
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
	return &DurableRequestLifecycleRunner{
		ledger: ledger, resolver: resolver, proposer: proposer, pinFencer: fencer,
	}, nil
}

// RunWave executes the complete durable lifetime of exactly one logical
// participant wave:
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
		runner.proposer == nil || runner.pinFencer == nil || ctx == nil {
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
	head, routePin, pending, readApplied, err := runner.openWaveRows(ctx, wave, keyDigest)
	if err != nil {
		return DurableRequestWaveResult{}, err
	}
	if head.PinID != wave.PinID || head.RequestDigest == (requestledger.Digest{}) ||
		head.PlanRoot == (requestledger.Digest{}) {
		return DurableRequestWaveResult{}, ErrDurableRequestConflict
	}
	completed := routePin.Phase == requestledger.RoutePinReleased &&
		routePin.WaveOrdinal == wave.Ordinal && head.NextStepOrdinal == wave.Ordinal+1 &&
		head.OutstandingRoutePinDigest == (requestledger.Digest{}) && pending.Revision == 0
	if completed {
		observation, readErr := runner.readWaveObservation(ctx, wave, routePin, readApplied)
		return DurableRequestWaveResult{Observation: observation, Revision: head.Revision}, readErr
	}
	beforeAdvance := head.NextStepOrdinal == wave.Ordinal
	afterAdvance := wave.Ordinal != ^uint64(0) && head.NextStepOrdinal == wave.Ordinal+1 &&
		head.OutstandingRoutePinDigest != (requestledger.Digest{})
	if !beforeAdvance && !afterAdvance {
		return DurableRequestWaveResult{}, ErrDurableRequestConflict
	}

	// A released record from the preceding wave is intentionally replaceable.
	if routePin.Phase == requestledger.RoutePinReleased &&
		routePin.WaveOrdinal+1 == wave.Ordinal && head.NextStepOrdinal == wave.Ordinal &&
		head.OutstandingRoutePinDigest == (requestledger.Digest{}) {
		routePin = requestledger.RoutePinRecord{}
	}
	if routePin.Phase != requestledger.RoutePinInvalid && routePin.WaveOrdinal != wave.Ordinal {
		return DurableRequestWaveResult{}, ErrDurableRequestConflict
	}

	var route ReplicatedRoute
	if routePin.Phase == requestledger.RoutePinInvalid {
		route, err = runner.resolveWave(ctx, wave)
		if err != nil {
			return DurableRequestWaveResult{}, err
		}
		acquire, physical, buildErr := appendDurableRequestRouteGateCommand(
			nil, route, wave, keyDigest, head.RequestDigest, head.PlanRoot,
			head.ContinuationDigest, head.NextStepOrdinal, routegate.OperationAcquireShared,
		)
		if buildErr != nil {
			return DurableRequestWaveResult{}, buildErr
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

	if routePin.Phase == requestledger.RoutePinAcquiring {
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
		head, err = runner.applyRoutePin(ctx, wave, head, routePin, next,
			requestledger.OperationRecordRoutePinAcquired)
		if err != nil {
			return DurableRequestWaveResult{}, err
		}
		routePin = next
	}

	var observation []byte
	if afterAdvance {
		observation, err = runner.readWaveObservation(ctx, wave, routePin, readApplied)
		if err != nil {
			return DurableRequestWaveResult{}, err
		}
	}
	if pending.Revision == 0 &&
		routePin.Phase == requestledger.RoutePinAcquired &&
		head.OutstandingRoutePinDigest == (requestledger.Digest{}) {
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
		result, applyErr := runner.ledger.ApplyCAS(ctx, wave.Home, wave.Key,
			DurableRequestLifecycleCAS{
				Operation:        requestledger.OperationAdvance,
				ExpectedRevision: head.Revision, Revision: continuation.Revision,
				Continuation: continuation,
			})
		if applyErr != nil {
			return DurableRequestWaveResult{}, applyErr
		}
		if result.Ledger.ResultCode != replicatedstate.ResultApplied {
			return DurableRequestWaveResult{}, ErrDurableRequestConflict
		}
		head, err = requestledger.AdvancePending(head, pending, continuation)
		if wave.Build != (requestledger.PayloadBuildRecord{}) {
			head, err = requestledger.AdvancePendingWithBuild(head, pending, continuation, wave.Build)
		}
		if err != nil {
			return DurableRequestWaveResult{}, errors.Join(err, ErrDurableRequestConflict)
		}
		pending = requestledger.PendingWaveRecord{}
	}

	if routePin.Phase == requestledger.RoutePinAcquired &&
		head.OutstandingRoutePinDigest == routePin.AcquiredEvidenceDigest {
		route, err = runner.resolvePersistedRoute(ctx, wave, routePin.Command)
		if err != nil {
			return DurableRequestWaveResult{}, err
		}
		release, _, buildErr := appendDurableRequestRouteGateCommand(
			nil, route, wave, keyDigest, routePin.RequestDigest, routePin.PlanRoot,
			routePin.PriorContinuationDigest, routePin.WaveOrdinal,
			routegate.OperationReleaseShared,
		)
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
		return requestledger.HeadRecord{}, ErrDurableRequestConflict
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
	route, err := runner.resolver.ResolveDurableRequestParticipant(ctx, wave.Participant)
	if err != nil {
		return ReplicatedRoute{}, err
	}
	if !durableRequestRouteMatchesParticipant(route, wave.Participant) ||
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
	route, err := runner.resolver.ResolveDurableRequestParticipant(ctx, wave.Participant)
	if err != nil {
		return ReplicatedRoute{}, err
	}
	if !durableRequestRouteMatchesParticipant(route, wave.Participant) ||
		!commandMatchesRoute(exact, route) {
		return ReplicatedRoute{}, ErrDurableRequestConflict
	}
	return route, nil
}

func durableRequestRouteMatchesParticipant(
	route ReplicatedRoute,
	participant DurableRequestLogicalParticipant,
) bool {
	return validReplicatedRoute(route) && route.Distribution == participant.Distribution &&
		route.Shard == participant.Shard && route.Group == participant.Group &&
		route.RangeIdentity == participant.RangeIdentity &&
		route.LineageDigest == participant.LineageDigest &&
		route.ForwardingRuleDigest == participant.ForwardingRuleDigest &&
		route.Command.SchemaGeneration == participant.SchemaGeneration &&
		route.Command.RelationManifestDigest == participant.RelationManifestDigest
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
		wave.GateEpoch == 0 || wave.Binding == (requestledger.Digest{}) ||
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
) ([]byte, requestledger.Digest, error) {
	identity, err := requestledger.DeriveRouteGateIdentity(
		keyDigest, requestDigest, planRoot, priorContinuation,
		wave.PinID, waveOrdinal,
	)
	if err != nil {
		return dst, requestledger.Digest{}, errors.Join(err, ErrDurableRequestConflict)
	}
	clientID := replication.ID128(wave.Identity.ID)
	sequence := uint64(1)
	if operation == routegate.OperationReleaseShared {
		sequence = 2
	}
	gate := routegate.Command{
		Operation: operation, Epoch: wave.GateEpoch,
		Identity: routegate.Identity(identity), Binding: routegate.Binding{1},
	}
	gateBytes, err := routegate.AppendCommand(nil, gate)
	if err != nil {
		return dst, requestledger.Digest{}, err
	}
	outer := replicatedTransactionCommandHeader(
		route, wave.Tenant, wave.Identity.RetryHome, clientID,
		wave.GateEpoch, sequence,
	)
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
