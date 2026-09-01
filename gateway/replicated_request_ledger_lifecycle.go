package gateway

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

// DurableRequestLedger is the exact production lifecycle boundary. ApplyCAS
// accepts the typed requestledger union, never caller-encoded bytes, and
// ReadRow reopens one bounded hidden row into its typed representation. The
// caller owns PendingSteps for the lifetime of a returned pending wave.
//
// This surface deliberately exposes every requestledger operation through one
// closed typed command union. Adding a lifecycle operation therefore changes
// requestledger.LastOperation and its build gate rather than growing a second
// gateway protocol or a parallel set of subtly different CAS methods.
type DurableRequestLedger interface {
	ApplyCAS(
		context.Context,
		DurableRequestLedgerHome,
		requestledger.RequestKey,
		DurableRequestLifecycleCAS,
	) (DurableRequestLifecycleCASResult, error)
	ReadRow(
		context.Context,
		DurableRequestLedgerHome,
		DurableRequestLifecycleRead,
	) (DurableRequestLifecycleRow, error)
}

// DurableRequestLifecycleCAS is the closed typed union for every canonical
// requestledger operation. Operation selects exactly one record field; the RF3
// adapter derives Payload and SubjectDigest and refuses mismatched shapes.
// SubjectDigest is used only by OperationSeal, whose canonical payload is
// empty. ExpectedRevision and Revision remain explicit replicated CAS scalars.
type DurableRequestLifecycleCAS struct {
	Operation        requestledger.Operation
	ExpectedRevision uint64
	Revision         uint64
	Seal             bool
	SubjectDigest    requestledger.Digest

	Head            requestledger.HeadRecord
	PlanPages       []requestledger.PlanPageRecord
	Pending         requestledger.PendingWaveRecord
	Continuation    requestledger.ContinuationRecord
	Terminal        requestledger.TerminalRecord
	Ack             requestledger.AckRequest
	GC              requestledger.GCRequest
	PayloadChunk    requestledger.PayloadChunkRecord
	PayloadBuild    requestledger.PayloadBuildRecord
	PlanningExpiry  requestledger.PlanningExpiryRequest
	RoutePin        requestledger.RoutePinRecord
	PayloadCleanup  requestledger.PayloadCleanupRequest
	Prepared        requestledger.PreparedTerminalRecord
	SchemaPin       requestledger.SchemaPinReleaseRecord
	PlanningRestart requestledger.PlanningRestartRequest
	PlanningCleanup requestledger.PlanningCleanupRequest
	IssuerAdvance   requestledger.IssuerAdvanceRequest
	IssuerOpen      requestledger.IssuerHighwaterRecord
}

// DurableRequestLifecycleCASResult is the authenticated replicated result of
// one inner revision CAS. ResultCode remains explicit: deterministic conflict,
// capacity, and wrong-range outcomes are durable protocol results, not
// transport errors.
type DurableRequestLifecycleCASResult struct {
	Ledger  replicatedstate.RequestLedgerCompletionResult
	Applied uint64
	Retries int
}

// DurableRequestLifecycleRead names one hidden row without allowing a caller
// to select an arbitrary byte bound. PendingSteps is required only for a
// pending-wave read and prevents attacker-controlled allocation while decoding
// its bounded step vector.
type DurableRequestLifecycleRead struct {
	Key            requestledger.RequestKey
	Kind           replicatedstate.RequestLedgerReadKind
	Ordinal        uint64
	ContentRoot    requestledger.Digest
	MinimumApplied uint64
	PendingSteps   []requestledger.StepRef
}

// DurableRequestLifecycleRow is a closed typed reopen image. Exactly one field
// corresponding to Kind is populated when Found is true. Raw owns the backing
// bytes for variable-size typed values; Pending.Steps aliases PendingSteps.
type DurableRequestLifecycleRow struct {
	Applied uint64
	Found   bool
	Kind    replicatedstate.RequestLedgerReadKind
	Retries int
	Raw     []byte

	Head         requestledger.HeadRecord
	PlanPage     requestledger.PlanPageRecord
	Pending      requestledger.PendingWaveRecord
	Terminal     requestledger.TerminalRecord
	Ack          requestledger.AckRecord
	Continuation requestledger.ContinuationRecord
	PayloadChunk requestledger.PayloadChunkRecord
	PayloadBuild requestledger.PayloadBuildRecord
	RoutePin     requestledger.RoutePinRecord
	Prepared     requestledger.PreparedTerminalRecord
	SchemaPin    requestledger.SchemaPinReleaseRecord
	IssuerStatus requestledger.IssuerLaneStatus
}

type durableRequestLedgerRF3Client interface {
	Apply(context.Context, DurableRequestLedgerHome, []byte) (ReplicatedRequestLedgerApplyResult, error)
	// Read transfers ownership of result.Value to its caller. Production
	// transport responses are detached allocations, so the typed lifecycle
	// adapter can reopen even maximum-size plan pages without a second copy.
	Read(context.Context, DurableRequestLedgerHome, ReplicatedRequestLedgerRead) (ReplicatedRequestLedgerReadResult, error)
}

// DurableRequestLedgerRF3 maps the typed lifecycle directly onto the shipped
// RF3 ledger transport. It owns no cache and invents no retry identity; the
// underlying client derives the byte-identical proposal identity from the
// canonical inner command.
type DurableRequestLedgerRF3 struct {
	client        durableRequestLedgerRF3Client
	readCollector DurableRequestLedgerReadCollector
}

type durableRequestWaveReadCut struct {
	Head    requestledger.HeadRecord
	Route   requestledger.RoutePinRecord
	Pending requestledger.PendingWaveRecord
	Applied uint64
}

type durableRequestWaveCutReader interface {
	ReadWaveCut(
		context.Context,
		DurableRequestLedgerHome,
		requestledger.RequestKey,
		[]requestledger.StepRef,
	) (durableRequestWaveReadCut, error)
}

type durableRequestProgressReadCut struct {
	Head         requestledger.HeadRecord
	Continuation requestledger.ContinuationRecord
	Applied      uint64
}

type durableRequestProgressCutReader interface {
	ReadProgressCut(
		context.Context, DurableRequestLedgerHome, requestledger.RequestKey,
	) (durableRequestProgressReadCut, error)
}

type durableRequestTerminalReadCut struct {
	Head         requestledger.HeadRecord
	Continuation requestledger.ContinuationRecord
	Prepared     requestledger.PreparedTerminalRecord
	SchemaPin    requestledger.SchemaPinReleaseRecord
	Terminal     requestledger.TerminalRecord
	Applied      uint64
}

type durableRequestTerminalCutReader interface {
	ReadTerminalCut(
		context.Context, DurableRequestLedgerHome, requestledger.RequestKey,
	) (durableRequestTerminalReadCut, error)
}

func NewDurableRequestLedgerRF3(
	client *ReplicatedRequestLedgerRF3,
	options ...DurableRequestLedgerRF3Option,
) (*DurableRequestLedgerRF3, error) {
	if client == nil {
		return nil, ErrDurableRequest
	}
	ledger := &DurableRequestLedgerRF3{client: client}
	for _, option := range options {
		if option == nil {
			return nil, ErrDurableRequest
		}
		option(ledger)
	}
	return ledger, nil
}

func (ledger *DurableRequestLedgerRF3) ApplyCAS(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key requestledger.RequestKey,
	cas DurableRequestLifecycleCAS,
) (DurableRequestLifecycleCASResult, error) {
	if ledger == nil || ledger.client == nil || ctx == nil || !key.Valid() ||
		home.Identity == ([32]byte{}) || home.Point == (requestledger.LedgerHome{}) {
		return DurableRequestLifecycleCASResult{}, ErrDurableRequest
	}
	keyDigest, err := requestledger.KeyDigest(key)
	if err != nil {
		return DurableRequestLifecycleCASResult{}, errors.Join(err, ErrDurableRequestConflict)
	}
	derivedHome, err := requestledger.Home(key)
	if err != nil || derivedHome != home.Point {
		return DurableRequestLifecycleCASResult{}, errors.Join(err, ErrDurableRequestConflict)
	}
	command, err := durableRequestLifecycleCommand(keyDigest, cas)
	if err != nil {
		return DurableRequestLifecycleCASResult{}, errors.Join(err, ErrDurableRequestConflict)
	}
	command.ExpectedRangeIdentity, command.Home = requestledger.Digest(home.Identity), home.Point
	inner, err := requestledger.AppendCommand(nil, command)
	if err != nil {
		return DurableRequestLifecycleCASResult{}, errors.Join(err, ErrDurableRequestConflict)
	}
	result, err := ledger.client.Apply(ctx, home, inner)
	if err != nil {
		return DurableRequestLifecycleCASResult{}, err
	}
	if result.Ledger.Operation != command.Operation ||
		result.Ledger.KeyDigest != command.KeyDigest ||
		result.Ledger.RequestDigest != command.RequestDigest ||
		result.Ledger.PlanRoot != command.PlanRoot ||
		result.Ledger.RangeIdentity != requestledger.Digest(home.Identity) ||
		result.Native.Outcome.AppliedIndex == 0 {
		return DurableRequestLifecycleCASResult{}, ErrDurableRequestConflict
	}
	if result.Ledger.ResultCode == replicatedstate.ResultRequestLedgerConflict &&
		cas.ExpectedRevision != 0 && result.Ledger.Revision > cas.ExpectedRevision {
		// A competing exact retry may already have installed the next wave.
		// Do not turn a proven advancing CAS loss into a terminal client error.
		// The service must reopen the sealed recipe at this applied fence; this
		// signal never authorizes replaying stale command construction in place.
		return DurableRequestLifecycleCASResult{}, &durableRequestAdvancedError{
			revision: result.Ledger.Revision, applied: result.Native.Outcome.AppliedIndex,
		}
	}
	return DurableRequestLifecycleCASResult{
		Ledger: result.Ledger, Applied: result.Native.Outcome.AppliedIndex,
		Retries: result.Native.Retries,
	}, nil
}

type durableRequestAdvancedError struct{ revision, applied uint64 }

func (err *durableRequestAdvancedError) Error() string {
	return "gateway: durable request advanced during exact retry"
}
func (err *durableRequestAdvancedError) Unwrap() error { return ErrDurableRequestConflict }

func durableRequestLifecycleCommand(
	keyDigest requestledger.Digest,
	cas DurableRequestLifecycleCAS,
) (requestledger.Command, error) {
	command := requestledger.Command{
		Operation: cas.Operation, ExpectedRevision: cas.ExpectedRevision,
		Revision: cas.Revision, KeyDigest: keyDigest, Seal: cas.Seal,
	}
	var err error
	switch cas.Operation {
	case requestledger.OperationCreate:
		command.RequestDigest, command.PlanRoot = cas.Head.RequestDigest, cas.Head.PlanRoot
		command.SubjectDigest = cas.Head.TerminalContractDigest
		command.Payload, err = requestledger.AppendHead(nil, cas.Head)
	case requestledger.OperationAppendPages:
		if len(cas.PlanPages) == 0 {
			return requestledger.Command{}, ErrDurableRequest
		}
		first := &cas.PlanPages[0]
		command.RequestDigest, command.PlanRoot = cas.Head.RequestDigest, first.PlanRoot
		command.SubjectDigest = first.PlanBuildID
		command.Payload, err = requestledger.AppendPlanPageBatch(nil, cas.PlanPages)
	case requestledger.OperationSeal:
		command.RequestDigest, command.PlanRoot = cas.Head.RequestDigest, cas.Head.PlanRoot
		command.SubjectDigest = cas.SubjectDigest
	case requestledger.OperationPutPending:
		command.RequestDigest, command.PlanRoot = cas.Pending.RequestDigest, cas.Pending.PlanRoot
		command.SubjectDigest = cas.Pending.WaveDigest
		command.Payload, err = requestledger.AppendPendingWave(nil, cas.Pending)
	case requestledger.OperationAdvance:
		command.RequestDigest, command.PlanRoot = cas.Continuation.RequestDigest, cas.Continuation.PlanRoot
		command.SubjectDigest = cas.Continuation.ContinuationDigest
		command.Payload, err = requestledger.AppendContinuation(nil, cas.Continuation)
	case requestledger.OperationComplete:
		command.RequestDigest, command.PlanRoot = cas.Terminal.RequestDigest, cas.Terminal.PlanRoot
		command.SubjectDigest = cas.Terminal.ResultDigest
		command.Payload, err = requestledger.AppendTerminal(nil, cas.Terminal)
	case requestledger.OperationAck:
		command.RequestDigest, command.PlanRoot = cas.Head.RequestDigest, cas.Head.PlanRoot
		command.SubjectDigest = requestledger.AckRequestDigest(cas.Ack)
		command.Payload, err = requestledger.AppendAckRequest(nil, cas.Ack)
	case requestledger.OperationGC:
		command.RequestDigest, command.PlanRoot = cas.Head.RequestDigest, cas.Head.PlanRoot
		command.SubjectDigest = cas.GC.ExpectedAckDigest
		command.Payload, err = requestledger.AppendGCRequest(nil, cas.GC)
	case requestledger.OperationStagePayloadChunk:
		command.RequestDigest, command.PlanRoot = cas.Head.RequestDigest, cas.PayloadChunk.PlanRoot
		command.SubjectDigest = cas.PayloadChunk.BuildDigest
		command.Payload, err = requestledger.AppendPayloadChunk(nil, cas.PayloadChunk)
	case requestledger.OperationBeginPayloadBuild, requestledger.OperationSealPayload:
		command.RequestDigest, command.PlanRoot = cas.PayloadBuild.RequestDigest, cas.PayloadBuild.PlanRoot
		command.SubjectDigest = cas.PayloadBuild.BuildDigest
		command.Payload, err = requestledger.AppendPayloadBuild(nil, cas.PayloadBuild)
	case requestledger.OperationExpirePlanning:
		command.RequestDigest, command.PlanRoot = cas.Head.RequestDigest, cas.Head.PlanRoot
		command.SubjectDigest = cas.PlanningExpiry.PlanBuildID
		command.Payload, err = requestledger.AppendPlanningExpiryRequest(nil, cas.PlanningExpiry)
	case requestledger.OperationBeginRoutePinAcquire,
		requestledger.OperationRecordRoutePinAcquired,
		requestledger.OperationBeginRoutePinRelease,
		requestledger.OperationRecordRoutePinReleased:
		command.RequestDigest, command.PlanRoot = cas.RoutePin.RequestDigest, cas.RoutePin.PlanRoot
		command.SubjectDigest = cas.RoutePin.RecordDigest
		command.Payload, err = requestledger.AppendRoutePin(nil, cas.RoutePin)
	case requestledger.OperationRecordRoutePinAcquiredPutPending:
		command.RequestDigest, command.PlanRoot = cas.RoutePin.RequestDigest, cas.RoutePin.PlanRoot
		command.SubjectDigest, err = requestledger.AcquiredPendingDigest(cas.RoutePin, cas.Pending)
		if err == nil {
			command.Payload, err = requestledger.AppendAcquiredPending(nil, cas.RoutePin, cas.Pending)
		}
	case requestledger.OperationCleanupPayload:
		command.RequestDigest, command.PlanRoot = cas.Head.RequestDigest, cas.Head.PlanRoot
		command.SubjectDigest = cas.PayloadCleanup.BuildDigest
		command.Payload, err = requestledger.AppendPayloadCleanupRequest(nil, cas.PayloadCleanup)
	case requestledger.OperationPrepareTerminal:
		command.RequestDigest, command.PlanRoot = cas.Prepared.RequestDigest, cas.Prepared.PlanRoot
		command.SubjectDigest = cas.Prepared.PreparedDigest
		command.Payload, err = requestledger.AppendPreparedTerminal(nil, cas.Prepared)
	case requestledger.OperationBeginSchemaPinRelease,
		requestledger.OperationRecordSchemaPinReleased:
		command.RequestDigest, command.PlanRoot = cas.SchemaPin.RequestDigest, cas.SchemaPin.PlanRoot
		command.SubjectDigest = cas.SchemaPin.RecordDigest
		command.Payload, err = requestledger.AppendSchemaPinRelease(nil, cas.SchemaPin)
	case requestledger.OperationRestartPlanning:
		command.RequestDigest, command.PlanRoot = cas.Head.RequestDigest, cas.Head.PlanRoot
		command.SubjectDigest = cas.PlanningRestart.NextPlanBuildID
		command.Payload, err = requestledger.AppendPlanningRestartRequest(nil, cas.PlanningRestart)
	case requestledger.OperationCleanupPlanning:
		command.RequestDigest, command.PlanRoot = cas.Head.RequestDigest, cas.Head.PlanRoot
		command.SubjectDigest = cas.PlanningCleanup.PlanBuildID
		command.Payload, err = requestledger.AppendPlanningCleanupRequest(nil, cas.PlanningCleanup)
	case requestledger.OperationAdvanceIssuerHighwater:
		command.RequestDigest, command.PlanRoot = cas.Head.RequestDigest, cas.Head.PlanRoot
		command.SubjectDigest = cas.IssuerAdvance.ExpectedHighwaterDigest
		command.Payload, err = requestledger.AppendIssuerAdvanceRequest(nil, cas.IssuerAdvance)
	case requestledger.OperationOpenIssuerLane:
		command.KeyDigest = cas.IssuerOpen.IssuerDigest
		command.RequestDigest = cas.IssuerOpen.HighwaterDigest
		command.PlanRoot = cas.IssuerOpen.HighwaterDigest
		command.SubjectDigest = cas.IssuerOpen.HighwaterDigest
		command.Payload, err = requestledger.AppendIssuerHighwater(nil, cas.IssuerOpen)
	default:
		return requestledger.Command{}, ErrDurableRequest
	}
	if err != nil || command.RequestDigest == (requestledger.Digest{}) ||
		command.PlanRoot == (requestledger.Digest{}) ||
		command.SubjectDigest == (requestledger.Digest{}) {
		return requestledger.Command{}, errors.Join(err, ErrDurableRequest)
	}
	return command, nil
}

func (ledger *DurableRequestLedgerRF3) ReadRow(
	ctx context.Context,
	home DurableRequestLedgerHome,
	read DurableRequestLifecycleRead,
) (DurableRequestLifecycleRow, error) {
	maximum := replicatedstate.RequestLedgerReadMaxBytes(read.Kind)
	if ledger == nil || ledger.client == nil || ctx == nil || !read.Key.Valid() ||
		maximum == 0 || read.MinimumApplied == 0 ||
		(read.Kind == replicatedstate.RequestLedgerReadPending &&
			len(read.PendingSteps) < requestledger.MaxPendingWaveSteps) {
		if ledger != nil && ledger.readCollector != nil {
			ledger.readCollector.ObserveDurableRequestLedgerRead(DurableRequestLedgerReadObservation{
				Kind: read.Kind, Failed: true,
			})
		}
		return DurableRequestLifecycleRow{}, ErrDurableRequest
	}
	derivedHome, err := requestledger.Home(read.Key)
	if err != nil || derivedHome != home.Point {
		if ledger.readCollector != nil {
			ledger.readCollector.ObserveDurableRequestLedgerRead(DurableRequestLedgerReadObservation{
				Kind: read.Kind, Failed: true,
			})
		}
		return DurableRequestLifecycleRow{}, errors.Join(err, ErrDurableRequestConflict)
	}
	result, err := ledger.client.Read(ctx, home, ReplicatedRequestLedgerRead{
		Key: read.Key, ExpectedRangeIdentity: requestledger.Digest(home.Identity),
		Kind: read.Kind, Ordinal: read.Ordinal, ContentRoot: read.ContentRoot,
		MinimumApplied: read.MinimumApplied, MaxBytes: uint32(maximum),
	})
	observation := DurableRequestLedgerReadObservation{
		Kind: read.Kind, ResponseBytes: uint64(len(result.Value)),
	}
	if result.Retries > 0 {
		observation.Retries = uint64(result.Retries)
	}
	if err != nil {
		observation.Failed = true
		if ledger.readCollector != nil {
			ledger.readCollector.ObserveDurableRequestLedgerRead(observation)
		}
		return DurableRequestLifecycleRow{}, err
	}
	row := DurableRequestLifecycleRow{
		Applied: result.Applied, Found: result.Found,
		Kind: result.AuthoritativeKind, Retries: result.Retries,
	}
	if !result.Found {
		if ledger.readCollector != nil {
			ledger.readCollector.ObserveDurableRequestLedgerRead(observation)
		}
		return row, nil
	}
	// Read transfers its detached transport value. Keep the sole backing buffer:
	// every variable-size typed record aliases Raw, and duplicating a maximum
	// plan page here otherwise adds a full page allocation and memory copy to
	// every recovery read.
	row.Raw = result.Value
	if err := openDurableRequestLifecycleRow(&row, read.PendingSteps); err != nil {
		observation.Failed = true
		if ledger.readCollector != nil {
			ledger.readCollector.ObserveDurableRequestLedgerRead(observation)
		}
		return DurableRequestLifecycleRow{}, errors.Join(err, ErrDurableRequestConflict)
	}
	if ledger.readCollector != nil {
		ledger.readCollector.ObserveDurableRequestLedgerRead(observation)
	}
	return row, nil
}

// ReadWaveCut reopens the head, route pin, and pending wave from one state
// machine snapshot behind one leader ReadIndex. The returned variable fields
// alias the transferred RF3 response buffer; pendingSteps remains caller-owned.
func (ledger *DurableRequestLedgerRF3) ReadWaveCut(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key requestledger.RequestKey,
	pendingSteps []requestledger.StepRef,
) (durableRequestWaveReadCut, error) {
	if ledger == nil || ledger.client == nil || ctx == nil || !key.Valid() ||
		len(pendingSteps) < requestledger.MaxPendingWaveSteps {
		if ledger != nil && ledger.readCollector != nil {
			ledger.readCollector.ObserveDurableRequestLedgerRead(DurableRequestLedgerReadObservation{
				Kind: replicatedstate.RequestLedgerReadWave, Failed: true,
			})
		}
		return durableRequestWaveReadCut{}, ErrDurableRequest
	}
	derivedHome, err := requestledger.Home(key)
	if err != nil || derivedHome != home.Point {
		if ledger.readCollector != nil {
			ledger.readCollector.ObserveDurableRequestLedgerRead(DurableRequestLedgerReadObservation{
				Kind: replicatedstate.RequestLedgerReadWave, Failed: true,
			})
		}
		return durableRequestWaveReadCut{}, errors.Join(err, ErrDurableRequestConflict)
	}
	result, err := ledger.client.Read(ctx, home, ReplicatedRequestLedgerRead{
		Key: key, ExpectedRangeIdentity: requestledger.Digest(home.Identity),
		Kind: replicatedstate.RequestLedgerReadWave, MinimumApplied: 1,
		MaxBytes: uint32(replicatedstate.MaxRequestLedgerWaveReadBytes),
	})
	observation := DurableRequestLedgerReadObservation{
		Kind:          replicatedstate.RequestLedgerReadWave,
		ResponseBytes: uint64(len(result.Value)),
	}
	if result.Retries > 0 {
		observation.Retries = uint64(result.Retries)
	}
	if err != nil {
		observation.Failed = true
		if ledger.readCollector != nil {
			ledger.readCollector.ObserveDurableRequestLedgerRead(observation)
		}
		return durableRequestWaveReadCut{}, err
	}
	if !result.Found || result.AuthoritativeKind != replicatedstate.RequestLedgerReadWave {
		observation.Failed = true
		if ledger.readCollector != nil {
			ledger.readCollector.ObserveDurableRequestLedgerRead(observation)
		}
		return durableRequestWaveReadCut{}, ErrDurableRequestConflict
	}
	value, err := replicatedstate.OpenRequestLedgerWaveReadValue(result.Value)
	if err != nil {
		observation.Failed = true
		if ledger.readCollector != nil {
			ledger.readCollector.ObserveDurableRequestLedgerRead(observation)
		}
		return durableRequestWaveReadCut{}, errors.Join(err, ErrDurableRequestConflict)
	}
	head, err := requestledger.OpenHead(value.Head)
	if err != nil || head.Key != key {
		observation.Failed = true
		if ledger.readCollector != nil {
			ledger.readCollector.ObserveDurableRequestLedgerRead(observation)
		}
		return durableRequestWaveReadCut{}, errors.Join(err, ErrDurableRequestConflict)
	}
	cut := durableRequestWaveReadCut{Head: head, Applied: result.Applied}
	if value.RouteFound {
		cut.Route, err = requestledger.OpenRoutePin(value.RoutePin)
		if err != nil {
			observation.Failed = true
			if ledger.readCollector != nil {
				ledger.readCollector.ObserveDurableRequestLedgerRead(observation)
			}
			return durableRequestWaveReadCut{}, errors.Join(err, ErrDurableRequestConflict)
		}
	}
	if value.PendingFound {
		view, openErr := requestledger.OpenPendingWaveInto(value.Pending, pendingSteps)
		if openErr != nil {
			observation.Failed = true
			if ledger.readCollector != nil {
				ledger.readCollector.ObserveDurableRequestLedgerRead(observation)
			}
			return durableRequestWaveReadCut{}, errors.Join(openErr, ErrDurableRequestConflict)
		}
		cut.Pending = view.Record()
	}
	if ledger.readCollector != nil {
		ledger.readCollector.ObserveDurableRequestLedgerRead(observation)
	}
	return cut, nil
}

func (ledger *DurableRequestLedgerRF3) ReadProgressCut(
	ctx context.Context, home DurableRequestLedgerHome, key requestledger.RequestKey,
) (durableRequestProgressReadCut, error) {
	result, observation, err := ledger.readCut(ctx, home, key, replicatedstate.RequestLedgerReadProgress,
		replicatedstate.MaxRequestLedgerProgressReadBytes)
	if err != nil {
		ledger.observeRequestLedgerRead(observation, true)
		return durableRequestProgressReadCut{}, err
	}
	value, err := replicatedstate.OpenRequestLedgerProgressReadValue(result.Value)
	if err != nil {
		ledger.observeRequestLedgerRead(observation, true)
		return durableRequestProgressReadCut{}, errors.Join(err, ErrDurableRequestConflict)
	}
	head, err := requestledger.OpenHead(value.Head)
	if err != nil || head.Key != key {
		ledger.observeRequestLedgerRead(observation, true)
		return durableRequestProgressReadCut{}, errors.Join(err, ErrDurableRequestConflict)
	}
	cut := durableRequestProgressReadCut{Head: head, Applied: result.Applied}
	if value.ContinuationFound {
		cut.Continuation, err = requestledger.OpenContinuation(value.Continuation)
		if err != nil {
			ledger.observeRequestLedgerRead(observation, true)
			return durableRequestProgressReadCut{}, errors.Join(err, ErrDurableRequestConflict)
		}
	}
	ledger.observeRequestLedgerRead(observation, false)
	return cut, nil
}

func (ledger *DurableRequestLedgerRF3) ReadTerminalCut(
	ctx context.Context, home DurableRequestLedgerHome, key requestledger.RequestKey,
) (durableRequestTerminalReadCut, error) {
	result, observation, err := ledger.readCut(ctx, home, key, replicatedstate.RequestLedgerReadTerminalCut,
		replicatedstate.MaxRequestLedgerTerminalReadBytes)
	if err != nil {
		ledger.observeRequestLedgerRead(observation, true)
		return durableRequestTerminalReadCut{}, err
	}
	value, err := replicatedstate.OpenRequestLedgerTerminalReadValue(result.Value)
	if err != nil {
		ledger.observeRequestLedgerRead(observation, true)
		return durableRequestTerminalReadCut{}, errors.Join(err, ErrDurableRequestConflict)
	}
	head, err := requestledger.OpenHead(value.Head)
	if err != nil || head.Key != key {
		ledger.observeRequestLedgerRead(observation, true)
		return durableRequestTerminalReadCut{}, errors.Join(err, ErrDurableRequestConflict)
	}
	cut := durableRequestTerminalReadCut{Head: head, Applied: result.Applied}
	if value.ContinuationFound {
		cut.Continuation, err = requestledger.OpenContinuation(value.Continuation)
	}
	if err == nil && value.PreparedFound {
		cut.Prepared, err = requestledger.OpenPreparedTerminal(value.Prepared)
	}
	if err == nil && value.SchemaPinFound {
		cut.SchemaPin, err = requestledger.OpenSchemaPinRelease(value.SchemaPin)
	}
	if err == nil && value.TerminalFound {
		cut.Terminal, err = requestledger.OpenTerminal(value.Terminal)
	}
	if err != nil {
		ledger.observeRequestLedgerRead(observation, true)
		return durableRequestTerminalReadCut{}, errors.Join(err, ErrDurableRequestConflict)
	}
	ledger.observeRequestLedgerRead(observation, false)
	return cut, nil
}

func (ledger *DurableRequestLedgerRF3) readCut(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key requestledger.RequestKey,
	kind replicatedstate.RequestLedgerReadKind,
	maximum int,
) (ReplicatedRequestLedgerReadResult, DurableRequestLedgerReadObservation, error) {
	observation := DurableRequestLedgerReadObservation{Kind: kind}
	if ledger == nil || ledger.client == nil || ctx == nil || !key.Valid() || maximum <= 0 {
		return ReplicatedRequestLedgerReadResult{}, observation, ErrDurableRequest
	}
	derivedHome, err := requestledger.Home(key)
	if err != nil || derivedHome != home.Point {
		return ReplicatedRequestLedgerReadResult{}, observation, errors.Join(err, ErrDurableRequestConflict)
	}
	result, err := ledger.client.Read(ctx, home, ReplicatedRequestLedgerRead{
		Key: key, ExpectedRangeIdentity: requestledger.Digest(home.Identity),
		Kind: kind, MinimumApplied: 1, MaxBytes: uint32(maximum),
	})
	observation.ResponseBytes = uint64(len(result.Value))
	if result.Retries > 0 {
		observation.Retries = uint64(result.Retries)
	}
	if err != nil {
		return ReplicatedRequestLedgerReadResult{}, observation, err
	}
	if !result.Found {
		return ReplicatedRequestLedgerReadResult{}, observation, ErrDurableRequestConflict
	}
	if result.AuthoritativeKind == replicatedstate.RequestLedgerReadAck {
		return ReplicatedRequestLedgerReadResult{}, observation, ErrDurableRequestAcknowledged
	}
	if result.AuthoritativeKind != kind {
		return ReplicatedRequestLedgerReadResult{}, observation, ErrDurableRequestConflict
	}
	return result, observation, nil
}

func (ledger *DurableRequestLedgerRF3) observeRequestLedgerRead(
	observation DurableRequestLedgerReadObservation,
	failed bool,
) {
	if ledger == nil || ledger.readCollector == nil {
		return
	}
	observation.Failed = failed
	ledger.readCollector.ObserveDurableRequestLedgerRead(observation)
}

func openDurableRequestLifecycleRow(
	row *DurableRequestLifecycleRow,
	pendingSteps []requestledger.StepRef,
) (err error) {
	if row == nil || !row.Found || len(row.Raw) == 0 {
		return ErrDurableRequest
	}
	switch row.Kind {
	case replicatedstate.RequestLedgerReadHead:
		row.Head, err = requestledger.OpenHead(row.Raw)
	case replicatedstate.RequestLedgerReadPlanPage:
		row.PlanPage, err = requestledger.OpenPlanPage(row.Raw)
	case replicatedstate.RequestLedgerReadPending:
		var view requestledger.PendingWaveView
		view, err = requestledger.OpenPendingWaveInto(row.Raw, pendingSteps)
		if err == nil {
			row.Pending = view.Record()
		}
	case replicatedstate.RequestLedgerReadTerminal:
		row.Terminal, err = requestledger.OpenTerminal(row.Raw)
	case replicatedstate.RequestLedgerReadAck:
		row.Ack, err = requestledger.OpenAck(row.Raw)
	case replicatedstate.RequestLedgerReadContinuation:
		row.Continuation, err = requestledger.OpenContinuation(row.Raw)
	case replicatedstate.RequestLedgerReadPayloadChunk:
		row.PayloadChunk, err = requestledger.OpenPayloadChunk(row.Raw)
	case replicatedstate.RequestLedgerReadPayloadBuild:
		row.PayloadBuild, err = requestledger.OpenPayloadBuild(row.Raw)
	case replicatedstate.RequestLedgerReadRoutePin:
		row.RoutePin, err = requestledger.OpenRoutePin(row.Raw)
	case replicatedstate.RequestLedgerReadPrepared:
		row.Prepared, err = requestledger.OpenPreparedTerminal(row.Raw)
	case replicatedstate.RequestLedgerReadSchemaPin:
		row.SchemaPin, err = requestledger.OpenSchemaPinRelease(row.Raw)
	case replicatedstate.RequestLedgerReadIssuerStatus:
		row.IssuerStatus, err = requestledger.OpenIssuerLaneStatus(row.Raw)
	default:
		return ErrDurableRequest
	}
	return err
}

var _ DurableRequestLedger = (*DurableRequestLedgerRF3)(nil)
