package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

// DurableRequestExecutionPinAuthority acquires or recovers the one logical
// execution pin sealed by a request recipe. A retry must return the same
// logical pin and a currently valid lease, never create parallel authority.
type DurableRequestExecutionPinAuthority interface {
	AcquireOrRecover(
		context.Context,
		DurableRequestTypedExecutionContext,
	) (ReplicatedRoute, executionpin.AcquireCertificate, executionpin.LeaseCertificate, error)
}

type DurableRequestExecutionPinRetirer interface {
	RetireTerminal(context.Context, DurableRequestTypedExecutionContext) error
}

// DurableRequestExecutionPinAckRetirer removes one gateway's local exact-retry
// session after replicated ACK state proves that the global pin was released.
// It exists for cross-gateway recovery after terminal plan rows were collected.
type DurableRequestExecutionPinAckRetirer interface {
	RetireAcknowledged(
		context.Context,
		DurableRequestLedgerHome,
		requestledger.AckRecord,
	) error
}

// DurableRequestService is the shipped typed request boundary. It admits the
// immutable recipe through RF3 lifecycle CAS, reopens it from authenticated
// pages, binds the logical execution-pin lease, and runs the distributed
// transaction protocol. It has no process-local request registry.
type DurableRequestService struct {
	topology *DurableRequestLedgerTopologyHolder
	ledger   DurableRequestLedger
	runner   DurableRequestTypedRunner
	pins     DurableRequestExecutionPinAuthority
	acks     *DurableRequestAckCollector
}

// DurableRequestBeginResult is the authoritative settlement of the fused
// request admission. Head is the materialized persisted head; Applied and the
// absolute expiry come from the same committed OperationCreate result.
type DurableRequestBeginResult struct {
	Home                     DurableRequestLedgerHome
	Head                     requestledger.HeadRecord
	Applied                  uint64
	PlanningLeaseExpiryIndex uint64
	Created                  bool
	ProgramMatches           bool
}

func NewDurableRequestService(
	topology *DurableRequestLedgerTopologyHolder,
	ledger DurableRequestLedger,
	runner *DurableRequestDistributedRunner,
	pins DurableRequestExecutionPinAuthority,
) (*DurableRequestService, error) {
	if _, ok := pins.(DurableRequestExecutionPinRetirer); !ok {
		return nil, ErrDurableRequest
	}
	if _, ok := pins.(DurableRequestExecutionPinAckRetirer); !ok {
		return nil, ErrDurableRequest
	}
	return newDurableRequestService(topology, ledger, runner, pins)
}

func newDurableRequestService(
	topology *DurableRequestLedgerTopologyHolder,
	ledger DurableRequestLedger,
	runner DurableRequestTypedRunner,
	pins DurableRequestExecutionPinAuthority,
) (*DurableRequestService, error) {
	if topology == nil || topology.Current() == nil || ledger == nil || runner == nil || pins == nil {
		return nil, ErrDurableRequest
	}
	acks, err := NewDurableRequestAckCollector(ledger)
	if err != nil {
		return nil, err
	}
	return &DurableRequestService{
		topology: topology, ledger: ledger, runner: runner, pins: pins, acks: acks,
	}, nil
}

func (service *DurableRequestService) Execute(
	ctx context.Context,
	request DurableRequest,
) (DurableRequestOutcome, error) {
	begin, err := service.Begin(ctx, request)
	if err != nil {
		return DurableRequestOutcome{}, err
	}
	if !begin.ProgramMatches {
		outcome, found, replayErr := service.Replay(ctx, request.Key)
		if !found && replayErr == nil {
			replayErr = ErrDurableRequestConflict
		}
		return outcome, replayErr
	}
	return service.ExecuteBegun(ctx, request, begin)
}

// Begin performs the sole issuer-sequence linearization and Create admission.
// It deliberately does not point-read before proposing. A read is used only
// to recover an outcome-unknown proposal or an already-progressed retry.
func (service *DurableRequestService) Begin(
	ctx context.Context,
	request DurableRequest,
) (DurableRequestBeginResult, error) {
	if service == nil || ctx == nil || !validDurableRequestLedgerKey(request.Key) ||
		!validDurableRequestLogicalProgram(request.Program) {
		return DurableRequestBeginResult{}, ErrDurableRequest
	}
	if !matchesDurableRequestProgramKey(request.Key, request.Program) {
		return DurableRequestBeginResult{}, fmt.Errorf("gateway: logical program key mismatch: %w", ErrDurableRequestConflict)
	}
	measurement, err := measureDurableRequestPlan(request.Key, request.Program)
	if err != nil {
		return DurableRequestBeginResult{}, err
	}
	home, err := service.home(request.Key)
	if err != nil {
		return DurableRequestBeginResult{}, err
	}
	head, applied, created, err := service.create(ctx, home, request.Key, measurement)
	if err != nil {
		return DurableRequestBeginResult{}, err
	}
	matches := durableRequestHeadMatchesMeasurement(head, request.Key, measurement)
	if !matches && (head.Phase == requestledger.PhasePlanning || head.Phase == requestledger.PhaseExpired) {
		return DurableRequestBeginResult{}, fmt.Errorf("gateway: unfinished planning measurement mismatch (phase=%d): %w", head.Phase, ErrDurableRequestConflict)
	}
	return DurableRequestBeginResult{Home: home, Head: head, Applied: applied,
		PlanningLeaseExpiryIndex: head.PlanningLeaseExpiryIndex,
		Created:                  created, ProgramMatches: matches}, nil
}

// ExecuteBegun continues from a Begin settlement without issuing a duplicate
// Create. This is the shipped path used after the gateway has incorporated the
// actual applied-index expiry proof into its operational execution context.
func (service *DurableRequestService) ExecuteBegun(
	ctx context.Context,
	request DurableRequest,
	begin DurableRequestBeginResult,
) (DurableRequestOutcome, error) {
	if service == nil || ctx == nil || begin.Applied == 0 ||
		begin.PlanningLeaseExpiryIndex == 0 ||
		!begin.ProgramMatches ||
		begin.Head.PlanningLeaseExpiryIndex != begin.PlanningLeaseExpiryIndex ||
		!validDurableRequestLedgerKey(request.Key) ||
		!validDurableRequestLogicalProgram(request.Program) ||
		!matchesDurableRequestProgramKey(request.Key, request.Program) {
		return DurableRequestOutcome{}, ErrDurableRequest
	}
	release, err := acquireDurableRequestStream(ctx)
	if err != nil {
		return DurableRequestOutcome{}, err
	}
	defer release()
	measurement, err := measureDurableRequestPlan(request.Key, request.Program)
	if err != nil {
		return DurableRequestOutcome{}, err
	}
	home, err := service.home(request.Key)
	if err != nil || home.Identity != begin.Home.Identity || home.Point != begin.Home.Point ||
		home.TopologyGeneration != begin.Home.TopologyGeneration ||
		!sameReplicatedCatalogRoute(home.borrowedRoute(), begin.Home.borrowedRoute()) ||
		!durableRequestHeadMatchesMeasurement(begin.Head, request.Key, measurement) {
		return DurableRequestOutcome{}, ErrDurableRequestConflict
	}
	head, applied := begin.Head, begin.Applied
	if head.Phase == requestledger.PhasePlanning {
		head, applied, err = service.appendPlan(ctx, home, request.Key, head, applied, measurement, request.Program)
		if err != nil {
			return DurableRequestOutcome{}, err
		}
	}
	return service.drive(ctx, home, request.Key, head, applied)
}

// Replay starts solely from the exact structured key and replicated state.
// An incomplete planning record needs the original program to supply missing
// immutable pages, so only Execute may resume that pre-seal state.
func (service *DurableRequestService) Replay(
	ctx context.Context,
	key DurableRequestLedgerKey,
) (DurableRequestOutcome, bool, error) {
	if service == nil || ctx == nil || !validDurableRequestLedgerKey(key) {
		return DurableRequestOutcome{}, false, ErrDurableRequest
	}
	home, err := service.home(key)
	if err != nil {
		return DurableRequestOutcome{}, false, err
	}
	head, ack, applied, err := service.openHead(ctx, home, key.RequestKey, 1)
	if err != nil {
		return DurableRequestOutcome{}, false, err
	}
	if ack.Revision != 0 {
		return DurableRequestOutcome{Acknowledged: true}, true, ErrDurableRequestAcknowledged
	}
	if head.Revision == 0 {
		return DurableRequestOutcome{}, false, nil
	}
	if head.RequestDigest != requestledger.Digest(key.Digest) {
		return DurableRequestOutcome{}, true, ErrDurableRequestConflict
	}
	if head.Phase == requestledger.PhasePlanning || head.Phase == requestledger.PhaseExpired {
		return DurableRequestOutcome{}, true, ErrDurableRequestUnresolved
	}
	outcome, err := service.drive(ctx, home, key, head, applied)
	return outcome, true, err
}

// Acknowledge proves possession of the raw terminal capability and performs
// bounded replicated collection through the typed ACK collector.
func (service *DurableRequestService) Acknowledge(
	ctx context.Context,
	key DurableRequestLedgerKey,
	terminalRevision uint64,
	resultDigest replication.Digest,
	token DurableRequestAckToken,
) (DurableRequestAckResult, error) {
	if service == nil || ctx == nil || !validDurableRequestLedgerKey(key) ||
		terminalRevision == 0 || resultDigest == (replication.Digest{}) ||
		token == (DurableRequestAckToken{}) {
		return DurableRequestAckResult{}, ErrDurableRequest
	}
	home, err := service.home(key)
	if err != nil {
		return DurableRequestAckResult{}, err
	}
	head, ack, applied, err := service.openHead(ctx, home, key.RequestKey, 1)
	if err != nil {
		return DurableRequestAckResult{}, err
	}
	if ack.Revision != 0 {
		plan := DurableRequestAckPlan{
			Home: home, Key: key.RequestKey, TerminalRevision: terminalRevision,
			ResultDigest: requestledger.Digest(resultDigest), AckToken: requestledger.AckToken(token),
		}
		if !ackMatchesPlan(ack, plan) || ack.RequestDigest != requestledger.Digest(key.Digest) {
			return DurableRequestAckResult{}, ErrDurableRequestConflict
		}
		result, collectErr := service.acks.AcknowledgeAndCollect(ctx, plan)
		if collectErr != nil {
			return result, collectErr
		}
		if retirer, ok := service.pins.(DurableRequestExecutionPinAckRetirer); ok {
			if retireErr := retirer.RetireAcknowledged(ctx, home, result.Ack); retireErr != nil {
				return result, errors.Join(retireErr, ErrDurableRequestUnresolved)
			}
		}
		return result, nil
	}
	if head.Phase != requestledger.PhaseTerminal || head.RequestDigest != requestledger.Digest(key.Digest) {
		return DurableRequestAckResult{}, ErrDurableRequestUnresolved
	}
	terminal, _, err := service.readTerminal(ctx, home, key.RequestKey, applied)
	if err != nil {
		return DurableRequestAckResult{}, err
	}
	if !durableRequestTerminalMatchesKey(terminal, key, head.PlanRoot) {
		return DurableRequestAckResult{}, ErrDurableRequestConflict
	}
	if terminal.Revision != terminalRevision ||
		terminal.ResultDigest != requestledger.Digest(resultDigest) {
		return DurableRequestAckResult{}, ErrDurableRequestConflict
	}
	retirer, ok := service.pins.(DurableRequestExecutionPinRetirer)
	if ok {
		execution, executionErr := service.openTerminalExecution(ctx, home, key, head, applied)
		if executionErr != nil {
			return DurableRequestAckResult{}, errors.Join(executionErr, ErrDurableRequestUnresolved)
		}
		if retireErr := retirer.RetireTerminal(ctx, execution); retireErr != nil {
			return DurableRequestAckResult{}, errors.Join(retireErr, ErrDurableRequestUnresolved)
		}
	}
	return service.acks.AcknowledgeAndCollect(ctx, DurableRequestAckPlan{
		Home: home, Key: key.RequestKey, TerminalRevision: terminalRevision,
		ResultDigest: requestledger.Digest(resultDigest), AckToken: requestledger.AckToken(token),
	})
}

func (service *DurableRequestService) home(key DurableRequestLedgerKey) (DurableRequestLedgerHome, error) {
	point, err := requestledger.Home(key.RequestKey)
	if err != nil {
		return DurableRequestLedgerHome{}, errors.Join(err, ErrDurableRequest)
	}
	home, _, ok := service.topology.Lookup(point)
	if !ok || home.Point != point || home.Identity == (replication.Digest{}) {
		return DurableRequestLedgerHome{}, ErrDurableRequestUnavailable
	}
	return home, nil
}

func (service *DurableRequestService) openHead(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key requestledger.RequestKey,
	minimum uint64,
) (requestledger.HeadRecord, requestledger.AckRecord, uint64, error) {
	row, err := service.ledger.ReadRow(ctx, home, DurableRequestLifecycleRead{
		Key: key, Kind: replicatedstate.RequestLedgerReadHead, MinimumApplied: max(uint64(1), minimum),
	})
	if err != nil {
		return requestledger.HeadRecord{}, requestledger.AckRecord{}, 0, err
	}
	if !row.Found {
		return requestledger.HeadRecord{}, requestledger.AckRecord{}, row.Applied, nil
	}
	switch row.Kind {
	case replicatedstate.RequestLedgerReadHead:
		return row.Head, requestledger.AckRecord{}, row.Applied, nil
	case replicatedstate.RequestLedgerReadAck:
		return requestledger.HeadRecord{}, row.Ack, row.Applied, nil
	default:
		return requestledger.HeadRecord{}, requestledger.AckRecord{}, 0, ErrDurableRequestConflict
	}
}

func durableRequestLifecycleExecutionContract(
	contract DurableRequestExecutionContract,
) requestledger.ExecutionContract {
	return requestledger.ExecutionContract{
		CatalogGeneration: contract.CatalogGeneration,
		PinID:             requestledger.PinID(contract.PinID), PinDigest: requestledger.Digest(contract.PinDigest),
		RouteSchemaCertificateDigest: requestledger.Digest(contract.RouteSchemaCertificateDigest),
		MaxPendingWaveBytes:          contract.MaxPendingWaveBytes,
		MaxContinuationBytes:         contract.MaxContinuationBytes,
		MaxTerminalBytes:             contract.MaxTerminalBytes,
		MaxActivePayloadBytes:        contract.MaxActivePayloadBytes,
		MaxActivePayloadChunks:       contract.MaxActivePayloadChunks,
		PlanBuildID:                  requestledger.Digest(contract.PlanBuildID),
		PlanBuildGeneration:          contract.PlanningLeaseGeneration,
		PlanningLeaseSpan:            contract.PlanningLeaseSpan,
		PlanningLeaseGeneration:      contract.PlanningLeaseGeneration,
		TerminalTransitionTag:        contract.CommitTransitionTag,
		FinalWaveCount:               contract.CommitFinalWaveCount,
		TerminalStateDigest:          requestledger.Digest(contract.CommitTerminalStateDigest),
		// The ledger head's TerminalSummaryDigest is the expected retirement
		// witness checked by ValidateTerminalContinuation, not the gateway's
		// distinct aggregate terminal-summary digest. Both remain authenticated
		// by the recipe's terminal contract; never substitute one for the other.
		TerminalSummaryDigest:      requestledger.Digest(contract.RetirementWitnessDigest),
		AbortTerminalTransitionTag: contract.AbortTransitionTag,
		AbortFinalWaveCount:        contract.AbortFinalWaveCount,
		AbortTerminalStateDigest:   requestledger.Digest(contract.AbortTerminalStateDigest),
	}
}

func durableRequestHeadForMeasurement(
	key DurableRequestLedgerKey,
	measurement durableRequestPlanMeasurement,
) (requestledger.HeadRecord, error) {
	contract := durableRequestLifecycleExecutionContract(measurement.contract)
	if len(measurement.Inline) != 0 {
		return requestledger.NewHeadWithExecutionContract(
			key.RequestKey, requestledger.Digest(key.Digest),
			requestledger.Digest(measurement.contract.TerminalContractDigest), contract,
			measurement.Inline,
		)
	}
	return requestledger.NewPagedHeadWithExecutionContract(
		key.RequestKey, requestledger.Digest(key.Digest),
		requestledger.Digest(measurement.contract.TerminalContractDigest),
		measurement.PlanBytes, requestledger.Digest(measurement.Root), contract,
	)
}

func (service *DurableRequestService) create(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	measurement durableRequestPlanMeasurement,
) (requestledger.HeadRecord, uint64, bool, error) {
	want, err := durableRequestHeadForMeasurement(key, measurement)
	if err != nil {
		return requestledger.HeadRecord{}, 0, false, errors.Join(err, ErrDurableRequestConflict)
	}
	result, applyErr := service.ledger.ApplyCAS(ctx, home, key.RequestKey, DurableRequestLifecycleCAS{
		Operation: requestledger.OperationCreate, Revision: 1, Head: want,
	})
	minimum := uint64(1)
	if applyErr == nil {
		if result.Ledger.ResultCode == replicatedstate.ResultApplied {
			materialized, materializeErr := requestledger.MaterializeCreateAtExpiry(
				want, result.Ledger.PlanningLeaseExpiryIndex,
			)
			if materializeErr == nil {
				return materialized, result.Applied, !result.Ledger.ExactDuplicate, nil
			}
			applyErr = errors.Join(materializeErr, ErrDurableRequestConflict)
			minimum = result.Applied
		} else {
			applyErr = fmt.Errorf("%w: Create settlement code=%d phase=%d revision=%d",
				ErrDurableRequestConflict, result.Ledger.ResultCode, result.Ledger.Phase, result.Ledger.Revision)
		}
	}
	head, ack, applied, readErr := service.openHead(ctx, home, key.RequestKey, minimum)
	if readErr == nil && ack.Revision != 0 {
		return requestledger.HeadRecord{}, 0, false, ErrDurableRequestAcknowledged
	}
	if readErr != nil || head.Revision == 0 {
		return requestledger.HeadRecord{}, 0, false, errors.Join(applyErr, readErr, ErrDurableRequestUnresolved)
	}
	if head.RequestDigest != requestledger.Digest(key.Digest) {
		return requestledger.HeadRecord{}, 0, false, fmt.Errorf("gateway: retained Create request digest mismatch: %w", ErrDurableRequestConflict)
	}
	return head, applied, false, nil
}

func durableRequestHeadMatchesMeasurement(
	head requestledger.HeadRecord,
	key DurableRequestLedgerKey,
	measurement durableRequestPlanMeasurement,
) bool {
	wantPages := uint64(measurement.PhysicalPages)
	if len(measurement.Inline) != 0 {
		wantPages = 0
	}
	return head.Key == key.RequestKey && head.RequestDigest == requestledger.Digest(key.Digest) &&
		head.PlanRoot == requestledger.Digest(measurement.Root) &&
		head.TotalPlanBytes == measurement.PlanBytes &&
		head.PlanPageCount == wantPages &&
		head.TerminalContractDigest == requestledger.Digest(measurement.contract.TerminalContractDigest) &&
		head.CatalogGeneration == measurement.contract.CatalogGeneration &&
		head.PlanBuildID == requestledger.Digest(measurement.contract.PlanBuildID) &&
		head.PlanBuildGeneration == measurement.contract.PlanningLeaseGeneration &&
		head.PlanningLeaseGeneration == measurement.contract.PlanningLeaseGeneration &&
		head.PlanningLeaseSpan == measurement.contract.PlanningLeaseSpan &&
		head.PlanningLeaseExpiryIndex > head.PlanningLeaseSpan
}

func (service *DurableRequestService) appendPlan(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	head requestledger.HeadRecord,
	applied uint64,
	measurement durableRequestPlanMeasurement,
	program DurableRequestLogicalProgram,
) (requestledger.HeadRecord, uint64, error) {
	err := streamDurableRequestPlan(measurement, program, durableRequestPlanPageSinkFunc(
		func(ordinal uint32, raw []byte) error {
			if uint64(ordinal) < head.AppendedPageCount {
				return nil
			}
			if uint64(ordinal) != head.AppendedPageCount || head.Phase != requestledger.PhasePlanning {
				return ErrDurableRequestConflict
			}
			page, pageErr := requestledger.NewPlanPageData(head, uint64(ordinal), head.PageChain, raw)
			if pageErr != nil {
				return errors.Join(pageErr, ErrDurableRequestConflict)
			}
			seal := uint64(ordinal)+1 == head.PlanPageCount
			result, applyErr := service.ledger.ApplyCAS(ctx, home, key.RequestKey,
				DurableRequestLifecycleCAS{
					Operation:        requestledger.OperationAppendPages,
					ExpectedRevision: head.Revision, Revision: head.Revision + 1,
					Seal: seal, Head: head, PlanPages: []requestledger.PlanPageRecord{page},
				})
			minimum := applied
			if applyErr == nil {
				if result.Ledger.ResultCode == replicatedstate.ResultApplied {
					minimum = result.Applied
				} else {
					applyErr = ErrDurableRequestConflict
				}
			}
			next, ack, nextApplied, readErr := service.openHead(ctx, home, key.RequestKey, minimum)
			if readErr != nil || ack.Revision != 0 || next.Revision <= head.Revision ||
				next.AppendedPageCount <= head.AppendedPageCount ||
				!durableRequestHeadMatchesMeasurement(next, key, measurement) {
				return errors.Join(applyErr, readErr, ErrDurableRequestUnresolved)
			}
			head, applied = next, nextApplied
			return nil
		}))
	if err != nil {
		return requestledger.HeadRecord{}, 0, err
	}
	if head.Phase != requestledger.PhaseSealed || head.AppendedPageCount != head.PlanPageCount ||
		head.PageChain != head.PlanRoot {
		return requestledger.HeadRecord{}, 0, ErrDurableRequestUnresolved
	}
	return head, applied, nil
}

func (service *DurableRequestService) drive(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	head requestledger.HeadRecord,
	applied uint64,
) (DurableRequestOutcome, error) {
	for {
		outcome, err := service.driveOnce(ctx, home, key, head, applied)
		var advanced *durableRequestAdvancedError
		if !errors.As(err, &advanced) {
			return outcome, err
		}
		if ctx.Err() != nil {
			return DurableRequestOutcome{}, errors.Join(err, ctx.Err())
		}
		next, ack, nextApplied, readErr := service.openHead(ctx, home, key.RequestKey, max(applied, advanced.applied))
		if readErr == nil && ack.Revision != 0 {
			return DurableRequestOutcome{Acknowledged: true}, ErrDurableRequestAcknowledged
		}
		if readErr != nil || next.Key != head.Key || next.KeyDigest != head.KeyDigest ||
			next.RequestDigest != head.RequestDigest || next.PlanRoot != head.PlanRoot ||
			next.Revision < advanced.revision || next.Revision <= head.Revision {
			return DurableRequestOutcome{}, errors.Join(err, readErr, ErrDurableRequestUnresolved)
		}
		// Strict durable revision progress and the caller's deadline bound this
		// loop. Rebuild all execution state from the retained recipe, never from
		// the losing invocation's route pin or partially staged payload.
		head, applied = next, nextApplied
	}
}

func (service *DurableRequestService) driveOnce(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	head requestledger.HeadRecord,
	applied uint64,
) (DurableRequestOutcome, error) {
	if head.Phase == requestledger.PhaseTerminal {
		terminal, _, err := service.readTerminal(ctx, home, key.RequestKey, applied)
		if err != nil {
			return DurableRequestOutcome{}, err
		}
		if !durableRequestTerminalMatchesKey(terminal, key, head.PlanRoot) {
			return DurableRequestOutcome{}, fmt.Errorf("gateway: retained terminal key/plan mismatch: %w", ErrDurableRequestConflict)
		}
		retirer, ok := service.pins.(DurableRequestExecutionPinRetirer)
		if ok {
			if head.SchemaPinReleaseCertificateDigest == (requestledger.Digest{}) {
				return DurableRequestOutcome{}, ErrDurableRequestUnresolved
			}
			execution, executionErr := service.openTerminalExecution(ctx, home, key, head, applied)
			if executionErr != nil {
				return DurableRequestOutcome{}, errors.Join(executionErr, ErrDurableRequestUnresolved)
			}
			if retireErr := retirer.RetireTerminal(ctx, execution); retireErr != nil {
				return DurableRequestOutcome{}, errors.Join(retireErr, ErrDurableRequestUnresolved)
			}
		}
		return durableRequestTypedOutcome(terminal)
	}
	if head.Phase != requestledger.PhaseSealed && head.Phase != requestledger.PhasePrepared {
		return DurableRequestOutcome{}, ErrDurableRequestUnresolved
	}
	descriptor := DurableRequestPlanDescriptor{
		TotalBytes: head.TotalPlanBytes, Root: replication.Digest(head.PlanRoot),
	}
	if len(head.InlinePlan) != 0 {
		descriptor.Inline = head.InlinePlan
	} else if head.PlanPageCount > math.MaxUint32 {
		return DurableRequestOutcome{}, ErrDurableRequestBound
	} else {
		descriptor.PageCount = uint32(head.PlanPageCount)
	}
	source := durableRequestPlanPageSourceFunc(func(ordinal uint32) ([]byte, error) {
		row, err := service.ledger.ReadRow(ctx, home, DurableRequestLifecycleRead{
			Key: key.RequestKey, Kind: replicatedstate.RequestLedgerReadPlanPage,
			Ordinal: uint64(ordinal), MinimumApplied: max(uint64(1), applied),
		})
		if err != nil || !row.Found || row.Kind != replicatedstate.RequestLedgerReadPlanPage {
			return nil, errors.Join(err, ErrDurableRequestUnresolved)
		}
		return row.PlanPage.Data, nil
	})
	reader, err := openDurableRequestRecipeStream(key, descriptor, source)
	if err != nil {
		return DurableRequestOutcome{}, err
	}
	recipe := DurableRequestRecipe{
		CatalogGeneration: reader.CatalogGeneration, Identity: reader.Identity,
		Contract: reader.Contract, Tenant: bytes.Clone(reader.Tenant),
		KeyDigest: reader.KeyDigest, RequestID: reader.RequestID,
		RequestDigest: reader.RequestDigest, ParticipantCount: reader.ParticipantCount,
		ParticipantStream: reader, ResumeRevision: head.Revision,
	}
	execution, err := NewDurableRequestTypedExecutionContext(home, key, recipe)
	if err != nil {
		return DurableRequestOutcome{}, err
	}
	if head.Phase == requestledger.PhasePrepared {
		reader, ok := service.ledger.(durableRequestTerminalCutReader)
		if !ok {
			return DurableRequestOutcome{}, ErrDurableRequestUnresolved
		}
		cut, readErr := reader.ReadTerminalCut(ctx, home, key.RequestKey)
		if readErr != nil {
			return DurableRequestOutcome{}, readErr
		}
		if err = validateDurableRequestPreparedCut(execution, cut); err != nil {
			return DurableRequestOutcome{}, err
		}
		execution.terminalCut = &cut
	}
	route, acquire, lease, err := service.pins.AcquireOrRecover(ctx, execution)
	if err != nil {
		return DurableRequestOutcome{}, fmt.Errorf("gateway: execution pin acquire/recover: %w", err)
	}
	execution, err = BindDurableRequestExecutionPin(execution, route, acquire, lease)
	if err != nil {
		return DurableRequestOutcome{}, fmt.Errorf("gateway: execution pin binding: %w", err)
	}
	terminal, runErr := service.runner.RunTyped(ctx, execution)
	if runErr == nil {
		if !durableRequestTerminalMatchesKey(terminal.Terminal, key, head.PlanRoot) {
			return DurableRequestOutcome{}, ErrDurableRequestConflict
		}
		retirer, ok := service.pins.(DurableRequestExecutionPinRetirer)
		if !ok {
			return DurableRequestOutcome{}, ErrDurableRequestUnresolved
		}
		if retireErr := retirer.RetireTerminal(ctx, execution); retireErr != nil {
			return DurableRequestOutcome{}, errors.Join(retireErr, ErrDurableRequestUnresolved)
		}
		return durableRequestTypedOutcome(terminal.Terminal)
	}
	var advanced *durableRequestAdvancedError
	if errors.As(runErr, &advanced) {
		return DurableRequestOutcome{}, runErr
	}
	after, ack, nextApplied, readErr := service.openHead(ctx, home, key.RequestKey, applied)
	if readErr == nil && ack.Revision != 0 {
		return DurableRequestOutcome{Acknowledged: true}, ErrDurableRequestAcknowledged
	}
	if readErr == nil && after.Phase == requestledger.PhaseTerminal {
		return service.drive(ctx, home, key, after, nextApplied)
	}
	return DurableRequestOutcome{}, errors.Join(runErr, readErr, ErrDurableRequestUnresolved)
}

func (service *DurableRequestService) openTerminalExecution(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	head requestledger.HeadRecord,
	applied uint64,
) (DurableRequestTypedExecutionContext, error) {
	descriptor := DurableRequestPlanDescriptor{
		TotalBytes: head.TotalPlanBytes, Root: replication.Digest(head.PlanRoot),
	}
	if len(head.InlinePlan) != 0 {
		descriptor.Inline = head.InlinePlan
	} else if head.PlanPageCount > math.MaxUint32 {
		return DurableRequestTypedExecutionContext{}, ErrDurableRequestBound
	} else {
		descriptor.PageCount = uint32(head.PlanPageCount)
	}
	source := durableRequestPlanPageSourceFunc(func(ordinal uint32) ([]byte, error) {
		row, err := service.ledger.ReadRow(ctx, home, DurableRequestLifecycleRead{
			Key: key.RequestKey, Kind: replicatedstate.RequestLedgerReadPlanPage,
			Ordinal: uint64(ordinal), MinimumApplied: max(uint64(1), applied),
		})
		if err != nil || !row.Found || row.Kind != replicatedstate.RequestLedgerReadPlanPage {
			return nil, errors.Join(err, ErrDurableRequestUnresolved)
		}
		return row.PlanPage.Data, nil
	})
	reader, err := openDurableRequestRecipeStream(key, descriptor, source)
	if err != nil {
		return DurableRequestTypedExecutionContext{}, err
	}
	return NewDurableRequestTypedExecutionContext(home, key, DurableRequestRecipe{
		CatalogGeneration: reader.CatalogGeneration, Identity: reader.Identity,
		Contract: reader.Contract, Tenant: bytes.Clone(reader.Tenant),
		KeyDigest: reader.KeyDigest, RequestID: reader.RequestID,
		RequestDigest: reader.RequestDigest, ParticipantCount: reader.ParticipantCount,
		ParticipantStream: reader, ResumeRevision: head.Revision,
	})
}

func (service *DurableRequestService) readTerminal(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key requestledger.RequestKey,
	minimum uint64,
) (requestledger.TerminalRecord, uint64, error) {
	row, err := service.ledger.ReadRow(ctx, home, DurableRequestLifecycleRead{
		Key: key, Kind: replicatedstate.RequestLedgerReadTerminal,
		MinimumApplied: max(uint64(1), minimum),
	})
	if err != nil || !row.Found || row.Kind != replicatedstate.RequestLedgerReadTerminal {
		return requestledger.TerminalRecord{}, 0, errors.Join(err, ErrDurableRequestUnresolved)
	}
	return row.Terminal, row.Applied, nil
}

func durableRequestTerminalMatchesKey(
	terminal requestledger.TerminalRecord,
	key DurableRequestLedgerKey,
	planRoot requestledger.Digest,
) bool {
	keyDigest, err := requestledger.KeyDigest(key.RequestKey)
	return err == nil && planRoot != (requestledger.Digest{}) &&
		terminal.KeyDigest == keyDigest &&
		terminal.RequestDigest == requestledger.Digest(key.Digest) &&
		terminal.PlanRoot == planRoot
}

func durableRequestTypedOutcome(terminal requestledger.TerminalRecord) (DurableRequestOutcome, error) {
	result, err := OpenDurableRequestResult(terminal.Result)
	if err != nil || terminal.AckToken == (requestledger.AckToken{}) ||
		terminal.ResultDigest != requestledger.ResultDigest(terminal.Result) ||
		result.CatalogGeneration != terminal.CatalogGeneration ||
		result.TerminalContractDigest != replication.Digest(terminal.TerminalContractDigest) {
		return DurableRequestOutcome{}, errors.Join(err, ErrDurableRequestConflict)
	}
	return DurableRequestOutcome{
		ReplicatedTransactionResult: ReplicatedTransactionResult{
			ID: result.Transaction, Committed: result.Committed, AffectedRows: result.AffectedRows,
		},
		CatalogGeneration: result.CatalogGeneration, ShardsFanned: int(result.ShardsFanned),
		Result: bytes.Clone(result.Payload), TerminalRevision: terminal.Revision,
		ResultDigest: replication.Digest(terminal.ResultDigest),
		AckToken:     DurableRequestAckToken(terminal.AckToken),
	}, nil
}
