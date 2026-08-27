package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"math"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

var requestLedgerStateDigestDomain = []byte("vibedb/replicated-state/request-ledger-state\x00")
var errStopRequestLedgerGCScan = errors.New("replicatedstate: stop request-ledger GC scan")

type requestLedgerStateDelta struct {
	rows          int64
	residentBytes int64
	reservedBytes int64
	ackRows       int64
	ackBytes      int64
}

type requestLedgerCommandPlan struct {
	rows       []transactionRowMutation
	delta      requestLedgerStateDelta
	completion RequestLedgerCompletionResult
}

type requestLedgerRows struct {
	home              requestledger.LedgerHome
	headRaw           []byte
	pendingRaw        []byte
	continuationRaw   []byte
	terminalRaw       []byte
	ackRaw            []byte
	payloadBuildRaw   []byte
	routePinRaw       []byte
	preparedRaw       []byte
	schemaPinRaw      []byte
	head              requestledger.HeadRecord
	pending           requestledger.PendingWaveRecord
	continuation      requestledger.ContinuationRecord
	terminal          requestledger.TerminalRecord
	ack               requestledger.AckRecord
	payloadBuild      requestledger.PayloadBuildRecord
	routePin          requestledger.RoutePinRecord
	prepared          requestledger.PreparedTerminalRecord
	schemaPin         requestledger.SchemaPinReleaseRecord
	headFound         bool
	pendingFound      bool
	continuationFound bool
	terminalFound     bool
	ackFound          bool
	payloadBuildFound bool
	routePinFound     bool
	preparedFound     bool
	schemaPinFound    bool
}

func (m *Machine) planRequestLedgerCommand(
	outer replication.CommandView,
	state State,
	snapshot pointSnapshot,
) (requestLedgerCommandPlan, error) {
	plan := requestLedgerCommandPlan{}
	if outer.Kind() != replication.CommandRequestLedger ||
		!m.options.RequestLedgerRange.enabled() {
		return plan, ErrAdmissionBound
	}
	var stepScratch []requestledger.StepRef
	ledgerBytes := outer.RequestLedgerBytes()
	if len(ledgerBytes) > 8 && requestledger.Operation(ledgerBytes[8]) == requestledger.OperationPutPending {
		stepScratch = m.requestLedgerSteps[:]
	}
	command, err := outer.OpenRequestLedgerInto(stepScratch)
	if err != nil {
		return plan, err
	}
	plan.completion = RequestLedgerCompletionResult{
		Operation: command.Operation, ResultCode: ResultRequestLedgerConflict,
		KeyDigest: command.KeyDigest, RequestDigest: command.RequestDigest,
		PlanRoot: command.PlanRoot, RangeIdentity: command.ExpectedRangeIdentity,
	}
	if command.ExpectedRangeIdentity != m.options.RequestLedgerRange.Identity ||
		!m.options.RequestLedgerRange.contains(command.Home) {
		plan.completion.ResultCode = ResultRequestLedgerWrongRange
		return plan, nil
	}
	if command.Operation == requestledger.OperationAdvanceIssuerHighwater {
		return planRequestLedgerIssuerAdvance(plan, command, snapshot)
	}
	if command.Operation == requestledger.OperationOpenIssuerLane {
		return m.planRequestLedgerIssuerOpen(plan, command, state, snapshot)
	}
	storedPendingScratch := m.requestLedgerSteps[:]
	if command.Operation == requestledger.OperationPutPending {
		storedPendingScratch = nil
	}
	rows, err := readRequestLedgerRows(snapshot, command.Home, command.KeyDigest, storedPendingScratch)
	if err != nil {
		return plan, err
	}
	var key requestledger.RequestKey
	if command.Operation == requestledger.OperationCreate {
		head, ok := command.Head()
		if !ok {
			return plan, ErrStateCorrupt
		}
		key = head.Key
	} else if rows.ackFound {
		key = rows.ack.Key
	} else if rows.headFound {
		key = rows.head.Key
	} else {
		// Absence is a normal CAS conflict. There is no full authenticated key
		// from which to manufacture range authority.
		plan.completion.ResultCode = ResultRequestLedgerNotFound
		return plan, nil
	}
	home, err := requestledger.Home(key)
	if err != nil {
		return plan, ErrStateCorrupt
	}
	if home != command.Home {
		plan.completion.ResultCode = ResultRequestLedgerWrongRange
		return plan, nil
	}
	if rows.ackFound {
		if rows.ack.KeyDigest != command.KeyDigest || rows.ack.RequestDigest != command.RequestDigest ||
			rows.ack.PlanRoot != command.PlanRoot {
			return witnessedRequestLedgerConflict(plan, rows.ack.Revision, requestledger.PhaseAcked, rows.ackRaw), nil
		}
		if command.Operation == requestledger.OperationGC {
			return planRequestLedgerGC(plan, command, rows, snapshot)
		}
		if command.Operation == requestledger.OperationAck {
			request, ok := command.AckRequest()
			if ok && command.Revision == rows.ack.Revision &&
				request.TerminalRevision == rows.ack.TerminalRevision &&
				request.ResultDigest == rows.ack.ResultDigest &&
				requestledger.AckTokenDigest(request.AckToken) == rows.ack.AckTokenDigest {
				return witnessedRequestLedgerApplied(
					plan, rows.ack.Revision, requestledger.PhaseAcked, rows.ackRaw, true,
				), nil
			}
		}
		return witnessedRequestLedgerConflict(plan, rows.ack.Revision, requestledger.PhaseAcked, rows.ackRaw), nil
	}
	if command.Operation == requestledger.OperationCreate {
		return m.planRequestLedgerCreate(plan, command, state, rows, snapshot)
	}
	if !rows.headFound || rows.head.KeyDigest != command.KeyDigest ||
		rows.head.RequestDigest != command.RequestDigest || rows.head.PlanRoot != command.PlanRoot {
		if rows.headFound {
			return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
		}
		return plan, nil
	}

	switch command.Operation {
	case requestledger.OperationAppendPages:
		return planRequestLedgerAppendPages(plan, command, rows, snapshot)
	case requestledger.OperationSeal:
		return planRequestLedgerSeal(plan, command, rows, snapshot)
	case requestledger.OperationPutPending:
		return planRequestLedgerPutPending(plan, command, rows)
	case requestledger.OperationAdvance:
		return planRequestLedgerAdvance(plan, command, rows)
	case requestledger.OperationComplete:
		return planRequestLedgerComplete(plan, command, rows)
	case requestledger.OperationAck:
		return planRequestLedgerAck(plan, command, rows, snapshot)
	case requestledger.OperationGC:
		return planRequestLedgerGC(plan, command, rows, snapshot)
	case requestledger.OperationBeginPayloadBuild:
		return planRequestLedgerBeginPayload(plan, command, rows)
	case requestledger.OperationStagePayloadChunk:
		return planRequestLedgerStagePayload(plan, command, rows, snapshot)
	case requestledger.OperationSealPayload:
		return planRequestLedgerSealPayload(plan, command, rows)
	case requestledger.OperationCleanupPayload:
		return planRequestLedgerCleanupPayload(plan, command, rows, snapshot)
	case requestledger.OperationPrepareTerminal:
		return planRequestLedgerPrepareTerminal(plan, command, rows)
	case requestledger.OperationBeginSchemaPinRelease:
		return planRequestLedgerBeginSchemaPinRelease(plan, command, rows, snapshot)
	case requestledger.OperationRecordSchemaPinReleased:
		return planRequestLedgerRecordSchemaPinReleased(plan, command, rows)
	case requestledger.OperationBeginRoutePinAcquire,
		requestledger.OperationRecordRoutePinAcquired,
		requestledger.OperationBeginRoutePinRelease,
		requestledger.OperationRecordRoutePinReleased:
		return planRequestLedgerRoutePin(plan, command, rows)
	case requestledger.OperationExpirePlanning:
		return planRequestLedgerExpirePlanning(plan, command, rows, state, snapshot)
	case requestledger.OperationRestartPlanning:
		return m.planRequestLedgerRestartPlanning(plan, command, rows, state, snapshot)
	case requestledger.OperationCleanupPlanning:
		return planRequestLedgerCleanupPlanning(plan, command, rows, snapshot)
	default:
		// The inner decoder has already accepted the canonical operation. A
		// newer-but-not-yet-integrated lifecycle operation must fail closed as
		// a durable CAS conflict; it must never poison every replica merely by
		// reaching the apply switch.
		return witnessedRequestLedgerConflict(
			plan, rows.head.Revision, rows.head.Phase, rows.headRaw,
		), nil
	}
}

func planRequestLedgerExpirePlanning(
	plan requestLedgerCommandPlan,
	command requestledger.CommandView,
	rows requestLedgerRows,
	state State,
	snapshot pointSnapshot,
) (requestLedgerCommandPlan, error) {
	request, _ := command.PlanningExpiry()
	if rows.head.Revision == command.Revision && rows.head.Phase == requestledger.PhaseExpired &&
		rows.head.PlanBuildID == request.PlanBuildID &&
		rows.head.PlanBuildGeneration == request.BuildGeneration {
		return witnessedRequestLedgerApplied(plan, rows.head.Revision, rows.head.Phase, rows.headRaw, true), nil
	}
	if rows.head.Revision != command.ExpectedRevision || rows.head.Phase != requestledger.PhasePlanning ||
		state.Applied == math.MaxUint64 || request.ObservedAppliedIndex > state.Applied+1 ||
		state.Applied+1 < rows.head.PlanningLeaseExpiryIndex {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	next, err := requestledger.MarkPlanningExpired(rows.head, request, command.Revision)
	if err != nil {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	materialized := materializedRequestLedgerRows(rows)
	beforeReserved, ok := requestLedgerReservedBytes(rows.head, materialized)
	if !ok {
		return plan, ErrStateCorrupt
	}
	afterReserved, ok := requestLedgerReservedBytes(next, materialized)
	if !ok {
		return plan, ErrStateCorrupt
	}
	plan.delta.reservedBytes += int64(afterReserved) - int64(beforeReserved)
	plan, err = deleteRequestLedgerPlanningExpiry(plan, rows.head, rows.home, snapshot)
	if err != nil {
		return plan, err
	}
	return replaceRequestLedgerHead(plan, rows, next)
}

func (m *Machine) planRequestLedgerRestartPlanning(
	plan requestLedgerCommandPlan,
	command requestledger.CommandView,
	rows requestLedgerRows,
	state State,
	snapshot pointSnapshot,
) (requestLedgerCommandPlan, error) {
	request, _ := command.PlanningRestart()
	if rows.head.Revision == command.Revision && rows.head.Phase == requestledger.PhasePlanning &&
		rows.head.PlanBuildGeneration == request.NextGeneration &&
		rows.head.PlanBuildID == request.NextPlanBuildID &&
		rows.head.PlanningLeaseExpiryIndex == request.NextLeaseExpiryIndex {
		return witnessedRequestLedgerApplied(plan, rows.head.Revision, rows.head.Phase, rows.headRaw, true), nil
	}
	if rows.head.Revision != command.ExpectedRevision || rows.head.Phase != requestledger.PhaseExpired ||
		state.Applied >= math.MaxUint64-rows.head.PlanningLeaseSpan-1 ||
		request.ObservedAppliedIndex > state.Applied+1 ||
		request.NextLeaseExpiryIndex != state.Applied+1+rows.head.PlanningLeaseSpan {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	next, err := requestledger.RestartPlanning(rows.head, request, command.Revision)
	if err != nil {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	materialized := materializedRequestLedgerRows(rows)
	beforeReserved, ok := requestLedgerReservedBytes(rows.head, materialized)
	if !ok {
		return plan, ErrStateCorrupt
	}
	afterReserved, ok := requestLedgerReservedBytes(next, materialized)
	if !ok || afterReserved < beforeReserved {
		return plan, ErrStateCorrupt
	}
	additional := afterReserved - beforeReserved
	consumed, ok := checkedRequestLedgerConsumption(
		state.RequestLedgerResidentBytes, state.RequestLedgerReservedBytes, additional,
	)
	ordinaryCapacity := m.options.RequestLedgerCapacityBytes - m.options.RequestLedgerCleanupReserveBytes
	if !ok || consumed > ordinaryCapacity {
		// Restart is a durable-state CAS, not a stateless Create. Leave the
		// compact expired fence intact and let the controller retry after
		// capacity is reclaimed.
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	plan.delta.reservedBytes += int64(additional)
	plan, err = putRequestLedgerPlanningExpiry(plan, next, rows.home, snapshot, false)
	if err != nil {
		return plan, err
	}
	return replaceRequestLedgerHead(plan, rows, next)
}

func planRequestLedgerCleanupPlanning(
	plan requestLedgerCommandPlan,
	command requestledger.CommandView,
	rows requestLedgerRows,
	snapshot pointSnapshot,
) (requestLedgerCommandPlan, error) {
	request, _ := command.PlanningCleanup()
	if rows.head.Revision == command.Revision && rows.head.Phase == requestledger.PhaseExpired &&
		rows.head.PlanBuildGeneration == request.BuildGeneration && rows.head.PlanBuildID == request.PlanBuildID {
		return witnessedRequestLedgerApplied(plan, rows.head.Revision, rows.head.Phase, rows.headRaw, true), nil
	}
	if rows.head.Revision != command.ExpectedRevision || rows.head.Phase != requestledger.PhaseExpired {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	chunk, err := requestledger.PlanPlanningCleanup(rows.head, request)
	if err != nil {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	var reclaimed uint64
	for offset := uint64(0); offset < chunk.PageCount; offset++ {
		ordinal := chunk.FirstOrdinal + offset
		key := requestledger.AppendPlanPageKey(nil, command.Home, command.KeyDigest, ordinal)
		raw, found, readErr := snapshot.appendRaw(nil, key)
		if readErr != nil {
			return plan, readErr
		}
		page, openErr := requestledger.OpenPlanPage(raw)
		if !found || openErr != nil || page.KeyDigest != command.KeyDigest ||
			page.PlanRoot != rows.head.PlanRoot || page.Ordinal != ordinal ||
			page.Count != rows.head.PlanPageCount {
			return plan, errors.Join(openErr, ErrStateCorrupt)
		}
		rowBytes := uint64(len(key) + len(raw))
		if reclaimed > math.MaxUint64-rowBytes {
			return plan, ErrStateCorrupt
		}
		reclaimed += rowBytes
		plan.rows = append(plan.rows, newTransactionDelete(key))
		plan.delta.rows--
	}
	if reclaimed != chunk.ReclaimedBytes {
		return plan, ErrStateCorrupt
	}
	next, err := requestledger.AdvancePlanningCleanup(rows.head, request, chunk, command.Revision)
	if err != nil {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	plan.delta.residentBytes -= int64(reclaimed)
	return replaceRequestLedgerHead(plan, rows, next)
}

func planRequestLedgerRoutePin(
	plan requestLedgerCommandPlan,
	command requestledger.CommandView,
	rows requestLedgerRows,
) (requestLedgerCommandPlan, error) {
	desired, _ := command.RoutePin()
	if rows.head.Revision == command.Revision && rows.routePinFound &&
		bytes.Equal(rows.routePinRaw, command.Payload) {
		return witnessedRequestLedgerApplied(plan, rows.head.Revision, rows.head.Phase, rows.routePinRaw, true), nil
	}
	if rows.head.Revision != command.ExpectedRevision || rows.head.Phase != requestledger.PhaseSealed ||
		rows.pendingFound || rows.preparedFound || rows.schemaPinFound {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	var next requestledger.HeadRecord
	var transitionErr error
	switch command.Operation {
	case requestledger.OperationBeginRoutePinAcquire:
		if rows.head.OutstandingRoutePinDigest != (requestledger.Digest{}) ||
			rows.routePinFound && (rows.routePin.Phase != requestledger.RoutePinReleased ||
				rows.routePin.WaveOrdinal == ^uint64(0) || rows.routePin.WaveOrdinal+1 != rows.head.NextStepOrdinal) ||
			!requestLedgerRouteCommandEvidenceAvailable(requestledger.RoutePinRecord{}, desired) {
			return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
		}
		next, transitionErr = requestledger.AdvanceHeadRoutePin(
			rows.head, requestledger.RoutePinRecord{}, desired, command.Revision,
		)
	case requestledger.OperationRecordRoutePinAcquired:
		if !rows.routePinFound || rows.routePin.Phase != requestledger.RoutePinAcquiring ||
			!requestLedgerRouteCompletionEvidenceAvailable(rows.routePin, desired) {
			return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
		}
		next, transitionErr = requestledger.AdvanceHeadRoutePin(
			rows.head, rows.routePin, desired, command.Revision,
		)
	case requestledger.OperationBeginRoutePinRelease:
		if !rows.routePinFound || rows.routePin.Phase != requestledger.RoutePinAcquired ||
			rows.head.OutstandingRoutePinDigest != rows.routePin.AcquiredEvidenceDigest ||
			!requestLedgerRouteCommandEvidenceAvailable(rows.routePin, desired) {
			return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
		}
		next, transitionErr = requestledger.AdvanceHeadRoutePin(
			rows.head, rows.routePin, desired, command.Revision,
		)
	case requestledger.OperationRecordRoutePinReleased:
		if !rows.routePinFound || rows.routePin.Phase != requestledger.RoutePinReleasing ||
			!requestLedgerRouteCompletionEvidenceAvailable(rows.routePin, desired) {
			return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
		}
		next, transitionErr = requestledger.MarkRoutePinReleased(rows.head, desired, command.Revision)
	default:
		return plan, ErrStateCorrupt
	}
	if transitionErr != nil {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	materialized := materializedRequestLedgerRows(rows)
	beforeReserved, ok := requestLedgerReservedBytes(rows.head, materialized)
	if !ok {
		return plan, ErrStateCorrupt
	}
	materialized.routePinBytes = len(command.Payload)
	afterReserved, ok := requestLedgerReservedBytes(next, materialized)
	if !ok {
		return plan, ErrStateCorrupt
	}
	key := requestledger.AppendRoutePinKey(nil, command.Home, command.KeyDigest)
	plan.rows = append(plan.rows, newTransactionPut(key, command.Payload))
	if rows.routePinFound {
		plan.delta.residentBytes += int64(len(command.Payload) - len(rows.routePinRaw))
	} else {
		plan.delta.rows++
		plan.delta.residentBytes += int64(len(key) + len(command.Payload))
	}
	plan.delta.reservedBytes += int64(afterReserved) - int64(beforeReserved)
	return replaceRequestLedgerHead(plan, rows, next)
}

func (m *Machine) planRequestLedgerCreate(
	plan requestLedgerCommandPlan,
	command requestledger.CommandView,
	state State,
	rows requestLedgerRows,
	snapshot pointSnapshot,
) (requestLedgerCommandPlan, error) {
	head, _ := command.Head()
	if rows.headFound {
		if rows.head.Revision == 1 && requestledger.SameCreateBytes(rows.headRaw, command.Payload) {
			plan.completion.PlanningLeaseExpiryIndex = rows.head.PlanningLeaseExpiryIndex
			return witnessedRequestLedgerApplied(plan, rows.head.Revision, rows.head.Phase, rows.headRaw, true), nil
		}
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	if rows.pendingFound || rows.continuationFound || rows.terminalFound {
		return plan, ErrStateCorrupt
	}
	if state.Applied == math.MaxUint64 {
		plan.completion.ResultCode = ResultRequestLedgerCapacity
		plan.completion.Phase = requestledger.PhaseInvalid
		return plan, nil
	}
	materialized, materializeErr := requestledger.MaterializeCreate(head, state.Applied+1)
	if materializeErr != nil || materialized.PlanningLeaseGeneration != materialized.PlanBuildGeneration {
		plan.completion.ResultCode = ResultRequestLedgerCapacity
		plan.completion.Phase = requestledger.PhaseInvalid
		return plan, nil
	}
	head = materialized
	encoded, err := requestledger.AppendHead(nil, head)
	if err != nil {
		return plan, err
	}
	key := requestledger.AppendHeadKey(nil, command.Home, head.KeyDigest)
	resident := uint64(len(key) + len(encoded))
	var expiryKey, expiryRaw []byte
	if head.Phase == requestledger.PhasePlanning {
		expiry, expiryErr := requestledger.NewPlanningExpiryIndexRecord(head)
		if expiryErr != nil {
			return plan, errors.Join(expiryErr, ErrStateCorrupt)
		}
		expiryKey = requestledger.AppendPlanningExpiryKey(nil, expiry.ExpiryAppliedIndex, command.Home, head.KeyDigest)
		expiryRaw, expiryErr = requestledger.AppendPlanningExpiryIndex(nil, expiry)
		if expiryErr != nil || resident > math.MaxUint64-uint64(len(expiryKey)+len(expiryRaw)) {
			return plan, errors.Join(expiryErr, ErrAdmissionBound)
		}
		resident += uint64(len(expiryKey) + len(expiryRaw))
	}
	_, reserved, reservationErr := requestledger.Reservation(head)
	if reservationErr != nil || resident > math.MaxUint64-reserved {
		return plan, ErrAdmissionBound
	}
	consumed, ok := checkedRequestLedgerConsumption(state.RequestLedgerResidentBytes,
		state.RequestLedgerReservedBytes, resident+reserved)
	ordinaryCapacity := m.options.RequestLedgerCapacityBytes - m.options.RequestLedgerCleanupReserveBytes
	if !ok || consumed > ordinaryCapacity {
		plan.completion.ResultCode = ResultRequestLedgerCapacity
		plan.completion.Phase = requestledger.PhaseInvalid
		return plan, nil
	}
	plan.rows = append(plan.rows, newTransactionPut(key, encoded))
	if len(expiryKey) != 0 {
		plan.rows = append(plan.rows, newTransactionPut(expiryKey, expiryRaw))
	}
	plan.delta = requestLedgerStateDelta{rows: int64(len(plan.rows)), residentBytes: int64(resident), reservedBytes: int64(reserved)}
	plan = witnessedRequestLedgerApplied(plan, head.Revision, head.Phase, encoded, false)
	plan.completion.PlanningLeaseExpiryIndex = head.PlanningLeaseExpiryIndex
	return m.planRequestLedgerSequencedCreate(plan, command, head, state, snapshot)
}

func planRequestLedgerAppendPages(
	plan requestLedgerCommandPlan,
	command requestledger.CommandView,
	rows requestLedgerRows,
	snapshot pointSnapshot,
) (requestLedgerCommandPlan, error) {
	pages, _ := command.Pages()
	if rows.head.Revision == command.Revision {
		// Exact duplicate proof is the already-published head chain plus every
		// immutable page byte in this batch. A changed page at one revision is a
		// conflict, never a silent duplicate.
		iter := pages.Iter()
		for {
			page, _, present := iter.Next()
			if !present {
				break
			}
			encoded, err := requestledger.AppendPlanPage(nil, page)
			if err != nil {
				return plan, err
			}
			key := requestledger.AppendPlanPageKey(nil, command.Home, command.KeyDigest, page.Ordinal)
			stored, found, err := snapshot.appendRaw(nil, key)
			if err != nil {
				return plan, err
			}
			if !found || !bytes.Equal(stored, encoded) {
				return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
			}
		}
		if command.Seal != (rows.head.Phase == requestledger.PhaseSealed) {
			return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
		}
		return witnessedRequestLedgerApplied(plan, rows.head.Revision, rows.head.Phase, rows.headRaw, true), nil
	}
	if rows.head.Revision != command.ExpectedRevision || rows.head.Phase != requestledger.PhasePlanning {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	next, err := requestledger.AdvanceHeadPageBatch(rows.head, pages, command.Revision, command.Seal)
	if err != nil {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	beforeReserved, ok := requestLedgerReservedBytes(rows.head, requestLedgerMaterialized{})
	if !ok {
		return plan, ErrStateCorrupt
	}
	afterReserved, ok := requestLedgerReservedBytes(next, requestLedgerMaterialized{})
	if !ok {
		return plan, ErrStateCorrupt
	}
	iter := pages.Iter()
	for added := uint64(0); added < pages.Count(); added++ {
		page, _, present := iter.Next()
		if !present {
			return plan, ErrStateCorrupt
		}
		encoded, err := requestledger.AppendPlanPage(nil, page)
		if err != nil {
			return plan, err
		}
		key := requestledger.AppendPlanPageKey(nil, command.Home, command.KeyDigest, page.Ordinal)
		_, found, err := snapshot.appendRaw(nil, key)
		if err != nil {
			return plan, err
		}
		if found {
			return plan, ErrStateCorrupt
		}
		plan.rows = append(plan.rows, newTransactionPut(key, encoded))
		plan.delta.rows++
		plan.delta.residentBytes += int64(len(key) + len(encoded))
	}
	plan.delta.reservedBytes += int64(afterReserved) - int64(beforeReserved)
	return replaceRequestLedgerHead(plan, rows, next)
}

func planRequestLedgerSeal(plan requestLedgerCommandPlan, command requestledger.CommandView, rows requestLedgerRows, snapshot pointSnapshot) (requestLedgerCommandPlan, error) {
	if rows.head.Revision == command.Revision && rows.head.Phase == requestledger.PhaseSealed {
		return witnessedRequestLedgerApplied(plan, rows.head.Revision, rows.head.Phase, rows.headRaw, true), nil
	}
	if rows.head.Revision != command.ExpectedRevision {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	next, err := requestledger.SealHead(rows.head, command.Revision)
	if err != nil {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	plan, err = deleteRequestLedgerPlanningExpiry(plan, rows.head, rows.home, snapshot)
	if err != nil {
		return plan, err
	}
	return replaceRequestLedgerHead(plan, rows, next)
}

func deleteRequestLedgerPlanningExpiry(
	plan requestLedgerCommandPlan,
	head requestledger.HeadRecord,
	home requestledger.LedgerHome,
	snapshot pointSnapshot,
) (requestLedgerCommandPlan, error) {
	record, err := requestledger.NewPlanningExpiryIndexRecord(head)
	if err != nil {
		return plan, errors.Join(err, ErrStateCorrupt)
	}
	key := requestledger.AppendPlanningExpiryKey(nil, record.ExpiryAppliedIndex, home, head.KeyDigest)
	raw, found, err := snapshot.appendRaw(nil, key)
	if err != nil || !found {
		return plan, errors.Join(err, ErrStateCorrupt)
	}
	stored, openErr := requestledger.OpenPlanningExpiryIndex(raw)
	if openErr != nil || stored != record {
		return plan, errors.Join(openErr, ErrStateCorrupt)
	}
	plan.rows = append(plan.rows, newTransactionDelete(key))
	plan.delta.rows--
	plan.delta.residentBytes -= int64(len(key) + len(raw))
	return plan, nil
}

func putRequestLedgerPlanningExpiry(
	plan requestLedgerCommandPlan,
	head requestledger.HeadRecord,
	home requestledger.LedgerHome,
	snapshot pointSnapshot,
	exactDuplicate bool,
) (requestLedgerCommandPlan, error) {
	record, err := requestledger.NewPlanningExpiryIndexRecord(head)
	if err != nil {
		return plan, errors.Join(err, ErrStateCorrupt)
	}
	key := requestledger.AppendPlanningExpiryKey(nil, record.ExpiryAppliedIndex, home, head.KeyDigest)
	if _, found, readErr := snapshot.appendRaw(nil, key); readErr != nil || found != exactDuplicate {
		return plan, errors.Join(readErr, ErrStateCorrupt)
	}
	raw, err := requestledger.AppendPlanningExpiryIndex(nil, record)
	if err != nil {
		return plan, err
	}
	plan.rows = append(plan.rows, newTransactionPut(key, raw))
	plan.delta.rows++
	plan.delta.residentBytes += int64(len(key) + len(raw))
	return plan, nil
}

func planRequestLedgerPutPending(plan requestLedgerCommandPlan, command requestledger.CommandView, rows requestLedgerRows) (requestLedgerCommandPlan, error) {
	pendingView, _ := command.Pending()
	pending := pendingView.Record()
	if rows.head.Revision == command.Revision && rows.pendingFound && bytes.Equal(rows.pendingRaw, pendingView.Bytes()) {
		return witnessedRequestLedgerApplied(plan, rows.head.Revision, rows.head.Phase, rows.pendingRaw, true), nil
	}
	if rows.head.Revision != command.ExpectedRevision || rows.pendingFound || rows.head.Phase != requestledger.PhaseSealed {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	if !rows.routePinFound || rows.routePin.Phase != requestledger.RoutePinAcquired ||
		rows.routePin.AcquiredEvidenceDigest != pending.RoutePinDigest {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	if pending.PayloadBuildDigest == (requestledger.Digest{}) {
		if rows.payloadBuildFound {
			return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
		}
	} else if !rows.payloadBuildFound || rows.payloadBuild.Phase != requestledger.PayloadBuildSealed ||
		rows.payloadBuild.BuildDigest != pending.PayloadBuildDigest ||
		rows.payloadBuild.KeyDigest != rows.head.KeyDigest ||
		rows.payloadBuild.RequestDigest != rows.head.RequestDigest ||
		rows.payloadBuild.PlanRoot != rows.head.PlanRoot ||
		rows.payloadBuild.PriorContinuationDigest != rows.head.ContinuationDigest ||
		rows.payloadBuild.WaveOrdinal != rows.head.NextStepOrdinal ||
		!requestLedgerPendingDynamicBounds(pending, rows.payloadBuild) {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	next, err := requestledger.InstallPendingWave(
		rows.head, pending, rows.payloadBuild, rows.routePin,
	)
	if err != nil {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	materialized := requestLedgerMaterialized{
		continuationBytes: len(rows.continuationRaw), routePinBytes: len(rows.routePinRaw),
	}
	beforeReserved, ok := requestLedgerReservedBytes(rows.head, materialized)
	if !ok {
		return plan, ErrStateCorrupt
	}
	materialized.pendingBytes = len(pendingView.Bytes())
	afterReserved, ok := requestLedgerReservedBytes(next, materialized)
	if !ok {
		return plan, ErrStateCorrupt
	}
	key := requestledger.AppendPendingKey(nil, command.Home, pending.KeyDigest)
	plan.rows = append(plan.rows, newTransactionPut(key, pendingView.Bytes()))
	plan.delta.rows++
	plan.delta.residentBytes += int64(len(key) + len(pendingView.Bytes()))
	plan.delta.reservedBytes += int64(afterReserved) - int64(beforeReserved)
	return replaceRequestLedgerHead(plan, rows, next)
}

func planRequestLedgerBeginPayload(plan requestLedgerCommandPlan, command requestledger.CommandView, rows requestLedgerRows) (requestLedgerCommandPlan, error) {
	build, _ := command.PayloadBuild()
	if rows.payloadBuildFound {
		if bytes.Equal(rows.payloadBuildRaw, command.Payload) {
			return witnessedRequestLedgerApplied(plan, rows.payloadBuild.Revision, rows.head.Phase, rows.payloadBuildRaw, true), nil
		}
		return witnessedRequestLedgerConflict(plan, rows.payloadBuild.Revision, rows.head.Phase, rows.payloadBuildRaw), nil
	}
	if rows.head.Phase != requestledger.PhaseSealed || rows.pendingFound ||
		build.KeyDigest != rows.head.KeyDigest || build.RequestDigest != rows.head.RequestDigest ||
		build.PlanRoot != rows.head.PlanRoot || build.PriorContinuationDigest != rows.head.ContinuationDigest ||
		build.WaveOrdinal != rows.head.NextStepOrdinal || build.Phase != requestledger.PayloadBuildStaging ||
		build.Revision != command.Revision || build.TotalBytes > rows.head.MaxActivePayloadBytes ||
		build.ChunkCount > rows.head.MaxActivePayloadChunks {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	key := requestledger.AppendPayloadBuildKey(nil, command.Home, command.KeyDigest)
	rowBytes := len(key) + len(command.Payload)
	plan.rows = append(plan.rows, newTransactionPut(key, command.Payload))
	plan.delta.rows++
	plan.delta.residentBytes += int64(rowBytes)
	plan.delta.reservedBytes -= int64(rowBytes)
	return witnessedRequestLedgerApplied(plan, build.Revision, rows.head.Phase, command.Payload, false), nil
}

func planRequestLedgerStagePayload(plan requestLedgerCommandPlan, command requestledger.CommandView, rows requestLedgerRows, snapshot pointSnapshot) (requestLedgerCommandPlan, error) {
	chunk, _ := command.PayloadChunk()
	chunkKey := requestledger.AppendPayloadChunkKey(nil, command.Home, command.KeyDigest, chunk.ContentRoot, chunk.Ordinal)
	storedChunk, chunkFound, err := snapshot.appendRaw(nil, chunkKey)
	if err != nil {
		return plan, err
	}
	if rows.payloadBuildFound && rows.payloadBuild.Revision == command.Revision {
		if chunkFound && bytes.Equal(storedChunk, command.Payload) &&
			rows.payloadBuild.BuildDigest == chunk.BuildDigest &&
			rows.payloadBuild.NextChunkOrdinal == chunk.Ordinal+1 && rows.payloadBuild.Chain == chunk.Chain {
			return witnessedRequestLedgerApplied(plan, rows.payloadBuild.Revision, rows.head.Phase, rows.payloadBuildRaw, true), nil
		}
		return witnessedRequestLedgerConflict(plan, rows.payloadBuild.Revision, rows.head.Phase, rows.payloadBuildRaw), nil
	}
	if !rows.payloadBuildFound || chunkFound || rows.payloadBuild.Revision != command.ExpectedRevision ||
		rows.payloadBuild.BuildDigest != command.SubjectDigest {
		if rows.payloadBuildFound {
			return witnessedRequestLedgerConflict(plan, rows.payloadBuild.Revision, rows.head.Phase, rows.payloadBuildRaw), nil
		}
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	next, err := requestledger.AdvancePayloadBuild(rows.payloadBuild, chunk, command.Revision)
	if err != nil {
		return witnessedRequestLedgerConflict(plan, rows.payloadBuild.Revision, rows.head.Phase, rows.payloadBuildRaw), nil
	}
	nextRaw, err := requestledger.AppendPayloadBuild(nil, next)
	if err != nil {
		return plan, err
	}
	buildKey := requestledger.AppendPayloadBuildKey(nil, command.Home, command.KeyDigest)
	rowBytes := len(chunkKey) + len(command.Payload)
	plan.rows = append(plan.rows, newTransactionPut(chunkKey, command.Payload), newTransactionPut(buildKey, nextRaw))
	plan.delta.rows++
	plan.delta.residentBytes += int64(rowBytes + len(nextRaw) - len(rows.payloadBuildRaw))
	plan.delta.reservedBytes -= int64(rowBytes)
	return witnessedRequestLedgerApplied(plan, next.Revision, rows.head.Phase, nextRaw, false), nil
}

func planRequestLedgerSealPayload(plan requestLedgerCommandPlan, command requestledger.CommandView, rows requestLedgerRows) (requestLedgerCommandPlan, error) {
	desired, _ := command.PayloadBuild()
	if rows.payloadBuildFound && rows.payloadBuild.Revision == command.Revision {
		if bytes.Equal(rows.payloadBuildRaw, command.Payload) {
			return witnessedRequestLedgerApplied(plan, rows.payloadBuild.Revision, rows.head.Phase, rows.payloadBuildRaw, true), nil
		}
		return witnessedRequestLedgerConflict(plan, rows.payloadBuild.Revision, rows.head.Phase, rows.payloadBuildRaw), nil
	}
	if !rows.payloadBuildFound || rows.payloadBuild.Revision != command.ExpectedRevision ||
		rows.payloadBuild.BuildDigest != command.SubjectDigest {
		if rows.payloadBuildFound {
			return witnessedRequestLedgerConflict(plan, rows.payloadBuild.Revision, rows.head.Phase, rows.payloadBuildRaw), nil
		}
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	next, err := requestledger.SealPayloadBuild(rows.payloadBuild, command.Revision)
	if err != nil || next != desired {
		return witnessedRequestLedgerConflict(plan, rows.payloadBuild.Revision, rows.head.Phase, rows.payloadBuildRaw), nil
	}
	key := requestledger.AppendPayloadBuildKey(nil, command.Home, command.KeyDigest)
	plan.rows = append(plan.rows, newTransactionPut(key, command.Payload))
	plan.delta.residentBytes += int64(len(command.Payload) - len(rows.payloadBuildRaw))
	return witnessedRequestLedgerApplied(plan, next.Revision, rows.head.Phase, command.Payload, false), nil
}

func requestLedgerPendingDynamicBounds(pending requestledger.PendingWaveRecord, build requestledger.PayloadBuildRecord) bool {
	for i := range pending.Steps {
		step := &pending.Steps[i]
		if step.TargetSource == requestledger.PayloadSourceDynamic &&
			(step.TargetOffset >= build.TotalBytes || step.TargetLength > build.TotalBytes-step.TargetOffset) {
			return false
		}
		if step.CommandSource == requestledger.PayloadSourceDynamic &&
			(step.CommandOffset >= build.TotalBytes || step.CommandLength > build.TotalBytes-step.CommandOffset) {
			return false
		}
	}
	return true
}

func planRequestLedgerAdvance(plan requestLedgerCommandPlan, command requestledger.CommandView, rows requestLedgerRows) (requestLedgerCommandPlan, error) {
	continuation, _ := command.Continuation()
	if rows.head.Revision == command.Revision && !rows.pendingFound && rows.continuationFound &&
		requestledger.SameContinuation(rows.continuation, continuation) {
		return witnessedRequestLedgerApplied(plan, rows.head.Revision, rows.head.Phase, rows.continuationRaw, true), nil
	}
	if rows.head.Revision != command.ExpectedRevision || !rows.pendingFound ||
		!rows.routePinFound || rows.routePin.Phase != requestledger.RoutePinAcquired ||
		rows.routePin.AcquiredEvidenceDigest != rows.pending.RoutePinDigest {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	var next requestledger.HeadRecord
	var err error
	if rows.pending.PayloadBuildDigest != (requestledger.Digest{}) {
		if !rows.payloadBuildFound || rows.payloadBuild.BuildDigest != rows.pending.PayloadBuildDigest {
			return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
		}
		next, err = requestledger.AdvancePendingWithBuild(
			rows.head, rows.pending, continuation, rows.payloadBuild,
		)
	} else {
		if rows.payloadBuildFound {
			return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
		}
		next, err = requestledger.AdvancePending(rows.head, rows.pending, continuation)
	}
	if err != nil {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	payloadResident := next.CleanupPayloadBytes
	materialized := requestLedgerMaterialized{
		pendingBytes: len(rows.pendingRaw), continuationBytes: len(rows.continuationRaw),
		routePinBytes: len(rows.routePinRaw), payloadResident: payloadResident,
	}
	beforeReserved, ok := requestLedgerReservedBytes(rows.head, materialized)
	if !ok {
		return plan, ErrStateCorrupt
	}
	materialized.pendingBytes = 0
	materialized.continuationBytes = len(command.Payload)
	afterReserved, ok := requestLedgerReservedBytes(next, materialized)
	if !ok {
		return plan, ErrStateCorrupt
	}
	pendingKey := requestledger.AppendPendingKey(nil, command.Home, command.KeyDigest)
	continuationKey := requestledger.AppendContinuationKey(nil, command.Home, command.KeyDigest)
	plan.rows = append(plan.rows, newTransactionDelete(pendingKey), newTransactionPut(continuationKey, command.Payload))
	plan.delta.rows--
	plan.delta.residentBytes -= int64(len(pendingKey) + len(rows.pendingRaw))
	if rows.continuationFound {
		plan.delta.residentBytes += int64(len(command.Payload) - len(rows.continuationRaw))
	} else {
		plan.delta.rows++
		plan.delta.residentBytes += int64(len(continuationKey) + len(command.Payload))
	}
	plan.delta.reservedBytes += int64(afterReserved) - int64(beforeReserved)
	return replaceRequestLedgerHead(plan, rows, next)
}

func planRequestLedgerCleanupPayload(
	plan requestLedgerCommandPlan,
	command requestledger.CommandView,
	rows requestLedgerRows,
	snapshot pointSnapshot,
) (requestLedgerCommandPlan, error) {
	request, _ := command.PayloadCleanup()
	if rows.head.Revision != command.ExpectedRevision || rows.pendingFound ||
		!rows.payloadBuildFound || rows.head.CleanupBuildDigest == (requestledger.Digest{}) ||
		rows.head.CleanupBuildDigest != request.BuildDigest ||
		rows.payloadBuild.BuildDigest != request.BuildDigest ||
		rows.head.OutstandingRoutePinDigest != (requestledger.Digest{}) {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	chunk, err := requestledger.PlanPayloadCleanup(rows.head, request)
	if err != nil {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	var reclaimed uint64
	for offset := uint64(0); offset < chunk.ChunkCount; offset++ {
		ordinal := chunk.FirstOrdinal + offset
		key := requestledger.AppendPayloadChunkKey(
			nil, command.Home, command.KeyDigest, rows.payloadBuild.ContentRoot, ordinal,
		)
		raw, found, readErr := snapshot.appendRaw(nil, key)
		if readErr != nil {
			return plan, readErr
		}
		payload, openErr := requestledger.OpenPayloadChunk(raw)
		if !found || openErr != nil || payload.KeyDigest != command.KeyDigest ||
			payload.ContentRoot != rows.payloadBuild.ContentRoot ||
			payload.BuildDigest != rows.payloadBuild.BuildDigest || payload.Ordinal != ordinal {
			return plan, errors.Join(openErr, ErrStateCorrupt)
		}
		rowBytes := uint64(len(key) + len(raw))
		if reclaimed > math.MaxUint64-rowBytes {
			return plan, ErrStateCorrupt
		}
		reclaimed += rowBytes
		plan.rows = append(plan.rows, newTransactionDelete(key))
		plan.delta.rows--
	}
	if chunk.Final {
		key := requestledger.AppendPayloadBuildKey(nil, command.Home, command.KeyDigest)
		rowBytes := uint64(len(key) + len(rows.payloadBuildRaw))
		if reclaimed > math.MaxUint64-rowBytes {
			return plan, ErrStateCorrupt
		}
		reclaimed += rowBytes
		plan.rows = append(plan.rows, newTransactionDelete(key))
		plan.delta.rows--
	}
	if reclaimed != chunk.ReclaimedBytes || reclaimed > rows.head.CleanupPayloadBytes {
		return plan, ErrStateCorrupt
	}
	next, err := requestledger.AdvancePayloadCleanup(
		rows.head, request, chunk, command.Revision,
	)
	if err != nil {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	materialized := requestLedgerMaterialized{
		continuationBytes: len(rows.continuationRaw), routePinBytes: len(rows.routePinRaw),
		payloadResident: rows.head.CleanupPayloadBytes,
	}
	beforeReserved, ok := requestLedgerReservedBytes(rows.head, materialized)
	if !ok {
		return plan, ErrStateCorrupt
	}
	materialized.payloadResident = next.CleanupPayloadBytes
	afterReserved, ok := requestLedgerReservedBytes(next, materialized)
	if !ok {
		return plan, ErrStateCorrupt
	}
	plan.delta.residentBytes -= int64(reclaimed)
	plan.delta.reservedBytes += int64(afterReserved) - int64(beforeReserved)
	return replaceRequestLedgerHead(plan, rows, next)
}

func planRequestLedgerComplete(plan requestLedgerCommandPlan, command requestledger.CommandView, rows requestLedgerRows) (requestLedgerCommandPlan, error) {
	terminal, _ := command.Terminal()
	if rows.head.Revision == command.Revision && rows.terminalFound &&
		bytes.Equal(rows.terminalRaw, command.Payload) {
		return witnessedRequestLedgerApplied(plan, rows.head.Revision, rows.head.Phase, rows.terminalRaw, true), nil
	}
	if rows.head.Revision != command.ExpectedRevision || rows.pendingFound || !rows.continuationFound ||
		rows.terminalFound || rows.payloadBuildFound || !rows.preparedFound || !rows.schemaPinFound ||
		rows.schemaPin.Phase != requestledger.SchemaPinReleased || rows.routePinFound {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	expected, err := requestledger.NewTerminal(
		rows.head, rows.prepared, rows.schemaPin, command.Revision,
	)
	if err != nil {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	expectedRaw, err := requestledger.AppendTerminal(nil, expected)
	if err != nil || !bytes.Equal(expectedRaw, command.Payload) {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	next, err := requestledger.MarkTerminal(rows.head, rows.prepared, rows.schemaPin, terminal)
	if err != nil {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	materialized := materializedRequestLedgerRows(rows)
	beforeReserved, ok := requestLedgerReservedBytes(rows.head, materialized)
	if !ok {
		return plan, ErrStateCorrupt
	}
	materialized.preparedBytes, materialized.schemaPinBytes = 0, 0
	afterReserved, ok := requestLedgerReservedBytes(next, materialized)
	if !ok {
		return plan, ErrStateCorrupt
	}
	key := requestledger.AppendTerminalKey(nil, command.Home, command.KeyDigest)
	preparedKey := requestledger.AppendPreparedTerminalKey(nil, command.Home, command.KeyDigest)
	schemaKey := requestledger.AppendSchemaPinReleaseKey(nil, command.Home, command.KeyDigest)
	plan.rows = append(plan.rows, newTransactionPut(key, command.Payload),
		newTransactionDelete(preparedKey), newTransactionDelete(schemaKey))
	plan.delta.rows--
	plan.delta.residentBytes += int64(len(key)+len(command.Payload)) -
		int64(len(preparedKey)+len(rows.preparedRaw)+len(schemaKey)+len(rows.schemaPinRaw))
	plan.delta.reservedBytes += int64(afterReserved) - int64(beforeReserved)
	return replaceRequestLedgerHead(plan, rows, next)
}

func planRequestLedgerPrepareTerminal(
	plan requestLedgerCommandPlan,
	command requestledger.CommandView,
	rows requestLedgerRows,
) (requestLedgerCommandPlan, error) {
	prepared, _ := command.PreparedTerminal()
	if rows.head.Revision == command.Revision && rows.preparedFound &&
		bytes.Equal(rows.preparedRaw, command.Payload) {
		return witnessedRequestLedgerApplied(plan, rows.head.Revision, rows.head.Phase, rows.preparedRaw, true), nil
	}
	if rows.head.Revision != command.ExpectedRevision || rows.head.Phase != requestledger.PhaseSealed ||
		rows.pendingFound || !rows.continuationFound || rows.payloadBuildFound ||
		rows.preparedFound || rows.schemaPinFound || rows.head.CleanupBuildDigest != (requestledger.Digest{}) ||
		rows.head.OutstandingRoutePinDigest != (requestledger.Digest{}) ||
		!rows.routePinFound || rows.routePin.Phase != requestledger.RoutePinReleased {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	next, err := requestledger.MarkTerminalPrepared(rows.head, rows.continuation, prepared)
	if err != nil {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	materialized := materializedRequestLedgerRows(rows)
	beforeReserved, ok := requestLedgerReservedBytes(rows.head, materialized)
	if !ok {
		return plan, ErrStateCorrupt
	}
	materialized.routePinBytes = 0
	materialized.preparedBytes = len(command.Payload)
	afterReserved, ok := requestLedgerReservedBytes(next, materialized)
	if !ok {
		return plan, ErrStateCorrupt
	}
	preparedKey := requestledger.AppendPreparedTerminalKey(nil, command.Home, command.KeyDigest)
	routeKey := requestledger.AppendRoutePinKey(nil, command.Home, command.KeyDigest)
	plan.rows = append(plan.rows, newTransactionPut(preparedKey, command.Payload), newTransactionDelete(routeKey))
	plan.delta.residentBytes += int64(len(preparedKey)+len(command.Payload)) -
		int64(len(routeKey)+len(rows.routePinRaw))
	plan.delta.reservedBytes += int64(afterReserved) - int64(beforeReserved)
	return replaceRequestLedgerHead(plan, rows, next)
}

func planRequestLedgerBeginSchemaPinRelease(
	plan requestLedgerCommandPlan,
	command requestledger.CommandView,
	rows requestLedgerRows,
	snapshot pointSnapshot,
) (requestLedgerCommandPlan, error) {
	release, _ := command.SchemaPinRelease()
	if rows.head.Revision == command.Revision && rows.schemaPinFound &&
		bytes.Equal(rows.schemaPinRaw, command.Payload) {
		return witnessedRequestLedgerApplied(plan, rows.head.Revision, rows.head.Phase, rows.schemaPinRaw, true), nil
	}
	if rows.head.Revision != command.ExpectedRevision || rows.head.Phase != requestledger.PhasePrepared ||
		!rows.preparedFound || rows.schemaPinFound || rows.routePinFound {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	expected, err := requestledger.NewSchemaPinRelease(
		rows.head, rows.prepared, command.Revision, release.Command,
	)
	if err != nil || expected.RecordDigest != release.RecordDigest ||
		!requestLedgerSchemaReleaseCommandAvailable(release) {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	// The release intent and its exact pin lease are one atomic system-row
	// publication. A concurrent Recover either wins first and invalidates this
	// intent, or observes the irreversible freeze; no read/CAS gap is exposed.
	outer, openErr := replication.OpenCommand(release.Command)
	nested, nestedErr := outer.OpenExecutionPin()
	if openErr != nil || nestedErr != nil {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	pin, found, err := executionPinRecordAt(snapshot, nested.PinID)
	if err != nil {
		return plan, err
	}
	frozen, freezeErr := executionpin.FreezeRelease(pin, nested)
	if !found || freezeErr != nil {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	encodedPin, err := executionpin.AppendRecord(nil, frozen)
	if err != nil {
		return plan, ErrExecutionPinStateCorrupt
	}
	next, err := requestledger.InstallSchemaPinRelease(rows.head, rows.prepared, release)
	if err != nil {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	materialized := materializedRequestLedgerRows(rows)
	beforeReserved, ok := requestLedgerReservedBytes(rows.head, materialized)
	if !ok {
		return plan, ErrStateCorrupt
	}
	materialized.schemaPinBytes = len(command.Payload)
	afterReserved, ok := requestLedgerReservedBytes(next, materialized)
	if !ok {
		return plan, ErrStateCorrupt
	}
	key := requestledger.AppendSchemaPinReleaseKey(nil, command.Home, command.KeyDigest)
	plan.rows = append(plan.rows, newTransactionPut(key, command.Payload))
	// Both existing values have fixed size: no new retained row or byte is
	// charged, and the active-index hash must track the frozen record exactly.
	pinKey, activeKey := executionPinRecordStorageKey(frozen.PinID), executionPinActiveStorageKey(frozen)
	activeDigest := sha256.Sum256(encodedPin)
	plan.rows = append(plan.rows, newTransactionPut(pinKey[:], encodedPin), newTransactionPut(activeKey[:], activeDigest[:]))
	plan.delta.rows++
	plan.delta.residentBytes += int64(len(key) + len(command.Payload))
	plan.delta.reservedBytes += int64(afterReserved) - int64(beforeReserved)
	return replaceRequestLedgerHead(plan, rows, next)
}

func planRequestLedgerRecordSchemaPinReleased(
	plan requestLedgerCommandPlan,
	command requestledger.CommandView,
	rows requestLedgerRows,
) (requestLedgerCommandPlan, error) {
	release, _ := command.SchemaPinRelease()
	if rows.head.Revision == command.Revision && rows.schemaPinFound &&
		bytes.Equal(rows.schemaPinRaw, command.Payload) {
		return witnessedRequestLedgerApplied(plan, rows.head.Revision, rows.head.Phase, rows.schemaPinRaw, true), nil
	}
	if rows.head.Revision != command.ExpectedRevision || rows.head.Phase != requestledger.PhasePrepared ||
		!rows.preparedFound || !rows.schemaPinFound ||
		rows.schemaPin.Phase != requestledger.SchemaPinReleasing {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	expected, err := requestledger.RecordVerifiedSchemaPinReleased(
		rows.schemaPin, release.Revision, release.Completion,
	)
	if err != nil || expected.RecordDigest != release.RecordDigest {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	// Exact execution-pin release command/completion semantics are verified at
	// this hook once the dependency-clean executionpin parser is integrated.
	if !requestLedgerSchemaReleaseEvidenceAvailable(release) {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	next, err := requestledger.MarkSchemaPinReleased(
		rows.head, rows.prepared, rows.schemaPin, release,
	)
	if err != nil {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	materialized := materializedRequestLedgerRows(rows)
	beforeReserved, ok := requestLedgerReservedBytes(rows.head, materialized)
	if !ok {
		return plan, ErrStateCorrupt
	}
	materialized.schemaPinBytes = len(command.Payload)
	afterReserved, ok := requestLedgerReservedBytes(next, materialized)
	if !ok {
		return plan, ErrStateCorrupt
	}
	key := requestledger.AppendSchemaPinReleaseKey(nil, command.Home, command.KeyDigest)
	plan.rows = append(plan.rows, newTransactionPut(key, command.Payload))
	plan.delta.residentBytes += int64(len(command.Payload) - len(rows.schemaPinRaw))
	plan.delta.reservedBytes += int64(afterReserved) - int64(beforeReserved)
	return replaceRequestLedgerHead(plan, rows, next)
}

func planRequestLedgerAck(plan requestLedgerCommandPlan, command requestledger.CommandView, rows requestLedgerRows, snapshot pointSnapshot) (requestLedgerCommandPlan, error) {
	request, _ := command.AckRequest()
	if !rows.headFound || !rows.terminalFound || rows.head.Phase != requestledger.PhaseTerminal ||
		rows.head.Revision != command.ExpectedRevision || request.TerminalRevision != rows.terminal.Revision ||
		request.ResultDigest != rows.terminal.ResultDigest || request.AckToken != rows.terminal.AckToken {
		return witnessedRequestLedgerConflict(plan, rows.head.Revision, rows.head.Phase, rows.headRaw), nil
	}
	prior, err := requestLedgerPriorBytes(snapshot, rows)
	if err != nil {
		return plan, err
	}
	ack, err := requestledger.NewAck(rows.head, rows.terminal, command.Revision, prior)
	if err != nil {
		return plan, err
	}
	encoded, err := requestledger.AppendAck(nil, ack)
	if err != nil {
		return plan, err
	}
	key := requestledger.AppendAckKey(nil, command.Home, command.KeyDigest)
	plan.rows = append(plan.rows, newTransactionPut(key, encoded))
	plan.delta.rows++
	plan.delta.residentBytes += int64(len(key) + len(encoded))
	plan.delta.ackRows++
	plan.delta.ackBytes += int64(len(key) + len(encoded))
	plan.delta.reservedBytes -= int64(len(key) + len(encoded))
	return witnessedRequestLedgerApplied(plan, ack.Revision, requestledger.PhaseAcked, encoded, false), nil
}

func planRequestLedgerGC(plan requestLedgerCommandPlan, command requestledger.CommandView, rows requestLedgerRows, snapshot pointSnapshot) (requestLedgerCommandPlan, error) {
	if !rows.ackFound {
		return plan, nil
	}
	request, ok := command.GCRequest()
	if !ok {
		return witnessedRequestLedgerConflict(plan, rows.ack.Revision, requestledger.PhaseAcked, rows.ackRaw), nil
	}
	if command.Revision == rows.ack.Revision && requestledger.SameAckGCTransition(rows.ack, request) {
		return witnessedRequestLedgerApplied(
			plan, rows.ack.Revision, requestledger.PhaseAcked, rows.ackRaw, true,
		), nil
	}
	if request.ExpectedAckDigest != rows.ack.AckDigest {
		return witnessedRequestLedgerConflict(plan, rows.ack.Revision, requestledger.PhaseAcked, rows.ackRaw), nil
	}
	if request.Action != requestledger.GCActionCollect || rows.ack.GCPhase != requestledger.AckGCCollecting ||
		command.ExpectedRevision != rows.ack.Revision || snapshot.overlay != nil || snapshot.value == nil {
		return witnessedRequestLedgerConflict(plan, rows.ack.Revision, requestledger.PhaseAcked, rows.ackRaw), nil
	}
	prefix := requestledger.AppendHeadKey(nil, command.Home, command.KeyDigest)
	prefix = prefix[:1+len(command.Home)+len(command.KeyDigest)]
	var reclaimed uint64
	var selected uint16
	more := false
	err := snapshot.value.RangePrefixRaw(prefix, func(key, value []byte) error {
		view, openErr := requestledger.OpenStorageKey(key)
		if openErr != nil || view.Home != command.Home || view.Key != command.KeyDigest {
			return errors.Join(openErr, ErrStateCorrupt)
		}
		if view.Kind == requestledger.StorageAck {
			return nil
		}
		if validateErr := validateSnapshotRequestLedgerRow(key, value); validateErr != nil {
			return validateErr
		}
		rowBytes := uint64(len(key) + len(value))
		if selected == request.MaxRows || rowBytes > uint64(request.MaxBytes)-reclaimed {
			more = true
			return errStopRequestLedgerGCScan
		}
		plan.rows = append(plan.rows, newTransactionDelete(key))
		selected++
		reclaimed += rowBytes
		return nil
	})
	if err != nil && !errors.Is(err, errStopRequestLedgerGCScan) {
		return plan, err
	}
	if selected == 0 || reclaimed == 0 || reclaimed > rows.ack.PriorEncodedBytes-rows.ack.ReclaimedBytes {
		return witnessedRequestLedgerConflict(plan, rows.ack.Revision, requestledger.PhaseAcked, rows.ackRaw), nil
	}
	final := !more && reclaimed == rows.ack.PriorEncodedBytes-rows.ack.ReclaimedBytes
	next, err := requestledger.AdvanceAckGC(
		rows.ack, request, command.Revision, rows.ack.GCCursor+uint64(selected), reclaimed, final,
	)
	if err != nil {
		return witnessedRequestLedgerConflict(plan, rows.ack.Revision, requestledger.PhaseAcked, rows.ackRaw), nil
	}
	plan, err = planRequestLedgerSequencedAckGCComplete(plan, rows, next, snapshot)
	if err != nil {
		return plan, err
	}
	nextRaw, err := requestledger.AppendAck(nil, next)
	if err != nil {
		return plan, err
	}
	ackKey := requestledger.AppendAckKey(nil, command.Home, command.KeyDigest)
	plan.rows = append(plan.rows, newTransactionPut(ackKey, nextRaw))
	plan.delta.rows -= int64(selected)
	plan.delta.residentBytes -= int64(reclaimed)
	plan.delta.residentBytes += int64(len(nextRaw) - len(rows.ackRaw))
	plan.delta.ackBytes += int64(len(nextRaw) - len(rows.ackRaw))
	return witnessedRequestLedgerApplied(plan, next.Revision, requestledger.PhaseAcked, nextRaw, false), nil
}

func replaceRequestLedgerHead(plan requestLedgerCommandPlan, rows requestLedgerRows, next requestledger.HeadRecord) (requestLedgerCommandPlan, error) {
	encoded, err := requestledger.AppendHead(nil, next)
	if err != nil {
		return plan, err
	}
	key := requestledger.AppendHeadKey(nil, rows.home, next.KeyDigest)
	plan.rows = append(plan.rows, newTransactionPut(key, encoded))
	plan.delta.residentBytes += int64(len(encoded) - len(rows.headRaw))
	return witnessedRequestLedgerApplied(plan, next.Revision, next.Phase, encoded, false), nil
}

func witnessedRequestLedgerApplied(plan requestLedgerCommandPlan, revision uint64, phase requestledger.Phase, raw []byte, duplicate bool) requestLedgerCommandPlan {
	plan.completion.ResultCode = ResultApplied
	plan.completion.Revision = revision
	plan.completion.Phase = phase
	plan.completion.StateDigest = requestLedgerStateDigest(raw)
	plan.completion.ExactDuplicate = duplicate
	return plan
}

func witnessedRequestLedgerConflict(plan requestLedgerCommandPlan, revision uint64, phase requestledger.Phase, raw []byte) requestLedgerCommandPlan {
	plan.completion.ResultCode = ResultRequestLedgerConflict
	plan.completion.Revision = revision
	plan.completion.Phase = phase
	plan.completion.StateDigest = requestLedgerStateDigest(raw)
	return plan
}

func requestLedgerStateDigest(raw []byte) requestledger.Digest {
	hash := sha256.New()
	_, _ = hash.Write(requestLedgerStateDigestDomain)
	_, _ = hash.Write(raw)
	var digest requestledger.Digest
	_ = hash.Sum(digest[:0])
	return digest
}

func readRequestLedgerRows(snapshot pointSnapshot, home requestledger.LedgerHome, key requestledger.Digest, pendingScratch []requestledger.StepRef) (requestLedgerRows, error) {
	rows := requestLedgerRows{home: home}
	var err error
	rows.ackRaw, rows.ackFound, err = snapshot.appendRaw(nil, requestledger.AppendAckKey(nil, home, key))
	if err != nil {
		return rows, err
	}
	if rows.ackFound {
		rows.ack, err = requestledger.OpenAck(rows.ackRaw)
		if err != nil || rows.ack.KeyDigest != key {
			return rows, errors.Join(err, ErrStateCorrupt)
		}
	}
	rows.headRaw, rows.headFound, err = snapshot.appendRaw(nil, requestledger.AppendHeadKey(nil, home, key))
	if err != nil {
		return rows, err
	}
	if rows.headFound {
		rows.head, err = requestledger.OpenHead(rows.headRaw)
		if err != nil || rows.head.KeyDigest != key {
			return rows, errors.Join(err, ErrStateCorrupt)
		}
	}
	rows.pendingRaw, rows.pendingFound, err = snapshot.appendRaw(nil, requestledger.AppendPendingKey(nil, home, key))
	if err != nil {
		return rows, err
	}
	if rows.pendingFound {
		if len(pendingScratch) == 0 {
			if openErr := requestledger.ValidatePendingWaveBytes(rows.pendingRaw); openErr != nil {
				return rows, errors.Join(openErr, ErrStateCorrupt)
			}
		} else {
			view, openErr := requestledger.OpenPendingWaveInto(rows.pendingRaw, pendingScratch)
			if openErr != nil || view.Key() != key {
				return rows, errors.Join(openErr, ErrStateCorrupt)
			}
			rows.pending = view.Record()
		}
	}
	rows.continuationRaw, rows.continuationFound, err = snapshot.appendRaw(nil, requestledger.AppendContinuationKey(nil, home, key))
	if err != nil {
		return rows, err
	}
	if rows.continuationFound {
		rows.continuation, err = requestledger.OpenContinuation(rows.continuationRaw)
		if err != nil || rows.continuation.KeyDigest != key {
			return rows, errors.Join(err, ErrStateCorrupt)
		}
	}
	rows.terminalRaw, rows.terminalFound, err = snapshot.appendRaw(nil, requestledger.AppendTerminalKey(nil, home, key))
	if err != nil {
		return rows, err
	}
	if rows.terminalFound {
		rows.terminal, err = requestledger.OpenTerminal(rows.terminalRaw)
		if err != nil || rows.terminal.KeyDigest != key {
			return rows, errors.Join(err, ErrStateCorrupt)
		}
	}
	rows.payloadBuildRaw, rows.payloadBuildFound, err = snapshot.appendRaw(nil, requestledger.AppendPayloadBuildKey(nil, home, key))
	if err != nil {
		return rows, err
	}
	if rows.payloadBuildFound {
		rows.payloadBuild, err = requestledger.OpenPayloadBuild(rows.payloadBuildRaw)
		if err != nil || rows.payloadBuild.KeyDigest != key {
			return rows, errors.Join(err, ErrStateCorrupt)
		}
	}
	rows.routePinRaw, rows.routePinFound, err = snapshot.appendRaw(nil, requestledger.AppendRoutePinKey(nil, home, key))
	if err != nil {
		return rows, err
	}
	if rows.routePinFound {
		rows.routePin, err = requestledger.OpenRoutePin(rows.routePinRaw)
		if err != nil || rows.routePin.KeyDigest != key {
			return rows, errors.Join(err, ErrStateCorrupt)
		}
	}
	rows.preparedRaw, rows.preparedFound, err = snapshot.appendRaw(nil, requestledger.AppendPreparedTerminalKey(nil, home, key))
	if err != nil {
		return rows, err
	}
	if rows.preparedFound {
		rows.prepared, err = requestledger.OpenPreparedTerminal(rows.preparedRaw)
		if err != nil || rows.prepared.KeyDigest != key {
			return rows, errors.Join(err, ErrStateCorrupt)
		}
	}
	rows.schemaPinRaw, rows.schemaPinFound, err = snapshot.appendRaw(nil, requestledger.AppendSchemaPinReleaseKey(nil, home, key))
	if err != nil {
		return rows, err
	}
	if rows.schemaPinFound {
		rows.schemaPin, err = requestledger.OpenSchemaPinRelease(rows.schemaPinRaw)
		if err != nil || rows.schemaPin.KeyDigest != key {
			return rows, errors.Join(err, ErrStateCorrupt)
		}
	}
	return rows, nil
}

type requestLedgerMaterialized struct {
	pendingBytes, continuationBytes int
	routePinBytes, preparedBytes    int
	schemaPinBytes                  int
	payloadResident                 uint64
}

func materializedRequestLedgerRows(rows requestLedgerRows) requestLedgerMaterialized {
	return requestLedgerMaterialized{
		pendingBytes: len(rows.pendingRaw), continuationBytes: len(rows.continuationRaw),
		routePinBytes: len(rows.routePinRaw), preparedBytes: len(rows.preparedRaw),
		schemaPinBytes: len(rows.schemaPinRaw),
	}
}

func requestLedgerReservedBytes(head requestledger.HeadRecord, rows requestLedgerMaterialized) (uint64, bool) {
	// Every admitted request prepays its permanent ACK tombstone. Cleanup
	// reserve is never needed to make terminal requests acknowledgeable.
	reserved := uint64(requestledger.FixedStorageKeyBytes + requestledger.AckRecordBytes)
	if head.Phase == requestledger.PhasePlanning && head.PlanPageCount != 0 {
		remainingPlan := head.TotalPlanBytes - head.AppendedPlanBytes
		remainingPages := head.PlanPageCount - head.AppendedPageCount
		pageOverhead := uint64(requestledger.PageStorageKeyBytes + requestledger.PlanPageRecordOverheadBytes)
		if remainingPages > math.MaxUint64/pageOverhead {
			return 0, false
		}
		pages := remainingPlan + remainingPages*pageOverhead
		if reserved > math.MaxUint64-pages {
			return 0, false
		}
		reserved += pages
	}
	if head.Phase == requestledger.PhaseTerminal {
		return reserved, true
	}
	for _, pair := range []struct {
		maximum  uint64
		resident int
	}{
		{head.MaxPendingWaveBytes, rows.pendingBytes},
		{head.MaxContinuationBytes, rows.continuationBytes},
		{requestledger.MaxRoutePinRecordBytes, rows.routePinBytes},
		{head.MaxTerminalBytes, 0},
		{requestledger.MaxSchemaPinReleaseRecordBytes, rows.schemaPinBytes},
		// PreparedTerminal owns the result before pin release; Terminal then
		// copies it after the authenticated release certificate.
		{head.MaxTerminalBytes, rows.preparedBytes},
	} {
		if pair.resident < 0 || uint64(pair.resident) > pair.maximum {
			return 0, false
		}
		remaining := pair.maximum - uint64(pair.resident)
		if pair.resident == 0 {
			remaining += requestledger.FixedStorageKeyBytes
		}
		if reserved > math.MaxUint64-remaining {
			return 0, false
		}
		reserved += remaining
	}
	ready := uint64(requestledger.ReadyStorageKeyBytes + requestledger.ReadyRecordBytes)
	if reserved > math.MaxUint64-ready {
		return 0, false
	}
	reserved += ready
	if head.MaxActivePayloadBytes != 0 {
		payloadMaximum, err := requestledger.PayloadReservation(head)
		if err != nil {
			return 0, false
		}
		if rows.payloadResident > payloadMaximum || reserved > math.MaxUint64-(payloadMaximum-rows.payloadResident) {
			return 0, false
		}
		reserved += payloadMaximum - rows.payloadResident
	}
	return reserved, true
}

func checkedRequestLedgerConsumption(resident, reserved, add uint64) (uint64, bool) {
	if resident > math.MaxUint64-reserved || resident+reserved > math.MaxUint64-add {
		return 0, false
	}
	return resident + reserved + add, true
}

func requestLedgerPriorBytes(snapshot pointSnapshot, rows requestLedgerRows) (uint64, error) {
	total := uint64(0)
	for _, row := range []struct {
		keyBytes int
		raw      []byte
		found    bool
	}{
		{requestledger.FixedStorageKeyBytes, rows.headRaw, rows.headFound},
		{requestledger.FixedStorageKeyBytes, rows.pendingRaw, rows.pendingFound},
		{requestledger.FixedStorageKeyBytes, rows.continuationRaw, rows.continuationFound},
		{requestledger.FixedStorageKeyBytes, rows.terminalRaw, rows.terminalFound},
	} {
		if row.found {
			value := uint64(row.keyBytes + len(row.raw))
			if total > math.MaxUint64-value {
				return 0, ErrStateCorrupt
			}
			total += value
		}
	}
	if rows.payloadBuildFound {
		return 0, ErrStateCorrupt
	}
	if rows.headFound && rows.head.PlanPageCount != 0 {
		var chain requestledger.Digest
		var planBytes uint64
		for ordinal := uint64(0); ordinal < rows.head.AppendedPageCount; ordinal++ {
			key := requestledger.AppendPlanPageKey(nil, rows.home, rows.head.KeyDigest, ordinal)
			raw, found, err := snapshot.appendRaw(nil, key)
			if err != nil || !found {
				return 0, errors.Join(err, ErrStateCorrupt)
			}
			page, err := requestledger.OpenPlanPage(raw)
			if err != nil || page.KeyDigest != rows.head.KeyDigest || page.PlanRoot != rows.head.PlanRoot ||
				page.Ordinal != ordinal || page.PreviousChain != chain || planBytes > math.MaxUint64-uint64(len(page.Data)) {
				return 0, errors.Join(err, ErrStateCorrupt)
			}
			rowBytes := uint64(len(key) + len(raw))
			if total > math.MaxUint64-rowBytes {
				return 0, ErrStateCorrupt
			}
			total += rowBytes
			planBytes += uint64(len(page.Data))
			chain = page.Chain
		}
		if planBytes != rows.head.AppendedPlanBytes || chain != rows.head.PageChain {
			return 0, ErrStateCorrupt
		}
	}
	return total, nil
}

func applyRequestLedgerStateDelta(state *State, delta requestLedgerStateDelta) error {
	if state == nil || !applySignedUint64(&state.RequestLedgerRows, delta.rows) ||
		!applySignedUint64(&state.RequestLedgerResidentBytes, delta.residentBytes) ||
		!applySignedUint64(&state.RequestLedgerReservedBytes, delta.reservedBytes) ||
		!applySignedUint64(&state.RequestLedgerAckRows, delta.ackRows) ||
		!applySignedUint64(&state.RequestLedgerAckBytes, delta.ackBytes) {
		return ErrStateCorrupt
	}
	return nil
}

func applySignedUint64(target *uint64, delta int64) bool {
	if target == nil {
		return false
	}
	if delta >= 0 {
		value := uint64(delta)
		if *target > math.MaxUint64-value {
			return false
		}
		*target += value
		return true
	}
	value := uint64(-(delta + 1)) + 1
	if *target < value {
		return false
	}
	*target -= value
	return true
}
