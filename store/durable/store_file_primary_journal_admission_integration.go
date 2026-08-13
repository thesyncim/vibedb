package durable

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibejson/document"
)

// Package-private deterministic seams bracket only caller-owned work. They run
// after writer unlock and never under admission.mu; production leaves them nil.
var (
	primaryJournalAdmissionInitialAppliedHook  func()
	primaryJournalAdmissionInitialHandoffHook  func()
	primaryJournalAdmissionRequestAppliedHook  func(*primaryJournalAdmissionRequest)
	primaryJournalCohortBeforePrepareHook      func(int) error
	primaryJournalCohortBeforeAppendHook       func(int) error
	primaryJournalCohortAfterDepositHook       func(int, uint64)
	primaryJournalCohortAfterCutHook           func(uint64)
	primaryJournalCohortBeforePressureSealHook func(int)
	primaryJournalCohortBeforeForcedSealHook   func()
	primaryJournalCohortAfterTerminalDrainHook func()
)

type primaryJournalCohortPlan struct {
	request     *primaryJournalAdmissionRequest
	route       storeio.ResidentPrimaryRoute
	baseValue   []byte
	stableSlot  uint8
	fixedExtent bool
}

type primaryJournalCohortOutcome struct {
	planned   int
	published int
	resolved  int
	terminal  bool
	complete  bool
	sticky    error
}

// primaryJournalAdmissionEligible is a construction certificate plus one-way
// atomic vetoes. It deliberately does not read journal or option slice state
// concurrently with Close or online index cutover.
func (c *Collection) primaryJournalAdmissionEligible() bool {
	return c != nil && c.primaryJournalAdmission != nil &&
		c.primaryJournalContexts != nil &&
		!c.onlineIndexBuild.Load() && !c.packedLogicalCutDisabled.Load()
}

func (c *Collection) tryPrimaryJournalAdmissionPut(
	key, src []byte,
) (handled, created bool, err error) {
	if !c.primaryJournalAdmissionEligible() {
		return false, false, nil
	}
	admission := c.primaryJournalAdmission
	if ticket, ok := admission.tryStartInitialPilot(); ok {
		created, continuation, applyErr :=
			c.putPrimaryWithSplitDetached(key, src)
		if primaryJournalAdmissionInitialAppliedHook != nil {
			primaryJournalAdmissionInitialAppliedHook()
		}
		c.handoffPrimaryJournalAdmission(ticket, nil)
		if primaryJournalAdmissionInitialHandoffHook != nil {
			primaryJournalAdmissionInitialHandoffHook()
		}
		return true, created, c.awaitPrimaryMutationDurability(
			continuation, applyErr,
		)
	}

	request, decision, ok := c.preparePrimaryJournalAdmissionRequest(
		primaryMutationPut, key, src,
	)
	if !ok {
		return true, false, c.primaryJournalAdmissionRejectionError()
	}
	return c.finishPrimaryJournalAdmissionRequest(request, decision)
}

func (c *Collection) tryPrimaryJournalAdmissionDelete(
	key []byte,
) (handled, deleted bool, err error) {
	if !c.primaryJournalAdmissionEligible() {
		return false, false, nil
	}
	admission := c.primaryJournalAdmission
	if ticket, ok := admission.tryStartInitialPilot(); ok {
		deleted, continuation, applyErr := c.deletePrimaryDetached(key)
		if primaryJournalAdmissionInitialAppliedHook != nil {
			primaryJournalAdmissionInitialAppliedHook()
		}
		c.handoffPrimaryJournalAdmission(ticket, nil)
		if primaryJournalAdmissionInitialHandoffHook != nil {
			primaryJournalAdmissionInitialHandoffHook()
		}
		deleted, err = c.awaitDeletePrimaryWithEmptyReclaim(
			key, deleted, continuation, applyErr,
		)
		return true, deleted, err
	}

	request, decision, ok := c.preparePrimaryJournalAdmissionRequest(
		primaryMutationDelete, key, nil,
	)
	if !ok {
		return true, false, c.primaryJournalAdmissionRejectionError()
	}
	return c.finishPrimaryJournalAdmissionRequest(request, decision)
}

func (c *Collection) primaryJournalAdmissionRejectionError() error {
	c.writer.RLock()
	defer c.writer.RUnlock()
	if c.closed {
		return ErrClosed
	}
	if failure := c.PersistenceError(); failure != nil {
		return failure
	}
	return ErrClosed
}

func (c *Collection) preparePrimaryJournalAdmissionRequest(
	kind primaryMutationKind, key, raw []byte,
) (
	request *primaryJournalAdmissionRequest,
	decision primaryJournalAdmissionDecision,
	ok bool,
) {
	admission := c.primaryJournalAdmission
	pool := c.primaryJournalContexts
	observed := admission.observe()
	context := pool.acquire()
	if context == nil {
		return nil, primaryJournalAdmissionDecision{}, false
	}
	request = admission.bindRequest(context)
	request.kind = kind
	request.key = key
	request.raw = raw
	request.rawLength = len(raw)

	if kind == primaryMutationPut {
		switch {
		case len(key) == 0 || len(key) > c.options.MaxKeyBytes:
			request.preflightErr = ErrKeyTooLarge
			request.prepared = true
		case len(raw) == 0 || len(raw) > c.options.MaxDocumentBytes:
			request.preflightErr = ErrDocumentTooLarge
			request.prepared = true
		case len(raw) > c.options.InlineValueBytes || len(raw) > pool.rawLimit:
			// The bounded preparation lane declined; the promoted baseline
			// re-enters the literal raw path without duplicate preflight work.
		default:
			pool.ensurePut(context)
			canonical, _, eligible, preflightErr := context.canonicalize(
				raw, c.options.Collection.IndexOptions,
			)
			request.canonical = canonical
			request.preflightErr = preflightErr
			request.prepared = eligible || preflightErr != nil
			if errors.Is(preflightErr, document.ErrIndexFull) {
				request.prepared = false
			}
		}
	}

	decision = admission.admitPrepared(request, observed)
	if decision.closed {
		request.resetPayload()
		pool.release(context)
		return nil, decision, false
	}
	return request, decision, true
}

func (c *Collection) finishPrimaryJournalAdmissionRequest(
	request *primaryJournalAdmissionRequest,
	decision primaryJournalAdmissionDecision,
) (handled, changed bool, err error) {
	admission := c.primaryJournalAdmission
	if decision.queued {
		for {
			signal := admission.waitSignal(request)
			if signal.done {
				break
			}
			if signal.role == primaryJournalAdmissionNoRole {
				panic("vibedb: recovery-journal admission woke without baton")
			}
			c.runPrimaryJournalAdmissionRole(
				request, signal.role, signal.ticket,
			)
			break
		}
	} else if decision.role != primaryJournalAdmissionNoRole {
		c.runPrimaryJournalAdmissionRole(
			request, decision.role, decision.ticket,
		)
	}

	result := request.result
	kind := request.kind
	key := request.key
	context := request.context
	request.resetPayload()
	c.primaryJournalContexts.release(context)
	if kind == primaryMutationDelete {
		result.deleted, result.err = c.awaitDeletePrimaryWithEmptyReclaim(
			key, result.deleted, result.continuation, result.err,
		)
		return true, result.deleted, result.err
	}
	result.err = c.awaitPrimaryMutationDurability(
		result.continuation, result.err,
	)
	return true, result.created, result.err
}

func (c *Collection) runPrimaryJournalAdmissionRole(
	self *primaryJournalAdmissionRequest,
	role primaryJournalAdmissionRole,
	ticket primaryJournalAdmissionTicket,
) {
	for {
		switch role {
		case primaryJournalAdmissionBaselineRole:
			c.applyPrimaryJournalAdmissionRequest(self)
			c.handoffPrimaryJournalAdmission(ticket, self)
			return
		case primaryJournalAdmissionCohortRole:
			batch, ok := c.primaryJournalAdmission.cohortBatch(ticket)
			if !ok {
				panic("vibedb: recovery-journal cohort baton lost batch")
			}
			outcome := c.publishPrimaryJournalAdmissionCohort(batch)
			if outcome.terminal {
				c.terminatePrimaryJournalAdmissionCohort(
					ticket, self, outcome.published, outcome.sticky,
				)
				return
			}
			if outcome.complete {
				c.handoffPrimaryJournalAdmission(ticket, self)
				return
			}
			suffixAt := max(outcome.published, outcome.resolved)
			if outcome.planned < 2 {
				suffixAt = 0
			}
			for _, request := range batch[suffixAt:] {
				request.forceBaseline = true
			}
			if primaryJournalCohortBeforePressureSealHook != nil {
				primaryJournalCohortBeforePressureSealHook(suffixAt)
			}
			c.completePrimaryJournalAdmissionPressure(
				ticket, suffixAt, self,
			)
			return
		default:
			panic("vibedb: invalid recovery-journal admission baton")
		}
	}
}

// completePrimaryJournalAdmissionPressure transfers the untouched suffix with
// sealPressure, then executes only that marked suffix through mature detached
// baseline Apply calls before waking any waiter. No durability wait occurs in
// this loop, so all successful baseline deposits can still share the existing
// group fence. Later unmarked arrivals remain a subsequent finite baton.
func (c *Collection) completePrimaryJournalAdmissionPressure(
	ticket primaryJournalAdmissionTicket,
	suffixAt int,
	self *primaryJournalAdmissionRequest,
) {
	decision, ok := c.primaryJournalAdmission.sealPressure(ticket, suffixAt)
	if !ok {
		// Close may acquire writer after publication/Add and before this state
		// transition. It has already detached passive arrivals and marked the
		// active ticket Closed; seal transfers that batch without choosing a new
		// baton. Preserve the published prefix and reject only untouched work.
		closed, sealOK := c.primaryJournalAdmission.seal(ticket)
		if !sealOK || !closed.closed {
			panic("vibedb: recovery-journal pressure seal lost ownership")
		}
		requests := closed.completed.detached()
		for index := suffixAt; index < len(requests); index++ {
			if requests[index].result.err == nil {
				requests[index].result.err = ErrClosed
			}
		}
		c.primaryJournalAdmission.publishTransition(
			requests, self, nil, primaryJournalAdmissionSignal{},
		)
		return
	}
	var completed primaryJournalAdmissionCompletion
	appendCompletion := func(next primaryJournalAdmissionCompletion) {
		if completed.count+next.count > len(completed.requests) {
			panic("vibedb: recovery-journal completion exceeds bound")
		}
		copy(
			completed.requests[completed.count:completed.count+next.count],
			next.requests[:next.count],
		)
		completed.count += next.count
	}
	appendCompletion(decision.completed)

	for decision.role != primaryJournalAdmissionNoRole &&
		decision.leader != nil && decision.leader.forceBaseline {
		if decision.role == primaryJournalAdmissionCohortRole {
			if primaryJournalCohortBeforeForcedSealHook != nil {
				primaryJournalCohortBeforeForcedSealHook()
			}
			converted, convertedOK := c.primaryJournalAdmission.sealPressure(
				decision.ticket, 0,
			)
			if !convertedOK {
				closed, sealOK := c.primaryJournalAdmission.seal(decision.ticket)
				if !sealOK || !closed.closed {
					panic("vibedb: forced baseline cohort conversion failed")
				}
				for _, request := range closed.completed.detached() {
					if request.result.err == nil {
						request.result.err = ErrClosed
					}
				}
				appendCompletion(closed.completed)
				decision = closed
				break
			}
			decision = converted
			if decision.role != primaryJournalAdmissionBaselineRole {
				panic("vibedb: forced cohort did not select baseline")
			}
			appendCompletion(decision.completed)
		}
		c.applyPrimaryJournalAdmissionRequest(decision.leader)
		decision, ok = c.primaryJournalAdmission.seal(decision.ticket)
		if !ok {
			panic("vibedb: forced baseline seal lost ownership")
		}
		appendCompletion(decision.completed)
	}

	c.primaryJournalAdmission.publishTransition(
		completed.detached(), self, decision.leader,
		primaryJournalAdmissionSignal{
			role: decision.role, ticket: decision.ticket,
		},
	)
}

// publishPrimaryJournalAdmissionCohort replaces only the narrow, fully
// prepared existing-row/equal-size Put prefix. One shared writer lock fences
// Close and structural writers while a fixed shadow plan derives same-key
// dependencies, redo records append in FIFO generation order, and one final
// packed cut publishes the successful prefix. It never uses overlay stripes.
func (c *Collection) publishPrimaryJournalAdmissionCohort(
	batch []*primaryJournalAdmissionRequest,
) primaryJournalCohortOutcome {
	outcome := primaryJournalCohortOutcome{}
	if len(batch) < 2 || len(batch) > primaryJournalAdmissionLimit ||
		batch[0].forceBaseline {
		return outcome
	}

	c.writer.RLock()
	defer c.writer.RUnlock()
	if c.closed {
		for _, request := range batch {
			request.result.err = ErrClosed
		}
		outcome.complete = true
		return outcome
	}
	if failure := c.PersistenceError(); failure != nil {
		for _, request := range batch {
			request.result.err = failure
		}
		outcome.complete = true
		return outcome
	}
	if !c.bufferedJournalAckLane() || c.journalReplaying ||
		c.onlineIndexBuild.Load() || c.primaryEpoch != nil ||
		c.options.Collection.Schema != nil || len(c.options.indexes) != 0 ||
		c.primaryUnifiedOverlay == nil || c.primaryJournalContexts == nil ||
		c.packedLogicalCutDisabled.Load() {
		return outcome
	}

	logicalView, logicalOK := c.writerLogicalView()
	state := logicalView.state
	router := c.primaryRouter.Load()
	if !logicalOK || state == nil || router == nil ||
		router.Generation() != logicalView.generation {
		// Planning is non-destructive. The mature path owns corruption error
		// precedence and classification, so promote this entire untouched phase.
		return outcome
	}

	var plans [primaryJournalAdmissionLimit]primaryJournalCohortPlan
	for index, request := range batch {
		if logicalView.generation+uint64(index) >= fileLogicalCutGenerationMask ||
			request == nil || request.forceBaseline ||
			request.kind != primaryMutationPut || !request.prepared ||
			request.preflightErr != nil || request.rawLength != len(request.raw) ||
			request.rawLength == 0 ||
			request.rawLength > c.options.MaxDocumentBytes ||
			len(request.key) == 0 || len(request.key) > c.options.MaxKeyBytes ||
			len(request.key) > storeio.CommonPrimaryLeafMaxKeyBytes ||
			len(request.canonical) == 0 ||
			len(request.canonical) > c.options.InlineValueBytes {
			break
		}
		plan, ok, err := c.planPrimaryJournalAdmissionReplacement(
			logicalView.generation, request, plans[:index],
		)
		if err != nil {
			// A provisional route/cache failure has not changed overlay or journal
			// state. Re-enter ordered baseline Apply instead of bulk-failing later
			// requests or turning a mature local error into sticky persistence.
			break
		}
		if !ok {
			break
		}
		plans[index] = plan
		outcome.planned++
	}
	if outcome.planned < 2 {
		return outcome
	}

	c.primaryUnifiedSeen = true
	for index := 0; index < outcome.planned; index++ {
		plan := &plans[index]
		request := plan.request
		generation := logicalView.generation + uint64(index) + 1
		if !c.journal.Fits(len(request.key), len(request.canonical)) {
			break
		}
		if hook := primaryJournalCohortBeforePrepareHook; hook != nil {
			if err := hook(index); err != nil {
				if errors.Is(err, storeio.ErrPageCachePinned) {
					break
				}
				request.result.err = err
				outcome.resolved = index + 1
				break
			}
		}
		prepared, err := c.primaryUnifiedOverlay.prepareWithLeafReservation(
			plan.route.Bucket, plan.route.Hash, generation,
			request.key, request.canonical, 0, 0,
			primaryUnifiedOverlayPut, plan.stableSlot,
			plan.route.Ref.Length, !plan.fixedExtent,
			storeio.CommonPrimaryUnifiedScalarPatch{},
		)
		if err != nil {
			if errors.Is(err, storeio.ErrPageCachePinned) {
				break
			}
			// The prepared overlay record is unpublished: count/used/head remain
			// unchanged, and the next serialized prepare may overwrite its slot.
			// Resolve only this request; the untouched suffix still enters baseline
			// FIFO ahead of later arrivals. max(published,resolved) prevents any
			// already-planned request from being signaled as a nil no-op.
			request.result.err = err
			outcome.resolved = index + 1
			break
		}
		var appendErr error
		if primaryJournalCohortBeforeAppendHook != nil {
			appendErr = primaryJournalCohortBeforeAppendHook(index)
		}
		var target uint64
		if appendErr == nil {
			target, appendErr = c.journalConcurrentOverlayAppend(
				storeio.RecoveryRecordKindPut, generation,
				request.key, request.canonical,
			)
		}
		if appendErr != nil {
			if errors.Is(appendErr, storeio.ErrRecoveryJournalFull) {
				break
			}
			outcome.terminal = true
			outcome.sticky = appendErr
			break
		}
		// The successful append is still invisible. Publish the one-way read-side
		// packed-cut certificate before linking the overlay record or storing the
		// final cut, so no reader can observe a logical generation without using
		// the logical view. Singleton and pressure-at-zero stores never pay it.
		c.primaryJournalCohortCutActive.Store(true)
		if primaryJournalCohortAfterDepositHook != nil {
			primaryJournalCohortAfterDepositHook(index, target)
		}
		c.primaryUnifiedOverlay.publish(prepared)
		request.result.created = false
		request.result.continuation = primaryMutationDurabilityContinuation{
			kind: primaryMutationDurabilityJournal, target: target,
		}
		if primaryJournalAdmissionRequestAppliedHook != nil {
			primaryJournalAdmissionRequestAppliedHook(request)
		}
		outcome.published++
	}

	if outcome.published != 0 {
		finalGeneration := logicalView.generation + uint64(outcome.published)
		finalCut, ok := packFileLogicalCut(finalGeneration, logicalView.delta)
		if !ok {
			panic("vibedb: invalid recovery-journal cohort logical cut")
		}
		router.AdvanceGeneration(finalGeneration)
		c.pageValidator.advanceGeneration(finalGeneration)
		c.logicalCut.Store(finalCut)
		if primaryJournalCohortAfterCutHook != nil {
			primaryJournalCohortAfterCutHook(finalGeneration)
		}
		for index := 0; index < outcome.published; index++ {
			c.durabilityWait.Add(1)
		}
		groupSize := uint64(outcome.published)
		c.journalCohortReplaces.Add(groupSize)
		c.journalCohortPublishGroups.Add(1)
		c.journalCohortPublishGroupSize.observe(groupSize)
		for largest := c.journalCohortLargestPublishGroup.Load(); groupSize > largest &&
			!c.journalCohortLargestPublishGroup.CompareAndSwap(
				largest, groupSize,
			); largest = c.journalCohortLargestPublishGroup.Load() {
		}
	}
	if outcome.terminal {
		sticky := c.poisonJournal(outcome.sticky)
		c.journalGroup.fail(sticky)
		outcome.sticky = sticky
		return outcome
	}
	if outcome.complete {
		return outcome
	}
	if max(outcome.published, outcome.resolved) == len(batch) {
		outcome.complete = true
	}
	return outcome
}

func (c *Collection) planPrimaryJournalAdmissionReplacement(
	generation uint64,
	request *primaryJournalAdmissionRequest,
	prior []primaryJournalCohortPlan,
) (primaryJournalCohortPlan, bool, error) {
	plan := primaryJournalCohortPlan{request: request}
	router := c.primaryRouter.Load()
	route, ok := router.Route(request.key)
	if !ok || route.Ref == (storeio.PageRef{}) {
		return plan, false, storeio.ErrSegmentedTabletRouterCorrupt
	}
	plan.route = route

	for index := len(prior) - 1; index >= 0; index-- {
		shadow := prior[index]
		if shadow.route.Bucket == route.Bucket &&
			shadow.route.Hash == route.Hash &&
			bytes.Equal(shadow.request.key, request.key) {
			if len(shadow.request.canonical) != len(request.canonical) {
				return plan, false, nil
			}
			plan.baseValue = shadow.baseValue
			plan.stableSlot = shadow.stableSlot
			plan.fixedExtent = len(plan.baseValue) != 0 &&
				bytes.Equal(request.canonical, plan.baseValue)
			if !plan.fixedExtent &&
				route.Ref.Length == storeio.CommonPrimaryLeafMaxExtentBytes {
				return plan, false, nil
			}
			return plan, true, nil
		}
	}

	leafLease, err := router.AcquireLeaf(c.cache, route)
	if err != nil {
		return plan, false, err
	}
	defer leafLease.Release()
	page := leafLease.Page()
	if storeio.PrimaryLeafClass(page) != storeio.CommonPrimaryLeafCompact {
		return plan, false, nil
	}
	leaf, ok := storeio.AdmittedCompactPrimaryStripe(
		page, c.storeID, route.Bucket,
	)
	if !ok {
		return plan, false, fmt.Errorf(
			"journal cohort admit compact bucket=%d: %w",
			route.Bucket, storeio.ErrCommonPrimaryLeafCorrupt,
		)
	}
	baseRank, baseFound := leaf.FindKey(request.key)
	if leaf.Len() > storeio.CommonPrimaryLeafWideSlots || !baseFound {
		return plan, false, nil
	}
	baseSlot, slotOK := leaf.PostingSlot(baseRank)
	if !slotOK {
		return plan, false, fmt.Errorf(
			"journal cohort posting slot bucket=%d rank=%d: %w",
			route.Bucket, baseRank, storeio.ErrCommonPrimaryLeafCorrupt,
		)
	}
	if _, overflow := leaf.OverflowRef(baseRank); overflow {
		return plan, false, nil
	}
	context := request.context
	// AppendValue may grow its destination. Keep the pool-owned slice header
	// unchanged until the decoded value proves it fit the fixed journal scratch;
	// a valid larger row is an eligibility decline, not corruption and not
	// retained unaccounted capacity on this context slot.
	baseValue, decoded := leaf.AppendValue(context.value[:0], baseRank)
	if !decoded {
		return plan, false, fmt.Errorf(
			"journal cohort decode base bucket=%d rank=%d: %w",
			route.Bucket, baseRank, storeio.ErrCommonPrimaryLeafCorrupt,
		)
	}
	if len(baseValue) > c.primaryJournalContexts.rawLimit ||
		cap(baseValue) > cap(context.value) {
		return plan, false, nil
	}
	context.value = baseValue
	plan.baseValue = baseValue
	current, disposition, overlaySlot := c.primaryUnifiedOverlay.lookup(
		route.Bucket, route.Hash, request.key, generation,
	)
	oldLen := len(plan.baseValue)
	plan.stableSlot = baseSlot
	switch disposition {
	case primaryUnifiedOverlayValue:
		oldLen = len(current)
		plan.stableSlot = overlaySlot
	case primaryUnifiedOverlayMissing:
	case primaryUnifiedOverlayDeleted:
		return plan, false, nil
	default:
		return plan, false, storeio.ErrCommonPrimaryLeafCorrupt
	}
	if oldLen != len(request.canonical) {
		return plan, false, nil
	}
	plan.fixedExtent = bytes.Equal(request.canonical, plan.baseValue)
	if !plan.fixedExtent &&
		route.Ref.Length == storeio.CommonPrimaryLeafMaxExtentBytes {
		return plan, false, nil
	}
	return plan, true, nil
}

func (c *Collection) terminatePrimaryJournalAdmissionCohort(
	ticket primaryJournalAdmissionTicket,
	self *primaryJournalAdmissionRequest,
	unresolvedAt int,
	sticky error,
) {
	drain, ok := c.primaryJournalAdmission.terminateAndDrain(
		ticket, unresolvedAt,
	)
	if !ok {
		panic("vibedb: recovery-journal terminal drain lost ownership")
	}
	if primaryJournalCohortAfterTerminalDrainHook != nil {
		primaryJournalCohortAfterTerminalDrainHook()
	}
	requests := drain.detached()
	for index, request := range requests {
		if index >= drain.unresolvedAt && request.result.err == nil {
			request.result.err = sticky
		}
	}
	c.primaryJournalAdmission.publishTransition(
		requests, self, nil, primaryJournalAdmissionSignal{},
	)
}

func (c *Collection) applyPrimaryJournalAdmissionRequest(
	request *primaryJournalAdmissionRequest,
) {
	if request == nil {
		panic("vibedb: nil recovery-journal admission request")
	}
	switch request.kind {
	case primaryMutationPut:
		request.result.created,
			request.result.continuation,
			request.result.err = c.putPrimaryPreparedWithSplitDetached(
			request.key, request.preparedInput(),
		)
	case primaryMutationDelete:
		request.result.deleted,
			request.result.continuation,
			request.result.err = c.deletePrimaryDetached(request.key)
	default:
		request.result.err = ErrClosed
	}
	if primaryJournalAdmissionRequestAppliedHook != nil {
		primaryJournalAdmissionRequestAppliedHook(request)
	}
}

// handoffPrimaryJournalAdmission seals the finite phase and makes the next
// caller runnable before the current caller releases preparation context or
// awaits device durability. This ordering is what lets several journal targets
// enter one existing group fence without a timer, yield, or extra goroutine.
func (c *Collection) handoffPrimaryJournalAdmission(
	ticket primaryJournalAdmissionTicket,
	self *primaryJournalAdmissionRequest,
) {
	decision, ok := c.primaryJournalAdmission.seal(ticket)
	if !ok {
		panic("vibedb: recovery-journal admission seal lost ownership")
	}
	c.primaryJournalAdmission.publishTransition(
		decision.completed.detached(), self, decision.leader,
		primaryJournalAdmissionSignal{
			role: decision.role, ticket: decision.ticket,
		},
	)
}

func (c *Collection) closePrimaryJournalAdmissionPassive(
	drain primaryJournalAdmissionDrain,
) {
	requests := drain.detached()
	for _, request := range requests {
		request.result.err = ErrClosed
	}
	if c.primaryJournalAdmission != nil {
		c.primaryJournalAdmission.publishTransition(
			requests, nil, nil, primaryJournalAdmissionSignal{},
		)
	}
}
