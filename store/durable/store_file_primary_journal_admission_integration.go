package durable

import (
	"errors"

	"github.com/thesyncim/vibejson/document"
)

// Package-private deterministic seams bracket only caller-owned work. They run
// after writer unlock and never under admission.mu; production leaves them nil.
var (
	primaryJournalAdmissionInitialAppliedHook func()
	primaryJournalAdmissionInitialHandoffHook func()
	primaryJournalAdmissionRequestAppliedHook func(*primaryJournalAdmissionRequest)
)

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
		return true, false, ErrClosed
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
		return true, false, ErrClosed
	}
	return c.finishPrimaryJournalAdmissionRequest(request, decision)
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
	switch role {
	case primaryJournalAdmissionBaselineRole:
		c.applyPrimaryJournalAdmissionRequest(self)
	case primaryJournalAdmissionCohortRole:
		batch, ok := c.primaryJournalAdmission.cohortBatch(ticket)
		if !ok {
			panic("vibedb: recovery-journal cohort baton lost batch")
		}
		for _, request := range batch {
			c.applyPrimaryJournalAdmissionRequest(request)
		}
	default:
		panic("vibedb: invalid recovery-journal admission baton")
	}
	c.handoffPrimaryJournalAdmission(ticket, self)
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
