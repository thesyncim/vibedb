package durable

import (
	"sync"
	"sync/atomic"
)

// primaryJournalAdmissionLimit is the complete population bound for one
// admission engine. Future integration gives every follower one context from a
// pool with the same bound, so the detached batch and the arrival queue can
// never contain more requests than this in total.
const primaryJournalAdmissionLimit = primaryConcurrentContextLimit

const primaryJournalAdmissionPhaseBits = 2

type primaryJournalAdmissionPhase uint8

const (
	primaryJournalAdmissionIdle primaryJournalAdmissionPhase = iota
	primaryJournalAdmissionBaselinePilot
	primaryJournalAdmissionCohort
	primaryJournalAdmissionClosed
)

type primaryJournalAdmissionRole uint8

const (
	primaryJournalAdmissionNoRole primaryJournalAdmissionRole = iota
	primaryJournalAdmissionBaselineRole
	primaryJournalAdmissionCohortRole
)

type primaryJournalAdmissionSignal struct {
	done   bool
	role   primaryJournalAdmissionRole
	ticket primaryJournalAdmissionTicket
}

type primaryJournalAdmissionResult struct {
	created      bool
	deleted      bool
	err          error
	continuation primaryMutationDurabilityContinuation
}

// primaryJournalAdmissionRequest is one context-bound follower. key and raw
// remain borrowed from the blocked public caller. canonical remains owned by
// context until Apply consumes it; the caller releases that context before
// awaiting the returned durability continuation. The engine retains only
// pointers and never interprets payload fields.
type primaryJournalAdmissionRequest struct {
	ordinal       uint64
	kind          primaryMutationKind
	key           []byte
	raw           []byte
	rawLength     int
	canonical     []byte
	preflightErr  error
	prepared      bool
	forceBaseline bool
	context       *primaryConcurrentContext
	result        primaryJournalAdmissionResult
	signal        primaryJournalAdmissionSignal
	signaled      bool
}

// primaryJournalAdmissionObservation is a possibly stale lock-free phase
// sample. A follower may do bounded private preparation after taking it, but it
// must bind its request under the engine mutex before relying on the sample.
type primaryJournalAdmissionObservation struct {
	phase primaryJournalAdmissionPhase
	epoch uint64
}

// primaryJournalAdmissionTicket proves which caller owns the current applying
// role. It prevents a delayed former leader from sealing a later epoch.
type primaryJournalAdmissionTicket struct {
	role  primaryJournalAdmissionRole
	epoch uint64
}

// primaryJournalAdmissionDecision contains the next caller baton. The engine
// never sends it itself: the caller releases the mutex before signaling leader,
// so channels and future writer/journal work never run under admissionMu.
type primaryJournalAdmissionDecision struct {
	role      primaryJournalAdmissionRole
	ticket    primaryJournalAdmissionTicket
	leader    *primaryJournalAdmissionRequest
	completed primaryJournalAdmissionCompletion
	stale     bool
	closed    bool
	queued    bool
	selfBaton bool
}

// primaryJournalAdmissionCompletion owns the request pointers completed by one
// state transition. It is returned by value, so its storage never aliases the
// engine's batch/queue arrays and remains stable while a later phase reuses
// them. Integration signals and releases these requests only after seal or
// sealPressure returns.
type primaryJournalAdmissionCompletion struct {
	requests [primaryJournalAdmissionLimit]*primaryJournalAdmissionRequest
	count    int
}

func (c *primaryJournalAdmissionCompletion) detached() []*primaryJournalAdmissionRequest {
	if c == nil || c.count == 0 {
		return nil
	}
	return c.requests[:c.count:c.count]
}

// primaryJournalAdmissionActive describes work that was already detached to a
// caller when Close published the terminal phase. Close owns and detaches the
// passive queue; this active caller continues to own its prepared pilot or
// complete cohort and later seals the ticket only to release engine references.
type primaryJournalAdmissionActive struct {
	role       primaryJournalAdmissionRole
	ticket     primaryJournalAdmissionTicket
	leader     *primaryJournalAdmissionRequest
	batchCount int
	prepared   bool
}

// primaryJournalAdmissionDrain transfers request ownership out of the engine.
// A close drain contains only passive queued requests and reports the active
// owner separately. A terminal drain contains the complete active phase first,
// followed by later queued arrivals; unresolvedAt identifies the first failed
// active request. Fixed storage keeps the transition allocation-free.
type primaryJournalAdmissionDrain struct {
	requests     [primaryJournalAdmissionLimit]*primaryJournalAdmissionRequest
	count        int
	activeCount  int
	unresolvedAt int
	active       primaryJournalAdmissionActive
}

func (d *primaryJournalAdmissionDrain) detached() []*primaryJournalAdmissionRequest {
	if d == nil || d.count == 0 {
		return nil
	}
	return d.requests[:d.count:d.count]
}

// primaryJournalAdmission is a bounded caller-driven admission state machine.
// There is exactly one applying baseline pilot or cohort leader. Requests that
// arrive while it works accumulate in queue; a cohort leader owns the immutable
// detached batch while later arrivals continue to use queue.
//
// Admission transitions never start a goroutine, yield, sleep, signal a
// channel, or allocate. Prepared followers may wait on the embedded condition;
// callers carry every applying baton and one transition wakes its complete
// fixed signal batch once.
type primaryJournalAdmission struct {
	mu sync.Mutex

	phase primaryJournalAdmissionPhase
	epoch uint64
	word  atomic.Uint64

	queue [primaryJournalAdmissionLimit]*primaryJournalAdmissionRequest
	count int

	batch      [primaryJournalAdmissionLimit]*primaryJournalAdmissionRequest
	batchCount int
	// preparedPilot is true only when the baseline pilot itself owns a follower
	// context. The initial literal-baseline pilot owns none. Counting it keeps
	// batch + queue + promoted pilot within the future context-pool bound.
	preparedPilot bool
	activeLeader  *primaryJournalAdmissionRequest
	activeRole    primaryJournalAdmissionRole
	wake          sync.Cond

	// Requests are indexed by the equally bounded context pool. A slot cannot
	// be rebound until its caller consumes the final signal and returns context.
	requests [primaryJournalAdmissionLimit]primaryJournalAdmissionRequest
}

func newPrimaryJournalAdmission() *primaryJournalAdmission {
	a := new(primaryJournalAdmission)
	a.wake.L = &a.mu
	for index := range a.requests {
		a.requests[index].ordinal = uint64(index + 1)
	}
	return a
}

func (a *primaryJournalAdmission) bindRequest(
	context *primaryConcurrentContext,
) *primaryJournalAdmissionRequest {
	if a == nil || context == nil {
		return nil
	}
	slot := int(context.poolSlot)
	if slot < 0 || slot >= len(a.requests) {
		panic("vibedb: invalid recovery-journal admission context")
	}
	request := &a.requests[slot]
	if request.signaled {
		panic("vibedb: stale recovery-journal admission signal")
	}
	ordinal := request.ordinal
	*request = primaryJournalAdmissionRequest{
		ordinal: ordinal, context: context,
	}
	return request
}

func (a *primaryJournalAdmission) waitSignal(
	request *primaryJournalAdmissionRequest,
) primaryJournalAdmissionSignal {
	if a == nil || request == nil {
		return primaryJournalAdmissionSignal{done: true}
	}
	a.mu.Lock()
	for !request.signaled {
		a.wake.Wait()
	}
	signal := request.signal
	request.signal = primaryJournalAdmissionSignal{}
	request.signaled = false
	a.mu.Unlock()
	return signal
}

// publishTransition publishes one complete state transition with one mutex
// acquisition and one Broadcast. self already carries its result/baton and is
// omitted. No callback, writer work, context release, or durability wait runs
// under admission.mu.
func (a *primaryJournalAdmission) publishTransition(
	completed []*primaryJournalAdmissionRequest,
	self, leader *primaryJournalAdmissionRequest,
	leaderSignal primaryJournalAdmissionSignal,
) {
	if a == nil || len(completed) == 0 && leader == nil {
		return
	}
	a.mu.Lock()
	published := false
	for _, request := range completed {
		if request == self {
			continue
		}
		if request == nil || request == leader || request.signaled {
			a.mu.Unlock()
			panic("vibedb: duplicate recovery-journal admission signal")
		}
		request.signal = primaryJournalAdmissionSignal{done: true}
		request.signaled = true
		published = true
	}
	if leader != nil && leader != self {
		if leader.signaled {
			a.mu.Unlock()
			panic("vibedb: duplicate recovery-journal admission leader signal")
		}
		leader.signal = leaderSignal
		leader.signaled = true
		published = true
	}
	if published {
		a.wake.Broadcast()
	}
	a.mu.Unlock()
}

func (r *primaryJournalAdmissionRequest) preparedInput() primaryPreparedPutInput {
	if r == nil {
		return primaryPreparedPutInput{}
	}
	return primaryPreparedPutInput{
		raw: r.raw, rawLength: r.rawLength, canonical: r.canonical,
		preflightErr: r.preflightErr, prepared: r.prepared,
	}
}

func (r *primaryJournalAdmissionRequest) resetPayload() {
	if r == nil {
		return
	}
	r.kind = 0
	r.key = nil
	r.raw = nil
	r.rawLength = 0
	r.canonical = nil
	r.preflightErr = nil
	r.prepared = false
	r.forceBaseline = false
	r.context = nil
	r.result = primaryJournalAdmissionResult{}
	if r.signaled || r.signal != (primaryJournalAdmissionSignal{}) {
		panic("vibedb: reset signaled recovery-journal admission request")
	}
}

func packPrimaryJournalAdmissionWord(
	phase primaryJournalAdmissionPhase, epoch uint64,
) uint64 {
	return epoch<<primaryJournalAdmissionPhaseBits | uint64(phase)
}

func unpackPrimaryJournalAdmissionWord(
	word uint64,
) primaryJournalAdmissionObservation {
	return primaryJournalAdmissionObservation{
		phase: primaryJournalAdmissionPhase(
			word & (1<<primaryJournalAdmissionPhaseBits - 1),
		),
		epoch: word >> primaryJournalAdmissionPhaseBits,
	}
}

func (a *primaryJournalAdmission) observe() primaryJournalAdmissionObservation {
	if a == nil {
		return primaryJournalAdmissionObservation{
			phase: primaryJournalAdmissionClosed,
		}
	}
	return unpackPrimaryJournalAdmissionWord(a.word.Load())
}

// tryStartInitialPilot preserves the no-follower data-path choice: the first
// caller receives a baseline role before it borrows any follower context. The
// future public integration invokes the literal established Put/Delete path for
// this ticket.
func (a *primaryJournalAdmission) tryStartInitialPilot() (
	primaryJournalAdmissionTicket, bool,
) {
	if a == nil {
		return primaryJournalAdmissionTicket{}, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.phase != primaryJournalAdmissionIdle {
		return primaryJournalAdmissionTicket{}, false
	}
	return a.openLocked(
		primaryJournalAdmissionBaselineRole, nil, false,
	), true
}

// admitPrepared binds a prepared follower to the current phase. The observation
// may span arbitrarily many completed epochs: revalidation under mu either
// queues it behind the current leader or, if the engine is idle, promotes the
// follower itself to a prepared baseline pilot. It never enqueues into the
// observed old epoch and never needs an unbounded retry loop.
func (a *primaryJournalAdmission) admitPrepared(
	request *primaryJournalAdmissionRequest,
	observed primaryJournalAdmissionObservation,
) primaryJournalAdmissionDecision {
	if a == nil || request == nil {
		return primaryJournalAdmissionDecision{closed: true}
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	stale := observed.phase != a.phase || observed.epoch != a.epoch
	switch a.phase {
	case primaryJournalAdmissionClosed:
		return primaryJournalAdmissionDecision{stale: stale, closed: true}
	case primaryJournalAdmissionIdle:
		ticket := a.openLocked(
			primaryJournalAdmissionBaselineRole, request, true,
		)
		return primaryJournalAdmissionDecision{
			role: primaryJournalAdmissionBaselineRole, ticket: ticket,
			leader: request, stale: stale,
		}
	case primaryJournalAdmissionBaselinePilot, primaryJournalAdmissionCohort:
		owned := a.count + a.batchCount
		if a.preparedPilot {
			owned++
		}
		if a.count >= len(a.queue) || owned >= primaryJournalAdmissionLimit {
			// Future integration makes this unreachable by giving every request
			// one slot from an equally bounded context pool. Panic before a broken
			// ownership invariant can overwrite the fixed queue.
			panic("vibedb: recovery-journal admission ownership exceeds bound")
		}
		a.queue[a.count] = request
		a.count++
		return primaryJournalAdmissionDecision{stale: stale, queued: true}
	default:
		return primaryJournalAdmissionDecision{stale: stale, closed: true}
	}
}

// seal completes one baseline or cohort phase and chooses the next finite
// phase from callers already queued. A single request always becomes a baseline
// pilot. Two or more form one detached cohort snapshot; arrivals after this
// method releases mu belong only to the following queue.
func (a *primaryJournalAdmission) seal(
	ticket primaryJournalAdmissionTicket,
) (primaryJournalAdmissionDecision, bool) {
	if a == nil {
		return primaryJournalAdmissionDecision{}, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.phase == primaryJournalAdmissionClosed &&
		a.ticketOwnsActiveLocked(ticket) {
		completion := a.completeActiveLocked(ticket)
		a.clearActiveLocked()
		return primaryJournalAdmissionDecision{
			completed: completion, closed: true,
		}, true
	}
	if !a.ticketOwnsCurrentLocked(ticket) {
		return primaryJournalAdmissionDecision{}, false
	}
	completion := a.completeActiveLocked(ticket)
	decision := a.selectNextLocked(false, false)
	decision.completed = completion
	return decision, true
}

// sealPressure completes a cohort after a successful prefix and moves the
// untouched suffix ahead of every arrival accumulated during publication. Its
// first member is forced through the established baseline path; the remaining
// suffix stays ahead of later arrivals while that pilot resolves pressure.
//
// Integration must call sealPressure BEFORE signaling or releasing successful
// prefix contexts. Until this transition completes, batchCount deliberately
// accounts for every context owned by the phase; releasing one early could let
// a new claimant make the conservative ownership sum appear over capacity.
func (a *primaryJournalAdmission) sealPressure(
	ticket primaryJournalAdmissionTicket, suffixAt int,
) (primaryJournalAdmissionDecision, bool) {
	if a == nil {
		return primaryJournalAdmissionDecision{}, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.ticketOwnsCurrentLocked(ticket) ||
		ticket.role != primaryJournalAdmissionCohortRole ||
		suffixAt < 0 || suffixAt >= a.batchCount {
		return primaryJournalAdmissionDecision{}, false
	}

	suffixCount := a.batchCount - suffixAt
	if suffixCount+a.count > len(a.queue) {
		panic("vibedb: recovery-journal admission ownership exceeds bound")
	}
	var completion primaryJournalAdmissionCompletion
	copy(completion.requests[:suffixAt], a.batch[:suffixAt])
	completion.count = suffixAt
	copy(
		a.queue[suffixCount:suffixCount+a.count],
		a.queue[:a.count],
	)
	copy(a.queue[:suffixCount], a.batch[suffixAt:a.batchCount])
	a.count += suffixCount
	a.clearBatchLocked()
	a.clearActiveLocked()
	decision := a.selectNextLocked(true, suffixAt == 0)
	decision.completed = completion
	return decision, true
}

// cohortBatch returns the immutable snapshot for the current cohort leader.
// Only that leader may call it, and it must stop using the returned view before
// sealing the ticket. Later followers mutate queue, never batch.
func (a *primaryJournalAdmission) cohortBatch(
	ticket primaryJournalAdmissionTicket,
) ([]*primaryJournalAdmissionRequest, bool) {
	if a == nil {
		return nil, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.ticketOwnsActiveLocked(ticket) ||
		ticket.role != primaryJournalAdmissionCohortRole ||
		a.batchCount < 2 {
		return nil, false
	}
	return a.batch[:a.batchCount:a.batchCount], true
}

// closeAndDrain atomically rejects future admission and detaches every passive
// queued request for signaling after mu is released. Work already detached to
// a baseline pilot or cohort remains owned by that caller; active reports the
// exact ticket and request population. That caller must finish its established
// result path and call seal, which observes Closed and only clears references --
// it can never select a later baton.
func (a *primaryJournalAdmission) closeAndDrain() primaryJournalAdmissionDrain {
	var drain primaryJournalAdmissionDrain
	if a == nil {
		return drain
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.phase == primaryJournalAdmissionClosed {
		return drain
	}

	if a.activeRole != primaryJournalAdmissionNoRole {
		drain.active = primaryJournalAdmissionActive{
			role: a.activeRole,
			ticket: primaryJournalAdmissionTicket{
				role: a.activeRole, epoch: a.epoch,
			},
			leader: a.activeLeader, batchCount: a.batchCount,
			prepared: a.preparedPilot,
		}
	}
	copy(drain.requests[:a.count], a.queue[:a.count])
	drain.count = a.count
	clear(a.queue[:a.count])
	a.count = 0
	a.phase = primaryJournalAdmissionClosed
	a.word.Store(packPrimaryJournalAdmissionWord(a.phase, a.epoch))
	return drain
}

// terminateAndDrain is the poison transition owned by the current caller. It
// publishes Closed and transfers the complete active phase plus every later
// arrival into one fixed drain. Unlike seal, it never chooses or signals a
// baton. For a cohort, unresolvedAt is the first request whose terminal result
// applies; earlier active entries are the already-published prefix. A baseline
// caller passes zero. The caller itself carries any returned self request.
func (a *primaryJournalAdmission) terminateAndDrain(
	ticket primaryJournalAdmissionTicket, unresolvedAt int,
) (primaryJournalAdmissionDrain, bool) {
	var drain primaryJournalAdmissionDrain
	if a == nil {
		return drain, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.ticketOwnsActiveLocked(ticket) {
		return drain, false
	}

	switch ticket.role {
	case primaryJournalAdmissionBaselineRole:
		if unresolvedAt != 0 {
			return drain, false
		}
		if a.activeLeader != nil {
			drain.requests[drain.count] = a.activeLeader
			drain.count++
			drain.activeCount = 1
		}
	case primaryJournalAdmissionCohortRole:
		if unresolvedAt < 0 || unresolvedAt >= a.batchCount {
			return drain, false
		}
		copy(drain.requests[:a.batchCount], a.batch[:a.batchCount])
		drain.count = a.batchCount
		drain.activeCount = a.batchCount
		drain.unresolvedAt = unresolvedAt
	default:
		return drain, false
	}
	if drain.count+a.count > len(drain.requests) {
		panic("vibedb: recovery-journal admission ownership exceeds bound")
	}
	copy(drain.requests[drain.count:drain.count+a.count], a.queue[:a.count])
	drain.count += a.count
	clear(a.queue[:a.count])
	a.count = 0
	a.clearBatchLocked()
	a.clearActiveLocked()
	a.phase = primaryJournalAdmissionClosed
	a.word.Store(packPrimaryJournalAdmissionWord(a.phase, a.epoch))
	return drain, true
}

func (a *primaryJournalAdmission) openLocked(
	role primaryJournalAdmissionRole,
	leader *primaryJournalAdmissionRequest,
	prepared bool,
) primaryJournalAdmissionTicket {
	if a.epoch == ^uint64(0)>>primaryJournalAdmissionPhaseBits {
		panic("vibedb: recovery-journal admission epoch exhausted")
	}
	a.epoch++
	switch role {
	case primaryJournalAdmissionBaselineRole:
		a.phase = primaryJournalAdmissionBaselinePilot
		if prepared != (leader != nil) {
			panic("vibedb: invalid prepared recovery-journal pilot")
		}
	case primaryJournalAdmissionCohortRole:
		a.phase = primaryJournalAdmissionCohort
		if leader == nil || !prepared {
			panic("vibedb: invalid recovery-journal cohort leader")
		}
	default:
		panic("vibedb: invalid recovery-journal admission role")
	}
	a.activeRole = role
	a.activeLeader = leader
	a.preparedPilot = role == primaryJournalAdmissionBaselineRole && prepared
	a.word.Store(packPrimaryJournalAdmissionWord(a.phase, a.epoch))
	return primaryJournalAdmissionTicket{role: role, epoch: a.epoch}
}

func (a *primaryJournalAdmission) setIdleLocked() {
	a.clearActiveLocked()
	a.phase = primaryJournalAdmissionIdle
	a.word.Store(packPrimaryJournalAdmissionWord(a.phase, a.epoch))
}

func (a *primaryJournalAdmission) ticketOwnsActiveLocked(
	ticket primaryJournalAdmissionTicket,
) bool {
	return ticket.epoch == a.epoch && ticket.role == a.activeRole &&
		ticket.role != primaryJournalAdmissionNoRole
}

func (a *primaryJournalAdmission) ticketOwnsCurrentLocked(
	ticket primaryJournalAdmissionTicket,
) bool {
	if ticket.epoch != a.epoch {
		return false
	}
	return a.ticketOwnsActiveLocked(ticket) &&
		(ticket.role == primaryJournalAdmissionBaselineRole &&
			a.phase == primaryJournalAdmissionBaselinePilot ||
			ticket.role == primaryJournalAdmissionCohortRole &&
				a.phase == primaryJournalAdmissionCohort)
}

func (a *primaryJournalAdmission) selectNextLocked(
	forceBaseline, selfBaton bool,
) primaryJournalAdmissionDecision {
	if a.count == 0 {
		a.setIdleLocked()
		return primaryJournalAdmissionDecision{}
	}
	if forceBaseline || a.count == 1 {
		leader := a.queue[0]
		copy(a.queue[:a.count-1], a.queue[1:a.count])
		a.count--
		a.queue[a.count] = nil
		ticket := a.openLocked(
			primaryJournalAdmissionBaselineRole, leader, true,
		)
		return primaryJournalAdmissionDecision{
			role:   primaryJournalAdmissionBaselineRole,
			ticket: ticket, leader: leader, selfBaton: selfBaton,
		}
	}

	if a.batchCount != 0 {
		panic("vibedb: recovery-journal admission batch still owned")
	}
	copy(a.batch[:a.count], a.queue[:a.count])
	a.batchCount = a.count
	clear(a.queue[:a.count])
	a.count = 0
	ticket := a.openLocked(
		primaryJournalAdmissionCohortRole, a.batch[0], true,
	)
	return primaryJournalAdmissionDecision{
		role:   primaryJournalAdmissionCohortRole,
		ticket: ticket, leader: a.batch[0],
	}
}

func (a *primaryJournalAdmission) clearBatchLocked() {
	clear(a.batch[:a.batchCount])
	a.batchCount = 0
}

func (a *primaryJournalAdmission) completeActiveLocked(
	ticket primaryJournalAdmissionTicket,
) primaryJournalAdmissionCompletion {
	var completion primaryJournalAdmissionCompletion
	switch ticket.role {
	case primaryJournalAdmissionBaselineRole:
		if a.preparedPilot && a.activeLeader != nil {
			completion.requests[0] = a.activeLeader
			completion.count = 1
		}
	case primaryJournalAdmissionCohortRole:
		copy(completion.requests[:a.batchCount], a.batch[:a.batchCount])
		completion.count = a.batchCount
		a.clearBatchLocked()
	}
	a.clearActiveLocked()
	return completion
}

func (a *primaryJournalAdmission) clearActiveLocked() {
	a.activeLeader = nil
	a.activeRole = primaryJournalAdmissionNoRole
	a.preparedPilot = false
}
