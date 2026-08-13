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

// primaryJournalAdmissionRequest is deliberately only an identity in phase 1.
// The integration phase will embed the borrowed key, prepared canonical value,
// provisional validation result, and caller-owned result signal. The state
// engine retains only pointers, so request lifetime remains owned by the
// blocked caller.
type primaryJournalAdmissionRequest struct {
	// ordinal is assigned by the future caller-side request preparation. The
	// state engine does not interpret it; retaining a non-zero-sized request also
	// gives every caller an unambiguous pointer identity.
	ordinal uint64
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
	role   primaryJournalAdmissionRole
	ticket primaryJournalAdmissionTicket
	leader *primaryJournalAdmissionRequest
	stale  bool
	closed bool
	queued bool
}

// primaryJournalAdmission is a bounded caller-driven admission state machine.
// There is exactly one applying baseline pilot or cohort leader. Requests that
// arrive while it works accumulate in queue; a cohort leader owns the immutable
// detached batch while later arrivals continue to use queue.
//
// No method waits, starts a goroutine, yields, sleeps, signals a channel, or
// allocates. Callers carry every returned baton.
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
	return a.openLocked(primaryJournalAdmissionBaselineRole), true
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
		ticket := a.openLocked(primaryJournalAdmissionBaselineRole)
		a.preparedPilot = true
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
	if !a.ticketOwnsCurrentLocked(ticket) {
		return primaryJournalAdmissionDecision{}, false
	}
	if ticket.role == primaryJournalAdmissionCohortRole {
		a.clearBatchLocked()
	} else {
		a.preparedPilot = false
	}
	return a.selectNextLocked(false), true
}

// sealPressure completes a cohort after a successful prefix and moves the
// untouched suffix ahead of every arrival accumulated during publication. Its
// first member is forced through the established baseline path; the remaining
// suffix stays ahead of later arrivals while that pilot resolves pressure.
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
	copy(
		a.queue[suffixCount:suffixCount+a.count],
		a.queue[:a.count],
	)
	copy(a.queue[:suffixCount], a.batch[suffixAt:a.batchCount])
	a.count += suffixCount
	a.clearBatchLocked()
	return a.selectNextLocked(true), true
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
	if !a.ticketOwnsCurrentLocked(ticket) ||
		ticket.role != primaryJournalAdmissionCohortRole ||
		a.batchCount < 2 {
		return nil, false
	}
	return a.batch[:a.batchCount:a.batchCount], true
}

func (a *primaryJournalAdmission) openLocked(
	role primaryJournalAdmissionRole,
) primaryJournalAdmissionTicket {
	if a.epoch == ^uint64(0)>>primaryJournalAdmissionPhaseBits {
		panic("vibedb: recovery-journal admission epoch exhausted")
	}
	a.epoch++
	switch role {
	case primaryJournalAdmissionBaselineRole:
		a.phase = primaryJournalAdmissionBaselinePilot
	case primaryJournalAdmissionCohortRole:
		a.phase = primaryJournalAdmissionCohort
	default:
		panic("vibedb: invalid recovery-journal admission role")
	}
	a.word.Store(packPrimaryJournalAdmissionWord(a.phase, a.epoch))
	return primaryJournalAdmissionTicket{role: role, epoch: a.epoch}
}

func (a *primaryJournalAdmission) setIdleLocked() {
	a.phase = primaryJournalAdmissionIdle
	a.word.Store(packPrimaryJournalAdmissionWord(a.phase, a.epoch))
}

func (a *primaryJournalAdmission) ticketOwnsCurrentLocked(
	ticket primaryJournalAdmissionTicket,
) bool {
	if ticket.epoch != a.epoch {
		return false
	}
	return ticket.role == primaryJournalAdmissionBaselineRole &&
		a.phase == primaryJournalAdmissionBaselinePilot ||
		ticket.role == primaryJournalAdmissionCohortRole &&
			a.phase == primaryJournalAdmissionCohort
}

func (a *primaryJournalAdmission) selectNextLocked(
	forceBaseline bool,
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
		a.preparedPilot = true
		ticket := a.openLocked(primaryJournalAdmissionBaselineRole)
		return primaryJournalAdmissionDecision{
			role:   primaryJournalAdmissionBaselineRole,
			ticket: ticket, leader: leader,
		}
	}

	if a.batchCount != 0 {
		panic("vibedb: recovery-journal admission batch still owned")
	}
	copy(a.batch[:a.count], a.queue[:a.count])
	a.batchCount = a.count
	clear(a.queue[:a.count])
	a.count = 0
	ticket := a.openLocked(primaryJournalAdmissionCohortRole)
	return primaryJournalAdmissionDecision{
		role:   primaryJournalAdmissionCohortRole,
		ticket: ticket, leader: a.batch[0],
	}
}

func (a *primaryJournalAdmission) clearBatchLocked() {
	clear(a.batch[:a.batchCount])
	a.batchCount = 0
}
