package raftstore

import (
	"errors"
	"sync"
	"sync/atomic"
)

var (
	ErrSubmissionBackpressure = errors.New("raftstore: node submission ring is full")
	ErrSubmissionPending      = errors.New("raftstore: submission is already pending")
	ErrSubmissionPanic        = errors.New("raftstore: node submission worker panicked")
	ErrSequencerActive        = errors.New("raftstore: direct persistence disabled while node sequencer is active")
)

const (
	submissionIdle uint32 = iota
	submissionQueued
	submissionWaiting
	submissionComplete
)

// Submission is caller-owned storage for one immutable group Ready and its
// completion. Prepare may be called again only after completion. Ready and all
// values reachable through it remain immutable from successful TrySubmit until
// Poll or Wait reports completion.
type Submission struct {
	Ready  NodeReady
	state  atomic.Uint32
	ticket atomic.Uint64
	err    error
	done   chan struct{}
}

// Initialize allocates the cold-path wait edge. The Submission itself remains
// caller-owned and can be reused without allocation through Prepare. Exactly
// one goroutine may call Wait for each successful submission.
func (s *Submission) Initialize() error {
	if s == nil || s.state.Load() == submissionQueued || s.state.Load() == submissionWaiting {
		return ErrSubmissionPending
	}
	if s.done == nil {
		s.done = make(chan struct{}, 1)
	}
	return nil
}

func (s *Submission) Prepare(ready NodeReady) error {
	if s == nil || s.done == nil {
		return ErrInvalid
	}
	if state := s.state.Load(); state == submissionQueued || state == submissionWaiting {
		return ErrSubmissionPending
	}
	s.Ready, s.err = ready, nil
	s.ticket.Store(0)
	s.state.Store(submissionIdle)
	return nil
}

func (s *Submission) Poll() (ticket uint64, done bool, err error) {
	if s == nil || s.state.Load() != submissionComplete {
		return 0, false, nil
	}
	return s.ticket.Load(), true, s.err
}

func (s *Submission) Wait() (uint64, error) {
	if s == nil || s.done == nil {
		return 0, ErrInvalid
	}
	for {
		switch state := s.state.Load(); state {
		case submissionComplete:
			return s.ticket.Load(), s.err
		case submissionQueued:
			if !s.state.CompareAndSwap(submissionQueued, submissionWaiting) {
				continue
			}
			<-s.done
		case submissionWaiting:
			return 0, ErrSubmissionPending
		default:
			return 0, ErrInvalid
		}
	}
}

// Separate cache lines keep producer reservation and consumer retirement from
// invalidating each other under sustained MPSC traffic.
type submissionRingIndex struct {
	value atomic.Uint64
	_     [56]byte
}

// Each slot occupies its own cache line. sequence publishes the pointer after
// initialization and releases the slot only after the consumer has copied it.
type submissionRingSlot struct {
	sequence atomic.Uint64
	value    *Submission
	_        [48]byte
}

type NodeSubmissionSequencer struct {
	store *NodeStore
	ring  []submissionRingSlot
	mask  uint64

	head submissionRingIndex
	tail submissionRingIndex

	wake       chan struct{}
	drained    chan struct{}
	done       chan struct{}
	closed     atomic.Bool
	submitters atomic.Int64
	stopOnce   sync.Once
	drainOnce  sync.Once
	fatal      atomic.Pointer[sequencerFailure]

	persist func([]NodeReady) error

	submitHookTest   func()
	claimedHookTest  func()
	completeHookTest func(*Submission, uint32)
}

type sequencerFailure struct{ err error }

func NewNodeSubmissionSequencer(store *NodeStore, capacity int) (*NodeSubmissionSequencer, error) {
	if store == nil || capacity < 2 || capacity&(capacity-1) != 0 || capacity > 1<<20 {
		return nil, ErrBounds
	}
	q := &NodeSubmissionSequencer{
		store: store, ring: make([]submissionRingSlot, capacity), mask: uint64(capacity - 1),
		wake: make(chan struct{}, 1), drained: make(chan struct{}), done: make(chan struct{}),
	}
	q.persist = store.persistSequencedWave
	for i := range q.ring {
		q.ring[i].sequence.Store(uint64(i))
	}
	store.mu.Lock()
	if store.closingFlag.Load() || store.closed || store.closing {
		store.mu.Unlock()
		return nil, ErrClosed
	}
	if store.sequencer != nil {
		store.mu.Unlock()
		return nil, ErrInvalid
	}
	store.sequencer = q
	store.mu.Unlock()
	go q.run()
	return q, nil
}

func (q *NodeSubmissionSequencer) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (q *NodeSubmissionSequencer) TrySubmit(submission *Submission) (uint64, error) {
	if q == nil || submission == nil || submission.done == nil {
		return 0, ErrInvalid
	}
	q.submitters.Add(1)
	defer func() {
		if q.submitters.Add(-1) == 0 && q.closed.Load() {
			q.drainOnce.Do(func() { close(q.drained) })
		}
	}()
	if fatal := q.fatal.Load(); fatal != nil {
		return 0, errors.Join(ErrPersistenceUnknown, fatal.err)
	}
	if q.closed.Load() {
		return 0, ErrClosed
	}
	if !submission.state.CompareAndSwap(submissionIdle, submissionQueued) {
		return 0, ErrSubmissionPending
	}
	if q.submitHookTest != nil {
		q.submitHookTest()
	}
	for attempt := 0; attempt < 32; attempt++ {
		position := q.tail.value.Load()
		// Unsigned subtraction is intentional: with capacity <= 2^20 and at
		// most capacity outstanding tickets, it remains correct across wrap.
		if position-q.head.value.Load() >= uint64(len(q.ring)) {
			submission.state.Store(submissionIdle)
			return 0, ErrSubmissionBackpressure
		}
		if q.tail.value.CompareAndSwap(position, position+1) {
			ticket := position + 1
			slot := &q.ring[position&q.mask]
			if q.claimedHookTest != nil {
				q.claimedHookTest()
			}
			slot.value = submission
			submission.ticket.Store(ticket)
			slot.sequence.Store(ticket)
			q.signal()
			return ticket, nil
		}
	}
	submission.state.Store(submissionIdle)
	return 0, ErrSubmissionBackpressure
}

func (q *NodeSubmissionSequencer) peek() (*Submission, bool) {
	position := q.head.value.Load()
	if position == q.tail.value.Load() {
		return nil, false
	}
	slot := &q.ring[position&q.mask]
	if slot.sequence.Load() != position+1 {
		return nil, false
	}
	return slot.value, slot.value != nil
}

func (q *NodeSubmissionSequencer) pop() *Submission {
	position := q.head.value.Load()
	slot := &q.ring[position&q.mask]
	value := slot.value
	slot.value = nil
	slot.sequence.Store(position + uint64(len(q.ring)))
	q.head.value.Store(position + 1)
	return value
}

func submissionGroupSeen(items *[MaxPersistGroupBatches]*Submission, count int, group uint64) bool {
	for i := 0; i < count; i++ {
		if items[i].Ready.GroupID == group {
			return true
		}
	}
	return false
}

func (q *NodeSubmissionSequencer) complete(s *Submission, err error) {
	s.err = err
	previous := s.state.Swap(submissionComplete)
	if q.completeHookTest != nil {
		q.completeHookTest(s, previous)
	}
	if previous == submissionWaiting {
		// Wait registered before completion won. There is exactly one waiter
		// and no prior token, so this send cannot block or become stale across
		// Submission reuse. If completion displaced Queued, Wait observes
		// Complete directly and no notification is needed.
		s.done <- struct{}{}
	}
}

func (q *NodeSubmissionSequencer) runWave(items *[MaxPersistGroupBatches]*Submission, ready *[MaxPersistGroupBatches]NodeReady, count int) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrSubmissionPanic
		}
	}()
	for i := 0; i < count; i++ {
		value := items[i].Ready
		at := i
		for at > 0 && ready[at-1].GroupID > value.GroupID {
			ready[at] = ready[at-1]
			at--
		}
		ready[at] = value
	}
	err = q.persist(ready[:count])
	clear(ready[:count])
	return err
}

func (q *NodeSubmissionSequencer) failAccepted(err error) {
	for {
		s, ok := q.peek()
		if !ok {
			return
		}
		q.pop()
		q.complete(s, err)
	}
}

func (q *NodeSubmissionSequencer) run() {
	defer close(q.done)
	var items [MaxPersistGroupBatches]*Submission
	var ready [MaxPersistGroupBatches]NodeReady
	for {
		count := 0
		for count < len(items) {
			s, ok := q.peek()
			if !ok || submissionGroupSeen(&items, count, s.Ready.GroupID) {
				break
			}
			items[count] = q.pop()
			count++
		}
		if count == 0 {
			if q.closed.Load() && q.head.value.Load() == q.tail.value.Load() {
				return
			}
			<-q.wake
			continue
		}
		err := q.runWave(&items, &ready, count)
		fatal := errors.Is(err, ErrPersistenceUnknown) || errors.Is(err, ErrSubmissionPanic)
		completionErr := err
		if fatal {
			completionErr = errors.Join(ErrPersistenceUnknown, err)
		}
		for i := 0; i < count; i++ {
			q.complete(items[i], completionErr)
			items[i] = nil
		}
		if fatal {
			failure := &sequencerFailure{err: completionErr}
			q.fatal.CompareAndSwap(nil, failure)
			q.closed.Store(true)
			if q.submitters.Load() == 0 {
				q.drainOnce.Do(func() { close(q.drained) })
			}
			<-q.drained
			q.failAccepted(completionErr)
			return
		}
	}
}

// Close rejects new submissions, waits for producers already inside TrySubmit,
// drains every accepted ticket exactly once, and then stops the worker. It does
// not close the underlying NodeStore.
func (q *NodeSubmissionSequencer) Close() error {
	if q == nil {
		return nil
	}
	q.stopOnce.Do(func() {
		q.closed.Store(true)
		if q.submitters.Load() == 0 {
			q.drainOnce.Do(func() { close(q.drained) })
		}
		<-q.drained
		q.signal()
	})
	<-q.done
	if failure := q.fatal.Load(); failure != nil {
		return errors.Join(ErrPersistenceUnknown, failure.err)
	}
	return nil
}
