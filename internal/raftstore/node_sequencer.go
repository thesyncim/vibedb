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
	submissionUnprepared uint32 = iota
	submissionIdle
	submissionQueued
	submissionWaiting
	submissionComplete
)

type submissionKind uint8

const (
	submissionReady submissionKind = iota + 1
	submissionBeginIncarnations
	submissionPersistIncarnations
	submissionRegisterGroup
	submissionDescriptorCatalog
)

// Submission is caller-owned storage for one immutable group Ready and its
// completion. Prepare may be called again only after completion. Ready and all
// values reachable through it remain immutable from successful TrySubmit until
// Poll or Wait reports completion.
type Submission struct {
	Ready        NodeReady
	kind         submissionKind
	count        uint8
	groups       [MaxPersistGroupBatches]uint64
	incarnations [MaxPersistGroupBatches]GroupIncarnation
	descriptor   GroupDescriptor
	catalog      descriptorCatalogCandidate
	state        atomic.Uint32
	ticket       atomic.Uint64
	err          error
	done         chan struct{}
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
	s.Ready, s.err, s.kind, s.count, s.catalog = ready, nil, submissionReady, 0, descriptorCatalogCandidate{}
	s.ticket.Store(0)
	s.state.Store(submissionIdle)
	return nil
}

func (s *Submission) invalidatePrepare() {
	s.Ready, s.err, s.kind, s.count, s.catalog = NodeReady{}, nil, 0, 0, descriptorCatalogCandidate{}
	s.ticket.Store(0)
	s.state.Store(submissionUnprepared)
}

func (s *Submission) prepareControl(kind submissionKind, count int) error {
	if s == nil || s.done == nil {
		return ErrInvalid
	}
	if state := s.state.Load(); state == submissionQueued || state == submissionWaiting {
		return ErrSubmissionPending
	}
	if count < 1 || count > MaxPersistGroupBatches {
		s.invalidatePrepare()
		return ErrInvalid
	}
	s.Ready, s.err, s.kind, s.count, s.catalog = NodeReady{}, nil, kind, uint8(count), descriptorCatalogCandidate{}
	s.ticket.Store(0)
	s.state.Store(submissionIdle)
	return nil
}

// PrepareBeginIncarnations copies caller-sorted dense log keys into fixed
// caller-owned Submission storage. Results are available through Incarnations
// after completion and remain valid until the next Prepare call.
func (s *Submission) PrepareBeginIncarnations(groups []uint64) error {
	if err := s.prepareControl(submissionBeginIncarnations, len(groups)); err != nil {
		return err
	}
	for i, group := range groups {
		if group == 0 || i > 0 && groups[i-1] >= group {
			s.invalidatePrepare()
			return ErrInvalid
		}
		s.groups[i] = group
	}
	return nil
}

// PreparePersistIncarnations copies an exact unknown-outcome retry into fixed
// caller-owned storage. The control operation is ordered in the same ticket
// stream as Ready persistence and forms a durability-wave boundary.
func (s *Submission) PreparePersistIncarnations(requests []GroupIncarnation) error {
	if err := s.prepareControl(submissionPersistIncarnations, len(requests)); err != nil {
		return err
	}
	for i, request := range requests {
		if request.GroupID == 0 || request.Incarnation == 0 || i > 0 && requests[i-1].GroupID >= request.GroupID {
			s.invalidatePrepare()
			return ErrInvalid
		}
		s.incarnations[i] = request
	}
	return nil
}

func (s *Submission) Incarnations() []GroupIncarnation {
	if s == nil || s.kind != submissionBeginIncarnations || s.state.Load() != submissionComplete {
		return nil
	}
	return s.incarnations[:s.count]
}

// PrepareRegisterGroup keeps the caller's immutable strings borrowed until
// completion; the fixed descriptor value itself is copied into the cell.
func (s *Submission) PrepareRegisterGroup(descriptor GroupDescriptor) error {
	if err := s.prepareControl(submissionRegisterGroup, 1); err != nil {
		return err
	}
	if descriptor.LogKey != 0 || validateGroupDescriptor(descriptor, true) != nil {
		s.invalidatePrepare()
		return ErrInvalid
	}
	s.descriptor = descriptor
	return nil
}

func (s *Submission) prepareDescriptorCatalog(candidate descriptorCatalogCandidate) error {
	if err := s.prepareControl(submissionDescriptorCatalog, 1); err != nil {
		return err
	}
	if candidate.id == ([16]byte{}) || candidate.through == 0 {
		s.invalidatePrepare()
		return ErrInvalid
	}
	s.catalog = candidate
	return nil
}

func (s *Submission) RegisteredGroup() (GroupDescriptor, GroupIncarnation, bool) {
	if s == nil || s.kind != submissionRegisterGroup || s.state.Load() != submissionComplete || s.err != nil {
		return GroupDescriptor{}, GroupIncarnation{}, false
	}
	return s.descriptor, s.incarnations[0], true
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

// Owns reports whether this sequencer is the sole submission owner installed
// on store. Adapters must prove this exact pointer binding before translating a
// portable group identity into its dense local log key.
func (q *NodeSubmissionSequencer) Owns(store *NodeStore) bool {
	return q != nil && store != nil && q.store == store
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
	state := submission.state.Load()
	if state != submissionIdle {
		if state == submissionQueued || state == submissionWaiting {
			return 0, ErrSubmissionPending
		}
		return 0, ErrInvalid
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
	if count == 1 && items[0].kind != submissionReady {
		s := items[0]
		switch s.kind {
		case submissionBeginIncarnations:
			return q.store.beginIncarnationsSequenced(s.groups[:s.count], s.incarnations[:s.count])
		case submissionPersistIncarnations:
			return q.store.persistIncarnationsSequenced(s.incarnations[:s.count])
		case submissionRegisterGroup:
			incarnation, registerErr := q.store.registerGroupSequenced(s.descriptor)
			if registerErr == nil {
				s.incarnations[0] = incarnation
				d, ok := q.store.descriptorForLogKey(incarnation.GroupID)
				if !ok {
					return ErrCorrupt
				}
				s.descriptor = d
			}
			return registerErr
		case submissionDescriptorCatalog:
			return q.store.publishDescriptorCatalogReferenceLocked(s.catalog, true)
		default:
			return ErrInvalid
		}
	}
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
			if !ok || count > 0 && (items[0].kind != submissionReady || s.kind != submissionReady) ||
				s.kind == submissionReady && submissionGroupSeen(&items, count, s.Ready.GroupID) {
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
