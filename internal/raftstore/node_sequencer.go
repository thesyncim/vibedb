package raftstore

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
)

var (
	ErrSubmissionBackpressure = errors.New("raftstore: node submission ring is full")
	ErrSubmissionPending      = errors.New("raftstore: submission is already pending")
	ErrSubmissionPanic        = errors.New("raftstore: node submission worker panicked")
	ErrSequencerActive        = errors.New("raftstore: direct persistence disabled while node sequencer is active")
	ErrMaintenanceActive      = errors.New("raftstore: node maintenance lane already has an owner")
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
	submissionCheckpoint
)

// Submission is caller-owned storage for one immutable group Ready and its
// completion. Prepare may be called again only after completion. Ready and all
// values reachable through it remain immutable from successful TrySubmit until
// Poll or Wait reports completion.
type Submission struct {
	Ready NodeReady
	// readySeries is fixed caller-owned storage. PrepareReadySeries copies
	// descriptors here and leaves their reachable protobuf values borrowed until
	// completion, matching the ordinary Prepare ownership contract.
	readySeries  [MaxReadySeries]raftmodel.PersistBatch
	seriesGroup  uint64
	seriesCount  uint8
	kind         submissionKind
	count        uint8
	groups       [MaxPersistGroupBatches]uint64
	incarnations [MaxPersistGroupBatches]GroupIncarnation
	descriptor   GroupDescriptor
	catalog      descriptorCatalogCandidate
	snapshot     *pb.Snapshot
	// queuedAt is published before the ring sequence. Its monotonic component
	// makes queue delay immune to UTC wall-clock steps. It is diagnostic only;
	// no ordering or durability decision depends on it.
	queuedAt time.Time
	state    atomic.Uint32
	ticket   atomic.Uint64
	err      error
	done     chan struct{}
	wake     atomic.Pointer[nodeSequencerWake]
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
	clear(s.readySeries[:])
	s.Ready, s.seriesGroup, s.seriesCount = ready, 0, 0
	s.err, s.kind, s.count, s.catalog, s.snapshot = nil, submissionReady, 0, descriptorCatalogCandidate{}, nil
	s.ticket.Store(0)
	s.state.Store(submissionIdle)
	return nil
}

func (s *Submission) invalidatePrepare() {
	clear(s.readySeries[:])
	s.Ready, s.seriesGroup, s.seriesCount = NodeReady{}, 0, 0
	s.err, s.kind, s.count, s.catalog, s.snapshot = nil, 0, 0, descriptorCatalogCandidate{}, nil
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
	clear(s.readySeries[:])
	s.Ready, s.seriesGroup, s.seriesCount = NodeReady{}, 0, 0
	s.err, s.kind, s.count, s.catalog, s.snapshot = nil, kind, uint8(count), descriptorCatalogCandidate{}, nil
	s.ticket.Store(0)
	s.state.Store(submissionIdle)
	return nil
}

// PrepareReadySeries copies a same-group series' fixed PersistBatch
// descriptors into this caller-owned submission. The pointed-to Entries,
// HardState and Snapshot values remain borrowed until completion. Unsupported
// multi-Ready snapshots are rejected before a ticket can be published; callers
// can submit those values as ordinary singleton Readies.
func (s *Submission) PrepareReadySeries(group uint64, batches []raftmodel.PersistBatch) error {
	if s == nil || s.done == nil {
		return ErrInvalid
	}
	if state := s.state.Load(); state == submissionQueued || state == submissionWaiting {
		return ErrSubmissionPending
	}
	if err := validateReadySeriesDescriptors(group, batches); err != nil {
		s.invalidatePrepare()
		return err
	}
	clear(s.readySeries[:])
	copy(s.readySeries[:], batches)
	// Keep the first logical Ready visible through the long-standing Ready
	// field for callers which inspect a completed Submission; the fixed series
	// storage is the authoritative value consumed by the worker.
	s.Ready, s.err, s.kind, s.count = NodeReady{GroupID: group, Batch: batches[0]}, nil, submissionReady, 0
	s.seriesGroup, s.seriesCount = group, uint8(len(batches))
	s.catalog, s.snapshot = descriptorCatalogCandidate{}, nil
	s.ticket.Store(0)
	s.state.Store(submissionIdle)
	return nil
}

// ReadySeriesLen reports the number of logical Readies represented by this
// submission. Ordinary Prepare submissions report one.
func (s *Submission) ReadySeriesLen() int {
	if s == nil || s.kind != submissionReady {
		return 0
	}
	if s.seriesCount == 0 {
		return 1
	}
	return int(s.seriesCount)
}

// nodeReady returns the immutable value published to the sequencer ring. A
// series copies its fixed descriptor array into the NodeReady envelope; the
// pointed-to protobuf values remain borrowed under the Submission ownership
// contract. It is called before publication by producers and after dequeue by
// the single worker, never while a caller may prepare the cell.
func (s *Submission) nodeReady() NodeReady {
	if s == nil || s.seriesCount == 0 {
		if s == nil {
			return NodeReady{}
		}
		return s.Ready
	}
	return NodeReady{GroupID: s.seriesGroup, series: s.readySeries, seriesCount: s.seriesCount}
}

func (s *Submission) nodeReadyGroup() uint64 {
	if s == nil {
		return 0
	}
	if s.seriesCount != 0 {
		return s.seriesGroup
	}
	return s.Ready.GroupID
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

// PrepareRegisterGroupWithSnapshot borrows an immutable bootstrap snapshot
// until completion. This cold control operation fences the descriptor, initial
// term/commit and incarnation together; it must not be used for an existing
// group's normal checkpoint maintenance.
func (s *Submission) PrepareRegisterGroupWithSnapshot(descriptor GroupDescriptor, snapshot *pb.Snapshot) error {
	if err := s.PrepareRegisterGroup(descriptor); err != nil {
		return err
	}
	if err := validateSnapshotBase(snapshot, descriptor.MemberID); err != nil {
		s.invalidatePrepare()
		return err
	}
	s.snapshot = snapshot
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

// PrepareCheckpoint borrows one immutable application snapshot until the
// submission completes. The checkpoint is ordered with Ready persistence and
// forms a durability-wave boundary, so its logical prefix truncation cannot
// overtake an earlier append or be overtaken by a later one.
func (s *Submission) PrepareCheckpoint(group uint64, snapshot *pb.Snapshot) error {
	if err := s.prepareControl(submissionCheckpoint, 1); err != nil {
		return err
	}
	if group == 0 || snapshot == nil || snapshot.GetMetadata() == nil || snapshot.GetMetadata().GetConfState() == nil ||
		snapshot.GetMetadata().GetIndex() == 0 || snapshot.GetMetadata().GetTerm() == 0 {
		s.invalidatePrepare()
		return ErrInvalid
	}
	s.groups[0], s.snapshot = group, snapshot
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

// NodeSubmissionSequencerStats is a detached, fixed-size diagnostics snapshot
// for one node submission lane. Counters are read from atomics, while queue,
// ownership and closed fields are current atomic or immutable gauges, so
// callers may collect the snapshot while producers and the worker are active.
// The ReadyWaveGroupHistogram index is the number of groups actually appended
// in an observed Ready frame, excluding idempotent retries; index zero is
// unused. Snapshot fields are sampled independently, so counter relationships
// need only hold after the relevant work has completed. QueueDepth is a bounded
// estimate of reserved tickets that have not been dequeued, excluding service.
//
// ReadyWavesSucceeded counts accepted Ready waves whose persistence call
// returned nil, including an exact idempotent retry which did not append.
// ReadyPersistAttempts/Successes/Failures count individual persistence calls,
// including backpressure retries and panics. Wave counters count the complete
// operation across those attempts. Wave duration includes retry waits; persist
// duration excludes waits between calls. Queue wait excludes wave service.
// ReadyDurableWaves and ReadyWaveGroupHistogram count only waves for which the
// underlying engine sequence advanced. ObservedAppendBarriers therefore
// reports successful append barriers rather than treating coalesced
// submissions as syncs. It excludes rotation and sealer metadata syncs, which
// do not advance the append sequence. A failed call may still contribute to
// these append counters if the engine advanced before the caller observed the
// error. Conversely, a poisoned engine suppresses its witness, so some
// post-sync failures contribute no append count. These are lower-bound
// observations, not total device sync counts or unknown-outcome resolution.
type NodeSubmissionSequencerStats struct {
	SubmissionAttempts      uint64
	AcceptedSubmissions     uint64
	RejectedSubmissions     uint64
	BackpressureSubmissions uint64
	ReadySubmissions        uint64
	ControlSubmissions      uint64
	ReadyQueueWaitNanos     uint64
	ControlQueueWaitNanos   uint64
	ReadyWavesAttempted     uint64
	ReadyPersistAttempts    uint64
	ReadyWavesSucceeded     uint64
	ReadyWavesFailed        uint64
	ReadyPersistSuccesses   uint64
	ReadyPersistFailures    uint64
	ReadyDurableWaves       uint64
	// ReadyLogicalBatches counts constituent logical Readies accepted by the
	// lane, while ReadySeriesSubmissions counts their physical submission
	// envelopes. A singleton ordinary Ready is one series of length one.
	ReadyLogicalBatches             uint64
	ReadySeriesSubmissions          uint64
	ReadySingletonSeriesSubmissions uint64
	ReadyMultiSeriesSubmissions     uint64
	ReadySeriesHistogram            [MaxReadySeries + 1]uint64
	// ReadyDurableLogicalBatches advances only when the engine append witness
	// proves that the corresponding physical wave reached the log.
	ReadyDurableLogicalBatches    uint64
	ReadyDurableSeriesSubmissions uint64
	ReadyDurableSeriesHistogram   [MaxReadySeries + 1]uint64
	FailedWaves                   uint64
	ControlWavesAttempted         uint64
	ControlPersistAttempts        uint64
	ControlWavesSucceeded         uint64
	ControlWavesFailed            uint64
	ControlPersistSuccesses       uint64
	ControlPersistFailures        uint64
	ObservedAppendBarriers        uint64
	ReadyObservedAppendBarriers   uint64
	ControlObservedAppendBarriers uint64
	ReadyPersistDurationNanos     uint64
	ControlPersistDurationNanos   uint64
	ReadyWaveDurationNanos        uint64
	ControlWaveDurationNanos      uint64
	ReadyWaveGroupHistogram       [MaxPersistGroupBatches + 1]uint64
	MultiGroupWaves               uint64
	QueueDepth                    uint64
	QueueCapacity                 uint64
	ActiveSubmitters              int64
	MaintenanceLaneClaimed        bool
	Closed                        bool
	CheckpointQueueSubmissions    uint64
	CheckpointQueueRejected       uint64
	CheckpointQueueWaitNanos      uint64
	CheckpointServiceNanos        uint64
}

// nodeSequencerCounters is kept separate from the public snapshot so no
// atomic values are copied. It also keeps the hot producer and worker paths
// allocation-free while Stats loads a detached value.
type nodeSequencerCounters struct {
	submissionAttempts              atomic.Uint64
	acceptedSubmissions             atomic.Uint64
	rejectedSubmissions             atomic.Uint64
	backpressureSubmissions         atomic.Uint64
	readySubmissions                atomic.Uint64
	controlSubmissions              atomic.Uint64
	readyQueueWaitNanos             atomic.Uint64
	controlQueueWaitNanos           atomic.Uint64
	readyWavesAttempted             atomic.Uint64
	readyPersistAttempts            atomic.Uint64
	readyWavesSucceeded             atomic.Uint64
	readyWavesFailed                atomic.Uint64
	readyPersistSuccesses           atomic.Uint64
	readyPersistFailures            atomic.Uint64
	readyDurableWaves               atomic.Uint64
	readyLogicalBatches             atomic.Uint64
	readySeriesSubmissions          atomic.Uint64
	readySingletonSeriesSubmissions atomic.Uint64
	readyMultiSeriesSubmissions     atomic.Uint64
	readySeriesHistogram            [MaxReadySeries + 1]atomic.Uint64
	readyDurableLogicalBatches      atomic.Uint64
	readyDurableSeriesSubmissions   atomic.Uint64
	readyDurableSeriesHistogram     [MaxReadySeries + 1]atomic.Uint64
	failedWaves                     atomic.Uint64
	controlWavesAttempted           atomic.Uint64
	controlPersistAttempts          atomic.Uint64
	controlWavesSucceeded           atomic.Uint64
	controlWavesFailed              atomic.Uint64
	controlPersistSuccesses         atomic.Uint64
	controlPersistFailures          atomic.Uint64
	observedAppendBarriers          atomic.Uint64
	readyObservedAppendBarriers     atomic.Uint64
	controlObservedAppendBarriers   atomic.Uint64
	readyPersistDurationNanos       atomic.Uint64
	controlPersistDurationNanos     atomic.Uint64
	readyWaveDurationNanos          atomic.Uint64
	controlWaveDurationNanos        atomic.Uint64
	readyWaveGroupHistogram         [MaxPersistGroupBatches + 1]atomic.Uint64
	multiGroupWaves                 atomic.Uint64
	checkpointQueueSubmissions      atomic.Uint64
	checkpointQueueRejected         atomic.Uint64
	checkpointQueueWaitNanos        atomic.Uint64
	checkpointServiceNanos          atomic.Uint64
}

// Stats returns a detached diagnostics snapshot without acquiring the node
// store or sequencer locks. It is safe to call concurrently with submission,
// persistence, checkpoint observation, and Close.
func (q *NodeSubmissionSequencer) Stats() NodeSubmissionSequencerStats {
	if q == nil {
		return NodeSubmissionSequencerStats{}
	}
	c := &q.stats
	var result NodeSubmissionSequencerStats
	result.SubmissionAttempts = c.submissionAttempts.Load()
	result.AcceptedSubmissions = c.acceptedSubmissions.Load()
	result.RejectedSubmissions = c.rejectedSubmissions.Load()
	result.BackpressureSubmissions = c.backpressureSubmissions.Load()
	result.ReadySubmissions = c.readySubmissions.Load()
	result.ControlSubmissions = c.controlSubmissions.Load()
	result.ReadyQueueWaitNanos = c.readyQueueWaitNanos.Load()
	result.ControlQueueWaitNanos = c.controlQueueWaitNanos.Load()
	result.ReadyWavesAttempted = c.readyWavesAttempted.Load()
	result.ReadyPersistAttempts = c.readyPersistAttempts.Load()
	result.ReadyWavesSucceeded = c.readyWavesSucceeded.Load()
	result.ReadyWavesFailed = c.readyWavesFailed.Load()
	result.ReadyPersistSuccesses = c.readyPersistSuccesses.Load()
	result.ReadyPersistFailures = c.readyPersistFailures.Load()
	result.ReadyDurableWaves = c.readyDurableWaves.Load()
	result.ReadyLogicalBatches = c.readyLogicalBatches.Load()
	result.ReadySeriesSubmissions = c.readySeriesSubmissions.Load()
	result.ReadySingletonSeriesSubmissions = c.readySingletonSeriesSubmissions.Load()
	result.ReadyMultiSeriesSubmissions = c.readyMultiSeriesSubmissions.Load()
	for i := range result.ReadySeriesHistogram {
		result.ReadySeriesHistogram[i] = c.readySeriesHistogram[i].Load()
		result.ReadyDurableSeriesHistogram[i] = c.readyDurableSeriesHistogram[i].Load()
	}
	result.ReadyDurableLogicalBatches = c.readyDurableLogicalBatches.Load()
	result.ReadyDurableSeriesSubmissions = c.readyDurableSeriesSubmissions.Load()
	result.FailedWaves = c.failedWaves.Load()
	result.ControlWavesAttempted = c.controlWavesAttempted.Load()
	result.ControlPersistAttempts = c.controlPersistAttempts.Load()
	result.ControlWavesSucceeded = c.controlWavesSucceeded.Load()
	result.ControlWavesFailed = c.controlWavesFailed.Load()
	result.ControlPersistSuccesses = c.controlPersistSuccesses.Load()
	result.ControlPersistFailures = c.controlPersistFailures.Load()
	result.ObservedAppendBarriers = c.observedAppendBarriers.Load()
	result.ReadyObservedAppendBarriers = c.readyObservedAppendBarriers.Load()
	result.ControlObservedAppendBarriers = c.controlObservedAppendBarriers.Load()
	result.ReadyPersistDurationNanos = c.readyPersistDurationNanos.Load()
	result.ControlPersistDurationNanos = c.controlPersistDurationNanos.Load()
	result.ReadyWaveDurationNanos = c.readyWaveDurationNanos.Load()
	result.ControlWaveDurationNanos = c.controlWaveDurationNanos.Load()
	for i := range result.ReadyWaveGroupHistogram {
		result.ReadyWaveGroupHistogram[i] = c.readyWaveGroupHistogram[i].Load()
	}
	result.MultiGroupWaves = c.multiGroupWaves.Load()
	result.QueueCapacity = uint64(len(q.ring))
	// Read head first: tail-first sampling can subtract a newer head from an
	// older tail and underflow. Concurrent progress can still overestimate
	// depth, so bound the estimate by the fixed ring capacity.
	head := q.head.value.Load()
	result.QueueDepth = min(q.tail.value.Load()-head, result.QueueCapacity)
	result.ActiveSubmitters = q.submitters.Load()
	result.MaintenanceLaneClaimed = q.maintenanceOwner.Load()
	result.Closed = q.closed.Load()
	result.CheckpointQueueSubmissions = c.checkpointQueueSubmissions.Load()
	result.CheckpointQueueRejected = c.checkpointQueueRejected.Load()
	result.CheckpointQueueWaitNanos = c.checkpointQueueWaitNanos.Load()
	result.CheckpointServiceNanos = c.checkpointServiceNanos.Load()
	return result
}

// ObserveCheckpointQueueSubmission records one accepted checkpoint capture
// task. It is called by the node-wide checkpoint coordinator after its bounded
// channel accepts the task.
func (q *NodeSubmissionSequencer) ObserveCheckpointQueueSubmission() {
	if q != nil {
		q.stats.checkpointQueueSubmissions.Add(1)
	}
}

// ObserveCheckpointQueueRejected records a checkpoint capture task rejected
// by the bounded coordinator queue.
func (q *NodeSubmissionSequencer) ObserveCheckpointQueueRejected() {
	if q != nil {
		q.stats.checkpointQueueRejected.Add(1)
	}
}

// ObserveCheckpointQueueWait records time spent waiting in the checkpoint
// coordinator queue. Nonpositive durations are ignored so a wall-clock step
// cannot turn diagnostics into a huge unsigned value.
func (q *NodeSubmissionSequencer) ObserveCheckpointQueueWait(wait time.Duration) {
	if q != nil && wait > 0 {
		q.stats.checkpointQueueWaitNanos.Add(uint64(wait))
	}
}

// ObserveCheckpointService records time spent capturing one application
// checkpoint after it leaves the bounded queue.
func (q *NodeSubmissionSequencer) ObserveCheckpointService(duration time.Duration) {
	if q != nil && duration > 0 {
		q.stats.checkpointServiceNanos.Add(uint64(duration))
	}
}

type NodeSubmissionSequencer struct {
	store *NodeStore
	ring  []submissionRingSlot
	mask  uint64

	head submissionRingIndex
	tail submissionRingIndex

	wake             chan struct{}
	drained          chan struct{}
	done             chan struct{}
	closed           atomic.Bool
	submitters       atomic.Int64
	stopOnce         sync.Once
	drainOnce        sync.Once
	fatal            atomic.Pointer[sequencerFailure]
	wakeMu           sync.Mutex
	ownerWakes       atomic.Pointer[nodeSequencerWakeSet]
	maintenanceOwner atomic.Bool
	capacityWaiters  atomic.Bool
	stats            nodeSequencerCounters

	persist func([]NodeReady) error

	submitHookTest   func()
	claimedHookTest  func()
	completeHookTest func(*Submission, uint32)
}

type sequencerFailure struct{ err error }
type nodeSequencerWake struct {
	owner *Submission
	fn    func()
}
type nodeSequencerWakeSet struct{ entries []nodeSequencerWake }

// nodeWaveAdmission is a conservative, allocation-free upper bound for the
// fixed arenas touched by one Ready after it joins a node durability wave.
// frameBytes deliberately charges the wave header once per Ready. That small
// overcharge makes costs additive, so the consumer can stop before the next
// ticket without encoding, locking the store, or later rejecting an already
// accepted Ready merely because unrelated groups happened to arrive together.
type nodeWaveAdmission struct {
	frameBytes int
	events     int
}

func addAdmissionInt(value *int, delta int) bool {
	if delta < 0 || *value > int(^uint(0)>>1)-delta {
		return false
	}
	*value += delta
	return true
}

func (s *NodeStore) readyWaveAdmission(ready NodeReady) (nodeWaveAdmission, error) {
	// A zero-value store is used by scheduler-only tests and injected transports.
	// Real node stores always authenticate nonzero bounds in NODEMETA.
	if s == nil || s.bounds.maxWaveBytes == 0 {
		return nodeWaveAdmission{}, nil
	}
	count := nodeReadySeriesCount(ready)
	if ready.GroupID == 0 || count < 1 || count > MaxReadySeries {
		return nodeWaveAdmission{}, ErrBounds
	}
	var batches [MaxReadySeries]raftmodel.PersistBatch
	if ready.seriesCount == 0 {
		batches[0] = ready.Batch
	} else {
		if err := validateReadySeriesDescriptors(ready.GroupID, ready.series[:count]); err != nil {
			return nodeWaveAdmission{}, err
		}
		copy(batches[:count], ready.series[:count])
	}
	plainBytes, nonemptyEntries, totalEntries, readyBytes := 0, 0, 0, 0
	for index := 0; index < count; index++ {
		batch := batches[index]
		if batch.NodeIncarnation == 0 || batch.ReadyID == 0 {
			return nodeWaveAdmission{}, ErrInvalid
		}
		if uint64(len(batch.Entries)) > s.bounds.maxEntriesPerGroup ||
			totalEntries > int(s.bounds.maxEntriesPerGroup)-len(batch.Entries) {
			return nodeWaveAdmission{}, ErrBounds
		}
		totalEntries += len(batch.Entries)
		for _, entry := range batch.Entries {
			if entry == nil || !addAdmissionInt(&plainBytes, len(entry.GetData())) {
				return nodeWaveAdmission{}, ErrBounds
			}
			if len(entry.GetData()) != 0 {
				nonemptyEntries++
			}
		}
		payloadBytes, err := readyPayloadSize(batch)
		if err != nil || !addAdmissionInt(&readyBytes, payloadBytes) {
			if err != nil {
				return nodeWaveAdmission{}, err
			}
			return nodeWaveAdmission{}, ErrBounds
		}
		if !canonicalEmptySnapshot(batch.Snapshot) {
			snapshotBytes, snapshotErr := snapshotPayloadSize(batch.Snapshot)
			if snapshotErr != nil || !addAdmissionInt(&readyBytes, snapshotBytes) {
				if snapshotErr != nil {
					return nodeWaveAdmission{}, snapshotErr
				}
				return nodeWaveAdmission{}, ErrBounds
			}
		}
	}
	if uint64(readyBytes) > s.bounds.maxWaveBytes {
		return nodeWaveAdmission{}, ErrBounds
	}

	// 92 covers the fixed wave header and both count varints. 169 covers the
	// worst batch descriptor (identity, replacement, checkpoint, HardState and
	// counts). A series identity adds a versioned span and one ReadyID/digest
	// pair per constituent; charge a fixed 32 bytes per constituent so the
	// admission remains conservative without encoding or allocating. Each entry
	// then has at most eight uint64 varints in blob form. AEAD contributes one
	// 16-byte tag per nonempty entry in the worst packing.
	frameBytes := 92 + 169
	if count > (int(^uint(0)>>1)-frameBytes)/32 ||
		!addAdmissionInt(&frameBytes, count*32) ||
		totalEntries > (int(^uint(0)>>1)-frameBytes)/80 ||
		!addAdmissionInt(&frameBytes, totalEntries*80) ||
		!addAdmissionInt(&frameBytes, plainBytes) ||
		nonemptyEntries > (int(^uint(0)>>1)-frameBytes)/16 ||
		!addAdmissionInt(&frameBytes, nonemptyEntries*16) {
		return nodeWaveAdmission{}, ErrBounds
	}
	events := 6
	if !addAdmissionInt(&events, totalEntries) ||
		uint64(frameBytes) > s.bounds.maxWaveBytes ||
		uint64(events) > s.bounds.maxSegmentEvents {
		return nodeWaveAdmission{}, ErrBounds
	}
	return nodeWaveAdmission{frameBytes: frameBytes, events: events}, nil
}

func (s *NodeStore) waveAdmissionsFit(current, next nodeWaveAdmission) bool {
	if s == nil || s.bounds.maxWaveBytes == 0 {
		return true
	}
	return next.frameBytes <= int(s.bounds.maxWaveBytes)-current.frameBytes &&
		next.events <= int(s.bounds.maxSegmentEvents)-current.events
}

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

// ClaimMaintenanceLane reserves the single bounded background producer which
// may prepare checkpoint work for this device sequencer.
func (q *NodeSubmissionSequencer) ClaimMaintenanceLane() error {
	if q == nil || q.closed.Load() {
		return ErrClosed
	}
	if !q.maintenanceOwner.CompareAndSwap(false, true) {
		return ErrMaintenanceActive
	}
	return nil
}

// ReleaseMaintenanceLane releases a previously claimed producer after all of
// its accepted work has completed.
func (q *NodeSubmissionSequencer) ReleaseMaintenanceLane() {
	if q != nil {
		q.maintenanceOwner.Store(false)
	}
}

// SetWakeFor registers the execution owner of one caller-reserved submission
// cell. Registration is cold-path and publishes one immutable callback vector;
// ordinary completion loads only that cell's callback. The vector is retained
// for capacity-pressure and fatal-failure broadcasts.
// Passing nil removes owner without disturbing any other execution lane.
func (q *NodeSubmissionSequencer) SetWakeFor(owner *Submission, wake func()) {
	if q == nil || owner == nil {
		return
	}
	q.wakeMu.Lock()
	defer q.wakeMu.Unlock()
	if wake == nil {
		owner.wake.Store(nil)
	} else {
		owner.wake.Store(&nodeSequencerWake{owner: owner, fn: wake})
	}
	current := q.ownerWakes.Load()
	count := 0
	if current != nil {
		count = len(current.entries)
	}
	position := count
	if current != nil {
		for index := range current.entries {
			if current.entries[index].owner == owner {
				position = index
				break
			}
		}
	}
	if wake == nil && position == count {
		return
	}
	nextCount := count
	if wake == nil {
		nextCount--
	} else if position == count {
		nextCount++
	}
	if nextCount == 0 {
		q.ownerWakes.Store(nil)
		return
	}
	next := &nodeSequencerWakeSet{entries: make([]nodeSequencerWake, nextCount)}
	if wake == nil {
		copy(next.entries, current.entries[:position])
		copy(next.entries[position:], current.entries[position+1:])
	} else {
		if current != nil {
			copy(next.entries, current.entries)
		}
		next.entries[position] = nodeSequencerWake{owner: owner, fn: wake}
	}
	q.ownerWakes.Store(next)
}

func (q *NodeSubmissionSequencer) notifyOwner() {
	if wakes := q.ownerWakes.Load(); wakes != nil {
		for index := range wakes.entries {
			if wakes.entries[index].fn != nil {
				wakes.entries[index].fn()
			}
		}
	}
}

func (q *NodeSubmissionSequencer) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func sequencerDurationNanos(duration time.Duration) uint64 {
	if duration <= 0 {
		return 0
	}
	return uint64(duration)
}

// engineAppendWitness is intentionally sampled only by the single sequencer
// worker. The engine samples sequence and actual appended group count under
// its write mutex, including synchronization with background sealer failures.
// Injected persistence functions which do not call the engine provide no
// append witness.
func (q *NodeSubmissionSequencer) engineAppendWitness() (sequence uint64, groups int) {
	if q == nil || q.store == nil || q.store.engine == nil {
		return 0, 0
	}
	return q.store.engine.AppendWitness()
}

func (q *NodeSubmissionSequencer) observeEngineSequence(before, after uint64, ready bool) uint64 {
	if after < before {
		return 0
	}
	delta := after - before
	if delta == 0 {
		return 0
	}
	q.stats.observedAppendBarriers.Add(delta)
	if ready {
		q.stats.readyObservedAppendBarriers.Add(delta)
	} else {
		q.stats.controlObservedAppendBarriers.Add(delta)
	}
	return delta
}

func (q *NodeSubmissionSequencer) observeReadyPersist(ready []NodeReady) (err error) {
	q.stats.readyPersistAttempts.Add(1)
	started := time.Now()
	// A panic bypasses the named return assignment. Count that attempt as a
	// failure while leaving the existing runWave recovery policy unchanged.
	err = ErrSubmissionPanic
	defer func() {
		q.stats.readyPersistDurationNanos.Add(sequencerDurationNanos(time.Since(started)))
		if err == nil {
			q.stats.readyPersistSuccesses.Add(1)
		} else {
			q.stats.readyPersistFailures.Add(1)
		}
	}()
	return q.persist(ready)
}

func (q *NodeSubmissionSequencer) observeControlPersist(s *Submission) (err error) {
	q.stats.controlPersistAttempts.Add(1)
	started := time.Now()
	err = ErrSubmissionPanic
	defer func() {
		q.stats.controlPersistDurationNanos.Add(sequencerDurationNanos(time.Since(started)))
		if err == nil {
			q.stats.controlPersistSuccesses.Add(1)
		} else {
			q.stats.controlPersistFailures.Add(1)
		}
	}()
	switch s.kind {
	case submissionBeginIncarnations:
		return q.store.beginIncarnationsSequenced(s.groups[:s.count], s.incarnations[:s.count])
	case submissionPersistIncarnations:
		return q.store.persistIncarnationsSequenced(s.incarnations[:s.count])
	case submissionRegisterGroup:
		incarnation, registerErr := q.store.registerGroupSequenced(s.descriptor, s.snapshot)
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
	case submissionCheckpoint:
		return q.store.publishGroupCheckpointSequenced(s.groups[0], s.snapshot)
	default:
		return ErrInvalid
	}
}

func (q *NodeSubmissionSequencer) rejectSubmission(err error) (uint64, error) {
	q.stats.rejectedSubmissions.Add(1)
	if errors.Is(err, ErrSubmissionBackpressure) {
		q.stats.backpressureSubmissions.Add(1)
		// A refused cell has no completion of its own. Preserve the capacity
		// retry edge for every registered owner, including a rejection racing
		// with the worker's last completion or an idle ring.
		q.capacityWaiters.Store(true)
		q.signal()
	}
	return 0, err
}

func (q *NodeSubmissionSequencer) TrySubmit(submission *Submission) (uint64, error) {
	if q == nil {
		return 0, ErrInvalid
	}
	q.stats.submissionAttempts.Add(1)
	if submission == nil || submission.done == nil {
		return q.rejectSubmission(ErrInvalid)
	}
	q.submitters.Add(1)
	defer func() {
		if q.submitters.Add(-1) == 0 && q.closed.Load() {
			q.drainOnce.Do(func() { close(q.drained) })
		}
	}()
	if fatal := q.fatal.Load(); fatal != nil {
		return q.rejectSubmission(errors.Join(ErrPersistenceUnknown, fatal.err))
	}
	if q.closed.Load() {
		return q.rejectSubmission(ErrClosed)
	}
	state := submission.state.Load()
	if state != submissionIdle {
		if state == submissionQueued || state == submissionWaiting {
			return q.rejectSubmission(ErrSubmissionPending)
		}
		return q.rejectSubmission(ErrInvalid)
	}
	if submission.kind == submissionReady {
		if _, err := q.store.readyWaveAdmission(submission.nodeReady()); err != nil {
			return q.rejectSubmission(err)
		}
	}
	if !submission.state.CompareAndSwap(submissionIdle, submissionQueued) {
		return q.rejectSubmission(ErrSubmissionPending)
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
			return q.rejectSubmission(ErrSubmissionBackpressure)
		}
		if q.tail.value.CompareAndSwap(position, position+1) {
			ticket := position + 1
			slot := &q.ring[position&q.mask]
			if q.claimedHookTest != nil {
				q.claimedHookTest()
			}
			slot.value = submission
			submission.queuedAt = time.Now()
			submission.ticket.Store(ticket)
			q.stats.acceptedSubmissions.Add(1)
			if submission.kind == submissionReady {
				q.stats.readySubmissions.Add(1)
				seriesLength := submission.ReadySeriesLen()
				q.stats.readySeriesSubmissions.Add(1)
				q.stats.readyLogicalBatches.Add(uint64(seriesLength))
				if seriesLength == 1 {
					q.stats.readySingletonSeriesSubmissions.Add(1)
				} else if seriesLength > 1 {
					q.stats.readyMultiSeriesSubmissions.Add(1)
				}
				if seriesLength >= 1 && seriesLength <= MaxReadySeries {
					q.stats.readySeriesHistogram[seriesLength].Add(1)
				}
			} else {
				q.stats.controlSubmissions.Add(1)
			}
			// Publication lets the consumer complete this cell and its caller
			// immediately prepare it again. Never read the cell after this edge.
			slot.sequence.Store(ticket)
			q.signal()
			return ticket, nil
		}
	}
	submission.state.Store(submissionIdle)
	return q.rejectSubmission(ErrSubmissionBackpressure)
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
		if items[i].nodeReadyGroup() == group {
			return true
		}
	}
	return false
}

func (q *NodeSubmissionSequencer) complete(s *Submission, err error) {
	wake := s.wake.Load()
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
	// Capture the callback before publishing completion: the owner can reuse
	// or unregister this cell as soon as Complete becomes visible.
	if wake != nil && wake.fn != nil {
		wake.fn()
	}
}

func (q *NodeSubmissionSequencer) runWave(items *[MaxPersistGroupBatches]*Submission, ready *[MaxPersistGroupBatches]NodeReady, count int) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrSubmissionPanic
		}
	}()
	if count == 1 && items[0].kind != submissionReady {
		return q.observeControlPersist(items[0])
	}
	for i := 0; i < count; i++ {
		value := items[i].nodeReady()
		at := i
		for at > 0 && ready[at-1].GroupID > value.GroupID {
			ready[at] = ready[at-1]
			at--
		}
		ready[at] = value
	}
	for {
		err = q.observeReadyPersist(ready[:count])
		if !errors.Is(err, ErrDurabilityBackpressure) {
			break
		}
		if waitErr := q.store.engine.WaitSeal(); waitErr != nil {
			err = errors.Join(ErrPersistenceUnknown, err, waitErr)
			break
		}
		// Backpressure without a pending seal means the background metadata
		// lane is replenishing reserves or checkpointing the catalog. Yield the
		// device sequencer so that maintenance can publish its fixed-size slot.
		time.Sleep(50 * time.Microsecond)
	}
	clear(ready[:count])
	return err
}

func (q *NodeSubmissionSequencer) observeSubmissionQueueWait(items *[MaxPersistGroupBatches]*Submission, count int) {
	now := time.Now()
	for i := 0; i < count; i++ {
		queuedAt := items[i].queuedAt
		if queuedAt.IsZero() {
			continue
		}
		wait := now.Sub(queuedAt)
		if wait <= 0 {
			continue
		}
		if items[i].kind == submissionReady {
			q.stats.readyQueueWaitNanos.Add(uint64(wait))
		} else {
			q.stats.controlQueueWaitNanos.Add(uint64(wait))
		}
	}
}

func (q *NodeSubmissionSequencer) observeWaveResult(items *[MaxPersistGroupBatches]*Submission, count int, err error, engineDelta uint64, appendedGroups int) {
	if count == 0 {
		return
	}
	ready := items[0].kind == submissionReady
	if ready {
		if err == nil {
			q.stats.readyWavesSucceeded.Add(1)
		} else {
			q.stats.readyWavesFailed.Add(1)
			q.stats.failedWaves.Add(1)
		}
		if engineDelta != 0 {
			q.stats.readyDurableWaves.Add(engineDelta)
			// One runWave call submits one physical frame. Attribute every logical
			// Ready only when its appended group count proves that no outer item was
			// an earlier exact retry. Mixed retry/new waves remain a lower bound
			// because the scalar witness does not identify the appended items.
			if engineDelta == 1 && appendedGroups == count {
				logical := uint64(0)
				for index := 0; index < count; index++ {
					seriesLength := items[index].ReadySeriesLen()
					if seriesLength < 1 || seriesLength > MaxReadySeries {
						continue
					}
					logical += uint64(seriesLength)
					q.stats.readyDurableSeriesSubmissions.Add(1)
					q.stats.readyDurableSeriesHistogram[seriesLength].Add(1)
				}
				q.stats.readyDurableLogicalBatches.Add(logical)
			}
			// A production Ready wave appends at most one frame. If a test
			// persistence adapter appends more, the final witness cannot tell
			// us the earlier frames' group counts, so do not invent them.
			if engineDelta == 1 && appendedGroups > 0 && appendedGroups <= MaxPersistGroupBatches {
				q.stats.readyWaveGroupHistogram[appendedGroups].Add(1)
				if appendedGroups > 1 {
					q.stats.multiGroupWaves.Add(1)
				}
			}
		}
		return
	}
	if err == nil {
		q.stats.controlWavesSucceeded.Add(1)
	} else {
		q.stats.controlWavesFailed.Add(1)
		q.stats.failedWaves.Add(1)
	}
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
		yielded := false
		var admission nodeWaveAdmission
		for count < len(items) {
			s, ok := q.peek()
			if !ok && count == 1 && items[0].kind == submissionReady && !yielded {
				// A just-completed wave wakes multiple independent owners. Give their
				// already-runnable work one scheduler turn before freezing a singleton
				// wave; never wait on a timer, missing group, or new arrival.
				yielded = true
				runtime.Gosched()
				continue
			}
			if !ok || count > 0 && (items[0].kind != submissionReady || s.kind != submissionReady) ||
				s.kind == submissionReady && submissionGroupSeen(&items, count, s.nodeReadyGroup()) {
				break
			}
			if s.kind == submissionReady {
				next, admissionErr := q.store.readyWaveAdmission(s.nodeReady())
				if admissionErr != nil {
					// TrySubmit performs the same immutable geometry check before
					// publishing a ticket. Reaching this branch means caller-owned
					// Ready memory changed after acceptance: fail this wave rather
					// than letting it contaminate a later durability decision.
					items[count] = q.pop()
					count++
					break
				}
				if count > 0 && !q.store.waveAdmissionsFit(admission, next) {
					break
				}
				admission.frameBytes += next.frameBytes
				admission.events += next.events
			}
			items[count] = q.pop()
			count++
		}
		if count == 0 {
			if q.capacityWaiters.Swap(false) {
				q.notifyOwner()
			}
			if q.closed.Load() && q.head.value.Load() == q.tail.value.Load() {
				return
			}
			<-q.wake
			continue
		}
		q.observeSubmissionQueueWait(&items, count)
		readyWave := items[0].kind == submissionReady
		if readyWave {
			q.stats.readyWavesAttempted.Add(1)
		} else {
			q.stats.controlWavesAttempted.Add(1)
		}
		started := time.Now()
		engineBefore, _ := q.engineAppendWitness()
		err := q.runWave(&items, &ready, count)
		engineAfter, appendedGroups := q.engineAppendWitness()
		engineDelta := q.observeEngineSequence(engineBefore, engineAfter, readyWave)
		waveDuration := sequencerDurationNanos(time.Since(started))
		if readyWave {
			q.stats.readyWaveDurationNanos.Add(waveDuration)
		} else {
			q.stats.controlWaveDurationNanos.Add(waveDuration)
		}
		q.observeWaveResult(&items, count, err, engineDelta, appendedGroups)
		fatal := errors.Is(err, ErrPersistenceUnknown) || errors.Is(err, ErrSubmissionPanic)
		completionErr := err
		if fatal {
			completionErr = errors.Join(ErrPersistenceUnknown, err)
		}
		for i := 0; i < count; i++ {
			q.complete(items[i], completionErr)
			items[i] = nil
		}
		if q.capacityWaiters.Swap(false) {
			q.notifyOwner()
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
			q.notifyOwner()
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
