package raftmember

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime/trace"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore"
	pb "go.etcd.io/raft/v3/raftpb"
)

const (
	pipelinedReadyAdmissionWindow = 8
	pipelinedMessageCapacity      = 512
	pipelinedAppendCapacity       = 128
	pipelinedResultCapacity       = 8
)

// pipelinedMessageQueue is an owner-thread-only fixed ring. Pipelined Ready
// admission proves capacity before RawNode transfers ownership, so overflow is
// always an invariant failure rather than a reason to grow on the hot path.
type pipelinedMessageQueue struct {
	items [pipelinedMessageCapacity]*pb.Message
	head  uint16
	count uint16
}

func (q *pipelinedMessageQueue) push(message *pb.Message) bool {
	if message == nil || int(q.count) == len(q.items) {
		return false
	}
	position := (int(q.head) + int(q.count)) % len(q.items)
	q.items[position] = message
	q.count++
	return true
}

func (q *pipelinedMessageQueue) front() (*pb.Message, bool) {
	if q.count == 0 {
		return nil, false
	}
	return q.items[q.head], true
}

func (q *pipelinedMessageQueue) pop() *pb.Message {
	message, ok := q.front()
	if !ok {
		return nil
	}
	q.items[q.head] = nil
	q.head = uint16((int(q.head) + 1) % len(q.items))
	q.count--
	return message
}

func (q *pipelinedMessageQueue) len() int       { return int(q.count) }
func (q *pipelinedMessageQueue) remaining() int { return len(q.items) - int(q.count) }

type pipelinedAppendWork struct {
	message        *pb.Message
	batch          raftmodel.PersistBatch
	earlyResponses uint16
}

type pipelinedAppendCompletion struct {
	message       *pb.Message
	responseIndex uint16
}

type pipelinedAppendCompletionQueue struct {
	items [pipelinedAppendCapacity]pipelinedAppendCompletion
	head  uint16
	count uint16
}

func (q *pipelinedAppendCompletionQueue) push(completion pipelinedAppendCompletion) bool {
	if completion.message == nil || int(q.count) == len(q.items) {
		return false
	}
	position := (int(q.head) + int(q.count)) % len(q.items)
	q.items[position] = completion
	q.count++
	return true
}

func (q *pipelinedAppendCompletionQueue) front() (*pipelinedAppendCompletion, bool) {
	if q.count == 0 {
		return nil, false
	}
	return &q.items[q.head], true
}

func (q *pipelinedAppendCompletionQueue) pop() {
	if q.count == 0 {
		return
	}
	q.items[q.head] = pipelinedAppendCompletion{}
	q.head = uint16((int(q.head) + 1) % len(q.items))
	q.count--
}

func (q *pipelinedAppendCompletionQueue) len() int { return int(q.count) }

type pipelinedApplyTask struct {
	message               *pb.Message
	requiredAppendReadyID uint64
}

type pipelinedApplyQueue struct {
	items [pipelinedAppendCapacity]pipelinedApplyTask
	head  uint16
	count uint16
}

func (q *pipelinedApplyQueue) push(task pipelinedApplyTask) bool {
	if task.message == nil || int(q.count) == len(q.items) {
		return false
	}
	position := (int(q.head) + int(q.count)) % len(q.items)
	q.items[position] = task
	q.count++
	return true
}

func (q *pipelinedApplyQueue) front() (pipelinedApplyTask, bool) {
	if q.count == 0 {
		return pipelinedApplyTask{}, false
	}
	return q.items[q.head], true
}

func (q *pipelinedApplyQueue) pop() pipelinedApplyTask {
	task, ok := q.front()
	if !ok {
		return pipelinedApplyTask{}
	}
	q.items[q.head] = pipelinedApplyTask{}
	q.head = uint16((int(q.head) + 1) % len(q.items))
	q.count--
	return task
}

func (q *pipelinedApplyQueue) len() int       { return int(q.count) }
func (q *pipelinedApplyQueue) remaining() int { return len(q.items) - int(q.count) }

// Separate cache lines prevent the producer's tail publication from sharing a
// line with the consumer's head publication under sustained WAL traffic.
type pipelinedRingIndex struct {
	value atomic.Uint64
	_     [56]byte
}

// pipelinedAppendRing is the single-owner-to-single-worker scheduling lane.
// Publication of tail happens after the slot write; publication of head
// happens after the slot has been copied and cleared.
type pipelinedAppendRing struct {
	head  pipelinedRingIndex
	tail  pipelinedRingIndex
	items [pipelinedAppendCapacity]pipelinedAppendWork
}

func (q *pipelinedAppendRing) push(work pipelinedAppendWork) bool {
	tail := q.tail.value.Load()
	if tail-q.head.value.Load() >= uint64(len(q.items)) {
		return false
	}
	q.items[tail%uint64(len(q.items))] = work
	q.tail.value.Store(tail + 1)
	return true
}

func (q *pipelinedAppendRing) front() (pipelinedAppendWork, bool) {
	head := q.head.value.Load()
	if head == q.tail.value.Load() {
		return pipelinedAppendWork{}, false
	}
	return q.items[head%uint64(len(q.items))], true
}

// copyPrefix copies the bounded contiguous prefix currently available in the
// ring without retiring any slot. Node-wide persistence must first accept the
// complete series; only then may popN release the copied entries. The caller
// owns dst and must pass a capacity no larger than MaxPersistGroupBatches.
func (q *pipelinedAppendRing) copyPrefix(
	dst *[raftstore.MaxPersistGroupBatches]pipelinedAppendWork,
) int {
	if q == nil || dst == nil {
		return 0
	}
	head := q.head.value.Load()
	available := q.tail.value.Load() - head
	if available == 0 {
		return 0
	}
	if available > uint64(len(dst)) {
		available = uint64(len(dst))
	}
	for index := uint64(0); index < available; index++ {
		dst[index] = q.items[(head+index)%uint64(len(q.items))]
	}
	return int(available)
}

// popN retires exactly count entries after the corresponding copied series
// has been accepted by the node submission lane. It deliberately performs no
// copy or allocation and clears each released slot before publishing head.
func (q *pipelinedAppendRing) popN(count int) bool {
	if q == nil || count <= 0 || count > len(q.items) {
		return false
	}
	head := q.head.value.Load()
	if uint64(count) > q.tail.value.Load()-head {
		return false
	}
	for index := 0; index < count; index++ {
		q.items[(head+uint64(index))%uint64(len(q.items))] = pipelinedAppendWork{}
	}
	q.head.value.Store(head + uint64(count))
	return true
}

func (q *pipelinedAppendRing) pop() (pipelinedAppendWork, bool) {
	head := q.head.value.Load()
	work, ok := q.front()
	if !ok {
		return pipelinedAppendWork{}, false
	}
	position := head % uint64(len(q.items))
	q.items[position] = pipelinedAppendWork{}
	q.head.value.Store(head + 1)
	return work, true
}

func (q *pipelinedAppendRing) len() int {
	return int(q.tail.value.Load() - q.head.value.Load())
}

type pipelinedAppendResult struct {
	works [raftstore.MaxPersistGroupBatches]pipelinedAppendWork
	count uint8
	err   error
}

// pipelinedResultRing is the inverse single-worker-to-single-owner lane.
type pipelinedResultRing struct {
	head  pipelinedRingIndex
	tail  pipelinedRingIndex
	items [pipelinedResultCapacity]pipelinedAppendResult
}

func (q *pipelinedResultRing) push(result pipelinedAppendResult) bool {
	tail := q.tail.value.Load()
	if tail-q.head.value.Load() >= uint64(len(q.items)) {
		return false
	}
	q.items[tail%uint64(len(q.items))] = result
	q.tail.value.Store(tail + 1)
	return true
}

func (q *pipelinedResultRing) pop() (pipelinedAppendResult, bool) {
	head := q.head.value.Load()
	if head == q.tail.value.Load() {
		return pipelinedAppendResult{}, false
	}
	position := head % uint64(len(q.items))
	result := q.items[position]
	q.items[position] = pipelinedAppendResult{}
	q.head.value.Store(head + 1)
	return result, true
}

func (q *pipelinedResultRing) len() int {
	return int(q.tail.value.Load() - q.head.value.Load())
}

type pipelinedWake struct {
	fn func()
}

type pipelinedRuntime struct {
	runtime *Runtime

	appendWork   pipelinedAppendRing
	appendDone   pipelinedResultRing
	workerWake   chan struct{}
	resultSpace  chan struct{}
	stop         chan struct{}
	done         chan struct{}
	stopOnce     sync.Once
	retryRequest atomic.Bool
	wake         atomic.Pointer[pipelinedWake]

	applyQueue         pipelinedApplyQueue
	directQueue        pipelinedMessageQueue
	appendCompleted    pipelinedAppendCompletionQueue
	appendOutstanding  int
	appendRetry        bool
	appendRetryReadyID uint64
	appendReadyID      uint64
	appendProcessedID  uint64
	applyCurrent       pipelinedApplyTask
	applyResponseIndex int
	applyFinished      bool
	applyReadyID       uint64
	admission          int
	appendTerm         uint64
	appendVote         uint64
	appendCommit       uint64
	durableTerm        uint64
	durableVote        uint64
	nodeSubmission     bool
	nodeWorks          [raftstore.MaxPersistGroupBatches]pipelinedAppendWork
	nodeWorkCount      uint8
}

func newPipelinedRuntime(runtime *Runtime) (*pipelinedRuntime, error) {
	if runtime == nil || runtime.stableStore() == nil {
		return nil, ErrRuntimeOwnership
	}
	hard, _, err := runtime.stableStore().InitialState()
	if err != nil {
		return nil, err
	}
	commit := max(hard.GetCommit(), runtime.node.PublishedApplied())
	p := &pipelinedRuntime{
		runtime:    runtime,
		appendTerm: hard.GetTerm(), appendVote: hard.GetVote(), appendCommit: commit,
		durableTerm: hard.GetTerm(), durableVote: hard.GetVote(),
	}
	if runtime.nodePersistence == nil {
		p.workerWake = make(chan struct{}, 1)
		p.resultSpace = make(chan struct{}, 1)
		p.stop = make(chan struct{})
		p.done = make(chan struct{})
		go p.runAppendWorker()
	}
	return p, nil
}

func signalPipelinedEdge(edge chan struct{}) {
	select {
	case edge <- struct{}{}:
	default:
	}
}

func (p *pipelinedRuntime) notifyOwner() {
	if wake := p.wake.Load(); wake != nil && wake.fn != nil {
		wake.fn()
	}
}

func (p *pipelinedRuntime) publishAppendResult(result pipelinedAppendResult) bool {
	for !p.appendDone.push(result) {
		select {
		case <-p.stop:
			return false
		case <-p.resultSpace:
		}
	}
	p.notifyOwner()
	return true
}

func (p *pipelinedRuntime) runAppendWorker() {
	defer close(p.done)
	var active pipelinedAppendResult
	var batches [raftstore.MaxPersistGroupBatches]raftmodel.PersistBatch
	for {
		if active.count == 0 {
			first, ok := p.appendWork.pop()
			if !ok {
				select {
				case <-p.stop:
					return
				case <-p.workerWake:
					continue
				}
			}
			active.works[0] = first
			active.count = 1
			for int(active.count) < len(active.works) {
				work, available := p.appendWork.pop()
				if !available {
					break
				}
				active.works[active.count] = work
				active.count++
			}
		} else {
			if !p.retryRequest.Swap(false) {
				select {
				case <-p.stop:
					return
				case <-p.workerWake:
					continue
				}
			}
		}

		for index := 0; index < int(active.count); index++ {
			batches[index] = active.works[index].batch
		}
		active.err = p.runtime.wal.PersistGroup(batches[:active.count])
		clear(batches[:active.count])
		if !p.publishAppendResult(active) {
			return
		}
		if active.err == nil {
			active = pipelinedAppendResult{}
		}
	}
}

func (p *pipelinedRuntime) stopAppendWorker() {
	if p == nil {
		return
	}
	if p.runtime.nodePersistence != nil {
		if p.nodeSubmission {
			_, _ = p.runtime.nodePersistence.Wait()
			p.nodeSubmission = false
		}
		return
	}
	p.stopOnce.Do(func() { close(p.stop) })
	<-p.done
}

// SetPipelinedWake installs the nonblocking scheduler notification used by the
// append lane. Installation is cold-path and may replace a prior Host during
// ownership setup; the worker loads the immutable callback atomically.
func (runtime *Runtime) SetPipelinedWake(wake func()) {
	if runtime == nil || runtime.pipelined == nil {
		return
	}
	if runtime.nodePersistence != nil {
		runtime.nodePersistence.sequencer.SetWakeFor(&runtime.nodePersistence.cell, wake)
		runtime.nodePersistence.sequencer.SetWakeFor(&runtime.nodePersistence.checkpoint, wake)
		return
	}
	if wake == nil {
		runtime.pipelined.wake.Store(nil)
		return
	}
	runtime.pipelined.wake.Store(&pipelinedWake{fn: wake})
}

// Pipelined reports whether this Runtime uses ordered asynchronous local
// storage messages.
func (runtime *Runtime) Pipelined() bool {
	return runtime != nil && runtime.pipelined != nil
}

func (runtime *Runtime) reserveProtocolInput() error {
	if runtime.pipelined != nil {
		// Proposal success is only bounded local-core admission. The exact WAL
		// capacity credit is acquired before accepting the resulting Ready.
		return nil
	}
	return runtime.reserveReadyWithWALMaintenance()
}

func (runtime *Runtime) reservePipelinedReady() error {
	p := runtime.pipelined
	if p.appendOutstanding >= pipelinedAppendCapacity ||
		p.applyQueue.remaining() == 0 || p.directQueue.remaining() < raftmodel.MaxPipelinedReadyMessages {
		return raftstore.ErrFull
	}
	if runtime.nodePersistence != nil {
		return nil
	}
	if p.admission != 0 {
		p.admission--
		return nil
	}
	count, err := runtime.wal.ReserveReadyCount(pipelinedReadyAdmissionWindow)
	if err != nil {
		if errors.Is(err, raftstore.ErrFull) && p.appendOutstanding == 0 {
			if maintainErr := runtime.maintainWALAdmission(err); maintainErr != nil {
				return maintainErr
			}
			count, err = runtime.wal.ReserveReadyCount(pipelinedReadyAdmissionWindow)
		}
		if err != nil {
			return err
		}
	}
	if count <= 0 {
		return raftstore.ErrFull
	}
	p.admission = count - 1
	return nil
}

func (p *pipelinedRuntime) enqueueAppend(message *pb.Message) error {
	if message == nil || p.appendOutstanding >= pipelinedAppendCapacity {
		return p.runtime.fail(errors.New("raftmember: pipelined append lane capacity violated"))
	}
	if p.appendReadyID == math.MaxUint64 {
		return p.runtime.fail(errors.New("raftmember: pipelined append Ready ID exhausted"))
	}
	p.appendReadyID++
	var hard *pb.HardState
	mustSync := len(message.GetEntries()) != 0
	if message.Term != nil {
		hard = &pb.HardState{Term: message.Term, Vote: message.Vote, Commit: message.Commit}
		mustSync = mustSync || message.GetTerm() != p.appendTerm || message.GetVote() != p.appendVote
		p.appendTerm, p.appendVote, p.appendCommit = message.GetTerm(), message.GetVote(), message.GetCommit()
	}
	earlyResponses := 0
	for _, response := range message.GetResponses() {
		if response.GetType() != pb.MsgApp || response.GetFrom() != p.runtime.identity.MemberID ||
			response.GetTo() == p.runtime.identity.MemberID || response.GetTerm() != p.durableTerm {
			break
		}
		if !p.directQueue.push(response) {
			return p.runtime.fail(errors.New("raftmember: early replication ring overflow"))
		}
		earlyResponses++
	}
	work := pipelinedAppendWork{message: message, batch: raftmodel.PersistBatch{
		NodeIncarnation: p.runtime.identity.NodeIncarnation,
		ReadyID:         p.appendReadyID, HardState: hard, Entries: message.GetEntries(),
		Snapshot: message.GetSnapshot(), MustSync: mustSync, TransferOwnership: true,
	}, earlyResponses: uint16(earlyResponses)}
	if !p.appendWork.push(work) {
		return p.runtime.fail(errors.New("raftmember: pipelined append ring overflow"))
	}
	p.runtime.traceAppendStage("submit", work.batch)
	p.appendOutstanding++
	if p.runtime.nodePersistence == nil {
		signalPipelinedEdge(p.workerWake)
		return nil
	}
	return p.submitNextNodeAppend()
}

func (p *pipelinedRuntime) submitNodeSeries(
	works *[raftstore.MaxPersistGroupBatches]pipelinedAppendWork,
	count int,
) error {
	if p.nodeSubmission || p.runtime.nodePersistence == nil {
		return nil
	}
	if works == nil || count <= 0 || count > len(works) {
		return p.runtime.fail(errors.New("raftmember: invalid node append series"))
	}
	var batches [raftstore.MaxPersistGroupBatches]raftmodel.PersistBatch
	for index := 0; index < count; index++ {
		batches[index] = works[index].batch
	}
	if _, err := p.runtime.nodePersistence.SubmitSeries(batches[:count]); err != nil {
		return err
	}
	copy(p.nodeWorks[:count], works[:count])
	clear(p.nodeWorks[count:])
	p.nodeWorkCount = uint8(count)
	p.nodeSubmission = true
	return nil
}

func (p *pipelinedRuntime) submitNextNodeAppend() error {
	if p.runtime.nodePersistence == nil || p.nodeSubmission || p.appendRetry {
		return nil
	}
	var works [raftstore.MaxPersistGroupBatches]pipelinedAppendWork
	count := p.appendWork.copyPrefix(&works)
	if count == 0 {
		return nil
	}
	firstReadyID := works[0].batch.ReadyID
	for index := 1; index < count; index++ {
		if firstReadyID > math.MaxUint64-uint64(index) ||
			works[index].batch.ReadyID != firstReadyID+uint64(index) {
			return p.runtime.fail(errors.New("raftmember: unordered node append series"))
		}
	}
	for candidate := count; candidate > 0; candidate-- {
		if err := p.submitNodeSeries(&works, candidate); err != nil {
			if errors.Is(err, raftstore.ErrSubmissionBackpressure) {
				return nil
			}
			// A bounded multi-Ready envelope can be unsupported even when a
			// proper prefix is independently valid (for example, a snapshot or
			// a conservative aggregate admission bound). Shrink only deterministic
			// series-shape failures; the singleton result remains authoritative.
			if candidate > 1 && (errors.Is(err, raftstore.ErrInvalid) ||
				errors.Is(err, raftstore.ErrBounds) ||
				errors.Is(err, raftstore.ErrUnsupportedSnapshot)) {
				continue
			}
			return p.runtime.fail(err)
		}
		if !p.appendWork.popN(candidate) {
			return p.runtime.fail(errors.New("raftmember: node append queue lost submitted series"))
		}
		return nil
	}
	return p.runtime.fail(errors.New("raftmember: node append series has no admissible prefix"))
}

func (p *pipelinedRuntime) requestAppendRetry() error {
	if p.runtime.nodePersistence != nil {
		if p.nodeSubmission || p.nodeWorkCount == 0 || p.nodeWorks[0].message == nil {
			return p.runtime.fail(errors.New("raftmember: invalid node append retry state"))
		}
		if err := p.submitNodeSeries(&p.nodeWorks, int(p.nodeWorkCount)); err != nil {
			return err
		}
		p.appendRetry = false
		return nil
	}
	p.appendRetry = false
	p.retryRequest.Store(true)
	signalPipelinedEdge(p.workerWake)
	return nil
}

func (p *pipelinedRuntime) consumeAppendResult() (DriveResult, bool, error) {
	var result pipelinedAppendResult
	if p.runtime.nodePersistence != nil {
		if !p.nodeSubmission {
			return DriveResult{}, false, nil
		}
		_, done, err := p.runtime.nodePersistence.Poll()
		if !done {
			return DriveResult{}, false, nil
		}
		if p.nodeWorkCount == 0 || int(p.nodeWorkCount) > len(result.works) {
			return DriveResult{}, true, p.runtime.fail(errors.New("raftmember: invalid node append completion"))
		}
		copy(result.works[:p.nodeWorkCount], p.nodeWorks[:p.nodeWorkCount])
		result.count, result.err = p.nodeWorkCount, err
		p.nodeSubmission = false
	} else {
		var ok bool
		result, ok = p.appendDone.pop()
		if !ok {
			return DriveResult{}, false, nil
		}
		signalPipelinedEdge(p.resultSpace)
	}
	if result.count == 0 || int(result.count) > p.appendOutstanding {
		return DriveResult{}, true, p.runtime.fail(errors.New("raftmember: invalid pipelined append completion"))
	}
	firstReadyID := result.works[0].batch.ReadyID
	if p.appendProcessedID == math.MaxUint64 || firstReadyID != p.appendProcessedID+1 {
		return DriveResult{}, true, p.runtime.fail(errors.New("raftmember: pipelined append completion skipped Ready"))
	}
	lastReadyID := result.works[result.count-1].batch.ReadyID
	for index := 0; index < int(result.count); index++ {
		if result.works[index].batch.ReadyID != firstReadyID+uint64(index) {
			return DriveResult{}, true, p.runtime.fail(errors.New("raftmember: unordered pipelined append completion"))
		}
	}
	if result.err != nil {
		if deterministicPersistFailure(result.err) || p.runtime.nodePersistence != nil && errors.Is(result.err, raftstore.ErrPersistenceUnknown) {
			return DriveResult{}, true, p.runtime.fail(result.err)
		}
		p.appendRetry = true
		p.appendRetryReadyID = firstReadyID
		return DriveResult{}, true, result.err
	}
	groupSynced := false
	for index := 0; index < int(result.count); index++ {
		groupSynced = groupSynced || result.works[index].batch.MustSync
	}
	if groupSynced {
		for index := 0; index < int(result.count); index++ {
			hard := result.works[index].batch.HardState
			if hard != nil {
				p.durableTerm, p.durableVote = hard.GetTerm(), hard.GetVote()
			}
		}
	}
	for index := 0; index < int(result.count); index++ {
		p.runtime.traceAppendStage("complete", result.works[index].batch)
		if !p.appendCompleted.push(pipelinedAppendCompletion{
			message: result.works[index].message, responseIndex: result.works[index].earlyResponses,
		}) {
			return DriveResult{}, true, p.runtime.fail(errors.New("raftmember: append completion ring overflow"))
		}
	}
	p.appendOutstanding -= int(result.count)
	p.appendProcessedID = lastReadyID
	p.appendRetryReadyID = 0
	if p.runtime.nodePersistence != nil {
		clear(p.nodeWorks[:p.nodeWorkCount])
		p.nodeWorkCount = 0
		if err := p.submitNextNodeAppend(); err != nil {
			return DriveResult{}, true, err
		}
	}
	return DriveResult{Kind: DrivePersisted, ReadyID: lastReadyID}, true, nil
}

func (p *pipelinedRuntime) deliverResponse(
	response *pb.Message,
	send func(OutboundMessage) error,
) error {
	if response.GetTo() == p.runtime.identity.MemberID {
		if err := p.runtime.node.StepPipelinedResponse(response); err != nil {
			return p.runtime.fail(err)
		}
		return nil
	}
	if _, err := validateOrdinaryMessage(response); err != nil {
		return p.runtime.fail(err)
	}
	if send == nil {
		return errors.New("raftmember: nil outbound sink")
	}
	err := send(OutboundMessage{Group: p.runtime.identity.Group,
		From: response.GetFrom(), To: response.GetTo(), Message: response})
	if err == nil {
		p.runtime.tracePeerStage("send", response)
	}
	return err
}

func (p *pipelinedRuntime) driveResponses(
	send func(OutboundMessage) error,
) (DriveResult, bool, error) {
	// The existing apply machinery temporarily owns Node.phase. Local append
	// responses mutate RawNode and therefore wait until the apply task has
	// reached FinishPipelinedApply and restored PhaseIdle.
	if p.applyCurrent.message != nil && !p.applyFinished {
		return DriveResult{}, false, nil
	}
	for {
		if completion, ok := p.appendCompleted.front(); ok {
			responses := completion.message.GetResponses()
			if int(completion.responseIndex) == len(responses) {
				p.appendCompleted.pop()
				continue
			}
			if int(completion.responseIndex) > len(responses) {
				return DriveResult{}, true, p.runtime.fail(errors.New("raftmember: append response cursor corrupt"))
			}
			if err := p.deliverResponse(responses[completion.responseIndex], send); err != nil {
				return DriveResult{}, true, err
			}
			completion.responseIndex++
			return DriveResult{Kind: DriveMessage}, true, nil
		}
		if p.applyCurrent.message == nil || !p.applyFinished {
			return DriveResult{}, false, nil
		}
		responses := p.applyCurrent.message.GetResponses()
		if p.applyResponseIndex == len(responses) {
			p.applyCurrent = pipelinedApplyTask{}
			p.applyResponseIndex = 0
			p.applyFinished = false
			continue
		}
		if p.applyResponseIndex < 0 || p.applyResponseIndex > len(responses) {
			return DriveResult{}, true, p.runtime.fail(errors.New("raftmember: apply response cursor corrupt"))
		}
		if err := p.deliverResponse(responses[p.applyResponseIndex], send); err != nil {
			return DriveResult{}, true, err
		}
		p.applyResponseIndex++
		return DriveResult{Kind: DriveMessage}, true, nil
	}
}

func (runtime *Runtime) drivePipelinedReady(
	workspace *ReadyWorkspace,
	send func(OutboundMessage) error,
	settle ResultSettlementSink,
) (DriveResult, error) {
	p := runtime.pipelined
	if _, pending := runtime.pendingAppliedResults(); pending {
		return runtime.settleAppliedResults(settle)
	}

	if p.appendRetry {
		readyID := p.appendRetryReadyID
		if err := p.requestAppendRetry(); err != nil {
			return DriveResult{}, err
		}
		return DriveResult{Kind: DriveCaptured, ReadyID: readyID}, nil
	}
	if err := p.submitNextNodeAppend(); err != nil {
		return DriveResult{}, err
	}
	if result, consumed, err := p.consumeAppendResult(); consumed {
		return result, err
	}
	if result, progressed, err := p.driveResponses(send); progressed || err != nil {
		return result, err
	}
	if p.applyCurrent.message != nil || p.applyQueue.len() != 0 {
		if p.applyCurrent.message == nil {
			next, _ := p.applyQueue.front()
			if next.requiredAppendReadyID > p.appendProcessedID {
				goto driveDirect
			}
			if p.applyReadyID == math.MaxUint64 {
				return DriveResult{}, runtime.fail(errors.New("raftmember: pipelined apply Ready ID exhausted"))
			}
			p.applyReadyID++
			p.applyCurrent = p.applyQueue.pop()
			if err := runtime.node.BeginPipelinedApply(p.applyReadyID, p.applyCurrent.message.GetEntries()); err != nil {
				return DriveResult{}, runtime.fail(err)
			}
		}
		if runtime.node.Phase() == raftmodel.PhaseEntriesApplied {
			readyID := p.applyReadyID
			if err := runtime.node.FinishPipelinedApply(); err != nil {
				return DriveResult{}, runtime.fail(err)
			}
			p.applyFinished = true
			return DriveResult{Kind: DriveEntriesFinished, ReadyID: readyID}, nil
		}
		requiresSettlement, err := runtime.node.NextApplyRequiresResultSettlement()
		if err != nil {
			return DriveResult{}, runtime.fail(err)
		}
		if requiresSettlement && (settle == nil || workspace == nil) {
			if settle == nil {
				return DriveResult{}, ErrResultSettlementRequired
			}
			return DriveResult{}, ErrReadyWorkspaceRequired
		}
		var normalWorkspace *raftmodel.NormalApplyBatchWorkspace
		if workspace != nil {
			normalWorkspace = &workspace.normal
		}
		applyRegion := trace.StartRegion(context.Background(), "raft.apply.execute")
		applied, applyErr := runtime.node.ApplyNextBatch(normalWorkspace)
		applyRegion.End()
		if applyErr != nil {
			return DriveResult{}, runtime.fail(applyErr)
		}
		if applied.Normal.Len() != 0 {
			return runtime.settleAppliedResults(settle)
		}
		if runtime.node.Phase() == raftmodel.PhaseEntriesApplied {
			readyID := p.applyReadyID
			if err := runtime.node.FinishPipelinedApply(); err != nil {
				return DriveResult{}, runtime.fail(err)
			}
			p.applyFinished = true
			return DriveResult{Kind: DriveEntriesFinished, ReadyID: readyID}, nil
		}
		return DriveResult{Kind: DriveEntry, ReadyID: p.applyReadyID}, nil
	}

driveDirect:
	if message, ok := p.directQueue.front(); ok {
		if _, err := validateOrdinaryMessage(message); err != nil {
			return DriveResult{}, runtime.fail(err)
		}
		if send == nil {
			return DriveResult{}, errors.New("raftmember: nil outbound sink")
		}
		if err := send(OutboundMessage{Group: runtime.identity.Group,
			From: message.GetFrom(), To: message.GetTo(), Message: message}); err != nil {
			return DriveResult{}, err
		}
		runtime.tracePeerStage("send", message)
		p.directQueue.pop()
		return DriveResult{Kind: DriveMessage}, nil
	}

	if err := runtime.reservePipelinedReady(); err != nil {
		return DriveResult{}, err
	}
	proposalCount := runtime.proposalBatchEntries
	proposalBytes := runtime.proposalBatchBytes
	ready, captured, err := runtime.node.CapturePipelinedReady()
	if err != nil {
		return DriveResult{}, runtime.fail(err)
	}
	if !captured {
		if runtime.nodePersistence == nil {
			p.admission++
		}
		return DriveResult{}, nil
	}
	appendCount := 0
	applyCount := 0
	directCount := 0
	for _, message := range ready.Messages {
		switch message.GetType() {
		case pb.MsgStorageAppend:
			appendCount++
		case pb.MsgStorageApply:
			applyCount++
		default:
			directCount++
		}
	}
	if appendCount > 1 || applyCount > 1 ||
		p.appendOutstanding+appendCount > pipelinedAppendCapacity ||
		applyCount > p.applyQueue.remaining() || directCount > p.directQueue.remaining() {
		return DriveResult{}, runtime.fail(errors.New("raftmember: pipelined Ready exceeds fixed scheduler capacity"))
	}
	for _, message := range ready.Messages {
		switch message.GetType() {
		case pb.MsgStorageAppend:
			if err := p.enqueueAppend(message); err != nil {
				return DriveResult{}, err
			}
		case pb.MsgStorageApply:
			if !p.applyQueue.push(pipelinedApplyTask{
				message: message, requiredAppendReadyID: p.appendReadyID,
			}) {
				return DriveResult{}, runtime.fail(errors.New("raftmember: pipelined apply ring overflow"))
			}
		default:
			if !p.directQueue.push(message) {
				return DriveResult{}, runtime.fail(errors.New("raftmember: pipelined direct ring overflow"))
			}
		}
	}
	if appendCount == 0 && runtime.nodePersistence == nil {
		p.admission++
	}
	runtime.proposalBatchEntries = 0
	runtime.proposalBatchBytes = 0
	result := DriveResult{Kind: DriveCaptured, ReadOutcomes: ready.ReadOutcomes,
		ProposalCount: proposalCount, ProposalBytes: proposalBytes}
	if len(ready.ReadOutcomes) != 0 {
		result.Kind = DriveReadStatesFinished
	}
	return result, nil
}

func (p *pipelinedRuntime) quiescent() bool {
	return p == nil || (p.appendOutstanding == 0 && p.appendWork.len() == 0 &&
		p.appendDone.len() == 0 && p.appendCompleted.len() == 0 &&
		!p.nodeSubmission && p.nodeWorkCount == 0 &&
		p.applyQueue.len() == 0 && p.directQueue.len() == 0 &&
		p.applyCurrent.message == nil && !p.appendRetry)
}

func (p *pipelinedRuntime) String() string {
	return fmt.Sprintf("append=%d/%d done=%d apply=%d completed=%d direct=%d",
		p.appendWork.len(), p.appendOutstanding, p.appendDone.len(), p.applyQueue.len(),
		p.appendCompleted.len(), p.directQueue.len())
}
