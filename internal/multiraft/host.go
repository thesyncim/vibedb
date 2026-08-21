// Package multiraft provides a bounded, single-owner in-process scheduler for
// non-serving raftmember runtimes. One Runtime is one range/shard Raft group;
// one-range and many-range deployments use the same Host path. Host contains no
// goroutines, wall clock, sockets, peer authentication, snapshot transfer, or
// client serving API. It scans only a runnable queue, so a group with no Ready,
// input, or queued logical tick consumes no scheduler-scan CPU after its initial
// probe, though it still owns ordinary Runtime, memory, and WAL state.
package multiraft

import (
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	AbsoluteMaxGroups       = 4096
	AbsoluteMaxQueueItems   = 65536
	AbsoluteMaxGroupItems   = 4096
	AbsoluteMaxOutboxItems  = 65536
	AbsoluteMaxPendingTicks = 4096
	AbsoluteMaxQueueBytes   = int64(1 << 30)
	AbsoluteMaxGroupBytes   = int64(128 << 20)
	AbsoluteMaxOutboxBytes  = int64(1 << 30)
)

var (
	ErrInvalidLimits = errors.New("multiraft: invalid host limits")
	ErrHostClosed    = errors.New("multiraft: host is closed")
	ErrHostFull      = errors.New("multiraft: host group capacity reached")
	ErrGroupExists   = errors.New("multiraft: group already exists")
	ErrGroupNotFound = errors.New("multiraft: group not found")
	ErrGroupBusy     = errors.New("multiraft: group is not quiescent")
	ErrGroupFaulted  = errors.New("multiraft: group is faulted")
	ErrQueueFull     = errors.New("multiraft: input queue is full")
	ErrOutboxFull    = errors.New("multiraft: outbound queue is full")
	ErrOutboundBound = errors.New("multiraft: outbound message exceeds host bound")
)

// Limits bounds every retained Host object. All fields are required; no zero
// value is interpreted as an implicit default.
type Limits struct {
	MaxGroups       int
	MaxQueueItems   int
	MaxQueueBytes   int64
	MaxGroupItems   int
	MaxGroupBytes   int64
	MaxOutboxItems  int
	MaxOutboxBytes  int64
	MaxPendingTicks int
}

// ProgressKind identifies the one scheduler operation attempted by RunOne.
type ProgressKind uint8

const (
	ProgressNone ProgressKind = iota
	ProgressReady
	ProgressMessage
	ProgressProposal
	ProgressTick
	ProgressCampaign
	ProgressFault
)

// Progress describes one completed Ready micro-step or one consumed input.
// When RunOne also returns an error, Done is still true if the input was
// consumed or a group was newly latched faulted. ReadOutcomes is owned by the
// caller and present only on the corresponding Ready completion step.
type Progress struct {
	Group        raftmember.GroupKey
	Kind         ProgressKind
	ReadyKind    raftmember.DriveKind
	ReadOutcomes []raftmodel.ReadOutcome
}

type memberRuntime interface {
	Identity() raftmember.RuntimeIdentity
	Failure() error
	Propose([]byte) error
	ProposeConfChange(pb.ConfChangeI) error
	ReadIndex([]byte) error
	Publication() (raftmodel.Publication, error)
	SnapshotState() (replicatedstate.State, error)
	Status() (raftmember.RuntimeStatus, error)
	Progress(uint64) (raftmodel.MemberProgress, bool, error)
	TransferLeader(uint64) error
	StepMessage(*pb.Message) error
	Tick() error
	Campaign() error
	DriveReady(func(raftmember.OutboundMessage) error) (raftmember.DriveResult, error)
	Close() error
}

type queuedMessage struct {
	message *pb.Message
	size    int64
}

type messageQueue struct {
	items []queuedMessage
	head  int
}

func (queue *messageQueue) len() int { return len(queue.items) - queue.head }

func (queue *messageQueue) push(item queuedMessage) {
	queue.compactForPush()
	queue.items = append(queue.items, item)
}

func (queue *messageQueue) pop() (queuedMessage, bool) {
	if queue.head == len(queue.items) {
		return queuedMessage{}, false
	}
	result := queue.items[queue.head]
	queue.items[queue.head] = queuedMessage{}
	queue.head++
	queue.compactAfterPop()
	return result, true
}

func (queue *messageQueue) compactForPush() {
	if queue.head == 0 || len(queue.items) < cap(queue.items) {
		return
	}
	queue.compact()
}

func (queue *messageQueue) compactAfterPop() {
	if queue.head == len(queue.items) {
		queue.items = nil
		queue.head = 0
		return
	}
	if queue.head >= 64 && queue.head >= len(queue.items)/2 {
		queue.compact()
	}
}

func (queue *messageQueue) compact() {
	copy(queue.items, queue.items[queue.head:])
	live := len(queue.items) - queue.head
	clear(queue.items[live:])
	queue.items = queue.items[:live]
	queue.head = 0
}

func (queue *messageQueue) clear() {
	clear(queue.items)
	queue.items = nil
	queue.head = 0
}

type proposalQueue struct {
	items [][]byte
	head  int
}

func (queue *proposalQueue) len() int { return len(queue.items) - queue.head }

func (queue *proposalQueue) push(item []byte) {
	if queue.head != 0 && len(queue.items) == cap(queue.items) {
		queue.compact()
	}
	queue.items = append(queue.items, item)
}

func (queue *proposalQueue) pop() ([]byte, bool) {
	if queue.head == len(queue.items) {
		return nil, false
	}
	result := queue.items[queue.head]
	queue.items[queue.head] = nil
	queue.head++
	if queue.head == len(queue.items) {
		queue.items = nil
		queue.head = 0
	} else if queue.head >= 64 && queue.head >= len(queue.items)/2 {
		queue.compact()
	}
	return result, true
}

func (queue *proposalQueue) compact() {
	copy(queue.items, queue.items[queue.head:])
	live := len(queue.items) - queue.head
	clear(queue.items[live:])
	queue.items = queue.items[:live]
	queue.head = 0
}

func (queue *proposalQueue) clear() {
	clear(queue.items)
	queue.items = nil
	queue.head = 0
}

type inputClass uint8

const (
	inputMessage inputClass = iota
	inputProposal
	inputTick
	inputCampaign
	inputClassCount
)

type groupState struct {
	key       raftmember.GroupKey
	memberID  uint64
	runtime   memberRuntime
	runnable  bool
	messages  messageQueue
	proposals proposalQueue
	ticks     int
	campaigns int
	items     int
	bytes     int64
	nextClass inputClass
	failure   error
	retiring  bool
}

type outboundItem struct {
	message raftmember.OutboundMessage
	size    int64
}

type outboundQueue struct {
	items []outboundItem
	head  int
}

func (queue *outboundQueue) len() int { return len(queue.items) - queue.head }

func (queue *outboundQueue) push(item outboundItem) {
	if queue.head != 0 && len(queue.items) == cap(queue.items) {
		queue.compact()
	}
	queue.items = append(queue.items, item)
}

func (queue *outboundQueue) pop() (outboundItem, bool) {
	if queue.head == len(queue.items) {
		return outboundItem{}, false
	}
	result := queue.items[queue.head]
	queue.items[queue.head] = outboundItem{}
	queue.head++
	if queue.head == len(queue.items) {
		queue.items = nil
		queue.head = 0
	} else if queue.head >= 64 && queue.head >= len(queue.items)/2 {
		queue.compact()
	}
	return result, true
}

func (queue *outboundQueue) compact() {
	copy(queue.items, queue.items[queue.head:])
	live := len(queue.items) - queue.head
	clear(queue.items[live:])
	queue.items = queue.items[:live]
	queue.head = 0
}

func (queue *outboundQueue) clear() {
	clear(queue.items)
	queue.items = nil
	queue.head = 0
}

// Host owns a bounded set of Runtime members and schedules one synchronous
// operation per RunOne call. It is deliberately not safe for concurrent use.
type Host struct {
	limits       Limits
	groups       map[raftmember.GroupKey]*groupState
	order        []*groupState
	runnable     []*groupState
	runnableHead int

	queueItems  int
	queueBytes  int64
	outbox      outboundQueue
	outboxBytes int64
	closed      bool
}

// NewHost validates every limit and relationship before allocating the Host.
func NewHost(limits Limits) (*Host, error) {
	if err := validateLimits(limits); err != nil {
		return nil, err
	}
	return &Host{limits: limits, groups: make(map[raftmember.GroupKey]*groupState)}, nil
}

func validateLimits(limits Limits) error {
	if limits.MaxGroups <= 0 || limits.MaxGroups > AbsoluteMaxGroups ||
		limits.MaxQueueItems <= 0 || limits.MaxQueueItems > AbsoluteMaxQueueItems ||
		limits.MaxGroupItems <= 0 || limits.MaxGroupItems > AbsoluteMaxGroupItems ||
		limits.MaxOutboxItems <= 0 || limits.MaxOutboxItems > AbsoluteMaxOutboxItems ||
		limits.MaxPendingTicks <= 0 || limits.MaxPendingTicks > AbsoluteMaxPendingTicks ||
		limits.MaxQueueBytes < raftmodel.MaxInboundMessageBytes || limits.MaxQueueBytes > AbsoluteMaxQueueBytes ||
		limits.MaxGroupBytes < raftmodel.MaxInboundMessageBytes || limits.MaxGroupBytes > AbsoluteMaxGroupBytes ||
		limits.MaxOutboxBytes < raftmodel.MaxInboundMessageBytes || limits.MaxOutboxBytes > AbsoluteMaxOutboxBytes ||
		limits.MaxGroupItems > limits.MaxQueueItems || limits.MaxGroupBytes > limits.MaxQueueBytes ||
		limits.MaxPendingTicks > limits.MaxGroupItems {
		return fmt.Errorf("%w: %+v", ErrInvalidLimits, limits)
	}
	return nil
}

// Add transfers ownership of runtime only on success. Failure leaves it with
// the caller. Successful addition forbids every later direct caller use.
func (host *Host) Add(runtime *raftmember.Runtime) error {
	if runtime == nil {
		return ErrGroupNotFound
	}
	return host.addRuntime(runtime)
}

func (host *Host) addRuntime(runtime memberRuntime) error {
	if host == nil || host.closed {
		return ErrHostClosed
	}
	if runtime == nil {
		return ErrGroupNotFound
	}
	identity := runtime.Identity()
	key := identity.Group
	if key == (raftmember.GroupKey{}) || identity.MemberID == 0 || identity.NodeIncarnation == 0 {
		return fmt.Errorf("%w: invalid runtime identity", ErrGroupNotFound)
	}
	if _, exists := host.groups[key]; exists {
		return ErrGroupExists
	}
	if len(host.order) >= host.limits.MaxGroups {
		return ErrHostFull
	}
	if failure := runtime.Failure(); failure != nil {
		return errors.Join(ErrGroupFaulted, failure)
	}
	group := &groupState{key: key, memberID: identity.MemberID, runtime: runtime}
	host.groups[key] = group
	host.order = append(host.order, group)
	host.wake(group)
	return nil
}

func (host *Host) lookup(key raftmember.GroupKey) (*groupState, error) {
	if host == nil || host.closed {
		return nil, ErrHostClosed
	}
	group := host.groups[key]
	if group == nil {
		return nil, ErrGroupNotFound
	}
	if group.failure != nil {
		return nil, errors.Join(ErrGroupFaulted, group.failure)
	}
	if group.retiring {
		return nil, ErrGroupBusy
	}
	return group, nil
}

// Remove closes and releases one fully quiescent group. The group must have no
// runnable Ready work, queued input, or retained outbound message. A close
// failure keeps the group owned in retiring state so a later Remove can retry;
// successful removal frees its active-group slot for range split/reallocation
// churn. Topology authorization and generation fencing remain caller duties.
func (host *Host) Remove(key raftmember.GroupKey) error {
	if host == nil || host.closed {
		return ErrHostClosed
	}
	group := host.groups[key]
	if group == nil {
		return ErrGroupNotFound
	}
	if !group.retiring {
		if group.runnable || group.items != 0 || group.messages.len() != 0 ||
			group.proposals.len() != 0 || group.ticks != 0 || group.campaigns != 0 ||
			host.hasOutboundFor(key) {
			return ErrGroupBusy
		}
		group.retiring = true
	}
	if group.runtime != nil {
		if err := group.runtime.Close(); err != nil {
			return err
		}
		group.runtime = nil
	}
	delete(host.groups, key)
	for index, candidate := range host.order {
		if candidate != group {
			continue
		}
		copy(host.order[index:], host.order[index+1:])
		host.order[len(host.order)-1] = nil
		host.order = host.order[:len(host.order)-1]
		break
	}
	return nil
}

func (host *Host) hasOutboundFor(key raftmember.GroupKey) bool {
	for index := host.outbox.head; index < len(host.outbox.items); index++ {
		if host.outbox.items[index].message.Group == key {
			return true
		}
	}
	return false
}

// EnqueueMessage validates and clones one bounded ordinary peer message.
func (host *Host) EnqueueMessage(key raftmember.GroupKey, message *pb.Message) error {
	group, err := host.lookup(key)
	if err != nil {
		return err
	}
	size, err := host.measureInbound(group, message)
	if err != nil {
		return err
	}
	if err := host.admitInput(group, int64(size)); err != nil {
		return err
	}
	owned := proto.Clone(message).(*pb.Message)
	host.enqueueOwnedMessage(group, owned, size)
	return nil
}

// AdoptMessage validates and takes ownership of one already detached ordinary
// peer message. Ownership transfers only when nil is returned. This avoids a
// second maximum-size clone between a hostile-wire decoder and the serialized
// Host ingress owner. The caller must not retain or mutate message after a
// successful call.
func (host *Host) AdoptMessage(key raftmember.GroupKey, message *pb.Message) error {
	group, err := host.lookup(key)
	if err != nil {
		return err
	}
	size, err := host.measureInbound(group, message)
	if err != nil {
		return err
	}
	if err := host.admitInput(group, int64(size)); err != nil {
		return err
	}
	host.enqueueOwnedMessage(group, message, size)
	return nil
}

func (host *Host) measureInbound(group *groupState, message *pb.Message) (int, error) {
	if message == nil || message.GetTo() != group.memberID || message.GetFrom() == 0 ||
		message.GetFrom() == group.memberID || raft.IsLocalMsgTarget(message.GetFrom()) {
		return 0, errors.New("multiraft: message has invalid group route")
	}
	return raftmember.MeasureOrdinaryMessage(message)
}

func (host *Host) enqueueOwnedMessage(group *groupState, message *pb.Message, size int) {
	group.messages.push(queuedMessage{message: message, size: int64(size)})
	host.chargeInput(group, int64(size))
	host.wake(group)
}

// EnqueueProposal clones one bounded encoded command for later Runtime
// admission. Queueing does not imply leadership or core admission.
func (host *Host) EnqueueProposal(key raftmember.GroupKey, data []byte) error {
	group, err := host.lookup(key)
	if err != nil {
		return err
	}
	if len(data) > raftmodel.MaxProposalBytes {
		return fmt.Errorf("%w: proposal exceeds bound", raftmodel.ErrAdmissionBound)
	}
	if err := host.admitInput(group, int64(len(data))); err != nil {
		return err
	}
	group.proposals.push(append([]byte(nil), data...))
	host.chargeInput(group, int64(len(data)))
	host.wake(group)
	return nil
}

// ProposeConfChange synchronously admits one caller-authorized membership
// operation. Control input is intentionally not queued: the caller must drive
// and retry after ErrReadyPending, preventing stale topology intent from
// lingering behind unrelated work.
func (host *Host) ProposeConfChange(key raftmember.GroupKey, change pb.ConfChangeI) error {
	group, err := host.lookup(key)
	if err != nil {
		return err
	}
	err = group.runtime.ProposeConfChange(change)
	host.finishDirectControl(group, err)
	return err
}

// ReadIndex synchronously starts one bounded quorum-confirmed read barrier.
// The exact terminal outcome is returned by a later RunOne Progress value.
func (host *Host) ReadIndex(key raftmember.GroupKey, context []byte) error {
	group, err := host.lookup(key)
	if err != nil {
		return err
	}
	err = group.runtime.ReadIndex(context)
	host.finishDirectControl(group, err)
	return err
}

// Publication returns the group's detached atomically published apply cut.
func (host *Host) Publication(key raftmember.GroupKey) (raftmodel.Publication, error) {
	group, err := host.lookup(key)
	if err != nil {
		return raftmodel.Publication{}, err
	}
	return group.runtime.Publication()
}

// SnapshotState returns one group's coherent durable state for control-plane
// verification of an installed certified learner base.
func (host *Host) SnapshotState(key raftmember.GroupKey) (replicatedstate.State, error) {
	group, err := host.lookup(key)
	if err != nil {
		return replicatedstate.State{}, err
	}
	return group.runtime.SnapshotState()
}

// Status returns one group's detached local Raft status.
func (host *Host) Status(key raftmember.GroupKey) (raftmember.RuntimeStatus, error) {
	group, err := host.lookup(key)
	if err != nil {
		return raftmember.RuntimeStatus{}, err
	}
	return group.runtime.Status()
}

// Progress returns one member's allocation-free replication progress from the
// local leader.
func (host *Host) Progress(
	key raftmember.GroupKey,
	memberID uint64,
) (raftmodel.MemberProgress, bool, error) {
	group, err := host.lookup(key)
	if err != nil {
		return raftmodel.MemberProgress{}, false, err
	}
	return group.runtime.Progress(memberID)
}

// TransferLeader synchronously admits an authorized leadership handoff. Like
// configuration control it is not queued behind stale topology intent.
func (host *Host) TransferLeader(key raftmember.GroupKey, transferee uint64) error {
	group, err := host.lookup(key)
	if err != nil {
		return err
	}
	err = group.runtime.TransferLeader(transferee)
	host.finishDirectControl(group, err)
	return err
}

func (host *Host) finishDirectControl(group *groupState, controlErr error) {
	if group == nil || group.runtime == nil {
		return
	}
	failure := group.runtime.Failure()
	if failure != nil || errors.Is(controlErr, raftmember.ErrRuntimeFailed) ||
		errors.Is(controlErr, raftmember.ErrRuntimeClosed) {
		if failure != nil {
			group.failure = failure
		} else {
			group.failure = controlErr
		}
		host.purgeGroup(group)
		return
	}
	if controlErr == nil {
		host.wake(group)
	}
}

// RequestTick retains one exact logical tick. Tick debt is bounded, counted as
// queue items, and serviced one tick per scheduling turn.
func (host *Host) RequestTick(key raftmember.GroupKey) error {
	group, err := host.lookup(key)
	if err != nil {
		return err
	}
	if group.ticks >= host.limits.MaxPendingTicks {
		return ErrQueueFull
	}
	if err := host.admitInput(group, 0); err != nil {
		return err
	}
	group.ticks++
	host.chargeInput(group, 0)
	host.wake(group)
	return nil
}

// RequestCampaign retains one exact campaign request. Requests are not
// coalesced because doing so would change protocol input semantics.
func (host *Host) RequestCampaign(key raftmember.GroupKey) error {
	group, err := host.lookup(key)
	if err != nil {
		return err
	}
	if err := host.admitInput(group, 0); err != nil {
		return err
	}
	group.campaigns++
	host.chargeInput(group, 0)
	host.wake(group)
	return nil
}

func (host *Host) admitInput(group *groupState, size int64) error {
	if size < 0 || group.items >= host.limits.MaxGroupItems || host.queueItems >= host.limits.MaxQueueItems ||
		size > host.limits.MaxGroupBytes-group.bytes || size > host.limits.MaxQueueBytes-host.queueBytes {
		return ErrQueueFull
	}
	return nil
}

func (host *Host) chargeInput(group *groupState, size int64) {
	group.items++
	group.bytes += size
	host.queueItems++
	host.queueBytes += size
}

func (host *Host) releaseInput(group *groupState, size int64) {
	group.items--
	group.bytes -= size
	host.queueItems--
	host.queueBytes -= size
}

func (host *Host) runnableLen() int { return len(host.runnable) - host.runnableHead }

func (host *Host) wake(group *groupState) {
	if group == nil || group.runnable || group.runtime == nil || group.failure != nil || group.retiring {
		return
	}
	if host.runnableHead != 0 && len(host.runnable) == cap(host.runnable) {
		copy(host.runnable, host.runnable[host.runnableHead:])
		live := len(host.runnable) - host.runnableHead
		clear(host.runnable[live:])
		host.runnable = host.runnable[:live]
		host.runnableHead = 0
	}
	group.runnable = true
	host.runnable = append(host.runnable, group)
}

func (host *Host) popRunnable() *groupState {
	if host.runnableHead == len(host.runnable) {
		return nil
	}
	group := host.runnable[host.runnableHead]
	host.runnable[host.runnableHead] = nil
	host.runnableHead++
	if host.runnableHead == len(host.runnable) {
		// Retain the bounded backing array. A hot group is normally woken again
		// after this pop; dropping the slice here would allocate a new runnable
		// queue on every Ready/input micro-step.
		host.runnable = host.runnable[:0]
		host.runnableHead = 0
	} else if host.runnableHead >= 64 && host.runnableHead >= len(host.runnable)/2 {
		copy(host.runnable, host.runnable[host.runnableHead:])
		live := len(host.runnable) - host.runnableHead
		clear(host.runnable[live:])
		host.runnable = host.runnable[:live]
		host.runnableHead = 0
	}
	if group != nil {
		group.runnable = false
	}
	return group
}

// RunOne performs at most one Ready lifecycle operation or consumes at most
// one queued input. Only explicitly runnable groups are visited, in persistent
// FIFO round-robin order; idle groups consume no scheduler scans. A group
// blocked by outbox capacity does not prevent another group from progressing.
func (host *Host) RunOne() (Progress, bool, error) {
	if host == nil || host.closed {
		return Progress{}, false, ErrHostClosed
	}
	count := host.runnableLen()
	if count == 0 {
		return Progress{}, false, nil
	}
	blockedOutbox := false
	for range count {
		group := host.popRunnable()
		if group == nil || group.runtime == nil || group.failure != nil || group.retiring {
			continue
		}
		ready, err := group.runtime.DriveReady(func(outbound raftmember.OutboundMessage) error {
			return host.enqueueOutbound(group, outbound)
		})
		if err != nil {
			if errors.Is(err, ErrOutboxFull) {
				blockedOutbox = true
				host.wake(group)
				continue
			}
			if failure := group.runtime.Failure(); failure != nil ||
				errors.Is(err, raftmember.ErrRuntimeFailed) || errors.Is(err, raftmember.ErrRuntimeClosed) {
				if failure != nil {
					group.failure = failure
				} else {
					group.failure = err
				}
				host.purgeGroup(group)
				return Progress{Group: group.key, Kind: ProgressFault}, true, err
			}
			// Persistence and other nonterminal boundary errors retain their exact
			// Runtime phase. Re-wake the group so an explicit later RunOne can retry.
			host.wake(group)
			return Progress{Group: group.key, Kind: ProgressReady}, false, err
		}
		if ready.Progressed() {
			host.wake(group)
			return Progress{
				Group: group.key, Kind: ProgressReady, ReadyKind: ready.Kind,
				ReadOutcomes: ready.ReadOutcomes,
			}, true, nil
		}

		progress, consumed, inputErr := host.runInput(group)
		if !consumed {
			continue
		}
		if errors.Is(inputErr, raftmember.ErrRuntimeFailed) || errors.Is(inputErr, raftmember.ErrRuntimeClosed) {
			group.failure = inputErr
			progress.Kind = ProgressFault
			host.purgeGroup(group)
		} else {
			host.wake(group)
		}
		return progress, true, inputErr
	}
	if blockedOutbox {
		return Progress{}, false, ErrOutboxFull
	}
	return Progress{}, false, nil
}

func (host *Host) purgeGroup(group *groupState) {
	if group == nil {
		return
	}
	host.queueItems -= group.items
	host.queueBytes -= group.bytes
	group.messages.clear()
	group.proposals.clear()
	group.ticks = 0
	group.campaigns = 0
	group.items = 0
	group.bytes = 0
	group.runnable = false
}

func (host *Host) runInput(group *groupState) (Progress, bool, error) {
	for checked := inputClass(0); checked < inputClassCount; checked++ {
		class := (group.nextClass + checked) % inputClassCount
		progress := Progress{Group: group.key}
		switch class {
		case inputMessage:
			item, ok := group.messages.pop()
			if !ok {
				continue
			}
			host.releaseInput(group, item.size)
			group.nextClass = (class + 1) % inputClassCount
			progress.Kind = ProgressMessage
			return progress, true, group.runtime.StepMessage(item.message)
		case inputProposal:
			data, ok := group.proposals.pop()
			if !ok {
				continue
			}
			host.releaseInput(group, int64(len(data)))
			group.nextClass = (class + 1) % inputClassCount
			progress.Kind = ProgressProposal
			return progress, true, group.runtime.Propose(data)
		case inputTick:
			if group.ticks == 0 {
				continue
			}
			group.ticks--
			host.releaseInput(group, 0)
			group.nextClass = (class + 1) % inputClassCount
			progress.Kind = ProgressTick
			return progress, true, group.runtime.Tick()
		case inputCampaign:
			if group.campaigns == 0 {
				continue
			}
			group.campaigns--
			host.releaseInput(group, 0)
			group.nextClass = (class + 1) % inputClassCount
			progress.Kind = ProgressCampaign
			return progress, true, group.runtime.Campaign()
		}
	}
	return Progress{}, false, nil
}

func (host *Host) enqueueOutbound(group *groupState, outbound raftmember.OutboundMessage) error {
	if outbound.Group != group.key || outbound.Message == nil || outbound.From != group.memberID ||
		outbound.To == 0 || outbound.To == group.memberID ||
		outbound.From != outbound.Message.GetFrom() || outbound.To != outbound.Message.GetTo() {
		return errors.New("multiraft: runtime returned a mismatched outbound message")
	}
	size, err := raftmember.MeasureOrdinaryMessage(outbound.Message)
	if err != nil {
		return err
	}
	messageBytes := int64(size)
	if messageBytes > host.limits.MaxOutboxBytes {
		return ErrOutboundBound
	}
	if host.outbox.len() >= host.limits.MaxOutboxItems ||
		messageBytes > host.limits.MaxOutboxBytes-host.outboxBytes {
		return ErrOutboxFull
	}
	owned, _, err := raftmember.CloneOrdinaryMessage(outbound.Message)
	if err != nil {
		return err
	}
	host.outbox.push(outboundItem{
		message: raftmember.OutboundMessage{
			Group: group.key, From: outbound.From, To: outbound.To, Message: owned,
		},
		size: messageBytes,
	})
	host.outboxBytes += messageBytes
	return nil
}

// PopOutbound transfers ownership of the next detached ordinary message to the
// caller. The Host retains no protobuf alias after the pop.
func (host *Host) PopOutbound() (raftmember.OutboundMessage, bool) {
	if host == nil || host.closed {
		return raftmember.OutboundMessage{}, false
	}
	item, ok := host.outbox.pop()
	if !ok {
		return raftmember.OutboundMessage{}, false
	}
	host.outboxBytes -= item.size
	return item.message, true
}

func (host *Host) clearQueues() {
	for _, group := range host.order {
		if group == nil {
			continue
		}
		group.messages.clear()
		group.proposals.clear()
		group.ticks = 0
		group.campaigns = 0
		group.items = 0
		group.bytes = 0
	}
	host.queueItems = 0
	host.queueBytes = 0
	host.outbox.clear()
	host.outboxBytes = 0
	for _, group := range host.runnable {
		if group != nil {
			group.runnable = false
		}
	}
	clear(host.runnable)
	host.runnable = nil
	host.runnableHead = 0
}

// Close stops admission, releases every queued payload, and closes every
// adopted Runtime in deterministic insertion order. Failed Runtime closes stay
// owned for retry by a later Close call.
func (host *Host) Close() error {
	if host == nil {
		return nil
	}
	if !host.closed {
		host.closed = true
		host.clearQueues()
	}
	var closeErr error
	remaining := false
	for _, group := range host.order {
		if group == nil || group.runtime == nil {
			continue
		}
		if err := group.runtime.Close(); err != nil {
			closeErr = errors.Join(closeErr, err)
			remaining = true
			continue
		}
		group.runtime = nil
	}
	if !remaining {
		clear(host.groups)
	}
	return closeErr
}
