// Package multiraft provides bounded in-process scheduling for raftmember
// runtimes. Host is one strict single-owner lane; ExecutionLanes deterministically
// partitions groups across independent Hosts so callers can drive lanes on
// separate cores without changing any group's Raft ordering. One Runtime is one
// range/shard Raft group; one-range and many-range deployments use the same Host
// path. The scheduler contains no hidden goroutines, wall clock, sockets, peer
// authentication, snapshot transfer, or client serving API. It scans only a
// runnable queue, so a group with no Ready, input, or queued logical tick consumes
// no scheduler-scan CPU after its initial probe, though it still owns ordinary
// Runtime, memory, and WAL state.
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
	ErrInvalidLimits             = errors.New("multiraft: invalid host limits")
	ErrHostClosed                = errors.New("multiraft: host is closed")
	ErrHostFull                  = errors.New("multiraft: host group capacity reached")
	ErrGroupExists               = errors.New("multiraft: group already exists")
	ErrGroupNotFound             = errors.New("multiraft: group not found")
	ErrGroupBusy                 = errors.New("multiraft: group is not quiescent")
	ErrGroupFaulted              = errors.New("multiraft: group is faulted")
	ErrQueueFull                 = errors.New("multiraft: input queue is full")
	ErrOutboxFull                = errors.New("multiraft: outbound queue is full")
	ErrOutboundBound             = errors.New("multiraft: outbound message exceeds host bound")
	ErrSettlementSinkRequired    = errors.New("multiraft: result settlement sink is required")
	ErrProposalSinkRequired      = errors.New("multiraft: proposal lifecycle sink is required")
	ErrProposalGroupSinkRequired = errors.New("multiraft: proposal group lifecycle sink is required")
	ErrProposalPendingRequired   = errors.New("multiraft: proposal group pending sink is required")
	ErrSourceClaimSinkRequired   = errors.New("multiraft: applied source claim sink is required")
	ErrSourceReleaseSinkRequired = errors.New("multiraft: applied source release sink is required")
	ErrSourceOwnerMismatch       = errors.New("multiraft: applied source owner mismatch")
	ErrProposalToken             = errors.New("multiraft: invalid tracked proposal token")
)

// ProposalToken is a fixed opaque serving-registry identity. The zero value is
// reserved for ordinary untracked Host proposals.
type ProposalToken [4]uint64

// ProposalAdmission is the terminal local-core admission result for one
// consumed tracked queue item. Cause is borrowed for the callback only.
type ProposalAdmission struct {
	Group       raftmember.GroupKey
	SourceOwner raftmember.AppliedSourceOwner
	SourceToken raftmember.AppliedSourceToken
	Token       ProposalToken
	Admitted    bool
	Cause       error
}

// ProposalLifecycleSink is invoked synchronously after a tracked queue item is
// unambiguously consumed. It must not block, retain Cause, or re-enter Host.
type ProposalLifecycleSink func(ProposalAdmission)

// ProposalGroupTerminationReason is one closed infrastructure boundary after
// which an admitted tracked proposal may no longer produce a local result.
type ProposalGroupTerminationReason uint8

const (
	ProposalGroupLeadershipLost ProposalGroupTerminationReason = iota + 1
	ProposalGroupRemoved
	ProposalGroupFaulted
	ProposalHostClosed
)

// ProposalGroupTermination identifies one exact group infrastructure boundary.
type ProposalGroupTermination struct {
	Group       raftmember.GroupKey
	SourceOwner raftmember.AppliedSourceOwner
	SourceToken raftmember.AppliedSourceToken
	Reason      ProposalGroupTerminationReason
}

// ProposalGroupLifecycleSink terminates admitted attempts that no longer have
// a local apply path. It runs synchronously on the serialized Host owner.
type ProposalGroupLifecycleSink func(ProposalGroupTermination)

// ProposalGroupPendingSink reports whether a group retains at least one
// locally admitted tracked attempt. It must be bounded, allocation-free, do no
// external work, and be safe to call on the serialized Host owner.
type ProposalGroupPendingSink func(
	raftmember.AppliedSourceOwner,
	raftmember.AppliedSourceToken,
) bool

// AppliedResultSettlementSink acknowledges one exact Runtime-owned applied
// range. The source owner and capability are borrowed for the call only.
type AppliedResultSettlementSink func(
	raftmember.AppliedSourceOwner,
	raftmember.AppliedSourceToken,
	raftmember.AppliedBatch,
) error

// AppliedSourceClaimSink claims one exact Runtime source before Host ownership
// transfers. A successful claim must return a nonzero registry/epoch token.
type AppliedSourceClaimSink func(
	raftmember.AppliedSourceOwner,
) (raftmember.AppliedSourceToken, error)

// AppliedSourceReleaseSink releases a claim only after the Runtime has closed.
type AppliedSourceReleaseSink func(
	raftmember.AppliedSourceOwner,
	raftmember.AppliedSourceToken,
) error

// ServingSinks is an all-or-nothing serving boundary. Keeping ownership,
// settlement, and proposal lifecycle hooks in one validated value prevents a
// Host from accidentally constructing an epoch-unfenced serving path.
type ServingSinks struct {
	Settle          AppliedResultSettlementSink
	Proposals       ProposalLifecycleSink
	ProposalGroups  ProposalGroupLifecycleSink
	ProposalPending ProposalGroupPendingSink
	ClaimSource     AppliedSourceClaimSink
	ReleaseSource   AppliedSourceReleaseSink
}

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

// Progress describes one completed Ready micro-step, one consumed input, or
// one bounded normal-proposal batch. ProposalCount and ProposalBytes report the
// exact queue prefix consumed by ProgressProposal, including a final rejected
// proposal. They are also retained when a proposal-origin terminal error
// promotes Kind to ProgressFault, and are zero for all other work. When RunOne
// also returns an error, Done is still true if input was consumed or a group
// was newly latched faulted. ReadOutcomes is owned by the caller and present
// only on the corresponding Ready completion step.
type Progress struct {
	Group         raftmember.GroupKey
	Kind          ProgressKind
	ReadyKind     raftmember.DriveKind
	ProposalCount int
	ProposalBytes int64
	AppliedCount  int
	AppliedFirst  uint64
	AppliedLast   uint64
	ReadOutcomes  []raftmodel.ReadOutcome
}

type memberRuntime interface {
	Identity() raftmember.RuntimeIdentity
	Failure() error
	Propose([]byte) error
	ProposeConfChange(pb.ConfChangeI) error
	ReadIndex([]byte) error
	Publication() (raftmodel.Publication, error)
	DurablePromotion(uint64) (raftmember.DurablePromotionProof, bool, error)
	SnapshotState() (replicatedstate.State, error)
	Status() (raftmember.RuntimeStatus, error)
	Progress(uint64) (raftmodel.MemberProgress, bool, error)
	TransferLeader(uint64) error
	StepMessage(*pb.Message) error
	Tick() error
	Campaign() error
	DriveReady(
		*raftmember.ReadyWorkspace,
		func(raftmember.OutboundMessage) error,
		raftmember.ResultSettlementSink,
	) (raftmember.DriveResult, error)
	HasPendingResultSettlement() bool
	Close() error
}

type snapshotBaseRuntime interface {
	SnapshotBaseCertificate() (replicatedstate.SnapshotBaseCertificate, error)
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

type queuedProposal struct {
	data    []byte
	token   ProposalToken
	tracked bool
}

type proposalQueue struct {
	items []queuedProposal
	head  int
}

func (queue *proposalQueue) len() int { return len(queue.items) - queue.head }

func (queue *proposalQueue) push(item queuedProposal) {
	if queue.head != 0 && len(queue.items) == cap(queue.items) {
		queue.compact()
	}
	queue.items = append(queue.items, item)
}

func (queue *proposalQueue) pop() (queuedProposal, bool) {
	if queue.head == len(queue.items) {
		return queuedProposal{}, false
	}
	result := queue.items[queue.head]
	queue.items[queue.head] = queuedProposal{}
	queue.head++
	if queue.head == len(queue.items) {
		queue.items = nil
		queue.head = 0
	} else if queue.head >= 64 && queue.head >= len(queue.items)/2 {
		queue.compact()
	}
	return result, true
}

func (queue *proposalQueue) peek() (queuedProposal, bool) {
	if queue.head == len(queue.items) {
		return queuedProposal{}, false
	}
	return queue.items[queue.head], true
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
	key               raftmember.GroupKey
	memberID          uint64
	sourceOwner       raftmember.AppliedSourceOwner
	sourceToken       raftmember.AppliedSourceToken
	sourceClaimed     bool
	runtime           memberRuntime
	runnable          bool
	messages          messageQueue
	proposals         proposalQueue
	ticks             int
	campaigns         int
	items             int
	bytes             int64
	nextClass         inputClass
	failure           error
	retiring          bool
	trackedLeaderTerm uint64
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
	ready       raftmember.ReadyWorkspace
	readyGroup  *groupState
	readySend   func(raftmember.OutboundMessage) error
	settle      raftmember.ResultSettlementSink
	serving     ServingSinks
	closed      bool
}

// NewHost validates every limit and relationship before allocating the Host.
func NewHost(limits Limits) (*Host, error) {
	return newHost(limits, settleNoLocalWaiters, ServingSinks{})
}

// NewHostWithResultSettlementSink constructs a Host whose published normal
// results are synchronously offered to settle. The sink is called on the Host
// owner goroutine and must follow [raftmember.ResultSettlementSink]. NewHost
// retains the explicit no-local-waiters behavior for non-serving callers.
func NewHostWithResultSettlementSink(
	limits Limits,
	settle raftmember.ResultSettlementSink,
) (*Host, error) {
	if settle == nil {
		return nil, ErrSettlementSinkRequired
	}
	return newHost(limits, settle, ServingSinks{})
}

// NewHostWithServingSinks constructs a Host with result-settlement and
// tracked-proposal lifecycle sinks. Every callback runs on the serialized Host
// owner and must not re-enter Host. Add claims the Runtime's exact applied
// source before ownership transfer; settlement and lifecycle callbacks carry
// that registry/epoch capability until close succeeds and release completes.
func NewHostWithServingSinks(limits Limits, sinks ServingSinks) (*Host, error) {
	if sinks.Settle == nil {
		return nil, ErrSettlementSinkRequired
	}
	if sinks.Proposals == nil {
		return nil, ErrProposalSinkRequired
	}
	if sinks.ProposalGroups == nil {
		return nil, ErrProposalGroupSinkRequired
	}
	if sinks.ProposalPending == nil {
		return nil, ErrProposalPendingRequired
	}
	if sinks.ClaimSource == nil {
		return nil, ErrSourceClaimSinkRequired
	}
	if sinks.ReleaseSource == nil {
		return nil, ErrSourceReleaseSinkRequired
	}
	return newHost(limits, nil, sinks)
}

func newHost(
	limits Limits,
	settle raftmember.ResultSettlementSink,
	serving ServingSinks,
) (*Host, error) {
	if err := validateLimits(limits); err != nil {
		return nil, err
	}
	host := &Host{
		limits: limits,
		groups: make(map[raftmember.GroupKey]*groupState),
		settle: settle, serving: serving,
	}
	if serving.Settle != nil {
		host.settle = host.settleOwnedBatch
	}
	host.readySend = host.enqueueReadyOutbound
	return host, nil
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
	owner := identity.AppliedSourceOwner()
	var sourceToken raftmember.AppliedSourceToken
	if host.serving.ClaimSource != nil {
		if owner.AllocationGeneration == 0 || owner.StoreID == ([16]byte{}) {
			return fmt.Errorf("%w: incomplete serving runtime identity", ErrSourceOwnerMismatch)
		}
		var claimErr error
		sourceToken, claimErr = host.serving.ClaimSource(owner)
		if claimErr != nil {
			return claimErr
		}
		if sourceToken.RegistryID == 0 || sourceToken.OwnerEpoch == 0 {
			releaseErr := host.serving.ReleaseSource(owner, sourceToken)
			if releaseErr != nil {
				host.groups[key] = &groupState{
					key: key, memberID: identity.MemberID, sourceOwner: owner,
					sourceToken: sourceToken, sourceClaimed: true, retiring: true,
				}
				host.order = append(host.order, host.groups[key])
			}
			return errors.Join(ErrSourceOwnerMismatch, releaseErr)
		}
	}
	group := &groupState{
		key: key, memberID: identity.MemberID, runtime: runtime,
		sourceOwner: owner, sourceToken: sourceToken,
		sourceClaimed: host.serving.ClaimSource != nil,
	}
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
	if group.runtime != nil && group.runtime.HasPendingResultSettlement() {
		return errors.Join(ErrGroupBusy, raftmember.ErrResultSettlementPending)
	}
	if !group.retiring {
		if group.runnable || group.items != 0 || group.messages.len() != 0 ||
			group.proposals.len() != 0 || group.ticks != 0 || group.campaigns != 0 ||
			host.hasOutboundFor(key) {
			return ErrGroupBusy
		}
		host.finishTrackedGroup(group, ProposalGroupRemoved)
		group.retiring = true
	}
	if group.runtime != nil {
		if err := group.runtime.Close(); err != nil {
			return err
		}
		group.runtime = nil
	}
	if err := host.releaseGroupSource(group); err != nil {
		return err
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

func (host *Host) releaseGroupSource(group *groupState) error {
	if group == nil || !group.sourceClaimed {
		return nil
	}
	if host.serving.ReleaseSource == nil ||
		group.sourceOwner == (raftmember.AppliedSourceOwner{}) {
		return ErrSourceOwnerMismatch
	}
	if err := host.serving.ReleaseSource(group.sourceOwner, group.sourceToken); err != nil {
		return err
	}
	group.sourceOwner = raftmember.AppliedSourceOwner{}
	group.sourceToken = raftmember.AppliedSourceToken{}
	group.sourceClaimed = false
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
	return host.enqueueProposal(key, data, ProposalToken{}, false)
}

// EnqueueTrackedProposal clones one command and atomically queues its fixed
// lifecycle token. Success transfers ownership of both command and token.
func (host *Host) EnqueueTrackedProposal(
	key raftmember.GroupKey,
	data []byte,
	token ProposalToken,
) error {
	if token == (ProposalToken{}) {
		return ErrProposalToken
	}
	if host == nil || host.serving.Proposals == nil ||
		host.serving.ProposalGroups == nil || host.serving.ProposalPending == nil ||
		host.serving.ClaimSource == nil || host.serving.ReleaseSource == nil {
		return ErrProposalSinkRequired
	}
	return host.enqueueProposal(key, data, token, true)
}

func (host *Host) enqueueProposal(
	key raftmember.GroupKey,
	data []byte,
	token ProposalToken,
	tracked bool,
) error {
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
	group.proposals.push(queuedProposal{
		data: append([]byte(nil), data...), token: token, tracked: tracked,
	})
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

// DurablePromotion returns the exact bounded durable-log witness for an
// unapplied target promotion.
func (host *Host) DurablePromotion(
	key raftmember.GroupKey,
	target uint64,
) (raftmember.DurablePromotionProof, bool, error) {
	group, err := host.lookup(key)
	if err != nil {
		return raftmember.DurablePromotionProof{}, false, err
	}
	return group.runtime.DurablePromotion(target)
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

// SnapshotBaseCertificate returns the exact immutable certificate retained by
// one member's current WAL generation.
func (host *Host) SnapshotBaseCertificate(
	key raftmember.GroupKey,
) (replicatedstate.SnapshotBaseCertificate, error) {
	group, err := host.lookup(key)
	if err != nil {
		return replicatedstate.SnapshotBaseCertificate{}, err
	}
	source, ok := group.runtime.(snapshotBaseRuntime)
	if !ok {
		return replicatedstate.SnapshotBaseCertificate{}, nil
	}
	return source.SnapshotBaseCertificate()
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
	if err == nil {
		host.observeTrackedLeadership(group)
	}
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

// RunOne performs at most one Ready lifecycle operation, consumes one queued
// non-proposal input, or admits one bounded prefix of currently queued normal
// proposals. It never waits for a proposal to join a batch: the group's next
// scheduler turn captures Ready. Only explicitly runnable groups are visited,
// in persistent FIFO round-robin order; idle groups consume no scheduler
// scans. A group blocked by outbox capacity does not prevent another group
// from progressing.
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
		host.readyGroup = group
		ready, err := group.runtime.DriveReady(&host.ready, host.readySend, host.settle)
		host.readyGroup = nil
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
		host.refreshTrackedPending(group)
		host.observeTrackedLeadership(group)
		if ready.Progressed() {
			host.wake(group)
			return Progress{
				Group: group.key, Kind: ProgressReady, ReadyKind: ready.Kind,
				AppliedCount: ready.Applied.Len(), AppliedFirst: ready.Applied.FirstIndex(),
				AppliedLast:  ready.Applied.LastIndex(),
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
			if progress.Kind != ProgressProposal {
				host.observeTrackedLeadership(group)
			}
			host.wake(group)
		}
		return progress, true, inputErr
	}
	if blockedOutbox {
		return Progress{}, false, ErrOutboxFull
	}
	return Progress{}, false, nil
}

// settleNoLocalWaiters is the Host's explicit settlement sink while this
// package remains non-serving. Host exposes no API that can register a local
// command waiter, so validating and acknowledging the whole applied range is
// sufficient. A serving layer must replace this sink with its waiter registry.
func settleNoLocalWaiters(batch raftmember.AppliedBatch) error {
	if batch.Len() <= 0 || batch.FirstIndex() == 0 ||
		batch.LastIndex() < batch.FirstIndex() ||
		batch.FinalPublication().Applied != batch.LastIndex() {
		return errors.New("multiraft: invalid applied result range")
	}
	for index := 0; index < batch.Len(); index++ {
		entry, ok := batch.Entry(index)
		if !ok || entry.Meta.Index != batch.FirstIndex()+uint64(index) {
			return errors.New("multiraft: invalid applied result entry")
		}
	}
	return nil
}

func (host *Host) settleOwnedBatch(batch raftmember.AppliedBatch) error {
	if host == nil {
		return ErrSourceOwnerMismatch
	}
	group := host.readyGroup
	if group == nil || group.runtime == nil || group.retiring ||
		host.serving.Settle == nil || !group.sourceClaimed ||
		group.sourceOwner == (raftmember.AppliedSourceOwner{}) ||
		group.sourceToken.RegistryID == 0 || group.sourceToken.OwnerEpoch == 0 ||
		batch.Source().Owner() != group.sourceOwner || batch.Group() != group.key {
		return ErrSourceOwnerMismatch
	}
	return host.serving.Settle(group.sourceOwner, group.sourceToken, batch)
}

func (host *Host) finishTrackedProposal(
	group *groupState,
	proposal queuedProposal,
	cause error,
) {
	if host == nil || group == nil || !proposal.tracked || host.serving.Proposals == nil {
		return
	}
	host.serving.Proposals(ProposalAdmission{
		Group:       group.key,
		SourceOwner: group.sourceOwner, SourceToken: group.sourceToken,
		Token: proposal.token, Admitted: cause == nil, Cause: cause,
	})
}

func (host *Host) finishTrackedGroup(
	group *groupState,
	reason ProposalGroupTerminationReason,
) {
	if host == nil || group == nil || host.serving.ProposalGroups == nil {
		return
	}
	host.serving.ProposalGroups(ProposalGroupTermination{
		Group:       group.key,
		SourceOwner: group.sourceOwner, SourceToken: group.sourceToken, Reason: reason,
	})
	group.trackedLeaderTerm = 0
}

func (host *Host) refreshTrackedPending(group *groupState) {
	if host == nil || group == nil || group.trackedLeaderTerm == 0 ||
		host.serving.ProposalPending == nil {
		return
	}
	if !host.serving.ProposalPending(group.sourceOwner, group.sourceToken) {
		group.trackedLeaderTerm = 0
	}
}

func (host *Host) observeTrackedLeadership(group *groupState) {
	if host == nil || group == nil || group.runtime == nil ||
		group.trackedLeaderTerm == 0 || host.serving.ProposalGroups == nil {
		return
	}
	status, err := group.runtime.Status()
	if err != nil || status.MemberID != group.memberID ||
		status.LeaderID != group.memberID || status.RaftState != raft.StateLeader ||
		status.Term != group.trackedLeaderTerm {
		host.finishTrackedGroup(group, ProposalGroupLeadershipLost)
	}
}

func (host *Host) preflightTrackedProposal(group *groupState) (uint64, error) {
	if group == nil || group.runtime == nil {
		return 0, ErrGroupNotFound
	}
	status, err := group.runtime.Status()
	if err != nil {
		if group.trackedLeaderTerm != 0 {
			host.finishTrackedGroup(group, ProposalGroupLeadershipLost)
		}
		return 0, err
	}
	if group.trackedLeaderTerm != 0 &&
		(status.MemberID != group.memberID || status.LeaderID != group.memberID ||
			status.RaftState != raft.StateLeader || status.Term != group.trackedLeaderTerm) {
		host.finishTrackedGroup(group, ProposalGroupLeadershipLost)
	}
	if status.MemberID != group.memberID || status.LeaderID != group.memberID ||
		status.RaftState != raft.StateLeader || status.Term == 0 {
		return 0, raftmodel.ErrNotLeader
	}
	return status.Term, nil
}

func (host *Host) purgeGroup(group *groupState) {
	if group == nil {
		return
	}
	for {
		proposal, ok := group.proposals.pop()
		if !ok {
			break
		}
		host.finishTrackedProposal(group, proposal, ErrGroupFaulted)
	}
	host.finishTrackedGroup(group, ProposalGroupFaulted)
	host.queueItems -= group.items
	host.queueBytes -= group.bytes
	group.messages.clear()
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
			item, ok := group.proposals.peek()
			if !ok {
				continue
			}
			group.nextClass = (class + 1) % inputClassCount
			progress.Kind = ProgressProposal
			batchEntries := 0
			batchBytes := int64(0)
			trackedChecked := false
			trackedLeaderTerm := uint64(0)
			var trackedLeadershipErr error
			for batchEntries < raftmodel.MaxProposalBatchEntries {
				dataBytes := int64(len(item.data))
				if batchEntries != 0 && (batchBytes >= raftmodel.MaxProposalBatchBytes ||
					dataBytes > raftmodel.MaxProposalBatchBytes-batchBytes) {
					break
				}
				consumed, popped := group.proposals.pop()
				if !popped {
					break
				}
				host.releaseInput(group, dataBytes)
				batchEntries++
				batchBytes += dataBytes
				progress.ProposalCount = batchEntries
				progress.ProposalBytes = batchBytes
				if consumed.tracked {
					if !trackedChecked {
						trackedChecked = true
						trackedLeaderTerm, trackedLeadershipErr =
							host.preflightTrackedProposal(group)
					}
					if trackedLeadershipErr != nil {
						host.finishTrackedProposal(group, consumed, trackedLeadershipErr)
						if !errors.Is(trackedLeadershipErr, raftmodel.ErrNotLeader) {
							host.refreshTrackedPending(group)
							return progress, true, trackedLeadershipErr
						}
						if batchBytes >= raftmodel.MaxProposalBatchBytes {
							break
						}
						item, ok = group.proposals.peek()
						if !ok {
							break
						}
						continue
					}
				}
				proposalErr := group.runtime.Propose(consumed.data)
				host.finishTrackedProposal(group, consumed, proposalErr)
				if proposalErr != nil {
					host.refreshTrackedPending(group)
					return progress, true, proposalErr
				}
				if consumed.tracked {
					group.trackedLeaderTerm = trackedLeaderTerm
				}
				if batchBytes >= raftmodel.MaxProposalBatchBytes {
					break
				}
				item, ok = group.proposals.peek()
				if !ok {
					break
				}
			}
			host.refreshTrackedPending(group)
			return progress, batchEntries != 0, nil
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

func (host *Host) enqueueReadyOutbound(outbound raftmember.OutboundMessage) error {
	if host == nil || host.readyGroup == nil {
		return errors.New("multiraft: outbound message outside Ready ownership")
	}
	return host.enqueueOutbound(host.readyGroup, outbound)
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
		if group.retiring && group.runtime == nil {
			continue
		}
		group.messages.clear()
		for {
			proposal, ok := group.proposals.pop()
			if !ok {
				break
			}
			host.finishTrackedProposal(group, proposal, ErrHostClosed)
		}
		host.finishTrackedGroup(group, ProposalHostClosed)
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
		if host.hasPendingResultSettlement() {
			return errors.Join(ErrGroupBusy, raftmember.ErrResultSettlementPending)
		}
		host.closed = true
		host.clearQueues()
	}
	var closeErr error
	remaining := false
	for _, group := range host.order {
		if group == nil {
			continue
		}
		if group.runtime != nil {
			if err := group.runtime.Close(); err != nil {
				closeErr = errors.Join(closeErr, err)
				remaining = true
				continue
			}
			group.runtime = nil
		}
		if err := host.releaseGroupSource(group); err != nil {
			closeErr = errors.Join(closeErr, err)
			remaining = true
		}
	}
	if !remaining {
		clear(host.groups)
		clear(host.order)
		host.order = nil
	}
	return closeErr
}

// hasPendingResultSettlement is a read-only close preflight. Callers must own
// the Host. It intentionally does not inspect ordinary queued input: Close may
// terminate inputs, but it may never discard a committed result awaiting its
// exact settlement retry.
func (host *Host) hasPendingResultSettlement() bool {
	if host == nil {
		return false
	}
	for _, group := range host.order {
		if group != nil && group.runtime != nil && group.runtime.HasPendingResultSettlement() {
			return true
		}
	}
	return false
}
