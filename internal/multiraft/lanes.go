package multiraft

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	pb "go.etcd.io/raft/v3/raftpb"
)

const AbsoluteMaxExecutionLanes = 64

var (
	ErrInvalidExecutionLanes = errors.New("multiraft: invalid execution lane count")
	ErrExecutionLane         = errors.New("multiraft: invalid execution lane")
)

// laneCounters occupies one cache line. executionLane has a 128-byte stride,
// so independently updated lanes never place their counters in the same line.
type laneCounters struct {
	calls      uint64
	rejected   uint64
	progressed uint64
	failures   uint64
	_          [32]byte
}

type executionLane struct {
	mu       sync.Mutex
	host     *Host
	counters laneCounters
	_        [48]byte
}

// ExecutionLanes routes each group to one immutable single-owner Host lane.
// Different lanes may execute concurrently; operations for one group remain
// serialized by its lane and retain Host's exact Raft/Ready ordering.
type ExecutionLanes struct {
	closeMu sync.Mutex
	closed  atomic.Bool
	lanes   []executionLane
}

// ExecutionLaneStats is an allocation-free snapshot when supplied through
// StatsInto with sufficient destination capacity. Queue and outbox bytes are
// exact retained payload charges; structural Host/runtime memory is excluded.
type ExecutionLaneStats struct {
	Lane        int
	Groups      int
	Runnable    int
	QueueItems  int
	QueueBytes  int64
	OutboxItems int
	OutboxBytes int64
	Calls       uint64
	Rejected    uint64
	Progressed  uint64
	Failures    uint64
}

// NewExecutionLanes constructs non-serving lanes with identical independent
// per-lane limits. Count must be a power of two so routing is one stable mask.
func NewExecutionLanes(count int, limits Limits) (*ExecutionLanes, error) {
	return newExecutionLanes(count, func() (*Host, error) { return NewHost(limits) })
}

// NewExecutionLanesWithResultSettlementSink constructs lanes sharing one
// concurrency-safe result sink. The sink may be invoked concurrently by
// different lanes and must not re-enter ExecutionLanes.
func NewExecutionLanesWithResultSettlementSink(
	count int,
	limits Limits,
	settle raftmember.ResultSettlementSink,
) (*ExecutionLanes, error) {
	if settle == nil {
		return nil, ErrSettlementSinkRequired
	}
	return newExecutionLanes(count, func() (*Host, error) {
		return NewHostWithResultSettlementSink(limits, settle)
	})
}

// NewExecutionLanesWithServingSinks constructs serving lanes sharing one set
// of concurrency-safe lifecycle sinks. Callbacks may run concurrently across
// lanes and must not re-enter ExecutionLanes.
func NewExecutionLanesWithServingSinks(
	count int,
	limits Limits,
	sinks ServingSinks,
) (*ExecutionLanes, error) {
	return newExecutionLanes(count, func() (*Host, error) {
		return NewHostWithServingSinks(limits, sinks)
	})
}

func newExecutionLanes(count int, build func() (*Host, error)) (*ExecutionLanes, error) {
	if count <= 0 || count > AbsoluteMaxExecutionLanes || count&(count-1) != 0 || build == nil {
		return nil, ErrInvalidExecutionLanes
	}
	set := &ExecutionLanes{lanes: make([]executionLane, count)}
	for index := range set.lanes {
		host, err := build()
		if err != nil {
			for prior := 0; prior < index; prior++ {
				_ = set.lanes[prior].host.Close()
			}
			return nil, err
		}
		set.lanes[index].host = host
	}
	return set, nil
}

// Lane returns the deterministic immutable lane for key.
func (set *ExecutionLanes) Lane(key raftmember.GroupKey) (int, error) {
	if set == nil || len(set.lanes) == 0 || key == (raftmember.GroupKey{}) {
		return 0, ErrExecutionLane
	}
	return int(groupLaneHash(key) & uint64(len(set.lanes)-1)), nil
}

func groupLaneHash(key raftmember.GroupKey) uint64 {
	const offset = uint64(1469598103934665603)
	const prime = uint64(1099511628211)
	hash := offset
	mix := func(bytes *[16]byte) {
		for _, value := range bytes {
			hash ^= uint64(value)
			hash *= prime
		}
	}
	mix(&key.ClusterID)
	mix(&key.ClusterIncarnation)
	for shift := uint(0); shift < 64; shift += 8 {
		hash ^= uint64(byte(key.TopologyRecoveryEpoch >> shift))
		hash *= prime
	}
	mix(&key.ShardIncarnation)
	mix(&key.GroupID)
	return hash
}

func (set *ExecutionLanes) laneFor(key raftmember.GroupKey) (*executionLane, error) {
	index, err := set.Lane(key)
	if err != nil {
		return nil, err
	}
	if set.closed.Load() {
		return nil, ErrHostClosed
	}
	return &set.lanes[index], nil
}

func (set *ExecutionLanes) addRuntime(runtime memberRuntime) error {
	if runtime == nil {
		return ErrGroupNotFound
	}
	lane, err := set.laneFor(runtime.Identity().Group)
	if err != nil {
		return err
	}
	lane.mu.Lock()
	defer lane.mu.Unlock()
	lane.counters.calls++
	if set.closed.Load() {
		lane.counters.rejected++
		return ErrHostClosed
	}
	err = lane.host.addRuntime(runtime)
	if err != nil {
		lane.counters.rejected++
	}
	return err
}

// Add transfers runtime ownership to its deterministic lane only on success.
func (set *ExecutionLanes) Add(runtime *raftmember.Runtime) error {
	if runtime == nil {
		return ErrGroupNotFound
	}
	return set.addRuntime(runtime)
}

func (set *ExecutionLanes) withGroup(
	key raftmember.GroupKey,
	operation func(*Host) error,
) error {
	lane, err := set.laneFor(key)
	if err != nil {
		return err
	}
	lane.mu.Lock()
	defer lane.mu.Unlock()
	lane.counters.calls++
	if set.closed.Load() {
		lane.counters.rejected++
		return ErrHostClosed
	}
	err = operation(lane.host)
	if err != nil {
		lane.counters.rejected++
	}
	return err
}

func (set *ExecutionLanes) Remove(key raftmember.GroupKey) error {
	return set.withGroup(key, func(host *Host) error { return host.Remove(key) })
}

func (set *ExecutionLanes) EnqueueMessage(key raftmember.GroupKey, message *pb.Message) error {
	return set.withGroup(key, func(host *Host) error { return host.EnqueueMessage(key, message) })
}

func (set *ExecutionLanes) AdoptMessage(key raftmember.GroupKey, message *pb.Message) error {
	return set.withGroup(key, func(host *Host) error { return host.AdoptMessage(key, message) })
}

func (set *ExecutionLanes) EnqueueProposal(key raftmember.GroupKey, data []byte) error {
	return set.withGroup(key, func(host *Host) error { return host.EnqueueProposal(key, data) })
}

func (set *ExecutionLanes) EnqueueTrackedProposal(
	key raftmember.GroupKey,
	data []byte,
	token ProposalToken,
) error {
	return set.withGroup(key, func(host *Host) error {
		return host.EnqueueTrackedProposal(key, data, token)
	})
}

func (set *ExecutionLanes) ProposeConfChange(key raftmember.GroupKey, change pb.ConfChangeI) error {
	return set.withGroup(key, func(host *Host) error { return host.ProposeConfChange(key, change) })
}

func (set *ExecutionLanes) ReadIndex(key raftmember.GroupKey, context []byte) error {
	return set.withGroup(key, func(host *Host) error { return host.ReadIndex(key, context) })
}

func (set *ExecutionLanes) TransferLeader(key raftmember.GroupKey, transferee uint64) error {
	return set.withGroup(key, func(host *Host) error { return host.TransferLeader(key, transferee) })
}

func (set *ExecutionLanes) RequestTick(key raftmember.GroupKey) error {
	return set.withGroup(key, func(host *Host) error { return host.RequestTick(key) })
}

func (set *ExecutionLanes) RequestCampaign(key raftmember.GroupKey) error {
	return set.withGroup(key, func(host *Host) error { return host.RequestCampaign(key) })
}

func (set *ExecutionLanes) Publication(key raftmember.GroupKey) (raftmodel.Publication, error) {
	lane, err := set.laneFor(key)
	if err != nil {
		return raftmodel.Publication{}, err
	}
	lane.mu.Lock()
	defer lane.mu.Unlock()
	lane.counters.calls++
	if set.closed.Load() {
		lane.counters.rejected++
		return raftmodel.Publication{}, ErrHostClosed
	}
	result, err := lane.host.Publication(key)
	if err != nil {
		lane.counters.rejected++
	}
	return result, err
}

func (set *ExecutionLanes) SnapshotState(key raftmember.GroupKey) (replicatedstate.State, error) {
	lane, err := set.laneFor(key)
	if err != nil {
		return replicatedstate.State{}, err
	}
	lane.mu.Lock()
	defer lane.mu.Unlock()
	lane.counters.calls++
	if set.closed.Load() {
		lane.counters.rejected++
		return replicatedstate.State{}, ErrHostClosed
	}
	result, err := lane.host.SnapshotState(key)
	if err != nil {
		lane.counters.rejected++
	}
	return result, err
}

func (set *ExecutionLanes) Status(key raftmember.GroupKey) (raftmember.RuntimeStatus, error) {
	lane, err := set.laneFor(key)
	if err != nil {
		return raftmember.RuntimeStatus{}, err
	}
	lane.mu.Lock()
	defer lane.mu.Unlock()
	lane.counters.calls++
	if set.closed.Load() {
		lane.counters.rejected++
		return raftmember.RuntimeStatus{}, ErrHostClosed
	}
	result, err := lane.host.Status(key)
	if err != nil {
		lane.counters.rejected++
	}
	return result, err
}

func (set *ExecutionLanes) Progress(
	key raftmember.GroupKey,
	memberID uint64,
) (raftmodel.MemberProgress, bool, error) {
	lane, err := set.laneFor(key)
	if err != nil {
		return raftmodel.MemberProgress{}, false, err
	}
	lane.mu.Lock()
	defer lane.mu.Unlock()
	lane.counters.calls++
	if set.closed.Load() {
		lane.counters.rejected++
		return raftmodel.MemberProgress{}, false, ErrHostClosed
	}
	result, found, err := lane.host.Progress(key, memberID)
	if err != nil {
		lane.counters.rejected++
	}
	return result, found, err
}

// RunOne executes one bounded Host turn on lane. Callers may dedicate one
// goroutine per lane or invoke distinct lane indices concurrently.
func (set *ExecutionLanes) RunOne(index int) (Progress, bool, error) {
	if set == nil || index < 0 || index >= len(set.lanes) {
		return Progress{}, false, ErrExecutionLane
	}
	lane := &set.lanes[index]
	lane.mu.Lock()
	defer lane.mu.Unlock()
	lane.counters.calls++
	if set.closed.Load() {
		lane.counters.rejected++
		return Progress{}, false, ErrHostClosed
	}
	progress, done, err := lane.host.RunOne()
	if done {
		lane.counters.progressed++
	}
	if err != nil {
		lane.counters.failures++
	}
	return progress, done, err
}

// PopOutbound transfers one message from a specific lane. Per-lane outboxes
// intentionally avoid a cross-core global queue and its head-of-line blocking.
func (set *ExecutionLanes) PopOutbound(index int) (raftmember.OutboundMessage, bool, error) {
	if set == nil || index < 0 || index >= len(set.lanes) {
		return raftmember.OutboundMessage{}, false, ErrExecutionLane
	}
	lane := &set.lanes[index]
	lane.mu.Lock()
	defer lane.mu.Unlock()
	lane.counters.calls++
	if set.closed.Load() {
		lane.counters.rejected++
		return raftmember.OutboundMessage{}, false, ErrHostClosed
	}
	message, ok := lane.host.PopOutbound()
	return message, ok, nil
}

// StatsInto appends one exact snapshot per lane in lane order.
func (set *ExecutionLanes) StatsInto(dst []ExecutionLaneStats) []ExecutionLaneStats {
	if set == nil {
		return dst
	}
	for index := range set.lanes {
		lane := &set.lanes[index]
		lane.mu.Lock()
		host := lane.host
		stats := ExecutionLaneStats{Lane: index,
			Calls: lane.counters.calls, Rejected: lane.counters.rejected,
			Progressed: lane.counters.progressed, Failures: lane.counters.failures}
		if host != nil {
			stats.Groups, stats.Runnable = len(host.groups), host.runnableLen()
			stats.QueueItems, stats.QueueBytes = host.queueItems, host.queueBytes
			stats.OutboxItems, stats.OutboxBytes = host.outbox.len(), host.outboxBytes
		}
		lane.mu.Unlock()
		dst = append(dst, stats)
	}
	return dst
}

// Close atomically stops new admission, then closes every lane in lane order.
// A failed underlying Runtime close remains owned and is retried by a later
// Close call; concurrent operations either finish before their lane closes or
// observe ErrHostClosed after acquiring that lane.
func (set *ExecutionLanes) Close() error {
	if set == nil {
		return nil
	}
	set.closeMu.Lock()
	defer set.closeMu.Unlock()
	set.closed.Store(true)
	var joined error
	for index := range set.lanes {
		lane := &set.lanes[index]
		lane.mu.Lock()
		if lane.host != nil {
			joined = errors.Join(joined, lane.host.Close())
		}
		lane.mu.Unlock()
	}
	return joined
}
