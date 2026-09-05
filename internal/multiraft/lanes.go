package multiraft

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/raftauthority"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
)

const AbsoluteMaxExecutionLanes = 64

const (
	executionLanesOpen uint32 = iota
	executionLanesClosing
	executionLanesClosed
)

var (
	ErrInvalidExecutionLanes = errors.New("multiraft: invalid execution lane count")
	ErrExecutionLane         = errors.New("multiraft: invalid execution lane")
)

// laneCounters fits inside executionLane's 256-byte stride. On supported
// amd64/arm64 targets with cache lines no larger than 128 bytes, the 192-byte
// gap between adjacent counter regions prevents false sharing even when the
// backing allocation itself is not cache-line aligned.
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
	_        [176]byte
}

// ExecutionLanes routes each group to one immutable single-owner Host lane.
// Different lanes may execute concurrently; operations for one group remain
// serialized by its lane and retain Host's exact Raft/Ready ordering. Limits
// are per lane: worst-case aggregate retained input is
// lane-count*MaxQueueBytes and aggregate retained outbox data is
// lane-count*MaxOutboxBytes, in addition to bounded per-lane structure/runtime
// ownership.
type ExecutionLanes struct {
	closeMu sync.Mutex
	state   atomic.Uint32
	lanes   []executionLane
}

// Count returns the immutable number of execution lanes.
func (set *ExecutionLanes) Count() int {
	if set == nil {
		return 0
	}
	return len(set.lanes)
}

// ExecutionLane is a narrow single-owner view of one lane. Group-addressed
// methods reject keys assigned to another lane, so ingress cannot accidentally
// cross owner loops. Close retires only this lane; ExecutionLanes.Close remains
// the aggregate shutdown and settlement boundary.
type ExecutionLane struct {
	set   *ExecutionLanes
	index int
}

// OwnerLane returns the single-owner capability for index.
func (set *ExecutionLanes) OwnerLane(index int) (*ExecutionLane, error) {
	if set == nil || index < 0 || index >= len(set.lanes) {
		return nil, ErrExecutionLane
	}
	return &ExecutionLane{set: set, index: index}, nil
}

func (lane *ExecutionLane) accepts(key raftmember.GroupKey) error {
	if lane == nil || lane.set == nil {
		return ErrExecutionLane
	}
	index, err := lane.set.Lane(key)
	if err != nil || index != lane.index {
		return ErrExecutionLane
	}
	return nil
}

func (lane *ExecutionLane) AdoptMessage(key raftmember.GroupKey, message *pb.Message) error {
	if err := lane.accepts(key); err != nil {
		return err
	}
	return lane.set.AdoptMessage(key, message)
}

func (lane *ExecutionLane) AdoptAuthorityMessage(
	key raftmember.GroupKey,
	message *raftauthority.Message,
) error {
	if err := lane.accepts(key); err != nil {
		return err
	}
	return lane.set.AdoptAuthorityMessage(key, message)
}

func (lane *ExecutionLane) AdoptAuthenticatedAuthorityMessage(
	key raftmember.GroupKey,
	source uint64,
	message *raftauthority.Message,
) error {
	if err := lane.accepts(key); err != nil {
		return err
	}
	return lane.set.AdoptAuthenticatedAuthorityMessage(key, source, message)
}

func (lane *ExecutionLane) StartReadAuthorityRound(key raftmember.GroupKey) error {
	if err := lane.accepts(key); err != nil {
		return err
	}
	return lane.set.StartReadAuthorityRound(key)
}

func (lane *ExecutionLane) ReadAuthorityToken(
	key raftmember.GroupKey,
) (raftauthority.AuthorityToken, error) {
	if err := lane.accepts(key); err != nil {
		return raftauthority.AuthorityToken{}, err
	}
	return lane.set.ReadAuthorityToken(key)
}

func (lane *ExecutionLane) ValidateReadAuthorityToken(
	key raftmember.GroupKey, token raftauthority.AuthorityToken,
) error {
	if err := lane.accepts(key); err != nil {
		return err
	}
	return lane.set.ValidateReadAuthorityToken(key, token)
}
func (lane *ExecutionLane) EnqueueTrackedProposal(key raftmember.GroupKey, data []byte, token ProposalToken) error {
	if err := lane.accepts(key); err != nil {
		return err
	}
	return lane.set.EnqueueTrackedProposal(key, data, token)
}
func (lane *ExecutionLane) EnqueueProposal(key raftmember.GroupKey, data []byte) error {
	if err := lane.accepts(key); err != nil {
		return err
	}
	return lane.set.EnqueueProposal(key, data)
}
func (lane *ExecutionLane) ProposeConfChange(key raftmember.GroupKey, change pb.ConfChangeI) error {
	if err := lane.accepts(key); err != nil {
		return err
	}
	return lane.set.ProposeConfChange(key, change)
}
func (lane *ExecutionLane) ReadIndex(key raftmember.GroupKey, context []byte) error {
	if err := lane.accepts(key); err != nil {
		return err
	}
	return lane.set.ReadIndex(key, context)
}
func (lane *ExecutionLane) TransferLeader(key raftmember.GroupKey, transferee uint64) error {
	if err := lane.accepts(key); err != nil {
		return err
	}
	return lane.set.TransferLeader(key, transferee)
}
func (lane *ExecutionLane) RequestTick(key raftmember.GroupKey) error {
	if err := lane.accepts(key); err != nil {
		return err
	}
	return lane.set.RequestTick(key)
}
func (lane *ExecutionLane) RequestCampaign(key raftmember.GroupKey) error {
	if err := lane.accepts(key); err != nil {
		return err
	}
	return lane.set.RequestCampaign(key)
}
func (lane *ExecutionLane) Publication(key raftmember.GroupKey) (raftmodel.Publication, error) {
	if err := lane.accepts(key); err != nil {
		return raftmodel.Publication{}, err
	}
	return lane.set.Publication(key)
}
func (lane *ExecutionLane) Status(key raftmember.GroupKey) (raftmember.RuntimeStatus, error) {
	if err := lane.accepts(key); err != nil {
		return raftmember.RuntimeStatus{}, err
	}
	return lane.set.Status(key)
}
func (lane *ExecutionLane) Progress(key raftmember.GroupKey, memberID uint64) (raftmodel.MemberProgress, bool, error) {
	if err := lane.accepts(key); err != nil {
		return raftmodel.MemberProgress{}, false, err
	}
	return lane.set.Progress(key, memberID)
}
func (lane *ExecutionLane) DurablePromotion(key raftmember.GroupKey, memberID uint64) (raftmember.DurablePromotionProof, bool, error) {
	if err := lane.accepts(key); err != nil {
		return raftmember.DurablePromotionProof{}, false, err
	}
	return lane.set.DurablePromotion(key, memberID)
}
func (lane *ExecutionLane) SnapshotState(key raftmember.GroupKey) (replicatedstate.State, error) {
	if err := lane.accepts(key); err != nil {
		return replicatedstate.State{}, err
	}
	return lane.set.SnapshotState(key)
}
func (lane *ExecutionLane) SnapshotAuthorizationFence(key raftmember.GroupKey) (replicatedstate.SnapshotFence, error) {
	if err := lane.accepts(key); err != nil {
		return replicatedstate.SnapshotFence{}, err
	}
	return lane.set.SnapshotAuthorizationFence(key)
}
func (lane *ExecutionLane) SnapshotBaseCertificate(key raftmember.GroupKey) (replicatedstate.SnapshotBaseCertificate, error) {
	if err := lane.accepts(key); err != nil {
		return replicatedstate.SnapshotBaseCertificate{}, err
	}
	return lane.set.SnapshotBaseCertificate(key)
}
func (lane *ExecutionLane) Remove(key raftmember.GroupKey) error {
	if err := lane.accepts(key); err != nil {
		return err
	}
	return lane.set.Remove(key)
}
func (lane *ExecutionLane) QuiesceSQLGeneration(key raftmember.GroupKey) error {
	if err := lane.accepts(key); err != nil {
		return err
	}
	return lane.set.QuiesceSQLGeneration(key)
}
func (lane *ExecutionLane) FenceCommittedSchemaGeneration(key raftmember.GroupKey) error {
	if err := lane.accepts(key); err != nil {
		return err
	}
	return lane.set.FenceCommittedSchemaGeneration(key)
}
func (lane *ExecutionLane) ObserveSchemaTransition(
	key raftmember.GroupKey, command []byte,
) (uint64, bool, error) {
	if err := lane.accepts(key); err != nil {
		return 0, false, err
	}
	return lane.set.ObserveSchemaTransition(key, command)
}
func (lane *ExecutionLane) InstallSQLGeneration(
	key raftmember.GroupKey, database *sqldriver.Database, apply *sqldriver.ReplicatedApply,
	expectedSQL sqldriver.ReplicatedShardStoreIdentity,
	expectedApply sqldriver.ReplicatedApplyIdentity,
) error {
	if err := lane.accepts(key); err != nil {
		return err
	}
	return lane.set.InstallSQLGeneration(key, database, apply, expectedSQL, expectedApply)
}

// Add transfers a Runtime to this exact deterministic lane. It exists for
// serialized live group adoption; callers cannot use a lane handle to place a
// group on a different owner.
func (lane *ExecutionLane) Add(runtime *raftmember.Runtime) error {
	if lane == nil || lane.set == nil || runtime == nil {
		return ErrGroupNotFound
	}
	index, err := lane.set.Lane(runtime.Identity().Group)
	if err != nil || index != lane.index {
		return errors.Join(ErrGroupNotFound, err)
	}
	return lane.set.Add(runtime)
}
func (lane *ExecutionLane) RunOne() (Progress, bool, error) {
	if lane == nil || lane.set == nil {
		return Progress{}, false, ErrExecutionLane
	}
	return lane.set.RunOne(lane.index)
}

// AsyncNotify exposes this lane's append-completion edge to its sole Owner.
// Each ExecutionLane wraps exactly one Host, so forwarding the Host channel
// preserves the same coalesced SPSC wakeup used by a standalone Host.
func (lane *ExecutionLane) AsyncNotify() <-chan struct{} {
	if lane == nil || lane.set == nil || lane.index < 0 || lane.index >= len(lane.set.lanes) {
		return nil
	}
	return lane.set.lanes[lane.index].host.AsyncNotify()
}

// WakePipelined drains this lane's completed groups after an append worker edge.
// The Owner is the serialized caller, but retain the lane lock so the wrapper
// has the same synchronization contract as every other ExecutionLane method.
func (lane *ExecutionLane) WakePipelined() {
	if lane == nil || lane.set == nil || lane.index < 0 || lane.index >= len(lane.set.lanes) {
		return
	}
	entry := &lane.set.lanes[lane.index]
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if lane.set.state.Load() == executionLanesOpen {
		entry.host.WakePipelined()
	}
}

func (lane *ExecutionLane) PopOutbound() (raftmember.OutboundMessage, bool) {
	if lane == nil || lane.set == nil {
		return raftmember.OutboundMessage{}, false
	}
	message, ok, _ := lane.set.PopOutbound(lane.index)
	return message, ok
}
func (lane *ExecutionLane) Close() error {
	if lane == nil || lane.set == nil {
		return nil
	}
	l := &lane.set.lanes[lane.index]
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.host.Close()
}

// ExecutionLaneStats is an allocation-free snapshot when supplied through
// StatsInto with sufficient destination capacity. Queue and outbox bytes are
// exact retained payload charges; structural Host/runtime memory is excluded.
// A concurrent StatsInto is a per-lane exact vector cut in lane order, not one
// globally atomic instant: work may progress on lanes not currently locked.
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
// Every lane independently enforces MaxGroupBytes and aggregate MaxQueueBytes.
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
	if set.state.Load() != executionLanesOpen {
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
	if set.state.Load() != executionLanesOpen {
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
	if set.state.Load() != executionLanesOpen {
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

func (set *ExecutionLanes) QuiesceSQLGeneration(key raftmember.GroupKey) error {
	lane, err := set.laneFor(key)
	if err != nil {
		return err
	}
	return lane.host.QuiesceSQLGeneration(key)
}
func (set *ExecutionLanes) FenceCommittedSchemaGeneration(key raftmember.GroupKey) error {
	lane, err := set.laneFor(key)
	if err != nil {
		return err
	}
	return lane.host.FenceCommittedSchemaGeneration(key)
}

func (set *ExecutionLanes) ObserveSchemaTransition(
	key raftmember.GroupKey, command []byte,
) (uint64, bool, error) {
	lane, err := set.laneFor(key)
	if err != nil {
		return 0, false, err
	}
	return lane.host.ObserveSchemaTransition(key, command)
}

func (set *ExecutionLanes) InstallSQLGeneration(
	key raftmember.GroupKey, database *sqldriver.Database, apply *sqldriver.ReplicatedApply,
	expectedSQL sqldriver.ReplicatedShardStoreIdentity,
	expectedApply sqldriver.ReplicatedApplyIdentity,
) error {
	lane, err := set.laneFor(key)
	if err != nil {
		return err
	}
	return lane.host.InstallSQLGeneration(key, database, apply, expectedSQL, expectedApply)
}

func (set *ExecutionLanes) EnqueueMessage(key raftmember.GroupKey, message *pb.Message) error {
	return set.withGroup(key, func(host *Host) error { return host.EnqueueMessage(key, message) })
}

func (set *ExecutionLanes) AdoptMessage(key raftmember.GroupKey, message *pb.Message) error {
	return set.withGroup(key, func(host *Host) error { return host.AdoptMessage(key, message) })
}

func (set *ExecutionLanes) AdoptAuthorityMessage(
	key raftmember.GroupKey,
	message *raftauthority.Message,
) error {
	return set.withGroup(key, func(host *Host) error {
		return host.AdoptAuthorityMessage(key, message)
	})
}

func (set *ExecutionLanes) AdoptAuthenticatedAuthorityMessage(
	key raftmember.GroupKey,
	source uint64,
	message *raftauthority.Message,
) error {
	return set.withGroup(key, func(host *Host) error {
		return host.AdoptAuthenticatedAuthorityMessage(key, source, message)
	})
}

func (set *ExecutionLanes) StartReadAuthorityRound(key raftmember.GroupKey) error {
	return set.withGroup(key, func(host *Host) error {
		return host.StartReadAuthorityRound(key)
	})
}

func (set *ExecutionLanes) ReadAuthorityToken(
	key raftmember.GroupKey,
) (raftauthority.AuthorityToken, error) {
	lane, err := set.laneFor(key)
	if err != nil {
		return raftauthority.AuthorityToken{}, err
	}
	lane.mu.Lock()
	defer lane.mu.Unlock()
	lane.counters.calls++
	if set.state.Load() != executionLanesOpen {
		lane.counters.rejected++
		return raftauthority.AuthorityToken{}, ErrHostClosed
	}
	token, err := lane.host.ReadAuthorityToken(key)
	if err != nil {
		lane.counters.rejected++
	}
	return token, err
}

func (set *ExecutionLanes) ValidateReadAuthorityToken(
	key raftmember.GroupKey, token raftauthority.AuthorityToken,
) error {
	lane, err := set.laneFor(key)
	if err != nil {
		return err
	}
	lane.mu.Lock()
	defer lane.mu.Unlock()
	lane.counters.calls++
	if set.state.Load() != executionLanesOpen {
		lane.counters.rejected++
		return ErrHostClosed
	}
	err = lane.host.ValidateReadAuthorityToken(key, token)
	if err != nil {
		lane.counters.rejected++
	}
	return err
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
	if set.state.Load() != executionLanesOpen {
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
	if set.state.Load() != executionLanesOpen {
		lane.counters.rejected++
		return replicatedstate.State{}, ErrHostClosed
	}
	result, err := lane.host.SnapshotState(key)
	if err != nil {
		lane.counters.rejected++
	}
	return result, err
}

func (set *ExecutionLanes) SnapshotAuthorizationFence(
	key raftmember.GroupKey,
) (replicatedstate.SnapshotFence, error) {
	lane, err := set.laneFor(key)
	if err != nil {
		return replicatedstate.SnapshotFence{}, err
	}
	lane.mu.Lock()
	defer lane.mu.Unlock()
	lane.counters.calls++
	if set.state.Load() != executionLanesOpen {
		lane.counters.rejected++
		return replicatedstate.SnapshotFence{}, ErrHostClosed
	}
	result, err := lane.host.SnapshotAuthorizationFence(key)
	if err != nil {
		lane.counters.rejected++
	}
	return result, err
}

func (set *ExecutionLanes) SnapshotBaseCertificate(
	key raftmember.GroupKey,
) (replicatedstate.SnapshotBaseCertificate, error) {
	lane, err := set.laneFor(key)
	if err != nil {
		return replicatedstate.SnapshotBaseCertificate{}, err
	}
	lane.mu.Lock()
	defer lane.mu.Unlock()
	lane.counters.calls++
	if set.state.Load() != executionLanesOpen {
		lane.counters.rejected++
		return replicatedstate.SnapshotBaseCertificate{}, ErrHostClosed
	}
	result, err := lane.host.SnapshotBaseCertificate(key)
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
	if set.state.Load() != executionLanesOpen {
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
	if set.state.Load() != executionLanesOpen {
		lane.counters.rejected++
		return raftmodel.MemberProgress{}, false, ErrHostClosed
	}
	result, found, err := lane.host.Progress(key, memberID)
	if err != nil {
		lane.counters.rejected++
	}
	return result, found, err
}

func (set *ExecutionLanes) DurablePromotion(
	key raftmember.GroupKey,
	memberID uint64,
) (raftmember.DurablePromotionProof, bool, error) {
	lane, err := set.laneFor(key)
	if err != nil {
		return raftmember.DurablePromotionProof{}, false, err
	}
	lane.mu.Lock()
	defer lane.mu.Unlock()
	lane.counters.calls++
	if set.state.Load() != executionLanesOpen {
		lane.counters.rejected++
		return raftmember.DurablePromotionProof{}, false, ErrHostClosed
	}
	result, found, err := lane.host.DurablePromotion(key, memberID)
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
	if set.state.Load() != executionLanesOpen {
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
	if set.state.Load() != executionLanesOpen {
		lane.counters.rejected++
		return raftmember.OutboundMessage{}, false, ErrHostClosed
	}
	message, ok := lane.host.PopOutbound()
	return message, ok, nil
}

// StatsInto appends one exact snapshot per lane in lane order. Under concurrent
// work the returned slice is an exact per-lane vector cut, not a globally
// atomic snapshot across lanes. Reusing capacity for every lane allocates no
// memory.
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

// Close atomically stops new admission and locks every lane in ascending order.
// Its first close attempt performs a read-only all-lane settlement preflight;
// any pending settlement restores the open state with no Runtime, queue, or
// lane mutation so RunOne can retry it. After a successful preflight, queues
// and Runtimes close in lane order. A failed Runtime close leaves the set in
// closing state, remains owned, and is retried by a later Close call.
func (set *ExecutionLanes) Close() error {
	if set == nil {
		return nil
	}
	set.closeMu.Lock()
	defer set.closeMu.Unlock()
	state := set.state.Load()
	if state == executionLanesClosed {
		return nil
	}
	firstAttempt := state == executionLanesOpen
	if firstAttempt {
		set.state.Store(executionLanesClosing)
	}
	for index := range set.lanes {
		set.lanes[index].mu.Lock()
	}
	defer func() {
		for index := len(set.lanes) - 1; index >= 0; index-- {
			set.lanes[index].mu.Unlock()
		}
	}()
	if firstAttempt {
		for index := range set.lanes {
			host := set.lanes[index].host
			if host != nil && host.hasPendingResultSettlement() {
				set.state.Store(executionLanesOpen)
				return errors.Join(ErrGroupBusy, raftmember.ErrResultSettlementPending)
			}
		}
	}
	var joined error
	for index := range set.lanes {
		lane := &set.lanes[index]
		if lane.host != nil {
			joined = errors.Join(joined, lane.host.Close())
		}
	}
	if joined == nil {
		set.state.Store(executionLanesClosed)
	}
	return joined
}
