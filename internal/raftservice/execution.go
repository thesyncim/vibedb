package raftservice

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

var ErrExecutionGroup = errors.New("raftservice: group is not assigned to an execution lane owner")

const (
	executionOwnersReady uint32 = iota
	executionOwnersRunning
	executionOwnersClosed
)

// ExecutionOptions configures one Owner loop for every lane in Lanes. Member
// metadata is split by the deterministic ExecutionLanes hash; callers never
// choose a lane at an ingress boundary.
type ExecutionOptions struct {
	Registry                   *raftserve.Registry
	Lanes                      *multiraft.ExecutionLanes
	Members                    []raftmember.RuntimeIdentity
	CommandFences              []CommandFence
	ReadSources                []ReadSource
	TransactionRecoverySources []TransactionRecoverySource
	MembershipAuthority        MembershipAuthority
	Outbound                   OutboundSink
	Pulse                      <-chan struct{}
	Limits                     Limits
	ProgressMetrics            *ProgressMetrics
}

// ExecutionOwners is the group-keyed serving capability over all execution
// lanes. It owns no transport: one shared transport can be installed as its
// Outbound sink and its HandleInbound method can serve one shared receiver.
type ExecutionOwners struct {
	lanes   *multiraft.ExecutionLanes
	owners  []*Owner
	metrics *ProgressMetrics
	byGroup atomic.Pointer[executionOwnerGroups]
	pulse   <-chan struct{}
	ticks   []chan struct{}

	state   atomic.Uint32
	started chan struct{}
	done    chan struct{}
}

type executionOwnerGroups struct {
	values map[raftmember.GroupKey]executionOwnerRoute
}

type executionOwnerRoute struct {
	owner *Owner
	ready *atomic.Bool
}

// ExecutionGroup is the complete serving metadata installed with one adopted
// Runtime. Runtime ownership transfers only when the serialized lane owner
// has published every field and invoked the supplied transport commit.
type ExecutionGroup struct {
	Runtime  *raftmember.Runtime
	Identity raftmember.RuntimeIdentity
	Command  CommandFence
	Read     ReadSource
	Recovery TransactionRecoverySource
}

func NewExecutionOwners(options ExecutionOptions) (*ExecutionOwners, error) {
	if options.Lanes == nil || options.Registry == nil || options.Lanes.Count() == 0 ||
		len(options.Members) == 0 || len(options.CommandFences) != len(options.Members) ||
		(len(options.ReadSources) != 0 && len(options.ReadSources) != len(options.Members)) ||
		(len(options.TransactionRecoverySources) != 0 && len(options.TransactionRecoverySources) != len(options.Members)) {
		return nil, ErrInvalidOwner
	}
	count := options.Lanes.Count()
	type laneMetadata struct {
		members    []raftmember.RuntimeIdentity
		commands   []CommandFence
		reads      []ReadSource
		recoveries []TransactionRecoverySource
	}
	metadata := make([]laneMetadata, count)
	seen := make(map[raftmember.GroupKey]struct{}, len(options.Members))
	for index, member := range options.Members {
		if _, duplicate := seen[member.Group]; duplicate {
			return nil, ErrInvalidOwner
		}
		seen[member.Group] = struct{}{}
		lane, err := options.Lanes.Lane(member.Group)
		if err != nil {
			return nil, ErrInvalidOwner
		}
		item := &metadata[lane]
		item.members = append(item.members, member)
		item.commands = append(item.commands, options.CommandFences[index])
		if len(options.ReadSources) != 0 {
			item.reads = append(item.reads, options.ReadSources[index])
		}
		if len(options.TransactionRecoverySources) != 0 {
			item.recoveries = append(item.recoveries, options.TransactionRecoverySources[index])
		}
	}
	result := &ExecutionOwners{
		lanes: options.Lanes, owners: make([]*Owner, count),
		metrics: options.ProgressMetrics,
		pulse:   options.Pulse, ticks: make([]chan struct{}, count),
		started: make(chan struct{}), done: make(chan struct{}),
	}
	groups := &executionOwnerGroups{values: make(map[raftmember.GroupKey]executionOwnerRoute, len(options.Members))}
	for lane := 0; lane < count; lane++ {
		view, err := options.Lanes.OwnerLane(lane)
		if err != nil {
			return nil, err
		}
		result.ticks[lane] = make(chan struct{}, 1)
		item := metadata[lane]
		owner, err := newOwner(Options{
			Registry: options.Registry, Members: item.members, CommandFences: item.commands,
			ReadSources: item.reads, TransactionRecoverySources: item.recoveries,
			MembershipAuthority: options.MembershipAuthority, Outbound: options.Outbound,
			Pulse: result.ticks[lane], Limits: options.Limits,
			ProgressMetrics: options.ProgressMetrics,
		}, view, true)
		if err != nil {
			return nil, err
		}
		result.owners[lane] = owner
		for _, member := range item.members {
			groups.values[member.Group] = executionOwnerRoute{owner: owner}
		}
	}
	result.byGroup.Store(groups)
	return result, nil
}

// ProgressMetrics returns the current bounded RF3 serving counter cut. It is
// safe to call concurrently with every owner lane.
func (owners *ExecutionOwners) ProgressMetrics() ProgressMetricsSnapshot {
	if owners == nil {
		return ProgressMetricsSnapshot{}
	}
	return owners.metrics.Snapshot()
}

// GroupProgressMetrics returns the local member identity and exact counters
// for one group without entering an owner lane.
func (owners *ExecutionOwners) GroupProgressMetrics(group raftmember.GroupKey) (raftmember.RuntimeIdentity, ProgressMetricsSnapshot, bool) {
	if owners == nil || owners.metrics == nil {
		return raftmember.RuntimeIdentity{}, ProgressMetricsSnapshot{}, false
	}
	return owners.metrics.GroupProgressMetrics(group)
}

// EnsureReadAuthorityRound offers one bounded due-threshold renewal or
// acquisition opportunity to the exact serialized execution lane. A usable
// token is reused until its conservative renewal lead window, so this method
// may be called for every read without starting a quorum round per read.
func (owners *ExecutionOwners) EnsureReadAuthorityRound(group raftmember.GroupKey) error {
	if owners == nil || owners.lanes == nil {
		return ErrOwnerClosed
	}
	return owners.lanes.EnsureReadAuthorityRound(group)
}

// ReadAuthorityRoundMetrics returns actual protocol rounds started across all
// groups. It is intentionally separate from ProgressMetrics.AuthorityRoundAttempts,
// which counts owner-side per-read offers and cannot prove quorum amortization.
func (owners *ExecutionOwners) ReadAuthorityRoundMetrics() raftmember.ReadAuthorityRoundMetrics {
	if owners == nil || owners.lanes == nil {
		return raftmember.ReadAuthorityRoundMetrics{}
	}
	return owners.lanes.ReadAuthorityRoundMetrics()
}

func (owners *ExecutionOwners) owner(group raftmember.GroupKey) (*Owner, error) {
	if owners == nil || group == (raftmember.GroupKey{}) {
		return nil, ErrExecutionGroup
	}
	groups := owners.byGroup.Load()
	if groups == nil {
		return nil, ErrExecutionGroup
	}
	route, found := groups.values[group]
	if !found || route.owner == nil || route.ready != nil && !route.ready.Load() {
		return nil, ErrExecutionGroup
	}
	return route.owner, nil
}

// installGroup runs the final transport publication on the owning lane. The
// callback must not fail: it is invoked only after Host ownership and serving
// metadata have been installed, before that lane may execute the Runtime.
func (owners *ExecutionOwners) installGroup(group ExecutionGroup, publish func()) error {
	if owners == nil || publish == nil || group.Runtime == nil ||
		group.Identity != group.Runtime.Identity() || owners.state.Load() != executionOwnersRunning {
		return ErrInvalidOwner
	}
	laneIndex, err := owners.lanes.Lane(group.Identity.Group)
	if err != nil || laneIndex < 0 || laneIndex >= len(owners.owners) {
		return errors.Join(ErrExecutionGroup, err)
	}
	owner := owners.owners[laneIndex]
	ready := new(atomic.Bool)
	return owner.installExecutionGroup(group, func() {
		current := owners.byGroup.Load()
		next := &executionOwnerGroups{values: make(map[raftmember.GroupKey]executionOwnerRoute, len(current.values)+1)}
		for key, value := range current.values {
			next.values[key] = value
		}
		next.values[group.Identity.Group] = executionOwnerRoute{owner: owner, ready: ready}
		owners.byGroup.Store(next)
		publish()
		ready.Store(true)
	})
}

func (owners *ExecutionOwners) removeGroup(
	identity raftmember.RuntimeIdentity,
	withdraw func(),
) error {
	group := identity.Group
	if owners == nil || group == (raftmember.GroupKey{}) || withdraw == nil ||
		owners.state.Load() != executionOwnersRunning {
		return ErrInvalidOwner
	}
	owner, err := owners.owner(group)
	if err != nil {
		return err
	}
	route := owners.byGroup.Load().values[group]
	return owner.removeExecutionGroup(identity, func() {
		if route.ready != nil {
			route.ready.Store(false)
		}
		withdraw()
		current := owners.byGroup.Load()
		next := &executionOwnerGroups{values: make(map[raftmember.GroupKey]executionOwnerRoute, len(current.values)-1)}
		for key, value := range current.values {
			if key != group {
				next.values[key] = value
			}
		}
		owners.byGroup.Store(next)
	})
}

// Run starts every lane owner before publishing readiness. The first lane
// exit cancels and joins all others, then closes the aggregate lane set.
func (owners *ExecutionOwners) Run(parent context.Context) error {
	if owners == nil || parent == nil || !owners.state.CompareAndSwap(executionOwnersReady, executionOwnersRunning) {
		return ErrOwnerClosed
	}
	ctx, cancel := context.WithCancelCause(parent)
	defer close(owners.done)
	defer owners.state.Store(executionOwnersClosed)
	results := make(chan error, len(owners.owners))
	for _, owner := range owners.owners {
		go func(owner *Owner) { results <- owner.Run(ctx) }(owner)
	}
	for _, owner := range owners.owners {
		<-owner.Started()
	}
	close(owners.started)
	go owners.fanoutTicks(ctx)
	first := <-results
	if first == nil {
		first = ErrOwnerClosed
	}
	cancel(first)
	joined := first
	for completed := 1; completed < len(owners.owners); completed++ {
		if err := <-results; err != nil && !errors.Is(err, first) {
			joined = errors.Join(joined, err)
		}
	}
	return errors.Join(joined, owners.lanes.Close())
}

func (owners *ExecutionOwners) fanoutTicks(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-owners.pulse:
			if !ok {
				owners.pulse = nil
				continue
			}
			for _, tick := range owners.ticks {
				select {
				case tick <- struct{}{}:
				default:
				}
			}
		}
	}
}

func (owners *ExecutionOwners) HandleInbound(ctx context.Context, inbound rafttransport.Inbound) error {
	owner, err := owners.owner(inbound.Group)
	if err != nil {
		return err
	}
	return owner.HandleInbound(ctx, inbound)
}
func (owners *ExecutionOwners) Probe(ctx context.Context, group raftmember.GroupKey) (ServingState, error) {
	owner, err := owners.owner(group)
	if err != nil {
		return ServingState{}, err
	}
	return owner.Probe(ctx, group)
}
func (owners *ExecutionOwners) Campaign(ctx context.Context, group raftmember.GroupKey) error {
	owner, err := owners.owner(group)
	if err != nil {
		return err
	}
	return owner.Campaign(ctx, group)
}
func (owners *ExecutionOwners) SubmitOwned(ctx context.Context, fence ServingFence, data []byte) (Result, error) {
	owner, err := owners.owner(fence.Group)
	if err != nil {
		return Result{}, err
	}
	return owner.SubmitOwned(ctx, fence, data)
}
func (owners *ExecutionOwners) SubmitOwnedAuthorized(
	ctx context.Context,
	fence ServingFence,
	data []byte,
	authorize ProposalAuthorization,
) (Result, error) {
	owner, err := owners.owner(fence.Group)
	if err != nil {
		return Result{}, err
	}
	return owner.SubmitOwnedAuthorized(ctx, fence, data, authorize)
}
func (owners *ExecutionOwners) ApplyMembership(ctx context.Context, request MembershipRequest) error {
	owner, err := owners.owner(request.Fence.Group)
	if err != nil {
		return err
	}
	return owner.ApplyMembership(ctx, request)
}
func (owners *ExecutionOwners) ProposeOwnershipTransition(ctx context.Context, fence ServingFence, command []byte) error {
	owner, err := owners.owner(fence.Group)
	if err != nil {
		return err
	}
	return owner.ProposeOwnershipTransition(ctx, fence, command)
}
func (owners *ExecutionOwners) ProposeSchemaTransition(ctx context.Context, fence ServingFence, command []byte) error {
	owner, err := owners.owner(fence.Group)
	if err != nil {
		return err
	}
	return owner.ProposeSchemaTransition(ctx, fence, command)
}
func (owners *ExecutionOwners) ObserveSchemaTransition(
	ctx context.Context, group raftmember.GroupKey, command []byte,
) (bool, error) {
	owner, err := owners.owner(group)
	if err != nil {
		return false, err
	}
	return owner.ObserveSchemaTransition(ctx, group, command)
}
func (owners *ExecutionOwners) QuiesceSchemaGeneration(ctx context.Context, fence ServingFence, command []byte) error {
	owner, err := owners.owner(fence.Group)
	if err != nil {
		return err
	}
	return owner.QuiesceSchemaGeneration(ctx, fence, command)
}
func (owners *ExecutionOwners) QuiesceCommittedSchemaGeneration(
	ctx context.Context, group raftmember.GroupKey, command []byte,
) error {
	owner, err := owners.owner(group)
	if err != nil {
		return err
	}
	return owner.QuiesceCommittedSchemaGeneration(ctx, group, command)
}
func (owners *ExecutionOwners) FenceCommittedSchemaGeneration(
	ctx context.Context, group raftmember.GroupKey, command []byte,
) error {
	owner, err := owners.owner(group)
	if err != nil {
		return err
	}
	return owner.FenceCommittedSchemaGeneration(ctx, group, command)
}
func (owners *ExecutionOwners) InstallSchemaGeneration(
	ctx context.Context, group raftmember.GroupKey,
	database *sqldriver.Database, apply *sqldriver.ReplicatedApply,
	expectedSQL sqldriver.ReplicatedShardStoreIdentity,
	expectedApply sqldriver.ReplicatedApplyIdentity,
) error {
	owner, err := owners.owner(group)
	if err != nil {
		return err
	}
	return owner.InstallSchemaGeneration(
		ctx, group, database, apply, expectedSQL, expectedApply,
	)
}
func (owners *ExecutionOwners) RetireReplicaSource(ctx context.Context, request ReplicaRetirementRequest) error {
	owner, err := owners.owner(request.Fence.Group)
	if err != nil {
		return err
	}
	return owner.RetireReplicaSource(ctx, request)
}
func (owners *ExecutionOwners) ObserveReplica(ctx context.Context, group raftmember.GroupKey, target uint64) (ReplicaObservation, error) {
	owner, err := owners.owner(group)
	if err != nil {
		return ReplicaObservation{}, err
	}
	return owner.ObserveReplica(ctx, group, target)
}
func (owners *ExecutionOwners) ObserveReplicaHealth(
	ctx context.Context, group raftmember.GroupKey, target uint64,
) (ReplicaHealthObservation, error) {
	owner, err := owners.owner(group)
	if err != nil {
		return ReplicaHealthObservation{}, err
	}
	return owner.ObserveReplicaHealth(ctx, group, target)
}
func (owners *ExecutionOwners) ReadPoint(ctx context.Context, request PointReadRequest) (PointReadResult, PointReadLease, error) {
	owner, err := owners.owner(request.Fence.Group)
	if err != nil {
		return PointReadResult{}, nil, err
	}
	return owner.ReadPoint(ctx, request)
}
func (owners *ExecutionOwners) ReadTransaction(ctx context.Context, request TransactionReadRequest) (TransactionReadResult, TransactionReadLease, error) {
	owner, err := owners.owner(request.Fence.Group)
	if err != nil {
		return TransactionReadResult{}, nil, err
	}
	return owner.ReadTransaction(ctx, request)
}
func (owners *ExecutionOwners) ReadRequestLedger(ctx context.Context, request RequestLedgerReadRequest) (RequestLedgerReadResult, RequestLedgerReadLease, error) {
	owner, err := owners.owner(request.Fence.Group)
	if err != nil {
		return RequestLedgerReadResult{}, nil, err
	}
	return owner.ReadRequestLedger(ctx, request)
}
func (owners *ExecutionOwners) ReadExecutionPin(ctx context.Context, request ExecutionPinReadRequest) (ExecutionPinReadResult, ExecutionPinReadLease, error) {
	owner, err := owners.owner(request.Fence.Group)
	if err != nil {
		return ExecutionPinReadResult{}, nil, err
	}
	return owner.ReadExecutionPin(ctx, request)
}
func (owners *ExecutionOwners) ReadLinearizableSnapshot(ctx context.Context,
	request LinearizableSnapshotRequest,
) (*LinearizableSnapshotCut, error) {
	owner, err := owners.owner(request.Fence.Group)
	if err != nil {
		return nil, err
	}
	return owner.ReadLinearizableSnapshot(ctx, request)
}
func (owners *ExecutionOwners) Started() <-chan struct{} {
	if owners == nil {
		return nil
	}
	return owners.started
}
func (owners *ExecutionOwners) Done() <-chan struct{} {
	if owners == nil {
		return nil
	}
	return owners.done
}
func (owners *ExecutionOwners) Running() bool {
	if owners == nil || owners.state.Load() != executionOwnersRunning {
		return false
	}
	for _, owner := range owners.owners {
		if !owner.Running() {
			return false
		}
	}
	return true
}
