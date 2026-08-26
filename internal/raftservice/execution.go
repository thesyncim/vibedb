package raftservice

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/rafttransport"
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
}

// ExecutionOwners is the group-keyed serving capability over all execution
// lanes. It owns no transport: one shared transport can be installed as its
// Outbound sink and its HandleInbound method can serve one shared receiver.
type ExecutionOwners struct {
	lanes   *multiraft.ExecutionLanes
	owners  []*Owner
	byGroup map[raftmember.GroupKey]*Owner
	pulse   <-chan struct{}
	ticks   []chan struct{}

	state   atomic.Uint32
	started chan struct{}
	done    chan struct{}
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
		byGroup: make(map[raftmember.GroupKey]*Owner, len(options.Members)),
		pulse:   options.Pulse, ticks: make([]chan struct{}, count),
		started: make(chan struct{}), done: make(chan struct{}),
	}
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
		}, view, true)
		if err != nil {
			return nil, err
		}
		result.owners[lane] = owner
		for _, member := range item.members {
			result.byGroup[member.Group] = owner
		}
	}
	return result, nil
}

func (owners *ExecutionOwners) owner(group raftmember.GroupKey) (*Owner, error) {
	if owners == nil || group == (raftmember.GroupKey{}) {
		return nil, ErrExecutionGroup
	}
	owner := owners.byGroup[group]
	if owner == nil {
		return nil, ErrExecutionGroup
	}
	return owner, nil
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
