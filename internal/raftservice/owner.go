// Package raftservice connects the synchronous Multi-Raft kernel to bounded
// serving and transport queues. One Owner goroutine is the sole caller of its
// Host; request handlers never enter Raft or its Runtime directly.
package raftservice

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
)

var (
	// ErrInvalidOwner reports an incomplete or unbounded owner configuration.
	ErrInvalidOwner = errors.New("raftservice: invalid owner configuration")
	// ErrIngressFull reports that the fixed owner ingress budget is exhausted.
	ErrIngressFull = errors.New("raftservice: owner ingress is full")
	// ErrPendingProposalsFull reports that commands retained by synchronous
	// proposal callers have exhausted their independent item or byte budget.
	// Raft peer ingress remains available when this client-facing budget fills.
	ErrPendingProposalsFull = errors.New("raftservice: pending proposal budget is full")
	// ErrPendingReadsFull reports exhaustion of the independent response item
	// or byte budget. It does not block peer ingress or committed apply.
	ErrPendingReadsFull = errors.New("raftservice: pending read budget is full")
	// ErrOwnerClosed reports admission outside the serialized owner's live Run
	// interval, including before the sole Host lane has started.
	ErrOwnerClosed = errors.New("raftservice: owner is closed")
	// ErrServingFence reports a request addressed to another local Runtime
	// incarnation, allocation, or leadership term.
	ErrServingFence = errors.New("raftservice: stale serving fence")
	// ErrOutcomeUnknown reports cancellation or owner loss after the exact
	// command entered the serving registry. Retrying the same command bytes is
	// safe; changing its request identity is not.
	ErrOutcomeUnknown = errors.New("raftservice: admitted command outcome is unknown")
)

// Limits bounds every object retained outside Host and rafttransport. Host and
// transport keep their own independent item and byte bounds.
type Limits struct {
	MaxIngressItems         int
	MaxIngressBytes         int64
	MaxPendingProposalItems int
	MaxPendingProposalBytes int64
	MaxPendingReadItems     int
	MaxPendingReadBytes     int64
	MaxPendingOutboundBytes int64
}

// OutboundSink accepts one detached Host message into a bounded transport
// queue. Success transfers no protobuf ownership: the sink must detach before
// returning. OrdinaryTransport satisfies this contract.
type OutboundSink interface {
	Send(raftmember.OutboundMessage) error
}

// Options fixes one serialized serving lane. Pulse supplies logical Raft ticks
// and also retries a retained outbound message after transport backpressure.
// Core never samples wall-clock time.
type Options struct {
	Registry      *raftserve.Registry
	Host          *multiraft.Host
	Members       []raftmember.RuntimeIdentity
	CommandFences []CommandFence
	ReadSources   []ReadSource
	Outbound      OutboundSink
	Pulse         <-chan struct{}
	Limits        Limits
}

type requestKind uint8

const (
	requestProposal requestKind = iota + 1
	requestInbound
	requestCampaign
	requestStatus
	requestReadLinear
	requestReadFollower
)

const (
	proposalDeliveryPending uint32 = iota
	proposalDeliveryAbandoned
	proposalDeliveryReady
)

// proposalDelivery closes the cancellation race between a request goroutine
// and the serialized Owner. Exactly one side wins waiter ownership.
type proposalDelivery struct {
	state atomic.Uint32
}

type ownerRequest struct {
	kind     requestKind
	group    raftmember.GroupKey
	data     []byte
	fence    ServingFence
	inbound  rafttransport.Inbound
	reply    chan ownerReply
	bytes    int64
	async    bool
	delivery *proposalDelivery
	read     readRequest
}

type ownerReply struct {
	waiter raftserve.Waiter
	state  ServingState
	err    error
	read   readAuthorization
}

type readRequest struct {
	fence          ServingFence
	minimumApplied uint64
	delivery       *readDelivery
}

type readAuthorization struct {
	source         ReadSource
	minimumApplied uint64
}

type readDelivery struct {
	state          atomic.Uint32
	reply          chan ownerReply
	source         ReadSource
	minimumApplied uint64
}

const (
	readDeliveryPending uint32 = iota
	readDeliveryAbandoned
	readDeliveryReady
)

// ReadSource exposes the replicated machine's coherent dense-relation point
// cut without table names or SQL interpretation.
type ReadSource interface {
	PointReadInto(replication.RelationID, []byte, uint64, int, []byte) (replicatedstate.PointReadResult, error)
}

// Result is one terminal deterministic apply result. Completion is the exact
// canonical completion envelope, not JSON and not a formatted string.
type Result struct {
	Outcome    raftserve.Outcome
	Completion []byte
}

// PointReadRequest selects one dense relation and exact consistency contract.
// Linearizable uses a leader ReadIndex barrier. Follower mode serves only when
// the local publication reaches MinimumApplied; it makes no time-based claim.
type PointReadRequest struct {
	Fence          ServingFence
	Relation       replication.RelationID
	Key            []byte
	MinimumApplied uint64
	MaxValueBytes  int
	Linearizable   bool
}

// PointReadResult owns Value. Found=true with an empty Value is distinct from
// a miss and is preserved by the native wire.
type PointReadResult struct {
	Applied uint64
	Found   bool
	Value   []byte
}

// PointReadLease holds the conservative response-memory reservation until the
// serving boundary has finished encoding and writing the result.
type PointReadLease interface {
	Release()
}

const (
	// Native read responses retain a five-byte frame header and a 309-byte
	// fixed body in addition to the detached store value. Charge each of the
	// two variable-sized allocations for worst-case 8 KiB allocator rounding;
	// this keeps the resident-memory contract conservative across size classes.
	pointReadEncodedFrameFixedBytes int64 = 5 + 309
	pointReadAllocatorSlopBytes     int64 = 2 * ((8 << 10) - 1)
)

func pointReadResponseCharge(maximum int) (int64, bool) {
	if maximum <= 0 {
		return 0, false
	}
	const maxInt64 = int64(^uint64(0) >> 1)
	fixed := pointReadEncodedFrameFixedBytes + pointReadAllocatorSlopBytes
	payload := int64(maximum)
	if payload > (maxInt64-fixed)/2 {
		return 0, false
	}
	return payload*2 + fixed, true
}

type pointReadLease struct {
	owner    *Owner
	bytes    int64
	released atomic.Bool
}

func (lease *pointReadLease) Release() {
	if lease != nil && lease.owner != nil && lease.released.CompareAndSwap(false, true) {
		lease.owner.releasePendingRead(lease.bytes)
	}
}

// CommandFence is the exact command/catalog contract served by one Runtime.
// SchemaGeneration authenticates dense relation IDs; the other scalars fence
// topology, protection, ownership, and routing interpretation before Raft
// admission.
type CommandFence struct {
	ReplicaSetVersion      uint64
	ActivePolicyGeneration uint64
	ProtectionEpoch        uint64
	OwnershipEpoch         uint64
	SchemaGeneration       uint64
	// RelationManifestDigest is the portable logical relation manifest for
	// SchemaGeneration. It must come from replicatedstate.Machine or the SQL
	// apply capacity profile and must never be the replica-local storage-bound
	// catalog digest.
	RelationManifestDigest [32]byte
	RoutingVersion         uint64
	RouteGeneration        uint64
}

// Valid reports whether every generation in the serving contract is present.
func (fence CommandFence) Valid() bool {
	return fence.ReplicaSetVersion != 0 && fence.ActivePolicyGeneration != 0 &&
		fence.ProtectionEpoch != 0 && fence.OwnershipEpoch != 0 &&
		fence.SchemaGeneration != 0 && fence.RelationManifestDigest != ([32]byte{}) &&
		fence.RoutingVersion != 0 &&
		fence.RouteGeneration != 0
}

// ServingFence is the exact live local-member and leader incarnation observed
// by a gateway handshake. It is checked on the serialized owner immediately
// before registry admission.
type ServingFence struct {
	Group                raftmember.GroupKey
	AllocationGeneration uint64
	Command              CommandFence
	MemberID             uint64
	StoreID              [16]byte
	NodeIncarnation      uint64
	Term                 uint64
}

// ServingState is one allocation-free owner handshake result.
type ServingState struct {
	Identity raftmember.RuntimeIdentity
	Command  CommandFence
	Status   raftmember.RuntimeStatus
}

// Fence returns the exact proposal fence represented by state.
func (state ServingState) Fence() ServingFence {
	return ServingFence{
		Group:                state.Identity.Group,
		AllocationGeneration: state.Identity.AllocationGeneration,
		Command:              state.Command,
		MemberID:             state.Identity.MemberID, StoreID: state.Identity.StoreID,
		NodeIncarnation: state.Identity.NodeIncarnation, Term: state.Status.Term,
	}
}

// NotLeaderError is a definite pre-admission refusal. Status carries the
// current term and leader hint observed on the serialized owner.
type NotLeaderError struct {
	Status raftmember.RuntimeStatus
}

func (e *NotLeaderError) Error() string { return raftmodel.ErrNotLeader.Error() }
func (e *NotLeaderError) Unwrap() error { return raftmodel.ErrNotLeader }

// UnknownOutcomeError preserves the exact command bytes required for a safe
// retry after local admission. Command is capacity-clamped and owned.
type UnknownOutcomeError struct {
	Command []byte
	Cause   error
}

func (e *UnknownOutcomeError) Error() string {
	return fmt.Sprintf("%v: %v", ErrOutcomeUnknown, e.Cause)
}

func (e *UnknownOutcomeError) Unwrap() []error {
	return []error{ErrOutcomeUnknown, e.Cause}
}

// Owner serializes Host access and owns one bounded pending outbound slot. A
// message is never discarded on transport backpressure: the Owner does not pop
// another Host message until the retained one is accepted.
type Owner struct {
	registry *raftserve.Registry
	host     *multiraft.Host
	groups   []raftmember.GroupKey
	members  map[raftmember.GroupKey]ownerMember
	outbound OutboundSink
	pulse    <-chan struct{}
	limits   Limits

	ingress chan ownerRequest
	ready   chan struct{}
	done    chan struct{}

	mu                   sync.Mutex
	ingressItems         int
	ingressBytes         int64
	pendingProposalItems int
	pendingProposalBytes int64
	pendingReadItems     int
	pendingReadBytes     int64
	readSequence         uint64
	pendingReads         map[[16]byte]*readDelivery
	started              bool
	closed               bool
	failure              error
}

type ownerMember struct {
	identity raftmember.RuntimeIdentity
	command  CommandFence
	read     ReadSource
}

// NewOwner validates and detaches one lane configuration. Runtime adoption and
// Host group addition must be complete before construction.
func NewOwner(options Options) (*Owner, error) {
	limits := options.Limits
	if options.Registry == nil || options.Host == nil || len(options.Members) == 0 ||
		len(options.CommandFences) != len(options.Members) ||
		limits.MaxIngressItems <= 0 || limits.MaxIngressItems > multiraft.AbsoluteMaxQueueItems ||
		limits.MaxIngressBytes <= 0 || limits.MaxIngressBytes > multiraft.AbsoluteMaxQueueBytes ||
		limits.MaxPendingProposalItems <= 0 ||
		limits.MaxPendingProposalItems > raftserve.AbsoluteMaxWaiters ||
		limits.MaxPendingProposalBytes <= 0 ||
		limits.MaxPendingProposalBytes > multiraft.AbsoluteMaxQueueBytes ||
		limits.MaxPendingReadItems <= 0 || limits.MaxPendingReadItems > raftmodel.MaxPendingReads ||
		limits.MaxPendingReadBytes <= 0 || limits.MaxPendingReadBytes > multiraft.AbsoluteMaxQueueBytes ||
		limits.MaxPendingOutboundBytes <= 0 ||
		limits.MaxPendingOutboundBytes > multiraft.AbsoluteMaxOutboxBytes {
		return nil, ErrInvalidOwner
	}
	if len(options.ReadSources) != 0 && len(options.ReadSources) != len(options.Members) {
		return nil, ErrInvalidOwner
	}
	seen := make(map[raftmember.GroupKey]struct{}, len(options.Members))
	groups := make([]raftmember.GroupKey, len(options.Members))
	members := make(map[raftmember.GroupKey]ownerMember, len(options.Members))
	for index, identity := range options.Members {
		group := identity.Group
		if group == (raftmember.GroupKey{}) {
			return nil, ErrInvalidOwner
		}
		if identity.AllocationGeneration == 0 || identity.MemberID == 0 ||
			identity.StoreID == ([16]byte{}) || identity.NodeIncarnation == 0 ||
			identity.RelationManifestDigest == ([32]byte{}) {
			return nil, ErrInvalidOwner
		}
		if !options.CommandFences[index].Valid() ||
			options.CommandFences[index].RelationManifestDigest != identity.RelationManifestDigest {
			return nil, ErrInvalidOwner
		}
		if _, duplicate := seen[group]; duplicate {
			return nil, ErrInvalidOwner
		}
		seen[group] = struct{}{}
		groups[index] = group
		var source ReadSource
		if len(options.ReadSources) != 0 {
			source = options.ReadSources[index]
		}
		members[group] = ownerMember{identity: identity, command: options.CommandFences[index], read: source}
	}
	return &Owner{
		registry: options.Registry, host: options.Host, groups: groups, members: members,
		outbound: options.Outbound, pulse: options.Pulse, limits: limits,
		ingress:      make(chan ownerRequest, limits.MaxIngressItems),
		ready:        make(chan struct{}),
		done:         make(chan struct{}),
		pendingReads: make(map[[16]byte]*readDelivery, limits.MaxPendingReadItems),
	}, nil
}

// Run becomes the sole Host owner until ctx is canceled or a terminal lane
// failure occurs. It may be called exactly once.
func (owner *Owner) Run(ctx context.Context) error {
	if owner == nil || ctx == nil {
		return ErrInvalidOwner
	}
	owner.mu.Lock()
	if owner.started || owner.closed {
		owner.mu.Unlock()
		return ErrOwnerClosed
	}
	owner.started = true
	close(owner.ready)
	owner.mu.Unlock()

	var pending raftmember.OutboundMessage
	readyBlocked := false
	for {
		transportBlocked := false
		if pending.Message != nil {
			if owner.outbound == nil {
				return owner.stop(errors.New("raftservice: outbound message has no transport"))
			}
			err := owner.outbound.Send(pending)
			switch {
			case err == nil:
				pending = raftmember.OutboundMessage{}
			case errors.Is(err, rafttransport.ErrBackpressure):
				// Retain exact ownership and continue serving bounded ingress. A
				// later pulse or request retries before another Host pop.
				transportBlocked = true
			default:
				return owner.stop(err)
			}
		}

		if pending.Message == nil {
			if outbound, ok := owner.host.PopOutbound(); ok {
				size, err := raftmember.MeasureOrdinaryMessage(outbound.Message)
				if err != nil || int64(size) > owner.limits.MaxPendingOutboundBytes {
					return owner.stop(errors.Join(ErrInvalidOwner, err))
				}
				pending = outbound
				continue
			}
		}

		if !transportBlocked && !readyBlocked {
			progress, done, err := owner.host.RunOne()
			switch {
			case errors.Is(err, multiraft.ErrOutboxFull):
				// The next loop transfers one Host-owned message into the retained
				// slot before entering RunOne again.
				continue
			case errors.Is(err, raftmember.ErrRuntimeFailed),
				errors.Is(err, raftmember.ErrRuntimeClosed),
				errors.Is(err, multiraft.ErrHostClosed):
				return owner.stop(err)
			case err != nil:
				// Runtime persistence and retryable result-settlement boundaries
				// retain the exact Ready phase. Do not spin on a failed device or
				// sink: the next bounded ingress event or logical pulse retries it.
				readyBlocked = true
			case done:
				owner.finishReadOutcomes(progress.ReadOutcomes)
				continue
			}
		}

		select {
		case <-ctx.Done():
			return owner.stop(context.Cause(ctx))
		case request := <-owner.ingress:
			readyBlocked = false
			handleErr := owner.handle(request)
			owner.release(request.bytes)
			if request.async && handleErr != nil {
				return owner.stop(handleErr)
			}
		case _, ok := <-owner.pulse:
			readyBlocked = false
			if !ok {
				owner.pulse = nil
				continue
			}
			for _, group := range owner.groups {
				if err := owner.host.RequestTick(group); err != nil {
					return owner.stop(err)
				}
			}
		}
	}
}

func (owner *Owner) handle(request ownerRequest) error {
	reply := ownerReply{}
	switch request.kind {
	case requestProposal:
		member, found := owner.members[request.group]
		if !found || !servingFenceMatchesIdentity(request.fence, member) {
			reply.err = ErrServingFence
			break
		}
		command, err := replication.OpenCommand(request.data)
		if err != nil {
			reply.err = err
			break
		}
		if !commandMatchesFence(command, request.fence) {
			reply.err = ErrServingFence
			break
		}
		status, err := owner.host.Status(request.group)
		if err != nil {
			reply.err = err
			break
		}
		if status.MemberID != member.identity.MemberID || status.LeaderID != member.identity.MemberID ||
			status.Term != request.fence.Term {
			reply.err = &NotLeaderError{Status: status}
			break
		}
		reply.waiter, reply.err = owner.registry.Enqueue(owner.host, request.group, request.data)
		if reply.err == nil {
			reply.waiter, reply.err = handoffProposalWaiter(request.delivery, reply.waiter)
		}
	case requestInbound:
		if request.inbound.Group != request.group || request.inbound.Message == nil {
			reply.err = ErrInvalidOwner
		} else {
			reply.err = owner.host.AdoptMessage(request.group, request.inbound.Message)
		}
	case requestCampaign:
		reply.err = owner.host.RequestCampaign(request.group)
	case requestStatus:
		member, found := owner.members[request.group]
		if !found {
			reply.err = multiraft.ErrGroupNotFound
			break
		}
		reply.state.Identity = member.identity
		reply.state.Command = member.command
		reply.state.Status, reply.err = owner.host.Status(request.group)
	case requestReadLinear, requestReadFollower:
		member, found := owner.members[request.group]
		if !found || member.read == nil ||
			!servingFenceMatchesIdentity(request.read.fence, member) {
			reply.err = ErrServingFence
			break
		}
		status, err := owner.host.Status(request.group)
		if err != nil {
			reply.err = err
			break
		}
		if status.MemberID != member.identity.MemberID || status.Term != request.read.fence.Term {
			reply.err = ErrServingFence
			break
		}
		if request.kind == requestReadFollower {
			if status.Applied < request.read.minimumApplied {
				reply.err = replicatedstate.ErrReadBehind
				break
			}
			reply.read = readAuthorization{source: member.read, minimumApplied: request.read.minimumApplied}
			break
		}
		if status.LeaderID != member.identity.MemberID {
			reply.err = &NotLeaderError{Status: status}
			break
		}
		context, contextErr := owner.nextReadContext(member.identity.NodeIncarnation)
		if contextErr != nil {
			reply.err = contextErr
			break
		}
		if err := owner.host.ReadIndex(request.group, context[:]); err != nil {
			reply.err = err
			break
		}
		request.read.delivery.source = member.read
		request.read.delivery.minimumApplied = request.read.minimumApplied
		owner.pendingReads[context] = request.read.delivery
		// The reply is settled only by the matching quorum barrier.
		return nil
	default:
		reply.err = ErrInvalidOwner
	}
	if request.kind == requestReadLinear && request.read.delivery != nil {
		owner.settleReadDelivery(request.read.delivery, reply)
	} else {
		request.reply <- reply
	}
	return reply.err
}

func (owner *Owner) nextReadContext(incarnation uint64) ([16]byte, error) {
	if owner.readSequence == ^uint64(0) || len(owner.pendingReads) >= owner.limits.MaxPendingReadItems {
		return [16]byte{}, ErrIngressFull
	}
	owner.readSequence++
	var context [16]byte
	binary.LittleEndian.PutUint64(context[:8], incarnation)
	binary.LittleEndian.PutUint64(context[8:], owner.readSequence)
	return context, nil
}

func (owner *Owner) finishReadOutcomes(outcomes []raftmodel.ReadOutcome) {
	for index := range outcomes {
		outcome := outcomes[index]
		if len(outcome.Barrier.Context) != 16 {
			continue
		}
		var key [16]byte
		copy(key[:], outcome.Barrier.Context)
		delivery, found := owner.pendingReads[key]
		if !found {
			continue
		}
		delete(owner.pendingReads, key)
		reply := ownerReply{err: outcome.Err}
		if outcome.Err == nil {
			reply.read.minimumApplied = max(delivery.minimumApplied, outcome.Barrier.Index)
			// Source was authenticated at admission and remains bound to the
			// immutable owner member for this allocation.
			reply.read.source = delivery.source
		}
		owner.settleReadDelivery(delivery, reply)
	}
}

func (owner *Owner) settleReadDelivery(delivery *readDelivery, reply ownerReply) {
	if delivery != nil && delivery.state.CompareAndSwap(readDeliveryPending, readDeliveryReady) {
		delivery.reply <- reply
	}
}

func handoffProposalWaiter(
	delivery *proposalDelivery,
	waiter raftserve.Waiter,
) (raftserve.Waiter, error) {
	if delivery == nil || delivery.state.CompareAndSwap(
		proposalDeliveryPending, proposalDeliveryReady,
	) {
		return waiter, nil
	}
	waiter.Cancel()
	return raftserve.Waiter{}, ErrOutcomeUnknown
}

func servingFenceMatchesIdentity(
	fence ServingFence,
	member ownerMember,
) bool {
	identity := member.identity
	return fence.Group == identity.Group &&
		fence.AllocationGeneration == identity.AllocationGeneration &&
		fence.Command == member.command &&
		fence.MemberID == identity.MemberID && fence.StoreID == identity.StoreID &&
		fence.NodeIncarnation == identity.NodeIncarnation && fence.Term != 0
}

func commandMatchesFence(command replication.CommandView, fence ServingFence) bool {
	return command.ClusterID == fence.Group.ClusterID &&
		command.ClusterIncarnation == fence.Group.ClusterIncarnation &&
		command.TopologyRecoveryEpoch == fence.Group.TopologyRecoveryEpoch &&
		command.ShardIncarnation == fence.Group.ShardIncarnation &&
		command.GroupID == fence.Group.GroupID &&
		command.AllocationGeneration == fence.AllocationGeneration &&
		command.ReplicaSetVersion == fence.Command.ReplicaSetVersion &&
		command.ActivePolicyGeneration == fence.Command.ActivePolicyGeneration &&
		command.ProtectionEpoch == fence.Command.ProtectionEpoch &&
		command.OwnershipEpoch == fence.Command.OwnershipEpoch &&
		command.SchemaGeneration == fence.Command.SchemaGeneration &&
		command.RoutingVersion == fence.Command.RoutingVersion &&
		command.RouteGeneration == fence.Command.RouteGeneration
}

func (owner *Owner) stop(cause error) error {
	if cause == nil {
		cause = ErrOwnerClosed
	}
	// Close is serialized with every earlier Host call. Its serving lifecycle
	// callbacks resolve queued proposals and terminate admitted attempts that
	// can no longer apply locally before Done becomes observable.
	cause = errors.Join(cause, owner.host.Close())
	owner.mu.Lock()
	if !owner.closed {
		owner.closed = true
		owner.failure = cause
		close(owner.done)
	}
	owner.mu.Unlock()
	for key, delivery := range owner.pendingReads {
		delete(owner.pendingReads, key)
		owner.settleReadDelivery(delivery, ownerReply{err: errors.Join(ErrOwnerClosed, cause)})
	}
	for {
		select {
		case request := <-owner.ingress:
			request.reply <- ownerReply{err: errors.Join(ErrOwnerClosed, cause)}
			owner.release(request.bytes)
		default:
			return cause
		}
	}
}

// ReadPoint authorizes through the serialized owner and reads an exact
// coherent replicated cut outside the Raft lane. The conservative response
// reservation is held across authorization, snapshot IO, and result copy.
func (owner *Owner) ReadPoint(
	ctx context.Context,
	request PointReadRequest,
) (PointReadResult, PointReadLease, error) {
	if owner == nil || ctx == nil || request.Relation == 0 ||
		len(request.Key) == 0 || len(request.Key) > replication.MaxMutationKeyBytes ||
		request.MinimumApplied == 0 || request.MaxValueBytes <= 0 ||
		request.MaxValueBytes > replication.MaxMutationValueBytes {
		return PointReadResult{}, nil, ErrInvalidOwner
	}
	// The server briefly owns the detached store result and its encoded frame.
	// Charge both rounded allocations and exact wire overhead before either can
	// exist.
	responseCharge, ok := pointReadResponseCharge(request.MaxValueBytes)
	if !ok {
		return PointReadResult{}, nil, ErrInvalidOwner
	}
	if err := owner.reservePendingRead(responseCharge); err != nil {
		return PointReadResult{}, nil, err
	}
	releaseReservation := true
	defer func() {
		if releaseReservation {
			owner.releasePendingRead(responseCharge)
		}
	}()
	key := make([]byte, len(request.Key))
	copy(key, request.Key)
	kind := requestReadFollower
	var reply ownerReply
	var err error
	if request.Linearizable {
		kind = requestReadLinear
		delivery := &readDelivery{reply: make(chan ownerReply, 1)}
		ownerRequest := ownerRequest{
			kind: kind, group: request.Fence.Group, reply: delivery.reply,
			bytes: int64(cap(key)), read: readRequest{
				fence: request.Fence, minimumApplied: request.MinimumApplied,
				delivery: delivery,
			},
		}
		reply, err = owner.enqueueRead(ctx, ownerRequest, delivery)
	} else {
		reply, err = owner.enqueue(ctx, ownerRequest{
			kind: kind, group: request.Fence.Group, reply: make(chan ownerReply, 1),
			bytes: int64(cap(key)), read: readRequest{
				fence: request.Fence, minimumApplied: request.MinimumApplied,
			},
		})
	}
	if err != nil {
		return PointReadResult{}, nil, err
	}
	value, err := reply.read.source.PointReadInto(
		request.Relation, key, reply.read.minimumApplied, request.MaxValueBytes,
		nil,
	)
	if err != nil || !pointReadFenceMatches(value.Fence, request.Fence) ||
		value.Fence.Applied < reply.read.minimumApplied || len(value.Value) > request.MaxValueBytes {
		if err != nil {
			return PointReadResult{}, nil, err
		}
		return PointReadResult{}, nil, ErrServingFence
	}
	releaseReservation = false
	return PointReadResult{Applied: value.Fence.Applied, Found: value.Found, Value: value.Value},
		&pointReadLease{owner: owner, bytes: responseCharge}, nil
}

func (owner *Owner) enqueueRead(
	ctx context.Context,
	request ownerRequest,
	delivery *readDelivery,
) (ownerReply, error) {
	if cause := context.Cause(ctx); cause != nil {
		return ownerReply{}, cause
	}
	if err := owner.publish(request); err != nil {
		return ownerReply{}, err
	}
	select {
	case reply := <-delivery.reply:
		return reply, reply.err
	case <-ctx.Done():
		if delivery.state.CompareAndSwap(readDeliveryPending, readDeliveryAbandoned) {
			return ownerReply{}, context.Cause(ctx)
		}
		reply := <-delivery.reply
		return reply, reply.err
	}
}

func pointReadFenceMatches(fence replicatedstate.SnapshotFence, serving ServingFence) bool {
	binding := fence.Binding
	return binding.ClusterID == serving.Group.ClusterID &&
		binding.ClusterIncarnation == serving.Group.ClusterIncarnation &&
		binding.TopologyRecoveryEpoch == serving.Group.TopologyRecoveryEpoch &&
		binding.ShardIncarnation == serving.Group.ShardIncarnation &&
		binding.GroupID == serving.Group.GroupID &&
		binding.AllocationGeneration == serving.AllocationGeneration &&
		binding.ActivePolicyGeneration == serving.Command.ActivePolicyGeneration &&
		binding.ProtectionEpoch == serving.Command.ProtectionEpoch &&
		binding.OwnershipEpoch == serving.Command.OwnershipEpoch &&
		binding.SchemaGeneration == serving.Command.SchemaGeneration &&
		fence.RelationManifestDigest == serving.Command.RelationManifestDigest &&
		fence.ReplicaSetVersion == serving.Command.ReplicaSetVersion &&
		binding.RoutingVersion == serving.Command.RoutingVersion &&
		binding.RouteGeneration == serving.Command.RouteGeneration
}

func (owner *Owner) reservePendingRead(bytes int64) error {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if !owner.started || owner.closed {
		return errors.Join(ErrOwnerClosed, owner.failure)
	}
	if owner.pendingReadItems == owner.limits.MaxPendingReadItems ||
		bytes > owner.limits.MaxPendingReadBytes-owner.pendingReadBytes {
		return ErrPendingReadsFull
	}
	owner.pendingReadItems++
	owner.pendingReadBytes += bytes
	return nil
}

func (owner *Owner) releasePendingRead(bytes int64) {
	owner.mu.Lock()
	owner.pendingReadItems--
	owner.pendingReadBytes -= bytes
	owner.mu.Unlock()
}

// publish reserves and enqueues atomically under the lifecycle mutex. Because
// channel capacity equals the item budget, a successful reservation always has
// one immediate slot. This prevents a producer that raced stop from publishing
// after the final drain and waiting forever for a reply.
func (owner *Owner) publish(request ownerRequest) error {
	bytes := request.bytes
	if owner == nil || request.reply == nil || bytes < 0 || bytes > owner.limits.MaxIngressBytes {
		return ErrIngressFull
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if !owner.started || owner.closed {
		return errors.Join(ErrOwnerClosed, owner.failure)
	}
	if owner.ingressItems == owner.limits.MaxIngressItems ||
		bytes > owner.limits.MaxIngressBytes-owner.ingressBytes {
		return ErrIngressFull
	}
	owner.ingressItems++
	owner.ingressBytes += bytes
	select {
	case owner.ingress <- request:
		return nil
	default:
		owner.ingressItems--
		owner.ingressBytes -= bytes
		return errors.Join(ErrInvalidOwner, ErrIngressFull)
	}
}

func (owner *Owner) release(bytes int64) {
	owner.mu.Lock()
	owner.ingressItems--
	owner.ingressBytes -= bytes
	owner.mu.Unlock()
}

func (owner *Owner) enqueue(ctx context.Context, request ownerRequest) (ownerReply, error) {
	if ctx == nil {
		return ownerReply{}, ErrInvalidOwner
	}
	if cause := context.Cause(ctx); cause != nil {
		return ownerReply{}, cause
	}
	if err := owner.publish(request); err != nil {
		return ownerReply{}, err
	}
	if request.delivery == nil {
		select {
		case reply := <-request.reply:
			return reply, reply.err
		case <-ctx.Done():
			return ownerReply{}, context.Cause(ctx)
		}
	}
	select {
	case reply := <-request.reply:
		return reply, reply.err
	default:
	}
	select {
	case reply := <-request.reply:
		return reply, reply.err
	case <-ctx.Done():
		if request.delivery.state.CompareAndSwap(
			proposalDeliveryPending, proposalDeliveryAbandoned,
		) {
			return ownerReply{}, errors.Join(ErrOutcomeUnknown, context.Cause(ctx))
		}
		// Owner won the handoff CAS and publishes to the buffered channel
		// immediately afterward. This receive cannot wait on Raft or storage.
		reply := <-request.reply
		return reply, reply.err
	}
}

// Submit registers and proposes one already-canonical command. Cancellation
// after registry admission returns UnknownOutcomeError with an owned exact-byte
// retry payload.
func (owner *Owner) Submit(
	ctx context.Context,
	fence ServingFence,
	command []byte,
) (Result, error) {
	return owner.submit(ctx, fence, command, false)
}

// SubmitOwned transfers one exact-length, capacity-clamped command allocation
// into the synchronous serving path. It exists for the native server decoder,
// which already owns its frame and retains its request bytes for the duration
// of the call. Unlike Submit, an enqueue-time unknown does not copy retry bytes:
// the gateway retains the canonical request and owns retry identity.
func (owner *Owner) SubmitOwned(
	ctx context.Context,
	fence ServingFence,
	command []byte,
) (Result, error) {
	return owner.submit(ctx, fence, command, true)
}

func (owner *Owner) submit(
	ctx context.Context,
	fence ServingFence,
	command []byte,
	transfer bool,
) (Result, error) {
	if len(command) == 0 || len(command) > replication.MaxCommandBytes {
		return Result{}, ErrInvalidOwner
	}
	if transfer && cap(command) != len(command) {
		return Result{}, ErrInvalidOwner
	}
	if ctx == nil {
		return Result{}, ErrInvalidOwner
	}
	if cause := context.Cause(ctx); cause != nil {
		return Result{}, cause
	}
	if err := owner.reservePendingProposal(int64(len(command))); err != nil {
		return Result{}, err
	}
	defer owner.releasePendingProposal(int64(len(command)))
	// Exact-length allocation keeps the ingress byte charge equal to retained
	// capacity instead of relying on append growth-class rounding.
	owned := command
	if !transfer {
		owned = make([]byte, len(command))
		copy(owned, command)
	}
	delivery := &proposalDelivery{}
	reply, err := owner.enqueue(ctx, ownerRequest{
		kind: requestProposal, group: fence.Group, fence: fence, data: owned,
		reply: make(chan ownerReply, 1), bytes: int64(cap(owned)),
		delivery: delivery,
	})
	if err != nil {
		if errors.Is(err, ErrOutcomeUnknown) {
			if transfer {
				return Result{}, err
			}
			return Result{}, &UnknownOutcomeError{
				Command: append([]byte(nil), owned...), Cause: err,
			}
		}
		return Result{}, err
	}
	waiter := reply.waiter
	outcome, waitErr := waiter.Wait(ctx)
	if waitErr != nil {
		waiter.Cancel()
		return Result{}, &UnknownOutcomeError{Command: owned[:len(owned):len(owned)], Cause: waitErr}
	}
	completion := make([]byte, 0, outcome.CompletionBytes)
	completion, taken, takeErr := waiter.TakeCompletionInto(completion)
	if takeErr != nil {
		waiter.Cancel()
		return Result{}, &UnknownOutcomeError{
			Command: owned[:len(owned):len(owned)], Cause: takeErr,
		}
	}
	result := Result{Outcome: taken, Completion: completion}
	if outcomeErr := taken.Err(); outcomeErr != nil {
		return result, outcomeErr
	}
	return result, nil
}

// reservePendingProposal runs before any command-sized allocation. The
// reservation stays charged across registry admission, Wait, completion take,
// cancellation, and exact unknown-outcome handoff.
func (owner *Owner) reservePendingProposal(bytes int64) error {
	if owner == nil || bytes <= 0 || bytes > owner.limits.MaxPendingProposalBytes {
		return ErrPendingProposalsFull
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if !owner.started || owner.closed {
		return errors.Join(ErrOwnerClosed, owner.failure)
	}
	if owner.pendingProposalItems == owner.limits.MaxPendingProposalItems ||
		bytes > owner.limits.MaxPendingProposalBytes-owner.pendingProposalBytes {
		return ErrPendingProposalsFull
	}
	owner.pendingProposalItems++
	owner.pendingProposalBytes += bytes
	return nil
}

func (owner *Owner) releasePendingProposal(bytes int64) {
	owner.mu.Lock()
	owner.pendingProposalItems--
	owner.pendingProposalBytes -= bytes
	owner.mu.Unlock()
}

// HandleInbound is the authenticated rafttransport receiver callback. Inbound
// already owns its protobuf message, so ingress transfers it without a clone.
func (owner *Owner) HandleInbound(ctx context.Context, inbound rafttransport.Inbound) error {
	if inbound.Message == nil {
		return ErrInvalidOwner
	}
	size, err := raftmember.MeasureOrdinaryMessage(inbound.Message)
	if err != nil {
		return err
	}
	_, err = owner.enqueue(ctx, ownerRequest{
		kind: requestInbound, group: inbound.Group, inbound: inbound,
		reply: make(chan ownerReply, 1), bytes: int64(size),
	})
	return err
}

// TryInbound transfers one authenticated inbound message into the fixed owner
// queue without waiting for Host adoption. It is the transport-style contract:
// acceptance means bounded local ownership, not Raft processing. A later Host
// refusal is a lane invariant failure and stops the Owner.
func (owner *Owner) TryInbound(inbound rafttransport.Inbound) error {
	if inbound.Message == nil {
		return ErrInvalidOwner
	}
	size, err := raftmember.MeasureOrdinaryMessage(inbound.Message)
	if err != nil {
		return err
	}
	request := ownerRequest{
		kind: requestInbound, group: inbound.Group, inbound: inbound,
		reply: make(chan ownerReply, 1), bytes: int64(size), async: true,
	}
	return owner.publish(request)
}

// Campaign requests an election through the serialized lane.
func (owner *Owner) Campaign(ctx context.Context, group raftmember.GroupKey) error {
	_, err := owner.enqueue(ctx, ownerRequest{
		kind: requestCampaign, group: group, reply: make(chan ownerReply, 1),
	})
	return err
}

// Status reads detached Raft status through the serialized lane.
func (owner *Owner) Probe(ctx context.Context, group raftmember.GroupKey) (ServingState, error) {
	reply, err := owner.enqueue(ctx, ownerRequest{
		kind: requestStatus, group: group, reply: make(chan ownerReply, 1),
	})
	return reply.state, err
}

// Done closes when the lane stops.
func (owner *Owner) Done() <-chan struct{} {
	if owner == nil {
		return nil
	}
	return owner.done
}

// Started closes after Run becomes the sole Host caller.
func (owner *Owner) Started() <-chan struct{} {
	if owner == nil {
		return nil
	}
	return owner.ready
}

// Running reports whether Run owns the Host and has not stopped.
func (owner *Owner) Running() bool {
	if owner == nil {
		return false
	}
	owner.mu.Lock()
	running := owner.started && !owner.closed
	owner.mu.Unlock()
	return running
}
