// Package raftservice connects the synchronous Multi-Raft kernel to bounded
// serving and transport queues. One Owner goroutine is the sole caller of its
// Host; request handlers never enter Raft or its Runtime directly.
package raftservice

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	pb "go.etcd.io/raft/v3/raftpb"
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
	// ErrOwnerClosed reports admission outside the serialized owner's live Run
	// interval, including before the sole Host lane has started.
	ErrOwnerClosed = errors.New("raftservice: owner is closed")
	// ErrServingFence reports a request addressed to another local Runtime
	// incarnation, allocation, or leadership term.
	ErrServingFence = errors.New("raftservice: stale serving fence")
	// ErrOutcomeUnknown reports cancellation or owner loss after the exact
	// command entered the serving registry. Retrying the same command bytes is
	// safe; changing its request identity is not.
	ErrOutcomeUnknown         = errors.New("raftservice: admitted command outcome is unknown")
	ErrMembershipUnauthorized = errors.New("raftservice: membership transition is not authorized")
	ErrMembershipStale        = errors.New("raftservice: membership transition is stale")
	ErrMembershipMalformed    = errors.New("raftservice: membership transition is malformed")
	ErrMembershipNotCaughtUp  = errors.New("raftservice: membership target is not caught up")
)

// Limits bounds every object retained outside Host and rafttransport. Host and
// transport keep their own independent item and byte bounds.
type Limits struct {
	MaxIngressItems         int
	MaxIngressBytes         int64
	MaxPendingProposalItems int
	MaxPendingProposalBytes int64
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
	Registry                 *raftserve.Registry
	Host                     *multiraft.Host
	Members                  []raftmember.RuntimeIdentity
	CommandFences            []CommandFence
	MembershipAuthorizations []MembershipAuthorization
	MembershipAuthority      MembershipAuthoritySink
	Outbound                 OutboundSink
	Pulse                    <-chan struct{}
	Limits                   Limits
}

type requestKind uint8

const (
	requestProposal requestKind = iota + 1
	requestInbound
	requestCampaign
	requestStatus
	requestMembership
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
	kind       requestKind
	group      raftmember.GroupKey
	data       []byte
	fence      ServingFence
	inbound    rafttransport.Inbound
	reply      chan ownerReply
	bytes      int64
	async      bool
	delivery   *proposalDelivery
	membership MembershipRequest
}

type ownerReply struct {
	waiter raftserve.Waiter
	state  ServingState
	err    error
}

// Result is one terminal deterministic apply result. Completion is the exact
// canonical completion envelope, not JSON and not a formatted string.
type Result struct {
	Outcome    raftserve.Outcome
	Completion []byte
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
	registry  *raftserve.Registry
	host      *multiraft.Host
	groups    []raftmember.GroupKey
	members   map[raftmember.GroupKey]ownerMember
	outbound  OutboundSink
	pulse     <-chan struct{}
	limits    Limits
	authority MembershipAuthoritySink

	ingress chan ownerRequest
	ready   chan struct{}
	done    chan struct{}

	mu                   sync.Mutex
	ingressItems         int
	ingressBytes         int64
	pendingProposalItems int
	pendingProposalBytes int64
	started              bool
	closed               bool
	failure              error
}

type MembershipAuthoritySink interface {
	PublishCommittedAuthority(raftmember.GroupKey, uint64, *pb.ConfState) error
	PublishDurablePromotion(raftmember.GroupKey, raftmember.DurablePromotionProof) error
	ClearDurablePromotion(raftmember.GroupKey) error
}

type ownerMember struct {
	identity  raftmember.RuntimeIdentity
	command   CommandFence
	authority MembershipAuthorization
}

type MembershipKind uint8

const (
	MembershipAddLearner MembershipKind = iota + 1
	MembershipPromoteVoter
	MembershipRemoveVoter
	MembershipTransferLeader
)

// MembershipAuthorization is the fixed metadata grant for one replica move.
// It is configured locally from the authenticated metadata cut and cannot be
// widened by a network request.
type MembershipAuthorization struct {
	TransitionID      [16]byte
	MetadataEpoch     uint64
	CatalogGeneration uint64
	SourceMember      uint64
	TargetMember      uint64
}

func (a MembershipAuthorization) Valid() bool {
	return a.TransitionID != ([16]byte{}) && a.MetadataEpoch != 0 &&
		a.CatalogGeneration != 0 && a.SourceMember != 0 && a.TargetMember != 0 &&
		a.SourceMember != a.TargetMember
}

// MembershipRequest carries no variable-sized data and is retained by value.
type MembershipRequest struct {
	Fence                     ServingFence
	Kind                      MembershipKind
	TransitionID              [16]byte
	MetadataEpoch             uint64
	CatalogGeneration         uint64
	ExpectedReplicaSetVersion uint64
	SourceMember              uint64
	TargetMember              uint64
	TransferTerm              uint64
}

// NewOwner validates and detaches one lane configuration. Runtime adoption and
// Host group addition must be complete before construction.
func NewOwner(options Options) (*Owner, error) {
	limits := options.Limits
	if options.Registry == nil || options.Host == nil || len(options.Members) == 0 ||
		len(options.CommandFences) != len(options.Members) ||
		(len(options.MembershipAuthorizations) != 0 &&
			len(options.MembershipAuthorizations) != len(options.Members)) ||
		(len(options.MembershipAuthorizations) != 0 && options.MembershipAuthority == nil) ||
		limits.MaxIngressItems <= 0 || limits.MaxIngressItems > multiraft.AbsoluteMaxQueueItems ||
		limits.MaxIngressBytes <= 0 || limits.MaxIngressBytes > multiraft.AbsoluteMaxQueueBytes ||
		limits.MaxPendingProposalItems <= 0 ||
		limits.MaxPendingProposalItems > raftserve.AbsoluteMaxWaiters ||
		limits.MaxPendingProposalBytes <= 0 ||
		limits.MaxPendingProposalBytes > multiraft.AbsoluteMaxQueueBytes ||
		limits.MaxPendingOutboundBytes <= 0 ||
		limits.MaxPendingOutboundBytes > multiraft.AbsoluteMaxOutboxBytes {
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
			identity.StoreID == ([16]byte{}) || identity.NodeIncarnation == 0 {
			return nil, ErrInvalidOwner
		}
		if !options.CommandFences[index].Valid() {
			return nil, ErrInvalidOwner
		}
		if _, duplicate := seen[group]; duplicate {
			return nil, ErrInvalidOwner
		}
		seen[group] = struct{}{}
		groups[index] = group
		var authority MembershipAuthorization
		if len(options.MembershipAuthorizations) != 0 {
			authority = options.MembershipAuthorizations[index]
			if !authority.Valid() {
				return nil, ErrInvalidOwner
			}
		}
		members[group] = ownerMember{identity: identity, command: options.CommandFences[index],
			authority: authority}
	}
	return &Owner{
		registry: options.Registry, host: options.Host, groups: groups, members: members,
		outbound: options.Outbound, pulse: options.Pulse, limits: limits,
		authority: options.MembershipAuthority,
		ingress:   make(chan ownerRequest, limits.MaxIngressItems),
		ready:     make(chan struct{}),
		done:      make(chan struct{}),
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
	if err := owner.syncMembershipAuthorities(); err != nil {
		return owner.stop(err)
	}

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
			_, done, err := owner.host.RunOne()
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
				if err := owner.syncMembershipAuthorities(); err != nil {
					return owner.stop(err)
				}
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

func (owner *Owner) syncMembershipAuthorities() error {
	if owner.authority == nil {
		return nil
	}
	for _, group := range owner.groups {
		publication, err := owner.host.Publication(group)
		if err != nil {
			return err
		}
		if err := owner.authority.PublishCommittedAuthority(
			group, publication.ReplicaSetVersion, publication.ConfState,
		); err != nil {
			return err
		}
		member := owner.members[group]
		if member.authority.TargetMember == 0 {
			continue
		}
		proof, found, err := owner.host.DurablePromotion(group,
			member.authority.TargetMember)
		if err != nil {
			return err
		}
		if found {
			if err = owner.authority.PublishDurablePromotion(group, proof); err != nil {
				return err
			}
		} else if err = owner.authority.ClearDurablePromotion(group); err != nil {
			return err
		}
	}
	return nil
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
		if reply.err == nil {
			publication, publicationErr := owner.host.Publication(request.group)
			if publicationErr != nil {
				reply.err = publicationErr
			} else if publication.ReplicaSetVersion != 0 {
				member.command.ReplicaSetVersion = publication.ReplicaSetVersion
				owner.members[request.group] = member
				reply.state.Command = member.command
			}
		}
	case requestMembership:
		reply.err = owner.applyMembership(request.membership)
	default:
		reply.err = ErrInvalidOwner
	}
	request.reply <- reply
	return reply.err
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

func (owner *Owner) applyMembership(request MembershipRequest) error {
	member, found := owner.members[request.Fence.Group]
	if !found || !member.authority.Valid() {
		return ErrMembershipUnauthorized
	}
	authority := member.authority
	if err := validateMembershipIdentity(request, authority); err != nil {
		return err
	}
	publication, err := owner.host.Publication(request.Fence.Group)
	if err != nil {
		return err
	}
	member.command.ReplicaSetVersion = publication.ReplicaSetVersion
	owner.members[request.Fence.Group] = member
	if !servingFenceMatchesIdentity(request.Fence, member) ||
		request.ExpectedReplicaSetVersion != publication.ReplicaSetVersion {
		return ErrMembershipStale
	}
	status, err := owner.host.Status(request.Fence.Group)
	if err != nil {
		return err
	}
	if status.MemberID != member.identity.MemberID || status.LeaderID != member.identity.MemberID ||
		status.Term != request.Fence.Term {
		return &NotLeaderError{Status: status}
	}
	var progress raftmodel.MemberProgress
	var progressFound bool
	if request.Kind == MembershipPromoteVoter || request.Kind == MembershipTransferLeader {
		progress, progressFound, err = owner.host.Progress(request.Fence.Group, request.TargetMember)
		if err != nil {
			return err
		}
	}
	if err := validateMembershipTransition(request, authority, publication, status,
		progress, progressFound); err != nil {
		return err
	}
	authorizationDigest := raftmember.MembershipTransitionDigest(request.Fence.Group,
		authority.TransitionID, authority.MetadataEpoch, authority.CatalogGeneration,
		authority.SourceMember, authority.TargetMember)
	context := append([]byte(nil), authorizationDigest[:]...)
	switch request.Kind {
	case MembershipAddLearner:
		return owner.host.ProposeConfChange(request.Fence.Group, &pb.ConfChange{
			Type: pb.ConfChangeAddLearnerNode.Enum(), NodeId: &request.TargetMember, Context: context,
		})
	case MembershipPromoteVoter:
		return owner.host.ProposeConfChange(request.Fence.Group, &pb.ConfChange{
			Type: pb.ConfChangeAddNode.Enum(), NodeId: &request.TargetMember, Context: context,
		})
	case MembershipTransferLeader:
		return owner.host.TransferLeader(request.Fence.Group, request.TargetMember)
	case MembershipRemoveVoter:
		// Removal is authorized only after the target is the observed leader.
		// This makes deleting the active leader unrepresentable on this wire.
		return owner.host.ProposeConfChange(request.Fence.Group, &pb.ConfChange{
			Type: pb.ConfChangeRemoveNode.Enum(), NodeId: &request.SourceMember, Context: context,
		})
	default:
		return ErrMembershipMalformed
	}
}

func validateMembershipTransition(
	request MembershipRequest,
	authority MembershipAuthorization,
	publication raftmodel.Publication,
	status raftmember.RuntimeStatus,
	progress raftmodel.MemberProgress,
	progressFound bool,
) error {
	if err := validateMembershipIdentity(request, authority); err != nil {
		return err
	}
	if publication.ConfState == nil || request.ExpectedReplicaSetVersion != publication.ReplicaSetVersion ||
		!containsSorted(publication.ConfState.GetVoters(), request.SourceMember) {
		return ErrMembershipStale
	}
	voters := publication.ConfState.GetVoters()
	learners := publication.ConfState.GetLearners()
	switch request.Kind {
	case MembershipAddLearner:
		if containsSorted(voters, request.TargetMember) || containsSorted(learners, request.TargetMember) {
			return ErrMembershipStale
		}
	case MembershipPromoteVoter:
		if !containsSorted(learners, request.TargetMember) || containsSorted(voters, request.TargetMember) {
			return ErrMembershipStale
		}
		if !caughtUp(progress, progressFound, status.Commit, true) {
			return ErrMembershipNotCaughtUp
		}
	case MembershipTransferLeader:
		if status.LeaderID != request.SourceMember || !containsSorted(voters, request.TargetMember) {
			return ErrMembershipStale
		}
		if !caughtUp(progress, progressFound, status.Commit, false) {
			return ErrMembershipNotCaughtUp
		}
	case MembershipRemoveVoter:
		if status.LeaderID != request.TargetMember || status.MemberID != request.TargetMember ||
			status.Term != request.TransferTerm ||
			!containsSorted(voters, request.TargetMember) {
			return ErrMembershipStale
		}
	}
	return nil
}

func validateMembershipIdentity(
	request MembershipRequest,
	authority MembershipAuthorization,
) error {
	if request.Kind < MembershipAddLearner || request.Kind > MembershipTransferLeader ||
		request.ExpectedReplicaSetVersion == 0 || request.SourceMember == 0 ||
		request.TargetMember == 0 || request.SourceMember == request.TargetMember {
		return ErrMembershipMalformed
	}
	if (request.Kind == MembershipRemoveVoter) != (request.TransferTerm != 0) {
		return ErrMembershipMalformed
	}
	if request.TransitionID != authority.TransitionID ||
		request.MetadataEpoch != authority.MetadataEpoch ||
		request.CatalogGeneration != authority.CatalogGeneration ||
		request.SourceMember != authority.SourceMember ||
		request.TargetMember != authority.TargetMember {
		return ErrMembershipUnauthorized
	}
	return nil
}

func caughtUp(progress raftmodel.MemberProgress, found bool, commit uint64, learner bool) bool {
	return found && progress.Learner == learner && progress.RecentActive &&
		progress.PendingSnapshot == 0 && progress.Match >= commit
}

func containsSorted(values []uint64, member uint64) bool {
	low, high := 0, len(values)
	for low < high {
		middle := int(uint(low+high) >> 1)
		if values[middle] < member {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low < len(values) && values[low] == member
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

// ApplyMembership serializes one fixed-size metadata-authorized control
// request with proposals, Ready handling, and peer ingress. A nil error means
// Raft accepted the control input; the caller must observe its exact applied
// ReplicaSetVersion before issuing the next transition.
func (owner *Owner) ApplyMembership(ctx context.Context, request MembershipRequest) error {
	if ctx == nil {
		return ErrInvalidOwner
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	_, err := owner.enqueue(ctx, ownerRequest{
		kind: requestMembership, group: request.Fence.Group,
		membership: request, reply: make(chan ownerReply, 1),
	})
	if err != nil && context.Cause(ctx) != nil {
		return errors.Join(ErrOutcomeUnknown, err)
	}
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
