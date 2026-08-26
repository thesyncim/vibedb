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

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
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
	ErrOutcomeUnknown                  = errors.New("raftservice: admitted command outcome is unknown")
	ErrMembershipUnauthorized          = errors.New("raftservice: membership transition is not authorized")
	ErrMembershipStale                 = errors.New("raftservice: membership transition is stale")
	ErrMembershipMalformed             = errors.New("raftservice: membership transition is malformed")
	ErrMembershipNotCaughtUp           = errors.New("raftservice: membership target is not caught up")
	ErrTransactionRecoveryUnauthorized = errors.New(
		"raftservice: transaction recovery is not authorized",
	)
	ErrRequestLedgerUnauthorized = errors.New(
		"raftservice: request ledger recovery is not authorized",
	)
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
	Registry                   *raftserve.Registry
	Host                       *multiraft.Host
	Members                    []raftmember.RuntimeIdentity
	CommandFences              []CommandFence
	ReadSources                []ReadSource
	TransactionRecoverySources []TransactionRecoverySource
	MembershipAuthority        MembershipAuthority
	Outbound                   OutboundSink
	Pulse                      <-chan struct{}
	Limits                     Limits
}

type requestKind uint8

const (
	requestProposal requestKind = iota + 1
	requestInbound
	requestCampaign
	requestStatus
	requestMembership
	requestReadLinear
	requestReadFollower
	requestReadTransaction
	requestReadRequestLedger
	requestReplicaObservation
	requestOwnershipTransition
	requestReplicaRetirement
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
	kind         requestKind
	group        raftmember.GroupKey
	data         []byte
	fence        ServingFence
	inbound      rafttransport.Inbound
	reply        chan ownerReply
	bytes        int64
	async        bool
	delivery     *proposalDelivery
	membership   MembershipRequest
	read         readRequest
	targetMember uint64
	operation    [32]byte
	step         [32]byte
	sourceMember uint64
}

type ownerReply struct {
	waiter      raftserve.Waiter
	state       ServingState
	err         error
	read        readAuthorization
	observation ReplicaObservation
}

// ReplicaObservation is one coherent control-plane cut collected by the sole
// serialized Host owner. Publication and State are durable apply evidence;
// Status and TargetProgress are transient liveness evidence and never grant
// serving or membership authority by themselves.
type ReplicaObservation struct {
	Publication    raftmodel.Publication
	Status         raftmember.RuntimeStatus
	TargetProgress raftmodel.MemberProgress
	ProgressFound  bool
	State          replicatedstate.State
	SnapshotBase   *replicatedstate.SnapshotBaseCertificate
}

type readRequest struct {
	fence          ServingFence
	minimumApplied uint64
	delivery       *readDelivery
}

type readAuthorization struct {
	source         ReadSource
	recovery       TransactionRecoverySource
	requestLedger  RequestLedgerSource
	minimumApplied uint64
}

type readDelivery struct {
	state          atomic.Uint32
	reply          chan ownerReply
	source         ReadSource
	recovery       TransactionRecoverySource
	requestLedger  RequestLedgerSource
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

// BatchReadSource is the optional packed multi-relation extension. Keeping it
// separate preserves narrow test and recovery sources while shipped replicated
// machines expose one coherent all-relation cut.
type BatchReadSource interface {
	PointReadBatchInto([]byte, uint64, int, []byte) (replicatedstate.PointReadBatchResult, error)
}

// TransactionRecoverySource exposes only the replicated transaction-control
// reader. It is intentionally separate from ordinary relation reads so
// installing a data-read source cannot enable coordinator discovery.
type TransactionRecoverySource interface {
	TransactionRecoveryReadInto(
		replicatedstate.TransactionRecoveryReadRequest,
		[]replicatedstate.TransactionRecoveryRecord,
		[]byte,
	) (replicatedstate.TransactionRecoveryReadResult, error)
}

// RequestLedgerSource exposes one exact full-key hidden-ledger row read. It is
// intentionally a separate narrow interface so transaction recovery or data
// read authority cannot name the private request-ledger collection.
type RequestLedgerSource interface {
	RequestLedgerReadInto(
		replicatedstate.RequestLedgerReadRequest,
		[]byte,
	) (replicatedstate.RequestLedgerReadResult, error)
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

// PointReadBatchRequest selects one leader ReadIndex for a packed positional
// multi-relation request. Packed is the canonical replicatedstate grammar and
// MaxResultBytes bounds its complete bitmap/length/value response.
type PointReadBatchRequest struct {
	Fence          ServingFence
	Packed         []byte
	MinimumApplied uint64
	MaxResultBytes int
}

type PointReadBatchResult struct {
	Applied uint64
	Data    []byte
}

// PointReadLease holds the conservative response-memory reservation until the
// serving boundary has finished encoding and writing the result.
type PointReadLease interface {
	Release()
}

// TransactionReadRequest wraps the exact replicated-state recovery operation
// with the live RF3 serving fence. Capability must be the one dedicated
// recovery bit; ordinary data or topology authority is never accepted here.
type TransactionReadRequest struct {
	Fence      ServingFence
	Capability serviceauthz.Capability
	Read       replicatedstate.TransactionRecoveryReadRequest
}

// TransactionReadResult owns Records and every record payload. Applied is the
// exact state-machine cut reached after the quorum ReadIndex barrier.
type TransactionReadResult struct {
	Applied  uint64
	Complete bool
	Records  []replicatedstate.TransactionRecoveryRecord
}

// TransactionReadLease holds the conservative response and scratch-memory
// reservation until the native serving boundary finishes encoding the result.
type TransactionReadLease interface {
	Release()
}

// RequestLedgerReadRequest wraps the full-key replicated-state read with the
// exact live RF3 serving fence. Only the dedicated service capability may use
// this path.
type RequestLedgerReadRequest struct {
	Fence      ServingFence
	Capability serviceauthz.Capability
	Read       replicatedstate.RequestLedgerReadRequest
}

type RequestLedgerReadResult struct {
	Applied           uint64
	Found             bool
	AuthoritativeKind replicatedstate.RequestLedgerReadKind
	Value             []byte
}

type RequestLedgerReadLease interface {
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

const transactionRecoveryRecordRetainedBytes int64 = 256

const (
	requestLedgerReadEncodedFrameFixedBytes int64 = 5 + 309 + 12
	requestLedgerReadAllocatorSlopBytes     int64 = 3 * ((8 << 10) - 1)
)

// requestLedgerReadResponseCharge covers all three simultaneously live
// payload-sized buffers: state-machine row arena, typed native value, and the
// final encoded response frame. The lease is released only after the serving
// boundary writes the frame.
func requestLedgerReadResponseCharge(maximum int) (int64, bool) {
	if maximum <= 0 {
		return 0, false
	}
	const maxInt64 = int64(^uint64(0) >> 1)
	fixed := requestLedgerReadEncodedFrameFixedBytes + requestLedgerReadAllocatorSlopBytes
	payload := int64(maximum)
	if payload > (maxInt64-fixed)/3 {
		return 0, false
	}
	return payload*3 + fixed, true
}

func transactionReadResponseCharge(
	request replicatedstate.TransactionRecoveryReadRequest,
) (charge int64, records int, scratch int, ok bool) {
	if err := replicatedstate.ValidateTransactionRecoveryReadRequest(request); err != nil {
		return 0, 0, 0, false
	}
	records = 1
	switch request.Kind {
	case replicatedstate.TransactionRecoveryLookupCoordinator,
		replicatedstate.TransactionRecoveryReadManifestPage:
		scratch = replicatedstate.MaxTransactionRecoveryPayloadArenaBytes
	case replicatedstate.TransactionRecoveryLookupParticipant:
	case replicatedstate.TransactionRecoveryScanCoordinator:
		records = int(request.MaxRows)
	default:
		return 0, 0, 0, false
	}
	encoded, valid := pointReadResponseCharge(int(request.MaxBytes))
	if !valid || records <= 0 {
		return 0, 0, 0, false
	}
	retained := int64(records)*transactionRecoveryRecordRetainedBytes + int64(scratch)
	const maxInt64 = int64(^uint64(0) >> 1)
	if retained < 0 || encoded > maxInt64-retained {
		return 0, 0, 0, false
	}
	return encoded + retained, records, scratch, true
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
	registry  *raftserve.Registry
	host      *multiraft.Host
	groups    []raftmember.GroupKey
	members   map[raftmember.GroupKey]ownerMember
	outbound  OutboundSink
	pulse     <-chan struct{}
	limits    Limits
	authority MembershipAuthority

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

type MembershipAuthority interface {
	CurrentTransitionGrant(raftmember.GroupKey) (membershipgrant.Grant, bool, error)
	PublishCommittedAuthority(raftmember.GroupKey, uint64, *pb.ConfState) error
	PublishDurablePromotion(raftmember.GroupKey, raftmember.DurablePromotionProof) error
	ClearDurablePromotion(raftmember.GroupKey) error
}

type ownerMember struct {
	identity raftmember.RuntimeIdentity
	command  CommandFence
	read     ReadSource
	recovery TransactionRecoverySource
	retiring bool
}

// ReplicaRetirementRequest is the exact final local-source fence for one
// replicated replica move. Operation and Step bind the call to its durable
// controller journal; the Owner independently proves that SourceMember is no
// longer a voter and no longer the current leader before closing the Runtime.
// TargetMember remains the replacement identity bound by the grant. No caller
// receives raw Host access.
type ReplicaRetirementRequest struct {
	Operation    [32]byte
	Step         [32]byte
	Fence        ServingFence
	SourceMember uint64
	TargetMember uint64
}

type MembershipKind uint8

const (
	MembershipAddLearner MembershipKind = iota + 1
	MembershipPromoteVoter
	MembershipRemoveVoter
	MembershipTransferLeader
)

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

// ValidateMembershipFields rejects every malformed fixed-width control
// envelope independently of metadata authorization or live Raft state. It is
// shared by the gateway admission edge and serialized owner so malformed
// requests cannot consume discovery/network work and cannot bypass the owner
// when submitted through an in-process path.
func ValidateMembershipFields(
	kind MembershipKind,
	transitionID [16]byte,
	metadataEpoch, catalogGeneration, expectedReplicaSetVersion uint64,
	sourceMember, targetMember, transferTerm uint64,
) error {
	if kind < MembershipAddLearner || kind > MembershipTransferLeader ||
		transitionID == ([16]byte{}) || metadataEpoch == 0 || catalogGeneration == 0 ||
		expectedReplicaSetVersion == 0 || sourceMember == 0 || targetMember == 0 ||
		sourceMember == targetMember ||
		(kind == MembershipRemoveVoter) != (transferTerm != 0) {
		return ErrMembershipMalformed
	}
	return nil
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
	if len(options.TransactionRecoverySources) != 0 &&
		len(options.TransactionRecoverySources) != len(options.Members) {
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
		var recovery TransactionRecoverySource
		if len(options.TransactionRecoverySources) != 0 {
			recovery = options.TransactionRecoverySources[index]
		}
		members[group] = ownerMember{identity: identity, command: options.CommandFences[index],
			read: source, recovery: recovery}
	}
	return &Owner{
		registry: options.Registry, host: options.Host, groups: groups, members: members,
		outbound: options.Outbound, pulse: options.Pulse, limits: limits,
		authority:    options.MembershipAuthority,
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
				if err := owner.syncMembershipAuthorities(); err != nil {
					return owner.stop(err)
				}
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
		grant, grantFound, err := owner.authority.CurrentTransitionGrant(group)
		if err != nil {
			return err
		}
		if !grantFound {
			continue
		}
		proof, found, err := owner.host.DurablePromotion(group,
			grant.TargetMember)
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
	case requestReplicaObservation:
		if request.targetMember == 0 {
			reply.err = ErrInvalidOwner
			break
		}
		if _, found := owner.members[request.group]; !found {
			reply.err = multiraft.ErrGroupNotFound
			break
		}
		reply.observation.Status, reply.err = owner.host.Status(request.group)
		if reply.err == nil {
			reply.observation.Publication, reply.err = owner.host.Publication(request.group)
		}
		if reply.err == nil {
			reply.observation.TargetProgress, reply.observation.ProgressFound, reply.err =
				owner.host.Progress(request.group, request.targetMember)
		}
		if reply.err == nil {
			reply.observation.State, reply.err = owner.host.SnapshotState(request.group)
		}
		if reply.err == nil {
			certificate, certificateErr := owner.host.SnapshotBaseCertificate(request.group)
			if certificateErr != nil && !errors.Is(certificateErr, replicatedstate.ErrSnapshotBase) {
				reply.err = certificateErr
			} else if certificate.Digest == reply.observation.State.SnapshotBaseDigest &&
				stateMatchesReplicaGroup(certificate.Manifest.State, request.group) &&
				certificate.Manifest.State.ConfState != nil &&
				containsSorted(certificate.Manifest.State.ConfState.GetLearners(), request.targetMember) &&
				!containsSorted(certificate.Manifest.State.ConfState.GetVoters(), request.targetMember) {
				reply.observation.SnapshotBase = &certificate
			}
		}
		if reply.err == nil {
			reply.err = owner.syncCommandFenceFromState(request.group, reply.observation)
		}
	case requestOwnershipTransition:
		reply.err = owner.applyOwnershipTransition(request.fence, request.data)
	case requestReplicaRetirement:
		reply.err = owner.retireReplica(request)
	case requestReadLinear, requestReadFollower, requestReadTransaction, requestReadRequestLedger:
		member, found := owner.members[request.group]
		if !found ||
			!servingFenceMatchesIdentity(request.read.fence, member) {
			reply.err = ErrServingFence
			break
		}
		if request.kind == requestReadTransaction {
			if member.recovery == nil {
				reply.err = ErrServingFence
				break
			}
		} else if request.kind == requestReadRequestLedger {
			if _, ok := member.recovery.(RequestLedgerSource); !ok {
				reply.err = ErrServingFence
				break
			}
		} else if member.read == nil {
			reply.err = ErrServingFence
			break
		}
		status, err := owner.host.Status(request.group)
		if err != nil {
			reply.err = err
			break
		}
		if status.MemberID != member.identity.MemberID {
			reply.err = ErrServingFence
			break
		}
		if status.Term != request.read.fence.Term {
			reply.err = &NotLeaderError{Status: status}
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
		request.read.delivery.recovery = member.recovery
		request.read.delivery.requestLedger, _ = member.recovery.(RequestLedgerSource)
		request.read.delivery.minimumApplied = request.read.minimumApplied
		owner.pendingReads[context] = request.read.delivery
		// The reply is settled only by the matching quorum barrier.
		return nil
	default:
		reply.err = ErrInvalidOwner
	}
	if (request.kind == requestReadLinear || request.kind == requestReadTransaction ||
		request.kind == requestReadRequestLedger) &&
		request.read.delivery != nil {
		owner.settleReadDelivery(request.read.delivery, reply)
	} else {
		request.reply <- reply
	}
	return reply.err
}

func (owner *Owner) syncCommandFenceFromState(
	group raftmember.GroupKey,
	observation ReplicaObservation,
) error {
	member, found := owner.members[group]
	if !found {
		return multiraft.ErrGroupNotFound
	}
	binding := observation.State.Binding
	if binding.ClusterID != group.ClusterID || binding.ClusterIncarnation != group.ClusterIncarnation ||
		binding.TopologyRecoveryEpoch != group.TopologyRecoveryEpoch ||
		binding.ShardIncarnation != group.ShardIncarnation || binding.GroupID != group.GroupID ||
		binding.AllocationGeneration != member.identity.AllocationGeneration ||
		observation.Publication.ReplicaSetVersion == 0 ||
		observation.State.ReplicaSetVersion != observation.Publication.ReplicaSetVersion ||
		binding.ActivePolicyGeneration == 0 || binding.ProtectionEpoch == 0 ||
		binding.OwnershipEpoch == 0 || binding.SchemaGeneration == 0 ||
		binding.RoutingVersion == 0 || binding.RouteGeneration == 0 {
		return ErrServingFence
	}
	member.command.ReplicaSetVersion = observation.Publication.ReplicaSetVersion
	member.command.ActivePolicyGeneration = binding.ActivePolicyGeneration
	member.command.ProtectionEpoch = binding.ProtectionEpoch
	member.command.OwnershipEpoch = binding.OwnershipEpoch
	member.command.SchemaGeneration = binding.SchemaGeneration
	member.command.RoutingVersion = binding.RoutingVersion
	member.command.RouteGeneration = binding.RouteGeneration
	owner.members[group] = member
	return nil
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
			reply.read.recovery = delivery.recovery
			reply.read.requestLedger = delivery.requestLedger
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
	return !member.retiring && fence.Group == identity.Group &&
		fence.AllocationGeneration == identity.AllocationGeneration &&
		fence.Command == member.command &&
		fence.MemberID == identity.MemberID && fence.StoreID == identity.StoreID &&
		fence.NodeIncarnation == identity.NodeIncarnation && fence.Term != 0
}

func (owner *Owner) applyOwnershipTransition(fence ServingFence, command []byte) error {
	member, found := owner.members[fence.Group]
	if !found || !servingFenceMatchesIdentity(fence, member) {
		return ErrServingFence
	}
	transition, err := replicatedstate.OpenOwnershipTransition(command)
	if err != nil || !ownershipTransitionMatchesFence(transition, fence) {
		return errors.Join(err, ErrServingFence)
	}
	publication, err := owner.host.Publication(fence.Group)
	if err != nil || publication.ReplicaSetVersion != transition.ExpectedReplicaSetVersion {
		return errors.Join(err, ErrServingFence)
	}
	status, err := owner.host.Status(fence.Group)
	if err != nil {
		return err
	}
	if status.MemberID != member.identity.MemberID || status.LeaderID != member.identity.MemberID ||
		status.Term != fence.Term {
		return &NotLeaderError{Status: status}
	}
	return owner.host.EnqueueProposal(fence.Group, command)
}

func ownershipTransitionMatchesFence(
	transition replicatedstate.OwnershipTransitionView,
	fence ServingFence,
) bool {
	return transition.ClusterID == fence.Group.ClusterID &&
		transition.ClusterIncarnation == fence.Group.ClusterIncarnation &&
		transition.TopologyRecoveryEpoch == fence.Group.TopologyRecoveryEpoch &&
		transition.ShardIncarnation == fence.Group.ShardIncarnation &&
		transition.GroupID == fence.Group.GroupID &&
		transition.AllocationGeneration == fence.AllocationGeneration &&
		transition.ExpectedReplicaSetVersion == fence.Command.ReplicaSetVersion &&
		transition.ActivePolicyGeneration == fence.Command.ActivePolicyGeneration &&
		transition.ProtectionEpoch == fence.Command.ProtectionEpoch &&
		transition.OwnershipEpoch == fence.Command.OwnershipEpoch &&
		transition.SchemaGeneration == fence.Command.SchemaGeneration &&
		transition.RoutingVersion == fence.Command.RoutingVersion &&
		transition.RouteGeneration == fence.Command.RouteGeneration
}

func (owner *Owner) retireReplica(request ownerRequest) error {
	member, found := owner.members[request.group]
	if !found {
		return multiraft.ErrGroupNotFound
	}
	// A retry after ErrGroupBusy remains authorized by the exact latched member
	// identity, but the retiring bit permanently fences data serving.
	wasRetiring := member.retiring
	member.retiring = false
	validFence := servingFenceMatchesIdentity(request.fence, member)
	member.retiring = wasRetiring
	if !validFence || request.operation == ([32]byte{}) || request.step == ([32]byte{}) ||
		request.sourceMember != member.identity.MemberID || request.targetMember == 0 ||
		request.targetMember == request.sourceMember {
		return ErrServingFence
	}
	publication, err := owner.host.Publication(request.group)
	if err != nil || publication.ReplicaSetVersion != request.fence.Command.ReplicaSetVersion {
		return errors.Join(err, ErrServingFence)
	}
	state, err := owner.host.SnapshotState(request.group)
	if err != nil || !retirementStateMatches(state, request.fence, request.sourceMember,
		request.targetMember) {
		return errors.Join(err, ErrServingFence)
	}
	status, err := owner.host.Status(request.group)
	if err != nil {
		return err
	}
	if status.LeaderID == request.sourceMember || status.Term != request.fence.Term {
		return &NotLeaderError{Status: status}
	}
	member.retiring = true
	owner.members[request.group] = member
	if err = owner.host.Remove(request.group); err != nil {
		return err
	}
	delete(owner.members, request.group)
	for index, group := range owner.groups {
		if group != request.group {
			continue
		}
		copy(owner.groups[index:], owner.groups[index+1:])
		owner.groups = owner.groups[:len(owner.groups)-1]
		break
	}
	return nil
}

func retirementStateMatches(
	state replicatedstate.State,
	fence ServingFence,
	sourceMember, targetMember uint64,
) bool {
	binding := state.Binding
	if binding.ClusterID != fence.Group.ClusterID ||
		binding.ClusterIncarnation != fence.Group.ClusterIncarnation ||
		binding.TopologyRecoveryEpoch != fence.Group.TopologyRecoveryEpoch ||
		binding.ShardIncarnation != fence.Group.ShardIncarnation ||
		binding.GroupID != fence.Group.GroupID ||
		binding.AllocationGeneration != fence.AllocationGeneration ||
		binding.ActivePolicyGeneration != fence.Command.ActivePolicyGeneration ||
		binding.ProtectionEpoch != fence.Command.ProtectionEpoch ||
		binding.OwnershipEpoch != fence.Command.OwnershipEpoch ||
		binding.SchemaGeneration != fence.Command.SchemaGeneration ||
		binding.RoutingVersion != fence.Command.RoutingVersion ||
		binding.RouteGeneration != fence.Command.RouteGeneration ||
		state.ReplicaSetVersion != fence.Command.ReplicaSetVersion ||
		len(state.ConfState.GetVotersOutgoing()) != 0 ||
		len(state.ConfState.GetLearnersNext()) != 0 || state.ConfState.GetAutoLeave() {
		return false
	}
	voters := state.ConfState.GetVoters()
	return !containsSorted(voters, sourceMember) && containsSorted(voters, targetMember)
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
	if !found || owner.authority == nil {
		return ErrMembershipUnauthorized
	}
	authority, authorityFound, err := owner.authority.CurrentTransitionGrant(request.Fence.Group)
	if err != nil || !authorityFound {
		return errors.Join(err, ErrMembershipUnauthorized)
	}
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
	if request.Kind == MembershipPromoteVoter || request.Kind == MembershipTransferLeader ||
		request.Kind == MembershipRemoveVoter {
		progress, progressFound, err = owner.host.Progress(request.Fence.Group, request.TargetMember)
		if err != nil {
			return err
		}
	}
	if err := validateMembershipTransition(request, authority, publication, status,
		progress, progressFound); err != nil {
		return err
	}
	authorizationDigest := authority.Digest()
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
		// The serialized owner is necessarily the live leader. Validation proves
		// it is not the retiring source and that the replacement is a caught-up
		// voter, without needlessly forcing leadership onto that replacement.
		return owner.host.ProposeConfChange(request.Fence.Group, &pb.ConfChange{
			Type: pb.ConfChangeRemoveNode.Enum(), NodeId: &request.SourceMember, Context: context,
		})
	default:
		return ErrMembershipMalformed
	}
}

func validateMembershipTransition(
	request MembershipRequest,
	authority membershipgrant.Grant,
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
		if status.LeaderID == request.SourceMember || status.Term != request.TransferTerm ||
			len(voters) != len(authority.InitialVoters)+1 || len(learners) != 0 ||
			len(publication.ConfState.GetVotersOutgoing()) != 0 ||
			len(publication.ConfState.GetLearnersNext()) != 0 ||
			publication.ConfState.GetAutoLeave() ||
			!containsSorted(voters, request.TargetMember) ||
			!caughtUp(progress, progressFound, status.Commit, false) {
			return ErrMembershipStale
		}
	}
	return nil
}

func validateMembershipIdentity(
	request MembershipRequest,
	authority membershipgrant.Grant,
) error {
	if err := ValidateMembershipFields(
		request.Kind, request.TransitionID, request.MetadataEpoch, request.CatalogGeneration,
		request.ExpectedReplicaSetVersion, request.SourceMember, request.TargetMember,
		request.TransferTerm,
	); err != nil {
		return err
	}
	if request.Fence.Group != authority.Group ||
		request.TransitionID != authority.TransitionID ||
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

// ReadPointBatch authorizes exactly one leader ReadIndex and then reads one
// coherent all-relation snapshot outside the serialized Raft lane. The source
// performs a complete intent pass before returning any value bytes.
func (owner *Owner) ReadPointBatch(
	ctx context.Context,
	request PointReadBatchRequest,
) (PointReadBatchResult, PointReadLease, error) {
	packed, packedErr := replicatedstate.OpenPointReadBatch(request.Packed)
	if owner == nil || ctx == nil || packedErr != nil || packed.Count() == 0 ||
		request.MinimumApplied == 0 || request.MaxResultBytes <= 0 ||
		request.MaxResultBytes > replicatedstate.MaxPointReadBatchBytes {
		return PointReadBatchResult{}, nil, ErrInvalidOwner
	}
	responseCharge, ok := pointReadResponseCharge(request.MaxResultBytes)
	if !ok {
		return PointReadBatchResult{}, nil, ErrInvalidOwner
	}
	if err := owner.reservePendingRead(responseCharge); err != nil {
		return PointReadBatchResult{}, nil, err
	}
	releaseReservation := true
	defer func() {
		if releaseReservation {
			owner.releasePendingRead(responseCharge)
		}
	}()
	owned := append([]byte(nil), request.Packed...)
	delivery := &readDelivery{reply: make(chan ownerReply, 1)}
	reply, err := owner.enqueueRead(ctx, ownerRequest{
		kind: requestReadLinear, group: request.Fence.Group, reply: delivery.reply,
		bytes: int64(cap(owned)), read: readRequest{
			fence: request.Fence, minimumApplied: request.MinimumApplied,
			delivery: delivery,
		},
	}, delivery)
	if err != nil {
		return PointReadBatchResult{}, nil, err
	}
	source, ok := reply.read.source.(BatchReadSource)
	if !ok {
		return PointReadBatchResult{}, nil, ErrInvalidOwner
	}
	value, err := source.PointReadBatchInto(
		owned, reply.read.minimumApplied, request.MaxResultBytes, nil,
	)
	if err != nil {
		return PointReadBatchResult{}, nil, err
	}
	if !pointReadFenceMatches(value.Fence, request.Fence) ||
		value.Fence.Applied < reply.read.minimumApplied ||
		len(value.Data) > request.MaxResultBytes {
		return PointReadBatchResult{}, nil, ErrServingFence
	}
	if _, err := replicatedstate.OpenPointReadBatchValue(value.Data); err != nil {
		return PointReadBatchResult{}, nil, ErrServingFence
	}
	releaseReservation = false
	return PointReadBatchResult{Applied: value.Fence.Applied, Data: value.Data},
		&pointReadLease{owner: owner, bytes: responseCharge}, nil
}

// ReadTransaction serves one bounded transaction-control recovery read after
// the existing leader-only quorum ReadIndex path authorizes its exact serving
// fence. The source executes outside the serialized Host lane against the
// resulting minimum applied cut; it cannot create a follower or lease read.
func (owner *Owner) ReadTransaction(
	ctx context.Context,
	request TransactionReadRequest,
) (TransactionReadResult, TransactionReadLease, error) {
	if owner == nil || ctx == nil {
		return TransactionReadResult{}, nil, ErrInvalidOwner
	}
	if request.Capability != serviceauthz.CapabilityTransactionRecovery {
		return TransactionReadResult{}, nil, ErrTransactionRecoveryUnauthorized
	}
	responseCharge, maxRecords, scratchBytes, ok := transactionReadResponseCharge(request.Read)
	if !ok {
		return TransactionReadResult{}, nil, ErrInvalidOwner
	}
	if err := owner.reservePendingRead(responseCharge); err != nil {
		return TransactionReadResult{}, nil, err
	}
	releaseReservation := true
	defer func() {
		if releaseReservation {
			owner.releasePendingRead(responseCharge)
		}
	}()
	delivery := &readDelivery{reply: make(chan ownerReply, 1)}
	reply, err := owner.enqueueRead(ctx, ownerRequest{
		kind: requestReadTransaction, group: request.Fence.Group, reply: delivery.reply,
		read: readRequest{
			fence: request.Fence, minimumApplied: request.Read.MinimumApplied,
			delivery: delivery,
		},
	}, delivery)
	if err != nil {
		return TransactionReadResult{}, nil, err
	}
	records := make([]replicatedstate.TransactionRecoveryRecord, 0, maxRecords)
	payload := make([]byte, 0, scratchBytes)
	value, err := reply.read.recovery.TransactionRecoveryReadInto(
		request.Read, records, payload,
	)
	if err != nil {
		return TransactionReadResult{}, nil, err
	}
	if !pointReadFenceMatches(value.Fence, request.Fence) ||
		value.Fence.Applied < reply.read.minimumApplied ||
		len(value.Records) > maxRecords {
		return TransactionReadResult{}, nil, ErrServingFence
	}
	returnedBytes := len(value.Records) * replicatedstate.TransactionRecoverySummaryBytes
	for index := range value.Records {
		if len(value.Records[index].Payload) > int(request.Read.MaxBytes)-returnedBytes {
			return TransactionReadResult{}, nil, ErrServingFence
		}
		returnedBytes += len(value.Records[index].Payload)
	}
	if returnedBytes > int(request.Read.MaxBytes) {
		return TransactionReadResult{}, nil, ErrServingFence
	}
	value.Records = value.Records[:len(value.Records):len(value.Records)]
	releaseReservation = false
	return TransactionReadResult{
		Applied: value.Fence.Applied, Complete: value.Complete, Records: value.Records,
	}, &pointReadLease{owner: owner, bytes: responseCharge}, nil
}

// ReadRequestLedger serves one bounded hidden-ledger row after the same
// leader-only quorum ReadIndex barrier used by transaction recovery. The full
// RequestKey and immutable range identity are revalidated inside the state
// machine after the barrier; digest-only reads are impossible.
func (owner *Owner) ReadRequestLedger(
	ctx context.Context,
	request RequestLedgerReadRequest,
) (RequestLedgerReadResult, RequestLedgerReadLease, error) {
	if owner == nil || ctx == nil {
		return RequestLedgerReadResult{}, nil, ErrInvalidOwner
	}
	if request.Capability != serviceauthz.CapabilityRequestLedger {
		return RequestLedgerReadResult{}, nil, ErrRequestLedgerUnauthorized
	}
	if replicatedstate.ValidateRequestLedgerReadRequest(request.Read) != nil {
		return RequestLedgerReadResult{}, nil, ErrInvalidOwner
	}
	responseCharge, ok := requestLedgerReadResponseCharge(int(request.Read.MaxBytes))
	if !ok {
		return RequestLedgerReadResult{}, nil, ErrInvalidOwner
	}
	if err := owner.reservePendingRead(responseCharge); err != nil {
		return RequestLedgerReadResult{}, nil, err
	}
	releaseReservation := true
	defer func() {
		if releaseReservation {
			owner.releasePendingRead(responseCharge)
		}
	}()
	delivery := &readDelivery{reply: make(chan ownerReply, 1)}
	reply, err := owner.enqueueRead(ctx, ownerRequest{
		kind: requestReadRequestLedger, group: request.Fence.Group, reply: delivery.reply,
		read: readRequest{
			fence: request.Fence, minimumApplied: request.Read.MinimumApplied,
			delivery: delivery,
		},
	}, delivery)
	if err != nil {
		return RequestLedgerReadResult{}, nil, err
	}
	dst := make([]byte, 0, request.Read.MaxBytes)
	value, err := reply.read.requestLedger.RequestLedgerReadInto(request.Read, dst)
	if err != nil {
		return RequestLedgerReadResult{}, nil, err
	}
	if !pointReadFenceMatches(value.Fence, request.Fence) ||
		value.Fence.Applied < reply.read.minimumApplied ||
		len(value.Value) > int(request.Read.MaxBytes) {
		return RequestLedgerReadResult{}, nil, ErrServingFence
	}
	value.Value = value.Value[:len(value.Value):len(value.Value)]
	releaseReservation = false
	return RequestLedgerReadResult{
		Applied: value.Fence.Applied, Found: value.Found,
		AuthoritativeKind: value.AuthoritativeKind, Value: value.Value,
	}, &pointReadLease{owner: owner, bytes: responseCharge}, nil
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

// ProposeOwnershipTransition admits one canonical ownership transition through
// the serialized Owner lane. A nil result means the exact bytes entered the
// bounded Host queue, not that they applied. Callers settle the outcome from
// ObserveReplica and must replay the same operation/step on uncertainty.
func (owner *Owner) ProposeOwnershipTransition(
	ctx context.Context,
	fence ServingFence,
	command []byte,
) error {
	if owner == nil || ctx == nil || len(command) == 0 ||
		len(command) > replicatedstate.MaxOwnershipTransitionBytes {
		return ErrInvalidOwner
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	owned := make([]byte, len(command))
	copy(owned, command)
	reply := make(chan ownerReply, 1)
	_, err := owner.enqueue(ctx, ownerRequest{
		kind: requestOwnershipTransition, group: fence.Group, fence: fence,
		data: owned, reply: reply, bytes: int64(len(owned)),
	})
	if err != nil && context.Cause(ctx) != nil {
		return errors.Join(ErrOutcomeUnknown, err)
	}
	return err
}

// RetireReplicaSource permanently fences the local member before removing its
// quiescent Runtime through the sole Host owner. ErrGroupBusy is retryable; the
// serving fence remains latched while the lane drains. ErrGroupNotFound is
// settled only by a caller that already durably journaled this exact request.
func (owner *Owner) RetireReplicaSource(
	ctx context.Context,
	request ReplicaRetirementRequest,
) error {
	if owner == nil || ctx == nil || request.Operation == ([32]byte{}) ||
		request.Step == ([32]byte{}) || request.SourceMember == 0 ||
		request.TargetMember == 0 || request.SourceMember == request.TargetMember {
		return ErrInvalidOwner
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	_, err := owner.enqueue(ctx, ownerRequest{
		kind: requestReplicaRetirement, group: request.Fence.Group, fence: request.Fence,
		operation: request.Operation, step: request.Step, sourceMember: request.SourceMember,
		targetMember: request.TargetMember, reply: make(chan ownerReply, 1),
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

// ObserveReplica collects the applied membership, local durable state, leader
// status, transfer fields, and target replication progress in one serialized
// Host turn. A follower returns ProgressFound=false because it has no
// authoritative leader progress tracker.
func (owner *Owner) ObserveReplica(
	ctx context.Context,
	group raftmember.GroupKey,
	targetMember uint64,
) (ReplicaObservation, error) {
	if ctx == nil || targetMember == 0 {
		return ReplicaObservation{}, ErrInvalidOwner
	}
	reply, err := owner.enqueue(ctx, ownerRequest{
		kind: requestReplicaObservation, group: group, targetMember: targetMember,
		reply: make(chan ownerReply, 1),
	})
	return reply.observation, err
}

func stateMatchesReplicaGroup(state replicatedstate.State, group raftmember.GroupKey) bool {
	return [16]byte(state.Binding.ClusterID) == group.ClusterID &&
		[16]byte(state.Binding.ClusterIncarnation) == group.ClusterIncarnation &&
		state.Binding.TopologyRecoveryEpoch == group.TopologyRecoveryEpoch &&
		[16]byte(state.Binding.ShardIncarnation) == group.ShardIncarnation &&
		[16]byte(state.Binding.GroupID) == group.GroupID
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
