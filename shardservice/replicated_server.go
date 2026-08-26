package shardservice

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

type replicatedOwner interface {
	Probe(context.Context, raftmember.GroupKey) (raftservice.ServingState, error)
	SubmitOwned(context.Context, raftservice.ServingFence, []byte) (raftservice.Result, error)
	ApplyMembership(context.Context, raftservice.MembershipRequest) error
	ReadPoint(context.Context, raftservice.PointReadRequest) (raftservice.PointReadResult, raftservice.PointReadLease, error)
	ReadTransaction(context.Context, raftservice.TransactionReadRequest) (raftservice.TransactionReadResult, raftservice.TransactionReadLease, error)
}

func replicatedRequestDigest(command []byte) [sha256.Size]byte {
	return sha256.Sum256(command)
}

// ReplicatedServer is the SQL-free RF3 shard endpoint. Serve owns bounded
// connection admission; connection authentication remains an explicit outer
// listener capability.
type ReplicatedServer struct {
	owner          replicatedOwner
	state          atomic.Uint32
	requestTimeout time.Duration
	frames         replicatedFrameByteBudget
	authorization  *serviceauthz.Gate
	audit          serviceauthz.AuditSink

	accepted      atomic.Uint64
	rejected      atomic.Uint64
	failed        atomic.Uint64
	active        atomic.Uint64
	frameRejected atomic.Uint64

	proposalUnknownSubmit               atomic.Uint64
	proposalUnknownAbandoned            atomic.Uint64
	proposalInvalidCompletion           atomic.Uint64
	proposalInvalidDeterministic        atomic.Uint64
	proposalInvalidCompletionReasons    atomic.Uint64
	proposalInvalidDeterministicReasons atomic.Uint64
	proposalInvalidDeterministicCode    atomic.Uint32
	proposalInvalidDeterministicApplied atomic.Uint64
	proposalInvalidDeterministicState   atomic.Uint64
}

// BindAuthorization installs the sole production authorization gate before
// the listener starts. Policy rotation occurs atomically through Gate.Rotate;
// every subsequent request observes one complete generation.
func (server *ReplicatedServer) BindAuthorization(
	gate *serviceauthz.Gate,
	audit serviceauthz.AuditSink,
) error {
	if server == nil || gate == nil || server.state.Load() != replicatedServerReady {
		return ErrReplicatedWire
	}
	server.authorization, server.audit = gate, audit
	return nil
}

const (
	AbsoluteMaxReplicatedConnections        = 65536
	AbsoluteMaxReplicatedInFlightFrameBytes = int64(1 << 30)
	AbsoluteMaxReplicatedRequestTimeout     = 5 * time.Minute
)

type replicatedFrameByteBudget struct {
	limit int64
	used  atomic.Int64
}

func (budget *replicatedFrameByteBudget) reserve(bytes int64) bool {
	if budget == nil || bytes <= 0 || bytes > budget.limit {
		return false
	}
	for {
		used := budget.used.Load()
		if used < 0 || bytes > budget.limit-used {
			return false
		}
		if budget.used.CompareAndSwap(used, used+bytes) {
			return true
		}
	}
}

func (budget *replicatedFrameByteBudget) release(bytes int64) {
	if budget == nil || bytes <= 0 {
		return
	}
	budget.used.Add(-bytes)
}

const (
	replicatedServerReady uint32 = iota
	replicatedServerRunning
	replicatedServerClosed
)

// ReplicatedServerStats is an allocation-free detached listener snapshot.
type ReplicatedServerStats struct {
	Accepted           uint64
	Rejected           uint64
	Failed             uint64
	Active             uint64
	FrameRejected      uint64
	InFlightFrameBytes int64

	ProposalUnknownSubmit               uint64
	ProposalUnknownAbandoned            uint64
	ProposalInvalidCompletion           uint64
	ProposalInvalidDeterministic        uint64
	ProposalInvalidCompletionReasons    ReplicatedCompletionInvalidReason
	ProposalInvalidDeterministicReasons ReplicatedDeterministicInvalidReason
	ProposalInvalidDeterministicCode    raftserve.OutcomeCode
	ProposalInvalidDeterministicApplied uint64
	ProposalInvalidDeterministicState   uint64
}

// ReplicatedDeterministicInvalidReason identifies the exact canonical-response
// predicate that rejected a deterministic applied outcome. The serving path
// records bits and numeric witnesses only; it does not allocate diagnostic
// strings or retain command bytes.
type ReplicatedDeterministicInvalidReason uint64

const (
	ReplicatedDeterministicInvalidState ReplicatedDeterministicInvalidReason = 1 << iota
	ReplicatedDeterministicInvalidCode
	ReplicatedDeterministicInvalidAppliedIndex
	ReplicatedDeterministicInvalidStateBehind
	ReplicatedDeterministicInvalidCompletionSequence
	ReplicatedDeterministicInvalidCompletionBytes
)

// ReplicatedCompletionInvalidReason is an allocation-free diagnostic bit set
// for a completed proposal that could not be represented by the canonical
// completion response. Multiple bits may be present. These values describe
// only server-side invariant failures; ordinary unknown outcomes have separate
// counters in ReplicatedServerStats.
type ReplicatedCompletionInvalidReason uint64

const (
	ReplicatedCompletionInvalidNil ReplicatedCompletionInvalidReason = 1 << iota
	ReplicatedCompletionInvalidState
	ReplicatedCompletionInvalidCompletionBound
	ReplicatedCompletionInvalidCompletionBytes
	ReplicatedCompletionInvalidValueBound
	ReplicatedCompletionInvalidEnvelope
	ReplicatedCompletionInvalidSequence
	ReplicatedCompletionInvalidClusterID
	ReplicatedCompletionInvalidClusterIncarnation
	ReplicatedCompletionInvalidTopologyRecoveryEpoch
	ReplicatedCompletionInvalidShardIncarnation
	ReplicatedCompletionInvalidGroupID
	ReplicatedCompletionInvalidAllocationGeneration
	ReplicatedCompletionInvalidReplicaSetVersion
	ReplicatedCompletionInvalidActivePolicyGeneration
	ReplicatedCompletionInvalidProtectionEpoch
	ReplicatedCompletionInvalidRoutingVersion
	ReplicatedCompletionInvalidRouteGeneration
	ReplicatedCompletionInvalidKind
	ReplicatedCompletionInvalidRefusal
	ReplicatedCompletionInvalidRequestDigest
	ReplicatedCompletionInvalidOutcomeCode
	ReplicatedCompletionInvalidAppliedIndex
	ReplicatedCompletionInvalidEmptyCompletion
	ReplicatedCompletionInvalidReadApplied
	ReplicatedCompletionInvalidValue
	ReplicatedCompletionInvalidStateBehind
)

// NewReplicatedServer binds the native RF3 protocol to one serialized owner
// lane. Command construction must still explicitly supply authenticated client
// and peer listeners before this becomes a public serving boundary.
func NewReplicatedServer(
	owner *raftservice.Owner,
	maxInFlightFrameBytes int64,
	requestTimeout time.Duration,
) (*ReplicatedServer, error) {
	if owner == nil || maxInFlightFrameBytes <= 0 ||
		maxInFlightFrameBytes > AbsoluteMaxReplicatedInFlightFrameBytes ||
		requestTimeout <= 0 || requestTimeout > AbsoluteMaxReplicatedRequestTimeout {
		return nil, ErrReplicatedWire
	}
	return &ReplicatedServer{owner: owner, requestTimeout: requestTimeout,
		frames: replicatedFrameByteBudget{limit: maxInFlightFrameBytes}}, nil
}

// ServeLoopbackDevelopment accepts a bounded number of unauthenticated native
// connections only on an explicit loopback listener. Production serving uses
// ServeAuthenticated. There is no
// user-space accept queue: a connection above maxConnections is closed
// immediately. Each admitted connection decodes at most one bounded frame at a
// time and submits through the sole Owner lane. The caller must provide either
// an authenticated listener or a loopback-only development listener.
func (server *ReplicatedServer) ServeLoopbackDevelopment(
	ctx context.Context,
	listener net.Listener,
	maxConnections int,
) error {
	if server == nil || server.owner == nil || ctx == nil || listener == nil ||
		maxConnections <= 0 || maxConnections > AbsoluteMaxReplicatedConnections ||
		!replicatedLoopbackListener(listener) ||
		!server.state.CompareAndSwap(replicatedServerReady, replicatedServerRunning) {
		return ErrReplicatedWire
	}
	stop := context.AfterFunc(ctx, func() { _ = listener.Close() })
	defer stop()
	defer server.state.Store(replicatedServerClosed)
	if context.Cause(ctx) != nil {
		_ = listener.Close()
	}

	slots := make(chan struct{}, maxConnections)
	var connections sync.WaitGroup
	defer func() {
		_ = listener.Close()
		connections.Wait()
	}()
	for {
		connection, err := listener.Accept()
		if err != nil {
			if cause := context.Cause(ctx); cause != nil {
				return cause
			}
			return err
		}
		select {
		case slots <- struct{}{}:
			server.accepted.Add(1)
			server.active.Add(1)
			connections.Add(1)
			go func(connection net.Conn) {
				defer connections.Done()
				defer func() {
					<-slots
					server.active.Add(^uint64(0))
				}()
				defer connection.Close()
				if err := server.ServeReplicatedConn(ctx, connection); err != nil &&
					context.Cause(ctx) == nil {
					server.failed.Add(1)
				}
			}(connection)
		default:
			server.rejected.Add(1)
			_ = connection.Close()
		}
	}
}

func replicatedLoopbackListener(listener net.Listener) bool {
	address, ok := listener.Addr().(*net.TCPAddr)
	return ok && address.IP != nil && address.IP.IsLoopback()
}

// Stats returns listener counters without touching the Owner lane.
func (server *ReplicatedServer) Stats() ReplicatedServerStats {
	if server == nil {
		return ReplicatedServerStats{}
	}
	return ReplicatedServerStats{
		Accepted: server.accepted.Load(), Rejected: server.rejected.Load(),
		Failed: server.failed.Load(), Active: server.active.Load(),
		FrameRejected:                server.frameRejected.Load(),
		InFlightFrameBytes:           server.frames.used.Load(),
		ProposalUnknownSubmit:        server.proposalUnknownSubmit.Load(),
		ProposalUnknownAbandoned:     server.proposalUnknownAbandoned.Load(),
		ProposalInvalidCompletion:    server.proposalInvalidCompletion.Load(),
		ProposalInvalidDeterministic: server.proposalInvalidDeterministic.Load(),
		ProposalInvalidCompletionReasons: ReplicatedCompletionInvalidReason(
			server.proposalInvalidCompletionReasons.Load()),
		ProposalInvalidDeterministicReasons: ReplicatedDeterministicInvalidReason(
			server.proposalInvalidDeterministicReasons.Load()),
		ProposalInvalidDeterministicCode: raftserve.OutcomeCode(
			server.proposalInvalidDeterministicCode.Load()),
		ProposalInvalidDeterministicApplied: server.proposalInvalidDeterministicApplied.Load(),
		ProposalInvalidDeterministicState:   server.proposalInvalidDeterministicState.Load(),
	}
}

// ServeReplicatedConn serves sequential native requests until EOF or the first
// framing/transport error. The caller owns authentication and closes conn.
func (server *ReplicatedServer) ServeReplicatedConn(
	ctx context.Context,
	conn net.Conn,
) error {
	return server.serveReplicatedConn(ctx, conn, rafttransport.NodeID{}, false)
}

func (server *ReplicatedServer) serveReplicatedConn(
	ctx context.Context,
	conn net.Conn,
	peer rafttransport.NodeID,
	authenticated bool,
) error {
	if server == nil || server.owner == nil || ctx == nil || conn == nil ||
		server.requestTimeout <= 0 || server.frames.limit <= 0 {
		return ErrReplicatedWire
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	for {
		err := server.serveReplicatedRequestAuthorized(ctx, conn, peer, authenticated)
		if err != nil {
			if errors.Is(err, errFrameBudget) {
				server.frameRejected.Add(1)
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

func (server *ReplicatedServer) serveReplicatedRequest(
	ctx context.Context,
	conn net.Conn,

) error {
	return server.serveReplicatedRequestAuthorized(ctx, conn, rafttransport.NodeID{}, false)
}

func (server *ReplicatedServer) serveReplicatedRequestAuthorized(
	ctx context.Context,
	conn net.Conn,
	peer rafttransport.NodeID,
	authenticated bool,
) error {
	requestCtx, cancel := context.WithTimeout(ctx, server.requestTimeout)
	defer cancel()
	deadline, _ := requestCtx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return err
	}
	request, charged, err := decodeReplicatedRequest(conn, &server.frames)
	if err != nil {
		return err
	}
	defer server.frames.release(charged)
	if authenticated {
		if !server.authorizeReplicated(peer, request) {
			return EncodeReplicatedResponse(conn, &ReplicatedResponse{
				Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalUnauthorized,
			})
		}
	}
	response := server.executeReplicated(requestCtx, request)
	if response.readLease != nil {
		defer response.readLease.Release()
	}
	return EncodeReplicatedResponse(conn, response)
}

func (server *ReplicatedServer) authorizeReplicated(
	peer rafttransport.NodeID,
	request *ReplicatedRequest,
) bool {
	if server == nil || server.authorization == nil || request == nil ||
		peer == (rafttransport.NodeID{}) {
		return false
	}
	generation := request.Authority.Generation
	if serviceauthz.CheckAndAudit(server.authorization, server.audit, peer, generation,
		serviceauthz.CapabilityDelegate) != serviceauthz.DecisionAllow {
		return false
	}
	return serviceauthz.CheckAndAudit(server.authorization, server.audit,
		request.Authority.Node, generation, request.Capability) == serviceauthz.DecisionAllow
}

func (server *ReplicatedServer) executeReplicated(
	ctx context.Context,
	request *ReplicatedRequest,
) *ReplicatedResponse {
	state, stateErr := server.owner.Probe(ctx, request.Fence.Group)
	wireState := replicatedWireState(state)
	if stateErr != nil {
		return &ReplicatedResponse{
			Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalUnavailable,
		}
	}
	if request.Fence.AllocationGeneration != state.Identity.AllocationGeneration {
		return &ReplicatedResponse{
			Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalStaleFence,
			HasState: true, State: wireState,
		}
	}
	if request.Operation == ReplicatedProbe {
		return &ReplicatedResponse{Kind: ReplicatedHandshake, HasState: true, State: wireState}
	}
	if request.Operation == ReplicatedMembership {
		err := server.owner.ApplyMembership(ctx, raftservice.MembershipRequest{
			Fence: raftservice.ServingFence{
				Group: request.Fence.Group, AllocationGeneration: request.Fence.AllocationGeneration,
				Command: request.Fence.Command, MemberID: request.Fence.MemberID,
				StoreID: request.Fence.StoreID, NodeIncarnation: request.Fence.NodeIncarnation,
				Term: request.Fence.Term,
			},
			Kind: request.Membership.Kind, TransitionID: request.Membership.TransitionID,
			MetadataEpoch:             request.Membership.MetadataEpoch,
			CatalogGeneration:         request.Membership.CatalogGeneration,
			ExpectedReplicaSetVersion: request.Membership.ExpectedReplicaSetVersion,
			SourceMember:              request.Membership.SourceMember,
			TargetMember:              request.Membership.TargetMember,
			TransferTerm:              request.Membership.TransferTerm,
		})
		if refreshed, refreshErr := server.owner.Probe(ctx, request.Fence.Group); refreshErr == nil {
			wireState = replicatedWireState(refreshed)
		}
		if err == nil {
			return &ReplicatedResponse{Kind: ReplicatedMembershipAccepted, HasState: true, State: wireState}
		}
		switch {
		case errors.Is(err, raftmodel.ErrNotLeader):
			return &ReplicatedResponse{Kind: ReplicatedNotLeader, HasState: true, State: wireState}
		case errors.Is(err, raftservice.ErrOutcomeUnknown), errors.Is(err, context.Canceled),
			errors.Is(err, context.DeadlineExceeded):
			return &ReplicatedResponse{Kind: ReplicatedOutcomeUnknown, HasState: true, State: wireState}
		case errors.Is(err, raftservice.ErrMembershipUnauthorized):
			return membershipRefusal(wireState, ReplicatedRefusalMembershipUnauthorized)
		case errors.Is(err, raftservice.ErrMembershipStale), errors.Is(err, raftservice.ErrServingFence):
			return membershipRefusal(wireState, ReplicatedRefusalMembershipStale)
		case errors.Is(err, raftservice.ErrMembershipMalformed):
			return membershipRefusal(wireState, ReplicatedRefusalMembershipMalformed)
		case errors.Is(err, raftservice.ErrMembershipNotCaughtUp):
			return membershipRefusal(wireState, ReplicatedRefusalMembershipNotCaughtUp)
		case errors.Is(err, raftmodel.ErrAdmissionBound), errors.Is(err, raftmodel.ErrConfChangePending),
			errors.Is(err, raftmodel.ErrLeaderTransferPending):
			return membershipRefusal(wireState, ReplicatedRefusalAdmissionBound)
		default:
			return membershipRefusal(wireState, ReplicatedRefusalUnavailable)
		}
	}
	if request.Operation == ReplicatedReadLeader || request.Operation == ReplicatedReadFollower {
		result, readLease, readErr := server.owner.ReadPoint(ctx, raftservice.PointReadRequest{
			Fence: raftservice.ServingFence{
				Group: request.Fence.Group, AllocationGeneration: request.Fence.AllocationGeneration,
				Command: request.Fence.Command, MemberID: request.Fence.MemberID,
				StoreID: request.Fence.StoreID, NodeIncarnation: request.Fence.NodeIncarnation,
				Term: request.Fence.Term,
			},
			Relation: request.Relation, Key: request.Key,
			MinimumApplied: request.MinimumApplied, MaxValueBytes: int(request.MaxValueBytes),
			Linearizable: request.Operation == ReplicatedReadLeader,
		})
		if readErr != nil && readLease != nil {
			readLease.Release()
			readLease = nil
		}
		if refreshed, refreshErr := server.owner.Probe(ctx, request.Fence.Group); refreshErr == nil {
			wireState = replicatedWireState(refreshed)
		}
		if readErr == nil {
			kind := ReplicatedReadMissing
			if result.Found {
				kind = ReplicatedReadFound
			}
			response := &ReplicatedResponse{Kind: kind, HasState: true, State: wireState,
				ReadApplied: result.Applied, Value: result.Value, readLease: readLease}
			if validReplicatedResponse(response) {
				return response
			}
			if readLease != nil {
				readLease.Release()
			}
			return &ReplicatedResponse{Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalUnavailable,
				HasState: true, State: wireState}
		}
		switch {
		case errors.Is(readErr, raftmodel.ErrNotLeader),
			errors.Is(readErr, raftmodel.ErrReadLeadershipLost):
			return &ReplicatedResponse{Kind: ReplicatedNotLeader, HasState: true, State: wireState}
		case errors.Is(readErr, raftservice.ErrServingFence):
			return &ReplicatedResponse{Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalStaleFence,
				HasState: true, State: wireState}
		case errors.Is(readErr, replicatedstate.ErrReadBehind):
			return &ReplicatedResponse{Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalReadBehind,
				HasState: true, State: wireState}
		case errors.Is(readErr, replicatedstate.ErrReadBufferBound):
			return &ReplicatedResponse{Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalReadBufferBound,
				HasState: true, State: wireState}
		case errors.Is(readErr, raftservice.ErrIngressFull),
			errors.Is(readErr, raftservice.ErrPendingReadsFull):
			return &ReplicatedResponse{Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalAdmissionBound,
				HasState: true, State: wireState}
		default:
			return &ReplicatedResponse{Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalUnavailable,
				HasState: true, State: wireState}
		}
	}
	if request.Operation == ReplicatedTransactionRead {
		read, ok := replicatedTransactionRecoveryRead(request.TransactionRead)
		if !ok {
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalUnavailable, HasState: true, State: wireState}
		}
		result, readLease, readErr := server.owner.ReadTransaction(ctx, raftservice.TransactionReadRequest{
			Fence: raftservice.ServingFence{
				Group: request.Fence.Group, AllocationGeneration: request.Fence.AllocationGeneration,
				Command: request.Fence.Command, MemberID: request.Fence.MemberID,
				StoreID: request.Fence.StoreID, NodeIncarnation: request.Fence.NodeIncarnation,
				Term: request.Fence.Term,
			},
			Capability: request.Capability, Read: read,
		})
		if readErr != nil && readLease != nil {
			readLease.Release()
			readLease = nil
		}
		if refreshed, refreshErr := server.owner.Probe(ctx, request.Fence.Group); refreshErr == nil {
			wireState = replicatedWireState(refreshed)
		}
		if readErr == nil {
			value, encodeErr := AppendReplicatedTransactionReadValue(nil,
				ReplicatedTransactionReadValue{
					Kind: request.TransactionRead.Kind, Complete: result.Complete,
					Records: result.Records,
				})
			logicalBytes := len(value) - replicatedTransactionReadValueHeaderBytes
			response := &ReplicatedResponse{
				Kind: ReplicatedTransactionReadResult, HasState: true, State: wireState,
				ReadApplied: result.Applied, Value: value, readLease: readLease,
			}
			if encodeErr == nil && logicalBytes >= 0 &&
				logicalBytes <= int(request.TransactionRead.MaxBytes) &&
				result.Applied >= request.TransactionRead.MinimumApplied &&
				validReplicatedResponse(response) {
				return response
			}
			if readLease != nil {
				readLease.Release()
			}
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalUnavailable, HasState: true, State: wireState}
		}
		switch {
		case errors.Is(readErr, raftmodel.ErrNotLeader),
			errors.Is(readErr, raftmodel.ErrReadLeadershipLost):
			return &ReplicatedResponse{Kind: ReplicatedNotLeader, HasState: true, State: wireState}
		case errors.Is(readErr, raftservice.ErrServingFence):
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalStaleFence, HasState: true, State: wireState}
		case errors.Is(readErr, replicatedstate.ErrReadBehind):
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalReadBehind, HasState: true, State: wireState}
		case errors.Is(readErr, replicatedstate.ErrReadBufferBound):
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalReadBufferBound, HasState: true, State: wireState}
		case errors.Is(readErr, replicatedstate.ErrTransactionRecoveryRead):
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalTransactionReadMalformed, HasState: true, State: wireState}
		case errors.Is(readErr, raftservice.ErrTransactionRecoveryUnauthorized):
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalUnauthorized, HasState: true, State: wireState}
		case errors.Is(readErr, raftservice.ErrIngressFull),
			errors.Is(readErr, raftservice.ErrPendingReadsFull):
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalAdmissionBound, HasState: true, State: wireState}
		default:
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalUnavailable, HasState: true, State: wireState}
		}
	}

	proposalState := wireState
	// SubmitOwned revalidates this complete fence immediately before registry
	// admission. A settled result therefore authenticates the request fence
	// even if an unrelated owner-lane transition followed the initial probe.
	proposalState.Fence = request.Fence
	result, err := server.owner.SubmitOwned(ctx, raftservice.ServingFence{
		Group:                request.Fence.Group,
		AllocationGeneration: request.Fence.AllocationGeneration,
		Command:              request.Fence.Command,
		MemberID:             request.Fence.MemberID, StoreID: request.Fence.StoreID,
		NodeIncarnation: request.Fence.NodeIncarnation, Term: request.Fence.Term,
	}, request.Command)
	if err == nil {
		// Settlement is a stronger witness than a second status probe: the
		// exact command has already been observed in a published applied batch.
		// Preserve the fence that SubmitOwned accepted and advance only its
		// monotonic Raft watermarks. Besides avoiding a serialized lane RTT,
		// this cannot accidentally pair the completion with a later fence.
		wireState = replicatedStateAtApplied(proposalState, result.Outcome.AppliedIndex)
		response := &ReplicatedResponse{
			Kind: ReplicatedCompletion, HasState: true, State: wireState,
			Outcome: result.Outcome, RequestDigest: replicatedRequestDigest(request.Command),
			Completion: result.Completion,
		}
		if validReplicatedResponse(response) {
			return response
		}
		server.proposalInvalidCompletion.Add(1)
		server.proposalInvalidCompletionReasons.Or(
			uint64(replicatedCompletionInvalidReasons(response)))
		return &ReplicatedResponse{Kind: ReplicatedOutcomeUnknown, HasState: true, State: wireState}
	}
	// Refresh the leader hint after a definite owner-lane refusal. The refresh
	// cannot turn an admitted unknown result into a definite claim.
	if refreshed, refreshErr := server.owner.Probe(ctx, request.Fence.Group); refreshErr == nil {
		wireState = replicatedWireState(refreshed)
	}
	switch {
	case errors.Is(err, raftmodel.ErrNotLeader):
		return &ReplicatedResponse{Kind: ReplicatedNotLeader, HasState: true, State: wireState}
	case errors.Is(err, raftservice.ErrOutcomeUnknown),
		errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded):
		// SubmitOwned was entered with a complete canonical proposal. A local
		// deadline cannot prove whether admission won the cancellation race.
		server.proposalUnknownSubmit.Add(1)
		return &ReplicatedResponse{Kind: ReplicatedOutcomeUnknown, HasState: true, State: wireState}
	case errors.Is(err, raftservice.ErrServingFence):
		return &ReplicatedResponse{
			Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalStaleFence,
			HasState: true, State: wireState,
		}
	case result.Outcome.Code == raftserve.OutcomeProposalAbandoned ||
		errors.Is(err, raftserve.ErrProposalAbandoned):
		server.proposalUnknownAbandoned.Add(1)
		return &ReplicatedResponse{Kind: ReplicatedOutcomeUnknown, HasState: true, State: wireState}
	case result.Outcome.Code == raftserve.OutcomeProposalRefused ||
		errors.Is(err, raftserve.ErrProposalRefused):
		return &ReplicatedResponse{
			Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalProposalRefused,
			HasState: true, State: wireState,
		}
	case result.Outcome == (raftserve.Outcome{Code: raftserve.OutcomeAdmissionBound}) &&
		len(result.Completion) == 0 && errors.Is(err, replicatedstate.ErrAdmissionBound):
		// The registry can observe a bounded local-core refusal through its
		// proposal-admission callback. AppliedIndex==0 is the proof that this
		// exact command never entered Raft, so expose a definite pre-admission
		// refusal instead of misclassifying it as an invalid applied result. The
		// caller decides whether its cause is transient; malformed or oversized
		// commands can reach the same bounded refusal and must not be spun on.
		return &ReplicatedResponse{
			Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalAdmissionBound,
			HasState: true, State: wireState,
		}
	case result.Outcome.Code > raftserve.OutcomeCompletion &&
		result.Outcome.Code < raftserve.OutcomeProposalRefused:
		wireState = replicatedStateAtApplied(proposalState, result.Outcome.AppliedIndex)
		response := &ReplicatedResponse{
			Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalDeterministic,
			HasState: true, State: wireState, Outcome: result.Outcome,
			RequestDigest: replicatedRequestDigest(request.Command),
		}
		if validReplicatedResponse(response) {
			return response
		}
		server.proposalInvalidDeterministic.Add(1)
		server.proposalInvalidDeterministicReasons.Or(
			uint64(replicatedDeterministicInvalidReasons(response)))
		server.proposalInvalidDeterministicCode.Store(uint32(response.Outcome.Code))
		server.proposalInvalidDeterministicApplied.Store(response.Outcome.AppliedIndex)
		server.proposalInvalidDeterministicState.Store(response.State.Applied)
		return &ReplicatedResponse{Kind: ReplicatedOutcomeUnknown, HasState: true, State: wireState}
	case errors.Is(err, raftservice.ErrIngressFull),
		errors.Is(err, raftservice.ErrPendingProposalsFull):
		return &ReplicatedResponse{
			Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalAdmissionBound,
			HasState: true, State: wireState,
		}
	default:
		return &ReplicatedResponse{
			Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalUnavailable,
			HasState: true, State: wireState,
		}
	}
}

func replicatedStateAtApplied(
	state ReplicatedMemberState,
	applied uint64,
) ReplicatedMemberState {
	if state.Applied < applied {
		state.Applied = applied
	}
	if state.Commit < applied {
		state.Commit = applied
	}
	return state
}

func replicatedDeterministicInvalidReasons(
	response *ReplicatedResponse,
) ReplicatedDeterministicInvalidReason {
	if response == nil {
		return ReplicatedDeterministicInvalidState | ReplicatedDeterministicInvalidCode |
			ReplicatedDeterministicInvalidAppliedIndex
	}
	reasons := ReplicatedDeterministicInvalidReason(0)
	if !response.HasState || !validReplicatedMemberState(response.State) {
		reasons |= ReplicatedDeterministicInvalidState
	}
	if response.Outcome.Code <= raftserve.OutcomeCompletion ||
		response.Outcome.Code >= raftserve.OutcomeProposalRefused {
		reasons |= ReplicatedDeterministicInvalidCode
	}
	if response.Outcome.AppliedIndex == 0 {
		reasons |= ReplicatedDeterministicInvalidAppliedIndex
	}
	if response.HasState && response.Outcome.AppliedIndex != 0 &&
		response.State.Applied < response.Outcome.AppliedIndex {
		reasons |= ReplicatedDeterministicInvalidStateBehind
	}
	if response.Outcome.CompletionAppliedSequence != 0 {
		reasons |= ReplicatedDeterministicInvalidCompletionSequence
	}
	if response.Outcome.CompletionBytes != 0 {
		reasons |= ReplicatedDeterministicInvalidCompletionBytes
	}
	return reasons
}

func replicatedCompletionInvalidReasons(
	response *ReplicatedResponse,
) ReplicatedCompletionInvalidReason {
	if response == nil {
		return ReplicatedCompletionInvalidNil
	}
	reasons := ReplicatedCompletionInvalidReason(0)
	if !response.HasState || !validReplicatedMemberState(response.State) {
		reasons |= ReplicatedCompletionInvalidState
	}
	if len(response.Completion) > replicatedstate.MaxCompletionEnvelopeBytes {
		reasons |= ReplicatedCompletionInvalidCompletionBound
	}
	if response.Outcome.CompletionBytes != len(response.Completion) {
		reasons |= ReplicatedCompletionInvalidCompletionBytes
	}
	if len(response.Value) > replication.MaxMutationValueBytes {
		reasons |= ReplicatedCompletionInvalidValueBound
	}
	completion, err := replication.OpenCompletion(response.Completion)
	if err != nil {
		reasons |= ReplicatedCompletionInvalidEnvelope
	} else {
		if completion.AppliedSequence != response.Outcome.CompletionAppliedSequence {
			reasons |= ReplicatedCompletionInvalidSequence
		}
		if completion.ClusterID != response.State.Fence.Group.ClusterID {
			reasons |= ReplicatedCompletionInvalidClusterID
		}
		if completion.ClusterIncarnation != response.State.Fence.Group.ClusterIncarnation {
			reasons |= ReplicatedCompletionInvalidClusterIncarnation
		}
		if completion.TopologyRecoveryEpoch != response.State.Fence.Group.TopologyRecoveryEpoch {
			reasons |= ReplicatedCompletionInvalidTopologyRecoveryEpoch
		}
		if completion.ShardIncarnation != response.State.Fence.Group.ShardIncarnation {
			reasons |= ReplicatedCompletionInvalidShardIncarnation
		}
		if completion.GroupID != response.State.Fence.Group.GroupID {
			reasons |= ReplicatedCompletionInvalidGroupID
		}
		if completion.AllocationGeneration != response.State.Fence.AllocationGeneration {
			reasons |= ReplicatedCompletionInvalidAllocationGeneration
		}
		if completion.ReplicaSetVersion != response.State.Fence.Command.ReplicaSetVersion {
			reasons |= ReplicatedCompletionInvalidReplicaSetVersion
		}
		if completion.ActivePolicyGeneration != response.State.Fence.Command.ActivePolicyGeneration {
			reasons |= ReplicatedCompletionInvalidActivePolicyGeneration
		}
		if completion.ProtectionEpoch != response.State.Fence.Command.ProtectionEpoch {
			reasons |= ReplicatedCompletionInvalidProtectionEpoch
		}
		if completion.RoutingVersion != response.State.Fence.Command.RoutingVersion {
			reasons |= ReplicatedCompletionInvalidRoutingVersion
		}
		if completion.RouteGeneration != response.State.Fence.Command.RouteGeneration {
			reasons |= ReplicatedCompletionInvalidRouteGeneration
		}
	}
	if response.Kind != ReplicatedCompletion {
		reasons |= ReplicatedCompletionInvalidKind
	}
	if response.Refusal != ReplicatedRefusalNone {
		reasons |= ReplicatedCompletionInvalidRefusal
	}
	if response.RequestDigest == ([sha256.Size]byte{}) {
		reasons |= ReplicatedCompletionInvalidRequestDigest
	}
	if response.Outcome.Code != raftserve.OutcomeCompletion {
		reasons |= ReplicatedCompletionInvalidOutcomeCode
	}
	if response.Outcome.AppliedIndex == 0 {
		reasons |= ReplicatedCompletionInvalidAppliedIndex
	}
	if len(response.Completion) == 0 {
		reasons |= ReplicatedCompletionInvalidEmptyCompletion
	}
	if response.ReadApplied != 0 {
		reasons |= ReplicatedCompletionInvalidReadApplied
	}
	if len(response.Value) != 0 {
		reasons |= ReplicatedCompletionInvalidValue
	}
	if response.HasState && response.Outcome.AppliedIndex != 0 &&
		response.State.Applied < response.Outcome.AppliedIndex {
		reasons |= ReplicatedCompletionInvalidStateBehind
	}
	return reasons
}

func membershipRefusal(state ReplicatedMemberState, code ReplicatedRefusalCode) *ReplicatedResponse {
	return &ReplicatedResponse{Kind: ReplicatedRefusal, Refusal: code, HasState: true, State: state}
}

func replicatedWireState(state raftservice.ServingState) ReplicatedMemberState {
	return ReplicatedMemberState{
		Fence: ReplicatedFence{
			Group:                state.Identity.Group,
			AllocationGeneration: state.Identity.AllocationGeneration,
			Command:              state.Command,
			MemberID:             state.Identity.MemberID, StoreID: state.Identity.StoreID,
			NodeIncarnation: state.Identity.NodeIncarnation, Term: state.Status.Term,
		},
		LeaderID: state.Status.LeaderID, Commit: state.Status.Commit,
		Applied: state.Status.Applied, CheckpointApplied: state.Status.CheckpointApplied,
	}
}

// RoundTripReplicated performs one exact request/response exchange. Any
// Propose transport error is outcome-unknown because the peer may have admitted
// the complete frame before the connection failed.
func RoundTripReplicated(
	ctx context.Context,
	conn net.Conn,
	request *ReplicatedRequest,
) (*ReplicatedResponse, error) {
	if ctx == nil || conn == nil || request == nil {
		return nil, ErrReplicatedWire
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return nil, err
		}
	}
	cancelDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = conn.SetDeadline(time.Now())
		close(cancelDone)
	})
	defer func() {
		if !stop() {
			<-cancelDone
		}
	}()
	if err := EncodeReplicatedRequestBorrowed(conn, request); err != nil {
		if request.Operation == ReplicatedPropose {
			return nil, &raftservice.UnknownOutcomeError{
				Command: append([]byte(nil), request.Command...), Cause: err,
			}
		}
		if request.Operation == ReplicatedMembership {
			return nil, errors.Join(raftservice.ErrOutcomeUnknown, err)
		}
		return nil, err
	}
	maximumResponse, boundErr := maximumReplicatedResponseBody(request)
	if boundErr != nil {
		return nil, boundErr
	}
	response, err := decodeReplicatedResponseLimit(conn, maximumResponse)
	if err != nil && request.Operation == ReplicatedPropose {
		return nil, &raftservice.UnknownOutcomeError{
			Command: append([]byte(nil), request.Command...), Cause: err,
		}
	}
	if err != nil && request.Operation == ReplicatedMembership {
		return nil, errors.Join(raftservice.ErrOutcomeUnknown, err)
	}
	return response, err
}
