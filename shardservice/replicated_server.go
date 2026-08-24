package shardservice

import (
	"context"
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
)

type replicatedOwner interface {
	Probe(context.Context, raftmember.GroupKey) (raftservice.ServingState, error)
	SubmitOwned(context.Context, raftservice.ServingFence, []byte) (raftservice.Result, error)
	ApplyMembership(context.Context, raftservice.MembershipRequest) error
}

// ReplicatedServer is the SQL-free RF3 shard endpoint. Serve owns bounded
// connection admission; connection authentication remains an explicit outer
// listener capability.
type ReplicatedServer struct {
	owner          replicatedOwner
	state          atomic.Uint32
	requestTimeout time.Duration
	frames         replicatedFrameByteBudget

	accepted      atomic.Uint64
	rejected      atomic.Uint64
	failed        atomic.Uint64
	active        atomic.Uint64
	frameRejected atomic.Uint64
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
}

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

// Serve accepts a bounded number of native gateway connections. There is no
// user-space accept queue: a connection above maxConnections is closed
// immediately. Each admitted connection decodes at most one bounded frame at a
// time and submits through the sole Owner lane. The caller must provide either
// an authenticated listener or a loopback-only development listener.
func (server *ReplicatedServer) Serve(
	ctx context.Context,
	listener net.Listener,
	maxConnections int,
) error {
	if server == nil || server.owner == nil || ctx == nil || listener == nil ||
		maxConnections <= 0 || maxConnections > AbsoluteMaxReplicatedConnections ||
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

// Stats returns listener counters without touching the Owner lane.
func (server *ReplicatedServer) Stats() ReplicatedServerStats {
	if server == nil {
		return ReplicatedServerStats{}
	}
	return ReplicatedServerStats{
		Accepted: server.accepted.Load(), Rejected: server.rejected.Load(),
		Failed: server.failed.Load(), Active: server.active.Load(),
		FrameRejected:      server.frameRejected.Load(),
		InFlightFrameBytes: server.frames.used.Load(),
	}
}

// ServeReplicatedConn serves sequential native requests until EOF or the first
// framing/transport error. The caller owns authentication and closes conn.
func (server *ReplicatedServer) ServeReplicatedConn(
	ctx context.Context,
	conn net.Conn,
) error {
	if server == nil || server.owner == nil || ctx == nil || conn == nil ||
		server.requestTimeout <= 0 || server.frames.limit <= 0 {
		return ErrReplicatedWire
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	for {
		err := server.serveReplicatedRequest(ctx, conn)
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
	response := server.executeReplicated(requestCtx, request)
	return EncodeReplicatedResponse(conn, response)
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

	result, err := server.owner.SubmitOwned(ctx, raftservice.ServingFence{
		Group:                request.Fence.Group,
		AllocationGeneration: request.Fence.AllocationGeneration,
		Command:              request.Fence.Command,
		MemberID:             request.Fence.MemberID, StoreID: request.Fence.StoreID,
		NodeIncarnation: request.Fence.NodeIncarnation, Term: request.Fence.Term,
	}, request.Command)
	if err == nil {
		if refreshed, refreshErr := server.owner.Probe(ctx, request.Fence.Group); refreshErr == nil {
			wireState = replicatedWireState(refreshed)
		}
		response := &ReplicatedResponse{
			Kind: ReplicatedCompletion, HasState: true, State: wireState,
			Outcome: result.Outcome, Completion: result.Completion,
		}
		if validReplicatedResponse(response) {
			return response
		}
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
		return &ReplicatedResponse{Kind: ReplicatedOutcomeUnknown, HasState: true, State: wireState}
	case errors.Is(err, raftservice.ErrServingFence):
		return &ReplicatedResponse{
			Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalStaleFence,
			HasState: true, State: wireState,
		}
	case result.Outcome.Code == raftserve.OutcomeProposalAbandoned ||
		errors.Is(err, raftserve.ErrProposalAbandoned):
		return &ReplicatedResponse{Kind: ReplicatedOutcomeUnknown, HasState: true, State: wireState}
	case result.Outcome.Code == raftserve.OutcomeProposalRefused ||
		errors.Is(err, raftserve.ErrProposalRefused):
		return &ReplicatedResponse{
			Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalProposalRefused,
			HasState: true, State: wireState,
		}
	case result.Outcome.Code > raftserve.OutcomeCompletion &&
		result.Outcome.Code < raftserve.OutcomeProposalRefused:
		response := &ReplicatedResponse{
			Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalDeterministic,
			HasState: true, State: wireState, Outcome: result.Outcome,
		}
		if validReplicatedResponse(response) {
			return response
		}
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
		defer conn.SetDeadline(time.Time{})
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()) })
	defer stop()
	if err := EncodeReplicatedRequest(conn, request); err != nil {
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
	response, err := DecodeReplicatedResponse(conn)
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
