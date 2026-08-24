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
	Submit(context.Context, raftservice.ServingFence, []byte) (raftservice.Result, error)
}

// ReplicatedServer is the SQL-free RF3 shard endpoint. Serve owns bounded
// connection admission; connection authentication remains an explicit outer
// listener capability.
type ReplicatedServer struct {
	owner replicatedOwner
	state atomic.Uint32

	accepted atomic.Uint64
	rejected atomic.Uint64
	failed   atomic.Uint64
	active   atomic.Uint64
}

const AbsoluteMaxReplicatedConnections = 65536

const (
	replicatedServerReady uint32 = iota
	replicatedServerRunning
	replicatedServerClosed
)

// ReplicatedServerStats is an allocation-free detached listener snapshot.
type ReplicatedServerStats struct {
	Accepted uint64
	Rejected uint64
	Failed   uint64
	Active   uint64
}

// NewReplicatedServer binds the shipped native protocol to one serialized
// owner lane.
func NewReplicatedServer(owner *raftservice.Owner) (*ReplicatedServer, error) {
	if owner == nil {
		return nil, ErrReplicatedWire
	}
	return &ReplicatedServer{owner: owner}, nil
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
	}
}

// ServeReplicatedConn serves sequential native requests until EOF or the first
// framing/transport error. The caller owns authentication and closes conn.
func (server *ReplicatedServer) ServeReplicatedConn(
	ctx context.Context,
	conn net.Conn,
) error {
	if server == nil || server.owner == nil || ctx == nil || conn == nil {
		return ErrReplicatedWire
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	for {
		request, err := DecodeReplicatedRequest(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		response := server.executeReplicated(ctx, request)
		if err := EncodeReplicatedResponse(conn, response); err != nil {
			return err
		}
	}
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

	result, err := server.owner.Submit(ctx, raftservice.ServingFence{
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
		return &ReplicatedResponse{
			Kind: ReplicatedCompletion, HasState: true, State: wireState,
			Outcome: result.Outcome, Completion: result.Completion,
		}
	}
	// Refresh the leader hint after a definite owner-lane refusal. The refresh
	// cannot turn an admitted unknown result into a definite claim.
	if refreshed, refreshErr := server.owner.Probe(ctx, request.Fence.Group); refreshErr == nil {
		wireState = replicatedWireState(refreshed)
	}
	switch {
	case errors.Is(err, raftmodel.ErrNotLeader):
		return &ReplicatedResponse{Kind: ReplicatedNotLeader, HasState: true, State: wireState}
	case errors.Is(err, raftservice.ErrOutcomeUnknown):
		return &ReplicatedResponse{Kind: ReplicatedOutcomeUnknown, HasState: true, State: wireState}
	case errors.Is(err, raftservice.ErrServingFence):
		return &ReplicatedResponse{
			Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalStaleFence,
			HasState: true, State: wireState,
		}
	case result.Outcome.Code != raftserve.OutcomePending:
		return &ReplicatedResponse{
			Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalDeterministic,
			HasState: true, State: wireState, Outcome: result.Outcome,
		}
	case errors.Is(err, raftservice.ErrIngressFull):
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
		return nil, err
	}
	response, err := DecodeReplicatedResponse(conn)
	if err != nil && request.Operation == ReplicatedPropose {
		return nil, &raftservice.UnknownOutcomeError{
			Command: append([]byte(nil), request.Command...), Cause: err,
		}
	}
	return response, err
}
