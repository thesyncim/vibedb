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
	ReadRequestLedger(context.Context, raftservice.RequestLedgerReadRequest) (raftservice.RequestLedgerReadResult, raftservice.RequestLedgerReadLease, error)
	ReadExecutionPin(context.Context, raftservice.ExecutionPinReadRequest) (raftservice.ExecutionPinReadResult, raftservice.ExecutionPinReadLease, error)
	ReadRouteGate(context.Context, raftservice.RouteGateReadRequest) (raftservice.RouteGateReadResult, raftservice.RouteGateReadLease, error)
}

type replicatedAuthorizedOwner interface {
	SubmitOwnedAuthorized(
		context.Context,
		raftservice.ServingFence,
		[]byte,
		raftservice.ProposalAuthorization,
	) (raftservice.Result, error)
}

func replicatedRequestDigest(command []byte) [sha256.Size]byte {
	return sha256.Sum256(command)
}

// ReplicatedServer is the native RF3 shard endpoint, including fenced SQL reads. Serve owns bounded
// connection admission; connection authentication remains an explicit outer
// listener capability.
type ReplicatedServer struct {
	owner          replicatedOwner
	state          atomic.Uint32
	requestTimeout time.Duration
	frames         replicatedFrameByteBudget
	sqlHints       replicatedSQLBudgetHints
	authorization  *serviceauthz.Gate
	audit          serviceauthz.AuditSink
	serving        func(raftservice.ServingState) bool
	transition     func(raftservice.ServingState, *ReplicatedRequest) bool
	local          atomic.Pointer[replicatedLocalBinding]

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
	semanticDispatch                    atomic.Uint64
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

// BindLocalGatewayPeerTLS binds a validated gateway credential to the local
// storage service. Storage and gateway identities are intentionally distinct:
// storageTLS supplies the exact physical NodeID used for local endpoint
// selection, while gatewayProfile supplies the authenticated delegate
// principal checked by every semantic call. Both profiles must have been
// constructed by rafttransport.NewPeerTLS and must share the same trust
// domain. The binding must be installed before the server starts and after its
// authorization gate has been installed. Rebinding the same two identities is
// permitted during rotation and cancels calls admitted under the old binding.
func (server *ReplicatedServer) BindLocalGatewayPeerTLS(
	storageTLS *ReplicatedServerTLS,
	gatewayProfile *rafttransport.PeerTLS,
) error {
	if server == nil || storageTLS == nil || gatewayProfile == nil ||
		server.authorization == nil || server.state.Load() == replicatedServerClosed {
		return ErrReplicatedWire
	}
	storageProfile, storageGeneration := storageTLS.snapshot()
	storage := storageProfile.LocalIdentity()
	principal := gatewayProfile.LocalIdentity()
	if !validReplicatedPeerIdentity(storage) || !validReplicatedPeerIdentity(principal) ||
		storage.Node == principal.Node || storage.TrustDomain != principal.TrustDomain {
		return ErrReplicatedAuthentication
	}
	gatewayProof, err := storageProfile.AuthorizePeerProfile(gatewayProfile)
	if err != nil {
		return err
	}
	storageProof, err := gatewayProfile.AuthorizePeerProfile(storageProfile)
	if err != nil {
		return err
	}
	old := server.local.Load()
	if old == nil && server.state.Load() != replicatedServerReady {
		return ErrReplicatedWire
	}
	if old != nil && (old.storage != storageTLS || old.principal != principal || old.node != storage.Node) {
		return ErrReplicatedAuthentication
	}
	bindingCtx, cancel := context.WithCancel(context.Background())
	binding := &replicatedLocalBinding{
		principal: principal, node: storage.Node, storage: storageTLS, generation: storageGeneration,
		gatewayProof: gatewayProof, storageProof: storageProof, ctx: bindingCtx, cancel: cancel,
	}
	storageTLS.mu.Lock()
	defer storageTLS.mu.Unlock()
	_, allowed := storageTLS.allow[principal.Node]
	if !allowed || storageTLS.generation != storageGeneration ||
		server.state.Load() == replicatedServerClosed || !server.local.CompareAndSwap(old, binding) {
		cancel()
		return ErrReplicatedAuthentication
	}
	if old != nil {
		old.cancel()
	}
	return nil
}

// BindServingAuthority installs an additional live committed-state gate for
// runtimes whose client-serving role can change after startup. The predicate
// is immutable once serving starts and observes the same serialized Owner cut
// used to execute the request.
func (server *ReplicatedServer) BindServingAuthority(
	serving func(raftservice.ServingState) bool,
) error {
	if server == nil || serving == nil || server.state.Load() != replicatedServerReady {
		return ErrReplicatedWire
	}
	server.serving = serving
	return nil
}

// BindTransitionalServingAuthority installs a narrow authenticated-request
// exception to the serving gate. It is intended for membership-only control
// on a pre-bound replacement voter before that replica enters the final RF3.
func (server *ReplicatedServer) BindTransitionalServingAuthority(
	transition func(raftservice.ServingState, *ReplicatedRequest) bool,
) error {
	if server == nil || transition == nil || server.state.Load() != replicatedServerReady {
		return ErrReplicatedWire
	}
	server.transition = transition
	return nil
}

const (
	AbsoluteMaxReplicatedConnections        = 65536
	AbsoluteMaxReplicatedInFlightFrameBytes = int64(1 << 30)
	AbsoluteMaxReplicatedRequestTimeout     = 5 * time.Minute

	defaultReplicatedSQLConcurrency      = int64(2)
	defaultReplicatedNativeFrameHeadroom = int64(32 << 20)
	// DefaultReplicatedInFlightFrameBytes admits two maximum 40 MiB fenced SQL
	// reservations plus both maximum 1 MiB SQL request bodies. The remaining
	// nearly 30 MiB leaves room for ordinary native traffic. SQL execution has
	// a separate 80 MiB quota so small workspaces cannot consume this headroom. It is a
	// process-wide bound shared by every RF3 group hosted by one shard process.
	DefaultReplicatedInFlightFrameBytes = defaultReplicatedSQLConcurrency*replicatedSQLMaximumReservationBytes +
		defaultReplicatedNativeFrameHeadroom
)

type replicatedFrameByteBudget struct {
	limit   int64
	used    atomic.Int64
	waiters atomic.Int32
	waitMu  sync.Mutex
	sqlUsed int64 // protected by waitMu; subset of used
	changed chan struct{}
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
	if budget.waiters.Load() != 0 {
		budget.waitMu.Lock()
		if budget.changed != nil {
			close(budget.changed)
			budget.changed = nil
		}
		budget.waitMu.Unlock()
	}
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
	SemanticDispatch                    uint64
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
	ReplicatedCompletionInvalidResult
)

// NewReplicatedServer binds the native RF3 protocol to a group-keyed serving
// capability. Command construction must still explicitly supply authenticated
// client and peer listeners before this becomes a public serving boundary.
func NewReplicatedServer(
	owner replicatedOwner,
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
		SemanticDispatch:                    server.semanticDispatch.Load(),
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
	response := server.executeReplicatedAuthenticated(requestCtx, request, authenticated)
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
	if request.Capability == serviceauthz.CapabilityRequestLedger &&
		request.Authority.Node != peer {
		// Internal ledger control is never a twice-forwarded capability. The
		// authenticated gateway service exercises its own narrow authority while
		// the end-user issuer remains sealed inside the inner Head.
		return false
	}
	return serviceauthz.CheckAndAudit(server.authorization, server.audit,
		request.Authority.Node, generation, request.Capability) == serviceauthz.DecisionAllow
}

func (server *ReplicatedServer) executeReplicated(
	ctx context.Context,
	request *ReplicatedRequest,
) *ReplicatedResponse {
	return server.executeReplicatedAuthenticated(ctx, request, false)
}

func (server *ReplicatedServer) executeReplicatedAuthenticated(
	ctx context.Context,
	request *ReplicatedRequest,
	authenticated bool,
) *ReplicatedResponse {
	return server.executeReplicatedAuthenticatedCall(ctx, request, authenticated, nil)
}

func (server *ReplicatedServer) executeReplicatedAuthenticatedCall(
	ctx context.Context,
	request *ReplicatedRequest,
	authenticated bool,
	sql *ShardRequest,
) *ReplicatedResponse {
	authorizedOwner, fusedProposal := server.owner.(replicatedAuthorizedOwner)
	fusedProposal = fusedProposal && request.Operation == ReplicatedPropose
	fusedRead := replicatedReadOperation(request.Operation)
	var readAuthorize raftservice.ProposalAuthorization
	if fusedRead {
		readAuthorize = func(candidate raftservice.ServingState) bool {
			return server.serving == nil || server.serving(candidate) ||
				(authenticated && server.transition != nil && server.transition(candidate, request))
		}
	}
	var state raftservice.ServingState
	var wireState ReplicatedMemberState
	if !fusedProposal && !fusedRead {
		var stateErr error
		state, stateErr = server.owner.Probe(ctx, request.Fence.Group)
		wireState = replicatedWireState(state)
		if stateErr != nil {
			return &ReplicatedResponse{
				Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalUnavailable,
			}
		}
		if server.serving != nil && !server.serving(state) &&
			(!authenticated || server.transition == nil || !server.transition(state, request)) {
			return &ReplicatedResponse{
				Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalUnavailable,
				HasState: true, State: wireState,
			}
		}
		if request.Fence.AllocationGeneration != state.Identity.AllocationGeneration {
			return &ReplicatedResponse{
				Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalStaleFence,
				HasState: true, State: wireState,
			}
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
	if request.Operation == ReplicatedQueryLeader {
		return server.executeReplicatedQueryCall(ctx, request, state, sql, func(candidate raftservice.ServingState) bool {
			return server.serving == nil || server.serving(candidate) ||
				(authenticated && server.transition != nil && server.transition(candidate, request))
		})
	}
	if request.Operation == ReplicatedReadBatchLeader {
		batchOwner, ok := server.owner.(interface {
			ReadPointBatch(context.Context, raftservice.PointReadBatchRequest) (raftservice.PointReadBatchResult, raftservice.PointReadLease, error)
		})
		if !ok {
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalUnavailable, HasState: true, State: wireState}
		}
		result, readLease, readErr := batchOwner.ReadPointBatch(ctx, raftservice.PointReadBatchRequest{
			Fence: raftservice.ServingFence{
				Group: request.Fence.Group, AllocationGeneration: request.Fence.AllocationGeneration,
				Command: request.Fence.Command, MemberID: request.Fence.MemberID,
				StoreID: request.Fence.StoreID, NodeIncarnation: request.Fence.NodeIncarnation,
				Term: request.Fence.Term,
			},
			Packed: request.BatchRead, MinimumApplied: request.MinimumApplied,
			MaxResultBytes: int(request.MaxValueBytes), Authorize: readAuthorize,
		})
		if readErr != nil && readLease != nil {
			readLease.Release()
			readLease = nil
		}
		if readErr == nil {
			wireState = replicatedWireState(result.State)
			wireState = replicatedReadState(wireState, request.Fence, result.Applied)
			response := &ReplicatedResponse{Kind: ReplicatedReadBatchResult,
				HasState: true, State: wireState, ReadApplied: result.Applied,
				Value: result.Data, readLease: readLease}
			if validReplicatedResponse(response) {
				return response
			}
			if readLease != nil {
				readLease.Release()
			}
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalUnavailable, HasState: true, State: wireState}
		}
		if refreshed, refreshErr := server.owner.Probe(ctx, request.Fence.Group); refreshErr == nil {
			wireState = replicatedWireState(refreshed)
		}
		switch {
		case errors.Is(readErr, raftmodel.ErrNotLeader),
			errors.Is(readErr, raftmodel.ErrReadLeadershipLost):
			return &ReplicatedResponse{Kind: ReplicatedNotLeader, HasState: true, State: wireState}
		case errors.Is(readErr, raftservice.ErrServingFence):
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalStaleFence, HasState: true, State: wireState}
		case errors.Is(readErr, raftservice.ErrServingAuthorization):
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalUnavailable, HasState: true, State: wireState}
		case errors.Is(readErr, replicatedstate.ErrReadBehind):
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalReadBehind, HasState: true, State: wireState}
		case errors.Is(readErr, replicatedstate.ErrReadBufferBound):
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalReadBufferBound, HasState: true, State: wireState}
		case errors.Is(readErr, replicatedstate.ErrTransactionIntentActive):
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalReadIntentActive, HasState: true, State: wireState}
		case errors.Is(readErr, raftservice.ErrIngressFull),
			errors.Is(readErr, raftservice.ErrPendingReadsFull):
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalAdmissionBound, HasState: true, State: wireState}
		default:
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalUnavailable, HasState: true, State: wireState}
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
			Linearizable: request.Operation == ReplicatedReadLeader, Authorize: readAuthorize,
		})
		if readErr != nil && readLease != nil {
			readLease.Release()
			readLease = nil
		}
		if readErr == nil {
			wireState = replicatedWireState(result.State)
			wireState = replicatedReadState(wireState, request.Fence, result.Applied)
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
		if refreshed, refreshErr := server.owner.Probe(ctx, request.Fence.Group); refreshErr == nil {
			wireState = replicatedWireState(refreshed)
		}
		switch {
		case errors.Is(readErr, raftmodel.ErrNotLeader),
			errors.Is(readErr, raftmodel.ErrReadLeadershipLost):
			return &ReplicatedResponse{Kind: ReplicatedNotLeader, HasState: true, State: wireState}
		case errors.Is(readErr, raftservice.ErrServingFence):
			return &ReplicatedResponse{Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalStaleFence,
				HasState: true, State: wireState}
		case errors.Is(readErr, raftservice.ErrServingAuthorization):
			return &ReplicatedResponse{Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalUnavailable,
				HasState: true, State: wireState}
		case errors.Is(readErr, replicatedstate.ErrReadBehind):
			return &ReplicatedResponse{Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalReadBehind,
				HasState: true, State: wireState}
		case errors.Is(readErr, replicatedstate.ErrReadBufferBound):
			return &ReplicatedResponse{Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalReadBufferBound,
				HasState: true, State: wireState}
		case errors.Is(readErr, replicatedstate.ErrTransactionIntentActive):
			return &ReplicatedResponse{Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalReadIntentActive,
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
			Capability: request.Capability, Read: read, Authorize: readAuthorize,
		})
		if readErr != nil && readLease != nil {
			readLease.Release()
			readLease = nil
		}
		if readErr == nil {
			wireState = replicatedWireState(result.State)
			wireState = replicatedReadState(wireState, request.Fence, result.Applied)
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
		if refreshed, refreshErr := server.owner.Probe(ctx, request.Fence.Group); refreshErr == nil {
			wireState = replicatedWireState(refreshed)
		}
		switch {
		case errors.Is(readErr, raftmodel.ErrNotLeader),
			errors.Is(readErr, raftmodel.ErrReadLeadershipLost):
			return &ReplicatedResponse{Kind: ReplicatedNotLeader, HasState: true, State: wireState}
		case errors.Is(readErr, raftservice.ErrServingFence):
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalStaleFence, HasState: true, State: wireState}
		case errors.Is(readErr, raftservice.ErrServingAuthorization):
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalUnavailable, HasState: true, State: wireState}
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
	if request.Operation == ReplicatedRequestLedgerRead {
		wireRead := request.RequestLedgerRead
		read := replicatedstate.RequestLedgerReadRequest{
			Key: wireRead.Key, ExpectedRangeIdentity: wireRead.ExpectedRangeIdentity,
			Kind: wireRead.Kind, Ordinal: wireRead.Ordinal, ContentRoot: wireRead.ContentRoot,
			MinimumApplied: wireRead.MinimumApplied, MaxBytes: wireRead.MaxBytes,
		}
		result, readLease, readErr := server.owner.ReadRequestLedger(ctx,
			raftservice.RequestLedgerReadRequest{
				Fence: raftservice.ServingFence{
					Group: request.Fence.Group, AllocationGeneration: request.Fence.AllocationGeneration,
					Command: request.Fence.Command, MemberID: request.Fence.MemberID,
					StoreID: request.Fence.StoreID, NodeIncarnation: request.Fence.NodeIncarnation,
					Term: request.Fence.Term,
				},
				Capability: request.Capability, Read: read, Authorize: readAuthorize,
			})
		if readErr != nil && readLease != nil {
			readLease.Release()
			readLease = nil
		}
		if readErr == nil {
			wireState = replicatedWireState(result.State)
			wireState = replicatedReadState(wireState, request.Fence, result.Applied)
			value, encodeErr := AppendReplicatedRequestLedgerReadValue(nil,
				ReplicatedRequestLedgerReadValue{
					Found: result.Found, AuthoritativeKind: result.AuthoritativeKind,
					Value: result.Value,
				})
			response := &ReplicatedResponse{
				Kind: ReplicatedRequestLedgerReadResult, HasState: true, State: wireState,
				ReadApplied: result.Applied, Value: value, readLease: readLease,
			}
			if encodeErr == nil && len(result.Value) <= int(wireRead.MaxBytes) &&
				result.Applied >= wireRead.MinimumApplied && validReplicatedResponse(response) {
				return response
			}
			if readLease != nil {
				readLease.Release()
			}
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalUnavailable, HasState: true, State: wireState}
		}
		if refreshed, refreshErr := server.owner.Probe(ctx, request.Fence.Group); refreshErr == nil {
			wireState = replicatedWireState(refreshed)
		}
		switch {
		case errors.Is(readErr, raftmodel.ErrNotLeader),
			errors.Is(readErr, raftmodel.ErrReadLeadershipLost):
			return &ReplicatedResponse{Kind: ReplicatedNotLeader, HasState: true, State: wireState}
		case errors.Is(readErr, raftservice.ErrServingFence):
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalStaleFence, HasState: true, State: wireState}
		case errors.Is(readErr, raftservice.ErrServingAuthorization):
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalUnavailable, HasState: true, State: wireState}
		case errors.Is(readErr, replicatedstate.ErrReadBehind):
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalReadBehind, HasState: true, State: wireState}
		case errors.Is(readErr, replicatedstate.ErrReadBufferBound):
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalReadBufferBound, HasState: true, State: wireState}
		case errors.Is(readErr, replicatedstate.ErrRequestLedgerRead):
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalRequestLedgerReadMalformed, HasState: true, State: wireState}
		case errors.Is(readErr, raftservice.ErrRequestLedgerUnauthorized):
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
	if request.Operation == ReplicatedRouteGateRead {
		return server.readRouteGate(ctx, request, wireState, readAuthorize)
	}
	if request.Operation == ReplicatedExecutionPinRead {
		wireRead := request.ExecutionPinRead
		result, readLease, readErr := server.owner.ReadExecutionPin(ctx,
			raftservice.ExecutionPinReadRequest{
				Fence: raftservice.ServingFence{
					Group: request.Fence.Group, AllocationGeneration: request.Fence.AllocationGeneration,
					Command: request.Fence.Command, MemberID: request.Fence.MemberID,
					StoreID: request.Fence.StoreID, NodeIncarnation: request.Fence.NodeIncarnation,
					Term: request.Fence.Term,
				},
				Capability: request.Capability, Pin: wireRead.Pin,
				MinimumApplied: wireRead.MinimumApplied, Authorize: readAuthorize,
			})
		if readErr != nil && readLease != nil {
			readLease.Release()
			readLease = nil
		}
		if readErr == nil {
			wireState = replicatedWireState(result.State)
			wireState = replicatedReadState(wireState, request.Fence, result.Applied)
			value, encodeErr := AppendReplicatedExecutionPinReadValue(nil,
				ReplicatedExecutionPinReadValue{Found: result.Found, Record: result.Record})
			response := &ReplicatedResponse{
				Kind: ReplicatedExecutionPinReadResult, HasState: true, State: wireState,
				ReadApplied: result.Applied, Value: value, readLease: readLease,
			}
			if encodeErr == nil && result.Applied >= wireRead.MinimumApplied &&
				validReplicatedResponse(response) {
				return response
			}
			if readLease != nil {
				readLease.Release()
			}
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalUnavailable, HasState: true, State: wireState}
		}
		if refreshed, refreshErr := server.owner.Probe(ctx, request.Fence.Group); refreshErr == nil {
			wireState = replicatedWireState(refreshed)
		}
		switch {
		case errors.Is(readErr, raftmodel.ErrNotLeader),
			errors.Is(readErr, raftmodel.ErrReadLeadershipLost):
			return &ReplicatedResponse{Kind: ReplicatedNotLeader, HasState: true, State: wireState}
		case errors.Is(readErr, raftservice.ErrServingFence):
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalStaleFence, HasState: true, State: wireState}
		case errors.Is(readErr, raftservice.ErrServingAuthorization):
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalUnavailable, HasState: true, State: wireState}
		case errors.Is(readErr, replicatedstate.ErrReadBehind):
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalReadBehind, HasState: true, State: wireState}
		case errors.Is(readErr, replicatedstate.ErrExecutionPinStateCorrupt):
			return &ReplicatedResponse{Kind: ReplicatedRefusal,
				Refusal: ReplicatedRefusalExecutionPinReadMalformed, HasState: true, State: wireState}
		case errors.Is(readErr, raftservice.ErrExecutionPinUnauthorized):
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
	servingFence := raftservice.ServingFence{
		Group:                request.Fence.Group,
		AllocationGeneration: request.Fence.AllocationGeneration,
		Command:              request.Fence.Command,
		MemberID:             request.Fence.MemberID, StoreID: request.Fence.StoreID,
		NodeIncarnation: request.Fence.NodeIncarnation, Term: request.Fence.Term,
	}
	var result raftservice.Result
	var err error
	if fusedProposal {
		result, err = authorizedOwner.SubmitOwnedAuthorized(
			ctx, servingFence, request.Command,
			func(candidate raftservice.ServingState) bool {
				return server.serving == nil || server.serving(candidate) ||
					(authenticated && server.transition != nil && server.transition(candidate, request))
			},
		)
		proposalState = replicatedWireState(result.State)
		proposalState.Fence = request.Fence
	} else {
		result, err = server.owner.SubmitOwned(ctx, servingFence, request.Command)
	}
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
	case errors.Is(err, raftservice.ErrServingAuthorization):
		return &ReplicatedResponse{
			Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalUnavailable,
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
	case result.Outcome == (raftserve.Outcome{Code: raftserve.OutcomeRetryRetired}) &&
		len(result.Completion) == 0 && errors.Is(err, replicatedstate.ErrRetryRetired):
		// Admission observed the durable session retirement floor, so this
		// exact request cannot execute again. No new Raft entry was applied:
		// preserve that distinction instead of manufacturing an apply witness.
		return &ReplicatedResponse{
			Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalRetryRetired,
			HasState: true, State: proposalState, Outcome: result.Outcome,
			RequestDigest: replicatedRequestDigest(request.Command),
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

func replicatedReadOperation(operation ReplicatedOperation) bool {
	switch operation {
	case ReplicatedReadLeader, ReplicatedReadFollower, ReplicatedReadBatchLeader,
		ReplicatedTransactionRead, ReplicatedRequestLedgerRead,
		ReplicatedExecutionPinRead, ReplicatedRouteGateRead:
		return true
	default:
		return false
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

func replicatedReadState(
	state ReplicatedMemberState,
	fence ReplicatedFence,
	applied uint64,
) ReplicatedMemberState {
	// Every successful read path revalidates this complete fence at the exact
	// data cut it returns. Preserve that evidence and advance only monotonic
	// Raft watermarks, rather than pairing the read with a later status probe.
	state.Fence = fence
	return replicatedStateAtApplied(state, applied)
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
		if !validReplicatedCompletionResult(completion) {
			reasons |= ReplicatedCompletionInvalidResult
		}
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
