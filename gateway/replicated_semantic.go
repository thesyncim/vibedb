package gateway

import (
	"bytes"
	"context"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

// ReplicatedCallRoundTripper is the semantic transport extension used by a
// fused node. Implementations own the single SQL encode/decode boundary when
// a call crosses the authenticated native transport.
type ReplicatedCallRoundTripper interface {
	DoReplicatedCall(context.Context, ReplicatedEndpoint, *shardservice.ReplicatedCall) (*shardservice.ReplicatedReply, error)
}

// ReplicatedNodeClient composes one embedded shard service with the existing
// authenticated remote transport. Local selection is by the exact physical
// NodeID bound to the PeerTLS profile; endpoint addresses are never treated as
// authentication. Local calls use the server's semantic dispatcher; production
// typed SQL calls avoid both sockets and nested request/result frames.
type ReplicatedNodeClient struct {
	localServer                                 *shardservice.ReplicatedServer
	localNode                                   rafttransport.NodeID
	remote                                      ReplicatedRoundTripper
	localCalls, remoteCalls                     atomic.Uint64
	semanticSQLCalls, legacyCalls               atomic.Uint64
	sqlRequestEncodings, sqlRequestEncodedBytes atomic.Uint64
}

// ReplicatedNodeClientStats has bounded counters and no request-specific labels.
// SQLRequestEncodedBytes counts actual inner frames produced by this adapter;
// local calls and custom semantic transports contribute no encoded bytes.
// LegacyCalls counts DoReplicated invocations, including native writes and
// point reads. SemanticSQLCalls counts typed SQL invocations via DoReplicatedCall.
type ReplicatedNodeClientStats struct {
	LocalCalls, RemoteCalls                     uint64
	SemanticSQLCalls, LegacyCalls               uint64
	SQLRequestEncodings, SQLRequestEncodedBytes uint64
}

func (client *ReplicatedNodeClient) Stats() ReplicatedNodeClientStats {
	if client == nil {
		return ReplicatedNodeClientStats{}
	}
	return ReplicatedNodeClientStats{
		LocalCalls: client.localCalls.Load(), RemoteCalls: client.remoteCalls.Load(),
		SemanticSQLCalls: client.semanticSQLCalls.Load(), LegacyCalls: client.legacyCalls.Load(),
		SQLRequestEncodings: client.sqlRequestEncodings.Load(), SQLRequestEncodedBytes: client.sqlRequestEncodedBytes.Load(),
	}
}

// NewReplicatedNodeClient binds the storage destination and distinct gateway
// principal to one ReplicatedServer and installs the remote transport. The
// server must have BindAuthorization called first. A nil remote is valid for
// a single-node local fixture; remote destinations then fail closed.
func NewReplicatedNodeClient(
	storageTLS *shardservice.ReplicatedServerTLS,
	gatewayProfile *rafttransport.PeerTLS,
	localServer *shardservice.ReplicatedServer,
	remote ReplicatedRoundTripper,
) (*ReplicatedNodeClient, error) {
	if storageTLS == nil || gatewayProfile == nil || localServer == nil ||
		storageTLS.LocalIdentity().Node == (rafttransport.NodeID{}) {
		return nil, ErrReplicatedRoute
	}
	if err := localServer.BindLocalGatewayPeerTLS(storageTLS, gatewayProfile); err != nil {
		return nil, err
	}
	return &ReplicatedNodeClient{
		localServer: localServer,
		localNode:   storageTLS.LocalIdentity().Node,
		remote:      remote,
	}, nil
}

// DoReplicated routes legacy native calls for compatibility. Query calls are
// admitted before the server decodes their SQL frame at the compatibility
// boundary; production QuerySQL calls DoReplicatedCall directly and never
// builds the inner frame.
func (client *ReplicatedNodeClient) DoReplicated(
	ctx context.Context,
	endpoint ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	if client == nil || ctx == nil || request == nil || !validAuthenticatedEndpoint(endpoint) {
		return nil, ErrReplicatedRoute
	}
	client.legacyCalls.Add(1)
	if endpoint.Node == client.localNode {
		call := &shardservice.ReplicatedCall{Request: *request}

		reply, err := client.doReplicatedCall(ctx, endpoint, call)
		if err != nil {
			return nil, err
		}
		if reply == nil {
			return nil, ErrReplicatedRoute
		}
		if reply.SQL != nil {
			// Legacy callers explicitly requested a wire envelope. QuerySQL
			// uses DoReplicatedCall and never enters this boundary.
			return shardservice.WireReplicatedReply(reply)
		}
		return &reply.Response, nil
	}
	if client.remote == nil {
		return nil, ErrReplicatedDial
	}
	client.remoteCalls.Add(1)
	return client.remote.DoReplicated(ctx, endpoint, request)
}

// DoReplicatedCall sends a semantic call through the local dispatcher or the
// authenticated remote adapter. It never routes a nonlocal endpoint to the
// local owner even when its address happens to match.
func (client *ReplicatedNodeClient) DoReplicatedCall(
	ctx context.Context,
	endpoint ReplicatedEndpoint,
	call *shardservice.ReplicatedCall,
) (*shardservice.ReplicatedReply, error) {
	if client == nil || ctx == nil || call == nil || !validAuthenticatedEndpoint(endpoint) {
		return nil, ErrReplicatedRoute
	}
	if call.SQL != nil {
		client.semanticSQLCalls.Add(1)
	}
	return client.doReplicatedCall(ctx, endpoint, call)
}

func (client *ReplicatedNodeClient) doReplicatedCall(ctx context.Context, endpoint ReplicatedEndpoint, call *shardservice.ReplicatedCall) (*shardservice.ReplicatedReply, error) {
	if endpoint.Node == client.localNode {
		client.localCalls.Add(1)
		lease, err := client.localServer.DispatchReplicated(ctx, *call)
		if err != nil {
			return nil, err
		}
		reply := lease.Reply()
		if err := shardservice.ValidateReplicatedReply(reply); err != nil {
			lease.Release()
			return nil, err
		}
		if reply.Response.HasState &&
			(reply.Response.State.Fence.MemberID != endpoint.Member ||
				reply.Response.State.Fence.StoreID != endpoint.StoreID ||
				reply.Response.State.Fence.NodeIncarnation != endpoint.NodeIncarnation) {
			lease.Release()
			return nil, ErrReplicatedRoute
		}
		return shardservice.DetachReplicatedReply(lease)
	}
	if client.remote == nil {
		return nil, ErrReplicatedDial
	}
	client.remoteCalls.Add(1)
	switch client.remote.(type) {
	case *AuthenticatedReplicatedClient, TCPReplicatedClient:
		return doRemoteReplicatedCallMeasured(ctx, client.remote, endpoint, call, client)
	}
	if semantic, ok := client.remote.(ReplicatedCallRoundTripper); ok {
		return semantic.DoReplicatedCall(ctx, endpoint, call)
	}
	return doRemoteReplicatedCallMeasured(ctx, client.remote, endpoint, call, client)
}

// DoReplicatedCall implements the semantic boundary for the simple TCP
// client. Authentication remains the caller's responsibility, exactly as it
// is for its legacy DoReplicated method.
func (client TCPReplicatedClient) DoReplicatedCall(
	ctx context.Context,
	endpoint ReplicatedEndpoint,
	call *shardservice.ReplicatedCall,
) (*shardservice.ReplicatedReply, error) {
	return doRemoteReplicatedCall(ctx, client, endpoint, call)
}

// DoReplicatedCall implements the semantic boundary for the authenticated
// pooled transport. SQL is encoded only after endpoint selection and only for
// this remote hop; connection authentication, bounded admission, response
// identity checks and poisoning remain owned by DoReplicated.
func (client *AuthenticatedReplicatedClient) DoReplicatedCall(
	ctx context.Context,
	endpoint ReplicatedEndpoint,
	call *shardservice.ReplicatedCall,
) (*shardservice.ReplicatedReply, error) {
	return doRemoteReplicatedCall(ctx, client, endpoint, call)
}

func doRemoteReplicatedCall(
	ctx context.Context,
	client ReplicatedRoundTripper,
	endpoint ReplicatedEndpoint,
	call *shardservice.ReplicatedCall,
) (*shardservice.ReplicatedReply, error) {
	return doRemoteReplicatedCallMeasured(ctx, client, endpoint, call, nil)
}

func doRemoteReplicatedCallMeasured(ctx context.Context, client ReplicatedRoundTripper, endpoint ReplicatedEndpoint, call *shardservice.ReplicatedCall, stats *ReplicatedNodeClient) (*shardservice.ReplicatedReply, error) {
	if client == nil || ctx == nil || call == nil {
		return nil, ErrReplicatedRoute
	}
	request, err := semanticCallToWire(call)
	if err != nil {
		return nil, err
	}
	if stats != nil && call.SQL != nil {
		stats.sqlRequestEncodings.Add(1)
		stats.sqlRequestEncodedBytes.Add(uint64(len(request.Query)))
	}
	response, err := client.DoReplicated(ctx, endpoint, request)
	if err != nil {
		return nil, err
	}
	return semanticReplyFromWire(response)
}

func semanticCallToWire(call *shardservice.ReplicatedCall) (*shardservice.ReplicatedRequest, error) {
	if err := shardservice.ValidateReplicatedCall(call); err != nil {
		return nil, err
	}
	request := call.Request
	if call.SQL == nil {
		return &request, nil
	}
	if size, err := shardservice.RequestFrameBytes(call.SQL); err != nil {
		return nil, err
	} else if size > shardservice.MaxReplicatedSQLRequestBytes {
		return nil, ErrResultLimit
	}
	var body bytes.Buffer
	if err := shardservice.EncodeRequest(&body, call.SQL); err != nil {
		return nil, err
	}
	if body.Len() > shardservice.MaxReplicatedSQLRequestBytes {
		return nil, ErrResultLimit
	}
	request.Query = body.Bytes()
	return &request, nil
}

func semanticReplyFromWire(response *shardservice.ReplicatedResponse) (*shardservice.ReplicatedReply, error) {
	if err := shardservice.ValidateReplicatedResponse(response); err != nil {
		return nil, err
	}
	reply := &shardservice.ReplicatedReply{Response: *response}
	if response.Kind != shardservice.ReplicatedQueryResult {
		return reply, nil
	}
	result, err := shardservice.DecodeReplicatedSQLResponse(response.Value)
	if err != nil ||
		(result.Kind != shardservice.ResponseRows && result.Kind != shardservice.ResponseError) {
		return nil, ErrReplicatedRoute
	}
	reply.SQL = result
	reply.Response.Value = nil
	if err := shardservice.ValidateReplicatedReply(reply); err != nil {
		return nil, err
	}
	return reply, nil
}

// doReplicatedCall is the retry executor's semantic counterpart to
// doReplicated. It applies the authenticated authority from context to both
// envelopes and preserves the per-attempt timeout.
func (executor *ReplicatedExecutor) doReplicatedCall(
	ctx context.Context,
	endpoint ReplicatedEndpoint,
	call *shardservice.ReplicatedCall,
) (*shardservice.ReplicatedReply, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, executor.attemptTimeout)
	defer cancel()
	// Attempts are sequential and no transport mutates the call (the server
	// deep-clones before executing, the wire path copies the envelope to
	// attach the encoded body), so stamping the same authority in place is
	// exact and saves two struct copies per attempt.
	if authority, ok := serviceauthz.FromContext(ctx); ok {
		call.Request.Authority = authority
		if call.SQL != nil {
			call.SQL.Authority = authority
		}
	}
	var (
		reply *shardservice.ReplicatedReply
		err   error
	)
	if semantic, ok := executor.client.(ReplicatedCallRoundTripper); ok {
		reply, err = semantic.DoReplicatedCall(attemptCtx, endpoint, call)
	} else {
		reply, err = doRemoteReplicatedCall(attemptCtx, executor.client, endpoint, call)
	}
	if err == nil {
		err = shardservice.ValidateReplicatedReply(reply)
	}
	if err == nil && reply.Response.HasState &&
		(reply.Response.State.Fence.MemberID != endpoint.Member ||
			reply.Response.State.Fence.StoreID != endpoint.StoreID ||
			reply.Response.State.Fence.NodeIncarnation != endpoint.NodeIncarnation) {
		return nil, ErrReplicatedRoute
	}
	return reply, err
}
