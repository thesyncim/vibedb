package shardservice

import (
	"bytes"
	"context"
	"slices"
	"strings"

	"github.com/thesyncim/vibedb/distribution"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

// ReplicatedCall is the transport-neutral operation envelope. SQL remains a
// typed request for the local path; the authenticated remote adapter is the
// only layer that turns it into the legacy Query bytes field.
type ReplicatedCall struct {
	Request ReplicatedRequest
	SQL     *ShardRequest
}

// ReplicatedReply is the transport-neutral result envelope. SQL is populated
// for a query result and is detached from the native response frame before a
// local reply lease is released.
type ReplicatedReply struct {
	Response ReplicatedResponse
	SQL      *ShardResponse
}

// ReplicatedReplyLease keeps read buffers and the shared admission charge
// alive until the consumer has detached or transferred the result. Reply
// returns nil after Release; callers must not retain the returned pointer past
// release unless they used DetachReplicatedReply first.
type ReplicatedReplyLease interface {
	Reply() *ReplicatedReply
	Release()
}

type replicatedReplyLease struct {
	reply       *ReplicatedReply
	readLease   interface{ Release() }
	frameBudget *replicatedFrameByteBudget
	frameBytes  int64
	released    bool
}

func (lease *replicatedReplyLease) Reply() *ReplicatedReply {
	if lease == nil || lease.released {
		return nil
	}
	return lease.reply
}

func (lease *replicatedReplyLease) Release() {
	if lease == nil || lease.released {
		return
	}
	lease.released = true
	if lease.readLease != nil {
		lease.readLease.Release()
		lease.readLease = nil
	}
	if lease.frameBudget != nil {
		lease.frameBudget.release(lease.frameBytes)
		lease.frameBudget = nil
		lease.frameBytes = 0
	}
}

// DetachReplicatedReply copies all borrowed response and SQL buffers and then
// releases lease-owned read memory. The returned reply has no lease attached;
// it is safe to retain after the dispatcher's snapshot and admission state are
// recycled.
func DetachReplicatedReply(lease ReplicatedReplyLease) (*ReplicatedReply, error) {
	if lease == nil {
		return nil, ErrReplicatedWire
	}
	reply := lease.Reply()
	if reply == nil {
		lease.Release()
		return nil, ErrReplicatedWire
	}
	detached := cloneReplicatedReply(reply)
	lease.Release()
	return detached, nil
}

// replicatedLocalBinding is an immutable certificate capability. The proofs
// check the full verified chain interval without cryptography on every call.
// Rotation replaces the binding and cancels its admitted requests.
type replicatedLocalBinding struct {
	principal                  rafttransport.PeerIdentity
	node                       rafttransport.NodeID
	storage                    *ReplicatedServerTLS
	generation                 uint64
	gatewayProof, storageProof *rafttransport.PeerProfileAuthorization
	ctx                        context.Context
	cancel                     context.CancelFunc
}

type replicatedLocalCall struct{ cancel context.CancelFunc }

// DispatchReplicated applies the same service authorization, owner admission,
// SQL quotas and complete serving fences as the authenticated socket path.
// Only BindLocalGatewayPeerTLS can install its certificate-bound capability.
func (server *ReplicatedServer) DispatchReplicated(ctx context.Context, call ReplicatedCall) (ReplicatedReplyLease, error) {
	if server == nil || ctx == nil || server.state.Load() == replicatedServerClosed {
		return nil, ErrReplicatedWire
	}
	binding := server.local.Load()
	if binding == nil || !binding.gatewayProof.Valid() || !binding.storageProof.Valid() {
		return nil, ErrReplicatedAuthentication
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	frameBytes, err := ReplicatedCallFrameBytes(&call)
	if err != nil {
		return nil, err
	}
	charge := int64(frameBytes - 5)
	if !server.frames.reserve(charge) {
		server.frameRejected.Add(1)
		// Match failure to admit a socket's request frame: no owner work or
		// serving-state witness exists yet, so this is a transport error.
		return nil, errFrameBudget
	}
	retained := false
	defer func() {
		if !retained {
			server.frames.release(charge)
		}
	}()
	requestCtx, cancel := context.WithTimeout(ctx, server.requestTimeout)
	defer cancel()
	// A binding context that can never be done needs no arm.
	if binding.ctx.Done() != nil {
		stop := context.AfterFunc(binding.ctx, cancel)
		defer stop()
	}
	active := &replicatedLocalCall{cancel: cancel}
	binding.storage.mu.Lock()
	_, allowed := binding.storage.allow[binding.principal.Node]
	admitted := allowed && binding.generation == binding.storage.generation &&
		server.local.Load() == binding && binding.ctx.Err() == nil &&
		server.state.Load() != replicatedServerClosed
	if admitted {
		binding.storage.localActive[active] = struct{}{}
	}
	binding.storage.mu.Unlock()
	if !admitted {
		return nil, ErrReplicatedAuthentication
	}
	defer func() {
		binding.storage.mu.Lock()
		delete(binding.storage.localActive, active)
		binding.storage.mu.Unlock()
	}()
	if call.Request.Fence.Group.ClusterID != binding.principal.TrustDomain.ClusterID ||
		call.Request.Fence.Group.ClusterIncarnation != binding.principal.TrustDomain.ClusterIncarnation ||
		!server.authorizeReplicated(binding.principal.Node, &call.Request) {
		return &replicatedReplyLease{reply: &ReplicatedReply{Response: ReplicatedResponse{
			Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalUnauthorized,
		}}}, nil
	}
	// SubmitOwned may outlive a canceled call. Its bytes become private only
	// after grammar, size, authentication and shared admission have succeeded.
	owned := cloneReplicatedCall(call)
	server.semanticDispatch.Add(1)
	response := server.executeReplicatedAuthenticatedCall(requestCtx, &owned.Request, true, owned.SQL)
	reply, replyErr := semanticReplyFromExecutedResponse(response)
	if replyErr != nil {
		if response != nil && response.readLease != nil {
			response.readLease.Release()
		}
		return nil, replyErr
	}
	retained = true
	return &replicatedReplyLease{reply: reply, readLease: response.readLease,
		frameBudget: &server.frames, frameBytes: charge}, nil
}

func semanticReplyFromExecutedResponse(response *ReplicatedResponse) (*ReplicatedReply, error) {
	if response == nil || !validReplicatedResponse(response) {
		return nil, ErrReplicatedWire
	}
	reply := &ReplicatedReply{Response: *response, SQL: response.sqlResult}
	if response.Kind != ReplicatedQueryResult || reply.SQL != nil {
		return reply, nil
	}
	// The shared query executor emits the bounded SQL response frame directly.
	// Remote calls decode that frame at the gateway boundary; do the same for an
	// in-process semantic call while its read lease still owns the frame, then
	// restore the transport-neutral typed reply shape.
	result, err := DecodeReplicatedSQLResponse(response.Value)
	if err != nil || result.Kind != ResponseRows && result.Kind != ResponseError {
		return nil, ErrReplicatedWire
	}
	reply.Response.Value = nil
	reply.Response.sqlResult = result
	reply.SQL = result
	return reply, nil
}

// ValidateReplicatedReply validates the common semantic result grammar used by
// local consumers and by the remote adapter after decoding its SQL frame.
func ValidateReplicatedReply(reply *ReplicatedReply) error {
	if reply == nil {
		return ErrReplicatedWire
	}
	response := reply.Response
	if response.Kind == ReplicatedQueryResult {
		if reply.SQL == nil || len(response.Value) != 0 ||
			(response.sqlResult != nil && response.sqlResult != reply.SQL) {
			return ErrReplicatedWire
		}
		response.sqlResult = reply.SQL
	} else if reply.SQL != nil || response.sqlResult != nil {
		return ErrReplicatedWire
	}
	if !validReplicatedResponse(&response) {
		return ErrReplicatedWire
	}
	return nil
}

func validReplicatedPeerIdentity(identity rafttransport.PeerIdentity) bool {
	return identity.Node != (rafttransport.NodeID{}) &&
		identity.TrustDomain.ClusterID != ([16]byte{}) &&
		identity.TrustDomain.ClusterIncarnation != ([16]byte{})
}

func validReplicatedSemanticCall(call *ReplicatedCall) bool {
	if call == nil {
		return false
	}
	request := &call.Request
	if call.SQL == nil {
		// Legacy native callers already carry a SQL frame. Admit those bytes
		// before decoding them; production semantic SQL always uses SQL below.
		return validReplicatedRequest(request)
	}
	if request.Operation != ReplicatedQueryLeader || len(request.Query) != 0 ||
		ValidateRequest(call.SQL) != nil || call.SQL.Authority != request.Authority {
		return false
	}
	// Reuse the complete native request grammar while substituting a bounded
	// marker for the wire-only SQL payload. The marker is never executed or
	// transmitted; it only selects the existing QueryLeader field checks.
	wireRequest := *request
	wireRequest.Query = []byte{1}
	return validReplicatedRequest(&wireRequest)
}

// ValidateReplicatedCall validates the transport-neutral semantic envelope
// without taking a local admission reservation or retaining caller buffers.
func ValidateReplicatedCall(call *ReplicatedCall) error {
	_, err := ReplicatedCallFrameBytes(call)
	return err
}

// ReplicatedCallFrameBytes returns the exact native frame footprint that a
// remote adapter would emit for call. SQL includes its one inner SQL frame;
// local dispatch uses this same value for the shared byte budget without
// actually encoding the inner frame.
func ReplicatedCallFrameBytes(call *ReplicatedCall) (int, error) {
	if !validReplicatedSemanticCall(call) {
		return 0, ErrReplicatedWire
	}
	if call.SQL == nil {
		return ReplicatedRequestFrameBytes(&call.Request)
	}
	inner, err := RequestFrameBytes(call.SQL)
	if err != nil {
		return 0, err
	}
	if inner > MaxReplicatedSQLRequestBytes {
		return 0, ErrReplicatedWire
	}
	// QueryLeader contributes max-value bytes, the four-byte payload length,
	// and the complete inner SQL frame to the common 242-byte native prefix.
	const replicatedRequestPrefixBytes = 242
	total := replicatedRequestPrefixBytes + 4 + 4 + inner + 5
	if total-5 > maxFrameBody {
		return 0, ErrReplicatedWire
	}
	return total, nil
}

// WireReplicatedReply materializes the legacy SQL envelope at an explicit
// transport compatibility boundary. The reply must remain leased or detached
// throughout this call.
func WireReplicatedReply(reply *ReplicatedReply) (*ReplicatedResponse, error) {
	if err := ValidateReplicatedReply(reply); err != nil {
		return nil, err
	}
	response := reply.Response
	response.sqlResult = nil
	if reply.SQL != nil {
		var body bytes.Buffer
		if err := EncodeResponse(&body, reply.SQL); err != nil {
			return nil, err
		}
		response.Value = body.Bytes()
	}
	return &response, nil
}

func cloneReplicatedCall(call ReplicatedCall) ReplicatedCall {
	return ReplicatedCall{
		Request: cloneReplicatedRequest(call.Request),
		SQL:     cloneShardRequest(call.SQL),
	}
}

func cloneReplicatedRequest(request ReplicatedRequest) ReplicatedRequest {
	request.Command = slices.Clone(request.Command)
	request.Command = request.Command[:len(request.Command):len(request.Command)]
	request.Key = slices.Clone(request.Key)
	request.BatchRead = slices.Clone(request.BatchRead)
	request.Query = slices.Clone(request.Query)
	return request
}

func cloneShardRequest(request *ShardRequest) *ShardRequest {
	if request == nil {
		return nil
	}
	clone := *request
	clone.SQL = strings.Clone(request.SQL)
	clone.Distribution = distribution.DistributionName(strings.Clone(string(request.Distribution)))
	clone.Shard = distribution.ShardID(strings.Clone(string(request.Shard)))
	clone.MinPosition.Distribution = distribution.DistributionName(strings.Clone(string(request.MinPosition.Distribution)))
	clone.MinPosition.Shard = distribution.ShardID(strings.Clone(string(request.MinPosition.Shard)))
	clone.Params = make([]Param, len(request.Params))
	for index, parameter := range request.Params {
		clone.Params[index] = parameter
		clone.Params[index].Bytes = slices.Clone(parameter.Bytes)
	}
	clone.ParamTypes = slices.Clone(request.ParamTypes)
	clone.AccessScopes = slices.Clone(request.AccessScopes)
	clone.GlobalIndexLookup.Relation = slices.Clone(request.GlobalIndexLookup.Relation)
	clone.GlobalIndexLookup.KeyTuples = cloneByteMatrix(request.GlobalIndexLookup.KeyTuples)
	clone.PrimaryKeyRead.PrimaryPath = slices.Clone(request.PrimaryKeyRead.PrimaryPath)
	clone.PrimaryKeyRead.Keys = cloneByteMatrix(request.PrimaryKeyRead.Keys)
	clone.DocumentScan.Relation = slices.Clone(request.DocumentScan.Relation)
	clone.DocumentScan.After = slices.Clone(request.DocumentScan.After)
	clone.Repartition.KeyColumns = slices.Clone(request.Repartition.KeyColumns)
	clone.Repartition.Targets = make([]RepartitionTarget, len(request.Repartition.Targets))
	for index, target := range request.Repartition.Targets {
		clone.Repartition.Targets[index] = target
		clone.Repartition.Targets[index].Address = slices.Clone(target.Address)
		clone.Repartition.Targets[index].Distribution = distribution.DistributionName(strings.Clone(string(target.Distribution)))
		clone.Repartition.Targets[index].Shard = distribution.ShardID(strings.Clone(string(target.Shard)))
	}
	clone.Exchange.Batch.Data = slices.Clone(request.Exchange.Batch.Data)
	clone.Exchange.Kinds = slices.Clone(request.Exchange.Kinds)
	clone.Exchange.GroupKeys = slices.Clone(request.Exchange.GroupKeys)
	clone.Transaction.Record = slices.Clone(request.Transaction.Record)
	clone.Transaction.ManifestSegment = slices.Clone(request.Transaction.ManifestSegment)
	if request.Transaction.manifestMeta != nil {
		meta := *request.Transaction.manifestMeta
		clone.Transaction.manifestMeta = &meta
	}
	return &clone
}

func cloneByteMatrix(values [][]byte) [][]byte {
	if values == nil {
		return nil
	}
	clone := make([][]byte, len(values))
	for index := range values {
		clone[index] = slices.Clone(values[index])
	}
	return clone
}

func cloneReplicatedReply(reply *ReplicatedReply) *ReplicatedReply {
	if reply == nil {
		return nil
	}
	clone := *reply
	clone.Response.Completion = slices.Clone(reply.Response.Completion)
	clone.Response.Value = slices.Clone(reply.Response.Value)
	clone.Response.readLease = nil
	clone.Response.sqlResult = nil
	clone.SQL = cloneShardResponse(reply.SQL)
	clone.Response.sqlResult = clone.SQL
	return &clone
}

func cloneShardResponse(response *ShardResponse) *ShardResponse {
	if response == nil {
		return nil
	}
	clone := *response
	clone.Columns = slices.Clone(response.Columns)
	for index := range clone.Columns {
		clone.Columns[index].Name = strings.Clone(response.Columns[index].Name)
	}
	clone.ErrorMessage = strings.Clone(response.ErrorMessage)
	clone.ReadPosition.Distribution = distribution.DistributionName(strings.Clone(string(response.ReadPosition.Distribution)))
	clone.ReadPosition.Shard = distribution.ShardID(strings.Clone(string(response.ReadPosition.Shard)))
	if response.Rows != nil {
		clone.Rows = make([][]Cell, len(response.Rows))
	}
	for rowIndex, row := range response.Rows {
		clone.Rows[rowIndex] = make([]Cell, len(row))
		for cellIndex, cell := range row {
			clone.Rows[rowIndex][cellIndex] = cell
			clone.Rows[rowIndex][cellIndex].Bytes = slices.Clone(cell.Bytes)
		}
	}
	clone.DocumentScan.Next = slices.Clone(response.DocumentScan.Next)
	clone.Transaction.Record = slices.Clone(response.Transaction.Record)
	clone.Exchange.Batch.Data = slices.Clone(response.Exchange.Batch.Data)
	return &clone
}

var _ ReplicatedReplyLease = (*replicatedReplyLease)(nil)
