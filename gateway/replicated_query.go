package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"sync"

	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

// ShardTransport keeps routing, bounded fan-out and merge in one executor.
type ShardTransport interface {
	Do(context.Context, string, *shardservice.ShardRequest) (*shardservice.ShardResponse, error)
	DoBatches(context.Context, string, *shardservice.ShardRequest, func(*shardservice.ShardResponse) error) error
}

type replicatedSQLSnapshotKey struct{}

var ErrReplicatedSQLPlanUnsupported = errors.New("gateway: RF3 SQL does not yet support global-index or repartition exchange plans")

type replicatedSQLAttempt struct {
	snapshot     *Snapshot
	mu           sync.Mutex
	observations []ReplicatedGroupReadObservation
}

func (a *replicatedSQLAttempt) resultObservations() []ReplicatedGroupReadObservation {
	a.mu.Lock()
	defer a.mu.Unlock()
	slices.SortFunc(a.observations, func(x, y ReplicatedGroupReadObservation) int { return bytes.Compare(x.RouteID[:], y.RouteID[:]) })
	return a.observations
}

// ReplicatedSQLTransport routes each physical read to its RF3 leader using the
// exact catalog generation pinned by Executor, never a freshly resolved route.
type ReplicatedSQLTransport struct{ Executor *ReplicatedExecutor }

func (transport *ReplicatedSQLTransport) Do(ctx context.Context, _ string, req *shardservice.ShardRequest) (*shardservice.ShardResponse, error) {
	if transport == nil || transport.Executor == nil || req == nil || ctx == nil {
		return nil, ErrReplicatedRoute
	}
	attempt, ok := ctx.Value(replicatedSQLSnapshotKey{}).(*replicatedSQLAttempt)
	if !ok {
		return nil, ErrNoCatalog
	}
	var endpoints [ServingReplicaCount]ReplicatedEndpoint
	route, ok := attempt.snapshot.ResolveReplicatedRoute(req.Distribution, req.Shard, endpoints[:0])
	if !ok || route.AllocationGeneration != uint64(req.AllocationGeneration) ||
		route.Command.RoutingVersion != uint64(req.RoutingVersion) || route.Command.OwnershipEpoch != uint64(req.OwnershipEpoch) {
		return nil, ErrStaleGeneration
	}
	return transport.Executor.QuerySQL(ctx, route, req)
}

// DoBatches splits a fully admitted bounded native result into the existing
// merger's fragments. No provisional rows escape a failed RF3 read.
func (transport *ReplicatedSQLTransport) DoBatches(ctx context.Context, address string, req *shardservice.ShardRequest, consume func(*shardservice.ShardResponse) error) error {
	if err := validateBatchCall(req, consume); err != nil {
		return err
	}
	copy := *req
	copy.RowBatch = shardservice.RowBatchRequest{}
	result, err := transport.Do(ctx, address, &copy)
	if err != nil {
		return err
	}
	if result.Kind == shardservice.ResponseError {
		return shardError(result)
	}
	var sequence uint32
	batchRows := int(req.RowBatch.BatchRows)
	if len(result.Columns) > 0 {
		batchRows = min(batchRows, int(shardservice.MaxRowBatchCells)/len(result.Columns))
	}
	if batchRows == 0 {
		return ErrResultLimit
	}
	for start := 0; ; {
		end, size := start, 0
		for end < len(result.Rows) && end-start < batchRows {
			rowBytes := 0
			for _, cell := range result.Rows[end] {
				rowBytes++
				if !cell.Null {
					rowBytes += 4 + len(cell.Bytes)
				}
			}
			if rowBytes > int(req.RowBatch.BatchBytes) {
				return ErrResultLimit
			}
			if size+rowBytes > int(req.RowBatch.BatchBytes) {
				break
			}
			size += rowBytes
			end++
		}
		batch := &shardservice.ShardResponse{Kind: shardservice.ResponseRowBatch, Rows: result.Rows[start:end],
			RowBatch: shardservice.RowBatchReply{Sequence: sequence, ColumnCount: uint32(len(result.Columns)), Final: end == len(result.Rows)}}
		if sequence == 0 {
			batch.Columns = result.Columns
		}
		if err := consume(batch); err != nil {
			return err
		}
		if end == len(result.Rows) {
			return nil
		}
		start = end
		sequence++
	}
}

func (executor *ReplicatedExecutor) QuerySQL(ctx context.Context, route ReplicatedRoute, req *shardservice.ShardRequest) (*shardservice.ShardResponse, error) {
	if executor == nil || executor.client == nil || ctx == nil || req == nil || !validReplicatedRoute(route) {
		return nil, ErrReplicatedRoute
	}
	forwarded := *req
	if authority, ok := serviceauthz.FromContext(ctx); ok {
		forwarded.Authority = authority
	}
	if size, err := shardservice.RequestFrameBytes(&forwarded); err != nil {
		return nil, err
	} else if size > shardservice.MaxReplicatedSQLRequestBytes {
		return nil, ErrResultLimit
	}
	preferred := route.Replicas[0].Member
	var joined error
	for attempt := 0; attempt < executor.maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		endpoint, state, err := executor.discoverLeader(ctx, route, preferred, serviceauthz.CapabilityDataRead)
		if err != nil {
			joined = errors.Join(joined, err)
			preferred = 0
			continue
		}
		call := &shardservice.ReplicatedCall{
			Request: shardservice.ReplicatedRequest{
				Operation: shardservice.ReplicatedQueryLeader, Authority: forwarded.Authority,
				Capability: serviceauthz.CapabilityDataRead, Fence: state.Fence,
				MaxValueBytes: shardservice.MaxReplicatedSQLResultBytes,
			},
			SQL: &forwarded,
		}
		reply, err := executor.doReplicatedCall(ctx, endpoint, call)
		if err != nil {
			executor.leaderHints.invalidate(route, endpoint, state)
			joined = errors.Join(joined, err)
			preferred = 0
			continue
		}
		if reply == nil {
			return nil, ErrReplicatedRoute
		}
		response := &reply.Response
		if validReplicatedUnauthorizedWithoutState(response) {
			return nil, &ReplicatedRefusalError{Code: response.Refusal}
		}
		if !validReplicatedResponseState(response) || response.State.Fence.Group != route.Group ||
			response.State.Fence.AllocationGeneration != route.AllocationGeneration || response.State.Fence.Command != route.Command {
			return nil, ErrStaleGeneration
		}
		switch response.Kind {
		case shardservice.ReplicatedQueryResult:
			if response.Refusal != shardservice.ReplicatedRefusalNone || response.RequestDigest != ([sha256.Size]byte{}) ||
				response.Outcome != (raftserve.Outcome{}) || len(response.Completion) != 0 ||
				response.ReadApplied == 0 || response.ReadApplied > response.State.Applied ||
				(len(response.Value) != 0 && (len(response.Value) < 5 || len(response.Value) > shardservice.MaxReplicatedSQLResultBytes)) || reply.SQL == nil {
				return nil, ErrReplicatedRoute
			}
			result := reply.SQL
			if result.Kind != shardservice.ResponseRows && result.Kind != shardservice.ResponseError {
				return nil, ErrReplicatedRoute
			}
			if result.Kind == shardservice.ResponseError {
				return nil, shardError(result)
			}
			if execution, ok := ctx.Value(replicatedSQLSnapshotKey{}).(*replicatedSQLAttempt); ok {
				execution.mu.Lock()
				execution.observations = append(execution.observations, ReplicatedGroupReadObservation{Group: route.Group, RouteID: replicatedRouteID(route), Applied: response.ReadApplied, Retries: attempt})
				execution.mu.Unlock()
			}
			executor.leaderHints.publish(route, endpoint, response.State)
			return result, nil
		case shardservice.ReplicatedNotLeader:
			if !validReplicatedNonterminalResponse(response) {
				return nil, ErrReplicatedRoute
			}
			executor.leaderHints.invalidate(route, endpoint, state)
			preferred = response.State.LeaderID
		case shardservice.ReplicatedRefusal:
			if !validReplicatedReadRefusal(response, response.Refusal) {
				return nil, ErrReplicatedRoute
			}
			if response.Refusal != shardservice.ReplicatedRefusalStaleFence {
				return nil, &ReplicatedRefusalError{Code: response.Refusal}
			}
			executor.leaderHints.invalidate(route, endpoint, state)
			preferred = response.State.LeaderID
		default:
			return nil, ErrReplicatedRoute
		}
	}
	return nil, errors.Join(ErrReplicatedLeader, joined)
}
