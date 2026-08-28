package shardservice

import (
	"bytes"
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/query"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// RF3 SQL uses the existing SQL codec and executor, with finite per-request
// bounds inside the shared server admission budget.
const (
	MaxReplicatedSQLRequestBytes = 1 << 20
	MaxReplicatedSQLResultBytes  = 4 << 20
	replicatedSQLWorkingBytes    = 8 << 20
	replicatedSQLMaxRows         = 100000
)

type replicatedSQLLease struct {
	budget *replicatedFrameByteBudget
	bytes  int64
}

func (l *replicatedSQLLease) Release() {
	if l.budget != nil {
		l.budget.release(l.bytes)
		l.budget = nil
	}
}

func (server *ReplicatedServer) executeReplicatedQuery(ctx context.Context, request *ReplicatedRequest, state raftservice.ServingState) *ReplicatedResponse {
	wireState := replicatedWireState(state)
	refuse := func(code ReplicatedRefusalCode) *ReplicatedResponse {
		return &ReplicatedResponse{Kind: ReplicatedRefusal, Refusal: code, HasState: true, State: wireState}
	}
	if request.Fence != wireState.Fence {
		return refuse(ReplicatedRefusalStaleFence)
	}
	reader := bytes.NewReader(request.Query)
	inner, err := DecodeRequest(reader)
	if err != nil || reader.Len() != 0 || inner.Authority != request.Authority ||
		string(inner.Distribution) != state.Identity.Distribution || string(inner.Shard) != state.Identity.Shard ||
		uint64(inner.AllocationGeneration) != request.Fence.AllocationGeneration ||
		uint64(inner.RoutingVersion) != request.Fence.Command.RoutingVersion ||
		uint64(inner.OwnershipEpoch) != request.Fence.Command.OwnershipEpoch {
		return refuse(ReplicatedRefusalStaleFence)
	}
	owner, ok := server.owner.(interface {
		ReadLinearizableDataInto(context.Context, raftservice.LinearizableDataReadRequest, *raftservice.LinearizableDataReadCut) error
	})
	if !ok {
		return refuse(ReplicatedRefusalUnavailable)
	}
	charge := int64(4*request.MaxValueBytes) + 3*replicatedSQLWorkingBytes
	if !server.frames.reserve(charge) {
		return refuse(ReplicatedRefusalAdmissionBound)
	}
	lease := &replicatedSQLLease{budget: &server.frames, bytes: charge}
	retained := false
	defer func() {
		if !retained {
			lease.Release()
		}
	}()
	var cut raftservice.LinearizableDataReadCut
	err = owner.ReadLinearizableDataInto(ctx, raftservice.LinearizableDataReadRequest{
		Fence: state.Fence(), Capability: request.Capability,
	}, &cut)
	if err != nil {
		switch {
		case errors.Is(err, raftmodel.ErrNotLeader), errors.Is(err, raftmodel.ErrReadLeadershipLost):
			return &ReplicatedResponse{Kind: ReplicatedNotLeader, HasState: true, State: wireState}
		case errors.Is(err, raftservice.ErrServingFence):
			return refuse(ReplicatedRefusalStaleFence)
		case errors.Is(err, replicatedstate.ErrTransactionIntentActive):
			return refuse(ReplicatedRefusalReadIntentActive)
		default:
			return refuse(ReplicatedRefusalUnavailable)
		}
	}
	defer cut.Close()
	source, ok := cut.Source().(interface {
		NewDataReadSession(context.Context, *replicatedstate.DataReadCut, query.ExecOptions) (*sqldriver.ReplicatedReadSession, error)
	})
	if !ok {
		return refuse(ReplicatedRefusalUnavailable)
	}
	result := executeFencedSQL(ctx, source, cut.Data(), inner, int(request.MaxValueBytes))
	var encoded bytes.Buffer
	if err := EncodeResponse(&encoded, result); err != nil {
		return refuse(ReplicatedRefusalUnavailable)
	}
	if encoded.Len() > int(request.MaxValueBytes) {
		return refuse(ReplicatedRefusalReadBufferBound)
	}
	refreshed, err := server.owner.Probe(ctx, request.Fence.Group)
	if err != nil {
		return refuse(ReplicatedRefusalUnavailable)
	}
	wireState = replicatedWireState(refreshed)
	response := &ReplicatedResponse{Kind: ReplicatedQueryResult, HasState: true, State: wireState,
		ReadApplied: cut.Data().Fence().Applied, Value: encoded.Bytes(), readLease: lease}
	if !validReplicatedResponse(response) {
		return refuse(ReplicatedRefusalUnavailable)
	}
	retained = true
	return response
}

func executeFencedSQL(ctx context.Context, source interface {
	NewDataReadSession(context.Context, *replicatedstate.DataReadCut, query.ExecOptions) (*sqldriver.ReplicatedReadSession, error)
}, cut *replicatedstate.DataReadCut, req *ShardRequest, maxBytes int) *ShardResponse {
	if req.ExecutionMode != ExecutionReadOnly || req.ReadPolicy != ReadStrong ||
		req.Transaction.Operation != 0 || req.Exchange.Operation != 0 || req.Repartition.present() ||
		req.DocumentScan.present() || req.GlobalIndexLookup.present() || req.MutationCapture ||
		!req.ReadFenceID.IsZero() || req.HasMinPosition || req.RowBatch.present() {
		return NewErrorResponse(ErrorMalformedRequest, "RF3 SQL read does not support this execution mode")
	}
	rows := replicatedSQLMaxRows
	if req.MaxRows > 0 && req.MaxRows < uint64(rows) {
		rows = int(req.MaxRows)
	}
	if req.MaxResultBytes > 0 && req.MaxResultBytes < uint64(maxBytes) {
		maxBytes = int(req.MaxResultBytes)
	}
	if req.Deadline > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, req.Deadline)
		defer cancel()
	}
	var flag query.CancelFlag
	stop := context.AfterFunc(ctx, flag.Cancel)
	defer stop()
	session, err := source.NewDataReadSession(ctx, cut, query.ExecOptions{
		Cancel: &flag, ResultRows: rows, ResultBytes: int64(maxBytes),
		MemoryBytes: replicatedSQLWorkingBytes, IntermediateBytes: replicatedSQLWorkingBytes,
		AggregateBytes: replicatedSQLWorkingBytes,
	})
	if err != nil {
		return classifyError(err)
	}
	defer session.Close()
	var prepared *sqldriver.Prepared
	if req.PartialAggregate {
		prepared, err = session.PreparePartialAggregate(ctx, req.SQL)
	} else {
		prepared, err = session.Prepare(ctx, req.SQL)
	}
	if err != nil {
		return classifyError(err)
	}
	defer prepared.Close()
	var cursor sqldriver.Cursor
	defer cursor.Close()
	if req.PrimaryKeyRead.present() {
		err = prepared.QueryCandidateKeysInto(ctx, runtimeArgs(req.Params), req.PrimaryKeyRead.PrimaryPath, req.PrimaryKeyRead.Keys, &cursor)
	} else {
		err = prepared.QueryInto(ctx, runtimeArgs(req.Params), &cursor)
	}
	if err != nil {
		return classifyError(err)
	}
	columns := responseColumns(prepared)
	return RowsResponse(columns, collectRows(&cursor, len(columns)))
}
