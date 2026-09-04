package shardservice

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"runtime/trace"
	"sync"

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

	// A fenced SQL read can retain the query result, its owned wire cells, the
	// inner SQL frame, and the outer RF3 frame while crossing its execution and
	// write boundaries. Memory, intermediate, and aggregate execution each have
	// their own independent allowance. Each execution tier reserves all of
	// these accounts before taking a read cut; the final tier is unchanged.
	replicatedSQLResultReservationCopies   = int64(4)
	replicatedSQLWorkingReservationBudgets = int64(3)
	replicatedSQLMaximumReservationBytes   = replicatedSQLResultReservationCopies*MaxReplicatedSQLResultBytes +
		replicatedSQLWorkingReservationBudgets*replicatedSQLWorkingBytes
)

func replicatedSQLReservationBytes(maximum uint32) int64 {
	return replicatedSQLResultReservationCopies*int64(maximum) +
		replicatedSQLWorkingReservationBudgets*replicatedSQLWorkingBytes
}

type replicatedSQLLease struct {
	budget *replicatedFrameByteBudget
	bytes  int64
}

func (l *replicatedSQLLease) Release() {
	if l.budget != nil {
		l.budget.releaseSQL(l.bytes)
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
	// All attempts share the caller's original execution deadline. A larger
	// tier never extends it and never holds a snapshot while waiting for memory.
	if inner.Deadline > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, inner.Deadline)
		defer cancel()
	}
	maximum := int(request.MaxValueBytes)
	if inner.MaxResultBytes > 0 && inner.MaxResultBytes < uint64(maximum) {
		maximum = int(inner.MaxResultBytes)
	}
	key := sha256.Sum256([]byte(inner.SQL))
	for tier := server.sqlHints.lookup(key); tier < len(replicatedSQLTiers); tier++ {
		budget := replicatedSQLTiers[tier]
		budget.resultBytes = min(budget.resultBytes, int(request.MaxValueBytes))
		response, grow := server.executeReplicatedQueryTier(ctx, request, state, inner, owner, budget, maximum)
		if !grow {
			if response.Kind == ReplicatedQueryResult {
				server.sqlHints.record(key, tier)
			}
			return response
		}
	}
	return refuse(ReplicatedRefusalReadBufferBound)
}

// Hints affect only the initial reservation, never plans, rows, authorization,
// deadlines or result limits. Different parameters/schema generations may need
// more memory and still escalate normally. Fixed direct-mapped storage bounds
// retention; collisions only discard an optimization. No SQL text is retained.
// Repeated large scans avoid executing their smaller failed tiers each time.
type replicatedSQLBudgetHints struct {
	mu      sync.Mutex
	entries [256]struct {
		key  [sha256.Size]byte
		tier uint8
	}
}

func (hints *replicatedSQLBudgetHints) lookup(key [sha256.Size]byte) int {
	hints.mu.Lock()
	entry := hints.entries[key[0]]
	hints.mu.Unlock()
	if entry.key == key {
		return int(entry.tier)
	}
	return 0
}

func (hints *replicatedSQLBudgetHints) record(key [sha256.Size]byte, tier int) {
	if tier <= 0 || tier >= len(replicatedSQLTiers) {
		return
	}
	hints.mu.Lock()
	entry := &hints.entries[key[0]]
	if entry.key != key || tier > int(entry.tier) {
		entry.key, entry.tier = key, uint8(tier)
	}
	hints.mu.Unlock()
}

type replicatedSQLBudget struct {
	resultBytes  int
	workingBytes int64
}

// Each failed read-only attempt releases its complete workspace and cut before
// the next reservation. No provisional rows escape. The final tier preserves
// the old maximum allowance; small queries no longer reserve that maximum.
var replicatedSQLTiers = [...]replicatedSQLBudget{
	{64 << 10, 256 << 10},
	{256 << 10, 1 << 20},
	{1 << 20, 4 << 20},
	{MaxReplicatedSQLResultBytes, replicatedSQLWorkingBytes},
}

func (budget replicatedSQLBudget) reservationBytes() int64 {
	return replicatedSQLResultReservationCopies*int64(budget.resultBytes) + replicatedSQLWorkingReservationBudgets*budget.workingBytes
}
func (budget replicatedSQLBudget) canGrow(err error, maximum int) bool {
	if budget.resultBytes < maximum && errors.Is(err, query.ErrResultBudget) {
		var result *query.ResultBudgetError
		// A caller row cap is independent of memory and must not trigger a rerun.
		return errors.As(err, &result) && !(result.RowLimit >= 0 && result.Rows > result.RowLimit)
	}
	return budget.workingBytes < replicatedSQLWorkingBytes && (errors.Is(err, query.ErrWorkBudget) || errors.Is(err, query.ErrIntermediateBudget) ||
		errors.Is(err, query.ErrAggregateBudget) || errors.Is(err, query.ErrJoinPairBudget))
}

type replicatedSQLReadOwner interface {
	ReadLinearizableDataInto(context.Context, raftservice.LinearizableDataReadRequest, *raftservice.LinearizableDataReadCut) error
}

func (server *ReplicatedServer) executeReplicatedQueryTier(ctx context.Context, request *ReplicatedRequest, state raftservice.ServingState,
	inner *ShardRequest, owner replicatedSQLReadOwner, budget replicatedSQLBudget, maximum int) (*ReplicatedResponse, bool) {
	wireState := replicatedWireState(state)
	refuse := func(code ReplicatedRefusalCode) (*ReplicatedResponse, bool) {
		return &ReplicatedResponse{Kind: ReplicatedRefusal, Refusal: code, HasState: true, State: wireState}, false
	}
	charge := budget.reservationBytes()
	admission := trace.StartRegion(ctx, "sql.read.admission")
	admitted := server.frames.reserveSQL(ctx, charge)
	admission.End()
	if !admitted {
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
	quorum := trace.StartRegion(ctx, "sql.read.quorum")
	err := owner.ReadLinearizableDataInto(ctx, raftservice.LinearizableDataReadRequest{Fence: state.Fence(), Capability: request.Capability}, &cut)
	quorum.End()
	if err != nil {
		switch {
		case errors.Is(err, raftmodel.ErrNotLeader), errors.Is(err, raftmodel.ErrReadLeadershipLost):
			return &ReplicatedResponse{Kind: ReplicatedNotLeader, HasState: true, State: wireState}, false
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
	execution := trace.StartRegion(ctx, "sql.read.execute")
	encoded, err := executeFencedSQLBudget(ctx, source, cut.Data(), inner, budget)
	execution.End()
	if err != nil {
		if ctx.Err() == nil && (budget.canGrow(err, maximum) ||
			errors.Is(err, errSQLReadFrameBound) && budget.resultBytes < int(request.MaxValueBytes)) {
			return nil, true
		}
		if errors.Is(err, errSQLReadFrameBound) {
			return refuse(ReplicatedRefusalReadBufferBound)
		}
		encoded, err = encodeSQLReadError(classifyError(err), budget.resultBytes)
		if err != nil {
			if errors.Is(err, errSQLReadFrameBound) {
				if budget.resultBytes < int(request.MaxValueBytes) && ctx.Err() == nil {
					return nil, true
				}
				return refuse(ReplicatedRefusalReadBufferBound)
			}
			return refuse(ReplicatedRefusalUnavailable)
		}
	}
	wireState = replicatedReadState(wireState, request.Fence, cut.Data().Fence().Applied)
	response := &ReplicatedResponse{Kind: ReplicatedQueryResult, HasState: true, State: wireState, ReadApplied: cut.Data().Fence().Applied, Value: encoded, readLease: lease}
	if !validReplicatedResponse(response) {
		return refuse(ReplicatedRefusalUnavailable)
	}
	retained = true
	return response, false
}

// These are the response shapes admitted by the RF3 SQL wire budget.
// Charge the full frame grammar without allocating or overflowing on lengths.
func replicatedSQLResponseFits(response *ShardResponse, limit int) bool {
	if response == nil || response.HasReadPosition || response.DocumentScan.Present || response.Exchange.present() || response.Transaction.Role != TransactionRoleNone {
		return false
	}
	remaining := limit
	take := func(n int) bool {
		if n < 0 || n > remaining {
			return false
		}
		remaining -= n
		return true
	}
	// Five-byte header, version, kind and absent read-position marker.
	if !take(8) {
		return false
	}
	switch response.Kind {
	case ResponseRows:
		if !take(8) {
			return false
		} // column and row counts
		for _, column := range response.Columns {
			if !take(8) || !take(len(column.Name)) {
				return false
			}
		}
		for _, row := range response.Rows {
			if len(row) != len(response.Columns) {
				return false
			}
			for _, cell := range row {
				if !take(1) {
					return false
				}
				if !cell.Null && (!take(4) || !take(len(cell.Bytes))) {
					return false
				}
			}
		}
		return true
	case ResponseError:
		return take(5) && take(len(response.ErrorMessage))
	default:
		return false
	}
}

func executeFencedSQLBudget(ctx context.Context, source interface {
	NewDataReadSession(context.Context, *replicatedstate.DataReadCut, query.ExecOptions) (*sqldriver.ReplicatedReadSession, error)
}, cut *replicatedstate.DataReadCut, req *ShardRequest, budget replicatedSQLBudget) ([]byte, error) {
	maxBytes := budget.resultBytes
	if req.ExecutionMode != ExecutionReadOnly || req.ReadPolicy != ReadStrong ||
		req.Transaction.Operation != 0 || req.Exchange.Operation != 0 || req.Repartition.present() ||
		req.DocumentScan.present() || req.GlobalIndexLookup.present() || req.mutationCapturePresent() ||
		!req.ReadFenceID.IsZero() || req.HasMinPosition || req.RowBatch.present() {
		return encodeSQLReadError(NewErrorResponse(ErrorMalformedRequest, "RF3 SQL read does not support this execution mode"), budget.resultBytes)
	}
	rows := replicatedSQLMaxRows
	if req.MaxRows > 0 && req.MaxRows < uint64(rows) {
		rows = int(req.MaxRows)
	}
	if req.MaxResultBytes > 0 && req.MaxResultBytes < uint64(maxBytes) {
		maxBytes = int(req.MaxResultBytes)
	}
	var flag query.CancelFlag
	stop := context.AfterFunc(ctx, flag.Cancel)
	defer stop()
	session, err := source.NewDataReadSession(ctx, cut, query.ExecOptions{
		Cancel: &flag, ResultRows: rows, ResultBytes: int64(maxBytes),
		MemoryBytes: budget.workingBytes, IntermediateBytes: budget.workingBytes,
		AggregateBytes: budget.workingBytes,
	})
	if err != nil {
		return nil, err
	}
	defer session.Close()
	prepared, err := prepareShardSQL(
		ctx, session, req.SQL, req.ParamTypes, req.PartialAggregate,
	)
	if err != nil {
		return nil, err
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
		return nil, err
	}
	encoding := trace.StartRegion(ctx, "sql.read.encode")
	defer encoding.End()
	return encodeSQLReadCursor(cursor.Snapshot(), prepared.Columns(), budget.resultBytes, &flag)
}

// sqlLimit leaves native frame capacity outside SQL execution reservations.
// Small custom budgets retain their previous ability to run one maximum query.
func (budget *replicatedFrameByteBudget) sqlLimit() int64 {
	return min(budget.limit, max(replicatedSQLMaximumReservationBytes, budget.limit-defaultReplicatedNativeFrameHeadroom))
}

// reserveSQLLocked requires waitMu. Workspace and shared frame accounting are
// committed together; failed reservations never hold partial capacity.
func (budget *replicatedFrameByteBudget) reserveSQLLocked(bytes int64) bool {
	if bytes > budget.sqlLimit()-budget.sqlUsed || !budget.reserve(bytes) {
		return false
	}
	budget.sqlUsed += bytes
	return true
}

func (budget *replicatedFrameByteBudget) releaseSQL(bytes int64) {
	budget.waitMu.Lock()
	budget.sqlUsed -= bytes
	budget.waitMu.Unlock()
	budget.release(bytes)
}

// reserveSQL applies bounded backpressure before taking a ReadIndex cut. Waiting
// requests retain only their already charged wire frames, never SQL workspaces
// or snapshots. Native operations retain nonblocking shared-frame admission.
func (budget *replicatedFrameByteBudget) reserveSQL(ctx context.Context, bytes int64) bool {
	if budget == nil || bytes <= 0 || bytes > budget.sqlLimit() || ctx.Err() != nil {
		return false
	}
	budget.waitMu.Lock()
	if budget.reserveSQLLocked(bytes) {
		budget.waitMu.Unlock()
		return true
	}
	if budget.waiters.Load() >= 16 {
		budget.waitMu.Unlock()
		return false
	}
	budget.waiters.Add(1)
	defer budget.waiters.Add(-1)
	for {
		if ctx.Err() != nil {
			budget.waitMu.Unlock()
			return false
		}
		if budget.reserveSQLLocked(bytes) {
			budget.waitMu.Unlock()
			return true
		}
		if budget.changed == nil {
			budget.changed = make(chan struct{})
		}
		changed := budget.changed
		budget.waitMu.Unlock()
		select {
		case <-ctx.Done():
			return false
		case <-changed:
		}
		budget.waitMu.Lock()
	}
}
