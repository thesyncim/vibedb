package shardservice

import (
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"runtime/trace"
	"sync"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
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
	point  sqldriver.ReplicatedReadLease
	cursor sqldriver.Cursor
	cancel query.CancelFlag
}

func (l *replicatedSQLLease) Release() {
	if l.budget != nil {
		l.budget.releaseSQL(l.bytes)
		l.budget = nil
	}
}

func (server *ReplicatedServer) executeReplicatedQuery(ctx context.Context, request *ReplicatedRequest, state raftservice.ServingState) *ReplicatedResponse {
	return server.executeReplicatedQueryCall(ctx, request, state, nil, nil)
}

func (server *ReplicatedServer) executeReplicatedQueryCall(
	ctx context.Context,
	request *ReplicatedRequest,
	state raftservice.ServingState,
	semantic *ShardRequest,
	authorize raftservice.ProposalAuthorization,
) *ReplicatedResponse {
	return server.executeReplicatedQueryCallValidated(ctx, request, state, semantic, authorize, false)
}

func (server *ReplicatedServer) executeReplicatedQueryCallValidated(
	ctx context.Context,
	request *ReplicatedRequest,
	state raftservice.ServingState,
	semantic *ShardRequest,
	authorize raftservice.ProposalAuthorization,
	semanticValidated bool,
) *ReplicatedResponse {
	wireState := replicatedWireState(state)
	refuse := func(code ReplicatedRefusalCode) *ReplicatedResponse {
		return &ReplicatedResponse{Kind: ReplicatedRefusal, Refusal: code, HasState: true, State: wireState}
	}
	if request.Fence != wireState.Fence {
		return refuse(ReplicatedRefusalStaleFence)
	}
	inner := semantic
	var err error
	if inner == nil {
		inner, err = DecodeReplicatedSQLRequest(request.Query)
		if err != nil {
			return refuse(ReplicatedRefusalStaleFence)
		}
	} else if !semanticValidated {
		err = ValidateRequest(inner)
		if err != nil {
			return refuse(ReplicatedRefusalStaleFence)
		}
	}
	if inner.Authority != request.Authority ||
		string(inner.Distribution) != state.Identity.Distribution || string(inner.Shard) != state.Identity.Shard ||
		uint64(inner.AllocationGeneration) != request.Fence.AllocationGeneration ||
		uint64(inner.RoutingVersion) != request.Fence.Command.RoutingVersion ||
		uint64(inner.OwnershipEpoch) != request.Fence.Command.OwnershipEpoch {
		return refuse(ReplicatedRefusalStaleFence)
	}
	owner := any(server.owner)
	if _, dataOK := owner.(replicatedSQLReadOwner); !dataOK {
		// A canonical single-key request can use the narrow point lane without
		// requiring the complete data-cut interface. Other requests still need
		// their ordinary snapshot source below.
		if !replicatedSQLPointReadEligible(inner) {
			return refuse(ReplicatedRefusalUnavailable)
		}
		if _, pointOK := owner.(replicatedSQLPointReadOwner); !pointOK {
			return refuse(ReplicatedRefusalUnavailable)
		}
	}
	if owner == nil {
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
		response, grow := server.executeReplicatedQueryTierCall(ctx, request, state, inner, owner, budget, maximum, authorize)
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
	bytes, ok := budget.reservationBytesChecked(0)
	if !ok {
		return 0
	}
	return bytes
}

// reservationBytesForPoint includes the full catalog-frozen point document
// bound in the admission charge. The point cut returns a detached value before
// the SQL session can copy it, so this charge must be secured before either
// operation starts. Keep the ordinary query reservation unchanged: its result,
// work, intermediate, and aggregate budgets still control SQL execution and
// tier growth independently of this source-side bound.
func (budget replicatedSQLBudget) reservationBytesForPoint(maxDocumentBytes uint32) (int64, bool) {
	return budget.reservationBytesChecked(maxDocumentBytes)
}

func (budget replicatedSQLBudget) reservationBytesChecked(extra uint32) (int64, bool) {
	if budget.resultBytes < 0 || budget.workingBytes < 0 {
		return 0, false
	}
	result := int64(budget.resultBytes)
	working := budget.workingBytes
	if result > math.MaxInt64/replicatedSQLResultReservationCopies ||
		working > math.MaxInt64/replicatedSQLWorkingReservationBudgets {
		return 0, false
	}
	result *= replicatedSQLResultReservationCopies
	working *= replicatedSQLWorkingReservationBudgets
	if result > math.MaxInt64-working {
		return 0, false
	}
	result += working
	extraBytes := int64(extra)
	if result > math.MaxInt64-extraBytes {
		return 0, false
	}
	return result + extraBytes, true
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

type replicatedSQLPointReadOwner interface {
	ReadLinearizablePointInto(context.Context, raftservice.LinearizablePointReadRequest, *raftservice.LinearizablePointReadCut) error
}

type replicatedSQLPointReadSource interface {
	NewPointReadSession(
		context.Context,
		replication.RelationID,
		[]byte,
		bool,
		[]byte,
		[]byte,
		query.ExecOptions,
	) (*sqldriver.ReplicatedReadSession, error)
}

type replicatedSQLDataReadReuseSource interface {
	AcquireReplicatedDataRead(
		context.Context, *replicatedstate.DataReadCut, string,
		[]sqldriver.ParamType, bool, query.ExecOptions,
	) (*sqldriver.ReplicatedReadLease, error)
}

type replicatedSQLPointReadReuseSource interface {
	AcquireReplicatedPointRead(
		context.Context, replication.RelationID, []byte, bool, []byte, []byte,
		string, []sqldriver.ParamType, bool, query.ExecOptions,
	) (*sqldriver.ReplicatedReadLease, error)
}

// replicatedSQLPointReadEligible is deliberately narrower than the wire's
// legacy candidate-key form. The point lane may borrow one exact live row only
// when every other part of the RF3 SQL contract is the existing read-only,
// strong, single-relation execution. Unsupported forms continue through the
// complete data snapshot path, preserving its validation and refusal behavior.
func replicatedSQLPointReadEligible(req *ShardRequest) bool {
	if req == nil || !req.PrimaryKeyRead.canonical() ||
		!req.PrimaryKeyRead.livePointEligible() {
		return false
	}
	return req.ExecutionMode == ExecutionReadOnly && req.ReadPolicy == ReadStrong &&
		req.Transaction.Operation == 0 && req.Exchange.Operation == 0 &&
		!req.Repartition.present() && !req.DocumentScan.present() &&
		!req.GlobalIndexLookup.present() && !req.mutationCapturePresent() &&
		!req.HasMinPosition && req.ReadFenceID.IsZero() && !req.RowBatch.present()
}

func (server *ReplicatedServer) executeReplicatedQueryTier(ctx context.Context, request *ReplicatedRequest, state raftservice.ServingState,
	inner *ShardRequest, owner any, budget replicatedSQLBudget, maximum int) (*ReplicatedResponse, bool) {
	return server.executeReplicatedQueryTierCall(ctx, request, state, inner, owner, budget, maximum, nil)
}

func (server *ReplicatedServer) executeReplicatedQueryTierCall(ctx context.Context, request *ReplicatedRequest, state raftservice.ServingState,
	inner *ShardRequest, owner any, budget replicatedSQLBudget, maximum int,
	authorize raftservice.ProposalAuthorization) (*ReplicatedResponse, bool) {
	wireState := replicatedWireState(state)
	refuse := func(code ReplicatedRefusalCode) (*ReplicatedResponse, bool) {
		return &ReplicatedResponse{Kind: ReplicatedRefusal, Refusal: code, HasState: true, State: wireState}, false
	}
	charge, chargeOK := budget.reservationBytesChecked(0)
	pointRead := replicatedSQLPointReadEligible(inner)
	if pointRead {
		if _, pointOwner := owner.(replicatedSQLPointReadOwner); pointOwner {
			charge, chargeOK = budget.reservationBytesForPoint(inner.PrimaryKeyRead.MaxDocumentBytes)
		}
	}
	if !chargeOK {
		return refuse(ReplicatedRefusalAdmissionBound)
	}
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
	if pointRead {
		if pointOwner, ok := owner.(replicatedSQLPointReadOwner); ok {
			return server.executeReplicatedPointQueryTierCall(
				ctx, request, state, inner, pointOwner, budget, maximum,
				authorize, lease, &retained,
			)
		}
	}
	dataOwner, ok := owner.(replicatedSQLReadOwner)
	if !ok {
		return refuse(ReplicatedRefusalUnavailable)
	}
	var cut raftservice.LinearizableDataReadCut
	quorum := trace.StartRegion(ctx, "sql.read.quorum")
	err := dataOwner.ReadLinearizableDataInto(ctx, raftservice.LinearizableDataReadRequest{Fence: state.Fence(), Capability: request.Capability, Authorize: authorize}, &cut)
	quorum.End()
	if err != nil {
		switch {
		case errors.Is(err, raftmodel.ErrNotLeader), errors.Is(err, raftmodel.ErrReadLeadershipLost):
			return &ReplicatedResponse{Kind: ReplicatedNotLeader, HasState: true, State: wireState}, false
		case errors.Is(err, raftservice.ErrServingFence):
			return refuse(ReplicatedRefusalStaleFence)
		case errors.Is(err, raftservice.ErrServingAuthorization):
			return refuse(ReplicatedRefusalUnavailable)
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

func (server *ReplicatedServer) executeReplicatedPointQueryTierCall(
	ctx context.Context,
	request *ReplicatedRequest,
	state raftservice.ServingState,
	inner *ShardRequest,
	owner replicatedSQLPointReadOwner,
	budget replicatedSQLBudget,
	maximum int,
	authorize raftservice.ProposalAuthorization,
	lease *replicatedSQLLease,
	retained *bool,
) (*ReplicatedResponse, bool) {
	wireState := replicatedWireState(state)
	refuse := func(code ReplicatedRefusalCode) (*ReplicatedResponse, bool) {
		return &ReplicatedResponse{
			Kind: ReplicatedRefusal, Refusal: code,
			HasState: true, State: wireState,
		}, false
	}
	pointRefuse := func(err error) (*ReplicatedResponse, bool) {
		switch {
		case errors.Is(err, raftmodel.ErrNotLeader), errors.Is(err, raftmodel.ErrReadLeadershipLost):
			return &ReplicatedResponse{Kind: ReplicatedNotLeader, HasState: true, State: wireState}, false
		case errors.Is(err, raftservice.ErrServingFence):
			return refuse(ReplicatedRefusalStaleFence)
		case errors.Is(err, raftservice.ErrServingAuthorization):
			return refuse(ReplicatedRefusalUnavailable)
		case errors.Is(err, replicatedstate.ErrReadBehind):
			return refuse(ReplicatedRefusalReadBehind)
		case errors.Is(err, replicatedstate.ErrReadBufferBound):
			return refuse(ReplicatedRefusalReadBufferBound)
		case errors.Is(err, replicatedstate.ErrTransactionIntentActive):
			return refuse(ReplicatedRefusalReadIntentActive)
		case errors.Is(err, raftservice.ErrIngressFull), errors.Is(err, raftservice.ErrPendingReadsFull):
			return refuse(ReplicatedRefusalAdmissionBound)
		default:
			return refuse(ReplicatedRefusalUnavailable)
		}
	}
	primary := inner.PrimaryKeyRead
	var cut raftservice.LinearizablePointReadCut
	quorum := trace.StartRegion(ctx, "sql.read.quorum")
	err := owner.ReadLinearizablePointInto(ctx, raftservice.LinearizablePointReadRequest{
		Fence: state.Fence(), Capability: request.Capability, Authorize: authorize,
	}, &cut)
	quorum.End()
	if err != nil {
		return pointRefuse(err)
	}
	defer cut.Close()
	point, err := cut.PointReadInto(
		ctx, primary.Relation, primary.Keys[0], int(primary.MaxDocumentBytes), nil,
	)
	if err != nil {
		return pointRefuse(err)
	}
	source, ok := cut.Source().(replicatedSQLPointReadSource)
	if !ok {
		return refuse(ReplicatedRefusalUnavailable)
	}
	execution := trace.StartRegion(ctx, "sql.read.execute")
	encoded, err := executeFencedSQLPointBudget(
		ctx, source, primary, inner, budget, point, lease,
	)
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
	wireState = replicatedReadState(wireState, request.Fence, point.Fence.Applied)
	response := &ReplicatedResponse{
		Kind: ReplicatedQueryResult, HasState: true, State: wireState,
		ReadApplied: point.Fence.Applied, Value: encoded, readLease: lease,
	}
	if !validReplicatedResponse(response) {
		return refuse(ReplicatedRefusalUnavailable)
	}
	*retained = true
	return response, false
}

// These are the response shapes admitted by the RF3 SQL wire budget.
// Charge the full frame grammar without allocating or overflowing on lengths.
func replicatedSQLResponseFits(response *ShardResponse, limit int) bool {
	if response == nil || response.HasReadPosition || !response.ReadPosition.IsZero() ||
		!response.DocumentScan.canonical() || response.DocumentScan.Present || !response.Exchange.canonical() || response.Exchange.present() ||
		validateTransactionReply(response.Transaction) != nil || response.Transaction.Role != TransactionRoleNone ||
		response.RowBatch != (RowBatchReply{}) || response.RowsAffected != 0 {
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
		if len(response.Columns) > maxColumns || len(response.Rows) > maxRows || response.ErrorKind != 0 || response.ErrorMessage != "" {
			return false
		}
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
				if cell.Null && len(cell.Bytes) != 0 {
					return false
				}
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
		return response.ErrorKind.valid() && len(response.Columns) == 0 && len(response.Rows) == 0 &&
			take(5) && take(len(response.ErrorMessage))
	default:
		return false
	}
}

func replicatedSemanticSQLResultValid(response *ShardResponse) bool {
	return (response != nil && (response.Kind == ResponseRows || response.Kind == ResponseError)) &&
		replicatedSQLResponseFits(response, MaxReplicatedSQLResultBytes)
}

func executeFencedSQLBudget(ctx context.Context, source interface {
	NewDataReadSession(context.Context, *replicatedstate.DataReadCut, query.ExecOptions) (*sqldriver.ReplicatedReadSession, error)
}, cut *replicatedstate.DataReadCut, req *ShardRequest, budget replicatedSQLBudget) (encoded []byte, err error) {
	defer func() {
		err = replicatedSQLContextError(ctx, err)
		if err != nil {
			encoded = nil
		}
	}()
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
	flag.BindDone(ctx.Done())
	options := query.ExecOptions{
		Cancel: &flag, ResultRows: rows, ResultBytes: int64(maxBytes),
		MemoryBytes: budget.workingBytes, IntermediateBytes: budget.workingBytes,
		AggregateBytes: budget.workingBytes,
	}
	if reusable, ok := source.(replicatedSQLDataReadReuseSource); ok {
		lease, acquireErr := reusable.AcquireReplicatedDataRead(
			ctx, cut, req.SQL, req.ParamTypes, req.PartialAggregate, options,
		)
		if acquireErr == nil {
			return executeFencedSQLLease(ctx, lease, req, budget, &flag, nil)
		}
		if !errors.Is(acquireErr, sqldriver.ErrReplicatedReadReuseUnsupported) {
			return nil, acquireErr
		}
	}
	session, err := source.NewDataReadSession(ctx, cut, options)
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

func executeFencedSQLPointBudget(
	ctx context.Context,
	source replicatedSQLPointReadSource,
	primary PrimaryKeyReadRequest,
	req *ShardRequest,
	budget replicatedSQLBudget,
	point replicatedstate.PointReadResult,
	frame *replicatedSQLLease,
) (encoded []byte, err error) {
	defer func() {
		err = replicatedSQLContextError(ctx, err)
		if err != nil {
			encoded = nil
		}
	}()
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
	flag := &frame.cancel
	flag.Reset()
	flag.BindDone(ctx.Done())
	defer flag.BindDone(nil)
	options := query.ExecOptions{
		Cancel: flag, ResultRows: rows, ResultBytes: int64(maxBytes),
		MemoryBytes: budget.workingBytes, IntermediateBytes: budget.workingBytes,
		AggregateBytes: budget.workingBytes,
	}
	if reusable, ok := source.(interface {
		AcquireReplicatedPointReadInto(context.Context, replication.RelationID, []byte, bool,
			[]byte, []byte, string, []sqldriver.ParamType, bool, query.ExecOptions, *sqldriver.ReplicatedReadLease) error
	}); ok {
		err := reusable.AcquireReplicatedPointReadInto(ctx, primary.Relation, primary.Keys[0],
			point.Found, point.Value, primary.PrimaryPath, req.SQL, req.ParamTypes, req.PartialAggregate, options, &frame.point)
		if err == nil {
			return executeFencedSQLLease(ctx, &frame.point, req, budget, flag, &frame.cursor)
		}
		if !errors.Is(err, sqldriver.ErrReplicatedReadReuseUnsupported) {
			return nil, err
		}
	}
	if reusable, ok := source.(replicatedSQLPointReadReuseSource); ok {
		lease, acquireErr := reusable.AcquireReplicatedPointRead(
			ctx, primary.Relation, primary.Keys[0], point.Found, point.Value,
			primary.PrimaryPath, req.SQL, req.ParamTypes, req.PartialAggregate,
			options,
		)
		if acquireErr == nil {
			return executeFencedSQLLease(ctx, lease, req, budget, flag, &frame.cursor)
		}
		if !errors.Is(acquireErr, sqldriver.ErrReplicatedReadReuseUnsupported) {
			return nil, acquireErr
		}
	}
	session, err := source.NewPointReadSession(
		ctx, primary.Relation, primary.Keys[0], point.Found, point.Value,
		primary.PrimaryPath, options,
	)
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
	if err := prepared.QueryCandidateKeysInto(
		ctx, runtimeArgs(req.Params), primary.PrimaryPath, primary.Keys, &cursor,
	); err != nil {
		return nil, err
	}
	encoding := trace.StartRegion(ctx, "sql.read.encode")
	defer encoding.End()
	return encodeSQLReadCursor(cursor.Snapshot(), prepared.Columns(), budget.resultBytes, flag)
}

func executeFencedSQLLease(
	ctx context.Context,
	lease *sqldriver.ReplicatedReadLease,
	req *ShardRequest,
	budget replicatedSQLBudget,
	flag *query.CancelFlag,
	cursor *sqldriver.Cursor,
) (encoded []byte, err error) {
	if lease == nil {
		return nil, sqldriver.ErrReplicatedReadLeaseClosed
	}
	// Finish invalidates the lease on normal return. During a panic these
	// defers close the cursor and retire its still-active execution slot.
	defer func() { _ = lease.Abort(nil) }()
	if cursor == nil {
		cursor = &sqldriver.Cursor{}
	}
	defer func() { _ = cursor.Close() }()
	if req.PrimaryKeyRead.present() {
		err = lease.QueryCandidateKeysInto(
			ctx, runtimeArgs(req.Params), req.PrimaryKeyRead.PrimaryPath,
			req.PrimaryKeyRead.Keys, cursor,
		)
	} else {
		err = lease.QueryInto(ctx, runtimeArgs(req.Params), cursor)
	}
	if err == nil {
		encoding := trace.StartRegion(ctx, "sql.read.encode")
		encoded, err = encodeSQLReadCursor(
			cursor.Snapshot(), lease.Columns(), budget.resultBytes, flag,
		)
		encoding.End()
	}
	if closeErr := cursor.Close(); err == nil {
		err = closeErr
	}
	finishErr := lease.Finish(err)
	if err == nil {
		err = finishErr
	}
	return encoded, err
}

// Preserve context errors for both query execution and result encoding.
func replicatedSQLContextError(ctx context.Context, err error) error {
	if contextErr := ctx.Err(); contextErr != nil && (err == nil || errors.Is(err, query.ErrCanceled)) {
		return contextErr
	}
	return err
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
