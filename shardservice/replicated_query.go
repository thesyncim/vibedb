package shardservice

import (
	"context"
	"errors"
	"math"
	"runtime/trace"
	"sync"
	"sync/atomic"
	"time"

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

const maxPooledPointReadScratchBytes = 64 << 10

type replicatedSQLLease struct {
	budget   *replicatedFrameByteBudget
	bytes    int64
	point    sqldriver.ReplicatedReadLease
	cursor   sqldriver.Cursor
	cancel   query.CancelFlag
	pointBuf []byte
	pointCut raftservice.LinearizablePointReadCut
	dataCut  raftservice.LinearizableDataReadCut
}

var replicatedSQLRequestPool = sync.Pool{
	New: func() any {
		return &ShardRequest{}
	},
}

var replicatedSQLLeasePool = sync.Pool{
	New: func() any {
		return &replicatedSQLLease{}
	},
}

func acquireReplicatedSQLLease(budget *replicatedFrameByteBudget, bytes int64) *replicatedSQLLease {
	lease := replicatedSQLLeasePool.Get().(*replicatedSQLLease)
	lease.budget = budget
	lease.bytes = bytes
	return lease
}

func (l *replicatedSQLLease) Release() {
	if l.budget != nil {
		l.budget.releaseSQL(l.bytes)
		l.budget = nil
		l.bytes = 0
		l.pointCut = raftservice.LinearizablePointReadCut{}
		l.dataCut = raftservice.LinearizableDataReadCut{}
		if cap(l.pointBuf) > maxPooledPointReadScratchBytes {
			l.pointBuf = nil
		} else {
			l.pointBuf = l.pointBuf[:0]
		}
		replicatedSQLLeasePool.Put(l)
	}
}

func (server *ReplicatedServer) executeReplicatedQueryCall(
	ctx context.Context,
	request *ReplicatedRequest,
	state raftservice.ServingState,
	semantic *ShardRequest,
	authorize raftservice.ProposalAuthorization,
) *ReplicatedResponse {
	return server.executeReplicatedQueryCallValidated(ctx, request, state, semantic, authorize, false, semantic != nil, false)
}

func (server *ReplicatedServer) executeReplicatedQueryCallValidated(
	ctx context.Context,
	request *ReplicatedRequest,
	state raftservice.ServingState,
	semantic *ShardRequest,
	authorize raftservice.ProposalAuthorization,
	semanticValidated bool,
	inProcessSemantic bool,
	fusedPoint bool,
	concurrentAuthorize ...raftservice.ConcurrentReadAuthorization,
) *ReplicatedResponse {
	wireState := replicatedWireState(state)
	refuse := func(code ReplicatedRefusalCode) *ReplicatedResponse {
		return server.fusedPointRefusalResponse(ctx, request, fusedPoint, &wireState,
			ReplicatedRefusal, code)
	}
	if !fusedPoint && request.Fence != wireState.Fence {
		return refuse(ReplicatedRefusalStaleFence)
	}
	inner := semantic
	var err error
	if inner == nil {
		pooled := replicatedSQLRequestPool.Get().(*ShardRequest)
		defer func() {
			*pooled = ShardRequest{}
			replicatedSQLRequestPool.Put(pooled)
		}()
		err = decodeReplicatedSQLRequestInto(request.Query, pooled)
		if err != nil {
			return refuse(ReplicatedRefusalStaleFence)
		}
		inner = pooled
	} else if !semanticValidated {
		err = ValidateRequest(inner)
		if err != nil {
			return refuse(ReplicatedRefusalStaleFence)
		}
	}
	if inner.Authority != request.Authority {
		return refuse(ReplicatedRefusalStaleFence)
	}
	// A fused point call has no status probe to populate state. Its exact
	// owner fence and the SQL allocation identity are checked immediately after
	// ReadLinearizablePointInto returns the serialized owner cut. Until then,
	// caller supplied Distribution and Shard names are only untrusted inputs.
	if !fusedPoint && !replicatedSQLRequestMatchesState(request, inner, state) {
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
		if deadline, hasDeadline := ctx.Deadline(); !hasDeadline || deadline.After(time.Now().Add(inner.Deadline)) {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, inner.Deadline)
			defer cancel()
		}
	}
	maximum := int(request.MaxValueBytes)
	if inner.MaxResultBytes > 0 && inner.MaxResultBytes < uint64(maximum) {
		maximum = int(inner.MaxResultBytes)
	}
	key := hashSQLBudgetKey(inner.SQL)
	semanticDirect := inProcessSemantic && semantic != nil && semanticValidated
	var directAuthorize raftservice.ConcurrentReadAuthorization
	if len(concurrentAuthorize) != 0 {
		directAuthorize = concurrentAuthorize[0]
	}
	for tier := server.sqlHints.lookup(key); tier < len(replicatedSQLTiers); tier++ {
		budget := replicatedSQLTiers[tier]
		budget.resultBytes = min(budget.resultBytes, int(request.MaxValueBytes))
		response, grow := server.executeReplicatedQueryTierCall(ctx, request, state, inner, owner, budget, maximum, authorize, fusedPoint, semanticDirect, directAuthorize)
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
// more memory and still escalate normally. Fixed direct-mapped atomic storage
// avoids global mutex contention and SHA-256 overhead on every read.
type replicatedSQLBudgetHints struct {
	entries [256]atomic.Uint64
}

func hashSQLBudgetKey(sql string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(sql); i++ {
		h ^= uint64(sql[i])
		h *= prime64
	}
	if h&^uint64(0xff) == 0 {
		h |= 1 << 8
	}
	return h
}

func (hints *replicatedSQLBudgetHints) lookup(key uint64) int {
	entry := hints.entries[byte(key^(key>>8))].Load()
	if entry&^uint64(0xff) == key&^uint64(0xff) {
		tier := int(entry & 0xff)
		if tier < len(replicatedSQLTiers) {
			return tier
		}
	}
	return 0
}

func (hints *replicatedSQLBudgetHints) record(key uint64, tier int) {
	if tier <= 0 || tier >= len(replicatedSQLTiers) {
		return
	}
	tag := key &^ uint64(0xff)
	slot := &hints.entries[byte(key^(key>>8))]
	for {
		cur := slot.Load()
		if cur&^uint64(0xff) == tag && int(cur&0xff) >= tier {
			return
		}
		if slot.CompareAndSwap(cur, tag|uint64(tier)) {
			return
		}
	}
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

type fencedSQLResult struct {
	encoded  []byte
	semantic *ShardResponse
	typed    []query.Cell
}

func (server *ReplicatedServer) executeReplicatedQueryTierCall(ctx context.Context, request *ReplicatedRequest, state raftservice.ServingState,
	inner *ShardRequest, owner any, budget replicatedSQLBudget, maximum int,
	authorize raftservice.ProposalAuthorization, fusedPoint, semanticDirect bool,
	concurrentAuthorize ...raftservice.ConcurrentReadAuthorization) (*ReplicatedResponse, bool) {
	wireState := replicatedWireState(state)
	refuse := func(code ReplicatedRefusalCode) (*ReplicatedResponse, bool) {
		return server.fusedPointRefusalResponse(ctx, request, fusedPoint, &wireState,
			ReplicatedRefusal, code), false
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
	lease := acquireReplicatedSQLLease(&server.frames, charge)
	retained := false
	defer func() {
		if !retained {
			lease.Release()
		}
	}()
	var directAuthorize raftservice.ConcurrentReadAuthorization
	if len(concurrentAuthorize) != 0 {
		directAuthorize = concurrentAuthorize[0]
	}
	if pointRead {
		if pointOwner, ok := owner.(replicatedSQLPointReadOwner); ok {
			return server.executeReplicatedPointQueryTierCall(
				ctx, request, state, inner, pointOwner, budget, maximum,
				authorize, directAuthorize, fusedPoint, semanticDirect, lease, &retained,
			)
		}
	}
	dataOwner, ok := owner.(replicatedSQLReadOwner)
	if !ok {
		return refuse(ReplicatedRefusalUnavailable)
	}
	cut := &lease.dataCut
	quorum := trace.StartRegion(ctx, "sql.read.quorum")
	dataFence := state.Fence()
	if fusedPoint {
		dataFence = replicatedServingFence(request.Fence)
	}
	dataAuthorize := authorize
	if directAuthorize != nil {
		dataAuthorize = nil
	}
	err := dataOwner.ReadLinearizableDataInto(ctx, raftservice.LinearizableDataReadRequest{
		Fence: dataFence, Capability: request.Capability,
		Authorize: dataAuthorize, ConcurrentAuthorize: directAuthorize,
	}, cut)
	quorum.End()
	if err != nil {
		switch {
		case errors.Is(err, raftmodel.ErrNotLeader), errors.Is(err, raftmodel.ErrReadLeadershipLost):
			return server.fusedPointRefusalResponse(ctx, request, fusedPoint, &wireState,
				ReplicatedNotLeader, ReplicatedRefusalNone), false
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
	if fusedPoint {
		cutState := cut.State()
		wireState = replicatedWireState(cutState)
		if !replicatedSQLRequestMatchesState(request, inner, cutState) ||
			cutState.Status.MemberID != cutState.Identity.MemberID ||
			cutState.Status.LeaderID != cutState.Identity.MemberID {
			return refuse(ReplicatedRefusalStaleFence)
		}
	}
	source, ok := cut.Source().(interface {
		NewDataReadSession(context.Context, *replicatedstate.DataReadCut, query.ExecOptions) (*sqldriver.ReplicatedReadSession, error)
	})
	if !ok {
		return refuse(ReplicatedRefusalUnavailable)
	}
	execution := trace.StartRegion(ctx, "sql.read.execute")
	res, err := executeFencedSQLBudget(ctx, source, cut.Data(), inner, budget, lease, semanticDirect)
	execution.End()
	if err != nil {
		if ctx.Err() == nil && (budget.canGrow(err, maximum) ||
			errors.Is(err, errSQLReadFrameBound) && budget.resultBytes < int(request.MaxValueBytes)) {
			return nil, true
		}
		if errors.Is(err, errSQLReadFrameBound) {
			return refuse(ReplicatedRefusalReadBufferBound)
		}
		classified := classifyError(err)
		if semanticDirect {
			if !replicatedSQLResponseFits(classified, budget.resultBytes) {
				if budget.resultBytes < int(request.MaxValueBytes) && ctx.Err() == nil {
					return nil, true
				}
				return refuse(ReplicatedRefusalReadBufferBound)
			}
			res = fencedSQLResult{semantic: classified}
		} else {
			encoded, err := encodeSQLReadError(classified, budget.resultBytes)
			if err != nil {
				if errors.Is(err, errSQLReadFrameBound) {
					if budget.resultBytes < int(request.MaxValueBytes) && ctx.Err() == nil {
						return nil, true
					}
					return refuse(ReplicatedRefusalReadBufferBound)
				}
				return refuse(ReplicatedRefusalUnavailable)
			}
			res = fencedSQLResult{encoded: encoded}
		}
	}
	wireState = replicatedReadState(wireState, request.Fence, cut.Data().Fence().Applied)
	var response *ReplicatedResponse
	if semanticDirect {
		response = &ReplicatedResponse{
			Kind: ReplicatedQueryResult, HasState: true, State: wireState,
			ReadApplied: cut.Data().Fence().Applied,
			sqlResult:   res.semantic, typedCells: res.typed, sqlOwned: true, readLease: lease,
		}
	} else {
		response = &ReplicatedResponse{
			Kind: ReplicatedQueryResult, HasState: true, State: wireState,
			ReadApplied: cut.Data().Fence().Applied, Value: res.encoded, readLease: lease,
		}
	}
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
	concurrentAuthorize raftservice.ConcurrentReadAuthorization,
	fusedPoint, semanticDirect bool,
	lease *replicatedSQLLease,
	retained *bool,
) (*ReplicatedResponse, bool) {
	wireState := replicatedWireState(state)
	refuse := func(code ReplicatedRefusalCode) (*ReplicatedResponse, bool) {
		return server.fusedPointRefusalResponse(ctx, request, fusedPoint, &wireState,
			ReplicatedRefusal, code), false
	}
	pointRefuse := func(err error) (*ReplicatedResponse, bool) {
		switch {
		case errors.Is(err, raftmodel.ErrNotLeader), errors.Is(err, raftmodel.ErrReadLeadershipLost):
			return server.fusedPointRefusalResponse(ctx, request, fusedPoint, &wireState,
				ReplicatedNotLeader, ReplicatedRefusalNone), false
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
	cut := &lease.pointCut
	quorum := trace.StartRegion(ctx, "sql.read.quorum")
	pointFence := state.Fence()
	if fusedPoint {
		pointFence = replicatedServingFence(request.Fence)
	}
	// The concurrent predicate is the complete native SQL authorization for
	// this point attempt. Leave the legacy field empty so a queued fallback
	// normalizes exactly this predicate instead of trusting two callbacks to
	// remain equivalent.
	pointAuthorize := authorize
	if concurrentAuthorize != nil {
		pointAuthorize = nil
	}
	err := owner.ReadLinearizablePointInto(ctx, raftservice.LinearizablePointReadRequest{
		Fence: pointFence, Capability: request.Capability, Authorize: pointAuthorize,
		ConcurrentAuthorize: concurrentAuthorize,
	}, cut)
	quorum.End()
	if err != nil {
		return pointRefuse(err)
	}
	defer cut.Close()
	cutState := cut.State()
	wireState = replicatedWireState(cutState)
	if !replicatedSQLRequestMatchesState(request, inner, cutState) ||
		cutState.Status.MemberID != cutState.Identity.MemberID ||
		cutState.Status.LeaderID != cutState.Identity.MemberID {
		return refuse(ReplicatedRefusalStaleFence)
	}
	point, err := cut.PointReadInto(
		ctx, primary.Relation, primary.Keys[0], int(primary.MaxDocumentBytes), lease.pointBuf[:0],
	)
	if cap(point.Value) <= maxPooledPointReadScratchBytes && cap(point.Value) > cap(lease.pointBuf) {
		lease.pointBuf = point.Value[:0]
	}
	// Authority-backed cuts can replace their admission with a fresh ReadIndex
	// cut after a final validation race. Carry the final serialized witness into
	// both the result and any refusal emitted below.
	wireState = replicatedWireState(cut.State())
	if err != nil {
		return pointRefuse(err)
	}
	source, ok := cut.Source().(replicatedSQLPointReadSource)
	if !ok {
		return refuse(ReplicatedRefusalUnavailable)
	}
	execution := trace.StartRegion(ctx, "sql.read.execute")
	res, err := executeFencedSQLPointBudget(
		ctx, source, primary, inner, budget, point, lease, semanticDirect,
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
		classified := classifyError(err)
		if semanticDirect {
			if !replicatedSQLResponseFits(classified, budget.resultBytes) {
				if budget.resultBytes < int(request.MaxValueBytes) && ctx.Err() == nil {
					return nil, true
				}
				return refuse(ReplicatedRefusalReadBufferBound)
			}
			res = fencedSQLResult{semantic: classified}
		} else {
			encoded, err := encodeSQLReadError(classified, budget.resultBytes)
			if err != nil {
				if errors.Is(err, errSQLReadFrameBound) {
					if budget.resultBytes < int(request.MaxValueBytes) && ctx.Err() == nil {
						return nil, true
					}
					return refuse(ReplicatedRefusalReadBufferBound)
				}
				return refuse(ReplicatedRefusalUnavailable)
			}
			res = fencedSQLResult{encoded: encoded}
		}
	}
	wireState = replicatedReadState(wireState, request.Fence, point.Fence.Applied)
	var response *ReplicatedResponse
	if semanticDirect {
		response = &ReplicatedResponse{
			Kind: ReplicatedQueryResult, HasState: true, State: wireState,
			ReadApplied: point.Fence.Applied,
			sqlResult:   res.semantic, typedCells: res.typed, sqlOwned: true, readLease: lease,
		}
	} else {
		response = &ReplicatedResponse{
			Kind: ReplicatedQueryResult, HasState: true, State: wireState,
			ReadApplied: point.Fence.Applied, Value: res.encoded, readLease: lease,
		}
	}
	if !validReplicatedResponse(response) {
		return refuse(ReplicatedRefusalUnavailable)
	}
	*retained = true
	return response, false
}

func replicatedSQLRequestMatchesState(
	request *ReplicatedRequest,
	inner *ShardRequest,
	state raftservice.ServingState,
) bool {
	if request == nil || inner == nil {
		return false
	}
	wireState := replicatedWireState(state)
	return request.Fence == wireState.Fence &&
		inner.Authority == request.Authority &&
		string(inner.Distribution) == state.Identity.Distribution &&
		string(inner.Shard) == state.Identity.Shard &&
		uint64(inner.AllocationGeneration) == request.Fence.AllocationGeneration &&
		uint64(inner.RoutingVersion) == request.Fence.Command.RoutingVersion &&
		uint64(inner.OwnershipEpoch) == request.Fence.Command.OwnershipEpoch
}

// fusedPointRefusalResponse performs the one bounded status refresh that a
// fused point call may need after failing before it has an accepted cut. A
// response with a state must carry an actual owner witness; if that witness is
// unavailable, it returns the grammar's no-state unavailable form. kind is
// normally ReplicatedRefusal; retaining ReplicatedNotLeader lets a refreshed
// leader hint drive the gateway's existing retry path.
func (server *ReplicatedServer) fusedPointRefusalResponse(
	ctx context.Context,
	request *ReplicatedRequest,
	fusedPoint bool,
	wireState *ReplicatedMemberState,
	kind ReplicatedResponseKind,
	code ReplicatedRefusalCode,
) *ReplicatedResponse {
	if wireState == nil || !validReplicatedMemberState(*wireState) {
		if !fusedPoint || wireState == nil || server == nil || server.owner == nil ||
			request == nil || ctx == nil || context.Cause(ctx) != nil {
			return &ReplicatedResponse{Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalUnavailable}
		}
		refreshed, err := server.owner.Probe(ctx, request.Fence.Group)
		if err != nil {
			*wireState = ReplicatedMemberState{}
			return &ReplicatedResponse{Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalUnavailable}
		}
		candidate := replicatedWireState(refreshed)
		if !validReplicatedMemberState(candidate) {
			*wireState = ReplicatedMemberState{}
			return &ReplicatedResponse{Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalUnavailable}
		}
		*wireState = candidate
	}
	if kind == ReplicatedRefusal && code != ReplicatedRefusalNone &&
		request != nil && wireState.Fence != request.Fence {
		code = ReplicatedRefusalStaleFence
	}
	return &ReplicatedResponse{Kind: kind, Refusal: code, HasState: true, State: *wireState}
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
}, cut *replicatedstate.DataReadCut, req *ShardRequest, budget replicatedSQLBudget, frame *replicatedSQLLease, semanticDirect bool) (res fencedSQLResult, err error) {
	defer func() {
		err = replicatedSQLContextError(ctx, err)
		if err != nil {
			res = fencedSQLResult{}
		}
	}()
	maxBytes := budget.resultBytes
	if req.ExecutionMode != ExecutionReadOnly || req.ReadPolicy != ReadStrong ||
		req.Transaction.Operation != 0 || req.Exchange.Operation != 0 || req.Repartition.present() ||
		req.DocumentScan.present() || req.GlobalIndexLookup.present() || req.mutationCapturePresent() ||
		!req.ReadFenceID.IsZero() || req.HasMinPosition || req.RowBatch.present() {
		errResp := NewErrorResponse(ErrorMalformedRequest, "RF3 SQL read does not support this execution mode")
		if semanticDirect {
			if !replicatedSQLResponseFits(errResp, budget.resultBytes) {
				return fencedSQLResult{}, errSQLReadFrameBound
			}
			return fencedSQLResult{semantic: errResp}, nil
		}
		encoded, encErr := encodeSQLReadError(errResp, budget.resultBytes)
		return fencedSQLResult{encoded: encoded}, encErr
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
	if reusable, ok := source.(replicatedSQLDataReadReuseSource); ok {
		lease, acquireErr := reusable.AcquireReplicatedDataRead(
			ctx, cut, req.SQL, req.ParamTypes, req.PartialAggregate, options,
		)
		if acquireErr == nil {
			return executeFencedSQLLease(ctx, lease, req, budget, flag, &frame.cursor, semanticDirect)
		}
		if !errors.Is(acquireErr, sqldriver.ErrReplicatedReadReuseUnsupported) {
			return fencedSQLResult{}, acquireErr
		}
	}
	session, err := source.NewDataReadSession(ctx, cut, options)
	if err != nil {
		return fencedSQLResult{}, err
	}
	defer session.Close()
	prepared, err := prepareShardSQL(
		ctx, session, req.SQL, req.ParamTypes, req.PartialAggregate,
	)
	if err != nil {
		return fencedSQLResult{}, err
	}
	defer prepared.Close()
	cursor := &frame.cursor
	defer cursor.Close()
	if req.PrimaryKeyRead.present() {
		err = prepared.QueryCandidateKeysInto(ctx, runtimeArgs(req.Params), req.PrimaryKeyRead.PrimaryPath, req.PrimaryKeyRead.Keys, cursor)
	} else {
		err = prepared.QueryInto(ctx, runtimeArgs(req.Params), cursor)
	}
	if err != nil {
		return fencedSQLResult{}, err
	}
	encoding := trace.StartRegion(ctx, "sql.read.encode")
	defer encoding.End()
	if semanticDirect {
		sem, typed, matErr := materializeSQLReadCursor(cursor.Snapshot(), prepared.Columns(), budget.resultBytes, flag)
		return fencedSQLResult{semantic: sem, typed: typed}, matErr
	}
	encoded, encErr := encodeSQLReadCursor(cursor.Snapshot(), prepared.Columns(), budget.resultBytes, flag)
	return fencedSQLResult{encoded: encoded}, encErr
}

func executeFencedSQLPointBudget(
	ctx context.Context,
	source replicatedSQLPointReadSource,
	primary PrimaryKeyReadRequest,
	req *ShardRequest,
	budget replicatedSQLBudget,
	point replicatedstate.PointReadResult,
	frame *replicatedSQLLease,
	semanticDirect bool,
) (res fencedSQLResult, err error) {
	defer func() {
		err = replicatedSQLContextError(ctx, err)
		if err != nil {
			res = fencedSQLResult{}
		}
	}()
	maxBytes := budget.resultBytes
	if req.ExecutionMode != ExecutionReadOnly || req.ReadPolicy != ReadStrong ||
		req.Transaction.Operation != 0 || req.Exchange.Operation != 0 || req.Repartition.present() ||
		req.DocumentScan.present() || req.GlobalIndexLookup.present() || req.mutationCapturePresent() ||
		!req.ReadFenceID.IsZero() || req.HasMinPosition || req.RowBatch.present() {
		errResp := NewErrorResponse(ErrorMalformedRequest, "RF3 SQL read does not support this execution mode")
		if semanticDirect {
			if !replicatedSQLResponseFits(errResp, budget.resultBytes) {
				return fencedSQLResult{}, errSQLReadFrameBound
			}
			return fencedSQLResult{semantic: errResp}, nil
		}
		encoded, encErr := encodeSQLReadError(errResp, budget.resultBytes)
		return fencedSQLResult{encoded: encoded}, encErr
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
			return executeFencedSQLLease(ctx, &frame.point, req, budget, flag, &frame.cursor, semanticDirect)
		}
		if !errors.Is(err, sqldriver.ErrReplicatedReadReuseUnsupported) {
			return fencedSQLResult{}, err
		}
	}
	if reusable, ok := source.(replicatedSQLPointReadReuseSource); ok {
		lease, acquireErr := reusable.AcquireReplicatedPointRead(
			ctx, primary.Relation, primary.Keys[0], point.Found, point.Value,
			primary.PrimaryPath, req.SQL, req.ParamTypes, req.PartialAggregate,
			options,
		)
		if acquireErr == nil {
			return executeFencedSQLLease(ctx, lease, req, budget, flag, &frame.cursor, semanticDirect)
		}
		if !errors.Is(acquireErr, sqldriver.ErrReplicatedReadReuseUnsupported) {
			return fencedSQLResult{}, acquireErr
		}
	}
	session, err := source.NewPointReadSession(
		ctx, primary.Relation, primary.Keys[0], point.Found, point.Value,
		primary.PrimaryPath, options,
	)
	if err != nil {
		return fencedSQLResult{}, err
	}
	defer session.Close()
	prepared, err := prepareShardSQL(
		ctx, session, req.SQL, req.ParamTypes, req.PartialAggregate,
	)
	if err != nil {
		return fencedSQLResult{}, err
	}
	defer prepared.Close()
	cursor := &frame.cursor
	defer cursor.Close()
	if err := prepared.QueryCandidateKeysInto(
		ctx, runtimeArgs(req.Params), primary.PrimaryPath, primary.Keys, cursor,
	); err != nil {
		return fencedSQLResult{}, err
	}
	encoding := trace.StartRegion(ctx, "sql.read.encode")
	defer encoding.End()
	if semanticDirect {
		sem, typed, matErr := materializeSQLReadCursor(cursor.Snapshot(), prepared.Columns(), budget.resultBytes, flag)
		return fencedSQLResult{semantic: sem, typed: typed}, matErr
	}
	encoded, encErr := encodeSQLReadCursor(cursor.Snapshot(), prepared.Columns(), budget.resultBytes, flag)
	return fencedSQLResult{encoded: encoded}, encErr
}

func executeFencedSQLLease(
	ctx context.Context,
	lease *sqldriver.ReplicatedReadLease,
	req *ShardRequest,
	budget replicatedSQLBudget,
	flag *query.CancelFlag,
	cursor *sqldriver.Cursor,
	semanticDirect bool,
) (res fencedSQLResult, err error) {
	if lease == nil {
		return fencedSQLResult{}, sqldriver.ErrReplicatedReadLeaseClosed
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
		if semanticDirect {
			res.semantic, res.typed, err = materializeSQLReadCursor(
				cursor.Snapshot(), lease.Columns(), budget.resultBytes, flag,
			)
		} else {
			res.encoded, err = encodeSQLReadCursor(
				cursor.Snapshot(), lease.Columns(), budget.resultBytes, flag,
			)
		}
		encoding.End()
	}
	if closeErr := cursor.Close(); err == nil {
		err = closeErr
	}
	finishErr := lease.Finish(err)
	if err == nil {
		err = finishErr
	}
	return res, err
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

// tryReserveSQL atomically reserves SQL and shared frame capacity without
// acquiring waitMu. Failed reservations never retain partial capacity.
func (budget *replicatedFrameByteBudget) tryReserveSQL(bytes int64) bool {
	limit := budget.sqlLimit()
	for {
		used := budget.sqlUsed.Load()
		if used < 0 || bytes > limit-used {
			return false
		}
		if budget.sqlUsed.CompareAndSwap(used, used+bytes) {
			if budget.reserve(bytes) {
				return true
			}
			budget.sqlUsed.Add(-bytes)
			return false
		}
	}
}

func (budget *replicatedFrameByteBudget) releaseSQL(bytes int64) {
	budget.sqlUsed.Add(-bytes)
	budget.release(bytes)
}

// reserveSQL applies bounded backpressure before taking a ReadIndex cut. Waiting
// requests retain only their already charged wire frames, never SQL workspaces
// or snapshots. Native operations retain nonblocking shared-frame admission.
func (budget *replicatedFrameByteBudget) reserveSQL(ctx context.Context, bytes int64) bool {
	if budget == nil || bytes <= 0 || bytes > budget.sqlLimit() || ctx.Err() != nil {
		return false
	}
	if budget.waiters.Load() == 0 && budget.tryReserveSQL(bytes) {
		return true
	}
	budget.waitMu.Lock()
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
		if budget.tryReserveSQL(bytes) {
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
