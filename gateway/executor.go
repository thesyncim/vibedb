package gateway

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/thesyncim/vibedb/distribution"
	queryplanner "github.com/thesyncim/vibedb/planner"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
)

// The bounded fan-out executor: it pins one catalog generation, routes, and
// dispatches the per-shard requests through a bounded worker pool under a global
// and per-shard deadline, enforcing an aggregate row and byte cap and cancelling
// outstanding shards on a hard failure or once the cap is hit. A stale routing
// version or ownership epoch reported by any shard triggers a bounded retry, but
// only after refreshing to a strictly newer catalog generation — one attempt
// never mixes generations. The partial-result policy is fail-closed: any shard
// failure fails the whole operation.

// ErrNoCatalog reports that no catalog generation has been published, so no
// operation can be routed.
var ErrNoCatalog = errors.New("gateway: no catalog generation is published")

// ErrWriteNotSupported reports a mutating statement submitted to the
// distributed read executor. Cross-shard writes require a separate protocol
// with durable command identity and completion semantics; Query refuses them
// before routing or network I/O so a fan-out can never partially commit.
var ErrWriteNotSupported = errors.New("gateway: distributed writes are not supported")

// WriteNotSupportedError identifies the statement kind refused by Query. It
// wraps ErrWriteNotSupported so callers can use errors.Is without parsing the
// diagnostic string.
type WriteNotSupportedError struct {
	Kind sqlast.Kind
}

func (e *WriteNotSupportedError) Error() string {
	return fmt.Sprintf("gateway: %s is not supported by the distributed read executor", e.Kind)
}

func (e *WriteNotSupportedError) Unwrap() error { return ErrWriteNotSupported }

// ErrStaleGeneration reports that a shard rejected the pinned generation and no
// strictly newer compatible generation was available to retry against, so the
// operation fails closed rather than routing against stale metadata.
var ErrStaleGeneration = errors.New("gateway: pinned catalog generation is stale and no newer generation is available")

// RefreshFunc obtains a catalog generation strictly newer than staleGen after a
// shard reports the pinned generation is stale. It must return a snapshot whose
// generation exceeds staleGen, or an error. A nil RefreshFunc re-reads the
// executor's catalog holder.
type RefreshFunc func(ctx context.Context, staleGen uint64) (*Snapshot, error)

// Options configure an [Executor]. The zero value is usable: the default
// operational profiles, a small retry budget, and the catalog holder as the
// refresh source.
type Options struct {
	// Profiles overrides the per-class operational profiles. A class absent from
	// the map keeps its DefaultProfiles entry.
	Profiles map[OperationClass]Profile
	// MaxRetries bounds stale-generation retries. Zero selects a conservative
	// default; a negative value disables retry.
	MaxRetries int
	// Refresh supplies a newer generation on a stale-generation retry. Nil
	// re-reads the catalog holder.
	Refresh RefreshFunc
}

// defaultMaxRetries bounds stale-generation retries when Options leaves it zero.
const defaultMaxRetries = 2

// Executor routes and dispatches bounded distributed reads over a pinned catalog
// generation. It is safe for concurrent use.
type Executor struct {
	client   *Client
	catalog  *CatalogHolder
	profiles map[OperationClass]Profile
	refresh  RefreshFunc
	maxRetry int
	routers  *routerPool
	metrics  Metrics
}

// NewExecutor returns an executor that dispatches through client and pins
// generations from catalog. Both are required.
func NewExecutor(client *Client, catalog *CatalogHolder, opts Options) *Executor {
	profiles := DefaultProfiles()
	for class, p := range opts.Profiles {
		profiles[class] = p
	}
	for class, p := range profiles {
		profiles[class] = p.withDefaults()
	}
	maxRetry := opts.MaxRetries
	if maxRetry == 0 {
		maxRetry = defaultMaxRetries
	}
	if maxRetry < 0 {
		maxRetry = 0
	}
	return &Executor{
		client:   client,
		catalog:  catalog,
		profiles: profiles,
		refresh:  opts.Refresh,
		maxRetry: maxRetry,
		routers:  newRouterPool(),
	}
}

// Metrics returns a snapshot of the executor's route and fan-out counters.
func (e *Executor) Metrics() MetricsSnapshot { return e.metrics.Snapshot() }

// Query is one bounded distributed read. SQL and its typed parameters are the
// only semantic inputs; the pinned catalog and shared SQL routing compiler
// derive the distribution, shard constraints, merge order, and global limit.
type Query struct {
	SQL    string
	Params []shardservice.Param

	Class OperationClass
}

// Result is a distributed read's merged outcome plus the routing metadata a
// caller reads for observability. Kind is ResponseRows for the read path;
// RowsAffected is meaningful only for a single-shard completion passthrough.
type Result struct {
	Kind         shardservice.ResponseKind
	Columns      []shardservice.Column
	Rows         [][]shardservice.Cell
	RowsAffected int64

	RouteKind     distribution.RouteKind
	Generation    uint64
	ShardsFanned  int
	Retries       int
	ScatterReason ScatterReason

	// PlanFingerprint and Planning expose deterministic physical-plan identity
	// and bounded search/space counters without retaining the memo or AST.
	PlanFingerprint string
	Planning        queryplanner.OptimizerStatistics
}

// Explanation is a no-dispatch distributed physical plan. It includes the
// selected topology shape, multidimensional cost, and bounded search/space
// counters. PhysicalPlan contains operator/private IDs, never SQL literals.
type Explanation struct {
	PhysicalPlan    string
	PlanFingerprint string
	Cost            queryplanner.Cost
	Planning        queryplanner.OptimizerStatistics
	RouteKind       distribution.RouteKind
	Generation      uint64
	Shards          int
	ScatterReason   ScatterReason
}

// Query routes and dispatches q, retrying against a refreshed generation when a
// shard reports the pinned generation is stale. It pins one generation per
// attempt and never mixes generations within an attempt.
func (e *Executor) Query(ctx context.Context, q Query) (*Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	profile := e.profileFor(q.Class)
	opctx, cancel := context.WithTimeout(ctx, profile.GlobalDeadline)
	defer cancel()
	args, err := queryRuntimeArgs(q.Params)
	if err != nil {
		return nil, err
	}

	var staleGen uint64
	for attempt := 0; ; attempt++ {
		if err := opctx.Err(); err != nil {
			return nil, err
		}
		snap, err := e.pin(opctx, attempt, staleGen)
		if err != nil {
			return nil, err
		}
		prepared, err := snap.Prepare(opctx, q.SQL)
		if err != nil {
			return nil, err
		}
		if prepared.statement.Kind != sqlast.KindSelect {
			// Query is the read path: it fans out and merges, so it must never
			// partially commit a mutation. Writes dispatch through Exec instead.
			return nil, &WriteNotSupportedError{Kind: prepared.statement.Kind}
		}
		bound, err := prepared.Bind(args)
		if err != nil {
			return nil, err
		}
		if useGlobalIndexRead(bound) {
			execution, indexErr := e.queryGlobalIndex(opctx, &q, bound, profile)
			if indexErr == nil {
				kind := execution.routeKind
				if execution.shardsFanned > 1 && kind != distribution.RouteScatter {
					kind = distribution.RouteTargeted
				}
				route := distribution.Route{
					Kind: kind, Distribution: bound.distribution,
					RoutingVersion: bound.manifest.Version(), Targets: execution.baseTargets,
				}
				physical, planning, planErr := optimizeGlobalIndexPlan(
					opctx, snap, bound, route, profile,
				)
				if planErr != nil {
					return nil, planErr
				}
				scatter := ScatterNone
				if kind == distribution.RouteScatter {
					scatter = ScatterAllShards
				}
				e.metrics.observeRoute(kind, execution.shardsFanned, scatter)
				res := execution.result
				res.RouteKind = kind
				res.Generation = snap.Generation()
				res.ShardsFanned = execution.shardsFanned
				res.Retries = attempt
				res.ScatterReason = scatter
				res.PlanFingerprint = physical.Fingerprint()
				res.Planning = planning
				return res, nil
			}
			if isStaleErr(indexErr) && attempt < e.maxRetry {
				staleGen = snap.Generation()
				e.metrics.observeRetry()
				continue
			}
			return nil, indexErr
		}
		pl, err := e.routeContext(opctx, snap, &q, bound, profile)
		if err != nil {
			return nil, err
		}
		e.metrics.observeRoute(pl.kind, len(pl.calls), pl.scatter)

		res, err := e.dispatch(opctx, pl, profile)
		if err == nil {
			res.RouteKind = pl.kind
			res.Generation = pl.generation
			res.ShardsFanned = len(pl.calls)
			res.Retries = attempt
			res.ScatterReason = pl.scatter
			res.PlanFingerprint = pl.physical.Fingerprint()
			res.Planning = pl.planning
			return res, nil
		}
		if isStaleErr(err) && attempt < e.maxRetry {
			staleGen = snap.Generation()
			e.metrics.observeRetry()
			continue
		}
		return nil, err
	}
}

// Exec is one bounded distributed write: a mutating statement that the pinned
// generation proves resident on exactly one shard. It reuses the read path's
// pin-prepare-bind-route-dispatch machinery, except the single dispatch target
// carries ExecutionReadWrite instead of the read-only fence, and a scatter, a
// cross-shard INSERT batch, or a replacement that moves a row's shard key is
// refused before any network I/O so a write never partially commits. An empty
// route is a successful local no-op: no shard is contacted. Atomic
// multi-statement and cross-shard writes dispatch through ExecBatch.
func (e *Executor) Exec(ctx context.Context, q Query) (*Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	profile := e.profileFor(q.Class)
	opctx, cancel := context.WithTimeout(ctx, profile.GlobalDeadline)
	defer cancel()
	args, err := queryRuntimeArgs(q.Params)
	if err != nil {
		return nil, err
	}

	var staleGen uint64
	for attempt := 0; ; attempt++ {
		if err := opctx.Err(); err != nil {
			return nil, err
		}
		snap, err := e.pin(opctx, attempt, staleGen)
		if err != nil {
			return nil, err
		}
		prepared, err := snap.Prepare(opctx, q.SQL)
		if err != nil {
			return nil, err
		}
		if prepared.statement.Kind == sqlast.KindSelect {
			// Exec is the write path: a SELECT has no affected rows to report and
			// must not be routed as a mutation.
			return nil, ErrExecRequiresMutation
		}
		bound, err := prepared.BindWrite(args)
		if err != nil {
			return nil, err
		}
		call, kind, scatter, err := e.routeWrite(snap, &q, bound, profile)
		if err != nil {
			return nil, err
		}

		var res *Result
		if call != nil && len(bound.globalIndexes) != 0 {
			participants, participantErr := appendBoundWriteParticipants(
				nil, *call, &q, bound, profile,
			)
			if participantErr != nil {
				return nil, participantErr
			}
			sortTransactionParticipants(participants)
			res, err = e.executeTransaction(opctx, snap, participants, profile)
		} else if call != nil {
			e.metrics.observeRoute(kind, 1, scatter)
			res, err = e.single(opctx, *call, profile)
		} else {
			e.metrics.observeRoute(kind, 0, scatter)
			res = &Result{Kind: shardservice.ResponseCompletion, RowsAffected: 0}
		}
		if err == nil {
			if len(bound.globalIndexes) == 0 {
				res.RouteKind = kind
			}
			res.Generation = snap.Generation()
			if call != nil && len(bound.globalIndexes) == 0 {
				res.ShardsFanned = 1
			}
			res.Retries = attempt
			res.ScatterReason = scatter
			return res, nil
		}
		if isStaleErr(err) && attempt < e.maxRetry {
			staleGen = snap.Generation()
			e.metrics.observeRetry()
			continue
		}
		return nil, err
	}
}

// routeWrite resolves one bound write plan against the pinned generation and
// returns its single dispatch target. An INSERT maps every VALUES row to its
// point target and is admitted only when all rows share one target; an UPDATE
// or DELETE routes its predicate targeted-only and is admitted only on a single
// or empty route, and a whole-document UPDATE is refused when its replacement
// would move the row to another shard. A nil call with a nil error is an empty
// route: a successful no-op that contacts no shard. Every refusal here happens
// before any network I/O.
func (e *Executor) routeWrite(snap *Snapshot, q *Query, bound *BoundWritePlan, p Profile) (*shardCall, distribution.RouteKind, ScatterReason, error) {
	if bound == nil || bound.generation != snap.Generation() || bound.manifest == nil {
		return nil, 0, ScatterNone, &CatalogError{Reason: "distributed write plan does not belong to the pinned catalog generation"}
	}
	mapper := distribution.NewNativeMapperWithBucketBits(bound.spec.Arity, bound.spec.EffectiveBucketBits())
	var (
		targets []distribution.Target
		route   distribution.Route
		err     error
	)
	if bound.kind == sqlast.KindInsert {
		targets, err = e.insertTargets(bound, mapper)
	} else {
		r, ok := e.targetedWriteRoute(bound, mapper)
		if !ok {
			return nil, 0, ScatterNone, &PlanError{
				Table: bound.table, Reason: "write predicate does not resolve to a single shard",
				cause: ErrWriteScatter,
			}
		}
		route = r
		targets = route.Targets
	}
	if err != nil {
		return nil, 0, ScatterNone, err
	}

	var kind distribution.RouteKind
	switch len(targets) {
	case 0:
		// An empty route is a successful local no-op: the statement matches no
		// rows on any shard, so nothing is dispatched and the affected count is
		// zero. A write never partially commits by contacting nothing.
		kind = distribution.RouteEmpty
		return nil, kind, ScatterNone, nil
	case 1:
		kind = distribution.RouteSingle
	default:
		// More than one target is a cross-shard write. An INSERT with rows that
		// route to different shards is the expected case; any other shape
		// reaching this point is a planner bug, so fail closed to the same
		// refusal rather than dispatching a partial batch.
		return nil, 0, ScatterNone, &PlanError{
			Table: bound.table, Reason: "write routes to more than one shard",
			cause: ErrWriteCrossShard,
		}
	}

	if bound.kind == sqlast.KindUpdate && len(bound.updateDoc) > 0 {
		if err := e.writeDocShardKeyMatchesTarget(bound, mapper, targets[0]); err != nil {
			return nil, kind, ScatterNone, err
		}
	}

	addr, err := snap.Address(targets[0].Endpoint)
	if err != nil {
		return nil, kind, ScatterNone, err
	}
	bucketBits, accessScopes := writeAccessScopes(bound, targets[0])
	call := &shardCall{
		target:  targets[0],
		address: addr,
		req: &shardservice.ShardRequest{
			SQL:                  q.SQL,
			Params:               q.Params,
			Distribution:         bound.distribution,
			Shard:                targets[0].Shard,
			AllocationGeneration: targets[0].AllocationGeneration,
			RoutingVersion:       bound.manifest.Version(),
			OwnershipEpoch:       targets[0].OwnershipEpoch,
			ReadPolicy:           p.ReadPolicy,
			ExecutionMode:        shardservice.ExecutionReadWrite,
			Deadline:             p.PerShardDeadline,
			MaxRows:              p.PerShardRows,
			MaxResultBytes:       p.PerShardBytes,
			BucketBits:           bucketBits,
			AccessScopes:         accessScopes,
		},
	}
	return call, kind, ScatterNone, nil
}

// insertTargets maps every VALUES row of a bound INSERT to its point target and
// returns the single target when all rows agree, else ErrWriteCrossShard. The
// mapper is a per-call scratch: it is not shared across goroutines, so it is
// built fresh here rather than borrowed from the executor's router pool.
func (e *Executor) insertTargets(bound *BoundWritePlan, mapper *distribution.NativeMapper) ([]distribution.Target, error) {
	if len(bound.rowKeys) == 0 {
		return nil, nil
	}
	target := distribution.Target{}
	set := false
	for i, key := range bound.rowKeys {
		point, err := mapper.PointFor(key)
		if err != nil {
			return nil, &PlanError{
				Table: bound.table, Reason: "insert row " + strconv.Itoa(i) + ": " + err.Error(),
				cause: ErrWriteCrossShard,
			}
		}
		t, ok := bound.manifest.ResolvePointTarget(point)
		if !ok {
			return nil, &PlanError{
				Table: bound.table, Reason: "insert row " + strconv.Itoa(i) + " maps outside the active manifest",
				cause: ErrWriteCrossShard,
			}
		}
		if !set {
			target = t
			set = true
			continue
		}
		if t.Shard != target.Shard || t.AllocationGeneration != target.AllocationGeneration ||
			t.Endpoint != target.Endpoint || t.OwnershipEpoch != target.OwnershipEpoch ||
			t.Role != target.Role {
			return nil, &PlanError{
				Table: bound.table, Reason: "insert row " + strconv.Itoa(i) + " routes to a different shard than row 0",
				cause: ErrWriteCrossShard,
			}
		}
	}
	return []distribution.Target{target}, nil
}

// targetedWriteRoute routes an UPDATE or DELETE predicate targeted-only against
// the pinned manifest and reports whether it resolved to a single or empty
// route. A scatter, unknown, or multi-shard route reports ok=false so the caller
// refuses it before any dispatch.
func (e *Executor) targetedWriteRoute(bound *BoundWritePlan, mapper *distribution.NativeMapper) (distribution.Route, bool) {
	r := e.routers.get()
	route, err := r.Route(bound.constraints, mapper, bound.manifest,
		distribution.NewRoutePolicy(distribution.AdmissionTargetedOnly, distribution.RouteLimits{}))
	e.routers.put(r)
	if err != nil {
		return distribution.Route{}, false
	}
	switch route.Kind {
	case distribution.RouteSingle, distribution.RouteEmpty:
		return route, true
	default:
		return distribution.Route{}, false
	}
}

// writeDocShardKeyMatchesTarget proves a whole-document UPDATE's replacement
// routes to the same target the predicate selected, so the replacement cannot
// move a row to another shard. It re-reads the replacement's shard key with the
// plan's compiled pointers and resolves it to its point target.
func (e *Executor) writeDocShardKeyMatchesTarget(bound *BoundWritePlan, mapper *distribution.NativeMapper, target distribution.Target) error {
	key, err := writeDocShardKey(bound.updateDoc, bound.keyPointers)
	if err != nil {
		return &PlanError{
			Table: bound.table, Reason: "update replacement document: " + err.Error(),
			cause: ErrWriteShardKeyMove,
		}
	}
	point, err := mapper.PointFor(key)
	if err != nil {
		return &PlanError{
			Table: bound.table, Reason: "update replacement document: " + err.Error(),
			cause: ErrWriteShardKeyMove,
		}
	}
	routed, ok := bound.manifest.ResolvePointTarget(point)
	if !ok {
		return &PlanError{
			Table: bound.table, Reason: "update replacement document maps outside the active manifest",
			cause: ErrWriteShardKeyMove,
		}
	}
	if routed.Shard != target.Shard || routed.AllocationGeneration != target.AllocationGeneration ||
		routed.Endpoint != target.Endpoint || routed.OwnershipEpoch != target.OwnershipEpoch ||
		routed.Role != target.Role {
		return &PlanError{
			Table: bound.table, Reason: "update replacement document routes to a different shard",
			cause: ErrWriteShardKeyMove,
		}
	}
	return nil
}

// Explain plans q against the currently pinned generation without opening a
// shard connection. It applies the same parameter binding, route admission,
// statistics, rules, objective, and memory limits as Query.
func (e *Executor) Explain(ctx context.Context, q Query) (*Explanation, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	args, err := queryRuntimeArgs(q.Params)
	if err != nil {
		return nil, err
	}
	snap, err := e.pin(ctx, 0, 0)
	if err != nil {
		return nil, err
	}
	prepared, err := snap.Prepare(ctx, q.SQL)
	if err != nil {
		return nil, err
	}
	if prepared.statement.Kind != sqlast.KindSelect {
		// Explain is a read-path diagnostic: it plans a distributed SELECT and
		// never dispatches a mutation, so it refuses non-SELECT statements.
		return nil, &WriteNotSupportedError{Kind: prepared.statement.Kind}
	}
	bound, err := prepared.Bind(args)
	if err != nil {
		return nil, err
	}
	profile := e.profileFor(q.Class)
	if useGlobalIndexRead(bound) {
		if bound.alwaysReason != "" || (bound.globalEmpty && bound.emptyReason != "") {
			reason := bound.alwaysReason
			if reason == "" {
				reason = bound.emptyReason
			}
			return nil, &PlanError{
				Table: bound.table, Reason: reason,
				cause: ErrDistributedPlanUnsupported,
			}
		}
		route, shards := globalIndexExplainRoute(bound)
		baseKind, admissionErr := admitGlobalIndexTargets(
			len(route.Targets), bound.manifest.ShardCount(), profile,
		)
		if admissionErr != nil {
			return nil, admissionErr
		}
		route.Kind = baseKind
		physical, planning, planErr := optimizeGlobalIndexPlan(
			ctx, snap, bound, route, profile,
		)
		if planErr != nil {
			return nil, planErr
		}
		kind := baseKind
		if shards > 1 && baseKind != distribution.RouteScatter {
			kind = distribution.RouteTargeted
		}
		scatter := ScatterNone
		if kind == distribution.RouteScatter {
			scatter = ScatterAllShards
		}
		return &Explanation{
			PhysicalPlan: physical.String(), PlanFingerprint: physical.Fingerprint(),
			Cost: physical.Cost, Planning: planning,
			RouteKind: kind, Generation: snap.Generation(),
			Shards: shards, ScatterReason: scatter,
		}, nil
	}
	physical, err := e.routeContext(ctx, snap, &q, bound, profile)
	if err != nil {
		return nil, err
	}
	return &Explanation{
		PhysicalPlan: physical.physical.String(), PlanFingerprint: physical.physical.Fingerprint(),
		Cost: physical.physical.Cost, Planning: physical.planning,
		RouteKind: physical.kind, Generation: physical.generation,
		Shards: len(physical.calls), ScatterReason: physical.scatter,
	}, nil
}

func queryRuntimeArgs(params []shardservice.Param) ([]any, error) {
	if len(params) == 0 {
		return nil, nil
	}
	args := make([]any, len(params))
	for i := range params {
		if !params[i].Valid() {
			return nil, fmt.Errorf("%w: parameter %d has invalid kind %d",
				ErrPlanParameters, i+1, params[i].Kind)
		}
		args[i] = params[i].RuntimeValue()
	}
	return args, nil
}

// profileFor returns the operational profile for class, falling back to the
// interactive profile for an unknown class.
func (e *Executor) profileFor(class OperationClass) Profile {
	if p, ok := e.profiles[class]; ok {
		return p
	}
	return e.profiles[ClassInteractive]
}

// pin returns the generation the attempt routes against: the current generation
// on the first attempt, or a strictly newer generation obtained by refresh on a
// retry. A refresh that cannot produce a newer generation fails closed.
func (e *Executor) pin(ctx context.Context, attempt int, staleGen uint64) (*Snapshot, error) {
	if attempt == 0 {
		snap := e.catalog.Current()
		if snap == nil {
			return nil, ErrNoCatalog
		}
		return snap, nil
	}
	if e.refresh != nil {
		snap, err := e.refresh(ctx, staleGen)
		if err != nil {
			return nil, err
		}
		if snap == nil || snap.Generation() <= staleGen {
			return nil, ErrStaleGeneration
		}
		return snap, nil
	}
	snap := e.catalog.Current()
	if snap == nil || snap.Generation() <= staleGen {
		return nil, ErrStaleGeneration
	}
	return snap, nil
}

// isStaleErr reports whether err is a shard's refusal of the pinned generation:
// a stale physical allocation, routing version, ownership epoch, or not-owner
// refusal after ownership moved. Each is retryable only against a newer
// generation.
func isStaleErr(err error) bool {
	return errors.Is(err, distribution.ErrRoutingVersion) ||
		errors.Is(err, distribution.ErrShardAllocation) ||
		errors.Is(err, distribution.ErrOwnershipEpoch) ||
		errors.Is(err, distribution.ErrNotShardOwner)
}

// dispatch executes the routed plan: an empty route returns no rows without
// contacting a shard, a single-shard route streams through unchanged, and a
// multi-shard route fans out and merges.
func (e *Executor) dispatch(ctx context.Context, pl *plan, p Profile) (*Result, error) {
	switch len(pl.calls) {
	case 0:
		if len(pl.aggregates) != 0 {
			return emptyAggregateResult(pl), nil
		}
		return &Result{Kind: shardservice.ResponseRows}, nil
	case 1:
		return e.single(ctx, pl.calls[0], p)
	default:
		return e.snapshotFanout(ctx, pl, p)
	}
}

// single performs a single-shard route: one round-trip, streamed through
// unchanged. The shard already applied the statement's own ORDER BY and LIMIT.
func (e *Executor) single(ctx context.Context, call shardCall, p Profile) (*Result, error) {
	opctx, cancel := context.WithTimeout(ctx, p.PerShardDeadline)
	defer cancel()
	resp, err := e.client.Do(opctx, call.address, call.req)
	if err != nil {
		return nil, err
	}
	rows := uint64(len(resp.Rows))
	bytes := responseBytes(resp)
	if err := checkAggregate(p, rows, bytes); err != nil {
		return nil, err
	}
	e.metrics.observeResult(rows, bytes)
	return &Result{Kind: resp.Kind, Columns: resp.Columns, Rows: resp.Rows, RowsAffected: resp.RowsAffected}, nil
}

// fanout dispatches every shard call concurrently through a bounded worker pool,
// enforces the aggregate caps, cancels outstanding shards on a hard failure or
// once a cap is hit, and merges the results. The partial-result policy is
// fail-closed: any shard failure fails the whole operation.
func (e *Executor) fanout(ctx context.Context, pl *plan, p Profile) (*Result, error) {
	opctx, cancel := context.WithTimeout(ctx, p.GlobalDeadline)
	defer cancel()

	results := make([]*shardservice.ShardResponse, len(pl.calls))

	var (
		mu         sync.Mutex
		firstErr   error
		totalRows  uint64
		totalBytes uint64
	)
	fail := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
		cancel()
	}

	jobs := make(chan int)
	workers := min(max(1, p.MaxConcurrency), len(pl.calls))
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for i := range jobs {
				select {
				case <-opctx.Done():
					return
				default:
				}

				sctx, sc := context.WithTimeout(opctx, p.PerShardDeadline)
				resp, err := e.client.Do(sctx, pl.calls[i].address, pl.calls[i].req)
				sc()
				if err != nil {
					fail(err)
					return
				}
				results[i] = resp

				rows := uint64(len(resp.Rows))
				b := responseBytes(resp)
				mu.Lock()
				totalRows += rows
				totalBytes += b
				over := checkAggregate(p, totalRows, totalBytes)
				mu.Unlock()
				if over != nil {
					fail(over)
					return
				}
			}
		})
	}
	go func() {
		defer close(jobs)
		for i := range pl.calls {
			select {
			case jobs <- i:
			case <-opctx.Done():
				return
			}
		}
	}()
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	var columns []shardservice.Column
	var rows [][]shardservice.Cell
	var err error
	if len(pl.aggregates) != 0 {
		columns, rows, err = mergeAggregateRows(results, pl.aggregates, p.MaxAggregateBytes)
	} else {
		columns, rows, err = mergeRows(results, pl.order, pl.limit)
	}
	if err != nil {
		return nil, err
	}
	e.metrics.observeResult(totalRows, totalBytes)
	return &Result{Kind: shardservice.ResponseRows, Columns: columns, Rows: rows}, nil
}

func emptyAggregateResult(pl *plan) *Result {
	const oidJSON int32 = 114
	columns := make([]shardservice.Column, len(pl.aggregates))
	row := make([]shardservice.Cell, len(pl.aggregates))
	for i := range pl.aggregates {
		columns[i] = shardservice.Column{Name: pl.aggHeaders[i], TypeOID: oidJSON}
		if pl.aggregates[i] == sqlast.AggCount {
			row[i].Bytes = []byte("0")
		} else {
			row[i].Null = true
		}
	}
	return &Result{Kind: shardservice.ResponseRows, Columns: columns, Rows: [][]shardservice.Cell{row}}
}

// checkAggregate reports ErrResultLimit when the running row or byte total
// exceeds the profile's aggregate cap.
func checkAggregate(p Profile, rows, bytes uint64) error {
	if p.MaxAggregateRows > 0 && rows > p.MaxAggregateRows {
		return fmt.Errorf("%w: buffered %d rows exceeds the %d aggregate cap", ErrResultLimit, rows, p.MaxAggregateRows)
	}
	if p.MaxAggregateBytes > 0 && bytes > p.MaxAggregateBytes {
		return fmt.Errorf("%w: buffered %d bytes exceeds the %d aggregate cap", ErrResultLimit, bytes, p.MaxAggregateBytes)
	}
	return nil
}

// responseBytes sums the encoded cell bytes of a row response.
func responseBytes(resp *shardservice.ShardResponse) uint64 {
	var n uint64
	for _, row := range resp.Rows {
		for _, c := range row {
			n += uint64(len(c.Bytes))
		}
	}
	return n
}
