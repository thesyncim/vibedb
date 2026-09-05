package gateway

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	queryplanner "github.com/thesyncim/vibedb/planner"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
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
// generation exceeds staleGen, or an error. The executor validates and
// publishes the result before leasing it, so refresh cannot bypass catalog
// lineage or generation-drain fences. A nil RefreshFunc re-reads the executor's
// catalog holder.
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
	// InternalAuthority is the gateway service principal used only by the
	// autonomous transaction-recovery loop. It must be explicitly configured
	// by authenticated production startup; the zero value cannot forward.
	InternalAuthority serviceauthz.Authority
	// Pressure receives bounded, catalog-fenced routing samples for autonomous
	// hot-shard scheduling. It is advisory and never grants serving authority.
	Pressure PressureObserver
}

// defaultMaxRetries bounds stale-generation retries when Options leaves it zero.
const defaultMaxRetries = 2

// Executor routes and dispatches bounded distributed reads over a pinned catalog
// generation. It is safe for concurrent use.
type Executor struct {
	client            ShardTransport
	catalog           *CatalogHolder
	profiles          map[OperationClass]Profile
	refresh           RefreshFunc
	maxRetry          int
	routers           *routerPool
	metrics           Metrics
	internalAuthority serviceauthz.Authority
	pressure          PressureObserver
}

// NewExecutor returns an executor that dispatches through client and pins
// generations from catalog. Both are required.
func NewExecutor(client ShardTransport, catalog *CatalogHolder, opts Options) *Executor {
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
		client:            client,
		catalog:           catalog,
		profiles:          profiles,
		refresh:           opts.Refresh,
		maxRetry:          maxRetry,
		routers:           newRouterPool(),
		internalAuthority: opts.InternalAuthority,
		pressure:          opts.Pressure,
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
	// ParamTypes is absent for schemaless SQL. A present vector carries the
	// gateway's analyzed SQL input domains through the shard protocol so an
	// independently prepared shard plan cannot resolve the same placeholders to a
	// different common type.
	ParamTypes []sqldriver.ParamType `json:",omitempty"`

	Class OperationClass
}

// Result is a distributed read's merged outcome plus the routing metadata a
// caller reads for observability. Kind is ResponseRows for the read path;
// RowsAffected is meaningful only for a single-shard completion passthrough.
type Result struct {
	// Observations records independent RF3 group cuts, not a global MVCC timestamp.
	Observations []ReplicatedGroupReadObservation
	Kind         shardservice.ResponseKind
	Columns      []shardservice.Column
	Rows         [][]shardservice.Cell
	RowsAffected int64
	// TransactionID is nonzero for an RF3 multi-group write. It lets the shipped
	// boundary report a typed durable identity rather than reducing recovery
	// state to an error string.
	TransactionID replication.ID128

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

// preparedQueryExecution is the session-local physical planning cache behind
// the pgwire prepared-statement path. PreparedPlan and planner.Plan are
// immutable; the remaining fields certify the catalog generation and route
// shape for which the physical tree was selected.
type preparedQueryExecution struct {
	generation uint64
	prepared   *PreparedPlan
	routeKind  distribution.RouteKind
	targets    int
	physical   *queryplanner.Plan
	planning   queryplanner.OptimizerStatistics
}

// Query routes and dispatches q, retrying against a refreshed generation when a
// shard reports the pinned generation is stale. It pins one generation per
// attempt and never mixes generations within an attempt.
func (e *Executor) Query(ctx context.Context, q Query) (*Result, error) {
	return e.queryWithProfile(ctx, q, e.profileFor(q.Class))
}

func (e *Executor) queryWithProfile(ctx context.Context, q Query, profile Profile) (*Result, error) {
	return e.queryWithProfileValidation(ctx, q, profile, -1)
}

// queryPreparedWithProfile is the pgwire read path. postgresStatement already
// performed the full typed semantic prepare and retains that compiled statement,
// so execution only needs to validate the bound transport values and arity.
// Keeping this entry point private prevents general Executor callers from
// bypassing validateTypedQuery.
func (e *Executor) queryPreparedWithProfile(
	ctx context.Context,
	q Query,
	profile Profile,
	preparedParams int,
	cache *preparedQueryExecution,
) (*Result, error) {
	return e.queryWithProfileValidation(ctx, q, profile, preparedParams, cache)
}

func (e *Executor) queryWithProfileValidation(
	ctx context.Context,
	q Query,
	profile Profile,
	preparedParams int,
	preparedCache ...*preparedQueryExecution,
) (*Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if preparedParams >= 0 {
		if err := validatePreparedQueryParameters(&q, preparedParams); err != nil {
			return nil, err
		}
	} else {
		if err := validateTypedQuery(ctx, &q); err != nil {
			return nil, err
		}
	}
	opctx, cancel := context.WithTimeout(ctx, profile.GlobalDeadline)
	defer cancel()
	args, err := queryRuntimeArgs(q.Params)
	if err != nil {
		return nil, err
	}
	var leases catalogLeaseSet
	defer leases.release()

	var staleGen uint64
	refreshedMiss := false
	for attempt := 0; ; {
		if err := opctx.Err(); err != nil {
			return nil, err
		}
		snap, lease, err := e.pin(opctx, attempt, staleGen)
		if err != nil {
			return nil, err
		}
		leases.add(lease)
		attemptContext := opctx
		var nativeAttempt *replicatedSQLAttempt
		if _, native := e.client.(*ReplicatedSQLTransport); native {
			nativeAttempt = &replicatedSQLAttempt{snapshot: snap}
			attemptContext = context.WithValue(opctx, replicatedSQLSnapshotKey{}, nativeAttempt)
		}
		var cache *preparedQueryExecution
		if len(preparedCache) != 0 {
			cache = preparedCache[0]
		}
		prepared := (*PreparedPlan)(nil)
		if cache != nil && cache.generation == snap.Generation() {
			prepared = cache.prepared
		}
		if prepared == nil {
			prepared, err = snap.Prepare(opctx, q.SQL)
			if err != nil {
				if errors.Is(err, ErrTableNotPlaced) && !refreshedMiss {
					refreshedMiss = true
					leases.releaseLast()
					if refreshErr := e.refreshAfterCatalogMiss(opctx, snap.Generation()); refreshErr != nil {
						return nil, preserveCatalogMiss(err, refreshErr)
					}
					staleGen = 0
					continue
				}
				return nil, err
			}
			if cache != nil {
				*cache = preparedQueryExecution{
					generation: snap.Generation(), prepared: prepared,
				}
			}
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
			if nativeAttempt != nil {
				return nil, ErrReplicatedSQLPlanUnsupported
			}
			execution, indexErr := e.queryGlobalIndex(attemptContext, &q, bound, profile)
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
					opctx, snap, bound, route, profile, execution.candidateRows,
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
				attempt++
				continue
			}
			return nil, indexErr
		}
		pl, err := e.routeContextCached(opctx, snap, &q, bound, profile, cache)
		if err != nil {
			return nil, err
		}
		e.observePressureCalls(pl.calls)
		e.metrics.observeRoute(pl.kind, len(pl.calls), pl.scatter)

		res, err := e.dispatch(attemptContext, pl, profile)
		if err == nil {
			if nativeAttempt != nil {
				res.Observations = nativeAttempt.resultObservations()
			}
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
			attempt++
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
	if err := validateTypedQuery(ctx, &q); err != nil {
		return nil, err
	}
	profile := e.profileFor(q.Class)
	opctx, cancel := context.WithTimeout(ctx, profile.GlobalDeadline)
	defer cancel()
	args, err := queryRuntimeArgs(q.Params)
	if err != nil {
		return nil, err
	}
	var leases catalogLeaseSet
	defer leases.release()

	var staleGen uint64
	refreshedMiss := false
	for attempt := 0; ; {
		if err := opctx.Err(); err != nil {
			return nil, err
		}
		snap, lease, err := e.pin(opctx, attempt, staleGen)
		if err != nil {
			return nil, err
		}
		leases.add(lease)
		prepared, err := snap.Prepare(opctx, q.SQL)
		if err != nil {
			if errors.Is(err, ErrTableNotPlaced) && !refreshedMiss {
				refreshedMiss = true
				leases.releaseLast()
				if refreshErr := e.refreshAfterCatalogMiss(opctx, snap.Generation()); refreshErr != nil {
					return nil, preserveCatalogMiss(err, refreshErr)
				}
				staleGen = 0
				continue
			}
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
		if err := rejectReplicatedGlobalIndexSQLTargets(snap, bound); err != nil {
			return nil, err
		}
		if call != nil && len(prepared.writeGlobalIndexes) != 0 &&
			(bound.kind == sqlast.KindUpdate || bound.kind == sqlast.KindDelete) {
			err = e.captureIndexedMutation(opctx, prepared, bound, *call, profile)
			if err != nil {
				if isStaleErr(err) && attempt < e.maxRetry {
					staleGen = snap.Generation()
					e.metrics.observeRetry()
					attempt++
					continue
				}
				return nil, err
			}
			if err := rejectReplicatedGlobalIndexSQLTargets(snap, bound); err != nil {
				return nil, err
			}
		}

		var res *Result
		if call != nil {
			e.observePressureCall(*call)
		}
		if call != nil && bound.requiresIndexTransaction() {
			targets, targetErr := appendBoundWriteTargets(
				nil, *call, &q, bound, profile,
			)
			if targetErr != nil {
				return nil, targetErr
			}
			sortTransactionTargets(targets)
			res, err = e.executeTransaction(opctx, snap, targets, profile)
		} else if call != nil {
			e.metrics.observeRoute(kind, 1, scatter)
			res, err = e.single(opctx, *call, profile)
		} else {
			e.metrics.observeRoute(kind, 0, scatter)
			res = &Result{Kind: shardservice.ResponseCompletion, RowsAffected: 0}
		}
		if err == nil {
			if !bound.requiresIndexTransaction() {
				res.RouteKind = kind
			}
			res.Generation = snap.Generation()
			if call != nil && !bound.requiresIndexTransaction() {
				res.ShardsFanned = 1
			}
			res.Retries = attempt
			res.ScatterReason = scatter
			return res, nil
		}
		if isStaleErr(err) && attempt < e.maxRetry {
			staleGen = snap.Generation()
			e.metrics.observeRetry()
			attempt++
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
	if _, replicated := snap.replicatedShardAt(bound.distribution, targets[0].Shard); replicated {
		return nil, kind, ScatterNone, ErrReplicatedSQLWriteUnavailable
	}

	addr, err := snap.Address(targets[0].Endpoint)
	if err != nil {
		return nil, kind, ScatterNone, err
	}
	bucketBits, accessScopes := writeAccessScopes(bound, targets[0])
	call := &shardCall{
		target: targets[0], pressureSource: pressureSourceForTarget(
			bound.manifest, bound.spec.EffectiveBucketBits(), targets[0],
		),
		address: addr,
		req: &shardservice.ShardRequest{
			SQL:                  q.SQL,
			Params:               q.Params,
			ParamTypes:           q.ParamTypes,
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
	if err := validateTypedQuery(ctx, &q); err != nil {
		return nil, err
	}
	args, err := queryRuntimeArgs(q.Params)
	if err != nil {
		return nil, err
	}
	var snap *Snapshot
	var lease catalogLease
	var prepared *PreparedPlan
	refreshedMiss := false
	for {
		snap, lease, err = e.pin(ctx, 0, 0)
		if err != nil {
			return nil, err
		}
		prepared, err = snap.Prepare(ctx, q.SQL)
		if err == nil {
			break
		}
		if !errors.Is(err, ErrTableNotPlaced) || refreshedMiss {
			lease.release()
			return nil, err
		}
		refreshedMiss = true
		staleGeneration := lease.generation
		lease.release()
		refreshCtx, cancel := context.WithTimeout(ctx, e.profileFor(q.Class).GlobalDeadline)
		refreshErr := e.refreshAfterCatalogMiss(refreshCtx, staleGeneration)
		cancel()
		if refreshErr != nil {
			return nil, preserveCatalogMiss(err, refreshErr)
		}
	}
	defer lease.release()
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
		route, shards, indexKeys, routeErr := globalIndexExplainRoute(bound, profile)
		if routeErr != nil {
			return nil, routeErr
		}
		baseKind, admissionErr := admitGlobalIndexTargets(
			len(route.Targets), bound.manifest.ShardCount(), profile,
		)
		if admissionErr != nil {
			return nil, admissionErr
		}
		route.Kind = baseKind
		candidateRows := -1
		if bound.globalEmpty {
			candidateRows = 0
		} else if bound.globalIndex != nil &&
			bound.globalIndex.program.metadata.Flags&IndexUnique != 0 {
			candidateRows = indexKeys
		}
		physical, planning, planErr := optimizeGlobalIndexPlan(
			ctx, snap, bound, route, profile, candidateRows,
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
func (e *Executor) pin(ctx context.Context, attempt int, staleGen uint64) (*Snapshot, catalogLease, error) {
	if e == nil || e.catalog == nil {
		return nil, catalogLease{}, ErrNoCatalog
	}
	if e.refresh != nil && attempt > 0 && staleGen > 0 {
		if err := e.catalog.refreshAfter(ctx, staleGen, e.refresh); err != nil {
			return nil, catalogLease{}, err
		}
	}
	lease := e.catalog.pinCurrent()
	if lease.snapshot == nil {
		return nil, catalogLease{}, ErrNoCatalog
	}
	if attempt > 0 && lease.generation <= staleGen {
		lease.release()
		return nil, catalogLease{}, ErrStaleGeneration
	}
	return lease.snapshot, lease, nil
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
		if _, native := e.client.(*ReplicatedSQLTransport); native {
			// Each native request owns a leader ReadIndex cut. Legacy transaction
			// read fences are neither supported nor a global RF3 snapshot.
			if pl.repartition {
				return nil, ErrReplicatedSQLPlanUnsupported
			}
			return e.fanout(ctx, pl, p)
		}
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

func (e *Executor) captureIndexedMutation(
	ctx context.Context,
	prepared *PreparedPlan,
	bound *BoundWritePlan,
	baseCall shardCall,
	p Profile,
) error {
	req := *baseCall.req
	req.ExecutionMode = shardservice.ExecutionReadOnly
	req.MutationImageCapture = true
	captureCtx, cancel := context.WithTimeout(ctx, p.PerShardDeadline)
	defer cancel()
	resp, err := e.client.Do(captureCtx, baseCall.address, &req)
	if err != nil {
		return err
	}
	if resp.Kind != shardservice.ResponseRows ||
		!validMutationImageCaptureColumns(resp.Columns) {
		return &PlanError{
			Table: bound.table, Reason: "base shard returned an invalid mutation capture response",
			cause: ErrGlobalIndexMaintenanceUnsupported,
		}
	}
	rows := uint64(len(resp.Rows))
	resultBytes := responseBytes(resp)
	if err := checkAggregate(p, rows, resultBytes); err != nil {
		return err
	}
	for i := range resp.Rows {
		if len(resp.Rows[i]) != 3 {
			return &PlanError{
				Table: bound.table, Reason: "base shard returned a malformed mutation capture row",
				cause: ErrGlobalIndexMaintenanceUnsupported,
			}
		}
	}
	return prepared.bindGlobalIndexCapture(bound, baseCall.target, resp.Rows)
}

func validMutationImageCaptureColumns(columns []shardservice.Column) bool {
	const oidJSON int32 = 114
	return len(columns) == 3 &&
		columns[0] == (shardservice.Column{Name: "primary_key", TypeOID: oidJSON}) &&
		columns[1] == (shardservice.Column{Name: "before_document", TypeOID: oidJSON}) &&
		columns[2] == (shardservice.Column{Name: "after_document", TypeOID: oidJSON})
}

// fanout dispatches every shard call concurrently through a bounded worker pool,
// enforces the aggregate caps, cancels outstanding shards on a hard failure or
// once a cap is hit, and merges the results. The partial-result policy is
// fail-closed: any shard failure fails the whole operation.
func (e *Executor) fanout(ctx context.Context, pl *plan, p Profile) (*Result, error) {
	if len(pl.aggregates) != 0 && len(pl.groupKeys) != 0 {
		return e.fanoutGroupedBatches(ctx, pl, p)
	}
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
		if len(pl.groupKeys) != 0 {
			columns, rows, err = mergeGroupedAggregateRows(
				results, pl.aggregates, pl.groupKeys, p.MaxAggregateBytes,
			)
			if err == nil {
				rows, err = finalizeGroupedRowsWindow(
					rows, pl.order, pl.offset, pl.limit, pl.hasLimit, p.MaxAggregateBytes,
				)
			}
		} else {
			columns, rows, err = mergeAggregateRows(results, pl.aggregates, p.MaxAggregateBytes)
		}
	} else {
		columns, rows, err = mergeRowsWindow(results, pl.order, 0, pl.limit, pl.hasLimit)
	}
	if err != nil {
		return nil, err
	}
	e.metrics.observeResult(totalRows, totalBytes)
	return &Result{Kind: shardservice.ResponseRows, Columns: columns, Rows: rows}, nil
}

const (
	distributedBatchRows  uint32 = 4 << 10
	distributedBatchBytes uint32 = 256 << 10
)

type groupedShardStream struct {
	batches chan *shardservice.ShardResponse
	done    chan error
}

// fanoutGroupedBatches incrementally combines grouped shard fragments in
// canonical route order. One unbuffered channel per active request lets a shard
// hold at most its current decoded frame while the coordinator is busy; the
// synchronous client callback propagates that pressure to the shard cursor.
func (e *Executor) fanoutGroupedBatches(
	ctx context.Context,
	pl *plan,
	p Profile,
) (*Result, error) {
	opctx, cancel := context.WithTimeout(ctx, p.GlobalDeadline)
	defer cancel()

	var merger *groupedAggregateMerger
	var err error
	var disjointRows [][]shardservice.Cell
	var disjointBytes uint64
	if !pl.groupLocal {
		merger, err = newGroupedAggregateMerger(pl.aggregates, pl.groupKeys, p.MaxAggregateBytes)
		if err != nil {
			return nil, err
		}
	}
	streams := make([]groupedShardStream, len(pl.calls))
	for i := range streams {
		streams[i] = groupedShardStream{
			batches: make(chan *shardservice.ShardResponse),
			done:    make(chan error, 1),
		}
	}

	var (
		errMu    sync.Mutex
		firstErr error
	)
	fail := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
		cancel()
	}
	readError := func() error {
		errMu.Lock()
		defer errMu.Unlock()
		return firstErr
	}

	jobs := make(chan int)
	workers := min(max(1, p.MaxConcurrency), len(pl.calls))
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for i := range jobs {
				req := *pl.calls[i].req
				req.RowBatch = distributedRowBatch(&req)
				sctx, stop := context.WithTimeout(opctx, p.PerShardDeadline)
				err := e.client.DoBatches(
					sctx, pl.calls[i].address, &req,
					func(batch *shardservice.ShardResponse) error {
						select {
						case streams[i].batches <- batch:
							return nil
						case <-sctx.Done():
							return sctx.Err()
						}
					},
				)
				stop()
				if err != nil {
					fail(err)
				}
				streams[i].done <- err
				close(streams[i].batches)
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

	var (
		columns    []shardservice.Column
		totalRows  uint64
		totalBytes uint64
	)
	for shard := range streams {
		shardRows := 0
		for {
			select {
			case batch, ok := <-streams[shard].batches:
				if !ok {
					if streamErr := <-streams[shard].done; streamErr != nil {
						fail(streamErr)
					}
					goto nextShard
				}
				if batch.RowBatch.Sequence == 0 {
					if columns == nil {
						columns = append([]shardservice.Column(nil), batch.Columns...)
					} else if !sameColumns(columns, batch.Columns) {
						fail(ErrMergeSchema)
						goto finished
					}
				}
				batchRows := uint64(len(batch.Rows))
				batchBytes := responseBytes(batch)
				if ^uint64(0)-totalRows < batchRows || ^uint64(0)-totalBytes < batchBytes {
					fail(ErrResultLimit)
					goto finished
				}
				totalRows += batchRows
				totalBytes += batchBytes
				if limitErr := checkAggregate(p, totalRows, totalBytes); limitErr != nil {
					fail(limitErr)
					goto finished
				}
				for row := range batch.Rows {
					if pl.groupLocal {
						if len(batch.Rows[row]) != len(pl.aggregates) {
							fail(ErrMergeSchema)
							goto finished
						}
						charge := uint64(48 + len(batch.Rows[row])*32)
						for _, cell := range batch.Rows[row] {
							charge += uint64(len(cell.Bytes))
						}
						if disjointBytes > p.MaxAggregateBytes || charge > p.MaxAggregateBytes-disjointBytes {
							fail(ErrResultLimit)
							goto finished
						}
						disjointBytes += charge
						continue
					}
					if addErr := merger.add(batch.Rows[row]); addErr != nil {
						fail(fmt.Errorf("%w: shard %d row %d: %v",
							ErrMergeAggregate, shard, shardRows+row, addErr))
						goto finished
					}
				}
				if pl.groupLocal {
					disjointRows = appendGroupedBatch(disjointRows, batch.Rows)
				}
				shardRows += len(batch.Rows)
			case <-opctx.Done():
				goto finished
			}
		}
	nextShard:
	}

finished:
	operationErr := opctx.Err()
	cancel()
	wg.Wait()
	if err := readError(); err != nil {
		return nil, err
	}
	if operationErr != nil {
		return nil, operationErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(columns) != len(pl.aggregates) {
		return nil, ErrMergeSchema
	}
	rows := disjointRows
	if merger != nil {
		rows, err = merger.finish()
		if err != nil {
			return nil, err
		}
	}
	rows, err = finalizeGroupedRowsWindow(
		rows, pl.order, pl.offset, pl.limit, pl.hasLimit, p.MaxAggregateBytes,
	)
	if err != nil {
		return nil, err
	}
	e.metrics.observeResult(totalRows, totalBytes)
	return &Result{Kind: shardservice.ResponseRows, Columns: columns, Rows: rows}, nil
}

func distributedRowBatch(req *shardservice.ShardRequest) shardservice.RowBatchRequest {
	rows := min(uint64(distributedBatchRows), req.MaxRows)
	bytes := min(uint64(distributedBatchBytes), req.MaxResultBytes)
	return shardservice.RowBatchRequest{BatchRows: uint32(rows), BatchBytes: uint32(bytes)}
}

func sameColumns(a, b []shardservice.Column) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func emptyAggregateResult(pl *plan) *Result {
	const oidJSON int32 = 114
	columns := make([]shardservice.Column, len(pl.aggregates))
	for i := range pl.aggregates {
		columns[i] = shardservice.Column{Name: pl.aggHeaders[i], TypeOID: oidJSON}
	}
	if len(pl.groupKeys) != 0 {
		return &Result{Kind: shardservice.ResponseRows, Columns: columns}
	}
	row := make([]shardservice.Cell, len(pl.aggregates))
	for i := range pl.aggregates {
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

// Shard-local groups need ownership, not a second hash table. Batch arenas
// preserve contiguous cells and payload while avoiding two allocations per row.
func appendGroupedBatch(dst [][]shardservice.Cell, rows [][]shardservice.Cell) [][]shardservice.Cell {
	cells, bytes := 0, 0
	for _, row := range rows {
		cells += len(row)
		for _, cell := range row {
			bytes += len(cell.Bytes)
		}
	}
	cellArena := make([]shardservice.Cell, cells)
	payload := make([]byte, 0, bytes)
	for _, row := range rows {
		out := cellArena[:len(row):len(row)]
		cellArena = cellArena[len(row):]
		for i, cell := range row {
			start := len(payload)
			payload = append(payload, cell.Bytes...)
			out[i] = shardservice.Cell{Null: cell.Null, Bytes: payload[start:len(payload):len(payload)]}
		}
		dst = append(dst, out)
	}
	return dst
}
