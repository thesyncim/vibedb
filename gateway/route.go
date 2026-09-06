package gateway

import (
	"context"
	"sync"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
	queryplanner "github.com/thesyncim/vibedb/planner"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibejson/x/byteview"
)

// The route glue: it turns one pinned snapshot generation plus a bound key into
// the concrete per-shard requests a fan-out dispatches. Routing reuses a
// distribution.Router, which is not safe for concurrent use, so each routing
// call borrows its own Router from a pool and returns it — no Router is ever
// shared between goroutines.

// shardCall is one resolved dispatch target: the routed target, the network
// address its endpoint resolves to in the pinned generation, and the fully
// populated request that admits against that shard's ownership coordinates.
type shardCall struct {
	target         distribution.Target
	pressureSource autosplit.SourceIdentity
	address        string
	req            *shardservice.ShardRequest
}

// plan is the routed result for one pinned generation: the physical route kind,
// the calls to dispatch, and the scatter classification for admission and
// metrics. Every call carries the same routing version and distribution, so the
// whole plan is pinned to one generation and never mixes generations.
type plan struct {
	kind         distribution.RouteKind
	distribution distribution.DistributionName
	version      distribution.RoutingVersion
	generation   uint64
	scatter      ScatterReason
	calls        []shardCall
	order        []OrderKey
	limit        int
	offset       int
	hasLimit     bool
	aggregates   []sqlast.AggKind
	groupKeys    []int
	aggHeaders   []string
	repartition  bool
	groupLocal   bool
	physical     *queryplanner.Plan
	planning     queryplanner.OptimizerStatistics
}

// routerPool hands each routing call its own distribution.Router. The Router
// reuses internal scratch buffers and is not concurrent-safe; a pool keeps one
// per active goroutine and reuses it across operations.
type routerPool struct {
	pool sync.Pool
}

func newRouterPool() *routerPool {
	return &routerPool{pool: sync.Pool{New: func() any { return distribution.NewRouter() }}}
}

// shardRequestPool recycles the ~1KB request descriptor shells built once
// per routed target. Only the shell is reused: SQL text, parameters, and
// scopes are per-query values assigned at construction, and release scrubs
// the shell so pooled memory retains no caller data. Dispatch uses each
// request strictly synchronously, and the bound query path releases every
// routed shell after its dispatch joins; requests built for any other flow
// are simply collected. A missed release only loses the saving.
var shardRequestPool = sync.Pool{New: func() any { return &shardservice.ShardRequest{} }}

// releaseShardCalls recycles one routed plan's request shells after their
// dispatch has joined. Calls must be the slice value owned by that plan.
func releaseShardCalls(calls []shardCall) {
	for i := range calls {
		if req := calls[i].req; req != nil {
			*req = shardservice.ShardRequest{}
			shardRequestPool.Put(req)
		}
	}
}

func (rp *routerPool) get() *distribution.Router { return rp.pool.Get().(*distribution.Router) }

func (rp *routerPool) put(r *distribution.Router) { rp.pool.Put(r) }

// route resolves q's bound key against the pinned snapshot under the profile's
// admission policy and returns the concrete per-shard calls. It looks up the
// distribution's manifest in the pinned generation, routes through a borrowed
// Router, resolves every target endpoint to an address in that same generation,
// and carries each target's ownership epoch and the manifest's routing version
// into the per-shard request. A missing distribution, a rejected route, or an
// unresolvable endpoint fails closed with a typed error.
func (e *Executor) route(snap *Snapshot, q *Query, bound *BoundPlan, p Profile) (*plan, error) {
	return e.routeContext(context.Background(), snap, q, bound, p)
}

func (e *Executor) routeContext(ctx context.Context, snap *Snapshot, q *Query, bound *BoundPlan, p Profile) (*plan, error) {
	return e.routeContextCached(ctx, snap, q, bound, p, nil)
}

func (e *Executor) routeContextCached(
	ctx context.Context,
	snap *Snapshot,
	q *Query,
	bound *BoundPlan,
	p Profile,
	cache *preparedQueryExecution,
) (*plan, error) {
	if bound == nil || bound.generation != snap.Generation() || bound.manifest == nil {
		return nil, &CatalogError{Reason: "distributed plan does not belong to the pinned catalog generation"}
	}
	mapper := distribution.NewNativeMapperWithBucketBits(bound.spec.Arity, bound.spec.EffectiveBucketBits())

	r := e.routers.get()
	route, err := r.Route(bound.constraints, mapper, bound.manifest, p.Policy)
	e.routers.put(r)
	if err != nil {
		return nil, err
	}
	if err := bound.ValidateRoute(route); err != nil {
		return nil, err
	}

	calls := make([]shardCall, len(route.Targets))
	partialAggregate := len(route.Targets) > 1 && len(bound.groupKeys) != 0
	for i := range route.Targets {
		t := route.Targets[i]
		bucketBits, accessScopes := readAccessScopes(bound, t)
		addr, err := snap.Address(t.Endpoint)
		if err != nil {
			return nil, err
		}
		// Borrow the descriptor shell; every field below is assigned, so
		// no stale pooled value can survive construction.
		req := shardRequestPool.Get().(*shardservice.ShardRequest)
		*req = shardservice.ShardRequest{
			SQL:                  q.SQL,
			Params:               q.Params,
			ParamTypes:           q.ParamTypes,
			PartialAggregate:     partialAggregate,
			Distribution:         route.Distribution,
			Shard:                t.Shard,
			AllocationGeneration: t.AllocationGeneration,
			RoutingVersion:       route.RoutingVersion,
			OwnershipEpoch:       t.OwnershipEpoch,
			ReadPolicy:           p.ReadPolicy,
			ExecutionMode:        shardservice.ExecutionReadOnly,
			Deadline:             p.PerShardDeadline,
			MaxRows:              p.PerShardRows,
			MaxResultBytes:       p.PerShardBytes,
			BucketBits:           bucketBits,
			AccessScopes:         accessScopes,
		}
		calls[i] = shardCall{
			target: t, pressureSource: pressureSourceForTarget(
				bound.manifest, bound.spec.EffectiveBucketBits(), t,
			),
			address: addr,
			req:     req,
		}
	}
	populateReplicatedPrimaryKeyRead(snap, bound, route, mapper, calls)

	var physical *queryplanner.Plan
	var planning queryplanner.OptimizerStatistics
	cacheable := cache != nil && len(bound.aggregates) == 0 && len(bound.groupKeys) == 0
	if cacheable && cache.generation == snap.Generation() && cache.physical != nil &&
		cache.routeKind == route.Kind && cache.targets == len(route.Targets) {
		physical, planning = cache.physical, cache.planning
	} else {
		physical, planning, err = optimizeDistributedPlan(ctx, snap, bound, route, p)
		if err != nil {
			return nil, err
		}
		if cacheable {
			cache.routeKind = route.Kind
			cache.targets = len(route.Targets)
			cache.physical = physical
			cache.planning = planning
		}
	}
	return &plan{
		kind:         route.Kind,
		distribution: route.Distribution,
		version:      route.RoutingVersion,
		generation:   snap.Generation(),
		scatter:      scatterReason(route.Kind, bound.constraints),
		calls:        calls,
		order:        bound.order,
		limit:        bound.limit,
		offset:       bound.offset,
		hasLimit:     bound.hasLimit,
		aggregates:   bound.aggregates,
		groupKeys:    bound.groupKeys,
		aggHeaders:   bound.aggHeaders,
		repartition:  physicalPlanContains(physical, queryplanner.OpRepartition),
		groupLocal:   bound.groupLocal,
		physical:     physical,
		planning:     planning,
	}, nil
}

// populateReplicatedPrimaryKeyRead adds the native primary-key candidate only
// when the pinned catalog and bound route prove one exact RF3 base relation and
// one physical destination. The original SQL remains on the request so the
// shard still evaluates every residual predicate and projection.
func populateReplicatedPrimaryKeyRead(
	snap *Snapshot,
	bound *BoundPlan,
	route distribution.Route,
	mapper *distribution.NativeMapper,
	calls []shardCall,
) {
	if snap == nil || bound == nil || len(calls) != 1 || len(route.Targets) != 1 ||
		route.Kind != distribution.RouteSingle || bound.generation != snap.Generation() ||
		len(bound.tables) != 1 || bound.tables[0] != bound.table || bound.spec.Arity != 1 ||
		bound.manifest == nil || mapper == nil || route.Distribution != bound.distribution ||
		route.RoutingVersion != bound.manifest.Version() {
		return
	}
	placement, spec, manifest, placed := snap.plannerTableFor(bound.table)
	if !placed || manifest == nil || manifest != bound.manifest ||
		placement.Distribution != bound.distribution || spec != bound.spec ||
		spec.Arity != 1 || len(placement.Columns) != 1 {
		return
	}
	entry, replicated := snap.replicatedTableAtBytes(byteview.Bytes(bound.table))
	if !replicated {
		return
	}
	profile, ok := snap.replicatedTableProfileAt(entry)
	if !ok || profile.Table != bound.table || profile.Relation == 0 ||
		profile.Relation > replication.MaxRelationID || profile.PrimaryKey == "" ||
		placement.Columns[0] != profile.PrimaryKey || profile.MaxKeyBytes == 0 ||
		profile.MaxDocumentBytes == 0 ||
		profile.MaxDocumentBytes > replication.MaxMutationValueBytes {
		return
	}
	scalar, ok := replicatedSQLExactConstraint(bound.constraints)
	if !ok {
		return
	}
	var keyStorage [replication.MaxMutationKeyBytes]byte
	key, ok := appendReplicatedSQLScalarKey(keyStorage[:0], scalar)
	if !ok || len(key) == 0 || len(key) > int(profile.MaxKeyBytes) {
		return
	}
	var values [1]distribution.Scalar
	values[0] = scalar
	point, err := mapper.PointFor(values[:])
	if err != nil {
		return
	}
	resolved, ok := manifest.ResolvePointTarget(point)
	target := route.Targets[0]
	if !ok || resolved.Shard != target.Shard ||
		resolved.AllocationGeneration != target.AllocationGeneration ||
		resolved.Endpoint != target.Endpoint || resolved.OwnershipEpoch != target.OwnershipEpoch ||
		resolved.Role != target.Role {
		return
	}
	calls[0].req.PrimaryKeyRead = shardservice.PrimaryKeyReadRequest{
		Relation:         profile.Relation,
		MaxDocumentBytes: profile.MaxDocumentBytes,
		PrimaryPath:      byteview.Bytes(profile.PrimaryKey),
		Keys:             [][]byte{key},
	}
}

func pressureSourceForTarget(
	manifest *distribution.Manifest, bucketBits uint8, target distribution.Target,
) autosplit.SourceIdentity {
	if manifest == nil || bucketBits == 0 {
		return autosplit.SourceIdentity{}
	}
	metadata, ok := manifest.ShardMetadataAt(target.ManifestOrdinal)
	if ok && metadata.ID == target.Shard &&
		metadata.AllocationGeneration == target.AllocationGeneration &&
		metadata.Epoch == target.OwnershipEpoch {
		return autosplit.SourceIdentity{Distribution: manifest.Distribution(),
			Shard: metadata.ID, AllocationGeneration: metadata.AllocationGeneration,
			Range: metadata.Range, BucketBits: bucketBits,
			RoutingVersion: manifest.Version(), OwnershipEpoch: metadata.Epoch}
	}
	return autosplit.SourceIdentity{}
}

func physicalPlanContains(plan *queryplanner.Plan, operation queryplanner.Operator) bool {
	if plan == nil {
		return false
	}
	if plan.Expression.Op == operation {
		return true
	}
	for i := range plan.Children {
		if physicalPlanContains(plan.Children[i], operation) {
			return true
		}
	}
	return false
}

// scatterReason classifies why a route scattered, best-effort, for metrics: a
// non-scatter route has no reason; a scatter with no usable bound leading prefix
// is an unknown route, otherwise the bound key still selected every active shard.
func scatterReason(kind distribution.RouteKind, cons distribution.BoundConstraints) ScatterReason {
	if kind != distribution.RouteScatter {
		return ScatterNone
	}
	if len(cons) == 0 || cons[0].Kind != distribution.DomainFinite {
		return ScatterUnknownRoute
	}
	return ScatterAllShards
}
