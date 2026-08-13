package gateway

import (
	"context"
	"sync"

	"github.com/thesyncim/vibedb/distribution"
	queryplanner "github.com/thesyncim/vibedb/planner"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
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
	target  distribution.Target
	address string
	req     *shardservice.ShardRequest
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
	aggregates   []sqlast.AggKind
	aggHeaders   []string
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
	if bound == nil || bound.generation != snap.Generation() || bound.manifest == nil {
		return nil, &CatalogError{Reason: "distributed plan does not belong to the pinned catalog generation"}
	}
	mapper := distribution.NewNativeMapper(bound.spec.Arity)

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
	for i := range route.Targets {
		t := route.Targets[i]
		addr, err := snap.Address(t.Endpoint)
		if err != nil {
			return nil, err
		}
		calls[i] = shardCall{
			target:  t,
			address: addr,
			req: &shardservice.ShardRequest{
				SQL:                  q.SQL,
				Params:               q.Params,
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
			},
		}
	}

	physical, planning, err := optimizeDistributedPlan(ctx, snap, bound, route, p)
	if err != nil {
		return nil, err
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
		aggregates:   bound.aggregates,
		aggHeaders:   bound.aggHeaders,
		physical:     physical,
		planning:     planning,
	}, nil
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
