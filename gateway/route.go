package gateway

import (
	"fmt"
	"sync"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/shardservice"
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
func (e *Executor) route(snap *Snapshot, q *Query, p Profile) (*plan, error) {
	man, ok := snap.Manifest(q.Distribution)
	if !ok {
		return nil, &CatalogError{Reason: fmt.Sprintf("distribution %q has no manifest in generation %d", q.Distribution, snap.Generation())}
	}
	spec, ok := snap.Spec(q.Distribution)
	if !ok {
		return nil, &CatalogError{Reason: fmt.Sprintf("distribution %q has no specification in generation %d", q.Distribution, snap.Generation())}
	}
	if spec.MapperVersion != distribution.NativeMapperVersion {
		return nil, &CatalogError{Reason: fmt.Sprintf(
			"distribution %q mapper version %d is unsupported",
			q.Distribution, spec.MapperVersion)}
	}
	if len(q.Constraints) != spec.Arity {
		return nil, &CatalogError{Reason: fmt.Sprintf(
			"distribution %q has %d constraints but generation %d requires arity %d",
			q.Distribution, len(q.Constraints), snap.Generation(), spec.Arity)}
	}
	mapper := distribution.NewNativeMapper(spec.Arity)

	r := e.routers.get()
	route, err := r.Route(q.Constraints, mapper, man, p.Policy)
	e.routers.put(r)
	if err != nil {
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
				SQL:            q.SQL,
				Params:         q.Params,
				Distribution:   route.Distribution,
				Shard:          t.Shard,
				RoutingVersion: route.RoutingVersion,
				OwnershipEpoch: t.OwnershipEpoch,
				ReadPolicy:     p.ReadPolicy,
				ExecutionMode:  shardservice.ExecutionReadOnly,
				Deadline:       p.PerShardDeadline,
				MaxRows:        p.PerShardRows,
				MaxResultBytes: p.PerShardBytes,
			},
		}
	}

	return &plan{
		kind:         route.Kind,
		distribution: route.Distribution,
		version:      route.RoutingVersion,
		generation:   snap.Generation(),
		scatter:      scatterReason(route.Kind, q.Constraints),
		calls:        calls,
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
