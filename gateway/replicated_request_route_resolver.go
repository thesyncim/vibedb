package gateway

import (
	"context"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
)

// CatalogDurableRequestRouteResolver resolves a sealed logical target
// against the currently published physical RF3 allocation. It accepts endpoint
// and leader movement only when every immutable logical authority witness still
// matches the recipe; schema or lineage drift fails closed before proposal.
type CatalogDurableRequestRouteResolver struct {
	catalog *CatalogHolder
}

func NewCatalogDurableRequestRouteResolver(
	catalog *CatalogHolder,
) (*CatalogDurableRequestRouteResolver, error) {
	if catalog == nil || catalog.Current() == nil {
		return nil, ErrDurableRequest
	}
	return &CatalogDurableRequestRouteResolver{catalog: catalog}, nil
}

func (resolver *CatalogDurableRequestRouteResolver) ResolveDurableRequestTarget(
	ctx context.Context,
	target DurableRequestLogicalTarget,
) (ReplicatedRoute, error) {
	if resolver == nil || resolver.catalog == nil || ctx == nil {
		return ReplicatedRoute{}, ErrDurableRequest
	}
	if err := ctx.Err(); err != nil {
		return ReplicatedRoute{}, err
	}
	snapshot := resolver.catalog.Current()
	if snapshot == nil {
		return ReplicatedRoute{}, ErrDurableRequestUnavailable
	}
	replicas := make([]ReplicatedEndpoint, 0, ServingReplicaCount)
	route, ok := snapshot.ResolveReplicatedRoute(
		target.Distribution, target.Shard, replicas,
	)
	if !ok || !durableRequestRouteMatchesTarget(route, target) {
		return ReplicatedRoute{}, ErrDurableRequestConflict
	}
	return route, nil
}

var _ DurableRequestRouteResolver = (*CatalogDurableRequestRouteResolver)(nil)

// resolveDurableSessionRoute is only for retiring a previously released wave's
// session. All command fences must still match; this does not authorize data
// work against a changed logical target or replacement allocation.
func (resolver *CatalogDurableRequestRouteResolver) resolveDurableSessionRoute(ctx context.Context, exact []byte) (ReplicatedRoute, error) {
	if resolver == nil || resolver.catalog == nil || ctx == nil {
		return ReplicatedRoute{}, ErrDurableRequest
	}
	if err := ctx.Err(); err != nil {
		return ReplicatedRoute{}, err
	}
	command, err := replication.OpenCommand(exact)
	if err != nil || command.Kind() != replication.CommandRouteGate {
		return ReplicatedRoute{}, ErrDurableRequestConflict
	}
	snapshot := resolver.catalog.Current()
	if snapshot == nil {
		return ReplicatedRoute{}, ErrDurableRequestUnavailable
	}
	route, ok := snapshot.ResolveReplicatedRoute(distribution.DistributionName(command.Distribution),
		distribution.ShardID(command.Shard), make([]ReplicatedEndpoint, 0, ServingReplicaCount))
	if !ok || !commandMatchesRoute(exact, route) {
		return ReplicatedRoute{}, ErrDurableRequestConflict
	}
	return route, nil
}
