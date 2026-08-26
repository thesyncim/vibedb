package gateway

import "context"

// CatalogDurableRequestRouteResolver resolves a sealed logical participant
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

func (resolver *CatalogDurableRequestRouteResolver) ResolveDurableRequestParticipant(
	ctx context.Context,
	participant DurableRequestLogicalParticipant,
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
		participant.Distribution, participant.Shard, replicas,
	)
	if !ok || !durableRequestRouteMatchesParticipant(route, participant) {
		return ReplicatedRoute{}, ErrDurableRequestConflict
	}
	return route, nil
}

var _ DurableRequestRouteResolver = (*CatalogDurableRequestRouteResolver)(nil)
