package gateway

import (
	"bytes"
	"context"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
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

// resolveDurableReleaseReceiptRoute resolves the current serving endpoint for
// a retained route-session release receipt. The receipt's command remains the
// exact historical identity; only its physical source and live serving fence
// are refreshed here. A replacement descriptor or split child cannot satisfy
// the full retained Group/allocation/distribution/shard identity.
func (resolver *CatalogDurableRequestRouteResolver) resolveDurableReleaseReceiptRoute(
	ctx context.Context,
	target DurableRequestLogicalTarget,
	exact []byte,
) (ReplicatedRoute, error) {
	if resolver == nil || resolver.catalog == nil || ctx == nil {
		return ReplicatedRoute{}, ErrDurableRequest
	}
	if err := ctx.Err(); err != nil {
		return ReplicatedRoute{}, err
	}
	command, err := replicatedstate.ValidateRouteReleaseReceiptCommand(exact)
	if err != nil || command.Kind() != replication.CommandRouteGate {
		return ReplicatedRoute{}, ErrDurableRequestConflict
	}
	if target.Distribution == "" || target.Shard == "" ||
		target.Group == (raftmember.GroupKey{}) || target.SchemaGeneration == 0 ||
		target.RelationManifestDigest == (replication.Digest{}) ||
		target.RangeIdentity == (replication.Digest{}) ||
		target.LineageDigest == (replication.Digest{}) ||
		target.ForwardingRuleDigest == (replication.Digest{}) {
		return ReplicatedRoute{}, ErrDurableRequestConflict
	}
	if !bytes.Equal(command.Distribution, []byte(target.Distribution)) ||
		!bytes.Equal(command.Shard, []byte(target.Shard)) ||
		command.ClusterID != target.Group.ClusterID ||
		command.ClusterIncarnation != target.Group.ClusterIncarnation ||
		command.TopologyRecoveryEpoch != target.Group.TopologyRecoveryEpoch ||
		command.ShardIncarnation != target.Group.ShardIncarnation ||
		command.GroupID != target.Group.GroupID ||
		command.AllocationGeneration == 0 ||
		command.SchemaGeneration != target.SchemaGeneration {
		return ReplicatedRoute{}, ErrDurableRequestConflict
	}
	snapshot := resolver.catalog.Current()
	if snapshot == nil {
		return ReplicatedRoute{}, ErrDurableRequestUnavailable
	}
	route, ok := snapshot.ResolveReplicatedRoute(
		target.Distribution, target.Shard,
		make([]ReplicatedEndpoint, 0, ServingReplicaCount),
	)
	if !ok || !validReplicatedRoute(route) ||
		route.Distribution != target.Distribution || route.Shard != target.Shard ||
		route.Group != target.Group ||
		route.AllocationGeneration != command.AllocationGeneration {
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
