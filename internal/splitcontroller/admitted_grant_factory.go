package splitcontroller

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

// AdmittedShardExecutorFactory creates one operation-scoped local executor
// from the already-pinned durable runtime store. It receives no path or route
// resolver: all physical and placement authority is supplied separately by the
// authenticated retained manifest or PlanIntent.
type AdmittedShardExecutorFactory func(
	context.Context, *gateway.Snapshot, *Plan, PlanAdmission, *RuntimeStoreLease,
) (ShardActionExecutor, error)

type AdmittedChildExecutorFactory func(
	context.Context, *gateway.Snapshot, *Plan, PlanAdmission, uint8, ChildReplicaTarget, *RuntimeStoreLease,
) (ShardActionExecutor, error)

type AdmittedSourceRuntime struct {
	Distribution   distribution.DistributionName
	Shard          distribution.ShardID
	Allocation     distribution.ShardAllocationGeneration
	ManifestDigest [32]byte
	Target         ShardActionTarget
	NewExecutor    AdmittedShardExecutorFactory
}

type LocalAdmittedGrantFactoryOptions struct {
	Node     rafttransport.NodeID
	Sources  []AdmittedSourceRuntime
	Children AdmittedChildExecutorFactory
}

// LocalAdmittedGrantFactory is the bounded bridge from a durably authenticated
// admission to executable shard-local capabilities. Source stores are selected
// by exact retained-manifest digest. Child stores are selected by the exact
// per-replica certificate digest carried in PlanIntent. No path, identity, or
// network route is derived locally.
type LocalAdmittedGrantFactory struct {
	options LocalAdmittedGrantFactoryOptions
}

func NewLocalAdmittedGrantFactory(
	options LocalAdmittedGrantFactoryOptions,
) (*LocalAdmittedGrantFactory, error) {
	if options.Node == (rafttransport.NodeID{}) || len(options.Sources) > AbsoluteMaxLocalPlanAdmissionStores ||
		len(options.Sources) == 0 && options.Children == nil {
		return nil, ErrRemoteExecution
	}
	for index, source := range options.Sources {
		if source.Distribution == "" || source.Shard == "" || source.Allocation == 0 ||
			source.ManifestDigest == ([32]byte{}) || !source.Target.valid() || source.NewExecutor == nil {
			return nil, ErrRemoteExecution
		}
		for prior := 0; prior < index; prior++ {
			if options.Sources[prior].ManifestDigest == source.ManifestDigest ||
				options.Sources[prior].Distribution == source.Distribution &&
					options.Sources[prior].Shard == source.Shard &&
					options.Sources[prior].Allocation == source.Allocation {
				return nil, ErrRemoteExecution
			}
		}
	}
	return &LocalAdmittedGrantFactory{options: options}, nil
}

func (factory *LocalAdmittedGrantFactory) BuildAdmittedShardActionGrants(
	ctx context.Context, catalog *gateway.Snapshot, plan *Plan, admission PlanAdmission,
	leases []*RuntimeStoreLease,
) ([]ShardActionGrant, error) {
	if factory == nil || ctx == nil || catalog == nil || plan == nil ||
		plan.OperationID() != admission.Operation || len(leases) == 0 ||
		len(leases) > AbsoluteMaxLocalPlanAdmissionStores {
		return nil, ErrRemoteExecution
	}
	used := make([]bool, len(leases))
	result := make([]ShardActionGrant, 0, len(leases))
	var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	for _, source := range factory.options.Sources {
		if source.Distribution != plan.source.Distribution || source.Shard != plan.source.Shard ||
			source.Allocation != plan.source.AllocationGeneration {
			continue
		}
		route, ok := catalog.ResolveReplicatedRoute(source.Distribution, source.Shard, replicas[:0])
		if !ok || !targetMatchesRoute(source.Target, route) ||
			!routeMemberOwnedByNode(route, source.Target.Member, factory.options.Node) {
			return nil, ErrRemoteExecution
		}
		lease, index, err := exactAdmissionLease(leases, used, source.ManifestDigest)
		if err != nil {
			return nil, err
		}
		executor, err := source.NewExecutor(ctx, catalog, plan, admission, lease)
		if err != nil || executor == nil {
			return nil, errors.Join(ErrRemoteExecution, err)
		}
		used[index] = true
		result = append(result, witnessedGrant(
			catalog, plan, admission, source.Target, executor, sourceSplitActionMask(), lease,
		))
	}
	for child := uint8(0); child < plan.childCount; child++ {
		target, ok := plan.Target(child)
		if !ok {
			continue
		}
		for _, replica := range target.Replicas {
			if replica.Node != factory.options.Node {
				continue
			}
			if factory.options.Children == nil {
				return nil, ErrRemoteExecution
			}
			lease, index, err := exactAdmissionLease(leases, used, replica.CertificateDigest)
			if err != nil {
				return nil, err
			}
			targetIdentity, err := remoteActionTargetForChildReplica(plan, child, replica)
			if err != nil {
				return nil, err
			}
			executor, err := factory.options.Children(ctx, catalog, plan, admission, child, replica, lease)
			if err != nil || executor == nil {
				return nil, errors.Join(ErrRemoteExecution, err)
			}
			used[index] = true
			result = append(result, witnessedGrant(
				catalog, plan, admission, targetIdentity, executor, childSplitActionMask(), lease,
			))
		}
	}
	if len(result) == 0 {
		return nil, ErrRemoteExecution
	}
	for _, consumed := range used {
		if !consumed {
			return nil, ErrRemoteExecution
		}
	}
	return result, nil
}

func exactAdmissionLease(
	leases []*RuntimeStoreLease, used []bool, digest [32]byte,
) (*RuntimeStoreLease, int, error) {
	if digest == ([32]byte{}) || len(leases) != len(used) {
		return nil, -1, ErrRemoteExecution
	}
	found := -1
	for index, lease := range leases {
		if used[index] || lease == nil || lease.registry == nil || lease.store == nil ||
			lease.registry.manifest != digest || lease.store.manifest != digest {
			continue
		}
		if found >= 0 {
			return nil, -1, ErrRemoteExecution
		}
		found = index
	}
	if found < 0 {
		return nil, -1, ErrRemoteExecution
	}
	return leases[found], found, nil
}

func routeMemberOwnedByNode(
	route gateway.ReplicatedRoute, member uint64, node rafttransport.NodeID,
) bool {
	for _, replica := range route.Replicas {
		if replica.Member == member {
			return replica.Node == node
		}
	}
	return false
}

func witnessedGrant(
	catalog *gateway.Snapshot, plan *Plan, admission PlanAdmission, target ShardActionTarget,
	executor ShardActionExecutor, actions uint16, lease *RuntimeStoreLease,
) ShardActionGrant {
	return ShardActionGrant{
		Operation: admission.Operation, PlanDigest: admission.PlanDigest, Target: target,
		Plan: plan, Executor: executor, Actions: actions, Admission: admission, Catalog: catalog,
		Leases: []*RuntimeStoreLease{lease},
	}
}

var _ PlanAdmissionGrantFactory = (*LocalAdmittedGrantFactory)(nil)
