package splitcontroller

import (
	"context"
	"errors"
	"sync"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/shardcontrol"
)

type PlanAdmissionRoutePublisher interface {
	InstallPlanRoutes(*gateway.Snapshot, *Plan, PlanAdmission) error
}

type admittedPlanRouteKey struct {
	operation OperationID
	digest    [32]byte
}

type admittedPlanRoutes struct {
	routes []gateway.ReplicatedRoute
}

// DynamicShardActionRoutes is the bounded gateway-local routing capability
// for admitted split operations. Child routes come from the authenticated
// PlanIntent and the exact catalog endpoint directory; no DNS or manifest
// fallback exists. A route becomes visible only after RF3 admission settles.
type DynamicShardActionRoutes struct {
	mu    sync.RWMutex
	plans map[admittedPlanRouteKey]admittedPlanRoutes
	limit int
}

func NewDynamicShardActionRoutes(limit int) (*DynamicShardActionRoutes, error) {
	if limit <= 0 || limit > AbsoluteMaxShardActionGrants {
		return nil, ErrShardControlRoute
	}
	return &DynamicShardActionRoutes{
		plans: make(map[admittedPlanRouteKey]admittedPlanRoutes, min(limit, 64)), limit: limit,
	}, nil
}

func (directory *DynamicShardActionRoutes) InstallPlanRoutes(
	catalog *gateway.Snapshot, plan *Plan, admission PlanAdmission,
) error {
	if directory == nil || catalog == nil || plan == nil ||
		plan.OperationID() != admission.Operation || admission.PlanDigest == ([32]byte{}) {
		return ErrShardControlRoute
	}
	if _, err := admission.Open(catalog); err != nil {
		return errors.Join(ErrShardControlRoute, err)
	}
	routes, err := exactAdmittedPlanRoutes(catalog, plan)
	if err != nil {
		return err
	}
	key := admittedPlanRouteKey{operation: admission.Operation, digest: admission.PlanDigest}
	directory.mu.Lock()
	defer directory.mu.Unlock()
	if _, found := directory.plans[key]; !found && len(directory.plans) == directory.limit {
		return ErrShardControlRoute
	}
	directory.plans[key] = admittedPlanRoutes{routes: routes}
	return nil
}

func (directory *DynamicShardActionRoutes) ResolveShardControl(
	_ context.Context, target ShardActionTarget, _ Action, request shardcontrol.Request,
) (gateway.ReplicatedRoute, error) {
	if directory == nil || !target.valid() || request.Operation == ([32]byte{}) ||
		request.PlanDigest == ([32]byte{}) {
		return gateway.ReplicatedRoute{}, ErrShardControlRoute
	}
	key := admittedPlanRouteKey{operation: OperationID(request.Operation), digest: request.PlanDigest}
	directory.mu.RLock()
	set, found := directory.plans[key]
	if found {
		for _, route := range set.routes {
			if targetMatchesRoute(target, route) {
				if target.Member != 0 {
					for _, replica := range route.Replicas {
						if replica.Member == target.Member {
							route.Replicas = []gateway.ReplicatedEndpoint{replica}
							break
						}
					}
				}
				directory.mu.RUnlock()
				return cloneReplicatedRoute(route), nil
			}
		}
	}
	directory.mu.RUnlock()
	return gateway.ReplicatedRoute{}, ErrShardControlRoute
}

// retire removes only the exact admitted route set. It is intentionally
// private: callers must pass through TerminalSplitOperationRetirer so catalog
// terminal authority is checked before a live route disappears.
func (directory *DynamicShardActionRoutes) retire(operation OperationID, digest [32]byte) {
	if directory == nil || operation == (OperationID{}) || digest == ([32]byte{}) {
		return
	}
	directory.mu.Lock()
	delete(directory.plans, admittedPlanRouteKey{operation: operation, digest: digest})
	directory.mu.Unlock()
}

func exactAdmittedPlanRoutes(catalog *gateway.Snapshot, plan *Plan) ([]gateway.ReplicatedRoute, error) {
	source, ok := catalog.ResolveReplicatedRoute(
		plan.source.Distribution, plan.source.Shard,
		make([]gateway.ReplicatedEndpoint, 0, gateway.ServingReplicaCount),
	)
	if !ok || len(source.Replicas) != gateway.ServingReplicaCount {
		return nil, ErrShardControlRoute
	}
	routes := make([]gateway.ReplicatedRoute, 1, int(plan.childCount))
	routes[0] = cloneReplicatedRoute(source)
	for child := uint8(0); child < plan.childCount; child++ {
		target, present := plan.Target(child)
		if !present {
			continue
		}
		route, err := exactPreparedChildRoute(catalog, plan, child, target)
		if err != nil {
			return nil, err
		}
		routes = append(routes, route)
	}
	return routes, nil
}

func exactPreparedChildRoute(
	catalog *gateway.Snapshot, plan *Plan, child uint8, target ChildTarget,
) (gateway.ReplicatedRoute, error) {
	if catalog == nil || plan == nil || len(target.Replicas) != gateway.ServingReplicaCount {
		return gateway.ReplicatedRoute{}, ErrShardControlRoute
	}
	binding := target.SQL.Binding
	group := raftmember.GroupKey{
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		ShardIncarnation:      binding.ShardIncarnation, GroupID: binding.GroupID,
	}
	command := raftservice.CommandFence{
		ReplicaSetVersion:      target.ReplicaSetVersion,
		ActivePolicyGeneration: target.Authority.ActivePolicyGeneration,
		ProtectionEpoch:        target.Authority.ProtectionEpoch,
		OwnershipEpoch:         target.Authority.OwnershipEpoch,
		SchemaGeneration:       target.Authority.SchemaGeneration,
		RelationManifestDigest: target.SQL.RelationManifestDigest,
		RoutingVersion:         target.Authority.RoutingVersion,
		RouteGeneration:        target.Authority.RouteGeneration,
	}
	if child >= plan.childCount {
		return gateway.ReplicatedRoute{}, ErrShardControlRoute
	}
	identity := plan.children[child]
	if identity.Shard == "" || group == (raftmember.GroupKey{}) || !command.Valid() ||
		uint64(identity.AllocationGeneration) != binding.AllocationGeneration {
		return gateway.ReplicatedRoute{}, ErrShardControlRoute
	}
	replicas := make([]gateway.ReplicatedEndpoint, len(target.Replicas))
	for index, replica := range target.Replicas {
		peerAddress, peerErr := catalog.Address(replica.Endpoint)
		nativeAddress, nativeErr := catalog.Address(replica.NativeEndpoint)
		controlAddress, controlErr := catalog.Address(replica.ControlEndpoint)
		if peerErr != nil || nativeErr != nil || controlErr != nil {
			return gateway.ReplicatedRoute{}, errors.Join(ErrShardControlRoute, peerErr, nativeErr, controlErr)
		}
		replicas[index] = gateway.ReplicatedEndpoint{
			Member: replica.Member, Node: replica.Node, StoreID: replica.StoreID,
			NodeIncarnation: replica.NodeIncarnation,
			Endpoint:        string(replica.Endpoint), DataAddress: peerAddress,
			NativeEndpoint: string(replica.NativeEndpoint), Address: nativeAddress,
			ControlEndpoint: string(replica.ControlEndpoint), ControlAddress: controlAddress,
		}
	}
	return gateway.ReplicatedRoute{
		Distribution: plan.source.Distribution, Shard: identity.Shard, Group: group,
		AllocationGeneration: binding.AllocationGeneration, Command: command, Replicas: replicas,
	}, nil
}

func cloneReplicatedRoute(route gateway.ReplicatedRoute) gateway.ReplicatedRoute {
	clone := route
	clone.Replicas = append([]gateway.ReplicatedEndpoint(nil), route.Replicas...)
	return clone
}
