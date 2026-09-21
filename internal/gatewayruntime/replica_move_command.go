package gatewayruntime

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rebalance"
	"github.com/thesyncim/vibedb/internal/rebalanceexec"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
)

func observeGatewayReplicaMoveCommand(ctx context.Context, observer gatewayReplicaObservationClient,
	cut rebalanceexec.MoveRoute, operation rebalance.OperationID, execution rebalance.ReplicatedMoveExecution,
) (raftservice.CommandFence, error) {
	if observer == nil {
		return raftservice.CommandFence{}, errGatewayReplicaControl
	}
	candidates := cut.Membership.AppendControlEndpoints(nil)
	if len(candidates) < gateway.ServingReplicaCount {
		return raftservice.CommandFence{}, errGatewayReplicaControl
	}
	request := replicacontrol.Request{Operation: [32]byte(operation), Step: execution.Proof,
		Group: cut.Membership.Serving.Group, TargetMember: cut.Target.Member,
		ExpectedReplicaSetVersion: execution.PublicationReplicaSet}
	var joined error
	for _, endpoint := range candidates {
		observation, err := observer.Observe(ctx, endpoint.Node, request)
		if err != nil {
			joined = errors.Join(joined, err)
			continue
		}
		if observation.Status.MemberID != endpoint.Member || observation.Status.LeaderID != endpoint.Member ||
			observation.Status.Term == 0 || observation.Publication.Applied < execution.PublicationApplied ||
			observation.Publication.ReplicaSetVersion != execution.PublicationReplicaSet {
			continue
		}
		binding := observation.State.Binding
		route := cut.Membership.Serving
		if binding.ClusterID != route.Group.ClusterID || binding.ClusterIncarnation != route.Group.ClusterIncarnation ||
			binding.TopologyRecoveryEpoch != route.Group.TopologyRecoveryEpoch || binding.GroupID != route.Group.GroupID ||
			binding.ShardIncarnation != route.Group.ShardIncarnation || binding.AllocationGeneration != route.AllocationGeneration ||
			binding.Distribution != string(route.Distribution) || binding.Shard != string(route.Shard) ||
			binding.ActivePolicyGeneration != route.Command.ActivePolicyGeneration || binding.ProtectionEpoch != route.Command.ProtectionEpoch ||
			binding.SchemaGeneration != route.Command.SchemaGeneration ||
			binding.OwnershipEpoch < route.Command.OwnershipEpoch || binding.RoutingVersion < route.Command.RoutingVersion ||
			binding.RouteGeneration < route.Command.RouteGeneration {
			return raftservice.CommandFence{}, rebalanceexec.ErrExecutionFence
		}
		command := route.Command
		command.ReplicaSetVersion = observation.Publication.ReplicaSetVersion
		command.OwnershipEpoch = binding.OwnershipEpoch
		command.RoutingVersion = binding.RoutingVersion
		command.RouteGeneration = binding.RouteGeneration
		return command, nil
	}
	return raftservice.CommandFence{}, errors.Join(joined, errGatewayReplicaControl)
}
