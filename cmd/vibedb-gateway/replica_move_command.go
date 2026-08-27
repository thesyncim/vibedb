package main

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
	candidates := gatewayReplicaMoveObservationCandidates(cut.Membership)
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

// The enrolled replacement can lead after promotion but before the final
// catalog roster is published. Only that one certified extra endpoint may be
// observed; arbitrary leader hints never extend this bounded directory.
func gatewayReplicaMoveObservationCandidates(route gateway.ReplicatedMembershipRoute) []gateway.ReplicatedEndpoint {
	if len(route.Serving.Replicas) != gateway.ServingReplicaCount {
		return nil
	}
	candidates := make([]gateway.ReplicatedEndpoint, 0, gateway.ServingReplicaCount+1)
	candidates = append(candidates, route.Serving.Replicas...)
	if !route.HasEnrolledTarget {
		return candidates
	}
	for _, endpoint := range candidates {
		if endpoint.Member == route.EnrolledTarget.Member {
			return candidates
		}
	}
	return append(candidates, route.EnrolledTarget)
}
