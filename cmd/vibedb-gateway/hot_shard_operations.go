package main

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/hotshard"
	"github.com/thesyncim/vibedb/internal/rebalance"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
)

// gatewayHotReplicaMoveFactory turns an advisory endpoint selection into a
// move only when the current catalog already carries every exact identity: the
// source voter, a distinct snapshot donor, and the selected enrolled target.
// It does not allocate a spare member or infer one from the static capacity
// file.
type gatewayHotReplicaMoveFactory struct {
	observations gatewayReplicaObservationClient
}

func (factory gatewayHotReplicaMoveFactory) BuildHotReplicaMove(
	ctx context.Context,
	catalog *gateway.Snapshot,
	admission [32]byte,
	work hotshard.MoveWork,
) (*rebalance.Plan, error) {
	if ctx == nil || catalog == nil || admission == ([32]byte{}) ||
		factory.observations == nil || work.Group == (hotshard.MoveWork{}).Group ||
		work.Selection.SourceEndpoint == "" || work.Selection.TargetEndpoint == "" {
		return nil, hotshard.ErrInvalidPressureCut
	}
	var workspace [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	route, ok := catalog.ResolveReplicatedMembershipRoute(
		work.Selection.Source.Distribution, work.Selection.Source.Shard, workspace[:0],
	)
	if !ok || route.Serving.Group != work.Group || !route.HasEnrolledTarget ||
		distribution.EndpointID(route.EnrolledTarget.Endpoint) != work.Selection.TargetEndpoint {
		return nil, hotshard.ErrInvalidPressureCut
	}

	var retiring, donor gateway.ReplicatedEndpoint
	for _, replica := range route.Serving.Replicas {
		if distribution.EndpointID(replica.Endpoint) == work.Selection.SourceEndpoint {
			retiring = replica
			continue
		}
		if donor.Member == 0 {
			donor = replica
		}
	}
	if retiring.Member == 0 || donor.Member == 0 ||
		route.EnrolledTarget.Member == retiring.Member ||
		route.EnrolledTarget.Member == donor.Member {
		return nil, hotshard.ErrInvalidPressureCut
	}

	request := replicacontrol.Request{
		Operation: admission,
		Step: gatewayReplicaObservationStep(
			rebalance.OperationID(admission), catalog.Generation(),
		),
		Group: work.Group, TargetMember: route.EnrolledTarget.Member,
		ExpectedReplicaSetVersion: route.Serving.Command.ReplicaSetVersion,
	}
	var leader replicacontrol.Observation
	var observeErrors error
	for _, replica := range route.Serving.Replicas {
		candidate, err := factory.observations.Observe(ctx, replica.Node, request)
		if err == nil && candidate.Status.MemberID == replica.Member &&
			candidate.Status.LeaderID == replica.Member && candidate.Status.Term != 0 &&
			candidate.Publication.ReplicaSetVersion == route.Serving.Command.ReplicaSetVersion &&
			exactHotMoveVoters(route.Serving.Replicas, candidate.Publication.ConfState.GetVoters()) {
			leader = candidate
			break
		}
		observeErrors = errors.Join(observeErrors, err)
	}
	if leader.Status.MemberID == 0 {
		return nil, errors.Join(observeErrors, hotshard.ErrInvalidPressureCut)
	}
	plan, err := rebalance.PlanReplicaMove(catalog, leader.Publication, rebalance.MoveRequest{
		Distribution:   work.Selection.Source.Distribution,
		Shard:          work.Selection.Source.Shard,
		Group:          work.Group,
		RetiringMember: retiring.Member, SnapshotSourceMember: donor.Member,
		TargetMember: route.EnrolledTarget.Member,
		Source:       work.Selection.SourceEndpoint, Target: work.Selection.TargetEndpoint,
		RetiringReplica: rebalance.ReplicaIdentity{
			Member: retiring.Member, Node: retiring.Node, StoreID: retiring.StoreID,
			NodeIncarnation: retiring.NodeIncarnation,
			ControlEndpoint: distribution.EndpointID(retiring.ControlEndpoint),
		},
	})
	if err != nil || plan == nil {
		return nil, errors.Join(err, hotshard.ErrInvalidPressureCut)
	}
	return plan, nil
}

func exactHotMoveVoters(replicas []gateway.ReplicatedEndpoint, voters []uint64) bool {
	if len(replicas) != gateway.ServingReplicaCount || len(voters) != len(replicas) {
		return false
	}
	for _, replica := range replicas {
		found := false
		for _, voter := range voters {
			found = found || voter == replica.Member
		}
		if !found {
			return false
		}
	}
	return true
}

var _ hotshard.MovePlanFactory = gatewayHotReplicaMoveFactory{}
