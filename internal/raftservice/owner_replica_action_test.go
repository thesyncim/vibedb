package raftservice

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"go.etcd.io/raft/v3/raftpb"
)

func TestReplicaActionFencesOwnershipAndRetirementExactly(t *testing.T) {
	group := peerServerTestGroup()
	fence := ServingFence{Group: group, AllocationGeneration: 3,
		Command: CommandFence{ReplicaSetVersion: 7, ActivePolicyGeneration: 5,
			ProtectionEpoch: 6, OwnershipEpoch: 8, SchemaGeneration: 9,
			RelationManifestDigest: [32]byte{4}, RoutingVersion: 10, RouteGeneration: 11},
		MemberID: 2, StoreID: [16]byte{3}, NodeIncarnation: 4, Term: 12}
	binding := replicatedstate.Binding{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
		TopologyRecoveryEpoch: group.TopologyRecoveryEpoch, Distribution: "d", Shard: "s",
		AllocationGeneration: fence.AllocationGeneration, ShardIncarnation: group.ShardIncarnation,
		GroupID: group.GroupID, ActivePolicyGeneration: fence.Command.ActivePolicyGeneration,
		ProtectionEpoch: fence.Command.ProtectionEpoch, OwnershipEpoch: fence.Command.OwnershipEpoch,
		SchemaGeneration: fence.Command.SchemaGeneration, RoutingVersion: fence.Command.RoutingVersion,
		RouteGeneration: fence.Command.RouteGeneration}
	command, err := replicatedstate.AppendOwnershipTransition(nil, replicatedstate.OwnershipTransition{
		From: binding, ExpectedReplicaSetVersion: fence.Command.ReplicaSetVersion,
		SourceMember: 1, TargetMember: 2, ToOwnershipEpoch: 9,
		ToRoutingVersion: 11, ToRouteGeneration: 12})
	if err != nil {
		t.Fatal(err)
	}
	transition, err := replicatedstate.OpenOwnershipTransition(command)
	if err != nil || !ownershipTransitionMatchesFence(transition, fence) {
		t.Fatalf("transition fence=%v", err)
	}
	stale := fence
	stale.Command.OwnershipEpoch--
	if ownershipTransitionMatchesFence(transition, stale) {
		t.Fatal("stale ownership fence accepted")
	}

	retired := binding
	retired.OwnershipEpoch = 9
	retired.RoutingVersion = 11
	retired.RouteGeneration = 12
	retireFence := fence
	retireFence.Command.OwnershipEpoch = 9
	retireFence.Command.RoutingVersion = 11
	retireFence.Command.RouteGeneration = 12
	retireFence.Command.ReplicaSetVersion = 8
	state := replicatedstate.State{Binding: retired, ReplicaSetVersion: 8,
		ConfState: &raftpb.ConfState{Voters: []uint64{2, 3, 4}}}
	if !retirementStateMatches(state, retireFence, 1, 2) {
		t.Fatal("exact retired state rejected")
	}
	state.ConfState.Voters = []uint64{1, 2, 3, 4}
	if retirementStateMatches(state, retireFence, 1, 2) {
		t.Fatal("still-voting source accepted")
	}

	member := ownerMember{identity: raftmember.RuntimeIdentity{Group: group,
		AllocationGeneration: fence.AllocationGeneration, MemberID: fence.MemberID,
		StoreID: fence.StoreID, NodeIncarnation: fence.NodeIncarnation,
		RelationManifestDigest: fence.Command.RelationManifestDigest}, command: fence.Command, retiring: true}
	if servingFenceMatchesIdentity(fence, member) {
		t.Fatal("retiring member served")
	}
	member.retiring = false
	owner := &Owner{members: map[raftmember.GroupKey]ownerMember{group: member}}
	if err := owner.syncCommandFenceFromState(group, ReplicaObservation{
		Publication: raftmodel.Publication{ReplicaSetVersion: 8}, State: state,
	}); err != nil {
		t.Fatal(err)
	}
	updated := owner.members[group].command
	if updated.ReplicaSetVersion != 8 || updated.OwnershipEpoch != 9 ||
		updated.RoutingVersion != 11 || updated.RouteGeneration != 12 {
		t.Fatalf("updated command=%+v", updated)
	}
}
