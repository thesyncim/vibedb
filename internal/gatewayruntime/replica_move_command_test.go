package gatewayruntime

import (
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rebalance"
	"github.com/thesyncim/vibedb/internal/rebalanceexec"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

func TestReplicaMoveCommandUsesAuthenticatedPostOwnershipCut(t *testing.T) {
	_, membership, _ := gatewayMembershipFixture()
	route := &membership.Serving
	route.Distribution, route.Shard, route.AllocationGeneration = "data", "all", 1
	route.Command = raftservice.CommandFence{ReplicaSetVersion: 1, ActivePolicyGeneration: 2,
		ProtectionEpoch: 3, OwnershipEpoch: 4, SchemaGeneration: 5,
		RelationManifestDigest: [32]byte{6}, RoutingVersion: 7, RouteGeneration: 8}
	cut := rebalanceexec.MoveRoute{Membership: membership, Target: membership.EnrolledTarget, Command: route.Command}
	binding := replicatedstate.Binding{ClusterID: route.Group.ClusterID, ClusterIncarnation: route.Group.ClusterIncarnation,
		TopologyRecoveryEpoch: route.Group.TopologyRecoveryEpoch, ShardIncarnation: route.Group.ShardIncarnation,
		GroupID: route.Group.GroupID, Distribution: "data", Shard: "all", AllocationGeneration: 1,
		ActivePolicyGeneration: 2, ProtectionEpoch: 3, OwnershipEpoch: 5, SchemaGeneration: 5,
		RoutingVersion: 8, RouteGeneration: 9}
	observation := replicacontrol.Observation{
		Status:      raftmember.RuntimeStatus{MemberID: 4, LeaderID: 4, Term: 2},
		Publication: raftmodel.Publication{Applied: 12, ReplicaSetVersion: 10},
		State:       replicatedstate.State{Binding: binding},
	}
	execution := rebalance.ReplicatedMoveExecution{PublicationApplied: 11, PublicationReplicaSet: 10, Proof: [32]byte{1}}
	got, err := observeGatewayReplicaMoveCommand(t.Context(), gatewayTestObservationClient{observation}, cut,
		rebalance.OperationID{1}, execution)
	want := route.Command
	want.ReplicaSetVersion, want.OwnershipEpoch, want.RoutingVersion, want.RouteGeneration = 10, 5, 8, 9
	if err != nil || got != want || cut.Command != route.Command {
		t.Fatalf("current command=%+v want=%+v err=%v", got, want, err)
	}
	for name, mutate := range map[string]func(*replicacontrol.Observation){
		"foreign-group":      func(o *replicacontrol.Observation) { o.State.Binding.GroupID[0]++ },
		"foreign-allocation": func(o *replicacontrol.Observation) { o.State.Binding.AllocationGeneration++ },
		"policy":             func(o *replicacontrol.Observation) { o.State.Binding.ActivePolicyGeneration++ },
		"schema":             func(o *replicacontrol.Observation) { o.State.Binding.SchemaGeneration++ },
		"protection":         func(o *replicacontrol.Observation) { o.State.Binding.ProtectionEpoch++ },
		"old-cut":            func(o *replicacontrol.Observation) { o.Publication.Applied = 1 },
		"different-set":      func(o *replicacontrol.Observation) { o.Publication.ReplicaSetVersion++ },
		"unenrolled-leader":  func(o *replicacontrol.Observation) { o.Status.MemberID, o.Status.LeaderID = 5, 5 },
	} {
		t.Run(name, func(t *testing.T) {
			wrong := observation
			mutate(&wrong)
			if _, err := observeGatewayReplicaMoveCommand(t.Context(), gatewayTestObservationClient{wrong}, cut,
				rebalance.OperationID{1}, execution); err == nil {
				t.Fatal("unrelated authority became a publication command")
			}
		})
	}
	if got := gatewayReplicaMoveObservationCandidates(membership); len(got) != gateway.ServingReplicaCount+1 {
		t.Fatalf("promoted enrolled leader not discoverable: %v", got)
	}
	membership.Serving.Replicas = append([]gateway.ReplicatedEndpoint(nil), membership.Serving.Replicas...)
	membership.Serving.Replicas[0] = membership.EnrolledTarget
	membership.HasEnrolledTarget = false
	membership.EnrolledTarget = gateway.ReplicatedEndpoint{}
	if got := gatewayReplicaMoveObservationCandidates(membership); len(got) != gateway.ServingReplicaCount {
		t.Fatalf("published target duplicated in observation set: %v", got)
	}
}
