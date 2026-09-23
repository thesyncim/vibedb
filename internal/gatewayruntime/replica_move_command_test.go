package gatewayruntime

import (
	"slices"
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

func TestReplicaMoveColdDiscoveryRetainsSourceUntilRemovalReceipt(t *testing.T) {
	catalog, _, observed := gatewayHotShardMoveFixture(t)
	catalog = gateway.NewCatalogHolder(catalog).Current()
	plan, err := rebalance.PlanReplicaMove(catalog, observed.publication, rebalance.MoveRequest{
		Distribution: "data", Shard: "all", Group: observed.grant.Group,
		RetiringMember: 1, SnapshotSourceMember: 2, TargetMember: 4, Source: "one", Target: "target",
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := rebalance.AppendReplicaMoveIntent(nil, catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := rebalance.InspectReplicaMoveIntent(raw)
	if err != nil {
		t.Fatal(err)
	}
	command := intent.Transition.SourceDescriptor.Command
	command.ReplicaSetVersion += 2
	command.OwnershipEpoch++
	command.RoutingVersion++
	command.RouteGeneration++
	published, err := gateway.BuildGroupOwnedShardTransition(catalog, intent.Transition,
		gateway.TransitionPhasePreRemove, gateway.ReplicatedReplicaDescriptor{}, command)
	if err != nil {
		t.Fatal(err)
	}
	postCommand := command
	postCommand.ReplicaSetVersion++
	postPublished, err := gateway.BuildGroupOwnedShardTransition(
		published, intent.Transition, gateway.TransitionPhasePostRemove,
		intent.Transition.Replacement, postCommand,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []struct {
		catalog *gateway.Snapshot
		phase   gateway.TransitionPhase
	}{
		{published, gateway.TransitionPhasePreRemove},
		{postPublished, gateway.TransitionPhasePostRemove},
	} {
		cut, err := resolveGatewayReplicaMoveRoute(state.catalog, intent.Request, intent.Transition, state.phase)
		if err != nil {
			t.Fatal(err)
		}
		if slices.ContainsFunc(cut.Membership.Serving.Replicas, func(e gateway.ReplicatedEndpoint) bool { return e.Member == 1 }) {
			t.Fatal("retiring source returned to public serving roster")
		}
		candidates := cut.Membership.AppendControlEndpoints(nil)
		hasSource := slices.ContainsFunc(candidates, func(e gateway.ReplicatedEndpoint) bool { return e.Member == 1 })
		if hasSource != (state.phase == gateway.TransitionPhasePreRemove) || len(candidates) > 4 {
			t.Fatalf("phase=%d has source=%t candidates=%d", state.phase, hasSource, len(candidates))
		}
		if state.phase == gateway.TransitionPhasePreRemove && (cut.Membership.RetiringSource.NativeEndpoint != "one-native" ||
			cut.Membership.RetiringSource.Address != "127.0.0.1:11" || cut.Membership.RetiringSource.ControlEndpoint != "one-control") {
			t.Fatalf("durable source endpoint was not restored: %+v", cut.Membership.RetiringSource)
		}
	}
	wrong := intent.Request
	wrong.RetiringReplica.StoreID[0]++
	if _, err := resolveGatewayReplicaMoveRoute(published, wrong, intent.Transition, gateway.TransitionPhasePreRemove); err == nil {
		t.Fatal("unrelated source identity admitted to transition discovery")
	}
}

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
	got, err := observeGatewayReplicaMoveCommand(t.Context(), gatewayTestObservationClient{observation: observation}, cut,
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
			if _, err := observeGatewayReplicaMoveCommand(t.Context(), gatewayTestObservationClient{observation: wrong}, cut,
				rebalance.OperationID{1}, execution); err == nil {
				t.Fatal("unrelated authority became a publication command")
			}
		})
	}
	if got := membership.AppendControlEndpoints(nil); len(got) != gateway.ServingReplicaCount+1 {
		t.Fatalf("promoted enrolled leader not discoverable: %v", got)
	}
	membership.Serving.Replicas = append([]gateway.ReplicatedEndpoint(nil), membership.Serving.Replicas...)
	membership.Serving.Replicas[0] = membership.EnrolledTarget
	membership.HasEnrolledTarget = false
	membership.EnrolledTarget = gateway.ReplicatedEndpoint{}
	if got := membership.AppendControlEndpoints(nil); len(got) != gateway.ServingReplicaCount {
		t.Fatalf("published target duplicated in observation set: %v", got)
	}
}
