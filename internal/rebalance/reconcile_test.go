package rebalance

import (
	"encoding/binary"
	"errors"
	"strconv"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func moveTestPoint(value byte) distribution.KeyspacePoint {
	var point distribution.KeyspacePoint
	point[0] = value
	return point
}

func moveTestGroup() raftmember.GroupKey {
	var group raftmember.GroupKey
	for i := range group.ClusterID {
		group.ClusterID[i] = byte(i + 1)
		group.ClusterIncarnation[i] = byte(i + 21)
		group.ShardIncarnation[i] = byte(i + 41)
		group.GroupID[i] = byte(i + 61)
	}
	group.TopologyRecoveryEpoch = 3
	return group
}

func moveTestSnapshot(t testing.TB) *gateway.Snapshot {
	t.Helper()
	manifest, err := distribution.NewManifest("data", 7, []distribution.Shard{
		{
			ID: "all", AllocationGeneration: 11,
			Range:   distribution.KeyRange{Start: moveTestPoint(0), End: distribution.KeyspaceEnd{Max: true}},
			Leaders: []distribution.EndpointID{"source"}, Epoch: 13,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := gateway.NewSnapshot(distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: "data", Arity: 1, MapperVersion: 1}},
		Placements:    []distribution.TablePlacement{{Table: "docs", Distribution: "data", Columns: []string{"/id"}}},
		Manifests:     []*distribution.Manifest{manifest},
	}, map[distribution.EndpointID]string{
		"source": "127.0.0.1:7001", "target": "127.0.0.1:7002",
	}, 9)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func moveTestPlan(t testing.TB) (*Plan, *gateway.Snapshot) {
	t.Helper()
	snapshot := moveTestSnapshot(t)
	plan, err := PlanReplicaMove(snapshot, raftmodel.Publication{
		Applied: 5, ReplicaSetVersion: 4,
		ConfState: &pb.ConfState{Voters: []uint64{1, 3}},
	}, moveTestRequest())
	if err != nil {
		t.Fatalf("PlanReplicaMove: %v", err)
	}
	return plan, snapshot
}

func moveTestRequest() MoveRequest {
	return MoveRequest{
		Distribution: "data", Shard: "all", Group: moveTestGroup(),
		SourceMember: 1, TargetMember: 2, Source: "source", Target: "target",
	}
}

func bindMoveTestPlan(plan *Plan) *Plan {
	next := *plan
	next.baseBound = true
	next.baseDigest = [32]byte{0x91}
	group := plan.request.Group
	next.baseState = replicatedstate.State{
		Binding: replicatedstate.Binding{
			ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
			TopologyRecoveryEpoch: group.TopologyRecoveryEpoch,
			Distribution:          string(plan.request.Distribution), Shard: string(plan.request.Shard),
			AllocationGeneration: 11, ShardIncarnation: group.ShardIncarnation, GroupID: group.GroupID,
			ActivePolicyGeneration: 2, ProtectionEpoch: 3, OwnershipEpoch: 13,
			SchemaGeneration: 4, RoutingVersion: 7, RouteGeneration: 9,
		},
		Applied: 8, LastTerm: 2, ConfState: plan.learnerConf,
		ReplicaSetVersion: 6, SnapshotBaseDigest: next.baseDigest,
	}
	return &next
}

func leaderStatus(member, commit uint64) raftmember.RuntimeStatus {
	return raftmember.RuntimeStatus{
		MemberID: member, LeaderID: member, Term: 3, Commit: commit, Applied: commit,
		RaftState: raft.StateLeader,
	}
}

func TestReplicaMoveReconcileRequiresEverySafetyFence(t *testing.T) {
	plan, catalog := moveTestPlan(t)
	observed := Observation{
		Catalog: catalog, Publication: raftmodel.Publication{
			Applied: 5, ReplicaSetVersion: 4, ConfState: plan.initialConf,
		},
		LeaderStatus: leaderStatus(1, 8),
	}
	action, err := Reconcile(plan, observed)
	if err != nil || action.Kind != ActionAddLearner || action.Member != 2 ||
		action.ConfChange().AsV2().GetChanges()[0].GetType() != pb.ConfChangeAddLearnerNode {
		t.Fatalf("initial action = %+v, %v", action, err)
	}

	observed.Publication = raftmodel.Publication{
		Applied: 6, ReplicaSetVersion: 6, ConfState: plan.learnerConf,
	}
	action, err = Reconcile(plan, observed)
	if err != nil || action.Kind != ActionCreateSnapshotBase {
		t.Fatalf("learner action = %+v, %v", action, err)
	}

	plan = bindMoveTestPlan(plan)
	action, err = Reconcile(plan, observed)
	if err != nil || action.Kind != ActionAwaitSnapshotInstall {
		t.Fatalf("unstaged action = %+v, %v", action, err)
	}
	observed.TargetState = plan.baseState
	observed.TargetStatus = raftmember.RuntimeStatus{MemberID: 2, Applied: 7}
	action, err = Reconcile(plan, observed)
	if err != nil || action.Kind != ActionAwaitCatchUp {
		t.Fatalf("lagging action = %+v, %v", action, err)
	}
	observed.TargetStatus.Applied = 8
	observed.ProgressFound = true
	observed.TargetProgress = raftmodel.MemberProgress{
		Match: 8, Next: 9, Learner: true, RecentActive: true,
	}
	action, err = Reconcile(plan, observed)
	if err != nil || action.Kind != ActionPromoteVoter ||
		action.ConfChange().AsV2().GetChanges()[0].GetType() != pb.ConfChangeAddNode {
		t.Fatalf("caught-up learner action = %+v, %v", action, err)
	}

	observed.Publication = raftmodel.Publication{
		Applied: 9, ReplicaSetVersion: 9, ConfState: plan.voterConf,
	}
	observed.TargetState.Applied = 9
	observed.TargetState.ConfState = plan.voterConf
	observed.TargetState.ReplicaSetVersion = 9
	observed.TargetProgress.Learner = false
	action, err = Reconcile(plan, observed)
	if err != nil || action.Kind != ActionTransferLeader || action.Member != 2 {
		t.Fatalf("voter action = %+v, %v", action, err)
	}
	observed.LeaderStatus = leaderStatus(2, 9)
	observed.TargetStatus = observed.LeaderStatus
	action, err = Reconcile(plan, observed)
	if err != nil || action.Kind != ActionAdvanceOwnership {
		t.Fatalf("target leader action = %+v, %v", action, err)
	}
	command, err := plan.OwnershipCommand(9)
	if err != nil {
		t.Fatal(err)
	}
	transition, err := replicatedstate.OpenOwnershipTransition(command)
	if err != nil || transition.ExpectedReplicaSetVersion != 9 ||
		transition.ToRouteGeneration != 10 {
		t.Fatalf("ownership command = %+v, %v", transition, err)
	}

	observed.TargetState.Binding.OwnershipEpoch++
	observed.TargetState.Binding.RoutingVersion++
	observed.TargetState.Binding.RouteGeneration++
	observed.TargetState.Applied = 10
	action, err = Reconcile(plan, observed)
	if err != nil || action.Kind != ActionPublishCatalog || action.CatalogGeneration != 10 {
		t.Fatalf("ownership action = %+v, %v", action, err)
	}
	nextCatalog, err := plan.CatalogSnapshot(catalog)
	if err != nil {
		t.Fatal(err)
	}
	observed.Catalog = nextCatalog
	action, err = Reconcile(plan, observed)
	if err != nil || action.Kind != ActionAwaitCatalogDrain || action.CatalogGeneration != 10 {
		t.Fatalf("published action = %+v, %v", action, err)
	}
	observed.LeaderStatus = leaderStatus(3, 10)
	observed.TargetStatus = raftmember.RuntimeStatus{MemberID: 2, Applied: 10}
	observed.TargetProgress = raftmodel.MemberProgress{
		Match: 10, Next: 11, RecentActive: true,
	}
	action, err = Reconcile(plan, observed)
	if err != nil || action.Kind != ActionTransferLeader || action.Member != 2 {
		t.Fatalf("post-publication leader loss action = %+v, %v", action, err)
	}
	observed.LeaderStatus = leaderStatus(2, 10)
	observed.TargetStatus = observed.LeaderStatus
	observed.OlderCatalogDrained = true
	action, err = Reconcile(plan, observed)
	if err != nil || action.Kind != ActionRemoveSource ||
		action.ConfChange().AsV2().GetChanges()[0].GetType() != pb.ConfChangeRemoveNode {
		t.Fatalf("drained action = %+v, %v", action, err)
	}
	observed.Publication = raftmodel.Publication{
		Applied: 11, ReplicaSetVersion: 11, ConfState: plan.removedConf,
	}
	observed.TargetState.Applied = 11
	observed.TargetState.ReplicaSetVersion = 11
	observed.TargetState.ConfState = plan.removedConf
	action, err = Reconcile(plan, observed)
	if err != nil || action.Kind != ActionRetireSource {
		t.Fatalf("removed action = %+v, %v", action, err)
	}
	observed.SourceRetired = true
	action, err = Reconcile(plan, observed)
	if err != nil || action.Kind != ActionComplete {
		t.Fatalf("complete action = %+v, %v", action, err)
	}
}

func TestReplicaMoveFailsClosedOnConcurrentTopology(t *testing.T) {
	plan, catalog := moveTestPlan(t)
	observed := Observation{
		Catalog: catalog, Publication: raftmodel.Publication{
			Applied: 5, ReplicaSetVersion: 4,
			ConfState: &pb.ConfState{Voters: []uint64{1, 3, 4}},
		},
		LeaderStatus: leaderStatus(1, 5),
	}
	if _, err := Reconcile(plan, observed); !errors.Is(err, ErrTopologyConflict) {
		t.Fatalf("unrelated membership err=%v, want ErrTopologyConflict", err)
	}
	newer, err := gateway.BuildManifestTransition(catalog, plan.targetManifest, 11)
	if err != nil {
		t.Fatal(err)
	}
	observed.Catalog = newer
	observed.Publication.ConfState = plan.initialConf
	if _, err := Reconcile(plan, observed); !errors.Is(err, ErrTopologyConflict) {
		t.Fatalf("skipped catalog generation err=%v, want ErrTopologyConflict", err)
	}
}

func TestReplicaMovePlanSpreadsRoutingWithoutChangingShardAllocation(t *testing.T) {
	plan, current := moveTestPlan(t)
	manifest := plan.TargetManifest()
	shard, _ := manifest.ShardInfo(0)
	if manifest.Version() != 8 || shard.ID != "all" || shard.AllocationGeneration != 11 ||
		shard.Range.End.Max != true || shard.Leaders[0] != "target" || shard.Epoch != 14 {
		t.Fatalf("target shard = version %d %+v", manifest.Version(), shard)
	}
	original, _ := current.Manifest("data")
	source, _ := original.ShardInfo(0)
	if source.Leaders[0] != "source" || source.Epoch != 13 {
		t.Fatalf("source manifest mutated: %+v", source)
	}
}

func TestReplicaMovePlanRecoversUnboundLearner(t *testing.T) {
	first, catalog := moveTestPlan(t)
	recovered, err := PlanReplicaMove(catalog, raftmodel.Publication{
		Applied: 6, ReplicaSetVersion: 6, ConfState: first.learnerConf,
	}, moveTestRequest())
	if err != nil {
		t.Fatalf("PlanReplicaMove learner recovery: %v", err)
	}
	action, err := Reconcile(recovered, Observation{
		Catalog: catalog,
		Publication: raftmodel.Publication{
			Applied: 6, ReplicaSetVersion: 6, ConfState: recovered.learnerConf,
		},
		LeaderStatus: leaderStatus(1, 6),
	})
	if err != nil || action.Kind != ActionCreateSnapshotBase {
		t.Fatalf("recovered learner action = %+v, %v", action, err)
	}
}

func TestReplicaMoveRecoversCertifiedPlanAcrossCutover(t *testing.T) {
	initial, sourceCatalog := moveTestPlan(t)
	bound := bindMoveTestPlan(initial)
	certificate := replicatedstate.SnapshotBaseCertificate{
		Manifest: replicatedstate.SnapshotArtifactManifest{State: bound.baseState},
		Digest:   bound.baseDigest,
	}

	recovered, err := recoverReplicaMoveCertificate(sourceCatalog, raftmodel.Publication{
		Applied: 9, ReplicaSetVersion: 9, ConfState: initial.voterConf,
	}, moveTestRequest(), certificate)
	if err != nil || !recovered.baseBound || recovered.baseDigest != bound.baseDigest {
		t.Fatalf("recover source catalog = %+v, %v", recovered, err)
	}

	targetCatalog, err := initial.CatalogSnapshot(sourceCatalog)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err = recoverReplicaMoveCertificate(targetCatalog, raftmodel.Publication{
		Applied: 11, ReplicaSetVersion: 11, ConfState: initial.removedConf,
	}, moveTestRequest(), certificate)
	if err != nil || !recovered.sourceManifest.Equal(initial.sourceManifest) ||
		!recovered.targetManifest.Equal(initial.targetManifest) {
		t.Fatalf("recover target catalog = %+v, %v", recovered, err)
	}

	if _, err := recoverReplicaMoveCertificate(sourceCatalog, raftmodel.Publication{
		Applied: 11, ReplicaSetVersion: 11, ConfState: initial.removedConf,
	}, moveTestRequest(), certificate); !errors.Is(err, ErrTopologyConflict) {
		t.Fatalf("removed source catalog error = %v", err)
	}
}

func BenchmarkTargetManifestForMove1024Shards(b *testing.B) {
	const shardCount = 1024
	shards := make([]distribution.Shard, shardCount)
	for index := range shards {
		start := uint64(index) << 54
		end := uint64(index+1) << 54
		binary.BigEndian.PutUint64(shards[index].Range.Start[:], start)
		if index == shardCount-1 {
			shards[index].Range.End.Max = true
		} else {
			binary.BigEndian.PutUint64(shards[index].Range.End.Point[:], end)
		}
		shards[index].ID = distribution.ShardID("shard-" + strconv.Itoa(index))
		shards[index].AllocationGeneration = distribution.ShardAllocationGeneration(index + 1)
		shards[index].Leaders = []distribution.EndpointID{"source"}
		shards[index].Epoch = 13
	}
	manifest, err := distribution.NewManifest("data", 7, shards)
	if err != nil {
		b.Fatal(err)
	}
	request := MoveRequest{
		Distribution: "data", Shard: shards[shardCount-1].ID,
		Source: "source", Target: "target",
	}
	if _, err := targetManifestForMove(manifest, request); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := targetManifestForMove(manifest, request); err != nil {
			b.Fatal(err)
		}
	}
}
