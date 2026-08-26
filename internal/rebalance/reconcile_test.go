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
	"github.com/thesyncim/vibedb/internal/raftservice"
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
			Leaders: []distribution.EndpointID{"source", "donor", "other"}, Epoch: 13,
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
		"donor": "127.0.0.1:7003", "other": "127.0.0.1:7004",
		"target-native": "127.0.0.1:7102", "donor-native": "127.0.0.1:7103",
		"other-native": "127.0.0.1:7104", "target-control": "127.0.0.1:7202",
		"source-control": "127.0.0.1:7201",
		"donor-control":  "127.0.0.1:7203", "other-control": "127.0.0.1:7204",
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
		ConfState: &pb.ConfState{Voters: []uint64{1, 3, 4}},
	}, moveTestRequest())
	if err != nil {
		t.Fatalf("PlanReplicaMove: %v", err)
	}
	return plan, snapshot
}

func moveTestRequest() MoveRequest {
	return MoveRequest{
		Distribution: "data", Shard: "all", Group: moveTestGroup(),
		RetiringMember: 1, SnapshotSourceMember: 3, TargetMember: 2,
		Source: "source", Target: "target",
		RetiringReplica: ReplicaIdentity{
			Member: 1, Node: [16]byte{1}, StoreID: [16]byte{11},
			NodeIncarnation: 21, ControlEndpoint: "source-control",
		},
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
			OwnedRange: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		},
		Applied: 8, LastTerm: 2, ConfState: plan.learnerConf,
		ReplicaSetVersion: 6, SnapshotBaseDigest: next.baseDigest,
	}
	return &next
}

func moveTestPostRemoveCatalog(t testing.TB, plan *Plan, replicaSetVersion uint64) *gateway.Snapshot {
	t.Helper()
	config := distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: "data", Arity: 1, MapperVersion: 1}},
		Placements: []distribution.TablePlacement{{
			Table: "docs", Distribution: "data", Columns: []string{"/id"},
		}},
		Manifests: []*distribution.Manifest{plan.targetManifest},
	}
	endpoints := map[distribution.EndpointID]string{
		"source": "127.0.0.1:7001", "target": "127.0.0.1:7002",
		"donor": "127.0.0.1:7003", "other": "127.0.0.1:7004",
		"source-control": "127.0.0.1:7201",
		"target-native":  "127.0.0.1:7102", "donor-native": "127.0.0.1:7103",
		"other-native": "127.0.0.1:7104", "target-control": "127.0.0.1:7202",
		"donor-control": "127.0.0.1:7203", "other-control": "127.0.0.1:7204",
	}
	descriptor := gateway.ReplicatedShardDescriptor{
		Distribution: "data", Shard: "all", Group: plan.request.Group,
		AllocationGeneration: 11,
		Command: raftservice.CommandFence{
			ReplicaSetVersion: replicaSetVersion, ActivePolicyGeneration: 2,
			ProtectionEpoch: 3, OwnershipEpoch: 14, SchemaGeneration: 4,
			RelationManifestDigest: [32]byte{1}, RoutingVersion: 8, RouteGeneration: 10,
		},
		Replicas: []gateway.ReplicatedReplicaDescriptor{
			{Member: 2, Node: [16]byte{2}, StoreID: [16]byte{12}, NodeIncarnation: 22,
				Endpoint: "target", NativeEndpoint: "target-native", ControlEndpoint: "target-control"},
			{Member: 3, Node: [16]byte{3}, StoreID: [16]byte{13}, NodeIncarnation: 23,
				Endpoint: "donor", NativeEndpoint: "donor-native", ControlEndpoint: "donor-control"},
			{Member: 4, Node: [16]byte{4}, StoreID: [16]byte{14}, NodeIncarnation: 24,
				Endpoint: "other", NativeEndpoint: "other-native", ControlEndpoint: "other-control"},
		},
	}
	snapshot, err := gateway.NewSnapshotWithReplicatedMetadata(
		config, endpoints, plan.postRemoveGeneration, nil, nil,
		[]gateway.ReplicatedShardDescriptor{descriptor},
	)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
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
		LeaderStatus: leaderStatus(3, 8),
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
	if err != nil || action.Kind != ActionCreateSnapshotBase || action.Member != 3 {
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
	observed.TargetProgress.Match = 9
	observed.TargetProgress.Next = 10
	observed.TargetStatus.Applied = 9
	observed.LeaderStatus = leaderStatus(1, 9)
	action, err = Reconcile(plan, observed)
	if err != nil || action.Kind != ActionTransferLeader || action.Member != 2 {
		t.Fatalf("retiring leader action = %+v, %v", action, err)
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
		transition.SourceMember != 3 || transition.TargetMember != 2 ||
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
	if err != nil || action.Kind != ActionAwaitCatalogDrain ||
		action.CatalogGeneration != plan.nextCatalogGeneration {
		t.Fatalf("post-publication leader loss action = %+v, %v", action, err)
	}
	observed.DrainedCatalogGeneration = 10
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
	if err != nil || action.Kind != ActionRefreshCatalogFence ||
		action.CatalogGeneration != 11 || action.ReplicaSetVersion != 11 {
		t.Fatalf("removed action = %+v, %v", action, err)
	}
	observed.Catalog = moveTestPostRemoveCatalog(t, plan, 11)
	action, err = Reconcile(plan, observed)
	if err != nil || action.Kind != ActionAwaitCatalogDrain || action.CatalogGeneration != 11 {
		t.Fatalf("post-remove publication action = %+v, %v", action, err)
	}
	observed.DrainedCatalogGeneration = 11
	action, err = Reconcile(plan, observed)
	if err != nil || action.Kind != ActionRetireSource || action.Member != 1 {
		t.Fatalf("post-remove drain action = %+v, %v", action, err)
	}
	observed.RetiringReplicaRetired = true
	action, err = Reconcile(plan, observed)
	if err != nil || action.Kind != ActionComplete {
		t.Fatalf("complete action = %+v, %v", action, err)
	}
}

func TestReplicaMoveTransfersOnlyRetiringSourceLeadership(t *testing.T) {
	plan, catalog := moveTestPlan(t)
	plan = bindMoveTestPlan(plan)
	state := plan.baseState
	state.Applied = 9
	state.ReplicaSetVersion = 9
	state.ConfState = plan.voterConf
	ready := Observation{
		Catalog: catalog,
		Publication: raftmodel.Publication{
			Applied: 9, ReplicaSetVersion: 9, ConfState: plan.voterConf,
		},
		LeaderStatus: leaderStatus(plan.request.RetiringMember, 9),
		TargetStatus: raftmember.RuntimeStatus{MemberID: plan.request.TargetMember, Applied: 9},
		TargetState:  state,
		TargetProgress: raftmodel.MemberProgress{
			Match: 9, Next: 10, RecentActive: true,
		},
		ProgressFound: true,
	}

	lagging := ready
	lagging.TargetProgress.Match = 8
	if action, err := Reconcile(plan, lagging); err != nil ||
		action.Kind != ActionAwaitCatchUp || action.Member != plan.request.TargetMember {
		t.Fatalf("source leader with lagging target action = %+v, %v", action, err)
	}
	if action, err := Reconcile(plan, ready); err != nil ||
		action.Kind != ActionTransferLeader || action.Member != plan.request.TargetMember {
		t.Fatalf("source leader with ready target action = %+v, %v", action, err)
	}

	otherLeader := ready
	otherLeader.LeaderStatus = leaderStatus(plan.request.SnapshotSourceMember, 9)
	// Once a live observation proves leadership has left the retiring source,
	// stale per-target progress cannot force an unnecessary second transfer.
	otherLeader.ProgressFound = false
	otherLeader.TargetProgress = raftmodel.MemberProgress{}
	otherLeader.TargetStatus = raftmember.RuntimeStatus{}
	if action, err := Reconcile(plan, otherLeader); err != nil ||
		action.Kind != ActionAdvanceOwnership || action.Member != plan.request.TargetMember {
		t.Fatalf("other leader action = %+v, %v", action, err)
	}

	staleLeader := ready
	staleLeader.LeaderStatus.Term = 0
	if action, err := Reconcile(plan, staleLeader); err != nil || action.Kind != ActionAwaitLeader {
		t.Fatalf("stale leader observation action = %+v, %v", action, err)
	}
}

func TestReplicaMoveFailsClosedOnConcurrentTopology(t *testing.T) {
	plan, catalog := moveTestPlan(t)
	observed := Observation{
		Catalog: catalog, Publication: raftmodel.Publication{
			Applied: 5, ReplicaSetVersion: 4,
			ConfState: &pb.ConfState{Voters: []uint64{1, 3, 4, 5}},
		},
		LeaderStatus: leaderStatus(3, 5),
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

func TestReplicaMoveRequiresDistinctHealthySnapshotDonor(t *testing.T) {
	catalog := moveTestSnapshot(t)
	publication := raftmodel.Publication{
		Applied: 5, ReplicaSetVersion: 4,
		ConfState: &pb.ConfState{Voters: []uint64{1, 3, 4}},
	}
	for name, mutate := range map[string]func(*MoveRequest){
		"retiring is not a voter": func(request *MoveRequest) { request.RetiringMember = 5 },
		"donor is not a voter":    func(request *MoveRequest) { request.SnapshotSourceMember = 5 },
		"retiring is donor": func(request *MoveRequest) {
			request.SnapshotSourceMember = request.RetiringMember
		},
		"donor is target": func(request *MoveRequest) {
			request.SnapshotSourceMember = request.TargetMember
		},
	} {
		t.Run(name, func(t *testing.T) {
			request := moveTestRequest()
			mutate(&request)
			if _, err := PlanReplicaMove(catalog, publication, request); !errors.Is(err, ErrInvalidPlan) {
				t.Fatalf("PlanReplicaMove error = %v", err)
			}
		})
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
		LeaderStatus: leaderStatus(3, 6),
	})
	if err != nil || action.Kind != ActionCreateSnapshotBase || action.Member != 3 {
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
	postRemoveCatalog := moveTestPostRemoveCatalog(t, initial, 11)
	recovered, err = recoverReplicaMoveCertificate(postRemoveCatalog, raftmodel.Publication{
		Applied: 11, ReplicaSetVersion: 11, ConfState: initial.removedConf,
	}, moveTestRequest(), certificate)
	if err != nil || recovered.PostRemoveCatalogGeneration() != postRemoveCatalog.Generation() ||
		recovered.OperationID() != initial.OperationID() {
		t.Fatalf("recover post-remove catalog = %+v, %v", recovered, err)
	}

	if _, err := recoverReplicaMoveCertificate(sourceCatalog, raftmodel.Publication{
		Applied: 11, ReplicaSetVersion: 11, ConfState: initial.removedConf,
	}, moveTestRequest(), certificate); !errors.Is(err, ErrTopologyConflict) {
		t.Fatalf("removed source catalog error = %v", err)
	}
}

func TestReplicaMoveRejectsWrongPostRemoveCatalogFence(t *testing.T) {
	plan, _ := moveTestPlan(t)
	plan = bindMoveTestPlan(plan)
	observed := Observation{
		Catalog: moveTestPostRemoveCatalog(t, plan, 12),
		Publication: raftmodel.Publication{
			Applied: 11, ReplicaSetVersion: 11, ConfState: plan.removedConf,
		},
		LeaderStatus: leaderStatus(2, 11), TargetStatus: leaderStatus(2, 11),
		TargetState: plan.baseState, DrainedCatalogGeneration: plan.postRemoveGeneration,
	}
	observed.TargetState.Binding.OwnershipEpoch++
	observed.TargetState.Binding.RoutingVersion++
	observed.TargetState.Binding.RouteGeneration++
	observed.TargetState.Applied = 11
	observed.TargetState.ReplicaSetVersion = 11
	observed.TargetState.ConfState = plan.removedConf
	if _, err := Reconcile(plan, observed); !errors.Is(err, ErrTopologyConflict) {
		t.Fatalf("wrong post-remove catalog fence error = %v", err)
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
