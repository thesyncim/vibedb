package splitcontroller

import (
	"crypto/sha256"
	"errors"
	"path/filepath"
	"strconv"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func TestNewPlanBindsExactCatalogAndChildRuntimeIdentity(t *testing.T) {
	plan, _, target, split := testPlan(t)
	current, next := plan.CatalogGeneration()
	if current != 19 || next != 20 {
		t.Fatalf("catalog generations = %d/%d", current, next)
	}
	if plan.OperationID() == (OperationID{}) {
		t.Fatal("zero operation identity")
	}
	got, ok := plan.Target(1)
	if !ok || got.SQL.Binding != target.SQL.Binding || got.Endpoint != "node-b" {
		t.Fatalf("target = %+v, %v", got, ok)
	}

	bad := target
	bad.Authority.RouteGeneration++
	if _, err := NewPlan(
		plan.sourceSnapshotForTest(t), split, plan.partitioner, []ChildTarget{bad},
	); !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("route-generation mismatch error = %v", err)
	}
	bad = target
	bad.Endpoint = "node-c"
	if _, err := NewPlan(
		plan.sourceSnapshotForTest(t), split, plan.partitioner, []ChildTarget{bad},
	); !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("leader mismatch error = %v", err)
	}

	// The plan deeply owns every replica-local string and identity.
	ownedTarget := cloneChildTarget(target)
	ownedPlan, err := NewPlan(
		plan.sourceSnapshotForTest(t), split, plan.partitioner, []ChildTarget{ownedTarget},
	)
	if err != nil {
		t.Fatal(err)
	}
	ownedTarget.Replicas[0].SQL.UserTable = "mutated"
	ownedTarget.Replicas[0].Apply.Storage = "mutated"
	retained, ok := ownedPlan.Target(1)
	if !ok || retained.Replicas[0].SQL.UserTable == "mutated" ||
		retained.Replicas[0].Apply.Storage == "mutated" {
		t.Fatalf("plan retained caller-owned replica identity: %+v", retained.Replicas[0])
	}

	// Caller-owned plan headers cannot relabel the accepted operation afterward.
	split.Source.Shard = "mutated"
	split.ChildCount = 3
	split.RetainedChild = 2
	state := testSourceState(plan)
	action, err := Reconcile(plan, Observation{
		Catalog: plan.sourceSnapshotForTest(t), SourceState: state,
		SourceStatus: testLeaderStatus(state),
	})
	if err != nil || action.Kind != ActionStartCapture {
		t.Fatalf("action after caller plan mutation = %+v, err = %v", action, err)
	}
}

func TestRecoverPlanCollapsesPublishedChildrenWithoutOldCatalog(t *testing.T) {
	plan, _, target, split := testPlan(t)
	published := plan.targetSnapshotForTest(t)
	recovered, err := RecoverPlan(
		published, 19, split, plan.partitioner, []ChildTarget{target},
	)
	if err != nil {
		t.Fatal(err)
	}
	if current, next := recovered.CatalogGeneration(); current != 19 || next != 20 {
		t.Fatalf("recovered generations = %d/%d", current, next)
	}
	if !recovered.sourceManifest.Equal(plan.sourceManifest) ||
		!recovered.targetManifest.Equal(plan.targetManifest) {
		t.Fatal("recovered manifests differ from original plan")
	}
	if recovered.OperationID() != plan.OperationID() {
		t.Fatalf("recovered operation id=%x want=%x", recovered.OperationID(), plan.OperationID())
	}
	if _, err := RecoverPlan(
		published, 18, split, plan.partitioner, []ChildTarget{target},
	); !errors.Is(err, ErrTopologyConflict) {
		t.Fatalf("wrong source generation error = %v", err)
	}
	wrong := plan.sourceSnapshotForTestAt(t, 20)
	if _, err := RecoverPlan(
		wrong, 19, split, plan.partitioner, []ChildTarget{target},
	); !errors.Is(err, ErrTopologyConflict) {
		t.Fatalf("wrong published manifest error = %v", err)
	}
}

func TestRecoverPlanRecognizesComposedSameDistributionPublication(t *testing.T) {
	middle := distribution.KeyspacePoint{0x80}
	current, err := distribution.NewManifest("orders", 11, []distribution.Shard{
		{
			ID: "left", AllocationGeneration: 7,
			Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Point: middle}},
			Leaders: []distribution.EndpointID{"node-a"}, Epoch: 5,
		},
		{
			ID: "right", AllocationGeneration: 8,
			Range:   distribution.KeyRange{Start: middle, End: distribution.KeyspaceEnd{Max: true}},
			Leaders: []distribution.EndpointID{"node-b"}, Epoch: 7,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	leftSplit := controllerComposedSplitPlan(
		t, current, "left", 7,
		distribution.KeyRange{End: distribution.KeyspaceEnd{Point: middle}},
		5, distribution.KeyspacePoint{0x40}, "left-tail", 9, "node-c",
	)
	rightSplit := controllerComposedSplitPlan(
		t, current, "right", 8,
		distribution.KeyRange{Start: middle, End: distribution.KeyspaceEnd{Max: true}},
		7, distribution.KeyspacePoint{0xc0}, "right-tail", 10, "node-d",
	)
	left, err := rangesplit.NewPartitioner(
		leftSplit, "docs", []string{"/tenant"}, distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	right, err := rangesplit.NewPartitioner(
		rightSplit, "docs", []string{"/tenant"}, distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	combined, err := rangesplit.ComposeManifestTransitions(
		current, []rangesplit.ManifestTransition{
			{Partitioner: right, Target: rightSplit.Manifest()},
			{Partitioner: left, Target: leftSplit.Manifest()},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	published, err := gateway.NewSnapshot(
		distribution.ClusterConfig{
			Distributions: []distribution.DistributionSpec{{
				Name: "orders", Arity: 1, MapperVersion: distribution.NativeMapperVersion,
			}},
			Placements: []distribution.TablePlacement{{
				Table: "docs", Distribution: "orders", Columns: []string{"/tenant"},
			}},
			Manifests: []*distribution.Manifest{combined},
		},
		map[distribution.EndpointID]string{
			"node-a": "127.0.0.1:1", "node-b": "127.0.0.1:2",
			"node-c": "127.0.0.1:3", "node-d": "127.0.0.1:4",
		},
		20,
	)
	if err != nil {
		t.Fatal(err)
	}
	target := testChildTarget(t, leftSplit, left)
	recovered, err := RecoverPlan(
		published, 19, leftSplit, left, []ChildTarget{target},
	)
	if err != nil {
		t.Fatal(err)
	}
	if stage, err := recovered.catalogStage(published); err != nil || stage != catalogTarget {
		t.Fatalf("composed published stage = %v, err = %v", stage, err)
	}
}

func controllerComposedSplitPlan(
	t testing.TB,
	current *distribution.Manifest,
	shard distribution.ShardID,
	allocation distribution.ShardAllocationGeneration,
	range_ distribution.KeyRange,
	epoch distribution.OwnershipEpoch,
	boundary distribution.KeyspacePoint,
	destination distribution.ShardID,
	destinationAllocation distribution.ShardAllocationGeneration,
	destinationLeader distribution.EndpointID,
) *autosplit.SplitPlan {
	t.Helper()
	source := autosplit.SourceIdentity{
		Distribution: current.Distribution(), Shard: shard,
		AllocationGeneration: allocation, Range: range_,
		BucketBits:     distribution.DefaultVirtualBucketBits,
		RoutingVersion: current.Version(), OwnershipEpoch: epoch,
	}
	plan, err := autosplit.PlanSplit(current, autosplit.SplitRequest{
		Recommendation: autosplit.Recommendation{
			Source: source, WindowSequence: 1, Kind: autosplit.RecommendationBinarySplit,
			Boundaries: [2]distribution.KeyspacePoint{boundary}, BoundaryCount: 1,
			CandidateBin: 32, CurrentPressurePPM: 950_000,
			PredictedPressurePPM: 700_000, BenefitPPM: 250_000,
		},
		RetainChild: 0, NextRoutingVersion: current.Version() + 1,
		AllocationHighWater: destinationAllocation - 1,
		Destinations: []autosplit.Destination{{
			Shard: destination, AllocationGeneration: destinationAllocation,
			Leaders: []distribution.EndpointID{destinationLeader}, OwnershipEpoch: 1,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestControlLoopStateIsFixedAndWarmAwaitAllocatesZero(t *testing.T) {
	if size := unsafe.Sizeof(Plan{}); size > 4096 {
		t.Fatalf("Plan size = %d B, want <= 4096 B", size)
	}
	if size := unsafe.Sizeof(Action{}); size > 16 {
		t.Fatalf("Action size = %d B, want <= 16 B", size)
	}
	plan, snapshot, _, _ := testPlan(t)
	observed := Observation{Catalog: snapshot, SourceState: testSourceState(plan)}
	if allocations := testing.AllocsPerRun(1_000, func() {
		action, err := Reconcile(plan, observed)
		if err != nil || action.Kind != ActionAwaitSourceLeader {
			panic("unexpected reconcile result")
		}
	}); allocations != 0 {
		t.Fatalf("warm await allocations = %v, want 0", allocations)
	}
}

func TestReconcileStartsOnlyFromExactSourceLeader(t *testing.T) {
	plan, snapshot, _, _ := testPlan(t)
	state := testSourceState(plan)
	action, err := Reconcile(plan, Observation{Catalog: snapshot, SourceState: state})
	if err != nil || action.Kind != ActionAwaitSourceLeader {
		t.Fatalf("without leader action = %+v, err = %v", action, err)
	}
	status := testLeaderStatus(state)
	action, err = Reconcile(plan, Observation{
		Catalog: snapshot, SourceState: state, SourceStatus: status,
	})
	if err != nil || action.Kind != ActionStartCapture {
		t.Fatalf("leader action = %+v, err = %v", action, err)
	}

	stale := state
	stale.Binding.RouteGeneration--
	if _, err := Reconcile(plan, Observation{
		Catalog: snapshot, SourceState: stale, SourceStatus: status,
	}); !errors.Is(err, ErrTopologyConflict) {
		t.Fatalf("stale source error = %v", err)
	}
}

func TestReconcileBuildsThenStagesOnePassArtifacts(t *testing.T) {
	plan, snapshot, _, _ := testPlan(t)
	state := testSourceState(plan)
	database, err := durable.OpenDatabase(t.TempDir(), durable.DatabaseOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	collection, err := database.CreateCollection("capture", durable.Options{OpaqueValues: true})
	if err != nil {
		t.Fatal(err)
	}
	capture, err := rangesplit.NewSourceCapture(plan.partitioner, "capture", collection)
	if err != nil {
		t.Fatal(err)
	}
	if err := capture.Begin(state, func(key, value []byte) error {
		return collection.Update(func(batch *durable.WriteBatch) error {
			return batch.Put(key, value)
		})
	}); err != nil {
		t.Fatalf("capture begin = %v", err)
	}
	observed := Observation{
		Catalog: snapshot, SourceState: state, SourceStatus: testLeaderStatus(state),
		Capture: capture,
	}
	action, err := Reconcile(plan, observed)
	if err != nil || action.Kind != ActionBuildArtifacts {
		t.Fatalf("artifact action = %+v, err = %v", action, err)
	}
	set := testArtifactSet(t, plan, state)
	observed.Artifacts = &set
	action, err = Reconcile(plan, observed)
	if err != nil || action.Kind != ActionStageChild || action.Child != 1 {
		t.Fatalf("stage action = %+v, err = %v", action, err)
	}

	set.Children[1].Descriptor.AllocationGeneration++
	if _, err := Reconcile(plan, observed); !errors.Is(err, ErrTopologyConflict) {
		t.Fatalf("changed artifact allocation error = %v", err)
	}
	set = testArtifactSet(t, plan, state)
	set.Partition.RouteGeneration++
	set.Children[1].Source.RouteGeneration++
	observed.Artifacts = &set
	if _, err := Reconcile(plan, observed); !errors.Is(err, ErrTopologyConflict) {
		t.Fatalf("changed artifact route generation error = %v", err)
	}
}

func TestChildActionsRequireMonotonicExactEvidence(t *testing.T) {
	plan, _, target, _ := testPlanWithChildLeaders(t, []distribution.EndpointID{
		"node-b", "node-c", "node-d",
	})
	certificate := rangesplit.CutoverCertificate{}
	observed := Observation{}
	action, ok, err := plan.childAction(observed, certificate)
	if err != nil || !ok || action.Kind != ActionActivateChild || action.Child != 1 {
		t.Fatalf("activate action = %+v, %v, %v", action, ok, err)
	}

	identity := sqldriver.ReplicatedApplyIdentity{
		Storage: "apply", ValidationDigest: sha256.Sum256([]byte("apply")),
		MaxSessions: 8, RetryWindow: 8,
	}
	child := &ChildObservation{
		Child: 1, Phase: ChildPhaseActivated, ApplyIdentity: identity,
		ApplyProfile: sqldriver.ReplicatedApplyCapacityProfile{
			Binding: target.SQL.Binding, Initialized: true,
			RelationManifestDigest: target.RelationManifestDigest,
			MaxSessions:            8, RetryWindow: 8,
		},
	}
	observed.Children[1] = child
	action, ok, err = plan.childAction(observed, certificate)
	if err != nil || !ok || action.Kind != ActionCreateChildWAL {
		t.Fatalf("WAL action = %+v, %v, %v", action, ok, err)
	}

	child.Phase = ChildPhaseWALCreated
	child.WALBinding = target.SQL.Binding
	action, ok, err = plan.childAction(observed, certificate)
	if err != nil || !ok || action.Kind != ActionAdoptChildRuntime {
		t.Fatalf("adopt action = %+v, %v, %v", action, ok, err)
	}

	child.Phase = ChildPhaseRuntimeAdopted
	child.RuntimeIdentity = testRuntimeIdentity(target)
	child.ReadyReplicas = testReadyServingStates(target, child.ApplyProfile, 1, 1)
	action, ok, err = plan.childAction(observed, certificate)
	if err != nil || !ok || action.Kind != ActionAwaitChildReady {
		t.Fatalf("minority ready action = %+v, %v, %v", action, ok, err)
	}
	child.ReadyReplicas = testReadyServingStates(target, child.ApplyProfile, 3, 1)
	for index := range child.ReadyReplicas {
		child.ReadyReplicas[index].Status.LeaderID = target.WAL.MemberID + 1
		if child.ReadyReplicas[index].Identity.MemberID == target.WAL.MemberID {
			child.ReadyReplicas[index].Status.RaftState = raft.StateFollower
		}
		if child.ReadyReplicas[index].Identity.MemberID == target.WAL.MemberID+1 {
			child.ReadyReplicas[index].Status.RaftState = raft.StateLeader
		}
	}
	action, ok, err = plan.childAction(observed, certificate)
	if err != nil || !ok || action.Kind != ActionAwaitChildReady {
		t.Fatalf("ready action = %+v, %v, %v", action, ok, err)
	}
	child.ReadyReplicas = testReadyServingStates(target, child.ApplyProfile, 2, 1)
	if action, ok, err = plan.childAction(observed, certificate); err != nil || ok {
		t.Fatalf("quorum-ready child action = %+v, %v, %v", action, ok, err)
	}

	child.ReadyReplicas = testReadyServingStates(target, child.ApplyProfile, 2, 1)
	child.ReadyReplicas[1].Identity.MemberID = child.ReadyReplicas[0].Identity.MemberID
	if _, _, err = plan.childAction(observed, certificate); !errors.Is(err, ErrTopologyConflict) {
		t.Fatalf("duplicate quorum member error = %v", err)
	}

	child.WALBinding.Authority.RouteGeneration++
	if _, _, err := plan.childAction(observed, certificate); !errors.Is(err, ErrTopologyConflict) {
		t.Fatalf("changed WAL binding error = %v", err)
	}
}

func TestChildActionsRejectSkippedPhaseAndPrematureEvidence(t *testing.T) {
	plan, _, target, _ := testPlan(t)
	certificate := rangesplit.CutoverCertificate{}
	identity := sqldriver.ReplicatedApplyIdentity{
		Storage: "apply", ValidationDigest: sha256.Sum256([]byte("apply")),
		MaxSessions: 8, RetryWindow: 8,
	}
	child := &ChildObservation{
		Child: 1, Phase: ChildPhaseActivated, ApplyIdentity: identity,
		ApplyProfile: sqldriver.ReplicatedApplyCapacityProfile{
			Binding: target.SQL.Binding, Initialized: true, MaxSessions: 8, RetryWindow: 8,
		},
		WALBinding: target.SQL.Binding,
	}
	observed := Observation{}
	observed.Children[1] = child
	if _, _, err := plan.childAction(observed, certificate); !errors.Is(err, ErrTopologyConflict) {
		t.Fatalf("premature WAL evidence error = %v", err)
	}

	child.WALBinding = sqldriver.ReplicatedShardStoreBinding{}
	child.Phase = ChildPhase(4)
	if _, _, err := plan.childAction(observed, certificate); !errors.Is(err, ErrTopologyConflict) {
		t.Fatalf("unknown phase error = %v", err)
	}

	child.Phase = ChildPhaseWALCreated
	child.WALBinding = target.SQL.Binding
	child.RuntimeIdentity = testRuntimeIdentity(target)
	if _, _, err := plan.childAction(observed, certificate); !errors.Is(err, ErrTopologyConflict) {
		t.Fatalf("premature runtime evidence error = %v", err)
	}
}

func testPlan(t testing.TB) (*Plan, *gateway.Snapshot, ChildTarget, *autosplit.SplitPlan) {
	return testPlanWithChildLeaders(t, []distribution.EndpointID{"node-b"})
}

func testPlanWithChildLeaders(
	t testing.TB,
	childLeaders []distribution.EndpointID,
) (*Plan, *gateway.Snapshot, ChildTarget, *autosplit.SplitPlan) {
	t.Helper()
	manifest, err := distribution.NewManifest("orders", 11, []distribution.Shard{{
		ID: "source", AllocationGeneration: 7,
		Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: []distribution.EndpointID{"node-a"}, Epoch: 5,
	}})
	if err != nil {
		t.Fatal(err)
	}
	config := distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{
			Name: "orders", Arity: 1, MapperVersion: distribution.NativeMapperVersion,
		}},
		Placements: []distribution.TablePlacement{{
			Table: "docs", Distribution: "orders", Columns: []string{"/tenant"},
		}},
		Manifests: []*distribution.Manifest{manifest},
	}
	snapshot, err := gateway.NewSnapshot(
		config,
		map[distribution.EndpointID]string{
			"node-a": "127.0.0.1:1", "node-b": "127.0.0.1:2",
			"node-c": "127.0.0.1:3", "node-d": "127.0.0.1:4",
		},
		19,
	)
	if err != nil {
		t.Fatal(err)
	}
	var boundary distribution.KeyspacePoint
	boundary[0] = 0x80
	source := autosplit.SourceIdentity{
		Distribution: "orders", Shard: "source", AllocationGeneration: 7,
		Range:          distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		BucketBits:     distribution.DefaultVirtualBucketBits,
		RoutingVersion: 11, OwnershipEpoch: 5,
	}
	split, err := autosplit.PlanSplit(manifest, autosplit.SplitRequest{
		Recommendation: autosplit.Recommendation{
			Source: source, Kind: autosplit.RecommendationBinarySplit,
			Boundaries: [2]distribution.KeyspacePoint{boundary}, BoundaryCount: 1,
			CandidateBin: 32, BenefitPPM: 1,
		},
		RetainChild: 0, NextRoutingVersion: 12, AllocationHighWater: 7,
		Destinations: []autosplit.Destination{{
			Shard: "right", AllocationGeneration: 8,
			Leaders: childLeaders, OwnershipEpoch: 1,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	partitioner, err := rangesplit.NewPartitioner(
		split, "docs", []string{"/tenant"}, distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	target := testChildTarget(t, split, partitioner)
	plan, err := NewPlan(snapshot, split, partitioner, []ChildTarget{target})
	if err != nil {
		t.Fatal(err)
	}
	return plan, snapshot, target, split
}

func testChildTarget(
	t testing.TB,
	split *autosplit.SplitPlan,
	partitioner *rangesplit.Partitioner,
) ChildTarget {
	t.Helper()
	child, _ := split.Child(1)
	identity := raftstore.Identity{
		ClusterID: testID(1), ClusterIncarnation: testID(2),
		Distribution: string(split.Source.Distribution), Shard: string(child.Shard),
		AllocationGeneration: uint64(child.AllocationGeneration),
		ShardIncarnation:     testID(3), GroupID: testID(4), MemberID: 1, StoreID: testID(5),
	}
	authority := sqldriver.ReplicatedAuthorityProfile{
		ActivePolicyGeneration: 1, ProtectionEpoch: 1,
		OwnershipEpoch: uint64(child.OwnershipEpoch), SchemaGeneration: 1,
		RoutingVersion: uint64(split.Manifest().Version()), RouteGeneration: 20,
	}
	replicas := make([]ChildReplicaTarget, len(child.Leaders))
	root := t.TempDir()
	for index, leader := range child.Leaders {
		store := testID(byte(30 + index))
		if index == 0 {
			store = identity.StoreID
		}
		replicaWAL := identity
		replicaWAL.MemberID = uint64(index + 1)
		replicaWAL.StoreID = store
		replicaBinding, bindErr := raftmember.BindingForNewWAL(replicaWAL, 1, authority)
		if bindErr != nil {
			t.Fatal(bindErr)
		}
		replicaSQL := sqldriver.ReplicatedShardStoreIdentity{
			Binding: replicaBinding, LogID: testID(byte(60 + index)),
			UserTable:              partitioner.CollectionName(),
			RelationManifestDigest: sha256.Sum256([]byte("child-local-relations-" + strconv.Itoa(index+1))),
		}
		replicaRoot := filepath.Join(root, "replica-"+strconv.Itoa(index+1))
		replicas[index] = ChildReplicaTarget{
			Member: uint64(index + 1), Node: testID(byte(20 + index)), StoreID: store,
			NodeIncarnation: uint64(index + 1),
			Endpoint:        distribution.EndpointID(string(leader) + "-peer"),
			NativeEndpoint:  leader,
			ControlEndpoint: distribution.EndpointID(string(leader) + "-control"),
			WAL:             replicaWAL,
			RuntimeRoot:     replicaRoot,
			WALPath:         filepath.Join(replicaRoot, "child.wal"),
			SQLPath:         filepath.Join(replicaRoot, "child.vdb"),
			SQL:             replicaSQL,
			Apply: sqldriver.ReplicatedApplyIdentity{
				Storage:          "apply-" + strconv.Itoa(index+1),
				CaptureStorage:   "capture-" + strconv.Itoa(index+1),
				ValidationDigest: sha256.Sum256([]byte("apply-validation-" + strconv.Itoa(index+1))),
				MaxSessions:      1, RetryWindow: 1,
				Placement: sqldriver.ReplicatedPlacementProfile{ShardKey: "/id", Range: child.Range},
			},
			CertificateDigest: sha256.Sum256([]byte("replica-certificate-" + strconv.Itoa(index+1))),
		}
	}
	return ChildTarget{
		Child: 1, Endpoint: child.Leaders[0], WAL: identity,
		Replicas:               replicas,
		ReplicaSetVersion:      1,
		RelationManifestDigest: sha256.Sum256([]byte("portable-child-relations")),
		TopologyRecoveryEpoch:  1, Authority: authority,
		SQL: replicas[0].SQL.Clone(),
	}
}

func testSourceState(plan *Plan) replicatedstate.State {
	return replicatedstate.State{
		Binding: replicatedstate.Binding{
			ClusterID:             replication.ID128(testID(1)),
			ClusterIncarnation:    replication.ID128(testID(2)),
			TopologyRecoveryEpoch: 1,
			Distribution:          string(plan.source.Distribution), Shard: string(plan.source.Shard),
			AllocationGeneration: uint64(plan.source.AllocationGeneration),
			ShardIncarnation:     replication.ID128(testID(7)), GroupID: replication.ID128(testID(8)),
			ActivePolicyGeneration: 1, ProtectionEpoch: 1,
			OwnershipEpoch: uint64(plan.source.OwnershipEpoch), SchemaGeneration: 1,
			RoutingVersion: uint64(plan.source.RoutingVersion), RouteGeneration: plan.current,
			OwnedRange: plan.source.Range,
		},
		Applied: 41, LastTerm: 7, LastKind: replicatedstate.RecordNormal,
		LastEntryType: raftpb.EntryNormal, LastEntryDigest: sha256.Sum256([]byte("entry")),
		DataChainDigest:     sha256.Sum256([]byte("data-chain")),
		ApplyContractDigest: sha256.Sum256([]byte("apply-contract")),
		SnapshotBaseDigest:  sha256.Sum256([]byte("base")),
		ConfState:           &raftpb.ConfState{Voters: []uint64{1, 2, 3}},
	}
}

func testLeaderStatus(state replicatedstate.State) raftmember.RuntimeStatus {
	return raftmember.RuntimeStatus{
		MemberID: 1, LeaderID: 1, Term: state.LastTerm,
		Commit: state.Applied, Applied: state.Applied, RaftState: raft.StateLeader,
	}
}

func testRuntimeIdentity(target ChildTarget) raftmember.RuntimeIdentity {
	return raftmember.RuntimeIdentity{
		Group: raftmember.GroupKey{
			ClusterID: target.WAL.ClusterID, ClusterIncarnation: target.WAL.ClusterIncarnation,
			TopologyRecoveryEpoch: target.TopologyRecoveryEpoch,
			ShardIncarnation:      target.WAL.ShardIncarnation, GroupID: target.WAL.GroupID,
		},
		Distribution: target.WAL.Distribution, Shard: target.WAL.Shard,
		AllocationGeneration: target.WAL.AllocationGeneration,
		MemberID:             target.WAL.MemberID, StoreID: target.WAL.StoreID, NodeIncarnation: 1,
	}
}

func testReadyServingStates(
	target ChildTarget,
	profile sqldriver.ReplicatedApplyCapacityProfile,
	count int,
	applied uint64,
) []raftservice.ServingState {
	states := make([]raftservice.ServingState, count)
	for index := range states {
		identity := testRuntimeIdentity(target)
		identity.MemberID = uint64(index + 1)
		identity.StoreID[0] = byte(index + 1)
		identity.NodeIncarnation = uint64(index + 1)
		state := raftservice.ServingState{
			Identity: identity,
			Command: raftservice.CommandFence{
				ReplicaSetVersion: 1, ActivePolicyGeneration: target.Authority.ActivePolicyGeneration,
				ProtectionEpoch:        target.Authority.ProtectionEpoch,
				OwnershipEpoch:         target.Authority.OwnershipEpoch,
				SchemaGeneration:       target.Authority.SchemaGeneration,
				RelationManifestDigest: profile.RelationManifestDigest,
				RoutingVersion:         target.Authority.RoutingVersion,
				RouteGeneration:        target.Authority.RouteGeneration,
			},
			Status: raftmember.RuntimeStatus{
				MemberID: identity.MemberID, LeaderID: target.WAL.MemberID,
				Term: 1, Commit: applied, Applied: applied, RaftState: raft.StateFollower,
			},
		}
		if identity.MemberID == target.WAL.MemberID {
			state.Status.RaftState = raft.StateLeader
		}
		states[index] = state
	}
	return states
}

func testArtifactSet(
	t testing.TB,
	plan *Plan,
	state replicatedstate.State,
) rangesplit.ChildArtifactSet {
	t.Helper()
	program, err := distribution.CompileDocumentPointProgram(
		[]string{"/tenant"}, distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	cut := rangesplit.ChildArtifactSourceCut{
		DataChainDigest: state.DataChainDigest, BaseDigest: state.SnapshotBaseDigest,
		EntryDigest: state.LastEntryDigest, Applied: state.Applied, Term: state.LastTerm,
		RouteGeneration: state.Binding.RouteGeneration,
	}
	set := rangesplit.ChildArtifactSet{Partition: rangesplit.PartitionStats{
		PlanDigest: plan.partitioner.Digest(), SourceDigest: cut.DataChainDigest,
		SourceBase: cut.BaseDigest, SourceEntry: cut.EntryDigest,
		SourceApplied: cut.Applied, SourceTerm: cut.Term,
		RouteGeneration: cut.RouteGeneration,
	}}
	child := plan.children[1]
	set.Children[1] = rangesplit.ChildArtifactManifest{
		Present: true, Child: 1, PlanDigest: plan.partitioner.Digest(),
		PlacementDigest: program.Digest(), Source: cut,
		TargetRoutingVersion: plan.targetManifest.Version(),
		Descriptor: rangesplit.ChildArtifactDescriptor{
			Range: child.Range, Shard: child.Shard,
			AllocationGeneration: child.AllocationGeneration,
			OwnershipEpoch:       child.OwnershipEpoch, LeaderCount: plan.leaderCounts[1],
		},
		EncodedBytes: 1, HeaderDigest: sha256.Sum256([]byte("header")),
		LastChunkDigest: sha256.Sum256([]byte("chunk")),
		Digest:          sha256.Sum256([]byte("artifact")),
	}
	return set
}

func testID(seed byte) [16]byte {
	var id [16]byte
	for index := range id {
		id[index] = seed + byte(index)
	}
	return id
}

func (p *Plan) sourceSnapshotForTest(t testing.TB) *gateway.Snapshot {
	return p.sourceSnapshotForTestAt(t, p.current)
}

func (p *Plan) sourceSnapshotForTestAt(t testing.TB, generation uint64) *gateway.Snapshot {
	t.Helper()
	config := distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{
			Name: "orders", Arity: 1, MapperVersion: distribution.NativeMapperVersion,
		}},
		Placements: []distribution.TablePlacement{{
			Table: "docs", Distribution: "orders", Columns: []string{"/tenant"},
		}},
		Manifests: []*distribution.Manifest{p.sourceManifest},
	}
	snapshot, err := gateway.NewSnapshot(
		config,
		map[distribution.EndpointID]string{
			"node-a": "127.0.0.1:1", "node-b": "127.0.0.1:2",
		},
		generation,
	)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func (p *Plan) targetSnapshotForTest(t testing.TB) *gateway.Snapshot {
	t.Helper()
	config := distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{
			Name: "orders", Arity: 1, MapperVersion: distribution.NativeMapperVersion,
		}},
		Placements: []distribution.TablePlacement{{
			Table: "docs", Distribution: "orders", Columns: []string{"/tenant"},
		}},
		Manifests: []*distribution.Manifest{p.targetManifest},
	}
	snapshot, err := gateway.NewSnapshot(
		config,
		map[distribution.EndpointID]string{
			"node-a": "127.0.0.1:1", "node-b": "127.0.0.1:2",
		},
		p.next,
	)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
