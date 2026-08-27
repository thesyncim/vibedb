package hotshard

import (
	"context"
	"errors"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/topologyscheduler"
)

type admissionSink struct {
	calls int
	fail  bool
	last  Admission
}

func (sink *admissionSink) SubmitHotShardAdmission(_ context.Context, admission Admission) error {
	sink.calls++
	sink.last = admission
	if sink.fail {
		return errors.New("outcome unknown")
	}
	return nil
}

func TestControllerQualifiesHotShardAndRetriesByteIdenticalAdmission(t *testing.T) {
	catalog, source, nodes := hotCatalog(t)
	policy := hotPolicy()
	controller, err := New(policy)
	if err != nil {
		t.Fatal(err)
	}
	sink := &admissionSink{}
	first := hotView(source, nodes, 1)
	admission, err := controller.Process(context.Background(), catalog, first, sink)
	if err != nil || !admission.Empty() || sink.calls != 0 {
		t.Fatalf("first window admission=%+v calls=%d err=%v", admission, sink.calls, err)
	}
	second := hotView(source, nodes, 2)
	sink.fail = true
	failed, err := controller.Process(context.Background(), catalog, second, sink)
	if err == nil || failed.SplitCount != 1 || failed.MoveCount != 0 || sink.calls != 1 {
		t.Fatalf("failed admission=%+v calls=%d err=%v", failed, sink.calls, err)
	}
	sink.fail = false
	retried, err := controller.Process(context.Background(), catalog, second, sink)
	if err != nil || retried.ID != failed.ID || retried != sink.last || sink.calls != 2 {
		t.Fatalf("retry admission=%+v calls=%d err=%v", retried, sink.calls, err)
	}
}

func TestControllerLogicalCooldownPreventsOscillationAndSurvivesRestart(t *testing.T) {
	catalog, source, nodes := hotCatalog(t)
	policy := hotPolicy()
	controller, _ := New(policy)
	sink := &admissionSink{}
	if _, err := controller.Process(context.Background(), catalog, hotView(source, nodes, 1), sink); err != nil {
		t.Fatal(err)
	}
	restarted, err := Restore(policy, controller.Checkpoint())
	if err != nil {
		t.Fatal(err)
	}
	qualified, err := restarted.Process(context.Background(), catalog, hotView(source, nodes, 2), sink)
	if err != nil || qualified.SplitCount != 1 {
		t.Fatalf("post-restart qualification=%+v err=%v", qualified, err)
	}
	for revision := uint64(3); revision <= 6; revision++ {
		admission, processErr := restarted.Process(context.Background(), catalog, hotView(source, nodes, revision), sink)
		if processErr != nil || !admission.Empty() {
			t.Fatalf("cooldown window %d admission=%+v err=%v", revision, admission, processErr)
		}
	}
	if sink.calls != 1 {
		t.Fatalf("oscillation admitted %d operations, want 1", sink.calls)
	}
}

func TestControllerClockSkewCannotAdvanceReplicatedEvidence(t *testing.T) {
	catalog, source, nodes := hotCatalog(t)
	controller, _ := New(hotPolicy())
	sink := &admissionSink{}
	view := hotView(source, nodes, 1)
	if _, err := controller.Process(context.Background(), catalog, view, sink); err != nil {
		t.Fatal(err)
	}
	before := controller.Checkpoint()
	// Replaying the same authority revision models arbitrary wall-clock jumps:
	// without a new replicated revision, neither evidence nor cooldown advances.
	if _, err := controller.Process(context.Background(), catalog, view, sink); !errors.Is(err, ErrInvalidPressureCut) {
		t.Fatalf("replayed revision error=%v", err)
	}
	if after := controller.Checkpoint(); after != before {
		t.Fatal("stale authority revision advanced state")
	}
}

func TestControllerFeedsReplicaMoveScheduler(t *testing.T) {
	catalog, source, nodes := hotCatalog(t)
	policy := hotPolicy()
	policy.Tracker.WindowCount = 8
	policy.Tracker.RequiredWindows = 8
	controller, _ := New(policy)
	sink := &admissionSink{}
	view := hotView(source, nodes, 1)
	view.Reports[0].Demand[autosplit.ResourceLiveBytes] = 300
	admission, err := controller.Process(context.Background(), catalog, view, sink)
	if err != nil || admission.SplitCount != 0 || admission.MoveCount != 1 ||
		admission.Moves[0].Selection.SourceEndpoint != "source" ||
		admission.Moves[0].Selection.TargetEndpoint != "target" {
		t.Fatalf("move admission=%+v err=%v", admission, err)
	}
}

func TestControllerSplitOnlyPolicyDoesNotInventReplicaAuthority(t *testing.T) {
	catalog, source, nodes := hotCatalog(t)
	policy := hotPolicy()
	policy.DisableMoves = true
	policy.Tracker.WindowCount = 8
	policy.Tracker.RequiredWindows = 8
	controller, err := New(policy)
	if err != nil {
		t.Fatal(err)
	}
	sink := &admissionSink{}
	view := hotView(source, nodes, 1)
	view.Reports[0].Demand[autosplit.ResourceLiveBytes] = 300
	admission, err := controller.Process(context.Background(), catalog, view, sink)
	if err != nil || !admission.Empty() || sink.calls != 0 {
		t.Fatalf("split-only admission=%+v calls=%d err=%v", admission, sink.calls, err)
	}
}

func TestControllerDoesNotComposeSplitAndMoveForSameAllocation(t *testing.T) {
	catalog, source, nodes := hotCatalog(t)
	controller, _ := New(hotPolicy())
	sink := &admissionSink{}
	first := hotView(source, nodes, 1)
	first.Reports[0].Demand[autosplit.ResourceLiveBytes] = 300
	if _, err := controller.Process(context.Background(), catalog, first, sink); err != nil {
		t.Fatal(err)
	}
	second := hotView(source, nodes, 2)
	second.Reports[0].Demand[autosplit.ResourceLiveBytes] = 300
	admission, err := controller.Process(context.Background(), catalog, second, sink)
	if err != nil || admission.SplitCount != 1 || admission.MoveCount != 0 {
		t.Fatalf("composed topology admission=%+v err=%v", admission, err)
	}
}

func TestControllerRetainedStateIsFixedAndBounded(t *testing.T) {
	// The controller owns all tracker, undo, candidate, and scheduler scratch;
	// request or tenant cardinality cannot grow it after construction.
	if size := unsafe.Sizeof(Controller{}); size > 4<<20 {
		t.Fatalf("controller retained size=%d, want <=4 MiB", size)
	} else {
		t.Logf("controller retained size=%d bytes for %d group reports", size, MaxReports)
	}
}

func hotPolicy() Policy {
	policy := DefaultPolicy()
	policy.Tracker = autosplit.TrackerPolicy{WindowCount: 2, RequiredWindows: 2,
		CooldownWindows: 4, MaxBoundaryDrift: 1, TriggerPressurePPM: 900_000}
	policy.Split.MinBenefitPPM = 1
	policy.Move.MinProjectedReliefPPM = 1
	return policy
}

func hotView(source autosplit.SourceIdentity, nodes []topologyscheduler.NodeCapacity, revision uint64) View {
	boundary := distribution.KeyspacePoint{0x80}
	return View{CatalogGeneration: 10, AuthorityRevision: revision, Nodes: nodes,
		Reports: []Report{{Group: hotGroup(1), MigrationBytes: 100,
			Recommendation: autosplit.Recommendation{Source: source, WindowSequence: revision,
				Kind: autosplit.RecommendationBinarySplit, BoundaryCount: 1,
				Boundaries: [2]distribution.KeyspacePoint{boundary}, CandidateBin: 32,
				CurrentPressurePPM: 950_000, PredictedPressurePPM: 700_000, BenefitPPM: 250_000}}}}
}

func hotCatalog(t testing.TB) (*gateway.Snapshot, autosplit.SourceIdentity, []topologyscheduler.NodeCapacity) {
	t.Helper()
	manifest, err := distribution.NewManifest("data", 7, []distribution.Shard{{
		ID: "shard", AllocationGeneration: 3,
		Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: []distribution.EndpointID{"source", "follower"}, Epoch: 5,
	}})
	if err != nil {
		t.Fatal(err)
	}
	endpoints := map[distribution.EndpointID]string{"source": "source:1", "follower": "follower:1", "target": "target:1"}
	catalog, err := gateway.NewSnapshot(distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: "data", Arity: 1, MapperVersion: distribution.NativeMapperVersion}},
		Placements:    []distribution.TablePlacement{{Table: "docs", Distribution: "data", Columns: []string{"/id"}}},
		Manifests:     []*distribution.Manifest{manifest},
	}, endpoints, 10)
	if err != nil {
		t.Fatal(err)
	}
	source := autosplit.SourceIdentity{Distribution: "data", Shard: "shard", AllocationGeneration: 3,
		Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}, BucketBits: distribution.DefaultVirtualBucketBits,
		RoutingVersion: 7, OwnershipEpoch: 5}
	node := func(endpoint distribution.EndpointID, domain uint32, used uint64) topologyscheduler.NodeCapacity {
		return topologyscheduler.NodeCapacity{CatalogGeneration: 10, Endpoint: endpoint, FailureDomain: domain,
			Flags:             topologyscheduler.NodePlacementReady,
			Capacity:          autosplit.CapacityVector{autosplit.ResourceLiveBytes: 1000},
			Used:              autosplit.CapacityVector{autosplit.ResourceLiveBytes: used},
			MigrationCapacity: 1000, MaxReceives: 4}
	}
	return catalog, source, []topologyscheduler.NodeCapacity{node("source", 1, 900), node("follower", 2, 300), node("target", 3, 100)}
}

func hotGroup(id byte) raftmember.GroupKey {
	var group raftmember.GroupKey
	group.ClusterID[0], group.ClusterIncarnation[0], group.ShardIncarnation[0], group.GroupID[0] = 1, 1, id, id
	group.TopologyRecoveryEpoch = 1
	return group
}

func BenchmarkControllerColdPressureCut(b *testing.B) {
	catalog, source, _ := hotCatalog(b)
	policy := hotPolicy()
	controller, _ := New(policy)
	sink := &admissionSink{}
	view := hotView(source, nil, 1)
	view.Reports[0].Recommendation = autosplit.Recommendation{
		Source: source, WindowSequence: 1, Kind: autosplit.RecommendationNone,
		Reason: autosplit.ReasonBelowTrigger, CurrentPressurePPM: 400_000,
	}
	revision := uint64(1)
	b.ReportAllocs()
	b.ReportMetric(float64(unsafe.Sizeof(Controller{})), "controller-B")
	b.ResetTimer()
	for b.Loop() {
		view.AuthorityRevision = revision
		view.Reports[0].Recommendation.WindowSequence = revision
		if _, err := controller.Process(context.Background(), catalog, view, sink); err != nil {
			b.Fatal(err)
		}
		revision++
	}
}
