package rangesplit

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
)

func TestValidateManifestTransitionChangesOnlyExactSource(t *testing.T) {
	plan := testSplitPlanWithNeighbor(t, "node-z")
	partitioner, err := NewPartitioner(
		plan, "docs", []string{"/tenant", "/sequence"},
		distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	middle := distribution.KeyspacePoint{0x80}
	current, err := distribution.NewManifest("orders", 11, []distribution.Shard{
		{
			ID: "source", AllocationGeneration: 7,
			Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Point: middle}},
			Leaders: []distribution.EndpointID{"node-a"}, Epoch: 5,
		},
		{
			ID: "neighbor", AllocationGeneration: 8,
			Range:   distribution.KeyRange{Start: middle, End: distribution.KeyspaceEnd{Max: true}},
			Leaders: []distribution.EndpointID{"node-z"}, Epoch: 3,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := partitioner.ValidateManifestTransition(current, plan.Manifest()); err != nil {
		t.Fatal(err)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if err := partitioner.ValidateManifestTransition(current, plan.Manifest()); err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("warm manifest validation allocations=%v, want 0", allocations)
	}
	shards := make([]distribution.Shard, plan.Manifest().ShardCount())
	for index := range shards {
		shards[index], _ = plan.Manifest().ShardInfo(index)
	}
	shards[len(shards)-1].Leaders[0] = "changed-neighbor"
	changedNeighbor, err := distribution.NewManifest("orders", 12, shards)
	if err != nil {
		t.Fatal(err)
	}
	if err := partitioner.ValidateManifestTransition(current, changedNeighbor); !errors.Is(err, ErrManifestTransition) {
		t.Fatalf("changed neighbor error=%v", err)
	}
	wrongCurrent, err := distribution.NewManifest("orders", 11, []distribution.Shard{
		{
			ID: "source", AllocationGeneration: 7,
			Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Point: middle}},
			Leaders: []distribution.EndpointID{"wrong-source"}, Epoch: 5,
		},
		{
			ID: "neighbor", AllocationGeneration: 8,
			Range:   distribution.KeyRange{Start: middle, End: distribution.KeyspaceEnd{Max: true}},
			Leaders: []distribution.EndpointID{"node-z"}, Epoch: 3,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := partitioner.ValidateManifestTransition(wrongCurrent, plan.Manifest()); !errors.Is(err, ErrManifestTransition) {
		t.Fatalf("wrong source error=%v", err)
	}
}

func TestComposeManifestTransitionsBatchesDisjointSources(t *testing.T) {
	middle := distribution.KeyspacePoint{0x80}
	current, err := distribution.NewManifest("orders", 11, []distribution.Shard{
		{
			ID: "left", AllocationGeneration: 1,
			Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Point: middle}},
			Leaders: []distribution.EndpointID{"node-a"}, Epoch: 3,
		},
		{
			ID: "right", AllocationGeneration: 2,
			Range:   distribution.KeyRange{Start: middle, End: distribution.KeyspaceEnd{Max: true}},
			Leaders: []distribution.EndpointID{"node-b"}, Epoch: 5,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	leftPlan := composedSplitPlan(
		t, current, "left", 1,
		distribution.KeyRange{End: distribution.KeyspaceEnd{Point: middle}},
		3, distribution.KeyspacePoint{0x40}, "left-tail", 3, "node-c",
	)
	rightPlan := composedSplitPlan(
		t, current, "right", 2,
		distribution.KeyRange{Start: middle, End: distribution.KeyspaceEnd{Max: true}},
		5, distribution.KeyspacePoint{0xc0}, "right-tail", 4, "node-d",
	)
	left, err := NewPartitioner(
		leftPlan, "docs", []string{"/tenant"}, distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewPartitioner(
		rightPlan, "docs", []string{"/tenant"}, distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	transitions := []ManifestTransition{
		{Partitioner: right, Target: rightPlan.Manifest()},
		{Partitioner: left, Target: leftPlan.Manifest()},
	}
	combined, err := ComposeManifestTransitions(current, transitions)
	if err != nil {
		t.Fatal(err)
	}
	if combined.Version() != 12 || combined.ShardCount() != 4 {
		t.Fatalf("combined version/shards = %d/%d", combined.Version(), combined.ShardCount())
	}
	want := [...]distribution.ShardID{"left", "left-tail", "right", "right-tail"}
	for ordinal := range want {
		metadata, ok := combined.ShardMetadataAt(ordinal)
		if !ok || metadata.ID != want[ordinal] {
			t.Fatalf("combined shard %d = %+v, ok=%v, want %q", ordinal, metadata, ok, want[ordinal])
		}
	}
	if err := left.ValidatePublishedManifestTransition(current, combined); err != nil {
		t.Fatalf("left published recognition = %v", err)
	}
	if err := right.ValidatePublishedManifestTransition(current, combined); err != nil {
		t.Fatalf("right published recognition = %v", err)
	}
	if allocations := testing.AllocsPerRun(1_000, func() {
		if err := left.ValidatePublishedManifestTransition(current, combined); err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("published split recognition allocations = %v, want 0", allocations)
	}
	changed := make([]distribution.Shard, combined.ShardCount())
	for ordinal := range changed {
		changed[ordinal], _ = combined.ShardInfo(ordinal)
	}
	changed[1].Epoch++
	changedCombined, err := distribution.NewManifest(
		combined.Distribution(), combined.Version(), changed,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := left.ValidatePublishedManifestTransition(
		current, changedCombined,
	); !errors.Is(err, ErrManifestTransition) {
		t.Fatalf("changed published child error = %v", err)
	}

	if _, err := ComposeManifestTransitions(current, []ManifestTransition{
		{Partitioner: left, Target: leftPlan.Manifest()},
		{Partitioner: left, Target: leftPlan.Manifest()},
	}); !errors.Is(err, ErrManifestTransition) {
		t.Fatalf("duplicate source error = %v", err)
	}
	if _, err := ComposeManifestTransitions(current, []ManifestTransition{
		{Partitioner: left, Target: rightPlan.Manifest()},
	}); !errors.Is(err, ErrManifestTransition) {
		t.Fatalf("mismatched target error = %v", err)
	}
}

func composedSplitPlan(
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
