package autosplit

import (
	"errors"
	"strconv"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
)

var splitPlanBenchmarkSink *SplitPlan

func TestPlanSplitBuildsBucketAlignedGenerationFencedManifest(t *testing.T) {
	current := actionManifest(t)
	source := actionSource(t, current)
	recommendation := testBinaryRecommendation(source, 9, 32)
	request := SplitRequest{
		Recommendation: recommendation, RetainChild: 0,
		NextRoutingVersion: current.Version() + 1, AllocationHighWater: 7,
		Destinations: []Destination{{
			Shard: "right", AllocationGeneration: 8,
			Leaders: []distribution.EndpointID{"node-b"}, OwnershipEpoch: 1,
		}},
	}
	plan, err := PlanSplit(current, request)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ChildCount != 2 || plan.RetainedChild != 0 || plan.Manifest().Version() != 12 {
		t.Fatalf("plan = %+v", plan)
	}
	left, _ := plan.Child(0)
	right, _ := plan.Child(1)
	if !left.Retained || left.Shard != "source" || left.OwnershipEpoch != 6 ||
		left.AllocationGeneration != 7 || right.Retained || right.Shard != "right" ||
		right.AllocationGeneration != 8 || right.OwnershipEpoch != 1 {
		t.Fatalf("children = %+v / %+v", left, right)
	}
	boundary := recommendation.Boundaries[0]
	if left.Range.End.Point != boundary || right.Range.Start != boundary ||
		!right.Range.End.Max {
		t.Fatalf("child ranges = %+v / %+v", left.Range, right.Range)
	}
	if target, ok := plan.Manifest().ResolvePointTarget(testPoint(^uint64(0))); !ok ||
		target.Shard != "right" || target.AllocationGeneration != 8 {
		t.Fatalf("right route = %+v,%v", target, ok)
	}
	// Input ownership never leaks into the immutable plan.
	request.Destinations[0].Leaders[0] = "mutated"
	right, _ = plan.Child(1)
	if right.Leaders[0] != "node-b" {
		t.Fatalf("plan retained caller leader backing: %+v", right.Leaders)
	}
}

func TestPlanSplitIsolatesExactlyOneVirtualBucket(t *testing.T) {
	current := actionManifest(t)
	source := actionSource(t, current)
	hot := testPoint(uint64(77)<<(64-source.BucketBits) | 99)
	recommendation := testIsolateRecommendation(source, 11, hot)
	plan, err := PlanSplit(current, SplitRequest{
		Recommendation: recommendation, RetainChild: 0,
		NextRoutingVersion: 12, AllocationHighWater: 7,
		Destinations: []Destination{
			{Shard: "hot", AllocationGeneration: 8, Leaders: []distribution.EndpointID{"node-b"}, OwnershipEpoch: 1},
			{Shard: "tail", AllocationGeneration: 9, Leaders: []distribution.EndpointID{"node-c"}, OwnershipEpoch: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.ChildCount != 3 {
		t.Fatalf("child count = %d", plan.ChildCount)
	}
	hotChild, _ := plan.Child(1)
	bucket, _ := distribution.VirtualBucketForPoint(hot, source.BucketBits)
	want, _ := distribution.VirtualBucketRange(bucket, source.BucketBits)
	if hotChild.Range != want || hotChild.Shard != "hot" {
		t.Fatalf("hot child = %+v, want range %+v", hotChild, want)
	}
}

func TestPlanSplitRejectsStaleOrUnsafeTopology(t *testing.T) {
	current := actionManifest(t)
	source := actionSource(t, current)
	base := SplitRequest{
		Recommendation: testBinaryRecommendation(source, 1, 32), RetainChild: 0,
		NextRoutingVersion: 12, AllocationHighWater: 7,
		Destinations: []Destination{{
			Shard: "right", AllocationGeneration: 8,
			Leaders: []distribution.EndpointID{"node-b"}, OwnershipEpoch: 1,
		}},
	}
	tests := []struct {
		name   string
		mutate func(*SplitRequest)
	}{
		{"stale source epoch", func(r *SplitRequest) { r.Recommendation.Source.OwnershipEpoch-- }},
		{"routing regression", func(r *SplitRequest) { r.NextRoutingVersion = current.Version() }},
		{"routing skip", func(r *SplitRequest) { r.NextRoutingVersion = current.Version() + 2 }},
		{"unaligned boundary", func(r *SplitRequest) { r.Recommendation.Boundaries[0][7] = 1 }},
		{"allocation below high water", func(r *SplitRequest) { r.Destinations[0].AllocationGeneration = 7 }},
		{"active shard id reuse", func(r *SplitRequest) { r.Destinations[0].Shard = "source" }},
		{"zero destination epoch", func(r *SplitRequest) { r.Destinations[0].OwnershipEpoch = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := base
			request.Destinations = append([]Destination(nil), base.Destinations...)
			test.mutate(&request)
			if _, err := PlanSplit(current, request); !errors.Is(err, ErrInvalidSplit) {
				t.Fatalf("PlanSplit error = %v, want ErrInvalidSplit", err)
			}
		})
	}
}

func TestPlanSplitRejectsExhaustedRoutingVersion(t *testing.T) {
	current, err := distribution.NewManifest("d", ^distribution.RoutingVersion(0), []distribution.Shard{{
		ID: "source", AllocationGeneration: 7,
		Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: []distribution.EndpointID{"node-a"}, Epoch: 5,
	}})
	if err != nil {
		t.Fatal(err)
	}
	source := actionSource(t, current)
	request := SplitRequest{
		Recommendation: testBinaryRecommendation(source, 1, 32), RetainChild: 0,
		NextRoutingVersion: 1, AllocationHighWater: 7,
		Destinations: []Destination{{
			Shard: "right", AllocationGeneration: 8,
			Leaders: []distribution.EndpointID{"node-b"}, OwnershipEpoch: 1,
		}},
	}
	if _, err := PlanSplit(current, request); !errors.Is(err, ErrInvalidSplit) {
		t.Fatalf("PlanSplit error = %v, want ErrInvalidSplit", err)
	}
}

func TestPlanSplitRejectsUnalignedSourceGeometry(t *testing.T) {
	boundary := testPoint(uint64(1)<<63 | 1)
	current, err := distribution.NewManifest("d", 11, []distribution.Shard{
		{
			ID: "source", AllocationGeneration: 7,
			Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Point: boundary}},
			Leaders: []distribution.EndpointID{"node-a"}, Epoch: 5,
		},
		{
			ID: "tail", AllocationGeneration: 8,
			Range:   distribution.KeyRange{Start: boundary, End: distribution.KeyspaceEnd{Max: true}},
			Leaders: []distribution.EndpointID{"node-z"}, Epoch: 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	source := actionSource(t, current)
	recommendation := testIsolateRecommendation(
		source, 1, testPoint(uint64(77)<<(64-source.BucketBits)),
	)
	_, err = PlanSplit(current, SplitRequest{
		Recommendation: recommendation, RetainChild: 0,
		NextRoutingVersion: 12, AllocationHighWater: 8,
		Destinations: []Destination{
			{Shard: "hot", AllocationGeneration: 9, Leaders: []distribution.EndpointID{"node-b"}, OwnershipEpoch: 1},
			{Shard: "middle", AllocationGeneration: 10, Leaders: []distribution.EndpointID{"node-c"}, OwnershipEpoch: 1},
		},
	})
	if !errors.Is(err, ErrInvalidSplit) {
		t.Fatalf("PlanSplit error = %v, want ErrInvalidSplit", err)
	}
}

func BenchmarkPlanSplitLargeManifest(b *testing.B) {
	const shardCount = 1024
	shards := make([]distribution.Shard, shardCount)
	for index := range shards {
		keyRange := distribution.KeyRange{Start: testPoint(uint64(index) << 54)}
		if index == shardCount-1 {
			keyRange.End.Max = true
		} else {
			keyRange.End.Point = testPoint(uint64(index+1) << 54)
		}
		shards[index] = distribution.Shard{
			ID:                   distribution.ShardID("s-" + strconv.Itoa(index)),
			AllocationGeneration: distribution.ShardAllocationGeneration(index + 1),
			Range:                keyRange, Leaders: []distribution.EndpointID{"node-a"}, Epoch: 1,
		}
	}
	manifest, err := distribution.NewManifest("d", 11, shards)
	if err != nil {
		b.Fatal(err)
	}
	sourceShard, _ := manifest.ShardInfo(shardCount - 1)
	source := SourceIdentity{
		Distribution: manifest.Distribution(), Shard: sourceShard.ID,
		AllocationGeneration: sourceShard.AllocationGeneration, Range: sourceShard.Range,
		BucketBits:     distribution.DefaultVirtualBucketBits,
		RoutingVersion: manifest.Version(), OwnershipEpoch: sourceShard.Epoch,
	}
	request := SplitRequest{
		Recommendation: testBinaryRecommendation(source, 1, 32), RetainChild: 0,
		NextRoutingVersion:  manifest.Version() + 1,
		AllocationHighWater: shardCount,
		Destinations: []Destination{{
			Shard: "new", AllocationGeneration: shardCount + 1,
			Leaders: []distribution.EndpointID{"node-b"}, OwnershipEpoch: 1,
		}},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		splitPlanBenchmarkSink, err = PlanSplit(manifest, request)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func actionManifest(t testing.TB) *distribution.Manifest {
	t.Helper()
	manifest, err := distribution.NewManifest("d", 11, []distribution.Shard{{
		ID: "source", AllocationGeneration: 7,
		Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: []distribution.EndpointID{"node-a"}, Epoch: 5,
	}})
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func actionSource(t testing.TB, manifest *distribution.Manifest) SourceIdentity {
	t.Helper()
	shard, ok := manifest.ShardInfo(0)
	if !ok {
		t.Fatal("source shard missing")
	}
	return SourceIdentity{
		Distribution: manifest.Distribution(), Shard: shard.ID,
		AllocationGeneration: shard.AllocationGeneration, Range: shard.Range,
		BucketBits:     distribution.DefaultVirtualBucketBits,
		RoutingVersion: manifest.Version(), OwnershipEpoch: shard.Epoch,
	}
}
