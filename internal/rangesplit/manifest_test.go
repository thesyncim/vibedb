package rangesplit

import (
	"errors"
	"testing"

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
