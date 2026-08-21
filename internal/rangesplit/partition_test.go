package rangesplit

import (
	"bytes"
	"errors"
	"strconv"
	"testing"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

func TestPartitionRowsScansOnceAndDispatchesExactly(t *testing.T) {
	plan := testSplitPlan(t, "node-b")
	partitioner, err := NewPartitioner(
		plan, "docs", []string{"/tenant", "/sequence"},
		distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	left := documentForChild(t, partitioner, 0)
	right := documentForChild(t, partitioner, 1)
	rows := []struct{ key, value []byte }{
		{[]byte("left"), left}, {[]byte("right"), right},
	}
	var got [autosplit.MaxSplitChildren][][]byte
	sinks := []RowSink{
		func(_, value []byte) error {
			got[0] = append(got[0], bytes.Clone(value))
			return nil
		},
		func(_, value []byte) error {
			got[1] = append(got[1], bytes.Clone(value))
			return nil
		},
	}
	scans := 0
	rangeRows := func(visit func(key, value []byte) error) error {
		scans++
		for _, row := range rows {
			if err := visit(row.key, row.value); err != nil {
				return err
			}
		}
		return nil
	}
	var workspace PartitionWorkspace
	stats, err := partitioner.partitionRows(testSourceState(plan), rangeRows, sinks, &workspace)
	if err != nil {
		t.Fatal(err)
	}
	if scans != 1 {
		t.Fatalf("source scans = %d, want 1", scans)
	}
	if len(got[0]) != 1 || !bytes.Equal(got[0][0], left) ||
		len(got[1]) != 1 || !bytes.Equal(got[1][0], right) {
		t.Fatalf("partitioned rows = %q / %q", got[0], got[1])
	}
	if stats.Rows[0] != 1 || stats.Rows[1] != 1 ||
		stats.Bytes[0] != uint64(len("left")+len(left)) ||
		stats.Bytes[1] != uint64(len("right")+len(right)) ||
		stats.PlanDigest != partitioner.Digest() ||
		stats.SourceDigest != ([32]byte{1}) || stats.SourceBase != ([32]byte{2}) ||
		stats.SourceEntry != ([32]byte{3}) ||
		stats.SourceApplied != 41 ||
		stats.SourceTerm != 7 || stats.RouteGeneration != 19 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestPartitionRowsAllowsRetainedChildWithoutCopy(t *testing.T) {
	plan := testSplitPlan(t, "node-b")
	partitioner, err := NewPartitioner(
		plan, "docs", []string{"/tenant", "/sequence"},
		distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	left := documentForChild(t, partitioner, 0)
	right := documentForChild(t, partitioner, 1)
	var copied int
	rangeRows := func(visit func(key, value []byte) error) error {
		if err := visit([]byte("l"), left); err != nil {
			return err
		}
		return visit([]byte("r"), right)
	}
	var workspace PartitionWorkspace
	stats, err := partitioner.partitionRows(
		testSourceState(plan), rangeRows,
		[]RowSink{nil, func(_, _ []byte) error { copied++; return nil }}, &workspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	if copied != 1 || stats.Rows[0] != 1 || stats.Rows[1] != 1 {
		t.Fatalf("copied=%d stats=%+v", copied, stats)
	}
}

func TestPartitionRowsFencesSourceAndPropagatesSinkFailure(t *testing.T) {
	plan := testSplitPlan(t, "node-b")
	partitioner, err := NewPartitioner(
		plan, "docs", []string{"/tenant", "/sequence"},
		distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	right := documentForChild(t, partitioner, 1)
	rangeRows := func(visit func(key, value []byte) error) error {
		return visit([]byte("r"), right)
	}
	var workspace PartitionWorkspace
	stale := testSourceState(plan)
	stale.Binding.OwnershipEpoch--
	if _, err := partitioner.partitionRows(
		stale, rangeRows, []RowSink{nil, func(_, _ []byte) error { return nil }}, &workspace,
	); !errors.Is(err, ErrSourceFence) {
		t.Fatalf("stale source error = %v", err)
	}
	want := errors.New("sink stopped")
	if _, err := partitioner.partitionRows(
		testSourceState(plan), rangeRows,
		[]RowSink{nil, func(_, _ []byte) error { return want }}, &workspace,
	); !errors.Is(err, want) {
		t.Fatalf("sink error = %v", err)
	}
}

func TestPartitionRowsAllocatesZeroWhenWarm(t *testing.T) {
	plan := testSplitPlan(t, "node-b")
	partitioner, err := NewPartitioner(
		plan, "docs", []string{"/tenant", "/sequence"},
		distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	document := documentForChild(t, partitioner, 1)
	key := []byte("row")
	rangeRows := func(visit func(key, value []byte) error) error {
		return visit(key, document)
	}
	sinks := []RowSink{nil, func(_, _ []byte) error { return nil }}
	state := testSourceState(plan)
	var workspace PartitionWorkspace
	if _, err := partitioner.partitionRows(state, rangeRows, sinks, &workspace); err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(1_000, func() {
		if _, err := partitioner.partitionRows(state, rangeRows, sinks, &workspace); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("warm one-pass partition allocations = %v, want 0", allocs)
	}
}

func TestSplitPlanDigestBindsEndpoints(t *testing.T) {
	first := testSplitPlan(t, "node-b")
	second := testSplitPlan(t, "node-c")
	a, err := SplitPlanDigest(first)
	if err != nil {
		t.Fatal(err)
	}
	aAgain, err := SplitPlanDigest(first)
	if err != nil {
		t.Fatal(err)
	}
	b, err := SplitPlanDigest(second)
	if err != nil {
		t.Fatal(err)
	}
	if a != aAgain || a == b || a == ([32]byte{}) {
		t.Fatalf("digests = %x / %x / %x", a, aAgain, b)
	}
}

func TestSplitPlanDigestBindsUnchangedManifest(t *testing.T) {
	first := testSplitPlanWithNeighbor(t, "node-z")
	changedNeighbor := testSplitPlanWithNeighbor(t, "node-y")
	a, err := SplitPlanDigest(first)
	if err != nil {
		t.Fatal(err)
	}
	b, err := SplitPlanDigest(changedNeighbor)
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatalf("full-manifest digests collided: %x / %x", a, b)
	}
}

func BenchmarkPartitionRowsOnePass(b *testing.B) {
	plan := testSplitPlan(b, "node-b")
	partitioner, err := NewPartitioner(
		plan, "docs", []string{"/tenant", "/sequence"},
		distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		b.Fatal(err)
	}
	document := documentForChild(b, partitioner, 1)
	key := []byte("row")
	rangeRows := func(visit func(key, value []byte) error) error {
		return visit(key, document)
	}
	sinks := []RowSink{nil, func(_, _ []byte) error { return nil }}
	state := testSourceState(plan)
	var workspace PartitionWorkspace
	if _, err := partitioner.partitionRows(state, rangeRows, sinks, &workspace); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(document)))
	b.ResetTimer()
	for range b.N {
		if _, err := partitioner.partitionRows(state, rangeRows, sinks, &workspace); err != nil {
			b.Fatal(err)
		}
	}
}

func testSplitPlan(t testing.TB, endpoint distribution.EndpointID) *autosplit.SplitPlan {
	t.Helper()
	current, err := distribution.NewManifest("orders", 11, []distribution.Shard{{
		ID: "source", AllocationGeneration: 7,
		Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: []distribution.EndpointID{"node-a"}, Epoch: 5,
	}})
	if err != nil {
		t.Fatal(err)
	}
	boundary := distribution.KeyspacePoint{0x80}
	source := autosplit.SourceIdentity{
		Distribution: "orders", Shard: "source", AllocationGeneration: 7,
		Range:          distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		BucketBits:     distribution.DefaultVirtualBucketBits,
		RoutingVersion: 11, OwnershipEpoch: 5,
	}
	plan, err := autosplit.PlanSplit(current, autosplit.SplitRequest{
		Recommendation: autosplit.Recommendation{
			Source: source, Kind: autosplit.RecommendationBinarySplit,
			Boundaries: [2]distribution.KeyspacePoint{boundary}, BoundaryCount: 1,
			CandidateBin: 32, BenefitPPM: 1,
		},
		RetainChild: 0, NextRoutingVersion: 12, AllocationHighWater: 7,
		Destinations: []autosplit.Destination{{
			Shard: "right", AllocationGeneration: 8,
			Leaders: []distribution.EndpointID{endpoint}, OwnershipEpoch: 1,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func testSourceState(plan *autosplit.SplitPlan) replicatedstate.State {
	return replicatedstate.State{
		Binding: replicatedstate.Binding{
			Distribution: string(plan.Source.Distribution), Shard: string(plan.Source.Shard),
			AllocationGeneration: uint64(plan.Source.AllocationGeneration),
			OwnershipEpoch:       uint64(plan.Source.OwnershipEpoch),
			RoutingVersion:       uint64(plan.Source.RoutingVersion), RouteGeneration: 19,
		},
		Applied: 41, LastTerm: 7, LastEntryDigest: [32]byte{3},
		LogicalDigest: [32]byte{1}, SnapshotBaseDigest: [32]byte{2},
	}
}

func testSplitPlanWithNeighbor(
	t testing.TB,
	neighbor distribution.EndpointID,
) *autosplit.SplitPlan {
	t.Helper()
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
			Leaders: []distribution.EndpointID{neighbor}, Epoch: 3,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	source := autosplit.SourceIdentity{
		Distribution: "orders", Shard: "source", AllocationGeneration: 7,
		Range:          distribution.KeyRange{End: distribution.KeyspaceEnd{Point: middle}},
		BucketBits:     distribution.DefaultVirtualBucketBits,
		RoutingVersion: 11, OwnershipEpoch: 5,
	}
	plan, err := autosplit.PlanSplit(current, autosplit.SplitRequest{
		Recommendation: autosplit.Recommendation{
			Source: source, Kind: autosplit.RecommendationBinarySplit,
			Boundaries: [2]distribution.KeyspacePoint{{0x40}}, BoundaryCount: 1,
			CandidateBin: 32, BenefitPPM: 1,
		},
		RetainChild: 0, NextRoutingVersion: 12, AllocationHighWater: 8,
		Destinations: []autosplit.Destination{{
			Shard: "middle", AllocationGeneration: 9,
			Leaders: []distribution.EndpointID{"node-b"}, OwnershipEpoch: 1,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func documentForChild(t testing.TB, partitioner *Partitioner, want int) []byte {
	t.Helper()
	var workspace distribution.DocumentPointWorkspace
	for sequence := 0; sequence < 100_000; sequence++ {
		document := append([]byte(`{"tenant":"acme","sequence":`), strconv.Itoa(sequence)...)
		document = append(document, '}')
		point, err := partitioner.program.Point(document, &workspace)
		if err != nil {
			t.Fatal(err)
		}
		if partitioner.childFor(point) == want {
			return document
		}
	}
	t.Fatalf("no document found for child %d", want)
	return nil
}
