package storeio

import (
	"fmt"
	"runtime"
	"testing"
)

func residentPrimaryRouterAllocationFixture(
	t testing.TB, count, fenceBytes int,
) *ResidentPrimaryRouter {
	t.Helper()
	entries := make([]residentRouteEntry, count)
	for rank := range count {
		const fixtureLocalsPerTablet = TabletLocalIdentityLocalCount / 4
		tablet := uint32(rank / fixtureLocalsPerTablet)
		local := uint32(rank % fixtureLocalsPerTablet)
		bucketValue, ok := MakeTabletLocalIdentityBucket(tablet, local)
		if !ok {
			t.Fatalf("bucket identity rank=%d", rank)
		}
		bucket := BucketID(bucketValue)
		logicalID, ok := CommonPrimaryLeafLogicalID(bucket)
		if !ok {
			t.Fatalf("leaf logical ID rank=%d", rank)
		}
		fence := make([]byte, fenceBytes)
		copy(fence, fmt.Sprintf("%08d", rank))
		for index := len(fmt.Sprintf("%08d", rank)); index < len(fence); index++ {
			fence[index] = 'x'
		}
		ref := PageRef{
			Offset: uint64(rank+1) * (64 << 10), LogicalID: logicalID,
			Generation: 100, Length: 4096, Kind: PagePrimaryLeaf,
		}
		entries[rank] = newResidentRouteEntry(fence, ref, bucket)
	}
	index, err := residentBucketBuild(entries)
	if err != nil {
		t.Fatalf("build bucket index: %v", err)
	}
	tree := buildResidentRouteTree(entries)
	router := &ResidentPrimaryRouter{
		storeID: [16]byte{0x72, 0x6f, 0x75, 0x74, 0x65, 0x72},
		tree:    tree, treeBytes: tree.bytes, buckets: index,
	}
	router.generation.Store(100)
	router.refreshTreeFloor()
	return router
}

func residentPrimaryRouterPartitionFixture(
	t testing.TB, router *ResidentPrimaryRouter, replacementCount int,
) (ResidentPrimaryRoute, []SegmentedTabletRouterLeaf, int) {
	t.Helper()
	source, ok := router.RouteAtRank(router.Len() - 1)
	if !ok {
		t.Fatal("rightmost source route")
	}
	tablet, sourceLocal16, ok := SplitTabletLocalIdentityBucket(
		uint32(source.Bucket),
	)
	if !ok {
		t.Fatal("source tablet identity")
	}
	rightLocal := uint32(sourceLocal16) + 17
	if rightLocal >= TabletLocalIdentityLocalCount {
		t.Fatalf("fixture source local %d leaves no room for right split", sourceLocal16)
	}
	leftLogicalID, ok := CommonPrimaryLeafLogicalID(source.Bucket)
	if !ok {
		t.Fatal("left logical ID")
	}
	leftRef := source.Ref
	leftRef.Offset += 1 << 30
	leftRef.Generation = 101
	leftRef.LogicalID = leftLogicalID
	leftFence := router.fence(router.Len() - 1)
	replacements := make([]SegmentedTabletRouterLeaf, 1, 2)
	replacements[0] = SegmentedTabletRouterLeaf{
		LocalID: sourceLocal16, Fence: leftFence, Ref: leftRef,
	}
	for index := range replacementCount - 1 {
		local := rightLocal + uint32(index)
		bucketValue, bucketOK := MakeTabletLocalIdentityBucket(tablet, local)
		if !bucketOK {
			t.Fatalf("right bucket identity %d/%d", tablet, local)
		}
		logicalID, logicalOK := CommonPrimaryLeafLogicalID(BucketID(bucketValue))
		if !logicalOK {
			t.Fatalf("right logical ID %d/%d", tablet, local)
		}
		ref := PageRef{
			Offset:    leftRef.Offset + uint64(index+1)*(64<<10),
			LogicalID: logicalID, Generation: 101,
			Length: 4096, Kind: PagePrimaryLeaf,
		}
		fence := append([]byte(nil), leftFence...)
		for suffix := 0; suffix <= index; suffix++ {
			fence = append(fence, byte(suffix))
		}
		replacements = append(replacements, SegmentedTabletRouterLeaf{
			LocalID: uint16(local), Fence: fence, Ref: ref,
		})
	}
	// The allocation bound separately charges the full current router image,
	// which covers the cloned left/source fence. addedFenceBytes therefore
	// follows the foreground caller and includes only newly owned right fences.
	var fenceBytes int
	for index := 1; index < len(replacements); index++ {
		fenceBytes += cap(replacements[index].Fence)
	}
	return source, replacements, fenceBytes
}

func TestResidentPrimaryRouterSplitLeafPartitionAllocationUpperBound(t *testing.T) {
	for _, fixture := range []struct {
		name       string
		count      int
		fenceBytes int
	}{
		{name: "small", count: 3, fenceBytes: 8},
		{name: "leaf-block-boundary", count: residentRouteBlockSize, fenceBytes: 8},
		{name: "leaf-block-growth", count: residentRouteBlockSize + 1, fenceBytes: 8},
		{name: "tree-root-boundary", count: residentRouteBlockSize * residentRouteFanout, fenceBytes: 8},
		{name: "tree-root-growth", count: residentRouteBlockSize*residentRouteFanout + 1, fenceBytes: 8},
		{name: "long-fences", count: residentRouteBlockSize + 1, fenceBytes: 4096},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			router := residentPrimaryRouterAllocationFixture(
				t, fixture.count, fixture.fenceBytes,
			)
			for _, replacementCount := range []int{2, 9} {
				t.Run(fmt.Sprintf("replacements-%d", replacementCount), func(t *testing.T) {
					source, replacements, fenceBytes :=
						residentPrimaryRouterPartitionFixture(t, router, replacementCount)
					bound, ok := router.SplitLeafPartitionAllocationUpperBound(
						len(replacements), fenceBytes,
					)
					if !ok || bound == 0 {
						t.Fatalf("allocation bound = %d,%v", bound, ok)
					}
					tablet, _, tabletOK := SplitTabletLocalIdentityBucket(
						uint32(source.Bucket),
					)
					if !tabletOK {
						t.Fatal("source tablet identity")
					}
					for index, replacement := range replacements {
						bucket, bucketOK := MakeTabletLocalIdentityBucket(
							tablet, uint32(replacement.LocalID),
						)
						if !bucketOK || replacement.Ref.Generation != 101 ||
							segmentedTabletRouterValidateLeafRef(
								replacement.Ref, BucketID(bucket),
								PagePrimaryLeaf, 101,
							) != nil {
							t.Fatalf("invalid replacement[%d]=%+v bucket=%d,%v", index,
								replacement, bucket, bucketOK)
						}
					}
					runtime.GC()
					var before, after runtime.MemStats
					runtime.ReadMemStats(&before)
					next, err := router.SplitLeafPartition(source, replacements, 101)
					runtime.ReadMemStats(&after)
					if err != nil {
						t.Fatal(err)
					}
					actual := after.TotalAlloc - before.TotalAlloc
					if actual > bound {
						t.Fatalf("SplitLeafPartition allocated %d bytes, above reserved %d", actual, bound)
					}
					if uint64(next.ResidentBytes()) > bound {
						t.Fatalf("next router resident bytes %d, above reserved %d",
							next.ResidentBytes(), bound)
					}
					runtime.KeepAlive(next)
				})
			}
		})
	}
}

func TestResidentPrimaryRouterSplitLeafPartitionAllocationUpperBoundRejectsBadLimits(
	t *testing.T,
) {
	router := residentPrimaryRouterGenerationTestFixture(t)
	for _, test := range []struct {
		name   string
		router *ResidentPrimaryRouter
		count  int
		fence  int
	}{
		{name: "nil router", count: 2},
		{name: "negative count", router: router, count: -1},
		{name: "one replacement", router: router, count: 1},
		{name: "negative fence bytes", router: router, count: 2, fence: -1},
		{name: "overflow route count", router: router, count: maxIntValue},
		{name: "overflow fence bytes", router: router, count: 2, fence: maxIntValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got, ok := test.router.SplitLeafPartitionAllocationUpperBound(
				test.count, test.fence,
			); ok || got != 0 {
				t.Fatalf("allocation bound = %d,%v, want 0,false", got, ok)
			}
		})
	}
}

func TestResidentPrimaryRouterCopyTabletLocalIDs(t *testing.T) {
	router := residentPrimaryRouterAllocationFixture(t, 129, 8)
	var locals [TabletLocalIdentityLocalCount / 64]uint64
	if !router.CopyTabletLocalIDs(0, &locals) {
		t.Fatal("CopyTabletLocalIDs returned false for populated tablet")
	}
	for _, local := range []uint32{0, 1, 127, 128} {
		if locals[local>>6]&(uint64(1)<<(local&63)) == 0 {
			t.Fatalf("tablet local %d missing from copied bitmap", local)
		}
	}
	if locals[2]&(uint64(1)<<1) != 0 {
		t.Fatal("tablet local 129 unexpectedly present")
	}
	if got := testing.AllocsPerRun(100, func() {
		if !router.CopyTabletLocalIDs(0, &locals) {
			panic("missing populated tablet")
		}
	}); got != 0 {
		t.Fatalf("CopyTabletLocalIDs allocations = %v, want zero", got)
	}
	locals[0] = ^uint64(0)
	if router.CopyTabletLocalIDs(1, &locals) {
		t.Fatal("CopyTabletLocalIDs found an absent tablet")
	}
	if locals != ([TabletLocalIdentityLocalCount / 64]uint64{}) {
		t.Fatal("absent tablet left stale bits in caller bitmap")
	}
	if router.CopyTabletLocalIDs(0, nil) {
		t.Fatal("CopyTabletLocalIDs accepted a nil destination")
	}
}
