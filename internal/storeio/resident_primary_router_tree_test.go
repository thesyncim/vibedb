package storeio

import (
	"fmt"
	"testing"
)

func TestResidentPrimaryRouterTallTreeShrinksAndCollapsesRoot(t *testing.T) {
	const count = residentRouteBlockSize*residentRouteFanout + 1
	entries := make([]residentRouteEntry, count)
	for i := range entries {
		bucketU, ok := MakeTabletLocalIdentityBucket(uint32(i/TabletLocalIdentityLocalCount), uint32(i%TabletLocalIdentityLocalCount))
		if !ok {
			t.Fatal("bucket")
		}
		bucket := BucketID(bucketU)
		id, _ := CommonPrimaryLeafLogicalID(bucket)
		var fence []byte
		if i != 0 {
			fence = []byte(fmt.Sprintf("%08d", i))
		}
		entries[i] = newResidentRouteEntry(fence, PageRef{Offset: uint64(i+1) * 4096, LogicalID: id, Generation: 1, Length: 4096, Kind: PagePrimaryLeaf}, bucket)
	}
	index, err := residentBucketBuild(entries)
	if err != nil {
		t.Fatal(err)
	}
	router := &ResidentPrimaryRouter{tree: buildResidentRouteTree(entries), buckets: index}
	router.treeBytes = router.tree.bytes
	router.generation.Store(1)
	router.refreshTreeFloor()
	old := router
	if len(router.tree.children) == 0 || len(router.tree.children[0].children) == 0 {
		t.Fatal("fixture did not build tall tree")
	}
	for generation := uint64(2); generation <= 130; generation++ {
		route, ok := router.RouteAtRank(router.Len() - 1)
		if !ok {
			t.Fatal("tail route")
		}
		router, err = router.RemoveLeaf(route, generation)
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(router.tree.children) == 0 || len(router.tree.children[0].children) != 0 {
		t.Fatal("one-child root did not collapse")
	}
	if old.Len() != count {
		t.Fatalf("old image length = %d", old.Len())
	}
	for _, rank := range []int{0, 127, 4096, router.Len() - 1} {
		route, ok := router.RouteAtRank(rank)
		if !ok {
			t.Fatalf("rank %d", rank)
		}
		resolved, ok := router.ResolveBucketID(route.Bucket)
		if !ok || resolved.Ref != route.Ref {
			t.Fatalf("bucket %d", route.Bucket)
		}
	}
}

func TestResidentPrimaryRouterReplaceTabletsRejectsFloorMoveAndExistingTablet(t *testing.T) {
	router := residentPrimaryRouterGenerationTestFixture(t)
	old, _ := router.RouteAtRank(0)
	leaf := SegmentedTabletRouterLeaf{LocalID: uint16(old.Bucket), Ref: old.Ref}
	leaf.Ref.Generation = 101
	if _, err := router.ReplaceTablets(0, []ResidentPrimaryTabletReplacement{{TabletID: 0, Floor: []byte("moved"), Leaves: []SegmentedTabletRouterLeaf{leaf}}}, 101); err == nil {
		t.Fatal("accepted moved first floor")
	}
	for rank := 1; rank < 3; rank++ {
		other, _ := router.RouteAtRank(rank)
		existing, _ := MakeTabletLocalIdentityBucket(1, uint32(rank))
		other.cell.meta.Store(uint64(other.Ref.Length) | uint64(existing)<<32)
	}
	router.buckets, _ = residentBucketBuild(router.treeEntries())
	newBucket, _ := MakeTabletLocalIdentityBucket(1, 3)
	newID, _ := CommonPrimaryLeafLogicalID(BucketID(newBucket))
	foreign := SegmentedTabletRouterLeaf{LocalID: 3, Ref: PageRef{Offset: 8192, LogicalID: newID, Generation: 101, Length: 4096, Kind: PagePrimaryLeaf}}
	if _, err := router.ReplaceTablets(0, []ResidentPrimaryTabletReplacement{{TabletID: 1, Leaves: []SegmentedTabletRouterLeaf{foreign}}}, 101); err == nil {
		t.Fatal("accepted disconnected existing tablet")
	}
}
