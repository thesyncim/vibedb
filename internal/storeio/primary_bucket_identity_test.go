package storeio

import "testing"

func TestPrimaryLogicalIDNamespaceIsContiguousAndCollisionFree(t *testing.T) {
	ranges := [...]struct {
		name       string
		first, end uint64
	}{
		{"primary leaves", PrimaryLeafLogicalIDBase, PrimaryLeafLogicalIDLimit},
		{"primary anchors", PrimaryAnchorLogicalIDBase, PrimaryAnchorLogicalIDLimit},
		{"tablet roots", PrimaryTabletRootLogicalIDBase, PrimaryTabletRootLogicalIDLimit},
		{"primary locators", PrimaryLocatorLogicalIDBase, PrimaryLocatorLogicalIDLimit},
		{"catalog leaves", PrimaryCatalogLeafLogicalIDBase, PrimaryCatalogLeafLogicalIDLimit},
		{"catalog branches", PrimaryCatalogBranchLogicalIDBase, PrimaryCatalogBranchLogicalIDLimit},
		{"catalog root", PrimaryCatalogRootLogicalID, PrimaryCatalogRootLogicalID + 1},
		{"tablet routes", PrimaryTabletRouteLogicalIDBase, PrimaryTabletRouteLogicalIDLimit},
	}
	for rank, current := range ranges {
		if current.first >= current.end {
			t.Fatalf("%s range is empty: [%d,%d)", current.name, current.first, current.end)
		}
		if rank != 0 && ranges[rank-1].end != current.first {
			t.Fatalf(
				"namespace gap or collision: %s ends at %d, %s starts at %d",
				ranges[rank-1].name, ranges[rank-1].end,
				current.name, current.first,
			)
		}
	}
	if PrimaryFirstDynamicLogicalID != ranges[len(ranges)-1].end {
		t.Fatalf(
			"first dynamic logical ID = %d, want %d",
			PrimaryFirstDynamicLogicalID, ranges[len(ranges)-1].end,
		)
	}
	if PrimaryLeafLogicalIDLimit-PrimaryLeafLogicalIDBase !=
		uint64(PrimaryBucketIDLimit) {
		t.Fatal("primary leaf band does not cover every BucketID exactly once")
	}
}
