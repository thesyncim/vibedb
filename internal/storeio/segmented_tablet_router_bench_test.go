package storeio

import (
	"fmt"
	"testing"
)

var (
	segmentedTabletRouterBenchmarkRoute  SegmentedTabletRouterRoute
	segmentedTabletRouterBenchmarkRef    PageRef
	segmentedTabletRouterBenchmarkZone   BucketZone
	segmentedTabletRouterBenchmarkCursor SegmentedTabletRouterCursor
	segmentedTabletRouterBenchmarkCOW    SegmentedTabletRouterCOWResult
	segmentedTabletRouterBenchmarkSplit  SegmentedTabletRouterSplitResult
)

func BenchmarkSegmentedTabletRouterResident(b *testing.B) {
	view, leaves, _ := segmentedTabletRouterTestView(b, 3072)
	target := 1536
	key := leaves[target].Fence
	hash := KeyHashBytes(segmentedTabletRouterTestSeed, key)
	bucket := segmentedTabletRouterTestBucket(leaves[target].Ref)

	b.Run("route/reused-hash", func(b *testing.B) {
		b.ReportAllocs()
		var route SegmentedTabletRouterRoute
		for b.Loop() {
			route = view.RouteHashed(hash, key)
		}
		segmentedTabletRouterBenchmarkRoute = route
	})
	b.Run("route/hash-included", func(b *testing.B) {
		b.ReportAllocs()
		var route SegmentedTabletRouterRoute
		for b.Loop() {
			route = view.Route(segmentedTabletRouterTestSeed, key)
		}
		segmentedTabletRouterBenchmarkRoute = route
	})
	b.Run("resolve/posting-bucket-id", func(b *testing.B) {
		b.ReportAllocs()
		var ref PageRef
		var zone BucketZone
		for b.Loop() {
			ref, zone, _ = view.ResolveBucketID(bucket)
		}
		segmentedTabletRouterBenchmarkRef = ref
		segmentedTabletRouterBenchmarkZone = zone
	})
	b.Run("lower-bound", func(b *testing.B) {
		b.ReportAllocs()
		var cursor SegmentedTabletRouterCursor
		for b.Loop() {
			cursor = view.LowerBound(key)
		}
		segmentedTabletRouterBenchmarkCursor = cursor
	})
	b.Run("ordered-scan/256-leaves", func(b *testing.B) {
		b.ReportAllocs()
		var route SegmentedTabletRouterRoute
		for b.Loop() {
			cursor := view.LowerBound(key)
			for range 256 {
				route, _ = cursor.Route(hash)
				cursor.Next()
			}
		}
		segmentedTabletRouterBenchmarkRoute = route
		b.ReportMetric(256, "leaves/op")
	})
}

func BenchmarkSegmentedTabletRouterOrdinaryCOW(b *testing.B) {
	view, leaves, refs := segmentedTabletRouterTestView(b, 3072)
	target := 1537
	bucket := segmentedTabletRouterTestBucket(leaves[target].Ref)
	leafRef := leaves[target].Ref
	leafRef.Offset += 8
	leafRef.Generation = 10_001
	leafRef.Length = 317
	zone := BucketZone{9, 8, 7, 6}
	pageID := uint8(target / SegmentedTabletRouterRowsPerPage)
	anchorRef := refs[pageID]
	anchorRef.Offset += 1 << 20
	anchorRef.Generation = 10_001
	rootDst := make([]byte, SegmentedTabletRouterRootBytes)
	pageDst := make([]byte, SegmentedTabletRouterAnchorPageBytes)
	b.ReportAllocs()
	b.SetBytes(SegmentedTabletRouterRootBytes +
		SegmentedTabletRouterAnchorPageBytes)
	var result SegmentedTabletRouterCOWResult
	for b.Loop() {
		var err error
		result, err = view.RewriteHandle(
			rootDst, pageDst, 10_001, bucket, leafRef, zone, anchorRef,
		)
		if err != nil {
			b.Fatal(err)
		}
	}
	segmentedTabletRouterBenchmarkCOW = result
	b.ReportMetric(float64(result.Bytes), "COW-B/op")
}

func BenchmarkSegmentedTabletRouterStructuralSplit(b *testing.B) {
	view, _, refs := segmentedTabletRouterTestView(b, 3072)
	const pageID = uint8(5)
	newPageID := view.pageCount
	leftRef := refs[pageID]
	leftRef.Offset += 2 << 20
	leftRef.Generation = 10_001
	rightRef := PageRef{
		Offset: 3 << 20,
		LogicalID: segmentedTabletRouterAnchorLogicalID(
			view.tabletID, newPageID,
		),
		Generation: 10_001,
		Length:     SegmentedTabletRouterAnchorPageBytes,
		Kind:       view.anchorKind,
	}
	rootDst := make([]byte, SegmentedTabletRouterRootBytes)
	locatorDst := make([]byte, SegmentedTabletRouterLocatorBytes)
	leftDst := make([]byte, SegmentedTabletRouterAnchorPageBytes)
	rightDst := make([]byte, SegmentedTabletRouterAnchorPageBytes)
	b.ReportAllocs()
	b.SetBytes(SegmentedTabletRouterRootBytes +
		SegmentedTabletRouterLocatorBytes +
		2*SegmentedTabletRouterAnchorPageBytes)
	var result SegmentedTabletRouterSplitResult
	for b.Loop() {
		var err error
		result, err = view.SplitAnchorPage(
			rootDst, locatorDst, leftDst, rightDst,
			10_001, pageID, newPageID, 128, leftRef, rightRef,
		)
		if err != nil {
			b.Fatal(err)
		}
	}
	segmentedTabletRouterBenchmarkSplit = result
	b.ReportMetric(float64(result.Bytes), "COW-B/op")
}

func BenchmarkSegmentedTabletRouterRoutingSpace(b *testing.B) {
	for _, rows := range []int{187, 195, 230} {
		b.Run(fmt.Sprintf("%d-rows", rows), func(b *testing.B) {
			value := SegmentedTabletRouterRoutingBytesPerDocument(
				TabletLocalIdentityLocalCount, rows,
			)
			b.ReportMetric(value, "B/document")
			for b.Loop() {
			}
			b.ReportMetric(value, "B/document")
			b.ReportMetric(35, "B/leaf")
		})
	}
}
