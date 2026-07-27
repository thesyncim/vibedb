package storeio

import (
	"fmt"
	"testing"
)

var (
	tabletAnchorMapLabBenchmarkRoute  TabletAnchorMapLabRoute
	tabletAnchorMapLabBenchmarkID     BucketID
	tabletAnchorMapLabBenchmarkImage  []byte
	tabletAnchorMapLabBenchmarkCursor TabletAnchorMapLabCursor
	tabletAnchorHandleBenchmarkRoute  TabletAnchorHandleLabRoute
	tabletAnchorHandleBenchmarkRef    PageRef
	tabletAnchorHandleBenchmarkZone   BucketZone
)

func BenchmarkTabletAnchorMapLabRoute(b *testing.B) {
	anchors := tabletAnchorMapLabTestAnchors(4300)
	view := openTabletAnchorMapLabTest(b, anchors)
	key := []byte("tenant/0042/document/00054321")
	hash := KeyHashBytes(tabletAnchorMapLabTestSeed, key)
	b.ReportMetric(view.BytesPerAnchor(), "B/anchor")
	b.ReportMetric(view.BytesPerAnchor()/230, "B/document")

	b.Run("hash-included", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		var route TabletAnchorMapLabRoute
		for b.Loop() {
			route = view.Route(tabletAnchorMapLabTestSeed, key)
		}
		tabletAnchorMapLabBenchmarkRoute = route
		b.ReportMetric(view.BytesPerAnchor(), "B/anchor")
		b.ReportMetric(view.BytesPerAnchor()/230, "B/document")
	})
	b.Run("reused-hash", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		var route TabletAnchorMapLabRoute
		for b.Loop() {
			route = view.RouteHashed(hash, key)
		}
		tabletAnchorMapLabBenchmarkRoute = route
		b.ReportMetric(view.BytesPerAnchor(), "B/anchor")
		b.ReportMetric(view.BytesPerAnchor()/230, "B/document")
	})
	b.Run("lower-bound", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		var cursor TabletAnchorMapLabCursor
		for b.Loop() {
			cursor = view.LowerBound(key)
		}
		tabletAnchorMapLabBenchmarkCursor = cursor
		b.ReportMetric(view.BytesPerAnchor(), "B/anchor")
		b.ReportMetric(view.BytesPerAnchor()/230, "B/document")
	})
	b.Run("walk-256", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		var bucket BucketID
		for b.Loop() {
			cursor := view.LowerBound(key)
			for range 256 {
				bucket, _ = cursor.Bucket()
				cursor.Next()
			}
		}
		tabletAnchorMapLabBenchmarkID = bucket
		b.ReportMetric(view.BytesPerAnchor(), "B/anchor")
		b.ReportMetric(view.BytesPerAnchor()/230, "B/document")
	})
}

func BenchmarkTabletAnchorHandleLabCombinedRoute(b *testing.B) {
	for _, geometry := range []struct {
		name      string
		localBits uint8
		leafCount int
		rows      int
	}{
		{name: "18-12/3072-leaves", localBits: 12, leafCount: 3072, rows: 187},
		{name: "17-13/4300-leaves", localBits: 13, leafCount: 4300, rows: 187},
	} {
		b.Run(geometry.name, func(b *testing.B) {
			view, leaves := tabletAnchorHandleLabTest(
				b, geometry.localBits, geometry.leafCount,
			)
			target := geometry.leafCount / 2
			key := []byte(fmt.Sprintf(
				"tenant/0042/document/%08d", target*230,
			))
			hash := KeyHashBytes(tabletAnchorMapLabTestSeed, key)
			b.ReportMetric(
				view.CombinedBytesPerAnchor(), "B/anchor",
			)
			b.ReportMetric(
				view.CombinedBytesPerAnchor()/float64(geometry.rows),
				"B/document",
			)
			b.Run("hash-included", func(b *testing.B) {
				b.ReportAllocs()
				var route TabletAnchorHandleLabRoute
				for b.Loop() {
					route = view.Route(tabletAnchorMapLabTestSeed, key)
				}
				tabletAnchorHandleBenchmarkRoute = route
				b.ReportMetric(
					view.CombinedBytesPerAnchor(), "B/anchor",
				)
				b.ReportMetric(
					view.CombinedBytesPerAnchor()/float64(geometry.rows),
					"B/document",
				)
			})
			b.Run("reused-hash", func(b *testing.B) {
				b.ReportAllocs()
				var route TabletAnchorHandleLabRoute
				for b.Loop() {
					route = view.RouteHashed(hash, key)
				}
				tabletAnchorHandleBenchmarkRoute = route
				b.ReportMetric(
					view.CombinedBytesPerAnchor(), "B/anchor",
				)
				b.ReportMetric(
					view.CombinedBytesPerAnchor()/float64(geometry.rows),
					"B/document",
				)
			})
			b.Run("resolve-bucket-id", func(b *testing.B) {
				b.ReportAllocs()
				var ref PageRef
				var zone BucketZone
				for b.Loop() {
					ref, zone, _ = view.ResolveBucketID(
						leaves[target].Bucket,
					)
				}
				tabletAnchorHandleBenchmarkRef = ref
				tabletAnchorHandleBenchmarkZone = zone
				b.ReportMetric(
					view.CombinedBytesPerAnchor(), "B/anchor",
				)
				b.ReportMetric(
					view.CombinedBytesPerAnchor()/float64(geometry.rows),
					"B/document",
				)
			})
		})
	}
}

func BenchmarkTabletAnchorMapLabCOWBatchRewrite(b *testing.B) {
	anchors := tabletAnchorMapLabTestAnchors(4300)
	view := openTabletAnchorMapLabTest(b, anchors)
	maxFence := int(view.maxFence)
	scratch := make([]byte, maxFence*2)
	dst := make([]byte, 1<<20)

	for _, editCount := range []int{1, 8} {
		edits := make([]TabletAnchorMapLabEdit, editCount)
		for rank := range edits {
			// Existing fences are multiples of 230; these lie strictly between
			// neighbors and model batched ordered-leaf splits.
			value := 500_001 + rank*10_001
			edits[rank] = TabletAnchorMapLabEdit{
				Operation: TabletAnchorMapLabInsert,
				Fence: []byte(fmt.Sprintf(
					"tenant/0042/document/%08d", value,
				)),
				Bucket: BucketID(10_000 + rank),
			}
		}
		b.Run(fmt.Sprintf("%d-insert", editCount), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(view.PersistentBytes())))
			b.ResetTimer()
			var image []byte
			for b.Loop() {
				var err error
				image, err = view.ApplyBatch(
					dst, scratch, view.header.Generation+1, edits,
				)
				if err != nil {
					b.Fatal(err)
				}
			}
			tabletAnchorMapLabBenchmarkImage = image
		})
	}
}
