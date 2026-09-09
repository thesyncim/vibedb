package storeio

import (
	"fmt"
	"sync/atomic"
	"testing"
)

var residentPrimaryRouterScalingSink *ResidentPrimaryRouter
var residentPrimaryRouterScalingRouteSink ResidentPrimaryRoute

func residentPrimaryRouterScalingFixture(tb testing.TB, count int) *ResidentPrimaryRouter {
	tb.Helper()
	var fences []byte
	rows := make([]uint64, count*residentPrimaryRouterWords)
	for rank := range count {
		start := uint32(len(fences))
		if rank != 0 {
			fences = fmt.Appendf(fences, "f%06d", rank)
		}
		end := uint32(len(fences))
		tabletID := uint32(rank / (TabletLocalIdentityLocalCount - 4))
		localID := uint32(rank % (TabletLocalIdentityLocalCount - 4))
		bucketValue, ok := MakeTabletLocalIdentityBucket(tabletID, localID)
		if !ok {
			tb.Fatalf("bucket for rank %d", rank)
		}
		bucket := BucketID(bucketValue)
		at := rank * residentPrimaryRouterWords
		rows[at] = uint64(start) | uint64(end)<<32
		rows[at+1] = uint64(4096 * (rank + 1))
		rows[at+2] = 100
		rows[at+3] = uint64(4096) | uint64(uint32(bucket))<<32
	}
	router := &ResidentPrimaryRouter{
		storeID: [16]byte{0x73, 0x63, 0x61, 0x6c, 0x65},
		fences:  fences, rows: rows,
		hints: make([]pageCacheFrameHint, count),
		empty: make([]atomic.Uint32, count),
	}
	residentPrimaryRouterScalingFinalize(router)
	router.generation.Store(100)
	return router
}

func BenchmarkResidentPrimaryRouterScaling(b *testing.B) {
	for _, count := range []int{1 << 10, 1 << 14, 1 << 17} {
		b.Run(fmt.Sprintf("leaves=%d", count), func(b *testing.B) {
			router := residentPrimaryRouterScalingFixture(b, count)
			residentBytes := float64(router.ResidentBytes())
			middle := count / 2
			route := mustResidentRoute(b, router, middle)
			tabletID, sourceLocal, ok := SplitTabletLocalIdentityBucket(uint32(route.Bucket))
			if !ok {
				b.Fatal("source bucket")
			}
			left := route.Ref
			left.Offset += uint64(count+1) * 4096
			left.Generation = 101
			rightBucketValue, ok := MakeTabletLocalIdentityBucket(
				tabletID, TabletLocalIdentityLocalCount-4)
			if !ok {
				b.Fatal("right bucket")
			}
			rightBucket := BucketID(rightBucketValue)
			rightLogical, ok := SegmentedTabletRouterLeafLogicalID(rightBucket)
			if !ok {
				b.Fatal("right logical ID")
			}
			right := PageRef{Offset: left.Offset + 4096, LogicalID: rightLogical,
				Generation: 101, Length: 4096, Kind: PagePrimaryLeaf}
			splitFence := []byte(fmt.Sprintf("f%06dz", middle))

			b.Run("split-middle", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					var err error
					residentPrimaryRouterScalingSink, err = router.SplitLeaf(
						route, left, rightBucket, splitFence, right, 101)
					if err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(residentBytes, "resident-B")
				b.ReportMetric(float64(count), "source-routes/op")
			})
			b.Run("partition-middle-3", func(b *testing.B) {
				replacements := make([]SegmentedTabletRouterLeaf, 3)
				locals := []uint16{uint16(sourceLocal), TabletLocalIdentityLocalCount - 4, TabletLocalIdentityLocalCount - 3}
				fences := [][]byte{router.fence(middle), []byte(fmt.Sprintf("f%06da", middle)), []byte(fmt.Sprintf("f%06db", middle))}
				for i := range replacements {
					bucketValue, _ := MakeTabletLocalIdentityBucket(tabletID, uint32(locals[i]))
					logicalID, _ := SegmentedTabletRouterLeafLogicalID(BucketID(bucketValue))
					replacements[i] = SegmentedTabletRouterLeaf{LocalID: locals[i], Fence: fences[i], Ref: PageRef{
						Offset: left.Offset + uint64(i)*4096, LogicalID: logicalID,
						Generation: 101, Length: 4096, Kind: PagePrimaryLeaf}}
				}
				b.ReportAllocs()
				for b.Loop() {
					var err error
					residentPrimaryRouterScalingSink, err = router.SplitLeafPartition(route, replacements, 101)
					if err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(residentBytes, "resident-B")
			})
			b.Run("remove-middle", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					var err error
					residentPrimaryRouterScalingSink, err = router.RemoveLeaf(route, 101)
					if err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(residentBytes, "resident-B")
			})
			b.Run("route-hit", func(b *testing.B) {
				keys := make([][]byte, 512)
				for i := range keys {
					keys[i] = []byte(fmt.Sprintf("f%06d", (i*8191)%count))
				}
				b.ReportAllocs()
				at := 0
				for b.Loop() {
					var found bool
					residentPrimaryRouterScalingRouteSink, found = router.Route(keys[at&511])
					if !found {
						b.Fatal("route hit missed")
					}
					at++
				}
				b.ReportMetric(residentBytes, "resident-B")
			})
			b.Run("route-before-first", func(b *testing.B) {
				key := []byte("a-before-first")
				b.ReportAllocs()
				for b.Loop() {
					residentPrimaryRouterScalingRouteSink, _ = router.Route(key)
				}
				b.ReportMetric(residentBytes, "resident-B")
			})
			b.Run("route-walk-256", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					for offset := range 256 {
						residentPrimaryRouterScalingRouteSink, _ = router.RouteAtRank(
							(middle + offset) % count)
					}
				}
				b.ReportMetric(residentBytes, "resident-B")
			})
			b.Run("resolve-bucket-rotating", func(b *testing.B) {
				buckets := make([]BucketID, 512)
				for i := range buckets {
					buckets[i] = mustResidentRoute(b, router, (i*8191)%count).Bucket
				}
				b.ReportAllocs()
				at := 0
				for b.Loop() {
					var found bool
					residentPrimaryRouterScalingRouteSink, found = router.ResolveBucketID(buckets[at&511])
					if !found {
						b.Fatal("bucket lookup missed")
					}
					at++
				}
				b.ReportMetric(residentBytes, "resident-B")
			})
		})
	}
}
