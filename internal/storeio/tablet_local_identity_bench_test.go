package storeio

import (
	"fmt"
	"testing"
)

var (
	tabletLocalIdentityBenchLocation TabletLocalIdentityLocation
	tabletLocalIdentityBenchImage    []byte
	tabletLocalIdentityBenchDesc     TabletLocalIdentityDescriptor
	tabletLocalIdentityBenchRetired  []TabletLocalIdentityRetirement
)

func BenchmarkTabletLocalIdentityResolve(b *testing.B) {
	view := tabletLocalIdentityTestView(b, 3686)
	b.Run("hit", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(2)
		b.ResetTimer()
		var location TabletLocalIdentityLocation
		for i := 0; i < b.N; i++ {
			location, _ = view.Resolve(uint16(i & 2047))
		}
		tabletLocalIdentityBenchLocation = location
	})
	b.Run("empty", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(2)
		b.ResetTimer()
		var location TabletLocalIdentityLocation
		for i := 0; i < b.N; i++ {
			location, _ = view.Resolve(4000)
		}
		tabletLocalIdentityBenchLocation = location
	})
}

func BenchmarkTabletLocalIdentityBatchUpdate(b *testing.B) {
	base := tabletLocalIdentityTestView(b, 3072)
	edits := make([]TabletLocalIdentityEdit, 32)
	for i := range edits {
		edits[i] = TabletLocalIdentityEdit{
			LocalID:   uint16(i * 64),
			Operation: TabletLocalIdentityMove,
			Location: TabletLocalIdentityLocation{
				AnchorPageID: 12,
				RowSlot:      uint8(i),
			},
		}
	}
	images := [2][]byte{
		make([]byte, TabletLocalIdentityImageBytes),
		make([]byte, TabletLocalIdentityImageBytes),
	}
	retirements := [2][]TabletLocalIdentityRetirement{
		make([]TabletLocalIdentityRetirement, 0, 64),
		make([]TabletLocalIdentityRetirement, 0, 64),
	}
	b.ReportAllocs()
	b.SetBytes(TabletLocalIdentityImageBytes)
	b.ResetTimer()
	view := base
	for i := 0; i < b.N; i++ {
		slot := i & 1
		image, descriptor, retired, err := RewriteTabletLocalIdentity(
			images[slot], retirements[slot][:0], view,
			uint64(i)+8, 1, edits,
		)
		if err != nil {
			b.Fatal(err)
		}
		view = TabletLocalIdentityView{
			image: image, retirements: retired, descriptor: descriptor,
		}
	}
	tabletLocalIdentityBenchImage = view.image
	tabletLocalIdentityBenchDesc = view.descriptor
	tabletLocalIdentityBenchRetired = view.retirements
}

func BenchmarkTabletLocalIdentityWalk(b *testing.B) {
	view := tabletLocalIdentityTestView(b, 3686)
	b.ReportAllocs()
	b.SetBytes(int64(view.Descriptor().LiveCount))
	b.ResetTimer()
	var location TabletLocalIdentityLocation
	for i := 0; i < b.N; i++ {
		cursor := view.Cursor()
		for {
			_, next, ok := cursor.Next()
			if !ok {
				break
			}
			location = next
		}
	}
	tabletLocalIdentityBenchLocation = location
}

func BenchmarkTabletLocalIdentityBytesPerLive(b *testing.B) {
	for _, occupancy := range []int{50, 75, 90} {
		live := TabletLocalIdentityLocalCount * occupancy / 100
		b.Run(fmt.Sprintf("%d-percent", occupancy), func(b *testing.B) {
			view := tabletLocalIdentityTestView(b, live)
			b.ReportAllocs()
			b.ResetTimer()
			var location TabletLocalIdentityLocation
			for i := 0; i < b.N; i++ {
				location, _ = view.Resolve(uint16(i % live))
			}
			tabletLocalIdentityBenchLocation = location
			b.ReportMetric(
				TabletLocalIdentityBytesPerLive(live),
				"B/live",
			)
			b.ReportMetric(
				float64(TabletLocalIdentityDescriptorBytes),
				"descriptor-B",
			)
			b.ReportMetric(
				float64(TabletLocalIdentityImageBytes),
				"locator-B",
			)
			b.ReportMetric(
				float64(TabletLocalIdentityMaxRetirementBytes),
				"worst-retired-B",
			)
		})
	}
}
