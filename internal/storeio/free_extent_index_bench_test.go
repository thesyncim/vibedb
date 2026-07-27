package storeio

import "testing"

var freeExtentIndexBenchmarkResult int

func BenchmarkFreeExtentIndexFirstFit(b *testing.B) {
	for _, count := range []int{256, 4_096, 46_200} {
		extents := freeExtentIndexTestExtents(count)
		for i := range extents {
			extents[i].Length = 4 << 10
		}
		// Large requests cross nearly the whole offset-sorted slice. This is
		// the fragmentation case the hierarchy exists to accelerate.
		extents[count-1].Length = 64 << 10
		storage := make([]uint64, FreeExtentIndexCapacity(count))
		var index FreeExtentIndex
		if !index.Rebuild(extents, storage) {
			b.Fatal("Rebuild failed")
		}

		b.Run(testCountName(count)+"/4KiB/linear", func(b *testing.B) {
			benchmarkLinearFreeExtentFirstFit(b, extents, 4<<10)
		})
		b.Run(testCountName(count)+"/4KiB/indexed", func(b *testing.B) {
			benchmarkIndexedFreeExtentFirstFit(b, &index, extents, 4<<10)
		})
		b.Run(testCountName(count)+"/64KiB/linear", func(b *testing.B) {
			benchmarkLinearFreeExtentFirstFit(b, extents, 64<<10)
		})
		b.Run(testCountName(count)+"/64KiB/indexed", func(b *testing.B) {
			benchmarkIndexedFreeExtentFirstFit(b, &index, extents, 64<<10)
		})
	}
}

func BenchmarkFreeExtentIndexUpdate(b *testing.B) {
	for _, count := range []int{256, 4_096, 46_200} {
		extents := freeExtentIndexTestExtents(count)
		storage := make([]uint64, FreeExtentIndexCapacity(count))
		var index FreeExtentIndex
		if !index.Rebuild(extents, storage) {
			b.Fatal("Rebuild failed")
		}
		rank := count / 2
		b.Run(testCountName(count), func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				extents[rank].Length ^= 4 << 10
				if !index.Update(extents, rank) {
					b.Fatal("Update failed")
				}
			}
		})
	}
}

func benchmarkLinearFreeExtentFirstFit(b *testing.B, extents []FreeExtent, want uint64) {
	b.ReportAllocs()
	result := 0
	for iteration := 0; iteration < b.N; iteration++ {
		rank, ok := linearFreeExtentFirstFit(extents, want)
		if !ok {
			b.Fatal("FirstFit failed")
		}
		result += rank
	}
	freeExtentIndexBenchmarkResult = result
}

func benchmarkIndexedFreeExtentFirstFit(
	b *testing.B, index *FreeExtentIndex, extents []FreeExtent, want uint64,
) {
	b.ReportAllocs()
	result := 0
	for iteration := 0; iteration < b.N; iteration++ {
		rank, ok := index.FirstFit(extents, want)
		if !ok {
			b.Fatal("FirstFit failed")
		}
		result += rank
	}
	freeExtentIndexBenchmarkResult = result
}
