package storeio

import "testing"

var retiredIntervalBenchmarkSink bool

func BenchmarkRetiredIntervalIndexLookup(b *testing.B) {
	for _, count := range []int{257, 4_096, 65_536} {
		index := newRetiredIntervalIndex(count)
		for rank := range count {
			if !index.insert(uint64(rank+1)*8192, 4096) {
				b.Fatal("index setup failed")
			}
		}
		b.Run(testCountName(count)+"/hit", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				retiredIntervalBenchmarkSink =
					index.overlaps(uint64(count/2+1)*8192, 1)
			}
		})
		b.Run(testCountName(count)+"/gap", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				retiredIntervalBenchmarkSink =
					index.overlaps(uint64(count+2)*8192, 4096)
			}
		})
		b.Run(testCountName(count)+"/mixed", func(b *testing.B) {
			b.ReportAllocs()
			for rank := 0; b.Loop(); rank++ {
				offset := uint64(count+2) * 8192
				if rank&1 == 0 {
					offset = uint64(rank%count+1) * 8192
				}
				retiredIntervalBenchmarkSink = index.overlaps(offset, 1)
			}
		})
	}
}

func BenchmarkExtentReclaimerIndexBuild(b *testing.B) {
	for _, count := range []int{257, 4_096, 65_536} {
		b.Run(testCountName(count), func(b *testing.B) {
			leases, err := NewGenerationLeases(
				GenerationLeaseOptions{MaxLeases: 1},
			)
			if err != nil {
				b.Fatal(err)
			}
			indexStorage := make(
				[]byte, RetiredIntervalIndexStorageBytes(count),
			)
			extentStorage := make(
				[]byte, RetiredExtentStorageBytes(count),
			)
			reclaimer, err := NewExtentReclaimer(
				leases,
				ExtentReclaimerOptions{
					MaxRetiredExtents:    count,
					IntervalIndexStorage: indexStorage,
					RetiredExtentStorage: extentStorage,
				},
			)
			if err != nil {
				b.Fatal(err)
			}
			extents := make([]FreeExtent, count)
			for rank := range count {
				extents[rank] = FreeExtent{
					Offset: uint64(rank+1) * 8192,
					Length: 4096, RetiredGeneration: 1,
				}
			}
			b.ReportAllocs()
			for b.Loop() {
				if err := reclaimer.Restore(extents); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				reclaimer.resetPendingLocked()
				b.StartTimer()
			}
		})
	}
}

func BenchmarkExtentReclaimerOpenArena(b *testing.B) {
	const capacity = 65_536
	indexStorage := make(
		[]byte, RetiredIntervalIndexStorageBytes(capacity),
	)
	extentStorage := make(
		[]byte, RetiredExtentStorageBytes(capacity),
	)
	leases, err := NewGenerationLeases(
		GenerationLeaseOptions{MaxLeases: 1},
	)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		var constructErr error
		extentReclaimerTestSink, constructErr = NewExtentReclaimer(
			leases,
			ExtentReclaimerOptions{
				MaxRetiredExtents:    capacity,
				IntervalIndexStorage: indexStorage,
				RetiredExtentStorage: extentStorage,
			},
		)
		if constructErr != nil {
			b.Fatal(constructErr)
		}
	}
}

func BenchmarkExtentReclaimerAppendReusableFullDestination(b *testing.B) {
	leases, err := NewGenerationLeases(
		GenerationLeaseOptions{MaxLeases: 1},
	)
	if err != nil {
		b.Fatal(err)
	}
	reclaimer, err := NewExtentReclaimer(
		leases, ExtentReclaimerOptions{MaxRetiredExtents: 1},
	)
	if err != nil {
		b.Fatal(err)
	}
	if err := reclaimer.Retire(FreeExtent{
		Offset: 4096, Length: 4096, RetiredGeneration: 1,
	}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		dst, appendErr := reclaimer.AppendReusable(nil, 2, 2, 1)
		if appendErr != nil || len(dst) != 0 {
			b.Fatal("full destination changed")
		}
	}
}

func BenchmarkExtentReclaimerRetireAgainstFencedSet(b *testing.B) {
	for _, count := range []int{
		64, 128, 256, 257, 512, 1_024, 4_096, 65_536,
	} {
		b.Run(testCountName(count), func(b *testing.B) {
			leases, err := NewGenerationLeases(
				GenerationLeaseOptions{MaxLeases: 1},
			)
			if err != nil {
				b.Fatal(err)
			}
			reclaimer, err := NewExtentReclaimer(
				leases,
				ExtentReclaimerOptions{MaxRetiredExtents: count + 1},
			)
			if err != nil {
				b.Fatal(err)
			}
			restored := make([]FreeExtent, count)
			for rank := range restored {
				restored[rank] = FreeExtent{
					Offset: uint64(rank+1) * 8192,
					Length: 4096, RetiredGeneration: 1,
				}
			}
			if err := reclaimer.Restore(restored); err != nil {
				b.Fatal(err)
			}
			candidate := FreeExtent{
				Offset: uint64(count+2) * 8192,
				Length: 4096, RetiredGeneration: 2,
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := reclaimer.Retire(candidate); err != nil {
					b.Fatal(err)
				}
				if err := reclaimer.CancelRetiredGeneration(2); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkExtentReclaimerLinearOverlapOracle(b *testing.B) {
	for _, count := range []int{
		64, 128, 256, 257, 512, 1_024, 4_096, 65_536,
	} {
		b.Run(testCountName(count), func(b *testing.B) {
			held := make([]FreeExtent, count)
			for rank := range held {
				held[rank] = FreeExtent{
					Offset: uint64(rank+1) * 8192,
					Length: 4096, RetiredGeneration: 1,
				}
			}
			candidate := FreeExtent{
				Offset: uint64(count+2) * 8192,
				Length: 4096, RetiredGeneration: 2,
			}
			b.ReportAllocs()
			for b.Loop() {
				if retiredIntervalLinearOverlap(held, candidate) {
					b.Fatal("unexpected overlap")
				}
			}
		})
	}
}

func retiredIntervalLinearOverlap(
	held []FreeExtent, candidate FreeExtent,
) bool {
	end := candidate.Offset + candidate.Length
	for _, extent := range held {
		if candidate.Offset < extent.Offset+extent.Length &&
			extent.Offset < end {
			return true
		}
	}
	return false
}
