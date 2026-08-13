package durable

import (
	"sync"
	"testing"
	"unsafe"
)

func TestStatsHistogramFootprintBound(t *testing.T) {
	const maxBytes = 192
	if got := unsafe.Sizeof(atomicStatsHistogram{}); got > maxBytes {
		t.Fatalf("atomic histogram footprint = %d bytes, limit %d", got, maxBytes)
	}
	if got := unsafe.Sizeof(StatsHistogram{}); got > maxBytes {
		t.Fatalf("snapshot histogram footprint = %d bytes, limit %d", got, maxBytes)
	}
}

func TestAtomicStatsHistogramSnapshot(t *testing.T) {
	var histogram atomicStatsHistogram
	for _, value := range []uint64{0, 1, 2, 3, 4, 7, 8, 9} {
		histogram.observe(value)
	}

	got := histogram.snapshot()
	if got.Count != 8 || got.Sum != 34 || got.Max != 9 {
		t.Fatalf("summary = count %d sum %d max %d", got.Count, got.Sum, got.Max)
	}
	wantBuckets := map[int]uint64{0: 1, 1: 1, 2: 6}
	for index, count := range got.Buckets {
		if count != wantBuckets[index] {
			t.Fatalf("bucket %d = %d, want %d", index, count, wantBuckets[index])
		}
	}
}

func TestAtomicStatsHistogramConcurrentObserve(t *testing.T) {
	var histogram atomicStatsHistogram
	const workers = 8
	const observations = 1_000
	var wait sync.WaitGroup
	wait.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			defer wait.Done()
			for range observations {
				histogram.observe(8)
			}
		}()
	}
	wait.Wait()

	got := histogram.snapshot()
	want := uint64(workers * observations)
	if got.Count != want || got.Sum != want*8 || got.Max != 8 ||
		got.Buckets[2] != want {
		t.Fatalf("snapshot = %+v, want count %d in bucket 2", got, want)
	}
}

func TestStatsHistogramBucketBoundaries(t *testing.T) {
	tests := map[uint64]int{
		0: 0, 1: 1, 2: 2, 16: 2, 17: 3, 256: 3, 257: 4,
		^uint64(0): StatsHistogramBuckets - 1,
	}
	for value, want := range tests {
		if got := statsHistogramBucket(value); got != want {
			t.Fatalf("bucket(%d) = %d, want %d", value, got, want)
		}
	}
}
