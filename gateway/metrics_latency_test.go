package gateway

import (
	"sync"
	"testing"
	"time"
)

func TestMetricsLatencyHistogramsBucketsQuantilesAndConcurrency(t *testing.T) {
	var m Metrics
	samples := []time.Duration{
		10 * time.Microsecond,     // bucket 0 (<=50us)
		50 * time.Microsecond,     // bucket 0 (<=50us)
		75 * time.Microsecond,     // bucket 1 (<=100us)
		1200 * time.Microsecond,   // bucket 5 (<=2500us)
		9 * time.Second,           // bucket 15 (+Inf >5s)
	}
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, d := range samples {
				m.ObserveReadLatency(d)
				m.ObserveWriteLatency(d * 2)
			}
		}()
	}
	wg.Wait()

	snap := m.Snapshot()
	if snap.ReadLatency.Count != uint64(4*len(samples)) {
		t.Fatalf("ReadLatency.Count = %d, want %d", snap.ReadLatency.Count, 4*len(samples))
	}
	if snap.WriteLatency.Count != uint64(4*len(samples)) {
		t.Fatalf("WriteLatency.Count = %d, want %d", snap.WriteLatency.Count, 4*len(samples))
	}
	if snap.ReadLatency.Buckets[0] != 8 {
		t.Fatalf("bucket[0] = %d, want 8", snap.ReadLatency.Buckets[0])
	}
	if snap.ReadLatency.Buckets[1] != 4 {
		t.Fatalf("bucket[1] = %d, want 4", snap.ReadLatency.Buckets[1])
	}
	if snap.ReadLatency.Buckets[5] != 4 {
		t.Fatalf("bucket[5] = %d, want 4", snap.ReadLatency.Buckets[5])
	}
	if snap.ReadLatency.Buckets[LatencyBucketCount-1] != 4 {
		t.Fatalf("overflow bucket = %d, want 4", snap.ReadLatency.Buckets[LatencyBucketCount-1])
	}
	if p50 := snap.ReadLatency.QuantileUS(0.50); p50 != 100 {
		t.Fatalf("p50 = %d us, want 100 us", p50)
	}
	if p95 := snap.ReadLatency.QuantileUS(0.95); p95 != LatencyBoundsUS[len(LatencyBoundsUS)-1] {
		t.Fatalf("p95 = %d us, want %d us", p95, LatencyBoundsUS[len(LatencyBoundsUS)-1])
	}
	var empty LatencyHistogramSnapshot
	if empty.QuantileUS(0.99) != 0 {
		t.Fatalf("empty QuantileUS = %d, want 0", empty.QuantileUS(0.99))
	}
}
