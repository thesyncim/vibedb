package durable

import (
	"math/bits"
	"sync/atomic"
)

// StatsHistogramBuckets is the number of fixed logarithmic buckets exported by
// StatsHistogram. Bucket zero contains zero, bucket one contains one, and each
// later bucket spans the next power-of-16 range: [2,16], [17,256], and so on.
// Count, Sum, and Max remain exact. The coarse fixed ranges keep each live
// histogram to 168 bytes rather than charging a compact collection more than
// half a KiB for a diagnostic distribution.
const StatsHistogramBuckets = 18

// StatsHistogram is an allocation-free coarse logarithmic distribution snapshot. Sum
// and Max use the same unit as the observed value (nanoseconds for latency
// fields, bytes for byte fields, and a plain count for group-size fields).
// Buckets are fixed power-of-16 ranges so sampling never allocates and the
// snapshot is stable across benchmark processes and artifact formats.
type StatsHistogram struct {
	Count   uint64
	Sum     uint64
	Max     uint64
	Buckets [StatsHistogramBuckets]uint64
}

type atomicStatsHistogram struct {
	count   atomic.Uint64
	sum     atomic.Uint64
	max     atomic.Uint64
	buckets [StatsHistogramBuckets]atomic.Uint64
}

func (h *atomicStatsHistogram) observe(value uint64) {
	h.count.Add(1)
	h.sum.Add(value)
	h.buckets[statsHistogramBucket(value)].Add(1)
	for previous := h.max.Load(); value > previous &&
		!h.max.CompareAndSwap(previous, value); previous = h.max.Load() {
	}
}

func statsHistogramBucket(value uint64) int {
	if value < 2 {
		return int(value)
	}
	return 2 + (bits.Len64(value-1)-1)/4
}

func (h *atomicStatsHistogram) snapshot() StatsHistogram {
	if h == nil {
		return StatsHistogram{}
	}
	result := StatsHistogram{
		Count: h.count.Load(),
		Sum:   h.sum.Load(),
		Max:   h.max.Load(),
	}
	for index := range result.Buckets {
		result.Buckets[index] = h.buckets[index].Load()
	}
	return result
}
