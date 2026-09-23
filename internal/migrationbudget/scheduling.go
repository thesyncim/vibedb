package migrationbudget

import (
	"math"
	"runtime/metrics"
)

const schedulingLatencyMetric = "/sched/latencies:seconds"

// SchedulingLatencyProbe reports the p99 time runnable goroutines waited for
// a CPU during each interval between calls. The runtime histogram is
// cumulative; the probe retains the previous counts and reduces only the
// interval delta, so one old stall cannot hold pressure high. It is not safe
// for concurrent use; one sampler owns it.
type SchedulingLatencyProbe struct {
	sample   []metrics.Sample
	previous []uint64
	delta    []uint64
}

// NewSchedulingLatencyProbe primes the baseline. It returns nil when the
// runtime does not export the scheduling-latency histogram.
func NewSchedulingLatencyProbe() *SchedulingLatencyProbe {
	probe := &SchedulingLatencyProbe{sample: []metrics.Sample{{Name: schedulingLatencyMetric}}}
	metrics.Read(probe.sample)
	if probe.sample[0].Value.Kind() != metrics.KindFloat64Histogram {
		return nil
	}
	histogram := probe.sample[0].Value.Float64Histogram()
	probe.previous = append([]uint64(nil), histogram.Counts...)
	probe.delta = make([]uint64, len(histogram.Counts))
	return probe
}

// P99Nanos returns the interval p99 scheduling latency, or zero when no
// goroutine was scheduled in the interval.
func (probe *SchedulingLatencyProbe) P99Nanos() uint64 {
	if probe == nil {
		return 0
	}
	metrics.Read(probe.sample)
	if probe.sample[0].Value.Kind() != metrics.KindFloat64Histogram {
		return 0
	}
	histogram := probe.sample[0].Value.Float64Histogram()
	if len(histogram.Counts) != len(probe.previous) {
		// The bucket layout is fixed for a runtime build; re-baseline if not.
		probe.previous = append(probe.previous[:0], histogram.Counts...)
		probe.delta = make([]uint64, len(histogram.Counts))
		return 0
	}
	var total uint64
	for index, count := range histogram.Counts {
		if count >= probe.previous[index] {
			probe.delta[index] = count - probe.previous[index]
		} else {
			probe.delta[index] = 0
		}
		total += probe.delta[index]
		probe.previous[index] = count
	}
	return histogramQuantileNanos(probe.delta, histogram.Buckets, total, 99)
}

// histogramQuantileNanos returns the upper bound of the bucket holding the
// requested percentile. buckets has len(counts)+1 boundaries in seconds.
func histogramQuantileNanos(counts []uint64, buckets []float64, total uint64, percentile uint64) uint64 {
	if total == 0 || len(buckets) != len(counts)+1 {
		return 0
	}
	rank := (total*percentile + 99) / 100
	var seen uint64
	for index, count := range counts {
		seen += count
		if seen < rank {
			continue
		}
		upper := buckets[index+1]
		if math.IsInf(upper, 1) {
			upper = buckets[index]
		}
		if upper <= 0 || math.IsNaN(upper) {
			return 0
		}
		nanos := upper * 1e9
		if nanos >= math.MaxUint64 {
			return math.MaxUint64
		}
		return uint64(nanos)
	}
	return 0
}
