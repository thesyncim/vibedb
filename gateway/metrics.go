package gateway

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/vibedb/distribution"
)

// LatencyBucketCount is the fixed number of logarithmic latency histogram
// buckets tracked per operation class (read and write). The first 15 buckets
// correspond to LatencyBoundsUS; the final bucket counts observations strictly
// above the last finite bound (+Inf).
const LatencyBucketCount = 16

// LatencyBoundsUS lists the inclusive upper bounds in microseconds for the 15
// finite latency histogram buckets. Bucket 15 is the +Inf overflow bucket.
var LatencyBoundsUS = [LatencyBucketCount - 1]uint64{
	50, 100, 250, 500,
	1_000, 2_500, 5_000, 10_000,
	25_000, 50_000, 100_000, 250_000,
	500_000, 1_000_000, 5_000_000,
}

func latencyBucketIndex(us uint64) int {
	for i, bound := range LatencyBoundsUS {
		if us <= bound {
			return i
		}
	}
	return LatencyBucketCount - 1
}

// LatencyHistogramSnapshot is a plain, copyable snapshot of one fixed-bucket
// microsecond latency histogram. Buckets are non-cumulative counts per bucket:
// Buckets[i] counts observations in (bound[i-1], bound[i]] (or [0, bound[0]]
// for i == 0, and > bound[14] for i == 15).
type LatencyHistogramSnapshot struct {
	Count   uint64
	SumUS   uint64
	Buckets [LatencyBucketCount]uint64
}

// QuantileUS returns the inclusive upper bound in microseconds of the bucket
// containing quantile q (0 < q <= 1). It returns 0 when Count == 0, and the
// largest finite bound when the quantile lands in the +Inf overflow bucket.
func (h LatencyHistogramSnapshot) QuantileUS(q float64) uint64 {
	if h.Count == 0 || q <= 0 {
		return 0
	}
	if q > 1 {
		q = 1
	}
	target := uint64(float64(h.Count) * q)
	if target == 0 {
		target = 1
	}
	var seen uint64
	for i, c := range h.Buckets {
		seen += c
		if seen >= target {
			if i < len(LatencyBoundsUS) {
				return LatencyBoundsUS[i]
			}
			return LatencyBoundsUS[len(LatencyBoundsUS)-1]
		}
	}
	return LatencyBoundsUS[len(LatencyBoundsUS)-1]
}

// Route and fan-out metrics: a small set of lock-free counters a caller reads to
// observe how the gateway routed and fanned out its operations.

// ScatterReason explains why a route was classified as scatter for admission and
// metrics. A route covering every active shard is scatter regardless of how it
// was derived.
type ScatterReason uint8

const (
	// ScatterNone is a route that did not scatter.
	ScatterNone ScatterReason = iota
	// ScatterAllShards is a route whose bound key nonetheless selected every
	// active shard.
	ScatterAllShards
	// ScatterUnknownRoute is a route with no usable bound prefix, which fans out
	// to every active shard.
	ScatterUnknownRoute
)

// String renders the reason name for diagnostics.
func (r ScatterReason) String() string {
	switch r {
	case ScatterNone:
		return "None"
	case ScatterAllShards:
		return "AllShards"
	case ScatterUnknownRoute:
		return "UnknownRoute"
	default:
		return "Invalid"
	}
}

type metricsShard struct {
	routeSingle   atomic.Uint64
	routeTargeted atomic.Uint64
	routeScatter  atomic.Uint64
	routeEmpty    atomic.Uint64

	shardsFanned  atomic.Uint64
	rowsReturned  atomic.Uint64
	bytesReturned atomic.Uint64
	retries       atomic.Uint64

	scatterAllShards atomic.Uint64
	scatterUnknown   atomic.Uint64

	readLatencyCount   atomic.Uint64
	readLatencySumUS   atomic.Uint64
	readLatencyBuckets [LatencyBucketCount]atomic.Uint64

	writeLatencyCount   atomic.Uint64
	writeLatencySumUS   atomic.Uint64
	writeLatencyBuckets [LatencyBucketCount]atomic.Uint64

	// Pad to a cache-line multiple so per-P shards never share cache lines.
	_ [16]byte
}

// Metrics accumulates route and fan-out counters across every operation an
// Executor runs. Its fields are sharded per-P via sync.Pool so concurrent
// readers never contend on a shared counter cache line; a caller reads a
// consistent-enough view with Snapshot. It is not copied by value.
type Metrics struct {
	pool sync.Pool
	mu   sync.Mutex
	all  []*metricsShard
}

func (m *Metrics) acquireShard() *metricsShard {
	if s, ok := m.pool.Get().(*metricsShard); ok {
		return s
	}
	s := new(metricsShard)
	m.mu.Lock()
	m.all = append(m.all, s)
	m.mu.Unlock()
	return s
}

// MetricsSnapshot is a plain, copyable view of the counters and latency
// histograms at one instant.
type MetricsSnapshot struct {
	RouteSingle   uint64
	RouteTargeted uint64
	RouteScatter  uint64
	RouteEmpty    uint64

	ShardsFanned  uint64
	RowsReturned  uint64
	BytesReturned uint64
	Retries       uint64

	ScatterAllShards    uint64
	ScatterUnknownRoute uint64

	ReadLatency  LatencyHistogramSnapshot
	WriteLatency LatencyHistogramSnapshot
}

// observeRoute records one routed operation: its kind, the shards it fanned to,
// and the scatter reason when it scattered.
func (m *Metrics) observeRoute(kind distribution.RouteKind, shards int, reason ScatterReason) {
	s := m.acquireShard()
	switch kind {
	case distribution.RouteSingle:
		s.routeSingle.Add(1)
	case distribution.RouteTargeted:
		s.routeTargeted.Add(1)
	case distribution.RouteScatter:
		s.routeScatter.Add(1)
	default:
		s.routeEmpty.Add(1)
	}
	if shards > 0 {
		s.shardsFanned.Add(uint64(shards))
	}
	switch reason {
	case ScatterAllShards:
		s.scatterAllShards.Add(1)
	case ScatterUnknownRoute:
		s.scatterUnknown.Add(1)
	}
	m.pool.Put(s)
}

// observeResult records the rows and bytes one completed operation returned.
func (m *Metrics) observeResult(rows, bytes uint64) {
	s := m.acquireShard()
	s.rowsReturned.Add(rows)
	s.bytesReturned.Add(bytes)
	m.pool.Put(s)
}

// observeRetry records one stale-generation retry.
func (m *Metrics) observeRetry() {
	s := m.acquireShard()
	s.retries.Add(1)
	m.pool.Put(s)
}

// ObserveReadLatency records one completed gateway read duration in the
// per-P sharded microsecond latency histogram.
func (m *Metrics) ObserveReadLatency(d time.Duration) {
	if m == nil {
		return
	}
	us := uint64(0)
	if d > 0 {
		us = uint64(d / time.Microsecond)
	}
	b := latencyBucketIndex(us)
	s := m.acquireShard()
	s.readLatencyCount.Add(1)
	s.readLatencySumUS.Add(us)
	s.readLatencyBuckets[b].Add(1)
	m.pool.Put(s)
}

// ObserveWriteLatency records one completed gateway write duration in the
// per-P sharded microsecond latency histogram.
func (m *Metrics) ObserveWriteLatency(d time.Duration) {
	if m == nil {
		return
	}
	us := uint64(0)
	if d > 0 {
		us = uint64(d / time.Microsecond)
	}
	b := latencyBucketIndex(us)
	s := m.acquireShard()
	s.writeLatencyCount.Add(1)
	s.writeLatencySumUS.Add(us)
	s.writeLatencyBuckets[b].Add(1)
	m.pool.Put(s)
}

// Snapshot returns a plain copy of the current counters and latency histograms.
func (m *Metrics) Snapshot() MetricsSnapshot {
	m.mu.Lock()
	shards := m.all
	m.mu.Unlock()
	var snap MetricsSnapshot
	for _, s := range shards {
		snap.RouteSingle += s.routeSingle.Load()
		snap.RouteTargeted += s.routeTargeted.Load()
		snap.RouteScatter += s.routeScatter.Load()
		snap.RouteEmpty += s.routeEmpty.Load()
		snap.ShardsFanned += s.shardsFanned.Load()
		snap.RowsReturned += s.rowsReturned.Load()
		snap.BytesReturned += s.bytesReturned.Load()
		snap.Retries += s.retries.Load()
		snap.ScatterAllShards += s.scatterAllShards.Load()
		snap.ScatterUnknownRoute += s.scatterUnknown.Load()
		snap.ReadLatency.Count += s.readLatencyCount.Load()
		snap.ReadLatency.SumUS += s.readLatencySumUS.Load()
		for i := range snap.ReadLatency.Buckets {
			snap.ReadLatency.Buckets[i] += s.readLatencyBuckets[i].Load()
		}
		snap.WriteLatency.Count += s.writeLatencyCount.Load()
		snap.WriteLatency.SumUS += s.writeLatencySumUS.Load()
		for i := range snap.WriteLatency.Buckets {
			snap.WriteLatency.Buckets[i] += s.writeLatencyBuckets[i].Load()
		}
	}
	return snap
}
