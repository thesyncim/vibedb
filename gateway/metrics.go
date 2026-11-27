package gateway

import (
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/distribution"
)

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

	// Pad to 128 bytes so per-P shards never share cache lines.
	_ [48]byte
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

// MetricsSnapshot is a plain, copyable view of the counters at one instant.
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

// Snapshot returns a plain copy of the current counters.
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
	}
	return snap
}
