package gateway

import (
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

// Metrics accumulates route and fan-out counters across every operation an
// Executor runs. Its fields are atomic and safe for concurrent update; a caller
// reads a consistent-enough view with Snapshot. It is not copied by value.
type Metrics struct {
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
	switch kind {
	case distribution.RouteSingle:
		m.routeSingle.Add(1)
	case distribution.RouteTargeted:
		m.routeTargeted.Add(1)
	case distribution.RouteScatter:
		m.routeScatter.Add(1)
	default:
		m.routeEmpty.Add(1)
	}
	if shards > 0 {
		m.shardsFanned.Add(uint64(shards))
	}
	switch reason {
	case ScatterAllShards:
		m.scatterAllShards.Add(1)
	case ScatterUnknownRoute:
		m.scatterUnknown.Add(1)
	}
}

// observeResult records the rows and bytes one completed operation returned.
func (m *Metrics) observeResult(rows, bytes uint64) {
	m.rowsReturned.Add(rows)
	m.bytesReturned.Add(bytes)
}

// observeRetry records one stale-generation retry.
func (m *Metrics) observeRetry() { m.retries.Add(1) }

// Snapshot returns a plain copy of the current counters.
func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		RouteSingle:         m.routeSingle.Load(),
		RouteTargeted:       m.routeTargeted.Load(),
		RouteScatter:        m.routeScatter.Load(),
		RouteEmpty:          m.routeEmpty.Load(),
		ShardsFanned:        m.shardsFanned.Load(),
		RowsReturned:        m.rowsReturned.Load(),
		BytesReturned:       m.bytesReturned.Load(),
		Retries:             m.retries.Load(),
		ScatterAllShards:    m.scatterAllShards.Load(),
		ScatterUnknownRoute: m.scatterUnknown.Load(),
	}
}
