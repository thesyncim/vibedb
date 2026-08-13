// Package gateway implements the stateless routing tier above the leader-only
// shard services: a thin shard client, an immutable authoritative catalog
// snapshot with atomic publication and durable persistence, and the endpoint
// membership that resolves an opaque endpoint to a network address.
//
// A gateway holds no authoritative state of its own. It reads a whole immutable
// [Snapshot] generation from a [CatalogHolder] once per operation and pins it,
// so routing and execution never mix two generations. The control plane hands
// the gateway complete snapshots; the gateway validates and atomically
// publishes them monotonically. [FileCatalogRefresher] reloads an atomically
// replaced snapshot after a shard reports stale routing metadata.
//
// Distributed queries accept SQL and typed parameters, not caller-authored
// routing facts. Each immutable snapshot owns a compact sorted table directory
// and a bounded generation-local prepared-plan cache. The shared SQL routing
// compiler derives shard constraints, merge ordering, and global limits; query
// shapes without a proven cross-shard merge or colocation rule fail before any
// shard is contacted.
//
// The client reuses both shardservice's length-prefixed codec and a bounded set
// of persistent net.Conns: one synchronous request/response round-trip per
// exclusively borrowed connection. It maps a
// shard's typed error frame back onto the distribution ownership sentinels and
// onto gateway sentinels for deadline, resource-limit, read-only, unsupported
// consistency, indeterminate-completion, and malformed refusals,
// so a caller matches a failure with errors.Is.
//
// [SessionVector] is a bounded immutable aggregate of per-shard logical
// positions, but it is intentionally not attached to Query or Result yet. The
// current catalog cannot prove position continuity across split/merge topology
// changes; routing integration requires a layout-provenance fence and certified
// lineage translation rather than dropping or numerically comparing positions.
//
// This package uses only repository-native routing, SQL, durability, and shard
// wire types; it imports no network-RPC or serialization framework.
package gateway
