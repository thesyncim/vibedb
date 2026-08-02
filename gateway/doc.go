// Package gateway implements the stateless routing tier above the leader-only
// shard services: a thin shard client, an immutable authoritative catalog
// snapshot with atomic publication and durable persistence, and the endpoint
// membership that resolves an opaque endpoint to a network address.
//
// A gateway holds no authoritative state of its own. It reads a whole immutable
// [Snapshot] generation from a [CatalogHolder] once per operation and pins it,
// so routing and execution never mix two generations. The control plane hands
// the gateway complete snapshots; the gateway validates and atomically
// publishes them, and any process can reload the last published generation from
// disk.
//
// The client reuses the shardservice length-prefixed codec over an ordinary
// net.Conn: one synchronous request/response round-trip per call. It maps a
// shard's typed error frame back onto the distribution ownership sentinels and
// onto gateway sentinels for deadline, resource-limit, and malformed refusals,
// so a caller matches a failure with errors.Is.
//
// This package depends only on the standard library, vibedb's distribution
// types, and the shardservice wire contract it speaks; it adds no root-module
// dependency and imports no network-RPC or serialization framework.
package gateway
