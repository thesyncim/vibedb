// Package gateway implements the development routing and execution layer for
// both static shards and the RF3 replicated path.
//
// Static execution pins an immutable [Snapshot], routes SQL to shardservice
// endpoints, and coordinates the bounded multi-shard operations admitted by
// that catalog generation. Replicated execution uses [ReplicatedExecutor], the
// shard-native and control protocols, generation-fenced RF3 routes, request
// ledgers, and the package's transaction and recovery protocols. Remote command
// wiring can wrap these paths in authenticated serving; the executor types are
// not themselves an authentication boundary. The static and replicated paths
// have different contracts, so support in one does not promise that the other
// admits the same operation.
//
// This package is an unreleased integration surface, not a stable client API.
// See docs/operations/distributed.md and docs/reference/protocols.md for the
// current boundaries and failure semantics.
package gateway
