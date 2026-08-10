// Package shardservice implements the first deployable leader-only shard service:
// the request/response types a gateway and a shard exchange, a stdlib-only
// length-prefixed codec for them, the static ownership admission gate a shard
// applies before it parses or executes anything, and a [Server] that executes an
// admitted statement locally through a borrowed sql/driver Session.
//
// The wire contract carries SQL text plus typed bound parameters, never a
// serialized execution plan or ConstraintProgram. A shard parses and plans the
// statement locally with the ordinary vibedb parser and planner, so no second
// frozen distributed plan format is introduced. Parameters use a typed
// representation that refines sql/driver's scalar/document split and materialize
// into standard-library values the local Session accepts.
//
// One connection is served by one goroutine that owns one single-consumer
// Session, mirroring pgwire. Each request's lifecycle is admit, pin a read
// snapshot (reads) or autocommit (writes), prepare the SQL text, bind the typed
// parameters, enforce the safe-zero read-only execution intent, execute, stream
// the result, and release the snapshot. Direct writers opt into read-write;
// distributed gateway requests are always read-only.
//
// The wire also reserves bounded logical applied positions for session reads.
// This leader-only phase has no replicated apply log: admission rejects every
// session read and every present minimum before SQL preparation, and successful
// strong reads never claim a read position. A later replicated apply path must
// supply the real log identity and applied-index proof before enabling them.
//
// This package depends only on the standard library, vibedb's distribution and
// query types, the sql/driver runtime, and durability error identity it carries
// over the wire; it imports no network-RPC or serialization framework.
package shardservice
