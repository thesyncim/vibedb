// Package shardservice implements the experimental leader-only shard service:
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
// The current service has no replicated apply log: admission rejects every
// session read and every present minimum before SQL preparation, and successful
// strong reads never claim a read position.
//
// NewServer durably claims nonzero ownership-epoch and routing-version
// high-waters in the bound shard SQL catalog after validating its options. One
// live claim excludes another server over the same open store, and Close holds
// it until every admitted connection drains. This is a bounded local startup
// fence only: it is not a distributed lease or election and cannot revoke a
// process serving a copied store.
//
// The package has no built-in transport authentication and the shipped command
// accepts loopback listeners only. It provides no replication, election,
// failover, follower read, online movement, or copied-store revocation.
package shardservice
