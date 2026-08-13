# SQL replicated binding

The SQL replicated binding is a durable, write-once identity fence. It
converts an explicitly initialized local shard SQL root into a root whose
ordinary SQL DML and DDL paths refuse writes with `ErrDirectWriteFenced`.
Binding is not serving authorization: it does not elect a leader, establish a
lease, authorize HA reads, or make the root safe to expose to clients.

## Frozen identity and layout

The catalog's optional `replicated_shard_store` member has one strict current
encoding. It stores the complete WAL placement tuple, topology
recovery epoch, six authority generations, and the WAL member/store identity.
It also stores the preexisting SQL `LogID` and the explicit user table name,
storage incarnation, primary JSON pointer, and durable mutation limits.

SQL `LogID`, WAL `StoreID`, and Raft `GroupID` are separate semantic
namespaces. No one is derived from or substituted for another. Normal reopen
requires the complete identity returned by bind, including `LogID`, so a
separately prepared SQL root bound to the same WAL tuple is rejected. This does
not revoke or distinguish a byte-identical SQL-root copy opened under that same
WAL tuple. The same identity-only limitation applies independently to a
byte-identical WAL-root copy. Preventing either copy from serving requires an
external authority witness or lease, which this binding does not provide.

The current binding accepts exactly one materialized, empty-at-bind, schema-free,
index-free user table and no views. `CREATE TABLE t (PRIMARY KEY (p))` with no
SQL column declarations creates this schema-free durable profile: the SQL
driver still requires `p` to resolve to a non-null scalar on every direct SQL
mutation, while additional JSON fields remain unconstrained. A table with a
declared column list retains its durable schema and cannot be bound.

## Publication and restart settlement

Bind holds the connector lock through the `refs == 0` check, catalog lock, and
publication. Therefore a racing connection either exists first and makes bind
return `ErrReplicatedShardStoreBusy`, or is created after publication with its
immutable direct-write fence set. A definite prepublication failure restores
local mode. A published or outcome-unknown failure retains and returns the
deterministic proposed full identity.

A process can still die after the catalog rename and before receiving that
return value. The narrow settlement open requires four independently retained
inputs: path, the exact WAL binding, the SQL `LogID` recorded before bind, and
the intended user table. These are compared before namespace and transaction
recovery; only then does it return the catalog-computed full identity. There is
no binding-only settlement path.

## Trusted apply activation

The follow-on [SQL replicated apply](sql-replicated-apply.md) keeps this
write-once binding intact and adds a separate strict `replicated_apply` catalog
member. That activation owns a hidden system collection and one opaque apply
claim; it never exposes the collection, transaction log, or underlying state
machine and never relaxes the direct SQL write fence. Its deterministic profile
binds the user table, primary pointer, required shard placement, ordered-key
grammar, and durable limits, and committed puts/deletes are checked against that
primary-key and placement contract inside ordered apply.

An activated root can no longer use this document's base-only open or
settlement path. Exact restart retains a second complete apply identity,
including the random hidden storage identity. A dedicated activation settlement
path accepts the base identity plus the intended bounded profile and returns
that full identity; both exact and settlement comparisons happen before SQL
namespace or transaction recovery.

## Deliberate non-goals

Neither binding nor trusted activation is serving authorization. There is no
client proposal/RPC path, leader or lease proof, authenticated peer transport,
`ReadIndex`, replicated position token, runtime snapshot/compaction, or reserved
completion capacity. The current command remains a blind unconditional
single-collection write set; it cannot represent SQL predicates, read sets,
range/phantom dependencies, arbitrary results, or cross-shard coordination.
Consequently local read-only SQL remains available, but no replicated Read
Committed or Serializable write claim follows from this checkpoint.
