# SQL replicated binding v1

The SQL replicated binding is a durable, write-once compatibility fence. It
converts an explicitly initialized local shard SQL root into a root whose
ordinary SQL DML and DDL paths refuse writes with `ErrDirectWriteFenced`.
Binding is not serving authorization: it does not elect a leader, establish a
lease, authorize HA reads, or make the root safe to expose to clients.

## Frozen identity and layout

The catalog's optional version-0 `replicated_shard_store` member uses its own
strict format version. It stores the complete WAL placement tuple, topology
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

Format v1 accepts exactly one materialized, empty-at-bind, schema-free,
index-free user table and no views. `CREATE TABLE t (PRIMARY KEY (p))` with no
SQL column declarations creates this schema-free durable profile: the SQL
driver still requires `p` to resolve to a non-null scalar on every direct SQL
mutation, while additional JSON fields remain unconstrained. A table with a
declared column list retains its durable schema and cannot be bound to v1.

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

## Deliberate non-goals

This slice has no public or hidden replicated-apply capability and no hidden
system collection. It fences the public SQL mutation surface; a future runtime
must supply a narrowly trusted apply boundary rather than relaxing that fence.
It also does not yet bind or enforce the relationship between a `CommandV1`
mutation key and the JSON value selected by the SQL primary pointer. Until that
contract exists, the binding must not be described as a complete serving or
replicated-runtime boundary. Runtime snapshot transfer is likewise separate
work built after this durable identity/fence foundation.
