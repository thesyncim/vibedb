# Replicated SQL binding

`BindReplicatedShardStore` permanently binds one prepared SQL root to one Raft
WAL identity and SQL log ID.

## Preconditions

The SQL root must have:

- No live session
- Exactly one user table
- No schema
- No index
- No view
- No prior materialization
- No local serving fence
- No transaction marker

Binding rejects a root that does not meet every precondition.

## Identity

Reopen checks complete WAL lineage and the retained SQL log ID. A separately
prepared root with a different SQL identity is refused.

Identity cannot distinguish a byte-identical copied root by itself. Deployment
controls must protect physical root ownership.

## Mutation fence

After binding, direct SQL DML and DDL are disabled. Replicated apply owns the
mutation path. The binding is not a reversible runtime mode.

## Serving boundary

`vibedb-shard serve` opens an ordinary statically owned local shard store and
does not use replicated binding. `vibedb-shard serve-rf3` instead requires an
externally prepared complete replicated binding and apply identity and opens
them exactly; it never binds or initializes a root.

## Implementation references

- `sql/driver/replicated_store.go`
- `sql/driver/replicated_store_test.go`
- `internal/raftstore/types.go`
