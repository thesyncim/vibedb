# Raft WAL

`internal/raftstore` provides one encrypted, preallocated, single-writer WAL
for a Raft member. It is internal and is not opened by the shipped commands.

## Identity

The immutable identity binds these fields:

- Cluster and cluster incarnation
- Distribution and shard
- Allocation generation and shard incarnation
- Group and member
- Store

The SQL binding also retains a separate SQL log ID. A replicated reopen must
match both identities.

## Default capacity

| Resource | Default |
| --- | ---: |
| File capacity | 256 MiB |
| Maximum record | 80 MiB |
| Record count | 65,536 |
| Entry count | 1,048,576 |
| Live-log bytes | 128 MiB |

The absolute file maximum is 4 GiB.

## Current image

Two authenticated 4 KiB current slots record adjacent generations. WAL records
form an authenticated digest chain from an immutable snapshot base.

Recovery can select the remaining authenticated slot when the inactive slot is
torn. The member enters quarantine before SQL binding after this fallback.

An unknown persistence outcome accepts only an exact retry. This rule prevents
a caller from replacing an uncertain record with different data.

The active WAL is append-only. Compaction creates an offline generation. It
does not rewrite the active file in place.

## Replicated SQL binding

`BindReplicatedShardStore` permanently converts a prepared local SQL root. The
root must have these properties:

- No live sessions
- Exactly one user table
- No schema
- No index
- No view
- No prior materialization
- No prior local serving fence
- No transaction marker

After the binding, direct SQL DML and DDL are fenced. Replicated apply owns the
mutation path.

Identity cannot distinguish a byte-identical copied root by itself. Deployment
controls must protect physical root ownership.

## Implementation references

- `internal/raftstore/types.go`, `store.go`, and `recovery.go`
- `sql/driver/replicated_store.go`
- `internal/raftstore/store_test.go`
- `sql/driver/replicated_store_test.go`
