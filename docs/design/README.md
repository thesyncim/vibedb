# Design guide

[Documentation](../README.md) / Design · [Stability](../status.md)

VibeDB shares one JSON document and storage engine across embedded Go, SQL,
and RF3 distributed execution. These guides cover ownership, invariants,
failure models, and source maps.

## Embedded and storage layers

| Layer | Covers | Guide |
| --- | --- | --- |
| System | Embedded layers and distributed physical nodes | [Architecture](../architecture.md) |
| Data | Collections, keys, canonical JSON, schemas, indexes | [Data model](../data-model.md) |
| Execution | Access paths, relational operators, plans, budgets | [Query execution](query-execution.md) |
| Transactions | Serializable conflict detection and crash-atomic commit | [Transactions](../transactions.md) |
| Persistence | Write profiles, recovery journals, crash boundaries | [Durability](../durability.md) |
| Storage | `store` and `store/durable` handle ownership and leases | [Storage engines](../store.md) |
| Encoding | Format 0 page layout, journals, Raft WAL, hidden state | [On-disk format](../format.md) |
| Optimization | Locality, costed global indexes, stats, SIMD kernels | [Distributed optimizer](../distributed-optimizer.md), [SIMD](../simd.md) |

## Distributed RF3 design

Read in order for the replicated runtime (or see
[distributed internals](../operations/distributed.md) for the condensed
operator view):

| Guide | Covers |
| --- | --- |
| [Request routing](routing.md) | Frontends, COW catalog generations, route seeds, command fences, forwarding, schema generations |
| [Replication](replication.md) | RF3 Multi-Raft groups, execution lanes, per-group WAL vs shared node log, snapshots, membership |
| [Exactly-once writes](exactly-once-writes.md) | Request identity, direct vs coordinated lanes, request ledger, route gates, execution pins |
| [Distributed transactions](distributed-transactions.md) | Fused prepare/decide/apply waves, intents, deterministic recovery, static lane |
| [Reads, leases, and time](reads-and-time.md) | `ReadIndex`, quorum read authority, follower floors, time domains, clock faults |
| [Topology changes](topology-changes.md) | Snapshot transfer, replica moves, 3↔4 scaling, frontend drain, hot-shard splits, migration pacing |
| [Backup and restore](backup-restore.md) | Per-group cut vectors, certificate-last publication, fresh-identity restore |
| [Security model](security.md) | mTLS 1.3 binary NodeIDs, ALPN separation, capabilities, delegation, WAL encryption |

## Core glossary

| Term | Meaning |
| --- | --- |
| **Generation** | Immutable published view (logical, physical, catalog, or schema) |
| **Shard / Raft group** | One routed key range backed by an independently ordered RF3 log and state machine |
| **Physical node** | One process hosting replicas from many Raft groups plus an embedded frontend |
| **Command fence** | Exact routing, ownership, schema, and policy coordinates verified before Raft admission |
| **Request identity** | Authenticated idempotency key under which a write's first committed outcome is retained |

See [qualification workflows](../qualification/README.md) for CI proof bounds
and [operator guides](../operations/README.md) for runbooks.
