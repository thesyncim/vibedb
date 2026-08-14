# Documentation

VibeDB is unreleased. These documents describe the current tree; they are not
release promises or a compatibility roadmap.

## Users

- [Store guide](store.md): native API, queries, snapshots, transactions,
  indexes, limits, and ownership.
- [SQL surface](design/sql-surface.md): accepted SQL and explicit refusals.
- [Capability matrix](capabilities.md): generated, executable combinations of
  operations, indexes, transactions, and durability.
- [Durability](durability.md): acknowledgement, crash, recovery, and
  unknown-outcome contracts.
- [Pgwire package contract](../pgwire/doc.go): protocol, authentication,
  result types, and PostgreSQL compatibility boundaries.

## Operators

- [Distributed server boundary](design/distributed-sharding.md): loopback
  shard/gateway commands, local fencing, supported distributed reads, and
  explicit HA/resharding exclusions.
- [Performance](performance.md): latest commit-pinned benchmark publication
  and reproduction guidance.
- [Security policy](../SECURITY.md): current reporting and support boundary.
- [Provenance](provenance.md): dependency and adapted-algorithm ledger.

## Storage and engineering

- [Architecture](architecture.md): current storage and execution map.
- [Format](format.md): sole accepted development on-disk grammar.
- [Recovery journal](design/recovery-journal.md): journal structure and
  recovery invariants.
- [Canonical materialization](design/canonical-materialization.md): bounded
  immutable publication.
- [Primary write concurrency](design/parallel-tablet-writers.md): concurrent
  mutation lane, bounds, and fallbacks.
- [Multi-table transactions](design/multi-table-transactions.md): current K+1
  decision protocol and exclusions.
- [Query planner](design/query-planner.md): optimizer and distributed execution
  subset.
- [Raft core](design/raft-core-selection.md), [Raft WAL](design/raft-wal.md),
  and [replicated state machine](design/replicated-state-machine.md): the
  non-serving replication foundation and its serving exclusions.
- [Ownership](../store/durable/OWNERSHIP.md) and [unsafe inventory](../UNSAFE.md):
  resource lifetime and audited unsafe scopes.
- [Contributing](../CONTRIBUTING.md): tests, format discipline, benchmarks, and
  documentation rules.

Files under `docs/design` are current technical contracts. Completed delivery
plans, rejected alternatives, and historical benchmark narratives are not kept
in the public documentation tree; Git history is the archive.
