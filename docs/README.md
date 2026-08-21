# VibeDB documentation

VibeDB is an embedded JSON database written in Go. It also provides a SQL
runtime and a PostgreSQL wire server. An experimental distributed runtime adds
shard routing, replicated state, and distributed execution.

Start with the page that matches your task.

## Get started

- [Install and run VibeDB](getting-started.md)
- [Select an API](api/README.md)
- [Understand the architecture](architecture.md)
- [Check current capabilities](capabilities.md)

## Use an API

- [Native embedded API](api/native.md)
- [Typed query API](api/query.md)
- [SQL and `database/sql`](api/sql.md)
- [PostgreSQL wire server](api/pgwire.md)

## Operate and inspect data

- [Durability and recovery](durability.md)
- [Offline verification, salvage, and repack](operations/verification.md)
- [Distributed runtime](operations/distributed.md)
- [Performance tests](performance.md)
- [Security boundary](../SECURITY.md)
- [Unsafe-code inventory](../UNSAFE.md)

## Design and internals

- [Storage model](store.md)
- [On-disk format](format.md)
- [SQL surface](design/sql-surface.md)
- [Query planner](design/query-planner.md)
- [Distributed transactions](design/distributed-transactions.md)
- [Placement tuple format](design/distribution-tuple-format.md)
- [Replicated state machine](design/replicated-state-machine.md)
- [Raft WAL](design/raft-wal.md)
- [Source provenance](provenance.md)

## Contribute

- [Contribution guide](../CONTRIBUTING.md)
- [Documentation language](STYLE.md)
- [Benchmark harness](../bench/competitive/README.md)
- [Allocation regression gate](../bench/gate/README.md)

## Status

The root Go module does not publish a tagged release in this repository. Pin a
tested commit when you use it as a dependency. The distributed commands are
experimental and bind only to loopback addresses.

The implementation is the source of truth. Each design page lists the source
files and tests that support its contract.
