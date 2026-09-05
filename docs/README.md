# VibeDB documentation

Store JSON in a Go application, query it through native or SQL interfaces, or
evaluate a replicated cluster. Start with the tutorial for the interface you
want; use the design guides to understand its behavior.

VibeDB is under development. Keep documentation, binaries, and data on the
same revision; [stability and compatibility](status.md) explains the boundaries.

## Get started

- [Embedded database tutorial](getting-started.md): write JSON, read it, close, and reopen.
- [Local RF3 cluster](operations/local-cluster.md): start physical nodes and connect with psql.
- [Choose an API](api/README.md): native Go, typed queries, `database/sql`, or PostgreSQL wire.

## Build an application

| Task | Guide |
| --- | --- |
| Store documents and maintain indexes | [Native API](api/native.md) |
| Understand keys, JSON values, and index definitions | [Data model](data-model.md) |
| Execute reusable typed plans | [Query API](api/query.md) |
| Use Go's SQL connection pool | [SQL API](api/sql.md) |
| Track application SQL compatibility gaps | [SQL workload tracker](compatibility/sql-workload.md) |
| Connect a PostgreSQL client | [PostgreSQL wire adapter](api/pgwire.md) |
| Commit related changes together | [Transactions](transactions.md) |
| Choose when writes become durable | [Durability and recovery](durability.md) |

## Understand the design

The [design guide](design/README.md) follows a request through the system:

1. [Architecture](architecture.md): embedded layers and distributed physical nodes.
2. [Query execution](design/query-execution.md): access paths, operators, and materialization.
3. [Storage](store.md) and [on-disk format](format.md): ownership, generations, and recovery records.
4. [Distributed internals](operations/distributed.md): routing, Raft, retries, and membership.

## Operate and diagnose

The [operator guide](operations/README.md) covers deployment, observation,
maintenance, and recovery for development clusters.

| Task | Guide |
| --- | --- |
| Start, stop, and reopen a cluster | [Local cluster](operations/local-cluster.md) |
| Diagnose startup, requests, and recovery failures | [Troubleshooting](operations/troubleshooting.md) |
| Collect counters and node diagnostics | [Observability](operations/observability.md) |
| Back up an embedded database | [Embedded backup](operations/embedded-backup.md) |
| Verify, salvage, or repack files | [Offline verification](operations/verification.md) |
| Export and restore RF3 group cuts | [Distributed backup and restore](operations/backup-restore.md) |
| Install an RF3 schema generation | [Schema rollouts](operations/schema-rollouts.md) |
| Exercise the Kubernetes test topology | [Kind qualification](operations/kubernetes.md) |

## Look up exact behavior

[Reference index](reference/README.md) · [SQL](reference/sql.md) ·
[CLI](reference/cli.md) · [Protocols](reference/protocols.md) ·
[Defaults and limits](reference/limits.md)

The generated [embedded capability matrix](capabilities.md) and
[distributed feature ledger](distributed-feature-state.md) link individual
capabilities to their implementation and tests.

## Contribute and investigate

- [Contributing](../CONTRIBUTING.md) and [documentation style](STYLE.md).
- [Performance methodology](performance.md), [benchmark archive](benchmarks/README.md),
  and [qualification records](qualification/README.md).
- [Research and proposals](design/research.md): dated investigations and planned work.
- [Source provenance](provenance.md), [unsafe-code boundary](../UNSAFE.md),
  and [security](../SECURITY.md).

Research records describe their recorded revisions. Current guides describe
the source beside them. Update both code and its guide when a contract changes.
