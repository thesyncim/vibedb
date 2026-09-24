# VibeDB documentation

Store JSON in a Go application, query and search it through native or SQL
interfaces, or run a replicated RF3 cluster. Start with the tutorial for the
interface you want; use the design guides to understand its behavior.

VibeDB is under development. Keep documentation, binaries, and data on the
same revision; [stability and compatibility](status.md) explains the
boundaries and lists current limitations.

## Get started

- [Embedded database tutorial](getting-started.md): write JSON, read it, close, and reopen.
- [Local RF3 cluster](operations/local-cluster.md): start physical nodes and connect with psql.
- [Choose an API](api/README.md): native Go, full-text search, typed queries, `database/sql`, or PostgreSQL wire.

## Build an application

| Task | Guide |
| --- | --- |
| Store documents and maintain indexes | [Native API](api/native.md) |
| Understand keys, JSON values, and index definitions | [Data model](data-model.md) |
| Search and rank text with tin indexes | [Full-text search](api/search.md) |
| Execute reusable typed plans | [Query API](api/query.md) |
| Use Go's SQL connection pool | [SQL API](api/sql.md) |
| Connect a PostgreSQL client | [PostgreSQL wire adapter](api/pgwire.md) |
| Commit related changes together | [Transactions](transactions.md) |
| Choose when writes become durable | [Durability and recovery](durability.md) |
| Track application SQL compatibility gaps | [SQL workload tracker](compatibility/sql-workload.md) |

## Understand the design

The [design guide](design/README.md) follows a request through the system.

Embedded engine:

1. [Architecture](architecture.md): embedded layers and distributed physical nodes.
2. [Data model](data-model.md) and [query execution](design/query-execution.md): access paths, operators, and materialization.
3. [Transactions](transactions.md) and [durability](durability.md): visibility, commit, and recovery.
4. [Storage engines](store.md) and [on-disk format](format.md): ownership, generations, and recovery records.
5. [SIMD kernels](simd.md) and [performance methodology](performance.md).

[Distributed design](design/README.md#distributed-design), in reading order:

| Guide | Covers |
| --- | --- |
| [Request routing](design/routing.md) | Frontends, catalog generations, route seeds, command fences, forwarding. |
| [Replication](design/replication.md) | RF3 groups, Raft configuration, execution lanes, the node log, snapshots, membership. |
| [Exactly-once writes](design/exactly-once-writes.md) | Request identity, the request ledger, outcome-unknown recovery. |
| [Distributed transactions](design/distributed-transactions.md) | Fused prepare, decide, and apply waves, and their recovery. |
| [Reads, leases, and time](design/reads-and-time.md) | Read modes, read authority, follower reads, clock faults. |
| [Topology changes](design/topology-changes.md) | Replica moves, seamless scale-out and scale-in, frontend drain, hot-shard splits. |
| [Backup and restore internals](design/backup-restore.md) | Cut vectors, certificate-last publication, fresh-identity restore. |
| [Security model](design/security.md) | Trust model, TLS identities, capabilities, delegation. |
| [Distributed optimizer](distributed-optimizer.md) | Locality, global-index costing, and statistics. |

## Operate and diagnose

The [operator guide](operations/README.md) covers deployment, observation,
topology changes, and recovery for development clusters.

| Task | Guide |
| --- | --- |
| Decide what a deployment can rely on today | [Deployment readiness](operations/production-readiness.md) |
| Size hosts, groups, disk, and connections | [Capacity planning](operations/capacity-planning.md) |
| Start, stop, and reopen a cluster | [Local cluster](operations/local-cluster.md) |
| Add, rebalance, or retire a physical node | [Scale and decommission](operations/scaling.md) |
| Understand automatic hot-shard splits | [Hot-shard splits](operations/hot-shard-splits.md) |
| Tune replica-move pacing | [Online replica migration](operations/migration.md) |
| Respond to a lost or restarted node | [Node failure and replacement](operations/node-failure.md) |
| Change builds | [Upgrades and compatibility](operations/upgrades.md) |
| Install an RF3 schema generation | [Schema rollouts](operations/schema-rollouts.md) |
| Collect counters and node diagnostics | [Observability](operations/observability.md) |
| Diagnose startup, request, and recovery failures | [Troubleshooting](operations/troubleshooting.md) |
| Back up an embedded database | [Embedded backup](operations/embedded-backup.md) |
| Verify, salvage, or repack files | [Offline verification](operations/verification.md) |
| Export and restore RF3 group cuts | [Distributed backup and restore](operations/backup-restore.md) |
| Exercise the Kubernetes test topology | [Kind qualification](operations/kubernetes.md) |
| Review routing, quorum, and retry semantics | [Distributed internals](operations/distributed.md) |

## Look up exact behavior

[Reference index](reference/README.md) · [SQL](reference/sql.md) ·
[CLI](reference/cli.md) · [Protocols](reference/protocols.md) ·
[Defaults and limits](reference/limits.md)

The generated [embedded capability matrix](capabilities.md) and
[distributed feature ledger](distributed-feature-state.md) link individual
capabilities to their implementation and tests.

## Contribute and investigate

- [Contributing](../CONTRIBUTING.md) and [documentation style](STYLE.md).
- [Developer guide](development/README.md): [repository map](development/repository-map.md),
  [build and test](development/build-and-test.md),
  [testing strategy](development/testing-strategy.md),
  [debugging distributed failures](development/debugging.md), and
  [coding conventions](development/conventions.md).
- [Qualification workflows and records](qualification/README.md): what each CI
  workflow proves, and dated qualification runs.
- [Benchmark reports](benchmarks/README.md) with their methods and raw evidence.
- [Source provenance](provenance.md), [unsafe-code boundary](../UNSAFE.md),
  and [security](../SECURITY.md).

## Research and history

- [Research and proposals](design/research.md): live plans, accepted
  decisions, and targets, including the
  [storage and runtime redesign](storage-runtime-redesign.md),
  [packed exact-index storage](exact-index-packed-storage-plan.md), and
  [performance and scale targets](performance-and-scale-goal.md).
- [History](history/README.md): superseded proposals and dated run reports,
  each linked to the page that replaced it.
- [CI performance investigation](ci-performance.md): the 2026-09-04 analysis
  behind the current CI sharding.

Research records describe their recorded revisions. Current guides describe
the source beside them. Update both code and its guide when a contract changes.
