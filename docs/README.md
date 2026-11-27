# VibeDB documentation

VibeDB is a JSON document database for Go with embedded, SQL, PostgreSQL wire,
and replicated RF3 cluster modes. All documentation describes the revision
checked out beside it; see [stability and compatibility](status.md) for
current boundaries and platform support.

## Quick start

- [Embedded tutorial](getting-started.md) — write, read, close, and reopen a local database.
- [Local RF3 cluster](operations/local-cluster.md) — launch a 3-node cluster and connect with `psql`.
- [API chooser](api/README.md) — pick between native Go, search, typed queries, `database/sql`, and `pgwire`.

## Application guides

| Guide | Covers |
| --- | --- |
| [Native API](api/native.md) | Collections, durability profiles, exact indexes, transactions, lifecycle |
| [Full-text search](api/search.md) | `tin` indexes, TINQL `==>`, BM25 `SCORE()`, `.tin` sidecars |
| [Query API](api/query.md) | Compiled plans, snapshots, workspaces, sessions |
| [SQL API](api/sql.md) | Embedded `database/sql` driver, schemas, transactions |
| [PostgreSQL wire](api/pgwire.md) | `psql`, `pgx`, `lib/pq`, JDBC, TLS + SCRAM, `pg_stat_*`, timeouts |
| [Data model](data-model.md) | Keys, canonical JSON, schemas, exact indexes |
| [Transactions](transactions.md) | Embedded serializable isolation, conflict detection, atomic commit |
| [Durability](durability.md) | Write profiles, recovery journals, crash boundaries |
| [SQL compatibility tracker](compatibility/sql-workload.md) | Tracked SQL dialect gaps and validation |

## Architecture and design

Start with [Architecture](architecture.md) and the [Design guide](design/README.md).

- **Storage & execution:** [Query execution](design/query-execution.md) ·
  [Storage engines](store.md) · [On-disk format](format.md) ·
  [SIMD kernels](simd.md) · [Distributed optimizer](distributed-optimizer.md)
- **Distributed RF3:** [Routing](design/routing.md) ·
  [Replication](design/replication.md) ·
  [Exactly-once writes](design/exactly-once-writes.md) ·
  [Distributed transactions](design/distributed-transactions.md) ·
  [Reads, leases, and time](design/reads-and-time.md) ·
  [Topology changes](design/topology-changes.md) ·
  [Backup and restore](design/backup-restore.md) ·
  [Security model](design/security.md)

## Operations

Start with the [Operator guide](operations/README.md).

- **Deployment & planning:** [Production readiness](operations/production-readiness.md) ·
  [Capacity planning](operations/capacity-planning.md) ·
  [Local cluster](operations/local-cluster.md) ·
  [Upgrades](operations/upgrades.md) ·
  [Kubernetes (Kind)](operations/kubernetes.md)
- **Day-2 operations:** [Observability](operations/observability.md) ·
  [Scaling and decommission](operations/scaling.md) ·
  [Hot-shard splits](operations/hot-shard-splits.md) ·
  [Replica migration](operations/migration.md) ·
  [Node failure](operations/node-failure.md) ·
  [Schema rollouts](operations/schema-rollouts.md) ·
  [Troubleshooting](operations/troubleshooting.md) ·
  [Distributed internals](operations/distributed.md)
- **Data protection:** [Embedded backup](operations/embedded-backup.md) ·
  [Distributed backup & restore](operations/backup-restore.md) ·
  [Offline verification](operations/verification.md)

## Reference and contracts

- [Reference index](reference/README.md): [SQL](reference/sql.md) ·
  [CLI](reference/cli.md) · [Protocols](reference/protocols.md) ·
  [Defaults and limits](reference/limits.md)
- Generated contracts: [Embedded capability matrix](capabilities.md) ·
  [Distributed feature ledger](distributed-feature-state.md)

## Development and verification

- [Developer guide](development/README.md): [Build & test](development/build-and-test.md) ·
  [Repository map](development/repository-map.md) ·
  [Testing strategy](development/testing-strategy.md) ·
  [Debugging](development/debugging.md) ·
  [Conventions](development/conventions.md)
- [Qualification workflows](qualification/README.md) ·
  [Performance & benchmarks](performance.md) ·
  [Benchmark methods](benchmarks/README.md)
- [Contributing](../CONTRIBUTING.md) · [Documentation style](STYLE.md) ·
  [Source provenance](provenance.md) · [Unsafe boundary](../UNSAFE.md) ·
  [Security](../SECURITY.md)
