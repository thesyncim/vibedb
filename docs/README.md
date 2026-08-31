# VibeDB documentation

> [!CAUTION]
> VibeDB is an unreleased development project. There is one current source,
> wire, and disk grammar—not a compatibility ladder. Different commits may be
> incompatible and may corrupt or refuse older development data. Use an exact
> commit and disposable or recoverable data.

The documentation is organized by intent: start, build, understand, operate,
and look up exact contracts. The embedded API is the shortest path. SQL,
pgwire, and distributed pages are explicit about their narrower experimental
boundaries.

## Start

1. [Read the stability contract](status.md).
2. [Run the embedded database](getting-started.md).
3. Choose an [API](api/README.md).

To explore replication locally, use the generated [RF3 development
cluster](operations/local-cluster.md). Do not begin with hand-authored cluster
manifests.

## Build

| Task | Guide |
| --- | --- |
| Store and retrieve JSON | [Native API](api/native.md) |
| Build reusable typed queries | [Typed query API](api/query.md) |
| Use `database/sql` | [SQL API](api/sql.md) |
| Connect pgx, lib/pq, psql, or JDBC | [PostgreSQL wire adapter](api/pgwire.md) |
| Group writes atomically | [Transactions](transactions.md) |
| Choose acknowledgement semantics | [Durability](durability.md) |

## Understand

- [Architecture](architecture.md): layers, ownership, and publication paths.
- [Data model](data-model.md): collections, keys, JSON, schemas, and indexes.
- [Storage layers](store.md): facade versus heap source model versus durable
  engine.
- [On-disk format](format.md): the current development image and recovery
  families.

## Operate and qualify

- [Operations home](operations/README.md)
- [Verify, salvage, and repack](operations/verification.md)
- [Distributed runtime](operations/distributed.md)
- [Backup and restore](operations/backup-restore.md)
- [Schema rollouts](operations/schema-rollouts.md)
- [Observability](operations/observability.md)
- [Kubernetes qualification lane](operations/kubernetes.md)

No operations page is a production runbook. Every distributed procedure is a
development or qualification workflow for one exact build.

## Reference

- [Reference index](reference/README.md)
- [CLI](reference/cli.md)
- [SQL dialect](reference/sql.md)
- [Wire and service protocols](reference/protocols.md)
- [Defaults and limits](reference/limits.md)
- [Executable embedded capabilities](capabilities.md)
- [Generated distributed feature ledger](distributed-feature-state.md)

Generated reference pages report checked-in evidence; they do not imply a
release, support tier, production readiness, or performance result.

## Contribute

- [Contribution workflow](../CONTRIBUTING.md)
- [Documentation standard](STYLE.md)
- [Performance evidence](performance.md)
- [Source and algorithm provenance](provenance.md)
- [Unsafe-code boundary](../UNSAFE.md)
- [Security policy](../SECURITY.md)

When prose and code disagree, tests and the referenced codec or implementation
are authoritative. Open a documentation issue or change the docs in the same
commit as the contract.
