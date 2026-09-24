# Choose an API

[Documentation](../README.md) · [Development status](../status.md)

VibeDB has four embedded interfaces over the same JSON document model and
storage engine. They share value semantics but not catalogs: a directory
opened with `vibedb.Open` and a catalog opened by the SQL driver are separate
formats.

| Interface | Choose it when | Key boundary |
| --- | --- | --- |
| [`vibedb`](native.md) | You want embedded JSON CRUD, exact indexes, typed queries, and serializable transactions | Recommended starting surface; builder queries only, no SQL text |
| [Full-text search](search.md) | You need ranked text search over one string field | Index rebuilt in memory per searched generation |
| [`query`](query.md) | You need reusable typed plans, SQL `SELECT` text over your own sources, or direct snapshot control | Results and workspaces have explicit ownership |
| [`database/sql`](sql.md) | Your application wants SQL tables, DDL, and connection pooling | VibeDB dialect over a durable catalog; one fixed synchronous durability profile |
| [`pgwire`](pgwire.md) | You need psql, pgx, lib/pq, or JDBC protocol access | PostgreSQL v3 protocol adapter, not PostgreSQL compatibility |
| `store` / `store/durable` | You need explicit page geometry, I/O modes, or recovery primitives | Expert API; read [storage engines](../store.md) first |

## Decision guide

- Start with the native facade unless the application needs SQL.
- Use `query` through the facade for typed queries. Call lower-level sources
  only when you own their snapshot and lifetime contracts.
- Use `database/sql` for embedded SQL. `OpenCluster` adds one-shard placement
  preflight to an embedded catalog; it does not start a network service or
  replication.
- Use pgwire to connect an existing PostgreSQL client. Test every discovery
  query and SQL shape the client emits.
- Distributed gateway protocols are internal development interfaces; see the
  [protocol reference](../reference/protocols.md).

## Shared rules

- Missing JSON paths and explicit JSON `null` are distinct. SQL exposes the
  difference with `IS MISSING`; projection renders both as null.
- Numbers compare and aggregate by exact decimal value; they never round
  through `float64`.
- Index candidates are rechecked against documents, so an index changes cost,
  never the answer.
- Bounded execution returns a typed error instead of dropping rows or
  returning a partial result.
- A persistence or transport error can leave a commit outcome unknown. Close,
  reopen, and reconcile by application identity before retrying.

## Current gaps

- No interface is a stable API; see [stability](../status.md).
- The native facade cannot execute SQL text, and SQL tables cannot be opened
  through the native facade.
- The embedded interfaces are single-process. Network access comes from the
  pgwire adapter or the separate distributed runtime.

For feature-by-feature evidence, see the generated [embedded capability
matrix](../capabilities.md). It does not describe the distributed gateway.
