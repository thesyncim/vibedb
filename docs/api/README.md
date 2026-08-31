# Choose an API

> [!CAUTION]
> Every API is an unreleased development contract. APIs, SQL, wire behavior,
> and disk data may break at any commit. Use docs and binaries from one exact
> commit and only disposable or recoverable data.

| Interface | Choose it when | Key boundary |
| --- | --- | --- |
| [`vibedb`](native.md) | You want embedded JSON CRUD, exact indexes, queries, and serializable transactions | Recommended starting surface; JSON-only |
| [`query`](query.md) | You need reusable typed plans or direct source/snapshot control | Results/workspaces have explicit ownership; bounded subset per source |
| [`database/sql`](sql.md) | Your Go application benefits from SQL and connection pooling | VibeDB dialect, fixed durable SQL lane |
| [`pgwire`](pgwire.md) | You need pgx/lib/pq/psql/JDBC protocol access | PostgreSQL v3 adapter, not PostgreSQL compatibility |
| `store/durable` | You need explicit page geometry, I/O mode, or recovery primitives | Expert API; read [storage layers](../store.md) first |

## Decision guide

- Start with the native facade unless an existing application requires SQL.
- Use `query` through the facade for most typed queries; call lower-level
  sources only when you own their snapshot/lifetime contracts.
- Use `database/sql` for local embedded SQL. `OpenCluster` adds placement
  preflight to an embedded catalog; it does not start a network or Raft cluster.
- Use pgwire only when a protocol client is a real requirement. Test every
  discovery and SQL shape your client emits.
- Treat distributed gateway protocols as internal development interfaces; see
  [protocol reference](../reference/protocols.md).

## Shared rules

- Missing JSON paths and explicit JSON `null` are distinct internally.
- Exact numbers do not pass through `float64` for equality or ordering.
- Candidate indexes are rechecked against source documents.
- Bounded execution may refuse work instead of silently dropping data or
  returning a partial result.
- A persistence or transport error can have an unknown commit outcome. Reopen
  or retry with the exact retained identity required by the API.

For feature-by-feature evidence, see the [embedded capability
matrix](../capabilities.md). It does not describe the distributed gateway.
