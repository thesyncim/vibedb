# Choose an API

[Documentation](../README.md) · [Stability](../status.md)

VibeDB offers five application surfaces over the same JSON document model and
storage engine. Native collections (`vibedb.Open`) and SQL catalogs
(`sql/driver`) use separate on-disk layouts.

| Interface | Choose it when | Primary boundary |
| --- | --- | --- |
| [`vibedb`](native.md) | Embedded JSON CRUD, exact indexes, typed queries, serializable transactions | Recommended default; builder queries only (no SQL text) |
| [Full-text search](search.md) | BM25-ranked TINQL search over one string path (`==>`, `SCORE()`) | Incremental with `.tin` sidecar; active index must fit in RAM |
| [`query`](query.md) | Reusable compiled plans, SQL `SELECT` over custom sources, explicit snapshot control | Caller manages snapshots, `Exec`, and result lifetimes |
| [`database/sql`](sql.md) | Embedded SQL tables, DDL, savepoints, Go connection pooling | VibeDB SQL dialect; fixed synchronous durability profile |
| [`pgwire`](pgwire.md) | Connecting `psql`, `pgx`, `lib/pq`, or JDBC | PostgreSQL v3 protocol adapter, not full PostgreSQL compatibility |
| `store` / `store/durable` | Explicit page geometry, I/O modes, or recovery primitives | Expert API; read [storage engines](../store.md) first |

## Shared rules

- **Null vs missing:** Absent paths and explicit JSON `null` stay distinct
  (`Exists` in `query`, `IS MISSING` in SQL); projections render both as NULL.
- **Exact decimals:** Numbers compare and aggregate by exact decimal value,
  never rounding through `float64`.
- **Rechecked indexes:** Exact-index candidates are rechecked against
  documents, so indexes change cost, never results.
- **Fail-closed budgets:** Exhausting a workspace or result budget returns a
  typed error rather than partial rows.
- **Unknown outcomes:** On a persistence or transport error, close/reopen and
  reconcile by application identity before retrying.

See the generated [embedded capability matrix](../capabilities.md) for
per-profile contract coverage, and [protocols](../reference/protocols.md) for
internal distributed gateway interfaces.
