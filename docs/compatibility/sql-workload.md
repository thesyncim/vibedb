# SQL workload compatibility tracker

[Documentation](../README.md) / [SQL reference](../reference/sql.md)

Tracks PostgreSQL-style application SQL capabilities against VibeDB's bounded
dialect. The [gap corpus](../../internal/conformance/sql_workload.go) defines
32 reduced statements that `sql/driver` verifies remain explicitly refused
until implemented with full semantics, metadata, and atomicity tests.

## Implemented capabilities

- **Conditional & null-safe expressions:** `COALESCE`, `GREATEST`, `LEAST`,
  `NULLIF`, `IS [NOT] TRUE/FALSE`, `IS [NOT] DISTINCT FROM` (up to 1,024
  variadic args; exact numeric, text, and Boolean domains).
- **Ordering & projection:** `NULLS FIRST/LAST`, computed `ORDER BY`, scalar
  `WHERE` before grouping, relation wildcards with computed outputs, hidden
  `HAVING` aggregates.
- **Upserts (`ON CONFLICT`):** Single-column primary-key `DO NOTHING` and
  `DO UPDATE` (including `WHERE` predicates and branch-aware global-index
  maintenance in embedded and RF3 SQL).
- **Distributed reads:** Coordinator-side evaluation over bounded shard source
  relations for non-colocated joins, CTEs, windows, `UNION`, `HAVING`, and
  global aggregates, with shard-pruning domain analysis shared between
  `EXPLAIN` and execution.

## Tracked gaps

| Priority | Missing capability | Scope required |
| --- | --- | --- |
| 1 | Composite primary keys & conflict targets | Multi-column PK catalog, key encoding, routing, uniqueness, transactions |
| 1 | PostgreSQL types, defaults, constraints | Timestamps, intervals, UUID, serial, type modifiers, defaults, CHECK/FK |
| 1 | JSONB operators & functions | Real JSONB type/casts, `?`/`||`/`-`, `jsonb_set`, array expansion/agg |
| 1 | Arrays & row-value predicates | `ANY`, `UNNEST`, array constructors/binding, composite-key pagination |
| 1 | Relational mutations & queues | `UPDATE ... FROM`, `DELETE ... USING`, modifying CTEs, `INSERT ... SELECT` |
| 1 | Row locking | `FOR UPDATE`, `SKIP LOCKED` with real concurrency semantics |
| 2 | Extended indexes | Expression, partial, ordered, and covering indexes |
| 2 | Extended query expressions | Computed `GROUP BY`, string/date functions, `DISTINCT ON`, `FILTER`, array/JSON aggs |
| 2 | Schema migrations | Full `ALTER TABLE` lifecycle beyond `ADD COLUMN` |
| 2 | Bulk & session parity | `COPY`, ORM catalog introspection |
| Excluded | PostgreSQL FTS (`tsvector`/`tsquery`) | VibeDB uses its own `USING tin` / `==>` / `SCORE()` surface ([search](../api/search.md)) |

## Validation

```sh
go test ./sql ./query ./sql/driver ./pgwire ./gateway ./shardservice ./cmd/vibedb-sql-audit
go test ./sql/driver -run TestSQLWorkloadCompatibilityTrackedGapsRemainExplicit
```

To audit a Go/SQL codebase against VibeDB's dialect, run:

```sh
go run ./cmd/vibedb-sql-audit -repo /path/to/repo -roots .
```
