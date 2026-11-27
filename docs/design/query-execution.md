# Query execution and operators

[Documentation](../README.md) / [Design](README.md) / Query execution

VibeDB compiles typed queries and SQL statements into plans over pinned
collection sources. Access paths narrow candidates; operators enforce exact
JSON value semantics within explicit memory and row budgets.

## Execution pipeline

```mermaid
flowchart LR
    Input[Typed query / SQL] --> Plan[Compile & bind]
    Plan --> Source[Pin source & select access path]
    Source --> Cand[Point / range / exact / tin / packed / scan]
    Cand --> Filter[Recheck predicates]
    Filter --> Ops[Joins, groups, windows, sort, limit]
    Ops --> Out[Owned columnar result]
```

Plans are immutable once compiled; execution workspaces (`Exec`) are
single-consumer and own snapshot and result lifetimes.

## Access paths

| Path | Work performed | Contract |
| --- | --- | --- |
| Primary point / range | Direct key lookup or key-interval scan | Preserves predicate, ordering, offset, and limit |
| Exact secondary index | Scalar or compound posting probe | Always rechecked against the document |
| Full-text (`tin`) index | Generation-pinned incremental index (`.tin` sidecar on disk); restricts candidates or reads ranked top-K for lone `==> ... ORDER BY SCORE() LIMIT` | Rechecked; exact top-K prefix match |
| Packed-column count / extrema | Evaluates compressed dict/FOR columns without JSON decode ([SIMD](../simd.md)) | Any declining leaf falls back to generic executor |
| Full scan | Sequential source scan | Respects budgets and cooperative cancellation |

## Relational operators and `EXPLAIN`

- **Joins:** Builder aliasless joins are semijoins; at most one fan-out join in
  builder queries. SQL supports `INNER`/`LEFT`/`RIGHT`/`FULL`/`CROSS`/`USING`
  with bounded `ON` predicates.
- **Aggregates, `DISTINCT ON`, and windows:** Exact JSON-decimal arithmetic
  with a dedicated exact aggregate state budget (16 MiB default). Extended
  aggregates (`STRING_AGG`, `ARRAY_AGG`, `JSON_AGG`/`JSONB_AGG`, `BOOL_AND`/
  `BOOL_OR`, `DISTINCT`/`FILTER`) and `DISTINCT ON` reuse execution arenas for
  zero-allocation steady-state execution.
- **CTEs & set ops:** Materialization, view expansion (depth 32, 1,024 refs),
  and recursion (1,000 iterations / 100,000 rows / 64 MiB) are hard-bounded.
- **Plan inspection:** `Query.Explain()` outputs logical plan JSON; SQL
  `EXPLAIN` / `EXPLAIN ANALYZE` include source metadata and runtime counters.

## Distributed execution

The gateway resolves shard routes from a pinned catalog generation, pushes
locality-preserving `GROUP BY` and exact shard-key filters to shards, and
merges non-local partial aggregates using zero-alloc grouped merge and bounded
top-K heaps ([distributed optimizer](../distributed-optimizer.md)).

## Limitations

- Embedded execution is rule-based/adaptive (no cost-based optimizer); the
  `planner` memo optimizer is used by the distributed gateway.
- Budgets bound named resource families (see [limits](../reference/limits.md)),
  not total process RSS.

## Source map

- Compilation & execution: [query/compiler.go](../../query/compiler.go), [execute.go](../../query/execute.go), [candidates.go](../../query/candidates.go)
- Operators: [query/join.go](../../query/join.go), [aggregate.go](../../query/aggregate.go), [sql_agg_ext.go](../../query/sql_agg_ext.go), [sql_distinct_on.go](../../query/sql_distinct_on.go), [window_statement.go](../../query/window_statement.go), [explain.go](../../query/explain.go)
- Full-text & optimizer: [query/match.go](../../query/match.go), [tin_topk.go](../../query/tin_topk.go), [planner/doc.go](../../planner/doc.go)
