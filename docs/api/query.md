# Query API

[Documentation](../README.md) / [API guides](README.md) · [Stability](../status.md)

The `query` package executes compiled typed queries and read-only SQL `SELECT`
statements over JSON document sources, returning column-oriented results. Use
the [SQL driver](sql.md) for DDL, DML, and transactions.

## Entry points

| Need | API | Reuse / ownership |
| --- | --- | --- |
| Go query builder | `query.Select(...)` | Immutable after first `Prepare`/`Run`/`Explain`; goroutine-safe |
| SQL `SELECT` text | `query.PrepareStatement(sql)` | Single-consumer; call `Release()` |
| One-off execution | `Query.Run` / `Statement.Run` | Allocates transient execution state; release `Result` |
| Allocation-free hot loop | `Query.RunInto(&exec, src)` | One caller-owned `Exec` per goroutine; call `exec.Release()` |

## Builder example

```go
var docs store.Segment
_, _ = docs.Append([]byte(`{"team":"red","active":true,"score":4}`))
_, _ = docs.Append([]byte(`{"team":"red","active":true,"score":7}`))

q := query.Select(query.Path("team"), query.Sum("score")).
	Where(query.Cmp("active", query.Eq, true)).
	GroupBy("team").
	OrderBy("team", query.Asc).
	Limit(10)

res, err := q.Run(query.FromSegment(&docs))
defer res.Release()
// Row 0: "red", 11
```

Paths accept dotted syntax (`user.name`), RFC 6901 pointers (`/user/name`), or
`""` for the whole document.

- **Aggregates:** `Count()` (rows), `Count("p")` (present non-null), `Sum`,
  `Avg`, `Min`, `Max` (exact decimal arithmetic; skip null/non-numeric).
- **Predicates:** `Cmp` (`Eq`/`Ne`/`Lt`/`Le`/`Gt`/`Ge`), `In`, `Like`, `ILike`,
  `Contains` (`@>`), `Exists`, `IsNull`, `Match` (tin FTS), plus `And`/`Or`/`Not`.
- **Clauses & inspection:** `Where`, `GroupBy`, `OrderBy`, `Limit`, `Join`,
  `Prepare`, `Explain`, `AppendSchema`.

## SQL `SELECT` statements

`query.PrepareStatement` supports the `SELECT` dialect in the
[SQL reference](../reference/sql.md) (aliases, `?` params, `HAVING`, `OFFSET`,
joins, subqueries, CTEs, set ops, windows, scalar expressions):

```go
stmt, err := query.PrepareStatement(`
	SELECT team, SUM(score) AS total FROM events
	WHERE active = ? GROUP BY team ORDER BY total DESC LIMIT 10`)
defer stmt.Release()

res, cur, err := stmt.Run(query.FromDatabase(catalog.Snapshot(), "events"), []any{true})
defer res.Release()
for cur.Next() {
	team, _ := cur.Cell(0).Text()
	fmt.Println(team, cur.Cell(1).JSON())
}
```

Always iterate the returned `Cursor` (not raw `Result`), because `Result` may
include hidden `HAVING` columns or pre-`OFFSET` rows. For multi-collection
queries, pass a coherent catalog cut (`FromDatabase` or `FromFileDatabase`).

## Sources and ownership

| Source | Constructor | Cell lifetime |
| --- | --- | --- |
| Heap segment / snapshot | `FromSegment`, `FromSnapshot` | Borrows source and `Exec.Workspace` |
| Durable snapshot / range / filter | `FromFile`, `FromFileRange`, `FromFileFiltered` | Copied into `Result` |
| Snapshot + staged writes | `FromSnapshotOverlay`, `FromFileOverlay` | Valid through execution & result lifetime |
| Coherent catalog cut | `FromDatabase`, `FromFileDatabase` | Caller closes durable cut |

- Reuse `var exec query.Exec` with `q.RunInto(&exec, src)` for zero-alloc hot
  loops; `RunInto` invalidates the previous result immediately (even on error).
- `Cell` provides `Bool`, `Int64`, `Float64`, `Text`, `TextBytes`, `IsNull`,
  `JSON`, and `AppendJSON`. Borrowed slices are read-only.

## Value semantics

- Builder predicates are **two-valued**; SQL statements use **three-valued**
  logic (`IS NULL` matches null and missing; `IS MISSING` matches absence).
- Comparisons operate within one JSON type; numbers compare by exact decimal
  value (`1 == 1.0 == 1e0`).
- `ORDER BY` / `GROUP BY` total type order: `null < bool < number < string < container`.
- `Match(path, tinql)` requires a tin index and is refused on overlays with
  staged writes, segments, and validated raw sources.

## Resource controls

Zero-valued `ExecOptions` apply finite defaults:

| Resource | Default | Disable |
| --- | ---: | ---: |
| Materialized result | 100,000 rows / 64 MiB | `ResultRows = -1`, `ResultBytes = -1` |
| Relation intermediates | 64 MiB | `IntermediateBytes = -1` |
| Exact aggregate state | 16 MiB (min 512 B) | — |
| Join-pair workspace | 64 MiB | `JoinPairBytes = -1` |
| Durable spill files | 1 GiB | `SpillBytes = -1` |
| Durable batch / workers | 4,096 rows / `GOMAXPROCS` | Set `BatchRows` / `Workers` |
| Execution memory target | 64 MiB (heap min 64 KiB) | — |

Exceeding a budget returns a typed error (e.g. `*ResultBudgetError`) instead of
partial results; optional index use falls back to a scan if its workspace does
not fit. Set `ExecOptions.Cancel` to a reusable `*CancelFlag` for cooperative
cancellation (`ErrCanceled`).

## Boundaries and limitations

- Builder joins are equi-joins (unread alias = semijoin; at most one fan-out
  join). SQL supports `INNER`/`LEFT`/`RIGHT`/`FULL`/`CROSS`/`USING` and bounded
  `ON` predicates.
- View expansion cap: depth 32, 1,024 refs, 16 MiB SQL. Recursive CTE cap:
  1,000 iterations, 100,000 rows, 64 MiB state.
- Embedded execution uses rule-based/adaptive path selection rather than a
  cost-based optimizer.
- Builder queries cannot express `HAVING`, `OFFSET`, windows, set ops, or
  multiple fan-out joins (use `PrepareStatement`).

## Source map

- Builder & joins: [query/query.go](../../query/query.go), [query/plan.go](../../query/plan.go), [query/join.go](../../query/join.go)
- Execution & statements: [query/exec.go](../../query/exec.go), [query/execute.go](../../query/execute.go), [query/sqlstmt.go](../../query/sqlstmt.go)
- Budgets & limits: [query/result_budget.go](../../query/result_budget.go), [query/heap_work_budget.go](../../query/heap_work_budget.go), [query/recursive_fixpoint.go](../../query/recursive_fixpoint.go)
