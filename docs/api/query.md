# Query API

> [!CAUTION]
> Unreleased development software: Query/SQL behavior, catalog contracts,
> protocol mappings, limits, and ownership may change on any commit. Pin the
> tested commit; builds are not compatible by default.

The `query` package executes typed queries over JSON documents. Use it when you
already own a VibeDB snapshot or segment and want typed cells without
`database/sql`. Use the SQL driver when you need a durable catalog, DDL, or
transaction management.

## Choose an entry point

| Need | Entry point | Reuse model |
|---|---|---|
| Build a projection/filter in Go | `query.Select(...)` | Mutable while chaining; immutable and concurrent-safe after first prepare/run |
| Execute SELECT text | `query.PrepareStatement(sql)` | `Statement` is reusable but single-consumer |
| Run once | `Query.Run` or `Statement.Run` | Call owns transient execution state |
| Run a hot loop | `RunInto(&exec, ...)` | One caller-owned `Exec` per goroutine |
| DDL, DML, or transactions | `sql/driver` | See [SQL API](sql.md) |

## Run a typed query

This complete example runs against an in-memory segment:

```go
package main

import (
	"fmt"
	"log"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store"
)

func main() {
	var docs store.Segment
	for _, doc := range []string{
		`{"team":"red","active":true,"score":4}`,
		`{"team":"red","active":true,"score":7}`,
		`{"team":"blue","active":false,"score":9}`,
	} {
		if _, err := docs.Append([]byte(doc)); err != nil {
			log.Fatal(err)
		}
	}

	q := query.Select(query.Path("team"), query.Sum("score")).
		Where(query.Cmp("active", query.Eq, true)).
		GroupBy("team").
		OrderBy("team", query.Asc).
		Limit(10)
	if err := q.Prepare(); err != nil {
		log.Fatal(err)
	}

	result, err := q.Run(query.FromSegment(&docs))
	if err != nil {
		log.Fatal(err)
	}
	defer result.Release()

	for row := range result.RowCount {
		team, _ := result.Columns[0].Cells[row].Text()
		fmt.Printf("%s %s\n", team, result.Columns[1].Cells[row].JSON())
	}
}
```

Paths may be dotted (`user.name`), RFC 6901 pointers (`/user/name`), or empty
for the whole document. The builder exposes:

| Area | Constructors |
|---|---|
| Projection | `Path` |
| Aggregate | `Count`, `Sum`, `Avg`, `Min`, `Max` |
| Predicate | `Cmp`, `In`, `Like`, `ILike`, `Contains`, `Exists`, `IsNull` |
| Boolean composition | `And`, `Or`, `Not` |
| Clauses | `Where`, `GroupBy`, `OrderBy`, `Limit`, `Join` |

`Count()` counts rows; `Count("path")` counts present, non-null values. Numeric
aggregates use exact JSON-decimal arithmetic. They skip null and nonnumeric
inputs and return null when nothing contributes.

## Execute SQL SELECT text

`PrepareStatement` adds aliases, `?` parameters, HAVING, OFFSET, joins,
subqueries, CTEs, sets, windows, and scalar expressions supported by the SQL
lowerer:

```go
stmt, err := query.PrepareStatement(`
	SELECT team, SUM(score) AS total
	FROM events
	WHERE active = ?
	GROUP BY team
	ORDER BY total DESC
	LIMIT 10`)
if err != nil {
	return err
}
defer stmt.Release()

result, cursor, err := stmt.Run(
	query.FromDatabase(snapshot, "events"),
	[]any{true},
)
if err != nil {
	return err
}
defer result.Release()
for cursor.Next() {
	team, _ := cursor.Cell(0).Text()
	total := cursor.Cell(1).AppendJSON(nil)
	_ = team
	_ = total
}
```

Iterate the returned `Cursor` for final SQL rows. The underlying `Result` may
contain hidden HAVING dependencies and rows before cursor-applied HAVING,
OFFSET, or an unpushed LIMIT. `Statement.Explain` and `Query.Explain` render the
prepared logical plan as versioned JSON without reading a source.

Use a coherent database source for a statement that reads more than one
collection. `FromDatabase` and `FromFileDatabase` make snapshot skew across a
join or subquery inexpressible. A single-collection source is rejected when a
child plan names a different collection.

The full textual surface and its intentional restrictions are in the
[SQL reference](../reference/sql.md).

## Select a source

| Source | Constructor | Result-cell ownership |
|---|---|---|
| In-memory segment | `FromSegment` | Cells borrow segment/execution bytes |
| Heap snapshot | `FromSnapshot` | Cells borrow snapshot/execution bytes |
| Durable snapshot | `FromFile` | Variable-width cells are copied into `Result` |
| Durable range/filter | `FromFileRange`, `FromFileFiltered` | Same durable ownership rule |
| Overlay | `FromFileOverlay`, `FromSnapshotOverlay` | Valid through the execution/result lifetime |
| Coherent catalog snapshot | `FromDatabase`, `FromFileDatabase` | Heap value needs no close; caller closes the durable snapshot |

The zero `Source` is invalid. A FROM-less SQL query uses an internal one-row
source; do not manufacture one with `Source{}`.

## Reuse execution storage

`Exec` retains result and workspace high-water buffers:

```go
var exec query.Exec
exec.Options.MemoryBytes = 32 << 20
exec.Options.ResultRows = 20_000

for _, snapshot := range snapshots {
	if err := q.RunInto(&exec, query.FromSnapshot(snapshot)); err != nil {
		exec.Release()
		return err
	}
	consume(exec.Result)
}
exec.Release()
```

Rules that matter:

- A compiled `Query` is concurrent-safe; an `Exec` is single-consumer. Give
  every goroutine its own `Exec`.
- Finish all chaining before `Prepare`, `Run`, or `Explain`; never mutate or
  copy a `Query` after its first use because it contains cached `sync.Once` state.
- `RunInto` invalidates the previous result immediately, including on failure.
- Segment and heap cells may borrow the source and `Exec.Workspace`; consume or
  copy them before modifying the source or running into the same `Exec` again.
- Call `Result.Release` after a one-shot result. Call `Exec.Release` when a
  retained high-water allocation should be dropped.
- `Statement` owns parsed/lowered arenas, is single-consumer, and must be
  released with `Statement.Release`. Its cursor is only a view over the result.

`Cell` offers typed accessors (`Bool`, `Int64`, `Float64`, `Text`) and JSON
encoding (`JSON`, `AppendJSON`). Borrowed byte slices are read-only.

## Value semantics

The Go builder intentionally differs from SQL in one place: builder predicates
are two-valued, while SQL statements use three-valued logic.

- Builder projection maps both an absent path and explicit JSON `null` to a
  null cell. Use `Exists(path)` to distinguish presence.
- SQL preserves the distinction internally: `IS NULL` matches null and
  missing, while `IS MISSING` matches only absence.
- Comparisons operate within JSON types. A null/missing value never satisfies
  a comparison.
- Numbers compare by exact decimal value, including integers beyond the exact
  `float64` range.
- ORDER BY and GROUP BY use the defined total order null, bool, number, string,
  container.
- Duplicate object keys resolve to the last occurrence.

## Resource controls

Zero-valued `ExecOptions` select finite defaults:

| Resource | Default | Disable limit |
|---|---:|---:|
| Materialized result | 100,000 rows and 64 MiB | `ResultRows = -1`, `ResultBytes = -1` |
| Relation intermediates | 64 MiB | `IntermediateBytes = -1` |
| Exact aggregate state | 16 MiB | No unlimited sentinel |
| Join-pair workspace | 64 MiB | `JoinPairBytes = -1` |
| Durable spill files | 1 GiB | `SpillBytes = -1` |
| Durable batch | 4,096 rows | Set `BatchRows` |
| General memory target | 64 MiB | Set `MemoryBytes` |
| Physical set tree | 1,000,000 rows / 64 MiB / depth 256 / 4,096 nodes | Per-kernel options |

The memory value is a work-admission limit for heap execution and a
batch/merge target for durable execution, not a promise that total process RSS
stays below it. Results and intermediates have independent budgets. Optional
indexes may be declined when their workspace does not fit; exact execution then
falls back to a scan. Budget exhaustion returns an error rather than changing
the answer.

Cancellation is cooperative. Install a reusable `CancelFlag` in
`ExecOptions.Cancel`; reset it only after the canceled execution has returned
and cleaned up workers and spill files.

## Important boundaries

- Builder joins are specialized: an aliasless join is a semijoin. An aliased
  join fans out only when a query path reads that alias; an unread alias changes
  nothing. Only one fanout relation is supported by the builder path.
- SQL joins support INNER, LEFT, RIGHT, FULL, CROSS, USING, and bounded ON
  predicates. They are not an arbitrary-expression join engine.
- SQL correlated subqueries and LATERAL are proof-bounded. Correlation under
  OR and several nested/correlated shapes are rejected at prepare time.
- View expansion is read-only and bounded to depth 32, 1,024 references, and
  16 MiB expanded SQL.
- Recursive CTE execution is finite by default: 1,000 evaluations, 100,000
  rows, and 64 MiB retained state.

## Source map

- Builder and reuse: `query/query.go:150-234`, `query/plan.go:7-107`
- Sources, ownership, and execution: `query/exec.go:12-60`, `query/exec.go:273-410`
- SQL statements and cursors: `query/sqlstmt.go:288-358`, `query/sqlstmt.go:986-1140`, `query/sqlstmt.go:1661-1801`
- Predicates and value semantics: `query/predicate.go:144-220`, `query/predicate.go:1084-1100`, `query/predicate.go:1391-1414`
- Budgets: `query/file_execute.go:22-115`, `query/result_budget.go:11-18`, `query/relation_runtime.go:10-14`
- Join/CTE/view boundaries: `query/join.go:135-207`, `query/recursive_fixpoint.go:10-148`, `query/view_expansion.go:10-75`
