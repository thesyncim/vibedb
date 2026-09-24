# Query API

[Documentation](../README.md) / [API guides](README.md) · [Development status](../status.md)

The `query` package executes typed queries over JSON documents and returns
column-oriented results. The native facade uses it for `Collection.Run`; call
it directly when you own a heap snapshot, a durable snapshot, or a catalog cut,
or when you want to run SQL `SELECT` text without the SQL driver. Use the
[SQL driver](sql.md) for DDL, DML, and SQL transactions.

## Choose an entry point

| Need | Entry point | Reuse model |
| --- | --- | --- |
| Build a projection or filter in Go | `query.Select(...)` | Build fully, then treat as immutable; safe for concurrent execution |
| Execute SQL `SELECT` text | `query.PrepareStatement(sql)` | Reusable, single-consumer; release it |
| Run once | `Query.Run` or `Statement.Run` | The call owns transient execution state |
| Run a hot loop | `Query.RunInto(&exec, source)` | One caller-owned `Exec` per goroutine |
| DDL, DML, or SQL transactions | `sql/driver` | See [SQL API](sql.md) |

## Run a typed query

This complete program runs against an in-memory segment:

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

Expected output:

```text
red 11
```

Paths may be dotted (`user.name`), RFC 6901 pointers (`/user/name`), or empty
for the whole document.

| Area | Constructors and methods |
| --- | --- |
| Projection | `Path` |
| Aggregate | `Count`, `Sum`, `Avg`, `Min`, `Max` |
| Comparison | `Cmp` with `Eq`, `Ne`, `Lt`, `Le`, `Gt`, `Ge` |
| Other predicates | `In`, `Like`, `ILike`, `Contains` (JSON containment, SQL `@>`), `Exists`, `IsNull`, `Match` (full text) |
| Boolean composition | `And`, `Or`, `Not` |
| Clauses | `Where`, `GroupBy`, `OrderBy`, `Limit`, `Join` |
| Inspection | `Prepare`, `Explain`, `AppendSchema` |

`Count()` counts rows; `Count("path")` counts present, non-null values.
`Sum`, `Avg`, `Min`, and `Max` use exact JSON-decimal arithmetic, skip null
and non-numeric inputs, and return null when nothing contributes. `In` sorts
and deduplicates its alternatives at compile time. `Like` supports `%`, `_`,
and backslash escapes.

## Execute SQL SELECT text

`PrepareStatement` accepts the `SELECT` surface of the
[SQL reference](../reference/sql.md): aliases, `?` parameters, HAVING, OFFSET,
joins, subqueries, CTEs, set operations, windows, and scalar expressions. This
complete program runs one over a heap catalog:

```go
package main

import (
	"fmt"
	"log"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store"
)

func main() {
	var catalog store.Database
	events, err := catalog.CreateCollection("events", store.Options{})
	if err != nil {
		log.Fatal(err)
	}
	for key, doc := range map[string]string{
		"e1": `{"team":"red","active":true,"score":4}`,
		"e2": `{"team":"red","active":true,"score":7}`,
		"e3": `{"team":"blue","active":true,"score":9}`,
		"e4": `{"team":"blue","active":false,"score":1}`,
	} {
		if _, err := events.Put(key, []byte(doc)); err != nil {
			log.Fatal(err)
		}
	}

	stmt, err := query.PrepareStatement(`
		SELECT team, SUM(score) AS total
		FROM events
		WHERE active = ?
		GROUP BY team
		ORDER BY total DESC
		LIMIT 10`)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Release()

	snapshot := catalog.Snapshot()
	result, cursor, err := stmt.Run(query.FromDatabase(snapshot, "events"), []any{true})
	if err != nil {
		log.Fatal(err)
	}
	defer result.Release()
	for cursor.Next() {
		team, _ := cursor.Cell(0).Text()
		fmt.Printf("%s %s\n", team, cursor.Cell(1).JSON())
	}
}
```

Expected output:

```text
red 11
blue 9
```

Iterate the returned `Cursor` for final SQL rows. The underlying `Result` can
hold hidden HAVING dependencies and rows that the cursor later removes with
HAVING, OFFSET, or an unpushed LIMIT. `Statement.Explain` and `Query.Explain`
render the prepared logical plan as versioned JSON without reading a source.

Use a coherent catalog source (`FromDatabase` or `FromFileDatabase`) for a
statement that reads more than one collection, so a join or subquery cannot
observe two different states. A single-collection source is rejected when the
plan names another collection.

A statement with neither `?` placeholders nor subqueries compiles once at
prepare time. Statements with placeholders or subqueries re-lower on each
execution.

## Select a source

| Source | Constructor | Result-cell ownership |
| --- | --- | --- |
| In-memory segment | `FromSegment` | Cells borrow segment and execution bytes |
| Heap snapshot | `FromSnapshot` | Cells borrow snapshot and execution bytes |
| Durable snapshot | `FromFile` | Variable-width cells are copied into the `Result` |
| Durable key range or filter | `FromFileRange`, `FromFileFiltered` | Same as durable snapshot |
| Snapshot plus staged writes | `FromFileOverlay`, `FromSnapshotOverlay` | Valid through the execution and result lifetime |
| Coherent catalog cut | `FromDatabase`, `FromFileDatabase` | Heap cut needs no close; the caller closes a durable cut |

The zero `Source` is invalid. A FROM-less SQL query uses an internal one-row
source; do not construct `Source{}`.

## Reuse execution storage

`Exec` holds the result, the transient workspace, execution options, and
execution statistics. A warmed `Exec` makes repeated runs allocation-free:

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

- A compiled `Query` is safe for concurrent execution; an `Exec` is
  single-consumer. Give each goroutine its own `Exec`.
- Finish chaining before the first `Prepare`, `Run`, or `Explain`. Do not
  mutate or copy a `Query` after first use; it caches its compiled plan.
- `RunInto` invalidates the previous result immediately, including on
  failure.
- Segment and heap cells may borrow the source and `Exec.Workspace`; consume
  or copy them before modifying the source or reusing the `Exec`.
- Call `Result.Release` after a one-shot result, and `Exec.Release` to drop
  retained high-water buffers.
- `Statement` owns parsed and lowered arenas, is single-consumer, and must be
  released. Its cursor is a view over the result and owns nothing.

`Cell` offers typed accessors (`Bool`, `Int64`, `Float64`, `Text`,
`TextBytes`, `IsNull`) and JSON encoding (`JSON`, `AppendJSON`). Borrowed byte
slices are read-only.

## Value semantics

The builder and SQL differ in one place: builder predicates are two-valued,
while SQL statements use three-valued logic.

- Builder projection maps an absent path and explicit JSON `null` to a null
  cell. Use `Exists(path)` to distinguish them.
- SQL keeps the distinction: `IS NULL` matches null and missing, and
  `IS MISSING` matches only absence.
- Comparisons operate within one JSON type. Null and missing never satisfy a
  comparison.
- Numbers compare by exact decimal value, including integers beyond the exact
  `float64` range; `1`, `1.0`, and `1e0` are equal.
- ORDER BY and GROUP BY use the total type order null, bool, number, string,
  container.
- When a stored object has duplicate member names, path reads resolve to the
  last occurrence.
- `Match(path, tinql)` tests the string at `path` against a TINQL query. It
  needs a tin index over the path in the source being queried. Heap snapshots
  and durable snapshots bind it and use the index to restrict the scan.
  Overlays with staged writes, segments, and validated raw sources refuse it
  with an error. See [full-text search](search.md).

## Resource controls

Zero-valued `ExecOptions` select finite defaults:

| Resource | Default | Disable limit |
| --- | ---: | ---: |
| Materialized result | 100,000 rows and 64 MiB | `ResultRows = -1`, `ResultBytes = -1` |
| Relation intermediates | 64 MiB | `IntermediateBytes = -1` |
| Exact aggregate state | 16 MiB (minimum 512 bytes) | No unlimited setting |
| Join-pair workspace | 64 MiB | `JoinPairBytes = -1` |
| Durable spill files | 1 GiB | `SpillBytes = -1` |
| Durable batch | 4,096 rows | Set `BatchRows` |
| Execution memory target | 64 MiB (heap minimum 64 KiB) | No unlimited setting |
| Workers | `GOMAXPROCS` | Set `Workers` |

`MemoryBytes` is a work-admission limit for heap execution and a batch and
merge target for durable execution. It is not a process RSS bound: fixed
worker minima and one maximum-size document can exceed it, and the result has
its own budget. Optional index use can be declined when its workspace does
not fit; execution then scans. Exhausting a budget returns a typed error such
as `*ResultBudgetError` instead of changing the answer.

Cancellation is cooperative. Set `ExecOptions.Cancel` to a reusable
`*CancelFlag`; execution returns `ErrCanceled` after cleaning up workers and
spill files. Reset the flag only after the canceled execution has returned.

## Important boundaries

- Builder joins are equi-joins. A join whose alias is never read is a
  semijoin (existence filter). A join whose alias is read fans out one row
  per matching pair. At most one join per builder query may fan out.
- SQL joins support INNER, LEFT, RIGHT, FULL, CROSS, USING, and bounded ON
  predicates; they are not an arbitrary-expression join engine.
- SQL correlated subqueries and LATERAL are limited to shapes the lowerer can
  prove; correlation under OR and several nested forms are rejected at
  prepare time.
- View expansion is bounded to depth 32, 1,024 references, and 16 MiB of
  expanded SQL.
- Recursive CTE execution stops at 1,000 iterations, 100,000 rows, or 64 MiB
  of retained state by default.

## Limitations

- The package is a development API with a large exported surface; many
  exported types exist for the SQL driver and are not intended as stable
  application entry points.
- There is no cost-based optimizer in the embedded path: access paths are
  chosen by fixed rules and adaptive thresholds, and some optimizations
  decline to a full scan.
- Budgets are per resource family, not a single process-memory ceiling.
- Builder queries cannot express HAVING, OFFSET, windows, set operations, or
  multiple fan-out joins; use `PrepareStatement` for those.

## Source map

- Builder, reuse, and join compilation: [query/query.go](../../query/query.go), [query/plan.go](../../query/plan.go), [query/join.go](../../query/join.go)
- Sources, ownership, and execution: [query/exec.go](../../query/exec.go), [query/execute.go](../../query/execute.go)
- SQL statements and cursors: [query/sqlstmt.go](../../query/sqlstmt.go)
- Predicates and value semantics: [query/predicate.go](../../query/predicate.go)
- Full-text binding: [query/match.go](../../query/match.go), [query/match_file.go](../../query/match_file.go)
- Budgets: [query/file_execute.go](../../query/file_execute.go), [query/result_budget.go](../../query/result_budget.go), [query/heap_work_budget.go](../../query/heap_work_budget.go), [query/aggregate.go](../../query/aggregate.go), [query/relation_runtime.go](../../query/relation_runtime.go)
- CTE and view bounds: [query/recursive_fixpoint.go](../../query/recursive_fixpoint.go), [query/view_expansion.go](../../query/view_expansion.go)
