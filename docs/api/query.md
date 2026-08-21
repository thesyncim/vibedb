# Typed query API

The `query` package builds typed queries over JSON collections. It can execute
against heap segments, coherent heap snapshots, durable snapshots, durable
files, overlays, primary-key ranges, and relation spools.

Use this package when you need programmatic query construction or reusable
execution storage. Use [SQL](sql.md) when SQL text and `database/sql` are a
better application boundary.

## Build and run a query

```go
q := query.Select(
	query.Path("team"),
	query.Sum("score"),
).
	Where(query.Cmp("active", query.Eq, true)).
	GroupBy("team").
	OrderBy("team", query.Asc)

result, err := q.Run(query.FromSegment(segment))
if err != nil {
	return err
}
fmt.Println(result.RowCount)
```

`FromSegment` is one source type. The source must not be the zero value.

## Reuse execution storage

```go
src := query.FromSegment(segment)
var exec query.Exec

for range 3 {
	if err := q.RunInto(&exec, src); err != nil {
		return err
	}
}
```

`Exec` retains work buffers up to the largest execution that it serves. It is
single-consumer. A compiled `Query` can be used concurrently only when each
caller has a separate executor.

Heap-backed results can borrow bytes until the next execution or source
mutation. Durable execution copies result bytes. Do not retain borrowed bytes
beyond their documented owner.

## Use a coherent database source

Cross-collection joins and subqueries need a coherent catalog source. Use
`query.FromDatabase` for heap data or `query.FromFileDatabase` for durable data.
A single collection source cannot resolve another relation.

```go
// database is an initialized *store.Database.
catalog := database.Snapshot()
result, err := q.Run(query.FromDatabase(catalog, "orders"))
if err != nil {
	return err
}
fmt.Println(result.RowCount)
```

The source captures all participating collections in one cut.

## Join behavior

The local executor supports inner, left, right, full, and cross joins through
the SQL lowering layer. It supports arbitrary `ON` predicates, composite
`USING`, derived tables, and explicit `LATERAL` sources.

A single physical inner or left equi-join can retain the storage-aware path.
That path measures the inner side and selects membership or keyed lookup.
Lookup mode can use a bloom prefilter. More general SQL shapes use the relation
join pipeline. It builds a hash table when equality keys are available and
uses nested-loop evaluation when they are not. Both paths enforce the
join-pair budget.

This local capability is wider than the distributed gateway. The gateway
currently permits only colocated inner and left joins with a complete shard-key
equality proof.

## Index use

Execution can select these access paths:

- Primary-key point access
- Primary-key range access
- Exact-index posting candidates
- A full scan

The complete predicate is always checked after candidate selection. An index
is an optimization, not a change to query semantics.

Execution binds the ready indexes exposed by its source snapshot. A query
compiled before index creation can use the index when it later runs against a
snapshot that includes it.

## Explain a plan

`q.Explain()` returns compile-time JSON without opening a source. After
`RunInto`, inspect `exec.Stats` for typed-query measurements.

`Statement.ExplainAnalyze` formats an `ExplainAnalysis` that execution already
collected. It does not run the statement. SQL `EXPLAIN ANALYZE` in the driver
runs the statement and returns the measured plan.

Plan names include `primary-key-point-or-scan`,
`primary-key-range-or-scan`, `adaptive-exact-index-or-scan`,
`adaptive-posting-or-scan`, `adaptive-join-or-scan`, and `full-scan`.

The `or-scan` suffix is important. Runtime data and resource conditions can
select the scan fallback.

## Resource controls

`query.ExecOptions` controls workers, durable batch size, working memory,
cancellation, result limits, intermediate limits, aggregate memory, spill,
and join budgets.

| Resource | Default |
| --- | ---: |
| Working memory | 64 MiB |
| Durable batch rows | 4096 |
| Result rows | 100,000 |
| Result bytes | 64 MiB |
| Intermediate bytes | 64 MiB |
| Aggregate bytes | 16 MiB |
| Spill bytes | 1 GiB |
| Join-pair bytes | 64 MiB |
| External merge fan-in | 32 |

Working memory below 64 KiB is invalid. Aggregate memory below 512 bytes is
invalid.

`-1` disables result, intermediate, spill, and join-pair limits. It does not
disable the aggregate budget.

Recursive execution has separate limits configured through
`RecursiveFixpointOptions` or `RecursiveCTELimits`:

| Resource | Default |
| --- | ---: |
| Recursive-term evaluations | 1,000 |
| Recursive result rows | 100,000 |
| Recursive fixpoint storage | 64 MiB |

Typed budget errors include:

- `ErrResultBudget`
- `ErrIntermediateBudget`
- `ErrAggregateBudget`
- `ErrWorkBudget`
- `ErrSpillBudget`
- `ErrJoinPairBudget`

Budget failure returns no partial materialized result.

## Implementation references

- `query/query.go`, `exec.go`, and `file_execute.go`
- `query/join.go` and `explain.go`
- `query/result_budget.go`, `heap_work_budget.go`, and `join_pair_budget.go`
- `query/example_test.go`
