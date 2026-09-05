# SQL workload compatibility tracker

This audit compares the Chat product and its shared database layer in the local
`chat` repository with VibeDB main at `9454ced0` (2026-09-05). The implementation
batches through null-safe comparisons are merged at `2a8723ff`; grouped
filtering builds on main at `a2ac5fd8`. **The application
cannot yet run unchanged on VibeDB.** This change implements the conditional
expressions, Boolean tests, explicit null ordering, INSERT NULL literals, and
single-column primary conflict targets, and computed sort keys described below. The remaining reduced
SQL cases stay executable as a gap inventory. Full-text search is excluded.

## Evidence and reproducibility

[Source evidence](sql-workload-evidence.json) records the source commit
`d68da7b1ad714143c2dfadb246aee3a2c9e166c9`, source-content SHA-256, source roots,
and file/line locations. The source checkout had tracked modifications, so the
content hash is the identity of the files actually scanned; the revision alone
does not identify the input. The scan covers 1,958 tracked Go/SQL files and
63,422 string, ORM-clause, and schema fragments. It excludes Go tests, mocks,
and testdata, and does not copy application SQL or connect to a database.

```sh
go run ./cmd/vibedb-sql-audit -repo /path/to/chat \
  -roots lib/chat,lib/core,lib/migrations/chat,lib/migrations/config,lib/sql,monolith/state,monolith/types,monolith/utils,projects/chat \
  > docs/compatibility/sql-workload-evidence.json
```

The tool works on any local Go/SQL repository; `-roots` defaults to `.`. The
roots above intentionally include shared monolith code that also serves other
products. They are an over-approximation of Chat's runtime dependencies, not a
claim that every match executes on a Chat request. Go AST string literals and
common ORM methods are classified by feature patterns. Dynamically assembled
queries, ORM-generated SQL, relation loading, and connection initialization
still require a live application/client test. Schema evidence includes
historical migrations, not just the resulting current schema. Counts are
source locations, not query execution counts or a compatibility percentage.

## Implemented surface

| Feature | Evidence locations | Implementation and limits |
| --- | ---: | --- |
| COALESCE | 99 | Lazy, ordered argument selection; same bounded scalar stage as CASE. Works in projections, assignments, conflict assignments, scalar predicates, and aggregate outputs. |
| GREATEST / LEAST | 69 | Ignore null inputs; exact numeric, text, and Boolean comparisons. Timestamp and PostgreSQL array types remain separate gaps. |
| NULLIF | 12 | Exact equality of supported scalar domains; returns the first argument unless equal. JSON/JSONB comparison remains a gap. |
| IS [NOT] TRUE/FALSE | 61 | Correct null/missing truth table and Boolean type checks. Bare Boolean filters and mixed scalar/path comparison or null-test OR trees use strict Boolean checks. Scalar WHERE runs before grouping through a bounded relation stage. |
| NULLS FIRST/LAST | 59 | Path, alias, ordinal, set, window-output, grouped, heap, and disk-spill ordering; distributed merge and planner ordering properties use the same placement. Existing default ordering is preserved. |
| NULL in INSERT column VALUES | — | Literal NULL reaches the existing nullable-field validator; primary-key and NOT NULL failures remain atomic. |
| Explicit ON CONFLICT target | 58 | A single primary-key column is validated at prepare and execution, including explicit transactions. Composite and secondary unique targets remain unsupported. Secondary unique violations are never silently skipped. |
| Computed ORDER BY | 69 GREATEST/LEAST locations include sort usage | Hidden scalar and aggregate sort keys run before OFFSET/LIMIT. Grouping, joins, derived relations, and window outputs retain their own semantic stages; distributed sorts evaluate the complete qualifying relation. |
| Scalar filtering before grouping | Reduced aggregate case | Filters the input relation before COUNT/SUM/AVG, grouped outputs, HAVING, and windows. Preserves prepared parameter ranges and headers across CTEs and joins, including chained and outer joins. Embedded, durable, and distributed reads share the same bounded execution stage. |
| Aggregate HAVING dependencies | Reduced grouped and global aggregate cases | HAVING can read aggregates absent from the SELECT list and combine grouped-key comparisons, null tests, and computed scalar predicates with AND/OR/NOT. Computed output runs after HAVING even without ORDER BY. Hidden reductions leave public result headers unchanged. Correlated LATERAL also supports hidden local and captured COUNT/SUM/AVG/MIN/MAX in its existing comparison, IN, BETWEEN, and null-test predicates; computed correlated predicates and post-reduction tails remain gaps. |
| IS [NOT] DISTINCT FROM | Reduced comparison case | Total null-safe comparison over supported scalar domains, with each operand evaluated once. Uses the shared predicate/CASE stage on embedded and distributed reads and mutations. Equality against a placement key retains finite shard routing through CTE aliases. Uncorrelated scalar WHERE runs before grouping; correlated grouped statements remain a gap. |

These features do not remove the existing restrictions on computed GROUP BY
expressions, mixed scalar/path pattern predicates, correlated grouped scalar
filters, derived-table wildcard projections, or distributed mutation syntax.
There is no general scalar-function catalog. Use explicit casts between
different scalar domains; general PostgreSQL unknown-literal/type coercion is
not claimed. Conditional Boolean/text typed literals reuse the existing CASE
common-type rules. The variadic expression bound is 1,024 arguments.

Reduced end-to-end tests exercise counters, nullable metadata, prepared
statements, upserts, transaction publication, type errors, recovery, and exact
numbers above 2^53. Conditional argument trees retain each argument once, and
the warmed query execution test requires zero allocations. The cold prepared
scalar sidecar grows by one slice; ordinary path queries retain their existing
execution path. RF3 insert-ignore introduces a distinct replicated mutation kind and changes
its authenticated apply-contract digest. Snapshot and data-chain golden vectors
include this contract; a mixed binary rollout requires the existing contract
fences and upgrade procedure.

## Distributed execution and routing

The coordinator can evaluate the original supported SELECT through the shared
SQL engine over bounded source relations when shard-fragment merging cannot
preserve its semantics. Regression cases cover global AVG, conditional
aggregates, DISTINCT, HAVING, OFFSET, CTEs, derived relations, UNION, windows,
and joins that are not colocated. Sources on one shard are fetched by one
combined SQL statement so they share a catalog snapshot. RF3 retains independent
leader observations for each group; this does not create a cluster-wide MVCC cut.

Routing computes a complete domain for every physical source occurrence. It
unions repeated consumers, follows CTE output aliases and plain derived
projections, propagates join-key equalities in null-preserving directions, and
combines OR and set-branch domains. Unknown predicates widen the domain. LIMIT,
aggregation, windows, recursion, and predicates with observable evaluation
boundaries prevent unsafe inherited pushdown. Unused CTE definitions retain
catalog validation but generate no source scans. Necessary top-level key
filters also run on the selected shards. EXPLAIN and execution use the same
source plan and fingerprint, with per-source fanout and scatter reasons.

Atomic RF3 `ON CONFLICT DO NOTHING` validates candidates before deciding the
primary-key branch, reports exact affected rows, and preserves retry results.
On legacy shards without global indexes, a single-owner `DO UPDATE` executes
the original conflict action atomically in the shard driver. Arbitrary shard-key
assignments still require a placement proof; copying the current or candidate
key and whole-document EXCLUDED replacement preserve the routed owner.
RF3 whole-document EXCLUDED replacement uses the native atomic put primitive
and preserves its exact affected-row and retry semantics. Declared RF3 column
upserts now replicate a bounded conflict program with bound scalar constants
and EXCLUDED column references. Each replica validates the candidate and column
names before selecting the insert or update branch, then patches its current
row atomically. Untouched fields and exact retry results are preserved.
Computed conflict assignments, global-index conflict maintenance, RETURNING, general mutation
predicates, and explicit transaction parity still need implementation. The
bounded coordinator read path also still needs full RF3 global-index read
integration. These are release gates for the requested distributed parity,
not claims closed by local tests.

## Remaining work

The [SQL workload gap corpus](../../internal/conformance/sql_workload.go)
contains 33 reduced statements. The
[driver gate](../../sql/driver/sql_workload_compatibility_test.go) verifies that
each still refuses at prepare or execution. When implementing a gap, replace
its refusal expectation with result, metadata, and atomicity coverage and
update this table. A parser accepting a statement does not close a gap.

| Priority | Missing capability | Source evidence / affected behavior |
| --- | --- | --- |
| 1 | Composite primary keys and conflict targets | 66 schema locations with multi-column primary keys. Channels use `(app_pk, cid)`; memberships use `(app_pk, channel_cid, user_id)`. Requires catalog, key encoding, routing, uniqueness, transaction, and recovery support, not a parser alias. |
| 1 | PostgreSQL field types, defaults, constraints | 333 timestamp/interval, 125 UUID/serial, 90 type-modifier, 389 default/constraint locations. Requires real timestamp/UUID/array semantics, value generation, casts, wire metadata, and schema validation. |
| 1 | JSONB and JSON operations | 296 JSONB, 45 JSON-function, 312 JSON-operator locations. Includes JSONB type/casts, key existence, merge/delete, jsonb_set, typeof, array expansion and aggregation. Existing JSON path access/containment is only a subset; JSON must not be relabeled JSONB. |
| 1 | PostgreSQL arrays and row-value predicates | 8 array-function, 33 array-syntax, 64 row-comparison locations. Includes ANY, UNNEST, array binding, constructors, aggregation and composite-key pagination. |
| 1 | Relational mutations and queues | 36 UPDATE-FROM/DELETE-USING and 14 modifying-CTE locations. Requires atomic shared-snapshot execution, INSERT-SELECT with target columns, conflict-action WHERE, and returned-row dependencies. |
| 1 | Locking and queue concurrency | 14 locking locations, including FOR UPDATE / SKIP LOCKED. Requires a real concurrency contract; accepting and ignoring lock clauses would be incorrect. |
| 2 | Expression, partial, ordered, and covering indexes | 205 index-related locations. Required both for schema acceptance and efficient channel/member queries; uniqueness predicates and access-path proofs must remain correct. |
| 2 | Remaining query expressions | Computed group keys; mixed scalar/path pattern predicates; correlated scalar filtering before aggregates; derived wildcard + scalar outputs; string/date functions; DISTINCT ON; aggregate DISTINCT/FILTER and array/JSON aggregation. |
| 2 | ALTER and migration lifecycle | 375 ALTER locations. Current ADD COLUMN is insufficient for historical migrations, type/default changes, drops, and index changes. |
| 2 | Bulk and client/session behavior | 5 COPY and 10 session/catalog locations, plus ORM-generated connection and relation queries not established by static extraction. Requires wire/client tests and bounded bulk ingestion. |
| 1 before application rollout | Distributed execution parity | Existing RF3 SQL mutation restrictions on ON CONFLICT, RETURNING, non-primary-key mutations, and bounded transactions still apply. Local database/sql success is not evidence that every RF3 path supports a feature. |
| Excluded | Full-text search | 88 evidence locations retained only to identify the excluded surface. No tsvector/tsquery, ranking, tokenization, or full-text indexes are implemented here. |

Start with keys and field types because the application's base schemas cannot
be installed without them. Then qualify JSON/array reads and writes, index
definitions, queue mutations/concurrency, and the distributed client workload.
Do not close the application-readiness claim until its actual generated SQL and
representative application tests pass against the intended VibeDB topology.

## Validation

```sh
go test ./sql ./query ./sql/driver ./pgwire ./gateway ./shardservice ./cmd/vibedb-sql-audit
go test ./sql/driver -run TestSQLWorkloadCompatibilityTrackedGapsRemainExplicit
```

Conditional null/evaluation behavior follows the supported domains of
[PostgreSQL conditional expressions](https://www.postgresql.org/docs/18/functions-conditional.html),
and Boolean tests follow its
[comparison predicates](https://www.postgresql.org/docs/18/functions-comparison.html).
VibeDB's existing type and execution restrictions are stated above rather than
being treated as complete PostgreSQL compatibility.
