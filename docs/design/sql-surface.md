# SQL surface

## One language, one runtime

The public relational surface is SQL through Go's `database/sql` package and an
experimental PostgreSQL wire-protocol endpoint supporting a documented SQL
subset. JSON is the stored row representation; it is not a second query
language or a parallel request grammar.

The implementation has one responsibility per layer:

- `sql` tokenizes and parses one bounded SQL dialect. Syntax errors carry byte,
  line, and column positions.
- `query` lowers parsed SELECT and mutation predicates into the shared compiled
  plan and evaluator.
- `sql/driver` owns the typed catalog/session runtime, schema and index policy,
  transactions, and durable collections. Its `database/sql` adapter is one
  consumer.
- `pgwire` maps PostgreSQL protocol messages onto the same typed prepared
  statements, parameter roles, `query.Cell` cursors, results, and transaction
  states.

The driver does not maintain a second predicate evaluator or a smaller
hand-written SELECT grammar. A SELECT accepted by the parser and query lowering
uses the same projection, predicate, grouping, aggregation, ordering, and join
implementation as the rest of the query package.

The protocol server does not route through `database/sql/driver.Rows`, which
would erase the distinction between a JSON string, an exact non-integer
number, and an object. It consumes the typed runtime directly, so Parse
produces one reusable statement, Describe reads its typed metadata, Bind uses
its scalar/document parameter roles, and Execute streams the same
`query.Cell` values the embedded adapter consumes.

A nested integration module runs pinned pgx v5 and lib/pq releases over
loopback TCP in CI. It covers SCRAM, DDL, schema errors, exact indexes,
whole-document and flat writes, prepared statements, an inner join,
rollback/read-your-writes, stable SQLSTATEs, and close/reopen persistence.
That is reproducible evidence for those two clients, not broad PostgreSQL
compatibility or catalog emulation.

## Catalog and table lifecycle

A DSN is a filesystem path to a SQL catalog:

```go
db, err := sql.Open("vibedb", "/data/app.vdb")
```

Each SQL table maps to one durable collection file beside that catalog. The
catalog persists the facts that are specific to the SQL view of a collection:
the table name, its declared JSON schema, its one primary-key path, and its
exact-index definitions. The catalog's parent directory must already exist.
Existing symlinks are resolved before the sibling catalog lock and table
directory are derived, so lexical aliases cannot acquire independent writer
leases for one database.

`CREATE TABLE` publishes catalog metadata but does not allocate an empty
collection file. The first write materializes the file with the mutable chunk
layout. Choosing one layout unconditionally is important: schema enforcement,
exact-index maintenance, statement batches, and transaction support do not
depend on the shape of the first INSERT.

`DROP TABLE` removes the table from the durable SQL catalog before retiring its
collection file. `DROP TABLE IF EXISTS` is an idempotent no-op for an absent
table. Physical collection cleanup is deferred while active snapshots or
cursors hold leases; a crash after catalog publication can leave only an
unreachable orphan file, never a catalog entry pointing at missing data.

Every newly created table and storage replacement receives a cryptographically
random, cataloged storage identity. `DROP TABLE` followed by same-name
`CREATE TABLE` can therefore publish the replacement immediately while an old
snapshot continues reading the retired file. Reopen removes only unreferenced
files in the driver's private, strictly recognized storage namespace; live and
legacy catalog paths remain protected. Recovery work and simultaneously
retired incarnations both have hard bounds.

`TRUNCATE [TABLE] name` publishes a fresh empty storage incarnation with the
same schema and exact-index definitions. `DROP INDEX [IF EXISTS] name [ON
table]` builds a replacement containing every document and all remaining exact
indexes, fences that file, and atomically switches the catalog identity. Active
snapshots continue to see the old incarnation until their leases close. A
crash before catalog publication leaves a recoverable orphan; a crash after it
leaves the new catalog authoritative. Unqualified index names must resolve to
exactly one table; `ON table` is the deterministic form when names repeat.

Catalog publication uses a synced temporary file, atomic replacement, and a
namespace durability fence (directory sync on Unix and a write-through move on
Windows). The first table file is fully committed before its directory entry is
fenced; a fence failure after publication reports
`durable.ErrCommitOutcomeUnknown` and retains the matching live metadata rather
than pretending the publication was rolled back. Every unresolved catalog or
table-directory fence is recorded and retried before a later mutation may
acknowledge success. Once the first table file is published, the catalog also
records that the table is materialized; a later reopen treats a missing file as
corruption instead of silently recreating an empty table.

A separate catalog lock file holds a process-and-filesystem writer lease for
the lifetime of the connector, so two independently opened handles cannot
overwrite one another's schema or index changes from stale catalog copies. The
current catalog format is version 0. Schemas and index definitions
are recompiled and validated when the database is reopened. A catalog may hold
at most 128 tables because this driver eagerly keeps one descriptor for every
materialized table; the explicit ceiling leaves descriptor headroom for
temporary publications, spill files, sockets, and the embedding application.

## Tables and JSON schemas

Every driver table declares exactly one scalar primary-key path:

```sql
CREATE TABLE users (
    id STRING PRIMARY KEY,
    name STRING NOT NULL,
    age INTEGER,
    profile OBJECT
)
```

The compact schemaless spelling still declares the required key:

```sql
CREATE TABLE events (PRIMARY KEY (id))
```

Declared paths may be nested. A declaration constrains those paths; it does not
turn the document into a closed object or reject undeclared fields.

The native type vocabulary is JSON's:

- `NULL`
- `BOOL`
- `NUMBER`
- `INTEGER`
- `STRING`
- `ARRAY`
- `OBJECT`
- `ANY`

Columns are nullable by default. `NOT NULL` requires a present, non-null value.
A primary key is implicitly `NOT NULL` and must resolve to a string, number, or
boolean. Common SQL names map to the JSON domain: for example, `TEXT` and
unparameterized `VARCHAR` mean `STRING`, `INT` and `BIGINT` mean `INTEGER`,
`DECIMAL` and `DOUBLE` mean `NUMBER`, and `JSON` means `ANY`. These aliases
select a JSON kind; they do not import another database's width, precision, or
storage representation. Length and precision suffixes are rejected rather than
ignored.

Names whose semantics go beyond a JSON kind are also rejected rather than
weakened silently. That includes sequence-generating `SERIAL`, fixed-scale
`MONEY`, normalized `JSONB`, fixed-width `CHAR`/`CHARACTER`/`NCHAR`, and
composite `RECORD`/`STRUCT`. Their errors name the missing behavior and suggest
the native JSON-domain type.

The store validates every inserted or replacement document against the
persisted schema. Validation therefore survives close and reopen and applies
equally to autocommit and transactional writes.

The parser can represent a compound primary-key declaration for other
front-ends, but this driver deliberately requires one path. It does not accept
a declaration whose semantics it cannot enforce.

## Exact indexes

An index is an exact posting index over one to four JSON paths:

```sql
CREATE INDEX by_kind ON events (kind)
CREATE INDEX by_tenant_state ON jobs (tenant, state)
```

Compound paths are order-sensitive. A named index uses its declared name; an
unnamed index receives a deterministic name derived from its paths.

Indexes may be declared before or after INSERT. On a populated table, the
builder reconciles immutable primary leaves in bounded writer steps, writes only
the new physical index, and atomically publishes the index root and canonical
catalog. Ordinary mutations carry no build log or build-state branch. Once
ready, INSERT, whole-document UPDATE, DELETE, and a transactional `WriteBatch`
maintain exact indexes in the same publication as the primary document change.

The query planner chooses eligible indexes automatically. An exact index is a
candidate-pruning structure, not an alternate source of truth; stored documents
are still the authoritative values.

There are no unique, partial, range, full-text, expression, or selectable index
methods.

## Ordinary views

`CREATE VIEW name [(column, ...)] AS query` stores an ordinary view definition
in the durable SQL catalog. The definition is reparsed and revalidated on
reopen, expands into the reader's plan, and owns no rows or indexes. Nested
views, CTEs, set expressions, joins, and supported uncorrelated predicate
subqueries in view queries use the same bounded query engine. A stored
definition cannot contain bind parameters. A column list may rename a leading
prefix of the result but may not be longer than it or create duplicate
effective output names.

Views form a bounded acyclic dependency graph. `DROP VIEW [IF EXISTS] name
[RESTRICT]` uses `RESTRICT` by default, as does `DROP TABLE` for a table with
dependent views. `CASCADE` and `CREATE OR REPLACE VIEW` are not approximated.
Prepared statements retain immutable view generations and refuse execution if
a referenced definition was dropped or replaced; transactions retain the view
definitions and table generations captured at `BEGIN`.

Ordinary views are read-only. `SELECT` and an `INSERT` query source may read
them, but `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, and index DDL cannot target
them. View references inside UPDATE/DELETE predicate or RETURNING subqueries
remain positioned `0A000` until those DML trees share view expansion. `CREATE`,
`DROP`, and `REFRESH MATERIALIZED VIEW` are positioned `0A000`: the durable
format has no atomic publication spanning catalog metadata and refreshed rows.

Object-kind errors are distinct from absence. `DROP VIEW [IF EXISTS]` against a
table, and `DROP TABLE [IF EXISTS]`, `TRUNCATE`, `CREATE INDEX`, table-qualified
`DROP INDEX`, or `INSERT`/`UPDATE`/`DELETE` against a view, return SQLSTATE
`42809` with the authored object position and an operation-specific hint.
`IF EXISTS` suppresses only absence. Prepared execution revalidates this rule
after table-to-view or view-to-table replacement races.

## INSERT and primary-key identity

The ordinary form binds one complete JSON document:

```sql
INSERT INTO users VALUES (?)
INSERT INTO users VALUES (?), (?)
```

The placeholder accepts a `string` or `[]byte` containing JSON. The flat form
builds a top-level object from scalar values:

```sql
INSERT INTO users (id, name, age) VALUES (?, ?, ?)
```

Flat INSERT columns must be distinct top-level fields. Nested construction and
container-valued flat operands are not synthesized; bind a complete document
when the row is nested.

The driver reads the declared primary-key path from each document and derives a
typed canonical storage key. Strings, booleans, and numbers cannot collide.
Numerically equal JSON spellings such as `1`, `1.0`, and `1e0` have one
identity, without converting through `float64`. Numeric identity supports the
full JSON exponent syntax rather than an `int64` exponent subset; the practical
bounds are the document and physical-key byte limits, not machine arithmetic.

INSERT means insert, not replacement. By default it returns
`driver.ErrDuplicatePrimaryKey` if the derived identity already exists or
appears twice in one VALUES statement. `ON CONFLICT DO NOTHING` is also
supported: conflicting rows are skipped atomically, including repeated keys in
the same VALUES batch, and a RETURNING projection reports only rows that were
inserted. Use UPDATE for replacement. `LastInsertId` is unavailable because
keys come from documents rather than a generated sequence.

An INSERT may instead take a complete query expression:

```sql
INSERT INTO archive
WITH selected AS (
  SELECT payload AS doc FROM events WHERE retired = TRUE
)
SELECT doc FROM selected
ON CONFLICT DO NOTHING
RETURNING id
```

The source may begin with `SELECT`, `WITH`, `TABLE`, or a parenthesized query;
set expressions, CTEs, joins, and ordinary views are valid inside it. Its
result must have exactly one column, and every emitted value must be a complete
JSON document. A target column list with a query source is rejected: this slice
does not infer row-to-document construction. Statically scalar outputs fail at
prepare time; dynamically typed outputs are checked row by row before any
write is visible. A `VALUES (?)` leaf propagates that document-parameter role
through nested set, CTE, and recursive-CTE source lineage without changing
unrelated scalar parameter roles.

The source is fully evaluated from one coherent pre-statement snapshot. This
also defines target-equals-source behavior: the query cannot observe documents
inserted by its own statement. Schema validation, primary-key derivation,
duplicate and existing-key decisions, routing, durable batch admission, and
the complete RETURNING projection are staged before one table publication.
Any source, cancellation, budget, shape, schema, key, conflict, routing, or
RETURNING error publishes no prefix. `ON CONFLICT DO NOTHING` preserves source
order and RETURNING reports only admitted rows.

Inside a transaction the source reads the isolation cut in effect when the
statement began plus the overlay that already existed; successful rows enter
that overlay as one atomic statement. The source and staged write share the normal result,
intermediate, cancellation, transaction, and durable document/batch limits.
The root query result is admitted while it grows against the remainder left by
every simultaneously live derived, CTE, set, window, join, and scalar spool;
that exact retained total seeds atomic write staging rather than opening a
second allowance. Validation tape, encoded-key/map ownership, document/index
storage, and publication records are admitted before growth. Existing-key
checks use a snapshot-linearized no-copy primary probe, so an unrelated large
stored document is never materialized merely to decide a conflict. Prepared
execution revalidates every physical and view dependency. Placed writes stream
the already-staged records through fixed-width stack routing state, so they
retain no second per-row vector; transaction staging charges the transaction
mutation shape rather than the smaller autocommit publication shape. RETURNING
does not open a new allowance: it releases exactly the source root Result that
it invalidates, then admits its nested work and root Result beside the
still-live source spools and atomic staging.

The allocation claim is deliberately lane-specific. Allocation-gated tests
show zero allocations after warm-up for the direct-session no-op conflict and
absent-source paths, including placed-routing and escaped composite shard-key
variants. They do not claim that a successful durable publication is
allocation-free.

The durable batch accepts documents above `InlineValueBytes` through
`MaxDocumentBytes`; the inline threshold selects a representation rather than
an admission limit. Mixed inline and overflow inserts, replacements, deletes,
duplicate-resolved keys, and exact-index changes stage before one collection
generation and one recovery-journal batch record. Pinned snapshots retain old
durable or volatile overflow chains until their generation can be reclaimed.
Admission, retirement-capacity, or journal failure cannot expose a partial
logical batch; recovery replays the complete journal record or none of it. The
warmed replacement benchmark currently reports the same one publication-state
allocation for the inline control and mixed overflow cases; this is not a
zero-allocation `Collection.Update` claim.

There is no caller-supplied physical-key row form. `VALUES` without a field list
contains exactly one complete document; a field-list INSERT includes the
declared primary-key field like any other field. The declaration is the one
source of row identity.

## SELECT

The driver exposes the shared SELECT surface, including:

- whole-document and JSON-path projection;
- `=`, `!=`, `<>`, `<`, `<=`, `>`, and `>=`;
- `IN`, `NOT IN`, `BETWEEN`, and `NOT BETWEEN`;
- `LIKE`, `NOT LIKE`, `ILIKE`, and `NOT ILIKE` with `%`, `_`, and the default
  backslash escape;
- `IS NULL`, `IS NOT NULL`, `IS MISSING`, and `IS NOT MISSING`;
- JSON containment with `@>`;
- `AND`, `OR`, `NOT`, and parentheses with SQL three-valued logic;
- `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`;
- `GROUP BY` and `HAVING`;
- multi-key `ORDER BY`;
- `LIMIT` and `OFFSET`.

The `database/sql` adapter uses positional `?` placeholders. `pgwire` accepts
PostgreSQL-style `$1`, `$2`, and so on and maps them onto the same typed
parameter slots. Named parameters are not supported.

## Scalar expressions and CAST

The scalar lane supports exact `+`, `-`, `*`, `/`, `%`, unary signs, and string
concatenation with `||` over paths, literals, parameters, and supported
aggregates. Decimal arithmetic uses the same bounded exact-number kernel as
aggregates and never narrows through `float64`.

Searched and simple `CASE` are supported as SELECT-list scalar expressions.
The complete CASE value may also be compared or tested with `IS [NOT] NULL`
in a WHERE scalar condition:

```sql
SELECT CASE WHEN active AND score >= ? THEN 'ready' ELSE 'held' END,
       CASE kind WHEN 'a' THEN 1 WHEN 'b' THEN 2 END
FROM jobs
```

A searched WHEN accepts boolean scalar conditions, comparisons,
`IS [NOT] NULL`, and `AND`/`OR`/`NOT`. Other predicate families, predicate
subqueries, and aggregate conditions remain positioned refusals. A simple CASE
compares its selector with each WHEN value in order using exact same-domain SQL
equality; NULL does not match NULL and no coercion is inferred.

CASE evaluates the selector once, conditions in source order, and only the
chosen result. AND/OR and later arms short-circuit, so conversions and
arithmetic errors in an unchosen per-row branch are unobserved. Statically
decisive searched and simple arms are also removed from the executable
dependency plan after full validation; their path payloads and exact aggregate
state consume no runtime budget, while aggregate/group cardinality remains SQL
correct. A data-dependent branch can still require its path column in the
shared base plan even on rows that choose another result, so that dependency's
intermediate/result footprint remains budgeted. An omitted ELSE produces SQL
NULL. Boolean, numeric, text, JSON, and NULL result domains are resolved at
prepare time where possible; irreconcilable static arms are positioned
`0A000`, while a conflicting dynamic value is `42804`. Explicit CAST is the
way to choose one result domain. CASE nesting shares the 64-level scalar bound,
a statement may hold at most 1,024 WHEN arms, and cancellation checks remain
bounded. Allocation-gated warmed query-engine CASE paths retain branch
workspace and allocate no heap objects; cold prepare, `database/sql`, and
protocol framing are outside that claim.

`CAST(expression AS type)` has four executable target domains:

- `TEXT`: preserves decoded strings, emits canonical boolean text and exact
  decimal text, and exposes container JSON without numeric conversion;
- `BOOLEAN` or `BOOL`: accepts booleans and trimmed, case-insensitive unique
  prefixes of `true`/`false`, `yes`/`no`, and `on`/`off`, plus `1` and `0`;
- `NUMERIC` or `DECIMAL`: accepts numbers or trimmed signed decimal text with
  an optional fraction and exponent through the bounded exact-decimal kernel;
- `JSON`: parses string input as JSON text and preserves its exact spelling;
  already typed non-string JSON scalar or container values retain their value.

SQL NULL propagates through every target. Invalid input text is `22P02`, an
unrepresentable number is `22003`, and a source/target kind mismatch is
`42804`; diagnostics point at the authored CAST and do not echo runtime input.
Type modifiers, multiword types, quoted type names, arrays, `JSONB`, and the
PostgreSQL `::` shorthand are positioned `0A000`, not silently weakened.

Projection expressions are lazy: WHERE filtering and final OFFSET/LIMIT row
admission happen before a projected CAST or arithmetic expression is
evaluated. A conversion or division error in a filtered or skipped row
therefore does not fail the statement or consume retained CAST workspace.
`ORDER BY` may name the explicit alias of a computed or aggregate SELECT
expression, or a positive one-based SELECT-list position. Positional keys
cover ordinary paths, computed scalars, window outputs, and grouped aggregate
outputs; wildcard
projections remain a positioned refusal because their expanded width is known
only from the prepared source schema. Deferred ordering evaluates and owns the
sort keys for every predicate-surviving row, performs one stable mixed
path/computed-key sort, and only then applies
OFFSET/LIMIT and the remaining projections. Consequently, with a positive
LIMIT, an invalid sort key still fails even when the selected slice omits its
row, while an unrelated projection error in an omitted row remains unobserved.
LIMIT 0 short-circuits the post-output scalar stage without evaluating its
keys or projections, matching the existing scalar execution bound while any
required base scan/grouping still obeys its own semantics. Duplicate matching
aliases are ambiguous (`42702`), while non-positive, fractional, or
out-of-range positions are invalid column references (`42P10`). Direct scalar
expressions in ORDER BY
remain positioned refusals; authors name the computed output or its position.
For a grouped deferred order, HAVING filters every reduced group before sort
keys are evaluated and before OFFSET/LIMIT select the result tail. A rejected
group therefore cannot consume a limited slot, and an invalid computed sort
key in a rejected group remains unobserved. Aggregate-output positions and
computed or aggregate output aliases all use this ordering.
Prepared warm execution reuses the bounded intermediate workspace and the
covered CAST and ordered-scalar variants allocate no heap objects.

## Set operations

`UNION`, `INTERSECT`, and `EXCEPT` support both `ALL` and `DISTINCT` over
`SELECT`, `VALUES`, and `TABLE` leaves. `INTERSECT` binds more tightly than
`UNION` and `EXCEPT`; parentheses retain authored grouping and give an operand
its own ORDER BY/LIMIT/OFFSET. A final tail names outputs from the syntactic
first operand or uses a positive output position; the first operand's names
and ordinal schema also describe the result. A deferred wildcard width cannot
bind a positional tail at parse time and is refused rather than guessed.

All operands must have equal width. Multiset multiplicity, SQL NULL equality
for row identity, exact decimal values, and container spellings are preserved.
Every leaf reads one coherent statement snapshot and the complete tree shares
finite row, byte, depth, node, result, and intermediate limits. Admission,
cancellation, dependency, or execution failure publishes no partial cursor;
prepared warm execution reuses the set workspace.

## Window functions

Window expressions are supported in the SELECT list for `ROW_NUMBER`, `RANK`,
`DENSE_RANK`, `NTILE`, `PERCENT_RANK`, `CUME_DIST`, `LAG`, `LEAD`,
`FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`, and the `COUNT`, `SUM`, `AVG`, `MIN`,
and `MAX` aggregates. Arguments, partition keys, and window order keys are JSON
paths; offsets, bucket counts, NTH positions, and LAG/LEAD defaults use the
documented literal or positional-parameter forms.

Inline and named windows support `PARTITION BY`, multi-key `ORDER BY` with
direction and explicit NULL placement, earlier-name inheritance, and `ROWS`,
`GROUPS`, or `RANGE` frames. Every frame boundary and `EXCLUDE NO OTHERS`,
`CURRENT ROW`, `GROUP`, or `TIES` is implemented. RANGE offsets retain exact
decimal spelling and require one numeric order key; NULL and missing order
values form peers rather than being coerced. The ordered default frame includes
the current peer group.

Stages with equivalent partition and order specifications share their sort.
The executor preserves stable peer order, SQL NULL/missing identity, exact
decimal aggregate behavior, transaction snapshots, finite memory admission,
cancellation, and failure atomicity. EXPLAIN reports the logical stages and
ANALYZE reports the measured execution. Prepared warm execution reuses window
state and the covered paths allocate no heap objects.

Window `FILTER`, DISTINCT window aggregates, null-treatment modifiers such as
`IGNORE NULLS`, arbitrary scalar arguments or keys, and window expressions
outside the SELECT list are not part of this SQL slice; final ORDER BY may name
the output alias or position of a SELECT-list window. A correlated window key
inside LATERAL and a window in a recursive CTE term remain positioned refusals.

Primary-key equality and membership use point reads. Eligible exact predicates
use posting masks and document rechecks. Predicates without a usable access path
remain correct full scans; an index is an optimization, not a requirement for a
WHERE clause. Sorting, grouping, aggregation, HAVING, OFFSET, and final
projection remain query-engine operations.

`EXPLAIN SELECT ...` returns one `QUERY PLAN` text column containing compact,
versioned JSON for the bound logical plan. It reports the full predicate tree,
source-aware access-path alternatives when the catalog is available, the
filter/late projection split, grouping, ordering, limit, and join shape. Plain
EXPLAIN never scans rows, but it validates every physical dependency against
one coherent cut: a fresh capture outside a transaction or the cut pinned by
BEGIN inside one. A stale prepared plan therefore cannot outlive a dropped or
unsnappable relation. `EXPLAIN ANALYZE SELECT ...` executes the target once
through the normal query path and adds measured elapsed time, result rows,
index work, scan work, spills, join strategy counters, and the measured access
path from that execution.
The physical choice remains adaptive where memory admission or cardinality
decides between indexed and scan paths; the JSON says so instead of pretending
to be a static choice. Both forms use the engine's vibejson encoder.

## Generalized joins

`INNER JOIN`, `LEFT [OUTER] JOIN`, `RIGHT [OUTER] JOIN`, `FULL [OUTER] JOIN`,
and `CROSS JOIN` compose into ordered chains. Each operand may be a physical
collection, a derived table, or a CTE relation:

```sql
WITH active AS MATERIALIZED (
  SELECT id, name FROM users WHERE enabled = TRUE
)
SELECT active.name, o.id, p.state
FROM active
JOIN (
  SELECT id, user_id, total FROM orders WHERE total >= ?
) AS o ON active.id = o.user_id AND o.total <= ?
LEFT JOIN payments AS p
  ON o.id = p.order_id AND p.state = 'settled'
ORDER BY o.id
```

An `ON` expression may contain multiple equality keys and residual predicates.
The executor hashes a composite key when at least one cross-relation equality
is available, then evaluates the complete `ON` expression on each candidate.
A keyless condition and `CROSS JOIN` use the bounded nested-loop kernel. SQL
NULL never equals another key. `JOIN ... USING (tenant, region, id)` is the
composite convenience spelling; every listed column participates in matching.

Fan-out is exact. LEFT and RIGHT preserve the corresponding input, FULL
preserves both, and every missing partner is represented by SQL NULL in all of
that operand's projected columns. CROSS emits the Cartesian product. A WHERE
predicate runs after the join relation exists, so filtering a nullable side has
ordinary SQL outer-join semantics rather than being pushed into an operand.

Output naming is independent of the selected physical kernel. An unaliased
path from source zero keeps its path (`id`, `value`); joined paths remain
range-variable-qualified (`o.id`, `o.value`). `AS` overrides that rule.
Explicitly repeated aliases remain repeated ordinal columns and are emitted as
duplicate pgwire `RowDescription` names; an ambiguous named reference is a
typed error.

The driver captures every participating durable collection in one coherent
snapshot. All publication gates are held while the generation leases are
acquired, so the joined generations genuinely coexisted. This prevents a join
from observing a new generation of one table with an already-obsolete
generation of another. Independent autocommit writes to different tables remain
separate publications; a multi-table transaction is the crash-atomic commit
path.

Autocommit generalized joins receive that coherent durable catalog directly.
Physical dependencies therefore stay on durable sources, and eligible operand
subplans retain primary-point and exact-index execution instead of first being
copied into an adapter catalog. A sole physical dependency hidden behind a CTE
still drives `Statement.Collection()` and the direct durable source. The legacy
single-clause physical INNER/LEFT one-key shape keeps its existing
storage-aware, bounded heap fan-out path; the prepared-plan classifier chooses
once and leaves that fast path unchanged. A statement without generalized
joins has no join state and pays one nil pointer test at execution.

Every physical, derived, and CTE operand spool and each live joined relation is
charged to the statement-wide `query.ExecOptions.IntermediateBytes` allowance.
Composite-hash build state, decoded keys, and output-pair workspace are charged
to `query.ExecOptions.JoinPairBytes`. Both default to finite 64 MiB limits.
Admission, cancellation, and evaluation errors publish no partial cursor or
pgwire `DataRow`, release the coherent cut, and leave prepared and protocol
sessions reusable. Warm typed `QueryInto` execution reuses relation and join
storage.

`EXPLAIN` reports every stage's join kind, logical algorithm, keys, residual
predicate, and operand. `EXPLAIN ANALYZE` adds the actual algorithm, build rows,
and emitted pair count from the one measured execution. Both plain and prepared
forms revalidate all recursively discovered physical dependencies before
execution.

`INNER JOIN LATERAL`, `LEFT JOIN LATERAL`, and `CROSS JOIN LATERAL` execute the
right relation per left row and can bind any preceding range variable. Nested
LATERAL chains inherit outer frames. Correlated paths are supported in the
right projection, predicates, join predicates, grouping, aggregates, and
HAVING; uncorrelated LATERAL operands take the ordinary derived-relation path.
Preparation compiles each placeholder-free correlated right relation once;
parameterized children lower once per top-level authored binding. Immutable
parser sidecars map every authored outer-path occurrence to a stable typed slot,
and a nested APPLY links inherited slots to its containing lexical frame.
Execution only rebinds those reusable slots for each left row; it does not
regenerate SQL, reparse the child, prepare another child plan, or lower the
child per outer row. Dynamic equality slots retain Segment postings and heap or
durable exact-index selection, including compound indexes. The ordinary
non-LATERAL path owns no slot frame. The APPLY executor preserves exact values,
three-valued gates, snapshot and transaction semantics, cancellation, and
shared intermediate/pair/aggregate budgets without memoizing rows whose
volatility is not proven.

Correlation on the nullable side of `RIGHT` or `FULL JOIN LATERAL`, `USING` on
a correlated LATERAL join, mixed local/outer OR predicates, correlated window
keys or DISTINCT projections, outer derived wildcards, and grouped-correlated
ORDER BY/LIMIT/OFFSET remain positioned `0A000`. `NATURAL JOIN` and mutation
joins also remain unsupported. SQL exposes no physical storage-key
pseudo-column: `"$key"` is an ordinary quoted JSON field. Join the declared JSON
primary-key fields when relational identity is intended.

## Predicate subqueries

Uncorrelated subqueries are supported in predicates:

```sql
SELECT id
FROM orders
WHERE customer_id IN (
  SELECT id FROM customers WHERE tier = ?
)
```

The accepted forms are `IN (SELECT ...)`, `NOT IN (SELECT ...)`,
`EXISTS (SELECT ...)`, and a scalar comparison such as
`customer_id = (SELECT id FROM aliases WHERE active = TRUE)`. The nested
statement runs once per outer execution against the same coherent catalog
snapshot. Its scalar output is retained in reusable typed slots and the outer
predicate lowers to the existing sorted membership or comparison plan; there
is no per-row nested execution and warmed execution allocates nothing.

`IN` preserves SQL three-valued logic when the nested result contains `NULL`.
An empty scalar result is `NULL`; a scalar result with more than one row is an
error. Every nested result is charged to one statement-wide intermediate byte
account; nesting does not mint another materialization allowance.

The bounded correlated forms are `EXISTS`, `NOT EXISTS`, `IN`, `NOT IN`, and a
path-to-scalar-subquery comparison as the complete outer `WHERE` predicate or
one of its top-level `AND` terms. One direct `NOT (...)` wrapper around such a
leaf is also accepted; a `NOT` over a larger boolean tree is not:

```sql
SELECT o.id
FROM orders AS o
WHERE EXISTS (
  SELECT 1
  FROM customers AS c
  WHERE c.id = o.customer_id
    AND c.active = TRUE
)
```

The proof boundary is intentionally mechanical:

- the outer statement and child each name exactly one physical relation;
- one or more top-level child predicates compare one local path to one captured
  outer path with `=`;
- every captured outer occurrence is consumed by those equalities, and every
  remaining child predicate is inner-only;
- `IN`/`NOT IN` and scalar forms project exactly one non-aggregate child path;
- the child contains no join, derived relation, CTE, set operation, nested
  subquery, grouping, aggregate, window, DISTINCT, ordering, limit, or offset.

A single-key `EXISTS` keeps the established adaptive semi-join implementation,
and direct `NOT EXISTS` keeps its two-valued anti-semi implementation. EXPLAIN
therefore retains `decorrelated-exists-semi` and
`decorrelated-exists-anti` for those plans. Composite `EXISTS`/`NOT EXISTS` and
correlated value forms use one fanout-free grouped mark build and report
`decorrelated-composite-exists-semi`,
`decorrelated-composite-exists-anti`, `decorrelated-correlated-in`,
`decorrelated-correlated-not-in`, or `decorrelated-correlated-scalar` in the
separate `marks` array. The child relation and its local filter execute once
per statement. Duplicate child rows never duplicate an outer row.

Every correlation tuple component must be non-NULL to address a group because
SQL `=` does not equate NULL or missing values. Consequently a NULL/missing
correlation component makes the child group empty. This distinction matters
for null-aware value predicates:

| Correlated group | `IN` | `NOT IN` |
| --- | --- | --- |
| no rows | FALSE | TRUE |
| exact non-NULL projected match | TRUE | FALSE |
| no match, but a projected NULL/missing exists | UNKNOWN | UNKNOWN |
| rows, no match or projected NULL, non-NULL probe | FALSE | TRUE |
| rows, NULL/missing probe | UNKNOWN | UNKNOWN |

Thus `NULL NOT IN (empty-correlated-group)` is TRUE, while `NULL NOT IN
(nonempty-group)` is UNKNOWN. Scalar comparison is similarly structural: no
group or one NULL/missing result yields UNKNOWN; exactly one known value uses
the authored operator; two rows raise cardinality violation `21000`. The two
rows need not differ—duplicate identical values and two NULL/missing rows still
violate scalar cardinality. A multi-row group raises only when an outer row
actually probes that group; unrelated or outer-filtered groups cannot fail the
statement during the build.

Correlation keys, probes, and projected values retain exact scalar comparison.
Numbers do not round through binary floating point. Untyped objects and arrays
use canonical stored JSON identity: storage-normalized object member order
compares equal, array order remains significant, and changed members or values
remain unequal. Variable-width grouped state is copied into the statement's
accounted reusable arena; it never retains snapshot-borrowed bytes.

The existing scalar single-key `EXISTS` path may use a ready exact outer index.
Grouped composite and value marks currently build the child once and scan the
complete outer relation. A matching compound index is accepted by the catalog
but is not claimed as a grouped candidate bound; indexed and unindexed runs are
required to be differential-equivalent. This conservative policy is mandatory
for containers until an index can certify structural completeness, and for
`NOT IN` and scalar comparison. Candidate-bounding scalar equality could hide a
second child row that must raise `21000`.

All relations come from one coherent statement snapshot. Explicit transactions
read their current isolation cut plus pending writes, self-correlation reuses
that same catalog generation, and prepared execution revalidates every physical
dependency before scanning. Group tables, deduplicated value membership, and
copied variable-width values are charged to the ordinary work-memory account.
Cancellation, exact one-byte-short admission, and scalar cardinality errors
publish no cursor, pgwire `DataRow`, or `CommandComplete`; prepared, driver, and
protocol sessions remain reusable after recovery.

Correlation below `OR`, generalized or nested `NOT`, unconsumed outer
references, non-equality correlation, correlation in `JOIN ON`, and nested
predicate subqueries inside the child remain positioned `0A000`. The same
refusal applies to correlated scalar subqueries in the projection list,
searched `CASE`, `HAVING`, or `ORDER BY`, and to every excluded child shape
listed above. Predicate subqueries inside a correlated LATERAL `WHERE` or
`HAVING` remain explicit positioned refusals: the APPLY slot frame is compiled
once, but inherited expression correlation is not yet threaded into a nested
predicate-subquery plan. LATERAL therefore does not silently reinterpret such
references as local paths. Pgwire maps refusal to one positioned `0A000`
ErrorResponse; positions count authored UTF-8 characters. A runtime `21000` or
refusal inside an explicit transaction reports failed-transaction status until
`ROLLBACK`.

## Common table expressions

SELECT-valued common table expressions, including the bounded recursive
subset, are supported by direct and prepared `database/sql` execution and by
pgwire's simple and extended protocols:

```sql
WITH active(id, score) AS MATERIALIZED (
  SELECT id, score FROM users WHERE enabled = TRUE
), ranked AS NOT MATERIALIZED (
  SELECT id, score FROM active WHERE score >= ?
)
SELECT id FROM ranked ORDER BY score DESC
```

A `WITH` list is lexical. A definition can see earlier siblings and inherited
outer definitions; a nested `WITH` may shadow them. It cannot see itself or a
later sibling as a CTE. Definitions are SELECT-only. Scope also reaches
predicate subqueries and every operand in a join chain.

Column alias lists rename the corresponding leading outputs. An alias list may
be shorter than the result, but not longer. Duplicate output names remain
distinct ordinals and are repeated verbatim in pgwire `RowDescription`; `*`
preserves them, while a named ambiguous reference is rejected. Undefined,
ambiguous, duplicate-definition, alias-arity, self/forward-reference, and
unsupported-feature errors carry typed SQLSTATEs and exact UTF-8 character
positions over pgwire.

Materialization policy is observable and exact:

- `MATERIALIZED` evaluates a definition once and shares its result;
- `NOT MATERIALIZED` evaluates each syntactic reference independently;
- the default shares a multiply referenced definition, while a single safe
  identity-shaped reference may be fused into its defining plan;
- every other single reference uses one private relation spool.

`EXPLAIN` reports each definition's mode, reason, reference count, and measured
evaluation count. Both plain and prepared EXPLAIN revalidate recursively found
physical dependencies, so a plan cannot silently survive `DROP TABLE`.

Physical dependencies are walked recursively once per definition and
deduplicated in source order. If the whole CTE graph reads one durable
collection, `Statement.Collection()` resolves that collection and the driver
keeps the direct durable source, primary-point path, and eligible exact-index
execution, including when the CTE is a generalized-join operand. Multiple
physical collections execute from one reusable coherent generation cut;
generalized joins consume that cut directly, while other catalog-requiring CTE
shapes retain their bounded adapter materialization. In a transaction the same
graph reads the current isolation cut plus the transaction overlay. Repeatable
Read and Serializable retain that cut from BEGIN; Read Committed refreshes it
per physical statement. Every mode preserves read-your-writes.

Relation spools share the statement-wide intermediate allowance described
below. Admission, cancellation, or binding failure publishes no partial spool
or result and leaves a prepared statement reusable. Warm CTE execution and the
ordinary no-CTE path allocate no marginal heap storage; a statement without
`WITH` does not initialize CTE state.

A recursive definition is an anchor followed by `UNION` or `UNION ALL` and one
recursive term containing exactly one direct self-reference. Execution is a
breadth-first fixpoint over the previous delta. `UNION` suppresses duplicate
SQL rows before they enter the next delta; `UNION ALL` preserves them.
Recursive definitions may use preceding ordinary CTEs, and later recursive
definitions may depend on earlier ones. Joins are admitted when the recursive
relation remains on a preserved side. Materialization policy, coherent catalog
snapshots, prepared dependency checks, exact values, cancellation, and the
statement-wide intermediate allowance continue to apply.

The default physical bounds are 1,000 recursive iterations, 100,000 result
rows, and 64 MiB of owned fixpoint state. Arity and every bound are checked
before partial publication. Anchor self-reference, multiple or nested
self-references, mutual/forward recursion, `INTERSECT`/`EXCEPT` recursion,
aggregation or grouping in the recursive term, a self-reference on the
nullable side of an outer join, top-level compound queries containing
`WITH RECURSIVE`, and `SEARCH`/`CYCLE` remain positioned typed refusals.
Data-modifying CTE bodies also remain unsupported. A source-independent
FROM-less SELECT is a one-row relation, so constant and parameterized SELECT
CTEs compose with the same materialization policy as stored-row definitions.

## Derived tables

A `SELECT` may be used as a `FROM` relation, including any operand in a join
chain:

```sql
SELECT d.customer, d.total
FROM (
  SELECT customer, SUM(amount) AS total
  FROM orders
  GROUP BY customer
) AS d
WHERE d.total > 100
ORDER BY d.customer
```

The alias is mandatory; `AS` is optional. The inner statement completes its
filtering, grouping, ordering, offset, and limit before the outer statement
consumes it. Outer filtering, grouping, aggregates, ordering, offset, limit,
and `d.*` are supported. Duplicate inner output names remain distinct
ordinals: wildcard expansion preserves them, while a named reference is a
typed ambiguous-column error. Exact decimal spellings, SQL NULL, and nested
JSON values cross the relation boundary without scalar conversion.

Derived rows are held in a private ordinal-addressed columnar spool and are
charged, along with every simultaneously live nested result, to
`query.ExecOptions.IntermediateBytes`. Row, column, scalar, and payload storage
is measured and admitted before any spool slice grows. Cells cross the boundary
without JSON row encoding or a second decode pass. The spool feeds the existing
scan, predicate, grouping, ordering, and aggregate semantics through a dedicated
columnar kernel; warmed in-memory execution remains allocation-free.

Typed sessions configure the shared allowance with
`Session.SetIntermediateLimit`. Pgwire servers expose the same control as
`Options.MaxIntermediateBytes`; zero selects the finite default and `-1`
explicitly disables the bound. Intermediate exhaustion is SQLSTATE `54000` and
cannot emit a partial result row.

The derived query reads the same catalog snapshot as the outer statement.
Ordinary joined derived relations materialize once as bounded operands; a
correlated `LATERAL` relation instead uses the APPLY path described above.

Joins inside a transaction use the current isolation cut plus that
transaction's staged overlay. Repeatable Read and Serializable preserve the
BEGIN cut; Read Committed refreshes the complete cut per physical statement.

## UPDATE and DELETE

UPDATE replaces a complete document:

```sql
UPDATE users SET "$doc" = ? WHERE id = ?
DELETE FROM users WHERE state = ?
```

There is no partial JSON path editor. A replacement document must validate
against the table schema and must retain the same derived primary key. Because
one UPDATE statement supplies one constant whole document, a predicate that
matches several distinct primary keys returns `ErrUpdatePrimaryKey` before
publication; use an explicit transaction with one replacement per key. DELETE
removes every selected document and its exact-index postings.

Bounded mutation windows are also supported:

```sql
DELETE FROM users WHERE state = ? ORDER BY id DESC LIMIT 10
UPDATE users SET "$doc" = ? WHERE state = ? ORDER BY id LIMIT ?
```

The current durable implementation accepts one `ORDER BY` term on the
declared primary-key path and requires `LIMIT`; an unordered `LIMIT` is also
supported. Selection is global and bounded before publication, so the limit
does not reset at scan-batch boundaries. `OFFSET`, multi-key ordering,
non-primary-key ordering, and `ORDER BY` without `LIMIT` remain explicit
errors.

Mutation WHERE clauses use the same predicate compiler as SELECT, including
three-valued logic. Primary-key equality and membership take the point path;
other predicates enumerate documents through the shared filter. Candidate
index pruning is currently a SELECT optimization, so a filtered UPDATE or
DELETE may scan even when the equivalent SELECT can prune through an index.

Multi-row INSERT and multi-document DELETE use one `Collection.Update` and are
failure-atomic within their table. `database/sql` accepts one statement per
`Exec`, so an atomic mixed-operation group also uses an explicit transaction.
The collection's bounded batch admission still applies.

## Transactions

Default and Read Committed transactions replace their committed base with one
coherent catalog cut before every physical statement. Repeatable Read and the
typed Snapshot alias retain the generation-leased BEGIN cut. Serializable also
retains the BEGIN cut. Proven primary-key point reads and misses are tracked
exactly; scans, secondary/range predicates, joins, nested execution, and
bounded-read overflow use a relation-coarse dependency. Publishing commits
validate those dependencies, rejecting write skew and phantoms while allowing
independent same-table point writers. Every policy overlays staged writes and
preserves read-your-writes; none holds a writer across client think-time.

A transaction may read and write several tables. One dirty table commits
through today's `Collection.Update` path. Two or more dirty tables commit
through the durable multi-collection protocol: conditional journal records in
each participant's journal and one decision sync in `txn.vtm`, so visibility
and crash recovery are all-or-nothing across participants on journal-backed
lanes. Writes to tables absent at BEGIN materialize an empty durable file as a
participant; an aborted or crashed transaction can leave that empty table
behind as documented catalog residue. DDL is rejected inside a transaction.

COMMIT uses a bounded per-table, per-key publication clock. Read Committed
stamps a key when it first enters the overlay from a statement cut; fixed-cut
modes stamp it at BEGIN. If that key was published after its observation,
including a change-and-restore (ABA), `driver.ErrTransactionConflict` is
returned (SQLSTATE `40001` on the wire) and nothing is published. This
first-committer-wins check retains revisions rather than copies of the original
documents, and disjoint-key writers remain independent. Conflict scope is
handle-mediated — the same `*sql.DB` or pgwire endpoint. The clock retains at
most 4,096 changed keys; overflow moves a history floor and conservatively
rejects observations older than that floor instead of risking a missed
conflict. There is no auto-retry: a retry re-runs caller code.
Otherwise all staged puts and deletes are submitted — one dirty table through
`Collection.Update`, several through `UpdateCollections` — including
exact-index maintenance. ROLLBACK closes the leases and discards the overlay.

A COMMIT whose decision-record sync fails returns
`ErrCommitOutcomeUnknown` (SQLSTATE `40003` on the wire), poisons every
collection under the catalog against further writes, and must be resolved by
closing and reopening the database. The unknown outcome is atomic: reopen
reveals every participating table's writes or none of them.

### Savepoints

`SAVEPOINT name`, `RELEASE [SAVEPOINT] name`, and
`ROLLBACK TO [SAVEPOINT] name` are supported in the SQL grammar, the
`database/sql` driver, and pgwire. Frames are LIFO marks over per-table overlay
watermarks (staged-order lengths plus a displaced-entry undo log for keys
overwritten after the mark). `ROLLBACK TO` rewinds the overlay without ending
the transaction and returns a failed session to in-transaction state — Ready
for Query `T` on the wire. `RELEASE` erases marks LIFO through the name;
duplicate names shadow earlier marks, operations select the newest, and
releasing that mark reveals the previous one. `ROLLBACK TO` retains its target.
Real commitment remains only at COMMIT. The stack is bounded at 64 frames
(`ErrTooManySavepoints`); an unknown name is
`ErrSavepointNotFound` (SQLSTATE `3B001`). Savepoint or release in a failed
transaction is SQLSTATE `25P02`. `database/sql` users reach all three as
statement text through `tx.Exec`. All three remain available in a read-only
transaction because they change only transaction-local control state; DML and
DDL remain refused. Nested native `Update` closures are a typed error, not an
implicit savepoint.

The typed runtime accepts Default/Read Committed, Repeatable Read/Snapshot, and
Serializable. `database/sql` maps its corresponding standard levels directly,
and the PostgreSQL spellings admitted by `pgwire` are listed below. Read
Uncommitted and Linearizable are refused rather than silently weakened.
Read-only transactions reject mutations.

Transactions and atomic multi-row statements are bounded by the collection's
`MaxBatchDocuments` and `MaxBatchBytes`, plus cross-table `TxnLimits`
(default 16 collections; document and byte totals four times the
single-collection defaults). Overflow returns `driver.ErrTransactionTooLarge`
with the failing dimension named and publishes nothing. Unsupported durability
lanes refuse multi-table commits with
`driver.ErrTransactionUnsupportedLane`.

PostgreSQL sessions expose the same state as ReadyForQuery `I`, `T`, or `E`.
`BEGIN`, `BEGIN READ WRITE`, `BEGIN READ ONLY`, and `BEGIN ISOLATION LEVEL` with
`READ COMMITTED`, `REPEATABLE READ`, or `SERIALIZABLE` map to the typed runtime;
other isolation claims are rejected rather than silently weakened. An error inside
an explicit transaction leaves ROLLBACK and `ROLLBACK TO` usable; other
commands report failed-transaction status (`25P02`) until one of those
recovers the session. COMMIT in that failed state performs the rollback and
emits PostgreSQL's `ROLLBACK` command tag. Chained transactions and
transaction-mode clauses beyond the admitted BEGIN spellings stay refused.

An extended-protocol batch of non-DDL stored-row statements runs in one
implicit transaction through Sync. A multi-statement simple Query without
explicit transaction control does the same, so an error rolls back earlier
writes in that message. Savepoint commands are admitted as non-terminal members
of an explicit transaction block. Catalog DDL cannot yet participate in the
durable transaction overlay: CREATE TABLE and CREATE INDEX are atomic
individually, must run outside an explicit transaction, and must be the only
non-empty statement in a simple Query message and the only catalog execution
between extended-protocol Sync points. This is an explicit `0A000` boundary
rather than partial transactional behavior. Extended-protocol DDL publishes
when its Execute completes; if a client then violates the boundary with another
catalog execution before Sync, that later execution is refused but the already
completed DDL is not retroactively rolled back.

## NULL, missing fields, and driver values

SQL predicate lowering implements three-valued logic. A projected missing path
and an explicit JSON null both scan as SQL `NULL`, while `IS MISSING` remains
available when document existence itself matters. A primary key may be neither
missing nor null.

Unlike conventional typed SQL, comparisons do not coerce values. Numbers
compare by exact decimal value, strings by decoded content, and different JSON
types use the engine's fixed total order. Aggregates are numeric and skip
non-numeric values. Ascending order places nulls first and descending order
places them last; `NULLS FIRST` and `NULLS LAST` are not accepted.

`SUM`, `MIN`, and `MAX` preserve exact decimal values and never route through
the durable store's optional float64 covers. `AVG` emits an exact finite
quotient when it fits the 34-significant-digit policy and otherwise rounds once
with ties to even. Exact coefficients, exponents, extrema, worker partials, and
result digits share one bounded execution budget (16 MiB by default); exhaustion
returns `query.ErrAggregateBudget` rather than silently narrowing to float64.

The result mapping is lossless:

| JSON value | `driver.Value` |
|---|---|
| null or missing projection | `nil` |
| boolean | `bool` |
| integral number fitting `int64` | `int64` |
| any other number | `[]byte` containing its exact decimal spelling |
| string | decoded `[]byte` |
| object or array | JSON `[]byte` |

Document parameters are JSON text in `string` or `[]byte`. Ordinary scalar
placeholders use the standard `database/sql/driver` values: `nil`, `bool`,
`int64`, `float64`, `string`, or `[]byte`. A `[]byte` in scalar position is a
JSON string, not an embedded document.

The driver additionally accepts `encoding/json.Number` in scalar position. It
preserves the decimal spelling as a numeric value instead of allowing
`database/sql` to convert the named string type. This is the lossless binding
for integers outside IEEE-754's exact range and works consistently in flat
INSERT values, primary-key and ordinary predicates, and integer-valued LIMIT or
OFFSET placeholders. Invalid number spellings and non-integral LIMIT/OFFSET
values are rejected.

## Bounded grammar

The parser rejects pathological statement shapes before execution:

- total SQL text is limited to 16 MiB;
- predicates may nest at most 64 levels;
- a statement may hold at most 65,536 placeholders;
- SELECT, GROUP BY, ORDER BY, table definitions, and multi-row VALUES have
  explicit item bounds of 1,024;
- an exact index may name at most four paths.

Execution retains its own bounded workspaces, query-memory budget, snapshot
lease limits, maximum document size, and write-batch limits. Crossing a bound
returns an error; the driver does not silently truncate work.

The SQL adapters use finite zero-value query limits: a 64 MiB work-memory
admission for row, ordering/grouping, join-membership, durable index-planning,
and durable batch/merge state; a separate 64 MiB fan-out pair budget; 100,000
rows and 64 MiB for a materialized result; 16 MiB for exact aggregate state;
and 1 GiB of live durable spill runs. Durable index planning pre-admits its
catalog, mask buffers, certificates, postings, and exact-recheck workspace. If
that optional optimization cannot fit, execution takes the exact full-scan
path rather than returning a resource error. The database/sql driver runs one
executor worker per pooled connection, and pgwire runs one per session, so
connection-level concurrency does not multiply a GOMAXPROCS worker pool inside
every connection.

Each bound parameter may carry at most 4 MiB, and the aggregate string,
exact-number, and document payload of one execution may carry at most 16 MiB.
The connection clears borrowed argument references as soon as binding and
execution finish, so a pooled idle connection does not pin caller buffers.

## Cancellation

The driver exposes context-aware connection, preparation, transaction-start,
query, and mutation paths. For a cancellable context, `database/sql` installs
one operation-local cooperative cancellation flag and joins its watcher before
returning. Context cancellation and deadlines can stop admission, catalog-lock
acquisition, parsing, validation, scans, joins, grouping, sorting, filtered
DML, spill I/O, and write preparation before the durable publication point.
The background path creates no watcher and retains the nil-flag execution path.
Once publication starts, the operation runs to a storage outcome instead of
returning a cancellation while a write continues invisibly. A namespace fence
failure after a visible catalog or table-file replacement is reported
explicitly as `durable.ErrCommitOutcomeUnknown`; it is not mislabeled as either
cancellation or rollback.

The same sentinel covers a recovery-journal ambiguity after a complete redo
record append when the following sync fails. In the synchronous journal lane,
the failed mutation is not published to the live reader, but close/reopen may
replay the complete record; an atomic batch therefore remains all-or-none while
its committed-versus-not-committed outcome is unknown. Definite pre-append and
append failures are not relabeled as ambiguous. A sync failure poisons the live
writer, preserves its device cause through `errors.Is`, and requires close,
reopen, and data reconciliation rather than retry on that handle. Concurrent
group-commit tickets resolve from their own captured fence: a completed sync is
not revoked by a later append poison, and a failed fence retains its own sync
cause independently of the first sticky diagnostic.

Pgwire maps `durable.ErrCommitOutcomeUnknown` to PostgreSQL SQLSTATE `40003`
(`statement_completion_unknown`), which takes precedence over a joined
cancellation error. It remains an `ERROR`, not a fatal connection error. The
extended protocol discards messages through the next `Sync`; simple protocol
still closes with `ReadyForQuery`. The protocol session is reusable afterward,
although the affected durable handle may remain poisoned. SQLSTATE `57014`
continues to mean an unambiguous canceled operation before publication.

The query executor has a reusable cooperative `query.CancelFlag`. It checks the
flag at bounded points in heap and durable scans, parallel workers, joins,
filtered DML, and spill I/O; cancellation drains worker pipelines, closes
leases, removes spill files, and exposes no partial materialized result.
`pgwire` connects PostgreSQL's out-of-band CancelRequest directly to that flag
and reports `57014`.

The typed runtime exposes the same signal through `Session.SetCancelFlag`.
The `database/sql` adapter advertises `StmtQueryContext` and
`StmtExecContext`; direct connection `ExecContext` uses that same prepared
execution path. It deliberately does not advertise `QueryerContext`, because a
connection-level query would also need to transfer ownership of its transient
prepared statement to the returned rows. `database/sql` performs that
lifecycle correctly through its prepared-statement fallback.

## Deliberately unsupported

The current subset rejects syntax that has no faithful shared plan or durable
operation:

- correlated predicate subqueries outside the documented grouped-decorrelation
  proof boundary, including nested boolean placement, generalized `NOT`,
  non-equality or unconsumed correlation, scalar subqueries in projection,
  `JOIN ON`, and `HAVING`;
- the correlated LATERAL shapes listed above, `NATURAL JOIN`, and derived-table
  column alias lists;
- data-modifying CTE bodies, mutual/forward or multiply self-referential
  recursion, recursive aggregation/grouping, `SEARCH`/`CYCLE`, and recursive
  `INTERSECT`/`EXCEPT`;
- window `FILTER`, DISTINCT window aggregates, null-treatment modifiers,
  arbitrary scalar window arguments/keys, and windows outside the SELECT list;
- general scalar placement beyond the documented SELECT-list and WHERE CASE
  forms, `COUNT(DISTINCT ...)`, the `::` cast shorthand, and user-defined
  functions;
- partial path UPDATE, `UPDATE ... FROM`, `DELETE ... USING`, and mutation joins;
- generated keys, target-column construction for INSERT query sources,
  defaults, `ON CONFLICT DO UPDATE`, `ON DUPLICATE KEY`, and nested flat-INSERT
  construction;
- `ALTER TABLE`/`ALTER INDEX`, materialized views and refresh, `CREATE OR
  REPLACE VIEW`, `DROP VIEW CASCADE`, and unique/check/foreign-key/default/
  generated constraints, plus SQL types without a JSON equivalent;
- unique/partial/range/full-text indexes, expression indexes, and selectable
  index methods;
- composite primary keys in the typed SQL runtime;
- transactional DDL, chained transactions, and isolation modes beyond the
  admitted BEGIN spellings;
- multi-table commits on durability lanes outside the journal-backed set
  (typed `ErrTransactionUnsupportedLane`).

Multi-table transactions and savepoints are supported; the storage contract is
in [multi-table-transactions.md](multi-table-transactions.md) and
[durability.md](../durability.md#database-transactions).

These are explicit errors rather than parser successes followed by
approximations.

`SELECT DISTINCT` is supported for non-aggregate projections by lowering the
projected tuple to the engine's spill-aware grouping key. It preserves
`ORDER BY`, `OFFSET`, and `LIMIT`; `COUNT(DISTINCT ...)` and DISTINCT queries
whose explicit grouping changes the projected tuple remain rejected rather
than being approximated.

`INSERT`, `UPDATE`, and `DELETE ... RETURNING path, ...` and `RETURNING *` are
supported. They reuse the SELECT projection engine over the final staged
documents, preserve mutation order, and complete projection admission before
the atomic write is published. DELETE returns pre-delete documents; UPDATE
returns replacement documents.
