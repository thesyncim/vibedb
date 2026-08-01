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

INSERT means insert, not upsert. It returns
`driver.ErrDuplicatePrimaryKey` if the derived identity already exists or
appears twice in one VALUES statement. The statement publishes nothing on that
error. Use UPDATE for replacement. `LastInsertId` is unavailable because keys
come from documents rather than a generated sequence.

There is no caller-supplied physical-key row form. `VALUES` without a field list
contains exactly one complete document; a field-list INSERT includes the
declared primary-key field like any other field. The declaration is the one
source of row identity.

## SELECT

The driver exposes the shared SELECT surface, including:

- whole-document and JSON-path projection;
- `=`, `!=`, `<>`, `<`, `<=`, `>`, and `>=`;
- `IN`, `NOT IN`, `BETWEEN`, and `NOT BETWEEN`;
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

Primary-key equality and membership use point reads. Eligible exact predicates
use posting masks and document rechecks. Predicates without a usable access path
remain correct full scans; an index is an optimization, not a requirement for a
WHERE clause. Sorting, grouping, aggregation, HAVING, OFFSET, and final
projection remain query-engine operations.

## Inner and left joins

The SQL join is a declared-field equi-join. Both `INNER JOIN` and
`LEFT [OUTER] JOIN` are supported:

```sql
SELECT u.id, o.total
FROM users AS u
INNER JOIN orders AS o ON u.id = o.user_id
WHERE o.state = ?
ORDER BY o.total DESC
```

A left join preserves each row from its driving `FROM` table. When no joined
document matches, every projected field from the joined alias is SQL `NULL`.
Fan-out remains exact: a driving row with three matches produces three rows,
while one with no match produces exactly one null-extended row.

The driver captures every participating durable collection in one coherent
snapshot. All publication gates are held while the generation leases are
acquired, so the joined generations genuinely coexisted. This prevents a join
from observing a new generation of one table with an already-obsolete
generation of another. It does not make independent writes to different tables
atomic.

Declared-field joins can produce several rows for one driving document. The
durable executor does not yet expand that pair space directly, so the driver
uses a bounded heap fallback:

1. Capture the coherent durable cut.
2. Measure all referenced keys and documents against the query working-set
   budget.
3. If admitted, materialize that exact cut into the heap executor, close the
   durable leases, and run the shared fan-out plan from the owning heap copy.
4. If it is too large, return `driver.ErrJoinMaterializationTooLarge` before
   query execution or partial results.

The current driver budget is a fixed 64 MiB, with a conservative charge of
`16 * (key bytes + document bytes) + 512 bytes` per source row to cover heap
indexes, structural tapes, metadata, and build scratch. The budget is an
admission bound on the estimated materialized working set, not merely a limit
on output rows.

The current relational plan has two explicit join limits:

- one statement may expand one joined relation, so the driver currently
  accepts one declared-field JOIN;
- that JOIN must relate the joined table directly to the driving `FROM` table;
  chained joins are rejected.

Only inner and left `JOIN ... ON left_path = right_path` are supported. SQL exposes no
physical storage-key pseudo-column; `"$key"` is an ordinary quoted JSON field.
Join the tables' declared JSON primary-key fields when relational identity is
the intended relationship.

The current executor has no post-join predicate phase. A `WHERE` condition over
the nullable side of a left join is therefore rejected rather than pushed into
the build side, which would silently change its meaning. Conditions over the
preserved table remain supported.

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
error. Every nested result remains subject to the query result row and byte
budgets.

Correlated subqueries are rejected rather than re-evaluated once per row, and
subqueries in `FROM`, the projection list, and `HAVING` remain unsupported.

Joins inside a transaction use the same bounded heap path over the snapshots
captured by BEGIN plus that transaction's staged overlay. They preserve
repeatable reads and read-your-writes.

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

BEGIN captures a generation-leased snapshot of every table present in the
catalog while the driver excludes its writers. Every later SELECT and mutation
reads that fixed cut overlaid with the transaction's staged writes. The result
is snapshot isolation with repeatable reads, phantom exclusion, and
read-your-writes.

A transaction may read several tables but may write exactly one. Durable
tables have independent publication roots, so there is no atomic commit across
two tables. DDL is rejected inside a transaction.

COMMIT uses a bounded per-table, per-key publication clock. If any written key
was published after BEGIN, including a change-and-restore (ABA),
`driver.ErrTransactionConflict` is returned and nothing is published. This
first-committer-wins check retains revisions rather than copies of the original
documents, and disjoint-key writers remain independent. The clock retains at
most 4,096 changed keys; overflow moves a history floor and conservatively
rejects older transactions instead of risking a missed conflict. Otherwise all
staged puts and deletes are submitted in one `Collection.Update`, including
exact-index maintenance. ROLLBACK closes the leases and discards the overlay.

The typed runtime accepts only its default and snapshot isolation levels;
`database/sql` maps those directly, and the PostgreSQL spellings admitted by
`pgwire` are listed below. Read-only transactions reject mutations.

Transactions and atomic multi-row statements are bounded by the collection's
`MaxBatchDocuments` and `MaxBatchBytes`. The SQL-created layout currently uses
the durable defaults (64 distinct keys and the default bounded byte
reservation). Overflow returns `driver.ErrTransactionTooLarge` wrapping
`durable.ErrBatchTooLarge` and publishes nothing.

PostgreSQL sessions expose the same state as ReadyForQuery `I`, `T`, or `E`.
`BEGIN`, `BEGIN READ WRITE`, `BEGIN READ ONLY`, and
`BEGIN ISOLATION LEVEL REPEATABLE READ` map to the typed runtime; other
isolation claims are rejected rather than silently weakened. An error inside
an explicit transaction leaves only ROLLBACK usable. COMMIT in that failed
state performs the rollback and emits PostgreSQL's `ROLLBACK` command tag.

An extended-protocol batch of non-DDL stored-row statements runs in one
implicit transaction through Sync. A multi-statement simple Query without
explicit transaction control does the same, so an error rolls back earlier
writes in that message. Catalog DDL cannot yet participate in the durable
transaction overlay: CREATE TABLE and CREATE INDEX are atomic individually,
must run outside an explicit transaction, and must be the only non-empty
statement in a simple Query message and the only catalog execution between
extended-protocol Sync points. This is an explicit `0A000` boundary rather
than partial transactional behavior. Extended-protocol DDL publishes when its
Execute completes; if a client then violates the boundary with another catalog
execution before Sync, that later execution is refused but the already
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

- correlated subqueries and subqueries outside predicates, common table
  expressions, set operations, window functions, `DISTINCT`, computed scalar
  expressions, pattern matching, and user-defined functions;
- right/full outer, cross, natural, `USING`, chained, and multiple fan-out joins;
- partial path UPDATE, `UPDATE ... FROM`, `DELETE ... USING`, mutation joins,
  mutation `ORDER BY`/`LIMIT`, and `UPDATE`/`DELETE RETURNING`;
- generated keys, `INSERT ... SELECT`, defaults, upsert/on-conflict forms, and
  nested flat-INSERT construction;
- `ALTER`, `DROP`, `TRUNCATE`, views, unique/check/foreign-key/default/generated
  constraints, and SQL types without a JSON equivalent;
- unique/partial/range/full-text indexes, expression indexes, and selectable
  index methods;
- `EXPLAIN`; physical index and join choices are late-bound against the live
  snapshot, arguments, and execution budgets, and there is not yet a stable
  plan-report API shared by both SQL adapters;
- composite primary keys in the typed SQL runtime and atomic transactions
  spanning more than one table.

These are explicit errors rather than parser successes followed by
approximations.

`INSERT ... RETURNING path, ...` and `RETURNING *` are supported. They reuse
the SELECT projection engine over the final staged documents, preserve
multi-row VALUES order, and complete projection admission before the atomic
write is published.
