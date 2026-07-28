# SQL surface

## Scope and layering

The SQL surface is intentionally small. `sql` tokenizes and parses with
recursive descent, and every syntax error carries a byte offset, line, and
column. `sql/driver` is the `database/sql` adapter registered as `vibedb`. It
uses exported `store/durable` storage calls and exported `query` preparation and
execution calls; SQL has no storage implementation or predicate evaluator of
its own.

A DSN is a filesystem path to a small SQL catalog. Each table maps to one
durable collection file beside it. The catalog owns SQL-only facts the base
store deliberately does not own: table names and the JSON path declared as the
primary key.

`CREATE TABLE t (PRIMARY KEY (id))` changes only the SQL catalog. It does not
create an empty primary collection. The collection file is materialized on the
first `INSERT`. An unindexed table is created then with the mutable durable
layout and receives the first `Put`. An indexed table is instead bulk-seeded
and written with `durable.CreateFromPrimary`, because that API requires at
least one document. This is a visible lifecycle rule, not a fabricated empty
primary graph.

## Dialect

Keywords are case-insensitive. Identifiers and JSON field names are
case-sensitive. `?` placeholders are positional.

### Definitions

```sql
CREATE TABLE t (PRIMARY KEY (id))
CREATE INDEX ON t(kind)
```

A table is a collection and its primary key is one scalar JSON path. An exact
index must be declared after `CREATE TABLE` and before the first `INSERT`,
because durable index definitions are frozen at collection creation. The
parser also accepts named indexes; an unnamed index receives a deterministic
name derived from its path.

### Inserts

```sql
INSERT INTO t VALUES (?)
INSERT INTO t (id, kind, active) VALUES (?, ?, ?)
```

The first form binds a complete JSON document as `string` or `[]byte`. The
second builds a flat JSON object from scalar driver values. The driver extracts
the declared primary-key path and encodes its JSON scalar without type
collisions as the collection key. The engine's `Put` operation is an upsert:
inserting an already-present primary key replaces its document and reports one
affected row.

The parser retains the older explicit `("$key", "$doc")` form for source
compatibility, but the document-derived forms are the SQL surface described
here.

### Reads

```sql
SELECT * FROM t WHERE id = ?
SELECT id, kind FROM t WHERE id IN (?, ?)
SELECT * FROM t WHERE kind = ?
SELECT * FROM t ORDER BY id LIMIT ?
SELECT COUNT(*) FROM t WHERE kind = ?
```

Supported row selection is primary-key equality or membership, exact equality
on a declared indexed path, or no predicate (a full scan). Projection may name
JSON paths or the whole document. `COUNT(*)` accepts the same predicates.
`ORDER BY` is restricted to the declared primary key; `LIMIT` is a
non-negative integer or placeholder.

Primary-key equality and membership are resolved to canonical storage keys and
use `durable.Snapshot.AppendRaw` point reads before the selected documents are
fed through the prepared query. Full scans execute from a durable snapshot.
Indexed equality is compiled by `query.PrepareStatement`; query's candidate
planner calls the snapshot's exact-index mask API and the durable executor
reads the selected posting masks. Ordering, projection, limiting, and counting
remain query-engine operations.

### Updates and deletes

```sql
UPDATE t SET "$doc" = ? WHERE id = ?
DELETE FROM t WHERE id = ?
DELETE FROM t WHERE id IN (?, ?)
DELETE FROM t WHERE kind = ?
```

UPDATE replaces a whole JSON document; there is no partial JSON path editor.
The replacement must retain the same derived primary key. DELETE removes the
selected storage keys. Primary-key equality and membership use point lookup;
other accepted predicates use `query.DMLStatement.Filter`, so mutation
selection is evaluated by the same compiled predicate as SELECT. These
mutations are available only on unindexed tables today.

Each autocommit operation uses the exported single-document `Put` or `Delete`
surface. Multi-document operations are applied document by document until the
transactional ordered-primary batch capability is available, so callers that
need statement-wide atomicity must currently restrict mutations to one
document.

### Transactions

`BEGIN` takes a generation-leased durable snapshot of every table present in
the SQL catalog at that instant. Every later SELECT and mutation in the
transaction is evaluated against that fixed generation overlaid with the
transaction's own staged writes. The implementation deliberately materializes
that merged view in one pass per statement. The resulting visibility is exact:
repeatable point reads, phantom exclusion, and read-your-writes for INSERT,
whole-document UPDATE, and DELETE. Nothing committed after BEGIN is visible.

A transaction may read several tables but may write exactly one. Durable
collections have independent roots, generations, and writer locks, so the
engine has no atomic publication spanning two tables. DDL is refused inside a
transaction for the same catalog-lifecycle reason.

`COMMIT` maps to one `Collection.Update` on the written table. Before filling
its `WriteBatch`, the callback compares every written key's live document with
the exact pre-image captured from the BEGIN snapshot. A changed key returns
`driver.ErrTransactionConflict` and publishes nothing (snapshot isolation with
first-committer-wins, not serializability). Otherwise every staged PUT and
DELETE is recorded in that one batch, producing one failure-atomic generation.
`ROLLBACK` closes the retained snapshots and discards the overlay.

Only the default and `database/sql` snapshot isolation levels are accepted.
Read-only transactions reject mutations. The engine's batch limits and gates
remain visible through driver sentinels:

- document-count or aggregate-byte overflow returns
  `driver.ErrTransactionTooLarge` wrapping `durable.ErrBatchTooLarge`;
- an indexed table returns `driver.ErrTransactionIndexedTable` wrapping
  `durable.ErrPrimaryBatchIndexedUnsupported`, because the batch does not yet
  run the posting maintainer;
- an ordered-primary async-visible lane returns
  `driver.ErrTransactionUnsupportedLane` wrapping
  `durable.ErrPrimaryBatchUnsupportedLane`.

All three abort without partial publication.

## NULL, missing fields, and types

Missing and explicit JSON null are distinct for existence tests in the query
engine, but a projected missing field and a projected JSON null both become SQL
`NULL` and scan as `nil`. `IS NULL` therefore matches either at the SQL value
boundary; `IS MISSING` is the explicit existence test accepted by the shared
parser but is outside this driver slice. A primary-key path may be neither
missing nor null.

The lossless `driver.Value` mapping is:

| JSON value | `driver.Value` |
|---|---|
| null or missing projection | `nil` |
| boolean | `bool` |
| integral number fitting `int64` | `int64` |
| any other number | `[]byte` containing exact decimal spelling |
| string | decoded `[]byte` |
| object or array | JSON `[]byte` |

Input document parameters are `string` or `[]byte`. Flat INSERT parameters are
`nil`, `bool`, `int64`, `float64`, `string`, or `[]byte` (the last maps to a
JSON string). Numbers are not unconditionally converted through `float64`,
because doing so would collapse exact JSON integers.

## Deliberately out

The following are rejected by the driver surface even where the shared parser
can represent a broader query:

- Joins wait for a durable multi-collection catalog snapshot that also carries
  this SQL catalog's per-table options.
- Subqueries wait for a query plan node and correlated-source execution.
- Non-primary-key `ORDER BY` waits for an ordered secondary access path; sorting
  an unbounded document scan is not exposed by this slice.
- Range predicates wait for range postings (or a primary-key range cursor
  exported at this layer); exact posting masks answer equality only.
- Partial UPDATE waits for an exported JSON path-set operation with defined
  duplicate-key, array, and missing-intermediate semantics.
- `ALTER`, `DROP`, constraints, defaults, and generated keys wait for durable
  catalog mutation and enforcement primitives.
- Mutating an indexed table waits for the sibling mutable exact-posting
  maintenance landing; current posting tiles are build-and-read and fail closed
  with `durable.ErrPrimaryExactIndexReadOnly`.

## Exported API gaps

The implementation encountered these engine boundaries:

1. `durable.CreateFromPrimary` cannot build an empty primary collection. SQL
   records CREATE TABLE lazily and creates the file on first INSERT.
2. Ordered-primary exact postings have no mutable maintenance surface.
   Indexed INSERT/UPDATE/DELETE are feature-gated.
3. The store does not persist a SQL primary-key declaration or derive a storage
   key from a JSON path. The SQL catalog records the path and owns the
   conversion.
4. `query` imports the root `sql` AST package, so a driver in that same package
   cannot import `query` without a cycle. The registered implementation lives
   in `sql/driver`.
