# SQL with `database/sql`

The SQL driver registers the name `vibedb`. Its data source name is the path to
one durable SQL catalog.

## Open a catalog

```go
package main

import (
	"context"
	"database/sql"
	"log"

	_ "github.com/thesyncim/vibedb/sql/driver"
)

func main() {
	db, err := sql.Open("vibedb", "app.vdb")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.ExecContext(ctx, `
		CREATE TABLE docs (
			id STRING PRIMARY KEY,
			category STRING NOT NULL,
			score INTEGER NOT NULL
		)
	`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.ExecContext(ctx,
		`INSERT INTO docs (id, category, score) VALUES (?, ?, ?)`,
		"a", "news", 10,
	)
	if err != nil {
		log.Fatal(err)
	}
}
```

The connector shares one durable catalog handle and one writer lease across
the connection pool.

## Query rows

```go
rows, err := db.QueryContext(ctx, `
	SELECT id, score
	FROM docs
	WHERE category = ?
	ORDER BY score DESC
	LIMIT ?
`, "news", 10)
if err != nil {
	log.Fatal(err)
}
defer rows.Close()

for rows.Next() {
	var id string
	var score int64
	if err := rows.Scan(&id, &score); err != nil {
		log.Fatal(err)
	}
}
if err := rows.Err(); err != nil {
	log.Fatal(err)
}
```

Use `Query` for `SELECT` and for a mutation with `RETURNING`. Use `Exec` for a
mutation that does not return rows. `LastInsertId` is not available.

One `database/sql` call accepts one SQL statement. It rejects a
semicolon-separated statement batch.

## Access paths and EXPLAIN

For a local `SELECT`, equality or a positive `IN` predicate on the declared
primary-key path can supply point candidates when it is the complete `WHERE`
predicate or is below only top-level `AND` nodes. If several usable primary-key
point predicates occur in one conjunction, the driver chooses equality before
`IN`; among `IN` predicates it chooses the shortest list. A primary-key
predicate below `OR` or `NOT` is not a sound bound and does not enable point
access.

Primary-key comparisons with `>`, `>=`, `<`, `<=`, or non-negated `BETWEEN`
can similarly bound an ordered range at the root or below top-level `AND`
nodes. Multiple range terms are intersected. Point access takes precedence
when a query has both a usable point and range bound.

Candidate selection never replaces predicate evaluation. The query engine
checks the complete `WHERE` predicate, including every residual conjunct, over
the point or range source. Prepared statements also revalidate the cached path
against the live table, so dropping and recreating a table with a different
primary key falls back to an eligible non-point path instead of probing the old
key. Runtime size or source constraints can also select a scan fallback.

`EXPLAIN` reports the source-aware candidate path without scanning rows.
`EXPLAIN ANALYZE` executes the statement and reports the measured path. Plan
names such as `primary-key-point-or-scan` and
`primary-key-range-or-scan` deliberately expose the possible fallback. See the
[typed query API](query.md#explain-a-plan) for the full access-path vocabulary.

## Parameters

Use `?` placeholders. The driver rejects named parameters.

One parameter has a 4 MiB limit. The total bound payload has a 16 MiB limit.
The driver rejects invalid UTF-8 and nonfinite numbers. It keeps exact numeric
values exact during normalization.

A named-column `INSERT ... VALUES` accepts JSON scalar driver values. A
`vibejson.RawValue` is accepted only when it contains one valid JSON number and
is emitted with that exact numeric spelling; it is never encoded as its Go
struct representation. `encoding/json.Number` remains accepted as an input
compatibility type, but production document encoding is performed by
`vibejson`.

A physical connection can have only one open `Rows`. Close or exhaust the rows
before you run another statement on that same connection.

## Create and alter tables and indexes

A table stores JSON documents and has exactly one primary JSON path. Compound
table primary keys are not supported by the driver.

Declared types map to JSON domains:

- `NULL`
- `BOOL`
- `NUMBER`
- `INTEGER`
- `STRING`
- `ARRAY`
- `OBJECT`
- `ANY`

Common SQL aliases map to these domains. `JSON` maps to `ANY`. Columns are
nullable unless they use `NOT NULL`.

Add one declared column with the bounded additive form:

```sql
ALTER TABLE docs ADD COLUMN IF NOT EXISTS note STRING
```

The driver validates every existing document against the resulting schema and
publishes the new table incarnation atomically. A nullable column therefore
works when older documents omit it. A `NOT NULL` column succeeds only when every
existing document already contains a non-null value of the declared type.
The current embedded implementation holds the catalog write lock while it
copies a materialized table, so other reads and writes wait for a large ALTER.
`ALTER TABLE` is not allowed inside an explicit transaction. Rename, drop,
type-change, default, and constraint-changing ALTER actions are not supported.

Primary-key values can be strings, booleans, or numbers. Numeric spellings
such as `1`, `1.0`, and `1e0` have one exact identity. Arrays and objects cannot
be primary keys.

`CREATE INDEX` creates an exact nonunique index. It supports one or more JSON
paths and online creation over an existing table. `CREATE UNIQUE INDEX` is not
supported on the local SQL surface.

## Insert, update, and delete

Insert a complete JSON document:

```sql
INSERT INTO docs VALUES (?)
```

You can also construct a flat top-level document from named columns. A
multi-row `INSERT ... VALUES` is atomic.

`INSERT ... SELECT` requires exactly one output column that contains complete
JSON documents. The source query reads the pre-statement snapshot.

Conflict handling supports `ON CONFLICT DO NOTHING`. It does not support a
conflict target or `DO UPDATE`.

`UPDATE` can replace the complete document:

```sql
UPDATE docs
SET "$doc" = ?
WHERE id = ?
```

For tables with declared columns, `UPDATE` can instead assign scalar literals
or placeholders to one or more top-level columns:

```sql
UPDATE employees
SET team = ?, score = 7, note = NULL
WHERE id = ?
```

Each matching document is updated independently, and unassigned fields are
preserved. Nested-path targets and row-dependent assignment expressions such as
`score = score + 1` are not supported. An update cannot change the primary key.
A single whole-document replacement cannot replace several rows that have
different primary keys.

`UPDATE` and `DELETE` support `WHERE`, `ORDER BY`, `LIMIT`, and `RETURNING`.
`UPDATE ... FROM` and `DELETE ... USING` are not supported.

## Run a transaction

```go
tx, err := db.BeginTx(ctx, &sql.TxOptions{
	Isolation: sql.LevelSerializable,
})
if err != nil {
	log.Fatal(err)
}
defer tx.Rollback()

if _, err := tx.ExecContext(ctx, `SAVEPOINT before_update`); err != nil {
	log.Fatal(err)
}

_, err = tx.ExecContext(ctx, `
	UPDATE docs
	SET "$doc" = ?
	WHERE id = ?
`, `{"id":"a","category":"news","score":11}`, "a")
if err != nil {
	_, _ = tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT before_update`)
	log.Fatal(err)
}

if err := tx.Commit(); err != nil {
	log.Fatal(err)
}
```

The driver accepts these isolation levels:

| Requested level | Read-cut behavior |
| --- | --- |
| Default or read committed | Refresh the coherent cut for each data statement. |
| Repeatable read or snapshot | Retain the `BEGIN` cut. |
| Serializable | Retain the `BEGIN` cut and validate read dependencies. |

Writes use optimistic first-committer-wins conflict detection. A multi-table
commit does not publish a partial result.

Transactions support read-your-writes and at most 64 savepoints. DDL is not
allowed in a transaction. A read-only transaction rejects DML and DDL.

## Resource limits

The `database/sql` adapter uses one query worker for each physical connection.
Pool concurrency supplies parallelism.

Default query limits include:

| Resource | Default |
| --- | ---: |
| Working memory | 64 MiB |
| Result rows | 100,000 |
| Result bytes | 64 MiB |
| Intermediate bytes | 64 MiB |
| Aggregate bytes | 16 MiB |
| Spill bytes | 1 GiB |
| Join-pair bytes | 64 MiB |
| Recursive-term evaluations | 1,000 |
| Recursive result rows | 100,000 |
| Recursive fixpoint storage | 64 MiB |

Budget errors return no partial materialized result. Use the typed runtime when
you need per-session cancellation, result, intermediate, or working-memory
limits. Aggregate, spill, and join-pair limits are not session setters.

## Important SQL boundaries

VibeDB is not PostgreSQL. It does not support schemas, roles, grants, COPY,
procedures, notifications, partial-document updates, general scalar function
calls, general `pg_catalog` SQL, or arbitrary PostgreSQL types.

See the [SQL surface reference](../design/sql-surface.md) for supported query
forms and exact refusals.

## Implementation references

- `sql/driver/driver.go`, `write.go`, `tx.go`, and `runtime.go`
- `sql/driver/validate.go`, `primary.go`, and `primary_range.go`
- `sql/parser.go`, `parse_ddl.go`, and `parse_dml.go`
- `sql/driver/surface_test.go`, `primary_range_test.go`, and
  `isolation_test.go`
