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

## Parameters

Use `?` placeholders. The driver rejects named parameters.

One parameter has a 4 MiB limit. The total bound payload has a 16 MiB limit.
The driver rejects invalid UTF-8 and nonfinite numbers. It keeps exact numeric
values exact during normalization.

A physical connection can have only one open `Rows`. Close or exhaust the rows
before you run another statement on that same connection.

## Create tables and indexes

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

`UPDATE` replaces the complete document:

```sql
UPDATE docs
SET "$doc" = ?
WHERE id = ?
```

Partial path assignment is not supported. An update cannot change the primary
key. A single constant replacement cannot replace several rows that have
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
- `sql/driver/validate.go` and `primary.go`
- `sql/parser.go`, `parse_ddl.go`, and `parse_dml.go`
- `sql/driver/surface_test.go` and `isolation_test.go`
