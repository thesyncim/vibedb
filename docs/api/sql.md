# SQL API

[Documentation](../README.md) / [API guides](README.md) · [Development status](../status.md)

VibeDB exposes one bounded SQL implementation through two Go APIs:

| API | Use it for |
| --- | --- |
| `database/sql` with driver name `vibedb` | Conventional Go applications and connection pooling |
| `sql/driver` typed runtime | Protocol adapters, explicit ownership, typed cells, and allocation reuse |

The dialect is VibeDB SQL over JSON documents, not PostgreSQL SQL. Each table
is a durable document collection with an optional declared schema. See the
[SQL reference](../reference/sql.md) for syntax and the
[PostgreSQL wire adapter](pgwire.md) for network access.

## Open a catalog with `database/sql`

The DSN is the catalog path. The driver owns the catalog file, a sibling
`<path>.lock` writer lock, and a `<path>.tables` directory that holds one
primary file and recovery journal per table. Back up all three together while
the catalog is closed.

This complete program uses a disposable catalog:

```go
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	vibedriver "github.com/thesyncim/vibedb/sql/driver"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "vibedb-sql-*")
	must(err)
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "app.vdb")

	connector, err := (vibedriver.Driver{}).OpenConnector(path)
	must(err)
	db := sql.OpenDB(connector)
	defer func() { must(db.Close()) }()

	_, err = db.ExecContext(ctx, `CREATE TABLE counters (
		id STRING PRIMARY KEY,
		value INTEGER NOT NULL
	)`)
	must(err)

	_, err = db.ExecContext(ctx,
		`INSERT INTO counters (id, value) VALUES (?, ?)`, "requests", 1)
	must(err)

	_, err = db.ExecContext(ctx,
		`UPDATE counters SET value = value + 1 WHERE id = ?`, "requests")
	must(err)

	var value int64
	must(db.QueryRowContext(ctx,
		`SELECT value FROM counters WHERE id = ?`, "requests").Scan(&value))
	fmt.Println(value)
}
```

Expected output:

```text
2
```

`sql.Open("vibedb", path)` works too, after a blank import of
`github.com/thesyncim/vibedb/sql/driver`.

- The native placeholder is `?`. Named `database/sql` arguments are rejected.
  Do not use PostgreSQL `$1` placeholders through this driver; pgwire rewrites
  them before parsing.
- Each call accepts exactly one statement.
- Use `Query` for `SELECT`, `EXPLAIN`, and mutations with `RETURNING`. Use
  `Exec` for DDL and mutations without `RETURNING`; using `Query` for DDL
  returns an error. `LastInsertId` is not supported.

## Store whole documents

`"$doc"` names the whole JSON document. Run these statements separately on a
fresh catalog:

```sql
CREATE TABLE users (id STRING PRIMARY KEY, name STRING, visits INTEGER);

INSERT INTO users VALUES ('{"id":"u1","name":"Ada","visits":1}');

INSERT INTO users (id, name, visits) VALUES ('u1', 'Ada Lovelace', 1)
ON CONFLICT DO UPDATE SET
    visits = users.visits + EXCLUDED.visits,
    name = EXCLUDED.name;

UPDATE users SET "$doc" = '{"id":"u1","name":"Augusta","visits":3}'
WHERE id = 'u1';
```

A one-value `INSERT ... VALUES (?)` stores a whole document; the primary-key
path must be present in it. `UPDATE` expressions read the current row. Upsert
assignments read the current row through the table name or alias (bare
`visits` is ambiguous) and the incoming row through `EXCLUDED`. `ON CONFLICT`
always means the primary key; see the
[reference](../reference/sql.md#insert-and-upsert) for the supported targets.

## Null and missing are different

An absent JSON path and an explicit JSON `null` stay distinct:

```sql
-- Matches {"note": null} and documents without note.
SELECT id FROM users WHERE note IS NULL;

-- Matches only documents without note.
SELECT id FROM users WHERE note IS MISSING;

-- Matches only documents where note is present and not null.
SELECT id FROM users WHERE note IS NOT NULL;
```

Projection and pgwire render both missing and null as SQL NULL. Authored
comparisons such as `note = NULL` are rejected.

## Transactions and savepoints

| Isolation | Read cut | Commit validation |
| --- | --- | --- |
| Read Committed (`database/sql` default) | One coherent cut per statement | Write conflicts |
| Repeatable Read or Snapshot | Cut captured at BEGIN | Write conflicts |
| Serializable | Cut captured at BEGIN | Write conflicts and exact or coarse read dependencies |

Other isolation levels are rejected. Repeatable Read is snapshot isolation: it
validates only write conflicts, so write skew between two transactions that
read overlapping rows and write disjoint rows is possible. Use Serializable
when an invariant spans rows. Transactions provide read-your-writes,
statement atomicity, first-committer-wins conflicts (SQLSTATE `40001` through
pgwire), and atomic multi-table commits. A multi-table commit uses the catalog
decision log and is crash-atomic. DDL inside a transaction is refused.

A transaction may hold up to 64 savepoints. A duplicate name shadows the
older savepoint. `RELEASE` removes the named savepoint and newer ones;
`ROLLBACK TO` keeps the named savepoint and removes newer ones.

This function can be added to the program above:

```go
func savepointExample(ctx context.Context, db *sql.DB) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	must(err)
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `SAVEPOINT before_audit`)
	must(err)
	_, err = tx.ExecContext(ctx,
		`INSERT INTO counters (id, value) VALUES (?, ?)`, "audit", 1)
	must(err)
	_, err = tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT before_audit`)
	must(err)
	must(tx.Commit())
}
```

Close or exhaust `Rows` before the next operation on the same connection,
especially inside a `*sql.Tx`. In the typed runtime, a prepare or execution
error fails an active transaction; recover with `ROLLBACK TO` or `ROLLBACK`.
`COMMIT` on a failed transaction rolls it back.

After an unknown commit result, close the pool (or the typed session and then
the database), reopen the catalog, and reconcile. Never blindly retry the
write.

## Typed runtime

The typed runtime gives explicit sessions and typed cells. Do not open the
same catalog through it while a `database/sql` pool holds the writer lock.
This function uses the imports and `must` helper above:

```go
func typedRead(ctx context.Context, path string) {
	database, err := vibedriver.Open(path)
	must(err)
	defer database.Close()
	session, err := database.NewSession(ctx)
	must(err)
	defer session.Close()
	prepared, err := session.Prepare(ctx,
		`SELECT id, value FROM counters WHERE id = ?`)
	must(err)
	defer prepared.Close()
	cursor, err := prepared.Query(ctx, []any{"requests"})
	must(err)
	defer cursor.Close()
	if !cursor.Next() {
		panic("counter not found")
	}
	value, ok := cursor.Cell(1).Int64()
	if !ok {
		panic("counter is not an integer")
	}
	fmt.Println(value)
}
```

Ownership rules:

- `Database` owns the catalog writer lock. `Database.Close` refuses new
  sessions; existing sessions keep the catalog alive until they close.
- `Session` is single-consumer and allows one live cursor and one active
  transaction.
- `Prepared.Close` releases parsed and compiled arenas and closes its live
  cursor. It is idempotent.
- Cursor cells borrow runtime storage until `Cursor.Close`; copy data that
  must survive. `Cursor.Close` releases the snapshot lease.
- Close cursor, then prepared statement, then session, then database. Close
  methods cascade safely.
- `Session.Tables` returns an owned snapshot of tables (sorted by name),
  declared columns, and exact indexes.

Configure `Session.SetResultLimits`, `SetIntermediateLimit`, and
`SetMemoryLimit` while the session is idle. Zero selects defaults; `-1`
disables the result or intermediate cap. A positive memory limit must be at
least 64 KiB.

## Values and limits

`database/sql` accepts nil, bool, integers, finite `float64`, UTF-8 strings
and bytes, `query.Number`, and numeric `vibejson.RawValue`. The typed runtime
also accepts every integer width, `float32`, pointer forms (`*bool`, `*int64`,
`*float64`, `*string`, `*[]byte`, `*query.Number`), and `RawValue` documents.
A document parameter must contain exactly one JSON value.

Returned `database/sql` values are nil for SQL NULL, `bool` for booleans,
`int64` for integral numbers that fit, and `[]byte` for other numbers,
strings, and JSON. Numbers use exact JSON-decimal identity: `1`, `1.0`, and
`1e0` compare equal. Do not round through `float64`.

| Boundary | Current limit |
| --- | ---: |
| SQL text | 16 MiB |
| Parameters | 65,536 |
| One parameter | 4 MiB |
| All argument payloads per execution | 16 MiB |
| Encoded primary key | 256 bytes |
| Document | 4 MiB |
| Mutation transaction | Per table: 64 documents and ~16 MiB; total: 16 tables, 256 documents, ~64 MiB |
| Items in one clause or list | 1,024 |
| Predicate/scalar depth | 64 |
| Subquery depth | 32 |
| Set nesting | 64 |
| Default result | 100,000 rows / 64 MiB |
| Default relation intermediates | 64 MiB |

## Distributed RF3 differences

The same SQL text reaches the replicated RF3 runtime through the gateway, but
its write lane is narrower than embedded execution. The RF3 pgwire backend is
autocommit-only: send each write in its own Query or Execute/Sync cycle. It
accepts `INSERT ... VALUES`, `DELETE`, and exact-primary-key `UPDATE` without
`RETURNING`, `ORDER BY`/`LIMIT`, nested targets, or primary-key moves. A
declared top-level `UPDATE` assignment may be a computed scalar, for example:

```sql
UPDATE users SET visits = visits + 1, name = name || '!' WHERE id = ?;
```

The coordinator evaluates assignments once from a complete preimage and
retains the canonical postimage with a length and SHA-256 guard, so recovery
replays the retained result instead of reevaluating it. See
[guarded preparation](../history/guarded-point-update-plan.md#private-committed-preimages)
and the [SQL reference](../reference/sql.md#update-and-delete) for the
current RF3 boundary.

## Limitations

- One process owns a catalog; there is no shared-catalog multi-process mode.
- The durability profile is fixed (synchronous journal); there is no
  buffered SQL mode.
- DDL cannot run inside a transaction, and each DDL statement changes one
  object.
- Only single-column primary keys are executable; no DEFAULT, CHECK,
  FOREIGN KEY, sequences, or generated columns.
- `LastInsertId`, named arguments, and multi-statement calls are not
  supported.
- The catalog format has no migration reader between development revisions.

## Source map

- `database/sql` adapter and values: [sql/driver/driver.go](../../sql/driver/driver.go), [sql/driver/stmt.go](../../sql/driver/stmt.go)
- Typed runtime and session limits: [sql/driver/runtime.go](../../sql/driver/runtime.go)
- Catalog files and writer lock: [sql/driver/catalog.go](../../sql/driver/catalog.go)
- Transactions and savepoints: [sql/driver/tx.go](../../sql/driver/tx.go), [sql/driver/savepoint.go](../../sql/driver/savepoint.go)
- Embedded mutations: [sql/driver/write.go](../../sql/driver/write.go), [sql/driver/column_update.go](../../sql/driver/column_update.go)
- Package contract: [sql/driver/doc.go](../../sql/driver/doc.go)
- RF3 postimages and replay: [gateway/replicated_sql_transaction.go](../../gateway/replicated_sql_transaction.go), [gateway/durable_sql_request_executor.go](../../gateway/durable_sql_request_executor.go)
