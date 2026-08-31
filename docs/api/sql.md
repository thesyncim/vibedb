# SQL API

> [!CAUTION]
> Unreleased development software: SQL, catalog, protocol, types, limits, and
> transactions may break on any commit. No old-image migration reader exists.
> Pin the catalog commit and test recovery.

VibeDB exposes one bounded SQL implementation through two Go APIs:

| API | Use it for |
|---|---|
| `database/sql` driver name `vibedb` | Conventional Go applications and connection pooling |
| `sql/driver` typed runtime | Protocol adapters, explicit ownership, typed cells, and allocation reuse |

SQL is a native document-database interface, not PostgreSQL SQL compatibility.
See the [PostgreSQL wire adapter](pgwire.md) and [SQL reference](../reference/sql.md).

## Open a catalog with `database/sql`

The DSN is the catalog path. This complete program uses a disposable catalog
through `database/sql` only:

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
	defer db.Close()

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

The native placeholder is `?`. Named `database/sql` arguments are rejected.
The pgwire adapter rewrites PostgreSQL `$1` placeholders before parsing; do not
use `$1` through this driver. Each native call accepts exactly one statement.

Use `Query` for SELECT and mutations with RETURNING. Use `Exec` for DDL and
mutations without RETURNING. `LastInsertId` is not available.

## Store whole documents

`"$doc"` names the whole JSON document. Run these statements separately on a
fresh catalog:

```sql
CREATE TABLE users (
    id STRING PRIMARY KEY, name STRING, visits INTEGER
);
INSERT INTO users VALUES ('{"id":"u1","name":"Ada","visits":1}');

INSERT INTO users (id, name, visits) VALUES ('u1', 'Ada Lovelace', 1)
ON CONFLICT DO UPDATE SET
    visits = users.visits + EXCLUDED.visits,
    name = EXCLUDED.name;

UPDATE users SET "$doc" = '{"id":"u1","name":"Augusta","visits":3}'
WHERE id = 'u1';
```

UPDATE expressions read the current row. Upsert expressions read both the
current target row (`users.visits`; bare `visits` is ambiguous) and incoming
`EXCLUDED` top-level fields. This is implemented behavior, not roadmap syntax.

The embedded behavior above is broader than the authenticated durable RF3 lane.
RF3 accepts a declared top-level computed assignment only for an exact-primary-
key UPDATE without RETURNING, ORDER BY/LIMIT, a nested target, or a primary-key
move. `ON CONFLICT` remains fenced. For example:

```sql
UPDATE users SET visits = visits + 1, name = name || '!' WHERE id = ?;
```

The coordinator linearizably reads the old row and evaluates every right-hand
side once against that same image. It retains the canonical postimage plus the
old value's exact length and SHA-256 check in the durable program; global-index
changes derive from that postimage, and recovery replays it without reevaluation.
The RF3 pgwire backend is autocommit-only: send each write in its own Query or
Execute/Sync cycle, not an explicit or multi-statement transaction.

`ON CONFLICT` always means the table primary key. `DO NOTHING` and `DO UPDATE`
are supported; explicit conflict targets, named constraints, action WHERE, and
nested `EXCLUDED` paths are not.

## Null and missing are different

An absent JSON path and explicit JSON `null` remain distinct internally:

```sql
-- Matches {"note": null} and documents without note.
SELECT id FROM users WHERE note IS NULL;

-- Matches only documents without note.
SELECT id FROM users WHERE note IS MISSING;

-- Matches only documents where note exists and is not null/missing.
SELECT id FROM users WHERE note IS NOT NULL;
```

Projection and pgwire both encode missing and JSON null as SQL NULL; retain the
distinction in predicates with `IS MISSING`. Authored comparisons such as
`note = NULL` are rejected.

## Transactions and savepoints

| Isolation | Read cut | Commit validation |
|---|---|---|
| Read Committed (default) | One coherent cut per statement | Write conflicts |
| Repeatable Read / Snapshot | Cut captured at BEGIN | Write conflicts |
| Serializable | Cut captured at BEGIN | Write and exact/relation-coarse read dependencies |

Transactions provide read-your-writes, statement atomicity, first-committer-
wins conflicts, and atomic multi-table commits. DDL is refused inside a
transaction. A transaction may hold up to 64 savepoints. Duplicate names
shadow older savepoints; RELEASE removes the named savepoint and newer ones;
ROLLBACK TO retains the named savepoint and removes newer ones.

This function can be added to the program above; every statement sees the same
`counters` table and all resources have one owner:

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

Close or exhaust `Rows` before another operation on the same physical
connection, especially inside `*sql.Tx`. In the typed runtime, a prepare or
execution error fails an active transaction; recover with ROLLBACK TO or
ROLLBACK. COMMIT on a failed transaction rolls it back.

After an unknown commit result, close the `database/sql` pool—or typed session
then database—reopen the catalog, and reconcile; never blindly retry the write.

## Typed runtime

Use the typed runtime when the caller needs explicit sessions and typed cells.
Do not open the same catalog through it while a `database/sql` pool still owns
the writer lease. This function uses the imports and `must` helper above:

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

Ownership rules are strict:

- `Database` owns the catalog writer lease. `Database.Close` prevents new
  sessions; existing sessions keep the catalog alive until they close.
- `Session` is single-consumer and permits at most one live cursor and one
  active transaction.
- `Prepared.Close` releases parsed and compiled arenas and closes its live
  cursor. It is idempotent.
- `Session.Tables` returns an owned snapshot: tables are sorted by name,
  declared columns by canonical path, and exact indexes by creation order.
- Cursor cells borrow runtime storage until `Cursor.Close`; copy data that must
  survive. `Cursor.Close` releases the snapshot lease and is idempotent.
- Cursor → prepared → session → database is clearest; Close methods safely
  cascade, and sessions outlive `Database.Close`.

`Session.SetResultLimits`, `SetIntermediateLimit`, and `SetMemoryLimit` must be
configured while idle with no live cursor. Zero selects defaults; `-1` disables
result/intermediate caps. A positive memory limit must be at least 64 KiB.

## Values and limits

`database/sql` accepts nil, bool, converter-supported integers, finite float64,
UTF-8 string/bytes, `query.Number`, and numeric `vibejson.RawValue`. The typed
runtime accepts every integer width, finite float32/64, those string/number
forms, and `*bool`, `*int64`, `*float64`, `*string`, `*[]byte`, or
`*query.Number`. It also accepts `RawValue` as a valid document. Document
string/bytes must contain one valid JSON value.

| Boundary | Current limit |
|---|---:|
| SQL text | 16 MiB |
| Parameters | 65,536 |
| One parameter | 4 MiB |
| All argument payloads per execution | 16 MiB |
| Encoded primary key | 256 bytes |
| Document | 4 MiB |
| Mutation transaction | Per table: 64 docs / 16 MiB values + keys; total: 16 tables / 256 docs / ~64 MiB |
| Clause/list items | 1,024 each |
| Predicate/scalar depth | 64 |
| Subquery depth | 32 |
| Set nesting | 64 |
| Default result | 100,000 rows / 64 MiB |
| Default relation intermediates | 64 MiB |

Returned `database/sql` values are nil for SQL NULL, bool for booleans, int64
for exactly representable integral numbers, and byte slices for other exact
numbers, strings, and JSON. NUMBER uses exact JSON-decimal identity: `1`, `1.0`,
and `1e0` compare as the same number. Do not round through `float64`.

## Source map

- Native/typed adapters and values: `sql/driver/driver.go:22-79`, `sql/driver/driver.go:308-419`, `sql/driver/runtime.go:22-242`
- Transactions and embedded mutations: `sql/driver/tx.go:89-184`, `sql/driver/savepoint.go:7-190`, `sql/driver/column_update.go:23-112`
- RF3 postimages and replay: `gateway/replicated_sql_transaction.go:128-565`, `gateway/durable_sql_request_executor.go:103-157`
