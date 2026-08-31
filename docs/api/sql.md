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

The DSN is the catalog path. Import the driver for registration:

```go
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/thesyncim/vibedb/sql/driver"
)

func main() {
	ctx := context.Background()
	db, err := sql.Open("vibedb", "app.vdb")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.ExecContext(ctx, `CREATE TABLE counters (
		id STRING PRIMARY KEY,
		value INTEGER NOT NULL
	)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.ExecContext(ctx,
		`INSERT INTO counters (id, value) VALUES (?, ?)`, "requests", 1)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.ExecContext(ctx,
		`UPDATE counters SET value = value + 1 WHERE id = ?`, "requests")
	if err != nil {
		log.Fatal(err)
	}

	var value int64
	if err := db.QueryRowContext(ctx,
		`SELECT value FROM counters WHERE id = ?`, "requests").Scan(&value); err != nil {
		log.Fatal(err)
	}
	fmt.Println(value)
}
```

The native placeholder is `?`. Named `database/sql` arguments are rejected.
The pgwire adapter rewrites PostgreSQL `$1` placeholders before parsing; do not
use `$1` through this driver.

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

This guide primarily describes the embedded adapters. The static distributed
gateway can also maintain global indexes for computed `UPDATE` expressions by
using canonical before/after images evaluated on the base shard. The strict
RF3 transaction lane, including its pgwire path, still refuses computed
assignments; it admits only the narrower documented direct-column and
whole-document forms.

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

```go
tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
if err != nil {
	return err
}
defer tx.Rollback()

if _, err := tx.ExecContext(ctx,
	`UPDATE counters SET value = value + 1 WHERE id = ?`, "requests"); err != nil {
	return err
}
if _, err := tx.ExecContext(ctx, `SAVEPOINT before_audit`); err != nil {
	return err
}
if _, err := tx.ExecContext(ctx,
	`INSERT INTO counters (id, value) VALUES (?, ?)`, "audit", 1); err != nil {
	if _, rollbackErr := tx.ExecContext(ctx,
		`ROLLBACK TO SAVEPOINT before_audit`); rollbackErr != nil {
		return rollbackErr
	}
}
return tx.Commit()
```

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

After an unknown commit result, close the `database/sql` pool—or typed session
then database—reopen the catalog, and reconcile; never blindly retry the write.

## Typed runtime

Use the typed runtime when connection/session ownership must be explicit:

```go
database, err := driver.Open("app.vdb")
if err != nil {
	return err
}
defer database.Close()

session, err := database.NewSession(ctx)
if err != nil {
	return err
}
defer session.Close()

prepared, err := session.Prepare(ctx,
	`SELECT id, value FROM counters WHERE id = ?`)
if err != nil {
	return err
}
defer prepared.Close()

cursor, err := prepared.Query(ctx, []any{"requests"})
if err != nil {
	return err
}
defer cursor.Close()
for cursor.Next() {
	id, _ := cursor.Cell(0).Text()
	value, _ := cursor.Cell(1).Int64()
	fmt.Println(id, value)
}
```

That example imports `github.com/thesyncim/vibedb/sql/driver`.

Ownership rules are strict:

- `Database` owns the catalog writer lease. `Database.Close` prevents new
  sessions; existing sessions keep the catalog alive until they close.
- `Session` is single-consumer and permits at most one live cursor and one
  active transaction.
- `Prepared.Close` releases parsed and compiled arenas and closes its live
  cursor. It is idempotent.
- Cursor cells borrow runtime storage until `Cursor.Close`; copy data that must
  survive. `Cursor.Close` releases the snapshot lease and is idempotent.
- Cursor → prepared → session → database is clearest; Close methods safely cascade, and sessions outlive Database.Close.

`Session.SetResultLimits`, `SetIntermediateLimit`, and `SetMemoryLimit` must be
configured while idle with no live cursor. Zero selects defaults; `-1` disables
result/intermediate caps. A positive memory limit must be at least 64 KiB.

## Values and limits

Both APIs accept nil, bool, finite floats, UTF-8 string/bytes, and `query.Number`; document string/bytes must be valid JSON.
`database/sql` accepts standard-convertible integers and `vibejson.RawValue` only as an exact number.
Typed runtime accepts all integer widths, `RawValue` as a valid document or exact-number scalar,
and `*bool`, `*int64`, `*float64`, `*string`, `*[]byte`, or `*query.Number`.

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
numbers, strings, and JSON. Do not assume every NUMBER fits `float64`.

## Parser ownership

The zero VibeDB `sql.Parser` is reusable but not concurrent. Its AST borrows
arena storage until the next parse or release. `Parse` accepts SELECT-family
queries; `ParseStatement` accepts all supported statements. Convert byte-offset
parse errors before presenting a character-oriented protocol diagnostic.

## Source map

- Driver registration and argument limits: `sql/driver/driver.go:22-79`, `sql/driver/driver.go:308-419`
- Typed ownership: `sql/driver/runtime.go:22-65`, `sql/driver/runtime.go:129-242`, `sql/driver/runtime.go:608-980`
- Transactions/savepoints: `sql/driver/tx.go:89-184`, `sql/driver/tx.go:2138-2486`, `sql/driver/savepoint.go:7-190`
- Mutations and null/missing: `sql/driver/column_update.go:23-112`, `sql/driver/write.go:2452-2577`, `sql/ast.go:684-691`
- Mutation-image capture: `sql/driver/mutation_capture.go`, `gateway/writer.go`, `gateway/transaction.go`
- Parser ownership and limits: `sql/parser.go:19-76`, `sql/parser.go:328-377`, `sql/error.go:9-38`
