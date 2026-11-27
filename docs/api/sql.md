# SQL API

[Documentation](../README.md) / [API guides](README.md) · [Stability](../status.md)

VibeDB provides embedded SQL over JSON document tables through two Go surfaces:

- **`database/sql`** (driver `"vibedb"`): standard Go pooling and scanning.
- **`sql/driver` typed runtime**: explicit sessions, typed `Cell`s, and zero-alloc reuse.

See the [SQL reference](../reference/sql.md) for dialect syntax and
[pgwire](pgwire.md) for network access.

## `database/sql` quick start

The DSN is the catalog path. Opening `<path>` owns `<path>`, `<path>.lock`, and
`<path>.tables/` (one primary + `.rjournal` per table); back up all three
together while closed.

```go
connector, err := (vibedriver.Driver{}).OpenConnector(path)
db := sql.OpenDB(connector)
defer db.Close()

_, err = db.ExecContext(ctx, `CREATE TABLE counters (id STRING PRIMARY KEY, value INTEGER NOT NULL)`)
_, err = db.ExecContext(ctx, `INSERT INTO counters (id, value) VALUES (?, ?)`, "requests", 1)
_, err = db.ExecContext(ctx, `UPDATE counters SET value = value + 1 WHERE id = ?`, "requests")

var value int64
err = db.QueryRowContext(ctx, `SELECT value FROM counters WHERE id = ?`, "requests").Scan(&value)
// value == 2
```

`sql.Open("vibedb", path)` also works with `_ "github.com/thesyncim/vibedb/sql/driver"`.

- Placeholders are `?` (named args and `$1` are rejected; `pgwire` rewrites
  `$1`).
- One statement per call. Use `Query` for `SELECT`, `EXPLAIN`, and `RETURNING`;
  use `Exec` for DDL and non-`RETURNING` DML. `LastInsertId` is unsupported.

## Whole documents, upserts, and null vs missing

`"$doc"` addresses the entire JSON document:

```sql
CREATE TABLE users (id STRING PRIMARY KEY, name STRING, visits INTEGER);
INSERT INTO users VALUES ('{"id":"u1","name":"Ada","visits":1}');

INSERT INTO users (id, name, visits) VALUES ('u1', 'Ada Lovelace', 1)
ON CONFLICT DO UPDATE SET visits = users.visits + EXCLUDED.visits, name = EXCLUDED.name;

UPDATE users SET "$doc" = '{"id":"u1","name":"Augusta","visits":3}' WHERE id = 'u1';
```

`IS NULL` matches both JSON `null` and absent paths; `IS MISSING` matches
absence only; `IS NOT NULL` matches present non-null values. Bare `col = NULL`
is rejected.

## Transactions and savepoints

| Isolation | Read cut | Commit validation |
| --- | --- | --- |
| Read Committed (`database/sql` default) | Coherent cut per statement | Write conflicts |
| Repeatable Read / Snapshot | Cut at `BEGIN` | Write conflicts (write skew possible) |
| Serializable | Cut at `BEGIN` | Write conflicts + exact/coarse read dependencies |

- Multi-table commits are crash-atomic via the catalog decision log. Conflicts
  are first-committer-wins (SQLSTATE `40001` via `pgwire`). DDL inside a
  transaction is refused.
- Up to 64 savepoints per transaction (`SAVEPOINT`, `RELEASE`, `ROLLBACK TO`).
- Close or exhaust `Rows` before the next operation on the same `*sql.Tx`. On
  an unknown commit outcome, close, reopen, and reconcile before retrying.

## Typed runtime (`sql/driver`)

Do not open the same catalog concurrently with `database/sql` (single-process
writer lock):

```go
database, err := vibedriver.Open(path)
defer database.Close()
session, err := database.NewSession(ctx)
defer session.Close()
prep, err := session.Prepare(ctx, `SELECT id, value FROM counters WHERE id = ?`)
defer prep.Close()
cur, err := prep.Query(ctx, []any{"requests"})
defer cur.Close()
if cur.Next() {
	v, _ := cur.Cell(1).Int64()
	fmt.Println(v)
}
```

- `Session` is single-consumer (one live cursor, one active transaction).
  Cursor cells borrow runtime storage until `cur.Close()`.
- Configure `Session.SetResultLimits`, `SetIntermediateLimit`, and
  `SetMemoryLimit` (min 64 KiB) while idle.

## Values and limits

- **Inputs:** `database/sql` accepts `nil`, `bool`, integers, finite `float64`,
  UTF-8 `string`/`[]byte`, `query.Number`, numeric `vibejson.RawValue`. Typed
  runtime adds all int widths, `float32`, pointers, and document `RawValue`.
- **Outputs:** `nil`, `bool`, `int64` (exact integral fit), or `[]byte` (exact
  decimal numbers, strings, JSON).
- **Bounds:** SQL text ≤ 16 MiB; ≤ 65,536 params (≤ 4 MiB each, ≤ 16 MiB
  total); PK ≤ 256 B; doc ≤ 4 MiB; Tx ≤ 64 docs/~16 MiB per table and ≤ 16
  tables / 256 docs / ~64 MiB total; clause items ≤ 1,024; defaults 100,000
  rows / 64 MiB result / 64 MiB intermediates.

## Distributed RF3 differences

RF3 SQL via the gateway is **autocommit-only** for writes (`INSERT VALUES`,
`ON CONFLICT DO NOTHING/UPDATE`, `DELETE`, exact-PK `UPDATE` without
`RETURNING`, `ORDER BY`/`LIMIT`, nested targets, or PK moves). Computed
top-level `UPDATE` assignments evaluate once against a committed preimage and
retain a SHA-256-guarded postimage for idempotent replay. See
[SQL reference](../reference/sql.md#update-and-delete).

## Limitations

- Single-process catalog lock; fixed synchronous durability (no buffered mode).
- Single-column primary keys only; no `DEFAULT`, `CHECK`, `FOREIGN KEY`,
  sequences, generated columns, or DDL inside transactions.

## Source map

- Adapter & runtime: [sql/driver/driver.go](../../sql/driver/driver.go), [sql/driver/runtime.go](../../sql/driver/runtime.go), [sql/driver/catalog.go](../../sql/driver/catalog.go)
- Transactions & writes: [sql/driver/tx.go](../../sql/driver/tx.go), [sql/driver/savepoint.go](../../sql/driver/savepoint.go), [sql/driver/write.go](../../sql/driver/write.go)
- RF3 SQL execution: [gateway/replicated_sql_transaction.go](../../gateway/replicated_sql_transaction.go), [gateway/durable_sql_request_executor.go](../../gateway/durable_sql_request_executor.go)
