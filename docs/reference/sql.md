# SQL reference

[Documentation](../README.md) / [Reference](README.md) · [Stability](../status.md)

Executable SQL dialect shared by `sql/driver` and `pgwire`. Syntax not listed
here is unsupported.

## Lexical rules and statements

- Keywords are case-insensitive; identifiers are case-sensitive (`"..."`
  preserves reserved text); strings are single-quoted; comments use `--` and
  `/* ... */`.
- Native placeholder is `?`; `pgwire` rewrites `$1`…`$32767` to `?`.
- Operators: `= != <> < <= > >= @> ==> + - * / % || :: -> ->>` plus regex
  (`~`, `~*`, `!~`, `!~*`, `SIMILAR TO`) and JSONB (`?`, `?|`, `?&`).

| Family | Statements |
| --- | --- |
| Query | `SELECT`, `VALUES`, `TABLE`, `UNION`, `INTERSECT`, `EXCEPT`, `EXPLAIN [ANALYZE]` |
| DML | `INSERT`, `UPDATE`, `DELETE` (with `RETURNING`, `UPDATE ... FROM`, `DELETE ... USING`, modifying CTEs) |
| DDL | `CREATE TABLE [AS SELECT]`, `ALTER TABLE`, `DROP TABLE`, `TRUNCATE`, `CREATE [UNIQUE] INDEX [... USING tin]`, `DROP INDEX`, `CREATE VIEW`, `DROP VIEW` |
| Wire & Tx | `BEGIN`/`START TRANSACTION`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `RELEASE`, `ROLLBACK TO`, wire `COPY ... FROM STDIN / TO STDOUT` |

DDL cannot run inside a transaction and operates on one object per statement.

## DDL: tables, indexes, and views

```sql
CREATE TABLE accounts (
    id STRING PRIMARY KEY,
    profile.name STRING NOT NULL,
    balance NUMERIC DEFAULT 0 CHECK (balance >= 0),
    active BOOLEAN NULL,
    tags ARRAY,
    metadata OBJECT
);
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS profile.tier STRING NULL;
CREATE UNIQUE INDEX IF NOT EXISTS accounts_email ON accounts(profile.email);
CREATE INDEX accounts_bio_tin ON accounts(profile.bio) USING tin;
```

- **Types:** `NULL`, `BOOL`/`BOOLEAN`, `NUMBER`/`DECIMAL`/`NUMERIC`/`FLOAT`/
  `REAL`/`DOUBLE`, `INTEGER`/`INT`/`INT2`/`INT4`/`INT8`/`BIGINT`/`SMALLINT`/
  `TINYINT`, `STRING`/`TEXT`/`VARCHAR`/`CLOB`, `ARRAY`, `OBJECT`, `ANY`/`JSON`,
  `TIMESTAMP`/`INTERVAL`.
- **Table constraints & DDL:** Composite primary keys (up to 4 columns),
  `UNIQUE`, `DEFAULT`, `CHECK` (`23514`), `ALTER TABLE` (`ADD`/`DROP`/`RENAME`/
  `SET TYPE`/`SET NOT NULL`/`DROP NOT NULL`/`SET DEFAULT`/`DROP DEFAULT`/
  `ADD CONSTRAINT CHECK`/`RENAME TO`), `CREATE TABLE ... AS SELECT`, and
  `CREATE INDEX` options (`WHERE`, expression, `INCLUDE`, `DESC`/`NULLS`).
  `FOREIGN KEY`, sequences, generated columns, and collations are unsupported.
- **Exact indexes:** 1–4 scalar paths (omits null/missing; unique rejects
  duplicate non-null tuples).
- **Full-text (`USING tin`):** Single string path for `==>` and `SCORE()` (max
  64 per catalog; `UNIQUE` refused; incremental `.tin` sidecar). See
  [search](../api/search.md).
- **Views:** Durable read-only views (no params, no `CREATE OR REPLACE`, no
  materialized views; expansion cap: depth 32, 1,024 refs, 16 MiB).

## INSERT and upsert

```sql
INSERT INTO accounts VALUES (?); -- whole JSON document
INSERT INTO accounts AS t (id, balance, active) VALUES (?, ?, TRUE)
ON CONFLICT (id) DO UPDATE SET balance = t.balance + EXCLUDED.balance
RETURNING id, balance;
```

- Supports `ON CONFLICT DO NOTHING` and `DO UPDATE` (including `WHERE`
  predicates and composite PK targets in embedded SQL; branch-aware
  `ON CONFLICT DO NOTHING/UPDATE` with global indexes in RF3 SQL).
- Qualify current-row fields with the table name/alias and incoming fields with
  `EXCLUDED`. `INSERT ... SELECT` and `INSERT ... DEFAULT VALUES` are supported
  in embedded SQL.

## UPDATE and DELETE

```sql
UPDATE accounts AS a SET balance = a.balance + ? WHERE a.id = ? RETURNING id, balance;
DELETE FROM accounts WHERE active = FALSE ORDER BY id LIMIT 100 RETURNING id;
```

- Embedded SQL supports `UPDATE ... FROM`, `DELETE ... USING`, data-modifying
  CTEs, `ORDER BY ... LIMIT`, and `RETURNING`. Primary keys cannot be modified
  in place.
- **RF3 gateway boundary:** Autocommit-only for writes; exact-PK `UPDATE`
  (whole-doc or declared top-level computed scalar assignments guarded by
  old-length + SHA-256 CAS), `INSERT VALUES` (including branch-aware
  `ON CONFLICT`), and `DELETE`; `ORDER BY`/`LIMIT` and nested targets remain
  fenced on RF3 mutations.

## SELECT, expressions, and predicates

```sql
WITH [RECURSIVE] ...
SELECT [ALL | DISTINCT [ON (...)]] projection [, ...]
[FROM relation] [WHERE predicate] [GROUP BY ...] [HAVING predicate]
[WINDOW name AS (...)] [ORDER BY key [ASC|DESC] [NULLS FIRST|LAST]]
[LIMIT n] [OFFSET n] [FOR UPDATE | FOR SHARE];
```

- **Paths & JSON:** Dotted/bracket paths (`profile.name`, `items[0]`,
  `"$doc"`), `->`/`->>`, JSONB `?`/`?|`/`?&`/`jsonb_set`, `ARRAY[...]`/`ANY`/
  `ALL`/`UNNEST`, row-value comparisons.
- **Expressions:** Exact decimal arithmetic (`+ - * / %`), `||`, `CAST`/
  `::` (`TEXT`, `BOOL`, `NUMERIC`, `JSON`), `CASE` (≤1,024 arms), `COALESCE`,
  `GREATEST`, `LEAST`, `NULLIF`, `TIMESTAMP`/`NOW() + INTERVAL`.
- **Aggregates:** `COUNT(*)`, `COUNT(path)`, `SUM`, `AVG`, `MIN`, `MAX` (with
  `DISTINCT` and `FILTER (WHERE ...)`), `STRING_AGG`, `ARRAY_AGG`, `JSON_AGG`/
  `JSONB_AGG`, `BOOL_AND`/`EVERY`, `BOOL_OR`. Exact decimal math (no `float64`
  rounding).
- **Predicates & 3VL:** Comparisons, `IS [NOT] NULL`, `IS [NOT] MISSING`
  (absence only), `IS [NOT] DISTINCT FROM`, `IS [NOT] TRUE/FALSE`, `IN`,
  `BETWEEN`, `@>`, `LIKE`/`ILIKE`, `==>`, `SCORE()`, `EXISTS`, scalar
  subqueries. Authored `x = NULL` is rejected.
- **Joins, CTEs, sets, windows:** `INNER`, `LEFT`, `RIGHT`, `FULL`, `CROSS`,
  `USING`, `LATERAL`; `WITH RECURSIVE` (cap: 1,000 iters, 100,000 rows, 64 MiB);
  `UNION`/`INTERSECT`/`EXCEPT [ALL|DISTINCT]`; full window suite
  (`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, `NTILE`, `PERCENT_RANK`,
  `CUME_DIST`, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`, aggregate `OVER` with
  `ROWS`/`GROUPS`/`RANGE` frames and `EXCLUDE`).
- **Virtual tables (`pgwire`):** `pg_stat_activity`, `pg_stat_statements`,
  `pg_stat_statements_reset()`, `pg_cancel_backend(pid)`,
  `pg_terminate_backend(pid)`, plus queryable `information_schema` /
  `pg_catalog` views.

## Limits and unsupported families

- **Limits:** SQL ≤16 MiB; ≤65,536 params (32,767 in `pgwire`); depth 64
  (subqueries 32); ≤1,024 items/list; savepoints ≤64; Tx ≤64 docs/table, ≤16
  tables / 256 docs / ~64 MiB; default result 100,000 rows / 64 MiB.
- **Unsupported:** `MERGE`, `REPLACE`, file-path `COPY`, `GRANT`/`REVOKE`,
  `VACUUM`, `ANALYZE`, `REINDEX`, `CLUSTER`, `LISTEN`/`NOTIFY`, `CALL`, `DO`,
  stored procedures, triggers, foreign keys, sequences, materialized views.

## Source map

- Parser: [sql/parser.go](../../sql/parser.go), [sql/lexer.go](../../sql/lexer.go), [sql/parse_dml.go](../../sql/parse_dml.go), [sql/parse_ddl.go](../../sql/parse_ddl.go)
- Execution & RF3: [sql/driver/write.go](../../sql/driver/write.go), [sql/driver/tx.go](../../sql/driver/tx.go), [gateway/replicated_sql_transaction.go](../../gateway/replicated_sql_transaction.go)
