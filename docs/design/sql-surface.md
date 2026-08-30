# SQL surface reference

VibeDB implements a bounded SQL dialect over JSON documents. This page
describes the local parser and runtime. Distributed execution has additional
restrictions in [Distributed query planning](query-planner.md).

## Parser limits

| Input | Maximum |
| --- | ---: |
| Statement text | 16 MiB |
| Expression depth | 64 |
| Subquery depth | 32 |
| Set-expression depth | 64 |
| Parameters | 65,536 |
| Items in one clause | 1024 |

The parser accepts one statement. A parser instance is reusable and
single-consumer. The next parse invalidates its borrowed AST.

## Statement classes

Supported classes are:

- `SELECT`
- `INSERT`
- `UPDATE`
- `DELETE`
- `CREATE TABLE`
- `CREATE INDEX`
- `DROP TABLE`
- `TRUNCATE`
- `DROP INDEX`
- `CREATE VIEW`
- `DROP VIEW`
- `SAVEPOINT`
- `RELEASE SAVEPOINT`
- `ROLLBACK TO SAVEPOINT`

Recognized unsupported families include `MERGE`, `REPLACE`, `COPY`, `ALTER`,
`GRANT`, `REVOKE`, `VACUUM`, `ANALYZE`, `REINDEX`, `CLUSTER`, SQL `PREPARE`,
SQL cursors, notifications, `LOCK`, procedures, and `DO`.

Unsupported recognized syntax returns `FeatureNotSupportedError`. Pgwire maps
it to SQLSTATE `0A000`.

## SELECT

`SELECT` supports:

- Ordinary and recursive `WITH`
- `DISTINCT`
- `FROM`, `WHERE`, `GROUP BY`, and `HAVING`
- Named windows
- `ORDER BY`, `LIMIT`, and `OFFSET`
- Set expressions
- Parameters
- Correlated outer references

A FROM-less query is valid when no expression needs a row source.

Top-level `ORDER BY` accepts a projected path, output alias, or 1-based output
position. Project and name a general scalar expression before you order by it.

Top-level `NULLS FIRST`, `NULLS LAST`, `COLLATE`, and `FETCH FIRST` are not
supported. Ascending top-level order puts null and missing values first.
Descending order puts them last. Window ordering has separate behavior: its
default is nulls last for ascending order and first for descending order, and
it accepts an explicit null order.

## Scalar expressions

Supported forms include:

- Arithmetic `+`, `-`, `*`, `/`, and `%`
- String concatenation `||`
- Unary `+` and `-`
- `CASE`
- `CAST` to `TEXT`, `BOOLEAN`, `NUMERIC`, and `JSON`
- JSON paths and nonnegative array subscripts
- Quoted identifiers
- The whole-document path

The numeric parser keeps exact decimal spelling. It does not route numeric
input through binary floating point first.

Supported predicates include comparison operators, `IN`, `NOT IN`,
`BETWEEN`, null and missing tests, JSON containment `@>`, `LIKE`, `ILIKE`,
Boolean operators, `EXISTS`, and scalar-subquery comparisons.

Unsupported forms include regular-expression operators, `LIKE ... ESCAPE`,
PostgreSQL `::` casts, general scalar function calls, named parser parameters,
and negative array subscripts.

The parser itself uses `?` placeholders. Pgwire rewrites `$n` placeholders
before parse.

## Aggregation

The aggregate set is exactly:

- `COUNT`
- `SUM`
- `AVG`
- `MIN`
- `MAX`

A nonaggregate projected field must be in the grouping set. An aggregate is
not valid in `WHERE`.

`HAVING` can compare grouped keys and projected aggregates. An aggregate in
`HAVING` must also appear in the SELECT list. Computed scalar expressions,
`IS MISSING`, containment, `LIKE`, and `ILIKE` are not executable after
reduction.

## Window functions

Supported window functions are:

- `ROW_NUMBER`, `RANK`, and `DENSE_RANK`
- `LAG` and `LEAD`
- `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`
- `NTILE`, `PERCENT_RANK`, and `CUME_DIST`
- `FIRST_VALUE`, `LAST_VALUE`, and `NTH_VALUE`

Frames support `ROWS`, `GROUPS`, and `RANGE`. Exclusions support `NO OTHERS`,
`CURRENT ROW`, `GROUP`, and `TIES`.

## Set expressions and VALUES

The runtime implements:

- `UNION` and `UNION ALL`
- `INTERSECT` and `INTERSECT ALL`
- `EXCEPT` and `EXCEPT ALL`

An operand can be `SELECT`, `VALUES`, `TABLE`, or a parenthesized set group. An
operand-local order or limit tail must be parenthesized.

A `VALUES` leaf accepts scalar literals, `NULL`, and placeholders. It is not a
general row-dependent expression engine. `WITH` directly before a `VALUES` or
`TABLE` root is not supported.

## CTEs

Ordinary CTEs support aliases, `MATERIALIZED`, `NOT MATERIALIZED`, and multiple
references.

A recursive CTE uses one anchor SELECT, `UNION` or `UNION ALL`, and one
recursive SELECT. Defaults limit execution to 1,000 recursive-term
evaluations, 100,000 result rows, and 64 MiB of retained fixpoint storage.

The validator refuses anchor self-reference, several recursive references,
mutual forward references, nested self-reference, aggregate recursive terms,
`INTERSECT` recursion, `SEARCH`, and `CYCLE`.

A recursive reference cannot be on the nullable side of an outer join. It can
be on the preserved side of a left join.

## Joins and derived tables

The local runtime supports `INNER`, `LEFT`, `RIGHT`, `FULL`, and `CROSS` joins.
It supports arbitrary `ON` predicates, composite `USING`, chained joins,
derived tables, and explicit `LATERAL`.

It does not support `NATURAL JOIN`, comma joins, `ON` or `USING` on a cross
join, or `JOIN LATERAL ... USING`.

Cross-table execution needs a coherent catalog source. A single collection
source cannot resolve another relation.

## Subqueries and correlation

Uncorrelated support includes `EXISTS`, membership predicates, scalar
subqueries, and scalar comparison with a subquery.

A scalar subquery that returns more than one row returns a cardinality error.
Pgwire maps it to SQLSTATE `21000`.

Supported correlated mark forms are `EXISTS`, `NOT EXISTS`, `IN`, `NOT IN`,
and scalar. The planner needs a provable equality-key correlation shape.

Correlation below `OR`, non-equality correlation, nested predicate subqueries,
and a correlated predicate subquery in `JOIN ON` are not supported.

Explicit LATERAL correlation supports multiple captured slots and nesting
depths.

## INSERT

`INSERT` accepts a complete JSON document:

```sql
INSERT INTO docs VALUES (?)
```

A complete document can be a placeholder, a single-quoted SQL string that
contains JSON text, or a bare JSON object or array. Bare scalar JSON and bare
`NULL` are not complete-document spellings.

It also accepts literal JSON and flat top-level field construction:

```sql
INSERT INTO docs (id, category, score) VALUES (?, ?, ?)
```

A multi-row insertion is atomic.

`INSERT ... SELECT` requires one output column of complete JSON documents. It
does not construct named columns from several outputs. The source query reads
the pre-statement snapshot.

Conflict handling supports `ON CONFLICT DO NOTHING` only.

## UPDATE and DELETE

`UPDATE` can assign the whole document through `"$doc"`. On a table with
declared columns it can also assign scalar literals or placeholders to one or
more top-level columns while preserving every unassigned field in each matching
document.

Nested-path targets, row-dependent assignment expressions such as
`score = score + 1`, and `UPDATE ... FROM` are not supported.

The replacement must preserve the primary key. One constant replacement
cannot update multiple rows that have different primary keys.

`DELETE` supports an optional `WHERE`. Without it, the statement deletes all
rows. `DELETE ... USING` is not supported.

Both statements support `ORDER BY`, `LIMIT`, and `RETURNING`.

## RETURNING

`INSERT`, `UPDATE`, and `DELETE` support projection through `RETURNING`. The
projection cannot contain an aggregate or a parameter.

Use `Query` for a statement with `RETURNING`. `Exec` rejects a row-producing
statement.

## Tables and types

The driver supports one scalar primary JSON path. A compound primary-key AST
can parse, but the driver refuses it.

Supported JSON domains are `NULL`, `BOOL`, `NUMBER`, `INTEGER`, `STRING`,
`ARRAY`, `OBJECT`, and `ANY`. Recognized SQL aliases normalize to these domains.

Supported constraints are nullability, `NOT NULL`, and `PRIMARY KEY`.

Unsupported column features include length or precision arguments, defaults,
unique constraints, collation, temporal types, UUID, binary types, enum,
serial types, money, fixed character types, `JSONB`, records, geometry,
network types, and XML.

The adapter reserves `$doc` for the complete document. `CREATE TABLE AS SELECT`
is not supported.

## Views

Durable ordinary views support create, drop with restrict behavior, and column
aliases. A view definition cannot contain parameters.

Materialized views, replace, cascade drop, and refresh are not supported.

## Exact indexes

`CREATE INDEX` supports `IF NOT EXISTS`, optional explicit names, several JSON
paths, and online creation over a materialized table.

Indexes are exact and nonunique. `CREATE UNIQUE INDEX` is not supported on the
local SQL surface.

An exact-index candidate never replaces predicate evaluation. Execution checks
the complete predicate after it gets candidates.

## Transactions

Default and read-committed transactions refresh their coherent read cut for
each data statement. Repeatable-read and snapshot transactions keep the
`BEGIN` cut. Serializable transactions also validate read dependencies.

Transactions provide read-your-writes and optimistic first-committer-wins
write conflict detection. DDL is not allowed in a transaction.

Savepoints support shadowed duplicate names, rollback, and release. The maximum
is 64.

## Implementation references

- `sql/parser.go`, `ast.go`, `dml.go`, and `set.go`
- `sql/parse_ddl.go`, `parse_dml.go`, and `unsupported.go`
- `query/sqlstmt.go`, `join.go`, and `cte_runtime.go`
- `sql/driver/validate.go`, `write.go`, and `tx.go`
