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
- `ALTER TABLE ... ADD COLUMN`
- `CREATE INDEX`
- `DROP TABLE`
- `TRUNCATE`
- `DROP INDEX`
- `CREATE VIEW`
- `DROP VIEW`
- `SAVEPOINT`
- `RELEASE SAVEPOINT`
- `ROLLBACK TO SAVEPOINT`

Recognized unsupported families include `MERGE`, `REPLACE`, `COPY`, `GRANT`,
`REVOKE`, `VACUUM`, `ANALYZE`, `REINDEX`, `CLUSTER`, SQL `PREPARE`, SQL cursors,
notifications, `LOCK`, procedures, and `DO`. ALTER actions other than the
bounded additive form are recognized and refused.

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
Output aliases accept both `expression AS name` and PostgreSQL's
`expression name` spelling. A standalone `*` cannot be aliased; qualify it as
`range_variable.*` when the projected document needs a name.

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
- `CAST` and PostgreSQL `::` casts to `TEXT`, `BOOLEAN`, `NUMERIC`, and `JSON`
- PostgreSQL typed string constants `BOOL 'value'`, `BOOLEAN 'value'`, and
  `TEXT 'value'`. Boolean input follows PostgreSQL's unique-prefix grammar and
  is validated once at prepare; these constants add no row-time conversion.
- JSON paths and nonnegative array subscripts
- Quoted identifiers
- The whole-document path

The numeric parser keeps exact decimal spelling. It does not route numeric
input through binary floating point first.

Supported predicates include comparison operators, `IN`, `NOT IN`,
`BETWEEN`, null and missing tests, JSON containment `@>`, `LIKE`, `ILIKE`,
Boolean operators, `EXISTS`, and scalar-subquery comparisons.

Unsupported forms include regular-expression operators, `LIKE ... ESCAPE`,
general scalar function calls, named parser parameters, cast type modifiers,
cast arrays, typed constants for other type names, qualified or modified typed
constants, and negative array subscripts.

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

When a supported PostgreSQL typed constant selects a `BOOLEAN` or `TEXT`
domain, set expressions use PostgreSQL's pairwise common-type rules. Bare
string literals, `NULL`, and placeholders begin as `unknown`; known strings are
converted during prepare, while inferred placeholders use the selected input
type at bind even when their producing leaf returns no rows. A `VALUES` column
resolves at its own query boundary. In a parenthesized operand, local `ORDER BY`
also finalizes a remaining unknown output as `TEXT`; a tail containing only
`LIMIT` and/or `OFFSET` leaves it available for the enclosing set operation's
common-type selection.

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
derived tables, explicit `LATERAL`, and comma-separated `FROM` items. A comma
item lowers to the same cross-product plan as `CROSS JOIN`.

PostgreSQL binds explicit `JOIN` more tightly than comma. The flat join AST
does not yet represent an explicit join tree on the right of a comma, so forms
such as `FROM a, b JOIN c ON ...` return `0A000` instead of being flattened
with incorrect `ON` scope or outer-join multiplicity. It also does not support
`NATURAL JOIN`, `ON` or `USING` on a cross join, or `JOIN LATERAL ... USING`.

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

Conflict handling uses the document-derived primary key as its implicit target.
`ON CONFLICT DO NOTHING` skips existing rows. Embedded `INSERT ... VALUES`
also supports `ON CONFLICT DO UPDATE SET` with either the whole-document form
`"$doc" = EXCLUDED."$doc"` or declared top-level scalar assignments whose
values are literals, placeholders, `NULL`, or `EXCLUDED.<column>`. The action is
atomic across the batch and returns final post-images through `RETURNING`.
Duplicate canonical candidate keys are cardinality violations. Explicit
targets, action predicates, nested/current-row expressions, and `INSERT ...
SELECT DO UPDATE` remain unsupported. RF3/distributed writes fail closed on
conflict updates; conflict-skipping inserts are also refused when READY global
indexes would require branch-aware maintenance.

Both conflict actions use only the document-derived primary key. A secondary
unique-index collision is a unique violation. It does not select a conflict
action.

## UPDATE and DELETE

`UPDATE` can assign the whole document through `"$doc"`. On a table with
declared columns it can also assign scalar literals, placeholders, `NULL`, or
supported scalar expressions to one or more top-level columns while preserving
every unassigned field in each matching document. Arithmetic, concatenation,
unary expressions, casts, and `CASE` are evaluated once per matched old row;
mixed right-hand sides are simultaneous.

Nested-path targets and `UPDATE ... FROM` are not supported. Static distributed
writes use shard-evaluated canonical postimages for maintained global indexes.
The strict RF3 mutation lane evaluates exact-primary-key computed updates at the
coordinator, retains the canonical postimage in the durable logical program,
and guards publication with the old row's exact length and digest. RF3
`RETURNING` remains a separate unsupported result-ordering contract.

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

`ALTER TABLE name ADD [COLUMN] [IF NOT EXISTS] path type [NULL|NOT NULL]` adds
one declared field. The embedded driver validates existing rows by building a
replacement storage incarnation and publishes the schema and copied rows
atomically. Existing indexes are retained. A nullable field admits older rows
where the path is absent; a required field does not. Schema DDL is rejected
inside an explicit transaction. The embedded copy currently holds the catalog
write lock, so ordinary reads and writes wait until the ALTER completes.

Rename, drop, type changes, defaults, nullability changes, and constraint
changes are not supported ALTER actions.

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

`CREATE INDEX` and `CREATE UNIQUE INDEX` support `IF NOT EXISTS`, optional
explicit names, and one to four distinct JSON paths. A nonunique index uses the
durable online build over a materialized table.

A unique index constrains the order-sensitive exact scalar tuple. Every indexed
path must be present and contain a Boolean, number, or string for the document
to participate. Numeric spellings with the same exact value have one identity.
If any tuple component is missing or JSON null, the document does not
participate. This is default `NULLS DISTINCT` behavior. Present arrays and
objects fail closed during index creation and later writes.

The unique build validates existing rows and publishes no index on a duplicate
or invalid container. INSERT, UPDATE, primary-key upsert, transaction commit,
and reopen enforce the same final-image constraint. Embedded pgwire maps a
conflict to SQLSTATE `23505`. The durable unique build currently holds the
catalog write lock while it validates the table.

Column and table `UNIQUE`, `NULLS NOT DISTINCT`, partial indexes, expression
indexes, ordering, collation, and selectable index methods are not supported.
In the local `OpenCluster` facade, a unique index on a placed table must contain
every shard-key path. RF3 SQL `CREATE UNIQUE INDEX` is refused before schema
rollout because it has no coordinated distributed uniqueness build.

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
- `sql/driver/validate.go`, `write.go`, `tx.go`, and `unique_index.go`
