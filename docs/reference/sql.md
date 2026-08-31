# SQL reference

> [!CAUTION]
> Unreleased development software: this describes one commit, not a stable
> dialect. Grammar, catalog encoding, semantics, limits, and protocol mappings
> may break on any commit. Older development catalogs are not migration-
> compatible unless explicitly stated otherwise.

This is the executable surface shared by the native driver and pgwire. The
parser can represent a few forms an execution adapter rejects; those are called
out below. PostgreSQL syntax not listed here is unsupported.

## Lexical rules

| Rule | Behavior |
|---|---|
| Keywords | Case-insensitive |
| Identifiers | Case-sensitive; double quotes preserve otherwise reserved text |
| Strings | Single-quoted |
| Comments | `--` line comments and `/* ... */` block comments |
| Native parameter | `?` |
| pgwire parameter | `$1`…`$32767`, rewritten to native parameters |
| Statement terminator | One optional trailing semicolon |
| Error position | Zero-based byte offset in parser errors |

`Parse`/`ParseStatement` accept one statement; pgwire splits bounded batches.
A zero `sql.Parser` is reusable but not concurrent, and its returned AST
borrows parser-arena storage only until the next parse or release.

Operators:

```text
=  !=  <>  <  <=  >  >=  @>
+  -  *  /  %  ||  ::  ->  ->>
```

Regex operators, bitwise operators, named parameters, and native `$n`
parameters are not part of the SQL parser.

## Statement summary

| Family | Implemented statements |
|---|---|
| Query | SELECT, VALUES, TABLE, UNION, INTERSECT, EXCEPT, EXPLAIN `[ANALYZE]` query |
| DML | INSERT, UPDATE, DELETE; optional RETURNING |
| Tables | CREATE TABLE, ALTER TABLE ADD COLUMN, DROP TABLE, TRUNCATE |
| Indexes | CREATE `[UNIQUE]` INDEX, DROP INDEX |
| Views | CREATE VIEW, DROP VIEW; read-only queries |
| Savepoints | SAVEPOINT, RELEASE, ROLLBACK TO |
| Wire-only transaction commands | BEGIN/START TRANSACTION, COMMIT, ROLLBACK |

DDL cannot execute inside a transaction. CREATE/DROP/TRUNCATE operate on one
catalog object per statement.

## CREATE TABLE

```sql
CREATE TABLE accounts (
    id STRING PRIMARY KEY,
    profile.name STRING NOT NULL, balance NUMERIC,
    active BOOLEAN NULL, tags ARRAY, metadata OBJECT
);
```

The runtime requires one primary-key path resolving to a present, non-null
string, bool, or number; numeric identity is exact (`1`, `1.0`, and `1e0` match).
The parser represents up to four key paths, but the driver rejects every count
other than one; a parser-only columnless declaration is not executable.

Type spellings are grouped as follows:

| Logical family | Accepted spellings |
|---|---|
| Null | NULL |
| Boolean | BOOL, BOOLEAN |
| Exact/general number | NUMBER, DECIMAL, NUMERIC, FLOAT, REAL, DOUBLE |
| Integer | INTEGER, INT, INT2, INT4, INT8, BIGINT, SMALLINT, TINYINT |
| String | STRING, TEXT, VARCHAR, CLOB |
| Container | ARRAY, OBJECT |
| Any JSON value | ANY, JSON |

Nested column paths are allowed. A table may declare at most 1,024 columns.
`NULL`, `NOT NULL`, and `PRIMARY KEY` are the only column constraints.

Not implemented: DEFAULT, UNIQUE, CHECK, FOREIGN KEY/REFERENCES, type modifiers,
COLLATE, generated/identity columns, and CREATE TABLE AS SELECT.

## ALTER, index, drop, truncate, and views

```sql
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS profile.tier STRING NULL;
CREATE UNIQUE INDEX IF NOT EXISTS accounts_email ON accounts(profile.email);
DROP INDEX IF EXISTS accounts_email;
TRUNCATE TABLE scratch; -- or: DROP TABLE IF EXISTS scratch;
```

ALTER TABLE supports only `ADD [COLUMN] [IF NOT EXISTS] path type
[NULL|NOT NULL]`. It validates existing documents before atomically publishing
the new catalog generation. It cannot add or change a primary key.

An index contains one to four exact scalar paths. Missing/null tuples are not
indexed; container values are invalid. A unique index rejects duplicate
non-null tuples. Index sort direction, expressions, predicates, collations,
INCLUDE, and USING methods are unsupported.

DROP TABLE/INDEX and TRUNCATE reject multiple objects, CASCADE/RESTRICT, and
identity options. A table with dependent views cannot be dropped until its
views are removed.

```sql
CREATE VIEW active_accounts (id, balance) AS SELECT id, balance FROM accounts WHERE active = TRUE;
DROP VIEW active_accounts RESTRICT;
```

Views are durable and read-only. Definitions cannot contain parameters.
CREATE OR REPLACE VIEW, materialized views, REFRESH, and DROP VIEW CASCADE are
unsupported. View expansion is bounded to depth 32, 1,024 references, and
16 MiB expanded SQL; cycles are rejected.

## INSERT and upsert

```sql
INSERT INTO accounts VALUES (?);
INSERT INTO accounts AS target (id, balance, active)
VALUES (?, ?, TRUE)
ON CONFLICT DO UPDATE SET
    balance = target.balance + EXCLUDED.balance, active = EXCLUDED.active
RETURNING id, balance;
```

`ON CONFLICT` targets only the implicit primary key; secondary unique-index
violations are errors. DO NOTHING and DO UPDATE are supported. Qualify current
fields with the table name or INSERT target alias and incoming fields with
`EXCLUDED`; a bare field is ambiguous. Parameters, arithmetic, concatenation,
casts, and CASE are valid in assignment expressions.

Explicit targets, `ON CONSTRAINT`, action WHERE, nested assignments or `EXCLUDED`
paths, and mixed document/column assignments are unsupported. A DO UPDATE batch containing the same canonical key twice fails atomically.

```sql
INSERT INTO archive SELECT "$doc" FROM accounts WHERE active = FALSE ON CONFLICT DO NOTHING;
```

INSERT…SELECT requires one whole-document output and no target list. VALUES
document rows accept parameter/quoted/bare JSON and are atomic. INSERT…SELECT
with ON CONFLICT DO UPDATE is execution-refused; use VALUES or DO NOTHING.

## UPDATE and DELETE

```sql
UPDATE accounts AS a
SET balance = a.balance + ?, active = TRUE
WHERE a.id = ? RETURNING id, balance;

DELETE FROM accounts WHERE active = FALSE ORDER BY id LIMIT 100 RETURNING id;
```

UPDATE supports an alias, row expressions, whole-document replacement, declared
top-level assignments, WHERE, path ORDER BY, LIMIT, and RETURNING. UPDATE cannot
change the primary key; one constant document therefore cannot replace several
distinct keys. DELETE supports the same tail but no alias or USING. Mutation
ORDER BY requires LIMIT; UPDATE FROM and mutation OFFSET are unsupported.

The durable RF3 gateway is narrower than embedded execution. It accepts whole-
document or declared top-level UPDATE only with exact-primary-key equality;
top-level assignments may use supported scalar right-hand sides and are
simultaneous. RETURNING, ORDER BY/LIMIT, nested targets, primary-key moves, and
ON CONFLICT are refused. The coordinator linearizably reads the old row,
evaluates each right-hand side once, and retains the canonical postimage with an
exact old-length/SHA-256 CAS. Global indexes derive from that postimage. An
ordinary exact retry may first replan; after admission, transaction execution
and recovery use the retained program instead of reevaluating expressions.

RETURNING accepts bounded path/scalar projections but no aggregates,
parameters, or SELECT tail. Execute a returning mutation through a query API;
its rows are materialized before publication.

## SELECT order and projections

Clause order:

```sql
WITH ...
SELECT [ALL | DISTINCT] projection [, ...]
[FROM relation]
[WHERE predicate]
[GROUP BY path [, ...]]
[HAVING predicate]
[WINDOW name AS (...)]
[ORDER BY key [ASC | DESC] [, ...]]
[LIMIT nonnegative_integer_or_parameter]
[OFFSET nonnegative_integer_or_parameter];
```

Projections may be a path, wildcard, scalar, aggregate, or window. A wildcard
cannot mix with a computed scalar. FROM-less SELECT rejects paths, wildcards,
aggregates, and windows; only source-independent scalars are valid.

`DISTINCT` lowers to grouping and rejects computed scalar projections and
explicit GROUP BY. GROUP BY accepts stored paths, not expressions or ordinals.
HAVING requires grouping/aggregation; its aggregates must also be projected and
its plain paths must be group keys.

ORDER BY accepts a path, projection alias, or positive one-based projection
ordinal. Aliases win name resolution. A computed sort expression must be
projected. Top-level NULLS FIRST/LAST and COLLATE are unsupported. Ascending
uses nulls first; descending uses nulls last.

LIMIT and OFFSET may appear in either order, at most once each. FETCH is
unsupported.

## Paths and JSON access

```sql
SELECT profile.name, profile['display name'], items[0],
       metadata -> 'region', metadata ->> 'region', "$doc"
FROM accounts;
```

Paths use dotted components or brackets containing a nonnegative integer or
constant string. Quoted `"$doc"` means the whole document.

`->` and `->>` accept constant, nonnumeric object keys only. Dynamic keys,
numeric keys, and array indexing through these operators are unsupported.
`->>` produces terminal text.

## Scalar expressions

- String, number, boolean, and NULL literals, plus parameters.
- Paths and path-to-path comparisons in supported contexts.
- Unary `+` and `-`.
- Arithmetic `+ - * / %`, concatenation `||`, and JSON access.
- CAST using `CAST(value AS type)` or `value::type`.
- Simple and searched CASE with at least one WHEN and optional ELSE.
- COUNT, SUM, AVG, MIN, and MAX.
- Window functions listed below.

CAST targets are TEXT, BOOL/BOOLEAN, NUMERIC/DECIMAL, and JSON. Type modifiers,
arrays, qualified/multiword types, and collations are unsupported. Typed
literal prefixes are limited to BOOL/BOOLEAN and TEXT.

There is no general scalar-function catalog. Arithmetic over a window result
is not supported. CASE is lazy and bounded to 1,024 WHEN arms.

Aggregates behave as follows:

| Aggregate | Behavior |
|---|---|
| `COUNT(*)` | Counts rows |
| `COUNT(path)` | Counts present, non-null values |
| SUM/AVG/MIN/MAX | Consume numeric values, skip null/nonnumeric, null on empty input |

Exact JSON-decimal semantics are preserved: `1`, `1.0`, and `1e0` compare
equally without a `float64` round trip. Aggregate DISTINCT is unsupported.

## Predicates and three-valued logic

```sql
WHERE active = TRUE AND tier IN ('pro', 'team') AND score BETWEEN 10 AND 20
  AND profile @> '{"verified":true}' AND name ILIKE 'ada%' AND deleted_at IS MISSING
```

Implemented predicates are comparisons, IS `[NOT]` NULL, IS `[NOT]` MISSING,
IN/NOT IN lists or subqueries, BETWEEN, `@>`, LIKE/ILIKE, EXISTS, AND/OR/NOT,
and bounded scalar subqueries.

`IS NULL` is true for explicit JSON null and an absent path. `IS MISSING` is
true only for absence. Projection and wire encoding render both as SQL NULL.
Authored `value = NULL` and authored NULL members of IN are rejected; a bound
parameter may evaluate to null and follows three-valued logic.

LIKE has no ESCAPE clause. SIMILAR TO, regular-expression operators,
`IS TRUE/FALSE`, and bare boolean-path predicates are unsupported.
Computed scalar WHERE cannot share a grouped/aggregate statement. Mixed scalar/path
predicates combine only under top-level AND, not OR/NOT; HAVING rejects IS MISSING/containment.

## Joins and derived relations

```sql
SELECT a.id, o.total
FROM accounts AS a
LEFT JOIN orders AS o
  ON a.id = o.account_id AND o.state = 'open';
```

| Join | Supported |
|---|---|
| INNER / bare JOIN | Yes |
| LEFT / RIGHT / FULL `[OUTER]` | Yes |
| CROSS or comma | Yes |
| USING `(simple_name, ...)` | Yes; merged output column |
| NATURAL | No |

ON is a bounded join predicate: equality keys plus supported residual
predicates or a boolean constant. It is not an arbitrary computed-expression
or subquery join condition. USING names must be simple and distinct.

Derived tables require an alias and do not accept a column alias list.
LATERAL supports correlated CROSS/INNER/LEFT joins. RIGHT/FULL LATERAL is
accepted only when decorrelated. LATERAL USING, forward references, and self
references are rejected.

## Subqueries

EXISTS/NOT EXISTS, IN/NOT IN, and scalar subqueries are implemented. A scalar
subquery returning zero rows yields null, one row yields its value, and more
than one row is an error.

Correlation is supported when it can be reduced to bounded conjunctive
equality/proven shapes. Correlation under OR, correlated ON subqueries, nested
predicate subqueries, and generalized NOT-over-OR shapes are rejected at
prepare time. This is deliberate bounded execution, not a planner hint.

## CTEs and recursion

```sql
WITH RECURSIVE walk(node) AS (
    SELECT root FROM roots
    UNION ALL
    SELECT edges.child FROM walk JOIN edges ON walk.node = edges.parent
)
SELECT node FROM walk;
```

WITH supports column aliases, MATERIALIZED/NOT MATERIALIZED hints, and
references to earlier sibling CTEs. Data-modifying CTEs and WITH before a
top-level VALUES/TABLE operand are unsupported.

A recursive CTE is one anchor SELECT, UNION or UNION ALL, and one direct
recursive SELECT. Evaluation is breadth-first/fixpoint and finite by default.
Mutual/forward recursion, nested self-reference, SEARCH/CYCLE, recursive
aggregate/window/GROUP/HAVING/ORDER/LIMIT/OFFSET, multiple self-references, and
self-reference on a nullable join side are unsupported.

## Sets, VALUES, and TABLE

UNION, INTERSECT, and EXCEPT support ALL and DISTINCT. INTERSECT binds tighter.
Operands may be SELECT, VALUES, TABLE, or a parenthesized set expression.

VALUES cells are literals, nulls, parameters, or typed bool/text constants—not
general expressions or DEFAULT. Bounds are 1,024 rows by 1,024 columns. TABLE
accepts one unqualified relation. A set tree has at most 2,047 nodes.

## Windows

Implemented functions:

```text
ROW_NUMBER  RANK  DENSE_RANK  LAG  LEAD  NTILE
PERCENT_RANK  CUME_DIST  FIRST_VALUE  LAST_VALUE  NTH_VALUE
COUNT  SUM  AVG  MIN  MAX ... OVER
```

Named/inline windows and inheritance are supported. PARTITION/ORDER accept
paths. ROWS, GROUPS, and RANGE accept UNBOUNDED, CURRENT ROW, and value
PRECEDING/FOLLOWING bounds plus EXCLUDE CURRENT ROW/GROUP/TIES/NO OTHERS.

SQL does not expose aggregate FILTER, aggregate DISTINCT in a window, or
IGNORE/RESPECT NULLS even though lower-level execution kernels contain related
internal machinery.

## Transactions and savepoints

The native shared parser recognizes SAVEPOINT, RELEASE, and ROLLBACK TO.
`database/sql` and typed runtime APIs start/commit/rollback transactions. The
pgwire adapter additionally parses BEGIN/START TRANSACTION, COMMIT, and full
ROLLBACK, including READ ONLY/WRITE and Read Committed, Repeatable Read, or
Serializable. READ UNCOMMITTED and chaining are unsupported.

Savepoint names are bare identifiers. Maximum depth is 64. Duplicate names
shadow older names. RELEASE removes the named savepoint and every newer one;
ROLLBACK TO restores and retains the named savepoint and drops newer ones.

## Current limits

| Boundary | Limit/default |
|---|---:|
| SQL source | 16 MiB |
| Parameters, native parser | 65,536 |
| Parameters, pgwire | 32,767 |
| Predicate/scalar depth | 64 |
| Subquery depth | 32 |
| Set nesting | 64 |
| Per clause/list, projection, or schema | 1,024 |
| Mutation transaction | Per table: 64 docs / 16 MiB values + keys; total: 16 tables / 256 docs / ~64 MiB |
| Default result | 100,000 rows / 64 MiB |
| Default intermediate / aggregate / join workspace | 64 MiB / 16 MiB / 64 MiB |
| Recursive CTE | 1,000 evaluations / 100,000 rows / 64 MiB |

## Unsupported SQL families

Unsupported families include MERGE, REPLACE, COPY, GRANT/REVOKE, COMMENT, VACUUM,
ANALYZE, REINDEX, CLUSTER, PREPARE/EXECUTE/DEALLOCATE, DECLARE/FETCH/MOVE,
LISTEN/NOTIFY/UNLISTEN, LOCK, CALL, and DO. PostgreSQL extensions, catalogs, procedures,
triggers, policies, sequences, materialized views, and replication are outside this dialect.

## Source map

- Parser and expressions: `sql/parser.go:19-76`, `sql/lexer.go:155-300`, `sql/scalar_parse.go:90-770`, `sql/predicate.go:503-745`
- SELECT and relations: `sql/parser.go:962-1209`, `sql/parser.go:2105-2455`, `sql/resolve.go:450-735`
- CTE/set/window: `sql/parse_cte.go:5-135`, `sql/recursive_cte.go:25-220`, `sql/set.go:314-873`, `sql/parser.go:1320-1954`
- DML and RF3 images: `sql/parse_dml.go:224-674`, `sql/driver/write.go:1095-1504`, `gateway/replicated_sql_transaction.go:128-565`
- DDL/views: `sql/parse_ddl.go:5-566`, `sql/parse_drop.go:31-187`, `sql/parse_view.go:5-118`
- Runtime transactions: `sql/driver/tx.go:89-258`, `sql/driver/savepoint.go:7-190`
