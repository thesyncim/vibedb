# JDBC and GoLand development probes

[Client guide](../../docs/api/pgwire.md)

> **Development status:** VibeDB's pgwire, catalog emulation, and SQL surface are
> incomplete and may change or break at any commit. These probes target a
> disposable development database. They are not production, release, or driver
> certification gates.

The Java programs in this directory are manual external-client probes. Normal
`go test` does not compile or run them, and current CI does not install Java or
the JDBC driver.

## Requirements

- a running VibeDB development pgwire endpoint with the needed tables;
- Java 17 or newer; and
- PostgreSQL JDBC 42.7.3, supplied as `PGJDBC_JAR`.

The repository does not vendor, download, hash, or assert the 42.7.3 JAR. The
program prints the connected driver name and version; selecting the requested
JAR is the operator's responsibility.

The examples below use loopback, `sslmode=disable`, and the development user
`local`. Do not reuse these connection settings outside a disposable local
cluster.

## Probe discovery and data-grid behavior

From the repository root:

```bash
java --class-path "$PGJDBC_JAR" integration/jdbc/Discovery.java \
  'jdbc:postgresql://127.0.0.1:7432/vibedb?sslmode=disable&user=local&prepareThreshold=1&connectTimeout=5&socketTimeout=10'
```

The default probe checks connection and transaction metadata, the exposed
database and `public` schema, selected GoLand discovery queries, search-path
handling, a data-grid query, whole-document access, missing-field `NULL`, and
`->>` text extraction. Several generic metadata calls are drainability checks;
they do not assert full PostgreSQL metadata semantics.

Enable an isolated write cycle only on disposable data:

```bash
java --class-path "$PGJDBC_JAR" integration/jdbc/Discovery.java \
  'jdbc:postgresql://127.0.0.1:7432/vibedb?sslmode=disable&user=local&prepareThreshold=1&connectTimeout=5&socketTimeout=10' \
  --writes --write-cycles=48
```

`--writes` uses a UUID-prefixed key and checks prepared INSERT, whole-document
UPDATE, read-after-write, DELETE, and final absence. `--write-cycles` accepts
1–128 and stops after the first failed cycle. The program does not blindly retry
an outcome-unknown mutation. It aggregates check failures and exits nonzero.

The probe does not establish arbitrary PostgreSQL functions, custom schemas,
role grants, MVCC catalogs, or field-level generated UPDATE support. PgJDBC's
synthesized `xmin` result is client behavior, not a VibeDB server feature.

## Verify the employees fixture

`Employees.java` is mutating even without a fixture: it always executes
`CREATE TABLE IF NOT EXISTS public.employees` and checks the six declared
columns.

Seed an empty table and verify 1,000 rows plus a prepared six-column city filter:

```bash
java --class-path "$PGJDBC_JAR" integration/jdbc/Employees.java \
  'jdbc:postgresql://127.0.0.1:7432/vibedb?sslmode=disable&user=local&socketTimeout=90' \
  docs/examples/employees-1000.sql
```

The program recognizes only the fixture's `INSERT INTO employees` statements;
it is not a general SQL-file runner. The fixture contains sixteen 64-row batches.
If the table already has 1,000 rows, seeding is skipped and verification runs.

### Resume a confirmed prefix

A partially seeded table is refused by default. Resume only after independently
establishing that the previous mutation outcome is settled. The existing row
count must be a multiple of 64, the rows must be the exact canonical prefix, and
the third argument must match that count:

```bash
java --class-path "$PGJDBC_JAR" integration/jdbc/Employees.java \
  'jdbc:postgresql://127.0.0.1:7432/vibedb?sslmode=disable&user=local&socketTimeout=90' \
  docs/examples/employees-1000.sql \
  --resume-confirmed-prefix=64
```

The program verifies `employee-0001` through the confirmed prefix before
skipping completed batches. Never guess the count or use this option to retry an
unknown write automatically.

Omit the fixture path to skip seeding. The program still creates/checks the table
and runs the prepared city-filter query against its current contents.

## Automated Go coverage is separate

These Go tests exercise captured catalog SQL and VibeDB's pgwire/gateway
implementations. They do not load PGJDBC, start GoLand, or run either Java file:

```bash
go test ./pgwire -run 'GoLand|Discovery|PublicNamespace|CatalogShim' -count=1
go test ./gateway -run 'PostgreSQL|RF3SQL' -count=1
```

For the broader race-enabled pgwire suite:

```bash
go test -race ./pgwire -count=1
```

Passing captured-query tests does not prove that a particular installed GoLand
or PGJDBC build works. Run the Java probe against the actual driver and endpoint
when that external-client behavior matters.

For manual UI inspection, refresh the GoLand data source and inspect
`vibedb / public / tables`. Use explicit whole-document SQL for writes; a grid
may generate unsupported field-level updates.
