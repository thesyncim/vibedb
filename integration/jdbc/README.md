# GoLand / PostgreSQL JDBC gate

Requires a running local RF3 development cluster with the seeded `documents`
table, Java 17+, and PostgreSQL JDBC **42.7.3**. No Java dependency is added to
the Go server. Use the same driver JAR selected in GoLand's data-source dialog.

```sh
java --class-path "$PGJDBC_JAR" integration/jdbc/Discovery.java \
  'jdbc:postgresql://127.0.0.1:7432/vibedb?sslmode=disable&user=local&prepareThreshold=1&connectTimeout=5&socketTimeout=10'
```

Add `--writes` to validate prepared public-qualified INSERT, whole-document
UPDATE, read-after-write, DELETE, and absence. It creates a UUID-prefixed test
key and deletes only that key in `finally`; existing records are not changed.
The process exits unsuccessfully on any failed check. Use only a disposable
development database, not a production connection.

The probe checks driver metadata, GoLand's search-path array, its data-grid
SELECT, `SELECT id, documents."$doc" FROM documents`, missing-field NULLs, and
`->>` text metadata/values with a prepared filter. The write gate also checks
text extraction after UPDATE.
PgJDBC's own synthesized `xmin` row is exercised but is not a server feature.
Only the current database and `public` are exposed. Arbitrary PostgreSQL
functions, custom schemas, role grants and MVCC catalogs are not implemented.

Verify the declared employees table through the same real driver:

```sh
java --class-path "$PGJDBC_JAR" integration/jdbc/Employees.java \
  'jdbc:postgresql://127.0.0.1:7432/vibedb?sslmode=disable&user=local&socketTimeout=90' \
  docs/examples/employees-1000.sql
```

This runs CREATE TABLE IF NOT EXISTS, checks declared columns, executes the
fixture's sixteen INSERTs only when the table is empty, then verifies 1,000 rows
and a prepared six-column city filter. Omit the file argument to skip seeding.
A partially seeded table is deliberately refused; do not automatically resubmit
an INSERT whose outcome is unknown.

The mandatory Go regressions replay the installed GoLand **2026.2**, PostgreSQL
16-dialect templates in `pgwire/catalog_goland*_queries.json`, plus captured
JDBC SQL in `catalog_jdbc_queries.json`. SQL originates in the installed IDE's
`PgIntroQueries.sql` and auxiliary ACL/search-path requests; version branches
are resolved for the advertised dialect. Markers capture only schema IDs,
relation selections, JDBC LIKE filters, and refresh hints. Every other SQL
token must match. Empty relation selections (`IN (NULL)`) return no objects.

```sh
go test ./pgwire -run 'GoLand|Discovery|PublicNamespace|CatalogShim' -count=1
go test -race ./pgwire -count=1
go test ./gateway -run 'PostgreSQL|RF3SQL' -count=1
```

For UI validation, refresh the data source in GoLand, expand
`vibedb / public / tables / documents`, and verify `id`, `$doc`, the primary
key and its index. Open the table, then run the explicit key/document SELECT
in an Auto-mode console. The table grid may generate unsupported field UPDATEs;
the console's documented whole-document DML is the supported write contract.
