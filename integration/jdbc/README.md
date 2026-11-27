# JDBC and GoLand development probes

[Client guide](../../docs/api/pgwire.md)

Manual Java 17+ probes against a disposable local `pgwire` endpoint using
PostgreSQL JDBC 42.7.3 (`PGJDBC_JAR`). Not run by `go test` or CI.

## Discovery and write-cycle probe

```sh
java --class-path "$PGJDBC_JAR" integration/jdbc/Discovery.java \
  'jdbc:postgresql://127.0.0.1:7432/vibedb?sslmode=disable&user=local&prepareThreshold=1&connectTimeout=5&socketTimeout=10'

# Optional disposable write cycle (1..128):
java --class-path "$PGJDBC_JAR" integration/jdbc/Discovery.java \
  'jdbc:postgresql://127.0.0.1:7432/vibedb?sslmode=disable&user=local&prepareThreshold=1&connectTimeout=5&socketTimeout=10' \
  --writes --write-cycles=48
```

Verifies connection metadata, `public` schema discovery, GoLand queries,
`"$doc"`, missing-field NULL, `->>`, and (with `--writes`) prepared
INSERT/UPDATE/DELETE cycles.

## Employees 1,000-row fixture probe

```sh
java --class-path "$PGJDBC_JAR" integration/jdbc/Employees.java \
  'jdbc:postgresql://127.0.0.1:7432/vibedb?sslmode=disable&user=local&socketTimeout=90' \
  docs/examples/employees-1000.sql
```

Seeds sixteen 64-row batches into `public.employees` (skipped if 1,000 rows
already exist) and verifies a prepared 6-column city filter. Pass
`--resume-confirmed-prefix=<multiple-of-64>` only after independently
confirming a partial prefix.

## Related Go tests

```sh
go test ./pgwire -run 'GoLand|Discovery|PublicNamespace|CatalogShim' -count=1
go test -race ./pgwire -count=1
```
