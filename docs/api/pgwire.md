# PostgreSQL wire adapter

[Documentation](../README.md) / [API guides](README.md) · [Stability](../status.md)

`pgwire` implements the PostgreSQL v3 **wire protocol** over a VibeDB SQL
catalog or RF3 gateway. It reports server version `16.0` for client handshake
compatibility, **not** PostgreSQL SQL, catalog, extension, or ORM parity
(`integration/pgcompat/approved-tests.txt` currently has zero approved upstream
regression tests).

## Start a server

Authentication is explicit (`Trust` is for loopback/Unix sockets only; use
`RequireTLS: true` + `SCRAM` for network listeners):

```go
database, err := driver.Open("app.vdb")
defer database.Close()

verifier, err := pgwire.NewVerifier("correct horse battery staple") // printable ASCII
server, err := pgwire.NewServer(database, pgwire.Options{
	Database:   "app",
	TLSConfig:  tlsConfig,
	RequireTLS: true,
	Auth: pgwire.SCRAM(func(user string) (pgwire.Verifier, bool) {
		return verifier, user == "app"
	}),
})
defer server.Close() // close server before database
```

For loopback testing with `pgwire.Trust()`:

```sh
psql 'postgresql://app@127.0.0.1:PORT/app?sslmode=disable' -c 'SELECT 1'
```

- Supports `SSLRequest` TLS upgrade and `SCRAM-SHA-256` (no direct TLS, MD5,
  GSS, `SCRAM-SHA-256-PLUS`, or SASLprep).
- `RequireTLS` applies to both normal startup and out-of-band `CancelRequest`.
- `NewServerWithBackend` attaches a custom `Backend` (such as the RF3 gateway)
  binding authenticated `SessionIdentity` to execution authority.

## Protocol and session surface

| Area | Behavior |
| --- | --- |
| Simple & extended query | Bounded multi-statement simple batches; `Parse`/`Bind`/`Describe`/`Execute`/`Close`/`Flush`/`Sync`; named/unnamed statements & portals; `$1`..`$32767` params (do not mix `$n` and `?`) |
| Embedded transactions | `BEGIN`/`COMMIT`/`ROLLBACK`, `SAVEPOINT`/`RELEASE`/`ROLLBACK TO`; Read Committed, Repeatable Read, Serializable; `READ ONLY`/`WRITE` |
| Timeouts & cancellation | `statement_timeout`, `idle_in_transaction_session_timeout`, out-of-band `CancelRequest`, `pg_cancel_backend(pid)`, `pg_terminate_backend(pid)` |
| Introspection virtual tables | Live `pg_stat_activity`, lock-free `pg_stat_statements`, and `pg_stat_statements_reset()` |
| Session settings (`SET`/`SHOW`/`RESET`/`DISCARD ALL`) | `search_path` (`public`), `application_name`, `client_encoding` (`UTF8`), `DateStyle`, `IntervalStyle`, `TimeZone`, `extra_float_digits`, `standard_conforming_strings` (`on`), `bytea_output`, `statement_timeout`, `idle_in_transaction_session_timeout` |
| Discovery shims | Exact captured queries for `psql` 18.4 (`\l`, `\dn`, `\dt`, `\di`, `\d`, `\df`, `\du`, `\dv`), GoLand, and JDBC |

- **RF3 gateway backend:** Autocommit-only for writes (`INSERT VALUES`,
  `ON CONFLICT DO NOTHING/UPDATE`, `DELETE`, exact-PK `UPDATE` without
  `RETURNING`). Explicit multi-statement write transactions are rejected.
- **Errors & SQLSTATEs:** `42601` syntax, `0A000` unsupported, `23505` unique,
  `40001` serialization, `54000` resource cap, `57014` cancellation/timeout,
  `08P01` protocol violation, `40003` unknown outcome (reconnect and reconcile;
  never blind-retry).

## Parameters and results

- **Accepted parameter OIDs:** `bool`, `int2`/`int4`/`int8`, `float4`/`float8`,
  text `numeric` (no binary `numeric`), `text`/`varchar`/`bpchar`/`name`/`unknown`,
  `bytea` (document slot only), `json`/`jsonb` (binary v1). Text numbers bind
  as exact JSON decimals; strings must be valid UTF-8 without NUL.
- **Result OIDs:** Schemaless values, exact numbers, and `SUM`/`AVG`/`MIN`/`MAX`
  return JSON OID 114; `COUNT` and integer windows return `int8`; typed
  bool/text columns return native OIDs. Both missing and JSON `null` encode as
  wire NULL (`-1` length). Complete results are preflighted before sending the
  first `DataRow`.

## Default bounds

| Resource | Default / hard bound |
| --- | ---: |
| Connections | 128 (`-1` disables) |
| Startup / write / idle timeouts | 10 s / 30 s / 5 min |
| Result / intermediates | 100,000 rows & 64 MiB / 64 MiB |
| Message body / DataRow / RowDescription | 16 MiB each |
| Named statements / portals / simple batch | 1,024 each |
| Result columns / parameters | 1,024 / 32,767 |
| Startup packet / retained read buf / error field | 10,000 B / 64 KiB / 4 KiB |

## Limitations

- Not PostgreSQL: no `COPY`, logical replication, `LISTEN`/`NOTIFY`, large
  objects, extensions, materialized views, holdable cursors, or general
  `pg_catalog`/`information_schema` queries.
- Discovery shims match exact captured client shapes; near misses fail.
- `RowDescription` reports zero table OID and attribute number.
- Tested in CI with `pgx` v5, `lib/pq`, `psql` 18.4, and JDBC 42.7.3.

## Source map

- Server, startup, TLS, SCRAM: [pgwire/server.go](../../pgwire/server.go), [pgwire/session.go](../../pgwire/session.go), [pgwire/scram.go](../../pgwire/scram.go)
- Extended protocol, rows, errors: [pgwire/extended.go](../../pgwire/extended.go), [pgwire/rows.go](../../pgwire/rows.go), [pgwire/pgerror.go](../../pgwire/pgerror.go)
- Commands, shims, `pg_stat_*`: [pgwire/command.go](../../pgwire/command.go), [pgwire/catalog_shim.go](../../pgwire/catalog_shim.go)
- Gateway RF3 writes & tests: [gateway/pgwire_write.go](../../gateway/pgwire_write.go), [integration/pgclient/pgclient_test.go](../../integration/pgclient/pgclient_test.go)
