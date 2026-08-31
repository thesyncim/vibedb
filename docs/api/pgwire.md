# PostgreSQL wire adapter

> [!CAUTION]
> Experimental, unreleased software: the wire protocol, SQL, catalog shims,
> types, and limits may break on any commit. Pin and test the exact commit used
> by every client.

`pgwire` is a PostgreSQL v3 **client-protocol adapter** over one VibeDB SQL
catalog. It is **not PostgreSQL compatibility and not ORM compatibility**. The
reported server version `16.0` helps clients choose a protocol-era handshake;
it makes no claim about PostgreSQL 16 SQL, catalogs, behavior, or extensions.

The current PostgreSQL regression ratchet has **zero approved tests**:
`integration/pgcompat/approved-tests.txt` contains no test names. Do not turn
an observational smoke run into a compatibility claim.

## Start a loopback endpoint

Authentication must be chosen explicitly. `Trust` is appropriate only inside
a trust boundary such as loopback or a protected Unix socket:

```go
package main

import (
	"errors"
	"fmt"
	"log"
	"net"

	"github.com/thesyncim/vibedb/pgwire"
	"github.com/thesyncim/vibedb/sql/driver"
)

func main() {
	database, err := driver.Open("app.vdb")
	if err != nil {
		log.Fatal(err)
	}
	defer database.Close()

	server, err := pgwire.NewServer(database, pgwire.Options{
		Auth:     pgwire.Trust(),
		Database: "app",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer server.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("postgres://app@127.0.0.1:" +
		fmt.Sprint(listener.Addr().(*net.TCPAddr).Port) + "/app?sslmode=disable")
	if err := server.Serve(listener); err != nil && !errors.Is(err, pgwire.ErrServerClosed) {
		log.Fatal(err)
	}
}
```

`NewServer` borrows the database. Close the server first; `Server.Close` stops
listeners, cancels sessions, waits for them, and then returns. Close the
database afterward.

## Require TLS and SCRAM

For a network listener, provide a certificate, require TLS, and derive the
in-memory verifier used by the SCRAM lookup:

```go
verifier, err := pgwire.NewVerifier("correct horse battery staple")
if err != nil {
	return err
}
server, err := pgwire.NewServer(database, pgwire.Options{
	Database:   "app",
	TLSConfig:  tlsConfig, // certificate or certificate callback required
	RequireTLS: true,
	Auth: pgwire.SCRAM(func(user string) (pgwire.Verifier, bool) {
		return verifier, user == "app"
	}),
})
if err != nil {
	return err
}
defer server.Close()
```

The public API does not yet serialize or reload `Verifier`; this example is
startup configuration, not a persistent credential-store design.

The adapter supports PostgreSQL SSLRequest negotiation and SCRAM-SHA-256.
Direct TLS, GSS encryption, MD5 authentication, SCRAM-SHA-256-PLUS channel
binding, authorization identities, and SASLprep are not implemented. Because
SASLprep is absent, `NewVerifier` accepts only nonempty printable-ASCII
passwords.

`RequireTLS` covers normal startup and out-of-band CancelRequest packets. A
cancel request arrives on its own connection and receives no response, as the
protocol requires.

`NewServerWithBackend` borrows a custom `Backend`; it receives authenticated `SessionIdentity` and must bind it to execution authority.
The server closes returned sessions. Autocommit-only writes must each stand alone.

## Protocol surface

| Area | Implemented behavior |
|---|---|
| Startup | Protocol 3; `user` required; nonempty configured database enforces its label, empty accepts any label |
| Simple query | Bounded multi-statement batches; stop at first error |
| Extended query | Parse, Bind, Describe, Execute, Close, Flush, Sync |
| Prepared state | Named/unnamed statements and portals, portal suspension |
| Transactions | BEGIN/START, COMMIT, ROLLBACK, SAVEPOINT, RELEASE, ROLLBACK TO |
| Isolation | Read Committed, Repeatable Read, Serializable; READ ONLY/WRITE |
| Cancellation | BackendKeyData plus out-of-band CancelRequest |
| Parameters | Text plus selected binary encodings |
| Results | Text plus selected binary encodings, preflighted before first DataRow |
| Discovery | Exact bounded shims for tested psql, GoLand, and JDBC probes |

Simple-query placeholders are rejected. Extended-query clients use `$1`
through `$32767`; repeated and out-of-order references work, and gaps count
toward the parameter count. Mixing `$n` and `?` is rejected. Rewriting ignores
quoted strings, identifiers, and comments.

On the embedded transactional backend, an idle batch of stored SQL is
preflighted and runs atomically in one implicit transaction. Explicit
transaction/session commands differ. DDL must be the only statement in a batch.

After an extended-protocol error, messages are discarded until Sync. Sync
commits or rolls back implicit work, closes non-holdable portals, and resets
the cycle. Only one live executor is retained; executing another suspended
portal invalidates the earlier live executor.

Extended-protocol DDL must be the sole execution. It publishes at Execute,
outside an implicit DML overlay; a later protocol error before Sync cannot undo it.

## SQL and session commands

The adapter executes the bounded VibeDB [SQL reference](../reference/sql.md),
not PostgreSQL SQL. It additionally parses transaction commands and a small
session-command surface:

- `SET`, `RESET`, `SHOW`, and `DISCARD ALL` for the fixed settings below.
- Fixed handshake SELECT probes used by supported clients.
- Exact psql 18.4 metadata query shapes behind `\l`, `\dn`, `\dt`, `\di`,
  `\d`, `\d name`, `\df`, `\du`, and `\dv`.

Supported settings are `search_path` (`public` only), `application_name`,
`client_encoding` (`UTF8`), `DateStyle`, `IntervalStyle`, `TimeZone`,
`extra_float_digits`, `standard_conforming_strings` (`on`), and
`bytea_output`. Role changes, statement/lock timeouts, and default transaction
settings are refused. SET and RESET are refused inside a transaction.

In a failed transaction, only COMMIT, ROLLBACK, and ROLLBACK TO may proceed.
COMMIT on an aborted transaction rolls it back and reports command tag
`ROLLBACK`. DDL is still prohibited inside transactions.

Errors carry mapped PostgreSQL SQLSTATEs and character positions. Unknown
commit outcome is `40003`, outranks cancellation, and requires reconnect/reconcile—not blind retry.

## Parameter types

Accepted PostgreSQL parameter OIDs:

| Family | OIDs |
|---|---|
| Boolean | `bool` |
| Integer | `int2`, `int4`, `int8` |
| Floating point | `float4`, `float8` |
| Exact number | `numeric` text input; binary numeric is unsupported |
| Text | `text`, `varchar`, `bpchar`, `name`, `unknown` |
| Whole document bytes | `bytea` only in a document slot |
| JSON | `json`, `jsonb` (binary jsonb version 1) |

Whole-document parameters default to JSON. SQL scalar parameters infer a
bounded bool/text domain where context requires it. A whole-document SQL NULL
is rejected; send JSON text `null` to store a JSON null document. Untyped text
uses JSON-scalar spelling inference, so `21` may bind as a number; declare text
when text is required. Scalar JSON objects and arrays are rejected in scalar
positions. Nonfinite floats are rejected.

Input strings must be valid UTF-8 and contain no NUL. Text-format `name` clips
at 63 bytes; overlength binary `name` is `42622`. Direct `bpchar` preserves
padding; coercion to a different string target trims it.

## Result types

Schemaless values, exact numbers, and SUM/AVG/MIN/MAX are sent as JSON OID 114.
Strings in those slots are JSON-quoted. COUNT and integer-valued window
functions use `int8`; statically typed bool/text/varchar/name/bpchar outputs use
their native OIDs. SQL NULL uses a `-1` length.

Missing and explicit JSON null both encode as wire NULL. Use `IS MISSING` in
the SQL predicate when absence must remain distinct. RowDescription currently
reports table OID and attribute number as zero.

The complete result is preflighted before the first DataRow is written. An
encoding or budget error therefore does not publish a partial result.

## Catalog discovery is a shim

There is no general `pg_catalog` or `information_schema` implementation. The
adapter recognizes exact query shapes captured from supported client paths.
Near misses execute as ordinary SQL and usually fail.

The psql shim reports tables and indexes in synthetic schema `public`; function,
role, and view listings are empty. That omission does not mean VibeDB lacks its
own read-only SQL views. GoLand/JDBC discovery exposes declared columns,
primary keys, and `"$doc"`; it never infers schema by scanning documents.

Repository interoperability gates cover pgx v5 and lib/pq over real loopback
TCP and SCRAM. TLS+SCRAM has a pgx gate. Stock psql 18.4 and Java 17 with JDBC
42.7.3 are opt-in integration checks, not broad support guarantees.

## Resource bounds

| Resource | Default or hard bound |
|---|---:|
| Connections | 128 |
| Startup/auth read timeout | 10 s |
| Socket write timeout | 30 s |
| Idle message timeout | 5 min |
| Result | 100,000 rows / 64 MiB |
| Relation intermediates | 64 MiB |
| Frontend message body | 16 MiB |
| Startup packet | 10,000 bytes |
| Retained read buffer | 64 KiB |
| Statement/portal identifier | 256 bytes |
| Simple statements per query | 1,024 |
| Named statements / portals | 1,024 each |
| Result columns | 1,024 |
| Parameters | 32,767 |
| Retained prepared input / binds / portals | 16 MiB each |
| DataRow / RowDescription | 16 MiB each |
| Error field | 4 KiB |

Zero-valued options select finite defaults. `-1` disables supported connection,
result, intermediate, or timeout limits; other execution limits still apply.

## Explicitly unsupported

Do not expect COPY, replication, LISTEN/NOTIFY, logical decoding, large objects,
PostgreSQL extensions or functions, materialized views, general catalog queries,
direct TLS, holdable cursors, or arbitrary ORM startup/discovery traffic.
Recognized unsupported SQL returns feature-not-supported; unknown frontend
messages are protocol violations.

## Source map

- Contract and compatibility boundary: `pgwire/doc.go:1-39`, `pgwire/server.go:40-51`
- Server lifecycle/options: `pgwire/server.go:54-155`, `pgwire/server.go:183-400`
- Startup/TLS/auth: `pgwire/session.go:438-744`, `pgwire/scram.go:18-70`, `pgwire/scram.go:96-528`
- Simple/extended state machines: `pgwire/session.go:831-1102`, `pgwire/extended.go:53-394`
- Parameters/results/errors: `pgwire/extended.go:810-1491`, `pgwire/rows.go:12-251`, `pgwire/pgerror.go:291-581`
- Commands and numbered parameters: `pgwire/command.go:192-374`, `pgwire/command.go:954-1670`
- Discovery and integration gates: `pgwire/catalog_shim.go:12-145`, `integration/pgclient/pgclient_test.go:35-369`
- Zero-test compatibility ratchet: `integration/pgcompat/approved-tests.txt:1-3`
