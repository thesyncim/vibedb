# PostgreSQL wire server

The `pgwire` package implements PostgreSQL frontend/backend protocol version
3.0 for the VibeDB SQL runtime. The endpoint is experimental. Protocol support
does not imply PostgreSQL SQL compatibility.

## Start a SCRAM server

```go
package main

import (
	"errors"
	"log"
	"net"

	"github.com/thesyncim/vibedb/pgwire"
	vibedriver "github.com/thesyncim/vibedb/sql/driver"
)

func main() {
	db, err := vibedriver.Open("app.vdb")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	verifier, err := pgwire.NewVerifier("change-this-password")
	if err != nil {
		log.Fatal(err)
	}

	server, err := pgwire.NewServer(db, pgwire.Options{
		Auth: pgwire.SCRAM(func(user string) (pgwire.Verifier, bool) {
			return verifier, user == "app"
		}),
		Database: "app",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer server.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:5432")
	if err != nil {
		log.Fatal(err)
	}

	if err := server.Serve(listener); !errors.Is(err, pgwire.ErrServerClosed) {
		log.Fatal(err)
	}
}
```

`Options.Auth` is required. Select SCRAM for a network service. `Trust()` is
only suitable for a trusted local development boundary.

Configure `TLSConfig` to enable TLS. Set `RequireTLS` only with a nonnil TLS
configuration.

## Protocol features

The server supports:

- Simple Query
- Extended `Parse`, `Bind`, `Describe`, `Execute`, `Close`, `Flush`, and `Sync`
- Named prepared statements and portals
- Portal suspension
- CancelRequest
- Optional TLS and required TLS
- SCRAM-SHA-256

An extended-protocol error discards messages until `Sync`. Transaction status
uses idle, in-transaction, and failed-transaction states.

When the session is idle, the first extended stored-row statement starts an
implicit transaction that finalizes at `Sync`. In an explicit transaction,
`Sync` only resynchronizes the protocol. It does not commit. DDL must be the
sole execution in an extended cycle. DDL publishes when `Execute` completes. A
later protocol error cannot roll it back.

The server does not support COPY, SQL cursors, notifications, procedures,
general server-side SQL `PREPARE`, or SCRAM-PLUS.

## Placeholders

Extended SQL accepts `$1`, `$2`, and later numbered placeholders. It also
accepts `?`. Do not mix the two forms in one statement.

Repeated and out-of-order `$n` placeholders are valid. Simple Query cannot bind
placeholders.

## Authentication boundary

SCRAM uses SCRAM-SHA-256 with 4096 PBKDF2 iterations. It does not implement
SCRAM-PLUS channel binding or SASLprep. Passwords are restricted to printable
ASCII.

The authentication callback receives the requested user and returns a stored
verifier. Unknown users use a mock verifier to reduce user-enumeration leakage.

## Simple-query batches

One Simple Query message can contain at most 1024 statements. The splitter
handles SQL literals and comments.

Stored-data SQL in an idle batch runs in one implicit transaction. The batch
stops on the first error and rolls back stored-data changes.

DDL must be the only nonempty statement. Stored SQL cannot be mixed freely
with session-setting commands.

The `database/sql` driver has a different rule. It accepts one statement per
call.

## Defaults and hard limits

| Limit | Default or maximum |
| --- | ---: |
| Connections | 128 default |
| Startup and authentication read timeout | 10 s default |
| Socket write timeout | 30 s default |
| Idle read timeout | 5 min default |
| Result rows | 100,000 default |
| Result bytes | 64 MiB default |
| Intermediate bytes | 64 MiB default |
| Message body | 16 MiB maximum |
| Startup packet | 10,000 bytes maximum |
| Statement or portal name | 256 bytes maximum |
| Prepared statements per session | 1024 maximum |
| Portals per session | 1024 maximum |
| Bind parameters | 32,767 maximum |
| Result columns | 1024 maximum |
| Error field | 4 KiB maximum |
| Prepared input and plan metadata | 16 MiB aggregate maximum |
| Prepared bind high-water storage | 16 MiB aggregate maximum |
| Portal storage | 16 MiB aggregate maximum |
| DataRow message | 16 MiB maximum |
| RowDescription message | 16 MiB maximum |

Program and query budget errors map to SQLSTATE `54000`. They do not return a
partial materialized result.

## SQLSTATE mapping

| Condition | SQLSTATE |
| --- | --- |
| Syntax error | `42601` |
| Unsupported feature | `0A000` |
| Missing table | `42P01` |
| Unique violation | `23505` |
| Check violation | `23514` |
| Serialization failure | `40001` |
| Unknown commit outcome | `40003` |
| Failed transaction | `25P02` |
| Read-only transaction | `25006` |
| Cardinality violation | `21000` |
| Query canceled | `57014` |
| Program or resource limit | `54000` |

Error positions use 1-based character positions.

## Result metadata

SQL results use JSON OID 114, int8 OID 20, boolean OID 16, or text OID 25.
Schemaless fields and numeric scalar results generally use JSON metadata.
`COUNT` and integer-valued window functions use int8. Boolean and text casts
use their native OIDs.

The Bind path accepts more input OIDs, including numeric and JSONB. It
normalizes those parameters into the VibeDB value model. The server does not
emit numeric or JSONB result OIDs.

A missing path and an explicit JSON null both become wire NULL. Use `IS
MISSING` in the query when the distinction is required.

Do not assume arbitrary native PostgreSQL typing for a schemaless cell.

## Client compatibility shims

The endpoint answers a small fixed set of handshake probes, including
`SELECT 1`, `version()`, `current_database()`, and `current_user`.

It reports server version `16.0` for client compatibility. This string is not a
claim of PostgreSQL 16 SQL compatibility.

The catalog shim recognizes exact PostgreSQL 18.4 `psql` query templates for
commands such as `\l`, `\dn`, `\dt`, `\di`, `\d`, `\dv`, `\df`, and `\du`.
Near matches are not a supported general catalog API.

Recognition does not mean full catalog exposure. `\dv`, `\df`, and `\du`
return empty results. The current shim does not expose VibeDB SQL views through
`\dv`. Bare `\d` lists tables only.

No claim exists for other `psql` versions, JDBC, an ORM, or general
`pg_catalog` queries.

## Session settings

The server accepts a limited compatibility set. It includes
`application_name`, UTF-8 client encoding, `DateStyle`, `TimeZone`,
`extra_float_digits`, and `standard_conforming_strings`.

It supports related `SHOW`, `RESET`, and `DISCARD ALL` operations. Unsupported
settings such as `search_path` and `statement_timeout` return `0A000`.

## Implementation references

- `pgwire/server.go`, `proto.go`, and `extended.go`
- `pgwire/session.go`, `command.go`, and `pgerror.go`
- `pgwire/scram.go` and `tls_test.go`
- `pgwire/catalog_shim.go`
- `integration/pgclient/pgclient_test.go` and `psql_test.go`
