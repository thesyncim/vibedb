// Package pgwire serves vibejson's SQL surface over the PostgreSQL frontend
// and backend protocol, version 3.0, implemented from the specification.
//
//	db := store.NewDatabase()      // or a durable collection
//	srv := pgwire.NewServer(pgwire.FromDatabase(db), pgwire.Options{})
//	ln, _ := net.Listen("tcp", "127.0.0.1:5433")
//	go srv.Serve(ln)
//
// A client then connects with any PostgreSQL driver and issues SELECT
// statements in the dialect [github.com/thesyncim/vibedb/sql] parses. It is a
// second front door onto work that already exists: sql parses, query lowers to
// a compiled plan, and the executor runs it. Nothing here re-implements any of
// that, and the database/sql driver in
// [github.com/thesyncim/vibedb/vibesql] is the same path with a different
// door.
//
// # Who this is for, and who it is not for
//
// This server targets programmatic clients: a Go, Python, Node, or Rust driver
// issuing SQL this engine supports. pgx v5 and lib/pq are driven against it and
// work — see "Verified against real drivers" below. psycopg and node-postgres
// are not tested here and are expected to work for the same reasons, which is
// an expectation and not a measurement.
//
// It does not target psql or BI tools, and that is a design decision rather
// than an unfinished feature. Those tools do not primarily issue SQL you wrote;
// they issue SQL they generate against pg_catalog. A single psql "\d users"
// expands to a query that joins pg_class to pg_namespace with a LEFT JOIN,
// filters with a subquery, casts with ::regclass, calls scalar functions like
// pg_table_is_visible and format_type, and tests membership with ANY over an
// array — every one of which this dialect refuses on purpose, because the
// executor has no operator for any of them. The same is true of JDBC's
// DatabaseMetaData, of every ORM's schema reflection, and of every BI tool's
// table browser.
//
// The refusal is checked rather than asserted: TestCatalogQueriesAreRefused
// feeds real psql, JDBC, and information_schema probes to the parser and logs
// where each stops. In practice the parser rejects them earlier than the list
// above suggests — at "pg_catalog.pg_class", because a schema-qualified
// relation name is indistinguishable from this dialect's dotted path syntax and
// is refused rather than guessed at. Supporting these queries would mean
// building a subquery-capable, outer-join-capable SQL engine with a schema
// namespace, to answer questions about a catalog that does not exist. That is a
// much larger project than this one and a different one.
//
// # What works
//
//   - Startup, including SSLRequest and GSSENCRequest (both refused in the
//     clear), protocol version negotiation, and the parameter report.
//   - Authentication: trust, or SCRAM-SHA-256. See "Authentication" below.
//   - The simple query protocol, including several statements in one message.
//   - The extended query protocol: Parse, Bind, Describe, Execute, Close,
//     Flush, Sync, with named and unnamed statements and portals, row limits
//     and PortalSuspended, and the error state that discards messages until
//     Sync.
//   - Text and binary result formats, for every column type this server emits.
//   - Errors as ErrorResponse with real SQLSTATE codes and, for a parse
//     failure, a character position.
//   - SET, RESET, SHOW, DISCARD, and a small fixed set of niladic expressions
//     (version(), current_database(), current_schema(), current_user, a literal
//     SELECT) so a driver's connect sequence and health check succeed.
//   - $1-style numbered parameters, which is what every client library writes,
//     including out-of-order and repeated ones. They are rewritten to this
//     dialect's '?' in front of the parser and their values are permuted at
//     Bind; command.go explains why the rewrite lives here and not in the
//     parser. Byte offsets are preserved by the rewrite, so an error's position
//     still points into the statement the client sent.
//
// # What does not work
//
//   - Transactions. BEGIN, COMMIT, and ROLLBACK are refused with 0A000 rather
//     than accepted as no-ops, and ReadyForQuery therefore always reports 'I'.
//     A no-op BEGIN would tell a client it had a transaction when it did not,
//     which is worse than an error in both directions: a caller wrapping reads
//     would believe in a consistency this cannot give across statements, and a
//     caller wrapping writes would find there are no writes at all.
//   - Writes of any kind. INSERT, UPDATE, DELETE, and every form of DDL are
//     refused with 0A000. The store is written by the application that owns it.
//   - pg_catalog and information_schema. There are no catalog tables; see
//     above. psql's backslash commands, JDBC's DatabaseMetaData, and ORM schema
//     reflection all fail as a result.
//   - COPY, the function-call subprotocol, LISTEN/NOTIFY, cursors (DECLARE and
//     FETCH), SQL-level PREPARE/EXECUTE, and replication.
//   - TLS. SSLRequest is answered 'N'. Put this behind a unix socket, a
//     loopback bind, or a TLS-terminating proxy.
//   - Query cancellation of a running scan. See "Cancellation" below.
//   - The SQL constructs the dialect itself refuses — subqueries, outer joins,
//     LIKE, CASE, CAST, arithmetic, DISTINCT, set operations, window functions,
//     and scalar functions. Each is refused by the parser with a message naming
//     the missing capability and a position;
//     [github.com/thesyncim/vibedb/sql] documents the full list and why.
//
// # Result types
//
// Every projected column is declared json (OID 114) and every COUNT is declared
// int8 (OID 20). The store is schemaless — one path holds a number in one
// document and a string in the next — and json is the only PostgreSQL type
// whose domain contains every value a cell can hold. It also has the property
// that decides the question: json's binary wire format is byte-for-byte its
// text format, so binary is supported for every column without any possibility
// of the subtly-wrong binary encoding that silently corrupts a client's values.
// rows.go carries the full argument, including why text, numeric, and jsonb
// were each rejected.
//
// Two consequences a caller should know before writing against this:
//
// A string comes back JSON-quoted. SELECT name yields "alice" with the quotes,
// because that is what distinguishes it from the number in the next row. What a
// driver does with that depends on the destination, and it is worth knowing
// which is which. Measured against pgx v5: scanning into an any gives the
// decoded Go value (a string, a float64), scanning into an int64 or a struct
// unmarshals into it, and scanning into a string gives the raw JSON text —
// quotes included. lib/pq, which has no json codec, always gives the raw bytes.
//
// A NULL means "null or absent" and does not say which. This engine defines an
// absent path and an explicit JSON null as one value, and the protocol has one
// NULL. Ask for the distinction in the statement, with this dialect's
// IS [NOT] MISSING.
//
// Numbers keep their exact decimal spelling in both directions. A projected
// number is sent as the document's own digits and nothing passes through
// float64, so a 19-digit identifier survives; a bound parameter that spells a
// JSON number is bound as an exact decimal literal. The one place float64 does
// intrude is inside the engine, for a computed non-integer aggregate, and that
// is noted where it happens.
//
// # Verified against real drivers
//
// pgx v5 and lib/pq are driven against this server outside the repository, so
// neither reaches the root module's dependency set. Both connect (pgx over
// SCRAM-SHA-256), prepare, bind, describe, execute, batch, and read rows.
// All five of pgx's query execution modes work, including the simple-protocol
// one. lib/pq reports the column types as JSON and INT8 through
// database/sql's ColumnType, and its Begin fails with 0A000 as intended.
//
// One measured result deserves repeating because it is the type mapping's real
// cost. A 19-digit integer arrives on the wire as its exact digits, and pgx
// scanning it into a string returns those digits; pgx scanning the same column
// into an any returns 9.007199254740992e+15, because encoding/json decodes a
// bare number into a float64. The wire is exact and the client's decoder is
// not, and the fix on every client is to read the column as text or as a
// deferred JSON value.
//
// # Bound parameters
//
// A placeholder in this dialect has no type — it compares against whatever the
// path holds — so ParameterDescription reports every parameter as unspecified,
// which is what makes most clients send parameters in text format with no
// declared OID. Such a parameter is read as a JSON scalar: 21 binds as the
// number 21, true as a boolean, null as SQL NULL, and anything else as a
// string.
//
// The inference has one edge, and it is worth knowing rather than discovering:
// binding the *string* "21" against a string-valued path matches nothing,
// because the text on the wire is the same as the number's. Declare the
// parameter's OID in Parse (pgx does this when it knows the Go type) or bind in
// binary with a declared OID, and the ambiguity is gone. extended.go explains
// why the opposite default — every untyped parameter is a string — is worse.
//
// # Authentication
//
// The zero [Options] authenticates nobody. That is correct for a loopback or
// unix-socket listener inside a trust boundary and wrong for anything else; it
// is spelled [Trust] rather than implied so that a server which authenticates
// nobody is a thing someone wrote down.
//
// [SCRAM] implements SCRAM-SHA-256 (RFC 5802, RFC 7677) from the standard
// library. md5 authentication is deliberately not implemented. SCRAM-SHA-256-
// PLUS is not implemented either, because channel binding needs a TLS channel
// and there is none; a client that requires channel binding is told so rather
// than downgraded. Passwords must be printable ASCII, because SASLprep is not
// implemented and this package will not guess at a normalization — see
// [NewVerifier].
//
// # Concurrency
//
// One connection is one session with its own prepared statements, portals, and
// [query.Exec], served by one goroutine. Nothing on the query path is shared,
// which is what makes the engine's single-consumer contract hold without a
// lock. Many sessions run concurrently against one [Source]; every execution
// takes its own snapshot, and a heap database or a durable collection is safe
// for concurrent readers by its own contract.
//
// One consequence is visible to a client. A session has one [query.Exec], so it
// has one live result set: a portal suspended by an Execute row limit cannot be
// resumed after another statement has executed on the same connection, and
// trying returns 55000 with an explanation. Reading a portal to completion, or
// executing without a row limit, avoids it entirely, which is what every client
// library does by default.
//
// # Cancellation
//
// The cancel-request subprotocol is implemented — a backend key is issued, and
// a cancel arriving on its own connection is matched to a session and delivered
// — but what it can stop is limited by the executor, which has no cancellation
// hook. A cancel is honoured before a statement starts executing and between
// the statements of a multi-statement simple query. A scan already running
// finishes.
//
// This is stated here rather than left to be discovered because the failure it
// produces is silent: a client that cancels a slow query and receives a
// complete result set has been misled about what happened. If you need a hard
// bound, close the connection; [Server.Close] does that for every session.
//
// # Security posture of the message reader
//
// reader.go parses bytes chosen by an unauthenticated peer, and it is written
// on that assumption: every allocation is bounded by a constant before a
// peer-supplied length is used for anything, every declared element count is
// checked against the bytes actually present, and every field accessor
// bounds-checks and latches a failure rather than panicking. FuzzFrontendMessage
// drives arbitrary bytes through the decoder and asserts the only two possible
// outcomes are a decoded message and an error.
package pgwire
