// Package pgwire provides an experimental PostgreSQL wire-protocol endpoint supporting a documented SQL subset.
// It implements frontend/backend protocol version 3.0 from the specification;
// it does not claim PostgreSQL SQL or catalog compatibility.
//
//	db, err := driver.Open("/data/app.vdb")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer db.Close()
//	srv, err := pgwire.NewServer(db, pgwire.Options{
//		Auth: pgwire.Trust(), // explicit; use SCRAM outside a trust boundary
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//	ln, _ := net.Listen("tcp", "127.0.0.1:5433")
//	go srv.Serve(ln)
//
// A pgx or lib/pq client then issues the bounded dialect
// [github.com/thesyncim/vibedb/sql] parses. SQL parsing, catalog validation,
// DDL/DML, transactions, query lowering, and execution live in one typed
// runtime under both this adapter and database/sql. This package implements
// protocol framing, parameter/result conversion, a bounded connect-sequence
// compatibility shim, and PostgreSQL transaction/error state; it does not
// duplicate the stored-row grammar or evaluator.
//
// # Who this is for, and who it is not for
//
// This server targets programmatic clients—a Go, Python, Node, or Rust driver
// issuing SQL this engine supports—and stock psql for direct SQL sessions. The
// in-repository suite drives the complete protocol state machine over net.Pipe
// without depending on one client's interpretation of it. A separate
// checked-in integration module also runs unmodified pgx v5 and lib/pq clients
// over loopback TCP and the official PostgreSQL 18.4 psql client in CI. That is
// narrow, reproducible evidence for those pinned client versions, not a general
// PostgreSQL-compatibility guarantee. psycopg and node-postgres have not been
// tested here.
//
// A general catalog SQL engine remains deliberately out of scope. A psql
// "\d users" expands to a query that joins pg_class to pg_namespace with a
// LEFT JOIN, filters with a subquery, casts with ::regclass, calls scalar
// functions like pg_table_is_visible and format_type, and tests membership
// with ANY over an array—every one of which this dialect refuses on purpose,
// because the executor has no operator for any of them. The same is true of
// JDBC's DatabaseMetaData, of every ORM's schema reflection, and of every BI
// tool's table browser, and those still fail here.
//
// psql's own basic meta-commands are the one bounded exception. The pinned
// PostgreSQL 18.4 psql client builds its catalog queries from fixed templates, so the exact texts it
// emits for \l, \dn, \dt, \di, \d, and \d <name> are knowable in advance, and
// this server recognizes those texts and answers each
// whole query from the SQL catalog without evaluating any of the SQL inside
// it; \d <name> currently accepts a bare ASCII identifier of at most 128
// bytes, and \df, \du, and \dv are recognized and honestly empty. The recognition
// runs only after the SQL front end has already refused the statement, so a
// query the dialect accepts never pays for it and an unrecognized query keeps
// the front end's original error unchanged — catalog_shim.go carries the full
// argument and the allocation measurement. Client-side commands that issue no
// SQL, such as "\q", work as before.
//
// The parser-level refusal is checked rather than asserted:
// TestCatalogQueriesAreRefused feeds real psql, JDBC, and information_schema
// probes to the parser and logs where each stops. In practice the parser
// rejects them at "pg_catalog.pg_class", because a schema-qualified relation
// name is indistinguishable from this dialect's dotted path syntax and is
// refused rather than guessed at. Supporting arbitrary catalog SQL would mean
// building a subquery-capable, outer-join-capable SQL engine with a schema
// namespace, to answer questions about a catalog that does not exist. That is
// a much larger project than this one and a different one; the recognition
// table answers the questions without building the engine.
//
// # What works
//
//   - Startup, including SSLRequest and GSSENCRequest (both refused in the
//     clear), protocol version negotiation, and the parameter report.
//   - Authentication: trust, or SCRAM-SHA-256. See "Authentication" below.
//   - The simple query protocol, including up to 1,024 statements in one
//     message.
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
//   - CREATE TABLE, CREATE INDEX, INSERT, UPDATE,
//     DELETE, SELECT, one declared-field inner JOIN, schema validation, exact
//     indexes, whole-document parameters, and affected-row command tags.
//   - The pinned PostgreSQL 18.4 psql client's basic introspection meta-commands
//     — \l, \dn, \dt, \di, \d, and \d <name> — answered from the SQL catalog
//     by the post-parse-failure recognition shim in catalog_shim.go. The name
//     capture is a bare ASCII identifier of at most 128 bytes; \df,
//     \du, and \dv are recognized and honestly empty.
//   - Explicit BEGIN/COMMIT/ROLLBACK with ReadyForQuery I/T/E state,
//     read-your-writes, rollback, failed-transaction behavior, read-only mode,
//     and implicit atomic batches for non-DDL stored-row statements.
//
// # What does not work
//
//   - DDL inside a transaction. CREATE TABLE and CREATE INDEX are atomic
//     individually, but the catalog cannot yet publish through the transaction
//     overlay. DDL must be the only non-empty statement in a simple Query and
//     the only catalog execution between extended-protocol Sync points.
//     Extended DDL publishes at Execute; if a client violates that boundary by
//     executing a later command before Sync, the later command is refused but
//     cannot retroactively roll back the completed DDL.
//   - Savepoints, chained transactions, two-table writes, and isolation modes
//     other than the runtime's REPEATABLE READ snapshot. Read-only and
//     read-write transaction access modes are supported.
//   - pg_catalog and information_schema as queryable tables. There are no
//     catalog tables; see above. psql's basic meta-commands (\l, \dn, \dt,
//     \di, \d, \d name) are answered by the recognition shim against a
//     writable SQL catalog; every other catalog consumer — JDBC's
//     DatabaseMetaData, ORM schema reflection, ad-hoc pg_catalog SQL — still
//     fails with the parser's refusal.
//   - COPY, the function-call subprotocol, LISTEN/NOTIFY, cursors (DECLARE and
//     FETCH), SQL-level PREPARE/EXECUTE, and replication.
//   - EXPLAIN and EXPLAIN ANALYZE return one JSON QUERY PLAN row for the
//     documented SELECT subset. The report is VibeDB's stable plan schema, not
//     PostgreSQL's textual plan format.
//   - TLS. SSLRequest is answered 'N'. Put this behind a unix socket, a
//     loopback bind, or a TLS-terminating proxy.
//   - The SQL constructs the dialect itself refuses — subqueries, right/full outer joins,
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
// JSON number is bound as an exact decimal literal. SUM, MIN, and MAX are exact
// decimal reductions too. AVG emits an exact finite quotient when possible and
// otherwise rounds once to 34 significant decimal digits, ties to even. A
// bounded reduction that cannot retain its exact coefficient returns an error
// instead of silently narrowing through float64.
//
// # Driver compatibility evidence
//
// The package tests verify startup, SCRAM, simple and extended query flows,
// pipelining and resynchronization, cancellation framing, every emitted type
// and format, and client connect-sequence shims directly at the protocol
// boundary. The nested integration/pgclient module is deliberately outside the
// root dependency graph and imports pinned pgx v5 and lib/pq releases. CI runs
// those clients and an official, digest-pinned PostgreSQL 18.4 psql client
// against a real TCP listener. Its gates cover encryption refusal and cleartext
// fallback; SCRAM startup; CREATE TABLE and CREATE INDEX; whole-document and
// flat writes; schema and duplicate-key SQLSTATEs; recovery after a
// simple-query error; pgx named prepared statements; a declared-field JOIN;
// lib/pq database/sql transactions; rollback, read-your-writes, and commit;
// JSON string framing; exact transport of a 19-digit number; graceful closes;
// and catalog reopen persistence.
//
// One result from that automated check is important to the type contract.
// A 19-digit integer arrives on the wire as its exact digits. A JSON codec that
// decodes a bare number into float64 can narrow it afterwards; the wire did
// not. Read exact identifiers as raw JSON text or a deferred JSON value.
//
// # Bound parameters
//
// Scalar placeholders have no inferred type — they compare against whatever
// the path holds — so ParameterDescription reports zero for an unspecified
// scalar position. The typed runtime identifies whole-document INSERT/UPDATE
// positions, and those default to json (OID 114), allowing pgx and similar
// clients to choose an object/array-capable encoder without guessing. OIDs
// explicitly declared in Parse are preserved in wire-parameter order. An
// unspecified scalar text parameter is read as a JSON scalar: 21 binds as the
// number 21, true as a boolean, null as SQL NULL, and anything else as a
// string.
//
// The inference has one edge, and it is worth knowing rather than discovering:
// binding the *string* "21" against a string-valued path matches nothing,
// because the text on the wire is the same as the number's. Declare the
// parameter's OID in Parse (pgx does this when it knows the Go type) or bind in
// binary with a declared OID, and the ambiguity is gone. Declared json/jsonb
// parameters in scalar positions use strict JSON scalar semantics, including
// quoted-string unescaping; arrays and objects are refused there because
// predicates bind scalar leaves. Whole-document positions accept one complete
// JSON value and preserve its exact bytes. Reusing one $n in both roles is a
// 42804 datatype mismatch. extended.go explains why the opposite scalar
// default — every untyped parameter is a string — is worse.
//
// The SQL parser accepts at most 65,536 placeholders in an embedded statement.
// The PostgreSQL protocol's Bind and ParameterDescription counts are signed
// Int16 values, so this server's protocol surface necessarily has the lower
// ceiling of 32,767 parameters per prepared statement. Parse rejects a wider
// statement before publishing it.
//
// # Authentication
//
// Authentication has no default: [NewServer] rejects a nil [Options.Auth].
// [Trust] is correct for a loopback or unix-socket listener inside a trust
// boundary and wrong for anything else; requiring it explicitly makes a server
// which authenticates nobody a choice someone wrote down rather than an
// omitted production setting.
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
// query executor, served by one goroutine. The database opens one typed SQL
// Session per authenticated connection. Nothing single-consumer is shared, so
// the query hot path needs no session lock. Many sessions run concurrently
// against one database, with independent snapshots and transaction overlays.
//
// Each session runs its executor with one worker. Session concurrency already
// supplies parallelism; inheriting query's standalone GOMAXPROCS default in
// every admitted connection would multiply retained durable worker pools and
// their arenas by [Options.MaxConnections].
//
// One consequence is visible to a client. A SQL session has one reusable query
// executor, so it has one live result set: a portal suspended by an Execute row
// limit cannot be resumed after another statement has executed on the same
// connection, and
// trying returns 55000 with an explanation. Reading a portal to completion, or
// executing without a row limit, avoids it entirely, which is what every client
// library does by default.
//
// Writable-catalog portals are non-holdable: Sync closes them when it commits
// or rolls back an implicit batch, and explicit COMMIT/ROLLBACK closes them as
// well. Prepared statements survive those boundaries. A later Execute naming
// a closed portal receives 34000, matching the protocol object's lifetime
// rather than retaining a durable snapshot past its transaction.
//
// # Shared typed runtime
//
// [NewServer] borrows a [github.com/thesyncim/vibedb/sql/driver.Database].
// Each authenticated connection opens one single-consumer typed Session.
// Prepare parses and lowers once, exposes scalar/document parameter roles and
// typed output metadata, and owns the reusable compiled statement. SELECT
// fills an embedded reusable cursor of query.Cell values; DDL/DML returns one
// affected-row result; transaction state is the source of ReadyForQuery I/T/E.
//
// The database/sql adapter maps this runtime to driver.Value, while this
// package maps the cells directly to PostgreSQL formats. Keeping the typed
// boundary below both prevents a JSON string, exact decimal, and container
// from collapsing into indistinguishable []byte values, avoids a second parse
// for Describe, and leaves one catalog, validation, index, mutation, and
// transaction implementation.
//
// # Cancellation
//
// The cancel-request subprotocol is implemented end to end. A backend key is
// issued, a cancel arriving on its own connection is matched to a session, and
// the session arms the query executor's reusable atomic CancelFlag only while
// a Query, Execute, or transaction-finalizing Sync is active. A request which
// arrives while the backend is idle is ignored rather than retained to cancel
// the next command. Before execution, UTF-8 admission, simple-query statement
// splitting, command classification, transaction/SET/fixed-result parsing,
// catalog recognition, Parse-message ownership copying, and numbered-parameter
// rewriting check the same signal at bounded byte or token intervals. Heap and
// durable scans, parallel workers, joins, filtered DML, and spill I/O then check
// it at bounded units of work. Cancellation drains reusable pipelines, releases
// snapshot leases, removes spill files, exposes no partial materialized result,
// and reports 57014. During DataRow delivery it stops at the next row boundary;
// rows already written cannot be retracted, which is ordinary PostgreSQL
// error-stream behavior.
//
// Socket admission keeps a small pre-startup cushion above MaxConnections so a
// CancelRequest has an opportunity to contend when all ordinary session slots
// are occupied. It is deliberately described as a cushion, not a guarantee:
// the server cannot know whether a newly accepted socket carries CancelRequest
// or an ordinary startup packet until it reads that packet, so slow ordinary
// startups can occupy every cushion slot. The finite default ReadTimeout bounds
// each partial read, and SSLRequest and GSSENCRequest are each accepted at most
// once, so negotiation cannot reset that deadline forever. Explicitly disabling
// ReadTimeout removes the partial-read bound and can make cancel admission
// starvation indefinite under a hostile or stalled peer.
//
// Cancellation is cooperative rather than an abandoned execution goroutine.
// Its latency is one bounded protocol pre-parser interval, then the executor's
// bounded checkpoint interval, plus any durable operation already inside one
// indivisible publication step. Once durable publication begins it runs to
// completion and returns its storage outcome; SQL DDL may explicitly return
// [github.com/thesyncim/vibedb/store/durable.ErrCommitOutcomeUnknown] after a
// namespace publication whose durability fence failed. Returning early while
// a write continued invisibly would be less correct than surfacing that
// outcome. In every case the cancellation signal is consumed by the operation
// it targeted and cannot poison the next statement.
//
// # Protocol resource bounds and hostile input
//
// reader.go parses bytes chosen by an unauthenticated peer, and it is written
// on that assumption. A startup packet is at most 10,000 bytes and a later
// frontend body at most 16 MiB; every declared element count is checked against
// the bytes actually present, and every field accessor bounds-checks and
// latches a failure rather than panicking. Text is checked against the UTF-8
// encoding the server reports. Protocol-side linear scans checkpoint
// cancellation at least once per 4 KiB of input, including inside quoted
// literals and comments, so the message-size bound cannot become a
// non-interruptible parser-size bound.
//
// Per-message bounds are not treated as per-session bounds. Prepared names,
// SQL, copied parameter OIDs, numbered-parameter occurrence maps and rewritten
// SQL; parser/AST/lowered-plan shape; portal arguments; and prepared compilers'
// bound-literal high-water storage each have aggregate session budgets.
// Prepared shape is admitted with a fixed arena allowance plus a conservative
// source-length multiplier covering nodes, registries, output metadata,
// escaped literals, and geometric slack. A repeated $1 is copied, decoded, and
// escape-scanned once for the portal, while its conservative per-occurrence
// compiler cost is admitted before Bind succeeds. The compiler charge uses the
// exact JSON-escaped string length and includes arena, formatting-scratch,
// predicate, and numeric-spelling slack.
//
// Simple-query statement iteration is allocation-free rather than materialized
// into a slice, and the message is capped at 1,024 statements. Preparing and
// executing each statement retains only that statement's bounded runtime
// objects. The no-FROM compatibility SELECT carries the SQL parser's same
// 1,024-column limit. DataRow and RowDescription bodies are each admitted
// against a 16 MiB bound before the writer is touched, preventing repeated
// values or names from amplifying a small query into an overflowing or
// partially emitted backend message.
//
// Each SELECT result is materialized under [Options.MaxResultRows] and
// [Options.MaxResultBytes] before any DataRow is encoded. Their zero values
// select finite defaults of [DefaultMaxResultRows] and
// [DefaultMaxResultBytes]; [UnlimitedResults] is the explicit opt-out.
// Exhaustion is reported as SQLSTATE 54000 and never emits a partial DataRow.
// Socket admission is likewise finite by default, and read, idle, and write
// deadlines cover startup, message reads, and every underlying socket write.
//
// Executor intermediates have independent finite zero-value limits as well.
// [query.ExecOptions.MemoryBytes] admits row-proportional heap columns,
// selections, ordering/grouping state, decoded text, join memberships, and
// durable index-planning, batch/merge, and inner-join work. An optional durable
// index plan whose conservative workspace bound does not fit declines to the
// exact full scan instead of failing the statement. Exact aggregates, fan-out
// pairs, and durable spill files have their own bounded defaults. Crossing a
// non-optional limit is reported as SQLSTATE 54000. These are memory and
// storage-admission bounds, independent of the cooperative cancellation
// checkpoints described above.
//
// FuzzFrontendMessage drives arbitrary bytes through the decoder and asserts
// the only two possible outcomes are a decoded message and an error.
package pgwire
