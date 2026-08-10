// Package driver registers the "vibedb" database/sql driver over durable JSON
// collections and the shared query executor.
//
// SQL is the request language. JSON is the row representation, not a parallel
// query grammar. The root sql package parses one statement, query lowers its
// SELECT or mutation predicate, and this package supplies catalog, storage,
// transaction, and database/sql policy.
//
// # Typed runtime and ownership
//
// [Open] exposes the same parser, catalog, prepared statements, executor, and
// transaction implementation to protocol adapters without routing typed cells
// through database/sql. A [Database] owns the catalog writer lease;
// [Database.NewSession] creates one single-consumer [Session]. Closing Database
// prevents new sessions immediately, while existing sessions retain the
// catalog until their own [Session.Close].
//
// [Session.Prepare] parses and lowers exactly once. [Prepared] reports the
// statement kind, placeholder count and scalar-versus-document [ParamKind], as
// well as immutable output names and typed [query.OutputColumn] metadata.
// [Prepared.Query] returns a [Cursor] that advances the executor's
// [query.Cursor] directly and exposes [query.Cell] without conversion or
// per-cell allocation. Protocol adapters embed cursor storage and use
// [Prepared.QueryInto], making a warmed point-query execution allocation-free.
// One session owns at most one live cursor; Cursor.Close is the deterministic
// snapshot-lease and borrowed-cell lifetime boundary. Caller argument slices
// are never cleared or rewritten: the session copies their interface headers
// into reusable connection-owned binding storage. Stable *string and *[]byte
// slots can carry whole documents without boxing their variable-width headers.
//
// Runtime transaction state follows PostgreSQL's failed-transaction rule. A
// prepare or execution error after Begin moves the session to
// [SessionFailedTransaction]; only Rollback remains usable. Commit in that state
// first rolls back every staged change and then returns [ErrTransactionFailed].
// A protocol error outside runtime execution can make the same transition with
// [Session.MarkFailed].
//
// # Catalog, schemas, and indexes
//
// The DSN is a catalog file path. Each table maps to one durable collection
// beside it. A separate lock file gives the connector an exclusive
// process-and-filesystem catalog writer lease until Close, preventing stale
// catalog rewrites by independently opened handles. CREATE TABLE records a
// persisted JSON schema and exactly one scalar JSON-path primary key without
// allocating an empty collection file. The first write materializes the
// mutable chunk layout. Catalog replacement and first table-file creation use
// platform namespace durability fences; a failure after publication reports
// durable.ErrCommitOutcomeUnknown without discarding the matching live state.
// A terminal database/sql Close still releases all connector-owned table
// descriptors and the writer lease after reporting such an error, because the
// database/sql connector close hook is not retryable.
//
// Declared JSON types, nullability, and required paths are enforced on every
// write and recompiled when the catalog reopens. CREATE INDEX declares an exact
// index over one to four paths. On a populated table it scans immutable primary
// leaves under bounded writer holds and publishes the complete index and its
// catalog entry atomically. INSERT, whole-document UPDATE, DELETE, and
// transactional batches maintain ready-index postings in the same publication
// as the primary change.
//
// # Documents and identity
//
// INSERT INTO t VALUES (?) accepts a string or []byte containing one complete
// JSON document. INSERT INTO t (a, b) VALUES (?, ?) builds a flat object from
// scalar values. The driver derives the storage key from the table's declared
// primary-key path. String, boolean, and exact numeric identities are
// type-separated; numerically equal spellings such as 1, 1.0, and 1e0 identify
// one row.
//
// INSERT, UPDATE, and DELETE ... RETURNING path, ... and RETURNING * execute
// through Query. The projection is evaluated from staged JSON documents before
// publication, without a storage reread. DELETE projects pre-delete documents;
// UPDATE projects replacements. A projection or result-budget failure therefore
// publishes nothing.
//
// INSERT rejects an existing or repeated derived key with
// ErrDuplicatePrimaryKey and never replaces it, unless the statement explicitly
// uses ON CONFLICT DO NOTHING, which skips those rows. UPDATE is the explicit
// replacement operation, accepts only SET "$doc" = ..., and cannot change the
// derived primary key. Because one statement supplies one replacement document,
// an UPDATE matching several distinct keys returns [ErrUpdatePrimaryKey]; use a
// transaction with one replacement statement per key. LastInsertId is
// unavailable because keys come from documents.
//
// # SELECT and joins
//
// SELECT exposes query.Statement's shared SQL surface: full predicates,
// projection, the five aggregates, grouping, HAVING, ordering, LIMIT, OFFSET,
// and positional placeholders. Primary-key equality and membership use point
// reads; eligible exact predicates use posting candidates; the remaining
// shapes use the shared scan executor.
//
// Non-recursive SELECT-valued common table expressions support lexical earlier
// siblings, nested shadowing, output aliases, and MATERIALIZED/NOT MATERIALIZED
// policy. A multiply referenced default CTE is shared; a safe single-reference
// identity shape fuses into its defining plan. One recursively discovered
// physical collection keeps the direct durable and exact-index path. Multiple
// collections use one coherent reusable capture, and transactional execution
// reads the current isolation cut plus staged writes. All relation spools share
// ExecOptions.IntermediateBytes, publish no partial result on error or
// cancellation, and retain no CTE state or marginal allocation when WITH is
// absent.
//
// SUM, MIN, and MAX preserve exact JSON decimals. AVG emits an exact finite
// quotient when it fits the query engine's 34-significant-digit policy and
// otherwise rounds once, ties to even. Exact reduction workspace is bounded
// and returns query.ErrAggregateBudget rather than falling back to float64.
//
// INNER, LEFT, RIGHT, FULL, and CROSS JOIN compose into chains whose operands
// may be durable collections, uncorrelated derived tables, or non-recursive
// CTEs. ON accepts composite equi-keys plus residual predicates; USING accepts
// one or more columns. Equi-key stages use the reusable composite-hash kernel,
// while keyless and cross stages use the exact bounded nested-loop kernel.
// Outer joins preserve the required side or sides and SQL-null-extend every
// missing partner. SQL exposes no physical-key pseudo-column: "$key" is an
// ordinary quoted JSON field, and a relationship based on identity names the
// declared primary-key field.
//
// The driver captures all participating durable collections in one coherent
// leased snapshot. Generalized joins receive that catalog cut directly, so a
// physical operand retains its durable scan and eligible exact-index path.
// Transactions execute against their statement or BEGIN cut plus the overlay.
// The legacy single-clause physical INNER/LEFT equi-join subset keeps its
// established bounded heap path and storage-aware strategy; classification
// happens once at preparation and ordinary point queries never initialize
// relation-join state.
//
// Operand spools and joined relations share ExecOptions.IntermediateBytes;
// hash-build and fan-out pair work use ExecOptions.JoinPairBytes. Exhaustion,
// cancellation, or binding failure returns no partial Cursor, and a prepared
// statement remains reusable. Warm QueryInto execution reuses generalized-join
// storage. Unaliased source-zero paths keep their path header and joined paths
// stay range-variable-qualified; explicit aliases override both, including
// intentional duplicate output names.
//
// # Transactions
//
// Default and Read Committed transactions refresh every cataloged table at one
// coherent generation cut before each physical statement. Repeatable Read and
// Snapshot retain the BEGIN cut, providing repeatable reads and phantom
// exclusion. Every mode overlays staged changes and preserves read-your-writes;
// joins consume the same statement cut under the relation and pair bounds.
//
// A transaction may read and write several tables. COMMIT validates every
// dirty table under the catalog lock (incarnation, bounded first-committer-wins
// conflict clock, write-set existence), materializes tables absent at BEGIN as
// empty durable participants, then publishes: one dirty table through today's
// Collection.Update path; two or more through durable.UpdateCollections against
// the driver-owned decision log in <catalog>.tables/. Disjoint keys may commit
// concurrently across tables. Serializable publishing commits additionally
// validate bounded exact primary-key reads (including misses) and promote
// scans, nested execution, or exact-read overflow to a relation-coarse
// dependency. This rejects write skew and phantoms without serializing
// independent point writers on the same table. An aborted or crashed
// transaction that wrote
// to a table absent at BEGIN can leave that empty table behind as catalog
// residue. SAVEPOINT / RELEASE / ROLLBACK TO manage a bounded overlay stack
// (64 frames); ROLLBACK TO does not lower high-water admission accounting and
// recovers a failed session to in-transaction state. DDL remains refused inside
// a transaction. If the bounded conflict history overflows, transactions older
// than its new floor fail conservatively rather than risk a missed conflict.
// A multi-table COMMIT whose decision sync fails returns
// durable.ErrCommitOutcomeUnknown: the unknown outcome is atomic across every
// participating table, and further writes under the catalog refuse until reopen.
//
// Default/Read Committed, Repeatable Read/Snapshot, and Serializable are
// accepted; weaker and linearizable levels are refused. Batch
// document and byte bounds return ErrTransactionTooLarge wrapping
// durable.ErrBatchTooLarge without partial publication. Catalog format version
// 0 is the current and only accepted format; this unreleased driver does not
// carry forward layouts with different key or index semantics.
//
// # Driver values
//
// JSON null and a missing projected path both scan as nil. bool scans as bool;
// an integral JSON number that fits scans as int64; other numbers retain their
// exact decimal spelling as []byte; strings scan as decoded []byte; objects and
// arrays scan as JSON []byte.
//
// Scalar inputs accept the ordinary database/sql driver values and additionally
// accept encoding/json.Number. The latter remains an exact numeric spelling
// through flat INSERT, predicates, document-derived numeric primary keys, and
// LIMIT/OFFSET; it is not converted through float64 or treated as a string.
// One argument may carry at most 4 MiB and one execution at most 16 MiB of
// aggregate string, exact-number, and document payload. Argument references are
// cleared after binding so an idle pooled connection does not pin caller data.
//
// # Context cancellation
//
// Connection, preparation, queries, transaction staging, and every mutation
// use context-aware driver interfaces. For a cancellable context, database/sql
// bridges ctx.Done to the executor's cooperative CancelFlag for exactly one
// operation and joins that watcher before returning. SELECT, JOIN, grouping,
// sorting, filtered UPDATE/DELETE, and spill work therefore stop at bounded
// checkpoints without leaking workers, temporary files, snapshots, partial
// results, or partially staged transaction state. A context with no Done
// channel retains the allocation-free nil-flag execution path.
//
// Cancellation can stop lock acquisition, parsing, validation, scans, and
// write preparation before the durable publication checkpoint. Once
// publication begins it runs to completion and returns its storage outcome,
// including durable.ErrCommitOutcomeUnknown when a published namespace change
// cannot be fenced. The driver never returns cancellation while a write
// continues in the background.
//
// The typed runtime additionally accepts a reusable [query.CancelFlag] through
// [Session.SetCancelFlag]. The executor observes that flag at bounded
// checkpoints, returns [query.ErrCanceled] only after its workers are parked
// and spill files are removed, and exposes no partial result. The nil-flag path
// adds one pointer comparison and no allocation. Protocol adapters may bridge
// a request context to that flag from their existing cancellation goroutine.
//
// database/sql advertises StmtQueryContext and StmtExecContext. Direct
// connection ExecContext uses the same prepared execution path, including for
// scan-shaped UPDATE and DELETE. Query memory, result, batch, and spill
// ceilings remain resource-admission bounds independent of deadlines.
//
// # Concurrency and pooling
//
// Each database/sql connection owns one single-consumer query.Exec and runs it
// with one executor worker. database/sql already obtains concurrency through
// separate pooled connections; inheriting query's standalone GOMAXPROCS default
// in every connection would multiply retained durable worker pools and their
// arenas by MaxOpenConns. Applications choose aggregate SQL concurrency with
// DB.SetMaxOpenConns.
package driver
