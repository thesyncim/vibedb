// Package vibesql is a database/sql driver over vibejson's query engine,
// registered under the name "vibejson".
//
//	db, err := sql.Open("vibejson", "/var/lib/app/users.vj")
//	rows, err := db.Query(`SELECT name, age FROM users WHERE age >= ? ORDER BY age`, 21)
//	res, err := db.Exec(`DELETE FROM users WHERE age < ?`, 18)
//
// It is a front end and nothing more. A statement is parsed by [sql], lowered
// by [query.PrepareStatement] onto the same compiled plan the programmatic
// builder produces, and executed by the same executor. There is no second
// evaluator, no second comparator, and no interpretation of SQL at run time.
//
// # What it costs
//
// SQL parsing is not free per query. It is free in steady state, and the
// difference is which API you use.
//
// A prepared statement — a *sql.Stmt, or any query database/sql prepares on
// your behalf and keeps — parses and compiles once. If it holds no placeholder,
// every later execution hands the identical plan to the identical executor:
// there is nothing left of SQL at that point, and the work is byte for byte the
// native builder's. If it holds placeholders, each execution re-lowers the plan,
// because a literal compiles into the plan as a typed constant; that re-lowering
// refills a retained compiler's arenas rather than allocating, so it is free of
// heap allocation but not free of work.
//
// An ad-hoc query — db.Query with a statement the pool does not keep — parses
// and compiles on every call and throws the plan away. In a loop that is the
// wrong shape. Hold a *sql.Stmt.
//
// The benchmarks measure all three against the equivalent builder query over
// the same source, rather than asserting the difference is small. On an Apple
// M4 Max, Go 1.26, 2000 documents, reading every cell as sql.RawBytes:
//
//	                      native   prepared     ad-hoc   rows out
//	point query          2.98 us    3.91 us    7.33 us          1
//	  no placeholder                3.45 us
//	filtered scan         124 us     164 us     168 us        600
//	grouped aggregate     127 us     130 us     133 us          3
//	inner join           1.26 ms    1.71 ms    1.72 ms       8000
//	primary-key join      559 us     621 us     628 us       2000
//
// Read it as three separate numbers. The per-execution overhead is the point
// query's 0.6-0.9 us, which is database/sql's pooled handoff, the *sql.Rows,
// and — for a placeholder statement — one re-lowering of the plan; a statement
// without placeholders shows the re-lowering as the 0.3 us between the two
// prepared rows. The per-row overhead is the gap that grows with rows out:
// about 60 ns per row on the filtered scan and 57 on the inner join's eight
// thousand pairs, and it is one interface box per projected cell, because
// driver.Value is any and an interface value cannot be reused across rows. Every database/sql driver pays exactly that; the tests measure
// it as a slope and hold it at one allocation per cell and nothing more. And
// the parse-and-compile is ad-hoc minus prepared, 3.4 us on the point query,
// which is invisible on the grouped aggregate only because the scan behind it
// costs forty times as much.
//
// # The DSN
//
// The DSN is a filesystem path to a durable collection file, or "memory:" plus
// the name of a source this process registered with [Attach] or
// [AttachCollection]. It is not a parameter language and does not have options.
//
// A path can only open a collection created with default options, because a
// durable collection's index catalog is frozen at creation and Open refuses a
// mismatch. A collection with declared indexes is opened by the application,
// which knows its options, and handed over with [AttachCollection].
//
// A JOIN needs [Attach]. Both sides of a join are read from one
// [store.DatabaseSnapshot] — the consistent cut that makes snapshot skew across
// a join inexpressible — and only a heap [store.Database] produces one; a
// durable file holds a single collection and has no catalog.
//
// # Types
//
// driver.Value is a closed set and JSON is not, so the mapping is chosen to
// change no value on the way out:
//
//	JSON null, or an absent path   nil
//	JSON true / false              bool
//	an integer int64 can hold      int64
//	any other number               []byte, its exact decimal digits
//	a string                       []byte, decoded
//	an object or an array          []byte, its exact JSON
//
// Numbers do not go through float64, and that is deliberate. This engine
// compares numbers by exact decimal value: 9007199254740992 and
// 9007199254740993 are two values here and one float64. A driver that returned
// float64 would return the wrong integer for an ordinary 19-digit identifier
// and would do it silently. Digits scan into a float64, a string, a
// json.Number, or a math/big value as you choose; a collapsed float cannot be
// uncollapsed.
//
// Strings are []byte because a projected string borrows the collection's bytes
// or the execution workspace's decoded-text buffer, both reused by the next
// execution. database/sql copies a []byte into every Scan destination except
// sql.RawBytes, so the copy happens exactly once, where the caller decides to
// keep the value. Scanning into a *string or a *[]byte works as expected;
// scanning into an *any yields []byte.
//
// ColumnTypeScanType reports any for a projection, because a schemaless path
// genuinely has no single type, and int64 for COUNT, which does.
//
// # Where this dialect and SQL disagree
//
// These are the deviations that remain after lowering. Anything not listed here
// behaves as SQL does, and the differential tests in this package and in
// [query] check that against an independent reference.
//
// Null comparison is three-valued, as SQL's is. This was the one divergence
// worth fixing rather than documenting: the engine's comparison answers false
// for a null cell, so its NOT answers true, and a row every other SQL database
// drops would be kept. Lowering builds a predicate for "is TRUE" and a separate
// one for "is FALSE" and recurses through Kleene's tables, so WHERE, NOT, AND,
// OR, IN, and BETWEEN all read as SQL reads them. No known shape diverges. The
// cost falls only where the dialects differed: a positive predicate lowers to
// exactly what the builder would have built.
//
// An absent path and an explicit null are one value. IS NULL is true for both,
// and SQL has no notion of an absent column at all. The distinction is
// available as "IS [NOT] MISSING", which is true only when the path resolves to
// nothing.
//
// Comparison is within type, with a cross-type total order. SQL raises a type
// error or casts; here values compare by exact decimal value within numbers, by
// decoded content within strings, and across types by the fixed order null <
// bool < number < string < container. So "age > '5'" is false for every numeric
// age rather than an error.
//
// MIN and MAX are numeric. They extract their argument as a numeric column and
// skip non-numeric values, so MIN over a string field is null rather than the
// least string.
//
// SUM and AVG skip non-numeric values rather than failing, so a column of mixed
// types produces a total over its numbers rather than a type error.
//
// ORDER BY puts nulls first ascending and last descending. SQL leaves the
// placement implementation-defined and PostgreSQL answers the other way for
// ASC. NULLS FIRST and NULLS LAST are refused rather than silently ignored.
//
// Duplicate object keys resolve to the last occurrence. SQL has no equivalent,
// because a row cannot have two columns of one name.
//
// '@>' — jsonb-style containment — is two-valued rather than three. Its left
// operand is a JSON document, and JSON null is a value a needle can match, so
// declining to answer for it would make "x @> null" unanswerable rather than
// precise. It is also not standard SQL, so there is no standard reading to
// diverge from. Inside HAVING it is refused outright, along with IS MISSING;
// see below.
//
// JOIN is SQL's inner join, with three restrictions. The joined collection's
// columns are projectable, groupable, orderable, and reducible through the
// range variable, and the result carries one row per matching pair. Pair order
// is defined and structural — driving ordinal, then joined ordinal — so it
// holds without an ORDER BY, and an absent joined path projects null.
//
// A WHERE condition that reads only the joined collection is moved into that
// join clause's own filter, which selects the same pairs. The move is applied
// only to a top-level ANDed term of WHERE, where it is provably the same
// question: such a term is TRUE of a pair exactly when it is TRUE of the pair's
// joined document, so removing the failing documents before the pairing removes
// exactly the pairs the term would have rejected. Under an OR, or under a NOT
// that also covers a driving column, that is false — "u.a = 1 OR o.b = 2" keeps
// a pair whose joined document fails the second test — so a WHERE term naming
// two collections at once is refused rather than moved anyway.
//
// The three restrictions: OUTER, CROSS, and NATURAL joins are refused by name,
// because an outer join needs a null discipline an inner join does not; a
// chained join, whose ON names a collection other than the FROM one, has no
// plan, because the engine crosses the driving collection with one clause's
// matches and not with another clause's output; and only one clause per
// statement may produce rows, a second being a cross product of two match sets
// this expansion does not build.
//
// A range variable whose name holds a '.' or a '/' is refused. Those are the
// bytes the engine's path language uses to separate segments and to mark a JSON
// Pointer, so a qualified path through such an alias would resolve to something
// else entirely.
//
// A join joining on the joined collection's primary key, spelled o."$key",
// that nothing outside the clause reads is answered by the cheaper semi-join
// operator. That is a plan choice under a proof — a key names at most one
// document, so one row per pair and one row per driving row are the same
// rows — and not a change of meaning.
//
// HAVING is applied after execution rather than by the plan, using the engine's
// own comparator over the result's cells. It therefore costs the statement its
// LIMIT pushdown, since a plan that stopped early would be cutting rows the
// filter had not yet rejected. Two leaf forms are refused inside it: IS MISSING,
// because a result cell records that a key is null and not whether it was
// absent, and '@>', because containment is answered on the execution path and a
// second copy of it here is exactly the duplication this design avoids.
//
// OFFSET is applied after execution too, and after HAVING, which is the order
// SQL specifies. Without HAVING the plan is asked for offset+limit rows so that
// skipping offset of them leaves limit behind.
//
// # Writing
//
// The driver writes as well as reads. A mutation goes through Exec and returns
// a driver.Result whose RowsAffected counts the documents whose stored state
// changed — created, replaced, or removed — and whose LastInsertId returns an
// error, because a document's key is a caller-supplied string and no integer
// stands for one. Reporting zero there would be answering a question the store
// does not understand.
//
//	INSERT INTO users VALUES ('u-1', ?)      -- key, then whole document
//	UPDATE users SET "$doc" = ? WHERE tier = 'free'
//	DELETE FROM users WHERE "$key" = ?
//
// The [sql] package documents what those statements mean and what they refuse:
// why INSERT takes a key and a document rather than a column list, why SET
// assigns only the whole document, and why the primary key is readable by
// exactly two condition shapes. The rest of this section is what the driver
// adds, which is atomicity and isolation.
//
// A statement is one atomic unit against a durable collection. Everything it
// does — the scan that decides which documents match, the reads a keyed
// statement performs, and the writes themselves — happens inside one
// durable.Collection.Update, which holds the collection's writer lock and
// publishes one generation. No other writer can intervene between the decision
// and the write, so an autocommit statement is serializable against every other
// writer with no conflict detection at all.
//
// A statement is not atomic against an in-process store.Database. That engine
// has Put and Delete and nothing that publishes several documents together, so
// a statement touching three documents is three publications. Every document a
// statement will write is validated before the first one is written, which
// removes the failure a caller actually produces — malformed or
// schema-violating JSON — but it does not make the statement one unit, and this
// is the difference the two backends have under identical syntax.
//
// A statement is bounded. A durable collection publishes at most
// Options.MaxBatchDocuments keys in one batch (64 by default), and a statement
// matching more is refused whole, naming both numbers, rather than split across
// two commits — because half of a statement is not a statement.
//
// # Transactions
//
// A transaction is one durable write batch, which is the largest atomic unit
// this engine has. It therefore writes exactly one collection: each durable
// collection has its own root, generation counter, and writer lock, and nothing
// in the engine publishes two of them together, so a statement naming a second
// collection is refused rather than written non-atomically. It is bounded by
// the same MaxBatchDocuments, checked as statements execute so the statement
// that would overflow the batch is the one that fails and the transaction stays
// usable. And it requires a durable collection: Begin over a store.Database is
// refused, because a transaction over it could only promise an atomicity it
// does not have.
//
// The isolation guarantee, stated precisely:
//
// Against readers, snapshot isolation, and it is the store's rather than this
// package's. Begin takes one durable.Snapshot holding a generation lease and
// every SELECT and mutation in the transaction reads that same generation plus
// the transaction's staged writes. A concurrent replacement therefore cannot
// change a repeated read and a concurrent matching insert cannot appear as a
// phantom. Visibility changes with one atomic store of the state pointer, so a
// commit is never partially visible.
//
// Against writers, snapshot isolation with first-committer-wins conflict
// detection. A transaction's statements execute when the application calls
// them against the begin snapshot and read the transaction's own staged writes;
// the writes themselves are held back until Commit. Commit re-reads, under the
// writer lock, every key the transaction wrote, compares the exact stored bytes
// against its begin-snapshot pre-image, and aborts the whole transaction with
// [ErrConflict] if any of them moved. Nothing is written on a conflict and the
// whole transaction may be retried.
//
// What that does not give is serializability. Snapshot isolation permits write
// skew and does not provide predicate conflict detection: a transaction whose
// condition matched three begin-snapshot documents continues to see those
// three, but it can commit alongside another transaction that inserted a fourth
// match because neither wrote what the other read. The alternative — holding
// the collection's writer lock from Begin to Commit — would turn an
// application's think time into a global write stall and a forgotten Rollback
// into a deadlock for every other writer in the process. An isolation level
// other than sql.LevelDefault or sql.LevelSnapshot is refused rather than
// silently downgraded.
//
// An autocommit statement is therefore stronger than a transaction containing
// only that statement, which is worth knowing rather than discovering.
//
// # Defining
//
// CREATE TABLE and CREATE INDEX go through Exec and report zero rows affected,
// which is the truth: RowsAffected counts documents and a definition writes
// none.
//
//	CREATE TABLE users (id STRING PRIMARY KEY, name TEXT, age INTEGER)
//	CREATE INDEX ON users (profile.region)
//
// Both are heap-only today, and both refusals are the engine's rather than this
// package's. CREATE TABLE creates a collection in a catalog; store.Database is
// one, and a durable collection is a single file with no catalog above it, so a
// CREATE TABLE against a file DSN has nowhere to go. CREATE INDEX adds an index
// to a live collection; the heap collection maintains new definitions and
// backfills existing documents, and a durable collection's indexes are frozen
// when the file is created. Each is refused by name rather than made a silent
// no-op, which is the one outcome that would leave a caller believing they had
// an index. A definition inside a transaction is refused too: a transaction is
// one document write batch, a catalog change is not part of it, and a rollback
// could not undo one.
//
// CREATE INDEX backfills to completion before it returns, because SQL's CREATE
// INDEX means the index is there when it returns and a caller who queried
// against a still-building one would get the scan fallback and conclude the
// index did nothing.
//
// The declared type vocabulary is JSON's, not SQL's, and what PRIMARY KEY
// enforces today is less than it declares. Both are documented in [sql].
//
// # What it will not do
//
// Exec of a SELECT is refused rather than run with its rows discarded, and
// Query of a mutation is refused rather than reported as an empty result. The
// two spellings are one character apart and the failure mode of guessing is
// silent.
//
// A context cancels a query before it starts and not once it is running. The
// executor has no cancellation hook, and checking a context in a loop that does
// not own the scan would be theatre.
//
// Named parameters are refused; the dialect has '?' placeholders, bound in
// source order.
//
// # Concurrency
//
// database/sql hands one connection to one goroutine at a time and runs many
// connections at once. Each connection owns its [query.Exec] — the buffers
// whose retained capacity makes a warmed execution allocation-free — and each
// prepared statement owns its own compiler and plan, so nothing execution
// touches is shared. The source is shared and is safe for concurrent readers by
// its own contract. Rows read the connection's Exec in place, which is safe
// precisely because database/sql holds the connection until they are closed.
//
// Writes add one shared thing, and it is the store's own: a durable collection
// has a single writer lock. Two connections mutating one collection serialize
// on it, which is what makes an autocommit statement atomic; a connection
// holding a transaction does not hold that lock between statements, which is
// what stops one application's think time from blocking every other writer, and
// is why a transaction needs the conflict check described above. A transaction
// belongs to the connection database/sql routed it to and is discarded if that
// connection is closed or reset with one still open.
//
// The auto-commit read path's allocation contract is unchanged by any of this.
// A warmed bind-execute-drain cycle still allocates nothing. A read-only
// transaction runs directly over its retained durable snapshot too. A SELECT
// after staged writes takes the general scan path and merges the begin snapshot
// with the bounded write set in O(collection rows + staged writes) time and
// O(staged writes) extra memory; it never copies the base collection. The
// ordinary write path allocates one string per matched key and one copy of each
// document written, and a transaction retains one copy of each pre-image the
// conflict check compares. The scan that decides which documents match
// allocates nothing per document examined: keys and documents are copied into
// retained buffers, and the filter batches them through a reused Segment.
//
// # The grammar
//
// See the [sql] package for the accepted grammar, the path language, the
// range-variable rule, and the list of constructs refused at parse time with a
// position.
package vibesql
