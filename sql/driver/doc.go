// Package driver registers the "vibedb" database/sql driver over durable
// collections and the query executor.
//
// The DSN is a catalog file path. CREATE TABLE records a table and its one
// declared JSON-path primary key in that catalog without creating a collection
// file. The first INSERT creates the table's collection beside the catalog.
// Unindexed tables use the mutable durable layout; a table with indexes is
// bulk-built from that first INSERT because CreateFromPrimary cannot represent
// an empty collection.
//
// INSERT INTO t VALUES (?) accepts a string or []byte containing one JSON
// document and derives its storage key from the declared primary-key path.
// INSERT INTO t (a, b) VALUES (?, ?) builds a flat JSON object. Put supplies
// upsert semantics. SELECT uses query.Statement; primary-key equality and IN
// first use durable Snapshot.AppendRaw point reads, while other supported
// predicates run through query's scan and exact-posting candidate paths.
// COUNT(*), LIMIT, prepared placeholders, concurrent readers, whole-document
// UPDATE, DELETE, and transactions work on unindexed tables.
//
// BEGIN captures a generation-leased snapshot of every table then present in
// the catalog. Every transactional SELECT and mutation reads that fixed
// generation overlaid with the transaction's own bounded write set, providing
// repeatable reads, phantom exclusion, and read-your-writes. The current
// implementation materializes that merged view in one pass per statement.
// Writes in one transaction must name one table, because one
// durable.Collection.Update is the engine's atomic publication unit. COMMIT
// checks each written key's begin-time pre-image (first committer wins), then
// records all changes in one durable.WriteBatch; ROLLBACK discards the overlay.
//
// Exact indexes are frozen into the first durable generation. Autocommit
// mutations return ErrIndexedTableReadOnly. Transactional indexed mutations
// return ErrTransactionIndexedTable, wrapping
// durable.ErrPrimaryBatchIndexedUnsupported. Transactions that exceed the
// collection's document or byte reservation return ErrTransactionTooLarge,
// wrapping durable.ErrBatchTooLarge. An ordered-primary async-visible lane
// returns ErrTransactionUnsupportedLane, wrapping
// durable.ErrPrimaryBatchUnsupportedLane. These gates fail without publishing
// a partial transaction.
//
// JSON null and a missing projected path both scan as nil. bool scans as bool;
// an integral JSON number that fits scans as int64; other numbers retain their
// exact decimal spelling as []byte; strings scan as decoded []byte; objects and
// arrays scan as JSON []byte.
package driver
