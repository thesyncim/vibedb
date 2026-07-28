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
// UPDATE, and DELETE work on unindexed tables.
//
// Exact indexes are frozen into the first durable generation. Once an indexed
// table has materialized, INSERT, UPDATE, and DELETE return
// ErrIndexedTableReadOnly until mutable exact postings land. BEGIN returns
// ErrAutocommitOnly until the ordered-primary transactional batch surface
// lands. These refusals fail before mutation; neither feature is simulated.
//
// JSON null and a missing projected path both scan as nil. bool scans as bool;
// an integral JSON number that fits scans as int64; other numbers retain their
// exact decimal spelling as []byte; strings scan as decoded []byte; objects and
// arrays scan as JSON []byte.
package driver
