// Package driver exposes VibeDB's JSON/SQL runtime through database/sql and a
// lower-level Session API.
//
// Importing the package registers driver name "vibedb". A database/sql DSN is
// the durable catalog path. Open and Connector APIs are also available when a
// caller needs explicit lifetime, options, or session control.
//
// SQL tables store canonical JSON documents in the same durable collection
// engine as the native facade. A schema constrains required paths and scalar
// kinds; exact indexes map one or more JSON paths to exact terms. DDL publishes
// one catalog statement atomically. DML publishes primary rows and exact-index
// postings in one generation cut.
//
// The driver executes the current sql package surface, including views,
// INSERT from values or queries, RETURNING, generalized joins, derived tables,
// predicate subqueries, set operations, ordinary and bounded recursive CTEs,
// scalar/CASE/CAST expressions, aggregates, HAVING, output ordering, windows,
// and EXPLAIN. Unsupported shapes return typed errors; see
// docs/reference/sql.md for the exact boundary.
//
// Transactions may read and write multiple tables. Read Committed is the
// database/sql default; Repeatable Read/Snapshot and Serializable are admitted
// explicitly. Multi-table commits on the fixed synchronous SQL durability
// profile use the catalog transaction decision log and are crash-atomic.
// SAVEPOINT, ROLLBACK TO, and RELEASE operate on bounded staged overlays. DDL
// inside a transaction is refused.
//
// Sessions are single-consumer. A *sql.DB may be shared and database/sql owns
// connection pooling. Prepared statements are reusable; each execution binds
// its own parameters and bounded workspace. Cancellation is checked during
// parsing, planning, scans, joins, sorting, grouping, windows, mutation
// staging, and result publication.
//
// Catalogs use one current unreleased development grammar. Open accepts that
// grammar exactly and has no migration reader for older development images.
// Tables are stored in the catalog's owned table directory with identity-bound
// primary and recovery-journal sidecars. A persistence error may have an
// unknown commit outcome; callers must close, reopen, and reconcile rather
// than blindly retrying the same handle.
//
// OpenCluster and OpenClusterConnector add one-shard placement/write preflight
// to an embedded catalog. They do not start a network service or provide
// replication. InitializeShardStore/OpenShardStore are the stricter identity
// boundary used by the experimental shard runtimes.
package driver
