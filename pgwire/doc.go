// Package pgwire serves the sql/driver runtime over PostgreSQL protocol v3.
// It is a VibeDB protocol adapter, not a PostgreSQL catalog or server clone.
//
// The server supports simple query and extended Parse/Bind/Describe/Execute/
// Sync flows, prepared statements, portals, transaction status, cancellation,
// text and supported binary parameter/result formats, SCRAM-SHA-256, and TLS
// through PostgreSQL SSLRequest negotiation. pgx, lib/pq, and psql loopback
// interoperability is exercised in the repository.
//
// SQL behavior is shared with sql/driver: schemas, exact indexes, DML with
// RETURNING, views, generalized joins, derived tables, predicate subqueries,
// set operations, ordinary and bounded recursive CTEs, scalar/CASE/CAST
// expressions, aggregates/HAVING, output ordering, the documented window
// subset, multi-table transactions, savepoints, and EXPLAIN use the same parser
// and execution engine.
//
// Result metadata reflects the selected expression. Whole documents and
// ordinary JSON-path values use JSON where their representation remains JSON;
// COUNT and integer window outputs use int8; scalar and CASE/CAST outputs use
// the matching numeric, text, or boolean PostgreSQL representation when known.
// NULL is transmitted as protocol NULL, not the bytes "null".
//
// The protocol layer enforces bounded startup, statement, portal, parameter,
// result, connection, and cancellation state. One connection owns one
// single-consumer SQL Session. Errors map to stable SQLSTATE classes, including
// syntax/feature errors, ambiguity, serialization failure, unknown transaction
// outcome, cancellation, and resource exhaustion.
//
// A network listener must configure Options.TLSConfig, RequireTLS, and SCRAM
// authentication. Trust authentication and plaintext listeners are suitable
// only for an explicitly trusted local boundary. Traditional SSLRequest is
// supported; direct TLS negotiation and SCRAM-SHA-256-PLUS channel binding are
// not.
//
// The package does not implement a queryable pg_catalog, general ORM/BI schema
// discovery, replication protocol, COPY, LISTEN/NOTIFY, materialized views, or
// PostgreSQL's complete type, function, extension, and wire surfaces. Ordinary
// VibeDB views are read-only. Unsupported SQL is rejected rather than passed
// through or reinterpreted.
package pgwire
