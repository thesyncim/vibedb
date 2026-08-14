// Package sql parses VibeDB's bounded SQL dialect into an explicit abstract
// syntax tree. Execution is provided by package query and sql/driver; this
// package never opens storage or runs a statement.
//
// The dialect is SQL over JSON documents, not a PostgreSQL parser. Supported
// statement families include:
//
//   - SELECT with projections, predicates, aggregates, GROUP BY, HAVING,
//     DISTINCT, ORDER BY aliases/positions, LIMIT/OFFSET, derived tables,
//     ordinary and recursive CTEs, predicate subqueries, set operations,
//     generalized joins, LATERAL, scalar CASE/CAST expressions, and the
//     documented window-function subset;
//   - INSERT from values or a query, UPSERT in the documented primary-key
//     forms, UPDATE, and DELETE, including supported RETURNING and bounded
//     mutation ordering/limits;
//   - CREATE/DROP TABLE, CREATE/DROP INDEX, CREATE/DROP VIEW, and TRUNCATE;
//   - BEGIN/COMMIT/ROLLBACK plus SAVEPOINT, ROLLBACK TO, and RELEASE; and
//   - EXPLAIN and EXPLAIN ANALYZE for executable statements.
//
// The exact accepted and refused shapes are documented in
// docs/design/sql-surface.md. A syntactically representable AST is not a promise
// that every execution surface supports that shape: the embedded driver,
// pgwire server, and distributed gateway each apply their own semantic and
// resource admission.
//
// Parsing is bounded. Clause counts, nesting, joins, parameters, expressions,
// paths, CTEs, windows, and arena growth have explicit limits. ParseContext
// provides cancellation. Limit failures and unsupported constructs return
// typed, positioned errors instead of silently changing semantics.
//
// A Parser owns the strings, slices, and nodes in its result. The AST remains
// valid only until the next call on that Parser. Parse is the convenience form
// for callers that need one statement and do not reuse parser storage.
// Positions are UTF-8 byte offsets into the original SQL text.
//
// Paths are bound identities, not strings left for execution to reinterpret.
// SELECT output aliases and positive ORDER BY positions are resolved during
// parsing. Ordinary path aliases reuse the selected path; scalar, aggregate,
// and window aliases bind to their one-based output. Duplicate matching output
// aliases are ambiguous. Qualified names never resolve as output aliases.
//
// NULL and a missing JSON path are distinct in the AST/runtime contract.
// Predicates use SQL three-valued logic, while exact JSON storage and indexes
// preserve the engine's canonical JSON identity rules.
package sql
