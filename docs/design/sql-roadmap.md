# Embedded SQL gap plan

This plan targets VibeDB's embedded JSON database and `database/sql` driver.
It is not a plan for complete PostgreSQL compatibility. Pgwire and RF3 should
adopt a feature only after its embedded semantics and failure behavior are
stable.

## Release-critical gaps

| Priority | Gap | Why it is required | Smallest safe slice |
| --- | --- | --- | --- |
| P0 | Declared-column `UPDATE` execution | Accepted syntax must not fail or silently use whole-document semantics. | Execute top-level scalar literal and placeholder assignments atomically in autocommit, transactions, `RETURNING`, capture, routing, and index maintenance. |
| P0 | Executable `ALTER TABLE ... ADD COLUMN` | A parsed and lowered DDL statement must work through the public embedded driver. | Compile one immutable target schema, validate all rows in a shadow storage incarnation, retain indexes, and cut over atomically. |
| P0 | Primary-key `ON CONFLICT DO UPDATE` | Idempotent ingestion needs an atomic create-or-update operation; application-side read/replace races. | Support the implicit primary-key conflict target first, with `EXCLUDED` values and the same assignment evaluator as `UPDATE`. |
| P0 | Unique secondary constraints | Business keys cannot be enforced safely outside the write/commit boundary. | Add unique exact-index metadata and enforce it for inserts, updates, upserts, transactions, rebuilds, and reopen before adding richer constraint syntax. |

The first three slices are implemented.
Primary-key conflict updates are currently an embedded `VALUES` feature;
distributed/RF3 writes and `INSERT ... SELECT DO UPDATE` fail closed until they
can carry the same branch-aware atomicity contract. Unique secondary
constraints are the remaining P0 item.

## Required follow-on slices

| Priority | Gap | Deliverable |
| --- | --- | --- |
| P1 | Complete migration cycle | Add defaults/backfill, set or drop nullability, and rename/drop/type migration through the same atomic rebuild path. |
| P1 | Row-dependent mutation expressions | Execute assignments such as `score = score + 1` without an unsafe client read/replace cycle. Share one deterministic scalar evaluator with upsert. |
| P1 | Common scalar functions | Start with `COALESCE`, `NULLIF`, `LOWER`, `UPPER`, `LENGTH`, substring, and bounded numeric/JSON helpers. |
| P1 | Aggregate composability | Add `COUNT(DISTINCT ...)`, expression arguments, grouping expressions, and non-projected aggregates in `HAVING`. |
| P1 | Constraint basics | Add `DEFAULT` and `CHECK`. Treat foreign keys as a separate product decision for the JSON-first embedded scope. |
| P1 | Catalog discovery | Expose stable table, column, index, and constraint metadata through a small SQL-visible vendor catalog. |
| P2 | Secondary-index planning parity | Durable secondary ranges already exist. Add heap parity, direction metadata, and index-backed `ORDER BY`; do not build another range engine. |
| P2 | Online local schema copy | Move the shadow copy outside the exclusive catalog lock, reconcile concurrent changes, and keep only validation/cutover serialized. |
| P2 | Transactional DDL | Add only after catalog generations and rollback ownership can publish several DDL changes atomically. |

Full PostgreSQL schemas, roles/grants, `COPY`, procedures, notifications,
arbitrary PostgreSQL types, general `pg_catalog`, and cross-shard PostgreSQL
transactions are outside the embedded must-have set.

## Fast merge order

1. Close accepted-syntax/runtime mismatches: declared-column `UPDATE`, then
   additive `ALTER TABLE`.
2. Add primary-key `ON CONFLICT DO UPDATE` without waiting for secondary
   uniqueness.
3. Freeze catalog metadata for unique indexes, defaults, and checks.
4. Implement unique enforcement and the shared mutation-expression evaluator.
5. Build the remaining migration actions on the storage-incarnation seam.
6. Add scalar and aggregate composability in independent files after the AST
   contract is stable.
7. Promote stable embedded behavior to pgwire and RF3 where those products
   support the same atomicity contract.

Parser/AST, catalog encoding, mutation integration, and storage replacement are
hotspots. Give each one an exclusive owner within a merge wave; use new files
for evaluators, unique enforcement, and upsert logic, with one integration owner
for `write.go` and `tx.go`.

## Verification cadence

- During an edit loop, run named tests for the changed behavior only.
- At a slice safe point, run the complete affected package plus focused tests
  for adjacent boundaries such as pgwire tags, SQLSTATEs, indexes, transactions,
  reopen, and routing.
- At a catalog or mutation milestone, run the core SQL, query, driver, pgwire,
  gateway, and affected store packages.
- Before release or merging a multi-slice milestone, run the serialized full
  suite, build/vet, cross-compilation, and the relevant race, crash, fault, RF3,
  and process tests.

Large K8s, RF3 process, fault-injection, race, allocation, and benchmark suites
belong at those safe points, not on every implementation commit.
