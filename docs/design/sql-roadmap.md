# Embedded SQL gap plan

This plan targets VibeDB's embedded JSON database and `database/sql` driver.
It is not a plan for complete PostgreSQL compatibility. Pgwire and RF3 should
adopt a feature only after its embedded semantics and failure behavior are
stable.

## Release-critical foundation status

| Status | Slice | Implemented boundary |
| --- | --- | --- |
| **Complete** | Direct declared-column `UPDATE` | Embedded autocommit, transactions, `RETURNING`, capture, routing, and index maintenance execute atomic top-level scalar literal, placeholder, and `NULL` assignments. Computed expressions are tracked separately below; nested assignments remain outside this slice. |
| **Complete (embedded/static)** | Row-dependent `UPDATE` expressions | Embedded `UPDATE` evaluates arithmetic, concatenation, unary expressions, casts, and `CASE` once per matched old row. Mixed assignments are simultaneous and atomic across `RETURNING`, transactions, primary-key checks, and local secondary/unique indexes. The additive mutation-image capture returns exact canonical before/after images, and the static gateway uses those images for maintained global indexes with before- and after-image guards. The legacy two-column capture and RF3 still reject computed assignments. |
| **Complete** | `ALTER TABLE ... ADD COLUMN` | The embedded driver validates one additive schema in a replacement storage incarnation, retains indexes, and publishes atomically. The copy still blocks ordinary work under the catalog write lock. |
| **Complete** | Primary-key `ON CONFLICT DO UPDATE` | Embedded `INSERT ... VALUES` supports whole-document replacement or declared top-level assignments from literals, placeholders, `NULL`, and `EXCLUDED`. The implicit primary key is the only target. |
| **Complete (embedded)** | Conflict-action expressions | Embedded `INSERT ... VALUES ... ON CONFLICT DO UPDATE` evaluates arithmetic, concatenation, unary expressions, casts, and `CASE` over explicitly qualified current-row and `EXCLUDED` namespaces. Mixed assignments are simultaneous, exact-number preserving, statement-atomic, transaction-aware, and integrated with `RETURNING` plus local secondary/unique indexes. Distributed lanes retain their blanket conflict-action fence. |
| **Complete (embedded)** | Mutation target aliases and conflict binding diagnostics | `UPDATE table [AS] alias` and `INSERT INTO table AS alias` preserve the physical collection while binding expressions and `RETURNING` through the effective target name. Aliases hide the original name, disambiguate a table literally named `excluded`, and pgwire reports hidden/ambiguous relations as `42P01`/`42P09`. Bare conflict-action names are classified eagerly against the transaction-visible catalog as declared-and-ambiguous (`42702`) or undefined (`42703`), including stale prepared plans. Distributed conflict actions remain fenced. |
| **Complete** | Unique secondary constraints | Embedded `CREATE UNIQUE INDEX` enforces exact scalar tuples across build, DML, upsert, transactions, reopen, aliases, and drop. Default `NULLS DISTINCT` applies. RF3 SQL creation remains fail-closed. |

These slices complete the original embedded P0 list. Embedded pgwire exposes
the same behavior. RF3 does not inherit upsert or unique-index DDL until it can
provide the same branch-aware and distributed uniqueness contracts.

## Required follow-on slices

| Priority | Gap | Deliverable |
| --- | --- | --- |
| P1 | Complete migration cycle | Add defaults/backfill, set or drop nullability, and rename/drop/type migration through the same atomic rebuild path. |
| P1 | RF3 mutation postimages | Carry the canonical evaluated postimage contract already used by static global-index maintenance into durable RF3 mutation planning and replay, then remove the RF3 computed-`UPDATE` fence. RF3 `RETURNING` remains a separate ordered-result problem. |
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

## Fast next plan

1. Extend the canonical mutation-image contract into RF3's durable request and
   mutation payloads. Keep the computed-`UPDATE` fence until replay, crash, and
   global-index tests prove the retained postimage is identical after recovery.
2. Add the first common scalar functions without
   widening the expression grammar beyond executable runtime support.
3. Add catalog metadata for `DEFAULT` and `CHECK`, then extend the existing
   storage-incarnation path with backfill and nullability changes.
4. Add aggregate composability and a small SQL-visible vendor catalog in
   independent lanes.
5. Add heap/range-planning parity and move local schema copies outside the
   exclusive catalog lock.
6. Promote an embedded feature to RF3 only after its coordinator, capture,
   routing, and failure behavior can preserve the embedded invariant.

Parser/AST, catalog encoding, mutation integration, and storage replacement
remain hotspots. Give each one an exclusive owner within a merge wave.

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
