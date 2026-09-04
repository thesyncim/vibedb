# Active performance and scale goal

User-authorized on 2026-09-04: redesign unreleased VibeDB freely, merge/push
validated changes, and continue until it is substantially faster than competitors
while being space efficient, horizontally scalable, and supporting nonblocking
schema changes. The task's active goal carries this objective across iterations.

## Acceptance gates

- At least 2x CockroachDB throughput on a representative, documented, matched
  workload matrix. Include point hits/misses, ordered ranges, grouped scans,
  inserts, updates, deletes, mixed workloads, contention, and cross-shard
  transactions; report p50/p95/p99, errors, CPU, memory, and disk bytes.
- Comparable durability and consistency for the measured operation classes.
  Keep default replicated durability on. Validate restart, lost replies,
  leader changes, partitions, and overlapping transactions. Unsupported SQL
  transaction semantics cannot count as parity.
- Demonstrated horizontal read/write scaling across node counts, with uniform
  and skewed placements, growth/rebalance and hot-key limits made explicit.
  Single-host process scaling does not prove independent-machine scaling.
- Measure allocated disk, logical data, retained history/metadata, temporary
  space and peak amplification through sustained mutations and reclamation.
  Empty-cluster allocation is distinct from data-dependent amplification.
- Schema evolution must allow ordinary reads/writes to progress during staging,
  backfill and cutover, with bounded retention and crash recovery. No claim that
  arbitrary schema operations are lock free merely because one lock was removed.

No gate passes from a microbenchmark, favorable subset, weakened durability,
missing result verification, or untested generalization. The benchmark matrix
must grow beyond the initial single-host autocommit comparison.

## Baseline and known gaps

Clean source `3e9d2306`; [complete comparison](benchmarks/crdb-sql-2026-09-04-concurrent/README.md):
120,000 verified samples. VibeDB updates: 390.5 ops/s C1 and 2,029.3 C8;
CockroachDB: 909.4 and 3,966.8. All measured read workloads are also slower.

The current PostgreSQL endpoint does not implement full serializable interactive
transactions; a per-statement quorum read is not a serializable transaction.
Schema and multi-node scale gates are unproven. Initial disk observations include
large fixed allocations and are insufficient for a space-efficiency claim.

## Current engineering priorities

1. Replace maximum-size SQL workspace reservation for every read with bounded,
   growing reservations. Preserve result/error behavior, total deadlines,
   ReadIndex fences and release-before-wait discipline.
2. Profile remaining replicated write execution, remove avoidable work and batch
   durable operations without changing acknowledgment guarantees.
3. Extend honest tests and measurements to data growth/reclamation, multiple
   active ranges, mixed and contested transactions, and schema changes under load.
4. Use those results to select storage, replication, execution and schema
   redesigns. Maintain raw failed runs alongside successful reruns.

Status: **active; no acceptance gate is complete**.
