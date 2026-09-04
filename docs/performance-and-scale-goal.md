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

User clarification: pursue a substantially better architecture, with breaking
changes allowed; competitor implementations are evidence, not the design target.

1. Connect serving to shared node storage and batching across ranges.
   Asynchronous startup/reload is now integrated and passed the Linux shipped
   crash/partition harness. Explicit node-log preparation and serving now share
   append/checkpoint ownership and pass initial multi-group restart tests.
   Complete live group registration, node-log fault qualification and benchmark
   integration; see [qualification scope](qualification/node-serving-2026-09-04/README.md).
2. Redesign committed/versioned read visibility so transactions need not block
   unrelated readers across a group. Retain serializable conflict validation.
3. Reduce fixed per-range allocation and duplicate log/data amplification, with
   compact versioned storage and measured reclamation under sustained writes.
4. Publish immutable schema generations while reads/writes retain older views;
   prove staging, backfill, cutover and recovery under load.
5. Measure mixed, skewed and multi-range workloads across node counts.

The [adaptive read-admission comparison](benchmarks/crdb-sql-2026-09-04-reads/README.md)
validated 120,000 more samples. C8 point hits: 10,200.5 ops/s; grouped scans:
1,242.6. All workloads still trail CRDB. The regressed precursor is retained.

The [asynchronous serving comparison](benchmarks/crdb-sql-2026-09-04-pipelined/README.md)
validated another 120,000 samples: C8 updates 2,416.1 ops/s versus CRDB 4,149.6.
Grouped-scan variance/regression is retained, not hidden. Diagnostic storage
accounting found about 3 GiB reserved across twelve per-member WALs. See the
[structural redesign target](storage-runtime-redesign.md).

Status: **active; no acceptance gate is complete**.
