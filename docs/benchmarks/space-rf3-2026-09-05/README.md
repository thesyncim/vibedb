# Shared-node RF3 history reclamation

The implemented saving removes obsolete Raft history on each physical replica.
It connects the existing authenticated dead-prefix reclamation protocol to the
background application-checkpoint worker and fixes an admission guard that
mistook an unread, completed seal notice for unfinished seal publication.

Checkpoint scheduling, document representation, foreground reads and writes,
acknowledgement boundaries, segment geometry, and reserve capacity are unchanged.
The comparison uses main `b2f716ec` and candidate `858a834d`. The integration
branch was subsequently rebased onto main `3021bece`; the storage/runtime source
and benchmark harness were byte-for-byte identical across that rebase. It was
then rebased onto latest main `f05df25e` for the deeper format research. That
newer main changes sidecar allocation and read paths, so the old timings must
not be relabeled as a comparison against it.

## Measured RF3 result

The nine-pair `858a834d` comparison saved 48.6% of allocated node-log bytes,
but **did not meet the performance requirement**: paired total-time ratio was
1.050 (95% interval 1.008–1.104), insert p99 1.125, update p99 1.093, and read
p99 1.164. All correctness trials passed. The complete
[raw comparison](final-sustained/node-space-comparison/comparison.json) is
retained; the `final-sustained` directory name identifies this narrowed
maintenance candidate, not a qualified final implementation. The change remains
unmerged while the regression and stronger architectural alternatives are
investigated. The completed [Astra max format research](../format-research-2026-09-05/README.md)
examines primary encoding, overflow geometry and a replicated recovery format
that could eliminate duplicate redo. The incomplete body-retirement experiment
was preserved outside the active source tree pending that architecture choice.

## Workload and accounting

The dedicated Linux CI job builds both revisions before measurement, then runs
nine alternating pairs on one runner. Both binaries contain the exact same
qualification test. Each fresh fixture has three independent server processes,
production 32-MiB node segments, and two persistent authenticated clients. Every
trial performs 1,024 inserts, 3,072 replacements, and 2,048 linearizable full-value
reads of 64-KiB documents. The test shortens application checkpoint cadence to
two seconds in **both test binaries**; production retains ten minutes.

Physical accounting includes every node-log inode, metadata, and active/spare
allocation across all three replicas after all servers have detached. It excludes
SQL primary files, collection journals, and gateway files. The measured saving
is therefore a node-log retention result, not a whole-database compression ratio.

Foreground timings include client command encoding, hashing, transport, durable
RF3 acknowledgement, and full-value reads. They exclude setup, document
construction, oracle canonicalization, and recovery. Every operation sample,
including stalls, is retained. Each trial verifies acknowledged values after a
full-cluster restart and after crashing/restarting each member, outside its timed
interval. Candidate trials additionally require an old segment to disappear and
an application checkpoint to advance on every replica.

Reported ratios are geometric means of complete candidate/base trial pairs;
95% bootstrap intervals resample paired trials, not individual operations. Nine
pairs characterize this workload and runner, and cannot prove identical latency
for every workload or storage device.

## Why these bytes were retained

Application checkpoints already logically truncate group history. Descriptor
catalog entries still pinned the oldest shared segment because the production
worker never published their checkpoint or requested physical reclamation. The
worker now checkpoints newly registered descriptors and asks the serial sealer
to reclaim a fully dead prefix before its normal application capture. Unchanged
descriptor catalogs avoid an extra file, directory scan, and durability wave.

A second blocker remained after checkpoint publication: `sealPending` includes
an already published seal whose completion notice will be consumed by the next
rotation. Reclamation runs on that same serial worker and now checks the
authenticated `HasPending` publication state. It leaves the completion notice
for the existing rotator or `WaitSeal` consumer. The new regression reclaims
before `WaitSeal` and verifies both the retained entry and the completion.

The existing live-group and sealed-summary fences still decide which segments
may be removed. A slow group pins its shared segments; dead holes behind a live
segment are not rewritten. Count/byte thresholds and crash-safe publication
remain intact. Reclamation can lag a checkpoint until another scheduled capture
and a qualifying sealed prefix. Idle nodes receive no forced rotation.
Each request retires at most 32 segments (up to 1 GiB with default segment
capacity). A larger backlog drains across captures; this change does not claim
a fixed retention bound under arbitrary write rates or slow groups.

## Architectural space budget

The [initial investigation](initial-investigation.md) includes the full primary
file breakdown, short microbenchmarks, and the original small storage fixture.
Those earlier microbenchmarks do not exercise background reclamation and are not
the final RF3 performance evidence.

| Component | Existing allocation or representation | Decision |
| --- | --- | --- |
| Shared-node Raft history | Superseded entries retained behind descriptor and completed-seal blockers | Reclaim the authenticated dead prefix on each replica |
| Active segment and two spares | 96 MiB per physical node, 288 MiB across RF3 | Preserve rotation headroom |
| Ordinary compact collection journal | 2.5 MiB per collection, including 512-KiB carry allowance | Separate from the replicated SQL profile |
| Replicated SQL sidecars | About 16 MiB per user relation and system collection, plus a 1-MiB transaction window per apply group, on every replica | Investigate eliminating redo already durable in Raft |
| Compact primary, repetitive corpus | 10.199 bytes/row over 100,000 rows | Preserve current encoding |
| Compact primary, high-cardinality corpus | 69.304 bytes/row over 100,000 rows | Preserve current encoding |
| Obsolete group checkpoint certificates | Small certificates, not full streamed snapshots | Separate authenticated cleanup work remains |

The primary corpora average 248.8 logical document bytes. Leaf slack is already
only about 0.08/0.07 bytes per row. Additional high-cardinality compression would
change decoding and mutation work, so this change targets retained history.

The replicated SQL journal geometry is defined in
[replicated_sidecars.go](../../../sql/driver/replicated_sidecars.go): each user
relation reserves 16,794,624 record bytes plus two 512-byte header sectors; the
system journal reserves at least 16,777,216 record bytes plus headers. The
ordinary 2.5-MiB journal figure in the initial investigation must not be used as
the production replicated SQL budget. Latest main `f05df25e` selects portable
sealed capacity: those exact logical sizes are not a universal physical backing
guarantee on every filesystem. A deeper review is examining whether
Raft can supply redo without duplicating full values in those sidecars.

## Correctness and evidence

- Complete Linux `raftstore`, `seglog`, and `raftmember` suites pass:
  [final maintenance run](linux-final-maintenance.txt).
- Targeted Darwin race checks pass, including concurrent maintenance,
  descriptor publication, and reclamation:
  [final race run](race-final-maintenance.txt).
- Blocking-I/O regressions prove that active appends and current reads complete
  while descriptor and reclamation checkpoint I/O is paused.
- The three-replica storage regression checks that a live second group pins the
  shared prefix, then verifies continued writes, reads, and reopen recovery.
- The real checkpoint worker integration verifies descriptor checkpoint
  publication through the production coordinator.
- The [final process validation](final-process-validation/go-test.jsonl) passes
  with initial-segment removals `[3, 3, 3]` and checkpoint indexes
  `[3752, 4052, 4063]`, including all restart/crash checks.

[Validation notes](validation-notes.md) retain exploratory failures, CI retries,
and the earlier scheduling experiment. That extra scheduling change was removed
before the final comparison. [sha256.json](sha256.json) inventories the evidence.

## Source map

- [Node maintenance](../../../internal/raftstore/node_catalog.go)
- [Background checkpoint worker](../../../internal/raftmember/node_checkpoint.go)
- [Reclamation admission](../../../internal/raftstore/seglog/reclaim.go)
- [Multi-group storage regression](../../../internal/raftstore/node_maintenance_test.go)
- [Reclamation, concurrency, and crash tests](../../../internal/raftstore/seglog/reclaim_test.go)
- [RF3 process qualification](../../../cmd/vibedb-shard/node_space_process_qualification_test.go)
- [Paired comparison runner](../../../scripts/bench/run-node-space-comparison.py)
