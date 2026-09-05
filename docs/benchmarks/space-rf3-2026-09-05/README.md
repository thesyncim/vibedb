# RF3 space reclamation from main 8e4e60f6

Measurements began on `8e4e60f6`. The final candidate was rebased onto fetched
main `b63e96c0`; storage and Raft directories were unchanged between those
bases. Linux storage, runtime, and three-server checks were rerun after the
rebase. The microbenchmark tables continue to describe their original builds.

The first useful architectural saving is to reclaim superseded shared-node
Raft history. The existing production checkpoint worker advanced application
checkpoints, but did not call either `CheckpointDescriptorCatalog` or
`ReclaimDeadNodeLogPrefix`. Descriptor entries could therefore pin the oldest
shared segment indefinitely, and the physical dead-prefix reclamation protocol
had no production caller.

The change connects those existing operations to the checkpoint coordinator's
serial background worker. An unchanged descriptor catalog needs no new file,
directory scan, or durability wave. Busy maintenance is retried on a subsequent
capture. Existing live-entry fences, sealed-summary fences, count/byte
thresholds, reserve allocation, and checkpoint frequency stay the same.

## Measured space

The current compact primary representation already stores the deterministic
100,000-row competitive corpora as follows. These are complete primary-file
sizes from `TestUnifiedSpaceCompetitiveCorpus`, excluding paired journals,
Raft logs, and other database files. The logical documents average 248.8 bytes.

| Corpus | Primary file bytes | Bytes/row | Compact payload bytes/row | Leaf extent bytes/row |
| --- | ---: | ---: | ---: | ---: |
| Repetitive | 1,019,904 | 10.199 | 8.956 | 9.052 |
| High cardinality | 6,930,432 | 69.304 | 68.017 | 68.157 |

The three-replica reclamation regression uses two groups per node, eight
128-KiB history records per primary group, 1-MiB segments, and the actual
authenticated shared log. It deliberately leaves an entry in the second group
live while checkpointing the first group. Reclamation must retain the entire
shared prefix until the second group's checkpoint is durable and sealed.

On Linux/ARM64, including active and spare segment allocations:

| Accounting | Before reclaim | After reclaim | Saved |
| --- | ---: | ---: | ---: |
| Apparent bytes across three node directories | 3,232,374 | 64,752 | 3,167,622 (98.00%) |
| Allocated bytes across three node directories | 17,547,264 | 14,303,232 | 3,244,032 (18.49%) |

This measures history reclamation in a small constructed fixture. It is not a
whole-database compression ratio, a default-geometry capacity estimate, or a
three-node network benchmark. The test also writes and reads another entry
after reclamation, reopens all three nodes, and checks both group checkpoints
and the new entry.

## Architectural priorities

1. **Reclaim certified dead shared-log history.** Implemented here. Saving is
   proportional to the dead history, and applies independently on every RF3
   replica. A slow group continues to pin shared segments. Sparse dead holes
   after a live segment remain retained; this change does not rewrite them.
2. **Keep reserve capacity distinct from live bytes.** Default node geometry
   reserves 32 MiB for the active segment and 64 MiB for two spares: 96 MiB per
   physical node, or 288 MiB over three nodes. This is per physical node, not
   per Raft group. Reducing it would change rotation headroom and the largest
   admissible wave, so it is not a free space saving under a write-latency
   constraint.
3. **Account for collection journals and hidden collections.** The compact
   buffered delta journal reserves 2.5 MiB per collection, including a 512-KiB
   carry allowance. Shrinking that allowance or sharing journals requires
   separate checkpoint-cadence, recovery, and contention qualification. The
   primary-file numbers above do not include this replicated overhead.
4. **Reclaim obsolete group checkpoint certificates.** Group checkpoints use
   new filenames and currently retain old certificates. These are compact
   certificates, not local copies of full streamed database snapshots. Their
   lifecycle needs its own authenticated cleanup and recovery tests; no
   speculative deletion is added here.
5. **Further primary compression needs a read-cost case.** Current leaf slack
   is only about 0.08/0.07 bytes per row. High-cardinality alphabet streams
   account for about 62.11 bytes per row. More compression there would change
   decoding and update work, so it requires measured point, scan, projection,
   insert, and replacement results before adoption.

## Performance and correctness boundary

No document encoding, read algorithm, mutation encoding, foreground append
method, or durability acknowledgement boundary changes. Maintenance runs
before a scheduled application capture; physical reclamation can lag a
checkpoint until a later capture and a qualifying sealed prefix. An idle
node does not receive a new forced checkpoint or forced segment rotation.

Two blocking-I/O regressions pause descriptor checkpoint work and dead-prefix
checkpoint work while proving that current reads and durable active appends
still complete. These establish that maintenance does not hold the foreground
mutex across that I/O. Device bandwidth contention and rotation backpressure
remain possible and require sustained workload measurements.

The retained evidence distinguishes apparent bytes, allocated blocks, unit
recovery checks, race checks, and microbenchmarks. No sustained fused RF3 SQL
p99 or network throughput claim follows from the storage regression.

Six samples per arm on an Apple M4 Max, Go 1.27 with SIMD, alternate
before/after and after/before order. The base is the clean `8e4e60f6` worktree;
the benchmarked runtime-source hashes and all four binary hashes are in
[environment.json](environment.json). Each benchmark uses one Go CPU and a
200-ms target. Other work can run on this shared host.

| Microbenchmark | Base median | Candidate median | Two-sided p |
| --- | ---: | ---: | ---: |
| Raw point read | 743.8 ns | 847.0 ns | 0.589 |
| Primary replacement | 4.601 us | 4.786 us | 0.093 |
| Low-cardinality full scan | 19.62 ms | 19.55 ms | 0.589 |
| High-cardinality full scan | 28.42 ms | 28.46 ms | 0.937 |
| Synchronous insert, one writer | 8.400 ms | 7.557 ms | 0.093 |
| Synchronous insert, eight writers | 4.026 ms | 3.908 ms | 0.818 |
| Synchronous insert, 64 writers | 950.4 us | 910.9 us | 0.937 |
| Node durability wave, one group | 8.066 ms | 8.800 ms | 0.394 |
| Node durability wave, eight groups | 8.220 ms | 9.150 ms | 0.485 |

**Performance is not qualified as equivalent.** No elapsed-time difference is
statistically significant in these short samples, but several medians are
slower and dispersion is large. Replacement p50 remains 1.625 us in both arms;
the candidate's mean-time samples include a large outlier. Raw point reads,
scans, replacements, and node durability waves report zero allocations per
operation at the benchmarks' reporting precision. These microbenchmarks do
not activate the new background maintenance path, so they cannot establish
the absence of device contention during sustained RF3 reclamation.

Final review changed the maintenance error branches to use the atomic
published-failure/close fence and preserve an independent engine failure when
metadata is unavailable. This path is outside the measured microbenchmarks.
Both the benchmarked and final source hashes are recorded, and a dedicated
failure regression plus final race/Linux reruns cover the final code.

Raw results and complete benchstat comparisons are retained in
[read-benchmarks](read-benchmarks/comparison.txt) and
[write-benchmarks](write-benchmarks/comparison.txt). The first write attempt
overlapped the Linux test run and was stopped; its partial results remain in
[interrupted-benchmarks](interrupted-benchmarks/commands.json) and are excluded
from every table. The write selection included all writer counts and omitted
root-level read benchmarks; the separate complete read campaign supplies the
read/replacement rows above.

Validation passed:

- The complete Linux `internal/raftstore`, `internal/raftstore/seglog`, and
  `internal/raftmember` suites on a dedicated Docker volume.
- Darwin race tests for node maintenance, descriptor checkpoints, and all
  `TestReclaim*` cases, including the new concurrent-I/O regression.
- The real node checkpoint worker integration, which verifies that a normal
  application capture publishes the descriptor checkpoint too.
- Shared-log preparation, three-server RF3 restart, and recovery across the
  segment entry-capacity boundary in `cmd/vibedb-shard`. The short server run
  does not itself reach the default ten-minute checkpoint interval.

An earlier Linux run used the container's overlay filesystem and skipped the
strict-allocation worker integration. That output is retained as
[linux-overlay-skipped.txt](linux-overlay-skipped.txt); the dedicated-volume
rerun in [linux-volume-focused.txt](linux-volume-focused.txt) passes it.

## Source map

- [Node log maintenance](../../../internal/raftstore/node_catalog.go)
- [Production checkpoint worker](../../../internal/raftmember/node_checkpoint.go)
- [Three-replica storage regression](../../../internal/raftstore/node_maintenance_test.go)
- [Reclamation and crash tests](../../../internal/raftstore/seglog/reclaim_test.go)
- [Production worker integration](../../../internal/raftmember/runtime_node_persistence_test.go)
