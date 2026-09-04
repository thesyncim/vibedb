# Distributed SQL result encoding

Continues after PR #137 (`b3acdfd7`), including main's bounded-scan changes.
Ordinary row responses now retain one cell slab and their owned read frame,
matching the existing row-batch ownership model. Each exposed row and cell
uses a capacity-clipped slice. RF3 SQL serializes a borrowed cursor directly
into one owned, preflighted frame before releasing its read cut. The wire
grammar, quorum proof, result limits, admission reservation, and fallback tiers
remain the same.

The direct encoder compares byte-for-byte with the former materialize-and-encode
path, including exact numeric values, NULLs, escaped text, HAVING, OFFSET, empty
results, cancellation and exact byte boundaries. The decoder tests verify
independent ownership from input buffers and append isolation across rows/cells.
Full shardservice and gateway suites pass, as do focused race tests. Initial
sandbox runs could not bind local TCP listeners; the full suites passed with
local networking allowed.

## End-to-end results

The public PostgreSQL path ran against one persistent 500,000-row RF3 fixture.
Every operation checked its returned values and order; each matrix also ran full-
table verification before and after. All 120 main-matrix trials completed with
zero errors. Each cell below is the median of three trials. Ratios are shown
separately for both execution orders rather than combining noisy trials.

### Pair 1: `before` then `after`

| Query | Clients | Baseline ops/s | Changed ops/s | Throughput | Baseline p50 µs | Changed p50 µs |
|---|---:|---:|---:|---:|---:|---:|
| point_hit | 1 | 3,511.4 | 3,416.1 | -2.7% | 264.7 | 269.0 |
| point_hit | 8 | 17,733.6 | 17,691.3 | -0.2% | 417.9 | 419.5 |
| point_miss | 1 | 3,514.5 | 3,574.5 | +1.7% | 264.9 | 256.7 |
| point_miss | 8 | 18,311.5 | 18,635.9 | +1.8% | 405.7 | 397.2 |
| range_32 | 1 | 2,878.1 | 2,843.1 | -1.2% | 327.3 | 331.7 |
| range_32 | 8 | 14,462.5 | 13,692.1 | -5.3% | 509.8 | 529.8 |
| range_64 | 1 | 2,643.6 | 2,556.2 | -3.3% | 358.4 | 368.8 |
| range_64 | 8 | 11,444.7 | 13,146.8 | +14.9% | 613.4 | 552.2 |
| range_256 | 1 | 1,336.7 | 1,318.8 | -1.3% | 673.2 | 616.7 |
| range_256 | 8 | 6,485.4 | 6,984.6 | +7.7% | 1105.8 | 1025.4 |

### Pair 2: `after-confirm` then `before-confirm`

The table keeps baseline and changed as its column order; execution used the
reverse order shown above.

| Query | Clients | Baseline ops/s | Changed ops/s | Throughput | Baseline p50 µs | Changed p50 µs |
|---|---:|---:|---:|---:|---:|---:|
| point_hit | 1 | 3,547.4 | 3,393.8 | -4.3% | 259.2 | 272.7 |
| point_hit | 8 | 17,504.5 | 14,033.6 | -19.8% | 412.3 | 450.1 |
| point_miss | 1 | 3,363.5 | 3,503.4 | +4.2% | 271.2 | 257.2 |
| point_miss | 8 | 18,110.7 | 17,896.2 | -1.2% | 414.4 | 405.3 |
| range_32 | 1 | 2,864.3 | 3,116.2 | +8.8% | 327.1 | 304.5 |
| range_32 | 8 | 6,647.9 | 15,244.3 | +129.3% | 688.0 | 477.4 |
| range_64 | 1 | 2,242.6 | 2,708.7 | +20.8% | 397.1 | 348.3 |
| range_64 | 8 | 12,231.1 | 12,359.7 | +1.1% | 596.2 | 574.5 |
| range_256 | 1 | 1,593.9 | 1,589.7 | -0.3% | 580.3 | 601.0 |
| range_256 | 8 | 6,669.8 | 6,944.2 | +4.1% | 1080.2 | 1037.7 |

The reproducible result is at C8: 64-row throughput changes by +14.9% and
+1.1% in the two orders; 256-row throughput changes by +7.7% and +4.1%, with
p50 improving by 7.3% and 3.9%. C1 changes are mostly within noise. The 32-row
C8 baseline suffered one host/controller slowdown, reversing the two paired
comparisons, so it does not establish a gain.

Longer point-only matrices (30,000 operations per trial, 3,000 warmups) ran in
changed, baseline, changed order. They also varied with run order and controller
activity. Point hits and misses do not show
a repeatable end-to-end improvement; some changed runs were several percent
lower. The encoder allocation reduction is real, but it is too small relative
to quorum and host noise to claim faster point reads. Raw reports include all
latency samples and the outliers.

## Baseline profile

A diagnostic-only profiled run also verified the full table and every operation.
The retained data-leader trace matched 33,880 complete reads: quorum wait had a
142.5 µs median and SQL execution 66.6 µs. Admission was 0.13 µs; the former
encoding phase was 1.92 µs median. Gateway CPU also attributed 23.8% cumulative
time to repeated TLS handshakes from background health observations. These are
profile attributions, not additive wall-clock phases or expected speedups.

The response-copy patch addresses allocation and encoding costs. A substantially
faster point-read path needs separate work on the quorum-read contract and
authenticated health/control connection lifecycle; weakening those contracts
was outside this change.

## Isolated codec measurements

Darwin arm64 / M4 Max, Go 1.27 with SIMD, one 500 ms sample. These are diagnostic
microbenchmarks, performed while untimed fixture setup was running, and do not
establish an end-to-end throughput ratio. The wire encoder comparison uses
the former collectRows + EncodeResponse algorithm as its oracle.

| Returned rows | Before ns/op | Direct ns/op | Before allocations | Direct allocations |
|---|---:|---:|---:|---:|
| 1 | 217.7 | 57.71 | 8 | 1 |
| 32 | 3,254 | 1,052 | 107 | 1 |
| 64 | 5,370 | 2,630 | 206 | 1 |
| 256 | 21,068 | 8,277 | 788 | 1 |

For a 4,096-row ordinary response, decoding drops from 12,296 to 9 allocations,
and 819,668 to 590,289 bytes per operation. Its new storage matches the already
optimized row-batch decoder. Raw before/after files are included.

## Qualification notes

Fresh-cluster CREATE TABLE returned a catalog-generation mismatch despite
creating an empty table. Setup confirmed COUNT(*) = 0, then ran the unchanged
64-row INSERT loop using a helper that replaces only the CREATE statement with
a count check and complete verification of any already committed prefix. Timed clients use the normal benchmark executable. All
setup failures and the helper patch are retained with the evidence.

The first follow-up fixture failed at row 244,608 with an unexpected EOF. Its
baseline server rejected a Raft snapshot message in the immutable-base WAL
runtime, and restart hit the same refusal. No timings were accepted from it.
The replacement fixture sets GOMAXPROCS=2 per server process, retains the same
12-CPU/24-GiB container limit, and runs without concurrent local tests or builds.
Both compared versions use identical settings and the same persistent dataset.

The merged PR's recovery/client CI job failed one of three native replica-
replacement repetitions after 120 seconds with no reachable leader; the next
repetition passed in 6.87 seconds. That test does not execute the modified SQL
query path. This is an unresolved CI failure, not a passing qualification.
That underlying failure also made the two platform summary jobs fail; their
other cross-compile, unit, SQL, SIMD, mutation, and race components succeeded.
Job: https://github.com/thesyncim/vibedb/actions/runs/33901235621/job/101116900883

After measurement, the branch integrated latest main `ede2b4f3`; its source
changes do not overlap the response patch. The distributed comparison therefore
isolates this patch on the shared `b3acdfd7` tree. Final tests below run on
`ede2b4f3` plus the response patch.
