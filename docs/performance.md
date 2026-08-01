# Performance

The current snapshot was regenerated on 2026-08-01 from clean engine commit
`7fe67691dd889a34951682d2522661c7741d8720`. The complete tables, exact
protocol, dependency versions, caveats, and reproduction commands are in
[bench/competitive/RESULTS.md](../bench/competitive/RESULTS.md). This page is
the short reading guide.

## What changed

The former write gap is closed in the measured buffered-visible CP64 lane.
With one client, vibedb is 2.52–2.79× Badger on YCSB-A, YCSB-B, YCSB-F, and
churn. Across 1, 8, and 32 clients it is 2.35–2.50× Badger on existing-key
replacement and 2.73–2.93× on mixed churn.

This does not mean every gap is closed. The ordered-scan mix remains 43.9%
behind Badger, and throughput scaling flattens after eight clients. Those are
the next two measured performance targets.

## Single-client workloads

Total operations per second, median of ten isolated repetitions on an Apple
M4 Max. Every workload has 10,000 documents, one client, buffered-visible
durability, and a CP64 acknowledged-mutation threshold.

| workload | vibedb | Badger | SQLite | vibedb / Badger |
| --- | ---: | ---: | ---: | ---: |
| YCSB-A | **770,327** | 305,605 | 111,375 | **2.52×** |
| YCSB-B | **2,704,197.5** | 1,027,076.5 | 330,809 | **2.63×** |
| YCSB-F | **732,317** | 277,999 | 99,433 | **2.63×** |
| Churn | **1,089,310** | 390,140 | 143,698 | **2.79×** |
| Scan mix | 166,707.5 | **297,290** | 123,688.5 | 0.56× |

Vibedb point-read p50 is 0.125 µs in the first four workloads. Update p50 is
1.83–1.90 µs, delete+restore p50 is 2.21 µs, and the ordinary workload
checkpoint p50 is 30.1–33.0 µs. Four workload checkpoint p99 values are
40.6–44.8 µs. Scan mix is the exception: its checkpoint p99 is 5.913 ms and
its full-scan p50/p99 is 1.816/2.340 ms.

## Concurrent writes

Median total operations per second:

| workload | clients | vibedb | Badger | vibedb / Badger |
| --- | ---: | ---: | ---: | ---: |
| 100% replacement | 1 | **408,753.5** | 173,801 | **2.35×** |
| 100% replacement | 8 | **623,980.5** | 249,857.5 | **2.50×** |
| 100% replacement | 32 | **648,988.5** | 272,191.5 | **2.38×** |
| mixed churn | 1 | **1,087,408.5** | 396,310.5 | **2.74×** |
| mixed churn | 8 | **1,621,065.5** | 594,384 | **2.73×** |
| mixed churn | 32 | **1,730,922.5** | 590,288 | **2.93×** |

The concurrent primary path uses at most 32 fixed, preallocated writer-scratch
contexts; 4,096 stripes hashed by complete bucket/leaf identity; parallel
canonicalization, routing, and leaf-local inspection; and a bounded
flat-combining publisher for generation assignment and overlay publication.
Its current qualification lane is buffered-visible, schemaless, unindexed,
and inline. Structural operations, overflow, split, and other unsupported
cases retain exclusive fencing and fall back to the general path.

That design improves useful concurrency without claiming lock-free commits.
From one to 32 clients, vibedb scales 1.59× on both workloads. It gains another
4.0% (replacement) and 6.8% (churn) from 8 to 32, rather than scaling linearly.

## Online space under churn

The churn harness keeps 100,000 documents live through 200,000 acknowledged
state changes: 80% replacements and 20% indivisible delete+reinsert pairs.
Every final key/value is verified outside the timed interval, and every Vibe
run reports zero pressure-forced checkpoints.

Cells are **apparent / allocated MiB**.

| measurement | low cardinality | high cardinality |
| --- | ---: | ---: |
| vibedb online after churn | **22.075 / 16.020** | 54.841 / 36.070 |
| production Badger online after churn | 273.948 / 31.820 | 279.414 / 37.285 |
| production Pebble online after churn, median of 3 | 79.133 / 81.129 | 84.244 / 86.055 |
| SQLite online after churn | 28.109 / 28.109 | **28.109 / 28.109** |
| vibedb offline Repack floor | **9.001 / 9.520** | **18.767 / 19.520** |

The Vibe online row is the important result. Physical checkpoint completion
performs bounded, work-conserving foreground hole punching, so obsolete extents
become reusable without a background compactor, an offline pass, or an
unbounded cleanup spike. The amount of reclaim work per durable generation is
fixed; unsupported filesystems safely retain logical reuse without the
allocated-byte optimization.

Low-cardinality Vibe is the smallest online image by both measures. At high
cardinality, SQLite is still smaller, while Vibe uses 5.10× fewer apparent
bytes and 3.3% fewer allocated bytes than production Badger. Offline
out-of-place `durable.Repack` is shown as a separate lower bound, not as a
requirement for stable online space.

## Bulk footprint

Both 100,000-document corpora contain 24,881,153 raw bytes (23.729 MiB). The
low-cardinality corpus compresses to 1.837 MiB with gzip-9 and the
shape-identical high-cardinality corpus to 8.041 MiB. Database cells include
the fully preallocated paired recovery journal and are
**apparent / allocated MiB**.

| engine/profile | low cardinality | high cardinality |
| --- | ---: | ---: |
| vibedb unified bulk pair | **9.001 / 9.520** | **18.767 / 19.520** |
| vibedb point-put build | 16.341 / 16.379 | 28.606 / 29.250 |
| SQLite | 28.109 / 28.109 | 28.109 / 28.109 |
| Pebble with Snappy | 33.978 / 34.000 | 40.993 / 41.027 |
| Badger with Snappy configured | 257.000 / 26.621 | 257.000 / 26.621 |

Badger's bulk image has not yet materialized its mutable table into compressed
SSTs, so use the churn table—not bulk—for a production-compressed comparison.
Vibedb's compactness comes from structural sharing inside the canonical leaf
format: repeated JSON skeletons and scalar spellings are shared, while shapes
that do not save bytes remain verbatim.

## CPU and scan gates

Five-sample medians from the same clean commit:

| gate | result |
| --- | ---: |
| stable native checkpoint leaf fold | **1.883 µs**, 0 allocs |
| full render/replan/encode | 255.615 µs, 0 allocs |
| ordered scan, 100k three-scalar documents | **23.07 ns/document**, 0 allocs |
| competitive full scan, low/high cardinality | **92.29 / 95.97 ns/document**, 0 allocs |
| masked scan, one occupied row per live posting tile | **173.5 ns/selected document**, 0 allocs |

The certified native fold is about 136× faster than full replanning. Historical
masked-density sweeps are not carried forward because the current named
benchmark reproduces one occupied row per live posting tile, not the former
1/4/16/dense matrix.

## Reading the numbers honestly

- Compare only equal durability lanes, checkpoint cadence, operation mix, and
  client count.
- Throughput cells are ten-sample medians from isolated child processes;
  footprint is one isolated run except where a three-run median is labeled.
- `online` includes all requested checkpoints and bounded foreground reclaim.
  `offline Repack` is always a separate row.
- Apparent and allocated bytes answer different questions and are both shown.
- Intrinsic/uncompressed storage is never ranked as if it were the same profile
  as a production-compressed LSM.
- Microbenchmarks are regression gates, not substitutes for cross-engine
  workload results.

See the [competitive harness guide](../bench/competitive/README.md) for the
full contract and [competitive results](../bench/competitive/RESULTS.md) for
reproduction commands.
