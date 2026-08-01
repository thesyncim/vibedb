# Competitive results

> **Current published snapshot.** Every table below was regenerated on
> 2026-08-01 from clean engine commit
> `7fe67691dd889a34951682d2522661c7741d8720`. The benchmark binaries also
> embed that revision with `vcs.modified=false`.

The headline result is no longer a narrow read-heavy win. At one client,
vibedb is 2.52–2.79× Badger on YCSB-A, YCSB-B, YCSB-F, and churn. Across 1,
8, and 32 clients it is 2.35–2.50× Badger on pure replacement writes and
2.73–2.93× on mixed churn. The remaining measured throughput deficit is the
ordered-scan mix, where vibedb is 43.9% behind Badger.

## Provenance and protocol

- Machine: Apple M4 Max (16 cores, 64 GB), macOS 26.3.1 / Darwin 25.3.0,
  APFS, Go 1.26.0.
- Competitors: bbolt 1.5.0, Badger 4.9.5, Pebble 1.1.5, modernc SQLite
  1.54.0.
- Corpus: 10,000 documents for throughput and 100,000 for churn-disk and
  footprint; low cardinality unless a table says otherwise.
- Throughput shape: 2,000 warmup operations, 20,000 measured operations,
  buffered-visible durability, and a CP64 acknowledged-mutation threshold.
- Each throughput cell is the median of ten recorded repetitions. Engines run
  in isolated child processes, with deterministic Latin-square ordering and
  one unrecorded conditioning pass per engine.
- Footprint and non-Pebble churn-disk cells are one isolated run. Pebble
  churn-disk cells are medians of three because its compaction scheduling moves
  the resulting image.
- Every suite records the commit, dirty bit, binary hash, effective options,
  corpus shape, engine order, repetitions, and pressure-forced checkpoints.
  All published suites are clean and report zero forced checkpoints.
- Correctness is checked outside timed intervals: corpus shape, operation
  trace, final key/value state, and complete consumption of returned scan
  bytes.

## Durability lanes

Results are never averaged across durability promises. The current snapshot
publishes the cross-engine **buffered-visible** lane: writes become visible
immediately and become durable at each scheduled checkpoint.

The harness also distinguishes **ordinary-sync** from **power-safe**. On this
Darwin host only vibedb (`DurabilitySync`) and SQLite can make the strongest
power-loss promise natively; bbolt, Badger, and Pebble stop at ordinary fsync
and fail closed when asked to enter the power-safe lane.

## Single-client mixed workloads

Total operations per second, median of ten:

| workload | vibedb | Badger | SQLite | Pebble | bbolt | vibedb / Badger |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| YCSB-A: 50% read, 50% update | **770,327** | 305,605 | 111,375 | 24,849 | 21,929 | **2.52×** |
| YCSB-B: 95% read, 5% update | **2,704,197.5** | 1,027,076.5 | 330,809 | 239,517 | 212,790 | **2.63×** |
| YCSB-F: 50% read, 50% read-modify-write | **732,317** | 277,999 | 99,433 | 24,553 | 21,607.5 | **2.63×** |
| Churn: 70% read, 25% update, 5% delete+restore | **1,089,310** | 390,140 | 143,698 | 34,914 | 30,099.5 | **2.79×** |
| Scan mix: 79.9% read, 15% update, 5% delete+restore, 0.1% full scan | 166,707.5 | **297,290** | 123,688.5 | 44,742 | 39,880.5 | 0.56× |

The first four workloads are 6.92–8.17× SQLite. Scan mix is still 1.35×
SQLite, but it is the clear Badger gap and should not be hidden inside an
overall average.

### vibedb operation latency

Median of the ten run-level percentiles, microseconds:

| workload | operation p50 / p99 | checkpoint p50 / p99 |
| --- | ---: | ---: |
| YCSB-A | read 0.125 / 0.666; update 1.833 / 30.625 | 30.251 / 41.000 |
| YCSB-B | read 0.125 / 0.563; update 1.896 / 33.125 | 33.021 / 44.792 |
| YCSB-F | read 0.125 / 0.646; RMW 1.917 / 30.583 | 30.125 / 41.375 |
| Churn | read 0.125 / 0.625; update 1.854 / 30.438; delete+restore 2.209 / 34.021 | 30.209 / 40.584 |
| Scan mix | read 0.334 / 0.667; update 1.916 / 28.167; delete+restore 2.250 / 37.104; full scan 1,816 / 2,340 | 29.167 / 5,913 |

The scan suite has a real checkpoint-tail outlier: its median run-level p99 is
5.913 ms. The other four checkpoint p99 values are 40.6–44.8 µs.

## Concurrent replacement and churn

Total operations per second, median of ten. `write` is 100% existing-key
replacement; `churn` has the same 70/25/5 read/update/delete+restore mix as
above.

| workload | clients | vibedb | Badger | SQLite | Pebble | bbolt | vibedb / Badger |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| write | 1 | **408,753.5** | 173,801 | 66,404.5 | 12,917 | 11,044 | **2.35×** |
| write | 8 | **623,980.5** | 249,857.5 | 56,404 | 13,002 | 11,029 | **2.50×** |
| write | 32 | **648,988.5** | 272,191.5 | 54,547 | 13,003.5 | 10,685 | **2.38×** |
| churn | 1 | **1,087,408.5** | 396,310.5 | 141,446.5 | 34,788.5 | 30,627.5 | **2.74×** |
| churn | 8 | **1,621,065.5** | 594,384 | 110,978 | 36,318.5 | 24,841.5 | **2.73×** |
| churn | 32 | **1,730,922.5** | 590,288 | 110,269 | 37,183.5 | 18,503 | **2.93×** |

vibedb scales from one to eight clients by 1.53× on replacement writes and
1.49× on churn; from one to 32 by 1.59× on both. It still saturates rather than
scaling linearly after eight clients. The important result is that concurrent
preparation is useful and the short generation-publication section does not
collapse the 32-client lane: churn rises another 6.8% from 8 to 32 while
Badger falls 0.7%.

## Disk under sustained churn

`cmd/churndisk` keeps 100,000 documents live through 200,000 acknowledged
state changes. Eighty percent of random choices are one-change replacements;
the rest are indivisible delete+reinsert pairs. Checkpoint and sampling
cadences are mutation thresholds, so a pair may cross one by a single change.

Cells are **apparent / allocated MiB**. `online` is measured immediately after
the workload and all requested CP64 checkpoints. For vibedb it already includes
bounded, foreground hole punching; it does not depend on background work,
offline maintenance, or a forced checkpoint. `offline` is a separate
out-of-place `durable.Repack` result and is not required to bound online growth.

### Intrinsic representation

Optional SST compression is disabled.

| engine | low online | low offline | high online | high offline |
| --- | ---: | ---: | ---: | ---: |
| vibedb | **22.075 / 16.020** | **9.001 / 9.520** | 54.841 / 36.070 | **18.767 / 19.520** |
| SQLite | 28.109 / 28.109 | 26.234 / 26.234 | **28.109 / 28.109** | 26.234 / 26.234 |
| bbolt | 45.750 / 45.750 | 45.750 / 45.750 | 45.750 / 45.750 | 45.750 / 45.750 |
| Pebble (median of 3) | 93.864 / 95.141 | 103.447 / 99.859 | 93.863 / 95.320 | 103.445 / 95.703 |
| Badger | 314.769 / 72.641 | 314.769 / 72.641 | 314.770 / 72.645 | 314.770 / 72.645 |

### Production-compressed LSM control

Pebble and Badger use the pinned releases' Snappy SST configuration. vibedb,
bbolt, and SQLite have no corresponding profile switch, so their rows are the
same measurement as above.

| engine | low online | low offline | high online | high offline |
| --- | ---: | ---: | ---: | ---: |
| vibedb | **22.075 / 16.020** | **9.001 / 9.520** | 54.841 / 36.070 | **18.767 / 19.520** |
| SQLite | 28.109 / 28.109 | 26.234 / 26.234 | **28.109 / 28.109** | 26.234 / 26.234 |
| bbolt | 45.750 / 45.750 | 45.750 / 45.750 | 45.750 / 45.750 | 45.750 / 45.750 |
| Pebble, Snappy (median of 3) | 79.133 / 81.129 | 58.445 / 54.203 | 84.244 / 86.055 | 76.311 / 70.730 |
| Badger, Snappy | 273.948 / 31.820 | 273.948 / 31.820 | 279.414 / 37.285 | 279.414 / 37.285 |

Low-cardinality vibedb is the smallest online image by both measures. At high
cardinality, SQLite remains smaller, but vibedb uses 5.10× fewer apparent
bytes and 3.3% fewer allocated bytes than production Badger. The online Vibe
plateau is reusable foreground-reclaimed capacity, not an accumulating log.
Offline Repack is shown only as a lower bound: it produces the smallest image
in both cardinalities but is not part of the online performance claim.

## Bulk footprint

The low- and high-cardinality corpora have identical shape and length:
24,881,153 raw bytes (23.729 MiB) for 100,000 documents. Their gzip-9 sizes are
1.837 MiB and 8.041 MiB, respectively. Cells below are one isolated run and
include vibedb's fully preallocated paired recovery journal. They are
**apparent / allocated MiB**.

### Intrinsic representation

| engine | low cardinality | high cardinality |
| --- | ---: | ---: |
| vibedb unified bulk pair | **9.001 / 9.520** | **18.767 / 19.520** |
| vibedb point-put build | 16.341 / 16.379 | 28.606 / 29.250 |
| SQLite | 28.109 / 28.109 | 28.109 / 28.109 |
| bbolt | 45.750 / 29.734 | 45.750 / 29.734 |
| Pebble | 50.611 / 50.664 | 50.611 / 50.664 |
| Badger | 257.000 / 26.621 | 257.000 / 26.621 |

### Production-compressed LSM control

| engine | low cardinality | high cardinality |
| --- | ---: | ---: |
| vibedb unified bulk pair | **9.001 / 9.520** | **18.767 / 19.520** |
| vibedb point-put build | 16.341 / 16.379 | 28.606 / 29.250 |
| SQLite | 28.109 / 28.109 | 28.109 / 28.109 |
| bbolt | 45.750 / 29.734 | 45.750 / 29.734 |
| Pebble, Snappy | 33.978 / 34.000 | 40.993 / 41.027 |
| Badger, Snappy configured | 257.000 / 26.621 | 257.000 / 26.621 |

Badger's bulk corpus is still resident in its configured mutable table, so the
bulk row has no meaningful compressed SST payload; the churn table is the
materialized production-compressed comparison. Vibedb's compactness is
structural: repeated canonical JSON skeletons and scalar spellings are shared
within a leaf, while uncommon shapes stay verbatim. It is not a generic gzip
comparison and does not require a second storage mode.

## Current CPU and scan gates

These five-sample Go microbenchmarks were captured from the same clean commit
on the same host. They are regression gates, not cross-engine database results.

| gate | median | allocation |
| --- | ---: | ---: |
| stable native checkpoint leaf fold | **1.883 µs** | 0 B, 0 allocs |
| full render/replan/encode of that leaf | 255.615 µs | 0 B, 0 allocs |
| ordered scan, 100k three-scalar documents | **23.07 ns/document** | 0 B, 0 allocs |
| full competitive scan, low cardinality | **92.29 ns/document** | 0 B, 0 allocs |
| full competitive scan, high cardinality | **95.97 ns/document** | 0 B, 0 allocs |
| masked scan, one occupied row per live posting tile | **173.5 ns/selected document** | 0 B, 0 allocs |

The certified native fold is 136× faster than full replanning. The old
1/4/16-row and 75%-density-crossover numbers are intentionally gone: the
current named benchmark reproducibly measures one real occupied row per live
posting tile, so only that result is published.

## Publishing rules

A replacement snapshot must:

1. name the exact commit, dirty bit, machine, OS, Go, and competitor versions;
2. run timed engines in isolated processes and publish repeated-sample medians;
3. validate equivalent corpus, trace, final state, and returned scan bytes;
4. match durability, checkpoint cadence, workload shape, and client count;
5. include requested checkpoint stalls in elapsed time;
6. report apparent and allocated bytes for both cardinalities;
7. label online foreground state separately from every offline maintenance
   hook;
8. name the storage profile and effective compression for every disk row; and
9. keep database results, microbenchmarks, and projections separate.

The rationale and complete harness contract are in the
[competitive benchmark guide](README.md).

## Reproduction

From `bench/competitive`:

```sh
go test . \
  -run '^(TestFullEquivalence|TestFullEquivalenceIndexedDurable|TestCorpusVariantsAreShapeMatched)$' \
  -count=1 -timeout=60m
go test ./cmd/... -count=1 -timeout=60m

go build -trimpath -o /tmp/vibedb-mixed ./cmd/mixed
go build -trimpath -o /tmp/vibedb-mixedsuite ./cmd/mixedsuite

# Single-client matrix: run each workload separately.
/tmp/vibedb-mixedsuite -mixed-bin=/tmp/vibedb-mixed \
  -workload=<ycsb-a|ycsb-b|ycsb-f|churn|scan> -clients=1 \
  -durability=buffered-visible -checkpoint-mutations=64 \
  -repetitions=10 -output=<file.tsv>

# Concurrent matrix: run each workload/client pair separately.
/tmp/vibedb-mixedsuite -mixed-bin=/tmp/vibedb-mixed \
  -workload=<write|churn> -clients=<1|8|32> \
  -durability=buffered-visible -checkpoint-mutations=64 \
  -repetitions=10 -output=<file.tsv>

go build -trimpath -o /tmp/vibedb-churndisk ./cmd/churndisk
go build -trimpath -o /tmp/vibedb-footprint ./cmd/footprint

/tmp/vibedb-churndisk -engine=<engine> -cardinality=<low|high> \
  -storage-profile=<intrinsic|production>
/tmp/vibedb-footprint -engine=<engine> -cardinality=<low|high> \
  -storage-profile=<intrinsic|production>
```

Run timing lanes serially on an otherwise idle host. The exact micro-gate
commands are listed in the [benchmark guide](README.md).
