# Performance

The authoritative measured tables live in
[bench/competitive/RESULTS.md](../bench/competitive/RESULTS.md). This document
explains their provenance, reproduces the current headline tables, and states
the rules a replacement measurement must satisfy.

## Provenance of the checked-in baseline

| Condition | Value |
| --- | --- |
| Baseline commit | `1a11b02233a125dd743bab22ce0612b0faee2abf` |
| Crash-safe refresh | `2535c32ccae8d15eec3f581a7f68cf93fce95585` |
| Compact-footprint refresh | `ce909e0469992cf3a8b3418e87ead7d1e734997c` |
| Existing-key mixed refresh | `45c2bb263b3efc8cb23afd1393391ca221f2320d` |
| Machine | Apple M4 Max, 16 cores, 64 GiB |
| OS / Go | macOS 26.3.1, darwin/arm64 / Go 1.26.0 |
| Read and space corpus | 100,000 documents, 23.73 MiB raw JSON |
| Read and space sampling | three isolated process runs; median |
| Heterogeneous mixed corpus | 10,000 documents |
| Heterogeneous mixed sampling | six isolated process runs; middle-pair median |
| Crash-safe mixed sampling | three isolated process runs; median |

The competitor versions, workload lengths, and historical qualifications are
recorded beside the source tables in
[RESULTS.md](../bench/competitive/RESULTS.md#conditions).
`TestFullEquivalence` and `TestCorpusVariantsAreShapeMatched` passed before
measurement.

## Current read and scan baseline

`Iteration` touches one byte per value. `Ordered all bytes` reads every value
byte and is the scan-throughput result.

| Engine | Random point | Iteration | Ordered all bytes | Point allocation | Scan allocation |
| --- | ---: | ---: | ---: | ---: | ---: |
| bbolt | 376.3 ns | 8.479 ns/doc | 79.39 ns/doc | 168 B / 3 | 576 B / 9 |
| Badger | 796.7 ns | 84.20 ns/doc | 203.7 ns/doc | 804 B / 8 | ~8.6 KiB / 44 |
| **vibedb** | 1,162 ns | **7.546 ns/doc** | 79.64 ns/doc | **0 B / 0** | **0 B / 0** |
| Pebble | 1,235 ns | 43.00 ns/doc | 112.4 ns/doc | 80 B / 2 | ~85 B / 1 |
| SQLite | 2,838 ns | 170.4 ns/doc | 244.5 ns/doc | 1,092 B / 22 | 27.4 MiB / 200,017 |

These rows measure the default durable representation at the baseline commit.
Candidate ordered-tablet primitives in `RESULTS.md` are isolated lab results,
not replacements for this table.

## Pinned mixed-workload diagnostics

The heterogeneous-default rows are pinned diagnostics, not a durability-matched
or sustained leaderboard. They use one blocking client, a 10,000-document
corpus, 2,000 warmup operations, and 20,000 measured operations. The old
`-sync=false` setting gave vibedb a continuous stable-persistence pipeline
while competitors primarily paid volatile buffering, and final maintenance
was outside the timer.

| Workload | vibedb | bbolt | Badger | Pebble | SQLite |
| --- | ---: | ---: | ---: | ---: | ---: |
| YCSB-B, 95% read / 5% update | 155,292 | 1,057,140 | 979,198 | **1,957,210** | 335,649 |
| YCSB-A, 50% read / 50% update | 14,714 | 155,332 | 286,277 | **1,178,533** | 180,891 |
| YCSB-F, 50% read / 50% RMW | 14,482 | 153,224 | 256,378 | **1,009,037** | 146,643 |
| Churn, read/update/delete+restore | 18,081 | 213,787 | 372,274 | **1,417,792** | 210,906 |
| Ordered-scan mix | 24,614 | 236,686 | 277,310 | **587,868** | 154,398 |

Values are total user operations per second. The replacement harness now uses
explicit durability modes, includes checkpoint stalls, and exposes forced
checkpoints. These pinned rows must be refreshed before being relabeled.

The pinned power-safe lane used 200 warmup and 2,000 measured operations. On
Darwin, vibedb and SQLite are the comparable `F_FULLFSYNC` pair; bbolt, Badger,
and Pebble have weaker boundaries and are shown only as operational context:

| Workload | vibedb | bbolt† | Badger† | Pebble† | SQLite | vibedb vs SQLite |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| YCSB-B | 3,279 | 2,439 | 468,384 | 4,866 | **3,677** | -10.8% |
| YCSB-A | 360 | 232 | 65,338 | 482 | **422** | -14.7% |
| YCSB-F | 385 | 233 | 68,758 | 470 | **411** | -6.3% |
| Churn | 532 | 339 | 87,684 | 691 | **605** | -12.1% |
| Ordered-scan mix | 726 | 449 | 100,501 | 951 | **814** | -10.8% |

`†` marks the weaker Darwin persistence boundary. The competitor adapters in
this historical lane also predate the existing-key semantic refresh, so use it
for durability-bound orientation rather than a new mutation claim.

## Current disk and retained-memory baseline

The corpus is 100,000 unindexed documents. Disk comparisons use allocated
blocks; apparent size is retained to expose sparse or preallocated files.

| Engine | Apparent / allocated | HeapAlloc | Runtime resident | Peak RSS |
| --- | ---: | ---: | ---: | ---: |
| **vibedb bulk, compact, low-cardinality** | **13.9 / 13.9 MiB** | 15.1 MiB | 25.7 MiB | 174.2 MiB |
| **vibedb bulk, compact, high-cardinality** | **26.1 / 26.1 MiB** | 15.1 MiB | 25.5 MiB | 176.3 MiB |
| Badger | 257.0 / **26.6 MiB** | 86.1 MiB | 97.0 MiB | 154.1 MiB |
| SQLite | 28.1 / 28.1 MiB | **2.5 MiB** | 13.0 MiB | 152.3 MiB |
| bbolt | 45.8 / 29.7 MiB | **2.5 MiB** | **12.9 MiB** | **91.4 MiB** |
| **vibedb bulk, verbatim** | 32.2 / 32.2 MiB | 16.6 MiB | 27.5 MiB | 174.4 MiB |
| vibedb Put replay | 35.9 / 36.5 MiB | 16.6 MiB | 28.8 MiB | 185.6 MiB |
| Pebble | 50.6 / 50.7 MiB | 36.3 MiB | 46.6 MiB | 114.9 MiB |

Compact bulk is explicitly selected, bulk-only evidence. It is not the mutable
default and not a read-performance claim. Heap, runtime resident memory, and
peak RSS have different scopes; none is total database size by itself.

## Exact secondary index

For an exact country index selecting 945 of 100,000 documents:

| Engine | Query | Allocations | Indexed file | Index bytes |
| --- | ---: | ---: | ---: | ---: |
| **vibedb** | **36.108 µs** | 1,328 B / 17 | 34.6 MiB | +2.4 MiB |
| SQLite | 539.705 µs | 568 B / 20 | 30.3 MiB | +2.2 MiB |

The key/value competitors expose no native JSON secondary index in this
harness.

## Measurement lanes

Every published row names its lane:

- **Machine:** CPU, memory, OS, architecture, filesystem where relevant, and
  Go version.
- **Commit:** exact Git commit and dirty-state fingerprint.
- **Workload:** corpus cardinality, document count, warmup, operation count,
  client/writer count, and deterministic seed.
- **Durability:** resolved acknowledgement mode, checkpoint strength, cadence,
  and whether any forced checkpoint occurred.
- **Sampling:** one engine per process where process-global memory matters,
  repetition count, ordering method, and median definition.

Never compare rows across durability or client-count lanes as if only the
engine name changed.

## Honesty rules

1. Correctness licenses performance. `TestFullEquivalence` must verify every
   key and every scanned byte before a cross-engine number is published.
2. Report medians of repeated isolated samples, never a single run.
3. Keep in-memory `store` out of durable-engine tables. It is an upper bound on
   what removing durability buys.
4. Publish apparent and allocated disk bytes; compare allocated bytes.
5. Put low- and shape-matched high-cardinality corpora side by side.
6. Keep verbatim bulk, compact bulk, and `Put` replay as separate artifacts.
7. Label `BenchmarkScan` as iteration-only and publish it beside
   `BenchmarkScanAllBytes`.
8. Match mutation semantics: existing-key resolution, blind upsert, and atomic
   conditional replace are distinct lanes.
9. Match durability by actual platform primitive. On Darwin, plain `fsync` or
   `msync` is not comparable with `F_FULLFSYNC`.
10. Include requested periodic and final checkpoint stalls in throughput.
    A same-cadence buffered run with forced checkpoints is not publishable.
11. Record every non-default tuning in `Engine.Tuning()` and retain an untuned
    arm where the call shape changes.
12. Charge index construction to indexed queries and keep load/open timing
    asymmetries explicit.
13. Do not call `HeapAlloc`, RSS, mmap bytes, or cache bytes “database size.”
14. Keep measured and projected values distinct. An isolated candidate
    primitive does not update a database-level table.

The detailed rationale and known asymmetries remain in the
[harness guide](../bench/competitive/README.md); the checked-in values and
their interpretation remain in
[RESULTS.md](../bench/competitive/RESULTS.md).

## Reproduction

From `bench/competitive`:

```sh
go test -run 'TestFullEquivalence|TestCorpusVariantsAreShapeMatched' \
  -count=1 -timeout=60m .

go test -run '^$' \
  -bench='^Benchmark(PointRead|Scan|ScanAllBytes)/(vibejson-durable|bbolt|badger|pebble|sqlite)$' \
  -benchtime=2s -count=3 -timeout=30m .

go build -o /tmp/vibedb-mixed ./cmd/mixed
go build -o /tmp/vibedb-mixedsuite ./cmd/mixedsuite
/tmp/vibedb-mixedsuite -mixed-bin=/tmp/vibedb-mixed \
  -workload=ycsb-a -durability=buffered-visible \
  -checkpoint-mutations=64 -output=mixed-ycsb-a-buffered.tsv

go build -o /tmp/vibedb-footprint ./cmd/footprint
for card in low high; do
  for engine in vibejson-durable bbolt badger pebble sqlite; do
    /tmp/vibedb-footprint -engine="$engine" -cardinality="$card"
  done
  /tmp/vibedb-footprint -engine=vibejson-durable -putloop -cardinality="$card"
  /tmp/vibedb-footprint -engine=vibejson-durable -compact -cardinality="$card"
done
```

Use the complete lane matrix and repetition commands in
[RESULTS.md](../bench/competitive/RESULTS.md#reproduction) when refreshing a
checked-in table.
