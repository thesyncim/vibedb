# Competitive results

This is the current reproducible baseline. It is deliberately blunt: a row is
included only when the harness drove equivalent operations and the result has
multiple isolated samples. Historical tables remain available in Git history;
they are not mixed with current numbers.

## Conditions

| | |
| --- | --- |
| Baseline commit | `1a11b02233a125dd743bab22ce0612b0faee2abf` |
| Crash-safe refresh commit | `2535c32ccae8d15eec3f581a7f68cf93fce95585` |
| Compact-footprint refresh commit | `ce909e0469992cf3a8b3418e87ead7d1e734997c` |
| Machine | Apple M4 Max, 16 cores, 64 GiB |
| OS | macOS 26.3.1, darwin/arm64 |
| Go | 1.26.0 |
| Competitors | bbolt 1.5.0, Badger 4.9.5, Pebble 1.1.5, modernc SQLite 1.54.0 (`go list -m -u` reported no updates on 2026-07-26) |
| Mixed corpus | 10,000 documents |
| Heterogeneous-default mixed samples | six isolated process-level runs |
| Heterogeneous-default existing-key refresh | `45c2bb263b3efc8cb23afd1393391ca221f2320d` |
| Crash-safe mixed samples | three isolated process-level runs |
| Heterogeneous-default mixed run | 2,000 warmup + 20,000 measured operations |
| Crash-safe mixed run | 200 warmup + 2,000 measured operations |
| Read/space corpus | 100,000 documents, 23.73 MiB raw JSON |
| Read/space samples | three isolated process-level runs |

`TestFullEquivalence` and `TestCorpusVariantsAreShapeMatched` passed before the
measurements. Medians below are the middle-pair average for six samples and the
middle observation for three samples.

## Current candidate primitives, not yet the default store

The complete competitor tables below still measure the current default durable
store. Two newer isolated primitives have passed their local gates but are not
included in those database-level numbers:

| Candidate at current `main` | Current M4 Max result | Promotion gate |
| --- | ---: | ---: |
| ordered leaf hit, hash included | 30.0–31.1 ns, 0 alloc | ≤45 ns |
| ordered leaf miss, hash included | 49.3–50.9 ns, 0 alloc | ≤55 ns |
| ordered leaf lexical iteration | 5.14–5.17 ns/doc, 0 alloc | ≤6 ns/doc |
| ordered leaf structural metadata at 218–244 live | 4.848–4.996 B/key | ≤5 B/key |
| aligned narrow leaf at 195 live | 4.887 structural, 0.118 slack B/key | ≤5 structural, ≤1 slack |
| aligned narrow equivalent all-key prehashed hit / miss | 52.2 / 14.3 ns, 0 alloc | no regression |
| aligned narrow lexical loop | ~3.1 ns/doc, 0 alloc | ≤6 ns/doc |
| combined tablet key route, hash included | 185.8 ns, 0 alloc | complete point ≤300 ns |
| resident posting-driven BucketID resolve | 25.1–26.4 ns, 0 alloc | no global-map walk |
| combined tablet routing metadata at 187 live | 0.161 B/document | whole-file gate |
| exact-term repeated-shape lookup | 5.7–25.9 ns, 0 alloc | no regression |
| exact-term repeated-shape iteration | 1.62–1.67 ns/posting, 0 alloc | no regression |

The ordered leaf still has material 4 KiB extent-rounding slack for small
records in the fixed 256-slot candidate; the adaptive 217-slot experiment
removes it for the measured 8+8-byte shape. Its wide churn class still has a
slow high-stash hit and is not promoted. The combined tablet result is a
resident monolithic codec measurement;
production segmentation still adds tablet-catalog, root, anchor-page, and
leaf-cache acquisition. None of these rows includes commit publication,
snapshot COW, secondary maintenance, or device I/O. They are evidence that the
local targets are feasible, not a new competitor victory. The complete tables
will move only after the new representation becomes the sole durable path and
the same harness is rerun.

The default `CreateFrom` representation remains **verbatim**. Older benchmark
prose incorrectly called that path compact. The harness now labels explicit
compact and verbatim bulk artifacts separately. The compact artifact is
measured below at the later refresh commit, but it is bulk-only evidence—not
the mutable default and not a read-performance claim.

## Dedicated reads and ordered scans

The scan corpus has 100,000 documents. `iteration` touches one byte per value;
`all bytes` reads every value byte and is the ordered-scan throughput result.

| Engine | Random point | Iteration | Ordered all bytes | Point allocation | Scan allocation |
| --- | ---: | ---: | ---: | ---: | ---: |
| bbolt | **376.3 ns** | 8.479 ns/doc | **79.39 ns/doc** | 168 B / 3 | 576 B / 9 |
| Badger | 796.7 ns | 84.20 ns/doc | 203.7 ns/doc | 804 B / 8 | ~8.6 KiB / 44 |
| **vibejson** | 1,162 ns | **7.546 ns/doc** | 79.64 ns/doc | **0 B / 0** | **0 B / 0** |
| Pebble | 1,235 ns | 43.00 ns/doc | 112.4 ns/doc | 80 B / 2 | ~85 B / 1 |
| SQLite | 2,838 ns | 170.4 ns/doc | 244.5 ns/doc | 1,092 B / 22 | 27.4 MiB / 200,017 |

Interpretation:

- vibejson has the fastest iteration scan and is within 0.3% of bbolt on the
  stronger all-byte scan.
- vibejson all-byte scan is 1.41x faster than Pebble, 2.56x faster than Badger,
  and 3.07x faster than SQLite.
- vibejson point lookup beats Pebble by about 6% and SQLite by 2.44x, but is
  1.46x behind Badger and 3.09x behind bbolt. This is an open performance gap.
- all three vibejson paths allocate nothing.

## Pinned heterogeneous-default mixed workloads

These are reproducible diagnostics, not a durability-matched or sustained
storage leaderboard. At the pinned refresh commit, the old `-sync=false`
flag selected different acknowledgement boundaries: vibejson could return
while a generation was still in its private
commit queue, but its bounded background worker continuously writes COW pages
through ordered stable-storage fences and can backpressure the foreground
during the timed run. The other engines make writes visible without a
comparable barrier: Pebble may retain recent WAL bytes in-process, bbolt omits
`fdatasync`, Badger omits `msync`, and SQLite `synchronous=OFF` omits `xSync`.
They do eventually write or flush; the mismatch is that their timed operations
primarily pay volatile buffering while vibejson pays continuous stable
persistence.

The 10,000-document corpus is only about 2.4 MiB. It fits inside the configured
64 MiB caches and Pebble memtable, and the 20,000-operation run does not force
sustained LSM flush, compaction, or value-log GC. The harness also uses one
blocking client and times no final drain/flush/checkpoint. Read this table as
“warm, unflushed, short-burst, whole-document JSON workload,” not as total
engine capacity.

Earlier rows allowed Pebble, Badger, and bbolt to use blind set/delete calls
even though vibejson necessarily resolved the current row and SQLite verified
one affected row. The refreshed existing-key lane below charges every engine
an existence resolution. It is a single-owner benchmark contract, not a claim
of atomic conditional replace under outside concurrent writers. The command
also consumes the next deterministic key-trace positions after warmup instead
of replaying the warmup prefix. A separate blind-upsert lane is still required.

| Workload | vibejson | bbolt | Badger | Pebble | SQLite |
| --- | ---: | ---: | ---: | ---: | ---: |
| YCSB-B, 95% read / 5% update | 155,292 | 1,057,140 | 979,198 | **1,957,210** | 335,649 |
| YCSB-A, 50% read / 50% update | 14,714 | 155,332 | 286,277 | **1,178,533** | 180,891 |
| YCSB-F, 50% read / 50% RMW | 14,482 | 153,224 | 256,378 | **1,009,037** | 146,643 |
| Churn, read/update/delete+restore | 18,081 | 213,787 | 372,274 | **1,417,792** | 210,906 |
| Ordered-scan mix | 24,614 | 236,686 | 277,310 | **587,868** | 154,398 |

Values are total user operations per second in this one-client burst. The row
still exposes a real vibejson weakness—full-generation mutation materialization
and commit-buffer backpressure—but it does not establish a 25–154x sustained
engine deficit.

The pinned harness stopped its throughput timer before `Measure -> DiskBytes` forced
vibejson `Flush`, Pebble `Flush`, bbolt `Sync`, Badger `Sync`, and a SQLite WAL
checkpoint. That means competitors' final maintenance is outside throughput,
whereas vibejson's bounded queue forces part of its stable-persistence cost
inside throughput. Do not promote this table to a leaderboard.

The replacement framework now names `buffered-visible`,
`async-stable-in-flight`, `ordinary-sync`, and `power-safe` explicitly,
rejects unsupported engine/mode pairs, checkpoints the loaded baseline and
warmup, includes periodic and final checkpoint stalls in total throughput, and
prints checkpoint p50/p95/p99 separately. These pinned values predate that
timing correction and must be refreshed before becoming current performance
claims. Durable-prefix subprocess recovery remains a required follow-up.

Vibejson now has a real bounded `buffered-visible` implementation: mutation
admission ordinarily does not wake the device worker, while a checkpoint
publishes the captured COW generation cut through the alternate-root protocol.
Bounded staging pressure may force an earlier checkpoint, so a publishable run
must report `forced-cp=0`; the mixed harness samples that counter outside the
timed interval. The next table refresh explicitly selects the ordinary-
filesystem checkpoint strength and can therefore compare all five file-backed
engines in the same buffered lane; the pinned heterogeneous table above still
cannot be relabelled.

### Mutation latency

| Operation | vibejson p50 / p95 / p99 | Best competitor p50 / p95 / p99 |
| --- | ---: | ---: |
| YCSB-B update | 37.979 / 52.228 / 3,975.479 µs | Pebble 1.083 / 1.895 / 4.667 µs |
| YCSB-A update | 50.125 / 78.812 / 4,129.458 µs | Pebble 1.021 / 1.688 / 2.667 µs |
| YCSB-F read-modify-write | 47.208 / 78.584 / 4,428.604 µs | Pebble 1.312 / 2.208 / 4.250 µs |
| Delete + restore | 131.604 / 2,330.729 / 5,895.480 µs | Pebble 1.395 / 2.187 / 3.625 µs |
| Ordered all-byte scan, 10k docs | 1,113.958 / 1,165.417 / 1,184.188 µs | bbolt 824.646 / 899.583 / 928.688 µs |

Existing-key resolution reduces the apparent throughput gap, but does not erase it:
Pebble remains 12.6x ahead on YCSB-B, 80.1x on YCSB-A, 69.7x on YCSB-F,
78.4x on churn, and 23.9x on the scan mix. vibejson's multi-millisecond p99
stalls are still the main limiter. A proposed write optimization does not pass
the default-path gate unless it removes those stalls without changing the read
path.

## Pinned synchronous-mode mixed workloads (pre existing-key refresh)

This is the old `-sync=true` matrix at refresh commit `2535c32`, in total user
operations per second. Its competitor adapters still use the older blind
mutation semantics, so it remains useful for durability-bound orientation but
must be refreshed before making a new existing-key performance claim. It
combined multiple synchronous strengths and therefore does **not** pretend the
guarantees are the same on Darwin:

- vibejson explicitly issues `F_FULLFSYNC`.
- SQLite uses `synchronous=FULL` and `fullfsync=1`; this is the comparable
  power-loss-safe pair.
- bbolt and Pebble issue plain `fsync`, which does not drain the drive's
  volatile cache on Darwin.
- Badger uses `msync(MS_SYNC)` and exposes no `F_FULLFSYNC` mode.

| Workload | vibejson | bbolt† | Badger† | Pebble† | SQLite | vibejson vs comparable SQLite |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| YCSB-B | 3,279 | 2,439 | 468,384 | 4,866 | **3,677** | -10.8% |
| YCSB-A | 360 | 232 | 65,338 | 482 | **422** | -14.7% |
| YCSB-F | 385 | 233 | 68,758 | 470 | **411** | -6.3% |
| Churn | 532 | 339 | 87,684 | 691 | **605** | -12.1% |
| Ordered-scan mix | 726 | 449 | 100,501 | 951 | **814** | -10.8% |

`†` means a weaker Darwin persistence boundary, so those larger or smaller
numbers are operational measurements, not crash-safety wins. The pinned
power-loss-safe result is unambiguous: vibejson trails SQLite by 6.3–14.7%
across all five mixes.

Median mutation latency distributions for the comparable pair:

| Operation | vibejson p50 / p95 / p99 | SQLite p50 / p95 / p99 | p50 gap |
| --- | ---: | ---: | ---: |
| YCSB-B update | 5.895 / 7.051 / 8.610 ms | **5.050 / 6.035 / 6.316 ms** | +16.7% |
| YCSB-A update | 5.773 / 6.160 / 7.083 ms | **4.845 / 5.233 / 6.866 ms** | +19.2% |
| YCSB-F read-modify-write | 5.044 / 6.030 / 7.059 ms | **4.927 / 5.151 / 6.019 ms** | +2.4% |
| Churn update | 5.076 / 6.087 / 6.524 ms | **4.879 / 5.579 / 6.033 ms** | +4.0% |
| Churn delete + restore | 10.096 / 12.091 / **12.588** ms | **9.125** / **10.948** / 13.988 ms | +10.6% |
| Scan-mix update | 5.160 / 6.174 / 7.884 ms | **4.858 / 5.424 / 7.055 ms** | +6.2% |
| Scan-mix delete + restore | 10.982 / 12.983 / 14.529 ms | **9.791 / 10.754 / 11.349 ms** | +12.2% |

The scan mix executes only two full scans per process, so its within-process
scan percentile is not statistically useful. Use the 100,000-document
dedicated ordered-scan table above for scan latency; use this section only for
the complete mixed-workload throughput.

## Historical fsync-class mixed workloads

This table is retained to make the measured history reproducible, but it is
**not the current power-loss durability table**. At the pinned commit,
vibejson, bbolt, and Pebble used Darwin `fsync`, which Apple documents as not
draining a drive's volatile cache. Badger used `msync(MS_SYNC)`. Only the
SQLite row enabled `fullfsync=1`.

The current table above supersedes these numbers. Do not quote the vibejson row
below as its current sync result.

| Workload | vibejson | bbolt | Badger* | Pebble | SQLite |
| --- | ---: | ---: | ---: | ---: | ---: |
| YCSB-B | 2,446† | 1,591† | 403,200† | 3,031† | **2,892** |
| YCSB-A | 306† | 171† | 14,869† | 321† | **297** |
| Churn | 329† | 253† | 49,390† | 431† | **530** |
| Ordered-scan mix | 573† | 317† | 36,559† | 607† | **599** |

Values are total user operations per second. `†` means the row did not drain
the Darwin drive cache and is not power-loss comparable with SQLite or current
vibejson.

vibejson mutation latency:

| Operation | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| YCSB-B update | 8.915 ms | 10.494 ms | 11.479 ms |
| YCSB-A update | 5.605 ms | 10.107 ms | 11.543 ms |
| Churn update | 9.421 ms | 10.946 ms | 12.107 ms |
| Delete + restore | 17.919 ms | 20.420 ms | 22.423 ms |
| Scan-mix update | 6.044 ms | 9.584 ms | 11.227 ms |

## Disk and retained memory

The corpus is 100,000 unindexed documents. Disk comparisons use allocated
blocks, not sparse apparent size. Low- and high-cardinality files were identical
for every row at this commit because the measured vibejson representation was
verbatim and competitor compression was disabled.

| Engine | Apparent / allocated | HeapAlloc | Runtime resident | Peak RSS |
| --- | ---: | ---: | ---: | ---: |
| **vibejson bulk, compact, low-cardinality** | **13.9 / 13.9 MiB** | 15.1 MiB | 25.7 MiB | 174.2 MiB |
| **vibejson bulk, compact, high-cardinality** | **26.1 / 26.1 MiB** | 15.1 MiB | 25.5 MiB | 176.3 MiB |
| Badger | 257.0 / **26.6 MiB** | 86.1 MiB | 97.0 MiB | 154.1 MiB |
| SQLite | 28.1 / 28.1 MiB | **2.5 MiB** | 13.0 MiB | 152.3 MiB |
| bbolt | 45.8 / 29.7 MiB | **2.5 MiB** | **12.9 MiB** | **91.4 MiB** |
| **vibejson bulk, verbatim** | 32.2 / 32.2 MiB | 16.6 MiB | 27.5 MiB | 174.4 MiB |
| vibejson Put replay | 35.9 / 36.5 MiB | 16.6 MiB | 28.8 MiB | 185.6 MiB |
| Pebble | 50.6 / 50.7 MiB | 36.3 MiB | 46.6 MiB | 114.9 MiB |

The raw corpus is 23.73 MiB. Verbatim vibejson bulk is 21% larger than Badger,
15% larger than SQLite, and 8% larger than bbolt, but 36% smaller than Pebble's
total including its live unretired WAL. Explicit compact bulk is deterministic
at 13.9 MiB on the highly redundant low-cardinality corpus and 26.1 MiB on the
shape-matched high-cardinality corpus. The latter is still 1.9% smaller than
Badger, 7.1% smaller than SQLite, 12.1% smaller than bbolt, and 48.5% smaller
than Pebble in this compression-disabled comparison.

That space result does not promote the existing compact codec. It is not
emitted by ordinary mutation replay, and its paired point/all-byte read cost is
reported separately before any default-format decision. The old 15.9 MiB
vibejson claim remains withdrawn: that earlier adapter did not actually enable
compact documents.

## Secondary index

An exact country index selecting 945 of 100,000 documents:

| Engine | Query | Allocations | Indexed file | Index bytes |
| --- | ---: | ---: | ---: | ---: |
| **vibejson** | **36.108 µs** | 1,328 B / 17 | 34.6 MiB | +2.4 MiB |
| SQLite | 539.705 µs | 568 B / 20 | 30.3 MiB | +2.2 MiB |

vibejson is 14.95x faster for this exact indexed filter. The key/value engines
do not expose native JSON secondary indexes. No current multi-index
deduplication footprint run exists, so this baseline makes no dedup ratio claim.

## Reproduction

From `bench/competitive`:

The mixed tables above are pinned measurements from before explicit
checkpoint accounting; use their listed commits for byte-for-byte historical
reproduction. The current commands below select every engine's mode explicitly
and include the final checkpoint in throughput, so their output is the refresh
input rather than a promise to reproduce the older values.

```sh
go test -run 'TestFullEquivalence|TestCorpusVariantsAreShapeMatched' -count=1 -timeout=60m .

go build -o /tmp/vibejson-mixed ./cmd/mixed
go build -o /tmp/vibejson-mixedsuite ./cmd/mixedsuite
for w in ycsb-b ycsb-a ycsb-f churn scan; do
  /tmp/vibejson-mixedsuite -mixed-bin=/tmp/vibejson-mixed \
    -workload="$w" -durability=buffered-visible \
    -checkpoint-mutations=64 -output="mixed-${w}-buffered.tsv"

  /tmp/vibejson-mixedsuite -mixed-bin=/tmp/vibejson-mixed \
    -engines=bbolt,badger,pebble,sqlite \
    -workload="$w" -operations=2000 -warmup=200 \
    -durability=ordinary-sync -checkpoint-mutations=0 \
    -output="mixed-${w}-ordinary-sync.tsv"

  /tmp/vibejson-mixedsuite -mixed-bin=/tmp/vibejson-mixed \
    -engines=vibejson-durable,sqlite \
    -workload="$w" -operations=2000 -warmup=200 \
    -durability=power-safe -checkpoint-mutations=0 \
    -output="mixed-${w}-power-safe.tsv"
done

go test -run '^$' \
  -bench='^Benchmark(PointRead|Scan|ScanAllBytes)/(vibejson-durable|bbolt|badger|pebble|sqlite)$' \
  -benchtime=2s -count=3 -timeout=30m .

go build -o /tmp/vibejson-footprint ./cmd/footprint
for rep in {1..3}; do
  for card in low high; do
    for e in vibejson-durable bbolt badger pebble sqlite; do
      /tmp/vibejson-footprint -engine="$e" -cardinality="$card"
    done
    /tmp/vibejson-footprint -engine=vibejson-durable -putloop -cardinality="$card"
    /tmp/vibejson-footprint -engine=vibejson-durable -compact -cardinality="$card"
  done
done

go test -run '^$' \
  -bench='^BenchmarkIndexedFilter/(vibejson-durable|sqlite)$' \
  -benchtime=2s -count=3 .
```

The short scan workload contains only two ordered scans per run, so its
within-run percentiles collapse to one order statistic. Use the dedicated
100,000-document scan benchmark for ordered-scan comparisons.
