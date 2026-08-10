# Competitive results

> **2026-08-10 focused development result (not a published-suite
> replacement).** Five isolated 1-second runs of
> `BenchmarkPointWriteSameSize/(vibedb|badger)/durability=buffered-visible`
> measured a 7.295 µs VibeDB median and 7.821 µs Badger median on the Apple M4
> Max. VibeDB is 6.7% lower-latency in this same-size replacement gate. Its
> median allocation result was 339 B and 0 allocations versus Badger's 3,311 B
> and 46 allocations. Before the compact column-patch change the VibeDB median
> was 39.562 µs, making the focused engine path 5.42× faster. The
> new byte-equivalence oracles compare single- and multi-shape column patches
> against a complete compact rebuild, and an end-to-end harness test requires
> the lane to engage rather than silently fall back. No storage-format or bulk
> footprint bytes changed. A separate three-sample disjoint-bucket qualification
> measured median replacement rates of 166.5k, 394.5k, and 718.5k ops/s at 1,
> 8, and 32 clients, with zero allocations and no materialization or exclusive
> fallbacks. The full mixed-workload tables below retain their
> 2026-08-03 provenance until that complete protocol is regenerated.

> **Current published snapshot.** The mixed-throughput, concurrency, bulk
> footprint, and CPU/scan-gate tables below were regenerated on 2026-08-03 from
> engine commit `c1dea2b25d15d810efc85e65ad5f312f34b903e5` (branch `main`) on an
> otherwise idle Apple M4 Max. Unlike the prior snapshot, these binaries were
> **not** built from a clean tree: at regeneration the working tree carried
> pending documentation and test changes **and one uncommitted non-test engine
> change**, `store/durable/store_file_primary_mutation.go` (a three-line
> `ErrPrimaryLeafSplitRequired` return on the primary mutation path; merged to
> `main` as `aaa4dfa` after these measurements were taken). Every
> regenerated binary therefore stamps `vcs.modified=true` and every suite TSV
> records `git-dirty=true`; by rule 1 of the publishing rules these are
> disclosed-dirty refresh measurements, not a clean-commit certified snapshot. An
> artifact-verifiable clean control isolates that uncommitted change. Because Go
> does not VCS-stamp linked-worktree builds, the control was built from a fresh
> `git clone` checked out detached at `c1dea2b` (empty `git status --porcelain`,
> no `store_file_primary_mutation.go` diff); both control binaries stamp
> `vcs.revision=c1dea2b` and `vcs.modified=false`, certifying the clean build
> independently of any runtime field. Run with those binaries, the control's
> vibedb single-client YCSB-A median is 56,593.0 total-ops/s, 2.4% above the
> dirty-tree published 55,256.5, and its Badger median is 266,553.5, 4.4% below
> the published 278,694 — both within this lane's run-to-run variance. The
> uncommitted change therefore does not materially move the published numbers,
> and the regression below is attributable to committed
> compact-primary-storage-default behavior, not to the uncommitted diff. That
> control's own suite TSV still records `git-dirty=true` because the harness
> resolves `git-root` from the process's runtime working directory — the shared
> dirty main checkout — so that field describes the runtime environment, not the
> certified build provenance the binaries' VCS stamp carries. The
> "Disk under sustained churn" section was **not** re-run and retains its older
> provenance, annotated in place.

This snapshot supersedes and reverses the prior headline. The prior tables were
stamped 2026-08-01 at clean commits `7fe6769` / `b5702bc`, which predate compact
primary storage becoming the default (commit `55e1918`, 2026-08-02) and the
subsequent optimization streak. Compact primary storage trades point- and
update-path throughput for a dramatically smaller on-disk image. Measured at
`c1dea2b`, single-client vibedb is now **0.20–0.30× Badger** on the update-heavy
YCSB-A, YCSB-F, churn, and scan-mix workloads, and only the 95%-read YCSB-B lane
remains ahead of Badger, at **1.13×**. In exchange the immutable unified-bulk
image is **0.973 MiB** at low cardinality and **6.609 MiB** at high — smaller
than the JSON corpus's own gzip-9 output. Both results are published as measured;
the throughput regression is a named, accepted cost of the compact-storage and
correctness work, not a defect to hide.

## Provenance and protocol

- Machine: Apple M4 Max (16 logical CPUs, 64 GB), macOS / Darwin 25.3.0, APFS,
  Go 1.26.5.
- Competitors: bbolt 1.5.0, Badger 4.9.5, Pebble 1.1.5, modernc SQLite
  1.54.0.
- Corpus: 10,000 documents for throughput and 100,000 for churn-disk and
  footprint; low cardinality unless a table says otherwise.
- Throughput shape: 2,000 warmup operations, 20,000 measured operations,
  buffered-visible durability, and a CP64 acknowledged-mutation threshold.
- Each throughput cell is the median of ten recorded repetitions. Engines run
  in isolated child processes, with deterministic Latin-square ordering and
  one unrecorded conditioning pass per engine.
- Footprint cells are one isolated run and are **apparent / allocated MiB**
  derived from the harness's exact byte columns.
- Every suite records the commit, dirty bit, binary hash, effective options,
  corpus shape, engine order, repetitions, and pressure-forced checkpoints.
  All regenerated suites report `maximum-forced-checkpoints=0` and the harness
  fields `publishable-suite=true`, `publishable-checkpoint-cadence=true`, and
  `publishable-repetition-count=true`; the single disqualifier from a *clean*
  snapshot is `git-dirty=true` / `vcs.modified=true`, disclosed above.
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

Total operations per second, median of ten. The leading engine per row is bold.

| workload | vibedb | Badger | SQLite | Pebble | bbolt | vibedb / Badger |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| YCSB-A: 50% read, 50% update | 55,256.5 | **278,694** | 106,493.5 | 24,601 | 22,270.5 | 0.20× |
| YCSB-B: 95% read, 5% update | **1,131,998.5** | 999,254 | 314,282.5 | 215,019 | 208,194.5 | **1.13×** |
| YCSB-F: 50% read, 50% read-modify-write | 55,673 | **254,557** | 95,063 | 23,583.5 | 21,693 | 0.22× |
| Churn: 70% read, 25% update, 5% delete+restore | 77,564 | **340,236** | 120,195.5 | 31,984 | 27,478.5 | 0.23× |
| Scan mix: 79.9% read, 15% update, 5% delete+restore, 0.1% full scan | 73,749.5 | **248,360** | 112,273.5 | 43,804 | 39,230 | 0.30× |

The read-dominated YCSB-B lane is the only one vibedb still leads: 1.13× Badger
and 3.60× SQLite. On the four update-heavy lanes vibedb now trails Badger by
3.3–5.0× and is also below SQLite on YCSB-A (0.52×) and YCSB-F (0.59×). The
compact primary leaf must render and re-plan on the mutation path; the CPU-gate
section quantifies the per-leaf render cost that this throughput regression
tracks.

### vibedb operation latency

Median of the ten run-level percentiles, microseconds:

| workload | operation p50 / p99 | checkpoint p50 / p99 |
| --- | ---: | ---: |
| YCSB-A | read 0.167 / 1.25; update 5.0625 / 36.583 | 32.5835 / 18,555.0625 |
| YCSB-B | read 0.125 / 1.042; update 5.167 / 197.312 | 199.417 / 891.312 |
| YCSB-F | read 0.167 / 1.25; RMW 5.25 / 38.0415 | 34.041 / 18,600.2915 |
| Churn | read 0.167 / 1.25; update 5.125 / 40.3745; delete+restore 8.9585 / 50.771 | 35.875 / 20,052.979 |
| Scan mix | read 0.375 / 2.3125; update 5.417 / 41.5415; delete+restore 9.583 / 59.8125; ordered scan 5,338.375 / 5,697.3545 | 38.5835 / 19,238.4375 |

Point-read p50 stays sub-microsecond, but update p50 rose to 5.06–5.42 µs (from 1.83–1.90 µs
pre-compact) and the checkpoint p99 rose to 18.6–20.1 ms on the update-heavy
lanes (from 40.6–44.8 µs). The compact checkpoint fold is the dominant new tail:
folding a mutated compact stripe on the foreground checkpoint path is far more
expensive than the prior in-place leaf write. YCSB-B, whose 5% update rate keeps
checkpoints small, retains a sub-millisecond checkpoint p99 (891.312 µs).

## Concurrent replacement and churn

Total operations per second, median of ten. `write` is 100% existing-key
replacement; `churn` has the same 70/25/5 read/update/delete+restore mix as
above. The leading engine per row is bold.

| workload | clients | vibedb | Badger | SQLite | Pebble | bbolt | vibedb / Badger |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| write | 1 | 27,257 | **160,065** | 62,483 | 12,501 | 11,079 | 0.17× |
| write | 8 | 25,202 | **225,019.5** | 50,881 | 13,356 | 11,668 | 0.11× |
| write | 32 | 24,578.5 | **244,008.5** | 52,943.5 | 13,147 | 11,212 | 0.10× |
| churn | 1 | 80,515.5 | **355,861** | 136,716.5 | 34,145.5 | 30,283 | 0.23× |
| churn | 8 | 76,935.5 | **502,165** | 103,564.5 | 38,027 | 26,259 | 0.15× |
| churn | 32 | 75,235 | **538,896.5** | 103,092 | 38,789.5 | 18,825.5 | 0.14× |

The concurrency picture also reversed. vibedb does not scale with client count
in this lane: replacement throughput edges *down* from 27,257 to 24,578.5
(1 → 32 clients) and churn from 80,515.5 to 75,235, because the compact
mutation-and-fold path serializes on the foreground checkpoint. Badger scales up
(write 1.52×, churn 1.51× from 1 to 32), so the vibedb / Badger ratio falls as
clients rise. Restoring concurrent scaling on the compact mutation path is the
named follow-up here.

## Disk under sustained churn

> **Not re-run in the 2026-08-03 pass.** The tables in this section retain their
> 2026-08-01 provenance from clean commit
> `7fe67691dd889a34951682d2522661c7741d8720`, which predates compact primary
> storage becoming the default. The `cmd/churndisk` matrix (five engines × two
> cardinalities × two profiles, each a sustained 200,000-mutation run) exceeds
> the wall-time budget of this pass and was deferred. Because the vibedb rows
> below reflect the pre-compact-default online-churn representation, they are
> **stale relative to the current default** and are kept only to avoid deleting
> published data; treat every vibedb cell here as pending a compact-default
> re-measurement. Partial vibedb-only churn-disk probes *were* re-run at
> `c1dea2b` (saved in the raw logs; not published as table cells because the
> cross-engine matrix was not re-run). They measure the current compact-default
> online image well below these pre-compact rows — for example high-cardinality
> intrinsic online 22.376 / 18.168 MiB (23,462,912 / 19,050,496 bytes) versus the
> 54.841 / 36.070 shown here, and low-cardinality online 5.497 / 5.426 MiB versus
> 22.075 / 16.020 — so the retained vibedb numbers are conservative: they
> understate, not overstate, the current default.

`cmd/churndisk` keeps 100,000 documents live through 200,000 acknowledged
state changes. Eighty percent of random choices are one-change replacements;
the rest are indivisible delete+reinsert pairs. Checkpoint and sampling
cadences are mutation thresholds, so a pair may cross one by a single change.

Cells are **apparent / allocated MiB**. `online` is measured immediately after
the workload and all requested CP64 checkpoints. `offline` is a separate
out-of-place `durable.Repack` result and is not required to bound online growth.

### Intrinsic representation (2026-08-01 / 7fe6769, pre-compact-default — not re-run)

Optional SST compression is disabled.

| engine | low online | low offline | high online | high offline |
| --- | ---: | ---: | ---: | ---: |
| vibedb | **22.075 / 16.020** | **9.001 / 9.520** | 54.841 / 36.070 | **18.767 / 19.520** |
| SQLite | 28.109 / 28.109 | 26.234 / 26.234 | **28.109 / 28.109** | 26.234 / 26.234 |
| bbolt | 45.750 / 45.750 | 45.750 / 45.750 | 45.750 / 45.750 | 45.750 / 45.750 |
| Pebble (median of 3) | 93.864 / 95.141 | 103.447 / 99.859 | 93.863 / 95.320 | 103.445 / 95.703 |
| Badger | 314.769 / 72.641 | 314.769 / 72.641 | 314.770 / 72.645 | 314.770 / 72.645 |

### Production-compressed LSM control (2026-08-01 / 7fe6769, pre-compact-default — not re-run)

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

These pre-compact online rows are retained for continuity only; the current
default's online-churn image is expected to differ and is a named
re-measurement.

## Bulk footprint

The low- and high-cardinality corpora have identical shape and length:
24,881,153 JSON bytes (23.729 MiB) plus 1,200,000 key bytes (1.144 MiB), or
26,081,153 key-inclusive logical bytes (24.873 MiB), for 100,000 documents.
Their JSON-only gzip-9 sizes are 1.837 MiB and 8.041 MiB, respectively (measured
1,925,945 and 8,431,529 bytes by `footprint -corpus-stats`, the harness's
JSON-only entropy control; both corpora hold 24,881,153 identical JSON bytes and
differ only in value entropy). Cells below are one isolated run and are
**apparent / allocated MiB**. VibeDB's ordinary buffered unified-bulk images are
immutable here, so their lazy recovery journal does not yet exist and these rows
are the complete footprint; the point-put rows have mutated and include the
sibling. Only the vibedb rows changed since 2026-08-01: the unified-bulk image
is measured at 0.973 / 6.609 MiB (resolving the earlier hand-edit), and the
point-put build fell from 16.341 / 28.606 to 4.122 / 11.821 MiB apparent.

### Intrinsic representation

| engine | low cardinality | high cardinality |
| --- | ---: | ---: |
| vibedb unified bulk, immutable | **0.973 / 0.973** | **6.609 / 6.609** |
| vibedb point-put build | 4.122 / 4.203 | 11.821 / 10.133 |
| SQLite | 28.109 / 28.109 | 28.109 / 28.109 |
| bbolt | 45.750 / 29.734 | 45.750 / 29.734 |
| Pebble | 50.611 / 50.664 | 50.611 / 50.664 |
| Badger | 257.000 / 26.621 | 257.000 / 26.621 |

### Production-compressed LSM control

| engine | low cardinality | high cardinality |
| --- | ---: | ---: |
| vibedb unified bulk, immutable | **0.973 / 0.973** | **6.609 / 6.609** |
| vibedb point-put build | 4.122 / 4.203 | 11.821 / 11.008 |
| SQLite | 28.109 / 28.109 | 28.109 / 28.109 |
| bbolt | 45.750 / 29.734 | 45.750 / 29.734 |
| Pebble, Snappy | 33.978 / 34.000 | 40.993 / 41.027 |
| Badger, Snappy configured | 257.000 / 26.621 | 257.000 / 26.621 |

Badger's bulk corpus is still resident in its configured mutable table, so the
bulk row has no meaningful compressed SST payload; the churn table is the
materialized production-compressed comparison. Vibedb's compactness is
structural: repeated canonical JSON skeletons and scalar spellings are shared
within a leaf, while uncommon shapes stay verbatim. It is not a generic gzip
comparison and does not require a second storage mode. The high-cardinality
point-put apparent image (11.821 MiB) exceeds the immutable unified-bulk image
(6.609 MiB) because it has mutated and carries its bounded sibling journal.

## Current CPU and scan gates

These five-sample Go microbenchmarks were re-run on 2026-08-03 at engine commit
`c1dea2b` on the same host, except for the masked-scan row noted below. They are
regression gates, not cross-engine database results.

| gate | median | allocation |
| --- | ---: | ---: |
| stable native checkpoint leaf fold | **1.826 µs** | 0 B, 0 allocs |
| full render/replan/encode of that leaf | 248.532 µs | 0 B, 0 allocs |
| ordered scan, 100k three-scalar documents | 117.4 ns/document | 0 B, 0 allocs |
| full competitive scan, low cardinality | 328.6 ns/document | 0 B, 0 allocs |
| full competitive scan, high cardinality | 471.5 ns/document | 0 B, 0 allocs |
| masked scan, one occupied row per live posting tile | 631.5 ns/selected document | 0 B, 0 allocs |

The certified native fold is about 136× faster than full replanning
(248.532 / 1.826 µs). The ordered-scan and full-competitive-scan gates regressed
against their pre-compact 2026-08-01 values (23.49 → 117.4 ns/document ordered;
91.57 / 94.21 → 328.6 / 471.5 ns/document competitive): reconstructing a compact
leaf on the read path is the same cost that depresses the update-heavy
throughput lanes. The masked-scan setup was repaired after compact stripes
replaced the retired class-5 leaves it inspected. That row is the median of
five 1-second samples run on 2026-08-10 at `387707c` plus the benchmark/test
fix, with Go 1.26.0 on the same Apple M4 Max. Samples ranged from 626.3 to
634.9 ns/selected document, with 0.2502 page pins per selected document, zero
cache misses, and zero allocations. The prior 178.4 ns/selected-document figure
is not carried forward because it predates compact primary storage.

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

This snapshot satisfies rules 2–9. It does not satisfy rule 1's clean-tree
expectation: it was regenerated against a dirty working tree and every binary
stamps `vcs.modified=true`. That deviation is disclosed at the top of this file
and in every saved TSV's `git-status` meta field. The rationale and complete
harness contract are in the [competitive benchmark guide](README.md).

## Reproduction

From `bench/competitive`. On a clean tree the binaries will stamp
`vcs.modified=false`; the tables above were produced on a dirty tree, so a
faithful reproduction of the *published* numbers must use commit `c1dea2b`, and a
clean-commit re-run is the named follow-up before re-asserting any headline
ratio.

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
# vibedb point-put build image adds -putloop.
```

Run timing lanes serially on an otherwise idle host. The exact micro-gate
commands are listed in the [benchmark guide](README.md).
