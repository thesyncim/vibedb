# Rightmost-tail split and scalar alphabet write qualification, 2026-09-27

The latest cumulative comparison uses the frozen `72c3a30a` candidate and a
main-derived storage control. Its ratio of median control time to candidate time
is 3.8686× for distinct varied payloads and 3.1213× for shared payloads. These
results are materially below the earlier `0478cfad` comparison (6.94× and 5.33×),
and the requested 10× minimum remains unmet. The earlier raw records remain
unchanged. See the [qualification index](../README.md) and the earlier
[write geometry report](../write-geometry-2026-09-26/README.md).

## Latest cumulative result: alphabet-union candidate, 2026-09-27

Three alternating 1× pairs compared frozen `72c3a30a` against a main-derived
storage control. Each run applied 131,072 rows in 2,048 batches of 64 through SQL
`ReplicatedApply`, included three durable collections, and timed the final full
checkpoint and fold. RF3, network traffic and proposal admission were excluded.
Database/schema setup, post-append and post-fold statistics, file-space samples,
and the final full-row oracle were outside timing. Per-batch key/value generation,
mutation and command construction, apply, and its completion/publication checks
were timed. File-space observations are sampled every 64 batches and do not
establish a maximum.

The paired sample order was control/candidate, candidate/control, then
control/candidate. Times below are `ns/op` values rounded to six decimal places
in seconds; raw values are in the JSON. Each
paired ratio is control time divided by candidate time. The final column is the
ratio of the two medians; it is reported separately from the median paired ratio.

| Workload | Control times, pairs 1/2/3 | Candidate times, pairs 1/2/3 | Paired ratios, pairs 1/2/3 | Ratio of medians |
| --- | --- | --- | --- | ---: |
| Distinct varied payloads | 94.316476 / 33.055896 / 30.558281 s | 32.254980 / 8.544646 / 6.635903 s | 2.9241× / 3.8686× / 4.6050× | 3.8686× |
| Shared 256-byte payloads | 34.416353 / 32.707544 / 28.256256 s | 10.478739 / 11.303406 / 6.991031 s | 3.2844× / 2.8936× / 4.0418× | 3.1213× |

The candidate's final allocated-file medians were 63,934,464 B for varied data
and 37,789,696 B for shared data, compared with 72,933,376 B and 42,487,808 B
for the control. These counts cover all three durable collections. Other storage
counters changed with the write path: user-relation device bytes were
167,862,272 → 28,471,296 B (varied) and 134,651,904 → 2,322,432 B (shared),
while system device bytes rose from 204,800 B to 1,945,600 B and 1,740,800 B,
respectively, due to periodic folds. Physical leaf-split counts were 511 → 2 for
varied data and 627 → 0 for shared data; barrier syncs were 2,052 → 76 and
2,516 → 68. The physical split counter does not count every logical tail split.
These counters are reported as measured; equality between control and candidate
is not expected.

The control is not a whole-main build. It is the frozen `6435ff51` binary made
from the diagnostic `5a689170` scaffold with seven storage files overlaid from
main `f9a9627a`; its SQL benchmark source is the `9a1365239` version, SHA-256
`729aecb98499032f9e667a91d5e71a694e1470ae31ec93cb48a3faecef3ece27`. A
list-only run of that binary confirmed both benchmark names. The candidate is
the frozen `72c3a30a` binary, SHA-256
`281abf63439f0d7c384bc6bedd9a51c65ecdd06560c5e6e43100c02650d181ac`; its
alphabet codec source SHA-256 is
`c7c3df1f1457387de4ac83e5be53811892730efedfe3d43f4bbd9cadea7678cb`. The
candidate benchmark source adds four untimed tail-charge metric reports to the
control source; the workload and timed code are unchanged. Exact provenance and
all per-run metrics are in [cumulative runs](alphabet-cumulative-runs.json) and
[cumulative metadata](alphabet-cumulative-metadata.json).

This host was an Apple M4 Max with 64 GiB on macOS 26.3.1(a), build 25D771280a;
the binaries reported Go 1.27.0 and Darwin/arm64. Sample times varied widely,
especially for the varied workload. A post-run snapshot around 08:42 UTC showed
load averages of 9.26 / 17.93 / 17.39 and 81% free memory. This snapshot was not
time-aligned with individual runs, and cannot establish or explain the timing
spread. The cumulative result is a shared-host diagnostic, not a general
throughput guarantee.

Two one-shot CPU profiles on the frozen candidate included benchmark setup and
oracle work as well as timed work. The shared profile recorded 3.93 s of CPU
samples over a 5.15 s pprof duration and 5.20 s process wall time; the varied
profile recorded 3.10 s over 4.34 s and 4.39 s, respectively. The exported tables
show cumulative `encodeShapeWithRankContext`, `finishAlphabet`, prefix parsing,
dictionary/alphabet measurement and front measurement entries. The shared
profile also sampled `RecoveryJournal.AppendPreparedConditionalBatch` for 1.80 s
(45.80% of CPU samples) and `TxnMarker.EntryCurrent` for 0.53 s (13.49%). The
varied profile sampled those entries for 1.03 s (33.23%) and 0.55 s (17.74%).
Cumulative profile entries overlap and are not additive. Profile elapsed times
are diagnostic only. See [profile summary](alphabet-profile-summary.json).

## Method for the earlier 0478 comparison

Each operation applies 131,072 rows as 2,048 batches of 64 through SQL
`ReplicatedApply`, then performs the final checkpoint and fold. The three durable
collections are included. Network traffic, RF3 replication and proposal admission
are excluded. Three runs per workload alternate main/candidate order. Per-batch
key/value generation, mutation and command construction, apply and its
completion/publication checks, and the final
checkpoint/fold are timed. Database/schema setup, final full-row verification,
post-run statistics and file-space sampling are outside the timer. The benchmark
samples file-space every 64 batches; those peak values are observations, not hard
limits. The host was an Apple M4 Max with 64 GiB on macOS
26.3.1(a), build 25D771280a. Both binaries report Go 1.27.0, `GOEXPERIMENT=simd`,
Darwin/arm64. Other applications remained active.

Each apply uses the checkpoint group's marker-backed publication path. The final
full-group checkpoint certifies and folds the last cut, and its full cost is inside
the timed operation; this measurement does not stop at batch publication.

The main comparison binary is a diagnostic scaffold at `5a689170`, with seven
storage files replaced from main `f9a9627a`; it is not a whole-main build. The
candidate binary is revision `0478cfad`. Source and binary hashes, the exact overlay,
per-run commands, and all raw values are in [metadata](metadata.json) and
[write runs](write-runs.json). Commands use the `project-env` wrapper and substitute
`<main-driver.test>` or `<candidate-driver.test>` for the frozen binary path. The
benchmark source adds four candidate-only tail-reservation reporting metrics; the
row workload and timing boundaries are unchanged.

## Write results

Medians of the three fixed-work runs include final fold time:

| Workload | Main | Candidate | Speedup | Main final allocated-file median | Candidate final allocated-file median | User relation device bytes: main → candidate | System device bytes: main → candidate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Distinct varied payloads | 24.252 s | 3.495 s | 6.94× | 73,195,520 B | 63,111,168 B | 167,862,272 → 28,471,296 B | 204,800 → 1,945,600 B |
| Shared 256-byte payloads | 27.919 s | 5.240 s | 5.33× | 42,487,808 B | 37,789,696 B | 134,651,904 → 2,322,432 B | 204,800 → 1,740,800 B |

The `append-leaf-splits` metric counts physical structural split transactions; it
does not count every logical tail split in the candidate. The candidate reports
61,813 B of current tail charge at the end of the varied append workload and a
5,245,093 B peak charge. After final fold, current charge is zero while the recorded
peak remains. Shared payload reports zero current charge and a 3,336,517 B peak.
Current charge covers lineage metadata and retained router history. Peak charge
also includes the dirty frame arena and in-flight frame, router, planning scratch
and fold reservations. These are admission-budget charges, not total process memory
or RSS. Final allocated file bytes cover all three collections. The user-relation
device bytes are not whole-process totals; system device bytes rose with periodic
folds. `write-runs.json` contains the twelve raw measurement rows and all recorded
metrics.

## Read control

One fresh matched pair ran 50 passes over each of four cases: varied/shared payloads
in sequential/permuted order. The input corpus was unchanged and the benchmark
source SHA-256 is identical for both binaries. Candidate versus main measurements
were 457.6 vs 462.8 ns/read (varied sequential), 707.7 vs 820.1 (varied permuted),
291.0 vs 330.3 (shared sequential), and 419.0 vs 493.2 (shared permuted). Every case
reported zero allocations and zero cache misses. This single pair is diagnostic; it
does not support a general read-latency claim. The complete counters and hashes are
in [read runs](read-runs.json).

## Validation and limits

The local full durable SIMD suite passed in 900.480 seconds. The final 0478 CI
snapshot had 45 successful checks and two optional skips across its workflows; [the
final CI run](https://github.com/thesyncim/vibedb/actions/runs/36287261792) includes
SQL and race results. An initial full local SQL run took 205.719 seconds and failed
`TestReplicatedApplyBatch64StructuralCertification`. Its fixture was corrected to
keep the certification assertion on the physical split path. Focused race and
SIMD-disabled checks passed after that correction. The initial SQL failure is
retained in this record rather than recast as a pass. CPU profiles include setup and
oracle work, so their timings are not used as benchmark comparisons.

This diagnostic is not an RF3 or end-to-end network throughput result. The fresh
candidate speeds up both fixed workloads substantially, but it does not meet the
10× target.

## Reproduction

Build the main-derived control in a clean checkout of scaffold `5a689170`, then
replace the seven storage files below with their `f9a9627a` versions and the SQL
benchmark file with the `9a1365239` version. This is the recorded storage and
benchmark overlay, not a full `main` build:

```sh
git show f9a9627a:internal/storeio/common_primary_unified_leaf.go > internal/storeio/common_primary_unified_leaf.go
git show f9a9627a:internal/storeio/common_primary_unified_plan.go > internal/storeio/common_primary_unified_plan.go
git show f9a9627a:internal/storeio/compact_primary_stripe.go > internal/storeio/compact_primary_stripe.go
git show f9a9627a:internal/storeio/primary_value_incremental.go > internal/storeio/primary_value_incremental.go
git show f9a9627a:store/durable/store_file_batch.go > store/durable/store_file_batch.go
git show f9a9627a:store/durable/store_file_primary_batch.go > store/durable/store_file_primary_batch.go
git show f9a9627a:store/durable/store_file_primary_batch_topology.go > store/durable/store_file_primary_batch_topology.go
git show 9a1365239:sql/driver/replicated_apply_batch64_bench_test.go > sql/driver/replicated_apply_batch64_bench_test.go
CODEX_AGENT_ID=codex-repro-main GOEXPERIMENT=simd project-env go test -c ./sql/driver -o ./main-driver.test
```

Build the candidate in a separate clean checkout so the overlaid control files
remain intact:

```sh
git worktree add --detach ../vibedb-candidate 0478cfad
cd ../vibedb-candidate
CODEX_AGENT_ID=codex-repro-candidate GOEXPERIMENT=simd project-env go test -c ./sql/driver -o ./candidate-driver.test
```

In these commands, `project-env` names the
configured project wrapper on the caller's `PATH`. Run both frozen binaries with
the exact arguments recorded in
[write runs](write-runs.json). The candidate binary and both raw output sets are
bound there by SHA-256; the baseline scaffold and source-overlay hashes are in
[metadata](metadata.json). The [read control](read-runs.json) uses a separate frozen
baseline binary and one matched 50× pair.

### Latest cumulative 72c comparison

The control build above is the recorded scaffold `5a689170` with seven storage
files from main `f9a9627a` and the SQL benchmark source from `9a1365239`. Build
the alphabet candidate in a separate clean checkout at `72c3a30af5360e822d51213078ad1b097d304b50`:

```sh
git worktree add --detach ../vibedb-candidate-72c 72c3a30af5360e822d51213078ad1b097d304b50
cd ../vibedb-candidate-72c
CODEX_AGENT_ID=codex-repro-72c GOEXPERIMENT=simd project-env go test -c ./sql/driver -o ./candidate-driver.test
shasum -a 256 ./candidate-driver.test
```

The candidate binary SHA-256 is
`281abf63439f0d7c384bc6bedd9a51c65ecdd06560c5e6e43100c02650d181ac`. Run each
workload in a separate process from its respective control or candidate checkout.
Repeat each pair three times in the recorded order: control/candidate,
candidate/control, control/candidate.

```sh
# SequentialInsert; run one command as one process from each checkout.
CODEX_AGENT_ID=cumulative-main-sequential GOEXPERIMENT=simd project-env ./main-driver.test -test.run='^$' -test.bench='^BenchmarkReplicatedApplyBatch64SequentialInsert$' -test.benchtime=1x -test.count=1 -test.benchmem -test.timeout=20m
CODEX_AGENT_ID=cumulative-72c-sequential GOEXPERIMENT=simd project-env ./candidate-driver.test -test.run='^$' -test.bench='^BenchmarkReplicatedApplyBatch64SequentialInsert$' -test.benchtime=1x -test.count=1 -test.benchmem -test.timeout=20m

# CompactSharedPayload; run one command as one process from each checkout.
CODEX_AGENT_ID=cumulative-main-shared GOEXPERIMENT=simd project-env ./main-driver.test -test.run='^$' -test.bench='^BenchmarkReplicatedApplyBatch64CompactSharedPayload$' -test.benchtime=1x -test.count=1 -test.benchmem -test.timeout=20m
CODEX_AGENT_ID=cumulative-72c-shared GOEXPERIMENT=simd project-env ./candidate-driver.test -test.run='^$' -test.bench='^BenchmarkReplicatedApplyBatch64CompactSharedPayload$' -test.benchtime=1x -test.count=1 -test.benchmem -test.timeout=20m
```

The exact per-workload command templates and source/binary hashes are also in
[cumulative metadata](alphabet-cumulative-metadata.json) and
[cumulative run records](alphabet-cumulative-runs.json).

## Follow-up: cached transaction-marker identity, 2026-09-27

The marker-entry identity change at `8fcae2eb91509116117f7568800487b2b2311ae2`
caches the marker descriptor's identity at open time. Each live-entry check
then uses `RawConn.Control` to verify the descriptor is still open and compares
a fresh lookup beneath the pinned directory with that cached identity. The
marker format and public write benchmark source were unchanged.

This follow-up compares the candidate against the frozen pre-change tail
candidate at `0478cfad76eb0fd4dd783c50850c991e060d3099`. The baseline is a
branch-candidate binary, not a whole-tree `main` build. Three alternating pairs
ran the same 131,072-row workloads with 64-row applies, the three durable
collections, with per-batch input generation, mutation/command assembly, apply
and completion/publication checks, and final checkpoint/fold inside the timed
total. Database/schema setup and the
final full-row oracle were outside timing. All twelve runs passed the benchmark
oracle. The measured medians were:

| Workload | Baseline 0478 | Cached identity | Baseline / candidate |
| --- | ---: | ---: | ---: |
| SequentialInsert | 3.514730 s | 3.511693 s | 1.0009× |
| CompactSharedPayload | 5.270060 s | 5.286029 s | 0.9970× |

The public results are flat and do not show a measurable application-level
throughput gain. Logical certificate, commit, barrier, journal, and tail-charge
counters matched. Per-run filesystem space observations are retained because
allocated blocks can vary by filesystem allocation granularity.

The isolated `EntryCurrent` microbenchmark did improve. Across three samples,
cached identity measured a median of 720.3 ns/op, 232 B/op, and 3 allocations
per operation. The descriptor-`Stat` control measured 1,609 ns/op, 440 B/op,
and 4 allocations per operation. This is a local method-level result and does
not imply an application speedup.

Focused race and SIMD-disabled tests passed for `internal/storeio` and
`store/durable`; test binaries compiled for Windows/amd64 and Linux/386. The
first focused storeio run caught that a closed descriptor's `RawConn.Control`
error did not match `os.ErrClosed`; the implementation now joins the closed-
file sentinel with the concrete error, and the final focused suites pass. Exact
source and binary hashes, commands, validation, and all twelve raw metrics are
in [marker identity metadata](marker-identity-metadata.json) and
[marker identity runs](marker-identity-runs.json). The raw local
`EntryCurrent` samples are in [marker identity kernel output](marker-identity-kernel.txt).

## Follow-up: fitting-tail source-render reuse, 2026-09-27

Commit 873d4dac3 carries append-only validation from the already rendered
source image into the fitting-tail path. This removes the second source render
from that path while preserving the split path and existing publication checks.
The benchmark source, logical storage and certificate counters, and tail-charge
measurements were unchanged.

This is an incremental comparison against the frozen pre-change 8fcae2eb branch
candidate, which already includes cached marker identity. It is not a fresh
comparison against main. Three alternating 1x pairs per workload ran the same
131,072-row, 64-row apply sequence. Per-batch key/value generation, mutation and
command assembly, apply and completion/publication checks, and final checkpoint/fold
were timed. Database/schema
setup and final full-row oracle verification were outside the timer. The median
times were:

| Workload | Frozen 8fca | Fitting reuse | Incremental ratio |
| --- | ---: | ---: | ---: |
| SequentialInsert | 3.661 s | 3.518 s | 1.041x |
| CompactSharedPayload | 5.553 s | 5.078 s | 1.094x |

The per-pair observations include a noisy result: CompactSharedPayload
repetition 1 was 6.911924292 s baseline and 7.780973423 s candidate, while
repetitions 2 and 3 were faster on the candidate. The median is a small
incremental result for these samples and does not show a uniform per-run gain.

The profile included setup and oracle work. In the baseline profile,
primaryTailBatchBaseRows accounted for 360 ms cumulative samples (7.09% of
5.08 s sampled CPU), all in RenderRecordsWithScratch. The candidate profile had
no samples for that helper; it was not sample-visible in that profile. These
inclusive profiles show where the optimization was directed but do not measure
comparative throughput by themselves.

Apparent file size matched in every pair. For SequentialInsert, final allocated
file bytes were 63,111,168 B on baseline repetitions 1 and 3 and 63,643,648 B
on repetition 2; the candidate was 63,643,648 B in all three. Allocated growth
was 25,853,952 B on baseline repetitions 1 and 3 and 26,386,432 B on repetition
2; candidate growth was 26,386,432 B in all three. This filesystem allocated
space difference is recorded as observed, with no confirmed causal attribution.
CompactSharedPayload final allocated bytes matched at 37,789,696 B in every
pair. B/op and allocs/op also varied slightly; the raw records retain each
value. Logical storage, certificate, barrier, journal, and tail-charge counters
matched across pairs.

This is a small incremental gain on two fixed workloads, not an application-wide
or 10x claim. See the [twelve raw measurements](fitting-reuse-runs.json),
[source and binary metadata](fitting-reuse-metadata.json), and
[profile notes](fitting-reuse-profile.txt).

## Follow-up: alphabet-union census reuse, 2026-09-27

Commit `72c3a30a` reuses the complete dictionary census as the spelling set for
alphabet measurement, avoiding a second scan of repeated strings while keeping
the selected encoding unchanged. This comparison is incremental against the
frozen `873d4dac3` fitting-tail candidate; it is not the cumulative comparison
against the main-derived control above. Each workload used three alternating
1× pairs of 131,072 rows. Per-batch key/value generation, mutation/command
assembly, apply and completion/publication checks, and final checkpoint/fold
were timed. Database/schema setup, post-run statistics and file-space sampling,
and final full-row verification were outside the timer. Both ratios below compare
baseline with candidate; the ratio of medians is distinct from the median of the
paired ratios.

| Workload | Baseline median | Candidate median | Ratio of medians | Median paired ratio |
| --- | ---: | ---: | ---: | ---: |
| SequentialInsert | 3.426476 s | 3.410310 s | 1.0047403× | 1.0097297× |
| CompactSharedPayload | 4.818759 s | 4.417639 s | 1.0907997× | 1.0818965× |

The ratios are small and vary by pair: SequentialInsert ranged from 0.9956× to
1.0173×, while CompactSharedPayload ranged from 1.0379× to 1.0994×. All 59
non-timing, non-allocation metric fields matched within each of the six pairs.
The standalone 4,096-row kernel measured 365.600 µs versus 281.649 µs on eight
repeated 256-byte spellings (1.298×), with zero allocations on both paths. Its
all-unique 256-byte control measured 588.950 µs versus 592.265 µs (0.994×), also
with zero allocations. These kernel measurements do not imply a comparable
public application gain. The standalone benchmark used `-test.benchtime=500ms`
and `-test.count=3`; all twelve output samples, source hashes and frozen driver
SHA-256 are in [kernel runs](alphabet-kernel-runs.json). See also the
[incremental raw runs](alphabet-incremental-runs.json).
