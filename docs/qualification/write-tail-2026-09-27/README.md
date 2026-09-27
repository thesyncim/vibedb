# Rightmost-tail split write qualification, 2026-09-27

This report records a matched storage performance diagnostic for the rightmost-tail
split candidate at `0478cfad76eb0fd4dd783c50850c991e060d3099`. It measures 6.94×
for distinct varied payloads and 5.33× for the shared-payload workload. The requested
10× minimum is not met. See the [qualification index](../README.md) and the earlier
[write geometry report](../write-geometry-2026-09-26/README.md), which remains intact.

## Method

Each operation applies 131,072 rows as 2,048 batches of 64 through SQL
`ReplicatedApply`, then performs the final checkpoint and fold. The three durable
collections are included. Network traffic, RF3 replication and proposal admission
are excluded. Three runs per workload alternate main/candidate order. The benchmark
samples file-space every 64 batches outside the timer. Those peak values are sampled
observations, not hard limits. The host was an Apple M4 Max with 64 GiB on macOS
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

| Workload | Main | Candidate | Speedup | Main final allocated files | Candidate final allocated files | User relation device bytes: main → candidate | System device bytes: main → candidate |
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
collections, full benchmark oracles, and final checkpoint/fold inside the timed
operation. All twelve runs passed. The measured medians were:

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
131,072-row, 64-row apply sequence. The timer covered batch application and
the final checkpoint/fold; corpus setup and full benchmark oracle verification
were outside the timer. The median times were:

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
