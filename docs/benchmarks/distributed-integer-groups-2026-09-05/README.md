# Distributed integer `GROUP BY` comparison

[Benchmark reports](../README.md)

This is a matched single-host RF3 read comparison for the typed integer
grouping path. The candidate evaluates `GROUP BY bucket` with integer
`COUNT(*)`/`SUM(score)` values without whole-row JSON/Segment materialization;
the exact fallback and cancellation budgets remain in force. The baseline is
`a0de0919c2965b7be9730d44dbb3bc36e16412e3`; the candidate is
`740ad5c53de8eaf0769ab3b0e551fd6d6e02ac7d`. Both VibeDB arms use the same
internal `candidate` fused fixture and topology. CockroachDB is the
comparison arm, pinned to
`cockroachdb/cockroach:v26.3.1@sha256:204f131510c78393adb02345f289a8dbb32e1491e26cc92b6c7751f3b97be3c5`;
the deterministic `rf3-sqlbench` result verifier is the oracle.

The completed campaign used four RF3 tables with 8,192 rows each, 256-byte
payloads, four uniform groups, three physical nodes, one SQL endpoint, C1/C8,
and the workloads `point_hit`, `point_miss`, `range_64`, `group_16`, and
`update_existing`. Each cell used 2,000 operations (1,000 scans), 500 untimed
warmup operations and three repetitions with per-operation verification.
The six sequential AB/BA arm runs completed with 288,000 verified operations
and zero errors. Execution ran in Linux/ARM64 Docker on macOS with a
12-CPU/24-GiB limit; VibeDB and the client used Go 1.27 with
`GOEXPERIMENT=simd`. Profiling was disabled
and default durability was unchanged. This is single-host fixture evidence,
not an independent-machine or horizontal-scaling result.

## Grouped result

Rates are medians of three repetitions. Ratios are computed within the same
execution order.

| Order | Clients | Baseline ops/s | Candidate ops/s | CRDB ops/s | Candidate / baseline | Candidate / CRDB |
|---|---:|---:|---:|---:|---:|---:|
| before-first | 1 | 258.8 | 1,202.3 | 336.9 | 4.646x | 3.569x |
| before-first | 8 | 1,983.1 | 8,806.9 | 2,378.7 | 4.441x | 3.702x |
| after-first | 1 | 167.6 | 819.9 | 341.1 | 4.890x | 2.404x |
| after-first | 8 | 1,148.5 | 6,466.3 | 2,351.3 | 5.630x | 2.750x |

The full 20-cell table includes median p50/p95/p99 latency in nanoseconds,
throughput, errors, samples and verified-operation counts for all three arms:
[summary.md](summary.md), [summary.tsv](summary.tsv), and
[summary.json](summary.json). The grouped values are also available as
[grouped-summary.md](grouped-summary.md) and
[grouped-summary.tsv](grouped-summary.tsv). The C8 `update_existing` control
changes by -2.7% in the before-first order and +10.1% in the after-first
order. Most other cells remain below CRDB; this report makes no general 2x or
all-matrix claim.

## Verification and reproduction

The four paired validator outputs each report 96,000 validated raw latency
samples and require every repetition to pass: [before baseline](validator-before-first-before.txt),
[before candidate](validator-before-first-after.txt), [after baseline](validator-after-first-before.txt),
and [after candidate](validator-after-first-after.txt). The small
[analysis helper](analyze.py) rechecks complete/verified reports, zero errors,
sample counts and the 96,000 candidate-operation total before writing the
machine-readable summaries. The five affected final suites passed (including
the 781.487-second durable suite), the focused race repetitions passed, and
the Linux SQL-driver real-path check passed. An earlier baseline full-storage
attempt timed out at ten minutes and remains retained as failed evidence.

Run this from the repository root with Go 1.27+ and Docker; `OUT` must be a
new absolute output directory that does not already exist:

```sh
python3 scripts/bench/run-distributed-read-comparison.py OUT \
  --baseline-ref a0de0919 --candidate-ref 740ad5c5 \
  --workloads point_hit,point_miss,range_64,group_16,update_existing \
  --clients 1,8 --groups 4 --physical-nodes 3 --rows 8192 \
  --operations 2000 --scans 1000 --warmup 500 --repetitions 3
```

The preceding attempt is retained in `raw-trials.tar.gz`: it stopped after
five passing runs when the final baseline startup hit the catalog conflict.
It contributes no result to the completed comparison.

## Retained evidence

* [raw-trials.tar.gz](raw-trials.tar.gz) contains both campaign trees,
  reports, latency CSVs, controls, manifests, command streams, logs, proofs
  and hashes. Executable `bin`, `before-bin` and `after-bin` trees are omitted.
  The only omitted files beneath the retained raw trees are 61 six-digit CRDB
  database WAL segments (505,657,779 bytes); their paths, sizes and SHA-256
  digests are recorded in [archive-omissions.json](archive-omissions.json).
* [profile-diagnostic.tar.gz](profile-diagnostic.tar.gz) contains both the
  initial `evidence` and corrected `evidence-fixed` baseline-only profile
  attempts, including profile-top/profile/trace files, source manifests and
  controls. It omits binaries, source worktrees and Go caches. The corrected
  report is complete and verified for 4,000 samples; the first attempt is
  retained with its validator failure. Its 120-second CPU/trace capture is
  diagnostic only and supplies no benchmark timing. Inclusive CPU summaries
  identify `store.(*Segment).Append` (~5.09 s), `store.(*Segment).buildDoc`
  (~4.08 s), and `vibejson.buildIndexOptions` (~3.50 s) as the largest
  materialization contributors in that profile.
* [validation-logs.tar.gz](validation-logs.tar.gz) contains the retained
  baseline/final suite logs, driver log, campaign logs, microbenchmark and
  unsafe audit; it excludes the draft README and binaries.
* [sha256.json](sha256.json) records archive sizes and SHA-256 digests.

From this report directory, extract archives into an empty directory and keep
the top-level names so raw paths remain stable:

```sh
mkdir evidence-unpacked
for archive in raw-trials.tar.gz profile-diagnostic.tar.gz validation-logs.tar.gz; do
  tar -tzf "$archive"
  tar -xzf "$archive" -C evidence-unpacked --no-same-owner
done
```
