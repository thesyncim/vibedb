# Distributed projected-range comparison

[Benchmark reports](../README.md)

This report measures the candidate's storage-native scalar projection for a
bounded ordered primary range. The candidate decodes only requested scalar
fields from compact storage; unsupported shapes take the exact generic
fallback, and the existing work and cancellation budgets remain in force.
The immutable baseline is `dc6304e0872386b3ac062c3351c1f381333ca9f9`; the
candidate is `88011a57321851ad98af78978207c538e35c3914`. Both VibeDB arms use
the same internal `candidate` fused fixture. CockroachDB is the comparison
arm, pinned to
`cockroachdb/cockroach:v26.3.1@sha256:204f131510c78393adb02345f289a8dbb32e1491e26cc92b6c7751f3b97be3c5`;
the deterministic `rf3-sqlbench` verifier is the oracle.

## Method

The completed matched campaign used four RF3 tables with 8,192 rows each,
256-byte payloads, four uniform groups, three fused physical nodes, one SQL
endpoint, C1/C8, and the seven workloads `point_hit`, `point_miss`,
`range_32`, `range_64`, `range_256`, `group_16`, and `update_existing`. Point
and update trials used 2,000 operations; range and grouped trials used 1,000
scans. Every trial had 500 untimed warmup operations, three repetitions and
per-operation verification. Each of the six sequential AB/BA fixture runs
contains 42 trials and 60,000 verified samples. Across both execution orders,
each arm has 120,000 verified samples and the campaign has 360,000 total with
zero errors. Profiling was disabled and default durability was unchanged.

Execution was in Linux/ARM64 Docker on macOS, with a shared 12-CPU/24-GiB
ceiling, Go 1.27 and `GOEXPERIMENT=simd`. This is single-host RF3 fixture
evidence; it does not measure independent-machine or horizontal scaling.
The exact retained runner command is in [campaign-command.txt](campaign-command.txt),
and the raw fixture command stream is in `raw-trials.tar.gz`.

The campaign ran in a quiet local window: other test, build and compression
work had been paused for at least one minute and did not overlap the fixture.
An unrelated pre-existing Docker container was left untouched; its observed
usage was 0% CPU and 1.626 GiB memory. CI validation ran remotely.

## Results

The candidate improves baseline throughput in every range cell by 0.5% to
19.3% using medians within each execution order. Most range cells remain below
the CRDB comparison arm. The reverse-order C8 `range_32` p99 rises 17.3%
(796,208 ns to 933,875 ns), so order and tail variance remain material.

| Order | Workload | Clients | Baseline ops/s | Candidate ops/s | CRDB ops/s | Candidate change | Candidate / CRDB |
|---|---|---:|---:|---:|---:|---:|---:|
| before-first | range_32 | 1 | 3,333.1 | 3,569.3 | 4,841.9 | +7.1% | 0.737x |
| before-first | range_32 | 8 | 18,996.5 | 19,597.2 | 32,089.3 | +3.2% | 0.611x |
| before-first | range_64 | 1 | 3,066.8 | 3,081.3 | 4,789.8 | +0.5% | 0.643x |
| before-first | range_64 | 8 | 17,605.0 | 18,720.1 | 31,125.1 | +6.3% | 0.601x |
| before-first | range_256 | 1 | 1,899.6 | 2,162.8 | 3,403.3 | +13.9% | 0.636x |
| before-first | range_256 | 8 | 11,606.8 | 13,422.3 | 19,173.8 | +15.6% | 0.700x |
| after-first | range_32 | 1 | 3,145.3 | 3,491.3 | 5,022.4 | +11.0% | 0.695x |
| after-first | range_32 | 8 | 17,512.9 | 18,677.2 | 33,550.9 | +6.6% | 0.557x |
| after-first | range_64 | 1 | 2,804.5 | 3,213.3 | 4,577.9 | +14.6% | 0.702x |
| after-first | range_64 | 8 | 16,526.4 | 19,074.5 | 31,581.8 | +15.4% | 0.604x |
| after-first | range_256 | 1 | 1,842.4 | 2,198.7 | 2,169.1 | +19.3% | 1.014x |
| after-first | range_256 | 8 | 11,995.1 | 13,066.4 | 14,267.2 | +8.9% | 0.916x |

The four-cell `group_16` result is retained as a control in the
[focused grouped table](grouped-summary.md); its before-first C8 candidate
rate is 0.923x baseline and its candidate/CRDB ratios span 2.1x–3.3x. Point
reads remain below CRDB in all four cells at 0.47x–0.55x candidate/CRDB, and
`update_existing` remains below CRDB in all four at 0.21x–0.84x. These controls
do not support an all-matrix CRDB win claim.

The complete [28-cell table](summary.md) retains throughput, p50/p95/p99,
errors, samples and ratios for every workload, client count and execution
order. Machine-readable copies are [summary.tsv](summary.tsv) and
[summary.json](summary.json); [range-summary.md](range-summary.md) and
[grouped-summary.md](grouped-summary.md) provide focused views.

The ABBA microbenchmark uses identical benchmark source in both detached
builds, while the production revisions differ; it compares only
`range_source`. Its `oracle_generic` path is a correctness oracle and is not a
before/after arm.
Fresh execution improves from 125,056 to 48,680 B/op and 1.8386x by median,
with 39,907.5 to 21,705.5 ns/op and 51 to 55 allocs/op. Warm reused `Exec`
reports 0 B/op and 0 allocs/op in both arms, with a 2.0365x median speed
ratio. See [micro-summary.md](micro-summary.md) and the retained raw logs.

## Validation and reproduction

The local [analysis helper](analyze.py) independently checks report completion,
all six report configurations, the expected VibeDB/CRDB diagnostic-mode
difference, unique `(client, ordinal)` sample identities, client bounds,
group/table mapping, endpoint zero, workload identity, zero errors and exact
sample totals before writing the summaries. VibeDB's acknowledged-snapshot
diagnostic mode is expected; CRDB uses `none` and is otherwise config-matched.

The final CI workflow [33968529548](https://github.com/thesyncim/vibedb/actions/runs/33968529548)
completed successfully. The compact [ARM64 projection proof](ci-arm64-projection-proof.txt)
records `TestReplicatedReadSessionUsesPrimaryProjectionLane` passing in 1.79 s;
the full SQL job log is compressed in [validation-logs.tar.gz](validation-logs.tar.gz).
The earlier standalone real-RF3 result is retained as supplemental pre-final-review
evidence in [supplemental-replicated-projection-linux.log](supplemental-replicated-projection-linux.log)
and reports 25.18 s; it is not a replacement for the final campaign.

Run from the repository root with Go 1.27+ and Docker. `OUT` must be a new
absolute directory:

```sh
OUT=/private/tmp/vibedb-projection-comparison-reproduction
python3 scripts/bench/run-distributed-read-comparison.py "$OUT" \
  --baseline-ref dc6304e0 --candidate-ref 88011a57 \
  --workloads point_hit,point_miss,range_32,range_64,range_256,group_16,update_existing \
  --clients 1,8 --groups 4 --physical-nodes 3 --rows 8192 \
  --operations 2000 --scans 1000 --warmup 500 --repetitions 3
```

## Retained evidence

- [raw-trials.tar.gz](raw-trials.tar.gz) retains both orders, all six arm
  reports, samples, latency CSVs, controls, manifests, command streams,
  server text logs and proofs. It omits 16 executable files and 31 numeric
  CRDB WAL segments (1,146,562,952 bytes total); exact paths, sizes and
  SHA-256 values are in [archive-omissions.json](archive-omissions.json).
- [profile-diagnostic.tar.gz](profile-diagnostic.tar.gz) retains the baseline
  9454 `range_64` CPU/trace diagnostic, controls, manifests and profile-top
  summaries. Focused views show `rangePrimaryGraphBuffer` and
  `PrimaryGraphCursor.VisitInlineDecoded` in the read materialization path;
  `mallocgc`, `growslice`, `memmove` and the batch/arena handoff remain
  allocation targets. The configured capture ceiling was 120 seconds; the
  retained CPU profiles each ran for 17.81–17.82 seconds. Startup is included,
  there is no heap profile, and this is not fair timing evidence.
- [validation-logs.tar.gz](validation-logs.tar.gz) retains the campaign,
  runner, race, unsafe-audit, documentation-check, independent-sample,
  microbenchmark and final CI logs. It excludes the two microbenchmark test
  executables; their hashes and build provenance are in
  [binary-provenance.json](binary-provenance.json).
- The current repository-wide link check passed; see
  [docs-check-current.log](docs-check-current.log). The archive and analysis
  checks are recorded in [archive-check.log](archive-check.log) and
  [analysis-check.log](analysis-check.log).
- [campaign-manifest.json](campaign-manifest.json) preserves the pinned
  revisions, image digests, limits, build metadata and binary hashes.
  [sha256.json](sha256.json) covers the report files and archives.

Extract archives into an empty directory and verify `sha256.json` first:

```sh
cd docs/benchmarks/distributed-projected-ranges-2026-09-05
python3 - <<'PY'
import hashlib, json
from pathlib import Path

root = Path('.')
manifest = json.loads((root / 'sha256.json').read_text())
for item in manifest['files']:
    path = root / item['path']
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    assert path.stat().st_size == item['bytes'], path
    assert digest == item['sha256'], path
print(f"verified {len(manifest['files'])} report files")
PY
mkdir evidence-unpacked
for archive in raw-trials.tar.gz profile-diagnostic.tar.gz validation-logs.tar.gz; do
  tar -tzf "$archive"
  tar -xzf "$archive" -C evidence-unpacked --no-same-owner
done
python3 analyze.py \
  evidence-unpacked/vibedb-projection-comparison-88011a57 \
  --output-dir /tmp/vibedb-projected-ranges-analysis
```

The implementation and focused fixture are mapped in
[query/file_projection.go](../../../query/file_projection.go),
[store/durable/store_file_projection.go](../../../store/durable/store_file_projection.go),
and [query/file_projection_bench_test.go](../../../query/file_projection_bench_test.go).
