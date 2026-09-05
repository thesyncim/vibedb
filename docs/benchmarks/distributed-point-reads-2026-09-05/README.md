# Distributed point-read evidence — 2026-09-05

This package records the point-read candidate at `93f85d56` against its immutable
baseline `b2f716ec`, with the shared client at `150912cf`. The matched campaign is a
single-host Linux arm64 RF3 fixture: three fused VibeDB processes, four logical data
groups, 12 CPUs, 24 GiB, 8,192 rows per table, 256-byte payloads, 20,000 operations
per trial, 1,000 warmups, 2,000 scans, three repetitions, and clients 1 and 8. It
completed six arms (both execution orders), 72 verified trials, and zero errors.

The end-to-end table is a diagnostic on this host. It is order-sensitive and does not
establish a robust CRDB win, an RF3 latency guarantee, or a multi-host scaling result.
The complete per-arm reports, including p99 values and every trial, are retained in
`raw-campaign.tar.gz` (the duplicated `raw/report.json`, published volumes, and
binaries are omitted).

## End-to-end matched campaign

`ops/s` and `p99` are medians of the three repetitions; `p99` is milliseconds. The final two columns show throughput change: `100 × (candidate ops/s / reference ops/s − 1)`. Positive values mean higher candidate throughput; lower p99 means lower tail latency. The machine-readable summaries retain throughput ratios, where values above 1 favor the candidate.

The recorded orders are `before → after → CRDB` (`before-first`) and `CRDB → after → before` (`after-first`).

| Order | Workload | Clients | Base ops/s | Candidate ops/s | CRDB ops/s | Candidate p99 | Base p99 | CRDB p99 | Throughput change vs base | Throughput change vs CRDB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| before-first | point_hit | 1 | 3621.7 | 4370.1 | 4423.9 | 0.331 | 0.410 | 0.432 | +20.7% | -1.2% |
| before-first | point_hit | 8 | 18780.3 | 24757.8 | 24284.9 | 0.553 | 0.827 | 0.499 | +31.8% | +1.9% |
| before-first | point_miss | 1 | 3793.5 | 4365.6 | 4411.5 | 0.302 | 0.400 | 0.438 | +15.1% | -1.0% |
| before-first | point_miss | 8 | 19382.5 | 25652.3 | 24084.1 | 0.573 | 0.762 | 0.514 | +32.3% | +6.5% |
| after-first | point_hit | 1 | 2331.7 | 3435.9 | 6466.2 | 0.521 | 2.203 | 0.388 | +47.4% | -46.9% |
| after-first | point_hit | 8 | 16004.6 | 17784.7 | 22339.5 | 0.773 | 0.911 | 0.665 | +11.1% | -20.4% |
| after-first | point_miss | 1 | 3629.0 | 3587.2 | 4326.6 | 0.415 | 0.445 | 0.414 | -1.2% | -17.1% |
| after-first | point_miss | 8 | 18440.0 | 18323.1 | 24070.8 | 0.738 | 0.722 | 0.511 | -0.6% | -23.9% |

Across these rows, candidate throughput changes versus baseline range from +11.1% to +47.4% for hits and −1.2% to +32.3% for misses. In the `before-first` order, the 8-client candidate exceeds CRDB throughput by 1.9% for hits and 6.5% for misses. In the reverse order, it falls below CRDB by 20.4% and 23.9%, respectively. Candidate p99 is higher than CRDB on all four 8-client rows. The results differ substantially between execution orders; this campaign does not establish the cause or a robust CRDB win.

## Local driver micro ABBA

This separate benchmark runs the prepared-only driver path in Linux arm64 and includes cursor JSON consumption and verification. It is not wire encoding throughput and is not RF3 end-to-end latency. Each workload has six samples per revision (two before and two after blocks, three repetitions per block); the full stdout is retained below.

| Workload | Base median ns/op | Candidate median ns/op | Change | Base B/op | Candidate B/op | Base allocs/op | Candidate allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| hit | 15674.5 | 10519.5 | -32.9% | 61536 | 20085 | 165 | 131 |
| miss | 9487.0 | 8328.0 | -12.2% | 12720 | 11544 | 135 | 110 |

The local micro median is approximately -33% for hits and -12% for misses. The
candidate hit path also drops from 61,536 to 20,085 B/op and 165 to 131 allocs/op;
the miss path drops from 12,720 to 11,544 B/op and 135 to 110 allocs/op. These are
small, fixed-driver measurements and should not be substituted for the end-to-end
campaign.

## Correctness and review evidence

The measured candidate revision (`93f85d56`) has native query coverage of 50 passes,
native driver coverage of 55 passes, and 41 combined race passes, with zero skips and
zero failures. The later integrated revision `0770c9a51bce442937ec965a5301bab9e5e252ef`
was checked separately with the shared-cache query/driver/shardservice focused suite:
114 passes, zero skips, zero failures. That integration check is correctness evidence
for the integrated tree; it is not the revision used for the timing campaign.

- Native logs: `correctness/query-93f85d56.log`, `correctness/driver-93f85d56.log`, `correctness/race-93f85d56.log`.
- Integrated-tree log: `correctness/integration-0770c9a5.log`.
- PR and checks: [PR 178](https://github.com/thesyncim/vibedb/pull/178), [checks](https://github.com/thesyncim/vibedb/pull/178/checks). All 37 PR checks passed. Merge commit `f05df25e8bebc13d9bfe11a2038bab43805f6c3d` and tested integration commit `0770c9a51bce442937ec965a5301bab9e5e252ef` share tree `809d3ff17301fc1bd9b04e156a47dc244bf113b4`. The timed revision remains `93f85d56`.
- Measured-candidate CI: [run 33983819485](https://github.com/thesyncim/vibedb/actions/runs/33983819485). Run `33981469160` belongs to the earlier `fe8` revision and is not evidence for the measured candidate.

## Provenance and reproducibility

The exact campaign and micro metadata are in `campaign-manifest.json`,
`micro-manifest.json`, and `provenance.json`. The campaign used Go 1.27.0 with
`GOOS=linux GOARCH=arm64 CGO_ENABLED=0 GOEXPERIMENT=simd` and the pinned CRDB image
`sha256:204f131510c78393adb02345f289a8dbb32e1491e26cc92b6c7751f3b97be3c5`.
The final driver micro used Go 1.27.1 in
`golang:1.27-bookworm@sha256:648f440f42a0958804efb24df176f806f9d353b41f1c0627f666428e40310f6b`,
with the shared host Go cache bind and the owned test-scratch volume recorded in its
manifest. `campaign-control-commands.jsonl` and `micro/commands.jsonl` retain the
executed command records; the copied controls preserve the launcher source.

## Diagnostic bottleneck note

`diagnostics/fused-miss-region-summary.json` is a startup-inclusive candidate miss
trace. It captured 43,424 `sql.read.quorum` regions with median 156.45 µs versus
17.79 µs execution, 0.128 µs admission, and 0.320 µs encode. This is a subset
profile used to locate a bottleneck, not a comparative latency result.

## Excluded attempts

The first overlay-backed run skipped native probes because strict allocation was
unsupported; it is retained only as a provenance note. The first volume reuse run
exited 255 because copied binaries lost executable mode. An earlier `fe8` micro run
was potentially contended. None of those attempts contributes to the tables above.

## Package contents

`raw-campaign.tar.gz` is below the 100 MiB archive limit. It retains one complete
`report.json` for each of the six order/engine arms, run metadata, VibeDB raw
sampling diagnostics, and capped tails of structured CRDB logs. It excludes duplicate
`raw/report.json` files, published storage volumes, and all server/client binaries.
`micro/replicated_point_cpu_bench_test.go.txt` retains the exact benchmark source without introducing a Go package into this documentation directory.
`summary.json` and `scalar-summary.csv` expose the machine-readable scalar results. `SHA256SUMS` covers every other file in this report directory.
