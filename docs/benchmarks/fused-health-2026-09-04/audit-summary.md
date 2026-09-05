Independent audit of the completed health-change rerun: before `494cf2aaf9f97779ca855833875024834f64a0d5`, after `2184dca377127f0a5a5de76d061ab410bc49470d`, CockroachDB 26.3.1. **120 verified trials / 153,600 samples, zero SQL errors; all 480 VibeDB diagnostic snapshots validate.** Only this rerun contributes numbers.

**After/before throughput geomeans: 1.06838× before-first and 1.13646× after-first. After/CRDB: 0.60729× and 0.45028×.** C8 update throughput improves 88.6% and 10.6%. Before-first has two p99 regressions above 10%, so the report does not claim guardrails or promotion passed.

The failed first campaign `vibedb-health-short-6aca5f18` remains separate. It failed during untimed CREATE after durable publication; both completed-rerun arms contain the same catalog acknowledgment correction. The two catalog-authority files were compared byte-for-byte across revisions. The recorded and actual before/after diff match exactly and contain only 12 health-change files, listed in the JSON.

**Same execution OS and architecture; SIMD enabled for VibeDB.** All eight actual binaries are ELF64 little-endian AArch64. Actual `go version -m` metadata for both VibeDB builds and the shared client confirms `GOOS=linux`, `GOARCH=arm64`, `GOEXPERIMENT=simd`, `CGO_ENABLED=0` and correct clean VCS revisions. The `darwin/arm64` Go version line identifies the compiler host. Every fixture ran on Linux `7.0.12-linuxkit`, aarch64, in the same resolved Linux ARM64 runtime image. CRDB is the pinned stock Linux ARM64 build. These checks prove build settings, not SIMD execution in every query.

Both VibeDB arms use three fused serving processes plus a supervisor; CRDB uses three serving processes. All use RF3, one SQL entrypoint, a shared client and fresh fixtures. Aggregate limits are 12 CPUs / 24 GiB including the client, with swap disabled. One table has 1,024 rows; C1/C8, two repetitions, 2,000 point/update operations or 200 range/group operations and 100 untimed warmups per trial.

Tables use medians of the two trial summaries within each cell and ordering. Throughput is successful operations/s; p99 is milliseconds. A/B means after/before; A/R means after/CRDB. Throughput ratios above 1 and p99 ratios below 1 favor after. Equal-weight geomeans use ten cells. **The orderings are not pooled.**

**before-first: before, after, crdb. Geomean A/B 1.068378×; A/R 0.607291×.**

| Workload | C | Before ops/s | After ops/s | CRDB ops/s | A/B | A/R | Before p99 ms | After p99 ms | CRDB p99 ms | A/B p99 | A/R p99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| point_hit | 1 | 4347.653 | 4216.152 | 6978.544 | 0.970 | 0.604 | 0.394 | 0.396 | 0.256 | 1.004 | 1.546 |
| point_hit | 8 | 21575.738 | 21795.320 | 41000.054 | 1.010 | 0.532 | 0.882 | 0.758 | 0.422 | 0.860 | 1.798 |
| point_miss | 1 | 4562.856 | 4345.179 | 6446.970 | 0.952 | 0.674 | 0.353 | 0.494 | 0.308 | 1.399 | 1.602 |
| point_miss | 8 | 22010.264 | 21482.258 | 24078.161 | 0.976 | 0.892 | 0.793 | 0.662 | 1.428 | 0.835 | 0.464 |
| range_64 | 1 | 2843.020 | 2761.507 | 4565.290 | 0.971 | 0.605 | 0.620 | 0.521 | 0.333 | 0.841 | 1.566 |
| range_64 | 8 | 14877.077 | 14929.136 | 16335.023 | 1.003 | 0.914 | 1.101 | 1.023 | 1.590 | 0.929 | 0.644 |
| group_16 | 1 | 1323.151 | 1308.602 | 2424.227 | 0.989 | 0.540 | 1.043 | 1.142 | 0.693 | 1.095 | 1.649 |
| group_16 | 8 | 8886.182 | 8958.391 | 11504.893 | 1.008 | 0.779 | 1.686 | 1.880 | 1.829 | 1.115 | 1.028 |
| update_existing | 1 | 336.402 | 390.506 | 1042.414 | 1.161 | 0.375 | 12.682 | 11.294 | 1.506 | 0.891 | 7.498 |
| update_existing | 8 | 1293.614 | 2440.071 | 6012.246 | 1.886 | 0.406 | 22.358 | 15.114 | 1.848 | 0.676 | 8.177 |

**after-first: crdb, after, before. Geomean A/B 1.136463×; A/R 0.450276×.**

| Workload | C | Before ops/s | After ops/s | CRDB ops/s | A/B | A/R | Before p99 ms | After p99 ms | CRDB p99 ms | A/B p99 | A/R p99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| point_hit | 1 | 2469.351 | 3378.398 | 7113.413 | 1.368 | 0.475 | 2.494 | 0.479 | 0.275 | 0.192 | 1.741 |
| point_hit | 8 | 11680.166 | 16868.262 | 44174.793 | 1.444 | 0.382 | 5.250 | 0.831 | 0.312 | 0.158 | 2.660 |
| point_miss | 1 | 3185.395 | 3577.456 | 7105.647 | 1.123 | 0.503 | 0.925 | 0.491 | 0.246 | 0.530 | 1.995 |
| point_miss | 8 | 15803.377 | 17246.944 | 43690.291 | 1.091 | 0.395 | 1.079 | 0.879 | 0.293 | 0.814 | 3.001 |
| range_64 | 1 | 2145.493 | 2206.674 | 4789.526 | 1.029 | 0.461 | 0.643 | 0.617 | 0.352 | 0.959 | 1.755 |
| range_64 | 8 | 11129.653 | 11631.980 | 29663.706 | 1.045 | 0.392 | 1.411 | 1.315 | 0.392 | 0.931 | 3.358 |
| group_16 | 1 | 1177.549 | 1154.811 | 2700.056 | 0.981 | 0.428 | 1.105 | 1.122 | 0.638 | 1.015 | 1.758 |
| group_16 | 8 | 7436.935 | 7694.091 | 17970.849 | 1.035 | 0.428 | 2.309 | 1.831 | 0.585 | 0.793 | 3.128 |
| update_existing | 1 | 515.306 | 634.056 | 1273.683 | 1.230 | 0.498 | 11.971 | 9.173 | 1.252 | 0.766 | 7.325 |
| update_existing | 8 | 2530.287 | 2798.080 | 4848.064 | 1.106 | 0.577 | 13.667 | 13.469 | 4.642 | 0.986 | 2.901 |

Before-first p99 regresses 39.9% for C1 point miss and 11.5% for C8 grouped aggregation. No per-order throughput cell loses more than 5%; after-first has no p99 loss above 10%. Those are observations on this short health-only subset, not a registered gate result.

**Observed gateway locality**, summed across nodes and both repetitions. Counts include control/legacy activity inside diagnostic brackets and are not all SQL operations.

| Order | Arm | Work | Successful ops | Local calls | Remote calls | Semantic SQL calls | SQL encodes |
|---|---|---|---:|---:|---:|---:|---:|
| before-first | before | reads | 17600 | 17600 | 1915 | 17600 | 0 |
| before-first | before | updates | 8000 | 17652 | 9637 | 0 | 0 |
| before-first | after | reads | 17600 | 17600 | 1842 | 17600 | 0 |
| before-first | after | updates | 8000 | 17545 | 8625 | 0 | 0 |
| after-first | after | reads | 17600 | 2422 | 17600 | 17600 | 17600 |
| after-first | after | updates | 8000 | 5080 | 16573 | 0 | 0 |
| after-first | before | reads | 17600 | 0 | 20511 | 17600 | 17600 |
| after-first | before | updates | 8000 | 0 | 22732 | 0 | 0 |

Semantic SQL reads are local for both before-first arms (no SQL encodes) and remote for both after-first arms (17,600 encodes each). Other local/remote calls cover legacy/control activity. Update/control locality differs too. This evidence supports reporting each order separately; it does not establish a pure temporal order effect or a single cause for every rate change.

**Update diagnostics:** each row covers 4,000 successful updates across two repetitions, with counters summed across all three nodes. Barriers are cluster totals and may include background work; they are not measured fsync latency.

| Order | Arm | C | Append barriers | Barriers/update | Multi-group / total waves | Local calls | Remote calls |
|---|---|---:|---:|---:|---:|---:|---:|
| before-first | before | 1 | 27470 | 6.8675 | 14 / 27470 | 8000 | 7642 |
| before-first | before | 8 | 6076 | 1.5190 | 82 / 6076 | 9652 | 1995 |
| before-first | after | 1 | 28202 | 7.0505 | 115 / 28202 | 8000 | 7431 |
| before-first | after | 8 | 5712 | 1.4280 | 35 / 5712 | 9545 | 1194 |
| after-first | after | 1 | 24714 | 6.1785 | 10 / 24714 | 4121 | 8002 |
| after-first | after | 8 | 4597 | 1.1493 | 23 / 4597 | 959 | 8571 |
| after-first | before | 1 | 24693 | 6.1733 | 2 / 24693 | 0 | 13155 |
| after-first | before | 8 | 5018 | 1.2545 | 22 / 5018 | 0 | 9577 |

Update semantic-SQL calls and SQL encoding counts were zero in every arm; legacy calls remained. Local native semantic dispatch counters are distinct from semantic SQL. Checkpoint submission, wait and service counters stayed zero in every trial bracket. Those counters cover the checkpoint coordinator; they do not measure the separate full SnapshotState health-observation path established by the preceding profile. Zero counters do not imply no checkpoint work or fsync, and this run does not qualify sustained checkpoint performance.

**Health reporting remained active.** Every VibeDB log contains health revision publication lines. Counts below refer to retained supervisor child-log text, not unique timed publication events.

| Order / arm | Publication log lines | Configuration warning log lines | Ineligible-replacement log lines |
|---|---:|---:|---:|
| before-first/before | 39 | 3 | 23 |
| before-first/after | 86 | 3 | 52 |
| after-first/after | 6 | 2 | 5 |
| after-first/before | 4 | 0 | 1 |

Warnings include `invalid replica health controller configuration` and `no eligible replica replacement`. Some before-first configuration warnings occur between update diagnostic snapshots; they are not all teardown-only. The logs lack timestamps on these lines, so this audit does not assign a precise latency impact. Native connection-service failure counters rose by 6/5 in before-first before/after and 5/2 in after-first before/after. SQL trials still completed and verified. Failed ready waves, native rejections, remote poison/rejection/handshake failures and checkpoint rejections stayed zero in trial brackets. Non-JSON inventory parser warnings are recorded separately in the JSON.

This short single-table/single-host result does not establish the registered 8,192-row/three-repetition matrix, multigroup benefit, independent-machine scaling, sustained checkpoint performance, full correctness qualification, promotion or the full 2× CRDB goal. After remains slower than CRDB in every throughput cell. The 200-sample range/group trials also have limited p99 resolution.

Both CRDB fixtures required one forced process stop (exit137) during bounded cleanup after measurement and verification; the other CRDB processes and all VibeDB supervisors exited0. This is a post-measurement cleanup limitation, not recovery qualification.

**Raw report sources:** Extract `raw-trials.tar.gz` beside this file to resolve the `completed/` paths below.

- [before-first / before](completed/baseline-c1-c8/before-first/before/report.json) — 20 trials, 25,600 samples; SHA256 in the JSON.
- [before-first / after](completed/baseline-c1-c8/before-first/after/report.json) — 20 trials, 25,600 samples; SHA256 in the JSON.
- [before-first / crdb](completed/baseline-c1-c8/before-first/crdb/report.json) — 20 trials, 25,600 samples; SHA256 in the JSON.
- [after-first / crdb](completed/baseline-c1-c8/after-first/crdb/report.json) — 20 trials, 25,600 samples; SHA256 in the JSON.
- [after-first / after](completed/baseline-c1-c8/after-first/after/report.json) — 20 trials, 25,600 samples; SHA256 in the JSON.
- [after-first / before](completed/baseline-c1-c8/after-first/before/report.json) — 20 trials, 25,600 samples; SHA256 in the JSON.

The repository archive retains the six completed reports and 480 diagnostic snapshots with exact bytes under `completed/`, alongside the control scripts and before/after patch. `failed-attempt/` retains the earlier incomplete comparison separately. See [README](README.md), [environment/build proof](environment.json), and [file hashes](sha256.json). Database files, node manifests, certificates, private keys and full command logs are excluded.
