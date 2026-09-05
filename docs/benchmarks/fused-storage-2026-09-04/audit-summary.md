Storage-only campaign interpretation: **after/before throughput geomeans 0.8842× and 0.8518×; after/CRDB 0.4525× and 0.4361×. No reliable overall benefit demonstrated.** The before-first ordering has 5 throughput and 6 p99 guardrail failures; after-first has 9 and 6. All unfavorable trials remain included. The source diff contains only the compact storage batch implementation and its test.

Matched fused audit: before `f2df4fac525fa0f73ff2e1b558f167e1e598f6c5`, after `7aa4f49602f85303d13689bce5beb1659e51cc52`.

**120 verified trials, 153,600 samples and 480 diagnostic snapshots validated; zero SQL errors.** Only this campaign contributes samples.

Actual binaries are ELF64 ARM64; both VibeDB builds and the shared client have verified Linux/ARM64/SIMD build settings and correct clean revisions. CRDB is the stock Linux ARM64 binary. Both VibeDB arms have three fused nodes, identical diagnostics and one shared client. Resource ceilings: 12 CPUs / 24 GiB, swap disabled, including the client.

One table, 1,024 rows, C1/C8, two repetitions. Entries are medians of two trial summaries per cell; p99 is milliseconds. Equal-weight geomeans use ten cells within each ordering. **Do not pool the orderings or attribute gains to code when request locality differs.**

**before-first: after/before 0.884203×; after/CRDB 0.452450×.**

| Workload | C | Before ops/s | After ops/s | CRDB ops/s | A/B | A/R | Before p99 | After p99 | CRDB p99 | A/B p99 | A/R p99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| point_hit | 1 | 4340.026 | 3913.484 | 7099.194 | 0.902 | 0.551 | 0.415 | 0.501 | 0.247 | 1.208 | 2.029 |
| point_hit | 8 | 21607.403 | 21132.762 | 43123.476 | 0.978 | 0.490 | 0.791 | 0.703 | 0.319 | 0.889 | 2.204 |
| point_miss | 1 | 4606.944 | 4412.537 | 7675.446 | 0.958 | 0.575 | 0.356 | 0.359 | 0.206 | 1.007 | 1.738 |
| point_miss | 8 | 21293.596 | 20849.119 | 42992.809 | 0.979 | 0.485 | 0.775 | 0.716 | 0.321 | 0.923 | 2.231 |
| range_64 | 1 | 2588.258 | 2586.588 | 5001.849 | 0.999 | 0.517 | 0.550 | 0.730 | 0.314 | 1.329 | 2.325 |
| range_64 | 8 | 14721.911 | 9349.218 | 24008.981 | 0.635 | 0.389 | 1.212 | 3.563 | 1.542 | 2.941 | 2.311 |
| group_16 | 1 | 1271.958 | 950.254 | 2608.457 | 0.747 | 0.364 | 1.193 | 4.516 | 0.570 | 3.784 | 7.929 |
| group_16 | 8 | 8363.927 | 5513.698 | 17971.624 | 0.659 | 0.307 | 2.096 | 6.206 | 0.585 | 2.961 | 10.613 |
| update_existing | 1 | 618.796 | 521.771 | 1232.579 | 0.843 | 0.423 | 8.593 | 11.321 | 1.935 | 1.317 | 5.849 |
| update_existing | 8 | 1964.520 | 2632.527 | 5254.254 | 1.340 | 0.501 | 22.157 | 13.490 | 2.796 | 0.609 | 4.825 |

Cells exceeding 5% throughput loss: 5; exceeding 10% p99 increase: 6.

**after-first: after/before 0.851836×; after/CRDB 0.436082×.**

| Workload | C | Before ops/s | After ops/s | CRDB ops/s | A/B | A/R | Before p99 | After p99 | CRDB p99 | A/B p99 | A/R p99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| point_hit | 1 | 3969.140 | 3277.277 | 7957.206 | 0.826 | 0.412 | 0.395 | 0.576 | 0.296 | 1.458 | 1.946 |
| point_hit | 8 | 19796.438 | 16368.808 | 40118.105 | 0.827 | 0.408 | 0.943 | 0.891 | 0.487 | 0.944 | 1.830 |
| point_miss | 1 | 4512.535 | 3378.568 | 6891.741 | 0.749 | 0.490 | 0.320 | 0.707 | 0.229 | 2.213 | 3.085 |
| point_miss | 8 | 21415.295 | 16059.419 | 44795.049 | 0.750 | 0.359 | 0.675 | 0.838 | 0.302 | 1.241 | 2.769 |
| range_64 | 1 | 2656.586 | 2002.773 | 4813.659 | 0.754 | 0.416 | 0.500 | 0.856 | 0.296 | 1.710 | 2.894 |
| range_64 | 8 | 13816.354 | 12102.159 | 30831.612 | 0.876 | 0.393 | 1.036 | 1.180 | 0.408 | 1.139 | 2.890 |
| group_16 | 1 | 1309.145 | 1194.733 | 2615.237 | 0.913 | 0.457 | 1.023 | 1.043 | 0.659 | 1.019 | 1.583 |
| group_16 | 8 | 8693.999 | 7666.997 | 14920.558 | 0.882 | 0.514 | 2.121 | 1.963 | 1.147 | 0.926 | 1.712 |
| update_existing | 1 | 654.348 | 748.372 | 1293.392 | 1.144 | 0.579 | 9.622 | 6.720 | 1.168 | 0.698 | 5.754 |
| update_existing | 8 | 2693.264 | 2325.407 | 6125.722 | 0.863 | 0.380 | 14.517 | 19.838 | 2.118 | 1.367 | 9.369 |

Cells exceeding 5% throughput loss: 9; exceeding 10% p99 increase: 6.

**Locality and counters:** totals across three nodes and both repetitions include background/control activity between snapshots.

| Order | Arm | Work | Successes | Local calls | Remote calls | Semantic SQL | SQL encodes |
|---|---|---|---:|---:|---:|---:|---:|
| before-first | before | reads | 17600 | 17600 | 1839 | 17600 | 0 |
| before-first | before | updates | 8000 | 17602 | 5838 | 0 | 0 |
| before-first | after | reads | 17600 | 17600 | 1970 | 17600 | 0 |
| before-first | after | updates | 8000 | 17508 | 6061 | 0 | 0 |
| after-first | after | reads | 17600 | 0 | 20008 | 17600 | 17600 |
| after-first | after | updates | 8000 | 0 | 21162 | 0 | 0 |
| after-first | before | reads | 17600 | 19554 | 0 | 17600 | 0 |
| after-first | before | updates | 8000 | 22778 | 0 | 0 | 0 |

**Update diagnostics:** 4,000 successful updates per row. Barriers are cluster totals, not per-node counts or direct fsync timings.

| Order | Arm | C | Barriers/update | Multi-group/total waves | Checkpoint count | Checkpoint wait ns | Checkpoint service ns |
|---|---|---:|---:|---:|---:|---:|---:|
| before-first | before | 1 | 6.3465 | 11/25386 | 0 | 0 | 0 |
| before-first | before | 8 | 1.5575 | 40/6230 | 0 | 0 | 0 |
| before-first | after | 1 | 6.3230 | 7/25292 | 0 | 0 | 0 |
| before-first | after | 8 | 1.3720 | 44/5488 | 0 | 0 | 0 |
| after-first | after | 1 | 6.0637 | 1/24255 | 0 | 0 | 0 |
| after-first | after | 8 | 1.4545 | 31/5818 | 0 | 0 | 0 |
| after-first | before | 1 | 6.2360 | 14/24944 | 0 | 0 | 0 |
| after-first | before | 8 | 1.3600 | 18/5440 | 0 | 0 | 0 |

**Runtime and cleanup qualifications:**

- before-first/before: 37 retained health publication lines; 3 configuration warning lines; 16 ineligible-replacement lines; native connection-service failure delta 5.
- before-first/after: 23 retained health publication lines; 4 configuration warning lines; 10 ineligible-replacement lines; native connection-service failure delta 4.
- after-first/after: 6 retained health publication lines; 1 configuration warning lines; 6 ineligible-replacement lines; native connection-service failure delta 1.
- after-first/before: 21 retained health publication lines; 7 configuration warning lines; 8 ineligible-replacement lines; native connection-service failure delta 4.
- before-first/crdb: post-measurement forced shutdown=True, server exit codes=[0, 0, 137]. This cleanup is not a durability or recovery qualification.
- after-first/crdb: post-measurement forced shutdown=True, server exit codes=[0, 137, 0]. This cleanup is not a durability or recovery qualification.

Controller logs are retained text counts, not deduplicated timed rates; untimestamped warnings do not establish a causal latency impact. Zero SQL errors does not mean every runtime counter was error-free. Counter details remain in JSON.

Short diagnostic only; do not pool localities or claim full promotion, multigroup/scaling qualification, sustained checkpoint performance or the full 2x CRDB objective. The 200-sample scan/group trials have limited p99 resolution. Preserve failures and partial campaigns separately; no earlier timings are included.

**Raw sources:**

- [before-first/before](baseline-c1-c8/before-first/before/report.json) — 20 trials / 25,600 samples; SHA256 in JSON.
- [before-first/after](baseline-c1-c8/before-first/after/report.json) — 20 trials / 25,600 samples; SHA256 in JSON.
- [before-first/crdb](baseline-c1-c8/before-first/crdb/report.json) — 20 trials / 25,600 samples; SHA256 in JSON.
- [after-first/crdb](baseline-c1-c8/after-first/crdb/report.json) — 20 trials / 25,600 samples; SHA256 in JSON.
- [after-first/after](baseline-c1-c8/after-first/after/report.json) — 20 trials / 25,600 samples; SHA256 in JSON.
- [after-first/before](baseline-c1-c8/after-first/before/report.json) — 20 trials / 25,600 samples; SHA256 in JSON.

Archive only approved report/diagnostic files and controls using an explicit allowlist. Preserve exact bytes and relative paths. Exclude private keys, node data/manifests, supervisor configuration and duplicate raw files.

**Per-trial placement and leadership findings:**

Before-first uses local semantic SQL for every before and after read trial. After-first is placement-confounded: every after read is remote (one SQL encoding per operation), every before read is local (zero SQL encodings). Keep these orders separate; the second-order losses cannot be assigned solely to source changes.

The first-order read bursts do not have an observed locality change: after range C8 repetition 1 runs at 5476 ops/s versus 13223 for repetition 2; after group C1 repetition 2 runs at 648 versus 1253 for repetition 1; after group C8 repetition 1 runs at 3203 versus 7825 for repetition 2. These three slow trials each have 200 local semantic SQL calls, no SQL encodes, zero append barriers, zero remote dials and zero native failures. Their cause is unresolved; they remain in all medians and guardrail evaluations.

Retained matching Raft transition lines show startup elections to term 2 before the first timed trial in every VibeDB fixture. No transition during timing was observed. Second-resolution bounded logs and missing per-trial leader/term gauges prevent proving continuous leader identity or attributing individual stalls to leadership.

Before-first update ratios are C1 0.843204 and C8 1.340035; after-first they are C1 1.143691 and C8 0.863416. No stable write speedup is reproduced across orderings.

The failed first bootstrap at `vibedb-storage-short-7aa4f496` recorded `runs: []` and contributes zero timings. It remains separate from this complete rerun.
