Guarded-update campaign: **C8 throughput improves 1.946× / 1.997× and p99 falls to 0.489× / 0.461× of baseline, with roughly half the cluster append barriers per update. Overall geomeans are 1.7905× / 0.8896×; no stable overall gain is established.** The reverse ordering has five throughput and seven p99 guardrail failures. All unfavorable samples remain included.

Matched fused audit: before `f2df4fac525fa0f73ff2e1b558f167e1e598f6c5`, after `7b8efb88436dba5452ef4ab46e9c3d72f242adb1`.

**120 verified trials, 153,600 samples and 480 diagnostic snapshots validated; zero SQL errors.** Only this campaign contributes samples.

Actual binaries are ELF64 ARM64; both VibeDB builds and the shared client have verified Linux/ARM64/SIMD build settings and correct clean revisions. CRDB is the stock Linux ARM64 binary. Both VibeDB arms have three fused nodes, identical diagnostics and one shared client. Resource ceilings: 12 CPUs / 24 GiB, swap disabled, including the client.

One table, 1,024 rows, C1/C8, two repetitions. Entries are medians of two trial summaries per cell; p99 is milliseconds. Equal-weight geomeans use ten cells within each ordering. **Do not pool the orderings or attribute gains to code when request locality differs.**

**before-first: after/before 1.790530×; after/CRDB 0.577423×.**

| Workload | C | Before ops/s | After ops/s | CRDB ops/s | A/B | A/R | Before p99 | After p99 | CRDB p99 | A/B p99 | A/R p99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| point_hit | 1 | 4174.916 | 4376.349 | 8205.454 | 1.048 | 0.533 | 0.418 | 0.367 | 0.225 | 0.876 | 1.631 |
| point_hit | 8 | 11429.143 | 21621.658 | 44437.651 | 1.892 | 0.487 | 6.327 | 0.740 | 0.390 | 0.117 | 1.900 |
| point_miss | 1 | 4298.936 | 4485.185 | 6935.143 | 1.043 | 0.647 | 0.437 | 0.386 | 0.329 | 0.883 | 1.171 |
| point_miss | 8 | 7360.529 | 22201.192 | 31716.451 | 3.016 | 0.700 | 12.737 | 0.680 | 1.273 | 0.053 | 0.535 |
| range_64 | 1 | 1227.108 | 2468.797 | 4387.452 | 2.012 | 0.563 | 12.327 | 0.757 | 0.403 | 0.061 | 1.876 |
| range_64 | 8 | 8369.855 | 15013.435 | 27042.313 | 1.794 | 0.555 | 18.110 | 0.936 | 0.542 | 0.052 | 1.727 |
| group_16 | 1 | 1134.912 | 1350.171 | 2296.504 | 1.190 | 0.588 | 4.496 | 0.934 | 1.228 | 0.208 | 0.760 |
| group_16 | 8 | 7037.001 | 8811.488 | 17045.599 | 1.252 | 0.517 | 5.361 | 1.939 | 0.647 | 0.362 | 3.000 |
| update_existing | 1 | 121.446 | 629.921 | 1079.297 | 5.187 | 0.584 | 107.951 | 10.912 | 4.121 | 0.101 | 2.648 |
| update_existing | 8 | 1924.373 | 3745.610 | 5917.755 | 1.946 | 0.633 | 19.508 | 9.531 | 2.418 | 0.489 | 3.942 |

Cells exceeding 5% throughput loss: 0; exceeding 10% p99 increase: 0.

**after-first: after/before 0.889618×; after/CRDB 0.494877×.**

| Workload | C | Before ops/s | After ops/s | CRDB ops/s | A/B | A/R | Before p99 | After p99 | CRDB p99 | A/B p99 | A/R p99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| point_hit | 1 | 3466.592 | 3913.952 | 4552.910 | 1.129 | 0.860 | 0.499 | 0.422 | 1.663 | 0.846 | 0.254 |
| point_hit | 8 | 17087.983 | 20242.315 | 28564.140 | 1.185 | 0.709 | 0.868 | 0.758 | 1.921 | 0.874 | 0.395 |
| point_miss | 1 | 3807.867 | 3931.305 | 4527.056 | 1.032 | 0.868 | 0.410 | 0.578 | 1.917 | 1.411 | 0.302 |
| point_miss | 8 | 17634.018 | 19173.967 | 38782.550 | 1.087 | 0.494 | 0.765 | 1.042 | 0.764 | 1.361 | 1.364 |
| range_64 | 1 | 2387.838 | 1095.398 | 4174.103 | 0.459 | 0.262 | 0.585 | 4.084 | 0.514 | 6.977 | 7.950 |
| range_64 | 8 | 11087.238 | 5188.823 | 21265.245 | 0.468 | 0.244 | 1.429 | 6.599 | 1.247 | 4.617 | 5.293 |
| group_16 | 1 | 1209.354 | 1000.750 | 1463.744 | 0.828 | 0.684 | 1.036 | 2.997 | 3.328 | 2.893 | 0.901 |
| group_16 | 8 | 8044.922 | 5968.087 | 17348.396 | 0.742 | 0.344 | 1.734 | 3.999 | 0.697 | 2.306 | 5.737 |
| update_existing | 1 | 504.447 | 396.417 | 1143.920 | 0.786 | 0.347 | 12.362 | 23.560 | 1.236 | 1.906 | 19.068 |
| update_existing | 8 | 2005.532 | 4004.346 | 6204.758 | 1.997 | 0.645 | 22.968 | 10.588 | 1.735 | 0.461 | 6.104 |

Cells exceeding 5% throughput loss: 5; exceeding 10% p99 increase: 7.

**Locality and counters:** totals across three nodes and both repetitions include background/control activity between snapshots.

| Order | Arm | Work | Successes | Local calls | Remote calls | Semantic SQL | SQL encodes |
|---|---|---|---:|---:|---:|---:|---:|
| before-first | before | reads | 17600 | 17605 | 1012 | 17600 | 0 |
| before-first | before | updates | 8000 | 17576 | 14610 | 0 | 0 |
| before-first | after | reads | 17600 | 17600 | 1919 | 17600 | 0 |
| before-first | after | updates | 8000 | 16011 | 4974 | 0 | 0 |
| after-first | after | reads | 17600 | 17600 | 2416 | 17600 | 0 |
| after-first | after | updates | 8000 | 16003 | 6930 | 0 | 0 |
| after-first | before | reads | 17600 | 2192 | 17602 | 17600 | 17600 |
| after-first | before | updates | 8000 | 6866 | 16526 | 0 | 0 |

**Update diagnostics:** 4,000 successful updates per row. Barriers are cluster totals, not per-node counts or direct fsync timings.

| Order | Arm | C | Barriers/update | Multi-group/total waves | Checkpoint count | Checkpoint wait ns | Checkpoint service ns |
|---|---|---:|---:|---:|---:|---:|---:|
| before-first | before | 1 | 8.1562 | 3868/32625 | 0 | 0 | 0 |
| before-first | before | 8 | 1.5190 | 31/6076 | 0 | 0 | 0 |
| before-first | after | 1 | 6.0633 | 21/24253 | 0 | 0 | 0 |
| before-first | after | 8 | 0.7700 | 2/3080 | 0 | 0 | 0 |
| after-first | after | 1 | 6.0830 | 30/24332 | 0 | 0 | 0 |
| after-first | after | 8 | 0.7615 | 0/3046 | 0 | 0 | 0 |
| after-first | before | 1 | 6.2103 | 8/24841 | 0 | 0 | 0 |
| after-first | before | 8 | 1.4923 | 44/5969 | 0 | 0 | 0 |

**Runtime and cleanup qualifications:**

- before-first/before: 173 retained health publication lines; 0 configuration warning lines; 156 ineligible-replacement lines; native connection-service failure delta 0.
- before-first/after: 14 retained health publication lines; 5 configuration warning lines; 0 ineligible-replacement lines; native connection-service failure delta 2.
- after-first/after: 13 retained health publication lines; 3 configuration warning lines; 0 ineligible-replacement lines; native connection-service failure delta 5.
- after-first/before: 8 retained health publication lines; 0 configuration warning lines; 7 ineligible-replacement lines; native connection-service failure delta 4.
- before-first/crdb: post-measurement forced shutdown=True, server exit codes=[137, 0, 0]. This cleanup is not a durability or recovery qualification.
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

**Interpretation and campaign limits:**

Guarded-update-only source comparison: before f2df4fac525fa0f73ff2e1b558f167e1e598f6c5; after 7b8efb88436dba5452ef4ab46e9c3d72f242adb1. The recorded patch contains three gateway production files, four test files and the plan document. The compact storage batch change is absent from this comparison.

Observed after/before throughput geomeans are 1.790530 before-first and 0.889618 after-first. After/CRDB are 0.577423 and 0.494877. Results remain separate by ordering. The first order has no measured cell guardrail breaches; the reverse order has five throughput losses above 5% and seven p99 increases above 10%. No full qualification passes.

C8 update is the strongest repeated signal: 1924.37 to 3745.61 ops/s (1.9464x) before-first and 2005.53 to 4004.35 (1.9967x) after-first. Corresponding p99 ratios are 0.4886 and 0.4610. Cluster append barriers per update fall from 1.519 to 0.770 and 1.49225 to 0.7615, about 49% in each order. These are recorded counter/latency improvements, not proof that source changes alone caused their full magnitude.

C1 does not show a stable gain: after/before throughput is 5.1868x then 0.7858x, and p99 ratios are 0.1011 then 1.9059. The first baseline C1 repetitions run at approximately 36.0 and 206.9 ops/s, with p99 165.9 and 50.0 ms. This severe baseline variation inflates the first-order ratio and is fully retained.

Every before-first read trial is local in both arms. Every after-first candidate read is local while every baseline read is remote (one encoding per operation). Update/control locality also differs. Leader placement therefore confounds the reverse ordering; do not pool orders or infer source-only speedup from their aggregate.

The first baseline has slow unchanged-read-path trials before UPDATE: C8 hit approximately 11846/11012 ops/s, C8 miss 6731/7990, and C8 range 3666/13074 across repetitions. The reverse-order candidate also has slow local range/group trials, including range C1 508/1682 ops/s and range C8 8341/2037. These changes in unchanged read workloads demonstrate significant campaign variability; their precise causes are unresolved and no unfavorable trial is excluded.

The before-first baseline retained Raft lines at 16:39:40 show member 1 moving from term 2 to term 3 during the read phase. The bounded logs lack sufficient group identity and history to attribute all earlier read bursts or the later slow C1 trial to that event. Other fixtures have no retained post-start transition line; missing lines do not prove no transition.

The campaign operator reported one live host snapshot during a baseline: Docker VM 330.6% CPU, WindowServer 42%, Codex renderer 38.7%, and no additional Go compilers in the top ten. The operator reports no team tests/builds/profile processing during timing. This one snapshot, without an exact trial timestamp supplied to this auditor, does not prove a quiet or exclusive host and is not used as a causal explanation for individual samples.

Both CRDB fixtures required a post-measurement forced shutdown, with one server process exiting 137 each; all VibeDB fixtures stopped without force and exited zero. This run does not qualify durability or recovery. Runtime warning/counter counts remain recorded separately from zero SQL errors.

One table with 1024 rows, two repetitions, C1/C8 and one Docker host remain diagnostic limits. No earlier campaign timings are pooled. C8 improvement is promising, but C1/read instability, placement differences, shared-host activity and failed reverse-order guardrails prevent an overall performance or promotion claim; after remains below CRDB in every throughput cell.

Geometric means of the ten cell p99 ratios (not a pooled latency percentile): before-first: after/before 0.188614×; after/CRDB 1.650185×; after-first: after/before 1.748994×; after/CRDB 2.003593×.
