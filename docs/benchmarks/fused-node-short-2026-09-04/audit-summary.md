Independent audit of candidate `27b89cd6` against corrected parent `82ea6abf` and CockroachDB 26.3.1. All six reports passed the archived strict validator: **120 verified trials, 153,600 samples, zero SQL errors**. Sources, binary/control hashes and matching workload settings were checked. Six unique containers and volumes were recorded.

**Throughput gain over parent: 8.14% in parent-first and 24.37% in candidate-first. Candidate throughput is 41.83% and 53.36% of CRDB, respectively. The parent geomean target remains unmet in both orders.**

Settings: one table, 1,024 rows, C1/C8, two repetitions; 2,000 point/update operations or 200 range/group operations per trial, 100 untimed warmups. One SQL entrypoint and one Docker host; aggregate ceiling 12 CPUs / 24 GiB including the shared client, swap disabled. This is a shortened diagnostic, not the registered 8,192-row/three-repetition matrix or multigroup qualification.

Each table uses medians of two trial summaries per cell. Throughput is successful operations/s; p99 is milliseconds. Throughput ratios above 1 favor the candidate; p99 ratios below 1 favor it. Equal-weight geomeans use all ten cells within each order. **Do not pool the orderings:** their candidate gateway locality differed.

**parent-first: parent, candidate, crdb. Throughput geomean C/P 1.081363×; C/CRDB 0.418276×.**

| Workload | Clients | Parent ops/s | Candidate ops/s | CRDB ops/s | C/P | C/CRDB | Parent p99 ms | Candidate p99 ms | CRDB p99 ms | C/P p99 | C/CRDB p99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| point_hit | 1 | 2917.809 | 3134.480 | 7128.033 | 1.074 | 0.440 | 0.704 | 0.561 | 0.241 | 0.796 | 2.330 |
| point_hit | 8 | 13908.392 | 15936.600 | 43495.465 | 1.146 | 0.366 | 1.173 | 0.947 | 0.317 | 0.807 | 2.993 |
| point_miss | 1 | 3218.344 | 3219.828 | 6712.198 | 1.000 | 0.480 | 0.624 | 0.511 | 0.304 | 0.819 | 1.681 |
| point_miss | 8 | 13898.648 | 16450.126 | 43779.815 | 1.184 | 0.376 | 1.221 | 0.824 | 0.312 | 0.675 | 2.642 |
| range_64 | 1 | 2014.713 | 2025.529 | 4567.375 | 1.005 | 0.443 | 0.816 | 0.766 | 0.352 | 0.939 | 2.179 |
| range_64 | 8 | 9298.553 | 12005.349 | 29743.867 | 1.291 | 0.404 | 2.246 | 1.096 | 0.432 | 0.488 | 2.535 |
| group_16 | 1 | 1053.134 | 1099.418 | 2437.041 | 1.044 | 0.451 | 1.371 | 1.156 | 0.653 | 0.843 | 1.770 |
| group_16 | 8 | 6419.257 | 7402.516 | 17594.601 | 1.153 | 0.421 | 2.464 | 2.102 | 0.619 | 0.853 | 3.395 |
| update_existing | 1 | 336.449 | 320.263 | 584.542 | 0.952 | 0.548 | 12.825 | 13.151 | 3.959 | 1.025 | 3.322 |
| update_existing | 8 | 1077.847 | 1086.925 | 3584.446 | 1.008 | 0.303 | 29.347 | 25.607 | 4.494 | 0.873 | 5.699 |

**candidate-first: crdb, candidate, parent. Throughput geomean C/P 1.243658×; C/CRDB 0.533580×.**

| Workload | Clients | Parent ops/s | Candidate ops/s | CRDB ops/s | C/P | C/CRDB | Parent p99 ms | Candidate p99 ms | CRDB p99 ms | C/P p99 | C/CRDB p99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| point_hit | 1 | 3004.552 | 3861.682 | 7208.847 | 1.285 | 0.536 | 0.697 | 0.470 | 0.232 | 0.674 | 2.028 |
| point_hit | 8 | 13791.533 | 20864.805 | 42647.841 | 1.513 | 0.489 | 1.135 | 0.727 | 0.313 | 0.641 | 2.324 |
| point_miss | 1 | 3399.024 | 4381.921 | 6797.480 | 1.289 | 0.645 | 0.566 | 0.352 | 0.241 | 0.621 | 1.462 |
| point_miss | 8 | 14476.858 | 20809.819 | 42901.729 | 1.437 | 0.485 | 1.097 | 0.811 | 0.316 | 0.739 | 2.567 |
| range_64 | 1 | 2037.053 | 2569.237 | 4354.711 | 1.261 | 0.590 | 0.839 | 0.546 | 0.336 | 0.650 | 1.622 |
| range_64 | 8 | 10589.457 | 14922.357 | 23190.553 | 1.409 | 0.643 | 1.356 | 1.021 | 0.722 | 0.753 | 1.415 |
| group_16 | 1 | 1136.900 | 1262.383 | 2483.076 | 1.110 | 0.508 | 1.149 | 0.961 | 0.624 | 0.836 | 1.540 |
| group_16 | 8 | 6756.771 | 8329.378 | 16986.108 | 1.233 | 0.490 | 3.008 | 1.731 | 0.769 | 0.575 | 2.252 |
| update_existing | 1 | 337.640 | 353.279 | 586.069 | 1.046 | 0.603 | 12.394 | 12.230 | 4.046 | 0.987 | 3.023 |
| update_existing | 8 | 1209.879 | 1167.554 | 2917.986 | 0.965 | 0.400 | 25.330 | 27.320 | 5.377 | 1.079 | 5.080 |

The worst parent throughput ratios are 0.951893 (C1 update, parent-first) and 0.965017 (C8 update, candidate-first). The largest parent p99 ratios are 1.025401 and 1.078563. These stay within the per-order 5%/10% guardrails for this subset; they do not establish a full gate pass.

**Candidate write diagnostics**, summed over all three nodes and both repetitions (4,000 successful updates per row). Counts include background activity between untimed snapshots. Append barriers are cluster totals, not per-node counts or a direct measurement of fsync latency.

| Order | Clients | Append barriers | Barriers/update | Local calls | Remote calls | Multi-group / total waves | Multi-group share |
|---|---:|---:|---:|---:|---:|---:|---:|
| parent-first | 1 | 24608 | 6.1520 | 0 | 16309 | 6 / 24608 | 0.0244% |
| parent-first | 8 | 6074 | 1.5185 | 0 | 11240 | 165 / 6074 | 2.7165% |
| candidate-first | 1 | 25498 | 6.3745 | 15472 | 0 | 8 / 25498 | 0.0314% |
| candidate-first | 8 | 6355 | 1.5888 | 12102 | 0 | 160 / 6355 | 2.5177% |

Update gateway semantic-SQL calls and SQL-request encoding counters were zero in every row; gateway legacy calls equal local plus remote calls. Native semantic transport dispatches were 0/0 for C1/C8 in parent-first and 15,472/12,102 in candidate-first. These transport dispatches are distinct from semantic SQL calls. C8 parent-first also recorded 206 remote dials. All checkpoint submissions, wait time, service time and rejections were zero across all candidate trial brackets. This run supplies no checkpoint-overhead measurement.

**Locality explains part of the order sensitivity.** Candidate parent-first read counters were entirely remote, with one SQL request encoding per measured read; candidate-first read counters were entirely local, with zero SQL request encodings. The same remote/local distinction held for updates. This is observed request locality, consistent with different ordinary frontend/leader placement, not proof that elapsed order itself caused the rate change. C1 updates still required about 6.2–6.4 cluster append barriers each; the counters support examining the remaining replicated write path but do not identify its critical-path timing.

SQL correctness checks all passed, but native connection-service failure counters increased by 5 and 6 in the two candidate fixtures during C1 updates; the audit does not attribute their cause. Failed ready waves, native rejections, remote poison/rejection/handshake-failure counters and checkpoint rejections were zero in measured brackets. Inventory parsers also recorded non-JSON manifest/marker warnings separately from SQL results.

Limits: a small single-table, single-host diagnostic with two trials per cell. The 200-sample scan/group trials have limited p99 resolution. No multigroup workload benefit, independent-machine scaling, checkpoint-heavy performance, registered promotion or full 2× CRDB result is established.

**Raw sources:**

- [parent-first / parent](baseline-c1-c8/parent-first/parent/report.json) — 20 trials, 25600 samples; full SHA256 in audit-summary.json.
- [parent-first / candidate](baseline-c1-c8/parent-first/candidate/report.json) — 20 trials, 25600 samples; full SHA256 in audit-summary.json.
- [parent-first / crdb](baseline-c1-c8/parent-first/crdb/report.json) — 20 trials, 25600 samples; full SHA256 in audit-summary.json.
- [candidate-first / crdb](baseline-c1-c8/candidate-first/crdb/report.json) — 20 trials, 25600 samples; full SHA256 in audit-summary.json.
- [candidate-first / candidate](baseline-c1-c8/candidate-first/candidate/report.json) — 20 trials, 25600 samples; full SHA256 in audit-summary.json.
- [candidate-first / parent](baseline-c1-c8/candidate-first/parent/report.json) — 20 trials, 25600 samples; full SHA256 in audit-summary.json.

Recommended repository retention: these two audit summaries plus `raw-trials.tar.gz`, containing only the six reports linked above and the 240 exact JSON files under the two candidate `diagnostics/` directories, retaining paths. Extract beside this Markdown for the links and strict validator. Exclude binaries, duplicate raw directories, source/node manifests, supervisor configuration, command logs, certificates and keys. Selected JSON was checked for PostgreSQL URLs and private-key material; retain its exact bytes to preserve diagnostic hashes.
