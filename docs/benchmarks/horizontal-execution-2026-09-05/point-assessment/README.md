# Frozen a2ac5fd8 point checkpoint assessment

This is a retained baseline-to-frozen-G checkpoint for the two point workloads. It freezes M at `5160e0f6c8dc5b252e5c5ff75984bdd6fe49db02` and G at `a2ac5fd8d052d42046dd4c3ee9f5502dc1e48eed`, with read authority disabled in both arms. It is diagnostic evidence from a fixed single Docker host, not the final S assessment and not a 2× CRDB claim.

The complete N3 retry2 run has 72 verified report cells and zero workload errors. The N6 run has 60 verified report cells and zero workload errors; its reverse-order M arm failed during startup before a report was produced. The valid N6 G/CRDB reverse-order pairs are retained, while the cumulative M comparison is incomplete.

## Frozen inputs and method

The shared immutable client is `150912cfbe250dcf16fd4bcfdfea52e13027ed48`. The Linux/arm64 runtime image is `sha256:648f440f42a0958804efb24df176f806f9d353b41f1c0627f666428e40310f6b` and the CockroachDB image is `sha256:204f131510c78393adb02345f289a8dbb32e1491e26cc92b6c7751f3b97be3c5`. Each arm used 16 SQL tables, 8,192 rows per table, 1,000 warmups, 20,000 measured operations, three repetitions, C1/C8, fresh fixtures, and both matched arm orders. VibeDB's RF3 inventory validated 16 logical VibeDB groups. The matching CRDB arm is an SQL oracle; its table/group topology is not asserted to match VibeDB. The cap was 12 CPUs and 24 GiB per fixture.

Throughput values below list the three repetitions in order, then their median and range. Latency cells list median p50/p95/p99 in microseconds across the repetitions, followed by the p99 repetition range. Every listed completed report has strict oracle verification and zero reported errors.

## Headline checkpoint comparisons

On complete paired rows, G/M is a measured within-checkpoint comparison. Across N3 orders and the valid N6 before-first order, the median-throughput G/M ratios range from 0.478× to 1.197×; the largest median-throughput G/CRDB ratio is 1.051× in the N6 before-first point-hit C8 row; one raw trial reaches 1.477×. The valid N6 after-first G/CRDB ratios are 0.663–0.825× and have no G/M value because M failed before reporting. These are per-order medians from a fixed host and do not establish a 2× result or an overall campaign claim.

| Nodes/order | Workload | C | G/M median-throughput ratio | G/CRDB median-throughput ratio |
| --- | --- | ---: | ---: | ---: |
| N3 before-first | point_hit | 1 | 1.131 | 0.894 |
| N3 before-first | point_hit | 8 | 1.167 | 0.892 |
| N3 before-first | point_miss | 1 | 1.146 | 0.935 |
| N3 before-first | point_miss | 8 | 0.478 | 0.645 |
| N3 after-first | point_hit | 1 | 1.197 | 0.569 |
| N3 after-first | point_hit | 8 | 1.076 | 0.842 |
| N3 after-first | point_miss | 1 | 1.058 | 0.862 |
| N3 after-first | point_miss | 8 | 0.902 | 0.730 |
| N6 before-first | point_hit | 1 | 1.090 | 0.850 |
| N6 before-first | point_hit | 8 | 1.065 | 1.051 |
| N6 before-first | point_miss | 1 | 1.084 | 0.883 |
| N6 before-first | point_miss | 8 | 0.901 | 0.717 |
| N6 after-first | point_hit | 1 | — | 0.720 |
| N6 after-first | point_hit | 8 | — | 0.663 |
| N6 after-first | point_miss | 1 | — | 0.825 |
| N6 after-first | point_miss | 8 | — | 0.715 |

## Valid throughput and latency

The first table contains complete paired rows plus the valid N6 after-first G/CRDB rows. N6 after-first includes only G and CRDB because M failed before producing a report; its G/CRDB ratios remain valid diagnostics. Ratios in this report are computed from the ratio of arm throughput medians; the three trial ratios remain in parentheses.

| Nodes/order | Workload | C | M ops/s (r1/r2/r3; median; range) | G ops/s (r1/r2/r3; median; range) | CRDB ops/s (r1/r2/r3; median; range) | G/CRDB (median throughput ratio; trial ratios) | Errors |
| --- | --- | ---: | --- | --- | --- | --- | ---: |
| N3 before-first | point_hit | 1 | 2988.5/3191.6/3335.9 (3191.6; 2988.5–3335.9) | 3116.9/3703.1/3608.5 (3608.5; 3116.9–3703.1) | 4038.4/4051.8/4028.4 (4038.4; 4028.4–4051.8) | 0.894 (trials 0.772/0.914/0.896) | M 0/3; pass; G 0/3; pass; CRDB 0/3; pass |
| N3 before-first | point_hit | 8 | 17193.1/19361.1/17959.4 (17959.4; 17193.1–19361.1) | 20955.0/20074.2/21141.7 (20955.0; 20074.2–21141.7) | 23534.5/23489.1/22475.5 (23489.1; 22475.5–23534.5) | 0.892 (trials 0.890/0.855/0.941) | M 0/3; pass; G 0/3; pass; CRDB 0/3; pass |
| N3 before-first | point_miss | 1 | 3295.8/3322.0/3449.0 (3322.0; 3295.8–3449.0) | 3817.3/3806.8/3212.4 (3806.8; 3212.4–3817.3) | 4072.7/4097.9/3645.5 (4072.7; 3645.5–4097.9) | 0.935 (trials 0.937/0.929/0.881) | M 0/3; pass; G 0/3; pass; CRDB 0/3; pass |
| N3 before-first | point_miss | 8 | 20021.4/18825.6/20211.4 (20021.4; 18825.6–20211.4) | 16398.4/9565.2/5036.1 (9565.2; 5036.1–16398.4) | 14026.5/14834.7/14921.5 (14834.7; 14026.5–14921.5) | 0.645 (trials 1.169/0.645/0.338) | M 0/3; pass; G 0/3; pass; CRDB 0/3; pass |
| N3 after-first | point_hit | 1 | 2841.6/2955.7/2833.0 (2841.6; 2833.0–2955.7) | 3058.2/3442.4/3401.5 (3401.5; 3058.2–3442.4) | 6861.6/5974.2/4139.3 (5974.2; 4139.3–6861.6) | 0.569 (trials 0.446/0.576/0.822) | M 0/3; pass; G 0/3; pass; CRDB 0/3; pass |
| N3 after-first | point_hit | 8 | 16155.0/18017.3/17265.5 (17265.5; 16155.0–18017.3) | 19164.9/18570.1/18172.6 (18570.1; 18172.6–19164.9) | 22045.7/23109.3/21635.8 (22045.7; 21635.8–23109.3) | 0.842 (trials 0.869/0.804/0.840) | M 0/3; pass; G 0/3; pass; CRDB 0/3; pass |
| N3 after-first | point_miss | 1 | 2949.3/3342.5/3381.7 (3342.5; 2949.3–3381.7) | 3537.8/3152.9/3589.2 (3537.8; 3152.9–3589.2) | 3959.1/4167.9/4103.7 (4103.7; 3959.1–4167.9) | 0.862 (trials 0.894/0.756/0.875) | M 0/3; pass; G 0/3; pass; CRDB 0/3; pass |
| N3 after-first | point_miss | 8 | 19882.8/19152.4/18149.1 (19152.4; 18149.1–19882.8) | 18233.7/17277.5/17108.6 (17277.5; 17108.6–18233.7) | 23703.0/23667.9/23652.9 (23667.9; 23652.9–23703.0) | 0.730 (trials 0.769/0.730/0.723) | M 0/3; pass; G 0/3; pass; CRDB 0/3; pass |
| N6 before-first | point_hit | 1 | 2602.2/2854.4/2897.8 (2854.4; 2602.2–2897.8) | 2757.0/3163.6/3110.3 (3110.3; 2757.0–3163.6) | 3945.3/3659.9/3497.6 (3659.9; 3497.6–3945.3) | 0.850 (trials 0.699/0.864/0.889) | M 0/3; pass; G 0/3; pass; CRDB 0/3; pass |
| N6 before-first | point_hit | 8 | 15366.6/15254.2/15325.7 (15325.7; 15254.2–15366.6) | 16576.0/16318.8/15237.6 (16318.8; 15237.6–16576.0) | 16142.2/11048.9/15530.7 (15530.7; 11048.9–16142.2) | 1.051 (trials 1.027/1.477/0.981) | M 0/3; pass; G 0/3; pass; CRDB 0/3; pass |
| N6 before-first | point_miss | 1 | 2980.5/2899.0/2951.8 (2951.8; 2899.0–2980.5) | 3305.7/3199.6/3065.1 (3199.6; 3065.1–3305.7) | 3742.3/3623.4/2505.2 (3623.4; 2505.2–3742.3) | 0.883 (trials 0.883/0.883/1.224) | M 0/3; pass; G 0/3; pass; CRDB 0/3; pass |
| N6 before-first | point_miss | 8 | 15400.9/15660.4/15298.4 (15400.9; 15298.4–15660.4) | 13878.7/15248.7/11446.1 (13878.7; 11446.1–15248.7) | 20808.5/18082.5/19350.0 (19350.0; 18082.5–20808.5) | 0.717 (trials 0.667/0.843/0.592) | M 0/3; pass; G 0/3; pass; CRDB 0/3; pass |
| N6 after-first | point_hit | 1 | — | 2679.1/2970.6/2786.9 (2786.9; 2679.1–2970.6) | 3704.6/3868.6/3935.4 (3868.6; 3704.6–3935.4) | 0.720 (trials 0.723/0.768/0.708) | G 0/3; pass; CRDB 0/3; pass |
| N6 after-first | point_hit | 8 | — | 14360.6/14784.0/14976.0 (14784.0; 14360.6–14976.0) | 22883.5/22299.5/22279.9 (22299.5; 22279.9–22883.5) | 0.663 (trials 0.628/0.663/0.672) | G 0/3; pass; CRDB 0/3; pass |
| N6 after-first | point_miss | 1 | — | 3282.9/3304.3/3151.4 (3282.9; 3151.4–3304.3) | 3978.3/4041.1/3582.5 (3978.3; 3582.5–4041.1) | 0.825 (trials 0.825/0.818/0.880) | G 0/3; pass; CRDB 0/3; pass |
| N6 after-first | point_miss | 8 | — | 15579.6/15229.1/15217.2 (15229.1; 15217.2–15579.6) | 21305.0/21239.6/22960.7 (21305.0; 21239.6–22960.7) | 0.715 (trials 0.731/0.717/0.663) | G 0/3; pass; CRDB 0/3; pass |

Latency uses the same valid report cells. Each arm cell is median p50/p95/p99 in microseconds; the bracketed value is the p99 range across the three repetitions.

| Nodes/order | Workload | C | M p50/p95/p99 us; p99 range | G p50/p95/p99 us; p99 range | CRDB p50/p95/p99 us; p99 range |
| --- | --- | ---: | --- | --- | --- |
| N3 before-first | point_hit | 1 | 291.0/448.9/649.2 [p99 621.0–695.5] | 263.2/359.3/518.2 [p99 410.8–789.2] | 237.4/326.6/465.1 [p99 450.3–466.0] |
| N3 before-first | point_hit | 8 | 393.4/623.2/798.8 [p99 783.4–899.5] | 361.0/571.6/728.7 [p99 718.7–921.6] | 332.5/449.0/528.1 [p99 518.0–735.0] |
| N3 before-first | point_miss | 1 | 283.3/420.0/585.1 [p99 570.5–621.8] | 254.5/342.1/473.4 [p99 393.7–559.2] | 234.8/326.2/460.5 [p99 456.9–944.3] |
| N3 before-first | point_miss | 8 | 378.8/595.8/734.9 [p99 732.0–900.6] | 664.0/1621.3/3228.6 [p99 1029.9–7221.6] | 398.0/1401.7/3614.0 [p99 2427.2–3859.8] |
| N3 after-first | point_hit | 1 | 325.6/473.6/649.7 [p99 616.2–715.6] | 284.9/366.1/453.1 [p99 446.0–680.0] | 144.7/258.8/408.2 [p99 262.6–470.0] |
| N3 after-first | point_hit | 8 | 431.5/708.9/906.2 [p99 864.4–1219.0] | 405.6/638.1/775.3 [p99 744.1–925.0] | 339.7/505.4/798.3 [p99 531.2–830.7] |
| N3 after-first | point_miss | 1 | 282.2/426.5/582.6 [p99 533.1–670.1] | 274.8/358.8/431.8 [p99 430.4–1252.2] | 232.2/323.3/471.9 [p99 457.7–491.2] |
| N3 after-first | point_miss | 8 | 390.0/620.8/793.5 [p99 742.2–983.8] | 411.1/690.1/1189.0 [p99 912.5–1439.2] | 331.3/442.8/506.5 [p99 502.7–519.4] |
| N6 before-first | point_hit | 1 | 329.5/467.1/673.0 [p99 663.0–874.8] | 303.6/421.2/602.8 [p99 544.0–756.3] | 261.8/368.0/473.2 [p99 468.5–484.1] |
| N6 before-first | point_hit | 8 | 497.0/751.0/941.8 [p99 937.1–942.0] | 467.8/715.6/891.4 [p99 853.0–1038.9] | 491.0/781.0/1041.5 [p99 976.1–1651.0] |
| N6 before-first | point_miss | 1 | 319.3/445.7/648.1 [p99 639.1–730.5] | 297.7/410.9/551.9 [p99 527.4–602.6] | 263.0/369.0/464.7 [p99 455.5–1140.8] |
| N6 before-first | point_miss | 8 | 493.2/759.8/945.0 [p99 912.4–964.2] | 532.8/855.9/1094.1 [p99 984.1–1378.5] | 399.6/570.8/690.0 [p99 653.9–734.6] |
| N6 after-first | point_hit | 1 | — | 327.6/472.5/788.5 [p99 632.7–954.5] | 246.4/344.5/456.6 [p99 443.0–588.7] |
| N6 after-first | point_hit | 8 | — | 516.0/770.3/955.5 [p99 899.7–992.0] | 346.1/471.9/584.6 [p99 526.2–598.6] |
| N6 after-first | point_miss | 1 | — | 288.9/400.5/573.5 [p99 544.8–579.2] | 239.2/343.1/477.6 [p99 459.0–609.2] |
| N6 after-first | point_miss | 8 | — | 503.1/736.8/878.0 [p99 854.2–893.2] | 353.8/506.0/814.2 [p99 528.0–859.8] |

## Fixed-host G/CRDB ratios across N3 and N6

These ratios compare the candidate G throughput with the matched CRDB throughput from the same order and fresh fixture. N3 has both orders. N6 after-first has a valid G/CRDB pair but no M arm, so it must not be read as a cumulative M→G result.

| Workload | C | N3 before-first | N3 after-first | N6 before-first | N6 after-first |
| --- | ---: | ---: | ---: | ---: | ---: |
| point_hit | 1 | 0.894 (trials 0.772/0.914/0.896) | 0.569 (trials 0.446/0.576/0.822) | 0.850 (trials 0.699/0.864/0.889) | 0.720 (trials 0.723/0.768/0.708) |
| point_hit | 8 | 0.892 (trials 0.890/0.855/0.941) | 0.842 (trials 0.869/0.804/0.840) | 1.051 (trials 1.027/1.477/0.981) | 0.663 (trials 0.628/0.663/0.672) |
| point_miss | 1 | 0.935 (trials 0.937/0.929/0.881) | 0.862 (trials 0.894/0.756/0.875) | 0.883 (trials 0.883/0.883/1.224) | 0.825 (trials 0.825/0.818/0.880) |
| point_miss | 8 | 0.645 (trials 1.169/0.645/0.338) | 0.730 (trials 0.769/0.730/0.723) | 0.717 (trials 0.667/0.843/0.592) | 0.715 (trials 0.731/0.717/0.663) |

The fixed-host node-count comparison is not independent-machine scaling: N3 and N6 are process counts sharing one Docker host, one aggregate CPU/memory ceiling, and one loopback SQL frontend. The ratio spread is therefore descriptive of these runs only.

## Variability, strict validation, and resource snapshots

Variability is visible in the repetition lists and p99 ranges. For example, N3 before-first G point-miss C8 is 16,398/9,565/5,036 ops/s with p99 values 1,030/3,229/7,222 us; those slow repetitions remain in the report. N6 first-order CRDB point-hit C8 is 16,142/11,049/15,531 ops/s with p99 values 1,042/1,651/976 us. No failed oracle cell was converted into a success.

| Output | Nodes | Completed/failed arm runs | Report cells; verified; errors | CPU delta ms (sum) | Read MiB (sum) | Write GiB (sum) | Max after memory.current GiB | OOM kills |
| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| n3_retry2 | 3 | 6/0 | 72; 72; 0 | 1083078.5 | 2.3 | 8.957 | 4.839 | 0 |
| n6 | 6 | 5/1 | 60; 60; 0 | 1099588.9 | 0.0 | 7.977 | 6.218 | 0 |
| n3_first_failed | 3 | 2/1 | 24; 24; 0 | 455480.6 | 0.0 | 3.405 | 4.768 | 0 |
| n3_retry1_build_failed | 3 | 0/0 | 0; 0; 0 | — | — | — | — | — |

The CPU and I/O values are sums of per-arm cgroup before/after snapshots for arms with both snapshots. Setup and seeding are included, so they are aggregate campaign resource deltas rather than per-query measurements. memory.current is a final snapshot and does not measure peak RSS. A dash means that no arm-level snapshot existed; the N6 aggregate is partial because its failed reverse-order M arm stopped before a report. The raw cgroup snapshots, Docker inventories and host limits are in each archive.

## Failed attempts retained

1. `/private/tmp/vibedb-green-main-assessment-points-3n` completed M/G candidate arms (24 report cells, zero errors) but the shared CRDB fixture failed while seeding `rf3_sql_group_15` at row 6592 with `unexpected EOF`. The initial Docker cleanup refusal and later repair evidence are retained. It has no CRDB comparison.
2. `/private/tmp/vibedb-green-main-assessment-points-3n-retry1` failed before a fixture because the default Go build cache lacked the `internal/raftstore` archive during the parent-vibedb build. It produced zero timed cells; the exact stderr and control log remain archived.
3. `/private/tmp/vibedb-green-main-assessment-points-3n-retry2` is the complete N3 retry: 72/72 report cells verified with zero errors across M, G and CRDB in both orders.
4. `/private/tmp/vibedb-green-main-assessment-points-6n` completed 36 first-order cells and 24 reverse-order G/CRDB cells. The reverse-order M startup exited with status 1 before a report; its server log records `gateway: replicated catalog compare-and-publish conflict`. The valid reverse-order G/CRDB pairs are retained, but the N6 cumulative M comparison is incomplete.

## Artifacts and interpretation limits

The exact extracted metrics are in `metrics.json`. Filtered raw archives are listed in `archive-manifest.json` and checked by `SHA256SUMS`. Each archive contains reports, samples, diagnostics, inventories, a filtered code/text source patch, control records and cgroup snapshots while omitting compiled binaries, CRDB database/WAL data and the repeated large working-tree patch. The original `before-after.patch` is retained on disk by exact SHA/size in each archive provenance; omitted archive payload sections are listed by path and SHA.

| Archive | Status | Retained raw files | Omitted bytes |
| --- | --- | ---: | ---: |
| [N3 retry2 complete](archives/green-main-points-3n-retry2-complete.tar.gz) | complete | 5,629 | 1,689,027,539 |
| [N6 incomplete](archives/green-main-points-6n-incomplete.tar.gz) | incomplete; reverse M startup failed | 5,436 | 2,246,670,070 |
| [N3 first attempt](archives/green-main-points-3n-first-failed.tar.gz) | CRDB setup failed | 2,707 | 1,071,594,310 |
| [N3 retry1](archives/green-main-points-3n-retry1-build-failed.tar.gz) | build failed before fixture | 11 | 154,857,122 |

The valid paired rows support measured checkpoint comparisons for these point workloads. They do not complete the nine-workload assessment, measure read-authority gain, establish independent-machine scaling, or establish a 2× result or an overall/final enabled-feature claim. The final S campaign remains pending liveness and startup qualification.
