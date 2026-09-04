# RF3 SQL comparison after adaptive read admission

Clean source `031b52c2`. All 120,000 measured samples and full-table checks passed.
Every workload remains below CockroachDB. The 2x goal is not met.

Same [method](../crdb-sql-method.md): Go 1.27, SIMD enabled, Linux arm64, RF3,
12 shared CPUs and 24 GiB, pinned CRDB v26.3.1, one shared disk volume, engines
run sequentially. VibeDB ran first. 8192 rows, 256-byte payloads, C1/C8,
1000 warmups, 2000 operations for every trial, three repetitions. No tests,
builds or profile processing overlapped timed measurements. No durability
settings were disabled. These are single-host warm-cache measurements.

Validated 120,000 raw latency samples; medians require all repetitions to pass.

| Workload | Clients | VibeDB ops/s | CRDB ops/s | VibeDB / CRDB |
|---|---:|---:|---:|---:|
| point_hit | 1 | 2,149.8 | 5,565.4 | 0.386× |
| point_hit | 8 | 10,200.5 | 32,465.8 | 0.314× |
| point_miss | 1 | 2,351.1 | 5,387.5 | 0.436× |
| point_miss | 8 | 10,566.2 | 31,991.8 | 0.330× |
| range_64 | 1 | 1,501.6 | 3,448.2 | 0.435× |
| range_64 | 8 | 7,074.6 | 23,247.3 | 0.304× |
| group_16 | 1 | 181.5 | 398.2 | 0.456× |
| group_16 | 8 | 1,242.6 | 2,369.8 | 0.524× |
| update_existing | 1 | 373.1 | 877.2 | 0.425× |
| update_existing | 8 | 1,958.6 | 4,454.1 | 0.440× |

Small queries now reserve 1 MiB, with bounded retries up to the previous 40 MiB
maximum. SQL execution has an 80 MiB subquota within the 112 MiB shared frame
budget. Request bodies still use the shared frame account; this is not an
unconditional native-progress guarantee under arbitrary input saturation.
A fixed 256-slot workspace-hint cache prevents repeated scans from redoing
smaller failed tiers. Hints contain no plans or results and cannot bypass a
caller limit, deadline, authorization or fresh quorum-fenced read.

The [precursor](precursor-a59f5e28/README.md) caught a C1 grouped-scan regression
to 114.7 ops/s. Hints restored 181.5 ops/s in this run. Compared with the earlier
[concurrent-write baseline](../crdb-sql-2026-09-04-concurrent/README.md), observed
C8 point hits rose from 6563.9 to 10200.5, ranges from 4270.1 to 7074.6, and
grouped scans from 354.8 to 1242.6. Those historical comparisons are not
interleaved old/new experiments. Updates were not accelerated by this change;
C8 updates measured 1958.6 versus the prior 2029.3. Raw tails and all repetitions
are retained so throughput medians do not hide latency variation.

Full shard-service, SQL-driver and gateway suites passed. Focused race tests
cover admission, cancellation, quota accounting and concurrent hint updates.
Wire-size checks are differential against the actual codec at exact boundaries.
Full-table verification exercises results larger than the first workspace tier.

These changes are a measured baseline for the architectural work, not evidence
of horizontal scalability, space efficiency, nonblocking DDL or full SQL
transaction parity. See [the active goal](../../performance-and-scale-goal.md).
