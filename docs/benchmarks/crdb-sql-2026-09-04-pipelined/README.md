# Asynchronous Raft serving comparison

Clean measured source `e4fd9aeb`. All 120,000 samples and full-table checks passed.
No workload exceeds CockroachDB. The active performance goal remains unmet.

Same [method](../crdb-sql-method.md) and configuration as the
[adaptive read run](../crdb-sql-2026-09-04-reads/README.md): RF3, Linux arm64,
12 shared CPUs, 24 GiB, engines sequential on the same volume; VibeDB first.
8192 rows, 256-byte payloads, C1/C8, 1000 warmups, 2000 measured operations per
trial for all five workloads, three repetitions. No profiling, tests or builds
overlapped timed measurements. Go 1.27 and SIMD enabled, pinned CRDB v26.3.1,
no weakened data durability settings. Single-host, warm-cache evidence only.

Validated 120,000 raw latency samples; medians require all repetitions to pass.

| Workload | Clients | VibeDB ops/s | CRDB ops/s | VibeDB / CRDB |
|---|---:|---:|---:|---:|
| point_hit | 1 | 2,470.7 | 5,578.8 | 0.443× |
| point_hit | 8 | 11,019.4 | 32,954.9 | 0.334× |
| point_miss | 1 | 2,497.6 | 5,195.5 | 0.481× |
| point_miss | 8 | 10,764.7 | 33,536.6 | 0.321× |
| range_64 | 1 | 1,496.5 | 3,474.6 | 0.431× |
| range_64 | 8 | 8,213.1 | 23,104.3 | 0.355× |
| group_16 | 1 | 182.6 | 406.3 | 0.450× |
| group_16 | 8 | 1,016.7 | 2,485.9 | 0.409× |
| update_existing | 1 | 429.3 | 936.2 | 0.459× |
| update_existing | 8 | 2,416.1 | 4,149.6 | 0.582× |

Startup and dynamic group reload now select the existing asynchronous append
runtime. The owner still applies and settles only after required durable
completion. This does not yet activate shared node-log persistence.

Compared with the preceding historical run, updates rose from 373.1 to 429.3
ops/s at C1 and 1958.6 to 2416.1 at C8. C8 ranges rose from 7074.6 to 8213.1;
C8 grouped scans fell from 1242.6 to 1016.7 and varied considerably across
repetitions (1016.7, 1291.0, 818.3). This is not a universal speedup or a
controlled interleaved A/B comparison. The scan variance needs further profiling.

Full raftmodel, raftmember, multiraft, raftservice and shard-command tests passed.
The Linux shipped fault harness and three-process composition test ran on a
Docker volume with strict physical allocation. An initial attempt on Docker's
writable overlay skipped those tests because strict unshare was unsupported;
that skip was not counted as qualification. The disk-backed run exercised
leader pause/resume, asymmetric partitions, crash/restart and exact retries.
It completed 256/256 waiter calls with zero waiter refusals, WAL growth of zero
and RSS growth of 35,639,296 bytes within the harness's configured bounds.
These checks do not establish full SQL transaction or distributed-failure parity.

Next work is node-owned persistence, space accounting and compact versioned
storage, as defined in [the redesign target](../../storage-runtime-redesign.md).
