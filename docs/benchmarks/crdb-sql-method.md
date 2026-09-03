# RF3 SQL comparison method

Run from the checkout being measured:

```sh
python3 scripts/bench/run-crdb-sql-comparison.py /absolute/new/evidence-directory
```

Go 1.27, Docker, and Python 3 are required. The runner builds VibeDB with
`GOEXPERIMENT=simd`, pins CockroachDB v26.3.1 by image digest, records binary
hashes and the source revision/status, and runs each engine sequentially. Use
`--order crdb-first` to reverse engine order. Never compile or run unrelated
benchmarks during the measurement window. Nonzero exit codes and failed trials
are evidence, not results to discard. The runner continues to the second engine
after a workload failure. Output files are never overwritten; the benchmark
creates a new table and does not drop existing tables.

Both engines share one Linux container capped at 12 CPUs and 24 GiB, the same
native Linux Docker volume, and loopback networking. This is a single-host,
warm-cache comparison, not independent failure domains, multi-region latency,
scale-out throughput, failover performance, or an out-of-memory workload.
VibeDB's development cluster runs three voters for each role, using nine shard
processes plus the gateway; CockroachDB runs three combined SQL/storage node
processes. The PostgreSQL client runs inside the same container and CPU quota.
VibeDB's additional gateway/network hop is part of its shipped SQL cost.

Client SQL uses plaintext trusted loopback connections in both engines;
inter-node TLS stays enabled. VibeDB retains strict physical allocation and
native durability checks; CockroachDB retains synchronous replication/storage
defaults. CockroachDB's table range metadata must prove exactly three voting
replicas before timing. Each CockroachDB node uses a 512 MiB cache and 512 MiB
SQL memory allowance; VibeDB uses its shipped bounded working sets. The shared
24 GiB ceiling is matched, but cache implementations and internal allocations
are not claimed equivalent. Neither engine has disabled fsync or replication.

The shared client creates the same table with a text primary key, an integer
bucket, an integer score, and a 256-byte text payload. There are 8,192 initial
rows and no secondary indexes. Each table has one initial data partition/range.
CockroachDB gets an explicit `ANALYZE` after loading. VibeDB currently has no
shipped distributed ANALYZE command; it uses the available optimizer metadata.
This schema does not establish anything about secondary-index update locality,
skewed multi-partition joins, correlated predicates, or planner search quality.

Each operation uses the same unnamed PostgreSQL extended parse/bind/execute
protocol, text parameters, and text results. VibeDB exposes strings as JSON
values, which the client decodes for comparison. Latency and throughput include
client request construction and result checking. Trials are closed-loop;
percentiles describe observed completed requests, without coordinated-omission
correction or a claim about open-loop overload latency. Warmup, setup,
replication proof, and whole-table verification are outside trial timing.

Workloads: deterministic primary-key hits and misses, 64 consecutive rows from
a lower key bound, 16 grouped count/sum aggregates, and computed updates of one
existing key per client. Updates use disjoint client keys, so they do not measure
contention. Every result is checked; after each trial all four fields of every
row are verified in bounded pages and the total row count is checked separately.
Unknown write outcomes are not blindly retried. A failed warmup invalidates the
workload and is recorded in `verification_error` and the client log.

Defaults are 20,000 point/update operations, 2,000 range/group operations,
1,000 warmup operations, three repetitions, and concurrency 1 and 8. Individual
latencies, errors, elapsed wall time, throughput, p50/p95/p99, and verification
status are retained in JSON. Compare medians across repetitions and keep all
runs. These runs are diagnostic baselines; publication requires independent
runs with reversed engine order, longer steady-state windows, additional data
sizes, multi-node hardware, and the workloads named above.

Version sources: [CockroachDB v26.3 releases](https://www.cockroachlabs.com/docs/releases/v26.3)
and [local cluster deployment](https://www.cockroachlabs.com/docs/stable/start-a-local-cluster.html).
