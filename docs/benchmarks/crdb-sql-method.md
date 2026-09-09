# RF3 SQL comparison method

Run from the checkout being measured:

```sh
python3 scripts/bench/run-crdb-sql-comparison.py /absolute/new/evidence-directory
python3 scripts/bench/run-crdb-sql-comparison.py /absolute/new/indexed-evidence \
  --indexes pack-leading --rows 10000000 --timeout 4h --no-verify-every-trial --seed-batch 64
```

Go 1.27, Docker, and Python 3 are required. The runner builds VibeDB with
`GOEXPERIMENT=simd`, pins CockroachDB v26.3.1 by image digest, records binary
hashes and the source revision/status, and runs each engine sequentially. Use
`--order crdb-first` to reverse engine order. Never compile or run unrelated
benchmarks during the measurement window. Nonzero exit codes and failed trials
are evidence, not results to discard. After measurements, the runner bounds
shutdown and records any forced termination; it does not claim a restart or
power-loss durability test. The runner continues to the second engine
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
bucket, an integer score, and a 256-byte ASCII text payload. The default 8,192-row
comparison has no secondary indexes. Pass `--indexes pack-leading` (or
`pack-nonleading`) to add a deterministic low-cardinality `shared` TEXT field plus
`a`/`b` and two compound indexes `(bucket, shared, a)` and `(bucket, shared, b)`
(or the non-leading order). That is the overlap-census shape used to measure
VibeDB exact-index packing against CockroachDB secondary indexes. Use
`--shared-cardinality 8` and `--shared-bytes 256` for the strong packing arm, or
`--indexes none` for the original PK-only schema. `--rows` accepts up to
10,000,000; 10M runs should also pass `--timeout 4h --no-verify-every-trial`.
VibeDB's table occupies one data group. CockroachDB retains its default range
boundaries; its setup verifies all voting replica counts equal three.
CockroachDB gets an explicit `ANALYZE` after loading. VibeDB currently has no
shipped distributed ANALYZE command; it uses the available optimizer metadata.
The PK-only schema does not establish anything about secondary-index update
locality. The pack-index variant does compare compound exact-index space and
seed write cost, still without claiming planner quality, skewed multi-partition
joins, or correlated predicates.

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
contention. Every result is checked; after each trial every stored field of every
row is verified in bounded pages and the total row count is checked separately.
The PK-only schema has four fields; pack-index runs also verify `shared`, `a`,
and `b`.
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
