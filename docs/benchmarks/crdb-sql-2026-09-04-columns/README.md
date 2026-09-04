# Direct durable columns: experiment has not established a performance win

Clean candidate `d175605579ed3b843efeee7c5e04158f26cabff7`, based on main
`fff5d689`. Both engines completed, full-table verification passed, and the
validator checked all **120,000 raw latency samples** with zero errors.
VibeDB delivered **0.329–0.504× CockroachDB throughput**. The work branch retains
this candidate; it is not being merged into main as a demonstrated performance
improvement.

[Throughput medians](summary.md), [latency percentiles](latencies.md), complete
compressed reports, client/server/runner logs, manifest, process lists, disk
allocation detail and [local experiments](local/README.md) are retained.

## Change and method

The candidate extracts top-level columns from validated durable JSON batches
without copying each document into a temporary structural index. Exact scalar
semantics, fallback paths, output ownership, cancellation and memory admission
remain covered by differential tests. Durability barriers, replication and
read-cut protocols are unchanged.

Same RF3 single-host matrix as the preceding comparison: 8,192 rows,
256-byte payload, C1/C8, 1,000 warmups, 2,000 measured operations per workload,
three repetitions. VibeDB first, CRDB second, sequentially in one Linux arm64
container limited to 12 CPU and 24 GiB, using a named disk volume. Go 1.27 with
SIMD experiment enabled. CRDB v26.3.1 is pinned by image digest in the manifest.
Inter-node TLS; loopback SQL trust/plaintext for both. Nine VibeDB shard
processes plus gateway versus three CRDB processes. No build, test or profile
processing overlapped timed measurement. The runner recorded a retried CRDB
initialization connection and forced post-measurement shutdown in its logs;
both workload clients exited successfully.

```sh
python3 scripts/bench/run-crdb-sql-comparison.py /new/evidence/path \
  --node-log --rows 8192 --operations 2000 --scans 2000 \
  --warmup 1000 --repetitions 3 --clients 1,8
```

## Outcome

The preceding source `a37b8471` comparison reported 0.330–0.595× CRDB. All new
ratios are lower, including grouped scans: C1 0.442× versus 0.465× and C8
0.504× versus 0.595×. Update ratios fall from 0.554×/0.463× to 0.432×/0.353×.
These are comparisons of separate runs, not a controlled causal A/B test.
Absolute throughput is substantially lower for both engines. Repeating the
unchanged local baseline also slowed by 44–49%, so host execution conditions
changed during this investigation. That observation does not excuse the
negative database result or establish that the candidate is faster.

VibeDB C8 update trials are 992.6, 1,474.3 and 1,651.0 ops/s, with p99
30.019, 22.005 and 22.622 ms. C1 update p99 spans 11.288–13.806 ms. The slow
trials and tails are retained. The cause of this instability remains unproven;
profiling needs to span sustained writes and correlate requests with durable
waves, rotation/checkpoint work and scheduling.

Removing temporary indexing alone is insufficient. The scan still reconstructs
complete JSON from typed compact streams before extracting fields; direct
column delivery from storage is a larger next design candidate. The separate
quorum-read round trip and distributed write/commit path also remain. A
controlled parent/candidate comparison is needed before assigning causality to
this experiment or promoting it as a throughput improvement.

This does not qualify representative multi-machine scalability, steady-state
space amplification, comparable interactive serializable transactions or
nonblocking schema evolution. The full performance-and-scale goal remains
active; no acceptance gate is completed by this run.
