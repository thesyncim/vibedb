# RF3 SQL comparison after durable update and checkpoint fixes

All trials passed, but **the target is not met**. VibeDB remains slower than
CockroachDB on every measured workload. In particular, adding clients does not
increase update throughput because the PostgreSQL table outbox serializes writes.

Source: `dc48cd2e` (clean tree; full revision and binary hashes in `manifest.json`).
CockroachDB: v26.3.1, pinned image digest recorded in the manifest.

## Method

Same [method](../crdb-sql-method.md): Go 1.27, Linux arm64 in Docker, three voting
replicas per group, sequential engines in one 12-CPU/24-GiB container with the same
disk volume. Default durable writes and inter-node TLS; SQL loopback plaintext
and trust for both. VibeDB uses three catalog, three ledger and three data
processes plus the gateway; CockroachDB uses three nodes. These are single-host,
warm-cache measurements, not independent-machine, failure or WAN measurements.

8,192 rows, 256-byte payloads, clients 1 and 8, 1,000 warmups, three repetitions,
and **2,000 measured operations per trial for every workload**. This differs from
the earlier report's 20,000 point/update operations. Compare engines within this
run; do not present its rate changes against older runs as a controlled speedup.
Each operation checks its result, and each trial verifies the complete table.
Profiling was disabled. No tests or builds ran during timed trials. Engine order
was VibeDB first; this small three-repetition run does not remove order effects.

## Results

The validator checked all 120,000 recorded latency samples, recomputed error
counts, percentiles and rates, and required all repetitions to pass. Values below
are median successful operations/second across the three repetitions.

| Workload | Clients | VibeDB ops/s | CRDB ops/s | VibeDB / CRDB |
|---|---:|---:|---:|---:|
| Point hit | 1 | 2,302.7 | 4,758.2 | 0.484× |
| Point hit | 8 | 6,787.4 | 31,848.8 | 0.213× |
| Point miss | 1 | 2,368.4 | 4,908.8 | 0.482× |
| Point miss | 8 | 7,002.5 | 27,026.5 | 0.259× |
| Ordered range, 64 rows | 1 | 1,504.4 | 3,288.7 | 0.457× |
| Ordered range, 64 rows | 8 | 4,348.1 | 22,225.8 | 0.196× |
| Group into 16 buckets | 1 | 180.0 | 409.9 | 0.439× |
| Group into 16 buckets | 8 | 353.0 | 2,396.3 | 0.147× |
| Existing-row update | 1 | 164.1 | 853.1 | 0.192× |
| Existing-row update | 8 | 162.8 | 4,107.8 | 0.040× |

These tests do not establish full CockroachDB feature or guarantee parity.
[The protocol changes](../../distributed-write-lane-proposal.md) retain exact
prepared mutation bytes across server-side recovery and preserve quorum and
storage durability. [The bottleneck investigation](../distributed-sql-bottlenecks-2026-09-04.md)
distinguishes measured latency from remaining hypotheses.

## Reproduction and retained evidence

Run from the recorded revision, choosing a new output directory:

```sh
python3 scripts/bench/run-crdb-sql-comparison.py /tmp/vibedb-crdb-reproduction \
  --rows 8192 --operations 2000 --scans 2000 --warmup 1000 --repetitions 3
python3 scripts/bench/summarize-crdb-sql.py \
  docs/benchmarks/crdb-sql-2026-09-04-writes/vibedb.json.gz \
  docs/benchmarks/crdb-sql-2026-09-04-writes/cockroachdb.json.gz
```

Compressed JSON retains every raw latency sample. The manifest, client logs,
exit codes, platform and storage observations are retained alongside it.
`interrupted-b533bae8` preserves the earlier incomplete attempt and its explicit
termination reason. It is excluded from all medians and ratios above.
