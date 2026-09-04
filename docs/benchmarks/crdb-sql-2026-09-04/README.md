# CockroachDB comparison after ordered primary LIMIT: 2026-09-04

**The target of twice CockroachDB throughput is not met.** CockroachDB remains
faster on every shared measured workload. VibeDB passes all 24 read trials but
still fails computed-update warmup; it has no valid update throughput.

VibeDB's 64-row range median is 2,069 ops/s with one client and 5,971 with eight.
The [previous run](../crdb-sql-2026-09-03/README.md) measured 483 and 1,007: about
4.3× and 5.9× higher throughput. That is a comparison between separate runs, not
an interleaved A/B experiment. The new CRDB medians are 4,588 and 31,841; VibeDB
still needs about 4.4× and 10.7× its current range throughput to meet the 2× target.

The implementation certifies the table's primary-key order, limits scan batch
size, and stops after the complete filter accepts enough rows. It preserves
full scans for different/descending orders, aggregates, joins, runtime SQL domain
resolution, and transaction overlays. It does not change consensus, durability,
resource limits, the benchmark client, or the dataset.

## Method and complete results

Same [method and reproduction harness](../crdb-sql-method.md): warm-cache,
single-host Linux ARM64 in Docker on an Apple M4 Max; 12 shared CPUs and 24 GiB;
three voters per group; inter-node TLS; default durable storage; 8,192 rows with
repeated 256-byte payload; no secondary indexes. Three trials each, 20,000 point
and update operations or 2,000 range/group operations, with 1,000 warmup operations.
Both engines use unnamed PostgreSQL extended requests and check every returned
value. The whole table is verified after every measured trial. Engines run
sequentially in the same container with fresh stores, VibeDB first. No test or
compile job was run concurrently with the measurement windows.

Validated 648,000 raw latency samples; medians require all repetitions to pass.

| Workload | Clients | VibeDB ops/s | CRDB ops/s | VibeDB / CRDB |
|---|---:|---:|---:|---:|
| point_hit | 1 | 3,264.5 | 7,232.7 | 0.451× |
| point_hit | 8 | 9,705.0 | 44,624.0 | 0.217× |
| point_miss | 1 | 3,339.9 | 7,329.4 | 0.456× |
| point_miss | 8 | 9,427.1 | 45,332.4 | 0.208× |
| range_64 | 1 | 2,069.4 | 4,587.7 | 0.451× |
| range_64 | 8 | 5,971.3 | 31,840.6 | 0.188× |
| group_16 | 1 | 245.3 | 572.0 | 0.429× |
| group_16 | 8 | 496.8 | 3,166.3 | 0.157× |
| update_existing | 1 | Incomplete/failed | 1,223.0 | N/A |
| update_existing | 8 | Incomplete/failed | 6,111.9 | N/A |

VibeDB recorded failure: update_existing warmup: ERROR: PostgreSQL write outcome unknown for request 3c0db83f94a1335bf484cd44c365b210; server retains the request for automatic recovery; do not resubmit the write without verifying its outcome; reconnecting does not resolve or cancel it (SQLSTATE 40003)


## Latency

Each displayed percentile is independently summarized by the median across all
three trials. These are closed-loop client latencies including request creation
and result checking, not an open-loop latency SLO.

| Workload | Clients | VibeDB p50 / p95 / p99 ms | CRDB p50 / p95 / p99 ms |
|---|---:|---:|---:|
| point_hit | 1 | 0.290 / 0.403 / 0.574 | 0.132 / 0.178 / 0.252 |
| point_hit | 8 | 0.700 / 1.623 / 2.299 | 0.174 / 0.244 / 0.301 |
| point_miss | 1 | 0.283 / 0.395 / 0.570 | 0.131 / 0.164 / 0.235 |
| point_miss | 8 | 0.722 / 1.650 / 2.271 | 0.170 / 0.238 / 0.286 |
| range_64 | 1 | 0.461 / 0.616 / 0.797 | 0.204 / 0.282 / 0.613 |
| range_64 | 8 | 1.015 / 2.909 / 4.271 | 0.239 / 0.325 / 0.430 |
| group_16 | 1 | 3.956 / 4.687 / 5.085 | 1.708 / 1.977 / 3.442 |
| group_16 | 8 | 14.601 / 31.221 / 43.055 | 1.982 / 4.664 / 5.384 |
| update_existing | 1 | Failed / not reached | 0.797 / 1.047 / 1.290 |
| update_existing | 8 | Failed / not reached | 1.281 / 1.703 / 2.010 |

## Provenance, failures and limits

Both binaries and the unchanged workload client were built from clean revision
`b5d84523dfbf50f5ff9b820a2dc2a6b83e3f4be4`. CockroachDB is the same pinned v26.3.1
image as the previous comparison. [Manifest](manifest.json) records binary hashes,
image identity, architecture, resource limits, raw-data hashes and runner outcomes.
The runner completed both engines, returned failure for VibeDB's update error,
and removed its own container and volume. Post-measurement shutdown details are
retained in the manifest; shutdown is not a durability/restart test.

All 30 CRDB trials passed. Its first single-client update trial was substantially
slower (644 ops/s) than its next two (1,223 and 1,271); all are retained. VibeDB's
lookup and grouped-aggregate results did not improve materially. Differences
between runs should not be attributed entirely to the range code, especially
for operations that cannot use it.

The update failure still follows direct inserts with coordinated ledger admission
under one sequence counter. A [write-lane proposal](../../distributed-write-lane-proposal.md)
describes a fix and its required recovery tests. Automatic approval review blocked
that protocol change; no write-protocol implementation is included in this revision.
Unknown writes were not retried under a new identity or removed to obtain a result.

This single-host, warm-cache, one-data-group comparison does not establish
multi-region locality, behavior with independent failures, large datasets,
secondary indexes, skewed workloads, or distributed-join superiority.

Validation passed: full SQL driver and shard-service suites; ordered range
result differentials and repeated executor reuse; query race/cancellation checks;
SQL path normalization, runtime type checks, and overlay visibility; Go vet.

Raw results: [VibeDB](vibedb.json.gz), [CRDB](cockroachdb.json.gz),
[VibeDB client log](vibedb-client.log), [CRDB client log](cockroachdb-client.log).
Recompute medians and check sample identities, errors, percentiles, throughput
and trial completeness with:

```sh
python3 scripts/bench/summarize-crdb-sql.py \
  docs/benchmarks/crdb-sql-2026-09-04/vibedb.json.gz \
  docs/benchmarks/crdb-sql-2026-09-04/cockroachdb.json.gz
```
