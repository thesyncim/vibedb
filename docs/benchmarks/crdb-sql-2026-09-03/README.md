# CockroachDB comparison: 2026-09-03

CockroachDB is faster on every measured shared workload in this run. VibeDB
passes the read-result checks but fails computed-update warmup, so it has no
valid update throughput result. These results do not establish competitive
superiority for VibeDB.

This is a warm-cache, single-host Linux ARM64 diagnostic comparison on an
Apple M4 Max through Docker: 12 shared CPUs, 24 GiB shared memory ceiling,
three voters, inter-node TLS, durable storage defaults, and the same PostgreSQL
workload client. Dataset: 8,192 rows, no secondary indexes, repeated 256-byte
payload. Full details and reproduction command: [method](../crdb-sql-method.md).

The tables show medians of three trials. Each trial has 20,000 point/update
operations or 2,000 range/group operations after 1,000 warmup operations. Every
returned value is checked, and the complete table is verified after each trial.
VibeDB passed all 24 read trials with zero operation errors. CockroachDB passed
all 30 trials with zero operation errors.

## Throughput

| Workload | Clients | VibeDB ops/s | CockroachDB ops/s | CRDB / VibeDB |
|---|---:|---:|---:|---:|
| Primary-key hit | 1 | 3,191 | 7,321 | 2.29× |
| Primary-key hit | 8 | 9,472 | 44,578 | 4.71× |
| Primary-key miss | 1 | 3,293 | 7,331 | 2.23× |
| Primary-key miss | 8 | 9,782 | 44,764 | 4.58× |
| 64-row range | 1 | 483 | 5,063 | 10.49× |
| 64-row range | 8 | 1,007 | 32,734 | 32.52× |
| 16-group count/sum | 1 | 264 | 609 | 2.30× |
| 16-group count/sum | 8 | 518 | 3,424 | 6.60× |
| Computed update | 1 | Failed warmup | 1,210 | N/A |
| Computed update | 8 | Not run after prior failure | 6,129 | N/A |

## Observed latency

All values are milliseconds, formatted **p50 / p95 / p99**. Each percentile is
independently summarized across the three trials. Latency includes client
construction and result checking; this is closed-loop latency, not an open-loop
SLO or a coordinated-omission-corrected histogram.

| Workload | Clients | VibeDB | CockroachDB |
|---|---:|---:|---:|
| Primary-key hit | 1 | 0.298 / 0.412 / 0.578 | 0.136 / 0.161 / 0.203 |
| Primary-key hit | 8 | 0.715 / 1.675 / 2.380 | 0.174 / 0.244 / 0.297 |
| Primary-key miss | 1 | 0.288 / 0.402 / 0.568 | 0.134 / 0.159 / 0.215 |
| Primary-key miss | 8 | 0.698 / 1.605 / 2.246 | 0.172 / 0.241 / 0.302 |
| 64-row range | 1 | 2.197 / 3.344 / 3.830 | 0.195 / 0.225 / 0.245 |
| 64-row range | 8 | 6.829 / 16.227 / 22.621 | 0.237 / 0.311 / 0.365 |
| 16-group count/sum | 1 | 3.663 / 4.432 / 4.790 | 1.592 / 1.817 / 3.246 |
| 16-group count/sum | 8 | 14.406 / 28.809 / 38.336 | 1.846 / 4.138 / 4.634 |
| Computed update | 1 | Failed warmup | 0.751 / 1.028 / 1.850 |
| Computed update | 8 | Not run after prior failure | 1.231 / 1.585 / 3.850 |

## Failures and changes driven by the comparison

The first eight-client VibeDB preflight had 9 failures in 16 point requests:
`replicated shard refusal 2` (execution-memory admission bound). The new bounded
queue removes that burst failure without increasing the 112 MiB frame/execution
budget. The longer run completed all eight-client read trials successfully.

RF3's borrowed transaction snapshot also ignored compiled primary-key ranges.
The snapshot now uses the existing range source when there are no pending writes
or split-ownership filters. Tests verify the old snapshot remains visible and
staged inserts, updates, and deletes retain their overlay semantics. Observed
single-client range throughput rose from about 284 ops/s in the short baseline
to 483 ops/s in the longer run. This is diagnostic evidence, not a controlled
publication-quality A/B experiment; that pilot did not retain a binary-hash
manifest.

Computed updates expose an unresolved sequence-domain defect. After direct
single-participant INSERTs, a computed UPDATE enters the coordinated ledger.
Direct writes advance the caller sequence without advancing that ledger's
contiguous admission sequence; Create then returns a sequence conflict. The
PostgreSQL endpoint reports SQLSTATE `40003` and retains the request. The client
stops rather than resubmitting an uncertain write. Both its warmup failure and
the successful CRDB update results are retained. VibeDB's eight-client update
trial was not reached.

The next work is to separate or durably reconcile direct/coordinated issuer
sequences, reduce range scanning and aggregation work, and improve concurrent
read admission efficiency. No conclusion about multi-region locality, distributed
join planning, secondary-index maintenance, or skew handling follows from this
single-table benchmark.

## Provenance and runner limitations

VibeDB's long run used **1ce8a3ff6b3466ee17b4e4fc84f86fa2eba2444d**.
CockroachDB's long run used **v26.3.1**, build commit
**3a0a7b8176595bd32687c34a2210e166c5535af4**, with the client built from
**410bf5979dfdc313aa0f34482439224d45f02f4e**. Between these two VibeDB revisions,
only documentation and CockroachDB setup orchestration changed. Database and
workload-client implementation were identical. Both source checkouts were clean
when their binaries were built.

The long measurements came from two sequential runner invocations on the same
Docker VM, with the same container limits and native-volume filesystem, but
fresh containers/volumes. The first saved the complete VibeDB measurements and
then failed at CRDB setup. The second saved the complete CRDB measurements and
then timed out draining the final CRDB node before VibeDB started. Neither
failure occurred during the saved measurement/verification windows. The runner
now separates cluster-setting statements and bounds post-measurement shutdown,
recording any forced termination. A separate small end-to-end run checks runner
behavior; its timings are not used in the tables.

Current main also contains later promotion-probe and CI changes. These numbers
are tied to the explicit measured revisions, not a claim about every later
commit. The full benchmark pair has not been repeated on separate machines or
in both engine orders. Earlier short pilot timings varied materially, especially
for CRDB; retain that uncertainty when using these diagnostic results.

Raw evidence:

- [VibeDB samples and failure](vibedb.json.gz)
- [CockroachDB samples](cockroachdb.json.gz)
- [Build hashes, resource settings, and run metadata](manifest.json)
- [Original admission failures](vibedb-before-backpressure.json.gz)
- [Original short single-client baseline](vibedb-before-range.json.gz)
- [VibeDB client log](vibedb-client.log) and [CRDB client log](cockroachdb-client.log)

Validation: affected planner, SQL driver, and shard-service suites passed;
focused admission race tests and synopsis fuzzing passed. The [runner check](runner-check.json)
completed both engines, preserved the expected update failure, and removed its
owned containers and volume.
