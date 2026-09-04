# RF3 SQL comparison with concurrent autocommit

All 120,000 measured operations passed result checks and full-table verification.
VibeDB update throughput is now **390.5 ops/s at one client** and **2,029.3 at
eight clients**. CockroachDB reached **909.4** and **3,966.8**, respectively.
The target of 2x CockroachDB is **not met**; every measured workload remains slower.

Source: clean `3e9d2306046cb341d7d15fef0fcc86784be804f9`.
CockroachDB: v26.3.1, pinned image digest in `manifest.json`.

## Method and complete results

Same [method](../crdb-sql-method.md): Linux arm64 Docker, 12 shared CPUs,
24 GiB, engines run sequentially on the same disk volume, RF3, inter-node TLS,
SQL loopback plaintext/trust for both. VibeDB has three catalog, three ledger and
three data processes plus a gateway; CockroachDB has three nodes. Go 1.27,
GOEXPERIMENT=simd. No data durability or quorum settings disabled.

8,192 rows with 256-byte payloads; clients 1 and 8; 1,000 warmups; three repetitions;
2,000 measured operations per trial for every workload. All use the same extended
unnamed PostgreSQL protocol. Profiling was disabled, and no tests or builds ran
concurrently with measurements. VibeDB ran first. These are warm-cache,
single-host measurements, not independent-machine, WAN or failure measurements.
Three repetitions do not eliminate order effects or host variability.

Median successful operations/second (all repetitions must pass):

| Workload | Clients | VibeDB | CockroachDB | VibeDB / CRDB |
|---|---:|---:|---:|---:|
| Point hit | 1 | 2,078.2 | 4,801.9 | 0.433x |
| Point hit | 8 | 6,563.9 | 32,015.1 | 0.205x |
| Point miss | 1 | 2,358.7 | 4,943.5 | 0.477x |
| Point miss | 8 | 7,018.5 | 33,237.1 | 0.211x |
| Ordered range, 64 rows | 1 | 1,470.9 | 3,547.7 | 0.415x |
| Ordered range, 64 rows | 8 | 4,270.1 | 23,425.8 | 0.182x |
| Group into 16 buckets | 1 | 170.8 | 402.9 | 0.424x |
| Group into 16 buckets | 8 | 354.8 | 2,424.0 | 0.146x |
| Existing-row update | 1 | 390.5 | 909.4 | 0.429x |
| Existing-row update | 8 | 2,029.3 | 3,966.8 | 0.512x |

The [previous run](../crdb-sql-2026-09-04-writes/README.md) observed VibeDB update
rates of 164.1 and 162.8 ops/s with the same workload parameters. The new rates
are approximately 2.38x and 12.46x those observations. This is historical context,
not an interleaved controlled old/new experiment or a general speedup claim.

## Changes and guarantees

The [implementation](../../distributed-write-lane-proposal.md) gives eligible
single-group PG autocommit writes 16 bounded independent issuer slots. Sequence
blocks are reserved durably before use, eliminating the two per-statement gateway
outbox syncs. Exact recipes remain in memory until a live slot knows its outcome;
restart skips all previously reserved sequences. Interrupted PG clients must
verify ambiguous outcomes before resubmitting non-idempotent work. Existing
outboxes and native durable requests retain their crash-replay contract.

Definitive conflicts can retry with a fresh identity/preimage. Read-only
preparation handles admission backpressure with bounded waits. Unknown execution
never permits SQL reevaluation under the same identity. Lone Raft proposals no
longer wait for a 500-microsecond batching timer; concurrent cohorts retain
bounded batching. Success still follows the existing replicated durability
barriers. Native session completions retain lazy snapshots for validation that
requires scans, fixing regressions in the earlier point-only lookup change.

Full gateway, gateway-command and replicated-state tests passed; after the
scheduling change, full Raft-service and gateway-command tests passed. Focused
race checks cover parallel identities, exact retry, reservation restart/failure,
legacy recovery isolation, completion lookup and proposal scheduling. The
reservation process-exit test exits without cleanup and reopens the OS lock and
sequence file. This is not a power-loss test. These checks and the SQL benchmark
do not establish complete CockroachDB feature or distributed-failure parity.

## Remaining bottleneck

The separate [diagnostic profile](profile/README.md) records 2,128 write regions:
mean preparation 0.598 ms, execution 2.567 ms, total write region 3.355 ms.
Only three fallback outbox saves occurred, all during initialization; there is
no per-request gateway journal publication in the warmed direct path. Execution
now dominates: replication, durable apply, scheduling and response transport.
Its duration is not a pure Raft-network or storage-only measurement.

The data leader accumulated about 0.80 s in Raft-log fdatasync and 0.31 s in other
fsync calls across the diagnostic workload. These aggregate cross-goroutine
waits must not be divided into a claimed per-update critical-path breakdown.
SQL reads still reserve large maximum-result buffers; this write change does not
remove their existing concurrency and query execution costs.

## Reproduction and evidence

```sh
python3 scripts/bench/run-crdb-sql-comparison.py /tmp/vibedb-concurrent-reproduction \
  --rows 8192 --operations 2000 --scans 2000 --warmup 1000 --repetitions 3
python3 scripts/bench/summarize-crdb-sql.py \
  docs/benchmarks/crdb-sql-2026-09-04-concurrent/vibedb.json.gz \
  docs/benchmarks/crdb-sql-2026-09-04-concurrent/cockroachdb.json.gz
```

The compressed JSON includes every latency and error sample. The validator
recomputes counts, errors, rates and percentiles; `summary.md` is its output.
Manifests record exact revisions, clean source state, binaries, images, limits,
and workload configuration. Client logs, platform/storage observations and exit
codes are retained. Forced shutdown notes concern post-measurement cleanup only.
The [failed precursor](failed-bfd7bf97/README.md) is retained and excluded from
all results above.
