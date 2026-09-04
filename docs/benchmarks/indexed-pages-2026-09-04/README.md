# Indexed point reads and ordered pages

Comparison against main `c0675b8af96e`, using the public PostgreSQL endpoint of
an RF3 development cluster. The dataset has 500,000 rows, each with an ordered
string primary key, numeric bucket and score, and a 256-byte payload. The
cluster runs a gateway plus three voters for each of the data, catalog, and
request-ledger groups (nine shard processes). This is one data shard with three
replicas, not a multi-data-shard scalability test.

## Changes

* Keep an independent count of accepted ordered rows across spill runs. The
  old counter was the length of the current merge slice, which resets on every
  spill. A page smaller than the requested LIMIT could spill repeatedly and
  cause the executor to scan to the end of the source.
* Execute indexed projections of up to 256 candidate rows synchronously.
  Certified primary ordering allows direct result materialization and an exact
  stop at LIMIT, including residual predicates. Unordered secondary candidates
  keep their ordinary sort. Oversized secondary batches use the existing
  spill executor.
* Reserve small input arenas for cold point reads and small pages. Copy
  projected variable-width cells directly into owned result storage without
  an intermediate detached copy. Warm paths retain their zero-allocation
  behavior.

Quorum fencing, snapshot ownership, cancellation, predicate evaluation, result
budgets, index admission, and the on-disk format retain their existing contracts.

## Method

Go 1.27, Linux arm64, `CGO_ENABLED=0`, `GOEXPERIMENT=simd`. Both versions use the
same Docker container (12 CPU, 24 GiB) and named disk volume. PostgreSQL is local
plaintext development authentication; inter-node traffic uses TLS. The setup
uses the standard 64-row SQL INSERT statements. No durability settings are
disabled. Reads use unnamed extended parse/bind/execute and autocommit, including
normal gateway routing and a leader ReadIndex cut. Builds and tests do not
overlap timed trials. Full-table verification runs before and after each
matrix, and every timed operation validates its returned values and order.

Points and 32/64-row ranges: C1/C8, three repetitions, 500 warmups per trial,
3,000 measured point operations or 1,500 range operations. The slow 256-row
baseline uses C1, three repetitions, one warmup and eight measured operations
per trial; the changed version uses the identical small matrix plus C8. Those
24 C1 samples per version establish the large throughput difference, not precise
tail latency. An attempted baseline C8 run timed out all eight requests at 30
seconds and is retained as failed evidence, not a throughput measurement.

Because the initial smaller-query results were noisy, a confirmation matrix ran
the changed version first, then baseline: 10,000 point operations or 5,000 range
operations, 1,000 warmups, three repetitions, C1/C8. The changed version also ran
256-row pages at this longer duration; baseline 256-row pages were excluded
because each request scanned nearly the whole table.

The initial combined matrix was stopped during 256-row warmup because main
scanned almost the entire corpus per request. Its partial log is retained, and
complete fast and C1-only slow baseline matrices were rerun separately. A baseline EXPLAIN ANALYZE for
`id >= 'key-00000017' ORDER BY id LIMIT 256` scanned 499,983 rows, emitted 256,
created 2,459 spill runs, and wrote about 141 MB of temporary data.

## Results

All completed accepted matrices verified every operation and the full table with
zero errors. Tables show medians of the three per-trial throughputs and p50s;
they are not percentiles pooled across trials. Raw sample vectors are retained.

The matched small C1 256-row matrix improves p50 from **3,009.282 ms to
0.590 ms** (about 5,100×). The longer changed-version matrix sustains 1,235 ops/s
at C1 and 7,965 ops/s at C8, with p50s 0.685 ms and 0.912 ms respectively.
Baseline C8 timed out, so there is no valid C8 throughput ratio.

The saved EXPLAIN execution drops from 2,912.614 ms to 0.280 ms, scanning
**499,983 → 256 rows** and spilling **141,440,290 → 0 bytes**. This measures
executor time; the SQL client numbers above include routing and quorum reads.

Point reads and 32/64-row pages do **not** show a consistent distributed
throughput improvement across both matrices. Host/scheduling variability is
large enough to reverse some results; these measurements do not support a
blanket end-to-end speedup claim for the smaller requests.

### Initial matrix: baseline, then changed

Raw reports: [baseline-fast.json.gz](baseline-fast.json.gz), [after-fast.json.gz](after-fast.json.gz).

| Query | Clients | Baseline ops/s | Changed ops/s | Baseline p50 ms | Changed p50 ms |
|---|---:|---:|---:|---:|---:|
| point_hit | 1 | 3,326.2 | 3,445.3 | 0.283 | 0.272 |
| point_hit | 8 | 13,999.3 | 13,263.1 | 0.535 | 0.558 |
| point_miss | 1 | 3,404.6 | 3,554.4 | 0.275 | 0.265 |
| point_miss | 8 | 14,859.1 | 14,820.7 | 0.509 | 0.501 |
| range_32 | 1 | 2,521.3 | 2,982.1 | 0.379 | 0.318 |
| range_32 | 8 | 12,417.8 | 9,279.4 | 0.601 | 0.760 |
| range_64 | 1 | 2,079.5 | 1,565.3 | 0.457 | 0.426 |
| range_64 | 8 | 11,034.2 | 11,692.1 | 0.680 | 0.633 |

### Longer confirmation: changed, then baseline

Raw reports: [baseline-confirm.json.gz](baseline-confirm.json.gz), [after-confirm.json.gz](after-confirm.json.gz).

| Query | Clients | Baseline ops/s | Changed ops/s | Baseline p50 ms | Changed p50 ms |
|---|---:|---:|---:|---:|---:|
| point_hit | 1 | 3,511.9 | 2,902.8 | 0.261 | 0.304 |
| point_hit | 8 | 14,380.2 | 14,344.3 | 0.519 | 0.523 |
| point_miss | 1 | 3,806.2 | 3,548.4 | 0.249 | 0.263 |
| point_miss | 8 | 15,482.8 | 13,348.5 | 0.487 | 0.563 |
| range_32 | 1 | 2,567.8 | 2,239.9 | 0.370 | 0.358 |
| range_32 | 8 | 12,243.5 | 9,886.2 | 0.613 | 0.683 |
| range_64 | 1 | 2,300.8 | 2,005.8 | 0.415 | 0.410 |
| range_64 | 8 | 5,255.6 | 11,452.9 | 1.227 | 0.639 |

### In-process diagnostics

One million durable rows, Go 1.27 with SIMD, Darwin arm64 on Apple M4 Max.
These isolate execution and do not replace the distributed results. Each is
one 1-second Go benchmark sample after warming 1,024 probe positions.

| Query | Baseline µs/op | Changed µs/op | Speedup |
|---|---:|---:|---:|
| Point | 0.346 | 0.356 | 0.97× |
| Primary/rows=1 | 17.419 | 3.094 | 5.63× |
| Primary/rows=32 | 50.155 | 16.716 | 3.00× |
| Primary/rows=64 | 77.333 | 30.529 | 2.53× |
| Primary/rows=256 | 232.586 | 113.104 | 2.06× |
| Secondary/rows=1 | 11.308 | 3.298 | 3.43× |
| Secondary/rows=32 | 36.573 | 25.780 | 1.42× |
| Secondary/rows=64 | 61.358 | 47.270 | 1.30× |
| Secondary/rows=256 | 202.076 | 170.189 | 1.19× |

All changed diagnostic cases report 0 B/op and 0 allocs/op after warming.
Raw storage point reads are unchanged; indexed projection execution is faster.
The separate cold point-arena benchmark reduces 16,432 to 432 B/op and
1,292 to 160 ns/op. It is not an end-to-end point-read latency measurement.

Evidence includes raw successful and failed reports, per-trial logs, readable
EXPLAIN plans, server shutdown diagnostics, regression output, and binary
SHA-256 hashes in [builds.json](builds.json). The synthetic cluster and volume
were removed after collecting these artifacts.

## Reproduction

Build the three server binaries from the baseline revision and changed source
into separate directories. Build `integration/pgclient/cmd/rf3-sqlbench` once
and use the same client for both versions. Start each version with explicit
binary paths, preserving the root directory between runs:

```sh
vibedb cluster dev --root /data/vibe500 --replicas 3 --node-log \
  --shard-binary /bench/VERSION/vibedb-shard \
  --gateway-binary /bench/VERSION/vibedb-gateway \
  --pg-listen 127.0.0.1:5432 --diagnostics-on-exit
```

Seed only once, using `-phase setup -rows 500000`. After readiness, run:

```sh
rf3-sqlbench -engine vibedb \
  -url 'postgresql://local@127.0.0.1:5432/vibedb?sslmode=disable' \
  -phase run -rows 500000 -operations 3000 -scans 1500 -warmup 500 \
  -repetitions 3 -clients 1,8 \
  -workloads point_hit,point_miss,range_32,range_64 \
  -verify-every-trial=false -output VERSION-fast.json

rf3-sqlbench -engine vibedb \
  -url 'postgresql://local@127.0.0.1:5432/vibedb?sslmode=disable' \
  -phase run -rows 500000 -operations 8 -scans 8 -warmup 1 \
  -repetitions 3 -clients 1 -workloads range_256 \
  -verify-every-trial=false -output VERSION-256.json
```

## Qualification and limitations

The new spill regression fails on main (4,096 rows scanned for LIMIT 512) and
passes after the fix. Differential page tests cover physical/secondary order
disagreement, exclusive endpoints, residual filters, small byte budgets,
secondary fallback, result budgets, cancellation/reuse, and result lifetime.
Final targeted race tests pass for query, store, SQL driver, and durable
point/range/index checks. The final full query, store, and SQL driver package
runs pass (214 s, 17 s, and 281 s respectively). The full durable package
run exceeded its default ten-minute timeout during `TestFilePrimaryChurnQualification`;
its targeted read tests passed.

A separate attempt to seed one million rows on unmodified main stopped at
row 621,632 with `primary macro-tablet split required`. No measurements were
accepted from that failed fixture. The final comparison uses a fresh 500,000-row
fixture. Larger INSERT sizes were also refused by existing statement/mutation
admission limits; the accepted fixture uses the default batch size of 64.

The million-row in-process `BenchmarkFileIndexedPage` is a diagnostic for
primary and secondary index execution. Its heap-built corpus avoids the separate
incremental macro-tablet growth limit and is not a distributed measurement.
`BenchmarkSegmentPointArena` isolates cold input materialization; it does not
measure end-to-end point latency.

Final validation commands (with `GOCACHE=/private/tmp/vibedb-go-cache` and
`GOEXPERIMENT=simd`):

```sh
go test ./query ./store ./sql/driver
go test -race ./query ./store ./sql/driver ./store/durable \
  -run 'TestFileSmallIndexedPages|TestPrimaryOrderedLimitSurvivesSpills|TestSegmentReserveRetainsViewsAndReset|Test.*Point|TestReplicatedReadSession|TestFileSnapshotOrderedIndexRange|TestIndexProbeMemoryBound' -count=1
(cd integration/pgclient && go test ./cmd/rf3-sqlbench)
go test ./query -run '^$' -bench '^BenchmarkFileIndexedPage$' -benchtime=1s -count=1
```
