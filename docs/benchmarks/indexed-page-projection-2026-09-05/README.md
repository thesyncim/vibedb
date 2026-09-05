# Prepared indexed point and ordered-page reads

This change accelerates the public PostgreSQL read path at two levels. Gateway
prepared statements retain their immutable physical plan and reusable parameter
and result storage. Shards execute one-document point reads directly from
storage-validated JSON, and primary-key ranges whose native bounds cover the
complete predicate project their ordered 32/64/256-row pages without building a
temporary structural tape or scalar-column/result-row intermediates. Complex
paths, escaped object keys, residual predicates, aggregates, and uncommon JSON
roots retain the general executor.

## Million-row page kernel

The durable benchmark contains 1,000,000 realistic JSON documents and probes
1,024 varying positions across the primary index. Setup and warming are outside
the timer. Each result is the median of three 3-second samples on Apple M4 Max,
Darwin arm64, Go 1.27. The result remains allocation-free after warmup.

| Returned rows | Main ns/op | Candidate ns/op | Latency |
|---:|---:|---:|---:|
| 32 | 15,890 | 12,584 | -20.8% |
| 64 | 29,406 | 22,663 | -22.9% |
| 256 | 111,537 | 83,276 | -25.3% |

Raw output is in `page-before.txt` and `page-after.txt`.

The in-process prepared PostgreSQL/RF3 gateway benchmark isolates the fixed
front-end, routing, and merge cost with transport responses held constant. Five
3-second samples give these medians:

| Prepared point path | ns/op | Bytes/op | Allocations/op |
|---|---:|---:|---:|
| Main | 5,897 | 14,957 | 109 |
| Candidate | 4,301 | 10,152 | 78 |
| Change | -27.1% | -32.1% | -28.4% |

Raw output is in `prepared-point-before.txt` and
`prepared-point-after.txt`.

## Public PostgreSQL through RF3

The end-to-end benchmark uses the shipped PostgreSQL extended unnamed
Parse/Bind/Execute path, gateway routing, authenticated native transport, a
linearizable RF3 leader read, shard SQL execution, response merge, and pgwire
encoding. Each fresh cluster contains three catalog, three ledger, and three
data processes plus the gateway. Each independently generated fixture has
500,000 deterministic 256-byte-payload rows, which is 1,953 times the largest
returned page.

The latest-main baseline ran before the candidate on a separate fresh cluster.
Every build ran 6,000 point operations or 3,000 range operations per trial, 500 warmups, five repetitions,
and 1 or 8 closed-loop clients. Every operation checked its returned values and
order. The harness also verified the complete table before and after each
50-trial matrix. All 100 trials completed with zero errors.

| Query | Clients | Main ops/s | Candidate ops/s | Throughput | Main p50 us | Candidate p50 us | p50 latency |
|---|---:|---:|---:|---:|---:|---:|---:|
| point hit | 1 | 3,959.2 | 4,079.5 | +3.0% | 236.6 | 228.7 | -3.3% |
| point hit | 8 | 20,441.9 | 20,797.6 | +1.7% | 368.8 | 364.3 | -1.2% |
| point miss | 1 | 4,052.3 | 4,099.5 | +1.2% | 229.4 | 227.2 | -0.9% |
| point miss | 8 | 21,478.4 | 21,990.9 | +2.4% | 356.0 | 348.0 | -2.3% |
| range 32 | 1 | 3,376.0 | 3,540.2 | +4.9% | 276.6 | 265.7 | -3.9% |
| range 32 | 8 | 16,595.2 | 17,996.0 | +8.4% | 449.7 | 418.7 | -6.9% |
| range 64 | 1 | 3,000.9 | 3,148.3 | +4.9% | 314.8 | 299.5 | -4.9% |
| range 64 | 8 | 14,013.7 | 15,428.0 | +10.1% | 524.4 | 485.8 | -7.4% |
| range 256 | 1 | 1,853.2 | 1,985.5 | +7.1% | 511.0 | 479.9 | -6.1% |
| range 256 | 8 | 7,796.5 | 8,959.6 | +14.9% | 935.7 | 826.7 | -11.6% |

Public point throughput improves 1.2-3.0% and median latency improves 0.9-3.3%;
network and quorum work dominate this one-row wall-clock result. The fixed
planning and materialization savings become clearer with wider pages. The
largest public gain is the 256-row range at eight clients: throughput rises
14.9% and median latency falls 11.6%. The isolated prepared gateway benchmark
shows the larger CPU/allocation reduction before network and quorum latency.

The original target was a 1,000,000-row distributed fixture. A single-shard RF3
load reached 621,632 rows and then stopped with the existing `primary macro-tablet
split required` capacity boundary. The 500,000-row qualification stays below
that known storage ceiling while remaining much larger than every returned
range. The million-row in-process durable benchmark does not have that
single-shard replicated-load constraint.

`baseline-confirm-500000.json.gz` and `candidate-500000.json.gz` contain every
per-operation latency sample and verification flag. `builds.json` records source
revisions, toolchains, and binary hashes.
