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
| Main | 6,505 | 14,958 | 109 |
| Candidate | 4,329 | 10,153 | 78 |
| Change | -33.5% | -32.1% | -28.4% |

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

The candidate ran before the confirmation baseline. Every build ran 6,000 point
operations or 3,000 range operations per trial, 500 warmups, five repetitions,
and 1 or 8 closed-loop clients. Every operation checked its returned values and
order. The harness also verified the complete table before and after each
50-trial matrix. All 100 trials completed with zero errors.

| Query | Clients | Main ops/s | Candidate ops/s | Throughput | Main p50 us | Candidate p50 us | p50 latency |
|---|---:|---:|---:|---:|---:|---:|---:|
| point hit | 1 | 3,847.7 | 3,923.5 | +2.0% | 244.9 | 238.5 | -2.6% |
| point hit | 8 | 19,237.1 | 19,378.8 | +0.7% | 390.1 | 384.3 | -1.5% |
| point miss | 1 | 3,953.9 | 4,046.0 | +2.3% | 236.4 | 229.9 | -2.7% |
| point miss | 8 | 20,588.1 | 20,849.1 | +1.3% | 368.7 | 364.8 | -1.1% |
| range 32 | 1 | 3,308.3 | 3,425.0 | +3.5% | 285.7 | 274.3 | -4.0% |
| range 32 | 8 | 15,874.3 | 16,959.8 | +6.8% | 466.7 | 439.2 | -5.9% |
| range 64 | 1 | 2,890.6 | 3,019.4 | +4.5% | 327.0 | 311.1 | -4.9% |
| range 64 | 8 | 13,591.5 | 14,617.0 | +7.5% | 542.8 | 509.9 | -6.1% |
| range 256 | 1 | 1,793.4 | 1,886.0 | +5.2% | 531.1 | 508.8 | -4.2% |
| range 256 | 8 | 7,340.0 | 8,498.8 | +15.8% | 990.8 | 865.2 | -12.7% |

Public point throughput improves 0.7-2.3% and median latency improves 1.1-2.7%;
network and quorum work dominate this one-row wall-clock result. The fixed
planning and materialization savings become clearer with wider pages. The
largest public gain is the 256-row range at eight clients: throughput rises
15.8% and median latency falls 12.7%. The isolated prepared gateway benchmark
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
