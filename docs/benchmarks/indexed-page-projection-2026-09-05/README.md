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
| 32 | 16,358 | 12,892 | -21.2% |
| 64 | 29,882 | 23,026 | -22.9% |
| 256 | 110,052 | 84,227 | -23.5% |

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
| point hit | 1 | 3,847.7 | 3,885.0 | +1.0% | 244.9 | 234.3 | -4.3% |
| point hit | 8 | 19,237.1 | 20,648.0 | +7.3% | 390.1 | 361.0 | -7.5% |
| point miss | 1 | 3,953.9 | 3,900.1 | -1.4% | 236.4 | 234.1 | -1.0% |
| point miss | 8 | 20,588.1 | 21,466.3 | +4.3% | 368.7 | 350.0 | -5.1% |
| range 32 | 1 | 3,308.3 | 3,380.5 | +2.2% | 285.7 | 273.5 | -4.3% |
| range 32 | 8 | 15,874.3 | 17,055.6 | +7.4% | 466.7 | 430.8 | -7.7% |
| range 64 | 1 | 2,890.6 | 2,957.8 | +2.3% | 327.0 | 312.0 | -4.6% |
| range 64 | 8 | 13,591.5 | 14,776.1 | +8.7% | 542.8 | 501.6 | -7.6% |
| range 256 | 1 | 1,793.4 | 1,843.0 | +2.8% | 531.1 | 512.5 | -3.5% |
| range 256 | 8 | 7,340.0 | 8,353.3 | +13.8% | 990.8 | 882.7 | -10.9% |

The single-client point throughput is effectively flat, although median
latency improves. The fixed planning and materialization savings become clearer
with concurrent clients and wider pages. The largest public gain is the
256-row range at eight clients: throughput rises 13.8% and median latency falls
10.9%. The isolated prepared gateway benchmark separately measures the reduced
CPU/allocation cost before network and quorum latency.

The original target was a 1,000,000-row distributed fixture. A single-shard RF3
load reached 621,632 rows and then stopped with the existing `primary macro-tablet
split required` capacity boundary. The 500,000-row qualification stays below
that known storage ceiling while remaining much larger than every returned
range. The million-row in-process durable benchmark does not have that
single-shard replicated-load constraint.

`baseline-confirm-500000.json.gz` and `candidate-500000.json.gz` contain every
per-operation latency sample and verification flag. `builds.json` records source
revisions, toolchains, and binary hashes.
