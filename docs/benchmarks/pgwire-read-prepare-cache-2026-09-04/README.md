# Gateway pgwire distributed-read prepare cache

Continues from merged PR #142 (`033a0239`). Extended unnamed PostgreSQL
Parse/Bind/Execute closes the backend statement before the next Parse. The
gateway therefore reparsed and semantically recompiled identical distributed
SELECT text on every operation even though one connection had just released the
same statement.

Each gateway PostgreSQL session now retains one recently closed distributed
SELECT. An exact SQL and parameter-type-hint match transfers sole ownership of
its `query.Statement` into the new backend statement. The entry is rejected when
the catalog generation changes. Local SELECTs, writes, DDL, statements above
4 KiB, and statements above 256 parameters are never cached. Closing the session
releases the retained compiler and AST. A one-entry design covers repeated
unnamed execution without retaining an SQL-keyed map per connection.

The cache does not bypass bind validation or query execution. Every operation
still checks parameter roles and types in pgwire, checks transport values and
arity in the gateway, pins a catalog generation, routes the indexed query, and
executes through the RF3 quorum-read transport.

## Isolated prepare cost

Darwin arm64 / Apple M4 Max, Go 1.27, five samples:

| Path | Median ns/op | Bytes/op | Allocations/op |
|---|---:|---:|---:|
| Cold parse and semantic prepare | 3,512 | 12,856 | 56 |
| Exact cache hit and close | 53.18 | 80 | 1 |

The exact gateway backend prepare/close path is about 66 times faster on a cache
hit. The benchmark uses the typed indexed point-query shape with ordered LIMIT
256. Raw results are in `prepare-bench.txt`.

## Distributed end-to-end results

The public PostgreSQL path ran against one persistent 244,608-row RF3 fixture:
three catalog, three ledger, and three data processes plus the gateway. The data
set is 956 times the largest requested page. Each matrix verified the complete
table before and after, and every operation checked its returned values and
order. All 200 trials completed with zero errors.

Trials used 6,000 point operations or 3,000 range operations, 500 warmups, five
repetitions, and 1 or 8 closed-loop clients. The client used extended unnamed
parse/bind/execute and one autocommit statement per operation. Cells are medians
of five repetitions.

### Pair 1: merged main then cache

| Query | Clients | Main ops/s | Cache ops/s | Throughput | Main p50 us | Cache p50 us |
|---|---:|---:|---:|---:|---:|---:|
| point hit | 1 | 3,590.2 | 3,892.4 | +8.4% | 262.4 | 236.8 |
| point hit | 8 | 18,026.7 | 19,850.4 | +10.1% | 409.3 | 379.8 |
| point miss | 1 | 3,649.3 | 4,092.7 | +12.1% | 258.0 | 226.2 |
| point miss | 8 | 18,892.7 | 20,609.4 | +9.1% | 394.0 | 366.1 |
| range 32 | 1 | 3,028.9 | 2,912.6 | -3.8% | 313.2 | 323.7 |
| range 32 | 8 | 15,070.6 | 16,018.0 | +6.3% | 488.5 | 461.1 |
| range 64 | 1 | 2,686.2 | 2,848.5 | +6.0% | 356.1 | 327.9 |
| range 64 | 8 | 13,292.1 | 13,513.1 | +1.7% | 550.0 | 535.4 |
| range 256 | 1 | 1,698.2 | 1,689.7 | -0.5% | 555.5 | 554.4 |
| range 256 | 8 | 7,060.5 | 7,303.5 | +3.4% | 1,028.7 | 980.5 |

### Pair 2: cache then merged main

The displayed columns remain main then cache; execution order was reversed.

| Query | Clients | Main ops/s | Cache ops/s | Throughput | Main p50 us | Cache p50 us |
|---|---:|---:|---:|---:|---:|---:|
| point hit | 1 | 3,364.6 | 3,492.6 | +3.8% | 272.6 | 265.9 |
| point hit | 8 | 17,078.3 | 18,835.4 | +10.3% | 431.7 | 397.5 |
| point miss | 1 | 3,618.4 | 3,922.0 | +8.4% | 249.4 | 235.0 |
| point miss | 8 | 19,605.6 | 20,396.2 | +4.0% | 375.2 | 368.0 |
| range 32 | 1 | 3,219.0 | 3,349.9 | +4.1% | 287.5 | 280.2 |
| range 32 | 8 | 15,347.2 | 15,931.3 | +3.8% | 474.5 | 466.5 |
| range 64 | 1 | 2,615.1 | 2,828.3 | +8.2% | 342.3 | 328.8 |
| range 64 | 8 | 13,330.3 | 12,814.6 | -3.9% | 544.7 | 562.3 |
| range 256 | 1 | 1,683.0 | 1,648.1 | -2.1% | 560.9 | 569.4 |
| range 256 | 8 | 7,204.1 | 7,069.9 | -1.9% | 1,000.1 | 1,020.6 |

Point lookups improve in both execution orders: hits by 3.8-10.3% and misses by
4.0-12.1%. C8 range-32 improves by 3.8-6.3%, and C1 range-64 by 6.0-8.2%.
Range-256 and C8 range-64 reverse direction, so no throughput gain is claimed
for those cases. The cache removes a fixed prepare cost, which is a smaller
share of queries returning more rows.

## Qualification

`builds.json` records exact source commit, toolchain, and SHA-256 hashes for all
six compared binaries. Full `query`, `pgwire`, and `gateway` suites pass with
loopback enabled for their TLS, durability, recovery, and full-stack tests.
Focused ownership tests cover exact reuse, type-hint mismatch, catalog-generation
invalidation, local-query exclusion, the SQL-size bound, and session teardown.
The benchmark itself exercises the shipped public PostgreSQL and distributed RF3
paths rather than an embedded or direct shard shortcut.
