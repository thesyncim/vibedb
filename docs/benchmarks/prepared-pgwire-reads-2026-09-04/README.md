# Retained pgwire semantic prepare

Continues after PR #139 (`f9902b47`). PostgreSQL extended-protocol execution
previously performed typed semantic preparation twice for every statement: once
when pgwire parsed the statement, then again in `Executor.queryWithProfile` on
every bind. The pgwire statement already owns the compiled semantic statement
and its exact parameter domains.

The distributed pgwire read path now uses a private executor entry point that
accepts the retained statement's parameter count. Every bind still validates
parameter arity, parameter payloads, and SQL type metadata. Public `Query`,
`Exec`, `ExecBatch`, transaction, and explain callers continue through complete
SQL parse and typed semantic validation. Physical routing still uses the pinned
catalog snapshot's prepared-plan cache and every query still executes through
the RF3 transport and quorum-read path.

## Isolated validation cost

Darwin arm64 / Apple M4 Max, Go 1.27, five samples:

| Validation | Median ns/op | Bytes/op | Allocations/op |
|---|---:|---:|---:|
| Reparse and semantic prepare | 5,567 | 12,784 | 54 |
| Retained pgwire statement | 8.49 | 0 | 0 |

This benchmark uses a typed indexed point query with an ordered LIMIT. It
measures only admission validation, not query execution or RF3 latency. Raw
output is in `validation-bench.txt`.

## Distributed end-to-end results

The public PostgreSQL path ran against a persistent 244,608-row, three-replica
RF3 fixture: three catalog, three ledger, and three data processes plus the
gateway. This is 956 times the largest 256-row page. Every operation checked its
returned data and ordering, and the harness verified the complete table before
and after each matrix. All 120 comparison trials completed with zero errors.

Each trial used 6,000 point operations or 3,000 range operations, 500 warmups,
three repetitions, and 1 or 8 closed-loop clients. The protocol was extended
unnamed parse/bind/execute with one autocommit statement per operation. Each
cell is the median of three repetitions.

### Pair 1: baseline then candidate

| Query | Clients | Baseline ops/s | Candidate ops/s | Throughput | Baseline p50 us | Candidate p50 us |
|---|---:|---:|---:|---:|---:|---:|
| point hit | 1 | 2,256.9 | 3,115.5 | +38.0% | 320.3 | 295.9 |
| point hit | 8 | 8,868.6 | 12,878.7 | +45.2% | 796.7 | 554.4 |
| point miss | 1 | 2,552.0 | 2,990.0 | +17.2% | 330.5 | 299.5 |
| point miss | 8 | 12,290.8 | 13,304.6 | +8.2% | 594.0 | 547.4 |
| range 32 | 1 | 2,327.3 | 2,439.0 | +4.8% | 374.9 | 367.1 |
| range 32 | 8 | 9,410.2 | 10,248.9 | +8.9% | 764.6 | 693.1 |
| range 64 | 1 | 2,215.8 | 2,347.4 | +5.9% | 411.8 | 389.9 |
| range 64 | 8 | 8,717.0 | 9,353.1 | +7.3% | 824.5 | 772.2 |
| range 256 | 1 | 1,398.5 | 1,328.5 | -5.0% | 673.3 | 690.9 |
| range 256 | 8 | 5,037.4 | 5,193.1 | +3.1% | 1,442.1 | 1,375.7 |

### Pair 2: candidate then baseline

The displayed columns remain baseline then candidate. Execution order was
reversed.

| Query | Clients | Baseline ops/s | Candidate ops/s | Throughput | Baseline p50 us | Candidate p50 us |
|---|---:|---:|---:|---:|---:|---:|
| point hit | 1 | 2,638.6 | 2,009.1 | -23.9% | 333.3 | 362.4 |
| point hit | 8 | 10,933.8 | 10,824.9 | -1.0% | 657.4 | 672.5 |
| point miss | 1 | 2,705.5 | 2,934.6 | +8.5% | 324.4 | 304.8 |
| point miss | 8 | 6,387.1 | 9,841.3 | +54.1% | 931.6 | 681.0 |
| range 32 | 1 | 2,236.5 | 1,886.4 | -15.7% | 401.4 | 426.2 |
| range 32 | 8 | 5,645.3 | 10,199.6 | +80.7% | 1,175.6 | 716.6 |
| range 64 | 1 | 999.3 | 2,396.8 | +139.8% | 592.3 | 370.9 |
| range 64 | 8 | 5,448.3 | 9,902.6 | +81.8% | 1,203.8 | 719.0 |
| range 256 | 1 | 1,106.4 | 1,562.8 | +41.3% | 745.5 | 596.2 |
| range 256 | 8 | 4,207.5 | 5,750.9 | +36.7% | 1,672.7 | 1,247.1 |

The second pair suffered sustained host/controller stalls late in the baseline
run, while early candidate point-hit and range-32 C1 trials also stalled. Its
large ratios establish direction for point misses and 64-row ranges but do not
measure a credible magnitude. The cleaner first pair supports an 8% floor for
point misses, roughly 6-7% for 64-row ranges, 9% for 32-row ranges at C8, and 3%
for 256-row ranges at C8. Point hits and single-client 32/256-row ranges reverse
between orders, so no repeatable end-to-end gain is claimed for those cells.

## Fixture and qualification notes

The original 500,000-row RF3 fixture completed a full baseline matrix with zero
errors and full-table verification. On the following restart, catalog member 3
failed cold recovery with `seglog: bounds exceeded`. The exact baseline and
candidate binaries both fail at the same point, proving that failure belongs to
the persisted fixture rather than this patch. No candidate timings were accepted
from that fixture. The baseline report and both recovery logs are retained.

The comparison therefore used an earlier 244,608-row RF3 fixture. A public SQL
`COUNT(*)` and the benchmark harness's complete content verifier confirmed its
exact row count before measurement. The same persisted files and process limits
were used for both binaries. `builds.json` records the exact baseline commit,
Go/SIMD toolchain, and hashes of all compared binaries.

Local `query`, `pgwire`, and `gateway` package suites pass. The pgwire and
gateway runs used authorized loopback because their TLS, failover, durability,
and full-stack tests create ephemeral localhost listeners. Focused tests verify
that the retained-statement path accepts canonical binds and continues to reject
wrong arity, invalid parameter payloads, and invalid SQL type metadata.
