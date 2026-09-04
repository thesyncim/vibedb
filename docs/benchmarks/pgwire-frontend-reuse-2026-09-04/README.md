# Pgwire exact unnamed-Parse reuse

Continues from merged PR #144 (`90e75d60`). The gateway cache made repeated
distributed backend prepare cheap, but pgwire still classified SQL, rewrote
numbered parameters, rebuilt parameter metadata and RowDescription, and
allocated a new prepared wrapper before it closed the prior unnamed statement.

Pgwire now reuses that still-live statement when the incoming unnamed Parse has
the exact same SQL bytes and declared OID vector. The backend must explicitly
opt in. The distributed PostgreSQL backend opts in only for a live non-local
SELECT whose catalog generation is still current and whose session is not
closed or in a failed transaction. A hit closes every portal derived from the
replaced statement and resets its Bind high-water charge, preserving PostgreSQL
replacement semantics and session memory accounting.

There is no additional SQL-keyed map or retained plan. Pgwire keeps the active
statement it already owns. Named statements, local SELECTs, writes, changed SQL,
changed parameter OIDs, canceled Parse messages, failed transactions, and stale
catalog generations take the existing prepare path.

## Isolated frontend cost

Darwin arm64 / Apple M4 Max, five samples. The cold case uses a zero-cost fake
backend statement so these numbers isolate pgwire classification, numbered
parameter rewriting, parameter metadata, and output-column construction.

| Path | Median ns/op | Bytes/op | Allocations/op |
|---|---:|---:|---:|
| Cold pgwire frontend prepare | 332 | 600 | 7 |
| Exact live-statement reuse | 8.26 | 0 | 0 |

The exact reuse decision is about 40 times faster and removes all frontend
allocations. Raw output is in `frontend-benchmark.txt`.

## Distributed end-to-end results

The public PostgreSQL endpoint ran with three catalog, three ledger, and three
data processes plus the gateway, all under `GOMAXPROCS=2`. Every measured query
used extended unnamed Parse/Bind/Execute and traversed gateway routing and an
RF3 quorum read. The deterministic fixture held 244,608 rows, 956 times the
largest 256-row result page.

Each binary ran 6,000 point operations or 3,000 range operations per trial, 500
warmups, five repetitions, and 1 or 8 closed-loop clients. Every operation
checked returned values and order; each matrix verified the entire table before
and after. Across the latest-main and earlier exploratory pairs, all 400 timed
trials and 1,680,000 measured operations completed with zero errors. Cells below
are medians of five repetitions.

### Latest main then candidate

The primary comparison uses main `d9828e9b` and the rebased candidate. Each
binary recovered its separately seeded but deterministic 244,608-row RF3 root.

| Query | Clients | Main ops/s | Candidate ops/s | Throughput | Main p50 us | Candidate p50 us |
|---|---:|---:|---:|---:|---:|---:|
| point hit | 1 | 3,856.2 | 3,965.7 | +2.8% | 243.5 | 237.1 |
| point hit | 8 | 19,985.7 | 20,104.7 | +0.6% | 377.0 | 377.6 |
| point miss | 1 | 4,006.9 | 4,047.2 | +1.0% | 234.2 | 231.1 |
| point miss | 8 | 20,734.8 | 20,671.9 | -0.3% | 365.8 | 367.2 |
| range 32 | 1 | 3,302.2 | 3,414.2 | +3.4% | 284.7 | 275.6 |
| range 32 | 8 | 16,439.6 | 16,202.6 | -1.4% | 453.2 | 454.7 |
| range 64 | 1 | 2,910.2 | 2,973.8 | +2.2% | 323.9 | 317.7 |
| range 64 | 8 | 14,066.1 | 14,140.5 | +0.5% | 522.9 | 520.8 |
| range 256 | 1 | 1,601.2 | 1,869.7 | +16.8% | 539.7 | 507.3 |
| range 256 | 8 | 7,643.9 | 7,797.9 | +2.0% | 958.2 | 941.2 |

The final main range-256 repetitions degraded sharply, so their large
single-client delta is not treated as an optimization result.

### Latest candidate then main

The displayed columns remain main then candidate; execution order was reversed.

| Query | Clients | Main ops/s | Candidate ops/s | Throughput | Main p50 us | Candidate p50 us |
|---|---:|---:|---:|---:|---:|---:|
| point hit | 1 | 3,898.1 | 3,856.9 | -1.1% | 241.2 | 242.4 |
| point hit | 8 | 19,859.7 | 19,785.8 | -0.4% | 380.8 | 378.5 |
| point miss | 1 | 3,992.3 | 3,996.2 | +0.1% | 235.1 | 235.0 |
| point miss | 8 | 20,506.5 | 20,509.7 | 0.0% | 367.1 | 369.9 |
| range 32 | 1 | 3,313.6 | 3,371.5 | +1.8% | 284.1 | 280.0 |
| range 32 | 8 | 16,423.6 | 16,500.3 | +0.5% | 453.5 | 453.1 |
| range 64 | 1 | 3,185.0 | 2,942.0 | -7.6% | 291.7 | 320.6 |
| range 64 | 8 | 14,299.4 | 13,944.4 | -2.5% | 513.2 | 525.6 |
| range 256 | 1 | 1,777.3 | 1,814.4 | +2.1% | 537.1 | 523.9 |
| range 256 | 8 | 7,600.2 | 7,585.3 | -0.2% | 954.5 | 954.8 |

Latest-main whole-query changes are mostly within host noise and several cells
reverse direction. Range 32 at one client improves by 1.8-3.4% in both orders,
but the removed frontend work is only about 0.3 microseconds inside a
200-900-microsecond distributed query, so no broad RF3 throughput gain is
claimed. The supported result is the 40-times faster, zero-allocation frontend
Parse reuse path.

### Earlier exploratory pairs

The first two pairs were built from main `6cb255c0` before later main changes
landed. One fresh candidate-then-main pair showed 3.8% at range 32, 8.5% at
range 64, and 5.3% at range 256 for one client. Another main-then-candidate pair
used a restarted shared root and favored the candidate in nine of ten cells,
but that root later failed a cold restart with `seglog: bounds exceeded` and had
large within-run drift. These results are retained in `medians.tsv` and `raw/`
but are not current-main performance claims. `latest-main-medians.tsv` contains
the primary pair.

The earlier fresh baseline seed also exercised outcome-unknown recovery: a
64-row RF3 write timed out at row 194,496, the resume loader proved 194,560 rows
had committed, verified the full prefix, and continued at the next key. The
final read matrix again verified all 244,608 rows before and after.

## Qualification

`builds.json` records the exact main commit, Go toolchain, SIMD experiment, and
SHA-256 hashes for all binaries. Focused tests cover exact reuse, portal cleanup,
Bind and statement accounting, SQL/OID mismatch, backend opt-in, cancellation,
failed transaction state, and catalog-generation invalidation. Full `query`,
`pgwire`, and `gateway` suites run with loopback enabled for protocol, TLS,
durability, recovery, and distributed full-stack coverage.
