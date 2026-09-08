# Batch slot preservation: September 8 improvement round

Existing-key PUT batches now preserve row slots in indexed leaves. When their
exact-index projections remain unchanged, they avoid rebuilding the indexes.
Changed projections use the established structural exact rebase, which keeps
device-write volume at baseline. Unique-index validation, certified replay,
and atomic publication keep their existing behavior. Inserts, deletes and
mixed topology retain their established placement path.

## Measured update results

Arithmetic means of two fixed-count runs on Apple M4 Max, Go 1.27, portable
backend, 256 MiB resident budget. Each fixture contains 20,000 documents,
256-byte shared strings, and two indexes: `(tenant,a,shared)` and
`(tenant,b,shared)`. Each run performs 20 batches of 64 existing-key updates.
Timing includes Update, final Flush, Snapshot materialization, and a second
Flush to make that physical cut durable. Fixture generation is excluded.

| Shared values | Changed field | Baseline µs/doc | Candidate µs/doc | Less time | Device B/doc, baseline → candidate |
| --- | --- | ---: | ---: | ---: | ---: |
| 8 | Shared by both indexes | 3,041.46 | 2,275.30 | 25.2% | 12,205 → 12,205 |
| 8 | Other indexed field | 3,076.58 | 2,228.09 | 27.6% | 12,387 → 12,387 |
| 8 | Unrelated field | 3,076.59 | 237.90 | 92.3% / 12.9× | 12,320 → 380.8 |
| 1,024 | Shared by both indexes | 3,358.96 | 2,503.72 | 25.5% | 16,669 → 16,669 |
| 1,024 | Other indexed field | 3,371.84 | 2,517.73 | 25.3% | 16,477 → 16,477 |
| 1,024 | Unrelated field | 3,364.02 | 396.17 | 88.2% / 8.5× | 16,586 → 3,405 |

Unrelated-field updates use 79.5–96.9% fewer device-write bytes. Their timed
Update-plus-Flush allocations fall from roughly 253,000–265,000 to 9 per
64-document batch. Changed-field batches still rebuild exact indexes and
retain substantial allocations; this round does not claim to eliminate that
remaining work. Device-write bytes are not final file-size savings.

## Read and insert controls

Prepared reads retain zero allocations. Two 500 ms runs per case produced:

| Read control | Baseline | Candidate | Time change |
| --- | ---: | ---: | ---: |
| Rotating exact hits/misses, 8 shared values | 411.7 ns | 413.3 ns | +0.4% |
| Rotating exact hits/misses, 1,024 shared values | 411.9 ns | 411.85 ns | ~0% |
| Primary point read, 32 dictionary strings | 256.65 ns | 256.5 ns | −0.1% |
| Primary point read, 128 dictionary strings | 262.4 ns | 261.6 ns | −0.3% |
| Primary scan, 32 dictionary strings | 121.1 ns/doc | 119.4 ns/doc | −1.4% |
| Primary scan, 128 dictionary strings | 128.4 ns/doc | 128.85 ns/doc | +0.4% |

The insert control starts with one row and inserts 1,280 rows; it is not an
insertion measurement at 20,000 rows. With final Flush included, its mean
time changes from 169.24 to 168.99 µs/doc for 8 shared values (−0.1%), and
169.58 to 176.02 µs/doc for 1,024 values (+3.8%). Device bytes are identical
in both cases. These short controls establish neither a throughput win nor a
tail-latency guarantee for inserts.

## Why the final policy is narrower than the first experiment

The first experiment kept changed-term deltas for ordinary batches too.
Repeated batches filled the bounded exact arenas and forced intermediate
checkpoints, increasing indexed-field device writes by 2–7×. That version is
not included. The final policy retains the cheap path for unchanged exact
projections and rebases changed projections, preserving the write volume
while removing row-placement work. Certified replay explicitly retains its
original delta and pressure behavior.

## Compression status and limits

This round enables no new durable compression. The rejected per-string LZ4
path remains removed. Pack codecs, authenticated pack pages, inventory
helpers and their tests are present as foundations; the production writer
still uses the existing exact leaves. The partial integration is preserved
separately and excluded from this passing checkpoint.

Before pack integration can ship it still needs consistent multi-member
packing in every builder, one decode per pack at Open, complete logical-route
and physical-inventory ownership validation, online-compaction integration,
and bounded sparse-pack cleanup. Existing 68–86% space figures on selected
overlap fixtures are projections, not measured final database sizes.

There is no new 10M-row, RF3, cold-cache, p99, long-running churn, or
CockroachDB comparison in this round. Two repetitions are a small engineering
screen, not a statistical performance qualification.

## Reproduction and evidence

Validation: the new slot/projection/uniqueness/fallback tests and selected
crash/replay regressions pass. Full `internal/storeio` tests pass (14 seconds).
Targeted durable race tests covering batches, indexed atomic publication,
snapshot isolation and unique-swap replay pass (149 seconds).

Baseline production is `bf4e8b44556e8f431d4743259c1fa5f61bc31c53`; the benchmark
checkout at `6a853cf417c2717874840f2af60d9ce4ac6fe173` has the identical
production tree. Copy the public benchmark and overlap fixture from this PR
into that checkout before compiling with the project cache wrapper.

Final `store_file_primary_batch.go` SHA-256:
`8fbd19b35a3b06e80ca0e3af1cacb90b8db8f322f612a7a894c030aa8e4edd8c`.
Identical public benchmark harness SHA-256:
`b551743c18a2958aedb2f0dc8ea23bb58073fad781866c400a8cd297b2b42616`.

Run each compiled baseline/candidate durable test binary separately:

```sh
<binary> -test.run '^$' -test.bench '^BenchmarkExactPackExistingUpdate$' -test.benchtime 20x -test.count 2 -test.benchmem
<binary> -test.run '^$' -test.bench '^BenchmarkExactPackResidentEqualityRotating$|^BenchmarkStringCompression(GetRaw|ScanAllBytes)$/distinct=(32|128)$' -test.benchtime 500ms -test.count 2 -test.benchmem
<binary> -test.run '^$' -test.bench '^BenchmarkExactPackBatchInsert$' -test.benchtime 20x -test.count 2 -test.benchmem
```

Raw results:

- [Baseline updates](benchmarks/storage-round-2026-09-08/final-base-writes.txt)
  and [candidate updates](benchmarks/storage-round-2026-09-08/final-candidate-writes.txt).
- [Baseline reads](benchmarks/storage-round-2026-09-08/final-base-reads.txt)
  and [candidate reads](benchmarks/storage-round-2026-09-08/final-candidate-reads.txt).
- [Baseline inserts](benchmarks/storage-round-2026-09-08/final-base-inserts.txt)
  and [candidate inserts](benchmarks/storage-round-2026-09-08/final-candidate-inserts.txt).
