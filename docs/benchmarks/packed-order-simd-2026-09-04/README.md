# Packed ordered FOR COUNT SIMD evidence

Development measurement collected on 2026-09-04 from an Apple M4 Max,
Darwin ARM64, with Go 1.27.0. The baseline is the clean commit
`ede2b4f34e9693831f3e9adca1840fd0ccbbf19a`. The candidate uses that same
commit plus the uncommitted ordered COUNT implementation in the candidate
worktree. The identical query fixture was copied byte-for-byte into both
worktrees; its SHA-256 is
`0d8cd31e80435fdf492630bd14b5c7ac8946058ca4b98bc4bcda14c5cc2517cf`.

Both revisions used `GOEXPERIMENT=simd`. The query test binaries were built
before timing and ran with `GOMAXPROCS=1`, `-test.cpu=1`, one reused executor,
and five 250 ms samples per side. Each paired round ran the baseline before the
candidate. Power and thermal conditions were uncontrolled. Every sample
passed the fixture's exact-result and full-scan assertions; candidate samples
also had to prove that all rows used the ordered token lane. Every sample
reported `0 B/op` and `0 allocs/op`. The raw files, command history,
environment metadata, and median calculations are retained in [`raw/`](raw/),
[`commands.txt`](commands.txt), [`metadata.txt`](metadata.txt),
[`medians.tsv`](medians.tsv), and [`nosimd.tsv`](nosimd.tsv).

## Workload and scope

The fixture creates a clean immutable snapshot with
`durable.CreateFromPrimary`, then runs an unindexed `Select(Count()).Where`
query over 16,384 rows. Each query is an integer ordering predicate on `n`:
`<`, `<=`, `>`, or `>=`. The executor uses one worker and scans all rows. The
ordered storage lane is eligible only for the FOR integer column; dictionary,
root, empty-key, fractional-literal, and other declined shapes continue
through the generic executor and are covered by the focused tests.

The FOR10 values are `((row*73)&1023)-512`, with a 10-bit signed offset. The
sparse threshold is `-500`, giving counts 192, 208, 16,176, and 16,192 for
`<`, `<=`, `>`, and `>=`; the zero threshold gives 8,192, 8,208, 8,176, and
8,192. The FOR16 values are `((row*32749)&65535)-32768`, with a 16-bit
signed offset. Over this 16,384-row prefix, threshold `-32760` gives 2, 2,
16,382, and 16,382; threshold zero gives 8,192 for each operator. Counts
come from the generated rows and are checked by the benchmark fixture.

This is a warmed local measurement of the durable query path on one ARM64
machine. It excludes SQL parsing, client protocols, cold I/O, concurrent
writes, and other CPUs. The baseline comparison includes generic query
execution, so its large latency reflects the ordered lane's integrated
storage and query path; it is not a kernel-only speed claim.

## Paired baseline and candidate medians

The speedup column is `baseline/candidate`; negative change means lower
candidate latency. Values are medians of the five samples. The query source
was identical in both test binaries.

| Query case, 16,384 rows | Baseline ns/op | Candidate SIMD ns/op | Speedup | Change |
| --- | ---: | ---: | ---: | ---: |
| FOR10 sparse `<`, 192 matches | 2,701,658 | 3,212 | 841.11x | -99.88% |
| FOR10 sparse `<=`, 208 matches | 2,713,644 | 3,096 | 876.50x | -99.89% |
| FOR10 sparse `>`, 16,176 matches | 2,736,733 | 3,109 | 880.26x | -99.89% |
| FOR10 sparse `>=`, 16,192 matches | 2,739,015 | 3,118 | 878.45x | -99.89% |
| FOR10 half `<`, 8,192 matches | 2,720,288 | 3,103 | 876.66x | -99.89% |
| FOR10 half `<=`, 8,208 matches | 2,727,898 | 3,116 | 875.45x | -99.89% |
| FOR10 half `>`, 8,176 matches | 2,733,729 | 3,100 | 881.85x | -99.89% |
| FOR10 half `>=`, 8,192 matches | 2,732,348 | 3,099 | 881.69x | -99.89% |
| FOR16 sparse `<`, 2 matches | 2,607,346 | 3,075 | 847.92x | -99.88% |
| FOR16 sparse `<=`, 2 matches | 2,610,025 | 2,963 | 880.87x | -99.89% |
| FOR16 sparse `>`, 16,382 matches | 2,655,671 | 3,074 | 863.91x | -99.88% |
| FOR16 sparse `>=`, 16,382 matches | 2,651,278 | 3,087 | 858.85x | -99.88% |
| FOR16 half `<`, 8,192 matches | 2,641,702 | 3,606 | 732.59x | -99.86% |
| FOR16 half `<=`, 8,192 matches | 2,628,277 | 3,618 | 726.44x | -99.86% |
| FOR16 half `>`, 8,192 matches | 2,629,877 | 3,491 | 753.33x | -99.87% |
| FOR16 half `>=`, 8,192 matches | 2,640,055 | 3,619 | 729.50x | -99.86% |

Across these cases the paired baseline-to-SIMD medians range from 726.44x to
881.85x. These figures describe this fixture and machine only; they are not a
timing threshold or a general database performance guarantee.

## SIMD control

The candidate was also run with `GOEXPERIMENT=nosimd` using the same source,
fixture, worker count, and five 250 ms rounds. The candidate SIMD
median was 2.25x–2.35x faster for FOR10, 2.25x–2.34x faster for sparse FOR16,
and 3.16x–3.25x faster for half-selective FOR16. This control isolates the
increment from the Go 1.27 SIMD counter within the candidate implementation;
it is separate from the baseline-to-candidate table.

The production dispatch retains scalar counting for `GOEXPERIMENT=nosimd`,
later Go releases, unsupported architectures, and runtime paths without the
required SIMD feature. The ordered query guard still declines atomically when
the complete resolved token stream cannot answer the exact integer ordering
predicate as FOR data.

## Reproduction

The original run compiled the identical fixture into both package test
binaries before timing. The ordered query benchmark selection is:

```sh
GOTOOLCHAIN=go1.27.0 GOEXPERIMENT=simd go test -c ./query \
  -o /absolute/evidence-directory/query-candidate-simd.test

GOMAXPROCS=1 VIBEDB_EXPECT_ORDERED=1 \
  /absolute/evidence-directory/query-candidate-simd.test \
  -test.run '^$' \
  -test.bench '^BenchmarkFilePackedOrderedCount(Wide)?$' \
  -test.benchtime 250ms -test.count 1 -test.cpu 1 -test.benchmem
```

Repeat the binary invocation five times for each role, with the baseline
followed by the candidate in each paired round. The repository's paired native
workflow uses
[`scripts/bench/run-packed-simd.sh`](../../../scripts/bench/run-packed-simd.sh)
to compile identical equality and ordered benchmark sources, alternate the
roles, validate benchmark metrics, and preserve raw artifacts. Its focused
checks include the ordered query tests, packed store tests, and the AMD64
AVX2-disabled scalar dispatch check.

## Source map

- [`query/file_packed_order_bench_test.go`](../../../query/file_packed_order_bench_test.go): durable FOR10/FOR16 fixtures, exact results and statistics, and ordered COUNT benchmarks.
- [`internal/storeio/compact_stream_codec_packed_order_test.go`](../../../internal/storeio/compact_stream_codec_packed_order_test.go): scalar differential, boundary, dispatch, and canary coverage for ordered packed counters.
- [`docs/simd.md`](../../simd.md): implementation guards, fallback behavior, and linked evidence reports.
