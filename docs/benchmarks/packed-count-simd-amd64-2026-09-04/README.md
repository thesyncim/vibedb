# AMD64 packed equality count SIMD measurements

This development measurement records the initial qualification of the packed
equality counters in PR 136, using Go 1.27.1 on GitHub Actions Linux/AMD64.
The runner exposed four CPUs of an AMD EPYC 9V74
80-Core Processor and reported AVX2 in its CPU feature set. The exact base is
commit `953981b69afd18b1b6adc558b586fc183531a8bb`. The compiled candidate is
the synthetic pull-request merge head
`3bc2fcc1d34ff270797a64966bb5d7a48123ca17`; the pull-request head recorded by
the run is `d0397bd4dee841b3de80ff5bc1844a3060f2370d`. The CI metadata in
[`metadata/`](metadata/) is authoritative for the checkout, runner, and Go
environment. The reviewed kernel source was
`e62792f32863d413336488d76658ea9fa82fe0af`, including its review fixes.

The tables apply to this initial revision pair. They predate the later
integration of updated `main`, including the independent changes from PRs 133
and 137. Subsequent integration qualification has its own revisions and CI
artifacts.

Both revisions used `GOEXPERIMENT=simd` and `GOAMD64=v1`. The four benchmark
binaries were compiled before timing, then run with `-test.cpu=1`
(`GOMAXPROCS=1`), five 250 ms
samples per side, and alternating base/head order. Power and thermal
conditions were not controlled. The exact command and order manifests are
[`benchmark-commands.tsv`](benchmark-commands.tsv) and
[`benchmark-order.tsv`](benchmark-order.tsv); the complete raw output is in
[`raw/`](raw/). These are warmed immutable, unindexed equality COUNT and
packed-counter measurements on this one runner. They do not make cold-storage,
concurrent, SQL, other-CPU, or global-database claims. Short-input correctness
is covered by parity tests; this AMD64 run did not measure tiny-call latency.

The base uses scalar dispatch. It already has direct scalar handling for the
8- and 16-bit calls; the candidate adds the AMD64 AVX2 counters for widths 7,
8, 10, and 16. The tables therefore measure the incremental AMD64 change over
that base, rather than the ARM64 measurements in the
[wide packed-count report](../packed-count-simd-wide-2026-09-04/README.md).
The 7- and 8-bit cases are dictionary fixtures, and the 10- and 16-bit cases
are frame-of-reference integer fixtures; those encodings describe the measured
inputs rather than an intrinsic meaning of a width.

## Fixtures

The raw stream and primary-stripe fixtures contain 4,096 values or records.
The durable query fixture contains 16,384 records with keys `row-%07d`. The
narrow label and integer fixtures use dictionary7 and FOR10, with generated
query counts of 128 and 16. The wide label uses the 17-byte spelling
`c%016x`, `id=(row*73)&255`, and
`(uint64(id)+1)*0x9e3779b97f4a7c15`; it selects dictionary8 and gives 64 query
matches. The wide integer is
`((row*32749)&65535)-32768`; it selects FOR16 and gives one query match in the
16,384-row fixture. The benchmark files verify exact counts with fixture setup
outside timing. The durable query cases check their result and scan statistics
during warmup and after timing.

## Raw counter and stripe results

The following are medians of the five samples in each role's raw files.
Speedup is `baseline/candidate`; change is
`(candidate/baseline)-1`, so a negative change means lower candidate latency.
All operations report zero bytes and zero allocations per operation.

| Operation, 4,096 rows | Base ns/op | AVX2 candidate ns/op | Speedup | Change |
| --- | ---: | ---: | ---: | ---: |
| Stream packed equality, width 7 | 3,462 | 294.6 | 11.75× | −91.49% |
| Stream spelling equality, width 7 | 3,908 | 735.2 | 5.32× | −81.19% |
| Stream packed equality, width 10 | 3,304 | 290.1 | 11.39× | −91.22% |
| Stream integer equality, width 10 | 3,219 | 287.7 | 11.19× | −91.06% |
| Stream packed equality, width 8 | 2,897 | 41.09 | 70.50× | −98.58% |
| Stream spelling equality, width 8 | 3,907 | 985.9 | 3.96× | −74.77% |
| Stream packed equality, width 16 | 5,132 | 123.0 | 41.72× | −97.60% |
| Stream integer equality, width 16 | 5,131 | 127.0 | 40.40× | −97.52% |
| Primary stripe packed equality, width 7 | 4,063 | 900.9 | 4.51× | −77.83% |
| Primary stripe packed equality, width 10 | 3,480 | 476.0 | 7.31× | −86.32% |
| Primary stripe packed equality, width 8 | 4,066 | 1,216 | 3.34× | −70.09% |
| Primary stripe packed equality, width 16 | 5,294 | 322.8 | 16.40× | −93.90% |

These are within-run comparisons of the counter, stream, and stripe layers. They
are evidence for this CPU and fixture set, not a universal speedup claim.

## Durable query results

Each query is `Select(Count()).Where(Cmp(field, Eq, value))` over a durable
snapshot created with `durable.CreateFromPrimary` and reopened without overlays
or declared indexes. A reused executor runs with one worker. Every case returns
one result row, scans all 16,384 rows through the token filter, and reports no
fallback rows, index work, candidate work, or data skipping. The following
medians use the same five alternating 250 ms samples and zero bytes and zero
allocations per operation.

| Durable query, 16,384 rows | Base ns/op | AVX2 candidate ns/op | Speedup | Change |
| --- | ---: | ---: | ---: | ---: |
| Dictionary7 equality COUNT, 128 matches | 20,994 | 8,297 | 2.53× | −60.48% |
| FOR10 integer equality COUNT, 16 matches | 18,565 | 6,506 | 2.85× | −64.96% |
| Dictionary8 equality COUNT, 64 matches | 21,353 | 9,640 | 2.22× | −54.85% |
| FOR16 integer equality COUNT, 1 match | 25,933 | 6,112 | 4.24× | −76.43% |

The query rows include durable graph traversal, template resolution, packed
counting, and count aggregation. They exclude SQL parsing, client protocols,
cold I/O, and concurrent writes. The query benchmark is the existing
`BenchmarkFilePackedEqualityCount` / `BenchmarkFilePackedEqualityCountWide`
fixture; no fixture source changes were made for this AMD64 evidence run.

## ARM64 control

The same initial revision pair was also measured on a GitHub Actions Linux/ARM64
runner exposing four Neoverse-N2 CPUs, using Go 1.27.1 and `GOEXPERIMENT=simd`.
The ARM64 counter source was unchanged between the two revisions. Five
alternating 250 ms samples per side, with `-test.cpu=1`, produced these query
medians; every sample reported zero bytes and zero allocations per operation.

| Durable query, 16,384 rows | Base ns/op | Candidate ns/op | Change |
| --- | ---: | ---: | ---: |
| Dictionary7 equality COUNT | 8,939 | 8,944 | +0.06% |
| FOR10 integer equality COUNT | 6,109 | 6,106 | −0.05% |
| Dictionary8 equality COUNT | 8,228 | 8,248 | +0.24% |
| FOR16 integer equality COUNT | 6,281 | 6,274 | −0.11% |

All four medians remain within 0.25% of the baseline, consistent with the
unchanged ARM64 implementation. This is a control, not an additional ARM64
speedup claim. The ten query sample files and essential runner/revision metadata
are preserved under [`arm64-control/`](arm64-control/). Its command and order
manifests cover the complete CI job; only query raw files are retained here.

## Validation and fallback

The enabled focused checks require AVX2, report `AVX2=true`, and pass the direct
AVX2 parity test. The packed storeio tests also pass with race detection and
`-gcflags=all=-d=checkptr=2`; the complete output is in
[`packed-race-checkptr-tests.txt`](packed-race-checkptr-tests.txt). The focused
storeio output is in
[`focused-storeio-tests.txt`](focused-storeio-tests.txt). The width-selection
and query integration checks include exact codec choices, exact counts, and
the expected full-scan statistics.

The fallback run sets `GODEBUG=cpu.avx2=off` while retaining
`GOAMD64=v1 GOEXPERIMENT=simd`. It reports `AVX2=false`; the AVX2-only direct
parity test is skipped as expected, while scalar parity, width-selection, and
query integration tests pass. Its exact command, environment, and output are
preserved in [`avx2-disabled/`](avx2-disabled/). This verifies scalar dispatch in
a v1 binary with the runtime AVX2 bit disabled.

## Reproduction

The paired run used the repository's benchmark script after compiling
identical benchmark files into test binaries for the selected base and
candidate revisions. The [setup commands](prepare-commands.txt) and benchmark
manifests preserve the CI invocations and alternation. Direct package commands
use the same selection:

```sh
GOEXPERIMENT=simd GOAMD64=v1 go test ./internal/storeio -run '^$' \
  -bench '^BenchmarkCompact(Stream(Packed|Spelling|Integer)|PrimaryStripePacked)Equality(7|8|10|16)$' \
  -benchmem -benchtime=250ms -count=1 -cpu=1

GOEXPERIMENT=simd GOAMD64=v1 go test ./query -run '^$' \
  -bench '^BenchmarkFilePackedEqualityCount(Wide)?$' \
  -benchmem -benchtime=250ms -count=1 -cpu=1
```

The [paired native workflow](../../../.github/workflows/packed-simd.yml)
records revision selection, `GOEXPERIMENT=simd`, `GOAMD64=v1`, focused checks,
and uploaded raw artifacts. The [initial report](../packed-count-simd-2026-09-04/README.md)
contains the original ARM64 width7/10 scalar comparison, and the
[wide ARM64 report](../packed-count-simd-wide-2026-09-04/README.md) contains
the ARM64 width8/16 extension. Portable parity remains in regular CI.

## Source map

- [`internal/storeio/compact_stream_codec_packed_equal_bench_test.go`](../../../internal/storeio/compact_stream_codec_packed_equal_bench_test.go): raw and stripe fixtures, exact results, and benchmarks.
- [`query/file_packed_count_bench_test.go`](../../../query/file_packed_count_bench_test.go): durable query fixture, exact counts, scan statistics, and benchmarks.
- [`internal/storeio/compact_stream_codec_simd_amd64.go`](../../../internal/storeio/compact_stream_codec_simd_amd64.go): Go 1.27 AMD64 AVX2 counters and runtime feature guard.
- [`internal/storeio/compact_stream_codec_simd_arm64.go`](../../../internal/storeio/compact_stream_codec_simd_arm64.go): corresponding ARM64 NEON counters.
- [`docs/simd.md`](../../simd.md): implementation overview and architecture gating.
