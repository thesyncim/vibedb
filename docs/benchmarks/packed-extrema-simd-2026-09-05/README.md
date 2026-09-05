# Packed integer extrema SIMD evidence

Development measurement for the durable unfiltered integer `MIN`/`MAX` lane.
The candidate is `affc989bf4fd9fb5f593b863c95bd65c566ffbde`; the immutable
baseline is `0951f0da7e61027807a06dbdc2a304fcb8e5e2ce`. Both revisions were
compiled with Go 1.27.0 and `GOEXPERIMENT=simd` on Darwin arm64. The benchmark
fixture hash is recorded in [metadata/benchmark-fixture-sha256.txt](metadata/benchmark-fixture-sha256.txt).

The fixture uses 16,384 durable rows created with `durable.CreateFromPrimary`,
64 document chunks, no overlays, and no index. FOR10 values are
`((row*73)&1023)-512`, with expected extrema `-512` and `511`. FOR16 values
are `((row*32749)&65535)-32768`, with expected extrema `-32768` and `32764`
over this exact prefix. Each case warms a reusable `Exec` and immutable
snapshot before timing. The candidate runs with `VIBEDB_EXPECT_EXTREMA=1`,
which asserts exact integer result cells, `RowsScanned=RowsTotal=16,384`,
`CoveringColumns=1`, `Workers=1`, `Batches=0`, no token or index work, and no
allocations. Baseline runs use the same source fixture with the generic path;
its baseline batch count is therefore allowed to differ.

## Durable query medians

Each block used five 250 ms samples with one benchmark CPU. Odd rounds ran base
then head; even rounds ran head then base. A second complete alternating block
was run with the same committed binaries. The table below uses the median of
all ten samples per role; no sample was dropped. The speedup column is
`baseline/candidate`.

| Case | Baseline ns/op | Candidate SIMD ns/op | Speedup | Candidate nosimd ns/op | SIMD/nosimd |
| --- | ---: | ---: | ---: | ---: | ---: |
| FOR10 `MIN` | 2,450,156.0 | 3,356.5 | 729.97x | 11,459 | 3.41x |
| FOR10 `MAX` | 2,452,657.0 | 3,362.5 | 729.41x | 11,483 | 3.42x |
| FOR10 `MIN` + `MAX` | 2,596,860.5 | 3,429.5 | 757.21x | 11,545 | 3.37x |
| FOR16 `MIN` | 2,357,648.5 | 3,422.5 | 688.87x | 12,614 | 3.69x |
| FOR16 `MAX` | 2,357,005.5 | 3,414.5 | 690.29x | 12,578 | 3.68x |
| FOR16 `MIN` + `MAX` | 2,508,627.5 | 3,499.0 | 716.96x | 12,689 | 3.63x |

Every timed output reports `0 B/op` and `0 allocs/op`. The complete paired
ten-sample medians and raw values are in [medians.tsv](medians.tsv),
[medians-raw.tsv](medians-raw.tsv), and [raw/](raw/). The first block is
retained as [medians-block1.tsv](medians-block1.tsv); the second block is in
[raw/repeat/](raw/repeat/). The five candidate nosimd samples are in
[nosimd-medians-raw.tsv](nosimd-medians-raw.tsv) and [raw/nosimd/](raw/nosimd/).

The requested existing-lane regression check is included in both paired
blocks. Two low-cost cases exceed the `+3%` median guard in the combined
ten-sample set:

| Existing case | Head vs baseline |
| --- | ---: |
| `BenchmarkCompactPrimaryStripePackedEquality8` | +6.51% |
| `BenchmarkFilePackedOrderedCountWide/sparse/lt` | +4.56% |

The no-regression gate therefore does not pass this combined run. These are the exact
medians from the retained samples; no statistical significance is claimed.
The extrema speedup and ARM64 SIMD-over-nosimd gates pass for this fixture.

## Provenance and reproduction

The paired binaries were compiled before timing from the exact revisions above
using `go test -run '^$' -c` for `./internal/storeio` and `./query`. The
candidate nosimd query binary used the candidate revision and the same fixture
with `GOEXPERIMENT=nosimd`. The exact compile commands, run commands, and
alternation order for both blocks are retained in
[metadata/compile-commands.txt](metadata/compile-commands.txt),
[benchmark-commands.tsv](benchmark-commands.tsv),
[benchmark-order.tsv](benchmark-order.tsv),
[benchmark-commands-repeat.tsv](benchmark-commands-repeat.tsv), and
[benchmark-order-repeat.tsv](benchmark-order-repeat.tsv). The script invocation
was:

```sh
GOEXPERIMENT=simd PACKED_SIMD_BENCHTIME=250ms PACKED_SIMD_REPETITIONS=5 \
  bash scripts/bench/run-packed-simd.sh <evidence-dir> \
  <base-storeio.test> <head-storeio.test> <base-query.test> <head-query.test>
```

The host was an Apple M4 Max MacBook Pro with 16 cores running Darwin arm64.
Run settings and toolchain details are in [metadata/platform-and-run.txt](metadata/platform-and-run.txt),
[metadata/go-env.txt](metadata/go-env.txt), and [metadata/cpu.txt](metadata/cpu.txt).
Power and thermal state were uncontrolled. The raw artifacts contain no test
binaries or build caches.

Focused SIMD and nosimd storeio/query tests passed. The focused race/checkptr
run passed, and Go 1.27.0 Linux AMD64 `GOAMD64=v1` test-binary cross-compiles
passed for both packages. `bash -n scripts/bench/run-packed-simd.sh` passed;
`shellcheck` and `actionlint` were unavailable on the host. The host cannot
execute the Linux AMD64 binary, so the AMD64 AVX2 speed gate remains a native
CI responsibility. The packed SIMD workflow includes the extrema fixture in
the paired base copy, focused tests, benchmark selection, and AVX2-disabled
parity gate; the regular CI regexes include its portable and fallback tests.

The workflow and raw harness deliberately retain measurements rather than
turning host timing into a universal CI threshold. The AMD64 native lane must
establish its required SIMD speed gate independently.

## Source

- [`query/file_packed_extrema_bench_test.go`](../../../query/file_packed_extrema_bench_test.go): durable FOR10/FOR16 fixtures, exact result/stat assertions, and benchmarks.
- [`query/file_packed_extrema_test.go`](../../../query/file_packed_extrema_test.go): durable correctness, fallback, and zero-allocation tests.
- [`scripts/bench/run-packed-simd.sh`](../../../scripts/bench/run-packed-simd.sh): alternating paired benchmark harness.
- [`.github/workflows/packed-simd.yml`](../../../.github/workflows/packed-simd.yml): native paired and AVX2-disabled CI lane.
