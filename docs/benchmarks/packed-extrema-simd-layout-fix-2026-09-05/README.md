# Packed SIMD layout regression follow-up

This development measurement isolates a source layout fix for the packed SIMD
implementation. The new extrema front end, scalar counters, and ARM64/AMD64
extrema kernels were moved into later files so they no longer sit between the
pre-existing equality kernels. The fixed candidate is
`a7fdae07b5625d1155666b158144c92b008bac7a`; the immutable baseline is
`0951f0da7e61027807a06dbdc2a304fcb8e5e2ce`. Both revisions used Go 1.27.0 with
`GOEXPERIMENT=simd` on Darwin arm64. The existing full extrema report remains
at [packed-extrema-simd-2026-09-05](../packed-extrema-simd-2026-09-05/README.md).

The relocation preserves the storage and query semantics. It changes only file
placement: `compact_stream_codec_simd_extrema.go` contains the generic extrema
front end and scalar counters, while the two arch tagged extrema files contain
the corresponding kernels and AMD64 reductions.

## Layout result

`nm -arch arm64` reports the following `countCompactPacked8EqualNEON`
addresses. The baseline and fixed candidate are both aligned at offset
`0x10` within a 64-byte block; the pre-fix candidate used for the address-only
comparison was at offset `0x30`.

| Binary | Revision | Address | Address mod 64 |
| --- | --- | ---: | ---: |
| baseline | `0951f0da` | `0x100224510` | `0x10` |
| pre-fix candidate | prior extrema layout | `0x100225170` | `0x30` |
| fixed candidate | `a7fdae07` | `0x100224890` | `0x10` |

The complete symbol output and command are in
[metadata/symbols.txt](metadata/symbols.txt). The pre-fix binary is retained
only as an address reference; it is not used for the timing table.

## Paired timing

Each requested lane used five complete rounds. Every round ran the baseline
storeio binary, baseline query binary, fixed candidate storeio binary, and
fixed candidate query binary in that order. Each sample used one benchmark CPU,
`-test.benchtime=2s`, `-test.count=1`, and `-test.benchmem`. The benchmark
assertions passed in every sample and reported `0 B/op` and `0 allocs/op`.

| Benchmark | Baseline samples (ns/op) | Fixed candidate samples (ns/op) | Baseline median | Candidate median | Candidate vs baseline |
| --- | --- | --- | ---: | ---: | ---: |
| `BenchmarkCompactPrimaryStripePackedEquality8` | 506.6, 511.9, 511.0, 514.7, 514.1 | 514.2, 511.1, 517.7, 517.0, 512.1 | 511.9 | 514.2 | +0.45% |
| `BenchmarkFilePackedOrderedCountWide/sparse/lt` | 2767, 2778, 2800, 2793, 2974 | 2796, 2965, 2991, 2981, 2991 | 2793 | 2981 | +6.73% |

The equality8 layout drift is absent in this run: the fixed candidate is within
`+0.45%` of the baseline. The ordered wide sparse/less-than case still exceeds
the repository's `+3%` regression guard in this sample set, so the complete
no-regression gate remains open. No statistical significance is claimed from
this focused timing check.

## Extrema control

These are fixed-candidate controls rather than a baseline comparison. They use
the same durable 16,384-row FOR10/FOR16 fixtures and the combined `MIN` plus
`MAX` query. The SIMD and nosimd binaries both asserted exact integer cells,
full row scans, one covering column, one worker, zero batches, and no token or
index work.

| Query | Candidate SIMD samples (ns/op) | Candidate nosimd samples (ns/op) | SIMD median | nosimd median | SIMD / nosimd |
| --- | --- | --- | ---: | ---: | ---: |
| `min-max/FOR10` | 3460, 3478, 3468, 3480, 3481 | 11578, 11613, 11601, 11617, 11599 | 3478 | 11601 | 3.34x |
| `min-max/FOR16` | 3532, 3515, 3514, 3515, 3526 | 12765, 12592, 12797, 12539, 12771 | 3515 | 12765 | 3.63x |

All control samples reported `0 B/op` and `0 allocs/op`. These controls show
that moving the extrema implementation does not remove its measured native
SIMD advantage on this host; they do not qualify AMD64 performance.

## Provenance and reproduction

The paired binaries were freshly compiled before timing from the exact
baseline and fixed candidate worktrees:

- baseline: `0951f0da7e61027807a06dbdc2a304fcb8e5e2ce`
- fixed candidate: `a7fdae07b5625d1155666b158144c92b008bac7a`
- platform: Apple M4 Max, Darwin arm64, Go 1.27.0
- run settings: `GOMAXPROCS=1`, `GOEXPERIMENT=simd`, five samples, 2 seconds per paired lane sample

The exact compile commands, benchmark commands, round order, environment,
fixture hashes, and validation commands are retained under [metadata/](metadata/).
The raw benchmark outputs are preserved under [raw/](raw/). No binaries or
build caches are included in this report. Power and thermal state were
uncontrolled.

Focused SIMD and nosimd storeio/query tests passed after the relocation;
`gofmt` and `git diff --check` passed. The host is arm64, so AMD64 AVX2
performance remains a native CI responsibility.

## Source

- [`internal/storeio/compact_stream_codec_simd_extrema.go`](../../../internal/storeio/compact_stream_codec_simd_extrema.go): generic extrema front end and scalar counters.
- [`internal/storeio/compact_stream_codec_simd_extrema_arm64.go`](../../../internal/storeio/compact_stream_codec_simd_extrema_arm64.go): ARM64 extrema kernels.
- [`internal/storeio/compact_stream_codec_simd_extrema_amd64.go`](../../../internal/storeio/compact_stream_codec_simd_extrema_amd64.go): AMD64 extrema reductions and kernels.
- [`query/file_packed_extrema_bench_test.go`](../../../query/file_packed_extrema_bench_test.go): durable extrema control fixture.
- [`query/file_packed_order_bench_test.go`](../../../query/file_packed_order_bench_test.go): ordered wide sparse/less-than fixture.
