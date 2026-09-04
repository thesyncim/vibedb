# SIMD in compressed equality counts

Development implementation: Go 1.27 SIMD can evaluate equality directly over
packed column values. A durable unindexed `COUNT(*) WHERE field = value` can
count matching dictionary IDs or integer offsets without reconstructing each
JSON document. The storage format and exact comparison rules stay unchanged.

The packed counter set covers 7-, 8-, 10-, and 16-bit values. The measured
fixtures use 7- and 8-bit dictionary IDs, representing dictionaries of up to
128 and 256 entries, and 10- and 16-bit frame-of-reference integer offsets,
representing spans of up to 1,024 and 65,536 values. The initial measurements
cover the 7-bit dictionary and 10-bit integer fixtures; the wide fixtures add
the 8-bit dictionary and 16-bit integer cases while using the same exact scalar
comparison rules. Other compact stream kinds can use these widths when their
encoding is valid.

The 7- and 10-bit paths gather overlapping byte pairs, shift and mask them
into independent values, compare those values with the needle, and accumulate
the matches. The improved 10-bit path uses four 16-byte loads for 32 rows over
an exact 40-byte span; the final load starts at byte 24 and overlaps the prior
loads, so it needs no 46-byte lookahead. Its independent accumulators flush at
bounded row chunks. The 8-bit path compares byte-aligned values directly, and
the 16-bit path compares byte-aligned little-endian `uint16` lanes directly.
Every vector loop admits only complete loads inside the logical packed input
and leaves remaining rows to the scalar counter. Padding bits do not
contribute to the result, and reductions are bounded to prevent lane overflow.

This is a useful SIMD boundary because the data is already contiguous and
typed. JSON field routing, pointer-heavy index traversal, and row materialization
can dominate other paths before arithmetic becomes significant. Exact decimal
`SUM` and `AVG` also cannot be replaced by reordered floating-point reductions.

Repository commands enable SIMD by default. Embedding applications using raw
Go commands must explicitly select the experiment:

```sh
GOEXPERIMENT=simd go build ./...
GOEXPERIMENT=nosimd go test ./internal/storeio -run 'Test(CountCompact|CompactPacked|CompactStream|CompactPrimaryStripe)'
```

The packed counters select the ARM64 NEON implementation only when the build is
Go 1.27 ARM64 with `GOEXPERIMENT=simd`. The implementation is gated by
`go1.27 && !go1.28 && goexperiment.simd && arm64`; Go 1.27 portable builds,
`GOEXPERIMENT=nosimd`, later Go releases, and other architectures retain the
scalar dispatch. The version guard deliberately requires revalidation when the
experimental API changes in a later Go release.

The [initial packed-count measurements](benchmarks/packed-count-simd-2026-09-04/README.md)
record the paired scalar/SIMD results for widths 7 and 10, exact fixtures, raw
samples, and validation scope. The [wide packed-count
measurements](benchmarks/packed-count-simd-wide-2026-09-04/README.md) report the
paired dictionary8 and FOR16 fixtures and retain their raw evidence. The [paired
native evidence workflow](../.github/workflows/packed-simd.yml) records the
exact base and candidate revisions, SIMD-focused checks, and raw benchmark
artifacts for that comparison. Portable parity remains in the regular CI job.

Each width follows the same evidence requirement: demonstrate that a real
workload reaches the kernel, preserve its scalar oracle, and measure the
integrated operation. The wide README is the source for those width 8 and 16
measurements; the implementation description alone makes no timing claim.

## Source map

- `internal/storeio/compact_stream_codec.go`: packed counters and exact stream comparisons.
- `internal/storeio/compact_stream_codec_simd_arm64.go`: guarded vector loads, width-specific unpacking, and bounded reductions.
- `internal/storeio/compact_stream_codec_dispatch_scalar.go`: portable dispatch.
- `internal/storeio/compact_primary_stripe.go`: compressed stripe count operations.
- `internal/storeio/primary_graph_unified_filter.go`: durable equality count integration.
- `internal/storeio/compact_stream_codec_test.go`: all-width and exact-number oracles.
- `.github/workflows/packed-simd.yml`: paired native base/candidate evidence lane.
- `.github/workflows/ci.yml`: SIMD and portable parity on ARM64 and AMD64.
