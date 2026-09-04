# SIMD in compressed equality counts

Development implementation: Go 1.27 SIMD can evaluate equality directly over
packed column values. A durable unindexed `COUNT(*) WHERE field = value` can
count matching dictionary IDs or integer offsets without reconstructing each
JSON document. The storage format and exact comparison rules stay unchanged.

The initial kernels target 7-bit dictionary IDs and 10-bit integer offsets.
These represent dictionaries containing 65–128 values and integer spans
requiring 513–1024 representable offsets. Both widths already have scalar
specializations; the vector implementation accelerates the same operation.

Each vector iteration gathers overlapping byte pairs, shifts and masks them
into independent values, compares those values with the needle, and accumulates
the matches. The implementation reads only complete vectors inside the packed
input and leaves the remaining rows to the scalar counter. Padding bits do not
contribute to the result. Reductions are bounded to prevent lane overflow.

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

The packed counters select ARM64 SIMD on Go 1.27 with the experiment enabled.
Other builds retain the scalar implementation. The version guard deliberately
requires revalidation when the experimental API changes in a later Go release.

The [local measurements](benchmarks/packed-count-simd-2026-09-04/README.md)
record the paired scalar/SIMD results, exact fixtures, raw samples, and
validation scope.

Further candidates should follow the same evidence requirement: demonstrate
that a real workload spends time in the kernel, preserve its scalar oracle,
and measure the integrated operation. Adjacent packed widths and fused
predicates over compressed values are candidates for separate measurements.
They are not implied by this implementation.

## Source map

- `internal/storeio/compact_stream_codec.go`: packed counters and exact stream comparisons.
- `internal/storeio/compact_stream_codec_simd_arm64.go`: guarded vector loads and bounded reductions.
- `internal/storeio/compact_stream_codec_dispatch_scalar.go`: portable dispatch.
- `internal/storeio/compact_primary_stripe.go`: compressed stripe count operations.
- `internal/storeio/primary_graph_unified_filter.go`: durable equality count integration.
- `internal/storeio/compact_stream_codec_test.go`: all-width and exact-number oracles.
- `.github/workflows/ci.yml`: SIMD and portable parity on ARM64 and AMD64.
