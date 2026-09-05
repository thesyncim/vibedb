# SIMD in compressed equality, ordered counts, and extrema

Development implementation: Go 1.27 SIMD can evaluate equality and integer
ordering directly over packed column values, and reduce integer `MIN`/`MAX`
over eligible FOR streams. A durable unindexed `COUNT(*) WHERE field = value`
or integer ordering predicate can count packed values without reconstructing
each JSON document, while an eligible durable integer extrema query can reduce
the packed values directly. The storage format and exact comparison rules stay
unchanged.

The packed counter set covers 7-, 8-, 10-, and 16-bit values. The measured
fixtures use 7- and 8-bit dictionary IDs, representing dictionaries of up to
128 and 256 entries, and 10- and 16-bit frame-of-reference integer offsets,
representing spans of up to 1,024 and 65,536 values. The initial measurements
cover the 7-bit dictionary and 10-bit integer fixtures; the wide fixtures add
the 8-bit dictionary and 16-bit integer cases while using the same exact scalar
comparison rules. Other compact stream kinds can use these widths when their
encoding is valid.

The ordered lane supports ungrouped durable `COUNT(*)` with one integer
literal and `<`, `<=`, `>`, or `>=`. It reports a storage result only when
every present resolved token stream is FOR encoded and can answer the exact
integer ordering without an unsafe overflow or container case. If any leaf
declines, the storage scan discards partial progress and the executor runs the
generic path for the whole operation. The codec uses one exact scalar
less-than counter and derives the other operators with overflow-safe threshold
and complement math; the same bounded width-specific counters serve the Go
1.27 ARM64 and AMD64 SIMD dispatches.

The extrema lane supports unfiltered, ungrouped durable `MIN` and `MAX` over
one named non-root numeric path. It admits an exact storage shape only when
every present resolved stream is FOR encoded and safe for integer reduction;
missing, null, and nonnumeric values retain the aggregate's existing skip
semantics. An unsupported leaf or unsafe stream atomically declines the
storage result so the generic executor remains authoritative. Accepted scans
return ordinary integer result cells and report one covering column, a full
row scan, and no token or index work.

The 7- and 10-bit paths gather overlapping byte pairs, unpack independent
values, compare those values with the needle, and accumulate the matches.
ARM64 uses lane shifts and masks; AMD64 uses multiplication by powers of two
followed by a literal shift to discard unwanted bits without AVX-512.
The improved 10-bit path uses four 16-byte loads for 32 rows over
an exact 40-byte span; the final load starts at byte 24 and overlaps the prior
loads, so it needs no 46-byte lookahead. Its independent accumulators flush at
bounded row chunks. The 8-bit path compares byte-aligned values directly, and
the 16-bit path compares byte-aligned little-endian `uint16` lanes directly.
Every vector loop admits only complete loads inside the logical packed input
and leaves remaining rows to the scalar counter. Padding bits do not
contribute to the result, and reductions are bounded to prevent lane overflow.
The extrema kernels use the same bounded unpacking and tails with unsigned
lane `UMIN`/`UMAX` on ARM64 and `VPMIN*`/`VPMAX*` on AMD64 before restoring the
signed FOR offset. Short inputs retain scalar counting, and the scalar
dispatch remains available when the experiment or runtime feature gate is
disabled.

This is a useful SIMD boundary because the data is already contiguous and
typed. JSON field routing, pointer-heavy index traversal, and row materialization
can dominate other paths before arithmetic becomes significant. Exact decimal
`SUM` and `AVG` also cannot be replaced by reordered floating-point reductions.

Repository commands enable SIMD by default. Embedding applications using raw
Go commands must explicitly select the experiment:

```sh
GOEXPERIMENT=simd go build ./...
GOEXPERIMENT=nosimd go test ./internal/storeio -run 'Test(CountCompact|CompactPacked|CompactInteger(Ordered|Interval|Extrema)|CompactStream|CompactPrimaryStripe)'
GOEXPERIMENT=nosimd go test ./query -run '^TestFilePacked(EqualityCount|OrderedCount|IntegerInterval|IntegerExtrema).*$'
```

The packed counters select Go 1.27 SIMD implementations on ARM64 and AMD64
when `GOEXPERIMENT=simd` is enabled. ARM64 uses NEON under
`go1.27 && !go1.28 && goexperiment.simd && arm64`. AMD64 uses AVX2 under
`go1.27 && !go1.28 && goexperiment.simd && amd64` after the runtime AVX2
feature check. Native CI qualifies AMD64 binaries built with `GOAMD64=v1`,
including scalar dispatch with `GODEBUG=cpu.avx2=off` when the runtime AVX2
bit is disabled.
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
artifacts for those native comparisons. The [AMD64 packed-count measurements](benchmarks/packed-count-simd-amd64-2026-09-04/README.md)
report the initial AVX2 counter and durable query qualification on a Go 1.27.1
AMD64 runner, with exact revision pairs and an unchanged ARM64 control.
The [ordered packed-count measurements](benchmarks/packed-order-simd-2026-09-04/README.md)
record local FOR10 and FOR16 `<`, `<=`, `>`, and `>=` durable COUNT evidence,
including the nosimd control and raw samples.
The [packed integer extrema measurements](benchmarks/packed-extrema-simd-2026-09-05/README.md)
record durable FOR10 and FOR16 `MIN`, `MAX`, and combined `MIN`+`MAX` query
evidence, including paired baseline/SIMD samples and the candidate nosimd
control. Native AMD64 SIMD qualification remains a CI lane; the local report
contains ARM64 measurements only.
The [packed extrema layout follow-up](benchmarks/packed-extrema-simd-layout-fix-2026-09-05/README.md)
records the source relocation that restores the pre-existing equality8 kernel
alignment, the focused ordered wide sparse/less-than check, and fixed-candidate
SIMD/nosimd extrema controls. The [final latest-main qualification](benchmarks/packed-extrema-simd-final-2026-09-05/README.md)
records the paired release measurements that close both existing-lane
regression checks and describes the native AMD64 SIMD/scalar gate.
Portable parity remains in the regular CI job.

Each width follows the same evidence requirement: demonstrate that a real
workload reaches the kernel, preserve its scalar oracle, and measure the
integrated operation. The linked reports tie the measurements to their
architecture, fixtures, and exact revision pairs.

## Source map

- `internal/storeio/compact_stream_codec.go`: packed counters and exact stream comparisons.
- `internal/storeio/compact_stream_codec_simd_arm64.go`: guarded vector loads, width-specific unpacking, and bounded reductions.
- `internal/storeio/compact_stream_codec_simd_amd64.go`: guarded AVX2 loads, width-specific unpacking, and bounded reductions.
- `internal/storeio/compact_stream_codec_simd_extrema.go`: extrema front end and scalar packed reductions.
- `internal/storeio/compact_stream_codec_simd_extrema_arm64.go`: ARM64 extrema kernels kept after the pre-existing packed kernels.
- `internal/storeio/compact_stream_codec_simd_extrema_amd64.go`: AMD64 extrema reductions and kernels kept after the pre-existing packed kernels.
- `scripts/bench/run-packed-extrema-avx2.sh`: same-binary AMD64 AVX2 enabled/disabled extrema speed qualification with a paired median gate.
- `internal/storeio/compact_stream_codec_dispatch_scalar.go`: portable dispatch.
- `internal/storeio/compact_primary_stripe.go`: compressed stripe count operations.
- `internal/storeio/primary_graph_unified_filter.go`: durable packed equality, ordered count, and extrema integration.
- `internal/storeio/compact_stream_codec_packed_extrema_test.go`: packed extrema oracle, safety, and dispatch tests.
- `query/file_packed_extrema_test.go`: durable extrema correctness and fallback tests.
- `query/file_packed_extrema_bench_test.go`: FOR10/FOR16 durable extrema benchmark fixtures.
- `query/file_packed_order_bench_test.go`: durable ordered COUNT fixtures and benchmarks.
- `internal/storeio/compact_stream_codec_packed_order_test.go`: ordered packed-counter oracle and dispatch tests.
- `internal/storeio/compact_stream_codec_test.go`: all-width and exact-number oracles.
- `.github/workflows/packed-simd.yml`: paired native base/candidate evidence lane.
- `.github/workflows/ci.yml`: SIMD and portable parity on ARM64 and AMD64.
