# SIMD kernels

[Documentation](README.md) / [Design](design/README.md) · [Stability](status.md)

VibeDB uses Go 1.27 experimental SIMD (`go1.27 && !go1.28 && goexperiment.simd`)
on ARM64 (NEON) and AMD64 (AVX2 with runtime feature check; no AVX-512) for
contiguous typed kernels where exact scalar parity holds. `make` enables
`GOEXPERIMENT=simd`; raw Go builds and other Go versions/architectures fall
back to identical scalar code.

## Vectorized paths

| Path | Eligible queries / ops | Widths / lanes | Fallback |
| --- | --- | --- | --- |
| Packed equality count | Durable unindexed `COUNT(*) WHERE field = val` | 7/8-bit dict, 10/16-bit FOR | Scalar per-leaf or generic executor |
| Packed ordered count | Durable unindexed `COUNT(*) WHERE field < / <= / > / >= int` | 10/16-bit FOR | Generic executor if any leaf declines |
| Packed extrema | Durable unfiltered `MIN`/`MAX` over one FOR integer path | 10/16-bit FOR (`UMIN`/`UMAX`, `VPMIN*`/`VPMAX*`) | Generic executor if any leaf declines |
| Blocked Bloom insert | Join Bloom filter signature insertion (`query/join_bloom.go`) | 8 signature words in registers (probes stay scalar) | Fused scalar insert |
| Full-text fold & BM25 | `internal/tin` ASCII case folding (16-byte) and multi-doc BM25 (`Float64x2`) | NEON / AVX2 | Scalar fold & BM25 |

Exact decimal `SUM` and `AVG` are never vectorized. Every packed vector loop
only loads complete in-bounds vectors and handles tails in scalar code so
padding bits never affect counts.

## Full-text fold and BM25 kernels

ASCII folding is bit-exact across SIMD and scalar. BM25 is gated against scalar
at `1e-12` relative tolerance (FMA rounding differs in the last bits); within a
single build, multi-document scoring is batch-order independent
(`TestBM25BatchingIndependent`). Single-document `ScoreSingle` (SQL `SCORE()`)
uses scalar math.

## Testing and verification

```sh
GOEXPERIMENT=simd go build ./...
GOEXPERIMENT=nosimd go test ./internal/storeio ./internal/tin
GOEXPERIMENT=nosimd go test ./query -run '^TestFilePacked.*$'
GODEBUG=cpu.avx2=off GOEXPERIMENT=simd go test ./internal/storeio ./internal/tin
```

CI qualifies SIMD and scalar parity on AMD64 and ARM64 in `ci.yml` and runs
paired native AVX2 checks in [`.github/workflows/packed-simd.yml`](../.github/workflows/packed-simd.yml).

## Source map

- Packed codecs: [internal/storeio/compact_stream_codec.go](../internal/storeio/compact_stream_codec.go), `compact_stream_codec_simd_*.go`, [primary_graph_unified_filter.go](../internal/storeio/primary_graph_unified_filter.go)
- Join Bloom: [query/join_bloom.go](../query/join_bloom.go)
- Full-text kernels: [internal/tin/fold_wide.go](../internal/tin/fold_wide.go), [bm25_wide.go](../internal/tin/bm25_wide.go), [bm25.go](../internal/tin/bm25.go)
- Benchmarks & scripts: [scripts/bench/run-packed-extrema-avx2.sh](../scripts/bench/run-packed-extrema-avx2.sh)
