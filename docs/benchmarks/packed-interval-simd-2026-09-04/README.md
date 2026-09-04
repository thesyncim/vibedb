# Packed integer interval count, Go 1.27 SIMD

This evidence compares base commit `de6c448d` with a storage-native fast path
for an ungrouped `COUNT(*)` whose predicate is an integer interval on one
field. The candidate recognizes the lower and upper comparisons, converts
them to a canonical half-open interval, and asks compact FOR storage to count
the range. For widths 7, 8, 10, and 16, the native kernels unpack each lane
once and apply both bounds in the same vector pass.

## Result

In this development measurement on an Apple M4 Max, base commit `de6c448d`
evaluates these queries through the generic row path in 2.17-2.38 ms for
16,384 rows. The candidate takes 2.41-3.48 us at the five-run median, a
625.9-979.0x end-to-end speedup, with 0 B/op and 0 allocations/op in every
candidate sample.

Provably empty or full-domain intervals still validate every target stream but
avoid scanning packed payloads, so SIMD and portable timings are similar.
Intervals that scan packed values measured 2.27-3.25x faster with NEON than
the portable packed scalar kernel in these sequential sample blocks. That
native ratio is directional rather than a statistically controlled claim;
the machine's power and thermal state were not controlled.

| shape | base `de6c448d` | SIMD | portable | end-to-end | SIMD / portable |
|---|---:|---:|---:|---:|---:|
| FOR10 empty | 2,227,635 ns | 2,410 ns | 2,572 ns | 924.3x | 1.07x |
| FOR10 sparse | 2,231,832 ns | 3,350 ns | 9,239 ns | 666.2x | 2.76x |
| FOR10 half | 2,361,290 ns | 3,073 ns | 6,970 ns | 768.4x | 2.27x |
| FOR10 full | 2,379,847 ns | 2,431 ns | 2,464 ns | 979.0x | 1.01x |
| FOR16 empty | 2,177,342 ns | 2,456 ns | 2,455 ns | 886.5x | 1.00x |
| FOR16 sparse | 2,175,732 ns | 3,476 ns | 11,296 ns | 625.9x | 3.25x |
| FOR16 half | 2,173,688 ns | 3,465 ns | 11,258 ns | 627.3x | 3.25x |
| FOR16 full | 2,315,316 ns | 2,406 ns | 2,481 ns | 962.3x | 1.03x |

## Method

- Hardware: Apple M4 Max, Darwin arm64.
- Toolchain: Go 1.27.0 with `GOEXPERIMENT=simd`; the fallback run uses
  `GOEXPERIMENT=nosimd`.
- Dataset: 16,384 durable-file rows, compact FOR width 10 or 16.
- Sampling: sequential five-sample blocks at 200 ms per shape with default
  `GOMAXPROCS`; the table reports the median `ns/op`.
- Base: `de6c448dc75838fbfa1b9ec63875ca8b0769190a`.
- Candidate: `4a42c581c5668172f80117a9719de0d20dc541c6`.
- The base checkout does not contain the candidate benchmark fixture. The
  exact copy step, fixture SHA-256, and resulting dirty base status are
  recorded in `commands.txt` and `metadata.txt`.
- The benchmark asserts one result row, the exact count, a full logical row
  population, no index/data-skipping path, and candidate interval-lane use.

Raw Go benchmark output is in `raw/`; exact commands are in `commands.txt`,
provenance is in `metadata.txt`, and machine-readable medians are in
`medians.tsv`.
