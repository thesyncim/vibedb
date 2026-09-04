# Packed integer interval count, Go 1.27 SIMD

This evidence compares current main with a storage-native fast path for an
ungrouped `COUNT(*)` whose predicate is an integer interval on one field. The
candidate recognizes the lower and upper comparisons, converts them to a
canonical half-open interval, and asks compact FOR storage to count the range.
For widths 7, 8, 10, and 16, the native kernels unpack each lane once and apply
both bounds in the same vector pass.

## Result

On an Apple M4 Max, current main evaluates these queries through the generic
row path in 2.17-2.38 ms for 16,384 rows. The candidate takes 2.43-3.54 us at
the five-run median, a 613.9-975.7x end-to-end speedup, with 0 B/op and 0
allocations/op in every candidate sample.

Intervals that are provably empty or cover the full integer domain normalize
to a constant count and therefore have the same SIMD and portable timings.
Intervals that scan packed values are 2.26-3.20x faster with NEON than the
portable packed scalar kernel.

| shape | current main | SIMD | portable | end-to-end | SIMD / portable |
|---|---:|---:|---:|---:|---:|
| FOR10 empty | 2,227,635 ns | 2,431 ns | 2,436 ns | 916.3x | 1.00x |
| FOR10 sparse | 2,231,832 ns | 3,392 ns | 8,194 ns | 658.0x | 2.42x |
| FOR10 half | 2,361,290 ns | 3,098 ns | 6,998 ns | 762.2x | 2.26x |
| FOR10 full | 2,379,847 ns | 2,439 ns | 2,440 ns | 975.7x | 1.00x |
| FOR16 empty | 2,177,342 ns | 2,478 ns | 2,480 ns | 878.7x | 1.00x |
| FOR16 sparse | 2,175,732 ns | 3,535 ns | 11,287 ns | 615.5x | 3.19x |
| FOR16 half | 2,173,688 ns | 3,541 ns | 11,345 ns | 613.9x | 3.20x |
| FOR16 full | 2,315,316 ns | 2,489 ns | 2,492 ns | 930.2x | 1.00x |

## Method

- Hardware: Apple M4 Max, Darwin arm64.
- Toolchain: Go 1.27.0 with `GOEXPERIMENT=simd`; the fallback run uses
  `GOEXPERIMENT=nosimd`.
- Dataset: 16,384 durable-file rows, compact FOR width 10 or 16.
- Sampling: five benchmark repetitions at 200 ms per shape; the table reports
  the median `ns/op`.
- Base: `de6c448dc75838fbfa1b9ec63875ca8b0769190a`.
- Candidate: `39093608066794b932f9b1b93281e4ff129b894b`.
- The benchmark asserts one result row, the exact count, a full logical row
  population, no index/data-skipping path, and candidate interval-lane use.

Raw Go benchmark output is in `raw/`; exact commands are in `commands.txt` and
machine-readable medians are in `medians.tsv`.
