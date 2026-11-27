# Benchmark methods

[Documentation](../README.md) / Benchmarks

Use [performance evidence](../performance.md) to pick a benchmark harness and
check publication rules. This directory holds reusable benchmark methods and
CI-referenced control fixtures; dated raw captures belong in CI artifacts or
git history rather than the working tree.

## Comparison methods

| Method | Covers |
| --- | --- |
| [RF3 SQL comparison](crdb-sql-method.md) | Matched-durability RF3 SQL comparison against CockroachDB |
| [Physical-node SQL comparison](fused-node-sql-method.md) | Multi-group physical-node SQL comparison protocol |
| [Online scale-in/out qualification](seamless-scale-in-out-method.md) | 3 → 4 → 3 scaling under open-loop traffic (`seamless-scale-in-out.yml`) |

## Harnesses

- **Allocation gate (`bench/gate`):** `go run ./bench/gate` rejects `allocs/op`
  regressions and `>5%` `B/op` regressions against base.
- **Competitive suite (`bench/competitive`):** See
  [bench/competitive/README.md](../../bench/competitive/README.md),
  [COVERAGE.md](../../bench/competitive/COVERAGE.md), and
  [RESULTS.md](../../bench/competitive/RESULTS.md).
- **RF3 chaos (`bench/rf3chaos`):** External-process fault injection harness
  ([bench/rf3chaos/README.md](../../bench/rf3chaos/README.md)).

## CI fixtures

`format-research-2026-09-05/rank-affine/checksum-alignment-control/` is a small
control fixture consumed by `.github/workflows/fused-node-rf3.yml` when the
optional primary-format comparison job is triggered manually.
