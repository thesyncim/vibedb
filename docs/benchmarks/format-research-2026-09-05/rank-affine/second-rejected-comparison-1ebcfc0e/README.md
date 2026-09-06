# Second rejected rank-affine paired comparison

This is the second independent rejected diagnostic for candidate
`1ebcfc0e8653324411a94042a90e45d3da8b4c20` against base
`3a7d3b15dfc82579c7080b4acf95a12383718737`. It is separate from the
[initial comparison](../initial-comparison/README.md) and any future
qualification campaign; their samples must not be pooled.

The remote [run 33992274581](https://github.com/thesyncim/vibedb/actions/runs/33992274581)
completed six alternating paired trials using the same benchmark source,
200 ms windows, and `GOMAXPROCS=1`. The physical-node RF3 qualification job
passed, but this comparison remains rejected because material `ns/op`
regressions remain.

Observed paired medians and bootstrap intervals were:

- `storeio/BenchmarkCompactRankFormat/low/Scan ns/op`: 800,856.5 -> 832,156.0, ratio 1.0383, 95% interval [1.0348, 1.0413].
- `storeio/BenchmarkCompactRankFormat/late-miss/Scan ns/op`: 319,226.5 -> 336,372.5, ratio 1.0543, 95% interval [1.0490, 1.0595].
- `storeio/BenchmarkCompactRankFormat/single/Build ns/op`: 3,002,424.5 -> 3,137,148.5, ratio 1.0412, 95% interval [1.0336, 1.0479].
- `durable/BenchmarkUnifiedScanAllBytesLowCardinality ns/op`: 33,319,952.0 -> 34,334,128.5, ratio 1.0306, 95% interval [1.0283, 1.0325].
- `durable/BenchmarkUnifiedScanAllBytesHighCardinality ns/op`: 54,905,806.0 -> 55,230,692.5, ratio 1.0093, 95% interval [1.0043, 1.0161].
- `durable/BenchmarkFileStoreBatchWrite/put ns/op`: 18,199.0 -> 18,805.5, ratio 1.0367, 95% interval [1.0111, 1.0707].
- `durable/BenchmarkFileStoreBatchWrite/update/batch=10 ns/op`: 1,820,520.0 -> 1,909,973.0, ratio 1.0373, 95% interval [1.0071, 1.0660].

The independent warmed Put check also failed the one-allocation contract.
This directory therefore records rejection and makes no performance
qualification claim.

The current-head CI failures are preserved as stable links:

- [unit ubuntu-latest durable](https://github.com/thesyncim/vibedb/actions/runs/33992230330/job/101376382730)
- [unit ubuntu-latest process](https://github.com/thesyncim/vibedb/actions/runs/33992230330/job/101376382723)
- [unit ubuntu-24.04-arm durable](https://github.com/thesyncim/vibedb/actions/runs/33992230330/job/101376382655)
- [distributed-clock-fault-matrix](https://github.com/thesyncim/vibedb/actions/runs/33992230320/job/101376382663)

The raw six-pair logs are in `primary-format-comparison/`.
`comparison.json` contains paired medians and intervals, `pairs.json` retains
each paired sample, and `environment.json` records runner and trial metadata.
The build identity files are in `primary-format-build/`.
