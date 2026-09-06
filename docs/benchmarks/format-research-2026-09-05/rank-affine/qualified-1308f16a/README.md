# Primary-format qualification at 1308f16a

Candidate: `1308f16abd614bfaa93718cf495a0ac80cbe251d`. Baseline: `02b75683f227470d6d198274f532bf39d5fa417d`.

[CI run 34020001179](https://github.com/thesyncim/vibedb/actions/runs/34020001179) passed the full primary/query comparison and physical RF3 qualification. The downloaded artifact directories below preserve raw samples, paired ratios, host/build metadata, binary digests and process evidence.

Linux AMD64, Go 1.27, `GOMAXPROCS=1`, six runs per arm, 200 ms benchmark windows, alternating baseline/candidate order. Percent changes are paired geometric means of candidate/baseline latency; negative is faster. Intervals are the recorded paired bootstrap 95% intervals.

| Benchmark | Time change | 95% interval |
| --- | ---: | ---: |
| `BenchmarkUnifiedGetRaw` | -22.30% | -23.21% to -21.53% |
| `BenchmarkUnifiedPrimaryReplace` | -8.91% | -10.26% to -7.51% |
| `BenchmarkUnifiedScanAllBytesHighCardinality` | -3.16% | -3.50% to -2.80% |
| `BenchmarkUnifiedScanAllBytesLowCardinality` | -3.11% | -3.74% to -2.71% |
| `BenchmarkFileStoreBatchWrite/put` | +0.58% | +0.18% to +1.00% |

No paired `ns/op` geometric mean regressed by more than 3% in this run. Batch puts increased by 0.58%; this small regression is retained rather than called a win. Large affine-query gains are workload-specific and must not be generalized to ordinary documents.

This is regression and engineering evidence against VibeDB main. It is not an endorsed cross-engine result, a claim about other hardware, or evidence for later commits. Subsequent WAL, split-recovery and transaction-retirement fixes need their own correctness/fault qualification; this run does not measure their latency.

Earlier failed or unqualified trials remain in adjacent directories. In particular, ordinary-read regressions discovered before renderer isolation are not removed from the record.

## Retained artifacts

- [Comparison and paired samples](primary-format-1308f16abd614bfaa93718cf495a0ac80cbe251d/primary-format-comparison/comparison.json)
- [Execution environment](primary-format-1308f16abd614bfaa93718cf495a0ac80cbe251d/primary-format-comparison/environment.json)
- [Exact baseline revision](primary-format-1308f16abd614bfaa93718cf495a0ac80cbe251d/primary-format-build/base-revision.txt)
- [Physical RF3 summary](fused-node-rf3-1308f16abd614bfaa93718cf495a0ac80cbe251d/summary.tsv)
