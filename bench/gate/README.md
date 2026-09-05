# Allocation regression gate

[Performance guide](../../docs/performance.md)

> **Development status:** The curated benchmark set and thresholds are internal
> development policy. They can change at any commit and are not a stable public
> performance contract.

The gate compares allocation measurements from the current checkout with a Git
base built in a temporary detached worktree on the same machine. It keeps no
checked-in baseline and never gates elapsed time.

The gate's baseline and candidate subprocesses default to
`GOEXPERIMENT=simd`. Set `GOEXPERIMENT=nosimd` for a portable comparison;
the selected experiment is printed before measurements.

## Run it

From the repository root, compare with the local merge base of `HEAD` and
`main`:

```bash
go run ./bench/gate
```

Compare with an exact locally available revision:

```bash
go run ./bench/gate -base <commit>
```

Keep the temporary base worktree for inspection:

```bash
go run ./bench/gate -base <commit> -keep
```

The tool does not fetch. The default therefore uses the caller's current local
`main`, which may be stale. It does not stash, reset, or otherwise rewrite the
primary worktree.

Pull-request CI is slightly different: GitHub checks out the synthetic test
merge and sets `GATE_BASE` to the pull request's exact base SHA. That is a
test-merge-versus-base comparison, not necessarily contributor-head versus its
merge base.

## Pass rules

The gate fails closed when a required row or metric is missing. For gated rows:

- any increase in `allocs/op` fails; and
- `B/op` may increase by at most 5%.

`BenchmarkFileStoreCreateFromFloor` reports its fractional `allocs/op` but gates
only `B/op`. The command parses Go benchmark timing as part of the input format,
but does not compare or report `ns/op` in its summary.

| Package | Benchmark | Fixed iterations | Gated metrics |
| --- | --- | ---: | --- |
| `store/durable` | `BenchmarkFileStoreCreateFromFloor` | `5x` | `B/op` |
| `store/durable` | `BenchmarkUnifiedScanAllBytesLowCardinality` | `5x` | allocations and bytes |
| `store/durable` | `BenchmarkUnifiedScanAllBytesHighCardinality` | `5x` | allocations and bytes |
| `internal/storeio` | `BenchmarkCompactPrimaryStripePointRead` | `20x` | allocations and bytes |
| `query` | `BenchmarkStoreQueryIndexedProjection` | `20x` | allocations and bytes |
| `query` | `BenchmarkStoreQueryIndexedCount` | `20x` | allocations and bytes |
| `query` | `BenchmarkApplyKernelLeftExactMemoizationWarm` | `20x` | allocations and bytes |

`bench/gate/main.go` is authoritative if this table and the implementation
diverge.

## Exit status

| Status | Meaning |
| ---: | --- |
| 0 | Every gated allocation metric passed |
| 1 | The comparison completed and found a regression |
| 2 | Git setup, build, benchmark execution, or result parsing failed |

## Claim boundary

This is one sequential current/base sample with fixed iteration counts. It has
no timing threshold, randomization, repeated statistical trial, clean-tree
requirement, or artifact upload. A pass means only that the curated allocation
policy passed on that invocation. It is not a throughput, latency, memory-peak,
or release-readiness result.

When a deliberate design change needs more allocation, update the source policy
and include the before/after evidence in the same change. Do not loosen the
threshold to hide a regression.
