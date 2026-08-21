# Allocation regression gate

`go run ./bench/gate` compares the working tree with a base revision on the
same machine.

## Pass and fail rules

The gate fails when:

- A gated `allocs/op` value increases.
- A gated `B/op` value increases by more than 5 percent.

The gate does not compare `ns/op`. Machine load can change time measurements
without changing allocation behavior.

## Baseline selection

Use the default merge-base with `main`:

```bash
go run ./bench/gate
```

Select an exact base:

```bash
go run ./bench/gate -base <commit>
```

CI sets `GATE_BASE` to the pull-request base commit.

The tool checks out the base in a temporary Git worktree. It does not stash or
reset the primary worktree.

## Curated benchmarks

The source in `bench/gate/main.go` is authoritative. The current set is:

| Package | Benchmark | Fixed time | Gates |
| --- | --- | ---: | --- |
| `store/durable` | `BenchmarkFileStoreCreateFromFloor` | `5x` | `B/op` |
| `store/durable` | `BenchmarkUnifiedScanAllBytesLowCardinality` | `5x` | allocations and bytes |
| `store/durable` | `BenchmarkUnifiedScanAllBytesHighCardinality` | `5x` | allocations and bytes |
| `internal/storeio` | `BenchmarkCompactPrimaryStripePointRead` | `20x` | allocations and bytes |
| `query` | `BenchmarkStoreQueryIndexedProjection` | `20x` | allocations and bytes |
| `query` | `BenchmarkStoreQueryIndexedCount` | `20x` | allocations and bytes |
| `query` | `BenchmarkApplyKernelLeftExactMemoizationWarm` | `20x` | allocations and bytes |

Bulk-build `allocs/op` is measured but not gated because it is fractional at
the fixed iteration count. Its `B/op` value remains gated.

## Exit status

- 0 means that all gated metrics passed.
- 1 means that the gate ran and found a regression.
- 2 means that the gate could not run or parse its results.

If a deliberate design change needs a new allocation, change the curated
policy and record the measurement evidence in the same pull request. Do not
loosen a gate without that evidence.
