# Allocation regression gate

[Performance guide](../../docs/performance.md)

Compares `allocs/op` and `B/op` against a Git base built in a temporary
detached worktree on the same machine (`GOEXPERIMENT=simd` default; set
`GOEXPERIMENT=nosimd` for portable). Never gates elapsed time.

## Run

```sh
go run ./bench/gate                  # compares against merge-base with local main
go run ./bench/gate -base <commit>   # compares against explicit local commit
go run ./bench/gate -base <commit> -keep
```

CI (`bench-gate.yml`) sets `GATE_BASE` to the PR base SHA.

## Pass rules and exit codes

Fails closed if any gated row or metric is missing, any `allocs/op` increases,
or any `B/op` increases by >5% (`BenchmarkFileStoreCreateFromFloor` gates `B/op`
only). Authoritative list lives in `bench/gate/main.go`:

- `store/durable`: `BenchmarkFileStoreCreateFromFloor` (5×), `BenchmarkUnifiedScanAllBytesLowCardinality` (5×), `BenchmarkUnifiedScanAllBytesHighCardinality` (5×)
- `internal/storeio`: `BenchmarkCompactPrimaryStripePointRead` (20×)
- `query`: `BenchmarkStoreQueryIndexedProjection` (20×), `BenchmarkStoreQueryIndexedCount` (20×), `BenchmarkApplyKernelLeftExactMemoizationWarm` (20×)

Exit codes: `0` pass, `1` regression found, `2` setup/build/parse error.
