# Performance measurement

VibeDB contains microbenchmarks, allocation regression gates, and a separate
cross-engine harness. A benchmark result is evidence only for its exact commit,
toolchain, platform, data shape, and command.

This documentation does not publish a current performance number. See
`bench/competitive/RESULTS.md` for the publication record.

## Run a focused Go benchmark

```bash
go test ./query \
  -run '^$' \
  -bench '^BenchmarkStoreQueryIndexedProjection$' \
  -benchmem \
  -count=10
```

Use a fixed `-benchtime` when you compare allocation counts. Keep the package,
benchmark regex, input flags, and `GOMAXPROCS` constant.

Correctness tests must pass before the benchmark. A benchmark is not a
correctness oracle.

## Replicated apply complexity

Normal replicated command admission and apply do not scan the shard image.
They read one bounded session header, at most one fixed ring slot, and the
command's mutation keys. Mutation planning is bounded by 64 distinct keys. It
performs indexed point reads plus bytewise validation and digest work on the
supplied changes.

The hot path advances a deterministic `DataChainDigest` from the prior chain
and the exact row changes. This digest is history-sensitive. It is not a
canonical incremental Merkle root and it cannot prove that images with
different histories contain the same rows.

Canonical `ImageDigest` work stays on cold full-image paths. Reopen and import
scan the image. Snapshot artifact creation computes the digest while it already
streams the image. An explicit audit scans a coherent read snapshot. Normal
admission and apply do not compute `ImageDigest`.

When you measure this contract, vary shard row count independently from command
mutation count and changed document bytes. Report point-update latency and
allocation data separately from full-image reopen, artifact, import, and audit
costs. The complexity contract is not a published throughput or latency claim.

## Run the allocation gate

```bash
go run ./bench/gate
```

The gate measures the working tree and the merge-base on the same machine. It
creates a temporary Git worktree for the base.

The gate fails when a gated `allocs/op` count increases. It also fails when a
gated `B/op` count increases by more than 5 percent. It prints `ns/op` only for
context and never uses time as a pass or fail signal.

See [the gate reference](../bench/gate/README.md) for the curated benchmark set.

## Run the competitive harness

The competitive harness is a nested Go module. Its dependencies do not belong
in the root module.

```bash
cd bench/competitive
go test ./...
```

Use the commands in [the generated coverage matrix](../bench/competitive/COVERAGE.md)
for exact measurement shapes.

The harness compares VibeDB with configured file-backed engines. It provides
separate commands for mixed load, repeated isolated mixed suites, footprint,
speed probes, and disk churn.

One engine runs in each measurement process when process RSS or Go heap data is
part of the result. This prevents retained state from another engine from
contaminating the measurement.

## Publication requirements

Record all of this metadata with a published result:

- VibeDB commit and dirty state
- Competitor versions and configuration
- Go version and build flags
- Operating system, architecture, CPU, memory, and filesystem
- Storage device and durability lane
- Corpus size, shape, cardinality, and seed
- Index configuration and cache budget
- Client count, warmup, operation count, and checkpoint cadence
- Complete command line
- Raw per-run rows
- Repetition count and summary method

Use at least nine isolated repetitions for the mixed-suite publication shape.
The harness uses a deterministic Latin-square engine order and can run one
discarded conditioning pass.

Report apparent bytes and allocated filesystem blocks separately. Report Go
heap and process residency separately. VibeDB uses mapped and I/O memory outside
the Go heap.

## Comparison rules

- Compare equivalent correctness and durability contracts.
- Keep the logical data and operation mix identical.
- Keep built-in compression policy explicit.
- Label a VibeDB-only command as diagnostic, not comparative.
- Label a projected value as a projection.
- Do not replace a database-level result with a codec microbenchmark.
- Keep tail latency and throughput as separate measures.
- Publish the raw result artifact with the summary.

## Churn and crash evidence

The disk-churn command measures a deterministic bounded mutation run. It is not
a time-based soak test.

Injected crash sweeps test recovery cuts. They do not measure real power-loss
frequency, storage-controller behavior, restart latency, or availability.

## Implementation references

- `bench/gate/main.go`
- `bench/competitive/bench_test.go`
- `bench/competitive/cmd/mixedsuite/main.go`
- `bench/competitive/cmd/footprint/main.go`
- `bench/competitive/cmd/churndisk/main.go`
- `bench/competitive/internal/coverage/manifest.go`
- `internal/replicatedstate/apply.go` and `digest.go`
- `internal/replicatedstate/read.go` and `snapshot_artifact.go`
