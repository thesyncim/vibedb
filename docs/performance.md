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

Session creation is a zero-mutation Open whose Raft apply index becomes the
client token; its retained result is sequence 1 and the first user mutation is
sequence 2. The ordinary path keeps one raw-binary header and a fixed raw-binary
ring with exactly `min(HighSequence, RetryWindow)` physical slots. `AckThrough`
and the fixed window define a logical retry floor without moving or expanding
the ring. Missing-header checks use one ordered session-prefix probe, not a scan
of every possible slot.

Retirement keeps the bounded image retryable. Exact Release is the cold
maintenance path: it validates and atomically deletes at most `RetryWindow`
slots. Its work is linear in the configured retry window, capped at 256, and is
independent of shard row count and historical operation count. Release retains
the epoch high-water, so reclaiming space does not trade away delayed-command
fencing.

The replicated SQL hidden collection's hard document limit is
`RetryWindow + 2`, rather than a fixed maximum-window allocation for every
shard. Normal transactions supply precise `BatchDocumentsHint` values, so the
durable dedup map reserves only the actual one-to-three system changes on the
hot path while retaining the ability to grow to the cold Release bound.
Benchmark ordinary mutation separately from Open, retirement, and Release, and
report retry windows 1, 8, and 256 so cold linear work is not blended into
hot-path latency or allocation results.

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

The default cardinality regression uses real durable collections at one row
and 65,536 rows. It requires an effective one-key `ApplyNormal` publication to
perform zero full-snapshot scans, exactly one attempted key and validator call,
at most four user-page reads, at most one bounded leaf conversion, and no empty
reclaim. A cold structural update may activate the fixed committer descriptor
arena, so the test keeps that visible and caps the measured apply at 20 MiB and
1,024 allocations. The benchmark additionally caps non-cold work at 64 KiB
and 256 allocations per operation. Timing remains diagnostic; the fixed-work
and allocation ceilings are the gates, not a latency claim.

The literal 10,000,000-row qualification is intentionally opt-in because
building its real on-disk image is setup work, not a suitable cost for every
pull request:

```bash
VIBEDB_APPLY_10M=1 go test -count=1 -timeout=80m \
  -json \
  -run '^TestMachineApplyPointUpdateTenMillionQualification$' \
  ./internal/replicatedstate
```

The `P0.1 10M-row apply qualification` workflow runs that qualification on a
manual dispatch. A pull request can qualify its exact head by adding the
`p01-apply-10m` label; ordinary pull requests do not pay the setup cost. The
workflow retains the candidate revision, clean/dirty state, toolchain, host,
memory, filesystem type and capacity, raw `go test -json` stream, a structured
result, setup/open durations, measured apply duration, and before/after
counters. The job fails if the exact named test is absent, lacks an exact test
pass event, or omits the structured result. A candidate closes the cardinality
exit gate only with a passing artifact for its exact commit. Setup and open
time are reported separately and never presented as point-update latency.

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

The RF3 external fault runner executes the shipped three-process shard command,
including process isolation, leader kill, deliberately unread response,
byte-identical outcome recovery, replica restart, durable acknowledgement
survival, bounded result-waiter reuse, and catch-up. It writes canonical raw TSV
evidence and treats a skipped test as a failed qualification:

```bash
go run ./bench/rf3chaos \
  -output "$(pwd)/rf3-chaos.tsv" \
  -runs 9 \
  -timeout 3m
```

Its per-run elapsed value covers the complete harness. It is not a failover or
recovery latency measurement. Publish those latency claims only after the
shipped protocol exposes and records the individual fault, election, routing,
settlement, and catch-up cuts.

Strict physical allocation for the recovery journal is Linux-only. On Darwin,
the external harness skips and the runner records a failed row and exits
nonzero; that local result is not qualification. Linux CI invokes the runner
explicitly so lack of allocation support cannot silently become a passing
gate.

## Implementation references

- `bench/gate/main.go`
- `bench/competitive/bench_test.go`
- `bench/competitive/cmd/mixedsuite/main.go`
- `bench/competitive/cmd/footprint/main.go`
- `bench/competitive/cmd/churndisk/main.go`
- `bench/rf3chaos/main.go`
- `bench/competitive/internal/coverage/manifest.go`
- `internal/replicatedstate/apply.go` and `digest.go`
- `internal/replicatedstate/read.go` and `snapshot_artifact.go`
