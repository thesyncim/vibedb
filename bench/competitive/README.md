# Benchmark harness

> **Development status:** VibeDB is under active development. Benchmark commands,
> output schemas, adapters, dependencies, and evidence rules can change or break at
> any commit. This directory is qualification infrastructure, not a stable benchmark
> product or a source of endorsed performance claims.

## Current result status

**No competitive result is published.** [RESULTS.md](RESULTS.md) contains no
performance, cost, storage, or scaling number. A green test, a generated coverage
cell, or a CI artifact is not a result.

This is a separate Go 1.26 module so competitor dependencies do not enter VibeDB's
root module. The current adapters use VibeDB from this checkout, Badger v4.9.5,
bbolt v1.5.0, Pebble v1.1.5, and modernc SQLite v1.54.0. `go.mod` is authoritative.

## Verify the harness

From the repository root:

```bash
cd bench/competitive
go test -count=1 ./...
```

This runs adapter oracles and command-contract tests. It does not benchmark the
machine and does not make unlike engines equivalent.

## Choose a command

Run any command with `-h` before using it; flags are intentionally not stable.

| Command | Use it for | Do not infer |
| --- | --- | --- |
| `go run ./cmd/mixed` | One deterministic workload in one engine process | A repeated or cross-engine comparison |
| `go run ./cmd/mixedsuite` | Isolated child runs in deterministic Latin-square order, with raw rows and summaries | Publication from fewer than nine recorded repetitions |
| `go run ./cmd/footprint` | Apparent file bytes, allocated filesystem blocks, Go heap, runtime residency, and maximum RSS | Device usage or a common memory architecture |
| `go run ./cmd/churndisk` | Bounded fixed-live-set churn and periodic storage samples | Media writes; Linux `write_bytes` is process-attributed storage-layer traffic |
| `go run ./cmd/saturation` | A client-count sweep and one fixed plateau rule | A universal engine capacity or a publication-bundle result |
| `go run ./cmd/snapshotpressure` | Control and durable pinned-snapshot phases on one image | Counterbalanced trials or identical memory accounting across adapters |
| `go run ./cmd/sqlsurface` | VibeDB/SQLite `database/sql`, or VibeDB loopback pgwire overhead | Comparison with a PostgreSQL server |
| `go run ./cmd/lifecycle` | Fresh-process open, conditioned hot open, Linux cold open, crash recovery, verify, or repack | Application startup time or production cutover safety |
| `go run ./cmd/outofram` | Streaming load and full-byte scan when logical data exceeds host RAM | Load throughput; load is qualification setup |
| `go run ./cmd/speedprobe` | VibeDB-only adaptive diagnostics | Comparative or publication-grade evidence |
| `go run ./cmd/clickhousefixture` | Deterministic typed or raw `JSONEachRow` fixture output | A ClickHouse run, driver, oracle, or comparison |
| `go run ./cmd/coveragegen` | Regenerate the coverage reference | Any measurement |
| `go run ./cmd/publishcheck` | Validate a fixed evidence inventory and create a receipt | Independent validation of every runner flag or a win |

The opt-in `VIBEDB_CHURN_DIAG=1` and `MASSIVE_CHURN=1` Go tests are research
diagnostics. They are not CI or publication lanes.

## Comparison contract

Use one engine per process for mixed-suite, footprint, saturation, and
above-RAM work. RSS and Go heap are process-wide.

Keep these axes explicit and identical where the adapters support them:

- corpus size, seed, cardinality, and `inline`, `mixed`, or `overflow-heavy`
  document shape;
- workload, warmup, operation count, client count, and checkpoint cadence;
- exact-index count and definitions;
- durability contract; and
- storage profile, cache budget, hard resource bounds, host, filesystem, and
  dependency revision.

### Adapter boundaries

| Adapter | Exact JSON indexes | Accepted durability contracts |
| --- | --- | --- |
| VibeDB | 0–3 | `buffered-visible`, `async-stable-in-flight`, `ordinary-sync`, `power-safe` |
| SQLite | 0–3 | `buffered-visible`, `ordinary-sync`, `power-safe` |
| Badger | none | `buffered-visible`, `ordinary-sync` |
| bbolt | none | `buffered-visible`, `ordinary-sync` |
| Pebble | none | `buffered-visible`, `ordinary-sync` |

Unsupported combinations fail; the harness does not silently weaken them.
The three physical exact indexes, when selected, are country, tier, and region
in that order.

The `intrinsic` and `production` storage-profile names describe only optional
adapter compression. They are not readiness levels. With the current pins,
`production` enables Snappy for Badger and Pebble; VibeDB, bbolt, and SQLite
report the setting as unsupported/no-op.

The nominal engine cache is 64 MiB, but this is not identical memory pressure:
bbolt memory-maps the database and relies on the OS cache, and SQLite is
configured with one physical connection. State these facts with any comparison.

### Metric boundaries

- Apparent bytes, allocated filesystem blocks, Go heap, runtime residency, and
  maximum RSS answer different questions; do not substitute one for another.
- `durability-payload-known=true` exposes adapter-reported bytes handed to its
  durability device. It is not filesystem, block-device, or media accounting.
- Linux `/proc/self/io` `write_bytes` is process-attributed storage-layer traffic,
  not media writes.
- `BenchmarkScan` touches only the first byte of each value. Use
  `BenchmarkScanAllBytes` for full-byte throughput evidence.
- A saturation point is specific to the recorded host and workload.
- RF3's in-process latency matrix is one three-replica group. It is not
  node/shard weak scaling, gateway-process throughput, split/rebalance evidence,
  or multi-group query evidence.

## Evidence levels

| Level | What it establishes | What it does not establish |
| --- | --- | --- |
| Generated coverage | A checked-in source shape exists for each project-defined cell | That the command ran, the product supports the case, or a result exists |
| Command smoke test | A bounded command and its local oracle completed | Stable timing or comparative performance |
| PR qualification | A clean-revision, small fixed evidence bundle passed selected structural checks | Publication-grade scale, dedicated-host control, or superiority |
| Publication candidate | The dedicated Linux runner completed and `publishcheck` accepted its fixed artifacts | That every configuration flag was independently checked or that VibeDB won |
| Published result | A reviewed summary links immutable raw evidence and a validator receipt | Results on another commit, host, workload, or topology |

The `competitive-evidence` workflow uploads claim-free PR qualification artifacts
for 30 days. Those transient artifacts must not be copied into `RESULTS.md`.

## Create a publication candidate

Use a dedicated Linux host and a clean, fixed revision. From the repository root:

```bash
scripts/bench/run-publishable-evidence.sh \
  /absolute/path/to/new-evidence-directory \
  OUT_OF_RAM_DOCUMENTS
```

The output directory must be absolute and absent. Choose `OUT_OF_RAM_DOCUMENTS`
so the exact logical corpus exceeds measured physical RAM; the runner checks the
inequality. The run includes embedded adapter cuts, footprint, churn, above-RAM
work, RF3 matrices, and nine external RF3 fault repetitions.

The final `VALIDATED.tsv` is created only after `cmd/publishcheck` accepts the
expected evidence. The validator checks inventory, provenance, selected row
contracts, repetition counts, metrics, counters, bounds, and file digests. It
does **not** independently pin every workload, corpus, operation, warmup, client,
cardinality, seed, conditioning, or storage-profile flag. Publication mode also
does not make arbitrary extra files part of the validated set. Review the runner,
raw metadata, and exact commands before writing a claim.

## Generated coverage reference

[COVERAGE.md](COVERAGE.md) is generated from
`internal/coverage/manifest.go`. Its 38 cells are the project's current harness
matrix; they omit horizontal weak scaling, split/rebalance under load, hot-key
skew, and multi-group distributed query execution.

Regenerate and verify it after changing the manifest or referenced commands:

```bash
cd bench/competitive
go generate .
go test -run '^TestBenchmarkCoverage' -count=1 ./internal/coverage
```

Do not edit `COVERAGE.md` by hand. An “implemented” cell means the source shape
exists with an oracle and machine-readable fields. It does not mean the command
ran or a result was published.
