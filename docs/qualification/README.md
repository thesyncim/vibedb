# Qualification workflows

[Documentation](../README.md) / Qualification

A **qualification** is a named CI workflow with fixed pass criteria: the named
test must exist, execute without skipping for the required repeat count, and
meet all stated bounds. Results apply only to the tested revision, runner, and
scenario. See [build and test](../development/build-and-test.md) and
[debugging](../development/debugging.md) to run gates locally.

## Standalone workflows

| Workflow | Proves | Trigger | Gate / profile |
| --- | --- | --- | --- |
| [`seamless-scale-in-out.yml`](../../.github/workflows/seamless-scale-in-out.yml) | Online 3 → 4 → 3 physical-node scaling (≥3 cycles) under open-loop load with session continuity, restarts, and `safe_to_stop` | PR, `main`, manual | `VIBEDB_SEAMLESS_SCALE_E2E=1`, `shared-runner` |
| [`fused-node-rf3.yml`](../../.github/workflows/fused-node-rf3.yml) | Physical nodes hosting many RF3 groups (3 and 6 nodes), live enrollment, `SIGKILL`, reopen | PR, `main`, manual | `VIBEDB_FUSED_RF3_PROCESS_E2E=1` |
| [`durable-rf3-external.yml`](../../.github/workflows/durable-rf3-external.yml) | Gateway replacement, shard `SIGSTOP`/`SIGKILL`, lost terminal/ACK replay; multi-table read batches under partition (`count=3`) | PR, `main`, manual | `VIBEDB_DURABLE_RF3_PROCESS_E2E=1`, `VIBEDB_READ_BATCH_RF3_PROCESS_E2E=1` |
| [`durable-rf3-multirelation.yml`](../../.github/workflows/durable-rf3-multirelation.yml) | Multi-table + global-index churn under leader kill and partition (`count=3`) | PR, `main`, manual | `VIBEDB_DURABLE_RF3_MULTIRELATION_E2E=1`, `VIBEDB_DURABLE_SQL_RF3_E2E=1` |
| [`restore-rf3-external.yml`](../../.github/workflows/restore-rf3-external.yml) | Fresh-identity restore, global index serving, leader `SIGKILL` failover (`count=3`) | PR, `main`, manual | `VIBEDB_RESTORE_RF3_PROCESS_E2E=1` |
| [`wal-retention.yml`](../../.github/workflows/wal-retention.yml) | Crash loop with bounded WAL growth (≤1 MiB), RSS, FD, and latency bounds (`count=3`) | PR, `main` | `VIBEDB_WAL_RETENTION_E2E=1` |
| [`clock-fault-matrix.yml`](../../.github/workflows/clock-fault-matrix.yml) | UTC step TLS rejection, logical-pulse stall recovery, leader isolation retries, process suspend (`live_process_utc_step` not injected) | PR, manual | `scripts/ci/clock-fault-matrix.sh` |
| [`dev-hot-split.yml`](../../.github/workflows/dev-hot-split.yml) | Zero-config and custom-table pressure-driven hot-shard splits + restart (`count=3`) | PR, manual | `VIBEDB_DEV_HOT_SPLIT_COUNT=3` |
| [`competitive-evidence.yml`](../../.github/workflows/competitive-evidence.yml) | Bounded matched-durability competitive + RF3 smoke run with receipt | PR, manual | `scripts/bench/run-ci-competitive-evidence.sh` |
| [`bench-gate.yml`](../../.github/workflows/bench-gate.yml) | Allocation gate (`allocs/op`, `B/op` vs base; time not gated) | PR | `go run ./bench/gate` |
| [`packed-simd.yml`](../../.github/workflows/packed-simd.yml) | SIMD parity, `checkptr` race, scalar fallback, AMD64 AVX2 extrema check | PR (packed), manual | `VIBEDB_TEST_REQUIRE_AVX2=1` |
| [`packed-read-throughput.yml`](../../.github/workflows/packed-read-throughput.yml) | Observation-only ABBA packed scan/point benchmarks (no threshold) | PR (packed) | none |
| [`postgresql-compatibility.yml`](../../.github/workflows/postgresql-compatibility.yml) | Pinned PostgreSQL 18.6 regression corpus via `pgwire` | Nightly, manual | `integration/pgcompat/run-postgres-regression.sh` |
| [`p01-apply-10m.yml`](../../.github/workflows/p01-apply-10m.yml) | Point update on 10M rows within page-read/split/alloc/growth bounds | Manual, label | `VIBEDB_APPLY_10M=1` |
| [`docs.yml`](../../.github/workflows/docs.yml) | Markdown links, anchors, and structure | PR, `main` | `make docs-check` |

## Qualification jobs in `ci.yml`

- **Recovery, replica replacement & PG clients:** Durable SQL RF3 recovery,
  automatic replica replacement (17 bounded metrics: failover ≤30 s,
  replacement ≤90 s, RSS ≤8 GiB), plus `pgx`, `lib/pq`, and `psql`.
- **Hot-shard & transport:** Write-driven hot-shard moves across 10 shard
  processes, `bench/rf3chaos`, mTLS cert rotation & confused-deputy faults, and
  exhaustive RF3 quorum cuts.
- **Repository contracts:** Six restore-activation publication cuts (`count=3`),
  stale generated contract checks, and deterministic Kubernetes manifest render.
- **Portable RF3 (`ubuntu-latest` + `macos-latest`):** Create, write, restart,
  and live replica replacement.
- **Kubernetes RF3 (Kind):** Three-worker Kind cluster render, bootstrap,
  rolling restart, and readiness check.

## Seamless-scale profiles

`TestSeamlessScaleInOutProcessQualification` selects bounds via
`VIBEDB_SEAMLESS_SCALE_PROFILE`:

| Profile | Scaling latency (p50/p95/p99 vs base) | Post-scale | Throughput (during / after) | Max pause | Load |
| --- | --- | --- | --- | --- | --- |
| `dedicated` (default) | 1.05× / 1.10× / 1.15× + 100 µs | same | ≥99% / ≥99% | 100 ms (continuity check) | Auto capacity search |
| `shared-runner` (CI) | 20× + 250 ms | 3× | ≥50% / ≥90% | 5 s (no continuity check) | Fixed 400 req/s |

Both profiles enforce zero errors, zero misses, and intact data. CI only proves
`shared-runner`; `dedicated` targets require controlled hardware. Setting any
`VIBEDB_SCALE_*` override produces non-strict diagnostic evidence. See
[seamless scale method](../benchmarks/seamless-scale-in-out-method.md).
