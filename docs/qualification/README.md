# Qualification workflows and records

[Documentation](../README.md) / Qualification

A **qualification** is a named validation workflow with fixed pass criteria.
It requires the named test to exist and to pass the stated number of times
without a skip. It also checks that the recorded evidence meets every bound.
A pass applies only to the tested revision, runner, and scenario. It says
nothing about a lane that did not run or about a later build.

This page lists the qualification workflows in `.github/workflows` and the
dated records kept in this directory. For how to run and debug them locally,
see [build and test](../development/build-and-test.md#process-and-qualification-gates)
and [debugging distributed failures](../development/debugging.md).

## Qualification workflows

Durations are job run times (from start to completion, excluding queue time)
of the five most recent successful runs as of 2026-09-23, unless noted.
Unless noted, every job runs on a GitHub-hosted, shared four-core runner.

### Standalone workflows

| Workflow | Proves | Trigger | Runner | Gate and profile | Typical duration | Evidence artifact |
| --- | --- | --- | --- | --- | --- | --- |
| [`seamless-scale-in-out.yml`](../../.github/workflows/seamless-scale-in-out.yml) | Online 3→4→3 physical-node scale with shipped binaries, over at least three cycles. Checks acknowledged data, session continuity, controller and target restarts, duplicate operations, `safe_to_stop`, and bounded degradation in each window. | PR and `main` push with Go changes; manual | `ubuntu-latest`, 45 min limit | `VIBEDB_SEAMLESS_SCALE_E2E=1`, `VIBEDB_SEAMLESS_SCALE_PROFILE=shared-runner` | 6.3–7.0 min | `seamless-scale-in-out-<sha>` (`qualification.tsv`, `go-test.jsonl`) |
| [`fused-node-rf3.yml`](../../.github/workflows/fused-node-rf3.yml) | One physical node process hosting members of many RF3 groups, with default 3 and explicit 6 physical nodes. Manual runs can add a paired primary-format performance job. | PR and `main` push with Go changes; manual | `ubuntu-latest`, 30 min | `VIBEDB_FUSED_RF3_PROCESS_E2E=1` | 3.5–3.9 min | `fused-node-rf3-<sha>`; `primary-format-<sha>` when requested |
| [`durable-rf3-external.yml`](../../.github/workflows/durable-rf3-external.yml) | Job 1: gateway replacement, shard `SIGSTOP`/`SIGKILL`, lost terminal and ACK responses with exact replay, no acknowledged loss. Job 2: multi-table read batches under partitions and leader kill with per-group ReadIndex. Both jobs use `count=3`. | PR, `main` push, manual | `ubuntu-latest`, 25 min each | `VIBEDB_DURABLE_RF3_PROCESS_E2E=1`; `VIBEDB_READ_BATCH_RF3_PROCESS_E2E=1` | 2.6–3.3 min; 2.1–2.8 min | `durable-rf3-external-<sha>`, `read-batch-rf3-external-<sha>` |
| [`durable-rf3-multirelation.yml`](../../.github/workflows/durable-rf3-multirelation.yml) | Two tables with local and global exact indexes under insert/update/delete churn, leader kill, and peer partition, with exact index visibility (`count=3`). Also checks unfinished-request recovery with the default pin span. | PR, `main` push, manual | `ubuntu-latest`, 25 min | `VIBEDB_DURABLE_RF3_MULTIRELATION_E2E=1`, `VIBEDB_DURABLE_SQL_RF3_E2E=1` | 3.0–3.6 min | `durable-rf3-multirelation-<sha>` |
| [`restore-rf3-external.yml`](../../.github/workflows/restore-rf3-external.yml) | Restored catalog and data serving on six shard processes: fresh catalog and roots, restored global index, write survives leader `SIGKILL` (`count=3`). | PR, `main` push, manual | `ubuntu-latest`, 15 min | `VIBEDB_RESTORE_RF3_PROCESS_E2E=1` | 1.2–1.6 min | `restore-rf3-external-<sha>` |
| [`wal-retention.yml`](../../.github/workflows/wal-retention.yml) | Crash loop over the shipped RF3 shard with bounded WAL growth (≤1 MiB), live ratio, RSS, file-descriptor, and latency bounds (`count=3`). | PR, `main` push | `ubuntu-latest`, 30 min | `VIBEDB_WAL_RETENTION_E2E=1` | 1.5–1.9 min | `wal-retention-<sha>` (`runs/run-*.tsv`) |
| [`clock-fault-matrix.yml`](../../.github/workflows/clock-fault-matrix.yml) | Independent UTC steps fail closed at the TLS handshake, a logical-pulse stall across restart yields one replicated recovery outcome, transactions retry exactly across leader isolation, and suspended processes catch up. `live_process_utc_step` is declared **not injected**. | PR, manual | `ubuntu-latest`, 22 min | `scripts/ci/clock-fault-matrix.sh`, `VIBEDB_RF3_QUALIFICATION_PATH` | 1.8–2.2 min | `clock-fault-matrix-<sha>` (`matrix.tsv`) |
| [`dev-hot-split.yml`](../../.github/workflows/dev-hot-split.yml) | A zero-config development cluster and a custom table each complete a replicated hot-shard split three times without a skip. The custom-table run also restarts. | PR, manual (not `main` push) | `ubuntu-24.04`, 20 min | `VIBEDB_DEV_HOT_SPLIT_COUNT=3`, `VIBEDB_DEV_HOT_SPLIT_CUSTOM_TABLE_E2E=1` | 6.0–6.1 min | None; job log only |
| [`competitive-evidence.yml`](../../.github/workflows/competitive-evidence.yml) | Bounded matched-durability competitive run and RF3 evidence on a clean Linux tree, with a validated receipt. The outputs are not publication-grade results. | PR, manual | `ubuntu-24.04`, 55 min | `scripts/bench/run-ci-competitive-evidence.sh` | 3.8–4.2 min | `competitive-evidence-<sha>-<run>-<attempt>` |
| [`bench-gate.yml`](../../.github/workflows/bench-gate.yml) | Allocation gate (`allocs/op`, `B/op` against the PR base; time not gated). Competitive lifecycle smoke, including a real cold-cache drop with `sudo`. | PR | `ubuntu-latest` | none | about 2 min per job | None |
| [`packed-simd.yml`](../../.github/workflows/packed-simd.yml) | SIMD parity, `checkptr` race, AVX2-disabled fallback, and AMD64 extrema AVX2 speed check. Records alternating raw benchmarks against the base. | PR touching packed kernels; manual with `base_ref` | `ubuntu-latest` and `ubuntu-24.04-arm`, 20 min | `VIBEDB_TEST_REQUIRE_AVX2=1` on x86-64 | 4.0–5.7 min (x86-64), 3.6–4.6 min (arm64) | `packed-simd-<runner>-<run_id>` |
| [`packed-read-throughput.yml`](../../.github/workflows/packed-read-throughput.yml) | Observation only: ABBA base-vs-head packed scan and point benchmarks, plus profiles. **No speed threshold.** | PR touching packed read paths | both architectures, 15 min | none | 2.9–4.1 min | `packed-read-throughput-<os>` (90 days) |
| [`postgresql-compatibility.yml`](../../.github/workflows/postgresql-compatibility.yml) | Pinned PostgreSQL 18.6 upstream regression corpus through `pgwire`; publishes a pass/fail report. | Nightly 03:23 UTC; manual (`full` or `smoke`) | `ubuntu-latest`, 60 min | `integration/pgcompat/run-postgres-regression.sh` | 1.6–2.2 min | `postgresql-18.6-compatibility-<sha>` |
| [`p01-apply-10m.yml`](../../.github/workflows/p01-apply-10m.yml) | A point update against 10,000,000 rows applies within fixed page-read, split, allocation, and file-growth bounds, with no full scan. | Manual; PR labeled `p01-apply-10m` | `ubuntu-latest`, 90 min | `VIBEDB_APPLY_10M=1` | Under 1 min; last success 2026-08-26 | `p01-apply-10m-<sha>` (`result.json`) |
| [`docs.yml`](../../.github/workflows/docs.yml) | Markdown links, anchors, and page structure. | PR, `main` push | `ubuntu-latest`, 5 min | none | under 1 min | None |

### Qualification jobs inside `ci.yml`

These run on every PR and `main` push as part of [`ci.yml`](../../.github/workflows/ci.yml):

| Job | Proves | Gate | Evidence artifact |
| --- | --- | --- | --- |
| Recovery, replica replacement and PostgreSQL clients | Durable SQL RF3 terminal/ACK recovery across leader partitions (`count=3`). Automatic replica replacement with 17 bounded metrics (failover ≤30 s, replacement ≤90 s, RSS ≤8 GiB) (`count=3`). pgx, lib/pq, and `psql` 18.4. | `VIBEDB_DURABLE_SQL_RF3_E2E`, `VIBEDB_REPLICA_REPLACEMENT_E2E`, `VIBEDB_TEST_PSQL` | `durable-sql-rf3-<sha>`, `replica-replacement-<sha>` |
| Hot-shard and transport qualification | Write-driven hot-shard move across ten shard processes with leader kill, response partition, and reopen. Includes `bench/rf3chaos`, authenticated transport rotation and confused-deputy faults (`count=3`), and exhaustive RF3 quorum cuts. | `VIBEDB_HOT_SHARD_MUTATION_E2E`, `VIBEDB_AUTH_TRANSPORT_PROCESS_E2E`, `VIBEDB_RF3_QUORUM_QUALIFICATION` | `hot-shard-mutation-<sha>` |
| repository contracts | Every restore activation publication cut (`count=3`, six cuts) and the group installer. Also checks stale generated files and deterministic Kubernetes manifests. | `VIBEDB_RESTORE_ACTIVATION_E2E` | `restore-activation-<sha>` |
| RF3 read-authority default and protocol | Read-authority selection and retained policy, physical preparation at 3 and 6 nodes, and live enrollment with retained restart. | `VIBEDB_PHYSICAL_TEST_SHARD_BINARY` | None |
| Kubernetes RF3 serving and restart qualification | Three-worker Kind cluster: render, bootstrap, serve, roll every RF3 role, then check readiness and ordinals. | `deploy/kubernetes/qualify-kind.sh` | `kubernetes-rf3-<sha>` |
| Native RF3 SQL and restart | Create, write, restart, and read back on RF3. Replaces a replica during live ownership retries (`count=3`), on Linux and macOS. | none | None |

### Seamless-scale profiles

`TestSeamlessScaleInOutProcessQualification` has two bound sets, selected by
`VIBEDB_SEAMLESS_SCALE_PROFILE`:

| Profile | Latency bound during scaling (p50/p95/p99 vs baseline) | After scaling | Throughput during / after | Max pause | Load |
| --- | --- | --- | --- | --- | --- |
| `dedicated` (default when unset) | 1.05× / 1.10× / 1.15× + 100 µs floor | same | ≥99% / ≥99% | 100 ms, with a completion-gap continuity check | Searches for the host's capacity |
| `shared-runner` (CI) | 20× + 250 ms floor | 3× | ≥50% / ≥90% | 5 s; no continuity check | Fixed 400 requests/s |

Both profiles enforce the same correctness contract. Every window must be free
of errors, timeouts, and misses, data must be intact, and fault recovery must
finish within the test's bound. **The product latency targets in `dedicated`
are not proven by CI.** They need controlled hardware, and no workflow runs
that profile. Setting any `VIBEDB_SCALE_*` override marks the result as
non-strict diagnostic evidence, which cannot satisfy the qualification. The
method and evidence format are in the
[seamless scale method](../benchmarks/seamless-scale-in-out-method.md).

### Reliability notes

Recent history, from GitHub Actions runs on 2026-09-22 and 2026-09-23:

- Seamless scale first passed on `main` on 2026-09-23. Before that, its
  development branch failed it repeatedly from 2026-09-14 onward. Treat it
  as a young gate.
- In the same period, most failed `ci` runs on development branches were in
  the `core` unit shards, `race distributed`, and the 386 cross-compile
  targets. Hot-shard and `dev-hot-split` had occasional failures, and the
  process shards failed rarely. A `main` run on 2026-09-23
  (`0ccfa8a1e`) failed in the hot-shard job and passed on the next commit.
  Check a failure against the latest `main` run before you attribute it to
  your change.
- `cancelled` runs are almost always superseded by a newer push.

## Recorded runs

Dated qualification records. Each one records its own revision, method, and
limitations.

| Record | Report |
| --- | --- |
| `horizontal-ci-2026-09-05` | [Horizontal CI checkpoint](horizontal-ci-2026-09-05/README.md) |
| `timer-backpressure-2026-09-05` | [Timer backpressure fixture race evidence](timer-backpressure-2026-09-05/README.md) |
| `read-authority-2026-09-05` | [Intermediate quorum read authority qualification](read-authority-2026-09-05/README.md) |
| `raft-owner-pressure-2026-09-06` | [Raft owner and WAL pressure qualification](raft-owner-pressure-2026-09-06/README.md) |
| `wide-update-client-2026-09-04` | [Wide-key client verification](wide-update-client-2026-09-04/README.md) |
| `sharded-clock-2026-09-04` | [Sharded transaction clock correctness review](sharded-clock-2026-09-04/README.md) |
| `node-serving-2026-09-04` | [Initial shared-node serving qualification](node-serving-2026-09-04/README.md) |
| `node-registration-2026-09-04` | [Shared-node registration qualification](node-registration-2026-09-04/README.md) |
| `node-fault-2026-09-04` | [Shared-node shipped-process fault qualification](node-fault-2026-09-04/README.md) |
| `fused-node-transport-2026-09-04` | [Fused-node transport qualification checkpoint](fused-node-transport-2026-09-04/README.md) |
| `fused-diagnostics-2026-09-04` | [Storage fold evidence without materialization](fused-diagnostics-2026-09-04/README.md) |
| `fused-catalog-refresh-2026-09-04` | [Physical frontend visibility and retained-root restart](fused-catalog-refresh-2026-09-04/README.md) |

[Historical documentation audit](documentation-audit-215fb05.md).

See [Contributing](../../CONTRIBUTING.md) for the checks a change needs and
[stability](../status.md) for compatibility rules.
