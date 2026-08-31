# Stability and current status

> [!CAUTION]
> **Assume breakage between commits.** VibeDB has no release, compatibility
> promise, support window, or project license. The only qualified restart is an
> exact same-build restart. There is no mixed-build rolling upgrade, downgrade,
> or best-effort migration contract. Keep the matching docs and binary together,
> and use only disposable or independently recoverable data.

Runtime behavior for this rewrite was audited from `main` at commit
`e7b87f44fdbbc0188bbeadd87f77eec78e24b780`. The documentation changes that
follow it do not turn that snapshot into a roadmap or guarantee about a later
commit.

## Maturity by surface

| Surface | Current use | Do not infer |
| --- | --- | --- |
| Native embedded API | Development; most direct and best-covered interface | Stable Go API or cross-commit disk compatibility |
| Typed query API | Development; bounded in-process execution | Full SQL equivalence or fixed total RSS |
| `database/sql` | Experimental VibeDB SQL dialect | ANSI or PostgreSQL compatibility |
| `pgwire` | Experimental PostgreSQL v3 client adapter | PostgreSQL server, catalog, ORM, extension, or replication compatibility |
| Static distributed mode | Development routing and integration lane | HA or Raft failover |
| RF3 distributed mode | Development and fault-qualification lane | Production readiness, global MVCC snapshots, or elastic operation |
| Kubernetes tools | Disposable Kind qualification | Operator, reconciler, deployment product, or production topology |

“Implemented,” “integrated,” and “used by a development command” are different
states. The [generated feature ledger](distributed-feature-state.md) keeps them
separate. “Used by a development command” does not mean released or supported.

## Compatibility rule

Build identities are opaque. Compatibility requires exact wire grammar, disk
grammar, and symmetric required-capability agreement; there is no numeric
version ordering. The common build-adoption gate is not yet wired into every
durable open path, so even a successful low-level open is not an upgrade
promise.

For any evaluation:

1. Record the full commit and dirty state.
2. Keep all peers and data on the exact same build.
3. Stop and close writers before copying data.
4. Preserve a restorable copy of the complete database directory.
5. Expect a later commit to reject or replace that image.

Development format fixtures are byte-exact test oracles, not old-format
readers. Intentional format changes replace the current format-0 fixture and
continue to reject obsolete layouts.

## Known defects in this snapshot

These are source-audit findings, not hypothetical limitations.

| Area | Current defect or sharp edge | Consequence |
| --- | --- | --- |
| Authorization | `SELECT 1 GARBAGE` is classified as read-only instead of the intended fail-closed read+write+schema set | `go test ./internal/serviceauthz` fails on untouched `main`; do not treat malformed-SQL classification as qualified |
| Service metrics | A nonzero-group request can panic when a metrics service was built with `Provider` that is not also `GroupProvider` | Group-serving configurations must supply `GroupProvider` |
| Distributed transaction journal | Compaction omits durable coordinator recovery-pulse records | Reopen after compaction can reset recovery-pulse state; do not rely on that compaction path for recovery authority |
| Facade options | `AdvancedOptions.Engine.SkipIndexes` is not defensively cloned | Caller mutation after `Open` can affect later lazy collections |
| Facade opaque values | A first lazy `Put` and later direct/transactional writes do not apply one consistent JSON rule | Do not enable opaque values through the root facade |
| Reopened bounds | Transactional writes use database-open defaults rather than a reopened collection's persisted key/document bounds | Do not assume direct and transactional bound parity with custom persisted limits |
| Collection names | NUL handling differs between memory and durable layers | Treat NUL as unsupported in portable collection names |
| Benchmark summaries | `mixedsuite` summary rows contain more grouping fields than the header declares | Do not consume its current summary TSV as a stable machine-readable schema |

Source and test locations are maintained in the relevant API/operations pages.
These defects are reasons the project must not be used for irreplaceable data.

## Test baseline

At the audited commit, a serial root-module run found the known
`internal/serviceauthz` failure above and continued into `store/durable`, where
the command was externally terminated. It therefore did **not** produce a
complete root-module result, and this page does not present the tree as green.
Focused core, store, query, SQL, pgwire, Raft, command, benchmark, and hermetic
client integration suites passed during the audit.

Opt-in or environment-specific gates were not all run locally:

- PostgreSQL's upstream corpus was not run.
- Stock `psql` and Java/JDBC live gates require explicit local dependencies.
- Linux-only external fault, `/proc`, direct-I/O, and Kubernetes Kind lanes were
  not reproduced on this Darwin host.

CI workflow presence does not prove branch protection or a mandatory release
gate. Raw CI artifacts are evidence for one run, not a benchmark publication or
support claim.

## PostgreSQL compatibility status

The pgwire adapter supports selected PostgreSQL v3 protocol behavior and client
shapes. The upstream PostgreSQL 18.6 harness currently has **zero approved
byte-for-byte regression tests**. With an empty approval set, semantic
mismatches are observational; timeout, client failure, or a future approved
regression is what fails the lane.

Therefore the accurate description is “PostgreSQL protocol adapter for the
VibeDB SQL subset,” never “PostgreSQL-compatible database.”

## Performance status

No competitive result is published in this repository. Coverage tables show
that harness shapes exist; qualification receipts validate artifact structure;
neither establishes a win, cost claim, or scaling result. See
[performance evidence](performance.md).

## Source map

- `internal/buildgate/profile.go`, `disk.go`, and `rolling_restart_test.go`
- `internal/serviceauthz/sql.go` and `sql_test.go`
- `internal/servicemetrics/service.go`
- `internal/distributedtxn/journal.go` and `journal_compact.go`
- `vibedb.go` and `vibedb_txn.go`
- `integration/pgcompat/approved-tests.txt`
- `bench/competitive/RESULTS.md`
- `bench/competitive/cmd/mixedsuite/main.go`
