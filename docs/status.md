# Stability and current status

> [!CAUTION]
> **Assume breakage between commits.** VibeDB has no release, compatibility
> promise, support window, or project license. The only qualified restart is an
> exact same-build restart. There is no mixed-build rolling upgrade, downgrade,
> or best-effort migration contract. Keep the matching docs and binary together,
> and use only disposable or independently recoverable data.

Runtime behavior for this rewrite was audited from `main` at commit
`4ad895ab750bcdc0f9277a227325c1c77cbf87f9`. The documentation changes that
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

| Evidence | Current defect or sharp edge | Consequence |
| --- | --- | --- |
| [Service metrics](../internal/servicemetrics/service.go) | A nonzero-group request can panic when a metrics service was built with `Provider` that is not also `GroupProvider` | Group-serving configurations must supply `GroupProvider` |
| [Distributed transaction compaction](../internal/distributedtxn/journal_compact.go) | Compaction omits durable coordinator recovery-pulse records | Reopen after compaction can reset recovery-pulse state; do not rely on that compaction path for recovery authority |
| [Shard codec](../shardservice/codec.go) / [replicated apply](../internal/replicatedstate/apply.go) / [transaction apply](../internal/replicatedstate/transaction_apply.go) / [build manifest](../internal/buildgate/manifest/current.txt) | Static capture added mutation-image marker `0xe4`; later RF3 postimage work also widened replicated global-index digest replacement and normalized prepare conflicts. Neither boundary advanced the generated wire or disk grammar IDs | Builds across either boundary can pass the preface yet disagree on requests or durable replay; use one exact commit and do not rely on the preface or in-place replay across these changes |
| [Facade option cloning](../vibedb.go) | `AdvancedOptions.Engine.SkipIndexes` is not defensively cloned | Caller mutation after `Open` can affect later lazy collections |
| [Facade validation](../vibedb.go) / [transaction validation](../vibedb_txn.go) | A first lazy `Put` and later direct or transactional writes do not apply one consistent JSON rule | Do not enable opaque values through the root facade |
| [Facade transaction bounds](../vibedb_txn.go) | Transactional writes use database-open defaults rather than a reopened collection's persisted key/document bounds | Do not assume direct and transactional bound parity with custom persisted limits |
| [Facade names](../internal/collectionname/collectionname.go) / [memory-store names](../store/store_collection.go) | NUL handling differs between memory and durable layers | Treat NUL as unsupported in portable collection names |
| [`mixedsuite` summaries](../bench/competitive/cmd/mixedsuite/main.go) | Summary rows contain more grouping fields than the header declares | Do not consume the current summary TSV as a stable machine-readable schema |

These defects are reasons the project must not be used for irreplaceable data.

## Validation record

Validation was incremental across the rewrite and its `main` merges; this is
the complete claim, not a blanket green-build assertion.

| Check | Result | Boundary |
| --- | --- | --- |
| `go test -p=1 -timeout=25m ./...` | **Incomplete** | An earlier run reached `store/durable` and was externally terminated; no complete serial root run finished after the final `main` merge |
| `go build ./...`; `go vet ./...` | Passed after the final merge | Root module at the audited commit above |
| Final-main changed packages | Passed after the final merge | `internal/raftstore` and `internal/raftmember`; the final upstream delta changed only Raft-store implementation and tests |
| Focused packages | Passed immediately before that final storage-only merge | `gateway`, `internal/replicatedstate`, `sql/driver`, both gateway/shard commands, `shardservice`, `sql`, `query`, `pgwire`, conformance, feature-state, build-gate, unsafe-audit, and service-authorization suites |
| Other focused packages and integrations | Passed during the audit | Core storage, Raft, benchmark tooling, and hermetic client modules; these do not replace the incomplete root run |
| `go test ./store/durable -timeout=30m` | **Timed out** | `TestFileStorePointReplayDoesNotExhaustRetirementCapacity` was active; that test passed alone in 51 seconds, but the package has no complete result |
| PostgreSQL 18.6 upstream corpus | Not run | The approval set remains empty; see [PostgreSQL compatibility status](#postgresql-compatibility-status) |
| Live `psql` and Java/JDBC gates | Not run | Require explicit local dependencies and flags |
| Linux fault, `/proc`, direct-I/O, and Kubernetes Kind lanes | Not run | This audit ran on Darwin |

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
