# Stability and compatibility

[Documentation](README.md) / Stability

VibeDB is an unreleased development project. There is no stable Go API, SQL
contract, wire protocol, or cross-commit disk format. Evaluate a pinned
revision and keep the corresponding binaries, documentation, and recovery
state together.

## Status by interface

| Interface | Available for evaluation | Main boundary |
| --- | --- | --- |
| Native embedded Go | JSON collections, exact indexes, tin full-text indexes, typed queries, serializable transactions. | API and data formats can change. |
| Full-text search | TINQL `==>` matching and BM25 `SCORE()` over in-memory and durable collections, in SQL, pgwire, and the Go API. | Postings are maintained incrementally across searched generations and persisted in a SHA-256-checksummed `.tin` collection sidecar for zero-scan reopen, while the active index must fit in memory. A transaction on any profile, `Memory` included, refuses `==>` and `query.Match` over a collection or table with staged writes. The RF3 schema carries tin declarations, but distributed `==>` execution is not qualified. |
| Typed query engine | Plans over heap and durable sources with explicit work budgets. | Source capabilities and result lifetimes differ. |
| `database/sql` | Embedded VibeDB SQL and transactions. | A bounded dialect; check the SQL reference. |
| PostgreSQL wire | Selected v3 protocol flows and client discovery behavior. | Protocol access does not establish PostgreSQL SQL, catalog, extension, or ORM compatibility. |
| RF3 distributed runtime | Physical nodes with embedded frontends, independent Raft groups, exactly-once write recovery, fused distributed transactions, online hot-shard split, and seamless scale-out and scale-in. | Development software with named fault and scaling qualifications on Linux. No production support, rolling upgrade, or global MVCC snapshot contract. |
| Kubernetes helpers | Fixed disposable Kind qualification topology. | Manifest rendering and preparation, without an operator reconciliation lifecycle. |

The generated [embedded capabilities](capabilities.md) and
[distributed ledger](distributed-feature-state.md) link individual features
to source and tests. Their implementation, integration, and qualification
columns answer different questions. [Deployment readiness](operations/production-readiness.md)
maps each operator workflow to its evidence level.

## Restart and data compatibility

Use the exact writer build to reopen development state. Rolling mixed-build
upgrades, downgrades, cross-build restore, and format migration are not
supported workflows. A successful low-level open does not prove compatibility:
the common build-adoption gate is not wired into every durable open path.

Build identities require matching wire/disk grammar and symmetric capability
agreement; they are not ordered version numbers. Format-0 fixtures are
byte-exact tests for the current grammar, not an archive of supported readers.
The local launcher similarly rejects obsolete development manifest layouts.

Before changing builds, preserve a restorable copy and record its writer
revision. Follow [embedded backup](operations/embedded-backup.md) or the
[RF3 restore protocol](operations/backup-restore.md) for the applicable data,
and read [upgrades and compatibility](operations/upgrades.md). Use disposable
or independently recoverable data during evaluation.

## Platform support

| Capability | Linux | macOS |
| --- | --- | --- |
| Embedded database, all facade profiles | Yes | Yes |
| RF3 preparation, serving, restart, and replica replacement | Yes | Yes; the `portable-rf3` job in `ci.yml` runs on `macos-latest` |
| Read-authority fast path | Default-on for fresh physical-node clusters when the elapsed clock qualifies | Unavailable; reads use quorum `ReadIndex` |
| Process fault, scaling, split, and restore qualifications | Yes | Not run; several process gates skip on Darwin |

Darwin durability uses its strongest implemented barrier; see
[durability](durability.md) for the power-loss boundary on each platform.

## Known limitations

- The root facade is a JSON API. Low-level opaque-value options are not a
  uniform alternative across direct, lazy, and transactional facade writes.
- Tin postings are maintained incrementally across searched generations and
  persisted in a SHA-256-checksummed collection sidecar (`.tin`) for zero-scan
  reopen and incremental catch-up after restarts, but the active index must
  fit in memory. See [search](api/search.md#limitations).
- Online split qualifies base relations end-to-end; global-index cut
  validation and multi-relation retained pruning are wired in
  `splitcontroller`, while end-to-end process qualification for globally
  indexed splits and repeated descendant splits remains pending, and a split
  cannot relieve a single hot key. See [topology changes](design/topology-changes.md#limitations).
- Scaling inputs can be generated from an existing cluster CA with
  `vibedb cluster init-operator` (operator credential and profile) and
  `vibedb cluster init-node` (empty-node preparation manifest and public node
  descriptor). External CA management and multi-host process supervision remain
  operator-provided.
- Controllers run on one designated frontend. A retiring frontend waits for
  clients to disconnect by default, or force-closes lingering sessions after
  `FrontendDrainGracePeriod` when configured.
- Follower applied-floor reads and multi-group read vectors have narrower
  semantics than a linearizable global snapshot.
- Resource budgets cover their named caches, workspaces, or overlays. Process
  memory and disk free-space admission watermarks are available via
  `internal/resourceguard` (`gateway.Options.ResourceGuard`), but OS-level RSS
  and file-descriptor limits still rely on host/container enforcement.

The distributed ledger lists partial qualification for each feature, and
[defaults and limits](reference/limits.md) lists the exact bounds.

## Validation evidence

Use the CI run for the exact revision under evaluation and the
[contribution checks](../CONTRIBUTING.md#root-checks) for local changes.
Platform-specific process, fault, psql, JDBC, and Kind lanes have additional
prerequisites. A skipped or unexecuted lane provides no result.

| Claim | Workflow |
| --- | --- |
| Embedded, SQL, and pgwire unit and race suites | `ci.yml` unit, SQL, SIMD, and race jobs on AMD64 and ARM64 Linux |
| Native RF3 create, restart, and replica replacement | `ci.yml` `portable-rf3` on Linux and macOS |
| Kill, partition, and exact replay of acknowledged writes | `durable-rf3-external.yml`, `durable-rf3-multirelation.yml`, and the `ci.yml` recovery job |
| Physical nodes hosting many RF3 groups | `fused-node-rf3.yml` at three and six nodes |
| Online 3 → 4 → 3 scale under traffic | `seamless-scale-in-out.yml` with the `shared-runner` profile |
| Hot-shard move and automatic split | `ci.yml` hot-shard job and `dev-hot-split.yml` |
| Restore into fresh identities | `restore-rf3-external.yml` and the `ci.yml` activation cuts |
| Clock faults and WAL retention | `clock-fault-matrix.yml`, `wal-retention.yml` |

The [qualification index](qualification/README.md) gives each workflow's
bounds, runner, duration, and artifacts. The seamless-scale latency targets in
its `dedicated` profile are not proven by CI. Dated
[qualification records](qualification/README.md) retain earlier runs and their
scope. The [earlier documentation audit](qualification/documentation-audit-215fb05.md)
is historical; its incomplete root-suite run is not the current build status.

## PostgreSQL compatibility status

The PostgreSQL 18.6 upstream harness has an empty approved regression set.
It records semantic differences; passing the lane does not establish upstream
regression compatibility. The [compatibility harness](../integration/pgcompat/README.md)
explains its failure rules and evidence.

Test the actual statements and discovery queries emitted by your client.
See [pgwire](api/pgwire.md) for selected client support and
[SQL](reference/sql.md) for the executable dialect.

## Performance status

The repository contains [dated benchmark reports](benchmarks/README.md),
including RF3 SQL comparisons and kernel measurements. Each applies to its
recorded revision, workload, hardware, and method. Several record results in
which VibeDB is slower than the comparison system. A short run or one kernel's
speedup does not establish overall capacity or horizontal scaling.

[Performance methodology](performance.md) explains evidence requirements.
The separate [competitive harness registry](../bench/competitive/RESULTS.md)
has no endorsed publication entries.

## License and support

No project license or support window is published in this checkout.
Third-party `LICENSE-*` and `PATENTS-*` files cover incorporated work; see
[source provenance](provenance.md). [Security](../SECURITY.md) describes the
reporting channel limitations and trust boundaries.
