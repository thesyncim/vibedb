# Stability and compatibility

[Documentation](README.md) / Stability

VibeDB is an unreleased development project. There is no stable Go API, SQL
contract, wire protocol, or cross-commit disk format. Evaluate a pinned
revision and keep the corresponding binaries, documentation, and recovery
state together.

## Status by interface

| Interface | Available for evaluation | Main boundary |
| --- | --- | --- |
| Native embedded Go | JSON collections, exact indexes, typed queries, serializable transactions. | API and data formats can change. |
| Typed query engine | Plans over heap and durable sources with explicit work budgets. | Source capabilities and result lifetimes differ. |
| `database/sql` | Embedded VibeDB SQL and transactions. | A bounded dialect; check the SQL reference. |
| PostgreSQL wire | Selected v3 protocol flows and client discovery behavior. | Protocol access does not establish PostgreSQL SQL, catalog, extension, or ORM compatibility. |
| RF3 runtime | Replicated groups, physical-node composition, routing, and recovery primitives. | Development and fault qualification; no production support or global MVCC snapshot contract. |
| Kubernetes helpers | Fixed disposable Kind qualification topology. | Manifest rendering and preparation, without an operator reconciliation lifecycle. |

The generated [embedded capabilities](capabilities.md) and
[distributed ledger](distributed-feature-state.md) link individual features
to source and tests. Their implementation, integration, and qualification
columns answer different questions.

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
[RF3 restore protocol](operations/backup-restore.md) for the applicable data.
Use disposable or independently recoverable data during evaluation.

## Known limitations

- RF3 preparation uses sealed recovery journals that require Linux strict
  allocation support. Native macOS preparation fails closed; this is separate
  from the embedded API, whose default example can run on macOS.
- The root facade is a JSON API. Low-level opaque-value options are not a
  uniform alternative across direct, lazy, and transactional facade writes.
- The competitive `mixedsuite` summary header does not describe every emitted
  grouping field. Use raw per-run rows; see [performance methodology](performance.md#known-mixedsuite-output-defect).
- Resource budgets cover their named caches, workspaces, or overlays. They do
  not establish a fixed total process-memory ceiling.
- Follower applied-floor reads and multi-group read vectors have narrower
  semantics than a linearizable global snapshot.

## Validation evidence

Use the CI run for the exact revision under evaluation and the
[contribution checks](../CONTRIBUTING.md#root-checks) for local changes.
Platform-specific process, fault, psql, JDBC, and Kind lanes have additional
prerequisites. A skipped or unexecuted lane provides no result.

[Qualification records](qualification/README.md) retain dated runs and their
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
recorded revision, workload, hardware, and method. A short run or one kernel's
speedup does not establish overall capacity or horizontal scaling.

[Performance methodology](performance.md) explains evidence requirements.
The separate [competitive harness registry](../bench/competitive/RESULTS.md)
has no endorsed publication entries.

## License and support

No project license or support window is published in this checkout.
Third-party `LICENSE-*` and `PATENTS-*` files cover incorporated work; see
[source provenance](provenance.md). [Security](../SECURITY.md) describes the
reporting channel limitations and trust boundaries.
