# Stability and compatibility

[Documentation](README.md) / Stability

VibeDB is unreleased development software. APIs, SQL behavior, wire protocols,
and on-disk formats can change between commits. Evaluate a pinned revision and
keep binaries, documentation, and data directories on the same build.

## Interface maturity

| Interface | Current scope | Primary boundary |
| --- | --- | --- |
| Native Go (`vibedb`) | JSON collections, exact indexes, `tin` full-text indexes, typed queries, serializable transactions | API and disk format can change |
| Full-text search (`tin`) | TINQL `==>` and BM25 `SCORE()` in Go, SQL, and `pgwire`; SHA-256-checksummed `.tin` sidecars | Active index is memory-resident; transactions with staged writes reject `==>`; distributed `==>` not qualified |
| Typed queries (`query`) | Compiled plans over heap and durable stores with bounded workspaces | Source capabilities and result lifetimes differ |
| `database/sql` | Embedded VibeDB SQL, DDL, transactions, savepoints | Bounded dialect; see [SQL reference](reference/sql.md) |
| PostgreSQL wire (`pgwire`) | Selected v3 flows, TLS + SCRAM, `statement_timeout`, `idle_in_transaction_session_timeout`, `pg_stat_activity`, `pg_stat_statements`, `pg_cancel_backend`/`pg_terminate_backend` | Not full PostgreSQL SQL, catalog, extension, or ORM compatibility |
| RF3 cluster | Physical nodes with embedded gateways, independent Multi-Raft groups, exactly-once writes, fused distributed transactions, online splits, online 3↔4 scaling, active-standby controller lease | Development-grade; Linux process qualifications; no rolling mixed-build upgrades or global MVCC snapshot |
| Kubernetes helpers | Fixed disposable Kind qualification topology | Manifest rendering/validation only; no reconciling operator |

Generated ledgers link features directly to code and tests:
[embedded capabilities](capabilities.md) and
[distributed feature state](distributed-feature-state.md). See
[deployment readiness](operations/production-readiness.md) for operator
evidence levels.

## Build and data compatibility

- Reopen data only with the exact commit that wrote it. Rolling mixed-build
  upgrades, downgrades, and cross-build restore are not yet qualified
  end-to-end (`internal/clusterversion` and `vibedb cluster version --check-peers`
  provide pre-flight version-window checks).
- Build identities require matching wire/disk grammar and symmetric capability
  agreement.
- Always take a restorable backup ([embedded](operations/embedded-backup.md) or
  [RF3](operations/backup-restore.md)) before changing builds; see
  [upgrades](operations/upgrades.md).

## Platform support

| Capability | Linux | macOS |
| --- | --- | --- |
| Embedded engine (all profiles) | Yes | Yes |
| RF3 create, serve, restart, replica replacement | Yes | Yes (`ci.yml` `portable-rf3`) |
| Quorum read-authority fast path | Default-on when clock qualifies | Unavailable (falls back to `ReadIndex`) |
| Process fault, scaling, split, and restore qualifications | Yes | Not run (process gates skip on Darwin) |

See [durability](durability.md) for per-platform sync barriers.

## Known limitations

- **JSON-only facade:** Root `vibedb` APIs require valid JSON documents;
  low-level opaque-value options are not uniform across facade write paths.
- **Full-text memory:** Incremental `.tin` sidecars enable zero-scan reopen,
  but active postings must fit in RAM ([search](api/search.md#limitations)).
- **Online splits:** Base-relation splits are process-qualified; globally
  indexed splits and repeated descendant splits lack end-to-end process
  qualification, and a split cannot relieve a single hot key
  ([topology changes](design/topology-changes.md#limitations)).
- **Cluster PKI & supervision:** `vibedb cluster init-operator`, `init-node`,
  `rotate-cert`, and `policy` bootstrap credentials from an existing CA, while
  external CA lifecycle and multi-host process supervision remain
  operator-provided.
- **No global MVCC cut:** Multi-group reads combine independent per-group cuts;
  follower applied-floor reads are weaker than linearizable reads.
- **Resource bounds:** `internal/resourceguard` (`gateway.Options.ResourceGuard`)
  enforces process memory and disk admission watermarks; hard OS-level RSS and
  FD ceilings still require host/container limits. See
  [defaults and limits](reference/limits.md).

## Verification and compatibility

- **CI qualification:** See [qualification workflows](qualification/README.md)
  for every workflow's bounds, runner, and pass criteria. Note that
  `seamless-scale-in-out.yml` runs the `shared-runner` profile in CI, not the
  stricter `dedicated` hardware latency targets.
- **PostgreSQL 18.6 harness:** The nightly harness in
  [`integration/pgcompat`](../integration/pgcompat/README.md) tracks semantic
  differences; it does not certify upstream PostgreSQL compatibility.
- **Benchmarks:** See [performance](performance.md) and
  [benchmark methods](benchmarks/README.md). The
  [competitive registry](../bench/competitive/RESULTS.md) has no endorsed
  entries.
- **License and security:** No project license is published; third-party
  `LICENSE-*` files apply to incorporated work ([provenance](provenance.md)).
  See [Security](../SECURITY.md) for reporting and trust boundaries.
