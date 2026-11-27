# Deployment readiness and topology

[Documentation](../README.md) / [Operations](README.md) / Deployment readiness

VibeDB is pre-release development software with no production support window.
Use this table to see which operator workflows are CI-qualified and what an
evaluation deployment must provide externally.

## Workflow maturity

| Workflow | Status | CI evidence | Guide |
| --- | --- | --- | --- |
| Local RF3 start, SQL write, restart | Development | `ci.yml` (`portable-rf3`, Linux & macOS) | [Local cluster](local-cluster.md) |
| Process kills, partitions, exact replay | Qualified (Linux) | `durable-rf3-external.yml`, `durable-rf3-multirelation.yml` | [Node failure](node-failure.md) |
| Online 3 → 4 → 3 node scaling | Qualified (Linux) | `seamless-scale-in-out.yml` (`shared-runner`) | [Scaling](scaling.md) |
| Automatic hot-shard split | Qualified (Linux, launcher) | `dev-hot-split.yml` | [Hot-shard splits](hot-shard-splits.md) |
| Live backup & fresh-identity restore | Restore serving/failover qualified (Linux) | `restore-rf3-external.yml` | [Backup & restore](backup-restore.md) |
| Single schema generation rollout | Development | Unit & process tests | [Schema rollouts](schema-rollouts.md) |
| Fixed Kubernetes Kind topology | Qualified (Kind only) | `ci.yml` (`kubernetes-rf3`) | [Kubernetes](kubernetes.md) |
| Embedded backup, verify, salvage, repack | Development | `cmd/vibedb-verify` tests | [Embedded backup](embedded-backup.md), [Verification](verification.md) |
| Rolling mixed-build upgrades | Not supported | Pre-flight version check only (`cluster version`) | [Upgrades](upgrades.md) |

## Physical node topology

Each physical node runs one `vibedb-shard serve-node` process with one shared
node log (up to 64 group replicas in checked-in manifests), replicas for its
assigned RF3 groups, and an embedded frontend.

- **Six mTLS endpoints per node:** Raft peer, RF3 native, snapshot transfer,
  replica control, gateway control, and frontend native client listener.
- **PostgreSQL wire:** Loopback Trust by default, or remote TLS +
  SCRAM-SHA-256 (`PGTLSConfig` + `PGUsers` with SQL-only capabilities).
- **Controller HA:** CAS-fenced active-standby controller lease failover
  (`internal/controllerlease`) is wired into `gatewayruntime` and `serve-node`.
- **PKI & policy tooling:** `vibedb cluster init-operator`, `init-node`,
  `rotate-cert`, and `policy (show|grant|revoke)` manage credentials and
  capabilities from an existing cluster CA.

## Operator responsibilities

1. **Multi-host placement & supervision:** Deploy across failure domains with
   your own process supervisor (`vibedb cluster dev` is single-host and stops
   all nodes on any child exit).
2. **External CA & secret storage:** Manage root CA lifecycle and distribute
   WAL/ACK keys securely.
3. **Exact-build rollouts:** Keep every process on the same commit.
4. **Off-host backups & alerting:** Copy server-local backups off-host; scrape
   `/metrics` and set OS-level RSS/FD limits alongside `ResourceGuard`
   watermarks.

## Source map

- [cmd/vibedb-shard/serve_node.go](../../cmd/vibedb-shard/serve_node.go) · [cmd/vibedb/cluster_dev_physical.go](../../cmd/vibedb/cluster_dev_physical.go) · [internal/serviceauthz/load.go](../../internal/serviceauthz/load.go)
