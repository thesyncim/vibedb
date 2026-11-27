# Operator guide

[Documentation](../README.md) / Operations · [Stability](../status.md)

Run, observe, scale, back up, and recover VibeDB clusters and embedded
databases. All procedures assume every process runs the exact same build.

## Plan and deploy

| Task | Guide |
| --- | --- |
| Check workflow maturity & topology | [Deployment readiness](production-readiness.md) |
| Size hosts, groups, disk, memory, and watermarks | [Capacity planning](capacity-planning.md) |
| Start, query, and restart a local 3-node RF3 cluster | [Local cluster](local-cluster.md) |
| Change builds safely | [Upgrades and compatibility](upgrades.md) |
| Run the Kind Kubernetes qualification lane | [Kubernetes (Kind)](kubernetes.md) |

## Operate and observe

| Task | Guide |
| --- | --- |
| Scrape Prometheus `/metrics`, `/healthz`, `/readyz`, `SIGUSR1`, slow queries | [Observability](observability.md) |
| Diagnose startup, SQL, quorum, or control failures | [Troubleshooting](troubleshooting.md) |
| Add, rebalance, or decommission physical nodes | [Scaling and decommission](scaling.md) |
| Configure and observe automatic hot-shard splits | [Hot-shard splits](hot-shard-splits.md) |
| Tune and observe replica-move pacing | [Replica migration](migration.md) |
| Handle node restarts, loss, and quorum loss | [Node failure](node-failure.md) |
| Coordinate an RF3 schema generation rollout | [Schema rollouts](schema-rollouts.md) |
| Review RF3 ownership, quorum, and retry invariants | [Distributed internals](distributed.md) |

## Protect and repair data

| Task | Guide |
| --- | --- |
| Back up and restore a closed embedded database | [Embedded backup](embedded-backup.md) |
| Export and restore live RF3 per-group cuts | [RF3 backup and restore](backup-restore.md) |
| Offline verify, salvage, repack, or online compact | [Verification and repair](verification.md) |

## Golden operator rules

- **Keep every operation/request ID:** On timeout or disconnect, retry with the
  **same** ID—never assume rollback.
- **Preserve original state before recovery:** Never delete journals, copy
  identities across nodes, or hand-edit manifests to bypass a refusal.
- **Recover at the failed layer:** File verification never grants Raft
  membership or serving authority.
