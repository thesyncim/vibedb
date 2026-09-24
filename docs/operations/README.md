# Operator guide

[Documentation](../README.md) / Operations

Run, observe, scale, preserve, and recover VibeDB clusters and embedded
databases. VibeDB is unreleased: every procedure here assumes one exact build
across all processes, and there is no supported production deployment. Start
with [deployment readiness](production-readiness.md) to see which workflows
are development-only and which have CI qualification behind them.

## Plan

| Task | Guide |
| --- | --- |
| Decide what a deployment can rely on today | [Deployment readiness and topology](production-readiness.md) |
| Size hosts, groups, disk, and connections | [Capacity planning](capacity-planning.md) |
| Change builds or move data to a new build | [Upgrades and compatibility](upgrades.md) |

## Run and observe

| Task | Guide | Success looks like |
| --- | --- | --- |
| Start a three-node RF3 cluster on one host | [Local cluster](local-cluster.md) | Readiness line, SQL write and read, same row after restart. |
| Read counters and node diagnostics | [Observability](observability.md) | Identified samples whose scope you understand. |
| Diagnose a failure | [Troubleshooting](troubleshooting.md) | The failed phase and affected identities are known before recovery. |
| Exercise the fixed Kubernetes topology | [Kind qualification](kubernetes.md) | `Kubernetes RF3 qualification passed` with evidence files. |

## Change topology

| Task | Guide | Success looks like |
| --- | --- | --- |
| Add, rebalance, or retire a physical node | [Scale and decommission](scaling.md) | Operation `complete`, or `safe_to_stop=true` with zero blockers before a stop. |
| Understand automatic hot-shard splits | [Hot-shard splits](hot-shard-splits.md) | `split_completed` advances; data stays readable. |
| Tune and observe replica-move pacing | [Online replica migration](migration.md) | Throttle counters rise under load while foreground latency holds. |
| Respond to a lost or restarted node | [Node failure and replacement](node-failure.md) | Every group back to three voters. |
| Install a schema generation | [Schema rollouts](schema-rollouts.md) | Catalog operation `Complete`, every replica drained. |

## Preserve and repair data

| Task | Guide | Input required |
| --- | --- | --- |
| Copy an embedded database | [Embedded backup](embedded-backup.md) | Complete directory after a successful close. |
| Check or rebuild a local store file | [Verify, salvage, and repack](verification.md) | Quiescent source or complete copy. |
| Export a live RF3 cluster and restore it | [Backup and restore](backup-restore.md) | `backup` capability, backup repository, fresh target identities. |

## Rules that apply everywhere

- Keep the operation or request ID of every mutating action. After a timeout
  or lost response, retry with the **same** ID. A missing response does not
  mean the action rolled back.
- Preserve the complete original state, including journals and keys, before
  any recovery step.
- Recover at the failed layer. A file repair cannot grant Raft membership or
  serving authority.
- Do not edit manifests, copy identities between nodes, or delete journals to
  get past a refusal.

## Reference

- [Command-line tools](../reference/cli.md) for flags, defaults, and exit codes.
- [Defaults and limits](../reference/limits.md) for admission bounds.
- [Development protocols](../reference/protocols.md) for wire grammar and
  retry rules.
- [Distributed internals](distributed.md) for routing, quorum, and
  outcome-unknown semantics.
