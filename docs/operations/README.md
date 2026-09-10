# Operator guide

[Documentation](../README.md) / Operations

Start and inspect a development cluster, preserve its data, and recover from
an interrupted operation. These procedures use one exact build. Cross-build
upgrades and a production deployment lifecycle are not supported; see
[stability and compatibility](../status.md).

## Start and observe

| Task | Procedure | Success looks like |
| --- | --- | --- |
| Run RF3 locally | [Local cluster](local-cluster.md) | Readiness line, successful SQL write and read, then same-build reopen. |
| Inspect activity | [Observability](observability.md) | Identified samples with usable coverage and understood counter scope. |
| Investigate a failure | [Troubleshooting](troubleshooting.md) | The failed phase and exact affected identities are known before recovery. |
| Test Kubernetes restart | [Kind qualification](kubernetes.md) | The prescribed probes and evidence checks complete. |

The local launcher defaults to three physical serving nodes. Each combines a
SQL frontend with Raft and storage. The Kind helper has its own fixed test
topology; `vibedb-operator` renders and prepares manifests rather than running
a reconciliation controller.

## Preserve and maintain data

| Task | Procedure | Input required |
| --- | --- | --- |
| Copy an embedded database | [Embedded backup](embedded-backup.md) | Complete directory after a successful close. |
| Check or rebuild a local store | [Verify, salvage, and repack](verification.md) | Quiescent source or complete quiescent copy. |
| Export a running RF3 cluster | [Backup and restore](backup-restore.md) | Authenticated catalog and replica controls; configured backup repository. |
| Move replicas during scale changes | [Online replica migration](migration.md) | One node-scoped migration budget and retained operation journals. |
| Add, rebalance, or retire a physical node | [Online replica migration](migration.md) and [CLI](../reference/cli.md) | Authenticated operation ID, revision-fenced status, zero blockers, and `safe_to_stop=true` before a stop. |
| Install a schema successor | [Schema rollouts](schema-rollouts.md) | Sealed successor catalog and replica-local bundles. |

For distributed operations, retain the operation ID, canonical request, plan,
and returned proof. After an ambiguous response, resolve the same operation
before creating another one. A missing response does not establish rollback.

## Prepare a recovery change

1. Record the commit, build identities, and affected group or file paths.
2. Preserve the complete original recovery state, including journals and keys.
3. Choose the procedure for the failed layer. A file repair cannot grant Raft
   membership or serving authority.
4. Verify the result at an isolated destination before directing traffic to it.

The [distributed design](distributed.md) explains quorum and retry semantics.
Use [CLI](../reference/cli.md), [limits](../reference/limits.md), and
[protocols](../reference/protocols.md) for exact flags and messages.
