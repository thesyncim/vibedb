# Node failure and replacement

[Documentation](../README.md) / [Operations](README.md) / Node failure

Each RF3 group commits via a majority (2 of 3 voters). When a physical node
fails, check which groups retain at least 2 reachable voters before taking
action.

## Quorum impact

| Reachable voters | Group behavior | Action |
| ---: | --- | --- |
| 3 | Normal | None |
| 2 | Commits with no remaining fault tolerance | Restart or replace the lost node promptly |
| 1 | No commits or linearizable reads | Restore a 2nd voter; never force membership |
| 0 | Unavailable | Recover processes/storage or restore from backup |

In a 3-node cluster, losing 1 node drops every group to 2 voters; losing a 2nd
node halts writes and linearizable reads.

## Recovery procedures

- **Local launcher (`vibedb cluster dev`):** Exits all nodes if any child
  stops. Fix the cause (disk, port, signal) and rerun the exact same launcher
  command and root (add `--diagnostics-on-exit` for child log tails).
- **Intact storage (restart in place):** Restart the same `serve-node` process
  with the same build and manifest. Wait for `vibedb-shard RF3 ready` and
  confirm a linearizable read. Never copy another node's directory or edit
  manifests.
- **Lost or untrusted storage (replace node):**
  1. Keep the failed node stopped and confirm every group still has 2 voters.
  2. Prepare and start a fresh node with a new NodeID (`vibedb cluster init-node`),
     then run `vibedb cluster join` ([scaling](scaling.md#add-a-node-join)).
  3. Decommission the failed node ID and incarnation with
     `vibedb cluster decommission` until `safe_to_stop=true`.
- **Permanent quorum loss (2+ voters lost):** Restore from backup into fresh
  identities ([backup and restore](backup-restore.md)). Rebuilding quorum from
  a single replica's local disk is unsupported.

Uncertain in-flight writes may have committed—reconcile under the original
request ID ([troubleshooting](troubleshooting.md#resolve-an-uncertain-write)).

## Limitations

- No automatic dead-node replacement; failed nodes remain in the directory
  until explicitly decommissioned (decommissioning an unreachable node is not
  yet end-to-end qualified).
- Torn-slot WAL quarantines require node replacement rather than local repair.

## Source map

- [cmd/vibedb/cluster_dev.go](../../cmd/vibedb/cluster_dev.go) · [internal/membershipgrant/grant.go](../../internal/membershipgrant/grant.go) · [internal/gatewayruntime/scaling_controller.go](../../internal/gatewayruntime/scaling_controller.go)
- CI gates: [`durable-rf3-external.yml`](../../.github/workflows/durable-rf3-external.yml), [`durable-rf3-multirelation.yml`](../../.github/workflows/durable-rf3-multirelation.yml)
