# Node failure and replacement

[Documentation](../README.md) / [Operations](README.md) / Node failure

Decide what to do when a physical node stops, loses its disk, or cannot be
trusted. Each RF3 group commits through a majority of its three voters, so the
first question is always which groups still have two reachable voters.
[Topology changes](../design/topology-changes.md#replica-moves-and-replacement)
explains how a replacement moves a replica, and
[replication](../design/replication.md#quorum-behavior) explains quorum loss.

## What a failure does

| Reachable voters in a group | Behavior | Operator action |
| ---: | --- | --- |
| 3 | Normal. | None. |
| 2 | Elects and commits with no remaining fault tolerance. | Restore or replace the third replica before any other change. |
| 1 | No commit and no linearizable read. Requests fail, time out, or return outcome-unknown. | Restore a second voter. Never force membership. |
| 0 | Unavailable. | Recover processes and storage. |

A physical node hosts one replica of every group placed on it, so one node
failure drops every such group to two voters at once. In a three-node cluster
that is every group. A second node failure stops all writes and linearizable
reads.

Leadership after a restart is not immediate: the group needs peer traffic and
heartbeats before a restarted replica can lead or serve. Requests that were in
flight when the node stopped can have committed; retry them under their
original identity, as described in [troubleshooting](troubleshooting.md#resolve-an-uncertain-write).

## Local launcher behavior

`vibedb cluster dev` does not tolerate a child failure. If any node process
exits, the supervisor reports `physical node <n> exited`, stops every other
child (SIGTERM, then SIGKILL after 10 seconds), and exits with status 1. It
never restarts a node. To recover:

1. Read the reported exit and any bounded child output. Rerun with
   `--diagnostics-on-exit` if you need the last 64 KiB of each child's log.
2. Fix the cause (disk space, port conflict, killed process).
3. Start the launcher again with the same build, root, and flags. Every node
   reopens its retained state.

You cannot use the launcher to observe a two-voter cluster serving traffic.
The fault behavior above is qualified by separate process tests instead; see
[Evidence](#evidence).

## Restart a node in place

When the node's data directory is intact, restart the same process with the
same build and manifest. It rejoins with its existing identity, replays its
node log, and catches up from the leader. Do not copy another node's
directory, reuse its identity, or edit its manifest to force progress.

**Success check:** the node logs `vibedb-shard RF3 ready`, and a
linearizable read through any frontend returns acknowledged data.

## Replace a lost node

When a node's storage is gone or untrustworthy, replace it with a fresh node
rather than repairing its files:

1. Keep the failed node stopped. Confirm every group still has two voters.
2. Prepare and start a new empty node with a new node ID, then add it with
   `vibedb cluster join`; see [scaling](scaling.md#add-a-node).
3. Decommission the failed node's ID and incarnation with
   `vibedb cluster decommission`, and poll until `safe_to_stop=true`.

This is the only replacement path the current tools support, and it carries
the [scaling prerequisites](scaling.md#prerequisites): an operator credential
and a hand-built empty-node manifest. Decommissioning a node that cannot answer
has not been qualified; the qualified scenario retires a live node.

Membership changes are always learner, catch-up, promote to four voters,
transfer leadership, then remove the source. A group never drops to two
configured voters as part of a replacement. See
[RF3 quorum and replica replacement](distributed.md#rf3-quorum-and-replica-replacement).

## Lose a quorum

If two of a group's three voters are permanently lost, the group cannot make
progress and no command forces it to. Restore from backup into fresh
identities; see [backup and restore](backup-restore.md). Rebuilding a quorum
from one surviving replica's local state is unsupported.

## Evidence

| Scenario | Qualification |
| --- | --- |
| Process kill before a request, racing admission, and after an applied lost reply; asymmetric peer partitions; replica restarts | [Shared-node fault record](../qualification/node-fault-2026-09-04/README.md), `durable-rf3-external.yml` |
| Multi-relation writes under process chaos | `durable-rf3-multirelation.yml` |
| Controller and target restart during a scale operation | `seamless-scale-in-out.yml` |
| Restored replicas serving and failing over | `restore-rf3-external.yml` |
| Kubernetes restart of every StatefulSet | [Kind qualification](kubernetes.md) |

## Limitations

- The launcher has no failure tolerance and no automatic restart.
- Loss of a whole host has not been qualified across separate machines.
- There is no automatic replacement. A failed node stays in the directory
  until an operator decommissions it.
- Device power-loss behavior is not proven by any lane.
- A torn log tail without an external anti-rollback proof keeps the replica
  non-serving. Replace the node rather than editing its files.

## Source map

| Concern | Source |
| --- | --- |
| Launcher child supervision | [cluster_dev.go](../../cmd/vibedb/cluster_dev.go) |
| Membership grant and move sequence | [internal/membershipgrant/grant.go](../../internal/membershipgrant/grant.go), [internal/raftservice/owner.go](../../internal/raftservice/owner.go) |
| Scaling controller | [scaling_controller.go](../../internal/gatewayruntime/scaling_controller.go) |
