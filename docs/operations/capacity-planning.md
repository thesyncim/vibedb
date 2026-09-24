# Capacity planning and resource bounds

[Documentation](../README.md) / [Operations](README.md) / Capacity planning

Size hosts for an RF3 evaluation cluster from the bounds VibeDB enforces and
the ones it does not. VibeDB bounds many named queues, caches, and
reservations. It does not bound total process memory, file descriptors, or
disk use, and it publishes no capacity model. Measure your own workload.

## What is and is not bounded

| Resource | Bounded by VibeDB? | Where to look |
| --- | --- | --- |
| Group replicas per physical node | Yes: 64 in checked-in manifests | [Groups per node](#groups-per-node) |
| Raft log geometry | Yes: fixed at preparation | [Disk](#disk) |
| Native read reservations, RF3 frames, SQL result sizes | Yes: admission accounts | [Limits](../reference/limits.md#shard-services-and-rf3) |
| Migration I/O and network rate | Yes: per-node budget | [Online replica migration](migration.md) |
| Backup repository bytes | Yes: gateway flags | [Backup and restore](backup-restore.md#configure-live-backup) |
| Process RSS | No | Host monitoring |
| Table data on disk | No | Host monitoring |
| File descriptors and sockets | Only per listener | Host limits |
| CPU | No quota; migration backs off on scheduling latency | Host monitoring |

A byte budget accounts logical payload or promised buffers. It is not RSS: the
Go allocator can keep freed pages, and unrelated work shares the heap.

## Groups per node

Each table, split child, and internal group (catalog, request ledger, the
built-in data table) has three replicas on three distinct nodes. The
checked-in node manifests accept at most 64 group replicas per node, and the
local launcher prepares every node log with room for 64 groups.

On a three-node cluster every group has a replica on every node, so the
cluster holds at most 64 groups in total, three of which are internal. Every
automatic [hot-shard split](hot-shard-splits.md) adds groups. Six nodes spread
replicas across subsets, which raises the total but not the per-node ceiling.

## Disk

Raft log geometry is fixed at preparation and must match on reopen. Which
bounds apply depends on the layout:

| Manifest configures | Log | Default geometry |
| --- | --- | --- |
| `node_log` (every local-launcher node) | One shared, segmented node log for all groups on the node | 32 MiB segments; 20 MiB maximum write wave |
| Per-group `wal` only | One WAL per group replica | 256 MiB file, 80 MiB record, 128 MiB live bytes; absolute maxima 4 GiB, 96 MiB, 2 GiB |

The node log reclaims segments after checkpoints; no single setting caps its
total size. SQL apply state (the table data) lives beside the log and grows
with your data; nothing bounds it.
Snapshot transfer stages artifacts on the target during a move. Schema
installation keeps up to 1 GiB of artifacts per shard process until drain.

The Kind qualification enforces, per process after a restart cycle with a tiny
workload, RSS at most 1 GiB, apparent durable bytes at most 1 GiB, and WAL
bytes at most 512 MiB. Those are test ceilings for that workload, not sizing
guidance.

## Memory

Budgets that commonly dominate a serving node:

| Account | Default | Scope |
| --- | ---: | --- |
| RF3 native in-flight frames | 112 MiB | One shard process, shared by all its groups |
| Worst-case RF3 SQL execution reservation | 40 MiB | One request; two fit in the default frame account |
| Native read in-flight bytes | 256 MiB | One frontend |
| Durable collection page cache | 64 MiB | One durable collection |
| Migration transient workspaces | 16 MiB | One physical node |
| Query result, intermediate, join, heap workspaces | 64 MiB each | One statement |

Add Go runtime overhead, connection buffers, TLS state, and allocator
retention. The [limits reference](../reference/limits.md) lists every
documented bound.

## Connections and listeners

| Listener | Connections | Concurrent TLS handshakes |
| --- | ---: | ---: |
| Frontend native client listener | 1,024 | 64 |
| RF3 native service | 64 | 16 |
| Replica control | 32 | 8 |
| Frontend to shard pools (each) | 4,096 | 64 |
| Loopback development pgwire | 16 | n/a |

Set the host file-descriptor limit well above the sum for the processes on
that host.

## Network and migration

Snapshot moves are paced per node: 64 MiB/s for serialization, disk read, and
disk write, and 32 MiB/s each for network send and receive, with two
concurrent heavy phases. Moving a shard of size S takes at least
`S / 32 MiB/s` on the network alone. Foreground pressure can reduce the rate
to 12.5% of the configured value or pause new phases; see
[migration](migration.md). A long `--wait` on a scaling operation should
cover that time.

## Limitations

- No capacity model, benchmark-derived sizing table, or throughput promise is
  published. Dated [benchmarks](../benchmarks/README.md) apply to their
  recorded revision and hardware only.
- Budgets do not add up to a process ceiling.
- The 64-group ceiling is a manifest bound, not a measured limit on how many
  groups one node can serve well.

## Source map

| Concern | Source |
| --- | --- |
| Groups per manifest | [rf3_manifest.go](../../cmd/vibedb-shard/rf3_manifest.go) (`maxRF3ManifestGroups`) |
| Launcher node-log group room | [cluster_dev_physical.go](../../cmd/vibedb/cluster_dev_physical.go) |
| WAL and node-log geometry | [internal/raftstore/types.go](../../internal/raftstore/types.go), [internal/raftstore/node_store.go](../../internal/raftstore/node_store.go) |
| RF3 frame account | [shardservice/replicated_server.go](../../shardservice/replicated_server.go) |
| Listener limits | [serve_rf3.go](../../cmd/vibedb-shard/serve_rf3.go), [serve_node.go](../../cmd/vibedb-shard/serve_node.go) |
| Migration defaults | [internal/migrationbudget/budget.go](../../internal/migrationbudget/budget.go) |
