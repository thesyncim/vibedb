# Observe an RF3 development cluster

[Documentation](../README.md) / [Operations](README.md) · [Development status](../status.md)

## Choose an observation path

| Need | Surface | Scope |
| --- | --- | --- |
| Routing and cached RF3 progress | Authenticated native `metrics` request | Gateway counters and independently collected member samples. |
| Physical-node persistence and local dispatch work | SIGUSR1 node diagnostics | Detached counters from one serving process and its current group inventory. |
| CPU, RSS, disk capacity, and network traffic | Host monitoring or a recorded profile | Operating-system measurements outside the gateway metrics response. |

### Collect a physical-node diagnostic

On a supported Unix host, identify the exact `vibedb-shard serve-node` PID in
your process supervisor, then send that process `SIGUSR1`. Do not signal the
launcher or an unrelated process. The serving process emits a bounded JSON
diagnostic and publishes `rf3-diagnostics.json` in the directory of its serving root.

The record includes PID, NodeID, group count, Ready waves, node-log append
barriers, checkpoint activity, local/remote dispatch, and storage-overlay fold
counters. Diagnostics use detached observations; they do not request a storage
snapshot, flush, or checkpoint. The diagnostic file itself is written and synced.

Compare samples only for the same process and compatible group/schema
inventory. Inspect availability, coverage, failure, and overflow fields before
using storage counters. A provider change can reset collection counters within
one PID. Keep raw samples when deriving rates or per-operation costs.

## Read the current counter snapshot

Send the closed canonical request over the gateway's authenticated native
listener:

```json
{"op":"metrics"}
```

The peer needs `topology` capability. No SQL, parameters, request identity,
class, routing hint, or result budget may accompany the operation. The composed
distributed path uses TLS; explicit development plaintext is limited to the
loopback-only listener.

The response can contain three objects:

| Object | Present when | Scope |
| --- | --- | --- |
| `metrics` | Always | Routing/fan-out counters from this gateway process |
| `distributed_metrics` | Replicated routes and shard-control collection are configured | Cached per-group/per-node RF3 cuts plus a saturating aggregate |
| `controller_metrics` | Move/split controllers are configured | Loop counters from this gateway process |

All values are unsigned 64-bit integers except `overflow` and identity fields.
They are raw counters/gauges, not rates. Derive rates and retain source/process
identity outside VibeDB.

## Gateway routing counters

`metrics` has this fixed field set:

| Field | Meaning |
| --- | --- |
| `route_single` | operations routed to one shard |
| `route_targeted` | operations routed to a bounded shard subset |
| `route_scatter` | scatter operations |
| `route_empty` | operations with no route |
| `shards_fanned` | cumulative selected-shard count |
| `rows_returned` | rows reported by completed results |
| `bytes_returned` | encoded result bytes reported by completed results |
| `retries` | stale-generation retries |
| `scatter_all_shards` | scatters whose resolved route covered every shard |
| `scatter_unknown_route` | scatters caused by no usable bound prefix |

These counters start with the gateway process. They do not include client wire
bytes, shard-control traffic, Raft traffic, retries below this accounting point,
or storage I/O.

## Distributed RF3 snapshot

Group-specific sampling requires a provider implementing `GroupProvider`.
A provider without that interface returns `ErrMetrics` for a group request.

The gateway fixes the group/member and unique-node sample directory from its
startup routes. A bounded worker set periodically performs authenticated
shard-control reads. Each internal request is exactly 80 bytes and each response
is exactly 408 bytes.

`distributed_metrics.members` contains two kinds of entries:

- `node_aggregate:false`: one exact group/member/node identity and nine RF3
  progress counters;
- `node_aggregate:true`: one physical process/node and 27 service-stage values.
  Its group is zero and member is zero.

Do not merge entries by a display label or infer a missing member. Complete
cluster, incarnation, topology-recovery epoch, shard, group, member, and node
identity define a group sample.

The aggregate includes `samples`, `collection_reads`, `collection_faults`, and
`overflow`. RF3 progress is summed only from group entries; service stages are
summed only from node aggregates, avoiding duplicate process-stage accounting.
Addition saturates at `uint64` maximum and sets `overflow=true`.

### RF3 progress fields

| Fields | Exact scope |
| --- | --- |
| `proposal_commands`, `proposal_bytes` | commands/bytes observed by the RF3 proposal path |
| `applied_entries` | entries applied by the state machine |
| `ready_persisted` | durable Ready-processing steps; not quorum acknowledgements |
| `snapshots_finished` | completed snapshot applies |
| `read_completions` | completed ReadIndex operations |
| `raft_faults` | faults counted by this RF3 runtime |
| `quorum_commit_advancements`, `committed_entries` | authoritative commit-index movement and entry count; no latency or byte total |

### Node service-stage fields

| Area | Fields |
| --- | --- |
| Checkpoint | `checkpoint_applied`, `checkpoints`, `physical_checkpoints`, `checkpoint_barrier_syncs` |
| WAL | `wal_live_bytes`, `wal_entries`, `wal_syncs` (completed durability phases, including the Ready record barrier and final current-slot sync) |
| Live backup | `backup_requests`, `backup_faults`, `backup_logical_bytes`, `backup_scan_bytes` |
| Snapshot transfer | `snapshot_transfer_chunks`, `snapshot_transfer_bytes`, `snapshot_resident_bytes` |
| Replica action | `replica_action_requests`, `replica_action_completions`, `replica_action_faults` |
| Split control | `split_control_requests`, `split_control_completions`, `split_control_faults` |
| Cold bootstrap | `bootstrap_requests`, `bootstrap_chunks`, `bootstrap_bytes`, `bootstrap_completions`, `bootstrap_faults`, `bootstrap_resident_bytes`, `bootstrap_inflight` |

`wal_live_bytes`, resident-byte fields, and in-flight work are gauges and may
decrease. Most event counts reset on process restart. Collection read/fault
counters describe gateway refresh attempts, not shard health authority.

### Backup double-scan accounting

A successful live backup pins one immutable group snapshot, scans it once to
derive exact geometry/hash, then scans the same cut again while streaming. On
success the service adds:

```text
backup_logical_bytes += artifact_bytes
backup_scan_bytes    += 2 * artifact_bytes
```

The 2× value is intentional read amplification. It does not mean two repository
copies or twice the network output. These byte counters advance only after the
complete export succeeds; partial failed scan work is represented by
`backup_faults`, not necessarily by proportional scan bytes.

## Controller loop counters

`controller_metrics` exposes move and split loop activity:

- move: `move_passes`, `move_discovered`, `move_advanced`, `move_completed`,
  `move_faults`, `move_duration_ns`, `move_duration_max_ns`;
- split: `split_passes`, `split_discovered`, `split_triggered`,
  `split_completed`, `split_faults`, `split_duration_ns`,
  `split_duration_max_ns`.

Durations cover controller passes in this gateway incarnation. They are not
per-operation phase latency, queue delay, foreground latency, or a replicated
operation clock. The replicated catalog records remain recovery/topology
authority.

## Interpretation limits

The metrics response is **not** a simultaneous cluster snapshot. Each remote
sample has its own collection time and exact per-source cut. The aggregate is a
telemetry sum, not a quorum certificate, common applied position, or global
consistency witness.

The current surface does not expose:

- latency histograms, percentiles, traces, or SLO/error-budget calculations;
- total client, Raft, shard-control, backup, or snapshot-transfer network bytes;
- filesystem, block-device, or media-write totals;
- leader identity, catalog age, controller queue depth, or exact replicated
  split/move/backup/restore phase;
- quorum, apply, checkpoint, response-settlement, failover, or recovery latency;
- CPU, file descriptors, process RSS, disk capacity, or host health; or
- durable counters across restart.

In particular, `proposal_bytes` is not total network traffic,
`snapshot_transfer_bytes` is not all cluster traffic, and `wal_syncs` counts
durability phases—not `File.Sync` calls, device writes, or media flushes. A
normal nonempty Ready contributes two phases. A completion count is not a
duration. Build alerts only from semantics the source actually exports.

Collection failure increments explicit fault counters and leaves the last cached
sample; it never changes routing, membership, cleanup, acknowledgement, split,
or move authority.

The [diagnostics qualification record](../qualification/fused-diagnostics-2026-09-04/README.md)
describes the storage-sampling checks.

## Source map

| Boundary | Source |
| --- | --- |
| Physical-node diagnostics | [rf3_diagnostics.go](../../cmd/vibedb-shard/rf3_diagnostics.go) |
| Gateway route/fan-out counters | [`gateway/metrics.go`](../../gateway/metrics.go) |
| Fixed sample directory, refresh, and saturating aggregate | [`gateway/distributed_metrics.go`](../../gateway/distributed_metrics.go) |
| Public request validation and exact response fields | [`internal/gatewayruntime/serve_metrics.go`](../../internal/gatewayruntime/serve_metrics.go) |
| Move/split controller counters | [`internal/gatewayruntime/controller_metrics.go`](../../internal/gatewayruntime/controller_metrics.go) |
| Fixed 80/408-byte authenticated RF3 exchange | [`internal/servicemetrics/service.go`](../../internal/servicemetrics/service.go) |
| Mapping runtime/WAL/checkpoint/service counters to node stages | [`cmd/vibedb-shard/rf3_metrics.go`](../../cmd/vibedb-shard/rf3_metrics.go) |
| Backup requests, faults, logical bytes, and double-scan bytes | [`internal/clusterbackupservice/service.go`](../../internal/clusterbackupservice/service.go) |
