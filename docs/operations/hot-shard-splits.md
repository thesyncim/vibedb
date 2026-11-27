# Hot-shard splits

[Documentation](../README.md) / [Operations](README.md) / Hot-shard splits

Hot data shards split automatically into three-replica child groups while
traffic continues. There is no manual split or merge command. Qualified on
Linux via `dev-hot-split.yml`. See
[topology changes](../design/topology-changes.md#hot-shard-splits) for the
cutover pipeline.

## Trigger policy

1. Each shard tracks a 7-dimension per-window load vector: write CPU, read CPU,
   scan CPU, I/O, requests, latency debt, and live bytes.
2. The active controller publishes a pressure cut every `hot-shard-interval`
   (default 1 s).
3. A shard triggers a split when its dominant resource is ≥90% of window
   capacity in **6 of the last 8 windows**, promises ≥10% benefit, and is not
   in its 8-window cooldown.
4. At most one split (and one replica move) is admitted per pressure cut.

## Capacity configuration

Pass `-hot-shard-capacity <path>` to the gateway (canonical `vibejson` format
1, ≤1 MiB) specifying `recorder_lanes`, `window_capacity`, `node_capacity`,
`migration_capacity`, `shard_migration_bytes`, `max_receives`, and per-node
overrides (`nodes`).

`vibedb cluster dev` writes a synthetic `hot-shard-capacity.vibejson` (64
units/window → ~58 ops/s trips the 90% threshold) to exercise splits locally;
measure and supply real capacity numbers for realistic workloads.

## Observe splits

- **Controller counters:** Native `{"op":"metrics"}` (or Prometheus `/metrics`)
  exposes `split_passes`, `split_discovered`, `split_triggered`,
  `split_completed`, and `split_faults`.
- **Shard counters:** `split_control_requests`, `split_control_completions`,
  `split_control_faults`.
- **Success check:** `split_completed` advances while existing keys remain
  readable.

## Limitations

- Automatic only (no manual split, pause, cancel, or cold-shard merge).
- Each child consumes a group slot on 3 nodes (64-group manifest ceiling).
- Cannot split a single hot key or the request-ledger group; globally indexed
  splits are not yet end-to-end process-qualified.

## Source map

- [autosplit/tracker.go](../../autosplit/tracker.go) · [internal/hotshard/controller.go](../../internal/hotshard/controller.go) · [internal/hotshard/static_capacity.go](../../internal/hotshard/static_capacity.go)
- [internal/topologyscheduler/admission.go](../../internal/topologyscheduler/admission.go) · [dev-hot-split.yml](../../.github/workflows/dev-hot-split.yml)
