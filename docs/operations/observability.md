# Observe an RF3 cluster

[Documentation](../README.md) / [Operations](README.md) / Observability

VibeDB exposes four observation surfaces. All counters are process-local and
reset on restart; derive rates and alerting externally.

## Observation surfaces

| Surface | Endpoint / trigger | What it exposes |
| --- | --- | --- |
| **Prometheus & probes** | `-metrics-http-listen` (`/metrics`, `/healthz`, `/readyz`) on `vibedb-gateway serve` and `vibedb-shard serve-node` | Prometheus v0.0.4 metrics: gateway routing, lock-free per-P read/write latency histograms, RF3 Raft aggregates, `ResourceGuard` memory/disk watermarks, controller lease status, cluster version window, live `pgwire` sessions & `pg_stat_statements` counters; drain-aware `/readyz` |
| **Slow-query observer** | `-slow-query-threshold <duration>` | Structured stderr log entries for queries exceeding the latency threshold |
| **Native `metrics` RPC** | `{"op":"metrics"}` over mTLS gateway listener (requires `topology` capability) | JSON snapshot of gateway routing, latency histograms, cached per-group/per-node RF3 counters, and move/split controller loops |
| **Node diagnostics** | `kill -USR1 <serve-node-pid>` | Detached JSON record to stderr (`VIBEDB_RF3_DIAGNOSTIC`) and `<node-dir>/rf3-diagnostics.json` (Ready waves, node-log syncs, checkpoints, local/remote dispatch, read authority, storage folds) |

## Prometheus and health endpoints

Enable `-metrics-http-listen` (loopback-only unless TLS is configured):

- `GET /metrics` — Prometheus exposition format covering:
  - Gateway routing (`route_single`, `route_targeted`, `route_scatter`,
    `route_empty`, `shards_fanned`, `rows_returned`, `bytes_returned`,
    `retries`) and cumulative microsecond `read_latency_us` / `write_latency_us`
    histograms.
  - RF3 progress & node service-stage aggregates (proposals, applied/committed
    entries, `ReadIndex` completions, WAL live bytes/syncs, checkpoints, backup
    logical/scan bytes, snapshot transfer, bootstrap).
  - `ResourceGuard` memory and disk free-space watermark gauges and admission
    refusal counters.
  - Active-standby controller lease state and epoch (`internal/controllerlease`).
  - Cluster version window (`internal/clusterversion`).
  - Live `pgwire` connection/session gauges and `pg_stat_statements` execution
    totals.
- `GET /healthz` — Process liveness.
- `GET /readyz` — Drain-aware readiness (fails once frontend drain begins).

## Native `metrics` and `SIGUSR1`

Send canonical JSON `{"op":"metrics"}` over the authenticated native listener
with a `topology`-capable principal. The response includes `metrics`,
`distributed_metrics` (refreshed at most 1/sec via 80/408-byte authenticated
shard-control RPCs; per-group `node_aggregate:false` and per-node
`node_aggregate:true` entries), and `controller_metrics` (move/split loop
passes, completions, faults, pass durations).

For scaling/migration pacing counters (`throttled_calls`, `throttled_bytes`,
`peak_active`, `max_active`), run `vibedb cluster status --json`.

## Interpretation rules

- Metrics are **not** a simultaneous cluster snapshot; each remote member
  sample has its own collection time and saturating `uint64` aggregate.
- Successful live backups intentionally report `backup_scan_bytes = 2 ×
  backup_logical_bytes` (two-pass hash + stream).
- `wal_syncs` counts durability phases (a normal Ready adds 2 phases), not raw
  device flushes.
- Host CPU, RSS, disk space, FD counts, and raw network bytes still require OS
  telemetry alongside VibeDB metrics.

## Source map

- HTTP `/metrics`, `/healthz`, `/readyz` & slow queries: [internal/gatewayruntime/serve_metrics.go](../../internal/gatewayruntime/serve_metrics.go), [cmd/vibedb-shard/serve_node.go](../../cmd/vibedb-shard/serve_node.go)
- Gateway & distributed counters: [gateway/metrics.go](../../gateway/metrics.go), [gateway/distributed_metrics.go](../../gateway/distributed_metrics.go), [internal/servicemetrics/service.go](../../internal/servicemetrics/service.go)
- Node diagnostics & RF3 mapping: [cmd/vibedb-shard/rf3_diagnostics.go](../../cmd/vibedb-shard/rf3_diagnostics.go), [cmd/vibedb-shard/rf3_metrics.go](../../cmd/vibedb-shard/rf3_metrics.go)
