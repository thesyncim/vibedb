# Observe an RF3 cluster

[Documentation](../README.md) / [Operations](README.md) / Observability

VibeDB exposes four observation surfaces. All counters are process-local and
reset on restart; derive rates and alerting externally.

## Observation surfaces

| Surface | Endpoint / trigger | What it exposes |
| --- | --- | --- |
| **Prometheus & probes** | `-metrics-http-listen` (`/metrics`, `/healthz`, `/readyz`) on `vibedb-gateway serve` and `vibedb-shard serve-node` | Prometheus v0.0.4 metrics, cumulative read/write histograms, and drain-aware readiness |
| **SQL live introspection** | `pg_stat_activity`, `pg_stat_statements`, `pg_stat_statements_reset()` over `pgwire` | Active sessions, backend PIDs, and lock-free normalized SQL latency/call/row stats |
| **Slow-query observer** | `-slow-query-threshold <duration>` | Structured stderr log entries for reads/writes exceeding the latency threshold |
| **Native `metrics` RPC & `SIGUSR1`** | `{"op":"metrics"}` (mTLS, `topology` capability) / `kill -USR1 <serve-node-pid>` | JSON gateway/RF3/controller snapshot; detached `<node-dir>/rf3-diagnostics.json` |

## Prometheus `/metrics`, `/healthz`, `/readyz`

Start `vibedb-shard serve-node` or `vibedb-gateway serve` with
`-metrics-http-listen 127.0.0.1:9090` (loopback-only unless TLS is configured;
preserved across `SIGHUP` manifest reloads):

```sh
curl -s http://127.0.0.1:9090/healthz
curl -s http://127.0.0.1:9090/readyz   # returns non-200 once frontend drain begins
curl -s http://127.0.0.1:9090/metrics
```

### Exported Prometheus metrics

| Family | Metric | Type | Description |
| --- | --- | --- | --- |
| Gateway routing | `vibedb_gateway_route_total`, `vibedb_gateway_scatter_reason_total` | counter | Routed operations by class; scatters by reason |
| Gateway volume | `vibedb_gateway_shards_fanned_total`, `vibedb_gateway_rows_returned_total`, `vibedb_gateway_bytes_returned_total`, `vibedb_gateway_retries_total` | counter | Shards fanned, rows/bytes returned, stale-generation retries |
| Gateway latency | `vibedb_gateway_read_duration_seconds`, `vibedb_gateway_write_duration_seconds` | histogram | Lock-free per-P read/write latency distributions |
| RF3 Raft | `vibedb_rf3_samples`, `vibedb_rf3_collection_reads_total`, `vibedb_rf3_collection_faults_total` | gauge / counter | Cached shard-control samples and refresh outcomes |
| RF3 progress | `vibedb_rf3_committed_entries_total`, `vibedb_rf3_applied_entries_total`, `vibedb_rf3_raft_faults_total` | counter | Summed Raft progress and faults across sampled groups |
| Controller & HA | `vibedb_controller_passes_total`, `vibedb_controller_faults_total`, `vibedb_controller_lease_leader`, `vibedb_controller_lease_epoch` | counter / gauge | Reconcile loops and CAS-fenced active-standby lease state (`1`=leader) |
| Cluster version | `vibedb_cluster_binary_version_info`, `vibedb_cluster_active_version_info` | gauge | Running binary version window and negotiated active version |
| `ResourceGuard` | `vibedb_resource_memory_bytes`, `vibedb_resource_memory_limit_bytes`, `vibedb_resource_disk_free_bytes`, `vibedb_resource_disk_min_free_bytes`, `vibedb_resource_watermark_tripped`, `vibedb_resource_admission_rejections_total` | gauge / counter | Hard process memory & free-disk admission watermarks and rejections |
| `pgwire` & SQL | `vibedb_pgwire_sessions`, `vibedb_pgwire_statements_tracked`, `vibedb_pgwire_statement_calls_total`, `vibedb_pgwire_statement_rows_total`, `vibedb_pgwire_statement_exec_seconds_total` | gauge / counter | Active sessions by state and cumulative `pg_stat_statements` totals |

## Live SQL introspection (`pgwire`)

```sql
-- Active connections and running queries
SELECT pid, usename, application_name, state, query
FROM pg_stat_activity ORDER BY pid;

-- Slowest normalized SQL fingerprints
SELECT queryid, calls, total_exec_time, mean_exec_time, rows, query
FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 10;

-- Cancel a running query or terminate a stuck session
SELECT pg_cancel_backend(12345);
SELECT pg_terminate_backend(12345);
SELECT pg_stat_statements_reset();
```

## Native `metrics` and `SIGUSR1`

Send canonical JSON `{"op":"metrics"}` over the authenticated native listener
with a `topology`-capable principal to fetch `metrics`, `distributed_metrics`
(refreshed ≤1/sec via 80/408-byte authenticated shard-control RPCs), and
`controller_metrics`. Send `kill -USR1 <serve-node-pid>` to dump Ready waves,
node-log syncs, checkpoints, local/remote dispatch, read authority, and storage
folds into `<node-dir>/rf3-diagnostics.json`. For migration pacing counters,
run `vibedb cluster status --json`.

## Interpretation rules

- Metrics are **not** a simultaneous cluster snapshot; each remote member
  sample has its own collection time and saturating `uint64` aggregate.
- Successful live backups report `backup_scan_bytes = 2 × backup_logical_bytes`
  (two-pass hash + stream). `wal_syncs` counts durability phases (2 per Ready),
  not raw device flushes.
- Pair VibeDB metrics with host CPU, RSS, disk, FD, and network telemetry.

## Source map

- HTTP `/metrics`, `/healthz`, `/readyz`: [internal/gatewayruntime/http_observability.go](../../internal/gatewayruntime/http_observability.go), [serve_metrics.go](../../internal/gatewayruntime/serve_metrics.go)
- Gateway & distributed counters: [gateway/metrics.go](../../gateway/metrics.go), [gateway/distributed_metrics.go](../../gateway/distributed_metrics.go), [internal/servicemetrics/service.go](../../internal/servicemetrics/service.go)
- Node diagnostics & `pg_stat_*`: [cmd/vibedb-shard/rf3_diagnostics.go](../../cmd/vibedb-shard/rf3_diagnostics.go), [pgwire/catalog_virtual_tables.go](../../pgwire/catalog_virtual_tables.go)
