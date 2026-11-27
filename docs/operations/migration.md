# Online replica migration

[Documentation](../README.md) / [Operations](README.md) / Replica migration

Replica moves (triggered by [scaling](scaling.md) and
[hot-shard splits](hot-shard-splits.md)) stream certified snapshot artifacts
while foreground traffic continues. All groups on a physical node share one
`migrationbudget.Budget`.

## Node budget defaults

Set optional top-level `migration_budget` in the `prepare-node-rf3` manifest
(fixed at node preparation; all groups on a node must agree):

| Class | Sustained rate | Burst |
| --- | ---: | ---: |
| `cpu` (serialization/hashing proxy) | 64 MiB/s | 4 MiB |
| `disk_read` / `disk_write` | 64 MiB/s each | 4 MiB |
| `network_send` / `network_receive` | 32 MiB/s each | 2 MiB |
| `max_active` (concurrent heavy phases) | 2 (range 1–256) | — |
| Transient chunk workspaces | 16 MiB total | — |

Operations larger than `burst_bytes` are segmented so no caller issues an
unbounded I/O. Receivers do not hold active permits while waiting on peer
sockets, avoiding cross-node permit deadlocks.

## Foreground pressure feedback

On shared node-log nodes, a 250 ms sampler scales effective migration rates
automatically without holding foreground locks:

| Signal | High (halve rate to ≥12.5% floor) | Severe (2 consecutive → pause new phases) |
| --- | --- | --- |
| Node-log queue occupancy | ≥75% | ≥90% |
| Ready-queue wait | ≥25 ms | ≥100 ms |
| Backpressure submissions | Any | Any |
| Go scheduler latency p99 | ≥5 ms | Never pauses (throttles only) |

Three consecutive quiet windows (queue ≤25%, no high signal) restore +12.5%
rate and resume paused work.

## Observability and timeouts

- Inspect `vibedb cluster status --json` (`budget.throttled_calls`,
  `throttled_bytes`, `peak_active`, `max_active`) or `Budget.Metrics()`.
- Size operation `--wait` deadlines to cover `shard_bytes / effective_rate`.
  Resumable repository cursors mean reconnects resume from the last durable
  offset—never mint a new operation ID.

## Limitations

- Budget values are fixed at node preparation; pressure thresholds are fixed in
  code.
- Dynamic pressure sampling applies only to shared node-log nodes (per-group
  WAL processes use static rates).
- `CPU` is a byte-work proxy, not an OS cgroup CPU quota.

## Source map

- [internal/migrationbudget/budget.go](../../internal/migrationbudget/budget.go) · [pressure.go](../../internal/migrationbudget/pressure.go)
- [cmd/vibedb-shard/rf3_migration.go](../../cmd/vibedb-shard/rf3_migration.go) · [internal/snapshottransfer/budget_io.go](../../internal/snapshottransfer/budget_io.go)
