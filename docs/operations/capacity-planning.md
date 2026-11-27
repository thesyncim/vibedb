# Capacity planning and resource bounds

[Documentation](../README.md) / [Operations](README.md) / Capacity planning

VibeDB enforces explicit admission bounds on queues, caches, frames, and
watermarks, but publishes no universal throughput or sizing model. Measure on
your target workload and set OS-level RSS, disk, and file-descriptor limits.

## Admission watermarks (`ResourceGuard`)

Both `vibedb-gateway serve` and `vibedb-shard serve-node` expose hard process
memory and disk free-space admission watermarks via `internal/resourceguard`:

- `-max-memory-bytes` — Rejects new work when process memory exceeds threshold.
- `-min-free-disk-bytes` + `-disk-watermark-path` — Rejects writes when free
  disk space falls below threshold.

Metrics and refusal counts are exported on Prometheus `/metrics`.

## Key per-node bounds

| Resource | Default / limit | Scope |
| --- | ---: | --- |
| Group replicas per node | 64 (checked-in manifests) | Physical node (3 internal + tables + split children) |
| Shared node log | 32 MiB segments, 20 MiB max wave | Physical node (segments reclaim after checkpoint) |
| Per-group WAL (standalone) | 256 MiB file, 80 MiB record, 128 MiB live (max 4 GiB / 96 MiB / 2 GiB) | Group replica |
| RF3 native in-flight frames | 112 MiB (40 MiB worst-case SQL reservation) | Shard process |
| Native read in-flight bytes | 256 MiB | Frontend |
| Durable collection page cache | 64 MiB | Collection |
| Query result / intermediate / join / heap | 64 MiB each | Statement |
| Migration workspace & rates | 16 MiB workspace; 64 MiB/s CPU/disk, 32 MiB/s net, 2 active phases | Physical node |
| Schema artifact retention | 1 GiB | Shard process until drain |

## Listener connection limits

| Listener | Max connections | Concurrent TLS handshakes |
| --- | ---: | ---: |
| Frontend native client | 1,024 | 64 |
| RF3 native service | 64 | 16 |
| Replica control | 32 | 8 |
| Frontend → shard pool (each) | 4,096 | 64 |
| Loopback dev `pgwire` | 16 | — |

Size host `ulimit -n` well above the sum across processes on the machine. Moving
a shard of size $S$ takes at least `S / 32 MiB/s` on network alone (longer
under foreground pressure backoff, down to 12.5% rate). See
[defaults and limits](../reference/limits.md) for the complete table.

## Source map

- [cmd/vibedb-shard/rf3_manifest.go](../../cmd/vibedb-shard/rf3_manifest.go) · [internal/resourceguard/](../../internal/resourceguard/) · [internal/migrationbudget/budget.go](../../internal/migrationbudget/budget.go)
