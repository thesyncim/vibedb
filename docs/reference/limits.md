# Defaults and limits

[Documentation](../README.md) / [Reference](README.md) · [Stability](../status.md)

Curated defaults and hard bounds by subsystem (binary units: MiB, GiB). Tighter
downstream limits always win. Zero selects a finite default unless noted; only
explicitly documented `-1` fields disable a cap. Byte budgets account logical
payloads/arenas, not total OS RSS.

## Facade, transactions, and full-text search

| Resource | Value | Kind |
| --- | ---: | --- |
| Collection name / key / document | 120 B / 256 B / 4 MiB | hard / default / default |
| Heap chunk size | 64 docs | hard |
| Single-collection Tx batch | 64 docs / 16,793,600 B | default |
| Multi-collection Tx | 16 collections / 256 docs / 67,174,400 B | default |
| Exact serializable read tracking | 4,096 keys & 1 MiB (then coarse); 128 collections | threshold / hard |
| Conflict history per collection | 4,096 written keys | hard |
| Tin full-text indexes | 1 path/index; 64 declarations/collection; 4 cached generations; parallel ≥32,768 docs (≤8 segments) | hard / threshold |

## Query, SQL, and `pgwire`

| Resource | Value | Kind |
| --- | ---: | --- |
| Materialized result | 100,000 rows / 64 MiB (`-1` disables) | default |
| Intermediate / join-pair / heap / exact agg | 64 MiB / 64 MiB / 64 MiB (min 64 KiB) / 16 MiB (min 512 B) | default |
| Recursive CTE / set tree | 1,000 iters, 100k rows, 64 MiB / 1M rows, 64 MiB, depth 256 | default / hard |
| SQL text / placeholders / depth / clause list | 16 MiB / 65,536 / 64 (subquery 32) / 1,024 | hard |
| `database/sql` single param / all params / savepoints | 4 MiB / 16 MiB / 64 | hard |
| `pgwire` startup / body / DataRow / Bind params | 10,000 B / 16 MiB / 16 MiB / 32,767 | hard |
| `pgwire` stmts / portals / simple batch / retained buf | 1,024 / 1,024 / 1,024 / 16 MiB each | hard |
| `pgwire` connections (standalone / dev gateway) | 128 (`-1` disables) / 16 (4 MiB result cap) | default / fixed |

## Routing and gateway profiles

| Resource | Value | Kind |
| --- | ---: | --- |
| Virtual buckets | 20 bits (1,048,576; range 8–24) | default / hard |
| Route candidates / targeted shards / manifest delta | 256 / 64 / 64 shards | default / hard |
| Catalog file / NDJSON request line | 16 MiB / 1 MiB | hard |
| Native RF3 reads: concurrency / bytes / scatter | 256 (max 65,536) / 256 MiB (max 64 GiB) / 16 (max 1,024) | default / hard |

| Operation class | Scatter | Concurrency | Aggregate rows / bytes | Per-shard rows / bytes | Whole / shard deadline |
| --- | --- | ---: | ---: | ---: | ---: |
| `interactive` | no | 4 | 50,000 / 16 MiB | 50,000 / 16 MiB | 5 s / 2 s |
| `batch` | yes | 16 | 1,000,000 / 256 MiB | 500,000 / 128 MiB | 60 s / 30 s |
| `admin` | yes | 32 | 10,000,000 / 1 GiB | 5,000,000 / 512 MiB | 5 min / 2 min |

## Shard services and RF3

| Resource | Value | Kind |
| --- | ---: | --- |
| Serving replicas / manifest groups / execution lanes | 3 / 64 / 8 (power of two, 1–64) | invariant / hard / default |
| Node-log segment / max wave | 32 MiB / 20 MiB | default |
| RF3 native conns / handshakes / frame pool / timeout | 64 / 16 / 112 MiB (max 1 GiB) / 15 s (max 5 min) | default / hard |
| RF3 SQL req / result / work / rows / reservation | 1 MiB / 4 MiB / 8 MiB / 100,000 / 40 MiB | hard |
| Replicated command / result / user relations / mutations | 16 MiB / 16 MiB / 59 / 65,536 | hard |
| Frontend client conns / shard pool conns / replica-ctl | 1,024 (64 hs) / 4,096 (64 hs) / 32 (8 hs) | default |
| Control req/resp / `--wait` / rebalance defaults | 1 MiB / 4 MiB / 24 h / 4,096 moves & 1 GiB | hard / default |
| Migration rates: CPU & disk / net / active / workspace | 64 MiB/s (4 MiB) / 32 MiB/s (2 MiB) / 2 / 16 MiB | default |
| Backup defaults: backups / artifacts / artifact / repo | 16 / 4,096 / 64 GiB / 256 GiB | default |
| Schema rollout: plan / bundle / gw ops / shard storage | 4 MiB / 64 MiB / 64 / 1 GiB (256 records) | hard |
| Exchange mailbox: batch / lifetime / static registry | 65,536 rows & 4 MiB / 16M rows & 1 GiB / 1,024 (512 MiB) | hard / default |

## Durable storage

| Resource | Value | Kind |
| --- | ---: | --- |
| Base page / default max extent / codec hard max | 4,096 B / 64 KiB / 64 MiB | invariant / default / hard |
| Resident page cache / snapshot leases / retired extents | 64 MiB / 1,024 / 65,536 (max 16,777,216) | default / hard |
| Exact index tuple / aliases / physical / page catalog | 4 comps (4,096 B) / 4,096 / 64 / 32 MiB | hard |
| `txn.vtm` decision log | 1 MiB default (16 MiB hard; 64 participants/record) | default / hard |
| Per-group Raft WAL (file / record / live) | 256 MiB / 80 MiB / 128 MiB (max 4 GiB / 96 MiB / 2 GiB) | default / hard |

## Source map

- [vibedb.go](../../vibedb.go) · [query/](../../query/) · [sql/parser.go](../../sql/parser.go) · [pgwire/proto.go](../../pgwire/proto.go)
- [gateway/profile.go](../../gateway/profile.go) · [shardservice/replicated_server.go](../../shardservice/replicated_server.go) · [store/durable/store_file_options.go](../../store/durable/store_file_options.go) · [internal/raftstore/types.go](../../internal/raftstore/types.go)
