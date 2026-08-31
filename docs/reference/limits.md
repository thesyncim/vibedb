# Defaults and limits

> [!CAUTION]
> **Unreleased and unstable.** These values describe the current source tree,
> not public compatibility contracts, capacity guarantees, SLOs, or SLAs. Any
> default or hard limit may change at any commit. Pin one exact build and verify
> the effective configuration of every process.

This page is a curated operating reference. It names bounds that commonly
explain admission failures or memory/disk planning; it is not an inventory of
every internal constant. MiB and GiB are binary units.

## How to read a bound

- **Default** is selected by a zero or omitted value only at the named layer.
- **Hard** cannot be enlarged by configuration at that layer.
- **Profile** is a built-in operational choice, not measured capacity.
- Smaller limits compose. A 4 MiB RF3 result bound still applies behind a
  pgwire parser that accepts a 16 MiB frontend message.
- Zero frequently selects a finite default, but elsewhere means absent,
  disabled, invalid, or the strongest enum value. Never assume zero means
  unlimited. Only a field that explicitly documents `-1` as unlimited has that
  meaning.
- Byte budgets usually account logical payload, retained arenas, or promised
  buffers. They are not exact process RSS or allocator commitments.
- A deadline is an admission/execution default, not a latency objective.

## Facade and transaction defaults

| Resource | Current value | Kind | Layer / source |
| --- | ---: | --- | --- |
| collection name | 120 UTF-8 bytes; nonempty | hard | facade, `internal/collectionname/collectionname.go` |
| key | 256 bytes | default | facade and durable collection, `vibedb.go` |
| document | 4 MiB; nonempty | default | facade and durable collection, `vibedb.go` |
| documents per chunk | 64 | default and hard | heap source model, `store/engine.go` |
| one collection batch | 64 documents; 16,793,600 bytes with default key bound | default | facade, `vibedb_txn.go` |
| multi-collection transaction | 16 collections; 256 documents; 67,174,400 bytes | default | facade, `vibedb.go` |
| exact serializable read tracking | 4,096 keys and 1 MiB before coarse dependency tracking | threshold | native transaction, `vibedb_txn.go` |
| serializable collection dependencies | 128 | hard | native transaction, `vibedb_txn.go` |

Crossing the key/byte tracking threshold does not make a transaction
non-serializable; it escalates that collection to a coarser dependency. The
collection-count ceiling is a refusal.

## Query execution

| Resource | Zero-value default | Explicit boundary | Layer / source |
| --- | ---: | --- | --- |
| materialized result | 100,000 rows; 64 MiB | `-1` disables the selected row/byte bound | query, `query/result_budget.go` |
| statement intermediate relations | 64 MiB | `-1` disables | query, `query/relation_runtime.go` |
| join pair workspace | 64 MiB | `-1` disables | query, `query/join_pair_budget.go` |
| heap execution workspace | 64 MiB | minimum 64 KiB; no `-1` opt-out | query, `query/heap_work_budget.go` |
| exact aggregate workspace | 16 MiB | minimum 512 bytes; no `-1` opt-out | query, `query/aggregate.go` |
| recursive fixpoint | 1,000 iterations; 100,000 rows; 64 MiB | each may be `-1`, but at least one must remain finite | query, `query/recursive_fixpoint.go` |
| set-expression tree | 1,000,000 cumulative rows; 64 MiB; depth 256; 4,096 nodes | configurable physical executor bounds | query, `query/set_tree.go` |

Result, intermediate, join-pair, heap, aggregate, recursive, and set-tree
accounts are independent. Enlarging one does not enlarge the others. Execution
returns a typed resource error rather than truncating an exact result.

## SQL and pgwire ingress

| Resource | Current value | Kind | Layer / source |
| --- | ---: | --- | --- |
| SQL statement text | 16 MiB | hard | parser, `sql/parser.go` |
| SQL placeholders | 65,536 | hard | parser, `sql/parser.go` |
| predicate/scalar nesting | 64 | hard | parser, `sql/parser.go` |
| nested subqueries | 32 | hard | parser, `sql/parser.go` |
| set-expression nesting | 64 | hard | parser, `sql/parser.go` |
| items in one select/from/group/order-style comma list | 1,024 | hard | parser, `sql/parser.go` |
| one `database/sql` parameter | 4 MiB | hard | SQL driver, `sql/driver/driver.go` |
| all `database/sql` arguments | 16 MiB | hard | SQL driver, `sql/driver/driver.go` |
| savepoint frames | 64 | hard | SQL driver, `sql/driver/savepoint.go` |
| pgwire startup packet | 10,000 bytes | hard | pgwire, `pgwire/proto.go` |
| pgwire frontend body; DataRow or RowDescription body | 16 MiB | hard | pgwire, `pgwire/proto.go` |
| pgwire Bind parameters | 32,767 | wire hard limit | pgwire, `pgwire/proto.go` |
| prepared statements / portals per session | 1,024 / 1,024 | hard | pgwire, `pgwire/proto.go` |
| simple statements in one Query message | 1,024 | hard | pgwire, `pgwire/proto.go` |
| retained prepared input / bind input / portal data | 16 MiB each per session account | hard | pgwire, `pgwire/proto.go` |
| standalone pgwire connections | 128 | zero-value default | pgwire, `pgwire/server.go` |
| standalone pgwire result | 100,000 rows; 64 MiB | zero default; explicit `-1` opt-out | pgwire, `pgwire/server.go` |
| gateway development pgwire | 16 connections; 100,000 rows; 4 MiB result | fixed command configuration | `cmd/vibedb-gateway/pgwire.go` |

The 16 MiB message bound does not imply that every 16 MiB query or result is
executable. Parser structure, session-retention, query workspace, RF3, and
result budgets can reject it earlier.

## Routing and gateway profiles

| Resource | Current value | Kind | Layer / source |
| --- | ---: | --- | --- |
| virtual bucket width | 20 bits (1,048,576 buckets) | default | routing, `distribution/bucket.go` |
| virtual bucket width range | 8–24 bits | hard | routing, `distribution/bucket.go` |
| route candidate mappings | 256 | nonpositive-value default | routing, `distribution/policy.go` |
| targeted shards | 64 | nonpositive-value default | routing, `distribution/policy.go` |
| shards changed by one manifest replacement | 64 | hard | routing, `distribution/manifest.go` |
| gateway catalog file | 16 MiB | hard | catalog, `gateway/catalog.go` |
| gateway NDJSON request line | 1 MiB | hard | gateway ingress, `cmd/vibedb-gateway/serve.go` |
| gateway decoded parameters | 65,536 | hard | gateway ingress, `serve_request_wire.go` |
| gateway decode metadata account | 8 MiB | hard, also constrained by 1 MiB line | gateway ingress, `serve_request_wire.go` |

The built-in operation profiles bound fan-out, aggregate result, transaction
material, and time independently:

| Class | Scatter | Concurrency | Aggregate rows / bytes | Per-shard rows / bytes | Transaction mutations / bytes | Whole / shard deadline |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| interactive | no | 4 | 50,000 / 16 MiB | 50,000 / 16 MiB | 50,000 / 16 MiB | 5 s / 2 s |
| batch | yes | 16 | 1,000,000 / 256 MiB | 500,000 / 128 MiB | 1,000,000 / 256 MiB | 60 s / 30 s |
| admin | yes | 32 | 10,000,000 / 1 GiB | 5,000,000 / 512 MiB | 10,000,000 / 1 GiB | 5 min / 2 min |

These names do not grant authorization and the deadlines are not service
objectives. The generic partially specified profile also substitutes finite
fallbacks; it never interprets nonpositive fan-out as unbounded.

### Native RF3 read admission

| Resource | Default | Hard maximum | Source |
| --- | ---: | ---: | --- |
| concurrent native reads | 256 | 65,536 | `gateway/replicated_data_read.go` |
| native read in-flight bytes | 256 MiB | 64 GiB | `gateway/replicated_data_read.go` |
| scatter workers | 16 | 1,024 | `gateway/replicated_data_read.go` |

The complete bounded response is reserved before dispatch. Per-route relation
limits and the caller's `max_result_bytes` may make the effective cap smaller.

## Shard services and RF3

| Resource | Current value | Kind | Layer / source |
| --- | ---: | --- | --- |
| static shard connections | 128 | zero-value default | static service, `shardservice/server.go` |
| static request / write / idle time | 30 s / 30 s / 5 min | zero-value defaults | static service, `shardservice/server.go` |
| static result / intermediate | 100,000 rows; 64 MiB / 64 MiB | zero-value defaults | static service, `shardservice/server.go` |
| static active read fences | 4,096 | zero-value default, always finite | static service, `shardservice/server.go` |
| serving replicas | exactly 3 | topology invariant | RF3 catalog, `gateway/replicated_catalog.go` |
| members in one replicated route | 64 | hard codec/client ceiling; serving route remains 3 | RF3 client, `gateway/replicated_native.go` |
| RF3 attempts / one attempt timeout | 16 / 5 min | hard maxima; configuration must be positive | RF3 client, `gateway/replicated_native.go` |
| retained leader hints | 4,096 default; 65,536 hard | cache only; eviction triggers discovery | RF3 client, `gateway/replicated_native.go` |
| RF3 native connections / concurrent TLS handshakes | 64 / 16 in checked-in `serve-rf3`; 65,536 connection hard maximum | handshakes cannot exceed connections | RF3 service, `cmd/vibedb-shard/serve_rf3.go`, `shardservice/replicated_server.go` |
| RF3 in-flight frame account | 112 MiB checked-in `serve-rf3` default; 1 GiB hard maximum | process-wide across every local RF3 group | RF3 service, `shardservice/replicated_server.go` |
| RF3 request timeout | 15 s in checked-in `serve-rf3`; 5 min hard maximum | one request | RF3 service, `cmd/vibedb-shard/serve_rf3.go`, `shardservice/replicated_server.go` |
| one RF3 SQL request / result / each work account / rows | 1 MiB / 4 MiB / 8 MiB / 100,000 | hard per-request bounds | RF3 SQL, `shardservice/replicated_query.go` |
| one worst-bound RF3 SQL execution reservation | 40 MiB | conservative shared-frame charge; the 112 MiB default admits two plus their request frames and refuses a third | RF3 SQL, `shardservice/replicated_query.go` |
| replicated command / result | 16 MiB / 16 MiB | hard | replicated apply, `internal/replication/types.go` |
| user relations per replicated bundle | 59 | hard dense slot count | replicated apply, `internal/replication/types.go` |
| mutations / key / value in one replicated command | 65,536 / 256 B / 4 MiB | hard | replicated apply, `internal/replication/types.go` |

`MaxAttempts` includes the first attempt. Retries reuse the exact original
command; increasing attempts is not permission to rebuild a mutation under a
new identity.

### Distributed transactions and exchange

| Resource | Current value | Meaning / source |
| --- | ---: | --- |
| inline transaction participants | 64 | limit of legacy single-record encoding only, **not total participants**; wider manifests are segmented, `internal/distributedtxn/codec.go` |
| intent scopes / mutation bytes per participant | 256 / 16 MiB | hard codec bounds, `internal/distributedtxn/codec.go` |
| exchange producers / queued batches / queued per producer | 1,024 / 4,096 / 64 | hard mailbox bounds, `internal/exchange/mailbox.go` |
| exchange batch | 65,536 rows / 4 MiB | hard, `internal/exchange/mailbox.go` |
| one mailbox lifetime total | 16,777,216 rows / 1 GiB | hard logical totals, `internal/exchange/mailbox.go` |
| static shard exchange registry | 1,024 mailboxes / 512 MiB reserved buffers | service defaults, `shardservice/server.go` |

Mailbox byte counts are admission promises for owned payload buffers, not an
exact whole-process memory bound.

## Durable storage

| Resource | Current value | Kind | Layer / source |
| --- | ---: | --- | --- |
| base page | exactly 4,096 bytes | format invariant | durable collection, `store/durable/store_file_options.go` |
| ordinary maximum page extent | 64 KiB | default | durable collection, `store/durable/store_file_options.go` |
| physical extent accepted from metadata | 64 MiB | hard | storage codec, `internal/storeio/superblock.go` |
| resident page/cache account | 64 MiB | default | durable collection, `store/durable/store_file_options.go` |
| concurrent reads / prefetch queue | 4 / 64 | defaults | durable collection, `store/durable/store_file_options.go` |
| snapshot leases | 1,024 | default | durable collection, `store/durable/store_file_options.go` |
| retired extents | 65,536 default; 16,777,216 hard | default / hard | durable collection, `store/durable/store_file_options.go` |
| exact index tuple | 4 components; 4,096 encoded bytes | hard | storage codec, `internal/storeio/index_term_key.go` |
| logical / physical exact indexes | 4,096 aliases / 64 physical | hard | page catalog, `internal/storeio/page_catalog.go` |
| page catalog canonical image | 32 MiB | hard | page catalog, `internal/storeio/page_catalog.go` |
| transaction decision log | 1 MiB default; 16 MiB hard; 64 participants/record | default / hard | `internal/storeio/txn_marker.go` |
| Raft WAL file / record / live bytes | 256 MiB / 80 MiB / 128 MiB | zero-value defaults | RF3 WAL, `internal/raftstore/types.go` |
| Raft WAL hard file / record / live bytes | 4 GiB / 96 MiB / 2 GiB | absolute maxima | RF3 WAL, `internal/raftstore/types.go` |

WAL capacity values are sealed into the authenticated format; reopen must use
the same values. A deployment manifest may deliberately select smaller valid
bounds, so the constructor default is not proof of an active process's profile.
On-disk development format version zero is edited in place; there are no legacy
compatibility decoders.

## Source map

| Layer | Primary source families |
| --- | --- |
| facade and transactions | `vibedb.go`, `vibedb_txn.go`, `store/engine.go` |
| query budgets | `query/*budget.go`, `relation_runtime.go`, `recursive_fixpoint.go`, `set_tree.go` |
| SQL and pgwire | `sql/parser.go`, `sql/driver/`, `pgwire/proto.go`, `pgwire/server.go` |
| routing and gateway | `distribution/bucket.go`, `policy.go`, `gateway/profile.go`, `gateway/replicated_*` |
| shard and RF3 service | `shardservice/server.go`, `replicated_server.go`, `replicated_query.go` |
| distributed records and exchange | `internal/replication/`, `distributedtxn/`, `exchange/` |
| durable formats | `store/durable/store_file_options.go`, `internal/storeio/`, `internal/raftstore/types.go` |
