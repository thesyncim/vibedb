# Low-level storage engines

[Documentation](README.md) / [Design](design/README.md) · [Stability](status.md)

Use the root `vibedb` package for normal applications. Use `store` or
`store/durable` directly only when you need explicit control over snapshots,
file descriptors, page geometry, `OpaqueValues`, or I/O backends.

## Ownership layers

| Package | Residency | Lifecycle owner | Snapshot rule |
| --- | --- | --- | --- |
| `vibedb` | Memory or disk | Database owns managed handles | Hidden behind facade |
| `store` | Heap / off-heap RAM | Go object owner | Immutable value; no `Close` |
| `store/durable` collection | Bounded cache + 1 file | Caller owns `*os.File`; engine owns I/O & lease | Explicit lease; must `Close` |
| `store/durable` database | Directory of `.vjc` + `txn.vtm` | Database owns descriptors | Explicit multi-collection lease; must `Close` |

## `store`: immutable heap engine

- Zero-value `store.Collection` is ready to use (do not copy after use). One
  writer lock serializes writes; readers load immutable snapshots lock-free.
- `Put` rebuilds one chunk (default `ChunkDocuments: 64`). Use `Builder` for
  bulk loads. `Delete` removes rows without tombstones.
- Options (`ChunkDocuments`, `IndexOptions`, `ShapeTapes`, `Postings`,
  `ValueDict`, `Schema`) control representation and freeze at creation.
- `store.Database.Snapshot()` captures a no-skew cut across all collections;
  `store.UpdateCollections(participants, fn)` publishes up to 16 collections
  atomically (64 keys / 16,793,600 B per collection default).

## `store/durable`: file-backed engine

- `durable.Create` (empty regular file) and `durable.Open` acquire an exclusive
  writer lease. Keep the `*os.File` and path untouched until
  `CloseCompleted(col.Close())` is true; then the caller closes the descriptor.
- `durable.OpenDatabase(dir, opts)` manages hex-encoded `.vjc` primaries,
  `.rjournal` siblings, and `txn.vtm`.
- **Defaults (`durable.Options{}`):** `PageSize: 4096` (fixed), `MaxPageSize:
  64 KiB`, `MaxKeyBytes: 256`, `MaxDocumentBytes: 4 MiB`, `MaxBatchDocuments:
  64`, `MaxBatchBytes: ~16 MiB`, `MaxRetiredExtents: 65,536`,
  `DurabilitySync`, `CheckpointPowerSafe`.
- `OpaqueValues: true` stores arbitrary nonempty bytes without JSON parsing,
  schemas, or indexes.
- **Batches:** `Collection.Update` provides logical failure-atomic publication
  (rows + exact indexes together). Topology preparation may publish a
  content-equivalent topology generation so `Generation may advance` even if a
  later step fails.
- **Snapshots & open cost:** Close snapshots promptly (`SnapshotInto` reuses
  storage) or retired extents will block writers. Opening an exact-indexed
  collection rebuilds its resident index epoch from persisted leaves.
- **Multi-collection commits:** `Database.Update` uses `K+1` syncs (`K`
  conditional prepares + `txn.vtm` decision; defaults: 16 collections, 256
  docs, 67,174,400 B). `ErrCommitOutcomeUnknown` poisons writes until full
  reopen.
- **Offline tools:** Run `Verify` and `Salvage` only on a quiescent file or
  copy. `Salvage` rebuilds from surviving primary leaves and skips overflow
  values (partial recovery, not a backup).

See [store/durable/OWNERSHIP.md](../store/durable/OWNERSHIP.md) for full lease
and lifetime rules.

## Limitations

- Misuse (copying handles, leaking snapshots) is not always detected at runtime.
- Heap `tin` builds hold the collection writer lock; durable `tin` persists
  sealed postings in a `.tin` sidecar while active postings stay resident
  ([search](api/search.md#limitations)).
- Exact-indexed open time and memory grow with the index set.

## Source map

- Heap: [store/engine.go](../store/engine.go), [store_builder.go](../store/store_builder.go), [store_database_txn.go](../store/store_database_txn.go)
- Durable: [store/durable/store_file_options.go](../store/durable/store_file_options.go), [store_file_open.go](../store/durable/store_file_open.go), [store_file_batch.go](../store/durable/store_file_batch.go), [store_database_txn.go](../store/durable/store_database_txn.go), [store_file_verify.go](../store/durable/store_file_verify.go)
