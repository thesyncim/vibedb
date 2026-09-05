# Low-level storage engines

[Documentation](README.md) / [Design](design/README.md) · [Development status](status.md)

Use the root `vibedb` package unless you need to own snapshots, descriptors, storage
geometry, or workspaces. This maps the lower-level `store` and `store/durable` contracts.

## Choose one ownership layer

| Package | Residency | Lifecycle owner | Snapshot rule |
| --- | --- | --- | --- |
| `vibedb` | Memory or disk profile | Database owns managed handles | Hidden behind facade reads and transactions |
| `store` | Heap/off-heap process memory | Go object owner | Value snapshot; no close required |
| `store/durable` collection | Bounded resident cache plus one file | Caller owns `*os.File`; engine owns I/O resources | Explicit lease; must close |
| `store/durable` database | Directory of collection files | Database owns opened descriptors | Explicit multi-collection lease; must close |

Do not mix lifecycle assumptions. A `vibedb.Collection` returned by
`Database.Collection` cannot be closed independently. A standalone durable
collection does not close the `*os.File` passed to `Create` or `Open`.

## `store`: immutable heap generations

`store.Collection` is the source-model JSON engine. Its zero value is usable and
must not be copied after use. One writer lock serializes mutations; readers load
immutable published state without taking that lock.

```go
c, err := store.New(store.Options{ChunkDocuments: 64})
created, err := c.Put("k", []byte(`{"answer":42}`))
snap, err := c.Snapshot()
raw, found := snap.GetRaw("k")
```

`Put` validates and copies the key and JSON source. It rebuilds one bounded chunk,
so repeated bulk loading pays `O(ChunkDocuments)` copying per row. For loads
larger than one chunk, use the single-goroutine `Builder` and its terminal `Build`.

`Delete` removes a row without a tombstone; a miss does not advance generation.
Existing snapshots keep their rows. A heap snapshot is concurrent-safe, needs no
`Close`, and remains valid after writes; `GetRaw` is borrowed from that snapshot.

### Heap options

| Option | Contract |
| --- | --- |
| `ChunkDocuments` | `1..64`; zero selects 64 |
| `IndexOptions` | JSON structural-index geometry |
| `ShapeTapes` | Compile repeated object shapes |
| `Postings` | Maintain containment/existence postings |
| `ValueDict` | Maintain the value dictionary |
| `Schema` | Optional valid compiled schema, frozen at initialization |

These are representation controls, not durability controls. Collection memory
can live outside Go's `HeapAlloc`; measure process RSS too.

### Heap catalog and coherent cuts

`store.Database` is a zero-value-ready catalog. `CreateCollection` freezes options.
`DropCollection` removes only the name; acquired handles and snapshots remain usable.

`Database.Snapshot` briefly locks every cataloged collection writer in name
order and captures one no-skew cut. It does not make a sequence of separate
writes atomic.

For atomic writes, use `store.UpdateCollections(participants, fn)`. It stages
all fallible work, locks all participant writers in global name order, then
publishes every planned state while those locks are held. Database snapshots see
the transaction before or after, never partly applied. Independent
single-collection reads can still observe their individual publication points.

The heap defaults admit at most 16 participant collections and, per participant,
64 distinct keys and 16,793,600 staged key/value bytes. Batches copy inputs,
deduplicate keys, and use last-write-wins. `Database.Update` includes every
currently cataloged collection as a participant, even when the callback dirties
only one; a catalog larger than 16 can therefore be refused. Select participants
explicitly when that distinction matters.

## `store/durable`: one owned file per collection

`durable.Create` requires an empty regular file; `durable.Open` recovers one.
Both take an exclusive writer lease. Until `Collection.Close` completes, keep
the descriptor and path stable; never independently access or alter the
primary/journal pair.

The engine owns caches, workers, the journal descriptor, and writer lease—not
the caller's primary descriptor. Close active snapshots first. A failed `Close`
can be retried; require `CloseCompleted` before closing the primary descriptor.

`durable.OpenDatabase(dir, options)` is the owned-catalog alternative. It uses
one encoded `.vjc` primary per collection, an optional paired `.rjournal`, and a
catalog transaction sidecar named `txn.vtm`. It owns the descriptors it opens.
Keep the resolved directory stable until `Database.Close` completes.

Names are encoded, never used as path elements; do not alias or manipulate engine files.

### Durable data contract

The zero-value `durable.Options` is a working, power-safe JSON configuration.
Important immutable or bounded controls include:

| Control | Zero-value behavior |
| --- | --- |
| `PageSize` | 4096 bytes; no other base page size is accepted |
| `MaxPageSize` | 64 KiB leaf/overflow ceiling |
| `MaxKeyBytes` | 256 bytes |
| `MaxDocumentBytes` | 4 MiB |
| `MaxBatchDocuments` | 64 distinct keys |
| `MaxBatchBytes` | Maximum keys plus up to 16 MiB of values |
| `MaxRetiredExtents` | 65,536 tracked copy-on-write extents |
| `Durability` | `DurabilitySync` |
| `CheckpointStrength` | `CheckpointPowerSafe` |

Geometry, schema presence, opaque mode, and index assertions are validated
against the selected root. Zero-options reopen reconstructs documented persisted
settings; it does not rewrite the file contract.

`OpaqueValues` stores non-empty uninterpreted bytes byte-for-byte. It is
incompatible with schemas, exact indexes, skip indexes, and JSON-only resident
options. JSON collections validate and canonicalize values before publication.

### Mutation and batch publication

Point `Put` and `Delete` publish one complete new reader-visible state or
nothing. `Collection.Update` copies and deduplicates staged keys and values; the
last operation for one key wins. Malformed JSON and schema failures are detected
before logical publication.

An `Update` is a logical failure-atomic publication: rows and exact-index postings
become visible as one unit. A fit problem may first publish one
content-equivalent topology generation and replan. After a later error,
Generation may advance while logical content remains unchanged. Generation is a
publication counter, not a content version.

`ErrBatchTooLarge` is an admission refusal with nothing published. Capacity and
retired-extent pressure are also bounded refusals; closing old snapshots can
release retired extents. A durability-fence failure is different: inspect
`PersistenceError`, stop writing, close, and recover by reopening.

### Durability modes

| Mode | Success and visibility | Failure after acknowledgement |
| --- | --- | --- |
| `DurabilitySync` | The normal lane syncs a journal record before publication and acknowledgement; a file created async and reopened sync instead uses a root-fence chain | Acknowledged mutation recovers under that lane's fence contract |
| `DurabilityAsyncVisible` | Publishes after bounded queue admission; persistence continues in background | Recent acknowledged state can be lost |
| `DurabilityBufferedVisible`, no recovery journal | Publishes from bounded memory; setup or checkpoint metadata may still perform device I/O | Acknowledged state is lost unless flushed or closed successfully |
| `DurabilityBufferedVisible` with `RecoveryJournal` | Publishes resident state, then syncs its redo before returning success | A successful acknowledgement is recoverable; visibility may precede that sync |

`Flush` waits until the current reader-visible generation is recoverable under
the selected checkpoint strength. `Close` fences publications, makes the
accepted cut durable as required by the selected mode, and releases engine
resources. `CheckpointFilesystem` is weaker than the power-safe default and is
accepted only for portable, buffered-write, buffered-visible checkpoints.

`RecoveryJournal` has mode-specific meaning. Ordinary sync collections are
journal-backed; the reopened chain-fence exception is not. With
buffered-visible it upgrades mutation acknowledgement to a bounded redo append
and sync; without it, acknowledgement remains volatile and `Flush` or `Close`
establishes recoverability at the selected strength. It does nothing for
async-visible.

An engine fence cannot prove every filesystem, controller, or device cache;
qualify the deployment's storage stack.

### Snapshots, scans, and open cost

`Collection.Snapshot` pins one immutable generation. `SnapshotInto` reuses
storage and closes/rebinds its prior lease. Close snapshots promptly and never
copy them after use; long leases can cause bounded write backpressure.

`Snapshot.AppendRaw` copies a value into caller storage. `Snapshot.RangeRaw` and
`Collection.RangeRawCurrent` visit keys in bytewise lexical order and lend key
and value bytes only for the callback. Prefetch is a hint, not a visibility or
durability barrier.

Open validates a bounded top-level graph rather than every primary row. Indexed
open still rebuilds the resident exact-index epoch by walking persisted exact-index
catalog/leaf geometry; plan startup against the actual index set.

### Durable multi-collection operations

`Database.Snapshot` and `SnapshotCollections` capture one no-skew cut and own one
lease per materialized collection; close the returned database snapshot.

`Database.Update` prepares each dirty participant, syncs each participant's
conditional record, then syncs one decision in `txn.vtm` before publishing. A
K-participant commit therefore performs K+1 syncs. Recovery reveals every
participant committed or none. Standalone `Open` fails closed on an uncovered
conditional record; reopen the complete database catalog instead.

The raw `durable.UpdateCollections` API requires explicit, non-zero
`TxnLimits` for two or more dirty collections. `Database.Update` owns defaults:
16 collections, 256 documents, and 67,174,400 total staged bytes. A one-dirty-
collection call routes through ordinary `Collection.Update` and does not consume
the multi-collection marker protocol.

Only the sync-journal and buffered-journal-ack lanes support a low-level
multi-collection durable commit. Other lanes return
`ErrDatabaseTransactionUnsupportedLane`. An ambiguous decision sync returns
`ErrCommitOutcomeUnknown`, poisons catalog writes, and requires a complete close
and reopen; recovery still resolves to all or none.

### Offline inspection and salvage

`Verify` and `Salvage` do not acquire the writer lease or apply recovery rollback.
Run them on a quiescent file or consistent copy, never beside a live writer.

`Verify` reports structural findings separately from I/O errors. `Salvage`
rebuilds a fresh empty output from surviving self-describing primary leaves;
overflow values are skipped and reported, so salvage can be partial. It is not a
backup strategy.

See the [durable ownership contract](../store/durable/OWNERSHIP.md) for handle,
lease, and storage lifetime rules.

## Source map

- Heap collection and snapshots: [store/engine.go](../store/engine.go), [store/store_builder.go](../store/store_builder.go)
- Heap catalog, cuts, and transactions: [store/store_collection.go](../store/store_collection.go), [store/store_database_snapshot.go](../store/store_database_snapshot.go), [store/store_database_txn.go](../store/store_database_txn.go)
- Shared schemas and indexes: [store/store_schema.go](../store/store_schema.go), [store/store_index.go](../store/store_index.go), [store/store_index_exact.go](../store/store_index_exact.go)
- Durable options and open: [store/durable/store_file_options.go](../store/durable/store_file_options.go), [store/durable/store_file_open.go](../store/durable/store_file_open.go)
- Durable batches and lifecycle: [store/durable/store_file_batch.go](../store/durable/store_file_batch.go), [store/durable/store_file_lifecycle.go](../store/durable/store_file_lifecycle.go)
- Durable catalogs, cuts, and transactions: [store/durable/store_database.go](../store/durable/store_database.go), [store/durable/store_database_snapshot.go](../store/durable/store_database_snapshot.go), [store/durable/store_database_txn.go](../store/durable/store_database_txn.go)
- Offline tools: [store/durable/store_file_verify.go](../store/durable/store_file_verify.go)
