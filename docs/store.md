# Store

This document describes the canonical `store` and `store/durable` APIs in the
current tree. Public Go documentation remains authoritative for individual
methods and option fields.

The proposed sole next-generation primary, its invariants, 100-billion-key
bounds, space accounting, weaknesses, and promotion gates are specified in
[ordered-hybrid-store.md](ordered-hybrid-store.md). That document describes a
target; this file describes the APIs and formats that are implemented today.

## Storage surfaces

| Surface | Purpose | Persistence |
| --- | --- | --- |
| `store.Segment` | Immutable self-contained batch of documents with its own tape, shape, and column machinery | Explicit segment image |
| `store.Collection` | Mutable in-memory keyed collection with immutable snapshots | Explicit full checkpoint |
| `store.Builder` | Bulk construction of a `store.Collection` | None |
| `durable.Collection` | Bounded-residency durable collection | Automatic incremental commits |
| `Collection.WriteTo` / `store.Open` | Portable immutable collection image | Explicit full checkpoint |

A collection is one physical JSON namespace. `store.Database` is an in-memory
catalog of independent `store.Collection` handles; it is not a durable
multi-collection database.

## In-memory collections

The zero `store.Collection` is usable. `store.New` is preferred when options are
known:

```go
db, err := store.New(store.Options{
	ChunkDocuments: 16,
	ShapeTapes:      true,
})
if err != nil {
	return err
}
```

`store.Options` is frozen by the first operation that initializes the store:

| Option | Current behavior |
| --- | --- |
| `ChunkDocuments` | Documents per immutable chunk; zero selects 64, valid values are 1–64 |
| `IndexOptions` | Structural-index configuration for each chunk |
| `ShapeTapes` | Deduplicates recurring object layouts within each chunk |
| `Postings` | Builds wildcard existence/scalar-containment postings from the first write |
| `ValueDict` | Enables the chunk-local scalar dictionary |
| `Schema` | Optional compiled schema; nil keeps the schemaless path |

### Mutation

`Put` validates one complete JSON value, copies a new key and the document, and
atomically inserts or replaces it. The caller may reuse both inputs after
return. A validation or schema failure publishes nothing.

Replacing a key:

- parses only the replacement document;
- shares unchanged immutable state with older snapshots;
- rebuilds at most the configured chunk and bounded metadata paths.

`Delete` removes the row from its chunk. It creates no tombstone, version chain,
or later compaction obligation. An empty chunk is removed immediately.

Writes are serialized. Each successful write publishes one immutable state
through an atomic pointer.

### Batched mutation

`durable.Collection.Update` applies many mutations as one generation:

```go
err := collection.Update(func(b *durable.WriteBatch) error {
    for _, row := range rows {
        if err := b.Put(row.Key, row.Document); err != nil {
            return err
        }
    }
    return nil
})
```

The batch rebuilds each touched chunk once rather than once per document,
descends each directory once rather than once per key, and publishes one root
behind one durability fence. It either publishes whole or publishes nothing: an
error from the closure or from any staged mutation aborts the transaction.

`Options.MaxBatchDocuments` bounds how many distinct keys one `Update` may carry
and sizes the transaction reservation; zero selects 64. A larger batch reports
`ErrBatchTooLarge` and publishes nothing, so callers split rather than discover a
half-applied batch after a crash. Keys are deduplicated as they are recorded, so
mutating the same key twice keeps only the last mutation. Document syntax is
validated when `Update` applies the batch, not when `Put` records it.

A collection built by `CreateFrom` carries two read accelerators that the batched
path releases rather than maintains: the compact index-group catalog and the
dense float64 scan projection. Exact postings and the chunk-tree scan stay
authoritative and current, so releasing them costs query speed, never an answer.
The single-document path already releases the projection on the first appending
`Put` after a bulk build.

### Snapshots and reads

`Snapshot` is O(1) and does not wait for an in-progress writer. A snapshot stays
valid after any later update or delete.

`Snapshot.GetRaw` is lock-free, clock-free, and allocation-free. The returned
`RawValue` borrows snapshot storage. Use `AppendRaw` when the bytes must outlive
that storage or be placed in caller-owned capacity.

`CompileKey` returns a verified stable-slot hint for repeated reads. A later
movement or delete does not make it unsafe: lookup falls back to the complete
key path when the hint no longer matches.

`Range`, pointer extraction, field extraction, index masks, and bitmap helpers
operate over the same immutable snapshot. Concurrent readers are safe.

### Schemas

`store.CompileSchema` creates an immutable schema reusable by heap, bulk, and
durable stores.

Schemas can constrain:

- the root JSON type;
- required nested RFC 6901 paths;
- allowed types at each path, including unions.

Unspecified fields remain allowed. `SchemaInteger` distinguishes JSON integer
spellings from other numbers. Successful validation walks the structural index
already built for the write and allocates no additional per-row representation.

### Exact indexes

`CreateIndex` declares one exact scalar index:

```go
info, err := db.CreateIndex(store.IndexDefinition{
	Name:  "tenant_country",
	Paths: []string{"/tenant", "/profile/country"},
})
```

An index accepts one to four RFC 6901 paths. One path is a scalar index; multiple
paths form an order-sensitive compound key. Missing paths and container values
are omitted. Null, booleans, exact JSON numbers, and decoded strings are
indexed.

Logical aliases with identical ordered paths share one physical compiled
definition, coverage cursor, packed base, and mutation root. They remain
independently named and droppable, and checkpoint reopen reconstructs the same
sharing. Reversing compound-path order is a different physical index.
Immutable query snapshots keep the public catalog once and use a compact
64-byte exact-index descriptor instead of repeating the 144-byte `IndexInfo`
payload beside every root.

Creation on existing data publishes `store.IndexBuilding`. Writes immediately
maintain covered state, while `BackfillIndex` advances old chunks in a
caller-bounded batch. Queries remain exact during construction by scanning
uncovered chunks. `store.IndexReady` means every live chunk is covered.

Hashes and fingerprints only prune candidates. Exact JSON values are verified
before a row is returned.

Postings use stable-slot chunk masks. Immutable index bases are packed outside
the Go heap on supported platforms; recent mutations remain in a snapshot-owned
persistent delta until a later fold. Readers merge both streams in row order.
Boolean intersections use linear advance for nearby masks and galloping advance
for skewed masks. An exact indexed `COUNT(*)` popcounts the final masks without
reopening JSON.

This is a Roaring-inspired execution strategy, not the Roaring serialization or
container format. Array, bitmap, and run-container adaptation is not currently
implemented.

`DropIndex` removes the logical index immediately. `ReclaimIndexes` bounds
physical wildcard-posting reclamation after the last user is gone.

### Bulk construction

`store.Builder` accepts unique keys, validates and copies documents directly
into final chunks, and builds declared indexes before publishing one collection.
`Append` is single-goroutine. `Build` transfers completed state and closes the
builder. Once final key compaction succeeds, Build is terminal; a later failure
releases every unpublished external block and the builder cannot be retried.

Use the builder for an initial corpus, `Collection.Update` for subsequent bulk
ingestion, and `Collection.Put` for individual mutations.

## Collection checkpoints

`Collection.WriteTo` writes a complete immutable checkpoint. It is not incremental:
every live chunk is streamed on each call, and later writes do not modify the
image.

`store.Open` validates the complete image before publication and returns a
normally mutable collection. Source and structural-tape bytes may borrow the
input image. Keep that image immutable and alive until the collection, all
snapshots, and all derived borrowed values are unreachable. Mutations after
open are heap-only until another `WriteTo`.

A checkpoint is a container, not a second document codec: its payload region is
a concatenation of `Segment` images written by `Segment.WriteTo`, and the
checkpoint manifest adds only the collection-level catalog (chunk directory, key
spellings, schema, index definitions, and free chunk ids). The
two formats share one fixed envelope — a 16-byte header (`ImageHeaderLen`) and a
40-byte checksummed footer (`ImageFooterLen`) around a self-describing manifest —
but keep independent magics, version lineages, and error taxonomies, so a
segment image and a checkpoint can move apart without either dragging the other.

The format is versioned and pre-v1. Cross-version compatibility is not promised
until the format is declared stable.

## Durable collections

`durable.Collection` is the general durable path. It uses checksummed copy-on-write
pages, alternating superblocks, bounded queues, and a fixed-size page cache.
The caller owns the `*os.File` lifetime; keep it open until `Collection.Close`
returns. See [docs/format.md](format.md) for the exact on-disk byte format.

`durable.Create` requires an empty file and durably initializes its first root.
`durable.Open` first acquires an exclusive writer lease, then performs bounded
recovery from the superblocks, selected root, and top-level directories. It
does not scan the complete key or document set at open. A second mutable handle
to the same file fails with `durable.ErrWriterLocked`.

### Configuration defaults

The zero `durable.Options` selects:

| Resource | Default |
| --- | ---: |
| Metadata page | 4 KiB |
| Maximum page/extent | 64 KiB |
| Read cache | 64 MiB |
| Maximum document | 4 MiB |
| Maximum key | 256 bytes |
| Portable read workers | 4 |
| Prefetch queue | 64 references |
| Snapshot leases | 1,024 |
| Retired extents | 65,536 |

All resident, queue, snapshot, and retired-extent capacities are fixed at open.
The pointer-free reusable-extent and free-fold planner arenas are allocated as
contiguous external blocks on supported systems rather than as per-fold Go heap
objects. `durable.Stats` reports their live, reserved, and external bytes
separately from the page cache and commit staging, together with I/O backends,
generations, and reclamation state.

`PageSize` and `MaxPageSize` remain power-of-two bounds. Ordinary document and
overflow extents between those bounds use the smallest whole `PageSize`
multiple that holds their bytes, avoiding power-of-two disk slack. A document
may exceed the ordinary page size up to `MaxDocumentBytes`; overflow chains
remain bounded by the transaction limits derived from the options.

### Durability

`Put`, `Delete`, and `Update` publish a copy-on-write
generation. Applications do not rewrite a checkpoint after each operation.

The zero-value `DurabilitySync` mode makes mutation success and reader
visibility wait for both the data barrier and the alternate-root barrier.
`DurabilityAsyncVisible` is the explicit asynchronous opt-in: a mutation
becomes reader-visible when the bounded committer accepts it. Use:

- `DurableGeneration` to observe the last fenced generation;
- `Flush` to wait until the current visible generation is durable;
- `Close` to stop new work, drain commits, and release owned resources.

`CommitCoalesce` bounds an optional group-commit window. It also affects the
latency of synchronous callers.

Recovery validates both superblocks and their roots and can fall back to the
previous complete generation. Corruption encountered when a lower page is
admitted is returned as an error. These guarantees still depend on the
filesystem and device honoring flush completion.

Any persistence failure poisons the live writer. Copy-on-write collections
continue serving the last confirmed durable generation; an asynchronous
canonical replacement rejects reads until reopen because recovery must first
repair or select its page image. `PersistenceError` exposes the sticky cause.
When the alternate root may already have reached storage, the cause matches
`ErrCommitOutcomeUnknown`; reopen before deciding whether to retry.

### Reads, snapshots, and reuse

`durable.Collection.Snapshot` acquires an explicit generation lease. Close it
promptly.
While a snapshot is active, extents reachable from that generation cannot be
reused. A long-lived snapshot therefore increases `PendingRetiredExtents` and
`PendingRetiredBytes`; it does not block newer reads or commits until configured
retirement capacity is exhausted.

`durable.Snapshot.AppendRaw` always copies exact JSON into caller storage and
never returns a borrowed cache page. Query execution and range scans use the
same lease.

Retirements are generation ordered. A pinned snapshot check is constant in the
pending retired-extent count, and eligible drains are proportional only to the
bounded number of extents reclaimed. Closing old snapshots promptly still
controls retained file space and descriptor pressure. Computing the snapshot
floor scans the fixed `MaxSnapshotLeases` table (1,024 slots by default), not
the retired set.

### Larger-than-RAM operation

`ResidentBytes` bounds the page cache rather than the logical file size. Metadata
and documents enter the cache on demand; eviction uses a bounded CLOCK arena.
The file can therefore be larger than RAM without making the Go heap
proportional to row count.

This is a residency property, not an equal-latency claim. Cold reads still pay
storage latency, and one document may be larger than a query's working-memory
target.

On Linux, `ReadMode` and `WriteMode` can try or require `O_DIRECT` through
independently owned descriptors. `Backend` can select the portable engine or
the pure-Go `io_uring` engine. `durable.Stats` reports the actual backend and
direct-I/O choices after fallback.

### Durable indexes and numeric covers

`durable.Options.Indexes` declares up to 64 exact scalar or compound indexes.
Definitions are fixed at creation and verified when reopened. Each write
maintains its postings transactionally.

`Float64Columns` declares up to 256 RFC 6901 paths. Numeric sidecars support
predicate-free `SUM`, `AVG`, `MIN`, and `MAX` without reopening JSON when the
query is fully covered. Missing, non-numeric, and non-finite values are absent
from the cover.

### Bulk creation

`durable.CreateFrom` converts a completed in-memory store directly into one
durable generation. It preserves keys, schema, and declared exact indexes
while packing documents and configured numeric covers without replaying
individual `Put` calls.

**The bulk path and a `Put` loop do not produce the same file.** Only the bulk
writer emits the `DocumentGroup` extent described in
[docs/format.md](format.md) — the page-local template table and value
dictionary that deduplicate structural repetition and repeated exact leaf
values. A collection built by replaying `Put` never reaches that
representation, and on a corpus with repeated field values it is several times
larger on disk than the same documents written in bulk. How much larger depends
entirely on how repetitive the values are, so neither figure is "the" durable
footprint; a measured comparison must state the corpus and report a
high-cardinality control beside the redundant case.

## Allocation and ownership

The caller-buffered operations are the steady-state allocation boundary:

- heap and durable `Snapshot.AppendRaw`;
- compiled-key reads;
- bitmap and masked-row appenders;
- reusable query `Result`, `Workspace`, and file-execution workspace.

An undersized destination may grow. A new index/query high-water mark, custom
method, or oversized value may allocate. Zero-allocation claims apply only to
the documented warmed path, not every convenience call.

The in-memory store copies `Put` input. `store.Open` may borrow its image.
`durable.Collection` copies writes and uses explicit snapshot leases for reads.

### Memory accounting

`store.Stats` separates caller-owned mapped image bytes from external key,
document, and packed-index bytes. Those counters do not include the Go heap or
total process RSS.

Bulk-built and opened immutable bases can use pointer-free external blocks, but
the mutable key HAMT, recent index deltas, and chunk publication paths
still use Go objects. The current in-memory collection therefore does not yet
have a row-count-independent GC footprint. The durable collection bounds page
payload residency, reusable extents, queues, and leases at open, but small
catalogs and generation state remain ordinary Go objects.

Measure source bytes, file bytes, external bytes, Go `HeapAlloc`, heap-object
count, RSS, and retained snapshot generations separately. None is a substitute
for another.

## Concurrency model

- heap and durable collections serialize mutations.
- `store.Snapshot` values are immutable and concurrent-safe.
- `durable.Snapshot` is immutable but owns a closeable lease.
- Prepared queries are concurrent-safe with a separate result/workspace pair
  per execution.
- `store.Builder`, query workspaces, readers, writers, and mutable result buffers
  are single-consumer.

## Current product boundaries

The repository currently has no replication, backup manager, point-in-time
restore, network protocol, distributed execution, cross-file transaction, or
durable multi-collection catalog. Query joins are not implemented. Those
features are not implied by the storage APIs above.
