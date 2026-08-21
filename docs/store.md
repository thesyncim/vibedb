# Storage model

The root facade owns lifecycle and selects one of three product profiles. The
`store` and `store/durable` packages expose the underlying engines for callers
that need explicit snapshots, geometry, I/O modes, or bulk construction.

## Product facade

`vibedb.Open` is the default entry point. It gives each collection one stable
lazy handle. The first valid mutation creates missing storage.

The facade owns collection descriptors and closes them with the database. A
database-managed collection returns `ErrManagedCollection` from its own
`Close` method.

## In-memory engine

`store.Collection` has one writer mutex and one atomically published immutable
state pointer. A point write rebuilds at most one chunk and then publishes a
new state.

Readers and snapshots do not take the writer lock. Heap snapshots are O(1),
need no close, and retain old rows through immutable state ownership.

The chunk size is 1 through 64 documents. Zero selects 64. A point-write
rebuild has cost proportional to this size. Use `store.Builder` for bulk load.

A failed put changes no state. A missing delete does not advance the
generation. Heap range order is stable chunk and slot order.

Raw heap reads can return borrowed memory. For a stable borrowed value, call
`Snapshot.GetRaw` and retain the snapshot. Treat `Collection.GetRaw` as
current-state borrowed data that a later mutation can retire. Use `AppendRaw`
when bytes must outlive the snapshot or mutation.

## Heap database

The zero value of `store.Database` is usable. A database snapshot locks all
collection writers in name order and captures one coherent instant.

`store.UpdateCollections` supports at most 16 participants, 64 keys per
participant, and 16,793,600 staged key-plus-value bytes per participant. It
validates all participants while it holds the writer locks. It then publishes
all state pointers.

A coherent database snapshot cannot observe a partial publication. Independent
collection reads can see different sides of a multi-collection commit. Use a
database snapshot when a cross-collection instant matters.

## Durable engine

`durable.Create` requires an empty file. It takes an in-process lock and an OS
writer lock, assigns a random store ID, and persists generation 1.

`durable.Open` does bounded recovery. It reads the alternate roots and the
catalog structures that they reference. It does not scan every row or posting
during open.

One mutable file has one writer. The lock covers path aliases and duplicate
descriptor identity when the platform can prove them.

The zero-value durable options select:

| Setting | Zero-value result |
| --- | --- |
| Backend | Auto, with portable fallback |
| Read mode | Buffered |
| Write mode | Buffered |
| Durability | Synchronous journal-before-visibility |
| Checkpoint strength | Power-safe |

Normalized resource defaults include:

| Resource | Default |
| --- | ---: |
| Base page | 4096 bytes |
| Maximum page | 64 KiB |
| Resident memory | 64 MiB |
| Read concurrency | 4 |
| Prefetch queue | 64 |
| Maximum key | 256 bytes |
| Inline value | 512 bytes |
| Maximum document | 4 MiB |
| Snapshot leases | 1024 |
| Retired extents | 65,536 |
| Batch documents | 64 |
| Batch bytes | 16,793,600 |
| Commit queue slots | 64 |

The option validator accepts at most 4096 logical exact-index names, 64
distinct physical path sets, and 8 skip indexes. Identical exact-index path
sets share one physical index. A canonical compound index tuple has a 4096-byte
limit.

## Durable reads and snapshots

A durable snapshot pins one generation lease and owns mutable scratch storage.
It is single-consumer and must be closed.

Create snapshots freely, but close them promptly. A long-lived snapshot pins
retired extents. When the retired-extent bound is full, a writer attempts
checkpoint and reclamation. It returns a retirement-capacity error if active
readers still prevent reuse. This error is not currently an exported durable
sentinel.

A durable database snapshot pins one generation lease for every captured
collection and must be closed. Active snapshots can make database close or
collection drop retryable.

Durable scans use lexical key order. Callback data is borrowed. `AppendRaw`
copies data into caller memory.

Prefetch is a bounded performance hint. A dropped prefetch cannot change a
query result.

## Mutation publication

Eligible point mutations use a resident inline overlay. Other mutations use a
routed copy-on-write path. Both paths publish one immutable generation.

`Collection.Update` gives one logical failure-atomic publication for rows and
exact postings. Preparation can publish a content-equivalent topology
generation before later validation rejects the logical batch. Thus,
Generation may advance even when no row or posting changes.

Do not use generation equality as the only test for a failed logical batch.
Compare the logical rows and indexes.

## Durable database catalog

`durable.OpenDatabase` resolves the directory to a stable absolute physical
path. Do not rename or replace the directory while it is open.

Default permissions are `0700` for a directory and `0600` for a file.

Each collection uses a primary file and an optional journal. The catalog
encodes a logical collection name as lowercase hexadecimal UTF-8 bytes:

```text
orders -> c-6f7264657273.vjc
```

The related journal appends `.rjournal`. The multi-collection decision log is
`txn.vtm`.

Collection names are byte identities. The catalog does not normalize Unicode.
Malformed canonical entries, symlink primaries, and recognizable case aliases
fail closed. Unrelated unrecognized files are ignored.

Drop removes and syncs the primary before it removes and syncs the journal. A
crash can leave an orphan journal, but it does not intentionally leave a live
primary without its required journal.

## Physical capacity

Normal files grow elastically. A sealed physical capacity is a specialized
configuration for a compatible async rooted layout. It proves or extends an
allocated prefix and does not eagerly write the complete ceiling.

`PhysicalCapacityBytes` requires asynchronous visibility, no recovery journal,
no canonical materialization, and a page-aligned representable capacity. The
platform must prove and synchronize strict allocation. Darwin does not provide
that proof and refuses this configuration.

A sealed recovery journal is Linux-only and synchronous. Its size must be
sector-aligned and large enough for the maximum conditional batch.

Free extents are reused. Optional hole punching can release physical blocks on
Linux and Darwin. Logical file high-water can stay large after physical release.

## Ownership rules

- Do not copy synchronized handles after first use.
- Close every durable snapshot.
- Keep borrowed bytes inside their callback or snapshot lifetime.
- Do not modify or replace an open primary, journal, catalog, lock entry, or
  database directory from another process.
- Copy a stopped complete database directory, not an isolated active file.
- Treat persistence errors as a close-and-reopen boundary.

The source does not define a safe live raw-file backup procedure.

## Implementation references

- `store/engine.go` and `store/store_database_snapshot.go`
- `store/store_database_txn.go` and `store/store_builder.go`
- `store/durable/store_file_open.go` and `store_file_operations.go`
- `store/durable/store_file_options.go` and `store_file_lifecycle.go`
- `store/durable/store_database.go` and `store_database_snapshot.go`
