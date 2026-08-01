# Store

This document describes the canonical `store` and `store/durable` APIs in the
current tree. Public Go documentation remains authoritative for individual
methods and option fields.

This file describes the implemented APIs; [architecture.md](architecture.md)
describes the current runtime shape and [format.md](format.md) is authoritative
for durable bytes.

## Storage surfaces

| Surface | Purpose | Persistence |
| --- | --- | --- |
| `store.Segment` | Immutable self-contained batch of documents with its own tape, shape, and column machinery | Explicit segment image |
| `store.Collection` | Mutable in-memory keyed collection with immutable snapshots | None; bulk source for `durable.CreateFromPrimary` |
| `store.Database` | In-memory catalog and consistent snapshot of independent collections | None beyond each collection |
| `store.Builder` | Bulk construction of a `store.Collection` | None |
| `durable.Collection` | Bounded-residency durable collection | Automatic incremental commits |
| `durable.Database` | Directory catalog and process-consistent snapshot; one durable file per collection | Per-collection automatic commits |

A collection is one physical JSON namespace. `store.Database` catalogs
in-memory collections. `durable.Database` catalogs one collection file per
name in a directory. Its multi-collection snapshot is a consistent in-process
read cut; it does not make separate collection writes one crash-atomic or
transactional operation.

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

Eligible schemaless, unindexed, inline point mutations prepare concurrently in
fixed per-writer scratch contexts. Routing and leaf inspection use 4,096 lock
stripes hashed from the full bucket/leaf identity, followed by a short bounded
publisher-combiner stage. Structural work, `Update` batches, indexed or
schema-validated writes, overflow values, and other ineligible shapes retain the
exclusive fallback. See [parallel primary writers](design/parallel-tablet-writers.md)
for the exact eligibility and resource bounds.

### Batched mutation

`durable.Collection.Update` applies many mutations as one logical
failure-atomic publication:

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

The logical batch rewrites each touched primary leaf once rather than once per
document, covers the group with one journal record synced once, and publishes
all row and exact-posting changes together. If its final rows do not fit the
current leaf topology, it may first publish one content-equivalent topology
generation and publish the logical batch in the following generation. The
logical changes either publish whole or publish nothing: an error from the
closure or a staged mutation exposes no subset of the batch. If an error occurs
after topology preparation, rows and exact postings remain unchanged but
`Generation` may advance.

`Options.MaxBatchDocuments` bounds how many distinct keys one `Update` may carry
and sizes the transaction reservation; zero selects 64. A larger batch reports
`ErrBatchTooLarge` and publishes nothing, so callers split rather than discover a
half-applied batch after a crash. Keys are deduplicated as they are recorded, so
mutating the same key twice keeps only the last mutation. Document syntax is
validated when `Update` applies the batch, not when `Put` records it.

### Snapshots and reads

`Snapshot` is O(1) and does not wait for an in-progress writer. A snapshot stays
valid after any later write.

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

`DropIndex` removes the logical index immediately. Chunk-level postings exist
only when `Options.Postings` requested them at build time, so no reclamation
follows a drop.

### Bulk construction

`store.Builder` accepts unique keys and validates and copies documents directly
into final chunks before publishing one collection; indexes are declared on the
built collection through `CreateIndex` plus `BackfillIndex`. `Append` is
single-goroutine. `Build` transfers completed state and closes the builder.
Once final key compaction succeeds, Build is terminal; a later failure releases
every unpublished external block and the builder cannot be retried.

Use the builder for an initial corpus and `Collection.Put` for individual
mutations.

## Persisting an in-memory collection

The heap store has no image checkpoint of its own. Persistence is bulk
conversion: `durable.CreateFromPrimary` walks a completed in-memory collection
once and writes one immutable ordered primary graph, after which reads and
mutations continue against the durable collection.

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

`Put`, `Delete`, and `Update` publish one complete generation. Applications do
not rewrite a checkpoint after each operation.

The zero-value `DurabilitySync` mode appends and power-safely syncs one bounded
recovery-journal record before applying and publishing the mutation; the root
is folded at a later checkpoint. `DurabilityAsyncVisible` is the explicit
asynchronous opt-in: a mutation becomes reader-visible when the bounded
committer accepts it. `DurabilityBufferedVisible` publishes after bounded
memory admission and relies on `Flush` or `Close` for crash persistence. Use:

- `DurableGeneration` to observe the last fenced generation;
- `Flush` to wait until the current visible generation is durable;
- `Close` to stop new work, drain commits, and release owned resources.

`CommitCoalesce` bounds optional grouping for the background committer and the
opt-in buffered-journal lane. It does not delay the journal-before-publish
`DurabilitySync` path.

Every ordinary buffered-visible primary store also owns a recovery-journal
sibling. An eligible class-5-only `Flush` can persist the complete consecutive
overlay interval as one format-v1 batch and one sync without first rewriting
the physical root. Compact entries cover existing-key integer, boolean, or null
replacements; deletes and unqualified puts retain logical full redo. A
structural publication, interval gap, exact capacity miss, or staging-pressure
guard falls back to the bounded physical checkpoint.

The ordinary v1 delta journal's current shipped geometry preallocates a 2.5 MiB
record region plus two 512-byte headers. The foreground guard keeps up to
512 KiB for one estimated future carried suffix, leaving the qualified 2 MiB
current append window. Linux normally reserves the complete region with
`fallocate`; the unsupported fallback and other platforms set its full size
once with truncate. Later acknowledgement writes are positional and never grow
the file. Per-mutation acknowledgement journals use their own option-derived
bounded capacity instead.

Journal format v0 remains the put/delete grammar and stays v0 when reopened;
current writers never place a scalar-patch kind into it. Format v1 is accepted
only for the ordinary buffered-delta lane. Each compact patch records the old
canonical span, new scalar spelling, and checksum of the expected complete
canonical document. Recovery reconstructs that result and fails closed unless
the checksum matches. Unknown versions, v1 entries in v0, malformed metadata,
or reopening v1 under an incompatible durability lane also fail closed.

V1 batch entries represent consecutive generations ending at the batch target.
If replay physically checkpoints a prefix under bounded pressure and crashes
again, the next Open uses the selected root generation to skip exactly that
durable prefix and resumes the suffix. It never reapplies a length-changing
scalar patch. Legacy v0 batches keep their one-generation, replay-from-zero
semantics. See [the recovery-journal design](design/recovery-journal.md) for the
record and crash-window details.

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

### Concurrent primary mutation lane

The narrow ordinary buffered-visible primary overlay can overlap independent
`Put` and `Delete` preparation without giving structural work a second,
uncoordinated writer. It is enabled only for a schemaless, unindexed collection
using the unified primary overlay with `DurabilityBufferedVisible` and without
`Options.RecoveryJournal`. Recovery replay, an online index build, an active
exact-index epoch, or any other lane uses the established exclusive path.

Eligible collections allocate a fixed scratch pool at open. Its size is the
configured visibility-slot count capped at 32 contexts. Every context receives
its maximum JSON index, canonicalization, token-span, and publication storage
up front and is reused; pool exhaustion waits on the bounded pool rather than
creating goroutine-local scratch. For `Put`, syntax validation and canonical
construction happen in the caller-owned context before taking the shared side
of the collection writer gate. The locked phase then rechecks close,
persistence, and mode state before using that provisional result.

Under the shared writer gate, routing produces the complete 30-bit `BucketID`.
A full-bit integer mix—not the tablet id or the low local-id bits—selects one of
4,096 cache-line-padded mutex stripes. The stripe serializes leaf acquisition,
existing-key inspection, stable-slot choice, and per-leaf overlay accounting
only for colliding bucket identities; unrelated buckets can perform those
stages concurrently.

Prepared requests enter a fixed-capacity flat-combining publisher. One caller
drains at most the 32 context-backed requests that reached the queue together,
assigns their consecutive generations, links their immutable overlay records,
and exposes one final router/state visibility cut. Leadership is handed to a
later arrival rather than allowing one producer stream to retain it. This short
publisher preserves the overlay's single-producer and global generation-order
contracts without moving JSON validation or leaf inspection back under an
exclusive collection writer.

The concurrent lane admits only leaf-local operations whose final bounds are
known: an inline replacement, resurrection of a tombstoned stable slot, an
insert that can claim a free stable slot and still fits the leaf's exact trivial
content bound, or an existing inline-row delete that does not empty the leaf.
Already-missing/deleted keys retain normal API semantics. Overflow values or
rows, non-unified leaves, stable-slot exhaustion, a required split, deletion of
the final row, structural metadata changes, and overlay/cache pressure fall
back safely to the exclusive path. A pressure cohort elects one coordinated
exclusive fold/retry so it does not enqueue one full checkpoint per caller.

This lane is bounded concurrency, not a lock-free algorithm or a claim of
linear scaling. Same-bucket or hash-colliding operations serialize on a stripe;
generation assignment and the final visibility cut serialize in the bounded
publisher; structural/checkpoint work still fences the shared gate exclusively.

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

Once a physical root is durable, the recovery journal has been recycled through
that root, and active snapshot generations exclude an extent, the same
foreground completion can return its filesystem blocks online. A
physical-generation guard grants one pass per exact newly authoritative root;
repeated completion and journal-only Flushes add no pass. Candidate discovery
is bounded to 1,024 exact free identities and 64 coalesced physical runs across
three independently advancing sources. Active sources redistribute unused
shares for at most three rounds. Spending is capped separately at six
successful deallocation calls and 20 MiB per physical generation; oversized
identities retain progress and continue at later boundaries.

The direct-reader fence and `snapshotGate` protect only coherent generation
sampling and the fixed candidate copy. Both are released before validation and
before any hole-punch syscall, while the collection writer lock prevents the
allocator from reusing a copied range. Linux uses
`fallocate(PUNCH_HOLE|KEEP_SIZE)` and Darwin uses `F_PUNCHHOLE`. `EINTR` is
retried at most four times. An unsupported platform/filesystem or any other
syscall error increments the corresponding hole-punch statistics and disables
later attempts for that open collection; it does not poison the writer or fail
the completed checkpoint.

This is the normal online reclamation path. It requires no background
compaction and no offline maintenance cycle. Logical extent reuse remains
available on unsupported filesystems, although returning blocks to the host
filesystem then requires an explicit rewrite such as `Repack`.

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

### Durable indexes

`durable.Options.Indexes` declares the initial exact scalar or compound index
names. A durable collection supports up to 4,096 logical names over at most 64
distinct ordered path definitions; logical aliases share one physical index.
`CreateIndex` / `CreateIndexContext` can build another definition over a live
collection and atomically publish its ready postings with the updated durable
catalog. The catalog is authoritative on reopen, while supplied option
definitions act as an assertion. Each write maintains the published postings
transactionally. One physical index spans deterministic, bounded term leaves
behind an ordered catalog; a giant term may span consecutive fixed-tile stripe
pieces, so posting volume is not capped by one 64 KiB leaf.

### Bulk creation

`durable.CreateFromPrimary` converts a completed in-memory store directly into
one durable generation. It preserves keys and declared exact indexes without
replaying individual `Put` calls. There is no document-format option: empty
creation, bulk creation, point mutations, batch mutations, structural
split/merge, and checkpoint fold all emit the same canonical unified leaf
grammar.

Each leaf chooses its physical extent from 4–64 KiB and chooses templated,
dictionary-backed, typed-token, or canonical-trivial row spellings inside that
one grammar. These are per-row encodings, not store modes. Bulk build and
checkpoint fold share the same deterministic planner, so a mutation does not
decompress or permanently fall back to another leaf representation.

## Allocation and ownership

The caller-buffered operations are the steady-state allocation boundary:

- heap and durable `Snapshot.AppendRaw`;
- compiled-key reads;
- bitmap and masked-row appenders;
- reusable query `Result`, `Workspace`, and file-execution workspace.

An undersized destination may grow. A new index/query high-water mark, custom
method, or oversized value may allocate. Zero-allocation claims apply only to
the documented warmed path, not every convenience call.

For query execution, `ExecOptions.MemoryBytes` is a hard admission limit for
heap/Segment row-proportional work and data-dependent planner, join, and JSON
containment storage. On the durable backend it is a batch/merge target rather
than a total RSS ceiling: fixed worker/ring minima and one document bounded by
the collection's `MaxDocumentBytes` schema may exceed the target. Durable
candidate planning, join membership, and containment tapes still fail before
unbounded growth; `ResultBytes`, `AggregateBytes`, `JoinPairBytes`, and
`SpillBytes` govern their separate resource classes.

Long-running core-query executions can use an optional reusable
`query.CancelFlag`:

```go
var stop query.CancelFlag
exec.Options.Cancel = &stop

// A control goroutine may call stop.Cancel() while RunInto is active.
err := prepared.RunInto(&exec, source)
if errors.Is(err, query.ErrCanceled) {
	// No partial Result is exposed. Workers, snapshot bindings, and spill files
	// have already been cleaned up, so exec can be reused.
}
stop.Reset() // only after every execution using stop has returned
```

Cancellation is cooperative. Scans, durable batches, joins, DML filters, and
spill I/O poll at phase boundaries and inside long executor loops, then finish
the internal drain and cleanup protocol before returning `query.ErrCanceled`. Leaving
`ExecOptions.Cancel` nil keeps the default hot path allocation-free and avoids
an atomic load; installing a dormant flag also remains allocation-free.

The in-memory store copies `Put` input. `durable.Collection` copies writes and
uses explicit snapshot leases for reads.

### Memory accounting

`store.Stats` reports external key, document, and packed-index bytes. Those
counters do not include the Go heap or total process RSS.

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

- heap collections serialize mutations; durable structural, indexed,
  journaled, and fallback mutations use the exclusive writer, while the narrow
  buffered inline-primary cases described above may overlap through fixed
  scratch contexts, bucket stripes, and the bounded publisher.
- `store.Snapshot` values are immutable and concurrent-safe.
- `durable.Snapshot` is immutable but owns a closeable lease.
- Prepared queries are concurrent-safe with a separate result/workspace pair
  per execution.
- `store.Builder`, query workspaces, readers, writers, and mutable result buffers
  are single-consumer.

## Current product boundaries

The repository currently has no replication, backup manager, point-in-time
restore, distributed execution, or cross-file transaction. The PostgreSQL
protocol-v3 server can expose the typed SQL catalog directly;
that path supports the documented DDL, DML, SELECT, prepared-statement, join,
and transaction subset. A nested integration module exercises pinned pgx v5
and lib/pq releases over loopback TCP in CI. That narrow evidence does not
imply general PostgreSQL compatibility or catalog emulation.

The core does provide two multi-collection catalogs:

- `store.Database` owns heap collections and takes a consistent heap snapshot.
- `durable.Database` owns one durable file per collection and takes a
  process-consistent leased read cut. `durable.SnapshotCollections` provides
  the same read cut for a catalog that owns the collection handles itself.

Neither catalog makes writes to separate collections atomic or crash-atomic.
Their shared guarantee is a coherent read snapshot.

The query engine runs existence and fan-out joins over heap database snapshots.
Its direct durable path currently runs existence joins but does not materialize
fan-out rows. The shared typed SQL runtime used by both `database/sql` and
`pgwire` closes that product-level gap for its supported declared-field inner
join: it captures the tables through `durable.SnapshotCollections`, admits the
complete input against a fixed, conservative 64 MiB working-set bound, copies
that coherent cut into the heap executor, and fails before execution if the
bound would be exceeded. The SQL catalog persists table names, JSON schemas,
primary-key paths, and exact-index definitions; it still commits one table at
a time.

Broader distributed and cross-file transactional features are not implied by
these catalog and join APIs.
