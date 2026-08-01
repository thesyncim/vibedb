# Architecture

This is the front door to vibedb's storage design. The ordered-tablet primary
graph is the layout: every collection — freshly created, bulk-built, or
reopened — reads, mutates, checkpoints, and indexes against it. For public
APIs, see [store.md](store.md); for bytes on disk, see [format.md](format.md).

## The central rule: one reader-visible representation

Every published generation is complete. A point read, ordered scan, exact-index
probe, and query all begin from one immutable root. Primary reads and scans
follow one canonical graph. An exact probe resolves one captured index epoch:
its immutable spanned-leaf fold base plus the bounded generation-stamped
posting and liveness records visible at the reader's generation. No reader
consults a WAL, tombstone set, unbounded merge structure, or version list.

That rule expands into a family of invariants:

1. One root selects the document primary, indexes, allocator state, and catalog
   for a generation.
2. Hashes and compact certificates may reject candidates; exact keys and JSON
   values remain authoritative.
3. A successful delete removes the row and postings immediately. It creates no
   tombstone or future merge obligation.
4. Snapshot-visible bytes are immutable. A writer either owns a frame
   exclusively or uses copy-on-write.
5. Every queue, cache, transaction, lease table, and retired-extent set is
   bounded at open.
6. Publication happens only after every query-visible structure for the new
   generation is complete.

The primary graph realizes this with a `StateRoot` selecting an ordered-tablet
catalog, its leaves, and an exact-index root whose per-index catalogs route to
ordered bounded term leaves.

## The ordered-tablet graph

The graph is:

```text
StateRoot
  ├─ TabletCatalog                 global lexical fences
  │    └─ TabletRoot               tablet-local identity and anchor routing
  │         └─ AnchorPage          stable leaf rows and current PageRefs
  │              └─ OrderedHashLeaf
  │                   ├─ exact keys and JSON values in lexical-rank order
  │                   └─ stable slots used by exact-term postings
  └─ PrimaryExactRoot              one record per physical exact index
       └─ ExactCatalog             level 0, or level 1 over level-0 pages
            └─ PrimaryExactLeaf    ordered term slices and giant-term stripes
                 └─ exact term → posting tiles
```

`StateRoot` selects the whole generation. `TabletCatalog` maps global lexical
fences to tablet identities and roots. A `TabletRoot` maps stable local leaf
identities to anchor rows and routes lexical fences to anchor pages. Each
anchor row holds the current reference for one ordered hash leaf. The leaf
uses bounded hash candidates for point lookup but stores records in lexical
order for lower bounds and scans.

The resulting identity is hierarchical:

```text
BucketID = (18-bit TabletID << 12) | 12-bit LocalLeafID
```

Exact-term postings name stable `(BucketID, slot)` locations, and the exact
secondary index that maps a JSON term to those postings is maintained in the
same publish as the document mutation that changes it. Ordinary mutations emit
absolute per-term and per-tile records; a slot-reassigning structural rewrite
emits a bounded rebase. At a fold, affected content-defined runs or giant-term
stripes are re-encoded through the deterministic cutter, while untouched term
leaves carry their durable page references forward. A complete key comparison
still decides every primary hit. The byte-level routing and leaf bounds are
specified in [format.md](format.md).

## Canonical frames and frame-native staging

The page cache is not merely a copy of durable bytes. In the deferred-canonical
lanes (buffered-visible and the journal-backed synchronous lane), an owned cache
frame is the canonical reader-visible page for its stable reference. A mutation
edits or replaces complete frames, then publishes a new in-process root.
Repeated changes to the same owned frame can coalesce into one checkpoint
after-image without adding a reader overlay.

Two ownership questions must remain separate:

- **Reader-exclusive:** can a snapshot or sealed checkpoint still observe the
  old frame? If yes, the writer must COW it.
- **Durable-reachable:** can the last durable root still reach its physical
  extent? If yes, checkpointing must write a new extent before publishing the
  next durable root.

Frame-native staging means the committer refers directly to validated cache
frames rather than copying every page into a second per-commit buffer. The
queue owns bounded references and small publication records; the frames remain
pinned until the durability or checkpoint boundary releases them.

## Read path

A point read on the primary graph follows:

```text
read root
  → tablet catalog
  → tablet root
  → anchor page
  → ordered hash leaf
  → bounded hash candidates
  → exact key confirmation
```

The direct point read (`Collection.AppendRaw`) protects the read with one
epoch slot — no lock, no per-call generation lease. A reader announces the
generation it is about to read in a padded per-core slot; bounded publication
and retirement scans read those slots lock-free, and a retired extent is not
reused until no epoch or lease can still reach its generation. The read falls
back to the older
generation-lease path only when the epoch table declines the entry (full,
writer fence, or Close). A long-lived `Snapshot` still holds a generation lease
rather than an epoch slot.

An ordered scan follows the rooted tablet cursor for that snapshot. It never
trusts a physical sibling pointer that a later COW generation could make stale,
and it never sorts hash order or subtracts tombstones. Query execution takes an
explicit heap or durable snapshot and uses the same source for indexes, scans,
and document rechecks.

## SQL and JSON documents

The relational boundary has one language: SQL. Embedded callers use
`database/sql`; PostgreSQL clients use the protocol-v3 `pgwire` front door.
Both adapters consume the same typed `sql/driver` catalog/session runtime for
DDL, DML, SELECT, prepared statements, and transactions. JSON is the row
representation, not a second request grammar. The `sql` package parses,
`query` lowers onto the shared compiled evaluator, and `sql/driver` supplies
catalog, storage, and transaction policy. This keeps SELECT and mutation
predicates on one implementation instead of duplicating them in an adapter.

Each SQL table is a durable collection plus catalog metadata for a declared
JSON schema, one scalar document-derived primary-key path, and exact index
definitions. Indexes can be added online and become visible with their durable
catalog entry in one publication. SQL-created tables use the ordered primary
graph; single writes maintain compound exact postings in the same publication,
and the driver batches through `Update` only where the collection supports it.
Catalog replacement and first table-file creation include the
platform namespace durability fence in addition to the durable file fence. The
catalog and collection are reopened together, so schema validation and index
maintenance do not disappear across process restarts.

A multi-table SELECT captures all participating durable collections while
holding their publication gates, then retains one generation lease per table.
The captured generations therefore genuinely coexisted. A declared-field inner
join that must emit matching pairs uses the heap executor after admitting the
complete captured input against the current fixed, conservative 64 MiB
working-set bound. The leases protect the cut while it is copied, then close;
the heap copy owns the same cut through result production. An oversized fallback
fails before execution. A join inside a transaction materializes the BEGIN cut
plus the transaction overlay under the same bound.

This coherent read cut does not create a cross-table commit. One SQL transaction
may read several tables but writes exactly one, matching the largest atomic
publication the storage layer actually has.

## Write path

A mutation is planned against one state:

1. Validate and copy the key and complete JSON value.
2. Resolve the existing row and affected exact-index tuples.
3. Construct complete after-images for every touched primary leaf/page and
   metadata path, plus absolute exact-index term/tile records or a bounded
   structural rebase when stable slots move.
4. Recheck ownership and the source generation.
5. COW any snapshot-visible or durable-reachable bytes; patch an exclusively
   owned frame in place when every eligibility check passes.
6. Link the exact-index records or rebase for the rewritten bucket in the same
   publish.
7. Publish one new root after the selected acknowledgement boundary. On the
   journal-backed synchronous lane the redo record is appended and synced
   before that publish, so visibility follows durability.

`Update` batches distinct keys, rewrites each touched leaf once, descends each
directory once, and publishes one logical failure-atomic publication. It is the
transactional `WriteBatch` — the SQL driver's `COMMIT` and the group-commit
primitive both flow through it, and the logical batch is one journal record with
one CRC and one sync. If its final rows do not fit the current leaf topology, a
content-equivalent topology generation may publish first and the logical batch
publishes in the following generation. A later failure exposes no subset of the
batch, although `Generation` may advance because the representation-only shape
is retained. Automatic combining applies the same machinery to overlapping
`Put` and `Delete` calls. The query-visible graph is complete at publication;
writer-only planning state is never published.

## Checkpoint path

Deferred-canonical mutations normally perform no root device write per mutation.
Buffered-visible does no device write at all on ordinary admission; the
journal-backed synchronous lane makes each acknowledgement durable with one
journal append plus sync and still defers its root. `Flush` captures the current
visible generation, materializes its reachable dirty/new primary frames and
newly encoded dirty exact leaves, carries clean exact leaves by `PageRef`, and
rebuilds the exact catalogs and exact root:

```text
seal visible root
  → walk dirty primary children and parents bottom-up
  → fold dirty exact runs/stripes; retain clean leaf refs
  → write complete COW page set plus fresh exact catalogs/root
  → data ordering barrier
  → write alternate inline root
  → final persistence barrier
  → advance durable root and release eligible staging
```

If bounded frame or retirement capacity would be exhausted, admission may
force this path early. A successful checkpoint always names one atomic
generation cut. Recovery chooses the previous complete root before the final
root boundary and the checkpointed root after it.

Automatically persisted modes use the same page/root protocol per generation
or commit group. See [durability.md](durability.md) for the exact boundary in
each mode.

## Snapshots and copy-on-write

Snapshot creation retains the current immutable root and acquires a bounded
generation lease; it does no per-key work. Later mutations may share unchanged
pages with that root. The first mutation of bytes the snapshot can observe
creates the minimum replacement path, leaving the snapshot's graph intact.

Retired extents remain unavailable for reuse until no snapshot can reach them.
A long-lived snapshot therefore consumes bounded retirement capacity and can
backpressure writers, but it does not make current readers traverse old
versions. Durable snapshots must be closed promptly.

## What the design deliberately excludes

vibedb is not an LSM tree. It deliberately excludes reader-visible memtables,
sorted runs, point and range tombstones, version chains, and merge cursors.
Those structures can make acknowledgement cheap by deferring canonical
materialization. They also make reads reconcile representations and create
flush, compaction, and space-amplification debt.

vibedb chooses the opposite trade: bounded writer work produces one
semantically complete generation before publication. Deletes reclaim their
logical representation immediately, scans walk one ordered view, and snapshots
pin roots rather than versions inside a shared read path. The cost is real:
isolated random writes
can rewrite page paths, and it shows in the synchronous lane, where a
per-mutation durable acknowledgement currently trails SQLite. Writer batching,
owned-frame in-place updates, and the recovery-only journal are admissible
optimizations because they do not change what readers consult.

## Design map

- [Hybrid mutations](design/hybrid-mutations.md): read-neutral batching and
  buffered checkpoints.
- [Canonical materialization](design/canonical-materialization.md): in-place
  frame updates and the gated async capsule path with recovery undo.
- [Recovery journal](design/recovery-journal.md): the recovery-only redo that
  now backs the synchronous lane.
- [Parallel tablet writers](design/parallel-tablet-writers.md): future
  per-tablet concurrency.
- [Distributed sharding](design/distributed-sharding.md): future routed
  ownership, replication, failover, and online resharding across independent
  durable roots.
- [Unification](design/unification.md): the unified mutable collection and
  its remaining performance gates.
- [SQL surface](design/sql-surface.md): the shared `database/sql` and `pgwire`
  contract over JSON documents, schemas, exact indexes, joins, and
  transactions.
