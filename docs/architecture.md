# Architecture

This is the front door to vibedb's storage design. It separates the invariants
that apply today from the ordered-tablet primary being qualified for promotion.
For public APIs, see [store.md](store.md); for bytes on disk, see
[format.md](format.md).

## The central rule: one reader-visible representation

Every published generation is complete. A point read, ordered scan, exact-index
probe, and query all begin from one immutable root and follow one canonical
graph. No reader reconciles a base with a memtable, WAL, tombstone set, delta
chain, or version list.

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

The current primary realizes this with a `StateRoot`, chunk directory,
fingerprint directory, document pages, and exact-index directory. The
promotion target replaces that primary with ordered tablets while preserving
the same invariant family.

## The ordered-tablet graph

The target graph is:

```text
StateRoot
  └─ TabletCatalog                 global lexical fences
       └─ TabletRoot               tablet-local identity and anchor routing
            └─ AnchorPage          stable leaf rows and current PageRefs
                 └─ OrderedHashLeaf
                      ├─ exact keys and JSON values in lexical-rank order
                      └─ stable slots used by exact-term postings
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

Exact-term postings name stable `(BucketID, slot)` locations. A complete key
comparison still decides every primary hit. See the
[ordered-hybrid promotion specification](design/ordered-hybrid-store.md) for
measured primitives, scale bounds, and promotion gates.

## Canonical frames and frame-native staging

The page cache is not merely a copy of durable bytes. In buffered-visible mode,
an owned cache frame is the canonical reader-visible page for its stable
reference. A mutation edits or replaces complete frames, then publishes a new
in-process root. Repeated changes to the same owned frame can coalesce into one
checkpoint after-image without adding a reader overlay.

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

For the current primary, a point read follows:

```text
snapshot root
  → fingerprint directory candidate
  → chunk directory
  → document page
  → exact key confirmation
  → exact JSON bytes
```

The ordered-tablet target follows:

```text
snapshot root
  → tablet catalog
  → tablet root
  → anchor page
  → ordered hash leaf
  → bounded hash candidates
  → exact key confirmation
```

An ordered scan follows the rooted directory or tablet cursor for that
snapshot. It never trusts a physical sibling pointer that a later COW
generation could make stale, and it never sorts hash order or subtracts
tombstones. Query execution takes an explicit heap or durable snapshot and
uses the same source for indexes, scans, and document rechecks.

## Write path

A mutation is planned against one state:

1. Validate and copy the key and complete JSON value.
2. Resolve the existing row and affected exact-index tuples.
3. Construct complete after-images for every touched leaf/page and metadata
   path.
4. Recheck ownership and the source generation.
5. COW any snapshot-visible or durable-reachable bytes; use qualified canonical
   materialization only when every eligibility check passes.
6. Publish one new root after the selected acknowledgement boundary.

`Update` batches distinct keys, rebuilds each touched chunk once, descends each
directory once, and publishes one failure-atomic generation. Automatic
combining applies the same machinery to overlapping `Put` and `Delete` calls.
The query-visible graph is complete at publication; writer-only planning state
is never published.

## Checkpoint path

Buffered-visible mutations normally perform no device write. `Flush` captures
the current visible generation and materializes only its reachable dirty/new
frames:

```text
seal visible root
  → walk dirty children and parents bottom-up
  → write complete COW page set
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

vibedb chooses the opposite trade: bounded writer work produces one canonical
generation before publication. Deletes reclaim their logical representation
immediately, scans walk one ordered view, and snapshots pin roots rather than
versions inside a shared read path. The cost is real: isolated random writes
can rewrite page paths, and the current mixed-workload results expose that
gap. Writer batching, owned-frame updates, and recovery-only journaling are
admissible optimizations because they do not change what readers consult.

## Design map

- [Ordered hybrid store](design/ordered-hybrid-store.md): target primary and
  exact-term index.
- [Hybrid mutations](design/hybrid-mutations.md): read-neutral batching and
  buffered checkpoints.
- [Canonical materialization](design/canonical-materialization.md): qualified
  in-place updates with recovery undo.
- [Template-columnar leaves](design/template-columnar-leaves.md): optional
  leaf codec and typed access.
- [Recovery journal](design/recovery-journal.md): current recovery-only redo.
- [Retained change log](design/retained-change-log.md): optional bounded logical
  commit history for crash recovery and CDC replay.
- [Parallel tablet writers](design/parallel-tablet-writers.md): future
  per-tablet concurrency.
- [Unification](design/unification.md): one eventual mutable collection.
