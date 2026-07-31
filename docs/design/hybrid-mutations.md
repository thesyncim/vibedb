# Read-neutral mutations

**Status:** implemented for point mutations and supported transactional
batches on the ordered primary graph. Indexed multi-document batches remain
unsupported and fail closed.

## Rule

A mutation publishes one complete canonical generation. Readers never consult
a memtable, tombstone table, delta chain, or write queue. Writer-side batching
and deferred checkpoint work are allowed only when they preserve that single
reader-visible representation.

## Point mutations

`Put` and `Delete` validate and stage all fallible work before publication.
The next root, primary leaf, exact-index posting changes, allocator state, and
retirements become visible together. A failed mutation publishes nothing.

Overlapping synchronous point calls may enter the bounded mutation combiner.
It copies admitted inputs, applies up to 64 unindexed inline mutations as one
transactional batch, and shares one journal barrier. The queue and worker are
writer-only; an uncontended call stays on the direct point path and readers
never consult the queue.

When the writer exclusively owns an eligible canonical cache frame, a
same-length update may patch that frame and defer resealing until checkpoint.
Every failed eligibility check uses copy-on-write. The optimization changes
writer work, not the snapshot or read path.

## Transactional batches

`Collection.Update` records mutations in a bounded `WriteBatch`. Duplicate keys
collapse to their final operation, each touched primary leaf is rewritten once,
and one generation is published. `MaxBatchDocuments` and the option-derived
byte bounds reject oversized batches before partial publication.

Callers must check `Collection.SupportsUpdate`. A collection configuration the
batch path cannot update atomically returns the corresponding typed error; it
does not fall back to a sequence of point mutations. In particular, indexed
multi-document batches remain future work.

## Durability lanes

- `DurabilitySync` uses the recovery journal before acknowledgement and
  visibility.
- `DurabilityAsyncVisible` admits bounded asynchronous work and may lose a
  recent acknowledged mutation on process or machine failure.
- `DurabilityBufferedVisible` publishes after bounded memory admission and
  relies on `Flush` or `Close` for crash persistence.

A checkpoint folds the current visible cut into canonical pages and publishes
one durable root. Queue and cache pressure may force an earlier checkpoint but
never a weaker durability result than the selected mode.

## Required invariants

- a snapshot observes all or none of a mutation or supported batch;
- primary rows and exact-index postings name the same generation;
- journal replay produces the same logical result as uninterrupted execution;
- every queue, batch, frame, and retirement set is bounded at open;
- warmed point mutations and supported batches retain their allocation gates;
- failure injection at every prepare, journal, data, and root boundary exposes
  only a verified prior or committed generation.

See [durability](../durability.md) for the public contracts and
[recovery journal](recovery-journal.md) for the redo ordering.
