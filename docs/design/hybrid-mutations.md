# Bounded foreground mutations

**Status:** implemented for point mutations and transactional batches on the
ordered primary graph, including batches that maintain exact indexes. Batch
support is selected by publication lane, not by whether the collection is
indexed.

## Rule

A mutation publishes one semantically complete generation. Deferred primary
lanes may represent it as an immutable canonical base plus a bounded,
generation-stamped row overlay; readers resolve point values and scans against
that exact cut, including overlay delete records. Readers never consult the
writer queue or recovery journal. Batching and deferred checkpoint work remain
bounded, and pressure is handled by foreground fold/backpressure rather than
background compaction.

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
The unified primary lane may instead link an immutable row record. Failed
eligibility checks use the exclusive structural/copy-on-write fallback. Both
forms preserve the same generation-stable logical answer, although overlay
records are part of the read path until folded.

## Transactional batches

`Collection.Update` records mutations in a bounded `WriteBatch`. Duplicate keys
collapse to their final operation, each touched primary leaf is rewritten once,
and the row and exact-posting changes form one logical failure-atomic
publication. When the final rows cannot fit the current leaf topology, a
content-equivalent topology generation may publish first and the logical batch
publishes in the following generation. A later failure cannot expose a subset
of the logical changes, but `Generation` may advance because the
representation-only topology is retained. `MaxBatchDocuments` and the
option-derived byte bounds reject oversized batches before partial logical
publication.

Callers must check `Collection.SupportsUpdate`. A collection configuration the
batch path cannot update atomically returns the corresponding typed error; it
does not fall back to a sequence of point mutations. Buffered-visible and
journal-backed synchronous collections support both indexed and unindexed
batches. `DurabilityAsyncVisible` and a journal-less synchronous reopen publish
through committer generation fences instead, so `Update` fails closed there
with `ErrPrimaryBatchUnsupportedLane`.

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
