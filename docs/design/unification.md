# Engine unification: one in-memory durable store

**Status:** in progress. The durable primary graph is now a mutable in-memory
canonical-frame engine with a transactional batch and one SQL language; what
remains is demoting the heap engine's mutable API, parallel writers, and
freezing the format.

**Idea:** one `Collection` operates on canonical frames, with either a real
device or a null device. Durability becomes a mode rather than a second
mutable engine.

This is the plan of record for collapsing the two mutable engines into one.
The goal is a single `Collection` that is simultaneously the in-memory store
and the durable store: reads and buffered writes execute against canonical
in-memory page frames at memory speed; durability is a property of the
configured checkpoint/durability mode, not a different engine.

The repository is unreleased. There is no compatibility obligation, so the
end state deletes duplicated machinery instead of deprecating it.

## Why

- Two mutable engines (`store.Collection` on the heap, `durable.Collection`
  on a page file) are two implementations of the same contract. Every
  feature, bug class, and benchmark is paid twice, and the architecture
  review lists the overlap as an open defect.
- The buffered-visible durable collection already holds its working set in
  canonical page-cache frames, and the ordered primary graph now mutates them
  in place: hot acknowledgements and point reads are measured in the sub-
  microsecond range (see [performance.md](../performance.md)). Once the read
  and acknowledgement targets (≤300 ns point, ≤0.45 µs update p50) are fully
  met, a separate heap engine has no performance reason to exist.
- An ephemeral collection is the same engine with a null device: identical
  API, identical semantics, durability disabled. One way of doing things.

## End-state surface

| Concept | Name |
| --- | --- |
| Mutable collection (the only one) | `Collection` |
| Immutable point-in-time view | `Snapshot` |
| Multi-collection catalog | `Database` |
| Typed wrapper | renamed to match the one noun set (no fifth word) |
| Document substrate | `DocSet` (kept: `Chunk` embeds it; executors build one per batch) |

`DocSet`/`Segment` remain internal substrate shared by the executor and the
page codecs. They are not a second engine.

## Sequencing (performance first, cutover second)

Executed:

1. **Primary read paths — done.** Tablet catalog root, point read, and lexical
   cursors read against the ordered primary graph, with epoch-protected direct
   reads (`93834e8`) replacing the per-call lease on the hot path.
2. **Bulk build and mutations — done.** Stable-slot update/delete and buffered
   acknowledgement against owned canonical leaf frames; the write batch is
   transactional on the primary graph (`a3ee052`), exact indexes are maintained
   in the same publish (`7e6f28e`), and the synchronous lane acknowledges
   through the journal (`70d39ea`).
3. **One SQL product — done.** One typed SQL runtime owns the durable catalog
   and runs transactions on the mutable chunk layout's atomic batch
   (`886c5fe`); both `database/sql` and `pgwire` consume it, and the second SQL
   surface was deleted (`0611fb9`). Moving that catalog onto the surviving
   primary graph belongs to the later engine-unification cutover rather than
   being implied by the SQL surface.
4. **Query-language collapse — done.** SQL is the only textual query language.
   JSON remains the stored row/document representation, and the Go builder
   remains a typed programmatic plan API rather than a second request grammar.
   The JSON query-document parser and its compatibility syntax were deleted
   before release.

Remaining:

5. **Heap mutable API demotion.** Collapse the heap `store.Collection` mutable
   surface into the one durable engine with a null device for the ephemeral
   mode; `Builder` feeds the bulk path directly. (Owned by later work.)
6. **Parallel tablet writers.** See
   [parallel-tablet-writers.md](parallel-tablet-writers.md).
7. **Deletions and format freeze.** Delete the legacy fingerprint/chunk
   primary and every codepath that exists only to keep two engines behaving
   alike; freeze the surviving format. Golden tests and docs/format.md follow
   the surviving format only.

Each stage lands only behind the measured gates; a stage that regresses a
published read/scan/space number does not merge.
