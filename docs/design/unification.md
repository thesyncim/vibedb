# Engine unification: one in-memory durable store

**Status:** plan of record; not the current public surface.

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
  canonical page-cache frames; with the ordered-hybrid primary its read and
  acknowledgement gates (≤300 ns point, ≤0.45 µs update p50) are in-memory
  numbers. At those gates a separate heap engine has no performance reason
  to exist.
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

1. **vNext primary read paths** (ordered-hybrid steps 4–5): tablet catalog
   root, point read, lexical cursors, with the promotion gates from
   [ordered-hybrid-store.md](ordered-hybrid-store.md). The cutover must not hand unified users a
   slower read path than the heap engine they lose.
2. **vNext bulk build and mutations** (steps 6–7): stable-slot update/delete,
   buffered acknowledgement against owned canonical leaf frames.
3. **API cutover**: merge the public durable surface into the one collection
   package; `Builder` feeds the bulk path directly; add the null-device
   ephemeral mode; port `query`, `sql`, `pgwire`, and `vibesql` to the one
   engine.
4. **Deletions** (step 11): legacy fingerprint/chunk primary, the heap
   engine's public mutable API, and every codepath that exists only to keep
   two engines behaving alike. Golden tests and docs/format.md follow the
   surviving format only.

Each stage lands only behind the measured gates; a stage that regresses a
published read/scan/space number does not merge.
