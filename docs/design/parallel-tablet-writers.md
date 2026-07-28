# Parallel tablet writers

**Status:** future design; projections only until the harness's 8- and
64-writer lanes run.

**Idea:** serialize mutations within one tablet, execute distinct tablets in
parallel, and batch only shared catalog/root edits through an epoch publisher.

This design is intra-process concurrency inside one physical collection.
Distributed ownership and replication use
[distributed sharding](distributed-sharding.md); they do not reuse `TabletID`
as a network-shard identity.

The single-writer lane must not regress, and the honest serialization floor is
recorded in [Honest limits](#honest-limits).

## Why the format already permits this

A tablet is an independent copy-on-write subtree: its leaves, anchor
pages, dense locator, and tablet root reference nothing outside the
tablet. The shared structures are the catalog (fences to tablet roots)
and the state root — and under the canonical-frame model, an eligible
same-size update patches an owned frame in place and moves no reference
at all. The write path therefore splits naturally:

- **Ref-preserving mutations** (the hot majority under skew): tablet-local
  frame work, no shared-structure contention, parallel across tablets by
  construction.
- **Ref-changing mutations** (first-touch COW, size class changes,
  inserts, deletes, splits): tablet-local staging plus one shared
  publication.

## Design

1. **Tablet write tokens.** A mutation routes to its tablet, acquires that
   tablet's token (a fixed array indexed by TabletID; no allocation), and
   performs all leaf/anchor/locator work under it. Distinct tablets never
   contend. Per-key ordering follows from single-token-per-tablet.
2. **Epoch publisher, PALM-style.** Ref changes enqueue (tablet, edits)
   to a bounded publication queue. One publisher per collection drains an
   epoch: partitions edits by catalog node and state root, rewrites every
   touched shared page exactly once, assigns the epoch's generation, and
   publishes one root. This is the batched-descent discipline the
   combiner already proved on the legacy directories, applied at the
   catalog tier.
3. **Visibility.** Ref-preserving patches are visible immediately through
   the unmoved references, exactly as buffered mode works today.
   Ref-changing mutations become visible at their epoch's publication.
   Both acknowledge per their durability lane; linearizability per key
   holds via the tablet token, and cross-key consistency is epoch-atomic.
4. **Checkpoints seal by epoch fence.** The sealer flips the epoch,
   waits only for in-flight holders of the old epoch (token generation
   stamps, bounded), and captures — new mutations proceed in the next
   epoch. No global write stall.
5. **The recovery journal composes multiplicatively.** The single-writer
   journal and its batch record — one record, one CRC, one sync for a whole
   group — have landed as the sync lane's acknowledgement; the group-commit
   primitive is [already the write batch's mechanism](recovery-journal.md#batch-records--the-group-commit-primitive).
   This phase generalizes it: writers append redo records concurrently and a
   group-commit leader shares one journal sync per window across every waiting
   writer, so the synchronous lane's throughput scales with writer count
   instead of dividing the fsync budget.
6. **Backpressure** stays bounded per tablet (dirty frames, staged edits)
   and globally (existing budgets). A tablet split under token ownership
   follows the spec's bounded structural transaction unchanged.

## Sequencing

1. Single-writer primary-graph mutations (stable slots, COW publication,
   the journal-backed sync lane) have landed and are unchanged by this phase.
2. Tablet tokens + epoch publisher behind the same single public writer
   entry (the combiner becomes the multi-writer front door).
3. Journal group commit across writers.
4. The harness's 8- and 64-writer lanes become promotion gates; the
   single-writer numbers must not regress.

## Honest limits

The publisher serializes shared-page rewrites; a uniform ref-changing
flood (bulk random inserts) degrades toward batched single-writer
throughput — which is the correct floor, since those mutations contend on
physical shared state. Skewed and read-heavy workloads, the common case,
gain nearly linearly with tablets touched.
