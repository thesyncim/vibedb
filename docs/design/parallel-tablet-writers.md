# Parallel tablet writers

**Status:** future design for parallel mutation staging inside one collection.
The current implementation can combine overlapping synchronous point calls,
but it still serializes each group's mutation staging and publication.

## Goal

Allow mutations routed to different tablets to perform their fallible work in
parallel while retaining one atomic, ordered publication point. This is local
in-process concurrency; distributed ownership and replication remain separate.

Every mutation keeps three explicit stages:

1. **Stage:** route, validate, reserve resources, prepare leaf changes, and
   derive exact-index posting changes in tablet-local scratch.
2. **Fence:** group the mutations covered by one recovery-journal record and
   durability barrier when the selected durability mode requires it.
3. **Publish:** under the collection-wide snapshot gate and reader fence,
   install router rows, exact-index state, page-validation state, and the new
   visible root in one serialized section.

The stage may be parallel. Fence strength follows the configured durability
mode. Publish remains single-writer.

## Current constraints

- A tablet token serializes two writers that target the same tablet.
- The resident router seqlock has one publisher; concurrent router mutation is
  forbidden.
- Exact-index posting tiles derive from stable `(BucketID, slot)` identities.
  Tablet-disjoint writers prepare disjoint contributions, but one publication
  links them to the document generation.
- Structural splits and merges remain stop-the-world checkpoints. Their rarity
  and cross-page retirement rules do not justify a second publication model.
- A logical batch spanning tablets is one atomic group. Partial publication is
  never an allowed pressure fallback.
- Admission, queue, journal, cache-frame, and transaction storage remains
  bounded by options fixed at open.

## Sequencing

1. Measure one- and multi-client buffered, ordinary-sync, and power-safe lanes
   with the same corpus and durability contract.
2. Introduce bounded tablet tokens and per-writer staging frames without
   changing publication.
3. Group staged mutations into one journal record and durability ticket.
4. Publish a group with one snapshot-gate/reader-fence section.
5. Extend exact-index contribution staging and indexed batches only after the
   unindexed path passes its gates.

## Correctness gates

- race-detector stress covers tablet routing, cache admission, cancellation,
  Close, splits, merges, and checkpoint overlap;
- every acknowledged group reopens as either entirely absent or entirely
  present at each injected crash boundary;
- snapshots observe a single generation and never a mixture of group members;
- exact-index probes remain byte-for-byte equivalent to a rebuild after
  concurrent indexed mutations;
- same-tablet contention preserves per-key linearizability;
- no steady-state mutation allocation appears after bounded arenas warm;
- the serialized publish section remains a minority of total work at the first
  supported writer count; otherwise batching depth is adjusted before adding
  another publication mechanism.

## Honest limits

Writers targeting one tablet still serialize. Power-safe groups still pay the
device's strongest fence, and that fence can hold the participating tablet
tokens. A structural flood approaches the current single-writer rate because
splits and merges retain the global checkpoint path. These are explicit floors,
not reasons to weaken snapshot or durability semantics.
