# Primary write concurrency

The buffered-visible, schemaless, unindexed durable primary supports bounded
parallel point-mutation preparation. This is a current implementation
contract, not a roadmap or a benchmark report.

## Pipeline

An eligible mutation uses three stages:

1. **Private preparation.** A caller claims one preallocated scratch context
   and validates/canonicalizes a `Put` before entering the shared writer path.
2. **Leaf-local inspection.** Under `writer.RLock`, the caller routes to the
   exact `BucketID`, locks one of 4,096 padded stripes, reloads current state,
   and performs bounded lookup, slot, size, and patch analysis.
3. **Serialized publication.** A fixed combiner elects one leader for the
   arrived cohort. It assigns consecutive generations, installs fully
   initialized overlay records, and publishes one complete visibility cut.

The cohort combines independent point calls; it is not a user-visible atomic
batch. Capacity pressure may publish a valid prefix and send untouched calls
through the coordinated fallback. Every successful call still has one
linearization point and one generation.

## Resource bounds

- At most 32 scratch contexts exist per collection, capped by configured file
  visibility slots.
- Contexts, request records, completion signals, canonical workspaces, and the
  32-entry publisher arrays are allocated at open and do not grow on the hot
  path.
- The lane admits source documents no larger than half the maximum primary
  leaf extent, canonical output no larger than one leaf, and at most 8,192 JSON
  tape entries.
- Context acquisition uses bounded CAS attempts and a condition variable; it
  does not spin indefinitely or allocate an unbounded workspace.
- Same-leaf writers serialize. Different leaves serialize only on a stripe
  hash collision or at final publication.

## Eligibility

The lane exists only when all of these are true:

- durability is `DurabilityBufferedVisible`;
- per-mutation recovery-journal admission is disabled;
- the collection has no schema or exact indexes; and
- the unified primary overlay is enabled.

It rechecks eligibility under the shared writer lock and declines while a
primary epoch, online index build, or journal replay is active.

Eligible class-5 leaves support inline replacement, stable-slot resurrection,
bounded insertion, and deletion that leaves at least one live row. A missing
delete is a no-op. Immutable overlay append is safe with active snapshots;
same-size arena reuse raises its separate reader fence.

## Coordinated fallback

The exclusive path remains authoritative for stronger durability, explicit
journaling, schemas, indexes, online index construction, replay, overflow,
splits, slot exhaustion, final-row deletion, topology changes, snapshots,
flushes, and lifecycle work.

One pressure coordinator owns fold/retry so overlay exhaustion does not create
an exclusive-writer stampede. Checkpoints and structural mutations retain
`writer.Lock` and therefore include every earlier published shared-lane cut.
No background compaction is involved.

## Qualification

Tests cover disjoint and colliding stripes, same-leaf races, delete/restore,
consecutive generation assignment, stable-slot resurrection, complete reader
visibility, snapshots and direct epochs, reader-fenced reuse, crash recovery,
flush/snapshot overlap, pressure coordination, scratch exhaustion, and fixed
context ownership under race instrumentation. Performance evidence belongs in
[the benchmark results](../../bench/competitive/RESULTS.md), pinned to the
exact commit that produced it.
