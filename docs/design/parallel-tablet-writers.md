# Parallel primary writers

**Status:** first phase implemented at commit `7fe6769`; the current
compact-default buffered-visible, schemaless, unindexed primary path was
qualified at clean commit `8c11423`.

The filename is historical. The implementation does not assign one writer token
per tablet: it hashes the full routed `BucketID` (the leaf identity) into 4,096
lock stripes. Independent leaves in the same tablet can therefore stage work in
parallel. A hash collision only adds harmless serialization between the two
colliding leaves.

## Implemented pipeline

An eligible point mutation has three bounded stages:

1. **Private preparation.** A caller claims one fixed scratch context and, for a
   `Put`, validates and canonicalizes JSON before acquiring `writer.RLock`.
   This CPU-private work does not convoy a waiting checkpoint or structural
   writer.
2. **Shared routing and leaf inspection.** Under `writer.RLock`, callers route in
   parallel. Each caller then locks only the stripe selected from the full
   routed `BucketID`, reloads state after acquiring it, and performs leaf-local
   lookup, slot selection, size accounting, and optional fixed-extent/scalar
   patch analysis.
3. **Serialized publication.** A bounded flat combiner elects one leader for the
   arrived cohort. The leader assigns one consecutive generation per logical
   mutation and appends fully initialized future-generation overlay records.
   It then advances the router, page validator, and public state to the final
   generation under one snapshot-gate/reader-fence visibility cut.

The publisher queue lock protects only fixed enqueue/drain bookkeeping;
publication itself is serialized. A continuously arriving caller cannot keep
leadership indefinitely: a later arrival receives the next turn.

These cohorts are groups of independent point calls, not user-visible atomic
batches. Capacity or generation-ceiling pressure may publish a valid prefix and
send the untouched suffix through the established coordinated fallback. Each
successful point call still has exactly one generation and linearizable
visibility.

## Fixed resource bounds

- The collection owns at most 32 scratch contexts. The actual count is the
  configured/default file-visibility slot count capped at 32.
- Every context is allocated at open with its JSON tape, token spans, canonical
  output, canonical workspace, publish request, and completion signal. It is not
  a `sync.Pool`, cannot disappear at a GC cycle, and never grows on the hot path.
- Source admitted to this lane is capped at
  `CommonPrimaryLeafMaxExtentBytes / 2` (currently 32 KiB), canonical output at
  one leaf, and the tape at 8,192 entries. Larger or exceptionally token-dense
  input falls back without growing retained scratch.
- Context acquisition uses bounded CAS attempts. True exhaustion sleeps on a
  condition variable and wakes as contexts return; it does not spin or allocate
  an unbounded per-writer workspace.
- The 4,096 mutex stripes are cache-line padded. Writers for the same routed
  leaf serialize; different leaves serialize only if their stripe hashes
  collide.
- The publisher queue and drain arrays are fixed at 32 entries, matching the
  maximum number of outstanding context-owned requests.

## Exact fast-lane scope

The context pool exists only when all of these open-time conditions hold:

- durability is exactly `DurabilityBufferedVisible`;
- the per-mutation `Options.RecoveryJournal` option is false (the ordinary
  buffered checkpoint-delta journal still exists);
- the collection has no schema and no indexes; and
- the bounded unified-primary overlay is enabled.

The lane rechecks those conditions under `writer.RLock` and also declines while
a primary epoch, online index build, or journal replay is active.

Within that mode, a class-5 unified leaf can perform:

- an inline existing-key replacement;
- resurrection of a tombstoned stable slot;
- insertion when a stable slot is free and the exact trivial-content bound
  still fits the current leaf; and
- deletion of an existing inline row while retaining its stable slot, provided
  at least one other live row remains in the leaf.

A delete of a missing or already tombstoned key is completed as a no-op without
publication. Snapshot leases and direct read epochs do not veto immutable
overlay appends because readers filter by generation. The optional same-size
arena-reuse optimization raises its own reader fence and uses a fresh append
when an older reader is active.

## Exclusive fallbacks

The mature coordinated writer path remains authoritative for:

- every durability mode other than buffered-visible and every explicit
  per-mutation recovery-journal write;
- schema validation, exact-index maintenance, online index construction, and
  journal replay;
- non-unified leaves, overflow values or existing overflow rows, inputs outside
  the fixed scratch/inline limits, and overlay-disabled collections;
- inserts that need a split, have no stable slot, or exceed the leaf's exact
  trivial-content bound;
- deleting the final live row in a leaf, because empty-route marking and eager
  leaf reclamation are structural; and
- overlay record/arena pressure, structural topology changes, checkpoints,
  snapshots, flushes, and lifecycle operations.

One pressure coordinator performs the exclusive fold/retry so a full overlay
does not turn one arrived cohort into a stampede of exclusive writers.
Checkpoint and structural operations retain `writer.Lock`; they wait for the
shared staging lanes and therefore include every preceding published cut.
There is no background compaction or offline-maintenance requirement in this
design.

## Qualification evidence

The concurrent-primary tests exercise the boundaries above, including:

- two disjoint stripes completing canonicalization, routing, lookup, and stripe
  acquisition before either is released to publication;
- same-leaf competing replacements/inserts and delete/restore cycles without
  lost values or document-count deltas;
- consecutive generation assignment, stable-slot resurrection, complete
  canonical values for concurrent readers, and exact visibility cuts;
- active snapshots and direct epochs, reader-fenced arena reuse, and crash
  recovery after concurrent insert/delete/restore churn;
- exclusive snapshot/flush overlap and canonicalization that does not convoy a
  waiting exclusive writer; and
- unsupported-shape fallback, bounded pressure retry, fixed scratch capacity,
  exhaustion wakeups, and unique context ownership under CAS stress.

## Current compact-default result at `8c11423`

On an Apple M4 Max, the clean buffered-visible CP64 qualification ran engines
serially and used a 10,000-document corpus, 2,000 warmup operations, 20,000
measured operations, and the median of 10 isolated repetitions. No
pressure-forced checkpoints occurred.
The clean-root evidence, binary hashes, and eleven-TSV manifest are in the
[competitive results](../../bench/competitive/RESULTS.md).

At `clients=N`, each worker owns one disjoint contiguous corpus shard and uses
an independently seeded Zipf stream within that shard. Every engine receives
the same deterministic traces at a fixed `N`, so the cross-engine ratios below
are fair. Changing `N` also changes the aggregate hot set and key/leaf locality,
so the one-to-`N` columns are not a pure same-trace scaling curve.

| Workload | Clients | VibeDB ops/s | Badger ops/s | VibeDB / Badger | VibeDB vs 1 client |
|---|---:|---:|---:|---:|---:|
| write | 1 | 237,524 | 165,319.5 | 1.44x | 1.00x |
| write | 8 | 257,265.5 | 239,677 | 1.07x | 1.08x |
| write | 32 | 269,722 | 252,644 | 1.07x | 1.14x |
| churn | 1 | 542,512.5 | 370,904.5 | 1.46x | 1.00x |
| churn | 8 | 201,188 | 535,117.5 | 0.38x | 0.37x |
| churn | 32 | 127,940 | 522,805.5 | 0.24x | 0.24x |

Observed replacement throughput at eight clients is 8.3% above one client,
and 32 clients are 13.6% above one client. VibeDB remains 1.07x ahead of Badger
at both larger counts. Mixed churn is the counterexample: eight and 32 clients
retain only 37.1% and 23.6% of the one-client VibeDB rate, while Badger rises.
The old `7fe6769` phase-one table predated compact primary storage and is
retained in Git history rather than presented as current evidence. Current
internal evidence points to contended bucket stripes and pressure folds whose
cut contains a currently deleted row, forcing that leaf off the native
replacement-patch path. The fixed 32-context pool remains a deliberate memory
bound, but this c32 result does not identify context capacity as its throughput
bottleneck.

## Next phases

1. **Journaled and stronger-durability writes:** keep preparation leaf-local,
   form a bounded journal group and durability ticket, and publish only after
   the required fence. Crash injection must prove each acknowledged point and
   any future atomic batch at every journal boundary.
2. **Indexed writes:** prepare exact-index posting changes in writer-private
   scratch, reserve all bounded primary/index resources before publication, and
   publish primary plus index generations in one visibility cut.
3. **Structural paths:** parallelize safe route/preparation work for overflow,
   split, slot-exhaustion, and final-row deletion while retaining one exclusive
   topology change and page-retirement fence.
4. **Publication scaling:** isolate the concurrent churn collapse without
   regressing the replacement lane, shorten or shard work before the visibility
   cut where correctness permits, and only raise the context cap when
   measurements show that retained scratch buys throughput.

None of these extensions may weaken bounded memory, stable-slot identity,
snapshot generations, crash semantics, or the exclusive checkpoint fence.
