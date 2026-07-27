# Read-neutral hybrid mutations

**Status:** batched directory/key work and automatic combining are implemented;
canonical-frame coalescing across a buffered checkpoint epoch remains future
work.

**Idea:** batch aggressively on the writer side, then publish one complete
canonical generation. No queue or intermediate representation is reachable
from a reader.

The acceptance matrix and measured random-mutation evidence are
[front-line gates](#workload-and-acceptance-matrix), not optional follow-up.

**Measured evidence:** in the
[cited 4,096-row asynchronous matrix](#current-random-mutation-evidence),
batch-64 replace/delete paths measure 20.8–35.9 µs/document versus
379–465 µs/document for batch 1, depending on operation and index state. Those
results prove batching leverage; they do not meet the ordered primary's
sub-microsecond acknowledgement gates.

## Decision

vibedb should not put an LSM tree, persistent mutation log, tombstone table,
or delta chain in front of the durable collection.

The durable mutation path should instead become a **materialized mutation
pipeline**:

```text
bounded request admission
        |
        v
automatic mutation combining
        |
        v
one batched COW materialization
        |
        v
one immutable state-root publication
```

The first two stages are transient writer machinery. They are never reachable
from a `StateRoot`, never survive as a read overlay, and never become another
level a snapshot has to search. A mutation is visible only after its complete
document, key, index, zone, and root state has been materialized.

This is a hybrid of log-like **admission** and page-native **materialization**,
not a hybrid of two durable read representations.

The ownership-checked in-place extension, undo protocol, fragmentation policy,
and rollout gates are specified in
[canonical-materialization.md](canonical-materialization.md).

## Buffered-visible checkpoint mode

Status: the current primary implements the crash-correct first slice with
fresh COW generations retained in bounded staging until `Flush` or `Close`.
Canonical-frame coalescing and overlapping sealed/mutable epochs remain future
work.

`DurabilityAsyncVisible` hides the stable-storage fence, but it still constructs
and queues one complete COW generation per logical mutation. It is therefore
not the equivalent of a memtable-style `NoSync` acknowledgement and cannot
close the sub-microsecond small-update gap by tuning the committer.

VNext adds an explicit `DurabilityBufferedVisible` contract built from
canonical in-memory frames, not a reader-visible delta:

```text
visibleRoot -> newest linearizable in-process state
sealedRoot  -> immutable cut currently being checkpointed
durableRoot -> newest crash-recoverable on-disk state
```

A successful buffered mutation means immediately visible and ordered inside
the current process. It does **not** mean process- or power-loss durability.
`Checkpoint`/`Flush` seals the current visible cut and returns only after that
cut becomes `durableRoot`; graceful `Close` checkpoints by default. Recovery
selects one complete durable checkpoint and may discard every acknowledged
mutation after it.

Checkpoint strength is a separate, explicit contract:

- `CheckpointPowerSafe` is the zero value and production default. Existing
  `DurabilitySync`, `DurabilityAsyncVisible`, and buffered-visible callers keep
  their strongest platform barrier unchanged. On Darwin the final boundary is
  `F_FULLFSYNC`, which asks the device to drain volatile caches.
- `CheckpointFilesystem` is accepted only for
  `DurabilityBufferedVisible` with `BackendPortable` and `WriteBuffered`. It
  performs ordinary filesystem sync twice: all COW pages, `fsync`, the
  alternate root, then `fsync`. This preserves process-crash recovery and the
  old-root/new-root atomic cut, but on Darwin it does not promise survival of a
  sudden power loss while bytes remain in a volatile drive cache.

The ordinary option exists for like-for-like ordinary `fsync`/`msync`
checkpoint comparisons. It is never selected by default and cannot silently
weaken a synchronous or automatically persisted collection.

Normal serialized `Put`, `Delete`, and `Update` admission also preflight both
dirty-cache bytes and committer staging. If the next worst-case transaction
would exhaust either bound, the collection checkpoints the accepted epoch
before beginning that mutation. `ErrCheckpointRequired` remains the
authoritative backstop for raw committer races or an admission estimate that
cannot be satisfied; steady Collection use does not require a caller-managed
flush/retry loop.

The current read path remains singular:

```text
root -> anchor -> ordered spine -> inline value or value tile
```

A dirty owned frame is the canonical page for its stable reference and remains
pinned until checkpoint. Readers never consult a queue, WAL, memtable, delta,
tombstone, or version chain. Repeated updates to one frame coalesce into its
one canonical after-image.

Two ownership facts are independent:

- `readerExclusive`: no snapshot or sealed checkpoint can observe the frame,
  so the writer may change the canonical in-memory bytes behind the stable
  reference;
- `durableReachable`: the prior durable root can still reach the physical
  extent, so checkpointing must shadow-copy it rather than overwrite it.

Snapshot creation retains `visibleRoot` in O(1). The first later write to a
frame reachable from that snapshot COWs the minimum route suffix; new readers
select the new frame directly, while the snapshot retains the old one. Sealing
a checkpoint acts like a snapshot so one bounded active mutation epoch can
continue while the sealed epoch is written.

Checkpointing walks only dirty/new frames reachable from `sealedRoot`, writes
each affected parent once bottom-up, syncs the complete page set, then syncs
one alternate root. An extent reachable from `durableRoot` cannot be reused
until a later checkpoint commits and all relevant snapshot leases close.

Dirty bytes, sealed bytes, staged references, retired extents, and active
epochs are hard-bounded. The writer blocks before admission or returns explicit
backpressure; it never acknowledges a mutation whose canonical frame cannot
remain pinned. Promotion requires:

- update acknowledgement p50/p99 at or below 0.45/1.5 microseconds on the
  prevalidated same-size path;
- no extra point-read page, branch, or lookup;
- repeated same-frame updates becoming one checkpoint page write;
- every crash cut recovering either the prior or sealed checkpoint;
- bounded memory and reported backpressure under uniform random writes and
  long snapshots.

## Non-negotiable invariants

The design is governed by these invariants:

1. Point reads keep their current path:

   ```text
   state -> key B+tree -> chunk radix -> document extent
   ```

2. A snapshot never probes a mutation queue, WAL, tombstone table, mutable
   memtable, delta page, or version chain.
3. A published exact index is complete for the published document state.
4. Deletes leave no persistent point tombstone and no later logical cleanup
   obligation.
5. A successful mutation retains the current visibility and durability
   contract. A fast acknowledgement may not mean "durable but not yet readable".
6. Every queue, batch, scratch arena, dirty-page reservation, snapshot lease,
   and retired-extent set remains bounded.
7. In synchronous and current async-visible modes, an isolated mutation remains
   an ordinary COW mutation. Buffered-visible mode may defer disk
   materialization only because its canonical frame is already the sole
   reader-visible representation and its weaker crash contract is explicit.

## Why a durable delta cannot satisfy the requirement

Suppose mutation `M` has been acknowledged but has not been folded into the
canonical COW graph.

There are only two possible point-read implementations:

- the reader consults the delta and then the base; or
- the reader consults only the base.

The first adds read work. The second may return the state before acknowledged
mutation `M`. Bloom filters, a small number of runs, a mapping table, or a
strict size bound reduce the cost of the first choice but do not remove it.

This is why Bw-tree-style page deltas, FASTER-style hybrid-log records, a
WiredTiger-style update chain, and a one-level mini-LSM are not the core design
here. They are useful designs with different read/visibility trade-offs. The
vibedb requirement selects materialization before publication.

## What exists now

`durable.Collection.Update` already supplies most of the materializer:

- duplicate keys collapse to their final mutation;
- every touched document chunk is rebuilt once;
- key and exact-index edits use batched tree descents;
- one state root and one generation are published;
- the operation is failure-atomic.

The first read-neutral improvement closes the remaining directory gap.
Previously the chunk radix was updated once per touched chunk, so later edits
inside a transaction rewrote directory pages staged by earlier edits. The
batched radix descent partitions edits by lane and writes every visited node
once.

For 128 replacements across 128 chunks, the sequential directory path retires
256 directory pages (one leaf and one root per replacement). The batched
descent retires three: the two leaves and their root. The published format and
point-read traversal are unchanged.

Per-chunk zone summaries are finalized while each replacement row set is still
available, then carried with the corresponding radix edit. The leaf publishes
the document reference and summary under one checksum, exactly as the
single-chunk path does.

## Automatic mutation combining

Explicit `Update` is necessary but not sufficient. Libraries, SQL clients, and
network sessions often issue independent `Put` or `Delete` calls. The engine
should combine concurrent requests without requiring every caller to construct
a batch.

### Bounded request queue

Each request owns a fixed-capacity slot containing:

- a monotonically increasing arrival sequence;
- one mutation or one explicit atomic batch;
- copied key and document bytes;
- the result location for `created`, `deleted`, or error;
- a completion state;
- whether the caller requires a durability wait.

The queue is bounded at open. A full queue applies backpressure before accepting
more bytes. Slots and byte arenas are reused rather than allocating a channel,
closure, or result object per mutation.

### Flat combiner

One goroutine becomes the combiner for the queue head. It drains a contiguous
group until the first configured bound:

- maximum requests;
- maximum distinct keys;
- maximum copied bytes;
- `MaxBatchDocuments`;
- optional coalescing target.

The combiner does not wait merely to make a lone operation look batched. It
extends a group when another request is already queued or a producer is known
to be in flight. An explicit latency window remains a caller policy, just like
`CommitCoalesce`.

This is **apply combining**, which happens before a generation is built.
Existing commit grouping happens after generations are built and shares
durability fences. Both are useful:

```text
requests --apply combine--> generations --commit group--> durability fences
```

Commit grouping alone cannot eliminate document or directory pages already
written by separate generations.

### Planning one group

The planner performs these steps:

1. Reject invalid request-local input before it can poison unrelated requests.
2. Resolve the group's distinct keys with one sorted key-tree traversal.
3. Replay requests in arrival order against a small group-local key overlay.
4. Compute each request's `created` or `deleted` result.
5. Collapse concurrent same-key writes to the final physical row state.
6. Partition the final mutations by document chunk and index routing key.
7. Materialize one generation with the existing batched COW machinery.

The key overlay is writer-only and exists for one group. It is not published
and is never read by a snapshot.

### Linearizability

Requests in one automatic group overlap in time: none has completed while the
group is being assembled. They can therefore be linearized in arrival-sequence
order at the one root publication.

For example, concurrent requests:

```text
Put("a", v1)
Put("a", v2)
Delete("b")
```

may publish only `a=v2` and the absence of `b`. The first caller still receives
the `created` result computed at its position in the logical order. No reader
was entitled to observe `a=v1` between two overlapping operations.

A request issued after an earlier request returns cannot enter the earlier
group, so real-time ordering is preserved.

An explicit `Update` remains one indivisible request. The combiner may place
other overlapping requests before or after it, but never inside it.

### Failure behavior

Input and schema failures belong to one request and are detected before group
materialization. An I/O error, corruption error, reservation failure, or failed
publication leaves the complete group unpublished and fails every request
whose result depended on it.

If the combined group exceeds a physical reservation unexpectedly, the
combiner retries smaller groups at request boundaries. It never splits an
explicit atomic `Update`.

## Faster updates

The update path should be improved in this order.

### 1. Batch directory publication

Status: implemented for key, exact-index, and chunk directories.

Each visited COW node is emitted once. This is the highest-confidence win
because it removes writer work without changing a byte a point reader must
interpret.

### 2. Batch key resolution

Status: implemented.

`resolveFileBatch` sorts caller-owned lookup scratch and
`LookupKeyTreeBatch` traverses each visited key page once before returning
results to their original request order. The ordinary `LookupKeyTree` point
path and the durable page format are unchanged.

This improves random updates and deletes even when every key lands in a
different document chunk. The resident 512-key control benchmark resolves a
key in 77-85 ns through the batch traversal versus 2.54-2.63 us through a loop
of point lookups on an Apple M4 Max, with zero allocations in both arms. The
structural test also asserts that a covered directory page is acquired once,
so the result is not dependent on one favorable timing run.

### 3. Automatic apply combining

This exposes the existing batched materializer to ordinary concurrent
operations. It removes repeated:

- chunk rebuilds when requests share a chunk;
- tree root-to-leaf copies;
- state-root pages;
- free-log work;
- generation bookkeeping.

It also lets same-key overwrite and insert-then-delete pairs disappear before
physical work begins.

### 4. Avoid reopening old JSON where evidence permits

An indexed replacement or delete may need the old tuple hashes and exact
certificates. Storing reverse mutation evidence could avoid reopening and
parsing the old JSON, but it is not automatically read-neutral:

- embedding evidence can enlarge document pages and reduce cache density;
- a separate writer-only tree adds write amplification;
- indirection can add I/O to mutation.

This optimization should be attempted only after profiles show old-value parse
cost dominates the batched writer, and only with point-read and cache-density
gates.

## Faster deletes

### Random point deletes

Automatic grouping and batched key resolution are the read-neutral answer.
They make acknowledgement cover one materialized generation for many deletes,
while removing every row, posting, and empty chunk immediately.

There is still an irreducible cost: if one random row is removed from each of
`N` different inline document pages, `N` replacement document pages must exist
before the canonical root can expose all `N` deletions. A tombstone can defer
that cost; a tombstone-free materialized representation cannot.

### Delete after recent insert

Concurrent or explicit batches can collapse an insert followed by a delete
before publication. Sequential operations where the insert has already
returned are separate committed states and cannot be erased retroactively
while snapshots may still reference the first state.

### Range and tenant deletes

A key-range delete can be implemented as a bounded streaming producer feeding
the same materializer. It remains proportional to affected chunks and index
entries, but shares tree descents and publications.

An O(1) logical range delete requires either:

- a range fence checked by readers; or
- a separately rooted partition/tablet that can be unlinked as one object.

The fence violates the zero-read-penalty rule. A tablet design can be valuable,
but its routing and physical data model are a separate decision and should not
be smuggled into the point-mutation path.

## What "zero read penalty" means

No system can promise identical wall-clock timing under every build, cache
state, and scheduler event. This design uses a stronger structural definition
and verifies it statistically:

- `Snapshot.AppendRaw` source and call graph do not gain a mutation check;
- the state-root and document lookup formats do not gain an overlay reference;
- a point read acquires the same logical pages;
- scans do not merge versions;
- exact-index probes do not subtract tombstones;

Every mutation change is gated by:

```text
BenchmarkFileStorePointRead
warm full scan
cold point read / page reads per lookup
exact-index hit and miss
zone-pruned scan
snapshot held across concurrent writes
```

A result outside normal run-to-run noise blocks the change even if write
throughput improves.

## Workload and acceptance matrix

Every phase must report time, physical work, and bounded memory:

| Dimension | Values |
| --- | --- |
| operation | insert, replace, delete, insert/delete churn |
| locality | one chunk, clustered chunks, uniform random keys |
| document | ~64 B, 4 KiB, overflow-sized |
| metadata | no index, exact index, multiple indexes, zones |
| writers | 1, 8, 64 |
| durability | asynchronous, synchronous |
| group | 1, explicit batch, automatic batch |
| store size | empty, 1K, 100K, larger than cache |

Required metrics:

- `ns/op` and `ns/document`;
- device bytes per logical mutation;
- directory pages written and retired;
- documents and generations per publication;
- mutations per durability fence;
- queue depth and backpressure events;
- P50 and P99 acknowledgement latency;
- cache hits, misses, page reads, and read bytes;
- fixed queue, scratch, and staging capacity.

Throughput results without the corresponding acknowledgement latency and
device-byte figures are incomplete.

### Current random-mutation evidence

The first completed matrix uses 4,096 resident rows, a uniform coprime key
stride, asynchronous durability, and three one-second runs per arm on an Apple
M4 Max. Values are the observed run ranges:

| Operation | Metadata | Batch 1 | Batch 64 | Speedup | Device bytes, 1 -> 64 |
| --- | --- | ---: | ---: | ---: | ---: |
| replace | none | 379-391 us/doc | 29.9-31.2 us/doc | 12-13x | 19.4-19.6 KB -> 7.74 KB |
| replace | exact index | 423-444 us/doc | 22.8-23.4 us/doc | 18-19x | 33.1-33.7 KB -> 7.95 KB |
| delete | none | 414-421 us/doc | 32.2-35.9 us/doc | 12-13x | 26.9-29.3 KB -> 5.46-5.49 KB |
| delete | exact index | 454-465 us/doc | 20.8-22.3 us/doc | 20-22x | 31.3-33.7 KB -> 5.52-5.53 KB |

The matrix exposed a free-log fold bound that a wide random indexed batch could
exceed after sustained churn. Fold-page reservation now scales with the
configured atomic batch up to the existing segment-index capacity; the
single-document baseline remains sixteen pages. A 256-generation regression
and 500-generation benchmark controls cover indexed replacement and
indexed deletion without abandonment or backpressure.

The unchanged warm point-read benchmark was also run three times against the
exact `main` base and this branch. Collection reads stayed inside the base's
1.55-1.73 us range, snapshot reads stayed within normal noise at roughly
1.52-1.60 us, and both retained identical page-read, miss, and allocation
counts.

## Delivery order

1. Batched chunk-directory descent and trustworthy device-byte benchmark.
   Implemented.
2. Sorted batched key resolution. Implemented.
3. Random replace/delete benchmarks with indexed variants. Implemented.
4. Bounded automatic mutation queue and flat combiner. Implemented.
5. Same-key logical-result oracle and crash matrix for combined requests.
   Implemented.
6. Read-neutral performance gate. Implemented and required for every further
   mutation change.
7. Only then evaluate writer-side reverse index evidence or adaptive chunk
   placement.

## Research lineage

The design uses ideas selectively:

- [PALM](https://www.vldb.org/pvldb/vol4/p795-sewall.pdf) demonstrates the
  value of partitioning a batch so each B+tree node is modified once.
- [The Bw-tree](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/bw-tree-icde2013-final.pdf)
  demonstrates cheap delta updates, and also makes the read trade-off explicit:
  a search walks the delta chain before the base page.
- [FASTER](https://www.microsoft.com/en-us/research/uploads/prod/2018/03/faster-sigmod18.pdf)
  demonstrates adaptive hot in-place updates plus a colder append path. Its
  hybrid-log trade-off is useful context, but vibedb's immutable snapshots
  and strict canonical-read requirement select a different boundary.

The result is deliberately narrower than all three: batch aggressively on the
writer side, publish one complete immutable representation, and keep readers
unaware that batching exists.
