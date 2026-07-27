# Canonical materialization

**Status:** narrow, capability-gated implementation. Same-length,
projection-safe asynchronous updates can qualify; the broader mutation set
remains gated.

**Idea:** overwrite a canonical page only after proving exclusive ownership,
and protect recovery with a durable before-image capsule. Every failed
eligibility check falls back to copy-on-write.

The implementation does not graduate by local latency alone. The crash,
snapshot, read-path, scan, and index requirements in
[Required gates](#required-gates) are the decision.

## Decision

Small updates and deletes may overwrite their canonical pages only when the
writer can prove exclusive ownership. Every other mutation uses the existing
copy-on-write path unchanged.

Current implementation status is deliberately narrower than that destination:
an explicitly asynchronous file with a caller-qualified damage granule may
materialize a same-length, projection-safe inline update. A zone change is
included by materializing the document page and its chunk-directory leaf in
one capsule. Deletes, inserts, grouped pages, overflow transitions, and
synchronous acknowledgement still use copy-on-write. The current sparse-write
implementation also requires buffered writes: direct-I/O alignment varies by
device and is rejected until it can be admitted rather than guessed.

The fast path is not a read overlay:

```text
writer batch -> complete page after-images -> durable undo capsule
             -> canonical sector writes -> alternate inline root

reader       -> canonical route -> canonical page
```

Readers never inspect the capsule, a tombstone, a memtable, a delta chain, or a
version list. Publication is allowed only after all query-visible structures
are fully materialized.

## Eligibility

The writer constructs complete after-images first, then holds the snapshot
publication gate while checking all of the following:

1. The current state pointer is the state used to construct the mutation.
2. The committer is healthy and orders this isolated materialization after
   every earlier accepted generation.
3. The undo capsule has room for every dirty sector.
4. Every target still resolves to the exact `PageRef` selected by the current
   root.
5. Every target is a uniquely owned canonical extent, not a grouped or shared
   representation.
6. Every active user snapshot is strictly older than the target page's
   generation.
7. Every target cache frame is ready, clean, and unpinned.
8. The encoded extent and its `PageRef` identity remain unchanged.
9. No split, merge, relocation, separator, fence, selector, or overflow
   transition is required.
10. Exact indexes, certificates, ordered stripes, and group accelerators remain
    complete for the after-image; a changed zone summary is included as a
    second canonical target.

Failure of any check is a normal fallback, not an error. The mutation continues
through copy-on-write. Materialized batches are hard queue boundaries and are
never group-committed with adjacent generations. The single writer constructs
them in generation order, and the persistence worker finishes every earlier
batch before writing their capsule or target sectors.

A canonical frame becomes eligible again when the background durability
callback clears its dirty generation. An immediate second update that arrives
before that fence deliberately falls back to copy-on-write; it never waits or
adds a reader overlay. Once the callback completes, another same-page update
can materialize without an explicit `Flush`.

## Stable page identity

A canonical materialization does not change the page header generation,
logical ID, extent, or kind. Changing any of those fields would invalidate
every parent `PageRef` and turn the optimization into a tree rewrite.

The page cache swaps the full validated after-image only when the matching
frame is clean and unpinned. It then marks that frame dirty through the target
publication generation. This is a writer-only operation; the ordinary cache
hit and scan paths are unchanged.

The snapshot gate covers the final ownership recheck, cache swaps, queue
acceptance, and state publication. Page construction and JSON/index work stay
outside the gate.

## Undo protocol

Two fixed, allocator-excluded 4 KiB capsule slots alternate. A capsule contains:

- store identity;
- monotonic capsule sequence;
- target root generation;
- sector size;
- exact target `PageRef` records;
- complete aligned before-sectors;
- canonical offsets and checksums;
- a checksum over the complete fixed slot.

The portable commit protocol is:

1. Encode the new capsule into the older slot.
2. Write and durably synchronize the complete capsule.
3. Write every changed canonical sector and any ordinary COW pages.
4. Durably synchronize the data.
5. Write and synchronize the alternate inline root.

The root generation is the commit marker. Recovery selects the newest
structurally valid root before deciding whether to touch target pages:

```text
root generation < capsule target generation:
    restore every recorded before-sector
    synchronize

root generation >= capsule target generation:
    do not read or replay capsule targets
```

The first case preserves the older alternate root after a torn data or root
write. The second case preserves the committed canonical after-images. A
prospective capsule that tears before its first durability barrier is ignored;
the preceding valid slot remains available.

The capsule is retained. Clearing it immediately would destroy the rollback
state needed by the alternate recovery root.

## Mutation touch sets

### Projection-neutral update

When the key, route, indexed tuples, zones, and representation class are
unchanged, only the document block and publication metadata are targets.
Unchanged exact-index tuples are eliminated before physical planning.

### Indexed update

The document block and only the posting/accelerator pages whose semantic value
changed are targets. If a posting edit changes a fence, splits a page, or
invalidates a certificate, the mutation uses COW.

### Delete

A non-structural delete:

- clears the document live slot and ordered permutation entry;
- immediately exposes the slot/record span for reuse;
- clears the exact compact route word;
- clears affected posting bits and updates stable accelerator metadata;
- publishes the new count and free hint.

It leaves no persistent tombstone and creates no later compaction obligation.
A boundary delete that changes a route/index fence uses COW.

### Insert

Initial integration leaves inserts entirely on the current COW and batch paths.
Canonical inserts are considered only after update/delete correctness and
competitive gates pass.

## Fragmentation policy

The planned hot document format uses a fixed slot directory with explicit
`start/keyEnd/rowEnd` offsets. An update chooses a fitting free interval and
redirects one slot in the fully materialized after-image. A delete returns its
interval immediately.

The writer reconstructs and coalesces free intervals in bounded scratch.
Page-local compaction runs only when no interval fits despite sufficient total
free bytes. Cold blocks are packed contiguously; mutating one materializes it
as one hot block before publication. Readers never merge a hot overlay with a
cold base.

The implemented same-length update already compares complete after-images and
records only changed qualified damage sectors. Page-local gap reuse, delete
reclamation, and hot-page compaction remain gated on integrating the sparse
document format. The checksum sector is included whenever its bytes change.

## Required gates

The path remains an explicit capability-gated option and cannot become the
default until all of these pass:

- exhaustive crash cuts before/after each capsule, data, and root write;
- active-snapshot old/new visibility and forced COW fallback;
- cache pin, stale-state, journal-capacity, and structural fallback tests;
- point-hit/miss allocations and page acquisitions unchanged;
- random point-read latency unchanged within noise;
- ordered scan iteration and full-byte throughput unchanged or faster;
- exact-index collision and certificate answers unchanged;
- one physical materialization per target page in a combined batch;
- matched-durability update/delete device-byte and latency measurements;
- insert, bulk-load, and snapshot benchmarks unchanged within noise.

On ordinary storage, one isolated synchronous mutation still pays ordered
durability barriers. The design wins steady-state update/delete work by
coalescing same-page mutations, writing bounded dirty sectors, retaining one
canonical representation, and eliminating compaction. Device atomic-write
support may later remove the undo phase for explicitly qualified sectors; it is
an optimization, not a correctness dependency.

## Research lineage

The design borrows writer-side batching and page ownership ideas without
adopting query-time overlays:

- [Bf-Tree](https://www.vldb.org/pvldb/vol17/p3442-hao.pdf) buffers updates in
  mini-pages, but point misses and scans consult/merge that state.
- [Bw-tree](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/bw-tree-icde2013-final.pdf)
  prepends page deltas, which readers traverse.
- [FASTER](https://www.microsoft.com/en-us/research/uploads/prod/2018/03/faster-sigmod18.pdf)
  combines in-place hot records with a hybrid log.
- [PALM](https://www.vldb.org/pvldb/vol4/p795-sewall.pdf) demonstrates
  dependency-aware, page-owned batch execution.
- [Closing the B-tree versus LSM-tree write-amplification gap](https://www.usenix.org/system/files/fast22-qiao.pdf)
  demonstrates that shadowing, localized logging, and compression can move a
  B-tree close to LSM write amplification without adopting an LSM read path.
