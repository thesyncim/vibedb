# Canonical materialization

**Status:** implemented behind strict eligibility checks. Materialized frames
are one possible base of the generation cut; deferred primary lanes may also
select a bounded immutable row overlay until foreground checkpoint folding.

## Current paths

Buffered-visible and journal-backed synchronous mutations may patch an
exclusively owned canonical leaf frame when the replacement has the same size
and does not require a structural change. The frame is volatile until the next
checkpoint, so this path needs no materialization capsule. A failed eligibility
check uses copy-on-write.

An asynchronous store configured with a non-zero
`MaterializationDamageGranule` may persist a same-length, projection-safe
inline update through the before-image capsule. A zone change may include the
document page and its route-summary page in the same capsule. Inserts, deletes,
overflow transitions, structural changes, and unqualified devices use
copy-on-write.

The capsule itself is recovery-only and never enters ordinary reads. Separately,
the unified deferred primary lane can publish bounded generation-stamped put or
delete records over these base pages. Point reads, scans, filters, and indexes
resolve the published base-plus-overlay cut exactly; no background merge or
unbounded version structure is involved.

## Eligibility

The writer constructs complete after-images before publication, then verifies:

1. the selected state is still current;
2. every target resolves to the exact `PageRef` selected by that state;
3. no active reader can observe a target that would be overwritten;
4. each target cache frame is ready, clean, exclusively owned, and unpinned;
5. the extent, page identity, and encoded length remain unchanged;
6. no split, merge, relocation, fence, slot reassignment, or overflow
   transition is required;
7. exact-index postings and route summaries remain complete for the after-image;
8. the bounded queue or capsule has capacity for the complete operation.

Failure of any check is a normal copy-on-write decision. A mutation never waits
for ownership and never weakens its configured acknowledgement contract.

## Before-image protocol

The persistent path uses two fixed, allocator-excluded 4 KiB capsule slots. A
capsule binds the store identity, sequence, target generation, qualified sector
size, exact target references, complete aligned before-sectors, and checksum.

Publication is ordered:

1. write and synchronize the alternate capsule slot;
2. write all changed canonical sectors and ordinary copy-on-write pages;
3. synchronize data;
4. write and synchronize the alternate inline root.

The inline-root generation is the commit marker. On open, a selected root older
than the capsule target causes every recorded before-sector to be restored and
synchronized. A root at or beyond the target ignores the capsule targets. A
torn prospective capsule is ignored in favor of the preceding valid slot.

The capsule remains intact because the alternate inline root may still require
its before-images.

## Invariants and qualification

- snapshots observe the complete old or new generation, never a mixture;
- page-cache replacement occurs under the snapshot gate and reader fence;
- every ordinary read remains independent of the capsule;
- crash tests cover each capsule, data, synchronization, and root boundary;
- stale-state, pinned-reader, dirty-frame, capacity, and structural cases prove
  copy-on-write selection;
- exact-index and route-summary answers match a full rebuild;
- warmed read allocation and page-acquisition gates remain unchanged;
- matched-durability benchmarks report latency and device bytes separately for
  copy-on-write and materialized updates.

Materialization is an optimization. Correctness never depends on a mutation
qualifying for it.
