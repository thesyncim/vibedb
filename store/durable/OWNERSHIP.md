# Durable collection ownership and persistence

This file is the review map for `Collection`. It describes the runtime rules
that must remain true when code is moved or a publication lane is changed. The
format and crash model remain specified in `docs/format.md` and
`docs/durability.md`; this document stays beside the implementation because the
fields and locks named below are implementation invariants.

## Source ownership

The large collection unit is split along lifecycle boundaries while retaining
one `durable` package:

- `store_file_options.go` owns option normalization and format-bound geometry.
- `store_file.go` owns the shared collection state and accounting types.
- `store_file_open.go` owns create, open, bounded recovery, and initial-root
  construction.
- `store_file_resources.go` owns cache, committer, reader-registry, reclaimer,
  and fixed-arena construction.
- `store_file_operations.go` owns the public point, snapshot, and observation
  methods.
- `store_file_lifecycle.go` owns pressure checkpoints, retirement admission,
  flush, close, and resource teardown.
- `store_file_durability.go` owns applied, visible, and durable state
  transitions. `store_file_inplace.go` owns the common foreground physical
  checkpoint boundary.
- `store_file_journal.go` owns journal pairing, replay, logical checkpointing,
  and recycling. Primary publication remains beside the primary mutation or
  batch algorithm that prepares it, so its invariants do not cross packages.

## Buffer ownership

| Resource | Owner and lifetime | Mutation rule |
| --- | --- | --- |
| Caller `*os.File` | The caller owns its lifetime. `Collection` borrows it through `Close` and never closes it. Any distinct direct-read or direct-write descriptor returned during open is collection-owned and closed by `closeResourcesLocked`. | The caller must not close or independently mutate the file while the collection is live. |
| Caller keys, JSON, and read destinations | Mutation input is borrowed only for the call and copied before it can outlive the call. Append/read destinations remain caller-owned. | A caller may reuse input after the method returns. Separate goroutines need separate mutable destinations. |
| Page-cache frames and leases | `PageCache` owns admitted frames. A page lease or generation/epoch pin owns the right to dereference one until release. | A sealed COW frame is immutable. The canonical-materialization exception is protected by its before-image journal and is not exposed as an unfenced partial image. |
| Commit buffers and `writeTransaction` | `Committer` owns its fixed buffer arena. The collection's reusable `writeTransaction` is exclusive-writer scratch and returns staged buffers on abort or transfers them to the committer on publish. | Never retain a transaction page slice after abort/publish/reset. |
| Free-set, retirement, materialization, primary, overflow, exact-index, and fold scratch | `Collection` allocates these bounded arenas at open. They are writer-owned unless a field comment explicitly assigns them to a fixed worker/context. `closeResourcesLocked` detaches every slice before unmapping its backing block. | Reset and reuse only under the documented writer or worker ownership; never publish a pointer into scratch. |
| Concurrent primary contexts | The fixed context pool lends exactly one context to a caller. Private validation/canonicalization happens before `writer.RLock`; the context is returned only after publication or fallback completes. | No context or its slices may be shared between callers or retained in a published record. Published overlay bytes are copied into the overlay arena. |
| Snapshots and current scans | A `Snapshot` owns a generation lease until `Snapshot.Close`. Point reads/current scans use a short `ReadEpoch`, falling back to a lease when a writer fence diverts entry. | A reader may dereference pages/router state only while its lease or epoch is active. |
| Recovery journal | The live collection owns the opened sibling journal handle. The root's store identity, journal identity, geometry, and base generation bind it to the data file. | Only journal helpers append, sync, replay, recycle, or close it; replay runs before `Open` returns. |

## Published state and immutability

- Each `fileStoreState` is a value snapshot for one generation. After an atomic
  state pointer publishes it, neither its root nor its file/free-log bounds are
  mutated.
- Data, index, directory, and root pages are sealed before publication and are
  then immutable COW images. Reclamation waits until the durable fallback floor
  and every snapshot/epoch reader have passed the retiring generation.
- Unified-overlay records are fully initialized before their generation becomes
  visible. Older readers filter future records by generation. Arena reuse is
  allowed only under a reader fence after proving no reader can observe the old
  bytes.
- The resident primary router is not a generally immutable object: structural
  work swaps it wholesale, while the ordinary publisher advances leaf/root
  metadata under the publication and reader-fence protocol. A reader must load
  it through the established capture path and keep its generation pin.
- Exact-index epoch bases are immutable after publication. Overlay links and
  epoch replacement are writer/publication-owned, and retired epochs remain
  pinned by generation until reclamation can recycle them.

## Synchronization

`writer` is the collection lifecycle fence. Its exclusive side owns structural
changes, indexed/batch fallback, allocator/reclaimer mutation, checkpointing,
recovery replay, and close. Its shared side admits only the qualified concurrent
inline-primary lane. That lane adds:

- one of 4,096 full-`BucketID` stripes for leaf-local inspection/accounting;
- a fixed-capacity publisher that assigns generations and publishes one final
  visibility cut; and
- `primaryConcurrentPressure` to elect one exclusive fold/fallback instead of
  allowing a pressure cohort to stampede the exclusive writer.

`snapshotGate` makes snapshot capture mutually exclusive with visibility
rollback, epoch replacement, destructive overlay reuse, and publication cuts
that require a reader fence. `visibilityMu` protects only `pendingVisible` and
the coordinated applied/visible/durable pointer transition. Where both are
needed, `snapshotGate` is acquired before `visibilityMu`; code already holding
`writer` acquires `snapshotGate` before publishing. The persistence callback
does not need `writer`, which prevents device completion from waiting behind an
unrelated writer.

Long snapshots use `GenerationLeases`; short direct readers use `ReadEpochs`.
`beginReaderFence` diverts new epoch entrants and waits for admitted epochs to
leave before bytes can be reused. A reclamation decision must account for both
registries. Atomic state/router pointers provide race-free selection, not
lifetime ownership; the lease or epoch supplies the latter.

## Poisoning

A committer write/barrier failure or recovery-journal append/sync/recycle
failure poisons the live writer. The first journal error is sticky and is joined
with any committer failure by `PersistenceError`. When the alternate root may
have landed, the error may match `ErrCommitOutcomeUnknown`.

After poison:

- no mutation, checkpoint, durability drain, or retry is allowed on the live
  handle;
- `Close` still releases owned resources and returns the sticky failure;
- buffered-visible and journal-backed COW generations may continue serving the
  last admitted immutable view; journal-less chain-fence or canonical-image
  ambiguity fails reads closed; and
- recovery is `Close` followed by `Open`, which selects the valid root and
  replays the paired journal before another logical mutation is attempted.

Validation, capacity, conflict, and unsupported-operation errors before the
point of no return do not poison. Optional hole-punch failure disables that
optimization for the live handle but neither poisons the database nor changes
allocator correctness.

## Persistence boundaries

| Boundary | What establishes it |
| --- | --- |
| Synchronous journal-backed mutation | The complete redo record is appended and synced before any in-memory publication. Success is both durable and visible. |
| Synchronous journal-less chain fence | Immutable pages and the alternate root pass the committer's final barrier; visibility is promoted only by the durable callback. |
| Async-visible mutation | Queue admission/publication acknowledges visibility. A later committer root barrier, `Flush`, or `Close` establishes crash persistence. |
| Ordinary buffered-visible mutation | Bounded memory admission/publication acknowledges visibility. Without per-mutation journaling it remains volatile until `Flush`/`Close`. |
| Buffered-visible with `RecoveryJournal` | The mutation may become visible before its redo fence, but the call returns only after its record is covered by the shared journal sync. |
| Journal-delta `Flush` | The complete eligible overlay suffix is appended and synced, then the logical durable watermark advances without requiring a physical-root rewrite. |
| Physical checkpoint | Referenced immutable pages are written, the data-ordering barrier completes, the alternate root is written, and the final root barrier completes. Only then are cache frames marked durable and the recovery journal recycled through that generation. |
| Recovery/open | The newest valid alternate root is selected, the journal identity/epoch is verified, covered records are replayed through ordinary mutation semantics, and the recovered cut is made ready before `Open` returns. |

Journal recycling is ordered after the physical root fence; replay is
idempotent if a crash leaves an older journal header beside a newer root. Hole
punching runs only after both root and journal authorities are stable and is not
itself a durability boundary.
