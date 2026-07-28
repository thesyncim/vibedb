# Recovery-only redo journal

**Status:** implemented for ordered-primary `DurabilitySync` and opt-in
ordered-primary buffered-visible acknowledgement. Comparative performance
effects remain projected until their qualification benchmarks run.

**Idea:** sync a bounded redo record in a separate file for acknowledgement,
while readers continue to use canonical frames only. Checkpoint later folds
the records into the ordinary root publication protocol.

Further performance claims are controlled by
[Projected effect and gates](#projected-effect-and-gates).

## The insight

The read-neutrality invariant forbids any representation a READER must
consult — memtables, deltas, version chains. It does not forbid a log that
only crash RECOVERY replays. The materialize-before-publish rule exists to
keep point reads singular; acknowledgement durability is a separate
concern, and conflating them is why a power-safe mutation currently pays
page-granular COW plus two ordered syncs while a WAL system appends a few
hundred bytes and syncs once.

## Design

A bounded redo journal in a SEPARATE file beside the store file, not a
region inside it: fdatasync flushes every dirty page of the file it is
called on, so an in-store journal region would drag concurrently
pre-written checkpoint pages into every acknowledgement sync and make
its latency unpredictable. A dedicated file keeps the sync domain to the
journal's own preallocated pages — the same reason every production WAL
is a separate file. The store file records the journal's identity
(name/UUID) in its root so recovery cannot pair mismatched files, and a
missing-but-referenced journal fails closed while a cleanly truncated
one is the ordinary empty state:

- Both synchronous and buffered-visible mutations are applied to the same
  canonical in-memory frames (in-place patch or frame COW); one redo record
  — key, value bytes, generation, checksum — is the unit of durability for
  each. The two lanes differ only in ORDER, because they make different
  visibility promises:
  - DurabilitySync appends the record and issues the mode's sync primitive
    on the journal alone BEFORE the mutation is applied and published, so
    visibility strictly follows durability. A concurrent reader in the
    append window sees the pre-mutation state — nothing is published yet —
    and a sync failure leaves nothing visible and sticky-poisons the
    collection per the journal poison rules. The record may only be synced
    once the apply is committed to succeed, so the primary commit is split
    into a fallible prepare (routing, eligibility, split/capacity checks —
    no visibility) and an infallible publish; the journal sync sits at that
    point of no return.
  - Buffered-visible applies and publishes first, then appends+syncs the
    record. Its acknowledgement contract already permits a volatile window,
    so the cheaper apply-then-journal order is correct there.
  Acknowledgement returns after that single bounded append+sync.
- Readers never consult the journal. Visibility comes from the canonical
  frames, exactly as now; the journal is write-only until recovery.
- Checkpoints are unchanged: materialize dirty frames, publish the
  alternate root, then truncate the journal head past the checkpointed
  generation in the same publication.
- Recovery selects the newest valid root, then replays journal records
  after that root's generation through the ordinary mutation path,
  stopping at the first invalid record. Replay is bounded by journal
  capacity; journal pressure forces a checkpoint exactly like staging
  pressure does today.
- Torn or reordered journal tails are detected per record (checksum +
  monotonic generation); a torn tail truncates, never corrupts — the root
  and its graph were never touched before their own two-phase publication.
- On a primary-layout store DurabilitySync's per-mutation acknowledgement
  IS the journal append plus its platform barrier (F_FULLFSYNC on Darwin);
  the deferred root is published on the same checkpoint cadence as
  buffered-visible, with the two-phase root protocol's own barriers at
  checkpoint time. The old chain-fence acknowledgement — publish a full
  copy-on-write generation and wait on the committer's root fence per
  mutation, two ordered device fences — is retired for this lane: nothing on
  the primary sync path waits on committer.Wait. A chunk-layout
  DurabilitySync store has no journal mutation lane and keeps the chain
  fence until the chunk store is deleted. The journal is therefore
  UNCONDITIONAL for primary DurabilitySync — it is how sync acknowledges,
  not an option — while the RecoveryJournal option remains the opt-in only
  for buffered-visible.
- Because visibility follows durability on the sync lane, a reader can never
  observe an acknowledged-but-not-yet-durable mutation, so that lane needs
  none of the fail-closed read logic the old publish-before-durable sync
  path required. A journal-backed collection retains its last admitted
  immutable view after a failure exactly as buffered-visible does — every
  visible generation is journal-durable. The fail-closed-if-visible-leads-
  durable read path survives only for the chunk sync lane and async
  canonical materialization, and is deleted with the chunk store.
- Linux cost parity with a production WAL requires three specifics, not
  optimizations: journal extents are preallocated and recycled so a
  record sync never commits filesystem metadata; record syncs use
  fdatasync, not fsync; and file growth uses fallocate. A journal that
  extends a file under each sync pays ext4/xfs metadata journaling and
  loses the entire advantage.
- Journal sync failures are terminal: after an fsync-class error Linux
  may drop the very dirty pages a retry would need, so the committer's
  existing sticky-failure poisoning covers the journal path with
  die-don't-retry semantics, never a retry loop.

## Batch records — the group-commit primitive

A multi-document `Update` on the primary graph makes its whole group
durable with ONE append and ONE sync, not one per document. The record
format carries this directly rather than framing it as first/continuation/
commit records across separate appends:

- A batch record reuses the 32-byte record prefix with `kind = Batch` and
  repurposes the two length words as `entryCount` and `bodyLen`. The body
  is a run of `entryCount` entries, each a 12-byte header (entry kind,
  reserved, keyLen, valueLen) followed by the key and value bytes. One CRC
  (and its complement) covers the prefix and the entire body; the record is
  padded to the sector granule exactly like a single record.
- **One CRC over the whole record is the atomicity mechanism.** A batch is
  a single sector-aligned append, so a torn or dropped append damages only
  this record's own tail; the CRC then fails and recovery truncates BEFORE
  it. Replay therefore replays either every entry of the group or none —
  there is no framing state in which half a batch survives. This is why the
  single-record-per-CRC choice beats a first/continuation/commit framing: a
  commit marker would need its own ordering argument against a torn tail,
  and the single self-contained record needs none.
- The record consumes ONE monotonic sequence number and carries ONE
  generation. Replay applies its entries in order through the ordinary
  Put/Delete path (an absent-key delete replays as a harmless no-op), the
  same reconstruction single records use, so a batch and the point mutations
  it is equivalent to recover identically.
- Ordering across the lanes is the single-record ordering applied to the
  group: the sync lane appends+syncs the batch record at the batch's point
  of no return, before any leaf pointer is published; buffered-visible
  publishes the batch and then appends+syncs it. Journal capacity for the
  whole record is reserved before the batch prepares a frame, so the sync
  lane's fence append cannot meet a full journal; a full journal folds into
  a checkpoint and recycles exactly as the single-record path does.
- This is the group-commit primitive the parallel-writer phase builds on: a
  future group of INDEPENDENT acknowledgements sharing one sync is the same
  record with unrelated entries. The multi-document batch is its first
  caller, not a special case of it.

## What this deliberately is not

Not a WAL the engine reads, not a second reader-visible representation,
not a replacement for COW publication. The journal is recovery metadata
with a strict lifetime: checkpoint truncates it; steady state without
crashes never reads it. The existing materialization journal precedent
already established this class of structure in the format.

It is also not a CDC stream. Recovery may omit a logical record once a
checkpoint makes that mutation durable, and recycle intentionally destroys old
records. The future [retained logical change log](retained-change-log.md)
captures every effective transaction above all physical mutation lanes and
keeps immutable slot incarnations under an explicit hard disk budget. That
opt-in mode reuses one commit log for recovery and replay; it does not tail or
pin this bounded recovery-only ring.

## Projected effect and gates

- Synchronous single-writer acknowledgement: from page COW + two ordered
  syncs to one bounded append + one sync — projected to close the
  measured 6-14% deficit against SQLite's comparable power-safe lane and
  overtake it, since the append is smaller than SQLite's page+WAL write.
- Group commit composes: concurrent synchronous writers share one journal
  sync through the existing commit-grouping machinery — at 8-64 writers
  the sync floor amortizes toward microseconds per acknowledgement, which
  is the entire reason the parallel-writer phase follows this one.
- A pwritev2(RWF_DSYNC) lane is worth a Linux lab: a FUA-class record
  write may deliver durability without the separate fdatasync syscall.
- Gates before promotion: power-safe mixed lanes vs SQLite on the same
  harness; crash matrix covering torn tails, reordered records, journal
  wrap, and checkpoint-concurrent crashes; recovery-time bounds at full
  journal; zero read-path deltas (the standing benchmark set).

## Sequencing

After the template-columnar lab verdict and the seqlock read landing;
before the per-tablet write-concurrency work, which multiplies its group
commit benefit.
