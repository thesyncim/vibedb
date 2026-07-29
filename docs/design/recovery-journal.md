# Recovery-only redo journal

**Status:** implemented on the ordered primary graph. `DurabilitySync`
acknowledges through the journal, and buffered-visible gains the same
per-mutation durable acknowledgement with `Options.RecoveryJournal`. Readers
still consult canonical frames only. What remains projected is the *win*: a
competitive refresh that measures the single-fence sync lane against SQLite,
cross-writer group commit, and a Linux FUA record write.

**Idea:** sync a bounded redo record in a separate file for acknowledgement,
while readers continue to use canonical frames only. A checkpoint later folds
the records into the ordinary root publication protocol.

## The insight

The read-neutrality invariant forbids any representation a READER must
consult — memtables, deltas, version chains. It does not forbid a log that
only crash RECOVERY replays. The materialize-before-publish rule exists to
keep point reads singular; acknowledgement durability is a separate concern,
and conflating them is why the retired chain-fence sync path paid page-granular
COW plus two ordered syncs while a WAL system appends a few hundred bytes and
syncs once.

## Design

A bounded redo journal lives in a SEPARATE file beside the store file, not a
region inside it: `fdatasync` flushes every dirty page of the file it is
called on, so an in-store journal region would drag concurrently pre-written
checkpoint pages into every acknowledgement sync and make its latency
unpredictable. A dedicated file keeps the sync domain to the journal's own
preallocated pages — the same reason every production WAL is a separate file.
The store root records the journal's identity so recovery cannot pair
mismatched files (`StateRoot.JournalID`, a random 128-bit value minted at
`CreateFromPrimary`), and a missing-but-referenced journal fails closed
(`ErrRecoveryJournalMissing`) while a cleanly recycled one is the ordinary
empty state.

- Both synchronous and buffered-visible mutations apply to the same canonical
  in-memory frames (in-place patch or frame COW); one redo record — key, value
  bytes, generation, checksum — is the unit of durability for each
  (`recoveryRecordKindPut`, `recoveryRecordKindDelete`). The two lanes differ
  only in ORDER, because they make different visibility promises:
  - `DurabilitySync` appends the record and issues the mode's sync primitive on
    the journal alone BEFORE the mutation is applied and published, so
    visibility strictly follows durability (`journalBeforePublishLocked`,
    called at the mutation's point of no return in `cowBufferedPrimaryMutation`
    before `publishFileState`). A concurrent reader in the append window sees
    the pre-mutation state — nothing is published yet — and a sync failure
    leaves nothing visible and sticky-poisons the collection. The primary
    commit is split into a fallible prepare (routing, eligibility, split and
    capacity checks, leaf and posting encode — no visibility) and an infallible
    publish; the journal sync sits at that point of no return, after
    journal capacity for the record is already reserved so the append cannot
    meet a full journal.
  - Buffered-visible applies and publishes first, then appends and syncs the
    record (`journalAckLocked`, after `publishFileState`). Its acknowledgement
    contract already permits a volatile window, so the cheaper
    apply-then-journal order is correct there, and a full journal folds into a
    checkpoint instead of poisoning.
  Acknowledgement returns after that single bounded append plus sync.
- Readers never consult the journal. Visibility comes from the canonical
  frames, exactly as before; the journal is write-only until recovery.
- Checkpoints materialize dirty frames, publish the alternate root, then
  recycle the journal head past the checkpointed generation
  (`Recycle(baseGeneration)`) in the same publication. Recycling rewrites the
  header and resets the append cursor without zeroing stale bytes — a fresh
  base sequence invalidates them.
- Recovery selects the newest valid root, then replays journal records after
  that root's generation through the ordinary mutation path, stopping at the
  first invalid record. Replay is bounded by journal capacity; journal pressure
  forces a checkpoint (`recoveryJournalCheckpointRecords`) exactly like staging
  pressure does.
- Torn or reordered journal tails are detected per record (checksum plus
  monotonic generation); a torn tail truncates, never corrupts — the root and
  its graph were never touched before their own two-phase publication.
- On the ordered primary graph `DurabilitySync`'s per-mutation acknowledgement
  IS the journal append plus its platform barrier (`F_FULLFSYNC` on Darwin);
  the deferred root is published on the same checkpoint cadence as
  buffered-visible, with the two-phase root protocol's own barriers at
  checkpoint time. The old chain-fence acknowledgement — publish a full
  copy-on-write generation and wait on the committer's root fence per mutation,
  two ordered device fences — is retired for this lane: nothing on the primary
  sync path waits on `committer.Wait`, and the sync-mode outer generation stays
  zero so the fence guard is skipped. The journal is therefore minted
  UNCONDITIONALLY for primary `DurabilitySync` — it is how sync acknowledges,
  not an option (`syncJournalLane`) — while `Options.RecoveryJournal` is the
  opt-in only for buffered-visible. A chunk-layout `DurabilitySync` store has no
  journal mutation lane and keeps the chain fence (`chainFenceSync`) until the
  chunk store is deleted; requesting a journal on a layout that cannot feed it
  fails closed (`ErrRecoveryJournalRequiresPrimary`).
- Because visibility follows durability on the sync lane, a reader can never
  observe an acknowledged-but-not-yet-durable mutation, so that lane needs none
  of the fail-closed read logic the old publish-before-durable sync path
  required. A journal-backed collection retains its last admitted immutable
  view after a failure exactly as buffered-visible does — every visible
  generation is journal-durable. The fail-closed-if-visible-leads-durable read
  path survives only for the chunk sync lane's chain fence and async canonical
  materialization, and is deleted with the chunk store.
- Journal identity pairing checks three things before replay
  (`RecoveryJournal.Pair`): identity (`StoreID` and `JournalID`;
  `ErrRecoveryJournalIdentity`), geometry (the store `PageSize`;
  `ErrRecoveryJournalGeometry`), and a base-generation epoch (a journal whose
  base generation is ahead of the store root — a store restored beside a live
  journal — is rejected with `ErrRecoveryJournalEpoch`).
- Linux cost parity with a production WAL requires three specifics, not
  optimizations: journal extents are preallocated and recycled
  (`fallocate`, then `Recycle`) so a record sync never commits filesystem
  metadata; the ordinary-sync record barrier is `fdatasync`, not `fsync`; and
  file growth never happens under a sync. (The power-safe barrier is stronger —
  `F_FULLFSYNC` on Darwin, `fsync` on Linux — since it must drain volatile
  caches, not just order data.) A journal that extends a file under each sync
  pays ext4/xfs metadata journaling and loses the entire advantage.
- Journal sync failures are terminal: after an fsync-class error Linux may drop
  the very dirty pages a retry would need, so the sticky-failure poisoning
  covers the journal path (`journalFailure`, `poisonJournalLocked`) with
  die-don't-retry semantics, never a retry loop. `PersistenceError` joins the
  committer failure with the journal failure and requires reopen.

## Batch records — the group-commit primitive

A multi-document `Update` on the primary graph makes its whole group durable
with ONE append and ONE sync, not one per document
(`recoveryRecordKindBatch`, `AppendBatch`, `updatePrimaryBatch`). The record
format carries this directly rather than framing it as first/continuation/
commit records across separate appends:

- A batch record reuses the 32-byte record prefix with `kind = Batch` and
  carries one `entryCount` and one `bodyLen`. The body is a run of `entryCount`
  entries, each a fixed header (entry kind, key length, value length) followed
  by the key and value bytes. One CRC (and its complement) covers the prefix
  and the entire body; the record is padded to the sector granule exactly like
  a single record.
- **One CRC over the whole record is the atomicity mechanism.** A batch is a
  single sector-aligned append, so a torn or dropped append damages only this
  record's own tail; the CRC then fails and recovery truncates BEFORE it.
  Replay therefore replays either every entry of the group or none — there is
  no framing state in which half a batch survives. This is why the
  single-record-per-CRC choice beats a first/continuation/commit framing: a
  commit marker would need its own ordering argument against a torn tail, and
  the single self-contained record needs none.
- The record consumes ONE monotonic sequence number and carries ONE generation.
  Replay applies its entries in order through the ordinary Put/Delete path (an
  absent-key delete replays as a harmless no-op), the same reconstruction
  single records use, so a batch and the point mutations it is equivalent to
  recover identically.
- Ordering across the lanes is the single-record ordering applied to the group:
  the sync lane appends and syncs the batch record at the batch's point of no
  return, before any leaf pointer is published; buffered-visible publishes the
  batch and then appends and syncs it. Journal capacity for the whole record is
  reserved before the batch prepares a frame (`FitsBatch`,
  `ensurePrimaryBatchJournalRoom`), so the sync lane's fence append cannot meet
  a full journal; a full journal folds into a checkpoint and recycles exactly
  as the single-record path does.
- This is the group-commit primitive the parallel-writer phase builds on: a
  future group of INDEPENDENT acknowledgements sharing one sync is the same
  record with unrelated entries. The multi-document batch — the transactional
  `WriteBatch` behind `Collection.Update` and the SQL driver's `COMMIT` — is
  its first caller, not a special case of it.

## What this deliberately is not

Not a WAL the engine reads, not a second reader-visible representation, not a
replacement for COW publication. The journal is recovery metadata with a strict
lifetime: checkpoint recycles it; steady state without crashes never reads it.
The materialization journal precedent already established this class of
structure in the format.

It is also not the distributed replication/changefeed log. That log has
independent shard-term, commit-sequence, and retention semantics in
[distributed sharding](distributed-sharding.md).

## Projected effect and gates

The mechanism has landed; these are the still-open measurements and extensions.

- Synchronous single-writer acknowledgement changed from page COW plus two
  ordered syncs to one bounded append plus one sync. The single-fence
  power-safe journal acknowledgement measures 4.05 ms at store level against
  the device's `F_FULLFSYNC` floor; the last published competitive tables
  ([RESULTS.md](../../bench/competitive/RESULTS.md)) predate wiring it as the
  sync lane and still show the two-fence deficit, so proving the overtake
  against SQLite's comparable power-safe lane is pending the next competitive
  refresh. The ordinary-sync lane currently measures 32.5 µs per journal
  acknowledgement and loses to SQLite and Badger; the active work is collapsing
  the sync lanes onto single-fence acknowledgements and, on Linux,
  group-committed FUA journal writes.
- Group commit composes: concurrent synchronous writers sharing one journal
  sync through the commit-grouping machinery is the parallel-writer phase's
  first payoff, since the sync floor then amortizes across writers instead of
  dividing the fsync budget.
- A `pwritev2(RWF_DSYNC)` lane is worth a Linux lab: a FUA-class record write
  may deliver durability without the separate `fdatasync` syscall.
- Gates before the sync lane is declared competitive: power-safe mixed lanes
  vs SQLite on the same harness; the crash matrix covering torn tails,
  reordered records, journal wrap, and checkpoint-concurrent crashes;
  recovery-time bounds at a full journal; zero read-path deltas (the standing
  benchmark set).

## Sequencing

The journal landed after the epoch-protected read work and is now the sync
lane's acknowledgement. What follows is the per-tablet write-concurrency work,
which multiplies its group-commit benefit, and the Linux FUA record-write lab.
