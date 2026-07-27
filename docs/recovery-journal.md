# Recovery-only redo journal

Design for closing the synchronous-write gap without touching the read
path. Everything here is projected until the qualification benchmarks run.

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

- A synchronous mutation is applied to the canonical in-memory frames
  exactly as buffered-visible does today (in-place patch or COW), then
  appends one redo record — key, value bytes, generation, checksum — and
  issues the mode's sync primitive on the journal alone. Acknowledgement
  returns after that single bounded append+sync.
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
- DurabilitySync keeps its platform barrier on the journal append
  (F_FULLFSYNC on Darwin); the two-phase root protocol keeps its own
  barriers at checkpoint time. Buffered-visible gains an optional
  per-mutation durable acknowledgement at the same journal price.
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

## What this deliberately is not

Not a WAL the engine reads, not a second reader-visible representation,
not a replacement for COW publication. The journal is recovery metadata
with a strict lifetime: checkpoint truncates it; steady state without
crashes never reads it. The existing materialization journal precedent
already established this class of structure in the format.

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
