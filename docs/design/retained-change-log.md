# Opt-in retained logical change log

**Status:** future design. No public change-feed API, retained-log format, or
cursor below is implemented. Every performance and space effect is a gate, not
a current product claim.

**Idea:** when a caller explicitly starts a change feed, promote the logical
redo record into one segmented commit log that serves both crash recovery and
ordered change replay. When the change log is inactive, allocate no change-log
storage, retain no history, and construct no events. Ordinary readers never
consult the log in either case.

The non-negotiable space rule is:

```text
sum(allocated data blocks of every change-log sibling)
  <= configured hard capacity
```

A slow or disconnected consumer cannot silently grow the log past that bound.
The default full-capacity action expires lagging cursors and reuses whole
slots; an explicit fail-admission policy may reject a mutation instead.
Neither policy exceeds the configured capacity or blocks forever.

## Decision

The change stream is an optional property of a collection, not another
reader-visible representation:

```text
logical transaction
  -> complete fallible prepare
  -> one checksummed logical commit frame
  -> append to the dedicated log
  -> one transaction durability fence per prepared steady-state commit group
  -> allocation-free canonical-frame publication
  -> acknowledge and expose the durable change high-water
```

The active log is the durability authority for every logged transaction.
Checkpointing folds committed transactions into the canonical page graph and
advances the recovery cursor. It does not destroy history that remains inside
the configured replay window.

The fence accepts only a complete `PreparedCommit`. That object owns the
logical frame, checksums, canonical page/leaf after-images, index changes,
retirement records, generation and commit sequence, and every log, cache, and
staging reservation. Routing, structural work, page acquisition, allocation,
validation, and all fallible device operations finish before the fence.
Post-fence publication may only install prebuilt references and counters in
preallocated memory; it performs no I/O, allocation, callback, or operation that
can report an ordinary error.

If an invariant breach nevertheless prevents post-fence publication, the
durable frame is already committed. The collection returns
`ErrCommitOutcomeUnknown`, sticky-poisons mutation and reads, and requires
reopen to replay the frame; it never rolls the log back or exposes an older live
view as if the commit failed.

This is deliberately not a second CDC file beside the recovery journal. Two
independently synced files would add a durability fence and create a cross-file
commit problem. The retained mode replaces the recovery-only record path for
its active history epoch; it does not duplicate it. The selected store root
names exactly one redo authority. Activation and deactivation quiesce admission
and durably hand that authority between the existing `.rjournal`, when
configured, and `.clog`.

## Why the current recovery journal is not a change stream

The existing bounded journal has useful point and whole-`Update` record shapes,
but the wrong coverage and lifetime contracts:

- a full journal may checkpoint the just-published mutation and omit its redo
  record because the root is then sufficient for recovery;
- journal capture remains wired into selected ordered-primary mutation lanes;
  legacy chunk-layout and root-fence paths emit no logical journal record;
- the checksummed batch record is the right atomicity precedent for one
  `Update`, but independently acknowledged requests sharing a fence still need
  distinct commit frames and cursor boundaries rather than one collapsed
  recovery batch;
- checkpoint recycle resets the journal cursor and permits every old byte to be
  overwritten;
- its sequence is recovery framing, not a stable cursor across history epochs.

All of those are valid recovery optimizations and CDC correctness failures.
Capture must therefore sit at the logical commit sequencer above every physical
mutation lane. Internal splits, merges, free-space folds, and reseals do not
emit changes; every effective user transaction does.

## Contracts

### Inactive means structurally absent

`MaxChangeLogBytes` and `MaxChangeConsumers` are immutable open-time bounds.
Zero `MaxChangeLogBytes` disables the facility. A positive value authorizes a
later activation but does not itself allocate storage or start work. Before
the first feed activates the log, and again after clean deactivation, there is:

- no change-log file, segment, manifest, cursor table, worker, or retained byte;
- no key or value copy and no event encoding;
- no additional durability write or barrier;
- no read-path branch, page acquisition, root field lookup, or overlay;
- at most one predictable nil check at the logical mutation boundary.

The existing durability mode still pays its existing recovery cost. That is not
a CDC cost. Disabled-mode write benchmarks must retain the same allocation,
device-byte, fence, and acknowledgement distributions within ordinary
run-to-run noise; a statistically significant regression blocks promotion.

History cannot be reconstructed retroactively. Activation returns a snapshot
and cursor for one exact crash-stable cut, and replay begins strictly after
that cursor. A durable offline feed or explicitly retained feed history is
active use and continues to occupy its bounded file.

### Per-collection total order

One monotonically increasing `CommitSeq` names each effective logical
transaction. It is distinct from `Generation`:

- a batch contains several logical mutations in one generation;
- structural maintenance may advance a generation without a logical mutation;
- a restored or divergent store may reuse generation numbers.

`Generation` in a transaction frame is semantic recovery data, not an implied
contiguous event sequence. Replay validates that it is greater than the current
root, performs any required unlogged structural preparation within the
intervening generation gap, and publishes the logical transaction at exactly
the recorded generation. If no structural work is needed, it jumps directly.
Consumers see no synthetic events for missing generation numbers.

The external cursor is opaque but carries the identity:

```text
{StoreID, HistoryEpoch, CommitSeq}
```

`HistoryEpoch` changes whenever retained history is installed anew, discarded,
or deliberately forked after restore. A cursor with the wrong store or epoch
fails explicitly; it never silently starts at a similarly numbered commit.
Physical slot numbers, incarnations, and byte offsets remain internal so
rotation and format changes do not leak into the API.

Each feed also has a fixed `FeedID` and incarnation. Those identify the durable
consumer slot for acknowledgements but are not part of a transferable replay
cursor. `EarliestAvailableCursor` is the greatest discarded commit boundary:
because replay is exclusive, reading after it returns the first retained
transaction. Cursor-expiration errors return that same resumable boundary.

Each alternate canonical root also seals an internal physical recovery cursor:

```text
RootLogCursor = {CommitSeq, Slot, Incarnation, BlockOffset}
```

It names the boundary immediately after the newest log transaction represented
by that root. Unlike the external cursor, it is format-specific. The recovery
cursor in the root selected during open is authoritative; a newer log manifest
may accelerate validation but can never advance an older fallback root's
starting point. A boundary at a sealed slot's end is canonicalized to the
already-fenced successor incarnation's start, never left pointing at the old
slot's end.

### Transaction frames

One frame contains the final effective operations of one transaction:

```text
length | checksum | commit-seq | generation | operation-count | flags
operation...
commit trailer
```

An operation contains collection-local kind, key length, value length, exact
key bytes, and:

- `Put`: the complete canonical after-image supplied by the caller;
- `Delete`: no value bytes.

The first version has no before-images, expressions, or JSON patch encoding.
After-images make replay idempotent and avoid reopening the old document.
Before-images roughly duplicate change bytes and require holding or copying old
page data, so they remain a separately measured future option.

Repeated changes to one key inside `Update` retain the batch's existing
last-write-wins semantics. A no-op delete or byte-identical write that publishes
no state emits no transaction. Consumers resume only between complete frames.
A transaction larger than one durability block may use several consecutive
blocks inside one segment, but it is invisible until its checksum-valid commit
trailer is durable; recovery and consumers discard an incomplete tail.
Normalization requires one segment payload to hold the largest legal
transaction frame. A transaction never crosses a segment boundary: if the
active remainder is too small, admission rotates to the already-prepared next
slot before writing any part of the transaction.

Group formation is bounded too. One commit group may consume at most the active
remainder plus every fully prepared slot reserved for that group; the first
version stops at the active remainder plus one successor. It never admits an
unconstrained group and then discovers that several maximum-size transactions
need unprepared slots.

The writer encodes framing and checksums once per commit group using bounded
buffers allocated at activation. It gathers already-owned prepared key/value
bytes for vectored I/O where the device supports it, or copies once into an
aligned pooled block where direct I/O requires it. It never re-encodes per
consumer. Foreground compression is absent from the first format: its CPU,
scratch space, and transient rewrite amplification are a poor fit for the hard
cap until measured otherwise.

### Bootstrap and replay

Activation selects one immutable `HistoryDurability` for the complete history
epoch. Every later feed must request the same strength or fail; changing it
requires deactivation and a new epoch. The first version exposes only commits
stable to the collection mode's documented recovery boundary: synchronous
commits immediately, and asynchronous or buffered commits only through their
durable high-water. “Durable” below means durable to `HistoryDurability`;
power-safe wording is used only when the selected device mode actually promises
it.

`SnapshotAndCursor` performs one atomic handshake under the logical publication
sequencer. It first closes any buffered or asynchronous volatile window, so
every mutation included in the snapshot is either already in the
history-durable log or represented by the selected history-durable root. That
activation/bootstrap flush is an intentional one-time feed cost, not a
mutation-path fence.

The handshake then:

1. durably establish the returned feed ID, incarnation, and retention cursor;
2. capture the immutable snapshot;
3. capture the last complete `CommitSeq` represented by that snapshot;
4. release the sequencer.

The consumer scans the snapshot, closes it promptly, then requests transactions
strictly after the cursor. There is no gap and no overlap other than deliberate
at-least-once redelivery after recovery.

A long initial scan still holds an ordinary generation lease and can create
retirement pressure. A later bulk-bootstrap path may export a durable checkpoint
tagged with the same cursor, allowing the live lease to close early; that is an
optimization, not a reason to weaken the initial handshake.

Replay is at-least-once. Consumer acknowledgement state may be persisted in
batches, never synced per delivered transaction. After a crash the server may
return to an older acknowledged cursor and resend complete frames. A consumer
gets effective exactly-once behavior only by applying a transaction and its
cursor atomically at the destination, or by deduplicating the cursor.

## File and framing

The log lives in one dedicated sibling file so syncing it never drags dirty
store pages into an acknowledgement:

```text
collection.vjc
collection.vjc.clog
```

Activation physically preallocates the complete computed-capacity file once. Its
length never grows while the history epoch is active. The file contains two
alternating manifest slots followed by a fixed number of equal-size circular
segment slots:

```text
manifest 0 | manifest 1 | segment slot 0 | ... | segment slot N-1
```

The store root and every log header repeat
`{StoreID, ChangeLogID, HistoryEpoch}`; the root additionally stores its
`RootLogCursor`. A missing referenced log, mismatched identity, stale
incarnation, or non-boundary offset fails closed. Keeping the log out of the
store file preserves the narrow sync domain; keeping every segment in one
preallocated log file removes runtime file growth, inode allocation, rename,
unlink, directory-sync, and transient-over-cap cases from slot rotation.
Lifecycle crashes can leave at most this one bounded sibling, reconciled by
identity on open.

The manifest records:

- identity and format;
- configured capacity and segment geometry;
- active and prepared slot incarnations;
- earliest available sequence and a checkpointed complete-sequence hint;
- a non-authoritative copy of the newest published `RootLogCursor`;
- a fixed-capacity consumer-slot table and expiration state;
- a monotonic manifest publication counter.

The active slot is append-only; a sealed slot is immutable until reuse. Its
alternating headers and trailer record a monotonic incarnation, sequence and
generation bounds, exact used bytes, checksums, and the next slot incarnation.
Reuse writes a new incarnation, so stale bytes from the previous circuit cannot
satisfy framing. A sparse in-slot index maps sampled commit sequences to block
offsets for bounded seek without a database-wide tree.

Durability blocks are damage-granule aligned and checksummed. Small records in
one group share a block and one fence; padding is paid once per durability
group rather than once per logical event. A torn or reordered tail invalidates
only its incomplete final group. A previously fenced block is never rewritten
within its slot incarnation.

Each group trailer carries its last complete commit sequence and is the
authoritative durable high-water for that group. The manifest's complete
sequence is only a lower-bound seek hint, updated at seal, checkpoint, or a
batched metadata publication; it is not rewritten for every transaction.
Reopen scans the bounded active tail from that hint to the first invalid group.
Thus an ordinary group writes payload plus its block framing under one fence,
not a second manifest record.

The next reusable slot is incarnation-written and made durable before admission
may rotate into it. Rotation therefore changes a fixed manifest slot on the
commit path; it does not grow a file or allocate filesystem metadata under the
acknowledgement fence. With the minimum two-slot geometry, the old active slot
is scheduled for checkpoint and reuse immediately after rotation. Transactions
that fit the current active slot may continue, but admission cannot consume its
rotation reserve until another successor is fenced. The rotation manifest and
first transaction group share the log's normal fence; preparation does not add
a second fence to that commit.

Expiration, checkpoint, and incarnation preparation do require maintenance
fences. They normally run ahead of the writer; if they fall behind, admission
waits before mutation and the wait is visible in rotation P99. “One fence”
therefore means one transaction-durability fence on the prepared steady path,
not zero maintenance I/O over the lifetime of the ring.

### Activation, deactivation, and file ownership

Activation is an explicit authority handoff:

1. stop mutation and feed admission under the publication sequencer;
2. flush the visible cut, checkpoint it, and recycle the current `.rjournal`
   when present, leaving a canonical root that covers every published mutation;
3. create the final `.clog` path, reserve its complete capacity, initialize and
   fence its identity, first consumer slot, and manifests, then sync the parent
   directory;
4. publish and fence an alternate root that names the new `ChangeLogID` and
   `HistoryEpoch`, seals the empty `RootLogCursor`, and clears `JournalID`;
5. capture that root's snapshot and cursor for the already-fenced consumer
   slot, then reopen admission and return.

The old root and its recycled `.rjournal` remain a valid fallback until ordinary
root rotation supersedes them, but any selected root names exactly one redo
authority. The `.rjournal` receives no new records while `.clog` is selected.

Deactivation performs the inverse handoff:

1. stop admission and mark every feed closing;
2. checkpoint every recovery-required frame to `HistoryDurability`;
3. initialize and fence the replacement `.rjournal` at that cut when the
   configured durability lane needs one;
4. publish and fence a root that clears `ChangeLogID` and either selects that
   `JournalID` or selects no redo authority;
5. revoke and close feed readers and the log writer, remove `.clog`, sync the
   parent directory, and reopen ordinary mutation admission.

A crash before activation's root publication leaves one unreferenced bounded
file; a crash after deactivation's root publication can do the same. Open
removes it only after normal recovery selects a root that does not name its
identity.

Reusing the one final path, and refusing a new activation until reconciliation,
means interrupted lifecycles cannot accumulate sidecars. A referenced file is
never guessed away: missing or mismatched identity fails closed.

Database backup, rename, collection drop, and orphan cleanup treat the store
and its referenced log as one identity-checked unit. They must not copy a
selected root without its log or delete a log still named by that root.

## Hard space bound and retention

### Capacity

Open requires an explicit positive `MaxChangeLogBytes` before activation is
available; zero remains disabled and reserves nothing. It is an immutable
requested ceiling, never a value the engine rounds upward. Open computes
`ChangeLogCapacityBytes` by rounding down to the device allocation granule and
rejects the result if the required geometry no longer fits.

Activation asks the `Device` to reserve that complete capacity before changing
the root. The portable file device uses `posix_fallocate`/`fallocate` where
their reservation guarantee is available and the equivalent full-allocation
preallocation operation on Darwin. A sparse `truncate` is not proof of
reservation. If the device or filesystem cannot promise physically backed
blocks, activation returns `ErrChangeLogReservationUnsupported` unless a future
weaker policy is separately designed and named. The bound includes:

- both manifest slots;
- all circular segment slots, including active, sealed, free, and prepared
  incarnations;
- durability-block padding and sparse indexes;
- every consumer slot and fixed on-file scratch region owned by the log.

One inode and directory entry remain filesystem metadata outside the byte-exact
file capacity; their count is constant, not proportional to history.

Normalization rejects a configuration that cannot hold:

1. fixed metadata;
2. at least two segment slots, one active and one fully prepared successor;
3. a segment payload large enough for the maximum admitted logical transaction
   and its commit-group framing;
4. total segment payload satisfying
   `MaxUncheckpointedBytes + MaxTransactionFrameBytes <= TotalSegmentPayload`.

`MaxTransactionFrameBytes` is derived at open from the existing
`MaxBatchBytes`, key/document bounds, operation-count bound, and exact framing
overhead. It is never a caller estimate.

Before accepting a transaction, admission reserves its complete worst-case log
bytes. No mutation becomes visible and no partial record is written unless the
whole transaction can finish inside the bound.

`MaxChangeConsumers` fixes the upper bound for the on-file slot table and
in-memory cursor state at open; their allocation remains lazy until activation.
Starting another feed after those slots are occupied fails before allocating
anything; subscribers never create unbounded queues or copies.

While active:

```text
0 < ChangeLogCapacityBytes <= MaxChangeLogBytes
ChangeLogFileBytes == ChangeLogCapacityBytes
sum(ChangeLogAllocatedBlocks) <= MaxChangeLogBytes
```

`Stats` exposes capacity, allocated blocks, live bytes, reclaimable bytes, slot
count, earliest and newest cursors, each consumer's lag, forced expirations,
and capacity rejections. The activation test verifies allocated blocks rather
than trusting a sparse apparent length.

On a device that honors reservation, this prevents unbounded CDC retention and
removes ordinary later file-growth `ENOSPC`. It cannot reserve the rest of a
shared filesystem against unrelated writers. Copy-on-write filesystems,
reflinks, and filesystem snapshots may retain old physical extents when a
preallocated slot is overwritten; a strict whole-volume ceiling therefore also
requires an operator quota or non-CoW storage domain. Vibedb reports that
capability instead of claiming those external blocks as engine-controlled. A
later device error follows the existing sticky persistence failure contract.

For a standalone collection this is the complete bound. A `durable.Database`
also requires an immutable database-wide change-log capacity at open:
activation reserves each collection's whole file against that aggregate
budget, and refuses a feed whose reservation would exceed it. One database
activation mutex serializes the reservation and root handoff. Reopen selects
each collection root, then counts every referenced log plus every
identity-valid unreferenced candidate against the ledger until the latter is
durably removed. It admits no new reservation before that reconciliation
completes. Per-collection caps therefore cannot silently multiply past the
database operator's declared ceiling, including across a lifecycle crash.

### Slot reuse order

A sealed slot is recovery-reusable once a root durable to `HistoryDurability`
covers its last commit and the root that normal recovery would select no longer
points into that slot incarnation. When the cut covers the slot's final frame,
the root must first seal the already-durable successor-start cursor. It is
CDC-reusable once no valid retained cursor needs it and the time/byte policy
selects it. Physical reuse requires both.

Reuse is manifest-first:

1. verify that the selected root cursor names another incarnation;
2. durably publish a manifest that expires every affected cursor and no longer
   names the old incarnation as retained;
3. write and fence the slot's next alternating header with a strictly greater
   incarnation and the expected first commit sequence;
4. publish that incarnation as the prepared successor;
5. rotate to it only after the active slot seals.

A crash at any step selects either the old manifest/incarnation or a new empty
prepared incarnation. Stale payload bytes cannot become live because their
incarnation and first sequence do not match. No file is created or removed and
allocated bytes never change during reuse.

### Concurrent replay and reuse

A feed read never pins a slot for an unbounded client lifetime.
`Feed.ReadChangesInto` uses optimistic incarnation validation:

1. validate the feed/cursor against `EarliestAvailableCursor`, then read the
   selected slot header and incarnation;
2. copy only complete transaction frames into the caller's bounded destination
   and validate every trailer and checksum;
3. reread the selected alternating header and earliest boundary before
   returning any new destination length;
4. return only if the slot incarnation and retained range still cover every
   copied frame.

Reuse durably advances the earliest boundary before fencing a new incarnation,
and never overwrites payload before that new header is durable. A raced read
therefore discards its tentative destination suffix and returns
`ErrChangeCursorExpired`, or retries once when its exact range is still retained.
It never returns bytes assembled from two incarnations and never spins
indefinitely.

### Full-capacity policy

Both policies first protect crash recovery:

1. reserve the complete pending frame before canonical mutation;
2. if the next slot still contains recovery-required frames, force a bounded
   checkpoint;
3. seal the exact `RootLogCursor` into the canonical root—canonicalized to the
   fenced successor start at an end-of-slot cut—and fence that root at
   `HistoryDurability`;
4. then and only then update the manifest's root-cursor hint;
5. consider only now-recovery-reusable slots for the selected CDC policy.

The default `ExpireLagging` policy then:

1. selects the oldest whole committed slots needed only by lagging consumers;
2. durably marks those consumers expired and advances
   `EarliestAvailableCursor`;
3. prepares enough slot capacity for the complete pending transaction;
4. admits the transaction.

An expired feed returns `ErrChangeCursorExpired` with the earliest available
cursor and must run a new snapshot bootstrap. It never receives a silently
truncated stream.

An explicit `FailAdmission` policy instead returns
`ErrChangeLogCapacity` before mutation publication. It is useful when losing a
feed is worse than write availability. The first design does not offer
unbounded growth or indefinite blocking as hidden third policies.

Checkpointing starts proactively before either uncheckpointed bound or the
rotation reserve is reached. If admission still cannot make a slot reusable, a
capacity/resource condition rejects the transaction before mutation; a
persistence failure poisons the writer. Neither path overwrites the only
recovery copy.

`MaxRetainedAge`, when non-zero, is another reuse ceiling, not a promise
that history survives that long: the byte cap always wins. A disconnected
durable consumer counts as active until dropped or expired. Closing the final
ephemeral feed deactivates by default and returns only after the bounded file is
removed. Keeping history without an attached reader requires an explicit
durable feed or `KeepHistoryActive`; that retained history is use, not a free
inactive state. Explicit deactivation seals the epoch, checkpoints its durable
cut, removes the bounded file, and invalidates every old cursor.

Acknowledgements are early-reclamation hints, never storage leases beyond the
cap. Even a valid durable consumer is expired when `ExpireLagging` must reuse
its oldest slot.

## Commit and visibility by durability mode

### Synchronous

Prepare is fallible; append and sync occur at the point of no return; canonical
publication is allocation-free. The same log fence establishes mutation
durability and CDC durability. No second root or CDC fence is permitted on the
steady path. Snapshot-contended COW and other physical fallbacks must still
enter this one logical log path rather than silently falling back to an
unlogged root commit.

A durable frame is not yet live-feed-visible. The in-memory
`PublishedCommitSeq` advances only after canonical publication, and live readers
are capped by it even if the on-disk durable high-water is newer. After a crash,
recovery replays the durable frame before advancing the published tail. A
consumer therefore never observes a mutation before ordinary readers can
observe its canonical state.

Concurrent callers form one ordered append group and share one barrier. Their
frames retain individual commit sequences and publish in that order. A lone
writer never waits merely to fill a group; an explicit coalescing window may
trade bounded latency for throughput exactly as the existing commit option
does.

### Async-stable-in-flight

Foreground publication retains its current bounded-queue semantics. The
foreground prepare captures the complete logical frame before publication. The
background worker appends and fences those frames before a canonical root is
allowed to cover the same commits, then advances the durable change high-water.
Consumers never receive a stable event ahead of that high-water. `Flush` closes
both the store and change-log windows.

### Buffered-visible

The cheapest form captures copied logical operations in the collection's
existing bounded staging and makes them replayable only when `Flush` writes the
commit group and reaches its persistence boundary before the checkpointed root.
It adds no per-mutation device operation, but CDC latency follows checkpoint
cadence.

A caller requiring immediately durable replay selects the journal-backed
acknowledgement lane and pays its one append plus one sync. The API must not
advertise an externally stable event that a crash can remove from the source.

## Checkpoint and recovery

The root and log retain separate floors:

- `RootLogCursor.CommitSeq`: newest transaction represented by the selected
  canonical root at `HistoryDurability`, paired with its physical seek boundary;
- `EarliestAvailableCursor`: greatest discarded transaction boundary, so replay
  after it begins with the first retained transaction.

Checkpointing advances the first. Consumer progress and retention advance the
second. A lagging consumer therefore retains log slots, never snapshots,
retired data extents, or versions in the canonical reader graph.

Open reads the double manifest, selects the newest valid root, seeks directly
to that root's `{Slot, Incarnation, BlockOffset}`, validates the cursor boundary,
and replays only transactions after its `CommitSeq`. It never substitutes the
newer manifest hint for an older selected fallback root. Historical slots below
that cursor are not decoded on open. Immutable open-time
`MaxUncheckpointedBytes` and
`MaxUncheckpointedTransactions` bounds fit within the log capacity and force
checkpoint admission before either is crossed. Recovery work therefore remains
bounded by that window, not by total CDC retention.

After replay, vibedb checkpoints the recovered state before advancing the
recovery floor. It does not erase retained CDC history. A second crash during
replay or checkpoint finds the same complete frames and replays them
idempotently.

## API shape

Names are provisional; the semantic split is not:

```text
StartChangeFeed(options) -> Feed, Snapshot, Cursor
Feed.ReadChangesInto(dst, after, maxBytes) -> batches, nextCursor
Feed.WaitForChanges(context, after)
Feed.Ack(cursor)
Feed.EarliestAvailableCursor()
Feed.Close()
DeactivateChangeLog()
```

`Feed.Ack` verifies the feed's durable slot ID and incarnation before advancing
it; an acknowledgement from an expired or recycled feed cannot move another
consumer's floor.

`ReadChangesInto` returns whole transactions and reuses caller storage on the
warmed path. A byte limit smaller than the next complete transaction reports
the required size rather than splitting it. Filtering, JSON/Avro encoding,
network sends, and user callbacks execute outside the collection writer and
outside durable acknowledgement. One canonical logical log serves every feed;
feed-specific predicates never multiply write work.

Tailers use bounded readahead on a sequential sibling descriptor and do not
enter the collection page cache. That prevents a direct read-path dependency,
not shared-device contention: an active high-rate tailer can still compete for
I/O bandwidth and CPU. Mixed read/write/tail benchmarks must report that
indirect cost instead of calling enabled CDC read-free.

The first version is per collection. `durable.Database` has independent
collection commit streams and no cross-collection transaction, so a
database-wide total order would require a new database commit sequencer and
belongs in a separate design.

## Failure behavior

- Append, sync, manifest, or slot-preparation failure poisons the active
  writer under the existing die-don't-retry persistence rule.
- A synced frame followed by a crash before in-memory publication may appear
  after recovery. This is ordinary unknown-outcome behavior and why consumers
  deduplicate cursors.
- Any unexpected post-fence publication failure returns
  `ErrCommitOutcomeUnknown`, fails reads closed, and relies on reopen replay;
  the engine cannot convert the already-durable frame into a failed commit.
- Corruption before the active tail fails closed; only an incomplete final
  durability group is a valid truncation.
- Cursor expiration is a feed error, not database corruption.
- Capacity rejection occurs before publication and does not poison the writer.
- Consumer code cannot run on, block, or fail the writer.
- Retained values remain recoverable from the log after deletion until their
  slot range is reused. Operators must set retention with data-erasure policy
  in mind.

## Acceptance gates

### Correctness and crash safety

- byte-exact codec fixtures for manifest, slot, block, transaction, and
  cursor framing;
- exhaustive crash cuts for append, group sync, publication, checkpoint,
  `.rjournal` authority handoff, directory sync, rotation, expiration, manifest
  flip, incarnation preparation, and slot reuse;
- torn, reordered, duplicated, mismatched-incarnation, and checksum-corrupt
  slot cases;
- second-crash recovery while replaying and while advancing the recovery floor;
- root fallback with a newer manifest proves recovery starts from the selected
  root's physical cursor, with seek bytes independent of retained-history size;
- reuse of the slot formerly named by the newest root, followed by a crash
  before another root publication, reopens through the canonicalized successor
  cursor rather than a stale incarnation;
- `Put`, `Delete`, `Update`, automatic combining, snapshot-contended COW,
  structural fallback, overflow values, and no-op mutations;
- a prepared-commit audit and fault sweep proves every allocation, structural
  action, cache acquisition, index update, and fallible I/O precedes the fence,
  while forced post-fence failure poisons and recovers the committed frame;
- replay from root generation `G` through an unlogged structural gap to a frame
  recorded at `G+2`, preserving that exact final generation without a fake
  event;
- whole-transaction delivery or no delivery at every cursor and byte limit;
- concurrent expiration and slot overwrite at every read boundary either
  returns one old incarnation or an explicit expired cursor, never mixed bytes;
- snapshot-plus-cursor oracle proving no gap under concurrent publication;
- concurrent first activation, additional-feed bootstrap, final-feed close, and
  deactivation races with stale feed incarnations rejected;
- mismatched durability joins are rejected, while deactivate/reactivate may
  select a new strength only with a new history epoch;
- group commit proving several transaction frames share one fence yet recover
  and replay at independent commit boundaries.

### Disabled and read-neutral performance

- zero change-log files, retained bytes, workers, copies, and allocations while
  disabled;
- identical point-read page acquisitions, cache decisions, and allocations;
- warm and cold point, ordered scan, exact-index, zone-pruned, and held-snapshot
  benchmarks within standing noise;
- disabled mutation allocations, device bytes, fences, and P50/P99
  acknowledgement within standing noise.

### Enabled write performance

- one logical encode and no old-document read for after-image mode;
- one logical append group and one transaction-durability fence on the prepared
  steady path;
- maintenance fences, rotation/reclamation P99, and admission waits reported
  separately rather than hidden in the steady-path count;
- no independently synced CDC and recovery records;
- operations, logical bytes, device bytes, padding, commits, and fences reported
  together;
- 1, 8, and 64 writer lanes with group size and P50/P99 acknowledgement;
- small, 4 KiB, overflow, batch-limit, skewed, and uniform workloads;
- mixed point-read, scan, write, and active-tailer workloads reporting device
  contention and page-cache residency;
- no synchronous file growth or directory metadata on the steady path.

### Space and lag

- write at least ten times the configured capacity with a permanently stalled
  consumer; allocated bytes never exceed the bound and the old cursor expires;
- unsupported sparse-only reservation fails activation, and supported devices
  verify apparent bytes, allocated blocks, and the database aggregate ledger;
- default full-capacity behavior expires the consumer and lets writes continue;
- fail-admission behavior rejects before publication and never spins;
- the maximum legal transaction at every remaining slot offset rotates or
  rejects atomically before publication;
- prepared incarnations remain inside the fixed bound across every crash;
- space amplification reported for high-cardinality and repetitive JSON, with
  before-images disabled;
- recovery time and bytes read remain independent of retained-history size.

No throughput result graduates without its device bytes, allocated space,
tail latency, forced-expiration count, and correctness run.

## Delivery order

1. Codec and fixed-capacity circular-slot/manifest lab with byte-exact corruption
   tests.
2. Logical transaction framing above every mutation lane, with disabled-mode
   structural and benchmark gates.
3. Prepared-path one-fence synchronous commit and group commit; no public CDC
   API yet.
4. Checkpoint recovery cursor and bounded-tail replay.
5. Slot rotation, hard-cap admission, expiration, and crash-safe reuse.
6. Snapshot-plus-cursor bootstrap and caller-buffered replay API.
7. Full crash, workload, read-neutrality, and space qualification.
8. Promote one path and delete any temporary duplicate journal route.

## Research lineage

The design takes the narrow pieces that fit vibedb's invariants:

- [RocksDB `GetUpdatesSince`](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/db.h)
  exposes transaction batches by sequence, while its WAL TTL and size options
  make replay availability explicitly dependent on retained log files.
- [PostgreSQL logical decoding](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html)
  supplies the exported-snapshot/consistent-point handshake, independent
  consumer slots, and at-least-once duplicate contract. Its warning that an
  abandoned slot can retain enough WAL to fill disk is exactly why vibedb's
  byte cap overrides every consumer.
- [etcd's watch API](https://etcd.io/docs/v3.6/learning/api/)
  resumes from logical revisions and returns the minimum available revision
  explicitly when compaction outruns a watcher. Vibedb adopts that honest
  expired-cursor contract instead of silently skipping retained history.
- [CockroachDB changefeeds](https://www.cockroachlabs.com/docs/stable/create-changefeed)
  separate initial scan from cursor-based resume and expose a resolved
  high-water. Vibedb needs no distributed timestamp frontier for one serialized
  collection; `CommitSeq` is the complete order.
- [SQLite sessions](https://www.sqlite.org/sessionintro.html) distinguish
  replayable after-image patchsets from conflict-detecting before-image
  changesets. The first vibedb format deliberately chooses the smaller,
  idempotent after-image boundary.

The result is not an LSM WAL, MVCC history, or reader overlay. It is one
optional, bounded logical commit history beside a complete canonical page
graph.
