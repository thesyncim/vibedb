# Durability

Durability is three separate choices: when a mutation is acknowledged, when it
becomes visible, and what persistence boundary a checkpoint or commit crosses.
Names such as “async” or “sync” are insufficient without all three.

The caller owns the `*os.File`. Keep it open until `Collection.Close` returns.
The guarantees below assume the filesystem, kernel, controller, and device
honor successful writes and synchronization calls.

## Mutation contracts

| Contract | Go option | Acknowledgement and visibility | Stable-storage work | Loss window |
| --- | --- | --- | --- | --- |
| buffered-visible | `DurabilityBufferedVisible` | success after bounded canonical staging; immediately reader-visible | none on ordinary admission, unless `RecoveryJournal` is set (one redo append plus sync per mutation) | process or machine failure may lose every acknowledged generation after the last successful `Flush`, or after the last synced per-mutation record when `RecoveryJournal` is set |
| async-stable-in-flight | `DurabilityAsyncVisible` | success after the bounded committer accepts the generation; immediately reader-visible | the background worker continuously writes ordered COW generations and roots | failure may lose acknowledged generations newer than `DurableGeneration`; `Flush` closes the window |
| synchronous, power-safe | `DurabilitySync` (zero value) | success waits for one journal append plus its power-safe sync, then applies and publishes — visibility strictly follows durability | strongest supported platform sequence: one journaled record barrier per mutation | after success, recovery selects the acknowledged generation; before success, outcome may require reopen |

All modes are linearizable inside the live process. “Buffered” weakens crash
survival, not read ordering. Bounded staging or retirement pressure may force a
buffered checkpoint before the requested cadence; `Stats.AutomaticCheckpoints`
and `Stats.RetirementPressureCheckpoints` expose those events.

`DurableGeneration` reports the newest generation protected by either a final
root boundary or a completed buffered checkpoint-delta journal sync. `Flush`
waits for or checkpoints the current reader-visible cut.
`Close` rejects new work, drains or checkpoints accepted work, and releases
resources; it does not close the caller's file.

## Sealed main-file capacity

`Options.PhysicalCapacityBytes` is an immutable ceiling for the collection's
main file, not an eager reservation of the entire ceiling. A non-zero value is
supported on Linux for rooted `DurabilityAsyncVisible` collections without a
recovery journal or canonical materialization. Other durability lanes cannot
be used because they may acknowledge deferred work whose exact rooted file
advance was not sealed first; they are rejected before the file is changed.

The collection tracks a smaller, monotone physical high-water. Create strictly
allocates only the initial rooted graph. `EnsurePhysicalAllocation` drains the
accepted committer cut, allocates every byte through a requested aligned
high-water, privatizes reflinked extents, and synchronizes that allocation
before writes may use it. Open repeats the complete-prefix proof and repairs
supported punched holes before recovery. Capped collections do not hole-punch,
so a certified prefix cannot later become sparse through online reclamation.
Linux filesystems must support `FALLOC_FL_UNSHARE_RANGE`; ext4 is the narrow
exception because it has no writable reflink support.

The certificate assumes exclusive allocation ownership while the collection is
open. Callers and other processes must not truncate, punch, reflink-clone
(`FICLONE`), or otherwise change allocation on the main file outside collection
APIs. The writer lease coordinates cooperating collection instances; advisory
locking cannot prevent an unrelated descriptor from violating this prerequisite.

This certificate applies only to the collection main file. It makes no claim
about a Raft log, recovery-journal suffix, or serving-layer reservation.

## Checkpoint strengths

`CheckpointStrength` applies only to buffered-visible `Flush` and `Close`,
including the journal sync used by a bounded delta checkpoint. It does not
weaken `DurabilitySync` or `DurabilityAsyncVisible`.

| Strength | Go option | Contract |
| --- | --- | --- |
| power-safe | `CheckpointPowerSafe` (zero value) | data pages and the alternate root cross the strongest platform boundary used by vibedb; intended to survive sudden power loss |
| filesystem | `CheckpointFilesystem` | two-phase ordinary filesystem synchronization; survives process failure, but on Darwin does not promise survival of volatile drive-cache loss |

`CheckpointFilesystem` is accepted only with buffered-visible durability, the
portable backend, and buffered writes. The public checkpoint operation is
`Flush`; “checkpoint” in design documents names the same captured-generation
protocol.

## Publication protocol

Normal COW publication is:

1. Write every new data and metadata page.
2. Cross the selected data ordering barrier.
3. Write the alternate checksummed inline root.
4. Cross the selected final persistence barrier.
5. Mark the generation durable and release eligible frames/extents.

The two inline root slots alternate. Recovery validates both newest-first,
including each candidate's inline state, free-log heads, canonical catalog, and
top-level primary and exact-root codecs, with fallback to the preceding
candidate when that selection-phase validation fails. After selection,
`store/durable.Open` validates the primary routing graph and fully admits the
exact catalogs and term leaves against live masks derived from that graph. A
descendant-only failure in this deeper admission fails `Open` closed; it does
not currently restart selection with the older inline root. The root write is
the commit point; a data page not named by the selected root is not visible
after recovery.

Qualified canonical materialization adds a durable undo capsule before changing
an existing extent. Its exact protocol and recovery cases are in
[the format](format.md#catalog-free-space-and-journals) and
[the design](design/canonical-materialization.md).

## Platform synchronization

| Platform / lane | Data phase | Final root phase | Meaning |
| --- | --- | --- | --- |
| Darwin, power-safe COW | `F_BARRIERFSYNC`, falling back to `F_FULLFSYNC` when unsupported | `F_FULLFSYNC` | orders data before root, then asks the drive to drain volatile caches |
| Darwin, filesystem checkpoint | `fsync` | `fsync` | ordinary filesystem/process-crash boundary; drive cache may remain volatile |
| Linux, power-safe COW | `fsync` | `fsync` | includes data and inode state required to recover the file |
| Linux, filesystem checkpoint | `fdatasync` | `fdatasync` | orders and persists preallocated data extents without forcing unrelated inode metadata |
| Other supported systems | `os.File.Sync` | `os.File.Sync` | platform File.Sync contract; no stronger device-specific primitive is claimed |

`F_BARRIERFSYNC` is an ordering primitive, not the final power-safe
acknowledgement. `fdatasync` is used only where the checkpoint writes
preallocated existing extents. A sync error is terminal for the live writer;
the code does not retry dirty pages that the kernel may already have discarded.

For a contiguous Linux commit, the `io_uring` backend can express the same two
durability epochs as a soft-linked `RWF_DSYNC` data write followed by an
`RWF_DSYNC` root write. The link prevents the root write from starting after a
failed or short data write. Unsupported filesystems retain the explicit data
barrier, root write, and final barrier path.

## Crash windows

### Buffered-visible

- Before `Flush`: the durable file names the previous checkpoint. All later
  acknowledged mutations may disappear after process or machine failure.
- An ordinary class-5 update/delete cut can complete `Flush` by appending the
  entire consecutive generation interval as one format-v1 recovery-journal
  batch and syncing it once. Eligible existing-key replacements carry only a
  compact scalar patch plus the checksum of the complete expected result;
  deletes and unqualified replacements retain full logical redo. Before that
  sync recovery ignores the tail; after it, recovery replays the exact cut over
  the previous physical root.
- A structural mutation, incomplete delta interval, journal or overlay
  pressure, snapshot materialization, or `Close` takes the full physical path
  below and recycles the journal after the new root is durable.
- During data writes or the first barrier: recovery selects the previous root.
- After data persistence but before the alternate root is durable: the new
  pages are unreachable; recovery selects the previous root.
- After the final root boundary: recovery selects the checkpointed cut.

### Async-stable-in-flight

- Before queue acceptance: the mutation failed and is not visible.
- After acceptance but before the root boundary: the mutation is visible in
  process but may be absent after a crash.
- After `DurableGeneration` reaches it, or after `Flush`: it has the same
  recovery contract as a completed synchronous generation.

If persistence fails, ordinary COW readers roll back to the last confirmed
durable state. A failed asynchronous canonical replacement rejects reads until
reopen because recovery must first restore or select the page image.

### Synchronous, journal-backed (primary graph)

- Before the journal record's sync completes: the call has not succeeded and
  nothing is applied or visible; recovery selects the last checkpointed root
  and replays the journal records that preceded this one.
- After the record's sync but before the next checkpoint: the mutation is
  durable and visible; a crash replays it from the journal onto the selected
  root. This is the window the deferred root trades for a single fence.
- After a checkpoint folds the record in: the mutation is part of a
  power-safe root and the journal head is recycled past it.
- A checkpoint's own final-root barrier reporting an error yields
  `ErrCommitOutcomeUnknown`; reopen determines which root won.

### Synchronous, chain-fence (journal-less reopen)

One synchronous configuration acknowledges through the committer's root fence
instead of the journal: a store created `DurabilityAsyncVisible` and reopened
`DurabilitySync` carries no journal to open, so each mutation publishes a COW
generation and waits for its root barriers. Before the data barrier the call
has not succeeded; between the data and final root barriers recovery normally
selects the old root; a final barrier error yields `ErrCommitOutcomeUnknown`
and reopen determines which root won; after success the new generation is
visible and power-safe under the platform assumptions above.

## Foreground physical reclamation

A completed physical root may run one bounded online hole-punch pass after the
root is durable and the recovery-journal base covers it. This is post-durability
space reclamation, not another commit point: it never changes allocator
metadata or apparent file length, and replay never depends on a successful
deallocation. Journal-only buffered `Flush` boundaries do not run the pass.

The physical-generation guard permits one pass for each newly authoritative
root. The planner samples the coherent durable root, fallback root, and journal
base under `snapshotGate`, and records exactly the authority it sampled only
after a successful planning result. Repeated notification for one generation
is therefore a no-op; a hard validation error before deallocation does not
consume authority for a different generation.

Candidate discovery is fixed at no more than 1,024 exact free identities and 64
coalesced runs. Reusable, pending-retirement, and absorbed-retirement sources
receive independent cursors and fair active-source shares; unused shares are
redistributed for at most three rounds. The spend phase performs no more than
six successful deallocation calls and no more than 20 MiB per physical
generation. An oversized exact extent is chunked and resumed at later physical
boundaries rather than making one boundary unbounded.

The reader fence is held only while the generation floors are sampled and the
bounded candidate values are copied. It and `snapshotGate` are released before
validation and before every `F_PUNCHHOLE` or `fallocate(PUNCH_HOLE)` syscall;
the writer lock continues to prevent allocation reuse. Linux and Darwin expose
the platform operations. Other platforms, unsupported filesystem responses,
or a syscall error disable further attempts for that open collection. `EINTR`
is retried at most four times; other errors are counted once and are not retried
at later boundaries. `HolePunchRanges`, `HolePunchBytes`,
`HolePunchSkippedRanges`, `HolePunchUnsupported`, and `HolePunchErrors` report
the outcome. Unsupported/error outcomes do not poison the writer or fail the
already successful durability boundary.

This online reclamation path requires neither a background compactor nor an
offline maintenance cycle. A filesystem without hole-punch support still
reuses the same logical free extents inside the store; only returning their
physical blocks to the filesystem is unavailable without a rewrite.

## Persistence failures

Any write or synchronization failure poisons the writer. A journal append or
sync failure is terminal in the same way: after an fsync-class error the kernel
may have dropped the dirty pages a retry would need, so the failure is sticky
and die-don't-retry, never a retry loop. Later mutations, checkpoints, and
close-drain work return the sticky `PersistenceError`, which joins any committer
failure with the journal failure. `errors.Is` may match
`ErrCommitOutcomeUnknown` when only recovery can decide the last committed
generation. Close the collection and reopen the file; do not retry the logical
mutation against the poisoned live handle.

Multi-collection prepare append and prepare sync failures use the same
append-or-sync-is-terminal, die-don't-retry rule: they poison the failing
collection with the plain persistence error and abort the transaction
definitely. They do not open the unknown-outcome window. Only a decision-record
sync failure escalates to `ErrCommitOutcomeUnknown`, and that poison widens to
every collection under the catalog (see below).

## Database transactions

A write that touches exactly one collection still takes today's
`Collection.Update` path: one journal record, one sync, byte-identical to the
single-collection contract. A write that dirties K ≥ 2 collections under one
catalog commits through the database transaction protocol:

1. Validate bounds and per-participant conflicts; refuse before staging.
2. Stage every participant under writers held in the process-global snapshot
   order. Any failure unwinds all staged work; nothing is journaled.
3. Prepare: append and sync one conditional batch record (recovery-journal
   format word `RecoveryJournalFormatConditional`, kind 5) in each
   participant's own journal. An append or sync failure poisons that
   collection with the plain persistence error and aborts the whole
   transaction definitely — the decision was never attempted.
4. Decide: append the decision record to the database decision log
   (`txn.vtm`) and cross one power-safe sync. **That sync is the sole atomic
   commit point.** Append failure is a definite abort. Sync failure is the
   unknown-outcome window below.
5. Publish: flip every participant's publication gate in the same global
   order so no multi-collection read cut observes a partial set.

Cost: a K-participant commit performs K+1 fsyncs (K prepare syncs plus the
decision sync) and holds K writers across them. Reducing that to one sync is a
named follow-up (shared redo in the decision log); this pass keeps the
K+1-sync protocol.

| Lane | Multi-collection visibility | Crash promise | Acknowledgement |
| --- | --- | --- | --- |
| sync-journal (fixed SQL default) | atomic: all gates flip together | crash-atomic | after K prepare syncs + decision sync |
| buffered-journal (power-safe / filesystem) | atomic | crash-atomic | same; durability precedes visibility for multi-collection commits, stronger than the lane's single-collection contract |
| buffered-volatile (both) | — | refused: `ErrDatabaseTransactionUnsupportedLane` | — |
| async-COW, sync chain-fence | — | refused: same typed error | — |
| Memory profile (heap store) | atomic: all writers held, all pointers flip | no crash dimension | in-process |

The facade Buffered profile maps to buffered-visible publication, so native
multi-collection transactions on that profile are a typed refusal in this
pass.

> A multi-collection COMMIT has exactly one unknown-outcome window: the
> decision record's sync. If that sync reports an error, COMMIT returns
> `ErrCommitOutcomeUnknown`, every collection handle under the catalog
> refuses further writes with the sticky persistence failure, and only
> closing and reopening the database resolves the outcome. The unknown
> outcome is atomic: reopen reveals either every participating collection's
> writes or none of them. There is no crash, error, or recovery in which one
> participant's writes survive without the others'.

Catalog-scope poison is intentional: a half-poisoned catalog could otherwise
commit collection B after collection A's outcome went unknown. After reopen,
probe any one participant key of the transaction — its presence decides the
whole transaction. Mint operation identities outside the retry loop.

## Recovery journal

On the ordered primary graph the [recovery-only redo journal][journal] backs
`DurabilitySync`: it is minted unconditionally and is how that lane
acknowledges — one bounded append plus one sync before publication — not an
option. With `Options.RecoveryJournal`, every mutation receives the
synchronous-style durable record and the sibling is created eagerly. Without
the option, a fresh or bulk-built buffered-visible primary has no sibling; its
first valid mutation creates one synchronously, and its first physical
`Flush`/`Close` roots the identity before any journal-only acknowledgement is
allowed. Later `Flush` calls use one bounded batch to protect an eligible
update/delete interval. No journal creation or compaction runs in the
background.
Readers never consult the journal; recovery alone replays records after the
newest valid root. The root records journal identity, and recovery pairs the
files by identity, page geometry, and base-generation epoch, failing closed on
a missing or mismatched sibling. A synchronous store whose root names no
journal (created async-visible, reopened sync) acknowledges through the chain
fence instead.

The journal has two independently checksummed 512-byte headers and a record
region sized once before its identity can be published. Later appends are
positional writes into that fixed region and never extend the file. The
ordinary buffered format-v1 delta lane currently uses a fully sized 2.5 MiB
record region with a foreground policy reserve of up to 512 KiB for the next
carried suffix, leaving a 2 MiB qualified current append window. Exact batch
fit remains authoritative: a wider current or future suffix takes the bounded
physical checkpoint fallback. Linux normally allocates the complete region
with `fallocate`; systems without that primitive set its full size once with
truncate. Per-mutation journals retain their separate option-derived bounded
capacity.

Journal format v0 admits legacy put/delete records and batches and remains
writable as v0 after reopen. Format v1 is restricted to the ordinary buffered
delta lane and additionally admits compact scalar patches inside batches. The
patch names the old scalar span and carries the new canonical integer, boolean,
or null spelling plus a checksum of the complete expected canonical document.
Replay reconstructs that whole result and fails closed unless its checksum
matches. Catalog-owned collections mint journals at format word
`RecoveryJournalFormatConditional` (numeric 2), which admits kinds 1, 2, 3,
and 5 (conditional batch) and rejects scalar-patch kind 4. Kind 5 carries the
same batch grammar as kind 3, prefixed by a 32-byte conditional header binding
the record to the database decision log. Current code rejects unknown versions,
scalar-patch entries in v0, a v1 journal reopened under the per-mutation or
synchronous lane, and kind 5 inside a legacy or scalar-patch journal; older
binaries reject a nonzero format word they do not know before decoding.

A v1 batch contains one entry for every consecutive logical generation ending
at its record generation. If replay is forced to checkpoint a prefix under
bounded staging pressure and is interrupted again, the next open derives the
covered prefix from the selected physical root and resumes at the first
uncovered entry. It does not reapply a length-changing scalar patch. V0 batches
keep their original atomic grammar—all entries share one generation and replay
starts at entry zero. Conditional batches (kind 5) apply only when the decision
log reports the transaction committed and names this collection among its
participants; undecided same-epoch records are presumed aborted and skipped.
See [the design][journal] and [format.md](format.md) for the complete framing,
failure, and group-commit details.

[journal]: design/recovery-journal.md
