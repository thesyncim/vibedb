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

## Crash windows

### Buffered-visible

- Before `Flush`: the durable file names the previous checkpoint. All later
  acknowledged mutations may disappear after process or machine failure.
- An ordinary class-5 update/delete cut can complete `Flush` by appending the
  entire consecutive generation interval as one recovery-journal batch and
  syncing it once. Before that sync recovery ignores the tail; after it,
  recovery replays the exact cut over the previous physical root.
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

## Recovery journal

On the ordered primary graph the [recovery-only redo journal][journal] backs
`DurabilitySync`: it is minted unconditionally and is how that lane
acknowledges — one bounded append plus one sync before publication — not an
option. Every buffered-visible primary store also owns the paired journal.
Without `Options.RecoveryJournal`, mutation acknowledgement stays volatile and
`Flush` uses one bounded batch to protect an eligible update/delete interval.
With the option, every mutation receives the synchronous-style durable record.
Readers never consult the journal; recovery alone replays records after the
newest valid root. The root records journal identity, and recovery pairs the
files by identity, page geometry, and base-generation epoch, failing closed on
a missing or mismatched sibling. A synchronous store whose root names no
journal (created async-visible, reopened sync) acknowledges through the chain
fence instead. See [the design][journal] for record and group-commit details.

[journal]: design/recovery-journal.md
