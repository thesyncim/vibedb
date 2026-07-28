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
| buffered-visible | `DurabilityBufferedVisible` | success after bounded canonical COW staging; immediately reader-visible | none on ordinary admission | process or machine failure may lose every acknowledged generation after the last successful `Flush` |
| async-stable-in-flight | `DurabilityAsyncVisible` | success after the bounded committer accepts the generation; immediately reader-visible | the background worker continuously writes ordered COW generations and roots | failure may lose acknowledged generations newer than `DurableGeneration`; `Flush` closes the window |
| synchronous, power-safe | `DurabilitySync` (zero value) | success and visibility wait for data and alternate-root barriers | strongest supported platform sequence | after success, recovery selects the complete new generation; before success, outcome may require reopen |

All modes are linearizable inside the live process. “Buffered” weakens crash
survival, not read ordering. Bounded staging or retirement pressure may force a
buffered checkpoint before the requested cadence; `Stats.AutomaticCheckpoints`
and `Stats.RetirementPressureCheckpoints` expose those events.

`DurableGeneration` reports the newest generation whose final root boundary
completed. `Flush` waits for or checkpoints the current reader-visible cut.
`Close` rejects new work, drains or checkpoints accepted work, and releases
resources; it does not close the caller's file.

## Checkpoint strengths

`CheckpointStrength` applies only to buffered-visible `Flush` and `Close`. It
does not weaken `DurabilitySync` or `DurabilityAsyncVisible`.

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

The two inline root slots alternate. Recovery validates both and selects the
newest root whose complete reachable graph is valid, with fallback to the
previous generation. The root write is the commit point; a data page that is
not named by a valid root is not visible after recovery.

Qualified canonical materialization adds a durable undo capsule before changing
an existing extent. Its exact protocol and recovery cases are in
[the format](format.md#materialization-journal) and
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

### Synchronous

- Before the data barrier: the call has not succeeded; recovery selects a
  complete root.
- Between data and final root barriers: recovery normally selects the old root.
- Once the alternate root may have reached storage but the final barrier
  reports an error, the result is `ErrCommitOutcomeUnknown`. Reopen determines
  which root won before the application retries.
- After success: the new generation is visible and power-safe under the
  platform assumptions above.

## Persistence failures

Any write or synchronization failure poisons the writer. Later mutations,
checkpoints, and close-drain work return the sticky `PersistenceError`.
`errors.Is` may match `ErrCommitOutcomeUnknown` when only recovery can decide
the last committed generation. Close the collection and reopen the file; do
not retry the logical mutation against the poisoned live handle.

## Recovery journal

The [recovery-only redo journal](design/recovery-journal.md) is the current
ordered-primary `DurabilitySync` acknowledgement path and an opt-in for
ordered-primary buffered-visible collections. It reduces acknowledgement to
one bounded append and one sync while keeping the journal out of every read
path. Comparative performance effects in the design remain projected until
their qualification runs.
