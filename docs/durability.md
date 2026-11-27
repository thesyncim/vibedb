# Durability and recovery

[Documentation](README.md) / [Design](design/README.md) · [Stability](status.md)

VibeDB separates four write boundaries: **visible** (readers see the
generation), **acknowledged** (call returned), **recoverable** (reopen
reconstructs it), and **physically rooted** (primary root points to it).

## Facade profiles

| Profile | Visible when | Success guarantees | Persistence action |
| --- | --- | --- | --- |
| `Durable` (default) | After power-safe journal sync | Recoverable before visibility | `Flush` folds/recycles as needed |
| `Buffered` | After bounded memory admission | Volatile until `Flush`/`Close` | `Flush` or `Close` persists cut |
| `Memory` | After heap publication | Process memory only | `Flush` is a no-op |

## Write lanes and recovery journals

- **Synchronous (`DurabilitySync`):** Append checksummed redo record to
  `.rjournal` → power-safe sync → apply & publish → acknowledge. A later
  checkpoint folds pages into the primary `.vjc` and recycles the journal.
- **Buffered (`DurabilityBufferedVisible`):** Volatile until `Flush`/`Close`
  (lazy file/journal setup may still do metadata I/O). Low-level
  `Options.RecoveryJournal` upgrades acknowledgement to sync redo before
  returning (durability-before-success).
- **Async (`DurabilityAsyncVisible`):** Low-level bounded queue with background
  committer; `Flush` waits for the sampled generation.
- **Recovery journal (`.rjournal`):** Cross-bound by identity to its primary
  root with alternating headers and checksummed records. Missing journal or
  identity mismatch fails closed. Recovery replays contiguous valid records,
  checkpoints, then recycles. Always keep primary + `.rjournal` + `txn.vtm`
  together.
- **Fixed-capacity RF3 sidecars:** RF3 journals and `txn.vtm` use portable
  fixed-capacity allocation on macOS and Linux (validated at reopen without
  rewriting). Strict private physical allocation remains opt-in on Linux
  ([strict allocation](../internal/storeio/strict_allocation_linux.go)).

## Low-level `store/durable` modes

| Setting | Default | Alternatives |
| --- | --- | --- |
| `Backend` | `BackendAuto` | `BackendPortable`, Linux `BackendIOUring` |
| `ReadMode` / `WriteMode` | `ReadBuffered` / `WriteBuffered` | `*DirectTry`, `*DirectRequire` |
| `Durability` | `DurabilitySync` | `DurabilityAsyncVisible`, `DurabilityBufferedVisible` |
| `CheckpointStrength` | `CheckpointPowerSafe` | `CheckpointFilesystem` (portable buffered-only) |

## Flush, close, and crash boundaries

- `Collection.Flush` persists the current visible generation. `Database.Flush`
  walks collections sequentially (not a cross-collection cut).
- `Close` stops admission, persists as required, and releases locks. Check
  `CloseCompleted(err)` to distinguish retryable teardown (active snapshots/tx)
  from a terminal close with a sticky error.
- **Poison & unknown outcomes:** An append/sync error or
  `ErrCommitOutcomeUnknown` poisons writers. Stop writing, close, reopen the
  complete directory, and reconcile by application identity.
- **Long-lived snapshots:** Holding durable snapshots pins retired extents
  (up to `MaxRetiredExtents`, default 65,536) and causes bounded write
  refusals until closed.
- **Platform barriers:** "Power-safe" uses the strongest barrier exposed by the
  OS (including Darwin full-sync); it cannot certify disk firmware, volatile
  controller caches, or hypervisors.

## Operator rules

- Close the database before copying a directory; never copy or modify live
  files behind the writer lock.
- Run offline `Verify`/`Salvage` only on a quiescent file or consistent copy.
- Replicated RF3 nodes add a node descriptor catalog and segmented shared Raft
  log; see [replication](design/replication.md) and [on-disk format](format.md#recovery-unit).

## Source map

- Profiles & modes: [vibedb.go](../vibedb.go), [store/durable/store_file_options.go](../store/durable/store_file_options.go), [store_file_durability.go](../store/durable/store_file_durability.go)
- Journal, lifecycle & crash tests: [store/durable/store_file_journal.go](../store/durable/store_file_journal.go), [store_file_lifecycle.go](../store/durable/store_file_lifecycle.go), [store_file_journal_crash_test.go](../store/durable/store_file_journal_crash_test.go)
