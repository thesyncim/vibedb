# Durability and recovery

Durability is the point at which acknowledged data can survive the selected
failure model. Visibility is the point at which readers can observe that data.
VibeDB makes both points explicit.

## Facade profiles

| Profile | Acknowledgement and visibility | Persistence action |
| --- | --- | --- |
| `vibedb.Durable` | A successful mutation is power-safe before it becomes visible. | `Flush` waits for or folds durable state as needed. `Close` finishes maintenance. |
| `vibedb.Buffered` | The mutation becomes visible from bounded memory and returns before device I/O. | A successful `Flush` or successful `Close` checkpoints the included visible generation. |
| `vibedb.Memory` | The mutation exists only in process memory. | `Flush` is a no-op. `Close` releases memory. |

The zero value and default are `Durable`.

The facade prevents low-level options from changing the selected profile. For
example, it rejects an explicit recovery-journal override.

## Low-level durability lanes

`store/durable.Options` has three mutation lanes:

- `DurabilitySync` is the zero value. It uses journal-before-visibility on a
  current primary layout.
- `DurabilityAsyncVisible` publishes after bounded queue admission. Persistence
  continues in the background.
- `DurabilityBufferedVisible` publishes from bounded memory without a device
  write. Flush or close creates the persistence boundary.

`CheckpointPowerSafe` is the zero-value checkpoint strength.
`CheckpointFilesystem` is weaker on storage stacks with volatile drive caches.
It is accepted only with buffered-visible, portable, buffered-write operation.

## Synchronous mutation order

The current synchronous primary path uses this order:

1. Append the redo record.
2. Sync the redo record with the power-safe barrier.
3. Apply and publish the visible state.

A successful mutation is durable before a reader can observe it.

An older development file that has no journal can reopen on the synchronous
chain-fence path. The open format and selected root determine that path.

## Buffered checkpoint

Buffered-visible mutation does not issue a device write before success. A
process or machine failure before a completed checkpoint can lose acknowledged
mutations.

`Flush` makes the current visible cut crash-safe. `Close` also performs the
final checkpoint. A close error can be retryable while readers or resources
remain active. Check `CloseCompleted` before you assume that teardown finished.

## Recovery journal

The recovery journal has its own identity. The primary root cross-binds store
ID and journal ID.

If the selected root requires a journal, open fails when the journal is missing
or has the wrong identity.

A journal append or sync error poisons the writer. Further mutation and
checkpoint work fails until close and reopen. Reopen replays valid redo and
resolves the root generation.

A journal append or sync error can have an unknown outcome because the
complete record might have reached stable storage despite the returned error.
Root and transaction-decision fences have equivalent ambiguity windows. Close
and reopen before you decide whether to retry.

An unresolved conditional transaction record makes a standalone collection
return `ErrCollectionInDoubt`. Open the complete database directory so the
decision log and all participants can be reconciled.

## Root publication

The file has two alternate roots. The selected root is the canonical physical
checkpoint. A journal-backed `Sync` commit can acknowledge after durable redo,
before it publishes another physical root. Recovery validates both roots,
selects a valid checkpoint, and replays valid later redo. Full-generation,
chain-fence, and checkpoint paths publish an alternate root.

A failure after root publication can make the commit outcome unknown. Do not
retry with different data. Close and reopen, then inspect the recovered state.

Recovery validates identity, generation, page kind, extent bounds, checksums,
and catalog digests. A conflict or corruption fails closed.

## Multi-collection transactions

One dirty durable collection uses the ordinary collection path. Two or more
dirty collections use conditional participant records and `txn.vtm`.

The protocol is:

1. Append a conditional prepare to each participant journal.
2. Sync all participant journals.
3. Append and sync one decision in `txn.vtm`.
4. Publish all participants while holding their snapshot gates.

The decision record is the only commit point. A transaction with `K`
collections needs `K` participant syncs and one decision-log sync.

When a valid decision log has no matching decision, recovery presumes abort. A
committed decision rolls every participant forward. If conditional participant
records exist but `txn.vtm` or a required participant is missing, recovery
fails closed.

Low-level `durable.UpdateCollections` requires explicit nonzero `TxnLimits`
for two or more dirty collections. `durable.Database.Update` supplies defaults.
One dirty collection uses ordinary `Collection.Update`, bypasses cross-
collection limits, and does not use `txn.vtm`. A transaction marker supports
at most 64 physical participants.

An ambiguous decision append or sync can return `ErrCommitOutcomeUnknown` and
poison the catalog. Reopen resolves an all-or-none state.

Synchronous journal and journal-backed buffered lanes support this primitive.
Volatile buffered, async, and chain-fence shapes fail closed when they cannot
provide the required protocol.

## Snapshots and reclamation

A durable snapshot pins retired extents. A long-lived snapshot can fill the
retired-extent table and return a retirement-capacity error to a writer. The
durable package does not currently export this as a public sentinel.

Close snapshots promptly. The engine attempts checkpoint and reclamation once
before it refuses a mutation. A refused mutation is not published.

Active snapshots can also make close retryable.

## Platform barriers

The power-safe checkpoint uses the strongest implemented platform primitive.
Darwin uses the full-sync class where available. The ordinary filesystem mode
does not promise sudden-power survival through a volatile drive cache.

Direct I/O and `io_uring` modes are Linux-specific. A `Try` mode can fall back,
and statistics report the result. A `Require` mode fails when the mode is not
available.

No software barrier can prove behavior for every filesystem, controller,
device cache, or power-loss condition. The test suite uses deterministic
injected write, sync, and torn-image failures.

## Operator rules

- Call `Flush` when buffered data must cross its persistence boundary.
- Treat a primary and its `.rjournal` sibling as one storage pair.
- Treat `txn.vtm` as part of a multi-collection database.
- Stop and close the database before a raw backup of the complete directory.
- Treat persistence errors and unknown outcomes as a close-and-reopen boundary.
- Do not assume that all reads stay available after a persistence failure.
- Use the [offline verifier](operations/verification.md) on quiescent data.

The public API does not define a safe live raw-file backup procedure.

## Implementation references

- `store/durable/store_file_durability.go`
- `store/durable/store_file_lifecycle.go`
- `store/durable/store_file_open.go`
- `internal/storeio/recovery_journal.go`
- `store/durable/store_database_txn.go`
- `internal/storeio/txn_marker.go`
