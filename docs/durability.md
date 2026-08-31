# Durability and recovery

> [!CAUTION]
> VibeDB is unreleased development software. Any commit may break APIs, disk
> formats, or wire behavior. Build and operate one exact tested commit only.
> Do not entrust irreplaceable data to VibeDB.

Durability asks which failures may lose a successful operation. VibeDB does not
treat visibility, acknowledgement, and persistence as synonyms.

## Root profiles

The root `vibedb` package is the application API. Its zero value and default
profile are `Durable`.

| Profile | When readers see a mutation | What success means | Persistence action |
| --- | --- | --- | --- |
| `Durable` | After its recovery record is power-safe | The mutation is recoverable before visibility | `Flush` folds/recycles state as needed |
| `Buffered` | After bounded in-memory admission | The visible mutation can still be lost | Successful `Flush` or `Close` makes the included cut recoverable |
| `Memory` | After heap publication | Process memory only | `Flush` is a no-op |

`Open(path)` uses a directory for `Durable` and `Buffered`; `Memory` ignores the
path. The facade rejects low-level options that conflict with the profile.

## Four different boundaries

Use precise terms when reasoning about a write:

| Boundary | Meaning |
| --- | --- |
| Visible | A new reader can observe the generation |
| Acknowledged | The mutation call returned success |
| Recoverable | Reopen can reconstruct the generation after the selected failure |
| Physically rooted | The primary file's selected root directly names the generation |

These boundaries can differ. A buffered `Flush` can make a generation
recoverable through one journal delta without folding all changed primary pages;
`DurableGeneration` then advances before the physical primary root does.

## Synchronous durable writes

The normal `DurabilitySync` primary path is journal-before-visibility:

1. append a checksummed recovery record;
2. synchronize the journal with the power-safe barrier;
3. apply and publish the mutation; and
4. acknowledge success.

No reader observes the generation before its redo is recoverable. A later
checkpoint folds it into the primary graph and recycles journal space.

A file created as `AsyncVisible` and reopened as `DurabilitySync` is a low-level
exception: it has no journal identity and uses a root-fence chain. Reads are
withheld or fail across the unsafe interval. Facade durable files use journals.

## Buffered writes

Ordinary `Buffered` acknowledgement leaves row state volatile, but may perform
I/O. A lazy collection may create its primary, and its first valid mutation may
mint, preallocate, and sync a `.rjournal` for later checkpoints. That metadata
work does not make the acknowledged mutation recoverable.

A process or machine failure before a successful `Flush` or `Close` may lose
acknowledged buffered mutations. Recovery returns the last complete recoverable
cut, never a promise to retain the latest visible cut.

Low-level `DurabilityBufferedVisible` also has an opt-in
`Options.RecoveryJournal` lane. It publishes resident state and requires the
redo record to synchronize before the mutation call returns success. Visibility
may precede that synchronization, so describe this as durability-before-success,
not durability-before-visibility. The root facade does not expose this override.

## Asynchronous visibility

`DurabilityAsyncVisible` acknowledges after bounded queue admission while a
background committer persists generations. `Flush` waits for the sampled
reader-visible generation and completes its physical durability work.

After an asynchronous persistence failure, the handle can roll back to the last
durable state or fail reads closed. Close and reopen before deciding to retry.

## Low-level mode matrix

These modes belong to `store/durable`, not to the root product profiles.

| Setting | Zero value | Other choices |
| --- | --- | --- |
| `Backend` | `BackendAuto` | `BackendPortable`, Linux `BackendIOUring` |
| `ReadMode` | `ReadBuffered` | `ReadDirectTry`, `ReadDirectRequire` |
| `WriteMode` | `WriteBuffered` | `WriteDirectTry`, `WriteDirectRequire` |
| `Durability` | `DurabilitySync` | `DurabilityAsyncVisible`, `DurabilityBufferedVisible` |
| `CheckpointStrength` | `CheckpointPowerSafe` | `CheckpointFilesystem` |

`Try` modes may fall back; inspect `Collection.Stats()` for the selected path.
`Require` modes fail construction if unavailable.

`CheckpointFilesystem` is accepted only with buffered visibility, the portable
backend, and buffered writes. It uses the ordinary filesystem sync class. On
Darwin and storage stacks with volatile caches it does not promise sudden-power
survival. It never weakens synchronous or asynchronous durability.

## Flush and close

`Collection.Flush` waits until the current reader-visible generation is
recoverable. Depending on the lane it can:

- append and synchronize a complete buffered delta;
- fold and recycle a synchronous journal;
- wait for the asynchronous committer; or
- publish a complete physical checkpoint.

`Database.Flush` walks collections one at a time. It attempts every collection
and returns the first mapped error. It is not a coherent database-wide
persistence cut: a concurrent writer can publish after its collection was
flushed.

`Close` stops new admission, drains publishers, establishes the final
persistence boundary, and releases engine-owned resources. A direct durable
collection does not close the caller-owned primary descriptor.

Close is repeatable, but retry may perform real work. Snapshots, pinned pages, or
unlock failure can leave teardown incomplete. Check `CloseCompleted`, release
the blocker, and retry. A sticky error may coexist with completed teardown.

## Recovery journal

Each journal has an identity cross-bound to the primary root. The journal uses
two alternating headers and an aligned, checksummed record region. Recovery
accepts only a contiguous valid sequence.

If the selected root requires a journal, a missing file or identity mismatch
fails closed. Treat a primary and its `.rjournal` sibling as one storage object:
move, restore, and back them up together.

An append or sync error can have an unknown outcome: a complete record may be
stable despite the error. The writer is poisoned until close and reopen; do not
retry different data against it.

Recovery replays valid redo, checkpoints the recovered generation, and only
then recycles the record prefix. A second crash during that fold can replay the
same intact record again.

## Batch atomicity

`durable.Collection.Update` publishes rows and exact-index postings as one
logical failure-atomic publication. A rejected sibling never exposes a partial
logical batch.

Routing preparation can first publish a content-equivalent topology generation.
A later validation or durability error can therefore leave the rows and
postings unchanged while `Generation may advance`. Compare logical state, not
only generation numbers, after a failed batch.

## Multi-collection recovery

Two or more dirty durable collections use conditional participant records and
the database decision log `txn.vtm`:

1. append every participant prepare;
2. synchronize all participant journals;
3. append and synchronize the sole commit decision; and
4. publish every participant under one local snapshot-gate cut.

For `K` participants, commit uses `K+1` synchronization operations. Recovery
must open the complete catalog. A valid decision rolls every participant
forward; no decision means presumed abort. Missing `txn.vtm`, a missing required
participant, or a standalone collection with unresolved conditional records
fails closed.

An ambiguous decision append or sync returns `ErrCommitOutcomeUnknown` and
poisons the catalog. Reopening the complete database resolves an all-or-none
state. See [Transactions](transactions.md) for profile support and limits.

## Snapshots and reclamation

A durable snapshot pins one generation and owns mutable scratch. It is
single-consumer and must be closed. Holding a snapshot across sustained writes
pins retired extents; once the bounded retirement table fills, a writer returns
a capacity error without publishing its mutation. Close the snapshot and retry.

The durable package currently exports no dedicated retirement-capacity
sentinel. Do not document one. Active snapshots can also make collection close,
database close, and collection drop retryable.

## Platform and failure limits

“Power-safe” means the strongest implemented platform barrier; Darwin uses the
full-sync class where available. It cannot prove every filesystem, controller,
firmware, cache, hypervisor, or power-loss condition.

The test suite injects write, sync, torn-record, and torn-root failures. Those
tests establish behavior for the modeled cuts, not a hardware certification or
service-level guarantee.

## Operator rules

- Stop writers and close the database before copying the complete directory.
- Include every primary, `.rjournal`, `txn.vtm`, and catalog-side file.
- Do not manipulate an open file or directory behind the writer lease.
- Treat persistence errors and unknown outcomes as a close-and-reopen boundary.
- Do not assume reads remain available after a persistence failure.
- Use offline verification against a quiescent file or copy.
- Never rely on a development image across a different commit.

The public API defines no safe live raw-file backup procedure.

## Source map

- Facade profiles: `vibedb.go`
- Durable modes and validation: `store/durable/store_file_options.go`
- Visibility and poison state: `store/durable/store_file_durability.go`
- Flush and close: `store/durable/store_file_lifecycle.go`
- Recovery journal: `store/durable/store_file_journal.go`
- Multi-collection protocol: `store/durable/store_database_txn.go`
- Recovery crash tests: `store/durable/store_file_journal_crash_test.go`
- Buffered crash tests: `store/durable/store_file_buffered_test.go`
