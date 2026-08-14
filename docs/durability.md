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

## Sealed sidecar capacity

The recovery journal and transaction decision log can independently carry an
immutable physical-allocation certificate. For a synchronous collection,
`Options.SealedRecoveryJournalBytes = N` requests an exact, sector-aligned
record-region capacity of `N` bytes; the two 512-byte headers make the complete
file exactly `1024 + N` bytes. Option normalization also requires the largest
admitted conditional batch, including its envelope and per-entry overhead, to
fit that region. The option is rejected for other durability lanes. On reopen,
it is also rejected when the selected root has no journal identity.

`TxnLogOptions{Capacity: N, SealedCapacity: true}` applies the same exact
contract to `txn.vtm`. A sealed decision log requires a non-zero,
sector-aligned `N`; it is minted lazily with the log and recycles within that
fixed region. A profile-qualified durable open requires the caller's exact
profile to match both the persisted seal bit and persisted capacity before any
record scan. Its header `Format` field, like the recovery journal's, is solely a
numeric-zero corruption sentinel. The paired recovery-journal path additionally
checks store id, journal id, page size, and recovery epoch before allocation
proof or scanning.
Supplying ordinary zero options for an existing sealed sidecar is an error, as
is supplying a sealed profile for an ordinary sidecar. The lower-level generic
`OpenRecoveryJournal` can self-describe and reprove a persisted seal, but its
capacity-immutable handle is not an externally qualified durable profile.

The recovery-journal hard ceiling is 16 MiB plus 17,408 bytes, enough for the
current replicated SQL profile's 16 MiB command budget, 64 maximum-size keys,
conditional framing, trailer, and sector padding. The transaction decision log
keeps a separate 16 MiB ceiling. These are allocation and hostile-header clamps;
arbitrary larger durable collection options remain unsealable and fail option
normalization.

`NewTxnLog` accepts `TxnLogOptions` only for a fresh catalog and refuses any
pre-existing `txn.vtm`. Existing decision logs reopen only through
`OpenCollectionsWithTransactions`, which requires the complete live collection
catalog before replay or recycle. `durable.Database` does not yet thread sealed
transaction-log options, so this checkpoint makes no sealed decision-log
promise for that wrapper.

Sealed create requires an empty regular file. On mutable open, an exact regular
EOF of `1024 + N` is required before allocation proof begins. Linux then runs
mode-zero `fallocate` across the complete prefix and
`FALLOC_FL_UNSHARE_RANGE` across that same prefix, synchronizes the result, and
checks exact EOF again before recovery scans records. Only
`FALLOC_FL_UNSHARE_RANGE` returning `EOPNOTSUPP` on a descriptor proved by
`fstatfs` to be ext4 is accepted. Other errors and filesystems fail closed;
sealed sidecars have no truncate fallback and are unsupported on platforms
without this proof. A sealed recovery journal is immutable and rejects
`GrowCapacity`; pressure must be handled within the fixed region through the
existing checkpoint/recycle path.

Offline inspection is deliberately weaker. The read-only inspection APIs
validate checksummed headers and exact apparent EOF, then scan without issuing
allocation, unshare, repair, or synchronization calls. That can diagnose a
sealed sidecar, but it does not qualify the file for a mutable recovery or a
serving process.

The certificate requires exclusive allocation ownership. No caller or external
process may truncate, extend, punch holes, reflink-clone, or otherwise alter a
sealed sidecar outside its owner. It covers only the recovery-journal or
decision-log file named by the option. It does not create a SQL capacity
identity, reserve main-file or Raft log/snapshot/range space, or certify a
node or range to serve traffic.

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
  entire consecutive generation interval as one kind-5 `DeltaBatch` and syncing
  it once. Every entry carries a complete logical put or delete. Before that
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
append-or-sync-is-terminal, die-don't-retry rule. A complete checksummed record
can reach the page cache despite a short/error result, and a failed sync can
still persist it, so both report `ErrCommitOutcomeUnknown` for the attempt and
poison the failing collection. With no matching decision, reopen resolves the
conditional as abort. An unexpected decision append or sync failure widens the
unknown-outcome poison to every collection under the catalog (see below).

## Database transactions

A write that touches exactly one collection still takes today's
`Collection.Update` path: one journal record, one sync, byte-identical to the
single-collection contract. A write that dirties K ≥ 2 collections under one
catalog commits through the database transaction protocol:

1. Validate bounds and per-participant conflicts; refuse before staging.
2. Stage every participant under writers held in the process-global snapshot
   order. Any failure unwinds all staged work; nothing is journaled.
3. Prepare: append and sync one kind-4 `ConditionalBatch` record in each
   participant's own journal. An append or sync failure poisons that
   collection and reports unknown for the attempt; reopen resolves it as abort
   if no exact decision exists.
4. Decide: append the decision record to the database decision log
   (`txn.vtm`) and cross one power-safe sync. **That sync is the sole atomic
   commit point.** Capacity and validation are definite preflight refusals;
   unexpected append or sync failures are the unknown-outcome window below.
5. Publish: flip every participant's publication gate in the same global
   order so no multi-collection read cut observes a partial set.

Recovery resolves every retained conditional, including one whose generation
the selected root appears to cover. A committed decision must bind the exact
`(StoreID, JournalID, PreparedGeneration)` tuple, with the prepared generation
equal to the conditional record generation. The decision remains available
until every participant has successfully completed its resolved fold and
journal recycle (or has a durable retirement); root coverage alone does not
release it.

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

> A multi-collection COMMIT has exactly one window where commit versus abort is
> unknown: an unexpected decision-record append or sync failure. COMMIT returns
> `ErrCommitOutcomeUnknown`, every collection handle under the catalog refuses
> further writes with the sticky persistence failure, and only closing and
> reopening the database resolves the outcome. Prepare append/sync failures use
> the same error classification because their journal side effect may exist,
> but without an exact decision recovery deterministically aborts them. A
> decision-stage unknown outcome is atomic: reopen reveals either every
> participating collection's writes or none of them. There is no crash, error,
> or recovery in which one participant's writes survive without the others'.

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

The journal has two independently checksummed 512-byte headers and a bounded
record region. The header's `Format` field is a corruption sentinel that must
contain numeric `0`; every other value is rejected. The initial region is
allocated before its identity can be published, and positional record appends
never extend it. An ordinary unsealed acknowledgement journal may explicitly
grow within the hard ceiling before an oversized append's point of no return;
the extension is preallocated before the alternate header publishes it. The
selector ignores only uninitialized or checksum-invalid torn slots. Any
checksum-authenticated invalid header is hard corruption, so an older
capacity/base or decision-log epoch can never hide acknowledged records. The
ordinary buffered delta lane currently selects a 2.5 MiB region with a policy
reserve of up to 512 KiB for the next carried suffix, leaving a 2 MiB qualified
current append window. Exact fit remains authoritative: a wider current or
future suffix takes the bounded physical checkpoint fallback. Per-mutation
journals use their separate option-derived bounded capacity. Sealed sidecars
instead follow the strict immutable-capacity proof above, reject growth, and
never use truncate as an allocation proof.

The one current record grammar assigns kinds 1 `Put`, 2 `Delete`, 3 `Batch`,
4 `ConditionalBatch`, and 5 `DeltaBatch`. Put, Delete, and Batch are the atomic
family: each advances one generation, and Batch contains one atomic ordered set
of put/delete entries. ConditionalBatch is the same one-generation atomic
family with a decision binding prefix (`MarkerID`, `MarkerEpoch`, `TxnID`).
DeltaBatch contains one complete logical put/delete entry per consecutive
generation ending at the record generation. One unrecycled window is either
atomic or delta, never mixed; appends refuse a family change and recovery treats
one as corruption.

If replay of a delta window checkpoints a prefix under bounded staging pressure
and is interrupted again, the next open derives the covered prefix from the
selected physical root and resumes at the first uncovered entry. Atomic and
conditional batches remain one-generation groups. Every retained conditional
is resolved even when the selected root appears to cover its generation. A
commit requires exact marker identity and epoch plus the participant tuple
`(StoreID, JournalID, PreparedGeneration)` matching the record; a current-epoch
transaction with no decision is presumed aborted, while binding or epoch
disagreement fails closed. Standalone open cannot resolve a retained
conditional and returns the typed in-doubt error. The final resolved fold must
succeed before journal recycle can consume the window, so the decision log is
retained until that boundary. See [the design][journal] and
[format.md](format.md) for complete framing, failure, and group-commit details.

[journal]: design/recovery-journal.md
