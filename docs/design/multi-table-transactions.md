# Multi-table transactions

**Status:** implemented on `main`. The named crash, conformance, SQL-driver,
pgwire, and verifier tests in this document are the executable acceptance
surface. This document remains the design authority for the landed behavior.

## Purpose

This work removed three former limitations and defines the semantics that
replaced them. The bullets below record the preimplementation baseline:

- a SQL transaction may read several tables but writes exactly one
  (`sql/driver/tx.go:364-368`; README "Important limitations";
  `docs/architecture.md` "matching the largest atomic publication the storage
  layer actually has");
- the store layer has no cross-collection atomic transaction
  (`store/durable/store_database.go:95-111`,
  `store/durable/store_database_snapshot.go` DatabaseSnapshot contract,
  `docs/store.md` "Current product boundaries");
- savepoints are refused (`sql/unsupported.go:56-57`, `pgwire/command.go`).

It also gives the native Go API its first transaction surface. Today the
facade has only point calls (`vibedb.go`: Put, Delete, Get, Range) and the
engine's largest atomic publication unit is one `durable.Collection.Update`
over one collection file (`store/durable/store_file_batch.go`).

DDL remains non-transactional. Its commit mechanism is a catalog-file rename
plus directory fsync, not a collection generation flip; bundling the two needs
a cross-mechanism coordinator that is out of scope. `ErrDDLInTransaction`
stands. Routing catalog mutations through the transaction decision log is the
natural adjacent follow-up and is flagged, not folded in.

## Current state this design builds on

Verified against the tree, because each of these is load-bearing:

- One durable file per collection, `c-<hex>.vjc`, with a paired
  `<file>.rjournal` recovery journal; the pairing is by identity, geometry,
  and epoch, failing closed on mismatch
  (`store/durable/store_database.go:86-116`,
  `internal/storeio/recovery_journal.go`).
- A batch journal record carries a complete, CRC-sealed logical redo group.
  The current kinds are Put=1, Delete=2, Batch=3, ConditionalBatch=4, and
  DeltaBatch=5. Put/Delete/Batch/ConditionalBatch are the one-generation atomic
  family; DeltaBatch is the consecutive-generation family, and one unrecycled
  window never mixes the two (`docs/design/recovery-journal.md`;
  `internal/storeio/recovery_journal.go`).
- The multi-collection read cut acquires every collection writer, then every
  publication gate, in one process-global order, and holds every gate at once
  while leasing; the deadlock argument is written down
  (`store/durable/store_database_snapshot.go`, `store_database.go:44-56`).
  A cut is consistency at capture, not write atomicity and not crash
  survival.
- The SQL transaction is already shaped for N tables: per-table leased
  snapshots, bounded overlays, a bounded first-committer-wins conflict clock
  (`sql/driver/tx.go`, `sql/driver/tx_conflict.go`). Exactly two things are
  one-table: the fence at `tx.go:364-368` and the `Commit` body, which drives
  one `Collection.Update`.
- The SQL driver does not hold a `durable.Database`; it opens per-table files
  directly in its own `<catalog>.tables/` directory and uses the caller-owned
  `SnapshotCollections` form for read cuts (`sql/driver/catalog.go`). The
  write primitive must therefore have a caller-owned form too.
- pgwire already maps `driver.ErrTransactionConflict` to SQLSTATE 40001 and
  `ErrCommitOutcomeUnknown` to 40003 (`pgwire/pgerror.go:75-76`). What is
  missing there is capability, not error plumbing.
- `validateWriteSet` checks key existence against a fresh snapshot; value
  conflicts are caught by the bounded conflict clock. Both generalize
  per-table without structural change.

## Design decision: the commit mechanism

Three shapes were evaluated in depth; the decision record is deliberate
because two of them are defensible.

**Chosen: two-phase commit over the existing per-collection recovery
journals, decided by one database-scoped marker record.** Each participant
durably prepares a kind-4 *conditional* batch record in its own
journal; one decision record in a database-scoped transaction log is then
appended and synced — that single sync is the sole atomic commit point; then
every participant publishes under all publication gates held at once.

**Rejected for this pass: one shared database-scoped redo journal** (the
RocksDB column-family shape: the commit record carries every participant's
redo; one append, one sync). It is the better steady-state machine — one sync
per commit regardless of participant count, no prepared-undecided states —
and it is the named perf follow-up. It is not the chosen vehicle because it
breaks the property `store_database.go:95-105` calls out as the point of the
layout: a collection file plus its own journal stops being self-describing
(committed redo would live only in the shared journal until each collection
checkpoints past it), which forces a durable enrollment mark in the StateRoot,
a permanent fail-closed rule for standalone `Open`, primary-format golden
regeneration, a log-before-root rule threaded through every root-writing
path, and cross-collection recycle coupling in which one lagging collection
pins other collections' redo bytes. The chosen design keeps every one of
those surfaces untouched and confines the new machinery to the coordinator,
one new sidecar file, and the conditional journal record.

**Rejected outright:**

- SQLite-style super-journal with file deletion as the commit point: commit by
  directory-entry existence contradicts checksummed-record, newest-first
  recovery, and SQLite's own documentation enumerates the filesystems where it
  breaks.
- Single-file relayout (all collections under one root): "a different storage
  engine, not a catalog over this one" (`store_database.go:98-102`). The
  unreleased primary grammar can still be replaced later; not this pass.

## On-disk artifacts

The primary file format is untouched. `DevelopmentFormatVersion` stays 0, the
StateRoot layout does not change, its existing physical-capacity field remains
at bytes `192:200`, the reserved suffix still begins at 200, and the primary
golden images remain byte-identical. The transaction machinery uses the current
recovery-journal grammar and the new sidecar file.

### Recovery journal conditional batch record

The primary store format stays at `DevelopmentFormatVersion == 0`. The current
recovery-journal header uses numeric `Format == 0` solely as a corruption
sentinel. Its current record kinds are Put=1, Delete=2, Batch=3,
ConditionalBatch=4, and DeltaBatch=5. Kinds 1 through 4 are the atomic family;
kind 5 is the consecutive-generation delta family, and one unrecycled window
never mixes the two.

A kind-4 conditional batch uses the same ordered put/delete entry grammar,
one-generation rule, CRC32C plus complement, and sector padding as kind 3. Its
body is prefixed by a 32-byte conditional header:

```
MarkerID    [16]byte   identity of the transaction decision log
MarkerEpoch uint64     decision-log epoch the record was written under
TxnID       uint64     transaction identity, unique within the epoch
```

Replay semantics for kind 4, stated as invariants:

- Every retained conditional is resolved, including a record whose generation
  the selected root appears to cover. Root coverage can reflect an interrupted
  private sequential replay of only part of a batch, so it is not independent
  proof that the conditional was completely applied. A database open resolves
  all conditionals before mutating any participant.
- A record resolves committed only when the decision resolver matches
  `(MarkerID, MarkerEpoch, TxnID)` and finds the exact participant tuple
  `(StoreID, JournalID, PreparedGeneration)`, with `PreparedGeneration` equal
  to the record generation. A decision that exists but lacks this exact tuple
  is a binding error and fails closed. A transaction with no decision in the
  current marker epoch is presumed aborted and skipped. Marker identity or
  epoch disagreement fails closed.
- An aborted conditional does not advance the collection's logical generation,
  so the immediately following atomic record may legitimately reuse that
  prepared generation. Sequence monotonicity remains the framing authority;
  the resolved outcomes determine the logical generation chain.
- Every decoded conditional — applied or skipped — forces the final resolved
  fold and `RecycleResolved`. A failure during resolution, replay, the fold, or
  recycle leaves the record intact. The decision log is retained until every
  participant has crossed this successful boundary (or has a durable
  retirement).
- Replay without a resolver — standalone `durable.Open` / `vibedb.OpenFile` —
  fails closed with the typed in-doubt error for any retained conditional,
  including root-covered records. The file becomes self-contained again only
  after a resolver-backed open successfully folds and recycles the window.
- If the database directory holds no decision log, the absent-log invariants
  L1–L4 below apply. Any retained conditional makes that absence fail closed;
  an empty conditional-free live window opens normally.

### The transaction decision log

One sidecar per database directory, fixed reserved name `txn.vtm`, present in
the native `durable.Database` directory or the SQL driver's
`<catalog>.tables/` directory. `validateDatabaseLayout` and
`removeOrphanRecoveryJournals` learn the name; it is not a decodable
collection filename (no `c-` prefix). The file is minted lazily, at the head
of the first multi-collection commit, and the mint is fenced: file creation,
both header sectors, region preallocation, file sync, and the
parent-directory fsync all complete before any participant journal may
append a kind-4 record that references the minted `MarkerID` (invariant L2
under Recovery; a mint failure is a definite abort with nothing journaled).
A conditional record therefore exists only under a durably linked decision
log — "journals hold conditionals, log absent" is unreachable by crash, which is
what lets recovery fail closed on that state instead of guessing (L3). The
container follows the recovery journal's discipline exactly: two alternating,
independently checksummed 512-byte header sectors, a bounded preallocated record
region, positional sector-aligned appends, strict sequence validation, torn-
tail truncation at the first invalid record, and recovery selecting the valid
header with the greater recycle count.

Header fields: numeric-zero `Format` corruption sentinel, `MarkerID [16]byte`
minted at creation, `Epoch uint64`, `BaseSequence uint64`, capacity, recycle
count, sealed-capacity flag, checksum. Two record kinds are each sealed by
CRC32C plus complement and padded to the append sector:

```
kind 1  decision            sequence (the database commit sequence, DCSN)
                            TxnID uint64
                            participantCount uint32
                            participants: {StoreID [16]byte,
                                           JournalID [16]byte,
                                           PreparedGeneration uint64} × N
kind 2  participant-retired sequence
                            StoreID [16]byte
```

A decision is the durable fact "transaction TxnID committed, naming these
participants at these generations." The database commit sequence gives
recovery a total order over multi-collection commits; there is no shared
generation counter and no external timestamp authority. A participant-retired
record is appended and synced by `DropCollection` after it has foreground-
checkpointed the collection past every conditional record (see DDL
interaction); it lets recovery distinguish a legitimately dropped participant
from a lost one.

**Recycle rule.** The log recycles by bumping `Epoch`, and recycling is legal
only when no participant journal's live window still holds a kind-4 record of
the current epoch and every recorded decision is discharged: each exact
participant has successfully completed its resolved fold and journal recycle,
or carries a durable retirement. A selected root at the prepared generation is
not enough. Log-region pressure therefore forces bounded foreground checkpoints
of lagging participants — the existing pressure-checkpoint idiom; no background
work is introduced anywhere in this design. Because the rule is absolute,
replay can treat any epoch mismatch as corruption and fail closed, which is
what makes the presumed-abort resolution sound.

One tampering window is accepted and named rather than closed: a same-epoch
log restored to an earlier content prefix is epoch-invisible, so replay
presumed-aborts the decisions the prefix lost wherever a participant had not
yet checkpointed past them. Closing it online would take cross-file sequence
anchoring in every participant root — a StateRoot change this pass forbids.
`cmd/vibedb-verify`'s pairing check is the operator-facing detector: it
reports any same-epoch conditional record that lacks a decision. E1 and E2
stay fail-closed because epoch mismatch and log absence are cheaply
detectable at open; the silent prefix window is not, and is recorded as
tension 6.

`TxnID` is minted from an in-memory monotonic counter seeded at open from the
decision scan's maximum. The scan alone cannot make identifiers unique:
aborted and undecided transactions never write a decision, so their stray
conditional records are invisible to the seed, and a reused
`(MarkerID, Epoch, TxnID)` triple could otherwise resurrect an aborted
record as committed after a later crash. Two replay invariants above close
that window structurally, not probabilistically: strays are consumed by a
successful resolver-backed reopen before another prepare can be appended, and
a decision binds the exact store identity, journal identity, and prepared
generation. Both open paths order all participant opens before the transaction
log accepts its first commit. Crash row R1 and a dedicated fuzz seed pin this
window.

## The commit protocol

`UpdateCollections` is the caller-owned primitive, the write dual of
`SnapshotCollections`; `durable.Database.Update` wraps it over the catalog. A
transaction whose write set touches exactly one collection takes today's
`Collection.Update` path unchanged — no conditional record, no decision, one
sync, byte-identical journal output. That equivalence is regression-pinned.

For K ≥ 2 participants, under the owning catalog's commit lock (the SQL
driver's `db.mu`; a facade commit mutex for native):

1. **Validate.** Transaction bounds (below), then per-participant conflict
   validation where the caller is a transaction surface. Any failure is a
   typed refusal; nothing has been staged.
2. **Stage.** Acquire every participant's writer in the process-global
   snapshot order, check lane eligibility and persistence poison, then run
   each participant's existing plan/build/capacity phases
   (`applyPrimaryBatch` up to, and excluding, its journal fence). All frames
   are dirty-but-invisible. Any failure unwinds every staged participant and
   releases in LIFO order: a definite rejection, nothing journaled, nothing
   visible. Per-participant pressure checkpoints and content-equivalent
   topology preparations behave exactly as they do today.
3. **Prepare.** A prepare may reference the decision log's `MarkerID` only
   after the log's mint fence has completed (invariant L2): a lazily minted
   log finishes creation through parent-directory fsync before this step
   begins, and a mint failure is a definite abort with nothing journaled.
   Append and sync one kind-4 conditional record per participant, in that
   participant's own journal, on its own lane machinery. The sync-journal
   lane appends and syncs before anything is visible, as its contract
   requires. Buffered-journal lanes append through their deposit machinery
   and then force a covering sync in place — for a multi-collection commit,
   durability precedes visibility on every supported lane. An append or sync
   failure is terminal and poisons that collection. A positional append may
   report a short/error result after the complete checksummed body reached the
   page cache; a sync may likewise have persisted it. Both therefore report
   `ErrCommitOutcomeUnknown` for the prepare attempt even though no transaction
   decision was appended. Reopen resolves the retained conditional as abort
   when no matching decision exists. Stray synced conditional
   records from an aborted attempt are contained rather than assumed away:
   replay presumes abort, consumes them at the collection's next reopen, and
   the participant binding keeps any later TxnID reuse from resurrecting
   them; the generation-aliasing invariant covers their reuse of a
   generation number.
4. **Decide.** Append the decision record and cross one power-safe sync.
   **This sync is the sole atomic commit point.** The protocol invariant is
   that the decision sync strictly follows every participant sync, so a
   durable decision implies durable participant redo. Capacity and validation
   are preflighted definite refusals. Any unexpected decision append or sync
   failure is `ErrCommitOutcomeUnknown` at database scope because a complete
   checksummed decision may still be selected on reopen.
5. **Publish.** Acquire every participant's publication gate write side in
   the same global order, flip every router pointer and file state across all
   participants, release gates then writers LIFO. Because the read cut
   acquires the same locks in the same total order, no `Database.Snapshot` or
   `SnapshotCollections` cut can observe a partial participant set, and the
   composition is deadlock-free by the existing argument. A plain
   single-collection `Snapshot` sees its collection before or after the flip,
   which is all it ever promised.

Cost, stated plainly: a K-participant commit performs K+1 fsyncs and holds K
writers across them. Multi-collection read cuts (`Database.Snapshot`,
`SnapshotCollections`) block for the duration of an in-flight commit because
both take the same writer set; single-collection snapshots and reads do not.
Reducing K+1 to 1 is the named follow-up above, justified only by measured
numbers.

### Recovery

`OpenDatabase` and the SQL driver both use one complete-catalog recovery pass;
caller-owned catalogs enter through `OpenCollectionsWithTransactions`, which
does not expose any collection until the whole set has passed preflight:

1. Load `txn.vtm` if present: newest semantically valid header, scan decisions
   and retirements, truncate the torn tail. A checksum-invalid torn alternate
   header may fall back; checksum-authenticated semantic damage fails closed
   instead of resurrecting an older epoch. Build the decision table. When the
   file is absent, the open is governed completely by the absent-log
   invariants L1–L4 below — absence is the common state, not a corruption
   state, and it never resolves by guesswork.
2. Open each collection with the resolver plumbed into journal replay.
   Every retained kind-4 record resolves as specified above. In-flight qualified
   materialization rolls back before any replay, unchanged.
3. Fail closed, database-wide, when a durable decision names a participant
   whose file or journal is missing or mismatched — unless a
   participant-retired record covers that StoreID. A committed transaction
   with an unrecoverable participant is a hard error, never a silent partial
   commit.
4. After every exact participant has successfully completed its resolved fold
   and journal recycle (or a durable retirement names it), the decision is
   discharged. The log is recyclable only after every decision is discharged
   and no current-epoch conditional remains.

Recovery is idempotent under a second crash: replay ends in the existing
checkpoint-plus-recycle per collection, decided records re-resolve
identically, and the decision log is only recycled after no live window needs
it.

#### The absent decision log

`txn.vtm` is minted lazily, so most databases never have one, and the
epoch-relative replay rules above have no referent without a log. Four
invariants define the absent state completely; each carries a named test.

- **L1 — clean absence.** No `txn.vtm` and no collection journal whose live
  window holds a kind-4 record: the database opens exactly as it does today,
  no transaction machinery engaged. Fresh directories, databases whose whole
  history is single-collection commits, and — by L2 — every pure crash image
  with an absent log land here. Test: `TestDatabaseTxnAbsentLogCleanOpen`.
- **L2 — the mint fence.** The mint (file creation, both header sectors,
  region preallocation, file sync, parent-directory fsync) completes before
  any prepare may reference the minted `MarkerID`. Corollary: a kind-4
  record exists in a journal only if the log's directory entry is durable,
  so no pure crash produces "conditional present, log absent". A crash during
  the
  mint itself leaves either no directory entry (L1) or a file with no valid
  header that no journal references — mint residue, durably removed so a later
  commit can create a fresh log, never a fail-closed state. A log with no valid
  header sector
  *while some journal holds a kind-4* is impossible by crash (both headers
  sync before the fence, and every later header rewrite alternates sectors)
  and fails closed as tampering. Test:
  `TestDatabaseTxnDecisionDirectoryFence` asserts both halves — a torn-away
  directory entry implies zero conditional records anywhere, and reopen is
  clean.
- **L3 — fail closed on impossible absence.** No `txn.vtm` while some
  collection journal's live window holds any retained kind-4 record:
  `OpenDatabase` fails closed database-wide with
  `ErrTransactionLogMissing`. By L2 the state is unreachable by crash; it
  implies out-of-band deletion or a partial restore, and presuming abort
  here would silently roll back commits that may have been acknowledged —
  the same reasoning that keeps W5 and E1 fail-closed. The fail-closed
  choice costs crash recovery nothing precisely because L2 makes the state
  tamper-only. (Without L2 this state would be crash-reachable at the
  first-ever multi-collection commit, and presumed abort would then be the
  only sound default: a never-fenced directory entry implies no decision
  under it was ever acknowledged — the decision sync strictly follows the
  fence — so aborting could lose nothing acknowledged. L2 is pinned exactly
  so the design never has to accept the silent-rollback window that default
  opens for out-of-band deletion.) Root-covered conditionals also trigger L3:
  root coverage does not prove complete batch consumption, and there is no
  resolver to establish the outcome. Test: `TestDatabaseTxnDecisionLogMissing`
  exercises retained conditional records and expects the typed error.
- **L4 — removal legality and idempotence.** Recovery removes `txn.vtm` as
  residue only when no collection journal's live window holds any kind-4
  record of its `MarkerID` and every decision it holds is discharged (each
  named participant's journal base proves successful resolved recycle past its
  prepared generation, or a retirement record covers the StoreID). The
  predicate is re-evaluated from disk on every open, so a crash before, during,
  or after the removal
  re-enters L4 or L1 with the identical outcome; recovery never removes the
  log in any other state. Out-of-band deletion *during* recovery does not
  disturb the running pass — the decision table is already in memory — and
  the next open lands in L1 or L3 by the rules above. Test:
  `TestDatabaseTxnLogRemovalIdempotent` (crash images straddling the
  removal), plus S2's second-crash determinism.

### Crash-window walkthrough

Participants A and B; "durable" means would survive power loss at that
instant.

| # | Window | Recovered outcome | Why |
| --- | --- | --- | --- |
| W1 | A's prepare durable, B's absent or torn | abort both | no decision exists; A's conditional is undecided, presumed abort |
| W1b | B's prepare append or sync fails live | `ErrCommitOutcomeUnknown` for the attempt; B poisoned; reopen aborts every prepare when no decision exists | a complete checksummed conditional may have landed despite the write/sync error |
| W2 | all prepares durable, decision absent or torn at any byte prefix | abort all | torn decision truncates; presumed abort everywhere |
| W3 | decision durable, crash before any or all publishes | roll all forward | publish is memory-only; replay resolves and applies each committed kind-4 |
| W4 | decision durable, A successfully resolved, folded, and recycled past its prepare, B not | complete B | A's journal base proves its participant discharged; B still resolves and replays |
| W5 | decision durable, B's journal missing or mismatched | fail closed database-wide | identity/geometry/epoch family, extended; no retirement record for B |
| W6 | decision append or sync errors live | `ErrCommitOutcomeUnknown`, catalog poisoned; reopen resolves all-or-nothing | a complete checksummed decision may have landed despite the reported write/sync error |
| W7 | crash with `txn.vtm`'s directory entry not durable | treated as absent; nothing to abort — no journal can hold a kind-4 under an un-fenced marker (L2) | the mint fence completes before the first prepare; a surviving entry is a valid-empty log or headerless mint residue, both removable before a later mint; database directory is contractually un-renamed |
| W8 | participant dropped, crash around the drop | consistent either way | drop folds the collection past its conditionals, appends and syncs a retirement, then deletes with today's ordered fences |
| W9 | in-flight materialization on a participant | materialization rolls back first, then the decision applies | existing recovery ordering, unchanged |
| A1 | aborted conditional at G+1, later applied record at G+1 | conditional skipped, later record applied | the resolved atomic chain lets the next record reuse an aborted prepare's generation; sequence order remains authoritative |
| R1 | txn N aborts leaving a stray synced conditional; resolver-backed reopen; a later transaction mints TxnID N again and its decision goes durable; crash before publish | the old aborted record never applies | the reopen folds and recycles the stray before any new prepare, and a decision binds StoreID, JournalID, and PreparedGeneration exactly |
| S2 | crash during recovery replay, reopen again | identical outcome | idempotent roll-forward; log recycled only when no window needs it |
| E1 | restored older log beside newer journals (or the reverse) | fail closed | absolute epoch rule |
| E2 | `txn.vtm` deleted or torn away out-of-band while any journal holds a retained kind-4 | fail closed database-wide: `ErrTransactionLogMissing` | unreachable by crash under L2; presumed abort would silently roll back acknowledged commits (L3), and root coverage is not resolution proof |

## Semantics

### Isolation: explicit, logical, and bounded

The native facade's read-write transactions are serializable. BEGIN registers
a database-global logical revision before capturing one coherent
`SnapshotCollections` cut. Exact point reads (including misses) retain bounded
per-collection read dependencies; scans use a coarse collection marker.
COMMIT locks the sorted union of read and written collections, validates those
dependencies and the exact write set, then publishes and records one logical
revision. This rejects write skew, phantoms, ABA changes, and a concurrent
insert after an observed miss. Overflow fails closed with a conflict rather
than weakening isolation.

SQL and pgwire expose three policies. Default and Read Committed capture one
coherent catalog cut per physical statement. Repeatable Read (also named
Snapshot by the typed API) retains the BEGIN cut. Serializable retains the
BEGIN cut and tracks proven primary-key point reads exactly, including misses.
Scans, ranges, secondary predicates, joins, nested execution, and bounded
exact-read overflow promote that table to a relation-coarse dependency. COMMIT
validates those reads in addition to exact first-committer-wins writes, so
disjoint point writers can proceed without weakening phantom or write-skew
protection. Every mode overlays staged writes and preserves read-your-writes.

`database/sql` accepts LevelDefault/LevelReadCommitted,
LevelRepeatableRead/LevelSnapshot, and LevelSerializable. Read Uncommitted and
Linearizable are refused with `ErrUnsupportedIsolation`. Conflict ordering is
entirely logical; wall-clock time and clock synchronization are not part of the
correctness protocol.

### Conflicts

Detection is the two existing mechanisms, generalized per written table: the
bounded conflict clock (value changes by committed writers; clock-history
overflow is itself a conflict, as today) and fresh-snapshot existence
validation. The clock moves to an internal package shared by the driver and
the facade. Facade point writes record into a collection's clock only while
at least one read-write transaction is open (an armed counter; the unarmed
cost is one atomic load), with a stated invariant carrying a race-test
obligation: every publication not visible in a transaction's begin snapshot
and committed before its commit validation is observable to that validation.
Conflict scope is handle-mediated writes — the same `vibedb.Database`,
`*sql.DB`, or pgwire endpoint. One sentinel per surface
(`vibedb.ErrTxConflict`, `driver.ErrTransactionConflict`, SQLSTATE 40001),
nothing published on conflict, and no auto-retry anywhere: a retry re-runs
user code, so the loop belongs to the caller.

### Bounds

All accounting completes before any durable work; exceeding any bound is a
typed refusal with nothing staged, journaled, or published.

- Per participant, unchanged: `MaxBatchDocuments`, `MaxBatchBytes`, the
  per-leaf batch ceiling.
- New cross-participant totals in `durable.TxnLimits`: `MaxCollections`
  (default 16, hard-capped by one decision record's participant capacity),
  `MaxDocuments`, `MaxBytes` (defaults four times the single-collection
  defaults; exact values are pinned beside the existing option defaults).
  Surfaced through facade `AdvancedOptions` and driver defaults; violations
  map to `vibedb.ErrTxTooLarge` / `driver.ErrTransactionTooLarge` with the
  failing dimension named in the message.
- `TxnLimits` is fail-closed at its zero value: the caller-owned
  `UpdateCollections` refuses any K ≥ 2 commit while a dimension is zero,
  staging nothing (the single-member route is governed by the collection's
  own bounds, unchanged). Zero-to-default substitution happens only in the
  layers that own defaults — `Database.Update` normalizes to the pinned
  package defaults, the facade through `AdvancedOptions` normalization, the
  SQL driver through its option normalization — never inside the primitive.
- Savepoint frames count against the transaction's existing staged budget;
  rollback-to does not lower high-water accounting (documented).
- Decision-log pressure forces bounded foreground folds; a decision that
  cannot fit an empty log is a refusal.
- No wall-clock transaction limit. Long transactions surface as
  lease/retirement capacity pressure, which is already bounded and metered.

### Per-lane guarantee

| Lane | Multi-collection visibility | Crash promise | Acknowledgement |
| --- | --- | --- | --- |
| sync-journal (fixed SQL default) | atomic: all gates flip together | crash-atomic | after K prepare syncs + decision sync |
| buffered-journal (power-safe / filesystem) | atomic | crash-atomic | same; durability precedes visibility for multi-collection commits, stronger than the lane's single-collection contract and stated as such |
| buffered-volatile (both) | — | refused: `durable.ErrDatabaseTransactionUnsupportedLane` | — |
| async-COW, sync chain-fence | — | refused: same typed error | — |
| Memory profile (heap store) | atomic: all writers held, all pointers flip | no crash dimension | in-process |

Buffered-volatile is refused rather than documented-weaker because
independent per-collection checkpoints could tear a transaction across files
after a crash — a new failure mode, not the lane's existing loss window —
and because one unrecycled recovery-journal window cannot mix conditional
atomic records with the delta family. The facade
Buffered profile maps to buffered-visible publication, so native
multi-collection transactions on that profile are a typed refusal in this
pass. That is a real capability gap on one of the three profiles; it is
recorded as an open tension with two candidate remedies (delta-lane
integration, or the shared-journal follow-up), neither in scope here.

### Unknown commit outcome

The clause below goes into `docs/durability.md` verbatim when the
implementation lands:

> A multi-collection COMMIT has exactly one window where commit versus abort is
> unknown: an unexpected decision-record append or sync failure. COMMIT returns
> `ErrCommitOutcomeUnknown`, every collection handle under the catalog refuses
> further writes with the sticky persistence failure, and only closing and
> reopening the database resolves the outcome. Prepare append/sync failures use
> the same error classification because their journal side effect may exist,
> but no decision was attempted, so recovery deterministically aborts them. A
> decision-stage unknown outcome is atomic: reopen reveals either every
> participating collection's writes or none of them. There is no crash, error,
> or recovery in which one participant's writes survive without the others'.

Poison widens from collection to catalog scope once decision append begins: a
half-poisoned catalog could otherwise commit collection B after collection A's
outcome went unknown. Prepare append and sync failures poison the affected
collection and report unknown for that attempt; absent a matching decision,
reopen deterministically resolves those prepares as abort. Guidance follows
FoundationDB's
discipline: mint operation identities outside the retry loop; after reopen,
probe any one participant key of the transaction — its presence decides the
whole transaction.

### Savepoints — SQL and pgwire only

SQLite/PostgreSQL semantics: LIFO marks; `SAVEPOINT name` records per-table
overlay watermarks (staged-order lengths plus a displaced-entry undo log for
keys overwritten after the mark); `ROLLBACK TO` rewinds the overlay without
ending the transaction and returns a failed session to in-transaction state —
the property client stacks rely on; `RELEASE` erases marks LIFO through the
name; duplicate names shadow earlier marks, operations select the newest, and
releasing that mark reveals the previous one; `ROLLBACK TO` retains its target;
real commitment only at COMMIT. Bounded count (documented constant, 64) with a
typed refusal. Savepoint control remains available in read-only transactions;
DML and DDL remain refused. The refusal strings in
`sql/unsupported.go` and `pgwire/command.go` are deleted, and the executable
refusal taxonomy moves in the same changes: the cross-adapter manifest row
pinning the savepoint refusal (`internal/conformance/unsupported_sql.go:16`,
executed against both public adapters by `pgwire/protocol_test.go`) is
replaced — not merely dropped — in the very change that makes `SAVEPOINT`
parse, by a still-refused, parser-owned family (`LOCK`;
`sql/unsupported.go`'s lock-manager refusal), keeping the manifest at five
families. Chained transactions cannot take that row's place: the manifest's
mechanics require the shared parser's typed refusal with identical wording
through both adapters, and chained-transaction refusal is a pgwire
command-layer decision `database/sql` never sees. pgwire's trailing-token
refusal message is reworded to stop naming savepoints; chained transactions
and transaction modes stay refused, regression-pinned. `database/sql`
users reach savepoints as statement text through `tx.Exec`, the ecosystem
convention. The native closure API deliberately does not grow savepoints:
closures compose by control flow, and nested `Update` is a typed error, not
an implicit savepoint.

## API surface

### Native facade

```go
// Closure lifetime: commit on nil, rollback on error, rollback and re-panic
// on panic. An escaped or finished *Tx is inert (ErrTxDone).
func (d *Database) Update(fn func(*Tx) error) error
func (d *Database) View(fn func(*Tx) error) error   // read-only coherent cut

// Manual lifetime for adapters and multi-call flows; Update/View wrap these.
func (d *Database) Begin() (*Tx, error)
func (d *Database) BeginReadOnly() (*Tx, error)

type Tx struct{ /* leased cut + bounded per-collection overlays */ }
func (t *Tx) Collection(name string) *TxCollection // never errors; facade convention
func (t *Tx) Commit() error   // second call ErrTxDone
func (t *Tx) Rollback() error // after Commit: nil no-op

// The facade Collection vocabulary minus lifecycle and DDL: no CreateIndex,
// no Flush, no Close. DDL-in-transaction is unrepresentable natively.
type TxCollection struct{ /* snapshot ⊕ overlay */ }
func (c *TxCollection) Get(key string) ([]byte, bool, error)
func (c *TxCollection) Append(dst []byte, key string) ([]byte, bool, error)
func (c *TxCollection) Put(key string, document []byte) (created bool, err error)
func (c *TxCollection) Delete(key string) (deleted bool, err error)
func (c *TxCollection) Range(fn func(key string, document []byte) error) error
func (c *TxCollection) Run(compiled *query.Query) (query.Result, error)

var (
    ErrTxConflict           = errors.New("vibedb: transaction conflict")
    ErrTxTooLarge           = errors.New("vibedb: transaction exceeds a bounded limit")
    ErrTxDone               = errors.New("vibedb: transaction is finished")
    ErrTxReadOnly           = errors.New("vibedb: mutation in a read-only transaction")
    ErrTxUnsupportedLane    = durable.ErrDatabaseTransactionUnsupportedLane
    ErrCommitOutcomeUnknown = /* alias of the storeio sentinel */
)
```

No facade method takes a `context.Context` today; `Begin` follows the house
convention. Writes to collections absent at BEGIN stage normally; commit
creates the empty collection first, then commits it as an ordinary
participant, so first-write-creates survives inside transactions. One
documented residue follows: if the transaction then aborts — conflict, typed
refusal, or a crash before the decision — the newly created empty collection
remains. It holds no documents and is benign, but it is user-visible catalog
residue, documented rather than silently garbage-collected; the same applies
to SQL tables materialized empty at commit. A
transaction whose write set touches one collection commits through
`Collection.Update` unchanged. `Run` composes the existing
`query.FromFileOverlay` for durable snapshots and a new symmetric heap
overlay source for the Memory profile.

### Durable engine

```go
// Caller-owned primitive, the write dual of SnapshotCollections.
func UpdateCollections(log *TxnLog, members []NamedCollection,
    limits TxnLimits, fn func(*DatabaseBatch) error) error

// Catalog-owned convenience over the same machinery.
func (d *Database) Update(fn func(*DatabaseBatch) error) error

type DatabaseBatch struct{ /* per-member staging */ }
func (b *DatabaseBatch) Collection(name string) (*WriteBatch, error) // existing WriteBatch

// TxnLog owns txn.vtm: lazy mint, decision append/sync, retirement,
// epoch recycle, pressure, catalog-scope poison.
type TxnLog struct{ /* unexported */ }
func NewTxnLog(dir string, options TxnLogOptions) (*TxnLog, error)
func (l *TxnLog) AdoptCollection(collection *Collection) error
func (l *TxnLog) DetachCollection(collection *Collection) error
func (l *TxnLog) Close() error

type TxnLimits struct{ MaxCollections, MaxDocuments int; MaxBytes int64 }

// Recovery for caller-owned catalogs (the SQL driver). The complete request
// set is validated privately before any collection is returned or journal is
// replayed/recycled. The returned order matches requests.
func OpenCollectionsWithTransactions(dir string, options TxnLogOptions,
    requests []TransactionCollectionOpen) ([]*Collection, *TxnLog, error)

var (
    ErrDatabaseTransactionUnsupportedLane = errors.New(
        "vibedb: a database transaction requires a journal-backed lane (sync-journal or buffered-journal)")
    ErrCollectionInDoubt = errors.New(
        "vibedb: collection holds an undecided database transaction; open its database directory")
    ErrTransactionParticipantMissing = errors.New(
        "vibedb: a committed database transaction names a missing collection")
    ErrTransactionLogMissing = errors.New(
        "vibedb: collection journals hold conditional transaction records but the database's decision log is missing")
)
```

The heap engine gains the visibility-atomic dual: stage per-collection entry
sets, hold every participant writer in the global order, flip all published
pointers inside the hold. No durability dimension; the Memory contract is
unchanged.

### database/sql driver and pgwire

The `writeTable` field and the fence are deleted; the dirty set is every
table with a non-empty overlay. `Commit` validates every dirty table under
`db.mu` (incarnation, clock, write set), materializes absent tables as empty
participants (an aborted or crashed transaction can leave that empty table
behind — the documented residue above), then commits: one dirty table through today's path, two or more
through `UpdateCollections` against a driver-owned `TxnLog` in
`<catalog>.tables/`, reconciled at driver open before per-table opens
complete. Error identities are unchanged; messages name the failing table.
The `Session` gains `Savepoint`, `RollbackTo`, `ReleaseSavepoint`;
`RollbackTo` is valid in the failed-transaction state and recovers it. pgwire
parses the three savepoint forms, admits them as non-terminal members of
transaction blocks, returns a failed session to `T` status on `ROLLBACK TO`,
and adds SQLSTATEs 3B001 (unknown savepoint) and 25P02 (savepoint or release
in a failed transaction). 40001 and 40003 keep their existing mappings; a
COMMIT that returns `ErrCommitOutcomeUnknown` is surfaced as the distinct
40003 error and never as a clean failure.

## Capability matrix expansion

`internal/conformance` gains a `Tables` dimension (`OneTable`,
`MultipleTables`; every existing row is `OneTable`), the lane sets
`DatabaseTxnLanes = {SyncJournal, BufferedJournalPowerSafe,
BufferedJournalFilesystem}` and `DatabaseTxnErrorLanes =
{BufferedVolatilePowerSafe, BufferedVolatileFilesystem, AsyncCOW,
SyncChainFence}`, a `Transaction` value for explicit transactions with
savepoints, and a strengthened `Rollback` obligation: a rejected sibling must
prove that **no participant collection** published rows or postings.

New rows, executed by the native, `database/sql`, and pgwire adapter matrix
tests as applicable:

| ID | Entry | Tables | Lanes | Result |
| --- | --- | --- | --- | --- |
| native-database-txn-unindexed | native | multiple | DatabaseTxnLanes | success, atomic, rollback |
| native-database-txn-indexed | native | multiple | DatabaseTxnLanes | success, atomic, rollback |
| native-database-txn-unsupported-lane | native | multiple | DatabaseTxnErrorLanes | documented error |
| database-sql-transaction-multi-table-unindexed | database/sql | multiple | SQL default | success, atomic, rollback |
| database-sql-transaction-multi-table-indexed | database/sql | multiple | SQL default | success, atomic, rollback |
| database-sql-transaction-savepoint | database/sql | one + multiple | SQL default | success |
| pgwire-transaction-multi-table-unindexed | pgwire | multiple | SQL default | success, atomic, rollback |
| pgwire-transaction-multi-table-indexed | pgwire | multiple | SQL default | success, atomic, rollback |
| pgwire-transaction-savepoint | pgwire | one + multiple | SQL default | success |
| pgwire-transaction-serialization-failure | pgwire | multiple | SQL default | documented error: SQLSTATE 40001 |

The native rows execute at two entry points, and both are matrix-executed,
not unit-covered: lane-exhaustively through the durable adapter
(`durable.Database.Update` can drive every journal lane directly) and
through a facade adapter driving `vibedb.Update` — the Durable profile for
the journal-lane rows, the Buffered profile for the unsupported-lane row,
and the Memory profile's visibility-atomic dual, which has no crash
dimension. The durable adapter is the lane authority; the facade adapter is
the API authority for the flagship native surface.

Per-lane crash notes are encoded on the rows; the rendered
`docs/capabilities.md` golden regenerates.

## Qualification

### Crash matrix

Every window in the walkthrough has a named test. The multi-collection fault
controller composes the existing per-collection journal fault seams with a
new decision-log seam mirroring `device_fault.go`; every reopen goes through
`OpenDatabase` (or the driver's reconciliation) and asserts the recovered set
is all-committed, all-aborted, or fail-closed — never a torn subset.

| Test | Window |
| --- | --- |
| TestDatabaseTxnCrashMatrix/prepare-subset | W1 |
| TestDatabaseTxnPrepareFailureAborts (append and sync seams; asserts outcome-unknown poison and deterministic abort on reopen) | W1b |
| TestDatabaseTxnDecisionTornTail (exhaustive byte-prefix sweep) | W2 |
| TestDatabaseTxnCrashMatrix/post-decision | W3 |
| TestDatabaseTxnCrashMatrix/partial-checkpoint | W4 |
| TestDatabaseTxnParticipantJournalMissing | W5 |
| TestDatabaseTxnDecisionSyncFailurePoisonsCatalog | W6 |
| TestDatabaseTxnDecisionDirectoryFence | W7 |
| TestDatabaseTxnDropParticipantRetiresFirst | W8 |
| TestDatabaseTxnMaterializationRollbackOrder | W9 |
| TestDatabaseTxnAbortedGenerationAliasing | A1 |
| TestDatabaseTxnStrayConditionalTxnIDReuse | R1 |
| TestDatabaseTxnRecoverySecondCrash | S2 |
| TestDatabaseTxnDecisionEpochMismatch | E1 |
| TestDatabaseTxnDecisionLogMissing (every retained conditional fails typed, including root-covered) | E2 / L3 |
| TestDatabaseTxnStandaloneOpenInDoubt | standalone fail-closed |
| TestDatabaseTxnPublishExcludesSnapshotCut (with -race) | publish vs read cut |

`TestDatabaseTxnAbsentLogCleanOpen` and `TestDatabaseTxnLogRemovalIdempotent`
pin L1 and L4 at the reconciliation level; the fence test above asserts both
halves of L2 (torn directory entry implies zero conditional records anywhere,
and reopen is clean).

### Fuzz and differential

- `FuzzTxnMarkerOpen`: hostile `txn.vtm` bytes fail closed or open valid and
  empty; bounded allocation via the capacity clamp.
- `FuzzDatabaseTxnRecovery`: mutate decision-log and journal-tail bytes of
  seeded two-collection crash images — the seed corpus includes committed,
  aborted, mixed, torn-decision, the R1 stray-conditional TxnID-reuse
  image, and the E2 deleted-log-with-live-conditional image (oracle for it:
  fail closed); oracle: fail closed, or every decided transaction is all-in
  or all-out across participants.
- `TestDatabaseTxnLinearizedModel`: randomized multi-table, single-table, and
  autocommit workload with induced crashes; each reopen must equal the model
  at a legal acknowledged prefix per lane, never a third value.
- Cross-surface differential: the shared conformance fixtures assert
  identical final contents across native, `database/sql`, and pgwire for the
  multi-table rows.

### Merge-blocking gates

1. The existing suite passes with zero modifications to existing
   single-collection crash and golden tests; the primary golden images are
   byte-identical.
2. Single-collection `Collection.Update` and single-table SQL commits produce
   byte-identical journal output to baseline, pinned by test.
3. Every named crash test above exists and passes; second-crash determinism
   included.
4. Every new conformance row executes at its entry points; the strengthened
   all-participants rollback assertion runs in every atomic expansion.
5. Every `ErrCommitOutcomeUnknown` return site has a test asserting
   catalog-wide poison and all-or-nothing reopen resolution.
6. Allocation tests: the single-table commit path is allocation-identical to
   baseline; the multi-collection path has a stated per-participant budget.
7. Doc lockstep at landing: no surviving "writes exactly one table" or
   "savepoints are not supported" text outside git history; every refused
   configuration returns a typed error with a test; zero TODO markers.
8. `cmd/vibedb-verify` validates decision-log/journal pairing offline.

## Delivery order

The storage primitive lands and is crash-qualified before any API layer
builds on it: storeio record and decision-log formats first; then the durable
phase split, coordinator, and recovery; then the crash matrix and fuzz
targets; only then the facade transaction, the SQL fence removal and
savepoints, pgwire, the conformance rows, and the documentation rewrite. The
executor plan names sixteen tasks with a dependency DAG, pairwise-disjoint
file ownership, and per-task test obligations.

## Exclusions and named follow-ups

- **Transactional DDL** — flagged adjacent (catalog mutations through the
  decision log); `ErrDDLInTransaction` stands.
- **Single-sync multi-collection commit** (shared redo in the decision log) —
  the perf follow-up, justified only by measured K+1-sync numbers.
- **Certified secondary/range Serializable dependencies** — a future
  concurrency refinement over the safe relation-coarse fallback; it requires
  index/range change certificates that cannot miss phantoms.
- **Buffered-volatile, async-COW, and chain-fence multi-collection
  transactions** — typed refusal this pass; the facade Buffered profile is
  therefore refused for native multi-collection transactions.
- **Native savepoints, auto-retry, wall-clock transaction limits** — out, for
  the reasons stated inline.
- **Cross-database and distributed transactions** — a different track; the
  decision log is deliberately not a public two-phase-commit surface, and
  `MarkerID` is a local identity, not a cluster one.
- **Single-file root-of-roots relayout** — rejected this pass; the unreleased
  primary grammar can still be replaced if the decision-log design disappoints.
- **Bench-gate integration** for transaction latency — lands with the
  bench-gate work on the main branch; this pass ships allocation pins and
  informational numbers only.

## Recorded tensions

Honest disagreements this design resolves by decision, not by proof:

1. **K+1 syncs versus one.** The shared-redo design is strictly better at
   steady state and strictly riskier to land first. If the bench numbers make
   K+1 unacceptable, the follow-up must define and qualify its own current
   grammar when it lands.
2. **Buffered-profile natives.** Refusing native multi-collection
   transactions on the Buffered profile is a visible capability gap on one of
   three profiles. Both remedies are real work and both are deferred.
3. **Catalog-wide poison.** Participants-only poison would preserve
   availability for unrelated collections after an unknown outcome; the
   simpler, fail-closed contract won. Documented, revisitable.
4. **Armed-clock invariant.** The facade's conflict clock arming protocol
   carries a stated invariant whose assurance is a race test, not a proof.
   The SQL driver's equivalent is structurally serialized under `db.mu`; the
   facade's is not, and the test obligation is correspondingly heavier.
5. **Savepoint high-water accounting** does not shrink on `ROLLBACK TO`;
   a transaction can be refused for size it no longer occupies. Bounded,
   documented, and simpler than reclaiming displaced-entry budgets.
6. **Same-epoch prefix restore.** A `txn.vtm` restored out-of-band to an
   earlier same-epoch content prefix is epoch-invisible, and replay silently
   presumed-aborts the decisions the prefix lost wherever a participant had
   not yet checkpointed past them. Detectable offline by `vibedb-verify`'s
   pairing check, not online without cross-file anchoring the StateRoot
   freeze forbids. Accepted; W5, E1, and E2 cover every cheaply detectable
   tampering shape, and this one is named rather than implied away.
