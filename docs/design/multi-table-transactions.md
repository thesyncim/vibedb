# Multi-table transactions

VibeDB implements bounded, crash-atomic multi-collection commits in the native
facade, `database/sql`, and pgwire. This document describes the current
contract; byte layouts are specified in [On-disk format](../format.md).

## User surfaces

- `Database.Update` and `Database.Begin` provide native serializable
  read-write transactions over multiple collections.
- `Database.View` and `BeginReadOnly` capture a coherent multi-collection read
  cut.
- SQL and pgwire support explicit transactions over multiple tables at Read
  Committed, Repeatable Read/Snapshot, and Serializable isolation where
  requested and admitted.
- SQL and pgwire support bounded `SAVEPOINT`, `ROLLBACK TO`, and `RELEASE`.
  Native transactions do not expose savepoints.

DDL remains atomic per statement but cannot run inside a transaction. A
catalog rename and a collection-generation transaction have different commit
mechanisms, so VibeDB refuses to pretend they are one atomic unit.

## Durable commit protocol

The synchronous and buffered-journal lanes use two-phase commit over the
existing per-collection recovery journals:

1. Validate transaction bounds, handle identity, conflicts, participant
   generations, and journal/decision-log capacity before publication.
2. Append and sync one conditional batch record to each participant journal.
3. Append and sync one decision record to the database-scoped `txn.vtm` marker.
   This is the atomic commit point.
4. Publish all participant generations while their publication gates are held
   in one global order.
5. Checkpoint participants and retire or recycle decision-log state through
   bounded foreground maintenance.

For `K` participants the durable commit uses `K` prepare syncs plus one
decision sync. There is no shared redo log and no hidden background
coordinator.

The marker and every conditional record bind exact transaction, participant,
store, journal, generation, and epoch identities. Recovery never decides a
record from filename membership or a partial prefix.

## Recovery

Open performs database-wide transaction recovery before returning collection
handles:

- a complete authenticated decision commits every named conditional record;
- an absent decision aborts a conditional only after directory-wide evidence
  proves the decision log is legitimately absent;
- torn, conflicting, duplicated, cross-epoch, missing-participant, or
  mismatched records fail closed;
- retired participants and orphan journals are reconciled before marker
  removal, with directory fences preserving crash ordering; and
- a sealed marker profile remains exact across removal and reminting.

Reconciliation is idempotent. A crash during cleanup may leave old evidence,
but reopen either continues the same decision or refuses corruption; it cannot
publish a participant subset.

## Isolation and conflicts

Native read-write transactions are serializable. They capture one coherent
begin cut and track bounded exact-key reads, escalating conservatively to a
collection dependency before a read-set limit can be exceeded. A global
logical revision clock supplies first-committer-wins validation. Time is not a
correctness input.

SQL Read Committed may refresh table snapshots as defined by the driver;
Repeatable Read/Snapshot pins a stable cut; Serializable adds conflict
validation. Conflict checks cover writes mediated by the same open database
handle. A second independent handle to the same files is outside that
in-process conflict domain and must not be used as a concurrent writer.

## Bounds

Transactions have explicit limits for participants, staged documents, staged
bytes, read keys, read bytes, read collections, conflict history, and
savepoints. Admission failure publishes nothing. Savepoint rollback restores
logical state but intentionally does not lower every high-water accounting
counter; a transaction can therefore remain too large after rolling work back.

The durability lanes are deliberate:

| Lane | Multi-collection commit |
| --- | --- |
| synchronous recovery journal | supported; crash-atomic |
| buffered with recovery journal | supported; crash-atomic at `Flush`/commit contract |
| memory | visibility-atomic; no crash guarantee |
| buffered without journal | refused |
| async COW | refused |
| synchronous chain-fence | refused |

Unsupported lanes return `ErrDatabaseTransactionUnsupportedLane` (or the
facade alias) before staging durable transaction evidence.

## Failure classification

Errors before a durability-bearing write are definite and leave the catalog
usable. Once an append or sync can have reached storage, VibeDB distinguishes
definite failure from unknown outcome.

An unknown decision outcome returns `ErrCommitOutcomeUnknown`, poisons the
catalog transaction log, and refuses later writes, collection detach/drop, or
marker reuse until the database is closed and reopened. Reopen resolves the
atomic outcome from durable evidence. Blind retry on the poisoned handle is
incorrect.

Prepare append/sync failures can also be unknown because a record may have
reached durable storage despite an error. Marker-capacity preflight errors are
definite. Cleanup or recycle failures are sticky and fail closed.

## Savepoints

SQL savepoints are bounded overlay marks, not durable subtransactions.
`ROLLBACK TO` restores staged table overlays and transaction status to the
named mark; `RELEASE` removes that mark and younger marks according to SQL
semantics. A savepoint never publishes and does not change the database
transaction's single commit point.

## Current exclusions

- transactional DDL;
- cross-database or distributed transactions;
- native savepoints and automatic transaction retry;
- wall-clock leases or time-based conflict ordering; and
- multi-collection commits on the refused durability lanes above.

The local `txn.vtm` marker is not a public two-phase-commit protocol and its
identity must not be reused as a cluster transaction ID.

## Qualification

The executable suite covers torn prefixes, append and sync faults, marker
recycle/removal, orphan journals, participant retirement, crash at every
prepare/decision/publication boundary, second-crash recovery, transaction-ID
reuse, capacity exhaustion, poisoning, exact-index participants, savepoints,
and randomized linearized models. The conformance matrix runs equivalent
multi-table histories through native, `database/sql`, and pgwire. The offline
verifier checks marker/journal pairing and reports incomplete or corrupt
transaction evidence.
