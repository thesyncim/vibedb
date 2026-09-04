# Durable SQL write domains and prepared single-participant updates

Implemented after explicit approval on 2026-09-04 to change the write protocol.
The earlier automatic-review block is superseded by that approval. This change
fixes a correctness defect exposed by the CockroachDB comparison and removes
unnecessary coordination for exact primary-key updates. It is not evidence of
performance or feature parity with CockroachDB.

## Independent request identities

Direct writes retain results in the data group. Coordinated writes admit their
sequence in the request ledger, whose contiguous sequence rule is unchanged.
Using one issuer for both meant direct inserts consumed sequence numbers the
ledger never saw: the next coordinated update could not be admitted.

The PostgreSQL endpoint retains one serialized, fsynced fallback outbox per table with two
independently granted issuer identities and counters. Direct-only execution
cannot fall through to ledger admission; coordinated execution cannot use the
direct path. Only a known refusal before admission permits switching domains.
An uncertain result retains the original identity, mode and command for recovery.
The coordinated installation is persisted before obtaining its grant. Terminal
cleanup advances only the corresponding domain's counter.

Native `DurableSQLRequestExecutor.Execute` now always uses the coordinated
protocol. Callers choosing `ExecuteMode` must use separate issuers for direct and
coordinated commands. `LegacyAuto` exists for legacy outbox recovery; it is not a
safe policy for new mixed-protocol issuers.

## Exact durable mutation recipes

The durable fallback writer prepares eligible direct commands before outbox publication.
Preparation validates SQL and, for a computed update, reads the preimage through
the existing linearizable point-read protocol. It cannot propose a mutation.
The writer then fsyncs the exact mutation recipe together with the SQL, complete
request identity and mode before executing it.

Recovery executes that same recipe without evaluating the SQL again. For an
update, the replicated command checks the original preimage digest atomically
with replacing the row. A conflicting write produces a terminal abort; an exact
retry returns the retained original result. A lost response cannot turn `n=n+1`
into a second increment.

The added UPDATE path requires one exact primary-key equality, one participant,
one relation, one existing-row preimage and one digest-guarded physical mutation.
A missing-row placeholder is ineligible: presence alone cannot authenticate
the earlier absence. Existing lowering rejects primary-key
movement, subqueries, ORDER BY, LIMIT and RETURNING. These limits matter: the
single digest guard covers the complete read set. General scans and multi-key
updates need additional read/absence/phantom guards and remain in the coordinated
protocol. Existing replay-stable direct insert/delete eligibility is retained.
Prepared plans are an internal trusted-client recipe, not a new public wire API.

## Concurrent PostgreSQL autocommit

Production PG writers now use a fixed pool of 16 independent direct issuer slots
for eligible single-group statements. A slot has at most one unresolved command.
The base journal's `.direct` file stores the authority, 16 stable random issuer
installations and reserved sequence high-water marks. It has a checksum and an
exclusive process lock. Before accepting work on startup, the gateway reserves
65,536 fresh sequence numbers for every slot using file sync, atomic rename and
directory sync. A block extension uses the same durability boundary. Failed
publication poisons allocation until reopen. Issuer grants are reopened
idempotently under the persisted installation.

Successful requests do not rewrite this reservation journal or the fallback
outbox. They still wait for the existing replicated durable commit. Direct
sequences may skip values; coordinated ledger sequences remain contiguous and
use separate issuers. Only 16 terminal witnesses per gateway/data group are
needed, regardless of the number of requests or restarts.

The live slot retains the owned SQL, complete request identity and exact mutation
recipe before proposing. An unknown outcome blocks reuse of that slot until the
same recipe resolves. It never triggers SQL reevaluation under the same identity.
A definitive preimage-conflict abort permits up to eight autocommit attempts,
each with a new identity and preimage. Failed attempts count toward request
latency. Other execution errors are not treated as permission to replan. Read-only
preparation retries transient admission pressure or leader changes up to eight
times with bounded backoff (1–16 ms), under the caller deadline. No proposal
is made until preparation succeeds.

**Crash contract:** after a gateway process crash, an interrupted PG statement
has an ambiguous outcome. The client must verify database state before deciding
to resubmit a non-idempotent statement. The gateway skips the entire previous
reservation on restart; it does not automatically reconstruct those interrupted
commands. This deliberately replaces the old stronger server outbox recovery
policy for eligible autocommit statements. A lost connection was never an
application idempotency key. This distinction follows CockroachDB's documented
[automatic implicit-transaction retries](https://www.cockroachlabs.com/blog/what-to-do-when-a-transaction-fails-in-cockroachdb/)
and its [40003 ambiguous-outcome guidance](https://github.com/cockroachlabs/cockroachdb-skills/blob/main/skills/cockroachdb-application-development/designing-application-transactions/SKILL.md).
No acknowledged data relies on gateway files for
its durability. Native durable requests and existing outboxes keep their exact
crash-replay contract. Copying a running gateway's local state to another active
instance is unsupported; it must not create competing owners of the same issuer.

A context-aware gate permits concurrent direct writes within a table and gives
coordinated fallback writes exclusive access. Pending legacy commands resolve
under their original identity before new work on that table. Other tables remain
independent. Atomic replicated guards and intents, rather than local gates,
enforce correctness against other gateways. Broad updates, missing-preimage
updates and multi-group writes continue through the existing fallback.

## Compatibility and failure handling

Journal versions 1/2 retain their legacy encoding and recovery policy. Versions
3/4 persist explicit execution domains; versions 5/6 additionally persist an exact
prepared recipe. Odd versions are untyped and even versions preserve parameter
types. Older binaries reject unsupported versions. Do not downgrade while a new
outbox exists without an explicit migration.

Open validates mode, version, issuer/grant/counter relationships and retained ACK
identity. Journal failures poison the writer. A pending legacy request is never
cleared or renumbered to recover availability. Existing legacy sequence-gap
requests and native callers with mixed-protocol history need a separate recovery
procedure; this change does not claim to repair those records automatically.
PostgreSQL reconnects carry no application idempotency key and are not exactly-once
application retries.

## Proposal scheduling

A lone proposal no longer waits for the fixed 500-microsecond coalescing timer.
It enters the existing Raft pipeline immediately. When concurrent pending or
learned cohort work exists, the bounded collector still assembles shared
Raft/storage durability batches. This changes admission scheduling only; quorum,
log persistence, apply, and result publication fences remain unchanged.

## Applied point reads

A point read holds the state machine publication lock while checking the hidden
transaction intent and reading the selected relation's current primary router.
The machine exclusively owns mutations to these collections, so both reads belong
to one completed applied state. The existing minimum applied index, ownership,
intent and response-size checks remain. Completion lookup likewise holds the publication lock while reading the live
hidden state for direct transaction and ledger results, preserving exact
retry/result checks without checkpointing. Native session completions retain
lazy snapshots for historical-slot and orphan-prefix validation.
Detached scans still acquire snapshots.

The old point path captured all collection snapshots and could force a physical
checkpoint after every update. Live point reads include acknowledged overlay
mutations without forcing that checkpoint. Quorum/read-index checks, fsync and
replicated durability publication are unchanged.

## Validation and remaining limits

Tests cover independent sequence domains across restart, lost replies in both
modes, retained ACKs, refusal cleanup failure, identity/version fences, exact
recipe publication before execution, replay without a new preimage, stale-preimage
abort, retained terminal replay, and point/completion reads that leave checkpoint counters
unchanged after every commit. The gateway suites and focused race checks exercise
these paths; the comparative harness verifies every row after each workload.

Fault-injection and unit restart coverage do not establish complete CockroachDB
guarantee parity. Broader process-kill, partition and multi-region histories are
still required. New tests cover parallel identities, no per-request journal
publication, reservation extension/failure, corrupt records, process exit without
cleanup, restart sequence gaps, late-command rejection, and exact unknown retries.
The 2x CockroachDB target remains unmet until a complete comparison proves it.
