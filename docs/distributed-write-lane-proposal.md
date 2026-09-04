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

The PostgreSQL endpoint keeps one serialized, fsynced outbox per table with two
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

The PostgreSQL writer prepares eligible direct commands before outbox publication.
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
one relation and one physical mutation. Existing lowering rejects primary-key
movement, subqueries, ORDER BY, LIMIT and RETURNING. These limits matter: the
single digest guard covers the complete read set. General scans and multi-key
updates need additional read/absence/phantom guards and remain in the coordinated
protocol. Existing replay-stable direct insert/delete eligibility is retained.
Prepared plans are an internal trusted-client recipe, not a new public wire API.

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

## Applied point reads

A point read holds the state machine publication lock while checking the hidden
transaction intent and reading the selected relation's current primary router.
The machine exclusively owns mutations to these collections, so both reads belong
to one completed applied state. The existing minimum applied index, ownership,
intent and response-size checks remain. Detached scans still acquire snapshots.

The old point path captured all collection snapshots and could force a physical
checkpoint after every update. Live point reads include acknowledged overlay
mutations without forcing that checkpoint. Quorum/read-index checks, fsync and
replicated durability publication are unchanged.

## Validation and remaining limits

Tests cover independent sequence domains across restart, lost replies in both
modes, retained ACKs, refusal cleanup failure, identity/version fences, exact
recipe publication before execution, replay without a new preimage, stale-preimage
abort, retained terminal replay, and point reads that leave checkpoint counters
unchanged after every commit. The gateway suites and focused race checks exercise
these paths; the comparative harness verifies every row after each workload.

Fault-injection and unit restart coverage do not establish complete CockroachDB
guarantee parity. Broader process-kill, partition and multi-region histories are
still required. The per-table outbox still serializes independent writes. The
2x CockroachDB target remains unmet.
