# Separate durable SQL execution lanes (proposal; not implemented)

The PostgreSQL RF3 outbox increments one issuer sequence after every terminal
write. Direct single-participant writes retain their result in the data group;
coordinated writes admit their sequence in the request ledger. Ledger admission
requires exactly the previous admitted sequence plus one. Consequently 128 direct
inserts followed by a computed update attempt ledger sequence 129 while the
ledger expects 1. The benchmark exposes this as an outcome-unknown update.

Automatic approval review rejected implementation of this protocol change on
2026-09-04 because changes to durable journals, issuer lanes, replay and consensus
sequencing could cause duplicate or unrecoverable writes. No write-protocol code
was changed. This document makes the proposed change and its validation reviewable.

## Proposed behavior

Keep one serialized, fsynced outbox per table, with two independently granted
issuer identities and two sequence counters. One identity only executes direct
single-participant commands; the other only executes coordinated ledger requests.
The mode and full identity must be durable before execution. The executor must
support strict modes: direct-only cannot fall back to the ledger, and coordinated
cannot take the direct path even if a later plan becomes eligible.

A fresh request first tries direct-only admission. Only a confirmed refusal before
any proposal or ledger admission allows the outbox to clear that attempt durably
and create a coordinated attempt under its independent identity. No timeout,
lost reply, leader change, storage error or uncertain admission permits switching
identity or mode. Recovery always uses the mode and identity retained on disk.

| Statement | Direct issuer sequence | Coordinated issuer sequence |
| --- | ---: | ---: |
| First direct insert | 1 | unused |
| Second direct insert | 2 | unused |
| Computed update | unchanged | 1 |
| Another direct insert | 3 | unchanged |
| Second computed update | unchanged | 2 |

## Compatibility and failure handling

Use a new journal version so older binaries reject records whose two-lane
semantics they cannot understand. Continue decoding legacy journal bytes exactly.
Recover any legacy pending request under its original identity before admitting
new work. An already retained outcome-unknown request must never be erased or
renumbered to make a benchmark pass. Existing legacy sequence-gap requests may
require a separate recovery protocol; this proposal does not claim to repair them.

The second issuer grant must be persisted with its first request. Restarts must
not regenerate it. ACK advancement changes only the coordinated counter; direct
terminal advancement changes only the direct counter. An unexpected result mode
is an error, never permission to acknowledge a different protocol.

Native callers also need an explicit mode contract and documented independent
issuers. Retaining automatic mode for compatibility would not fix existing native
callers that mix direct and coordinated operations on one issuer.

## Required validation before merge

- Real three-voter insert/update/insert/update via PostgreSQL, checking all rows.
- Alternation across restart, including a retained direct result and ledger ACK.
- Lost replies after direct application and after ledger admission: original
  nonce, mode and sequence retained, each update applied once.
- Proven pre-admission refusal: safe transition to a different issuer only after
  the old outbox attempt is durably removed.
- Journal write/fsync failures at each transition, corrupt or mismatched grants,
  invalid mode/version, overflow, and old-binary version fencing.
- Catalog/routing changes between planning and retry cannot switch protocols.
- Concurrent table requests retain ordering; unrelated tables keep progressing.
- Full RF3 SQL comparison includes update failures and costs; no weakened fsync,
  replication, isolation, result validation or retry accounting.

This is a correctness prerequisite for the performance target, not evidence that
VibeDB is twice as fast as CockroachDB. A separate profile and optimization cycle
is needed once computed updates run correctly.
