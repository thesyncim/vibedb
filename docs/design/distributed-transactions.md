# Distributed transaction protocol

`gateway.Executor.ExecBatch` runs a byte-bounded atomic write across shards
and tables. The gateway proves every statement's owner before it sends
transaction traffic. A participant is one exact shard target, not one row or
table; mutations routed to the same fenced target share a participant.

The executor has two transaction authorities. General SQL uses the static
shard journal described below. A batch whose every table has replicated
metadata and whose mutations resolve to one or more RF3 groups uses the fused
Raft path. Classification and complete validation happen before either
authority receives I/O; a batch never crosses both.

## Bounds

- Participant count: no semantic count limit
- Inline coordinator fast path: at most 64 participants and 32 KiB
- Segmented coordinator manifest: 64 KiB pages and at most 64 MiB encoded
- Active segmented pages per shard journal: 512 MiB
- Retained transaction journal per shard: 8 GiB, including control reserve
- Maximum participant mutation payload: 16 MiB
- Maximum participant intent scopes: 256
- Transaction ID: random nonzero 128-bit value

The gateway sorts participants by distribution and shard. The first
participant is the coordinator. Small transactions retain the compact `VTC1`
record. Wider transactions use prefix-compressed `VTM1` pages bound by a fixed
`VTCM` descriptor. The begin request carries `VTCM` and page zero together, so
the shard proves the coordinator's routing, allocation, and ownership fences
before writing either record. Later pages are ordered, independently
checksummed, and authenticated by the descriptor root before commit.

Admission remains finite without turning an encoding convenience into a
product limit. Operation profiles cap total mutation count, canonical mutation
bytes, deadline, and in-flight shard requests. Staging and recovery retain one
manifest page plus a worker-sized result window instead of the aggregate page
set or one response per participant. A shard journal reserves the exact bytes
needed to decide and retire every admitted local record. Retiring a segmented
coordinator immediately drops its resident page set, including during replay;
the append-only journal currently retains the historical entries on disk until
its finite ceiling rather than compacting them.

## RF3 SQL lowering

The public RF3 lane deliberately accepts only statement shapes with one exact,
byte-native meaning:

- one whole-document insert row, lowered to insert-if-absent;
- one exact-primary-key whole-document update, lowered to replace-if-present;
- one exact-primary-key delete.

The update document must preserve the placement key. Returning clauses,
multi-row inserts, column-list inserts, conflict clauses, residual predicates,
ordering, limits, repeated relation keys, ready global-index write programs,
and mixed static/RF3 tables are refused before execution. Multiple statements
and tables in one group become sorted numeric relation batches on the same
participant and commit atomically. Ordered keys and document bytes enter Raft;
SQL and table-name strings do not.

Each dense replicated relation currently admits at most 64 distinct mutations
in one apply batch. This state-machine bound is independent of participant
count: a participant can contain several relation batches, and the segmented
coordinator format can represent more than 64 groups.

Insert and update use replicated conditional mutation kinds rather than a
gateway pre-read. This keeps the affected-row result and conflict decision in
the same deterministic apply. The state machine returns one fixed-width
affected-row result, stores it with the session result, and reproduces it on an
exact retry.

The NDJSON caller supplies a nonzero 128-bit request ID distinct from the
generated transaction ID. Its registry key combines the ID with stable request
scope. Authenticated traffic uses the certificate's node identity but excludes
authorization-policy generation, allowing the same node to retry across policy
rotation. Local/plaintext traffic uses a distinct scope and cannot alias an
authenticated node.

The request digest is independent of catalog generation, routes, lowering, and
mutation placement. It hashes the exact ordered statements, operation class,
parameter kinds, boolean values, and parameter bytes. Reuse in the same scope
with different caller bytes fails before orchestration. Concurrent exact
duplicates share one execution or recovery call.

The request registry has a strict finite capacity; the shipped command admits
65,536 identities. It never evicts executing work, live recovery ownership, or
terminal outcomes to make room. The command performs no automatic terminal
expiry and exposes no client ACK or expiry operation. The command never calls
the registry's scoped `Forget` API. An embedding may call `Forget` only after
it has an application-level acknowledgement that the terminal result no longer
needs retry protection. At 65,536 retained entries, new RF3 writes backpressure
instead of dropping idempotency evidence. A durable replicated ledger or safe
explicit client ACK is required before terminal reclamation can become shipped
behavior.

## State machines

The coordinator states are:

```text
Staging -> Committed -> Retired
       \-> Aborted  -> Retired
```

The participant states are:

```text
Staged -> Prepared -> Applied -> Released
   |          |
   +----------+-> Aborted -> Released
```

An exact retry of a state transition is idempotent. A transition also checks
the expected revision. Recovery can create an `Aborted` tombstone for a
missing participant at the initial revision.

## Commit sequence

1. Stage the mutation at each participant.
2. Prepare each participant.
3. Commit the coordinator record.
4. Apply the mutation at each participant.
5. Release participant state.
6. Retire the coordinator record.

The durable coordinator commit is the commit point. Apply and cleanup can
finish later.

### SQL-atomic apply marker

A SQL participant publishes its user-table mutation and one hidden applied
marker through the same database transaction log. The marker is keyed by the
raw 16-byte transaction ID and retains only the applied revision and affected
row count; the larger staged mutation remains in the participant journal.

Before publication, an existing marker is read under the catalog write lock.
An exact revision match returns the retained affected-row count and discards
the retry's SQL mutation. A different revision or malformed marker reports a
distributed transaction conflict, so neither retry path republishes user
data.

The hidden collection uses raw opaque values. Its compact binary codec is part
of the repository's unreleased format 0, not JSON and not a released protocol
version. The sole codec sentinel admits only the current grammar. Stale
development text fails closed without an alternate compatibility decoder or
migration ladder. `docs/format.md` records the exact envelope and storage bounds.

Participant staging acquires scoped durable barriers. An overlapping
participant fails fast. This behavior prevents a cross-shard deadlock.
Disjoint bucket scopes can proceed concurrently.

## Failure results

`TransactionOutcomeUnknownError` means that the gateway cannot resolve the
coordinator commit. Keep the transaction ID. Do not run the SQL again as a new
transaction.

`CommittedTransactionError` means that the commit is durable, but later apply
or cleanup work did not finish. Recover the same transaction ID.

A journal sync failure poisons the journal handle. A torn final append can be
truncated during recovery. Corruption before the tail fails closed.

The NDJSON RF3 response carries `transaction_id` plus `committed:true` after a
durable decision. If the decision cannot be established, it carries the same
transaction identity plus `outcome_unknown:true`. The client must retry the
same batch with the same request ID, not invent a new request.

## Recovery

`RecoverTransaction` scans current catalog shards and requires exactly one
coordinator. It replays committed work idempotently. Segmented recovery reads
and root-verifies one page at a time against one pinned catalog route index. It
can abort an incomplete coordinator or participant set after the recovery
deadline without allocating in proportion to the aggregate manifest.

`RecoverAll` scans non-retired coordinators on all current shards. The shipped
gateway calls it every five seconds.

The RF3 orchestrator instead retains its exact recovery handle in the bounded
request registry. An exact retry or the five-second RF3 sweep performs
leader-only transaction recovery reads and resumes the fused state machine.
This recovers a hidden commit and caches the terminal result without exposing
the live handle to the client.

Request replay occurs before the executor pins a catalog or lowers SQL. A
terminal, pending, or executing entry therefore survives a newer catalog
generation, split, or move without being replanned. Replay returns the first
execution's catalog generation and shard count, while pending recovery keeps
the original generation and participant route metadata in its handle.

An error is retained only when execution produced transaction identity, a
commit proof, or recovery ownership. A plain pre-admission or transient error
is delivered to the waiters on that attempt and then removed from the registry;
a later retry can pin the then-current catalog.

That ownership is process-local. There is no durable request/result ledger
that another gateway can load, and a gateway restart loses pending handles and
terminal request-ID mappings. Replicated coordinator and participant records
remain durable, but the shipped command cannot rediscover them by the caller's
request ID after that loss.

Recovery matches the routing version, allocation generation, and ownership
epoch that the transaction recorded. The implementation does not prove
recovery across arbitrary later resharding or retired topology history.

## Global-index use

The static transaction path expands base-table and global-index mutations into
the same protocol. The replicated state machine can also atomically apply base
and global-index relation batches, and a digest check guards old-document
capture during update and delete. The public RF3 SQL lowering does not yet
construct those global-index mutations and therefore rejects such tables.

The RF3 read side also remains narrower than the static path: there is no
cross-group RF3 `SELECT` snapshot or multi-table RF3 query contract.

## Implementation references

- `gateway/transaction.go`
- `gateway/recovery.go`
- `gateway/transaction_manifest.go`
- `gateway/replicated_sql_transaction.go`
- `gateway/replicated_request_registry.go`
- `gateway/replicated_transaction.go`
- `gateway/replicated_transaction_recover.go`
- `gateway/writer.go`
- `internal/distributedtxn/codec.go`
- `internal/distributedtxn/manifest.go`
- `internal/distributedtxn/journal.go`
- `shardservice/mutation_batch.go`
