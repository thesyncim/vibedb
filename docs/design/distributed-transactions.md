# Distributed transaction protocol

## Contract

Gateway-routed autocommit remains the fast path for a write whose complete write
set belongs to one shard. A write touching more than one shard uses a durable,
idempotent transaction protocol. It must never expose a participant subset,
retry an outcome-unknown mutation as a new operation, or infer an outcome from a
transport error.

The gateway remains stateless. The transaction's first participant in canonical
`(distribution, shard, allocation-generation)` order is its coordinator. The
coordinator role is data, not connection, affinity: another gateway can recover
the transaction by its 128-bit transaction ID.

## Identity and fences

One transaction carries:

- a random 128-bit transaction ID minted once outside every retry loop;
- the pinned catalog generation;
- an ordered participant vector containing distribution, shard, routing
  version, allocation generation, ownership epoch, and a SHA-256 digest of that
  participant's mutation batch;
- a monotonically increasing coordinator record revision; and
- a bounded absolute recovery deadline used only to elect abort while no commit
  decision exists.

Transaction identity, participant digest, and ownership coordinates are exact
match fences. Reusing an ID with different bytes is corruption, not a retry.
Split, merge, or ownership movement cannot translate an in-flight transaction;
the old allocation remains responsible until resolution.

## States

Coordinator records use this monotone state machine:

```
STAGING -> COMMITTED -> RETIRED
    \----> ABORTED --> RETIRED
```

Participant records use:

```
ABSENT -> STAGED -> PREPARED -> APPLIED -> RELEASED
               \-> ABORTED <-/
```

Transitions are compare-and-set operations over the transaction ID, exact
participant digest, and prior state. Repeating the same transition returns its
retained result. A conflicting transition is rejected.

## Visibility protocol

1. **Stage.** The coordinator first durably records `STAGING` with the complete
   participant/digest vector. Participants then retain their compact mutation
   batches in parallel and acquire visibility barriers. Nothing is applied to
   user data.
2. **Prepare in parallel.** Each participant executes its entire batch inside a
   serializable local transaction, checks SQL and constraints, and rolls the
   transaction back while its barrier excludes ordinary shard traffic. Only a
   successful dry run advances the durable participant to `PREPARED`.
3. **Record the decision.** Only after every listed participant is `PREPARED`
   does the coordinator durably transition to `COMMITTED`. This is the sole
   commit point. A transport response never creates or erases that decision.
4. **Apply.** Participants re-execute the prepared bytes and publish all local
   tables plus the raw transaction-ID outcome in one crash-atomic local database
   transaction, then advance to `APPLIED`. The barrier remains held.
5. **Release.** After every participant is applied, barriers are released and
   the coordinator is retired. An unreleased shard blocks rather than exposing
   a mixed cut.

Before the coordinator commit decision, `STAGED` and `PREPARED` participants
may durably abort and release their barriers. After `COMMITTED`, recovery may
only drive apply, release, and retirement; it may never choose abort.

## Recovery

Every protocol request and response is a complete idempotent operation. A
gateway that loses a response first asks transaction status. Participant and
coordinator restart scan only their bounded active-transaction index, rebuild
visibility barriers before serving requests, and redrive monotone transitions.
No recovery path scans user rows.

An unavailable coordinator makes participants unavailable only for operations
intersecting their visibility barrier; it never guesses abort or exposes a
partial commit. A future replicated coordinator applies the same record through
the shard Raft log without changing transaction semantics.

## Encoding and bounds

- The shard service has one wire format. Transaction commands are an optional
  marked suffix; ordinary request and response bytes are unchanged.
- Transaction IDs are 16 raw bytes, never UUID strings.
- States and operation kinds are one byte.
- Shard identities are length-prefixed bytes already owned by the catalog.
- Participant vectors are sorted once and packed contiguously in durable form.
- Parameters retain the shard wire's byte-native `vibejson.RawValue` contract.
- One participant stores one statement/batch envelope, not one record per row.
- Coordinator records contain participant digests and states, never SQL or row
  documents.
- Active transaction count, participants per transaction, batch bytes, retained
  result bytes, and recovery work are hard bounded.

Initial production bounds:

| Resource | Bound |
| --- | ---: |
| participants per transaction | 64 |
| mutation bytes per participant | 16 MiB wire-frame bound |
| active transactions per shard | 4,096 |
| coordinator record | 52 bytes + 60 bytes/participant + distribution/shard identities |
| participant fixed metadata | 120 bytes + coordinator identities + mutation |

## Implementation status

The current branch implements the single-format transaction suffix, checksummed
coordinator and participant records, independent colocated coordinator and
participant roles, append-once journals, explicit prepare, a durable commit
decision, SQL-atomic multi-table participant apply keyed by the raw transaction
ID, and a shard visibility barrier. `Executor.ExecBatch` partitions statements
by full participant identity and runs stage/prepare/commit/apply/release with
bounded parallelism. Apply retries return the retained affected-row count
without executing the SQL mutation twice. End-to-end tests cover cross-shard,
multi-table, and prepare-failure rollback.

Automatic background redrive and key/range-scoped barriers are not enabled yet;
ordinary traffic currently waits behind any active participant on its shard.
The common read-timestamp contract is also still pending. Their integration
order is specified in
[Distributed system target](distributed-system.md).

## Latency and throughput

The single-shard path remains one request/response and one local commit.
Multi-shard work uses persistent pooled connections. The acknowledged critical
path is:

```
stage coordinator
stage participants (parallel)
prepare participants (parallel dry run)
commit coordinator decision
apply participants + release (parallel phases)
```

The current safe implementation waits for apply and release before returning
success. It deliberately does not claim a one-round parallel-commit latency
until replicated intents, coordinator proof, and autonomous redrive make that
claim true. It still stores one compact intent envelope per participant rather
than one record per mutated row.

Hot-path requirements:

- no JSON trees, standard-library JSON, or parameter string materialization;
- no goroutine per row or per parameter;
- one worker slot per participant, bounded by the existing fan-out pool;
- zero-copy request decoding and compiled `vibejson` pointer extraction;
- one hash pass over each participant batch;
- deterministic participant order and contiguous arenas instead of maps where
  cardinality is known; and
- allocation and encoded-size benchmarks treated as compatibility gates.

## Non-goals

This protocol does not claim a cross-shard snapshot for arbitrary read-only
transactions. It provides atomic commit and mixed-visibility prevention for the
write statement and concurrent operations intersecting its participant shards.
Serializable multi-statement distributed transactions require global read-set
validation and timestamp/snapshot coordination. Those requirements are part of
the distributed system target; this participant foundation does not claim them.
