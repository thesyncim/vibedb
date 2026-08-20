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
- the pinned catalog generation and routing version;
- an ordered participant vector containing shard, allocation generation,
  ownership epoch, and a SHA-256 digest of that participant's mutation batch;
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
ABSENT -> STAGED -> APPLIED -> RELEASED
               \-> ABORTED
```

Transitions are compare-and-set operations over the transaction ID, exact
participant digest, and prior state. Repeating the same transition returns its
retained result. A conflicting transition is rejected.

## Visibility protocol

1. **Stage in parallel.** The coordinator durably records `STAGING` with the
   complete participant/digest vector while every participant concurrently
   validates ownership, conflicts, limits, and SQL, retains one compact mutation
   batch, and acquires its visibility barrier. Nothing is applied to user data.
2. **Implicit commit.** Once the gateway has durable success from the `STAGING`
   record and every listed participant, the transaction is committed and may be
   acknowledged immediately. This is one network/consensus barrier. An observer
   that did not witness those responses proves the same condition by checking
   every listed participant. A recovery that finds a missing intent first fences
   that participant against the old coordinator revision, then may abort.
3. **Apply.** Participants apply their retained mutation and move to `APPLIED`
   atomically in one local database transaction. User data may have changed,
   but the visibility barrier still blocks reads and unrelated writes.
4. **Make explicit.** After every participant reports `APPLIED`, the coordinator
   records `COMMITTED`. This is asynchronous cleanup and is not on client
   latency. Only explicit or proven implicit commit authorizes release.
5. **Release.** Participants verify the coordinator's committed revision and
   release their barriers. A released shard contains new data; an unreleased
   shard blocks instead of exposing old data, so no read can return a mixed cut.

Abort removes retained intents and releases barriers only after a durable abort
record. A `STAGING` transaction is implicitly committed when every listed
digest exists. Recovery may choose abort only after proving at least one digest
missing and durably fencing every missing participant against late staging.

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
| coordinator record | 52 bytes + 50 bytes/participant + identities |
| participant fixed metadata | 104 bytes + coordinator identity + mutation |

## Implementation status

The current branch implements the single-format transaction suffix, checksummed
coordinator and participant records, the append-once shard journal with compact
transition deltas, idempotent stage/status after restart, SQL-atomic participant
apply keyed by the raw transaction ID, and a recovery-aware shard visibility
barrier. Apply retries return the retained affected-row count without executing
the SQL mutation twice. Ordinary traffic waits while a participant is staged or
applied and resumes only after release.

Gateway batch partitioning, coordinator-owned redrive, and automatic cleanup
are not enabled yet. Until those pieces exist, gateway writes remain
single-shard; the low-level transition path is not presented as completed
cross-shard execution. Their integration order, key/range-scoped barrier target,
and common read-timestamp contract are specified in
[Distributed system target](distributed-system.md).

## Latency and throughput

The single-shard path remains one request/response and one local commit.
Multi-shard work uses persistent pooled connections. The acknowledged critical
path is:

```
stage coordinator + all participant batch intents (one parallel barrier)
acknowledge implicit commit
apply + mark explicit commit + release (asynchronous)
```

The coordinator's participant intent is committed atomically with its `STAGING`
record. Lagging apply/release work stays safely fenced and is redriven in the
background. This matches the one-consensus-round latency floor of parallel
commit systems while using one intent envelope per shard instead of one intent
record per mutated key.

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
