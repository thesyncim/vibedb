# Distributed transaction protocol

`gateway.Executor.ExecBatch` runs a byte-bounded atomic write across shards
and tables. The gateway proves every statement's owner before it sends
transaction traffic. A participant is one exact shard target, not one row or
table; mutations routed to the same fenced target share a participant.

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

## Recovery

`RecoverTransaction` scans current catalog shards and requires exactly one
coordinator. It replays committed work idempotently. Segmented recovery reads
and root-verifies one page at a time against one pinned catalog route index. It
can abort an incomplete coordinator or participant set after the recovery
deadline without allocating in proportion to the aggregate manifest.

`RecoverAll` scans non-retired coordinators on all current shards. The shipped
gateway calls it every five seconds.

Recovery matches the routing version, allocation generation, and ownership
epoch that the transaction recorded. The implementation does not prove
recovery across arbitrary later resharding or retired topology history.

## Global-index use

Base-table mutations and global-index mutations use the same protocol. This
keeps the hidden index relation atomic with its base row. A digest check guards
old-document capture during update and delete.

## Implementation references

- `gateway/transaction.go`
- `gateway/recovery.go`
- `gateway/transaction_manifest.go`
- `gateway/writer.go`
- `internal/distributedtxn/codec.go`
- `internal/distributedtxn/manifest.go`
- `internal/distributedtxn/journal.go`
- `shardservice/mutation_batch.go`
