# Distributed transaction protocol

`gateway.Executor.ExecBatch` runs a fixed-participant atomic write across
shards and tables. The gateway proves every statement's owner before it sends
transaction traffic.

## Bounds

- Maximum participants: 64
- Maximum participant mutation payload: 16 MiB
- Maximum coordinator record: 32 KiB
- Maximum participant intent scopes: 256
- Transaction ID: random nonzero 128-bit value

The gateway sorts participants by distribution and shard. The first
participant is the coordinator.

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
coordinator. It replays committed work idempotently. It can abort an incomplete
staging record after its recovery deadline.

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
- `gateway/writer.go`
- `internal/distributedtxn/codec.go`
- `internal/distributedtxn/journal.go`
- `shardservice/mutation_batch.go`
