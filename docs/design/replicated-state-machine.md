# Replicated state machine

`internal/replicatedstate` applies deterministic replicated commands to one
hidden system collection and one user collection. It is a non-serving internal
component.

## Command contract

A command carries two kinds of identity:

- Immutable binding contains cluster ID, cluster incarnation, topology
  recovery epoch, distribution, shard, allocation generation, shard
  incarnation, and group ID.
- Mutable fences contain replica-set version, active-policy generation,
  protection epoch, ownership epoch, schema generation, routing version, and
  route generation.

Member ID, WAL store ID, SQL log ID, and SQL-root layout belong to the
surrounding WAL and SQL reopen identity. They are not fields in a replicated
command.

An immutable mismatch is terminal. A stale mutable fence produces a durable
deterministic stale completion.

One command can contain at most 64 distinct mutations. The machine retains at
most 1,048,576 completions. Completion identity makes an exact retry
idempotent and rejects a conflicting retry.

The current kernel does not garbage-collect completions. Runtime adoption
rejects a WAL and apply pair unless every entry that the immutable-base WAL can
still admit has a remaining completion slot.

User data, completion data, and state publish atomically. An invariant failure
poisons the machine until reopen.

## SQL apply boundary

User validation must be deterministic. It can return a deterministic
wrong-shard result. Replicated apply cannot depend on local time, a random
value, or a network result.

Direct SQL mutation is not permitted after replicated binding. The apply claim
owns the prepared SQL root.

## Snapshot artifacts

A snapshot artifact streams one coherent system and user image. It has a
hash-chained header, chunks, and footer.

The default chunk target is 4 MiB. The minimum chunk size is 4 KiB. A row is
never split between chunks.

Verified cursors permit exact byte-offset resume. The receiver must persist a
cursor checkpoint after the related row effects. The default cursor checkpoint
budget is 64 MiB.

Staging uses caller-owned non-serving collections. `OpenCandidate` validates
state, completions, placement, all user rows, and the logical digest. Opening a
candidate does not grant membership or serving authority.

The runtime does not transport snapshots through Raft messages. A learner must
receive a certified base through an external offline process. It can then
catch up with append entries.

## Implementation references

- `internal/replicatedstate/types.go`, `machine.go`, and `apply.go`
- `internal/replication/command.go`
- `internal/replicatedstate/snapshot_artifact.go`
- `internal/replicatedstate/snapshot_stage.go`
- `sql/driver/replicated_store.go`
