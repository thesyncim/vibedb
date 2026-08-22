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

## Publication identities

Normal command admission and apply use point reads for the completion and
mutation keys. They do not scan the user image. Planning, validation, and digest
work are O(changed keys plus changed document bytes), bounded by the command
mutation limit, and independent of the shard row count.

`DataChainDigest` is the deterministic, history-sensitive transition fence for
the replicated publication. An effective row mutation advances it from the
previous chain, the frozen apply contract, and the sorted exact before-and-after
rows. A command that does not change user rows preserves it.

`ImageDigest` is a separate canonical identity for one validated, ordered user
image. Reopen and import compute it on their cold full-image paths. Snapshot
artifact creation computes it during the required image stream. A caller can
also request an explicit full-image audit from a coherent read snapshot.

The machine does not maintain a canonical incremental Merkle root.
`DataChainDigest` can differ for equal current images that have different
mutation histories. Canonical same-content comparison uses `ImageDigest`, which
still requires a complete ordered scan or an existing complete image stream.

The user and system collections are exclusive state-machine storage. Direct
out-of-band mutation violates the apply contract. Ordinary reopen validates
every user row and the frozen apply contract, but after transition history
exists it cannot derive the expected canonical image from `DataChainDigest`.
Serving certification must compare `ImageDigest` at the same applied cut;
`OpenCandidate` performs that comparison for transferred images.

## SQL apply boundary

User validation must be deterministic. It can return a deterministic
wrong-shard result. Replicated apply cannot depend on local time, a random
value, or a network result.

Direct SQL mutation is not permitted after replicated binding. The apply claim
owns the prepared SQL root.

## Snapshot artifacts

A snapshot artifact streams one coherent system and user image. It has a
hash-chained header, chunks, and footer. Its footer includes the canonical
`ImageDigest` computed during the user-image stream.

The default chunk target is 4 MiB. The minimum chunk size is 4 KiB. A row is
never split between chunks.

Verified cursors permit exact byte-offset resume. The receiver must persist a
cursor checkpoint after the related row effects. The default cursor checkpoint
budget is 64 MiB.

Staging uses caller-owned non-serving collections. `OpenCandidate` validates
state, completions, placement, all user rows, and the artifact `ImageDigest`.
Opening a candidate does not grant membership or serving authority.

The runtime does not transport snapshots through Raft messages. A learner must
receive a certified base through an external offline process. It can then
catch up with append entries.

## Implementation references

- `internal/replicatedstate/types.go`, `machine.go`, and `apply.go`
- `internal/replicatedstate/digest.go` and `read.go`
- `internal/replication/command.go`
- `internal/replicatedstate/snapshot_artifact.go`
- `internal/replicatedstate/snapshot_stage.go`
- `sql/driver/replicated_store.go`
