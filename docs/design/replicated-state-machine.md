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
deterministic stale result in the client session ring.

Each stable session record is keyed by tenant and client ID. The record verifies
that identity and stores the current client epoch, `RetryHome`, cumulative
`AckThrough` watermark, accepted sequence high-water, status, and fixed retry
window. `RetryHome` is the fixed-width routing discriminator retained through
intact-shard ownership transitions. This component does not route or migrate
session rows during a physical range split. The client epoch and the stable key
together identify the current session.

The first command for a stable identity, or for its next epoch, has sequence 1
and `AckThrough` 0. An active epoch accepts only the next sequence. A new
sequence cannot regress `AckThrough`. An exact retained retry is idempotent and
can advance `AckThrough` without replacing its ring slot. Reuse of a retained
sequence with a different `RetryHome`, fingerprint, or `LogicalCommandDigest`
is a conflict.

The terminal `uint64` client sequence is reserved for retirement. An ordinary
command at that sequence returns `ErrSessionSequence`.

The logical retirement floor is the greater of `AckThrough` and the sequence
high-water minus the retry-window width, with subtraction clamped at zero. A
retry at or below this floor, or from an older epoch, returns `ErrRetryRetired`
and never executes. A session-retire command has no mutations and acknowledges
the previous high-water. With current mutable fences it seals the epoch. A stale
retirement records `ResultStaleFence` and leaves the epoch active. Only the next
epoch can reuse a sealed stable record, and it must start at sequence 1. A stale
retirement at the terminal sequence is an unstored `ErrStaleCommand` refusal.
It leaves the session unchanged so the client can resubmit the same sequence
with refreshed fences.

Each stable identity owns one session header and at most `RetryWindow` fixed
ring slots. `MaxSessions` is at most 1,048,576. `RetryWindow` is at most 256.
Retirement does not remove the stable identity or decrease `SessionCount`. It
lets the next epoch reuse the same header and slots. Successive accepted
operations for a retained identity do not increase dedupe rows after its ring
is populated. The machine refuses a new stable identity after `MaxSessions`.

User data, replicated state, a changed session header, and a changed ring slot
publish atomically. A committed session refusal or conflict advances the Raft
apply position without executing user mutations or inserting a result slot.
An invariant failure poisons the machine until reopen.

## Hidden record storage

The hidden system collection uses the `ValidationOpaqueBinary` profile and
durable opaque-value mode. State, session headers, and ring slots are raw
checksum-protected binary records. They do not pass through user JSON
validation and do not use hexadecimal JSON wrappers.

A ring slot stores fixed-size result metadata, not a complete completion
envelope. `LookupCompletion` reconstructs the canonical completion from the
machine binding, session header, and retained slot metadata. It compares the
supplied fingerprint and `LogicalCommandDigest` with the slot before it accepts
an exact retry.

The hidden image contains one state row, one row per retained stable identity,
and at most `RetryWindow` slot rows per identity. Reopen validates this image in
one ordered pass. Its session scratch state is proportional to the configured
session count, and its slot work is bounded by `MaxSessions * RetryWindow`.

`ValidateImmutableBaseApplyCapacity` validates this bounded session profile
against a live immutable-base WAL. `AdoptRuntime` uses that proof directly.
Runtime adoption therefore binds the exact WAL/apply cut and fixed session
limits without requiring the retry-ring ceiling to cover the complete sealed
WAL suffix.

## Publication identities

Normal command admission and apply use one indexed point read for the session
header. A retained sequence adds one indexed ring-slot read. User planning uses
point reads for mutation keys. These paths do not scan the hidden or user
image. Mutation planning is bounded by 64 distinct keys. It performs indexed
point reads plus bytewise validation and digest work on the supplied changes.

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
state, session headers, ring slots, placement, all user rows, and the artifact
`ImageDigest`. Opening a candidate does not grant membership or serving
authority.

The separate range-split child artifact currently transfers user rows and an
empty child apply state. It does not partition session headers or slots by
`RetryHome`. Split-safe retry serving therefore remains unimplemented. A
source with any retained session header or slot cannot pass the split
controller's source-seal or catalog-publication gates, and direct transition
construction also rejects it. A serving split must migrate the exact retained
session image while preserving the original completion lineage before it can
publish child ownership.

The runtime does not transport snapshots through Raft messages. A learner must
receive a certified base through an external offline process. It can then
catch up with append entries.

## Implementation references

- `internal/replicatedstate/types.go`, `machine.go`, and `apply.go`
- `internal/replicatedstate/session_codec.go` and `state_codec.go`
- `internal/replicatedstate/digest.go` and `read.go`
- `internal/replication/command.go` and `completion.go`
- `internal/raftmember/apply_capacity.go`
- `internal/replicatedstate/snapshot_artifact.go`
- `internal/replicatedstate/snapshot_stage.go`
- `sql/driver/replicated_store.go`
