# Replicated SQL apply

Replicated apply is an internal deterministic mutation path for a bound SQL
root. It is not a public SQL session and is not connected to the shipped shard
command.

## Ownership

A runtime adopts one healthy WAL, one bound SQL database, and one apply claim.
Direct SQL mutation remains fenced while the claim is active.

## Staged child activation

`OpenReplicatedChildStage` claims one bound user collection before any child
row is visible through SQL. It requires an exact child artifact, placement
program, shard range, allocation generation, ownership epoch, routing version,
and apply profile. The claim accepts verified artifact chunks and consecutive
tail batches. It does not expose the collection or the state machine.

`Activate` requires a sealed cutover certificate. It publishes or reuses the
hidden apply collection, initializes the replicated state row, and transfers
the exclusive connector reference to `ReplicatedApply`. It does not copy the
user rows or replace their storage incarnation. SQL sessions remain refused
until a runtime claims the apply owner or the owner closes explicitly.

An invalid artifact-output profile fails before hidden storage publication. An
uncertain catalog publication returns the intended apply identity and keeps the
stage exclusive. An exact retry settles publication and resumes activation.

## Determinism

A replicated command contains immutable binding identity and mutable state
fences. User validation cannot depend on local time, random input, or a network
result.

A wrong immutable binding is terminal. A stale mutable fence records a durable
deterministic stale result in the client session ring.

## Atomic publication

The apply machine updates user data, replicated state, a changed session
header, and a changed retry slot in one publication. A stable session header is
keyed by tenant and client ID. It stores the current client epoch, `RetryHome`,
cumulative `AckThrough` watermark, sequence high-water, status, and configured
retry window. `RetryHome` is the fixed-width routing discriminator retained
through intact-shard ownership transitions. Physical split activation does not
yet migrate session rows.

An active epoch accepts strictly consecutive sequences. An exact retained retry
is idempotent and can advance `AckThrough`. Reuse with a different `RetryHome`,
fingerprint, or `LogicalCommandDigest` is a conflict. A retry older than the
cumulative watermark or fixed ring window returns `ErrRetryRetired` and never
executes.

A session-retire command has no mutations and acknowledges the prior sequence
high-water. With current mutable fences it seals the current epoch. A stale
retirement records a stale-fence result and leaves the epoch active. The next
epoch must be exactly one higher and starts with sequence 1 and `AckThrough` 0.
The next epoch reuses the same fixed ring.

The terminal `uint64` client sequence is reserved for retirement. An ordinary
command at that sequence returns `ErrSessionSequence`. A stale terminal
retirement is an unstored `ErrStaleCommand` refusal. It leaves the session
unchanged so the client can resubmit the same sequence with refreshed fences.

One command permits at most 64 distinct mutations. The frozen apply identity
sets `MaxSessions` and `RetryWindow`. The system image contains one state row,
one header per retained stable identity, and at most `RetryWindow` fixed slots
per identity. Retirement reuses this state. It does not reduce `SessionCount`.
New identities are refused at `MaxSessions`.

The hidden apply collection stores state, session headers, and retry slots as
raw checksum-protected binary values in durable opaque-value mode. It does not
wrap these records in hexadecimal JSON. A slot retains compact fixed-size
result metadata. Completion lookup reconstructs the canonical completion
envelope from the machine binding, session header, and slot. It checks the
supplied fingerprint and `LogicalCommandDigest` before it accepts an exact
retry.

Admission and apply use one indexed session-header point read and at most one
indexed slot point read. Reopen validates the hidden image in one ordered pass.
Its hidden row count and session scan work are bounded by
`1 + MaxSessions + MaxSessions * RetryWindow`, not by the number of applied
operations.

An invariant or corruption error poisons the machine until reopen.

Range-split child activation initializes empty session state and currently
transfers only the child user image and tail. It cannot yet preserve retries
whose `RetryHome` moves to a non-retained child, and it cannot reconstruct an
old completion from a child's new immutable binding without retained origin
metadata. The split path remains non-serving until that migration contract is
implemented and certified.

## Runtime order

The member runtime persists a Raft `Ready` before it sends related messages. It
then applies committed entries, records read states, and advances Raft.

A deterministic WAL or apply failure latches terminal runtime failure.

`ValidateImmutableBaseApplyCapacity` checks the live WAL cut and the bounded
session counts. `AdoptRuntime` does not yet use it. Runtime adoption still calls
the deprecated `ValidateImmutableBaseNoGCCompletionCapacity` suffix qualifier.
It therefore still requires the derived session-ring ceiling to cover the
complete sealed WAL suffix.

## Implementation references

- `internal/replicatedstate/apply.go` and `machine.go`
- `internal/replicatedstate/session_codec.go` and `read.go`
- `internal/raftmember/runtime.go`, `apply.go`, and `apply_capacity.go`
- `sql/driver/replicated_store.go` and `replicated_apply.go`
- `sql/driver/replicated_child_stage.go`
- `internal/rangesplit/activate.go`
