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
deterministic stale completion.

## Atomic publication

The apply machine updates user data, completion data, and replicated state in
one publication. It derives a completion key from tenant, client ID, client
epoch, and client sequence. An exact retained command is idempotent. Reusing
that tuple with different command content is a conflict.

One command permits at most 64 distinct mutations. The completion table retains
at most 1,048,576 entries.

The current kernel does not garbage-collect completions. `AdoptRuntime`
rejects a WAL and apply pair unless every entry that the immutable-base WAL can
still admit has a remaining completion slot.

An invariant or corruption error poisons the machine until reopen.

## Runtime order

The member runtime persists a Raft `Ready` before it sends related messages. It
then applies committed entries, records read states, and advances Raft.

A deterministic WAL or apply failure latches terminal runtime failure.

## Implementation references

- `internal/replicatedstate/apply.go` and `machine.go`
- `internal/raftmember/runtime.go`, `apply.go`, and `admission.go`
- `sql/driver/replicated_store.go` and `replicated_apply.go`
- `sql/driver/replicated_child_stage.go`
- `internal/rangesplit/activate.go`
