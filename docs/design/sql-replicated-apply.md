# Replicated SQL apply

Replicated apply is an internal deterministic mutation path for a bound SQL
root. It is not a public SQL session and is not connected to the shipped shard
command.

## Ownership

A runtime adopts one healthy WAL, one bound SQL database, and one apply claim.
Direct SQL mutation remains fenced while the claim is active.

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
