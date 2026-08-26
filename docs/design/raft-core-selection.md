# Raft core boundary

The repository uses `go.etcd.io/raft/v3` version 3.7.0 for the Raft state
machine. VibeDB owns persistence, scheduling, identity checks, transport frame
validation, SQL apply, and resource admission around that dependency.

Internal packages compose this code into the RF3 serving path.
`vibedb-shard serve-rf3` constructs that composition for one externally
prepared stable three-voter group. General SQL remains on the static shard
path; canonical `get` and supported exact-key `exec_batch` requests use RF3.

## Configuration controls

The Raft model uses an explicit configuration. It bounds message size,
in-flight messages, uncommitted bytes, proposal queues, and read state. The
runtime refuses work before an admission bound can be exceeded.

The integration uses full immutable identities. These identities include
cluster, distribution, shard, allocation, group, member, and store lineage.
Mutable ownership and routing fences are separate.

## Persistence order

The runtime drains one `Ready` before it accepts the next `Ready`. It executes
these micro-steps:

1. Capture the `Ready` value.
2. Persist hard state, entries, and required metadata.
3. Send outbound messages.
4. Finish the message stage.
5. Process the snapshot stage.
6. Apply committed entries.
7. Record read states.
8. Advance Raft.

The current immutable-base runtime admits no in-band `Ready` snapshot. It
fails terminally during capture when `HasSnapshot` is set. Certified offline
bases are the only implemented snapshot path.

The runtime sends no message before stable persistence. A retryable message
sink error retains the exact send position. A deterministic WAL or apply error
latches terminal runtime failure.

## Threat boundary

`internal/rafttransport` validates deterministic frames and static identities.
Its internal transport foundation authenticates supplied raw connections with
mutual TLS. It derives the exact binary node ID and cluster trust domain from a
critical private certificate extension. The caller still owns address
discovery, raw socket creation, listener bounds, and certificate operations.

The transport rejects snapshot messages, recursive response graphs,
configuration entries, unknown protobuf fields, and oversized inputs. These
checks limit parser and graph amplification before allocation.

The repository does not provide these product components:

- RF3 artifact preparation or dynamic peer/address discovery
- Certificate enrollment, rotation, or revocation operations
- Online empty-learner snapshot installation and activation
- A membership or snapshot lifecycle controller around the fixed shard server

## Multi-Raft scheduler

`internal/multiraft.Host` is a single-owner synchronous scheduler. It has no
goroutine, wall clock, socket, or client-serving API.

`RunOne` does one bounded unit of work. Ready work has priority. A proposal
unit admits only the currently queued prefix, capped at 64 entries and a 1 MiB
multi-entry coalescing target, before the next group turn captures one `Ready`.
A valid 1–16 MiB proposal occupies its turn alone. The 64 MiB uncaptured-input
limit remains an independent safety ceiling, not a scheduler fairness budget.
The scheduler never waits for another proposal and exposes no batching clock,
so it adds no wall-clock hold. Configuration changes and read barriers require
an empty input window, so they cannot cross a normal-proposal batch. Input
classes and runnable groups use round-robin fairness. Idle groups leave the
runnable queue.

Hard maxima include 4096 groups, 65,536 global queue items, 4096 items per
group, and 1 GiB of global queue and outbox bytes.

## Implementation references

- `go.mod`
- `internal/raftmodel/config.go`
- `internal/raftmember/runtime.go`
- `internal/multiraft/host.go`
- `internal/rafttransport/registry.go`, `frame.go`, and `preflight.go`
- `internal/rafttransport/identity.go`, `stream.go`, and `transport.go`
- `docs/design/raft-peer-transport.md`
