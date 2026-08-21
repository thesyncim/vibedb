# Raft core selection and threat model

**Status:** current non-serving Raft kernel. The pinned core, synchronous
driver, bounded model/simulator, static-snapshot append-only WAL, exact member
runtime, in-process Multi-Raft host, and an ordinary-message frame/roster
validator that accepts a caller-supplied authenticated NodeID are executable.
The runtime/host now surface model-checked context-free configuration proposals
and exact quorum-safe `ReadIndex` outcomes, but no serving API consumes them.
There is no serving replication, failover, in-band snapshot transport, or live WAL compaction
path, peer authentication, or network transport.

## Exact selection and provenance

VibeDB uses the upstream [`etcd-io/raft`](https://github.com/etcd-io/raft)
protocol core through module `go.etcd.io/raft/v3` at exactly `v3.7.0`:

| Property | Pinned value |
| --- | --- |
| upstream tag | `v3.7.0` |
| tag commit | `b867cf13f6bc0dae21204302df97bc2355c3af55` |
| Go module sum | `h1:BGzlwx07bLv8PW6OU5HObuz1y4hlPZUXA07pM1mPUh4=` |
| Go module-file sum | `h1:6gX6T2X907DjnjsFLODnTxba77stjs84W9gTTI0GUNA=` |
| upstream license | Apache License 2.0 |
| local license copy | [`LICENSE-ETCD-RAFT`](../../LICENSE-ETCD-RAFT) |
| license SHA-256 | `cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30` |
| runtime dependency | `google.golang.org/protobuf v1.36.11`, module sum `h1:fV6ZwhNocDyBLK0dj+fg8ektcVegBBuEolpbTQyBNVE=`; also the Raft core's sole runtime module dependency |
| runtime dependency notices | BSD-3-Clause [`LICENSE-PROTOBUF`](../../LICENSE-PROTOBUF) and [`PATENTS-PROTOBUF`](../../PATENTS-PROTOBUF) |
| runtime notice SHA-256 | license `4835612df0098ca95f8e7d9e3bffcb02358d435dbb38057c844c99d7f725eb20`; patents `96f408bfae65bf137fc2525d3ecb030271c50c1e90799f87abf8846d8dd505cc` |
| local wrapper | `internal/raftmodel` |

The module's tag contains no `NOTICE` file. The local license is byte-for-byte
the upstream `LICENSE` at the pinned tag. The protobuf runtime named above is
imported directly by the integration and is also the core's only runtime module
dependency; both of its distributed notice files are copied byte-for-byte.
Test-only dependencies recorded in `go.sum` are not linked into VibeDB. The
dependency is consumed unmodified; no upstream source is copied into VibeDB.

The selection is deliberately narrow. For a fixed internal election-timeout
sample and exact input order, the core provides deterministic Raft state
transitions, log replication, quorum tracking, joint configuration machinery,
snapshots, flow control, leadership transfer, and `ReadIndex`. The core itself
samples election-timeout jitter from `crypto/rand`; fresh nodes with the same
durable state and tick sequence need not choose the same deadline. The
application supplies wall-clock tick scheduling, but not that private jitter.
The core does **not** provide durable storage, networking, peer authentication,
authorization, encryption, proposal admission, command and completion
encoding, state-machine apply/publication, snapshot files, placement policy,
range fencing, or cross-shard transactions. Each omitted facility remains
VibeDB's responsibility and has a separate gate.

## Audited configuration

`internal/raftmodel.NewConfig` is the only baseline constructor. Its test
enumerates every field in upstream `raft.Config`, so adding a field or changing
an option requires a deliberate audit rather than inheriting new zero-value
behavior.

| `raft.Config` field | Selection | Reason |
| --- | --- | --- |
| `ID`, `Storage`, `Applied` | supplied per member | Durable member identity, storage, and replay cut cannot be global defaults. Upstream validates reserved IDs and storage presence. |
| `HeartbeatTick` | `1` | One abstract tick between leader heartbeats. Wall time is supplied outside the core and is not a safety input. |
| `ElectionTick` | `10` | Upstream's recommended 10:1 election-to-heartbeat ratio; deployment tick duration remains separately bounded and tested. |
| `AsyncStorageWrites` | `false` | Use the smaller synchronous `Ready`/`Advance` contract first. Async local append/apply messages remain unsupported until their additional ordering and crash states are modeled and qualified. |
| `MaxSizePerMsg` | 1 MiB | Bounds normal append batching and retry cost. A single oversized entry still requires independent proposal and transport rejection. |
| `MaxCommittedSizePerReady` | 4 MiB | Soft target for normal apply batching above the one-entry progress case. One independently admitted 16 MiB entry may exceed it and must still make progress. |
| `MaxUncommittedEntriesSize` | 64 MiB | Rejects new proposals rather than allowing unbounded leader-log growth without a quorum. |
| `MaxInflightMsgs` | `64` per follower | Bounds optimistic-replication message bookkeeping. |
| `MaxInflightBytes` | 8 MiB per follower | Soft watermark rather than a hard ceiling. A follower just below the watermark may admit one 16 MiB entry message, so transport and process budgets cover nearly 24 MiB plus protobuf overhead per follower. |
| `CheckQuorum` | `true` | A leader that cannot confirm quorum activity steps down. This improves fail-closed behavior but is not a substitute for proposal fencing. |
| `PreVote` | `true` | A returning partitioned member cannot increment terms before checking whether it could win. |
| `ReadOnlyOption` | `ReadOnlySafe` | Linearizable `ReadIndex` confirms a quorum; it does not rely on a clock-drift bound or leader lease. |
| `Logger` | `nil` | Upstream installs its default logger; no per-group adapter is installed. |
| `TraceLogger` | `nil` | Optional state tracing is disabled. Normal builds compile the hook as a no-op. |
| `DisableProposalForwarding` | `true` | Followers reject proposals. A future serving path must route, validate identity and fences, canonicalize, and admit at the current leader, with explicit retry after leadership changes. |
| `DisableConfChangeValidation` | `false` | Retain upstream propose-time validation. The local driver also admits a configuration proposal only at a fully applied predecessor and replays upstream `Changer` validation before proposal and apply; topology-bound context validation remains open. |
| `StepDownOnRemoval` | `true` | A removed or demoted leader immediately stops acting as leader. Request fencing remains independently mandatory. |

All sizes are per Raft group. They prevent accidental unbounded queues but are
not hostile-input limits: codecs, RPC framing, scheduler queues, snapshot
streams, and aggregate Multi-Raft memory must each enforce their own smaller
or equal budgets.

## Integration safety contract

One serialized owner drives each `RawNode`; no concurrent caller may feed
ticks, messages, proposals, reads, or membership changes. With synchronous
storage writes, each `Ready` batch is processed under this minimum order:

Before `CaptureReady`, the driver admits a bounded aggregation window only for
Ready work caused by observed protocol input: at most 64 calls, 4,096 input
units, and 64 MiB of Entry/Snapshot payload bytes. Every call costs at least one
unit and an inbound append costs one unit per Entry. A Ready already exposed at
construction or after `Advance` blocks further input until capture.
Configuration proposals remain stricter and require no pending Ready. Capture
closes the input window, and no new protocol input is accepted until that Ready
is fully advanced. Combined with complete message and proposal byte limits,
this keeps every durability-bearing Ready inside the disk store's sealed entry
and byte envelope.

1. append entries and persist `HardState` and any snapshot to the member's
   durable store; a snapshot always needs its crash-safe publication protocol,
   independently of `Ready.MustSync`, while that flag controls the required
   entry/`HardState` sync barrier;
2. only after the prerequisite persistent state is safe, release outbound
   messages to the caller-supplied sink; no authenticator is included, so a
   serving layer must authenticate the peer before forwarding them;
3. install/apply a snapshot first, then apply its committed suffix entries in
   log order, atomically publishing the new root, applied index, exact
   `ConfState`, and durable completion before acknowledging a command;
4. expose a `ReadState` only after the published applied index is at least its
   read index; and
5. call `Advance` only after every required part of that batch has completed.

Restart also closes the persistence-to-install crash cut explicitly: when the
stable snapshot base is newer than or equal to the state machine's atomically
published cut, node recovery idempotently reconciles that exact durable
snapshot before it constructs `RawNode`. Equality is not trusted from index and
configuration alone: the state machine verifies its persisted snapshot
identity/manifest, logical digest, and replica-set version and rejects different
bytes or a regressed publication at the same cut. A failure leaves the member
non-serving and retryable; recovery never starts the protocol core with an
applied index below its durable log base.

Each member store also durably allocates a strictly increasing, never-reused
node-incarnation counter before a reconstructed driver accepts input. Together
with Ready's per-incarnation sequence, that counter is the persistence retry
identity and stale-process fence; it is not a random startup token.

Configuration entries are applied exactly once and passed to
`ApplyConfChange` in committed order. The executable foundation currently
accepts context-free add/remove/promote operations only after the durable log
and published predecessor are caught up, and fail-stops rather than permitting
an upstream changer panic. It deliberately rejects `Context` and metadata-only
updates until the topology-authority binding and rejection-as-application-no-op
port are executable. Health, wall time, topology observations, or optimizer
output may propose a later qualified configuration command but may not alter
the deterministic apply decision. The exact committed/applied `ConfState`, not
a topology cache, defines the acknowledgement quorum.

## Threat model and fail-closed boundaries

The qualified target is crash-fault tolerance, not Byzantine consensus. It
models process and host crashes, restart at any persistence boundary, message
loss, duplication, delay, and reordering, partitions, clock pause/jump/drift,
slow or failed disks, stale topology, and retries after ambiguous outcomes.
Safety assumes:

- a quorum does not permanently lose or equivocate about data already reported
  durable;
- a serving layer must supply cryptographic member identity so an
  unauthorised process or restored foreign store cannot speak as a voter;
- persistent storage obeys the append/truncate, atomicity, and sync results it
  reports, or the integration detects corruption and refuses to serve;
- the command/state-machine apply function is deterministic; and
- at most one serialized driver owns a member incarnation at a time.

Raft does not defend against compromised voters, forged authenticated peer
identity, malicious payloads, memory corruption, a quorum of corrupt disks,
operator-forced incompatible histories, or side-channel/resource-exhaustion
attacks. AEAD envelopes, checksums, generation/incarnation binding, bounded
decoders, admission control, and store-open fencing address parts of that
surface outside the core; none upgrades the protocol to Byzantine fault
tolerance.

Clock readings affect only election and availability timing. The selected
`ReadOnlySafe` path never treats a wall-clock lease as a linearizability proof.
Arbitrary drift may therefore delay elections or trigger leadership churn, but
cannot authorize a read or write. Deadline expiry reports uncertainty to the
caller and never cancels a proposal that may already commit.

`CheckQuorum`, `PreVote`, and `StepDownOnRemoval` reduce stale-leader exposure,
but the service must still check cluster, shard, member/store incarnation,
current term/leadership, ownership/protection epoch, route generation, and
range state on every proposal and before publishing its result. Until the
durable driver and simulator gates pass, no API may expose a Raft-derived term,
commit sequence, HA acknowledgement, or linearizable distributed-read claim.

## Unsupported extensions

- `AsyncStorageWrites` and its local append/apply message protocol;
- lease-based reads and any clock-derived read fast path;
- follower proposal forwarding;
- unbounded message, inflight, uncommitted, or apply queues;
- disabling upstream configuration-change validation;
- more than one pending configuration change per group;
- configuration-change `Context`, metadata-only member updates, or topology
  authorization before the context-aware apply port exists;
- witness voters, non-Raft voting roles, weighted quorums, flexible quorums,
  or custom election/commit rules;
- protocol-source forks or local changes to the selected core; and
- deriving cross-shard serializability, placement survival, range ownership,
  or backup guarantees from per-group Raft alone.

Changing the module version, checksum, field inventory, any table value, or
an unsupported extension is a new core-qualification event. It requires
upstream changelog/source review, license comparison, deterministic and fault
simulation, wire/storage compatibility review, and a freshly recorded pin.

The module declares `go 1.26` and an upstream development-toolchain preference
for `go1.26.4`. VibeDB supports the Go 1.26 patch line rather than forcing that
preference on its main module; this initial qualification ran on `go1.26.0`,
while CI's `1.26` selector tracks current patch releases.

## Current executable non-serving kernel

Deliberately separate packages now make the contract testable without
claiming a production replicated store:

- `internal/raftmodel` owns one actual upstream `RawNode`, proposal/read
  admission, exact `Ready` sequencing, retry-safe persistence IDs, ordered
  configuration application, reader publication, and crash recovery from the
  published applied cut. Its micro-steps expose cuts between individual
  outbound messages, committed entries, and `ReadState` values.
- `internal/raftsim` owns deterministic choice and queue primitives, bounded
  logical time, a checksummed scenario-bound trace, a logical atomic store, an
  exact-prefix state machine, and RF1/RF3 cluster execution with message loss,
  duplication, reordering, partitions, definite/ambiguous persistence failures,
  process crash, and restart. Current histories are scripted or generated by a
  test harness and replay from their complete trace; a package-level
  seed-to-trace runner remains open. The model uses explicit recorded
  `Campaign` decisions, permits `Tick` only on an observed leader, and never
  pretends the core's private randomized follower/candidate timeout is
  seed-controlled. Leader heartbeat/check-quorum timing uses the fixed
  configured election threshold.
- `internal/replication` freezes pure command and completion byte envelopes
  consumed by the replicated apply boundary. The codecs establish bounded
  canonical data, not a WAL, blob store, transport, or serving path.
- `internal/raftstore` implements the identity-bound, encrypted,
  preallocated append-only `StableStore` and exact Ready retry boundary on
  Linux and macOS. It has a static bootstrap snapshot and no compaction,
  transport, serving path, or general anti-rollback witness; its full qualified
  contract and quarantine limits are in [Raft WAL](raft-wal.md).
- `internal/raftmember.Runtime` exclusively adopts one exact WAL, SQL root,
  apply claim, and Node; mints a fresh incarnation; enforces
  `AdmitCommand` -> `ReserveReady` -> `Node.Propose`; and drives one explicit
  persistence-before-send Ready micro-step at a time. It also exposes the
  existing model-checked configuration proposal port, a `ReadIndex` issue port,
  detached applied publication, and exact terminal read outcomes. `ReadIndex`
  does not consume worst-case WAL reservation because it cannot append entries
  or advance `HardState`; its empty Ready still proves the durable namespace
  before messages are released. Runtime rejects snapshots and exposes no raw
  component or serving API.
- `internal/multiraft.Host` owns a bounded set of independent range runtimes.
  A single-range deployment adds one group to the same Host. A FIFO runnable
  queue and rotating input classes provide operation-count fairness without a
  goroutine, timer, or polling scan per idle group. It is an in-process outbox,
  not peer transport, and one synchronous slow operation can still block its
  caller. Direct membership/read controls are deliberately not queued: a
  topology/serving coordinator must drain and retry `ErrReadyPending`, so stale
  control intent never lingers behind unrelated group work. `RunOne` transfers
  completed read outcomes exactly once to its caller.
- `internal/rafttransport` owns a bounded immutable caller-supplied static
  member/role/NodeID roster and one canonical ordinary-message frame. One
  registry is designed for one node-pair connection to multiplex every range
  group. The
  decoder checks full group lineage, replica-set roster digest, peer/member
  routing, roles, flat protobuf shape, and exact bounds before allocation.
  It accepts an already-authenticated NodeID; it does not implement TLS,
  enrollment, sockets, reconnect, flow control, dynamic membership, or
  snapshot transfer. Configuration entries are rejected while the roster is
  static.

The simulator's `MemoryStore` is an indivisible logical durability oracle, not
an implementation template. It cannot represent torn sectors, fsync lies,
snapshot files, compaction, or recovery-journal interaction. Its purpose is to
prove integration ordering and make each logical failure trace byte-exact.
The disk store exercises the same driver boundary against a crash-tested
static-snapshot format. Runtime snapshots, compaction, and serving remain
prohibited until their separate gates exist.

Simulator proposal references and completion lookups are model-local. A
`RespondProposal` event means that a live replica has found the exact reference
in its applied prefix after an explicit client retry; it may therefore succeed
after process restart. This proves the acknowledgement cut for the logical
model, but does not exercise the frozen production command fingerprint,
completion codec, duplicate re-proposal path, retry-home routing, or completion
GC. Static cluster scenarios also do not propose configuration entries. Driver
tests exercise ordered configuration apply, while the simulator's prefix chain
currently records the resulting `ConfState`, not distinct configuration command
context bytes. Context-aware topology authorization and configuration-prefix
refinement remain open gates.

Current executable bounds include nine voters per modeled group, 4,096 exact
applied records, at most 4,096 scenario proposal inputs, 64 MiB of aggregate
scenario proposal payloads, 65,536 network envelopes, 128 MiB of concurrently
retained encoded network data, 16 MiB per logical snapshot, 1,024 pending
reads, 64 protocol calls, 4,096 input units, and 64 MiB of payload per
uncaptured Ready, and 262,144 trace events. The driver independently caps an
applied `ConfState` at 64 total incoming/outgoing voter and learner references;
joint membership counts both voter sets. These are integration/model ceilings,
not promises that a production deployment should use the largest value
simultaneously.
