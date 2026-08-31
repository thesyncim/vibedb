> [!CAUTION]
> **Unreleased development and qualification software only.** Distributed VibeDB has no
> compatibility promise, production support boundary, or production SLA. Run every component
> and artifact from the **exact same build and source revision**. Formats, protocols, commands,
> fences, and recovery may break at any time. Qualification is not a rolling-upgrade, failover,
> or production-readiness guarantee.

# Distributed internals and operation

This page explains the boundaries to preserve while bringing up, observing, or recovering the
current distributed path. It is not a wire-format specification or a supported internal API.

## Start with the deployment model

“Static” and “RF3” describe different things. Treating them as synonyms leads to unsafe
recovery and misleading availability claims.

| Term | What it means | What it does not mean |
| --- | --- | --- |
| Embedded or local operation | One process owns a local database without the distributed RF3 serving path. | A replicated database or a quorum-backed fallback for RF3. |
| Static bootstrap | The immutable index-one snapshot and initial `ConfState` from which one Raft group starts. | Membership must remain static forever, or that the group has one member. |
| Static peer enrollment | A bounded list of authenticated nodes that may participate in configured groups. | Permission to send traffic; committed group membership grants that authority. |
| RF3 serving policy | A steady-state catalog, data, or request-ledger group has three voters and commits through a majority. | A property built into the generic Raft kernel. The kernel can represent other voter counts. |

Replacement temporarily has four voters and returns to three only after catch-up, safe
leadership, and source removal. Generic tests also exercise RF1 and RF2; that does not make a
non-RF3 layout a supported distributed deployment.

## Follow one operation through the ownership chain

The distributed path is a chain of single-owner boundaries. There is no hidden layer that can
repair an invalid route, reinterpret a command, or make a stale replica authoritative.

```mermaid
flowchart LR
    C[Client request] --> G["Gateway<br/>pin catalog generation"]
    G -->|exact route and command fence| O["Replica Owner<br/>serving admission"]
    O --> H["Multi-Raft Host<br/>bounded scheduling"]
    H --> R["Group Runtime<br/>WAL + SQL apply + Raft node"]
    R --> N[Raft Ready]
    N -->|persist first| W[(Authenticated WAL)]
    N -->|then send| P[Peer transport]
    N -->|then apply| M[(Replicated state)]
    M -->|published result| O
```

The boundaries have distinct jobs:

1. The gateway pins one catalog generation and sends exact ownership, schema, route,
   replica-set, and policy coordinates.
2. The replica `Owner`, sole caller of its host, rejects any command or serving-fence mismatch.
3. The host gives runnable groups bounded Ready, input, proposal, and logical-tick turns.
4. One runtime owns one group identity, WAL namespace, SQL apply root, state machine, and node;
   physical and logical incarnations prevent accidental path adoption.
5. The node persists Ready, releases messages, applies and publishes, then advances.

An accepted proposal is not committed, and a transport write is not proposal acceptance. A log
entry is not externally complete until deterministic apply and result publication finish.

## Catalog generations are operation leases

Catalog publication is atomic: a reader sees one whole old or new immutable generation, never
a mixture. Planning, endpoints, ownership, route versions, and scatter targets share that cut.

An operation holds its lease through return. A stale-fence retry pins a newer generation and
rebuilds from the original input, while earlier leases remain held so drains see old attempts.

This gives two important rules:

- Never splice an endpoint, ownership epoch, or schema relation from a newer catalog into an
  already encoded command. Forwarding, where explicitly supported, preserves the exact old
  command bytes and checks a catalog-authorized forwarding window.
- Treat generation drain as a metadata and execution fence, not as a database snapshot. Two
  groups read with separate Raft barriers do not become one global point-in-time read merely
  because their routes came from one catalog generation.

Publication only moves forward. Exact-predecessor cutovers compare-and-publish; a stale
controller must re-observe and replan, not publish an unrelated higher generation.

## Raft persistence and reads

The current profile uses pre-vote, quorum checking, safe ReadIndex, heartbeat tick 1, and
election tick 10. Proposal forwarding is disabled. Ticks are logical inputs supplied by the
owner; the consensus core does not sample wall-clock time or run an autonomous ticker.

Ready processing is an ordered durability protocol:

1. Capture one Ready and assign its `(node incarnation, Ready ID)`.
2. Persist entries and hard state in their required order, then synchronize the WAL.
3. Release outbound messages. A failed callback may cause the same message to be sent again.
4. Apply committed entries serially and atomically publish state, membership, and results.
5. Release eligible read barriers and advance the Raft node.

The same Ready ID may be retried only with the exact same bytes. Persistence failure is
retryable while that captured batch remains owned. Apply failure is terminal for the runtime:
publication may be ambiguous, so restart and recovery must reconcile WAL and applied state.

### Read modes

| Read path | Admission | Guarantee |
| --- | --- | --- |
| Leader data read | Exact serving fence, current leader/term, quorum-backed ReadIndex, local apply through the barrier, storage-generation lease | A group-local leader/quorum cut. It is not a multi-group snapshot. |
| Route-gate or catalog-authority read | Leader and quorum-backed ReadIndex, exact authority revision | Linearizable authority observation for that group. |
| Explicit follower read | Exact serving fence and term, local applied index at or above the caller’s floor | A local applied-floor read only. **It is not linearizable and has no bounded-staleness promise.** |

If leadership or node incarnation changes before a ReadIndex barrier becomes locally applied,
the barrier is lost and the read fails. It is never silently answered from the new leadership
state. Data reads also filter transferred ownership before joins, aggregation, and `LIMIT`, and
fail closed when a group-level active transaction intent blocks the requested cut.

## WAL generations and snapshots

The Raft WAL is bounded, preallocated, encrypted/authenticated, digest-chained, and tied to an
immutable placement identity. The default profile is a 256 MiB file with 80 MiB maximum records,
65,536 records, 1,048,576 entries, and 128 MiB live data; configuration may choose lower valid
bounds. Capacity exhaustion is an admission failure, not permission to overwrite live history.

Startup authenticates the family manifest, header, dual current slots, records, key, recovery
epoch, and SQL/apply binding before adoption. One torn current slot can be selected around, but
the process cannot prove whether the damage was a crash tear or post-ack rollback without an
external anti-rollback witness. Such a root is quarantined and must not serve or rejoin merely
because local recovery found a plausible higher slot.

Snapshots do not travel as ordinary Raft `MsgSnap` messages. The ordinary node, runtime, WAL
Ready path, and peer frame preflight reject `MsgSnap`.

The supported mechanism is a separate authenticated snapshot-data traffic class:

1. Capture a bounded collection artifact and certificate at an exact applied index and term.
2. Transfer and stage it in a non-serving target. Staged rows carry no routing or serving
   authority.
3. Verify the complete image and install/checkpoint it before making a new WAL base selectable.
4. Publish and activate the sibling WAL generation, then adopt it through normal runtime
   recovery.

Artifact data is therefore out-of-band; only the certified snapshot/base relationship enters
the Raft/WAL lifecycle. A crash during staging can replay at most one chunk. A crash during WAL
generation activation is settled from the authenticated family state before runtime adoption.
There is no cluster-wide or multi-group snapshot protocol here.

## Peer transport: authenticate, bound, then distrust delivery

Internal peers use mutual TLS 1.3. A critical certificate extension carries the exact cluster
ID, cluster incarnation, and node ID; subject names, DNS names, and common names do not grant
peer authority. Ordinary Raft, snapshot data, shard-native, SQL, client, and control traffic use
separate ALPN classes. Handshake and stream deadlines are mandatory.

TLS authenticates a node. The per-group registry then checks whether that node is enrolled and
whether the exact committed roster/version authorizes its voter or learner role. Frames are
checked for group, source, destination, trust domain, roster, role, size, and transition grant
before protobuf allocation or Raft admission.

Delivery remains a lossy, duplicating boundary:

- `Send` means validation, bounded queue reservation, and local ownership of encoded bytes.
- A completed socket write increments local counters; it is **not** a receiver ACK, Raft ACK,
  commit acknowledgement, or apply acknowledgement.
- A write failure retains the batch, so retry can duplicate a frame.
- Exact transport backpressure may cause the owner to discard one ordinary packet to avoid
  head-of-line blocking. Raft retransmission is expected to repair it.
- Queue, byte, coalescing, peer, and deadline limits fail closed; they are not elastic buffers.

Certificate validity and I/O deadlines are explicit wall-clock seams. Raft ordering, leases,
and recovery do not derive authority from that clock.

## RF3 quorum and replica replacement

In a healthy steady-state RF3 group, any two voters form a quorum. One reachable voter cannot
elect or commit. It must refuse, time out, or return an outcome-unknown condition rather than
claim progress.

| Reachable voters | Expected behavior |
| ---: | --- |
| 3 | Elect and commit, subject to normal fencing and capacity. |
| 2 | Elect and commit with reduced fault tolerance. Restore the third replica before another fault. |
| 1 | No safe commit. Reads requiring ReadIndex and all writes fail or time out. |
| 0 | Unavailable. Recover processes/storage; do not manufacture membership. |

Replacement is an externally authorized, resumable sequence—not automatic joint consensus:

```mermaid
stateDiagram-v2
    [*] --> Grant: linearizable catalog grant
    Grant --> Learner: add target
    Learner --> CaughtUp: active, no pending snapshot, match >= commit
    CaughtUp --> RF4: promote target voter
    RF4 --> Transferred: move leadership if source is leader
    Transferred --> RF3: remove source voter
    RF3 --> [*]: catalog observes exact final roster
```

The grant binds one source, one target, the initial three voters, catalog generation, and exact
transition digest. Adding the learner does not authorize removal. Promotion must be durably
observed. Removal is accepted only from the four-voter intermediate state, with no learner or
joint configuration, after an exact same-term leadership transfer when needed.

Absence of a grant is not by itself revocation; revocation requires a linearizable catalog
observation. After restart, durable membership returns, but volatile leadership does not. The
group needs real peer traffic and heartbeats before it can serve as leader again.

## Retries and outcome-unknown

Retry safety exists at three layers:

| Layer | Purpose |
| --- | --- |
| In-flight waiter registry | Coalesces the exact same local attempt onto one enqueue and shares settlement with bounded waiters. |
| Replicated session ring | Retains recent sequence outcomes under tenant/client/session epoch, retry home, fingerprint, logical digest, and cumulative ACK. |
| Durable request ledger | Recovers multi-step or cross-group work from authenticated request identity through planning, preparation, terminal outcome, ACK, and reclamation. |

The safe operator/client rule is simple: after possible admission, retry the **exact canonical
request bytes with the same identity**. Do not create a new request ID, change the fingerprint,
or infer failure from a lost connection.

- A refusal before local core admission is safe to reroute or rebuild as directed by its typed
  error.
- Cancellation or connection loss after registry admission returns outcome-unknown with the
  exact retry bytes because the entry may still commit and apply.
- An exact duplicate within the retained session window returns the retained result. Changed
  bytes under the same logical identity conflict.
- Cumulative ACK advances retention; a sequence below the retained floor is retired, not
  guessed. Session release validates and removes the full bounded retry ring atomically.
- Durable ledger ACK is final only after all pre-ACK durable bytes are reclaimed. Work wider
  than 256 participants proceeds in monotone waves; 256 is a wave bound, not a workflow bound.

Transport retry and request retry solve different problems. Re-sending a peer frame repairs
Raft delivery. Re-submitting the exact client command settles whether the logical operation
committed and what result it produced.

## Static indexed updates and deletes

> [!IMPORTANT]
> The checked-in static listener exposes this path only through `exec` for one
> single-base-owner statement. Independently placed index writes may add
> transaction participants, but general multi-statement or cross-base-shard
> static `exec_batch` is not exposed. Public `exec_batch` is reserved for
> authenticated durable RF3 and never takes an unsequenced fallback.

The static gateway can maintain independently placed global indexes for a
computed `UPDATE`, or for a `DELETE`, without evaluating an update assignment
at the coordinator:

1. The base shard preflights the complete selected batch and returns canonical
   primary key, before-document, and after-document images without publishing.
   A delete has a null after image. The capture is row/byte bounded and requires
   both read and write authority even though its execution mode is read-only.
2. The gateway validates every image and base route, derives old index deletes
   and new index puts, and retains sorted primary keys plus SHA-256 before- and
   after-image digests. It does not durably retain the full postimages.
3. The base participant is staged as old-key/digest precondition, original SQL,
   then new-key/digest check. A serializable prepare executes that sequence and
   rolls it back, proving that the SQL still produces the captured postimages
   before any participant can commit.
4. After the coordinator decision, apply executes the staged batch again. Base
   work remains in authored order; on each final index participant every delete
   is ordered before every put, including across statements, so an atomic
   unique-key swap can release old claims first.

All global-index lifecycle states are write-maintained; only `Ready` is
read-plannable. A computed right-hand side is evaluated once during capture,
but the SQL is executed again during prepare and apply, so this is not an
exactly-once evaluation contract.

This path is static-only. Strict RF3 durable transactions accept
whole-document and direct declared-column updates, but still reject computed
assignments because the durable RF3 replay program does not retain their
evaluated postimages. The new static path has local and in-process failure-
atomicity tests; it has no new external process, crash, or recovery
qualification gate.

## Transactions and logical clocks

> [!WARNING]
> Current static distributed-transaction journal compaction omits durable
> coordinator recovery-pulse records. Reopening after compaction can reset that
> pulse state. Do not treat the compacted journal as qualified recovery
> authority for an in-flight coordinator.

Replicated commands carry deterministic bytes and explicit fences. They do not contain local
time, SQL text, a serialized planner, a physical WAL generation, or a Raft term/index chosen by
the client. At apply, transaction control, participant intents, relation mutations, and state
publication are committed atomically within that group. Conflicting ordinary mutations fail
closed while an active intent owns the affected scope.

The request ledger coordinates recoverable cross-group work, but it does not create a global
clock. Its program is bound to exact catalog/schema/routing identities and progresses through
monotone durable revisions. Recovery replays those durable facts rather than process memory.

Time-like values have separate domains:

- Raft elections and maintenance use logical ticks.
- Catalog publication, ownership, route gates, and transaction records use monotone revisions
  or epochs.
- Execution-pin leases are catalog applied-index intervals, not wall-clock durations. External
  effects must re-read the exact lease certificate and stay inside that interval.
- The bounded transaction conflict clock implements first-committer-wins. History overflow or
  uncertainty causes a conservative conflict; it never permits an unproven commit.
- X.509 validity, network deadlines, and caller-supplied session deadlines are outer wall-clock
  inputs. They do not provide cross-group timestamp ordering.

There is no global MVCC timestamp, global snapshot read, or wall-clock-derived consensus lease.

## Failure handling

| Observation | Meaning | Safe response |
| --- | --- | --- |
| Not leader or leadership lost | The local serving term is no longer authoritative. | Refresh the exact route/fence and retry under the same logical identity. |
| Outcome unknown / connection lost after admission | Commit and apply may still happen. | Retry identical canonical bytes and identity; query durable ledger state when applicable. |
| Transport backpressure | A bounded local queue refused work; one ordinary Raft packet may have been dropped. | Relieve pressure and let Raft repair; do not count `Send` as delivery. |
| Retryable Ready persistence error | The exact captured batch is still pending. | Preserve the root, restore the storage condition, and drive another owner pulse/ingress. |
| Apply failure | Durable publication may be ambiguous. | Stop that runtime and recover from WAL plus applied state; do not continue in place. |
| Torn-slot quarantine | Local WAL recovery lacks an external anti-rollback proof. | Keep the replica non-serving and rebuild or certify it through the supported recovery path. |
| No RF3 majority | The group cannot safely commit or perform ReadIndex. | Restore connectivity/processes; never force a voter set from local observations. |
| Snapshot staging interruption | The target is incomplete and non-serving. | Resume the certified artifact transfer; do not expose staged rows. |
| Stale catalog or ownership fence | Topology changed after planning. | Re-pin a newer catalog generation and rebuild from the original logical request. |

## Hard bounds worth monitoring

These are admission ceilings, not sizing recommendations. A manifest may configure smaller
values, and front-end commands may impose tighter limits than the reusable core.

| Resource | Current hard/default boundary |
| --- | --- |
| Multi-Raft core | Per host/lane: 4,096 groups, 65,536 queued items, 1 GiB queued bytes; 1–64 power-of-two lanes, with aggregate capacity scaling by lane count. |
| Group membership | At most 64 roster references in the generic registry; RF3 steady state is three voters. |
| Raft command/input | 16 MiB proposal command, approximately 17 MiB inbound message, 4 MiB committed Ready batch. |
| Peer transport | 4,096 peers, 65,536 queued frames, 1 GiB queued bytes; 256-frame coalescing. |
| WAL defaults | 256 MiB file, 80 MiB record, 65,536 records, 1,048,576 entries, 128 MiB live bytes. |
| Read barriers | 1,024 pending contexts and 256 KiB aggregate context bytes per node. |
| Proposal results | 65,536 outstanding identities/attempts/waiters, 64 attempts per identity, 128 MiB retained completions. |
| Request ledger | 32 KiB inline plan, 512 KiB pages, 1 GiB aggregate plan, 256 targets per physical wave. |

Watch queues, WAL generations, read barriers, retained results, ledger cleanup reserve,
leadership churn, and catalog drains. Raising a limit requires complete same-build qualification.

## Current non-guarantees

Do not infer any of the following from the implementation or its tests:

- rolling or mixed-version compatibility, downgrade, or online format migration;
- a production availability, durability, data-loss, failover-time, or latency SLA;
- linearizable follower reads or a bounded follower-staleness interval;
- ordinary Raft `MsgSnap` transfer;
- receiver or consensus acknowledgement from `Send` or a socket-write counter;
- automatic or joint-consensus replica replacement;
- a cluster-wide snapshot, global MVCC timestamp, or unrestricted clock-fault tolerance;
- online request-ledger range changes—a different range requires a fresh certified group;
- replicated local `UNIQUE` indexes;
- durable fulfillment of large completion-digest references by a finished blob store;
- proof from the deterministic simulator of physical WAL tears, TLS/framing, process behavior,
  autonomous election timing, or external-network qualification;
- production PKI provisioning or permission to bypass the exact certificate/trust-domain model.

Several formats explicitly identify themselves as unreleased and have no legacy decoder.
Preserve artifacts, but assume only the exact creating build can understand them.

## Source map

- Catalog pinning/routing: [`gateway/catalog.go`](../../gateway/catalog.go), [`gateway/route.go`](../../gateway/route.go)
- Serving: [`internal/raftservice/owner.go`](../../internal/raftservice/owner.go), [`internal/raftservice/data_read.go`](../../internal/raftservice/data_read.go)
- Group ownership: [`internal/raftmember/runtime.go`](../../internal/raftmember/runtime.go), [`internal/multiraft/host.go`](../../internal/multiraft/host.go)
- Raft: [`internal/raftmodel/config.go`](../../internal/raftmodel/config.go), [`internal/raftmodel/node.go`](../../internal/raftmodel/node.go), [`internal/raftmodel/ports.go`](../../internal/raftmodel/ports.go)
- WAL/snapshots: [`internal/raftstore/store.go`](../../internal/raftstore/store.go), [`internal/raftstore/generation_activate.go`](../../internal/raftstore/generation_activate.go), [`internal/replicatedstate/snapshot_artifact.go`](../../internal/replicatedstate/snapshot_artifact.go)
- Peer transport: [`internal/rafttransport/identity.go`](../../internal/rafttransport/identity.go), [`internal/rafttransport/registry.go`](../../internal/rafttransport/registry.go), [`internal/rafttransport/transport.go`](../../internal/rafttransport/transport.go)
- Membership: [`internal/membershipgrant/grant.go`](../../internal/membershipgrant/grant.go), [`internal/raftservice/owner.go`](../../internal/raftservice/owner.go)
- Retry/state: [`internal/raftserve/registry.go`](../../internal/raftserve/registry.go), [`internal/replicatedstate/apply.go`](../../internal/replicatedstate/apply.go), [`internal/requestledger/types.go`](../../internal/requestledger/types.go)
- Static indexed mutations: [`sql/driver/mutation_capture.go`](../../sql/driver/mutation_capture.go), [`shardservice/execute.go`](../../shardservice/execute.go), [`gateway/writer.go`](../../gateway/writer.go), [`gateway/transaction.go`](../../gateway/transaction.go)
- Fences/clocks: [`internal/executionpin/transition.go`](../../internal/executionpin/transition.go), [`internal/routegate/machine.go`](../../internal/routegate/machine.go), [`internal/routeforward/resolve.go`](../../internal/routeforward/resolve.go), [`internal/txnclock/clock.go`](../../internal/txnclock/clock.go)
