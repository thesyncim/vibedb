# Distributed server boundary

VibeDB is embedded-first. Its distributed packages and commands are
experimental, unreleased, and currently provide a leader-only read path,
single-shard fast writes, and synchronous fixed-participant atomic write
batches—not a high-availability database.

This document is the operator contract for what exists now. It intentionally
does not describe unfinished features as current capability. The implementation
architecture and ordered delivery gates are in
[Distributed system target](distributed-system.md). That target uses
tenant-independent virtual buckets: a tenant is never assigned to one physical
shard.

## Current components

| Component | Current responsibility |
| --- | --- |
| `distribution` | Canonical placement scalars, full-tuple virtual-bucket mapping, affinity/tenant placement validation, immutable shard manifests, routing versions, allocation generations, and ownership epochs |
| `sql/driver.OpenCluster` | Opt-in, one-shard embedded placement and write preflight; no network |
| `vibedb-shard` / `shardservice` | One locally fenced, leader-only SQL store served over the bounded shard protocol; ordinary reads remain one frame, opted-in internal reads can use bounded sequence-checked row frames, and owner-fenced exchange commands provide retry-safe backpressure plus exact partition-local grouped reduction without generic JSON parsing |
| `gateway` / `vibedb-gateway` | Immutable catalog validation, generation-pinned routing, scoped coherent read fan-out/result merging, a single-shard write fast path, synchronous multi-table/cross-shard `ExecBatch`, periodic durable coordinator redrive, byte-native grouped streaming, and memory-costed worker-to-worker hash repartition; general range/join exchange is not yet wired |
| `planner` | Bounded memo/rule/cost/statistics primitives used by the distributed planning layer |
| `autosplit` | Fixed-space striped request telemetry, sustained bucket-aligned recommendations, and allocation-high-water/generation-fenced desired split manifests; it cannot publish topology, copy state, catch up a destination, or move ownership |
| `internal/rebalance` | Non-serving, stateless intact-shard replica movement: exact membership stages, certified learner-base/install proof, applied catch-up, promotion, leader transfer, ordered ownership-fence advance, catalog generation CAS, old-generation drain, source removal, and retirement action |
| `internal/rangesplit` | Non-serving one-pass physical child row partitioning: complete desired-manifest and placement-program digests, exact source fences, compiled `vibejson` extraction, no-copy retained-child support, and bounded deterministic hash-chained artifacts for non-retained children; verification proves framing, key order, and row placement before a durable chunk callback, but no destination installer or cutover authority is wired |

The repository also contains a non-serving Raft foundation: a pinned upstream
Raft core, append-only immutable-base WAL, local replicated-apply state machine,
bounded in-process scheduler/outbox, and a frame/roster validator that accepts
a caller-supplied authenticated NodeID. These internal packages are not wired
to either server command, a public API, or operator configuration.

`internal/exchange` contains a bounded worker mailbox:
raw attempt-fenced identities, unbiased fixed-stage partition selection,
registry capacity reservation, per-producer sequence/credit enforcement,
retry-digest idempotence, acknowledgment-based redelivery, backpressure,
deadlines, and deterministic cancellation cleanup. The shard wire exposes
owner-fenced open/push/pull+ack/cancel/reduce commands. The physical planner
selects this path for multi-shard grouped queries when centralized state cannot
satisfy the active memory objective; smaller plans retain the lower-network
gateway finalizer. A gateway-internal lifecycle coordinator and producer core
can already open partitions in bounded parallelism, exact-hash canonical JSON
group identities, build borrowed-decode row blocks, sequence pushes, and
terminate empty partitions. An additive read-only shard fragment streams its
cursor directly to those destination mailboxes over persistent per-partition
peer connections; exchange-only destination connections allocate no SQL session.
Every destination reducer drains while producers run, combines the exact shared
COUNT/SUM/MIN/MAX program, and publishes bounded result blocks to a second
mailbox before the gateway gathers disjoint partitions.

## Network and trust boundary

Both server commands accept loopback listeners only. This is a deliberate
fail-closed boundary because neither protocol provides built-in transport
authentication or authorization.

- `vibedb-shard` uses the length-prefixed shard-service protocol.
- `vibedb-gateway` accepts newline-delimited JSON request envelopes. It is not
  a pgwire endpoint.
- A production deployment must not expose either listener beyond a trusted
  local boundary.

The gateway catalog is supplied externally as an immutable snapshot. The
gateway can validate, inspect, load, pin, and reload a strictly newer catalog.
Both durable and in-memory publication also expose an expected-generation CAS,
so a movement planned from generation N cannot overwrite unrelated topology.
The repository does not provide a topology authority or a server-wired workflow
service.

## Placement and tenant boundary

The current native mapper hashes the complete canonical placement tuple into a
20-bit virtual bucket and routes the bucket through the immutable manifest.
Explicit bucket metadata requires bucket-aligned shard boundaries and exposes
allocation-free bucket-interval ownership for future movement. A tenant-scoped
placement marks `TenantPath`; validation rejects a tuple containing only that
path, so tenant identity cannot become the complete physical shard key. Tables
may name an affinity group when equal placement ordinals are intended to be
colocated.

Because the full tuple is hashed, a predicate binding only `TenantPath` cannot
predict the remaining locality key and routes as a scatter. A full placement
tuple routes to one bucket. Tenant-only access becomes narrow through an index,
not through a false assumption that one tenant belongs to one shard.

## Store initialization and local fencing

Create a shard store explicitly:

```text
vibedb-shard init -store <path> -distribution <name> -shard <id> \
  -allocation-generation <n>
```

Serve that exact identity with nonzero ownership and routing coordinates:

```text
vibedb-shard serve -store <path> -listen 127.0.0.1:<port> \
  -distribution <name> -shard <id> -allocation-generation <n> \
  -epoch <n> -routing-version <n>
```

Initialization binds the SQL catalog to its distribution, shard, allocation
generation, and random local LogID. Serving durably advances local ownership
and routing high-water marks and holds the only serving claim for that open
store until its sessions drain.

This is same-store process fencing. It is not a distributed lease, election,
quorum, or copied-store revocation mechanism. It cannot prove replica
freshness, revoke another process serving a copied store, or protect against a
trusted caller opening the files through a separate direct handle.

## Gateway query contract

The gateway's query operation accepts explicitly read-only SQL and typed
parameters. Parameter payloads are byte-native across the gateway/shard wire:
exact numbers remain `vibejson` raw values, and strings and documents remain
borrowed bytes, avoiding `encoding/json` and intermediate Go strings in routing,
statistics-bound encoding, merge-key decoding, and shard binding. Ordered
multi-shard merge validates each numeric cell once, then uses the query
engine's exact allocation-free byte comparator directly; it does not
canonicalize or allocate inside heap comparisons. One query
pins one catalog generation, resolves the relevant immutable manifest, routes
to leader endpoints, applies per-shard and operation-wide deadlines, and merges
within configured result, memory, fan-out, and concurrency limits.

Gateway requests also carry canonical virtual-bucket access intervals. Durable
transaction participants bind those intervals into their mutation digest, and
the shard's sorted interval index blocks only overlapping traffic. Direct
requests with no interval remain whole-shard scoped and therefore fail safe.

When a shard server is constructed with an `autosplit.Recorder`, request
completion contributes exact request, response-byte, and service-time totals
to one fixed-space striped window. A one-bucket access scope is attributed to
that canonical virtual bucket; wider scopes retain boundary-crossing evidence,
while missing or mismatched locality is retained as an exact separate total,
projected proportionally only while costing boundaries, and charged as
unbounded fan-out rather than inventing a hot key. Exchange-control traffic is
excluded. The disabled default adds no recorder lock or clock read to ordinary
requests. A terminal response is recorded before it becomes visible to the
caller, so a concurrent window rotation cannot move a completed request into
the next interval.

Sustained evidence can produce either a bucket-aligned binary split or exact
single-bucket isolation. `PlanSplit` then validates the full source allocation,
routing version, ownership epoch, bucket geometry, and lifetime allocation
high-water before constructing an immutable desired manifest. This is planning,
not rebalancing: the returned manifest is not publishable until a separate data
plane has transferred and installed the now-available certified snapshot
artifact, applied the ordered mutation tail, closed the final source gap, and
passed cutover validation. Artifact export is deterministic, bounded-memory,
hash-chained, and checkpointed. Non-serving destination files can now resume at
an atomically persisted cursor, apply bounded local batches, and pass a full
candidate-open proof without retaining a second artifact copy. A verified
candidate can now be bound to a fresh immutable Raft WAL at the exact cut, and
a learner catches up its suffix through ordinary `AppendEntries`. For an intact
shard allocation, the non-serving rebalance kernel now admits only the exact
sequence: learner membership, certified base identity, target apply at the
leader commit, voter promotion, explicit leader transfer, one binary replicated
ownership/routing/catalog-fence increment, expected-generation catalog CAS,
old-generation operation drain, source-voter removal, and source retirement.
It fails closed on an unrelated catalog or membership edit and reconstructs its
next action from durable evidence after restart. Peer transfer orchestration,
topology authorization, target SQL-root construction, server integration, and
physical split cutover are not implemented here. The first child data-plane
primitive is present in `internal/rangesplit`: it binds the exact source
allocation and complete desired-manifest digest, scans the source user image
once, builds one reusable `vibejson` structural index per row, computes the
canonical native placement point, and dispatches borrowed key/value bytes to
exactly one of at most three child sinks. The retained child may use a nil sink,
avoiding a redundant copy. Its warmed row path is allocation-free. Every other
child can be written during that same scan as a bounded deterministic
hash-chained artifact. The header binds the complete plan, placement program,
source applied/base cut, exact child range/allocation/ownership, and ordered
endpoint set; the footer certifies row/byte/chunk totals. Verification checks
the chain and strict key order, then recomputes every document's `vibejson`
placement before exposing an entire borrowed chunk to a receiver that can
atomically persist rows and its checkpoint. This still does not install a child
database, translate a source mutation tail into child groups, change validation
ownership, or publish topology.

A multi-shard query establishes an ephemeral coherent vector cut before reading:
it acquires the same leased raw identity on every target, reads only while that
identity and exact scope validate, then releases in parallel. A writer holds its
scoped admission token through publication. Transaction staging turns that
token into a durable participant barrier without an admission gap. If a writer
or participant wins on any target, the gateway releases the partial cut and
retries with a fresh identity, avoiding distributed lock cycles. Lease expiry
is the crash cleanup, and a finite per-shard active-fence limit bounds abandoned
scope storage. Single-shard reads bypass this bridge and retain one round trip.

Currently executable distributed shapes include:

- projection and filters that can be lowered for shard-local execution;
- colocated `INNER` and `LEFT` joins;
- `Gather` and ordered `MergeGather`;
- global `LIMIT`; and
- global and grouped `COUNT`, `SUM`, `MIN`, and `MAX` over mergeable
  shard-local states. Grouped finalization uses the query engine's exact
  composite key identity and a memory-capped columnar accumulator. A bounded
  exact final sort implements grouped `ORDER BY`; `ORDER BY ... LIMIT K` uses
  O(K) heap state when the group includes the complete placement key, which
  proves shard-local groups and makes local top-K pushdown sound. Grouped
  `HAVING`, `AVG`, and LIMIT over cross-shard group identities remain refused.

## Gateway write contract

The gateway's write operation (`Executor.Exec`, or the serve front-end with
`"op": "exec"`) executes one mutating statement that the pinned generation
proves resident on exactly one shard. It reuses the read path's
pin-one-generation, prepare, bind, route, and stale-epoch retry machinery, but
dispatches a single leader call with the shard service's read-write execution
mode instead of a fan-out, so a write commits as one local shard statement and
never partially commits.

Currently executable distributed writes include:

- `INSERT ... VALUES` whose rows all route to the same shard by their shard
  key, flat or whole-document, with an optional `RETURNING` projection;
- `UPDATE ... WHERE` whose shard-key equality or `IN` predicate resolves to
  one shard, whose whole-document replacement does not move the row's shard
  key; and
- `DELETE ... WHERE` whose shard-key equality or `IN` predicate resolves to
  one shard.

A write predicate that matches no shard routes to an empty set: a successful
local no-op that contacts no shard.

The distributed layer rejects a write before any network I/O:

- an `INSERT` whose rows route to more than one shard;
- an `UPDATE` or `DELETE` whose predicate does not resolve to one shard;
- an `UPDATE` whose replacement document moves the row to another shard;
- `INSERT ... SELECT`, `TRUNCATE`, and every DDL statement; and
- a `SELECT` submitted to the write operation.

The read operation still rejects before dispatch:

- every mutation and DDL statement;
- every write shape, which belongs to `Exec` or `ExecBatch`;
- non-colocated, `RIGHT`, and `FULL` joins;
- distributed `AVG`, grouped aggregation, `HAVING`, `DISTINCT`, windows, and
  `OFFSET`; and
- plans that exceed route, fan-out, result, intermediate-memory, or deadline
  limits.

The exact embedded SQL surface is broader than the distributed subset. A query
working through `database/sql` or pgwire does not imply that the gateway can
execute it.

## Freshness and catalog changes

The current shard service is leader-only, so a successful routed read is a
read from the configured leader process. A multi-shard result is protected from
mixed visibility with the scoped vector fence above, but it is not yet a
portable scalar MVCC timestamp and cannot be replayed later. Session-position
types are reserved, but the public gateway query/result path does not accept or
return session positions, and the shard service refuses a minimum-position
request before SQL execution.

Ordinary catalog publication validates monotonic catalog, routing, allocation,
range, leader, and ownership coordinates. Intact-shard replica cutover has an
exact-generation publication and drain protocol in the non-serving kernel.
Desired split planning applies the same generation and allocation-lineage
fences, but there is no online range-split executor. Operators must not publish
a desired split manifest or treat an arbitrary catalog edit as a safe split,
merge, move, or replica reconfiguration protocol.

## What is not implemented

- peer enrollment, mutually authenticated peer transport, or per-peer flow
  control;
- serving Raft replication, replicated client writes, automatic election, or
  failover;
- serving follower/session reads or a coherent SQL read bound to the
  non-serving runtime's `ReadIndex` outcome;
- runtime Raft log compaction, authenticated snapshot transport, or a
  server-wired topology authority; portable coherent artifact export,
  resumable non-serving staging/candidate validation, model-checked membership,
  and intact-shard move reconciliation are exposed only through the non-serving
  kernel;
- persistent destination staging/install and ordered cutover for an online
  split or merge; bounded hot-bucket evidence, desired split planning, one-pass
  allocation-free child row partitioning, deterministic filtered child
  artifact framing/verification, source artifact export, offline whole-shard
  destination install, and intact-shard replica relocation primitives exist,
  but confer no serving authority by themselves;
- a replicated scalar MVCC/closed-timestamp snapshot or historical distributed
  reads; the current leader-only vector fence is ephemeral;
- arbitrary distributed SQL transaction sessions and single-statement scatter
  mutation: `ExecBatch` is a bounded fixed-participant atomic batch, while a
  scattered `INSERT ... VALUES`, scatter `UPDATE`/`DELETE`, and `INSERT ...
  SELECT` remain refused;
- global-index lookup plus owner-grouped base fetch, UPDATE/DELETE old-row
  capture, and online build/catch-up workers; the catalog, zero-allocation
  byte-native routing, and atomic READY-index INSERT/unique-claim path exist,
  but the index is not yet a query-planner access path;
- distributed backup, restore, PITR, or disaster-recovery orchestration; and
- a release-scale or 100 TB qualification claim.

## Operator tools

`vibedb-gateway validate -catalog <path>` validates a catalog snapshot without
serving it. `inspect` prints its generation, distributions, shard geometry,
allocation identities, ownership epochs, and referenced endpoints. These tools
validate the current file; they do not establish its authority.

For exact routing encodings, see
[Placement scalar and tuple codec](distribution-tuple-format.md). For the
implemented planner core, see [Query planner](query-planner.md). For the
distributed implementation target, see
[Distributed system target](distributed-system.md). For the
non-serving Raft boundary, see
[Raft core selection](raft-core-selection.md), [Raft WAL](raft-wal.md), and
[Replicated state machine](replicated-state-machine.md).
