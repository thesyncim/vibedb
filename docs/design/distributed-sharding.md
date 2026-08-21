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
| `vibedb-shard` / `shardservice` | One locally fenced, leader-only SQL store served over the bounded shard protocol |
| `gateway` / `vibedb-gateway` | Immutable catalog validation, generation-pinned routing, scoped coherent read fan-out/result merging, a single-shard write fast path, synchronous multi-table/cross-shard `ExecBatch`, and periodic durable coordinator redrive |
| `planner` | Bounded memo/rule/cost/statistics primitives used by the distributed planning layer |
| `autosplit` | Fixed-space, shadow-only split recommendation; it has no production caller and cannot publish or move topology |

The repository also contains a non-serving Raft foundation: a pinned upstream
Raft core, append-only static-base WAL, local replicated-apply state machine,
bounded in-process scheduler/outbox, and a frame/roster validator that accepts
a caller-supplied authenticated NodeID. These internal packages are not wired
to either server command, a public API, or operator configuration.

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
gateway can validate, inspect, load, pin, and reload a strictly newer catalog;
the repository does not provide a complete topology service or workflow
controller.

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
- global `COUNT`, `SUM`, `MIN`, and `MAX` over mergeable shard results.

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
range, leader, and ownership coordinates. There is no online range-movement
workflow. Operators must not treat a catalog edit as a safe split, merge, move,
or replica reconfiguration protocol.

## What is not implemented

- peer enrollment, mutually authenticated peer transport, or per-peer flow
  control;
- serving Raft replication, replicated client writes, automatic election, or
  failover;
- follower/session reads or `ReadIndex` integration;
- runtime Raft snapshots, WAL compaction, dynamic membership, or snapshot
  transfer;
- online split, merge, move, catch-up, or topology recovery;
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
