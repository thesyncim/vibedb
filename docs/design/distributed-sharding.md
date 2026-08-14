# Distributed server boundary

VibeDB is embedded-first. Its distributed packages and commands are
experimental, unreleased, and currently provide a leader-only read path—not a
high-availability database.

This document is the operator contract for what exists now. It intentionally
does not describe a future replication or resharding architecture.

## Current components

| Component | Current responsibility |
| --- | --- |
| `distribution` | Canonical placement scalars, tuple encoding, immutable shard manifests, routing versions, allocation generations, and ownership epochs |
| `sql/driver.OpenCluster` | Opt-in, one-shard embedded placement and write preflight; no network |
| `vibedb-shard` / `shardservice` | One locally fenced, leader-only SQL store served over the bounded shard protocol |
| `gateway` / `vibedb-gateway` | Immutable catalog validation, generation-pinned routing, bounded read-only fan-out, and result merging |
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

The gateway accepts explicitly read-only SQL and typed parameters. One query
pins one catalog generation, resolves the relevant immutable manifest, routes
to leader endpoints, applies per-shard and operation-wide deadlines, and merges
within configured result, memory, fan-out, and concurrency limits.

Currently executable distributed shapes include:

- projection and filters that can be lowered for shard-local execution;
- colocated `INNER` and `LEFT` joins;
- `Gather` and ordered `MergeGather`;
- global `LIMIT`; and
- global `COUNT`, `SUM`, `MIN`, and `MAX` over mergeable shard results.

The distributed layer rejects before dispatch:

- every mutation and DDL statement;
- cross-shard writes and transactions;
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
read from the configured leader process. Session-position types are reserved,
but the public gateway query/result path does not accept
or return session positions, and the shard service refuses a minimum-position
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
- cross-shard transactions or a common real-time distributed snapshot;
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
non-serving Raft boundary, see
[Raft core selection](raft-core-selection.md), [Raft WAL](raft-wal.md), and
[Replicated state machine](replicated-state-machine.md).
