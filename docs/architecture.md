# Architecture

VibeDB has four main layers:

1. The root facade owns database lifecycle and product durability profiles.
2. The heap and durable stores own JSON collections, indexes, and snapshots.
3. The query and SQL packages own parsing, planning, and local execution.
4. The experimental distributed packages own static SQL routing and an RF3
   point-read path.

The internal Raft, replicated-state, and Raft-service packages form the RF3
serving composition. `vibedb-shard serve-rf3` constructs it for one externally
prepared stable three-voter group. It does not provision or repair the group,
and the public gateway uses it only for canonical point `get` requests. SQL,
writes, scatter reads, and multi-table reads remain on or are limited by the
static path.

## Product facade

`vibedb.Open` returns a database catalog of stable lazy collection handles. A
handle does no I/O until a valid mutation needs storage.

The facade selects durable, buffered, or memory operation. It freezes schemas,
index definitions, admission bounds, and file permissions during open.

Each collection has an independent direct-mutation fence. Unrelated
collections can accept writes concurrently. A serializable facade transaction
takes participating fences in name order during validation and publication.

## Immutable generations

Both storage engines publish immutable collection generations.

The heap engine replaces one atomic state pointer. A point write rebuilds at
most one bounded chunk.

The durable engine keeps a selected root as the canonical physical checkpoint.
Later generations can exist as journal redo or as a resident overlay. Recovery
starts from the selected root and applies valid redo. It cannot recover
volatile overlay-only generations. A full-generation, chain-fence, or
checkpoint path publishes an alternate root that references immutable pages
and persisted catalogs.

Readers observe one generation cut. A heap snapshot retains immutable state. A
durable snapshot also pins a reclamation lease and owns scratch storage.

## Logical and physical publication

Rows and exact postings use one logical failure-atomic publication. A durable
batch can first publish a content-equivalent topology generation that prepares
space or structure. A later validation error can reject the logical change.
Thus, Generation may advance with no logical row or index change.

This distinction separates representation maintenance from application data.

## Read path

A point read resolves the primary-key structure. A scan uses an immutable
source and lexical durable key order.

Query execution can choose a primary point, primary range, exact-index
candidate set, skip-index pruning, join candidate path, or full scan. Candidate
selection never removes the final predicate check.

A coherent database snapshot captures all cataloged collections at one instant.
Use this source for cross-collection joins, subqueries, and application reads
that must not observe part of a multi-collection commit.

## Write path

`Put` first resolves existing storage without creating it so persisted bounds
remain authoritative after a zero-option reopen. For a genuinely absent lazy
collection, it validates key and document bounds before it creates the file.
The memory path canonicalizes JSON in the facade. The durable path validates
and canonicalizes JSON during durable mutation preparation.

The selected engine then reserves bounded resources, maintains the primary row
and exact postings, completes its durability action, and publishes the visible
logical generation.

The synchronous journal path makes redo durable before it publishes the
visible logical generation and returns success. Buffered visibility publishes
from memory and defers the device checkpoint to `Flush` or `Close`.

## Multi-collection transaction path

A facade transaction captures one coherent database cut. It keeps bounded
per-collection overlays and tracks exact or coarse read dependencies.

Commit releases read leases, serializes validation, and locks participant
fences in name order. A conflict publishes nothing.

The heap database flips all participant state pointers while all writers are
locked. The durable database writes conditional participant records and one
decision record before it publishes the participants.

Independent collection readers do not make a coherent cross-collection cut.
Database snapshots do.

## SQL path

The SQL parser produces a bounded typed AST. The SQL driver lowers one
statement into query or mutation execution over a shared durable catalog.

`database/sql` uses one query worker per physical connection. The pool supplies
parallelism. Pgwire uses the same SQL runtime and adds PostgreSQL protocol
state, authentication, TLS options, cancellation, and compatibility shims.

The generic `planner` package provides a memo and physical property framework.
The gateway uses a bounded subset for remote access, gather, aggregation,
repartition, and global-index paths.

## Distributed path

The gateway has two distinct public data paths:

```text
SQL request          -> gateway -> static shard service -> local SQL catalog
canonical point get -> gateway -> replicated catalog route -> RF3 native relation
```

The gateway pins one immutable catalog generation per attempt, proves a bounded route,
dispatches SQL and typed parameters, and merges the complete result. The shard
admits distribution, allocation, routing, and ownership identity before SQL.

The replicated catalog stores exact RF3 shard coordinates and base-table
profiles. A profile binds the table, primary-key path, dense relation ID,
schema generation, relation-manifest digest, and key and document limits. A
point read pins one catalog generation and resolves the ordered key without a
SQL or string conversion.

The gateway shares one bounded authenticated native connection pool across
catalog and point-read traffic. A bounded four-way leader-hint cache avoids
repeated probes. The executor validates the complete route and serving fence,
follows `NotLeader` responses, and retries within the configured attempt and
deadline bounds.

A linearizable point read follows the leader and uses Raft `ReadIndex`. An
`at_least_applied` read carries the exact `RouteID` and nonzero applied index
from an earlier response. The gateway rejects a different route lineage before
network I/O and selects a replica that has reached the requested index. Every
successful response returns the current `RouteID` and applied index.

The RF3 point-read boundary does not fall back to SQL. A missing table profile,
stale serving fence, unavailable quorum, or mismatched position returns a typed
refusal.

`vibedb-shard serve-rf3` opens exact retained WAL, SQL, and apply artifacts,
constructs one bounded Multi-Raft host plus authenticated peer transport, and
serves the authenticated native replicated protocol. There is no RF3
initializer, membership or snapshot orchestrator, or public RF3 write path.
Scatter and multi-table RF3 reads are also absent.

The static shard service accepts only its `ReadStrong` policy and serves a
statement-level snapshot from a statically configured leader endpoint. This
label is not a Raft linearizability proof because the serving path has no
election or replication. Multi-shard reads use short-lived scoped vector
fences. Ordinary writes require one-owner proof. A separate fixed-participant
protocol supports atomic write batches.

The static SQL path has no Raft replication, follower read, or endpoint
failover. The RF3 leader cache and retry logic apply only to canonical point
reads and replicated catalog operations.

The internal range-split data plane is not part of the runnable path. It scans
one certified source image once and routes each borrowed row to at most three
children. It uses a compiled `vibejson` placement program and does not use
`encoding/json`. It can omit the retained child copy.

The same package writes deterministic hash-chained child artifacts. A verifier
checks framing, key order, and document placement before it exposes a chunk.
An ordered tail translator derives one exact batch for every child, including
empty advances and shard-key moves. A non-serving child stage applies verified
rows and tail batches to one durable collection. It validates the complete
artifact image before tail catch-up and persists a fixed-size cursor through an
atomic file replacement on Unix. An optional replicated-state capture writes
each exact before-and-after transition in the same durable
transaction as its source publication. A terminal ownership-fence entry must
advance all mutable serving coordinates together. Every child durably records
its empty seal batch, scans and hashes its complete ordered final image, and
rechecks that image on reopen before a fixed-size cutover certificate can be
issued. The certificate binds every non-retained child image. A sealed stage
can initialize the standard replicated-state snapshot base in place without a
second durable user-row copy. The SQL driver holds an exclusive non-serving
claim while it receives the child. Activation converts that claim to a
base-pending replicated apply owner without changing the user collection
incarnation. That owner rejects proposal, apply, lookup, export, and SQL
serving; it accepts only the exact authenticated snapshot base. Transaction 2
certifies the base binding before the ordinary replicated apply owner is
exposed. A planned WAL identity breaks the bootstrap cycle. The final WAL is
allocated once from the newer snapshot base and is rechecked against the SQL
binding before the existing Raft runtime can adopt it.

The certificate is evidence, not topology authority. It authorizes only the
conditional catalog successor after every child is ready. The catalog CAS is
published durably before destructive cleanup; older catalog leases must then drain.
An unforgeable sealed catalog capability binds the durable CAS receipt, drained serving generation,
operation identity, manifest, and cutover certificate. Only that witness authorizes retained cleanup, which plans bounded
ordered key batches, checkpoints each batch before proposal, and confirms exact
atomically captured replicated deletes. Concurrent retained-range writes are
advanced one captured entry at a time. The current serving path can apply and
capture an out-of-range post-publication write; cleanup then detects that entry
and halts before further pruning or completion. A final persisted, bounded incremental scan certifies the retained
image without an unbounded controller turn. A crash may leave
duplicate physical bytes, but never a routing gap or overlapping authority.

The internal topology scheduler has a second non-serving path for replica
movement. It consumes fixed-width, exact-generation capacity reports, nets
source releases against target reservations in seven resource dimensions, and
selects endpoints only when the maximum projected dominant pressure improves.
Current replicas, failure domains, receive concurrency, migration ingress, and
per-node concentration are hard bounds. The warm scheduler is fixed-memory and
allocation-free. Its result is still advisory: an external owner attaches Raft
member identities and drives the stateless `internal/rebalance` proof sequence.
Leader-only manifest cutover shares the immutable range index and untouched
leader storage instead of rebuilding every shard.

## External memory and unsafe code

The engine uses pointer-free mapped arenas, typed byte views, SIMD loads, and
platform I/O mappings. This memory can live outside the Go heap.

The garbage collector cannot find pointers that exist only in mapped storage.
Owning state objects keep the related blocks alive. Close and snapshot
lifetimes define when borrowed data becomes invalid.

See [UNSAFE.md](../UNSAFE.md) for the generated production-file inventory and
review rules.

## Implementation map

| Layer | Main packages |
| --- | --- |
| Product facade | root `vibedb` package |
| Heap storage | `store` |
| Durable storage | `store/durable`, `internal/storeio` |
| Query and SQL | `query`, `sql`, `sql/driver`, `planner` |
| PostgreSQL protocol | `pgwire` |
| Distributed runtime | `distribution`, `gateway`, `shardservice` |
| Replication kernel | `internal/raft*`, `internal/multiraft`, `internal/replicatedstate` |
| Range-split kernel | `autosplit`, `internal/topologyscheduler`, `internal/rangesplit`, `internal/splitcontroller` |

## Implementation references

- `vibedb.go`, `vibedb_txn.go`, and `vibedb_query.go`
- `store/engine.go` and `store/durable/store_file.go`
- `query/exec.go` and `sql/driver/runtime.go`
- `gateway/executor.go`, `gateway/replicated_data_read.go`,
  `gateway/replicated_table.go`, and `shardservice/server.go`
- `internal/raftmember/runtime.go`
- `autosplit/action.go`
- `internal/topologyscheduler/admission.go`, `feedback.go`, `planning.go`,
  `capacity_placement.go`, and `replica_move.go`
- `internal/rangesplit/partition.go`, `artifact.go`, `tail.go`, `stage.go`, and
  `source_capture.go`
- `internal/rangesplit/cutover.go`
- `internal/replicatedstate/capture.go`
- `sql/driver/replicated_child_stage.go`
- `internal/raftmember/staged_child.go`
- `internal/splitcontroller/reconcile.go`
- `internal/splitcontroller/execute.go`
- `internal/rebalance/plan.go` and `reconcile.go`
