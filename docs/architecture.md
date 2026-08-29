# Architecture

VibeDB's current credible and implemented product boundary is the embedded
engine. The distributed source tree is an experimental serving and
qualification lane. It is not yet a production control plane or a compatibility
promise.

The repository has four main layers:

1. The root facade owns database lifecycle and product durability profiles.
2. The heap and durable stores own JSON collections, indexes, and snapshots.
3. The query and SQL packages own parsing, planning, and local execution.
4. The experimental distributed packages own a legacy static SQL path, RF3
   native paths, and an opt-in RF3 PostgreSQL development adapter.

The internal Raft, replicated-state, and Raft-service packages form the RF3
serving composition. `vibedb-shard serve-rf3` constructs it from externally
prepared group bundles. The native gateway uses RF3 for canonical point
`get`, exact-key `read_batch`, sequenced `exec_batch`, and supported SQL reads
when it is constructed with a replicated catalog. The optional
loopback-only PostgreSQL development listener also executes supported
distributed reads over RF3 and sends supported autocommit writes through the
durable request path. Native `query` and `exec` use the gateway's configured
SQL transport. Explicit static development mode uses the static shard service.
Replicated mode uses RF3 for supported reads and refuses `exec` against RF3
tables.

There is no object-storage segment layer or stateless-compute architecture in
the current implementation. Durable embedded and RF3 replicas own local files,
WAL, and metadata. The gateway also owns durable controller/session journals
when RF3 request execution is enabled. There is no AI control plane or
workload-driven automatic exact-index manager. Applications declare exact
indexes explicitly.

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
parallelism. The embedded pgwire server uses the same SQL runtime and adds
PostgreSQL protocol state, authentication, TLS options, cancellation, and
compatibility shims. The pgwire package also has a backend interface that the
experimental gateway uses for RF3 access.

The generic `planner` package provides a memo and physical property framework.
The gateway uses a bounded subset for remote access, gather, aggregation,
repartition, and global-index paths.

## Distributed path

### Current native gateway paths

The newline-delimited native gateway has four distinct data paths:

```text
query / exec             -> gateway -> configured SQL transport (static, or RF3 reads)
canonical get            -> gateway -> replicated catalog -> RF3 native relation
exact-key read_batch     -> gateway -> replicated catalog -> one RF3 read cut per group
sequenced exec_batch     -> gateway -> request ledger / transaction runner -> RF3 relations
```

These are not interchangeable implementations of one database. Static SQL and
RF3 use different serving authorities and storage lifecycles. A request is
classified before data I/O. A mixed static/RF3 write batch fails closed.

The SQL executor pins one immutable catalog generation per attempt, proves a
bounded route, dispatches SQL and typed parameters, and merges the complete
result. With `-dev-static-catalog`, its `ReadStrong` name means a statement-level
snapshot from the configured owner. It is not Raft linearizability: the static
path has no election, replication, follower reads, or endpoint failover. With a
replicated catalog, supported SELECT plans use the RF3 SQL transport and leader
`ReadIndex`. Direct `exec` refuses RF3 tables, whose public writes must use the
durable sequenced path.

The replicated catalog stores exact RF3 shard coordinates and base-table
profiles. A profile binds the table, primary-key path, dense relation ID,
schema generation, relation-manifest digest, and key and document limits. A
point read pins one catalog generation and resolves the ordered key without a
SQL or string conversion.

`read_batch` accepts a bounded list of exact-primary-key SQL reads. It groups
them by RF3 group, takes one leader `ReadIndex` cut per group, and returns a
sorted observation vector. Those independent cuts are not a common MVCC
timestamp or a serializable cross-group snapshot.

`exec_batch` first validates and lowers every supported statement to numeric
relation operations. Its public shapes are one or more complete-document
INSERT rows or unique top-level named-column scalar rows encoded as canonical runtime documents,
a whole-document update selected by exact primary key, and exact-primary-key
delete with equality or a finite `IN` set. One multi-row INSERT can span RF3
groups. The request can also span tables and maintain cataloged global-index
relations. Residual predicates, arbitrary SQL mutations, repeated relation
keys, and mixed static/RF3 batches are refused before execution.

The RF3 write boundary is durable and sequenced. An authenticated client first
opens a fixed lane for its persisted installation ID. Replicated catalog
authority returns a grant digest. Each `exec_batch` then carries that exact
grant reference, one strictly monotonic lane sequence, and a nonzero 128-bit
request ID. The transport supplies the principal and tenant. The client cannot
substitute them in the request.

One home-group transition advances the issuer high-water and creates the
request head. Replicated request-ledger state retains the admitted program,
pending waves, terminal result, and ACK state. An exact retry therefore resumes
or returns the sealed execution rather than replanning against current
topology. There is no unsequenced RF3 fallback.

The gateway shares one bounded authenticated native connection pool across
catalog, point-read, proposal, and transaction-recovery traffic. A bounded
leader-hint cache avoids repeated probes. The executor validates the route and
serving fence, follows `NotLeader` responses, and retries within configured
attempt and deadline bounds.

A linearizable point read follows the leader and uses Raft `ReadIndex`. An
`at_least_applied` read carries the exact `RouteID` and nonzero applied index
from an earlier response. The gateway rejects a different route lineage before
network I/O and selects a replica that has reached the requested index. Every
successful response returns the current `RouteID` and applied index.

The RF3 point-read boundary does not fall back to SQL. A missing table profile,
stale serving fence, unavailable quorum, or mismatched read position returns a
typed refusal.

### RF3 PostgreSQL development adapter

`vibedb-gateway serve -pg-dev-listen` adds a trust-authenticated,
loopback-only PostgreSQL endpoint. Supported SELECT statements use RF3
distributed planning and one independent `ReadIndex` cut per participating
group. Supported single-statement autocommit writes use the same durable
request-ledger service as native `exec_batch`. Transaction blocks do not add a
cross-statement snapshot or atomic write unit. The endpoint refuses savepoints,
repeatable-read and serializable isolation, write transaction blocks, and
unsupported DDL or mutation shapes.

When a local development supervisor supplies `-pg-dev-ddl-socket`, the adapter
can coordinate `CREATE TABLE`. It does not provide general distributed DDL.
Notably, ordinary SQL `CREATE INDEX` is not an online RF3 coordinator.

### RF3 lifecycle boundary

`vibedb-shard serve-rf3` opens retained local WAL, SQL, and apply artifacts and
serves authenticated RF3 protocols. `vibedb cluster dev` and the Kubernetes
renderer prepare fixed development/test topologies. Learner bootstrap,
replacement, split, move, schema-rollout, backup, and restore primitives are
present and have focused qualification paths. They are not a general
reconciliation operator or an elastic scaling product.

The checked-in Kubernetes lane creates one catalog group, one request-ledger
group, one data group, and one gateway. Three data replicas are replication,
not three data shards. Its successful qualification does not establish
multi-shard scaling, gateway HA, production PKI, arbitrary resharding, or
cloud/object-storage durability.

Current topology limitations are architectural, not documentation omissions:

- catalog and topology mutations serialize through one replicated catalog
  authority.
- placement is range/allocation based and does not keep all tenant data on one
  shard by default.
- the distributed server exposes one catalog/database identity. It has no
  logical-database create or lifecycle API.
- there is no common cross-group MVCC timestamp.
- replicas retain local files and WAL rather than immutable object-store
  segments.
- split and move require pre-provisioned, authenticated topology inventory.
- the explicit `get`, `read_batch`, and `exec_batch` lanes are exact-key shapes.
  RF3 SQL reads implement a bounded planner subset, not full SQL compatibility
  or PostgreSQL-compatible distributed ACID transactions.

See [Distributed feature state](distributed-feature-state.md) for the generated
source/test ledger. See the operation guides for the exact qualification and
manual-control boundaries.

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
  `gateway/replicated_query.go`, `gateway/replicated_sql_read.go`,
  `gateway/replicated_sql_transaction.go`, `gateway/pgwire.go`,
  `gateway/durable_sql_request_executor.go`,
  `gateway/replicated_request_service.go`,
  `gateway/replicated_request_issuer_collector.go`,
  `gateway/replicated_request_ledger_catalog.go`, `gateway/replicated_table.go`,
  and `shardservice/server.go`
- `cmd/vibedb-gateway/durable_request_runtime.go`,
  `durable_exec_batch_wire.go`, `issuer_open_wire.go`,
  `exec_batch_ack_wire.go`, `pgwire.go`, and `pgwire_ddl.go`
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
- `internal/splitcontroller/local_observation_provider.go` and
  `composite_shard_executor.go`
- `internal/hotshard/collector.go`, `controller.go`, and `operation_sink.go`
- `internal/rebalance/plan.go` and `internal/rebalanceexec/controller.go`
- `internal/kubeoperator/bootstrap.go`, `render.go`, and `validate.go`
