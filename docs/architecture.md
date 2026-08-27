# Architecture

VibeDB has four main layers:

1. The root facade owns database lifecycle and product durability profiles.
2. The heap and durable stores own JSON collections, indexes, and snapshots.
3. The query and SQL packages own parsing, planning, and local execution.
4. The experimental distributed packages own static SQL routing plus RF3
   point reads and a strict exact-key transaction path.

The internal Raft, replicated-state, and Raft-service packages form the RF3
serving composition. `vibedb-shard serve-rf3` constructs it from one or more
externally prepared stable three-voter group bundles. The public gateway uses
it for canonical point `get` requests and
multi-table `read_batch` and `exec_batch` operations over one or more RF3
groups. General SQL and non-exact scatter reads remain on the static path.

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

The gateway has four distinct public data paths:

```text
general SQL request       -> gateway -> static shard service -> local SQL catalog
canonical point get      -> gateway -> replicated catalog route -> RF3 native relation
exact-key read_batch     -> gateway -> pinned catalog -> one RF3 read cut per group
exact-key exec_batch     -> gateway -> RF3 transaction orchestrator -> RF3 native relations
```

The gateway pins one immutable catalog generation per attempt, proves a bounded route,
dispatches SQL and typed parameters, and merges the complete result. The shard
admits distribution, allocation, routing, and ownership identity before SQL.

The replicated catalog stores exact RF3 shard coordinates and base-table
profiles. A profile binds the table, primary-key path, dense relation ID,
schema generation, relation-manifest digest, and key and document limits. A
point read pins one catalog generation and resolves the ordered key without a
SQL or string conversion.

The replicated catalog also has an exact schema rollout protocol. Shards can
prepare immutable relation bundles under a crash-safe journal and return
contract-bound receipts. Catalog authority can record one prepared rollout,
activate the exact target generation, or abort before activation. The shard
installer supports authorization, activation, old-generation drain, and reopen.
The experimental `vibedb-gateway schema-rollout` command composes the
authenticated control client and bounded controller. It consumes an exact
target catalog and per-replica bundles. It does not expose general SQL DDL.

An `exec_batch` that resolves every statement to replicated table metadata and
one or more RF3 groups takes the replicated transaction path. The gateway
validates the complete batch before shard I/O and lowers supported SQL
to numeric relation batches over ordered key and document bytes. The supported
shapes are single- or multi-row whole-document insert, an exact-primary-key
whole-document update, and exact-primary-key delete with equality or a finite
`IN` key set. Mutations for co-located tables share one group participant.
Same-group multi-statement and multi-relation batches remain atomic. A ready
global index becomes one or more independently routed relation participants.
Update and delete bind index removal to the exact prior base value. Mixed
static/RF3 batches, repeated relation keys, and residual predicates fail closed
instead of falling back after partial RF3 execution.

The RF3 write boundary is durable and sequenced. An authenticated client first
opens a fixed lane for its persisted installation ID. Replicated catalog
authority returns a grant digest. Each `exec_batch` then carries that exact
grant reference, one strictly monotonic lane sequence, and a nonzero 128-bit
request ID. The transport supplies the principal and tenant. The client cannot
substitute them in the request.

One fused home-group transition advances the contiguous issuer high-water and
creates the request head. The request digest covers the exact ordered SQL,
operation class, parameter kinds, boolean values, and parameter bytes. A gap,
rewind, forged grant, or reuse with different bytes fails closed. There is no
unsequenced or process-local RF3 fallback.

The request ledger stores adjacent immutable home ranges with exact route
authority. Its replicated grammar retains streamed plans, pending command
waves, terminal results, ACKs, bounded collection state, issuer lanes, and
contiguous issuer high-water. One logical execution pin fences the complete
transaction program by controller epoch and catalog-group applied index.

Replay reads replicated state before it plans new work. An outcome-unknown
duplicate resumes the sealed program and a terminal duplicate returns the same
result. A replacement gateway can perform both operations through the shared
catalog and ledger authority. The result carries an authenticated ACK
capability. An exact `ack_exec_batch` retry resumes bounded collection after a
lost response. A completed ACK retry performs no new write.

`vibedb-gateway serve` constructs the catalog-bound ledger topology, RF3 ledger
client, execution-pin sessions, distributed runner, issuer authority, terminal
authority, and ACK collector. Any missing durable authority fails startup.

The gateway shares one bounded authenticated native connection pool across
catalog, point-read, proposal, and transaction-recovery traffic. A bounded
four-way leader-hint cache avoids repeated probes. The executor validates the complete route and serving fence,
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
runs retained groups on bounded execution lanes with shared authenticated peer
transport, and serves the authenticated native replicated protocol. The local
`vibedb cluster dev` command can prepare and supervise an explicitly no-HA RF1
member or an RF3 development topology plus gateway. Learner bootstrap and a
resumable replica-move controller exist. They are not a general topology
operator. RF3 development assigns a distinct authenticated NodeID to every
role process and generates the strict replica-control inventory consumed by
automatic hot-shard split admission; a NodeID never ambiguously names multiple
control listeners. The dev inventory intentionally contains no replica-move
candidates because it provisions no certified cold target host. Public RF3
exact-key reads include bounded multi-table and multi-group batches with one
ReadIndex cut per group. Exact-key `exec_batch` supplies multi-table and
multi-group writes, including ready global-index maintenance. A common
cross-group MVCC timestamp remains absent.

RF3 safety authority does not use elapsed wall time for log order, reads, or
transaction recovery. Static and RF3 transaction recovery advance bounded
replicated pulses. Execution pins use controller epochs and applied-index lease
fences. Hot-shard evidence uses catalog generations and replicated authority
revisions. Local time still controls TLS validity, network and context
deadlines, retry scheduling, catalog-session deadline construction, and static
read-fence leases.

The static shard service accepts only its `ReadStrong` policy and serves a
statement-level snapshot from a statically configured leader endpoint. This
label is not a Raft linearizability proof because the serving path has no
election or replication. Multi-shard reads use short-lived scoped vector
fences. Ordinary writes require one-owner proof. A separate byte-bounded
protocol supports atomic write batches.

The static SQL path has no Raft replication, follower read, or endpoint
failover. The RF3 leader cache and retry logic apply to canonical point reads,
replicated catalog operations, and the exact-key transaction lane.

Range-split primitives and action services remain internal. The durable runtime
can reconstruct bounded source and child observations, exact plan admission,
dynamic action grants, source capture, child lifecycle, publication, and retained
pruning after restart. The gateway command can scan replicated split-operation
records and trigger configured shard-control endpoints. `serve-rf3` still passes
nil split and plan-admission handlers to its control mux. The commands therefore
do not form an end-to-end split data plane. The internal splitter scans
one certified source image once and routes each borrowed row to at most three
children. It uses a compiled `vibejson` placement program and does not use
`encoding/json`. It can omit the retained child copy.

The same package writes deterministic hash-chained child artifacts. A verifier
checks framing, key order, and document placement before it exposes a chunk.
An ordered tail translator derives one exact batch for every child, including
empty advances and shard-key moves. A non-serving child stage applies verified
rows and tail batches to one durable collection. It updates a constant-space
authenticated image accumulator with those durable effects and persists a
fixed-size cursor through an atomic file replacement on Unix. An optional replicated-state capture writes
each exact before-and-after transition in the same durable
transaction as its source publication. A terminal ownership-fence entry must
advance all mutable serving coordinates together. Every child durably records
its empty seal batch and seals its accumulated image in O(1) before a fixed-size
cutover certificate can be issued. Recovery can explicitly audit the physical
image once. The certificate binds every non-retained child image. Global-index
relations maintain a canonical placement accumulator so their range ownership
can also be certified without a cutover scan. A sealed stage
can initialize the standard replicated-state snapshot base in place without a
second durable user-row copy. The SQL driver holds an exclusive non-serving
claim while it receives the child. Activation converts that claim to a
base-pending replicated apply owner without changing the user collection
incarnation. That owner rejects proposal, apply, lookup, export, and SQL
serving. It accepts only the exact authenticated snapshot base. Transaction 2
certifies the base binding before the ordinary replicated apply owner is
exposed. A planned WAL identity breaks the bootstrap cycle. The final WAL is
allocated once from the newer snapshot base and is rechecked against the SQL
binding before the existing Raft runtime can adopt it.

The certificate is evidence, not topology authority. It authorizes only the
conditional catalog successor after a coherent voting quorum for every child
has applied at least the sealed source cut under the exact relation manifest.
The catalog CAS is published durably before destructive cleanup. Older catalog
leases must then drain.
An unforgeable sealed catalog capability binds the durable CAS receipt, drained serving generation,
operation identity, manifest, and cutover certificate. Only that witness authorizes retained cleanup, which plans bounded
ordered key batches, checkpoints each batch before proposal, and confirms exact
atomically captured replicated deletes. Concurrent retained-range writes are
advanced one captured entry at a time. The current serving path can apply and
capture an out-of-range post-publication write. Cleanup then detects that entry
and halts before further pruning or completion. A final persisted, bounded incremental scan certifies the retained
image without an unbounded controller turn. A crash may leave
duplicate physical bytes, but never a routing gap or overlapping authority.

The hot-shard path records routed request pressure in bounded per-allocation
lanes and publishes canonical cuts through catalog RF3 when the gateway receives
`-hot-shard-capacity`. An internal clockless controller consumes those cuts,
qualifies sustained pressure, selects one split or replica move, and hands an
idempotent admission to the existing journals. The gateway command publishes
the pressure cut and, when an authenticated replica-control manifest supplies
the exact split and move authorities, runs the replicated pass and its
operation sink. Admission is fenced by catalog generation and replicated
pressure revision; tenant identity and wall time are not placement authority.

The topology scheduler consumes fixed-width, exact-generation capacity reports.
It nets source releases against target reservations in seven resource dimensions
and selects endpoints only when the maximum projected dominant pressure improves.
Current replicas, failure domains, receive concurrency, migration ingress, and
per-node concentration are hard bounds. The warm scheduler is fixed-memory and
allocation-free. The independent replica-control command path attaches Raft
member identities and drives the resumable `internal/rebalanceexec` sequence
when the operator supplies a strict control manifest. Pressure-selected
admission uses that same command path and catalog operation journal.
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
  `gateway/replicated_sql_read.go`, `gateway/replicated_sql_transaction.go`,
  `gateway/durable_sql_request_executor.go`,
  `gateway/replicated_request_service.go`,
  `gateway/replicated_request_issuer_collector.go`,
  `gateway/replicated_request_ledger_catalog.go`, `gateway/replicated_table.go`,
  and `shardservice/server.go`
- `cmd/vibedb-gateway/durable_request_runtime.go`,
  `durable_exec_batch_wire.go`, `issuer_open_wire.go`, and
  `exec_batch_ack_wire.go`
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
