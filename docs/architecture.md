# Architecture

[Documentation](README.md) / [Design](design/README.md) · [Development status](status.md)

VibeDB is an embedded database first. SQL and distributed execution reuse the
same collection and publication machinery; they are not separate storage
engines.

## System map

```mermaid
flowchart TB
    App[Go application] --> Native[Native API]
    App --> Driver[database/sql driver]
    Client[PostgreSQL client] --> Frontend[pgwire frontend]
    Native --> Query[Query execution]
    Driver --> Query
    Frontend --> Coordinator[SQL coordinator]
    Coordinator --> Query
    Native --> Storage[Durable collections]
    Query --> Storage
    Storage --> Files[Pages and recovery journals]
```

The native facade owns the database lifecycle. The SQL driver adds a catalog,
table schemas, and SQL transactions. The query engine executes against pinned
sources from the heap or durable store; the heap store also serves as a
reference model. Read [storage engines](store.md) before owning those lower
handles directly.

In distributed mode, a physical node combines an embedded frontend, replicas
of many independent Raft groups, and shared persistence and scheduling:

```mermaid
flowchart LR
    Client[SQL or native client] --> Frontend
    subgraph Node["One physical node"]
        Frontend["Frontend<br/>catalog pin, planning, request identity"] --> Dispatch[Authenticated dispatch]
        Dispatch --> Local[Local replica owners]
        Local --> Lanes["Execution lanes<br/>one owner per lane"]
        Lanes --> Groups["Independent Raft groups<br/>catalog, request ledger, data"]
        Groups --> Log["Shared node log<br/>one sync per wave"]
        Groups --> Apply[Replicated apply]
        Apply --> Collections[Durable collections]
    end
    Dispatch --> Remote[Remote node over mTLS]
    Groups <--> Peers[Raft peers on other nodes]
```

Local dispatch avoids a socket and wire encoding but keeps identity,
authorization, bounds, and serving checks. The local launcher starts three
physical nodes by default (six is also available), each hosting one replica of
every RF3 group. Physical-node count, Raft-group count, and replication factor
are separate quantities. See the [local cluster guide](operations/local-cluster.md).

RF3 collection journals and transaction markers retain fixed file sizes through
portable allocation on macOS and Linux. Allocation is performed at creation;
reopen validates geometry and replays records. This avoids Linux-specific
private-block proofs on the normal RF3 path while retaining write durability
barriers. The stronger strict reservation mode remains a separate low-level
contract; see [durability](durability.md) and [header flags](format.md#recovery-journal).

Each owner retains its handles until dependent work is released. This applies
to borrowed bytes, snapshots, query results, sessions, and network reservations.

## Embedded write path

1. The facade validates the collection name, key, document, and selected
   profile.
2. JSON documents are canonicalized unless an expert low-level opaque mode is
   used. The root facade should be treated as JSON-only.
3. The collection builds a new immutable logical generation and maintains exact
   index postings with the primary row.
4. The selected durability lane records or schedules persistence.
5. Publication exposes one immutable generation to new readers.

Readers pin a generation and do not observe an in-place mutation of that
generation. Heap snapshots are lightweight immutable values. Durable snapshots
hold explicit leases and must be closed.

## Publication vocabulary

Three terms prevent misleading atomicity claims:

- **Logical publication** is the visible rows and exact-index postings.
- **Topology publication** changes a content-equivalent physical shape, such as
  a split or representation generation, without changing logical content.
- **Durable publication** is the recoverable cut established by the selected
  journal/root/certificate protocol.

A batch provides **logical failure-atomic publication**: all admitted logical
changes become visible or none do. Preparing that batch may first publish a
**content-equivalent topology generation**. **Generation may advance** even
when the later logical mutation is rejected. Code must compare content or the
appropriate durable fence, not infer “row changed” from a generation number.

## Embedded transaction path

Native transactions take a coherent database cut, stage changes per collection,
and validate serializable dependencies at commit. A one-collection commit uses
the collection batch path. A multi-collection durable commit prepares each
participant, synchronizes those prepares, records one decision, then publishes
the participant cut. An ambiguous decision poisons the catalog until reopen.

SQL uses the same durable collection machinery but adds a catalog, declared
table metadata, statement overlays, isolation levels, and savepoints. DDL is
not accepted inside a transaction.

## Query path

The typed query engine compiles immutable plans. Heap execution reads a pinned
snapshot. Durable execution late-binds persistent exact indexes, admits bounded
workspace, and falls back to a full scan when an optimization cannot fit. An
exact-index candidate posting is always rechecked against the document.
Full-text (`==>`) predicates bind a generation-pinned tin index per
execution and use its postings to restrict candidates. A heap tin index is
built from the same state being scanned, so a lone `==>` over its compact
candidate mask is answered by the mask without a per-row recheck; every
other full-text shape rechecks each candidate.

Result memory is a separate budget from intermediate work. One-off results and
session-owned results have different lifetimes; both must follow their API's
release rule.

## Distributed path

The [design guide](design/README.md) covers each step in depth.

1. **Route.** The frontend pins one immutable catalog generation and builds
   commands carrying exact route and command fences. Replicas refuse any
   mismatch before proposal admission, and the frontend rebuilds from the
   original request under a newer generation.
   See [request routing](design/routing.md).
2. **Replicate.** Each shard, the catalog, and the request ledger is an
   independent RF3 Raft group. Groups on one physical node share a node log
   (one sync per multi-group wave) and execution lanes, never consensus.
   See [replication](design/replication.md).
3. **Write exactly once.** Single-group writes use one proposal whose result the
   data group retains under the request identity. Multi-group writes run a
   sealed program in the request ledger through route-gate pins and a fused
   prepare/decide/apply protocol. After an uncertain response, recovery uses
   the original identity. See [exactly-once writes](design/exactly-once-writes.md)
   and [distributed transactions](design/distributed-transactions.md).
4. **Read.** A leader read is a linearizable per-group cut, through `ReadIndex`
   or an optional quorum read authority. Follower reads guarantee only a
   requested applied floor. Multi-group results combine independent cuts; there
   is no global timestamp. See [reads, leases, and time](design/reads-and-time.md).
5. **Change topology.** Replica replacement, node scale-in and scale-out, and
   hot-shard splits are durable, catalog-fenced operations. State moves as
   certified snapshot artifacts, never as Raft `MsgSnap`.
   See [topology changes](design/topology-changes.md).

## Why these boundaries exist

| Choice | Benefit | Cost or constraint |
| --- | --- | --- |
| Immutable published generations | Readers retain a coherent view while writers prepare a successor. | Long-held views retain resources and must be released. |
| Exact index candidates with document rechecks | Index acceleration preserves the document comparison rules. | Index maintenance and rechecks still consume work. |
| Bounded query workspaces | Admission and allocation have explicit limits. | A query can fall back or fail when a required operator cannot fit. |
| Shared physical-node persistence | Independent groups can share append and checkpoint scheduling. | Group identities, ordering, and acknowledgement fences must remain independent. |
| Co-located frontend and storage | Local requests can avoid transport work. | Remote routing and quorum coordination still apply. |

## Security boundaries

The embedded packages open no network listener. An embedding application owns
its process, filesystem, and network boundary.

Distributed service paths bind TLS 1.3 identities to exact binary NodeIDs and
separate traffic classes. Authorization policies grant explicit capabilities
at an exact policy generation. Development plaintext is opt-in and
literal-loopback only. TLS rotation can close an in-flight stream; a lost
response can therefore mean the operation committed. See the
[security model](design/security.md).

Writer locks coordinate cooperating processes. They cannot prevent an external
administrator or process from truncating, replacing, copying, or editing live
files.

## Boundaries that matter

- `ResidentBytes` bounds selected cache and mutable overlay memory, not total
  process RSS, exact-index epochs, catalogs, or every off-heap allocation.
- A successful socket write is not peer receipt or consensus acknowledgement.
- A follower applied-index floor is not a linearizable or bounded-staleness
  guarantee.
- Backup certificates bind per-group cuts; they are not global wall-clock
  snapshots.
- Autosplit records pressure and recommends split points; the split controller
  performs the durable, catalog-fenced split.
- The distributed runtime is development software with partial qualification;
  see the [distributed feature ledger](distributed-feature-state.md).
- The planner package is infrastructure. Operator names and test rules do not
  imply that every physical plan is used by SQL.

## Source map

| Area | Implementation and tests |
| --- | --- |
| Native ownership and transactions | [Facade](../vibedb.go), [transactions](../vibedb_txn.go), [snapshot regressions](../vibedb_txn_snapshot_internal_test.go) |
| Query and optimizer | [Query package](../query/), [planner](../planner/), [execution guide](design/query-execution.md) |
| Distributed design | [Design guide](design/README.md), [distributed internals](operations/distributed.md) |
| Physical-node provisioning | [Placement](../cmd/vibedb/cluster_dev_physical.go), [composition tests](../cmd/vibedb/cluster_dev_physical_test.go) |
| Embedded frontend | [serve-node](../cmd/vibedb-shard/serve_node.go), [gateway runtime](../internal/gatewayruntime/) |
| Consensus and persistence | [Replica ownership](../internal/raftmember/), [Multi-Raft](../internal/multiraft/), [node log](../internal/raftstore/) |
| Storage and apply | [Durable store](../store/durable/), [replicated state](../internal/replicatedstate/) |
