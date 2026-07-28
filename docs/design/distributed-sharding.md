# Distributed sharding and replication

**Status:** future design; contract and qualification plan only. No network
routing, replica protocol, distributed ownership, or cross-shard guarantee is
implemented today.

**Idea:** place independent durable collections behind stateless routers, give
each shard one fenced write owner and an explicit RF1, RF2, or RF3 durability
profile, keep the strongly consistent topology service out of the steady-state
query path, and move ranges with a copy/catch-up/verify/switch workflow. Scale
readers by adding replicas and writers by adding independently owned shards.

**Decision:** follow the Vitess separation of a cached, strongly consistent
control plane from a primary/replica data plane, while retaining vibedb's
canonical-root storage model inside every shard. Do not claim CockroachDB's
global transaction or snapshot contract until the separate gates for those
features pass. "No Raft" means no repository-owned Raft group in the ordinary
query path; a linearizable topology and fencing authority is still required,
and acknowledged shard writes still require quorum-choice semantics.

## Why this is a separate design

A macro-tablet is an in-file lexical subtree. `TabletID`, `LocalLeafID`, and
`BucketID` name storage inside one `StateRoot`; the proposed
[parallel tablet writers](parallel-tablet-writers.md) share that root and one
publisher. A distributed shard is different:

- it is a placement, replication, ownership, and failure unit;
- it has an independent root, journal, cache, and file lifetime;
- it owns a range in a logical keyspace rather than a subtree reference in
  another shard's root; and
- its failure cannot make another shard's root invalid.

This document therefore reserves *tablet* for the existing in-file structure
and *shard* for the distributed unit. A local tablet split remains one bounded
storage transaction. A distributed shard split is a long-running workflow.

The current authoritative atomic unit is one `durable.Collection.Update`.
Initially a shard is therefore identified by `(keyspace, collection, ShardID)`
and owns one physical collection partition. Co-sharded collections may share
placement, but a transaction that writes more than one collection remains
unsupported until a common shard-group commit protocol lands.

## Contract before mechanism

The first release target is a strong shard-local contract, not a transparent
CockroachDB clone.

| Operation or failure | Initial contract |
| --- | --- |
| point read from owner | current and linearizable with respect to acknowledged writes |
| point or batch write | atomic within one collection shard |
| acknowledged synchronous write | durable on the configured RF1, RF2, or RF3 write quorum |
| one crash or permanent disk loss | profile-specific: RF3 stays writable, RF2 preserves acknowledged data but stops writes, and RF1 may lose data |
| minority network partition | loses write availability; never gains a second owner |
| topology outage | cached routes continue while the current ownership lease remains valid; writes stop before that lease becomes uncertain |
| replica read | explicitly stale at an advertised applied `CommitSequence`, or waits for a supplied session token |
| cross-shard read | one pinned local snapshot per shard under one `RoutingVersion`; not initially a common real-time snapshot |
| cross-shard write | rejected before any participant publishes |
| online reshard | copy, catch up, verify, fence, switch, reverse-stream, then retire |
| synchronous replica/profile change | seed and verify asynchronously, then atomically activate exactly the old or new configuration |
| writer-node addition | split or move ranges to new single-owner shards without making one shard multi-owner |

### Replication profiles

`RF` counts synchronous data members including the owner; it does not mean
"followers in addition to the owner." Extra asynchronous read replicas do not
count toward RF and never satisfy a write acknowledgement or promotion quorum
until a staged membership change admits them.

| Profile | Layout | Successful write | After one data-member loss |
| --- | --- | --- | --- |
| RF1 | owner only | local record and root are durable | unavailable if the owner is lost; acknowledged data may be lost |
| RF2 | owner plus one synchronous replica | both members durably accept | acknowledged data remains on one copy, but writes stop until repair or an explicit profile downgrade |
| RF3 | owner plus two synchronous replicas | owner and either replica durably accept | the remaining two can preserve history, elect an owner, and continue |

Failure claims require synchronous members in independent declared failure
domains. Two files on one host do not constitute RF2 fault tolerance.

RF3 is the production default and the only initial profile promising both
no acknowledged-write loss and continued write availability after any one
data-member failure. RF2 is a lower-cost, fail-closed profile. RF1 supports
development, bulk-load, and explicitly single-node deployments; it provides
local durability but no database-level redundancy. In a routed cluster, RF1
still requires fenced topology ownership.

Profiles never degrade automatically. Changing RF1/RF2/RF3 is a persisted
membership workflow: seed and verify added members before strengthening the
acknowledgement set; fence and drain removed members before weakening it.
An operator or keyspace policy must explicitly accept the new failure
contract.

RF2 and RF3 are not consensus-free protocols. Their writes become
quorum-chosen under the declared profile, and a later term may become writable
only after the required members install one history containing every possibly
acknowledged record. Those are consensus semantics even though the planned
protocol is a single-owner ordered log rather than an adoption of Raft. RF1
has no distributed data quorum, but its ownership lease still depends on the
consensus-backed topology service.

If the RF3 state machine cannot be proven, automatic failover is removed and
the contract falls back to Vitess-style operator-controlled reparenting.
Losing the required profile quorum, losing the control-plane quorum, or
violating the stated clock and fencing assumptions is unavailable rather than
silently lossy. Byzantine servers, a compromised topology authority, and
correlated loss of every durable copy are outside the initial fault model.

### Position relative to Vitess and CockroachDB

The intended split is Vitess-like routing and operations with a stronger,
explicit shard-replication contract. It is not CockroachDB-equivalent SQL.

| Property | Initial vibedb target | Typical Vitess shape | CockroachDB |
| --- | --- | --- | --- |
| routing | stateless cached router plus explicit shard key | VTGate plus Vindex/keyspace ranges | distributed SQL over automatically split ranges |
| write ownership | one fenced owner per shard | one MySQL primary per shard | one leaseholder over a Raft-replicated range |
| acknowledged durability | explicit RF1/RF2/RF3; RF3 waits for owner plus one durable replica | configured MySQL semi-sync or async replication | Raft quorum |
| current read | shard owner | primary tablet | leaseholder, with other modes explicitly selected |
| follower read | explicit eventual or `replica-at-least` | explicit tablet type and freshness policy | follower-read contract at a safe timestamp |
| cross-shard read initially | scatter over a declared vector of local cuts | scatter/gather without a universal transaction snapshot | one MVCC timestamp under the transaction contract |
| cross-shard write initially | rejected | optional modes including 2PC, with narrower isolation than serializable | distributed serializable transaction |
| resharding | VReplication-style copy/tail/verify/switch | VReplication workflows | automatic range split and rebalancing |

Exact claims are established by the gates in this document, not inherited
from either comparison system.

### Competitive hypothesis

The plausible winning lane is a locality-heavy workload whose transactions
and exact-index probes include the shard key. Its warm path is one router hop,
one owner, no topology lookup, and the existing canonical local read path;
independent shards can use independent writers. Replica reads can add
throughput when the caller accepts an explicit sequence or staleness contract.

That is a hypothesis, not a result. A synchronous write still pays follower
durability, and a workload dominated by global scans, globally serializable
transactions, or one hot shard key is not the intended advantage. "Beats
CockroachDB" is admissible only for a published matched-contract benchmark;
before Phase 7 it cannot describe the global SQL contract.

### Claims deliberately deferred

The initial contract does **not** claim:

- serializable transactions across shards or collections;
- a globally consistent timestamped snapshot;
- current reads from arbitrary replicas;
- active-active writers for one shard;
- automatic discovery of a correct application shard key;
- global lexical locality after hash-based placement;
- availability in both sides of a network partition; or
- that an asynchronous replica protects an acknowledged write.

Optional two-phase commit may later provide atomic cross-shard writes, but 2PC
alone does not provide serializable isolation or prevent fractured reads.
Those claims require a separate concurrency-control and retained-history
design.

## Terminology

The names are intentionally distinct from existing uses of generation and
epoch:

| Name | Meaning |
| --- | --- |
| `ShardID` | stable distributed placement identity |
| `RoutingVersion` | immutable version of the complete route manifest |
| `DurabilityProfile` | RF1, RF2, or RF3 acknowledgement and failure contract |
| `ReplicaSetVersion` | version of the synchronous members eligible under that profile |
| `ShardTerm` | monotonically increasing ownership/fencing term |
| `CommitSequence` | monotonically increasing logical mutation order across the shard lifetime |
| `StateRoot.Generation` | local physical publication generation; never a network log position |
| `AcceptedSequence` | highest contiguous record durably accepted by one member |
| `ChosenSequence` | highest contiguous record known to have reached a quorum |
| `AppliedSequence` | highest commit sequence published in a replica's canonical root |
| `SafeTime` | future timestamp below which a replica promises no earlier write can appear |
| `GCWatermark` | oldest root/log cut that may still be required |

External row identities and continuation state qualify the existing
`(BucketID, slot)` with `ShardID`. `BucketID` remains shard-local.

## Architecture

```text
application
    |
    v
VibeGate (stateless SQL/router/scatter-gather)
    |
    +-- shard A owner ---- replica A1
    |                \---- replica A2
    |
    +-- shard B owner ---- replica B1
    |                \---- replica B2
    |
    `-- shard C owner ---- replica C1
                     \---- replica C2

VibeTopo (linearizable metadata, locks, watches, and ownership leases)
VibeFlow (snapshot/catch-up/verify/switch/repair workflows)
```

The diagram shows RF3 shards. RF2 omits one synchronous replica and RF1 omits
both. The names describe roles, not required packages or processes.

### VibeGate

Routers are stateless and horizontally replicated. Each caches:

- collection schemas and shard-key extractors;
- keyspace-ID ranges;
- shard owners and read replicas;
- `RoutingVersion`, `DurabilityProfile`, `ReplicaSetVersion`, and `ShardTerm`;
  and
- query plans for single-shard and scatter execution.

A steady-state single-shard operation performs no topology RPC. A stale route
receives a typed response containing the newer routing version and owner hint;
the router refreshes and retries only when the request's idempotency rules
allow it.

### VibeTopo

The topology backend must provide linearizable compare-and-swap, watch, lock,
and renewable-lease operations. It contains small, infrequently changing
records:

```text
ClusterID
RoutingVersion
Keyspace {
    schema version
    shard-key function and version
    routes[] {
        [lower keyspace ID, upper keyspace ID)
        ShardID
        DurabilityProfile
        ReplicaSetVersion
        ShardTerm
        owner
        eligible replicas
        workflow state
    }
}
```

An external etcd, ZooKeeper, Consul, or equivalent service may implement this
interface. Using such a service externalizes consensus; it does not remove the
need for consensus from ownership changes or quorum choice from acknowledged
data.

No high-frequency load statistic belongs in the authoritative manifest.
Telemetry and scan advertisements are separately replaceable hints.

### VibeFlow

The workflow controller owns resumable operations:

- seed or replace a replica;
- strengthen or weaken a shard's durability profile;
- planned and emergency owner changes;
- split, merge, and move shards;
- verify logical agreement;
- rebuild a lagged replica from a snapshot; and
- retire old data after the rollback and snapshot windows close.

Every transition is persisted before its side effects are considered complete.
Any controller instance may resume an interrupted workflow.

## Routing and physical order

The routing decision and the local primary order must not be conflated.

### Selected default: hash the locality key

Each sharded collection declares an immutable shard-key extractor. For a
multi-tenant collection the default shape is:

```text
keyspaceID = H(tenantID)
localKey   = encode(tenantID, collection, primaryKey)
```

Shards own contiguous ranges of `keyspaceID`. Hashing distributes sequential
tenant or user identifiers while keeping one tenant's related rows and local
lexical scans together. The complete `localKey` remains authoritative inside
the shard and uses the ordered-tablet graph unchanged.

This choice has an explicit cost: a query without the shard key may contact
every candidate shard, and a global `ORDER BY primaryKey` performs a k-way
merge. It does not preserve the ordered-hybrid store's lexical order across
the complete cluster. The invariant that one shard's scan walks one canonical
representation remains intact; merging independent authoritative shard
streams is not base/delta reconciliation.

### Alternate raw-key range mode

A collection may instead route on the raw primary-key prefix. This preserves
targeted global lexical ranges and permits tablet-aligned movement when route
and tablet fences coincide. It also concentrates monotonically increasing
writes in the rightmost shard. An automatic splitter cannot make one hot key
or one sequential tail execute on multiple owners.

The two modes are visible schema choices. The system never silently changes a
collection's routing function.

### Shard-key changes

Changing a row's shard key is a delete plus insert across shards. It is
rejected in the initial contract. A later distributed transaction may perform
it atomically; a reshard workflow may rewrite the whole collection under a new
routing function.

## Very large logical tables

One logical SQL table or durable collection may span any number of shards.
Each route owns one disjoint subset of its rows and stores that subset in an
independent physical collection/root; the schema and logical table identity
remain shared. The authoritative routing manifest grows with shard ranges,
not with rows, so a multi-terabyte table does not imply row-level topology
metadata.

The shard key determines whether the largest unit can keep splitting:

| Workload shape | Suitable route | Cost |
| --- | --- | --- |
| many independently sized tenants | `H(tenantID)` | one tenant stays colocated, but an exceptional tenant remains one-owner limited |
| one very large tenant with broad parallel access | a fixed high-cardinality virtual bucket derived from `(tenantID, primaryKey)` | writes spread, but tenant-wide queries fan out across buckets |
| one very large tenant dominated by ordered ranges | raw `(tenantID, primaryKey)` ranges | targeted range scans stay ordered, but a sequential tail may be hot |

Virtual buckets are stable schema identities; physical shard splits move
bucket ranges without rehashing every row. The system never guesses or changes
this choice automatically. A shard key that maps an entire huge tenant to one
value is intentionally unsplittable until an explicit reshard-key migration.

Large-table splits remain online and resumable: pin a source snapshot, copy the
selected ranges, tail logical changes, verify, briefly fence the moving
ranges, and switch one routing version. The workflow may run for hours or
days, but foreground copy/checksum bandwidth, retained-log bytes, and
source-plus-destination disk amplification stay bounded. If catch-up exceeds
those bounds, the destination is reseeded from a newer snapshot rather than
forcing unbounded retention.

A query without a usable shard key scatters under fan-out and memory limits
and merges authoritative local streams. If the exact candidate set exceeds an
admission bound, the query is rejected explicitly rather than returning a
partial result. A bulk mutation spanning shards is decomposed into retry-safe
shard-local batches and is not globally atomic before the
distributed-transaction phase.

## Replicated commit stream

The existing recovery journal is paired to one store, bounded, and recycled
after checkpoint. It may contribute its logical batch-record codec, but it is
not the distributed replica log.

Each shard has a separate bounded replication log with a different lifetime.
An authoritative logical record contains:

```text
ClusterID
ShardID
DurabilityProfile
ReplicaSetVersion
ShardTerm
CommitSequence
previous record digest
request/idempotency ID
schema and routing version
ordered Put/Delete batch
record checksum
```

`CommitSequence` is independent of `StateRoot.Generation`: logical replicas
may checkpoint at different times and select different physical extents while
publishing identical documents.

The log retains records until every required replica has either consumed them
or installed a later verified snapshot. A configured byte and record ceiling
keeps retention bounded. A lagging replica is throttled, replaced from a
snapshot, or removed through a topology change before it can exhaust the
owner. It never forces unbounded growth.

Each member tracks accepted, chosen, and applied watermarks separately from
reader publication state. An accepted tail is not reader-visible. A record
becomes chosen after durable acceptance reaches the configured profile:
one-of-one for RF1, two-of-two for RF2, or two-of-three for RF3. A client
result is never successful before that point.

### Synchronous acknowledgement

For one stable term:

1. The owner validates and prepares the complete local mutation without
   publication.
2. It assigns the next sequence, appends the logical record locally, and
   advances its durable `AcceptedSequence`.
3. It sends the record to every eligible synchronous replica; RF1 skips this
   step.
4. A replica verifies replica-set version, term, sequence, digest, schema, and
   checksum, appends durably, advances `AcceptedSequence`, and acknowledges
   receipt.
5. After zero RF1, one RF2, or one RF3 follower acknowledgement, the record
   reaches its configured one-of-one, two-of-two, or two-of-three choice. The
   owner advances its durable chosen watermark, publishes the prepared
   canonical root, and returns success with `(ShardID, CommitSequence)`.
6. Replicas learn the chosen watermark, apply chosen records in order, and
   advance `AppliedSequence`; any copy outside the acknowledgement set catches
   up asynchronously.

RF2 and RF3 acknowledgement therefore covers two durable failure domains; RF1
covers only the owner's domain. It does not wait for a replica to make the row
reader-visible.

A failure after local acceptance but before the client receives success is an
ambiguous outcome. The owner may not skip or discard that sequence and
continue; it must choose the record, choose a no-op resolution under the same
request contract, or stop serving. Promotion preserves every possibly chosen
record and may adopt an accepted ambiguous tail. Durable request IDs make a
retry return the resolved result rather than apply the mutation twice.
Deduplication retention is a declared time/space contract carried in the
client token; after it expires, the service returns a typed indeterminate
result rather than silently reapplying an old request.

The protocol must never fall back from synchronous to asynchronous
acknowledgement. An unavailable eligible replica applies backpressure or fails
the write.

### Replica application and verification

Replicas replay the ordinary mutation path into their own canonical graph.
They expose:

- durable and applied sequence watermarks;
- the current record-chain digest;
- a logical snapshot digest for verification; and
- replication lag in records, bytes, and wall time.

Snapshot transfer may use physical pages when formats and geometry match, but
logical agreement is authoritative. VDiff-style verification compares sorted
keys, exact values, index results, counts, and checksums at named cuts; root
byte equality is not required.

## Ownership, fencing, and failover

A route version or term stored only in the topology is not fencing. A
partitioned old owner cannot see a replacement term.

### Renewable ownership lease

The owner holds a topology-backed lease for `(ShardID, ShardTerm)` and converts
the authority's expiry into a conservative monotonic local deadline. It stops
admitting writes before that deadline can become uncertain. Replicas stop
acknowledging the owner at the same term deadline. The next term cannot be
granted until the prior lease is expired or the old owner is hard-fenced.

The clock-error budget, renewal interval, and safety margin are configuration
and qualification inputs. If the required bound cannot be established, the
deployment uses manual or infrastructure hard fencing rather than lease-based
automatic failover.

All data-plane servers start non-serving and read-only. Direct access that
bypasses the router and ownership check is outside the supported deployment.

### Promotion

Planned promotion:

1. acquire the shard workflow lock;
2. stop new owner admissions and drain accepted records;
3. bring the candidate to the current `ChosenSequence`;
4. close and relinquish the old lease;
5. grant the next term and publish the new route;
6. make the candidate writable; and
7. repoint and repair the remaining replicas.

Planned moves work for every profile while the current acknowledgement set is
healthy. Automatic emergency promotion after a data-member loss is initially
an RF3 feature: RF1 has no surviving synchronous candidate and RF2 cannot form
its two-member write quorum after either member is lost.

Emergency promotion:

1. acquire the workflow lock;
2. prove the old lease expired or hard-fence the old owner;
3. collect replica-set version, installed term, accepted/chosen sequence, and
   digest state from a replica quorum;
4. reconcile one contiguous prefix that preserves every record that could have
   been chosen by a prior quorum, resolving an ambiguous accepted tail
   deterministically;
5. durably install the new term and adopted prefix on a data quorum;
6. grant the matching ownership lease and publish the route;
7. make the candidate owner-current readable or writable only after it has
   applied the adopted prefix; and
8. keep divergent or unreachable replicas non-serving until repaired.

Selection is not "pick the most advanced replica." The protocol model must
define the term-install and prefix-selection rule and prove quorum
intersection across repeated promotions. This is the safety-critical core of
the design.

There is no automatic promotion without a control-plane quorum and a data
replica quorum. The minority remains unavailable.

### Replica replacement and profile changes

The membership state machine explicitly covers RF1, RF2, and RF3, with RF3 as
the production availability proof target. A replacement or newly added member
receives and verifies a snapshot plus log tail before it becomes eligible. The
current acknowledgement set installs the new `DurabilityProfile` and
`ReplicaSetVersion` before the new set activates; removed members are drained
and fenced before their data is retired.

Replica addition is a long-running preparation with one atomic activation:

1. Create the member as non-serving and ineligible for acknowledgements.
2. Install a verified snapshot and tail through the current
   `ChosenSequence`.
3. Stop new admissions briefly and drain the old configuration's accepted
   tail; persist that stop fence on the old write quorum.
4. Persist a prepared configuration record at one exact sequence on the old
   write quorum and every member required by the new profile.
5. Relinquish the old ownership lease. If the prior owner cannot confirm
   relinquishment, wait for lease expiry or hard-fence it.
6. In one topology transaction, increment `RoutingVersion`, compare-and-swap
   `ReplicaSetVersion` and `DurabilityProfile`, and grant the new
   `ShardTerm`/lease.
7. Resume acknowledgements only after the owner and new members observe that
   exact configuration; stale-term/config requests are rejected.
8. Drain removed members and retire them only after read, snapshot, retry, and
   rollback leases release them.

The topology transaction is the membership linearization point. Before it,
only the old set may acknowledge, although the persisted stop fence can leave
that set safely paused. Recovery may cancel or resume preparation under the
old configuration with a valid fenced term. After the transaction, only the
new set may acknowledge and recovery resumes that persisted set. No request is
acknowledged by a half-old/half-new policy. Once the transaction commits,
recovery only rolls forward or performs another fully fenced transition;
`ShardTerm` never moves backward.

Adding an asynchronous read replica uses the same seed/tail/verify preparation
but does not fence writes or change RF. One routing-manifest compare-and-swap
makes it visible with its advertised `AppliedSequence`; removal first makes it
unroutable, then drains existing reads before retirement.

Normal transitions require the current profile's write quorum. If RF1 loses
its owner or RF2 loses either member, the shard cannot change configuration
automatically. Recovery restores the missing member from a surviving copy or
backup, or uses an explicit disaster-recovery operation after infrastructure
hard fencing. Such a forced downgrade is never reported as ordinary failover
and cannot preserve a guarantee whose last durable copy was lost.

Concurrent membership changes are rejected. The deterministic model must
prove quorum intersection through every RF1↔RF2↔RF3 transition. A generalized
membership protocol is not inferred from a topology edit.

## Read contracts

Read freshness is selected per request rather than inferred from the endpoint:

| Mode | Source | Contract | Availability |
| --- | --- | --- | --- |
| `owner-current` | owner | current and linearizable within the shard | initial |
| `replica-eventual` | any healthy read replica | latest locally applied root; no time bound | initial |
| `replica-at-least(token)` | replica or owner | `AppliedSequence` is at least the supplied session sequence | initial |
| `replica-bounded(maxAge)` | safe-time-qualified replica | result is no older than the declared wall-time bound | future safe-time phase |
| `as-of(timestamp)` | any replica retaining the required root | one exact timestamped shard snapshot | future safe-time phase |

### Owner-current

The owner serves the current canonical root after all previously acknowledged
local publications. Reads require no replication or topology round trip.

### Eventual and session-consistent replicas

An eventual replica returns its highest fully applied canonical root. It never
serves an accepted or chosen-but-unapplied tail. This mode may be arbitrarily
stale within operational retention limits; a router's wall-time lag threshold
is a placement policy, not a correctness guarantee.

A write response supplies a session token containing at least
`RoutingVersion`, `ShardID`, `ShardTerm`, and `CommitSequence`. For
`replica-at-least(token)`, a replica serves only after its `AppliedSequence`
reaches that sequence. Read-your-writes therefore selects a sufficiently
applied replica, waits within the caller's deadline, or routes to the owner.

Asynchronous read replicas may be added and removed at runtime after
snapshot-plus-tail verification. They do not change RF and do not protect an
acknowledged write unless a separate membership workflow promotes them into
the synchronous set.

### Session tokens across resharding

A split or move cannot discard a token merely because ownership acquired a
new `ShardID`. Route activation publishes a durable transition certificate:

```text
source RoutingVersion, ShardID, ShardTerm
source final CommitSequence
target keyspace range, ShardID, ShardTerm
target activation AppliedSequence and root digest
expiry
```

For a point read, the router selects the certificate covering the key. For a
range read, it builds the required target vector. A target may satisfy an old
token only when the certificate covers the token's source sequence and proves
that the target's applied activation root includes every source mutation for
its range through that cut. It then waits for at least the target activation
sequence before serving.

Transition certificates and source cut metadata remain available through the
session-token and retry-deduplication windows. A token that cannot be proven or
has expired returns a typed token-expired/indeterminate result; the router
never silently treats it as an unqualified stale read.

### Future safe-time reads

Timestamp-bounded follower reads and globally consistent snapshots require:

- a hybrid or logical clock assigned at commit;
- retained root history indexed by commit time;
- a per-shard `SafeTime` after which earlier writes are forbidden;
- transaction visibility rules across participants; and
- a cluster `GCWatermark`.

This is a separate promotion phase. Retaining roots disables otherwise-safe
in-place updates and delays extent reuse, so its write and space cost must be
measured rather than described as free.

`replica-bounded(maxAge)` requires an applied root whose state remains valid
through a closed timestamp `T >= now - maxAge`; otherwise it waits or routes
to the owner. `as-of(T)` selects the latest root at or before `T` only after
every involved shard has closed through `T`. A cross-shard eventual read
remains a vector of per-shard cuts, while a common `as-of(T)` becomes one
globally meaningful read timestamp.

## Cross-shard scans

A baseline scatter query captures one immutable `RoutingVersion`, resolves the
intersecting shard set, and asks each shard to pin a local snapshot. Results
are:

- concatenated in route order only when routing order is also query order; or
- merged by the requested logical key otherwise.

The coordinator propagates the selected read mode to every participant.
`owner-current` still means one current cut per shard, not that all cuts
coexisted. `replica-eventual` and session-token reads produce an explicit
vector of potentially different sequences. Only the future common
`as-of(timestamp)` mode claims one distributed read timestamp.

The snapshot vector is stable for the query lifetime but does not claim that
all roots coexisted at one real-time instant. A global-snapshot mode waits for
the future safe-time phase.

Every participant must accept the same `RoutingVersion` before the coordinator
emits a row. If any shard reports `MOVED`, the coordinator releases all pins
and restarts the whole query on one newer manifest; it never combines partial
results from different route versions. A route switch keeps already pinned
source snapshots readable until their leases expire.

### Lightweight scan metadata

The route manifest contains only authoritative keyspace bounds. Shards may
advertise counts, index coverage, and zone summaries tagged with
`(ShardTerm, CommitSequence, root digest)`.

A coordinator may prune a shard only when the summary is certified for the
exact pinned cut and proves exclusion. Stale or missing metadata may change
cost and ordering but never the answer. Otherwise the shard is scanned.

### Pagination

A distributed cursor owns:

- routing version;
- shard IDs and terms;
- pinned commit sequences;
- last logical key and tie-breaker;
- sort and predicate identity; and
- an expiry bounded by snapshot lease capacity.

The initial implementation uses server-side leased cursors rather than placing
an unbounded shard vector in a client token. Expiry returns a typed restart
error. Restarting begins strictly after the last complete logical key under a
fresh route and may only be offered for query shapes whose duplicate/omission
proof is explicit.

Long scans pin old roots and replication log cuts. Admission limits their
count, duration, and retained bytes.

## Online split, move, and merge

Every workflow is a persisted state machine:

```text
Prepare
  -> Copy
  -> Tail
  -> Verify
  -> SwitchReplicaReads
  -> FenceWrites
  -> CatchUp
  -> Activate
  -> Reverse
  -> Retire
```

### Split or move

1. Allocate destination replicas and record the intended non-serving routes.
2. Pin a source snapshot at `copySequence`.
3. Copy rows selected by the destination keyspace-ID ranges.
4. Stream later logical records through the same filter.
5. Verify exact source and target rows at a common sequence.
6. Optionally switch stale replica reads to exercise the targets.
7. Fence new source writes for the moving ranges.
8. Apply through the final source sequence and verify the chain.
9. Compare-and-swap one new `RoutingVersion` with transition certificates;
   targets begin serving under new terms and stale source requests return
   `MOVED`.
10. Reverse-stream target changes to the source during a bounded rollback
    window.
11. Retire source data only after rollback, snapshot, retry-deduplication, and
    backup retention windows close.

Cutover timeout cancels before route activation. After activation, recovery
rolls forward or uses the explicit reverse workflow; it never exposes
overlapping writable ranges.

### Merge

A merge uses the same workflow with multiple source ranges and one target.
It is permitted only when combined capacity and load remain under the
post-merge hysteresis floor.

### Failure requirements

Fault injection covers a crash or topology outage after every transition and
every external side effect. Resuming the workflow must produce exactly one
write authority and one complete authoritative logical row set for each
range; redundant or rollback copies remain non-serving. Copy and tail
operations are idempotent by `(source ShardID, CommitSequence, key)`.

## Automatic scaling policy

Automation chooses *when* and *where* to split or move after a human or schema
has chosen the routing function.

Per-shard telemetry includes:

- live and allocated bytes;
- read, write, and scan CPU;
- QPS and bandwidth;
- p50/p95/p99 latency by operation;
- hot-key and keyspace-ID histograms;
- replica lag and retained log bytes;
- snapshot-pinned retired bytes; and
- disk and cache pressure.

A split requires a sustained threshold, a candidate boundary that improves the
selected objective, sufficient destination capacity, and a cooldown since the
last placement change. The planner estimates the increase in scatter queries
before approving a boundary. Move and merge decisions use separate thresholds
to prevent oscillation.

One hot row, one hot tenant, and a raw-key sequential tail are reported as
unsplittable rather than repeatedly resharded.

### Adding and removing capacity at runtime

Registering a node makes its CPU, disk, and failure domain available to the
placement controller; it does not make that node a concurrent writer for an
existing shard. To add writer capacity, VibeFlow creates destination members
at the table's configured RF, seeds and verifies them, then runs the online
split or move workflow. Route activation gives each destination shard exactly
one fenced owner, so aggregate writer count rises with independently owned
shards.

Phase 5 exposes this as an operator-controlled runtime operation. Phase 6 may
trigger it automatically from sustained load and size thresholds. Removing a
writer node first moves every owned shard and drains its replica/cursor/log
obligations; an abrupt unregister never transfers ownership.

Reader scaling is independent: seed an asynchronous replica, catch it up,
verify it, and add it to stale-read routing. A separate staged membership
workflow can also change a shard among RF1, RF2, and RF3 at runtime. Neither
operation silently changes acknowledgement semantics.

Runtime writer scaling therefore works for a very large divisible table, but
never turns one indivisible hot shard key into multiple write authorities.

## Indexes, constraints, and transactions

Exact indexes and stable postings remain local to a collection shard. A query
with a shard key uses the local index. Without one, the router scatters the
index probe and merges exact results.

The initial contract excludes:

- global unique constraints;
- foreign keys crossing shards;
- a lookup index whose row and target update must commit on different shards;
- sequences requiring one globally serialized counter; and
- atomic DDL mixed with data movement.

Small immutable reference collections may be copied to every shard through a
verified workflow. Mutable lookup indexes need either careful ordered locking
with repair or a distributed transaction; they are not ordinary local exact
indexes.

The SQL layer currently provides snapshot isolation with first-committer-wins,
writes exactly one table per transaction, and rejects indexed transactional
writes on the current primary batch path. Distribution does not upgrade those
semantics. Before any "shard-local ACID SQL" claim spans collections, a
shard-group root or recoverable group transaction must close those local gaps.

## Bounded resources and backpressure

Every distributed resource has an open-time or cluster-policy bound:

- router route and plan cache;
- accepted request count and bytes;
- replication log records and bytes;
- follower in-flight records;
- snapshot-transfer bytes and workers;
- workflow concurrency per disk and node;
- deduplication records and retention;
- distributed cursor count and age;
- retained root and retired-extent bytes; and
- telemetry cardinality.

Crossing a bound backpressures, replaces a replica from a snapshot, rejects a
new cursor, or pauses a workflow. It never grows memory or disk without a
declared ceiling.

## Delivery plan

Each phase is independently useful and must pass before the next one broadens
the claim.

### Phase 0 — contract and deterministic model

Status: this document.

- Freeze the terms, failure model, acknowledgement point, and unsupported
  operations.
- Build a deterministic protocol model with an owner, zero to two synchronous
  replicas, optional read replicas, topology, client retries, clock bounds,
  dropped/reordered/duplicated messages, and crashes.
- Model accepted, quorum-chosen, applied, published, term-installed, and
  replica-set states separately.
- Prove no two valid owners overlap, each profile meets its declared
  acknowledged-write contract, and no later installed term can discard a
  possibly chosen record.

**Gate:** exhaustive bounded histories satisfy ownership safety,
profile-specific acknowledged-write retention, per-shard linearizability, and
termination when the profile's required members and a control-plane quorum
remain connected under the declared delay and clock bounds.

### Phase 1 — local replication record

- Add `ShardID`, `DurabilityProfile`, `ReplicaSetVersion`, `ShardTerm`,
  `CommitSequence`, request ID, and digest-chain codecs under
  `internal/storeio`.
- Add a distinct bounded replication-log file and snapshot watermark.
- Export and apply logical batch records through `durable.Collection`.
- Reuse record body encoding only where it remains independently versioned
  from the recovery journal.

**Gate:** byte-exact codec tests, corrupt/truncated/reordered rejection,
snapshot-plus-tail differential equality, idempotent replay, bounded full-log
behavior, and no store read-path change.

### Phase 2 — static RF1, RF2, and RF3 shards

- Preserve the existing local durable path as RF1, then add RF2 all-member
  acknowledgement and RF3 two-member quorum acknowledgement.
- Implement transport, flow control, durable follower receipt, ordered apply,
  eventual/session read modes, and snapshot installation.
- Keep ownership static; failure requires operator restart of the same owner.
- Expose accepted/chosen/applied watermarks and logical verification.

**Gate:** every single crash and I/O fault cut reopens to a profile-legal
prefix; RF1 loses no acknowledged write across an owner restart, RF2 loses no
acknowledged write after either single member loss but stops, and RF3 retains
a recoverable data quorum after any one member loss. Static owner loss remains
unavailable until Phase 3. Owner-current reads remain linearizable and a slow
follower cannot exhaust an unbounded resource.

### Phase 3 — topology and failover

- Define the topology interface and one supported backend.
- Add renewable ownership leases, quorum-installed terms, prefix
  reconciliation, planned promotion, emergency promotion, query buffering,
  and durable retry deduplication.
- Add one-at-a-time replica replacement and RF1↔RF2↔RF3 transitions with
  prepared configuration records, explicit replica-set versions, and one
  atomic topology cutover.
- Add startup read-only behavior and explicit infrastructure fencing hooks.

**Gate:** partition matrix proves one writable owner; minority writes stop;
RF3 promotion preserves every acknowledged record; RF1 and RF2 fail closed
when their required members are absent; stale routers and old owners cannot
publish; a crash at every membership transition leaves exactly the old or new
configuration authoritative, with a safe paused state permitted but no mixed
acknowledgement set; RF3 failover completes within the declared RTO when
quorums exist.

### Phase 4 — static multi-shard routing

- Add shard-key schema, keyspace-ID routing, route caching, typed `MOVED`
  responses, single-shard plans, and scatter reads.
- Keep placement operator-defined.
- Qualify local-key order and global merge semantics.

**Gate:** route intervals have no gaps or overlap; every key has exactly one
owner across stale-route retries; heap/durable/single-shard/distributed query
differentials agree; topology is absent from the steady query path.

### Phase 5 — online workflows

- Add replica seeding, logical filtered copy, tailing, VDiff, read switch,
  write fence, atomic route switch, reverse stream, and retirement.
- Support split and move first; merge second.
- Support operator-controlled node addition/removal and durability-profile
  changes through the same resumable workflow machinery.

**Gate:** crash after every workflow action resumes safely; concurrent mutation
and scan differentials show no missing or duplicate row; source deletion never
precedes every retention cut; pre-cutover session tokens translate to proven
target cuts or fail explicitly; foreground p99 stays within the declared
resharding budget.

### Phase 6 — automatic placement

- Add load collection, histograms, split-point selection, capacity placement,
  runtime node registration, throttling, hysteresis, cooldowns, and
  unsplittable-hotspot reporting.
- Automate only workflows already safe under operator initiation.

**Gate:** controlled skew and growth workloads scale without oscillation,
capacity oversubscription, or unbounded catch-up; adding eight independent
shards reaches at least 75% of ideal throughput scaling on matched hardware.

### Phase 7 — stronger distributed semantics

These are separate subprojects rather than a condition for initial sharding:

1. timestamped root history, `SafeTime`, and `GCWatermark`;
2. consistent global read snapshots, backup/restore cuts, and stateless
   resumable pagination;
3. atomic 2PC with replicated participant and decision recovery;
4. serializable cross-shard concurrency control;
5. shard-group atomicity across co-located collections; and
6. global constraints and lookup indexes.

Each new mode keeps the shard-local fast path structurally unchanged and
publishes its additional latency, storage, and availability contract.

## Qualification matrix

Correctness precedes competitive measurement.

### Safety and recovery

- crash owner, each replica, topology leader, and workflow controller at every
  protocol boundary;
- run the crash and partition matrix independently for RF1, RF2, RF3, and
  each permitted profile transition;
- drop, duplicate, delay, reorder, and partition every message class;
- inject torn records, lost writes, sync failures, ENOSPC, and corrupt
  snapshots;
- exercise stale routes, expired leases, duplicate requests, and late old-owner
  recovery;
- carry session tokens and cursors across every split/move cutover and
  retention expiry;
- exercise ambiguous accepted tails, repeated promotion, and replica
  replacement at every persisted transition;
- exercise mixed-version rolling upgrade, schema-version skew, and downgrade
  rejection;
- hold old snapshots through write, failover, split, rollback, and retirement;
- verify exact rows, indexes, order, and root/log reclamation after recovery.

The deterministic model is the oracle for protocol histories. Store crash tests
remain the oracle for physical roots.

### Performance and scale

Publish separate lanes for:

- one shard and 1/8/64 clients;
- 1/2/8/32 independent shards;
- RF1, RF2, and RF3 synchronous data-member profiles;
- zero, one, and three additional asynchronous read replicas;
- local, cross-AZ, and injected-latency placement;
- owner-current, replica-eventual, replica-at-least, bounded-stale, and as-of
  reads where implemented;
- shard-key point work, tenant range scans, and all-shard scans;
- steady state, replica catch-up, reshard, and failover; and
- uniform, Zipf, sequential-tail, and single-hot-key distributions.

Report throughput at a fixed p99 SLO, p50/p95/p99/max, errors and retries,
CPU/op, allocation, network bytes, device bytes, live/allocated disk, replica
lag, forced checkpoints, and recovery time.

Competitive CockroachDB and Vitess rows must match:

- acknowledgement and replica count;
- availability-zone topology and RTT;
- isolation and read freshness;
- indexes, constraints, and transaction shape;
- corpus, shard key, distribution, and client placement; and
- resharding/failure activity.

An RF1 or asynchronous result beside an RF3 synchronous competitor is labeled
as a different product contract, not a database-engine win.

Structural gates:

1. no topology RPC on a warm single-shard query;
2. exactly one router-to-owner hop for a routed operation;
3. zero follower round trips for RF1 and one eligible follower round trip for
   RF2/RF3 synchronous acknowledgement;
4. bounded queues, logs, snapshots, cursors, and workflow concurrency;
5. no reader-visible log, delta, or merge representation inside a shard;
6. no single-shard storage/read regression outside measured noise; and
7. at least 75% ideal throughput scaling from one to eight independent shards
   before an automatic-scaling claim.

## Honest limits

This architecture moves coordination out of the common case; it does not make
coordination disappear.

- The topology service is a consensus system even when vibedb does not
  implement its consensus protocol.
- RF2/RF3 synchronous replica durability costs at least one network round
  trip; RF1 has no redundant copy.
- RF1 cannot survive owner loss, and RF2 cannot continue writes after one
  member loss without an explicit repair or contract downgrade.
- Fresh reads do not scale across asynchronous followers.
- One hot shard key remains one-owner limited.
- Hash distribution trades global lexical locality for balanced writers.
- Cross-shard scans pay fan-out, merge, and snapshot-retention costs.
- Automatic resharding cannot repair a semantically wrong shard key cheaply.
- Strong global snapshots and transactions add history, coordination, and
  failure-recovery state.
- Operational correctness—backup, upgrades, placement, throttling, repair, and
  observability—is part of the database, not deployment polish.

## References and precedent

These sources inform the architecture; no implementation source is copied:

- [Vitess topology service](https://vitess.io/docs/24.0/concepts/topology-service/)
  describes cached consistent metadata outside the steady query path.
- [Vitess replication](https://vitess.io/docs/archive/21.0/reference/features/mysql-replication/)
  documents asynchronous and semi-synchronous primary/replica durability.
- [Vitess VReplication](https://vitess.io/docs/25.0/reference/vreplication/vreplication/)
  specifies resumable copy, catch-up, verification, routing switch, and
  journaling.
- [Vitess distributed transactions](https://vitess.io/docs/24.0/reference/features/distributed-transaction/)
  distinguishes shard-local ACID, best-effort multi-shard commits, and atomic
  2PC without full isolation.
- [PlanetScale sharding](https://planetscale.com/docs/vitess/sharding) and
  [Vindexes](https://planetscale.com/docs/vitess/sharding/vindexes) document
  explicit shard-key selection and keyspace-ID routing.
- [CockroachDB replication](https://www.cockroachlabs.com/docs/v26.2/architecture/replication-layer)
  and [transaction layer](https://www.cockroachlabs.com/docs/v26.2/architecture/transaction-layer/)
  define the stronger quorum and distributed-transaction comparison contract.
- Fischer, Lynch, and Paterson,
  [Impossibility of Distributed Consensus with One Faulty Process](https://www.cs.cornell.edu/courses/cs614/2003sp/papers/FLP85.pdf),
  gives the failure-model boundary behind ownership changes.
