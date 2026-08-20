# Distributed system target

This document is the implementation contract for the unreleased distributed
tier. It does not describe a second protocol generation and it does not turn
unfinished work into a current capability. The current serving boundary remains
documented in [Distributed server boundary](distributed-sharding.md).

The target combines an explicitly routed, stateless fast path with replicated
distributed transactions and snapshots when an operation genuinely spans
shards. Single-shard work must not pay a cluster-wide timestamp, transaction,
or query-exchange tax. Cross-shard work must retain atomicity and a coherent
read cut instead of silently weakening correctness.

## Placement model

A tenant is an isolation and accounting identity, never a physical shard.
Tenant-scoped tables explicitly mark `TenantPath`; validation requires it to be
one component of a placement tuple with at least one workload locality key:

```text
(tenant, account)
(tenant, user)
(tenant, order)
```

The complete canonical placement tuple is hashed once with the current native
mapper and its high-order bits select a virtual bucket. The initial bucket space
is 20 bits. Catalog manifests map
contiguous bucket intervals to allocation identities, so routing is a bounded
binary search over ranges rather than a million-entry directory. The bucket
width is part of the distribution identity and cannot change in place.

One tenant therefore occupies as many buckets and physical shards as its
locality keys require. A small tenant touches only the buckets selected by its
few keys; a hot tenant spreads without a tenant-wide migration. Operators move,
split, and replicate bucket intervals, not tenants.

Tables that need cheap joins or transactions may join an affinity group. Every
member uses the same distribution, tuple codec, bucket width, and declared
affinity-key paths. Affinity is explicit metadata, not an inference from a
column named `tenant`. A request outside an affinity group uses the distributed
path.

## Execution lanes

### Routed lane

The gateway pins one immutable catalog generation, compiles `vibejson`
pointers once, derives the virtual bucket without materializing strings or JSON
trees, and sends one byte-native request to the owning shard. This lane covers:

- point and bounded-range reads whose placement domain is known;
- colocated joins and multi-table transactions in one affinity bucket;
- mutations whose base rows and shard-local indexes have one owner; and
- index lookups whose independently sharded index resolves to one bounded base
  route.

Autocommit remains one local transaction and one request/response. Replication
adds only the owning Raft group's quorum cost; the gateway does not create a
distributed transaction record for this lane.

### Distributed lane

The gateway partitions a mutation batch by allocation, chooses the first
canonical participant as coordinator, stages one bounded envelope per
participant in parallel, and uses the transaction protocol in
[Distributed transactions](distributed-transactions.md). Retries reuse the
same raw 128-bit identity. Recovery owns participant resolution and cleanup;
transport failure never becomes a new mutation.

Concurrency barriers are currently scoped to sorted half-open virtual-bucket
intervals. A transaction blocks an intersecting operation without stopping an
entire shard; absent scope metadata deliberately falls back to a whole-shard
barrier. A future row-key refinement can narrow hot buckets further without
changing participant identity. Admission bounds active intents, participants,
retained bytes, range count, and recovery work.

Read-only fan-out pins one catalog generation and one read timestamp. Each
participant waits until its applied/closed timestamp covers that cut. A
read-write transaction additionally validates its read dependencies before the
commit decision. Session vectors remain available for causal reads that do not
require a cluster-wide snapshot.

## Indexes

Shard-local indexes remain the preferred colocated access path. A global index
is an ordinary hidden relation with its own distribution and Raft groups; there
is no central index service. Its ordered key is:

```text
(encoded index values, encoded base primary key)
```

An entry may carry a bounded covering payload. Equality and range predicates
route to index bucket intervals, and returned base locators are grouped by
owner before fetch. Strong index maintenance participates in the same
transaction as the base mutation. A globally unique index is owned by the
bucket containing its encoded unique key, which serializes conflicting claims
without a cluster-wide lock.

An online build has one monotone incarnation and the following catalog states:

```text
BUILDING -> CATCHING_UP -> READY -> DRAINING
```

Workers backfill disjoint snapshot ranges, consume the retained mutation stream
from the snapshot cut, verify counts and digests by range, and only then publish
`READY`. Plans pin index ID and incarnation. Retirement waits for the oldest
pinned catalog generation before reclaiming storage.

Statistics are collected per bucket range and merged into bounded catalog
sketches. Planning accounts for selectivity, covering width, base-fetch fanout,
skew, hot keys, network bytes, memory, and spill. Statistics never sit on the
foreground mutation path.

## Analytical lane

The analytical lane borrows the strongest current ClickHouse ideas without
turning foreground transactional storage into a MergeTree:

- scans produce fixed-capacity column vectors and retain raw document bytes for
  late materialization;
- min/max, exact-value, Bloom, and full-text summaries skip immutable row
  groups before decoding;
- covering projections maintain alternate sort/order layouts through the same
  committed mutation stream as global indexes;
- predicates, runtime filters, projection pruning, partial aggregation, and
  local top-K are pushed below joins and exchanges;
- parallel hash joins partition build and probe batches into CPU-local lanes;
- one query may schedule disjoint ranges across caught-up replicas; and
- immutable snapshots and cold projections may live in object storage behind a
  content-addressed local cache, while foreground intents, journals, and Raft
  state remain on quorum-controlled local storage.

The row-oriented routed lane remains authoritative for point reads and writes.
Columnar projections are derived, incarnation-fenced structures: the optimizer
may ignore them without changing results, and repair can rebuild them from a
certified snapshot plus committed mutation tail.

## Distributed query execution

The gateway retains colocated execution whenever possible. Non-colocated plans
are split into vectorized stages joined by bounded exchange operators:

- broadcast only below an explicit byte and row threshold;
- hash repartition on canonical encoded keys;
- partial aggregation followed by merge/final aggregation;
- local top-K followed by merge top-K; and
- external runs for bounded-memory sort, distinct, hash join, and aggregation.

Top-K, filters, runtime join filters, and partial aggregates move as close to
scans as semantics allow. Stage placement accounts for data locality and
replica load; it does not force all intermediate state through the gateway.

Every exchange carries an operation deadline, byte/row budget, bounded channel,
and cancellation signal. Backpressure reaches shard scans; it never creates an
unbounded gateway buffer or one goroutine per row.

## Replication and movement

Every physical bucket interval belongs to one Raft group with a topology-issued
allocation generation and ownership epoch. Serving requires a quorum-backed
lease or equivalent leadership proof. Client request identities and
transaction transitions are replicated state-machine inputs, so leader loss
does not duplicate acknowledged work.

Read-only analytical work can use parallel replicas at one certified read
timestamp. Range leases prevent duplicate scan work, and losing a worker
reissues only its unfinished ranges. Replica parallelism never permits a
stale or mixed snapshot.

Movement uses snapshot plus ordered catch-up:

1. publish a non-serving destination allocation;
2. copy a snapshot at a certified applied position;
3. stream and apply the bounded delta from that position;
4. freeze the source interval briefly and close the remaining gap;
5. publish one catalog generation that transfers ownership; and
6. drain plans and transactions pinned to the old allocation before deletion.

Split and merge are the same protocol with different source and destination
intervals. In-flight transactions remain bound to their original allocation;
movement cannot translate a participant identity underneath them.

## Control, security, and recovery

The topology authority publishes signed, monotone catalog generations and owns
membership, placement, index-build, movement, backup, and restore workflows.
Node and gateway traffic uses mutually authenticated TLS. Authorization and
quotas are tenant-scoped even though placement is not.

Backups capture one catalog generation and one recoverable timestamp, then copy
content-addressed shard snapshots plus the bounded log tail required for PITR.
CDC consumes the same ordered committed mutation stream and includes raw
transaction identity for deduplication. Admission control accounts separately
for CPU, memory, scan bytes, exchange bytes, intent bytes, and recovery work.

Rolling upgrades preserve the single wire format through capability negotiation
and additive optional fields. A node refuses a command it cannot execute; the
cluster does not fork into named protocol generations.

## Delivery order and gates

1. Preserve and benchmark the routed byte-native write/read lane.
2. Add virtual buckets and affinity groups without changing placement-scalar
   identity.
3. Finish gateway transaction partitioning, recovery, scoped intents, and
   timestamped reads.
4. Add independently sharded global indexes, projections, data skipping, and
   online build.
5. Add vectorized multi-stage exchange, pushdown, parallel hash joins, spill,
   and parallel replica scheduling.
6. Wire the existing Raft foundation into serving, enable movement, and add
   disaggregated immutable snapshot/cold-data caching.
7. Add topology workflows, TLS/auth, backup/PITR, CDC, quotas, and upgrades.

Every step requires deterministic encoding tests, crash/restart tests, stale
catalog and ownership fences, bounded-memory tests, fault injection, race and
vet gates, full repository tests, and allocation/latency/throughput benchmarks.
Competitive claims require reproducible end-to-end results; component
microbenchmarks are not a distributed database result.
