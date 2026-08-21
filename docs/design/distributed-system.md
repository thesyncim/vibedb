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

The current leader-only bridge gives read-only fan-out a coherent vector cut:
the gateway acquires one leased raw-ID fence on every routed shard in parallel,
executes the reads, and releases the cut. Fences and writers carry the same
virtual-bucket scopes, so unrelated traffic proceeds. Acquisition is an
all-or-nothing try/release loop; it cannot deadlock against a transaction that
wins participant admission on another shard. An ordinary one-shard read never
enters this protocol and remains one request/response.

Replicated serving replaces that bridge with one certified read timestamp.
Each participant waits until its applied/closed timestamp covers the cut, using
the useful CRDB property—timestamped MVCC visibility backed by consensus—without
putting a timestamp-oracle round trip on routed work. A read-write transaction
additionally validates its read dependencies before the commit decision.
Session vectors remain available for causal reads that do not require a
cluster-wide snapshot.

The CRDB reference is deliberately limited to its strongest correctness
mechanisms: [serializable read refresh, recoverable parallel commit, and
Raft-synchronized closed
timestamps](https://www.cockroachlabs.com/docs/v26.3/architecture/transaction-layer),
plus leaseholder/range fencing. These protect the distributed lane and follower
reads; they do not add a timestamp-oracle, DistSQL, or transaction-record round
trip to a proven one-bucket routed operation.

## Indexes

Shard-local indexes remain the preferred colocated access path. A global index
is an ordinary hidden relation with its own distribution and Raft groups; there
is no central index service. Its ordered key is:

```text
non-unique: (encoded index values, encoded base locator)
unique:     (encoded index values) -> encoded base locator
```

An entry may carry a bounded covering payload. Equality and range predicates
route to index bucket intervals, and returned base locators are grouped by
owner before fetch. Strong index maintenance participates in the same
transaction as the base mutation. A globally unique index is owned by the
bucket containing its encoded unique key, which serializes conflicting claims
without a cluster-wide lock.

The current implementation compiles lifecycle-wide global-index key/locator
programs with `vibejson`. INSERT, UPDATE, and DELETE maintain base and index
participants in the same durable fixed-set transaction; UPDATE/DELETE capture
canonical old documents and validate the exact selected primary set inside the
serializable base participant. Index entries use canonical binary tuple keys
and compact scalar-array locators, with one shard-local ID/incarnation marker
instead of repeating that fence in every value. READY finite equality domains
expand under a hard Cartesian bound, group sorted locator keys by independent
index owner, pin one snapshot and send one request per index shard, then group
the returned locators by base owner and fetch exact native primary keys rather
than rescanning predicates. The index stores locator-only projection values,
not duplicated base documents.

An online build has one monotone incarnation and the following catalog states:

```text
BUILDING -> CATCHING_UP -> READY -> DRAINING
```

Foreground writes maintain BUILDING and CATCHING_UP incarnations but reads use
only READY. Backfill workers scan disjoint base-shard primary ranges through a
bounded exclusive native cursor. Every idempotent index PUT commits with a
serializable digest check on the exact base document, so a concurrent
UPDATE/DELETE either maintains the entry after the PUT or conflicts the page;
the scanner cannot resurrect a stale entry. Tasks fence catalog generation,
index ID/incarnation, base routing version, and allocation generation, and the
controller checkpoints a cursor only after the page commits.

Each executor operation holds a catalog-generation lease. Publication and
lease acquisition share a short lock, which makes the local drain
acknowledgement stable: after BUILDING is published no late operation can pin an
older write plan. The build planner waits for that local drain automatically;
the cluster controller must gather the same acknowledgement from every serving
gateway before dispatching historical tasks, and publishes READY only after all
tasks complete. Cluster-wide ack aggregation and checkpoint scheduling remain
control-plane work. Retirement uses the same generation-drain primitive before
reclaiming an old incarnation.

Statistics are collected per bucket range and merged into bounded catalog
sketches. Planning accounts for selectivity, covering width, base-fetch fanout,
skew, hot keys, network bytes, memory, and spill. Statistics never sit on the
foreground mutation path. Publication and request-bound encoding are
byte-native: `vibejson` validates and decodes canonical scalar strings directly
into caller scratch, and numeric bounds normalize from borrowed exact spellings
without stdlib JSON or intermediate mantissa strings. Ordered cross-shard
number merges validate once per cell and compare exact arbitrary exponents
without allocation inside the heap.

## Analytical lane

The analytical lane follows current ClickHouse's multi-stage, exchange-aware,
vectorized architecture—not the older model that forwards a complete query to
each shard—without turning foreground transactional storage into a MergeTree:

- scans produce fixed-capacity column vectors and retain raw document bytes for
  late materialization;
- declared scalar min/max summaries skip immutable primary stripes before
  decoding; exact-value, Bloom, and full-text summaries remain later bounded
  structures rather than automatic write amplification on every field;
- covering projections maintain alternate sort/order layouts through the same
  committed mutation stream as global indexes;
- predicates, runtime filters, projection pruning, partial aggregation, and
  local top-K are pushed below joins and exchanges;
- parallel hash joins partition build and probe batches into CPU-local lanes;
- one query may schedule disjoint ranges across caught-up replicas; and
- immutable snapshots and cold projections may live in object storage behind a
  content-addressed local cache, while foreground intents, journals, and Raft
  state remain on quorum-controlled local storage.

The reference direction is ClickHouse's current execution stack, not its old
`Distributed`-table bottleneck: [multi-stage distributed
execution](https://clickhouse.com/blog/multi-stage-distributed-query-execution-clickhouse-cloud)
repartitions intermediate rows between stages; [runtime join
filters](https://clickhouse.com/blog/clickhouse-fast-joins) reject probe work
before hash lookup; [distributed index
analysis](https://clickhouse.com/blog/index-sharding-clickhouse-cloud-petabyte-scale-indexing)
assigns index ranges independently of data reading; and current [batched lazy
materialization](https://clickhouse.com/blog/clickhouse-release-25-12) finds
row identities from narrow sort/filter columns before fetching wide payloads.
VibeDB adopts these operator shapes under certified transactional snapshots and
bounded ownership-aware work leases rather than copying ClickHouse Cloud's
shared-storage assumptions.

The row-oriented routed lane remains authoritative for point reads and writes.
Columnar projections are derived, incarnation-fenced structures: the optimizer
may ignore them without changing results, and repair can rebuild them from a
certified snapshot plus committed mutation tail.

The storage foundation for the first bullet is serving locally now. Up to eight
catalog-persisted RFC 6901 paths carry compact ordered scalar extrema in every
primary stripe. Query planning turns only sound conjunctive immutable scalar
comparisons into byte-native bounds; rejected stripes advance the primary graph
without decoding keys, reconstructing documents, or resolving overflow chains.
Containers, oversized scalar keys, overflow rows, and bounded metadata pressure
disable pruning for only the affected stripe/path. Updates conservatively widen
old extrema or take the normal deterministic full-leaf rebuild, so summaries
can add false positives but never false negatives. Distributed stage scheduling
and parallel-replica range assignment remain separate work.

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

The worker mailbox state machine now exists in `internal/exchange`.
Raw operation/stage/partition/attempt keys fence retries without formatted IDs;
registries reserve aggregate buffer capacity before publication; mailboxes
enforce per-batch, per-producer-credit, live-queue, total-stage, sequence,
producer-count, and deadline limits. Accepted payload ownership transfers as
opaque bytes without JSON or string conversion. Cancellation and deadline
expiry wake every blocked producer/consumer, and multiply-high partition
selection avoids modulo bias. Additive shard-wire commands now open, push,
pull with explicit acknowledgment, and cancel these mailboxes after ordinary
allocation/routing/ownership admission. Open and push retries are idempotent;
an unacknowledged pull is redelivered, so a lost response cannot drop a batch.
Authenticated peer admission remains required before enabling this transport
outside the current trusted/loopback boundary. General hash joins and range
exchange also remain pending; grouped hash reduction is serving and cost gated.

The intermediate data plane also has one compact row-block representation.
It frames opaque value bytes and nulls directly, validates bounded row/cell/byte
counts once, and then decodes borrowed cells without per-cell allocation. A
reusable partition-block set reserves worst-case memory before a stage starts
and grows individual arenas lazily. Exact composite JSON group identities feed
a fixed xxhash plus multiply-high reduction, so equal numeric/string spellings
always meet on one worker. The gateway lifecycle coordinator can open partitions
in bounded parallelism and its producer core emits retry-sequenced blocks plus
an explicit final batch for every partition. A read-only shard fragment now
wires its SQL cursor directly to persistent destination-worker connections;
the fragment carries only SQL/typed parameters plus bounded key ordinals and
ownership coordinates, never a serialized relational plan. Destination reducers
consume all input partitions concurrently with producers, use the same
byte-native exact combiner as the gateway fallback, and stream disjoint final
groups through a second bounded mailbox. A terminal output batch makes a
completed reduce retry detectable without replaying drained input. Physical
`OpRepartition` is selected when centralized grouped state violates the memory
objective; otherwise the lower-network gateway finalizer remains the winning
plan.

The first transport primitive now serves: an additive row-batch request keeps
the ordinary routed request/response bytes unchanged, while opted-in reads send
sequence-checked terminal frames with explicit per-batch and total row/byte
bounds. The gateway consumes each frame synchronously, validates the negotiated
limits again, borrows cell payloads directly from the bounded frame, and closes
rather than drains on consumer cancellation. Schema is sent only in sequence
zero. This removes the second whole-result wire copy and establishes exchange
backpressure; the current SQL cursor still materializes its locally bounded
result before framing, so this is not yet a vector-streaming scan or an
inter-node hash/range exchange.

The lower-cardinality multi-shard grouped fallback consumes this lane directly. A
bounded worker set opens shard streams in parallel; unbuffered per-request
handoffs let each active shard retain only its current decoded frame, while the
final merger drains in canonical route order for deterministic first-appearance
semantics. Group keys and winning extrema move into packed merger-owned byte
arenas before a frame is released. This is a bounded streaming gather/final
aggregate. Higher estimated grouped state uses the separate worker-to-worker
hash path above; range repartition and distributed joins remain pending.

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

Automatic hot-shard control operates on bucket intervals, never tenants. Nodes
publish bounded per-interval EWMAs for admitted rows/bytes, service time, queue
delay, CPU, storage growth, Raft-log pressure, and replica lag. The topology
controller first relocates a lease when capacity already exists, adds and
catches up a replica for read or failure-domain pressure, and splits then moves
an interval for sustained write/size skew. Decisions require a minimum observed
window, projected benefit above copy/catch-up cost, high/low watermarks,
cooldowns, and cluster-wide movement budgets; inverse cold intervals may merge
only after a longer quiet window. Catalog generation and ownership-epoch
cutovers use the movement protocol above, so a stale router cannot write both
sides.

A single virtual bucket is the irreducible placement unit in the current
format. Reads for one hot key can use certified followers or a derived cache,
but conflicting writes to one logical row must still serialize. Workloads that
need more write parallelism must include a real locality component in the
placement tuple; a future row-key intent refinement may reduce contention
inside a bucket, but the controller must never pretend that cloning one mutable
key creates independent write ownership.

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

1. **Serving:** preserve and benchmark the routed byte-native write/read lane.
2. **Serving:** add tenant-independent virtual buckets and affinity metadata
   without changing placement-scalar identity.
3. **Partly serving:** gateway transaction partitioning, durable recovery,
   scoped intents, and coherent leader-only vector cuts are present; replicated
   MVCC/closed timestamps replace the read-fence bridge when serving Raft is
   wired.
4. **In progress:** independently sharded global-index CRUD, batched finite-key
   locator projections, exact grouped base fetch, resumable online-build data
   plane, local generation drains, native primary-range skipping, and bounded
   ordered secondary-index range masks are present. Compact catalog-persisted
   scalar min/max summaries now prune non-indexed primary stripes through the
   local durable query path; cluster-wide build orchestration and richer
   bounded skip structures remain pending. Distributed statistics bounds and
   numeric merge keys are byte-native and allocation-free in their warmed hot
   loops.
5. **Partly serving:** shard-local execution already has bounded parallel
   batches, filter-first/lazy projection, exact-index pushdown, covering
   aggregates, adaptive joins, and spill. Distributed grouped COUNT/SUM/MIN/MAX
   now run as shard-local partial aggregation plus a memory-capped, exact
   columnar final stage. Parsed partial fragments remove shard-local final
   ordering and limits without serializing a plan or synthesizing SQL; bounded
   exact final sorting and O(OFFSET+LIMIT) top-K work even when group identities
   span shards. Path-projection DISTINCT reuses the same canonical grouped state.
   The additive row-batch wire lane now provides sequence-checked terminal
   frames, exact row/byte caps, synchronous backpressure, schema-once delivery,
   and borrowed byte-native decode while preserving the one-frame routed lane;
   grouped partial/final fan-out consumes it incrementally in deterministic
   route order without whole-shard response materialization.
   A bounded worker mailbox/partition state machine and owner-fenced shard-wire
   lifecycle commands, canonical blocks, exact partitioning, bounded gateway
   lifecycle/producer primitives, direct shard-cursor producers, partition-local
   grouped reducers, and memory-costed physical hash-repartition selection are
   also present. General hash/range join exchange follows, then runtime filters,
   batched row-ID late materialization, distributed index
   analysis, and guarded parallel-replica range scheduling.
6. **Partly present:** shard request completion can feed a striped fixed-space
   recorder with exact virtual-bucket pressure, service-time, result-byte, and
   fan-out evidence. Sustained decisions isolate no unit smaller than a virtual
   bucket, and desired split manifests are fenced by source generation,
   ownership epoch, bucket geometry, and lifetime allocation high-water.
   Controller window publication and capacity feedback remain to be wired.
   The non-serving Runtime/Host now expose context-free model-checked
   configuration proposals, detached applied publication, and exact quorum-safe
   `ReadIndex` outcomes without making reads consume worst-case WAL headroom.
   Serving Raft, external topology authorization, server-wired learner
   publication/transport, merge planning, and disaggregated immutable
   snapshot/cold-data caching remain pending; no desired
   split is serving authority until those gates pass. The replicated-state
   boundary can now stream and verify a deterministic hash-chained artifact for
   one certified applied cut with bounded reusable memory, apply resumed ranges
   into non-serving durable destination files without retaining an artifact
   copy, persist atomic hash-chain cursors, and fully validate a non-serving
   candidate. That candidate can bind a small exact certificate into a fresh
   immutable-base WAL and apply the ordered tail through ordinary Raft
   `AppendEntries`. An intact-shard stateless reconciler now binds exact
   membership, base digest, target apply/progress, promotion, leader transfer,
   replicated ownership-fence advance, expected-generation catalog CAS,
   generation drain, removal, and retirement evidence. A separate physical
   split kernel now binds the complete desired-manifest identity and performs a
   single source scan that routes each borrowed row through one compiled
   `vibejson` placement program into one fixed child range, without copying the
   retained child and without warmed row-path allocation. It does not persist
   or certify those filtered outputs, translate the ordered source tail into
   independent child groups, authenticate transport, provide topology
   authority, construct target SQL roots, wire servers, or grant serving
   ownership by itself.
7. **Pending:** topology workflows, TLS/auth, backup/PITR, CDC, quotas, and
   upgrades.

Every step requires deterministic encoding tests, crash/restart tests, stale
catalog and ownership fences, bounded-memory tests, fault injection, race and
vet gates, full repository tests, and allocation/latency/throughput benchmarks.
Competitive claims require reproducible end-to-end results; component
microbenchmarks are not a distributed database result.
