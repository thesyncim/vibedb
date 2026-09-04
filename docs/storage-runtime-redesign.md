# Storage and runtime redesign target

This is the implementation direction for the active performance goal. Breaking
formats and interfaces is authorized. The target is a compact engine that shares
durable work across ranges, serves versioned reads without group-wide exclusion,
and publishes new schema generations without stopping ordinary traffic. CRDB
is a comparison baseline; copying its components is not an acceptance criterion.

## Node-owned durability and scheduling

Move log files, append queues, checkpoint scheduling and device admission to a
physical node/store. Raft groups retain independent ordering and identities, but
a bounded batch of ready groups shares one durable append barrier. A slow group
must not occupy the execution lane while its disk operation is pending.

Current implementation: `prepare-node-rf3` and an explicit `node_log` serving
manifest connect fresh multi-group preparation, startup, schema log access,
runtime adoption and shutdown to one node owner. The append sequencer and
checkpoint worker are shared. Linux tests cover 17 prepared groups, two-group
SQL recovery/swap, and three serving instances electing leaders before and after
restart. See [scope and raw qualification](qualification/node-serving-2026-09-04/README.md).

Fresh hot-group registration now authenticates a prepared SQL root and static
bootstrap before durably registering its descriptor through the live sequencer.
Retries preserve newer log history and incarnation state. `cluster dev --node-log`
and the comparison runner’s `--node-log` select this fresh path. The segmented
capacity contract permits valid history spanning multiple segment entry limits.
See [registration qualification](qualification/node-registration-2026-09-04/README.md).

Node-log split/replica movement, interrupted admission at process-crash boundaries,
sustained reclamation and simultaneous multi-group fault histories remain. The
[shipped node-log fault campaign](qualification/node-fault-2026-09-04/README.md)
now passes acknowledged-write, lost-reply, partition and restart checks for one
RF3 group across three processes. The first
[matched node-log comparison](benchmarks/crdb-sql-2026-09-04-node/README.md) validates
120,000 samples but still trails CRDB on every workload. Fixture allocation falls
to about 1.34 GiB; sustained amplification is unqualified. C8 grouped-scan relative
throughput improves while C8 update relative throughput regresses.

Continue the new node manifest and fresh preparation format. Do not spend this
unreleased redesign building a legacy data migration framework. Preserve exact
cluster/group/store identity checks, durable incarnation allocation and retained
checkpoint authority in the new format. Recovery must reconstruct acknowledged
writes from the retained log plus authenticated checkpoints.

## Versioned storage and compact execution layout

Target immutable base blocks plus compact versioned deltas, with stable row
identifiers and a primary-key access structure. Measure row-oriented hot-point
access and typed column-oriented scan blocks as parts of the same engine. Avoid
duplicating full documents across indexes, journals and scan representations.
Choose block sizes and representations from byte, cache-miss and update-cost
measurements; SIMD optimizes the selected representation rather than rescuing
an expensive layout. The minimum compiler remains Go 1.27.

Committed data and unresolved transaction writes need separate visibility.
Readers retain a validated version; unrelated intents must not stop every read
in a group. Serializable validation must cover keys and predicates across ranges,
including absent-key reads and schema changes. Reclamation follows the oldest
live read/schema/replication retention bound, with explicit backpressure.

## Schema publication and placement

Publish immutable schema generations. New requests bind the published generation;
existing requests retain the descriptors and storage interpretation they began
with. Backfill runs incrementally, validates concurrent changes, and publishes
only after its cutover proof is durable. Old generations retire after their
readers and recovery obligations drain.

Decouple physical storage blocks, scheduling lanes, Raft ranges and placement
units. Split or move the unit that is hot rather than forcing every concern to
share a process-sized boundary. A single conflicting key still requires an
order; report that limit and optimize batching instead of claiming unlimited
scaling for identical-key writes.

## Measured implementation gates

1. Shipped node-log composition: multiple groups per node, independent group
   progress, shared sync counts, restart, interrupted group admission and reclaim.
2. Space: fixed bytes per empty range; live/logical bytes and peak/steady
   amplification under growth, updates, deletes and retention release.
3. Read/write performance: uniform, skewed, mixed and contested workloads,
   growing range and node counts, all errors and latency tails retained.
4. Transaction/schema correctness: adversarial histories, failover, clock and
   network faults, concurrent schema publication, and recovery from every durable
   publication boundary.

This file describes the target and current integration gaps. It does not claim
that versioned storage, serializable SQL transactions or lock-free schema
evolution have already been implemented or qualified.
