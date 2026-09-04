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

Current implementation: startup and reload now use AdoptPipelinedRuntime.
NodeStore, NodeSubmissionSequencer, AdoptNodeRuntime and NodeCheckpointCoordinator
exist and have internal tests; the shipped command path still opens separate
legacy WALs. Completing the design means wiring preparation, startup, hot group
addition, checkpoint reclamation, schema recovery and shutdown through one
node owner. Adding another unused abstraction is insufficient.

Use a new node manifest and fresh preparation format. Do not spend this
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
