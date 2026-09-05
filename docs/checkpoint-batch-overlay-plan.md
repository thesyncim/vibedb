# Stage row overlays for checkpoint-group batches

The next storage experiment targets repeated compressed-leaf reconstruction
in RF3 apply. `ApplyNormalBatch` already combines committed commands into one
checkpoint-group transaction. However, `commitTransitionLocked` stages each
dirty collection through `stagePrimaryBatchForJournalLocked`, which first
materializes any existing overlay and then renders and compresses complete
leaves. The profile at `0383eb86` attributes 12.6–25.8% of node CPU to this
path. That identifies work to remove; it is not an end-to-end speedup estimate.

The distinct proposed path stages immutable row replacements in the existing
primary overlay, then folds them into compressed leaves later. It does not
repeat the reverted compact-column patch experiment. Initial eligibility is
deliberately precise: checkpoint-group ownership, nonopaque data, no secondary
indexes, and an entire batch of existing inline replacements with a proved
eventual encoding bound. Unsupported batches keep their current copy-on-write
path and recovery behavior.

For comparison, Pebble separates batch mutation insertion from ordered
visibility publication; concurrent batches cannot advance its visible sequence
past an unfinished predecessor. Its source also distinguishes visibility from
completion of WAL synchronization. This is architectural context, not a claim
about the exact measured CRDB build, and it does not authorize relaxing VibeDB's
existing durability fences. See the primary [Pebble commit pipeline source](https://github.com/cockroachdb/pebble/blob/master/commit.go)
and [atomic batch contract](https://github.com/cockroachdb/pebble/blob/master/batch.go),
consulted on 2026-09-04.

## Preparation and publication

Prepare the full replacement vector at one collection generation using private
record and arena cursors, private hash and bucket heads, and cumulative fold
reservations. Repeatedly calling the current single-record `prepare` is invalid:
it would reuse the same unpublished count and arena position. Existing overlay
records and the admitted base both participate in current-value resolution.

No visible record/head/count/arena-prefix, aggregate, generation or collection
state may change on failed preparation. A complete staged batch owns all its
bytes until the existing infallible publication phase. Publish only under the
checkpoint group's existing all-member snapshot gates and decision ordering.
Retain kind-4 conditional journal records, the marker decision, `visibleTxn`,
`visibleApplied`, and every Raft append/commit/apply/result-settlement fence.

Reserve the complete eventual physical fold before publication: cumulative raw
size, dirty buckets, leaf/parent budgets and old-reader retention all count.
Pressure must unwind preparation and enter the existing checkpoint/retry path.
`checkpointGroupPhysicalFence` must continue rejecting materialization of an
uncertified suffix. Old records remain immutable; no arena reuse shortcut is
allowed while readers can reach them. Multiple records at one generation must
not enter the existing single-record-per-generation delta shortcut.

Source review rejected compressed payload size plus raw row-length deltas as
that encoding proof. Equal-length replacements can widen a complete column or
create new shapes, including when the base extent starts below its maximum.
The fold encoder cannot split a leaf after publication. The initial fast path
therefore needs a certified integer-column envelope: immutable shape/template,
one changed numeric hole per pending bucket epoch, and a conservative bound
that includes the encoder's dictionary preference and summary growth. Later
updates must retain that certificate or account for the union of all changed
columns. A maximum-extent check alone is insufficient.

The batch must also retain each mutation's own routing hash. Bucket traversal
may accept equal generations only with strictly decreasing immutable record
indexes and valid published bounds; the journal delta shortcut must still
reject multiple records at one generation. Retention reservations must include
all earlier dirty overlay buckets as well as this batch and pending physical
parents. Sign-specific checked addition must protect the aggregate arithmetic.

## Evidence required

Check same-leaf and cross-leaf batches, hash/bucket collisions, repeated updates
over an existing overlay, failed preparation and retry, pressure and fallback,
old pinned readers, snapshot folds and durable reopen. Existing checkpoint-group
crash cuts must remain all-old or all-new, and replicated batch tests must retain
exact terminal outcomes and complete checkpoint recovery. Include unsupported
indexed, opaque, overflow and mixed operations in the parity checks.

Measure actual overlay use and fold work, then run sustained SIMD Linux/ARM64
C1/C8 comparisons through multiple checkpoints and with mixed reads that force
materialization. The overlay has a 32,768-record and at-most-8-MiB arena bound;
short runs alone can hide eventual fold costs. Retain both fixture orders and
all regressions, with locality and host activity disclosed. Report logical and
allocated storage bytes as well as throughput and tails.

## Remaining consensus costs

`runtime_pipelined.enqueueAppend` emits entry-append work followed by commit
HardState work. `NodeStore.persistWave` sends nonduplicate Ready work through
`seglog.Engine.PersistWave`, which synchronizes before completion. Roughly two
waves across three replicas is consistent with the measured C1 cluster total
of about six barriers per update; C8 amortizes them over a batch of commands.
These are not six serial waits on the client critical path. Background work
prevents exact attribution from aggregate counters alone.

The node sequencer already combines distinct groups, and apply already combines
committed commands. Extra batching delay has no demonstrated C1 benefit here.
This storage change is expected to leave append-barrier counts unchanged. It
does not establish transaction parity, independent-machine scaling, nonblocking
schema completion or the ≥2× CockroachDB performance objective.
