# Parallel tablet writers

**Status:** executable design, validated against the tree at the
routed-splits/journal/epoch/batch state (post `c6df176`). Every claim below is
either **(verified)** against named code or **(projection)** with the
measurement that will decide it. The design assumes ONE layout — the ordered
primary graph with a single mutation path — because the legacy chunk layout's
source is deleted in a sibling worktree that merges before this work starts.

**Idea:** keep every mutation's tablet-local work parallel across tablets,
fence durability as a group through the one shared recovery journal, and
publish visibility as a group through the one serialized pointer-swap section
that already exists. The write path becomes a three-stage pipeline — *stage in
parallel, fence as a group, publish as a group* — and the pipeline lands
first on today's single writer (where it already wins the ordinary-sync lane)
before any sharding exists.

---

## 1. What this design reconciles with

The previous draft predated five landed systems. This section is the verified
inventory the rest of the design builds on; read it as "what is already true."

1. **The recovery journal is the acknowledgement primitive.** One append
   stream per store in a sibling preallocated file; `DurabilitySync`
   acknowledges by appending and syncing a redo record at the mutation's point
   of no return, BEFORE anything is visible
   (`journalBeforePublishLocked`, called from `cowBufferedPrimaryMutation`
   between the last fallible prepare step and the router/state publish —
   verified, `store_file_primary_mutation.go`). Buffered-visible +
   `Options.RecoveryJournal` journals AFTER publish (`journalAckLocked`).
   The batch record — one record, one CRC, one sync for N entries
   (`RecoveryRecordKindBatch`, `AppendBatch`, verified
   `internal/storeio/recovery_journal.go`) — is explicitly documented as the
   group-commit primitive whose "future group of INDEPENDENT acknowledgements
   sharing one sync is the same record with unrelated entries"
   ([recovery-journal.md](recovery-journal.md#batch-records--the-group-commit-primitive)).
   This design is that future.
2. **Readers are lock-free behind a writer-side fence.** Point reads claim an
   epoch slot and re-validate (`enterReadEpoch`, 4 ns entry); the writer
   raises the divert word inside `snapshotGate` and scans slots
   (`beginReaderFence` / `ReadEpochs.BeginWriterFence`, Dekker protocol
   documented on the type — verified, `internal/storeio/read_epochs.go`).
   The reclaim floor is `min(leases.Minimum, epochs.Minimum,
   oldestRecoveryGeneration)` computed in `ExtentReclaimer.AppendReusable`
   (verified, `internal/storeio/generation_leases.go`).
3. **One committer, manual checkpoints, one durable root.** The buffered and
   sync-journal lanes both run the committer in `ManualCheckpoint` mode;
   `checkpointBufferedLocked` = materialize pending parents → `Flush` →
   `MarkDurable` → journal `Recycle` in that order (verified,
   `store_file_inplace.go`). A flush-less materialize records its cut in
   `primaryCheckpointBase` so the next materialize derives from the in-memory
   cut, not a stale `durableState` (the double-retire fix — verified,
   `materializePrimaryParentsLocked`).
4. **Structural transactions are checkpoints.** Splits/merges/reclass hold
   the global writer, flush pending parents before planning
   (`flushPendingForStructural` — a full checkpoint), commit one bounded
   transaction, and flush again after publish so `durableState` passes the
   structural generation (verified, `store_file_primary_structural.go`).
5. **The exact index publishes with the mutation.** Posting tiles are
   per-leaf (`TileID = BucketID<<2 | quadrant`), so tablet-disjoint writers
   touch disjoint tiles; but the *resident* snapshot install
   (`installPrimaryExactResidentLocked`) swaps two collection-wide fields
   (`primaryLive`, `primaryExact`) under `snapshotGate`, and `prepare` copies
   the whole live map minus the mutated bucket (verified,
   `store_file_primary_exact_mutation.go`). The batch path refuses indexed
   collections today (`ErrPrimaryBatchIndexedUnsupported`).

Two further verified facts shape everything below:

- **The entire mutation path is serialized under one `sync.Mutex`**
  (`c.writer`), and on the ordinary-sync lane the 32.5 µs journal fdatasync is
  paid INSIDE that lock, so concurrent callers divide the fsync budget instead
  of sharing it (verified, `putPrimary` / `journalAckLocked`). This — not the
  journal itself — is the whole ordinary-sync loss.
- **The resident router is a single-writer seqlock.** `UpdateLeaf` brackets
  its row stores with `version.Add(1)` pairs; two concurrent updaters would
  interleave to an even version mid-write and let a reader admit a torn row
  (verified, `internal/storeio/resident_primary_router.go`). Router updates
  therefore stay inside one serialized publish section forever; sharding must
  never touch the router concurrently.

## 2. The numbers to beat, and where the losses come from

From [RESULTS.md](../../bench/competitive/RESULTS.md) (M4 Max, medians of 10):

| lane | vibedb today | best competitor | gap mechanism |
|---|---|---|---|
| buffered ycsb-a | 195,351 | Badger 241,812 | Badger amortizes across its own internal parallelism; our writer is one lock |
| buffered churn | 265,137 | Badger 316,547 | same |
| ordinary-sync ycsb-a | 17,949 | Badger 59,728 (SQLite 27,871) | each caller pays a private 32.5 µs journal sync under the writer lock; Badger group-commits its value log |
| power-safe ycsb-a | 378 | SQLite 431 | two full fences per commit vs one; the single-fence journal ack (4.05 ms) is wired but the group amortization is not |

Cost decomposition that the pipeline exploits (all measured): buffered apply
is ~4.7 µs per update (in-workload p50), hot in-place acks are ~412 ns,
ordinary journal sync is 32.5 µs, power-safe fence is 4.05 ms
(`F_FULLFSYNC` floor 4–5 ms), buffered checkpoint p50 330–355 µs, epoch read
entry 4 ns. The shape is unambiguous: **the sync is 7–860× the apply**, so
sharing one sync across N callers is worth far more than parallelizing the
apply — and it needs no sharding at all. Sharding then attacks the second
ceiling (serialized apply at ~4.7 µs ≈ 213 k/s).

## 3. Architecture: stage → fence → publish

Every write, on every lane, decomposes into the three stages the code already
separates for one writer:

1. **Stage (parallel per tablet).** Route, admission, leaf rewrite or
   in-place patch preparation, dirty-frame admission, posting contribution
   derivation. All fallible work. Tablet-local by construction: a leaf, its
   anchor path, its locator, and its tablet root reference nothing outside
   the tablet (`BucketID = TabletID<<12 | LocalLeafID`,
   [ordered-hybrid-store.md](ordered-hybrid-store.md)).
2. **Fence (grouped, shared journal).** One batch record covering every
   staged-but-unpublished member, one sync. Sync lane: fence BEFORE publish.
   Buffered lane: fence AFTER publish (its contract permits the volatile
   window). Same ordering rules as today, applied to groups.
3. **Publish (grouped, serialized pointer swaps).** One `snapshotGate` +
   reader-fence critical section installs the group: router row stores, exact
   resident swap, `pageValidator.update`, `publishFileState`, volatile
   retirements. This section is O(pointer swaps) — sub-microsecond — so
   serializing it is not the bottleneck **(projection: publish-section
   occupancy < 15 % at 8 writers; measured by a phase-2 counter gate)**.

The phases land the stages in reverse risk order: phase 1 groups the fence
under the existing single writer (largest win, no concurrency); phase 2
parallelizes staging.

## 4. Sharding model

### 4.1 Tablet-affine write tokens, caller-executes

A mutation routes to its tablet and acquires that tablet's **write token** — a
fixed array of futex-cheap mutexes indexed by `TabletID & mask`, allocated
once at open (no allocation, zero-GC directive holds). The caller's goroutine
executes the stage itself. There is **no dispatcher and no per-tablet writer
goroutine**: a goroutine hop costs hundreds of nanoseconds against a 412 ns
hot ack, and per-key ordering already falls out of
single-token-per-tablet (two writers to the same key contend on the same
token; distinct tablets never contend).

Token index aliasing (two tablets hashing to one token) is correctness-neutral
— it only serializes more than necessary — so the table can stay small
(e.g. 256 tokens) regardless of tablet count.

### 4.2 What stops being collection-global

The single-writer implementation keeps mutable scratch on the collection.
Under N writers each of these moves into a per-token **shard frame** or into
the serialized publish/checkpoint sections (verified inventory from
`store_file.go` / `store_file_primary_mutation.go`):

| today (collection field) | becomes |
|---|---|
| `pointKeyScratch`, `primaryLeafScratch`, `primaryRootScratch`, `bufferedValueBefore` | per-shard scratch (pure staging) |
| `bufferedFirstTouches` (in-place window) | per-shard table; entries are frame refs of the shard's own tablets |
| `primaryPendingParents` (bounded pending set) | per-shard slices; **global budget** enforced by an atomic count so total stays ≤ today's capacity; checkpoint drains all shards |
| `retireScratch`, `retireRefScratch` | checkpoint/structural-only (exclusive gate) — unchanged semantics |
| `primaryVolatileRetired` | publish-section-owned (leader mutates it) |
| dirty-cache budget (`ensureDirtyCapacityFor`) | atomic reservation against the same global budget; pressure escalates to a checkpoint (§6.3) |
| `super.FileEnd` for volatile refs | atomic virtual-extent cursor: staging fetch-adds the leaf size to reserve a unique volatile offset; published state's `FileEnd` is the high-water mark. The offsets are identities only — materialize rewrites them to real placed pages, so cursor holes from aborted stages are harmless (verified: `cowBufferedPrimaryMutation` admits at virtual offsets past FileEnd already) |

Generation assignment: stages seal their leaf frames with a **birth ticket**
drawn from the shared atomic generation counter; the publish leader publishes
the group at the group's maximum ticket, so root generations stay monotonic
and every frame's birth generation ≤ its publishing root generation. Journal
records never carry tickets — the batch record carries the published group
generation (one generation per record is the existing format, verified
`AppendBatch`), so journal generation order equals append order and the
`scanTail` monotonicity invariant is untouched. Cost of this choice: an
in-place eligibility check (`safeFromReaders(birth)`) becomes slightly
conservative when a ticket predates an active reader that could never have
seen the frame; that only forfeits an in-place patch to COW, never safety.

### 4.3 Cross-tablet batches (`Update`)

A `WriteBatch` touching k tablets acquires the k tokens **in ascending
TabletID order** (the canonical order, §10 — deadlock-free), stages every
leaf frame, and then enters the pipeline as ONE deposit: one batch record
(the record format already spans arbitrary buckets — entries are just
key/value/kind), one sync, one publish installing every leaf pointer under
one group generation. Failure atomicity is unchanged: all staging is fallible
and nothing is visible or journaled until the whole batch has staged
(verified: `updatePrimaryBatch` already implements prepare-all →
one-record-one-sync → publish-all under the single writer; sharding only
changes which locks the prepare holds). Until phase 3, `Update` simply takes
the exclusive writer gate (§6.3) — semantically identical, no token juggling.

## 5. Durability pipeline

### 5.1 Journal strategy: ONE shared journal with a group-commit sequencer

**Decision: one journal per store, group-committed. Per-tablet journals are
rejected.** The arguments, from the measured numbers:

- **Cross-tablet atomicity dies with per-tablet journals.** The batch
  record's atomicity mechanism is one CRC over one sector-aligned append —
  torn tails self-truncate, no framing state can survive half a batch
  (verified, recovery-journal.md and `decodeRecoveryBatchRecord`). A batch
  spanning tablets split across journals needs a cross-file commit marker
  plus an ordering argument between files — exactly the framing complexity
  the single-record design exists to avoid. Recovery would need to merge
  streams by generation while proving no stream lost a middle record another
  stream's batch depends on.
- **The power-safe fence is device-global.** `F_FULLFSYNC` drains the whole
  device cache (4.05 ms measured); N journals issuing N concurrent full
  fences still serialize on the same drain. Parallel journal files buy
  literally nothing in the lane where the fence is 860× the apply.
- **Ordinary-sync parallelism is unproven and unnecessary.** Concurrent
  `fdatasync` on distinct files shares one device queue; whether it scales at
  all on APFS/M4 is unmeasured **(projection — a phase-0 microbench can bound
  it, but the chosen design does not depend on the answer)**. The shared
  journal's group commit already amortizes the 32.5 µs sync to ~4 µs/op at 8
  waiting writers, below the apply cost — the sync stops being the
  bottleneck, so multiplying sync streams optimizes a solved problem.
- **One stream keeps recovery exactly as it is** (§9): one `Replay` cursor,
  one base generation, one recycle discipline tied to the one durable root.
  Per-tablet journals would need per-tablet recycle floors against a single
  durable root — the folding discipline (recycle only after the durable fold,
  verified `checkpointBufferedLocked` ordering) becomes k-way.

This is the Badger model (writers stage records, one syncer fences batches)
— and Badger's 59.7k ordinary-sync ycsb-a on this exact harness is the
existence proof that group commit alone, without any storage-engine
parallelism, wins this lane.

### 5.2 The sequencer and its windows

The **journal sequencer** is a small state machine in front of the existing
`RecoveryJournal` (which stays single-threaded behind it):

- Writers deposit `(kind, key, value)` intents into a bounded lock-protected
  ring and wait on a sequence.
- A **leader** (the depositing writer that finds no sync in flight — flat
  combining, no dedicated goroutine) drains the ring: one `AppendBatch` with
  the group generation, one `Sync(journalPowerSafe)`, then wakes every waiter
  whose record the sync covered.
- **Windows are natural, not timed.** While a sync is in flight, arrivals
  queue; the next leader takes everything queued. The device's own sync
  latency IS the window: 32.5 µs windows at fdatasync class, 4 ms windows at
  power-safe — the slower the fence, the bigger the group, which is exactly
  the right self-tuning. This mirrors the committer's own measured lesson:
  "how many generations are queued when the worker picks one up" decides
  group size, not any configured limit (verified comment on
  `Options.GroupLimit`).
- Capacity: `Fits`/`FitsBatch` is checked at deposit; a full journal
  escalates to a checkpoint exactly as `ensurePrimaryBatchJournalRoom` does
  today, and the depositors re-enter after the recycle.
- **Failure: poison the group, die-don't-retry.** An append or sync error
  poisons via `poisonJournalLocked` (unchanged sticky semantics); every
  waiter in the group and every later depositor gets the persistence error.
  No waiter can be acknowledged out of a failed group.

### 5.3 What `CommitCoalesce` becomes

`CommitCoalesce` configured the *committer's* fence-sharing window — a knob
for the retired chain-fence sync path. On the primary graph nothing on the
mutation path waits on the committer fence (verified: sync-lane outer
generation stays zero, `waitPublished` is never armed on the canonical path),
so the knob is vestigial there. Disposition (formats and APIs are breakable
pre-release):

- The committer keeps `CoalesceDelay` internally for the checkpoint flush it
  still owns; the public `Options.CommitCoalesce` is **repurposed as the
  optional upper bound on a journal group window**: zero (default) =
  natural windows only; a positive value lets a leader linger up to that long
  after its sync completes if the ring is non-empty, trading acknowledged
  latency for group size. Expected useful only at power-safe, where the fence
  is so expensive that a bounded linger is nearly free
  **(projection: decided by the phase-1b power-safe lane sweep at 0/0.5/1 ms)**.

### 5.4 Lane orderings under grouping (unchanged invariants)

- **Buffered-visible (+journal), the competitive "ordinary-sync" lane:**
  apply + publish first (per its volatile-window contract), then deposit the
  redo intent and wait for the covering sync. The writer lock (later: tablet
  token) is NOT held across the wait — the append intent is deposited inside
  the lock (preserving journal order = publish order), the sync wait happens
  outside. A checkpoint racing between deposit and sync makes the record
  redundant, not lost: the recycle epoch bumps, and waiters whose generation
  ≤ the new durable root complete immediately (their durability came from the
  root; verified analogue: `journalAckLocked`'s full-journal fallback counts
  the ack against the chain lane today).
- **DurabilitySync:** stage everything fallible, deposit, **wait for the
  group sync, then publish**. Visibility strictly follows durability for
  every member: no reader can observe any group member before the whole
  group's single record is durable (the group inherits
  `journalBatchBeforePublishLocked`'s ordering, verified). The tablet token
  is held across the wait — per-tablet serialization during a sync is
  inherent to visibility-follows-durability, and cross-tablet writers
  proceed and join the same fence, which is the scaling model.

## 6. Publication protocol

### 6.1 The publish section is a flat-combining leader

Exactly one thread at a time executes the publish section, which is today's
code verbatim: `snapshotGate.Lock()` → `beginReaderFence()` → router
`UpdateLeaf` row stores → `installPrimaryExactResidentLocked` →
`pageValidator.update` → `publishFileState` → volatile retirements →
`endReaderFence()` → `snapshotGate.Unlock()`. Writers that finish staging
try-acquire the publish lock; the holder drains a bounded publish queue and
installs everyone's deposits as one group under ONE generation (the max birth
ticket, §4.2), building one `fileStoreState` per group instead of one per
mutation (an allocation-rate improvement over today). An uncontended writer
publishes its own deposit inline with no handoff, so the single-writer hot
path (412 ns in-place acks) keeps its exact instruction sequence
**(gate: single-writer regression ≤ 3 %)**.

Deposits are plain data: router row updates `(rank, ref, generation)`, doc
count delta, FileEnd high-water, volatile refs to retire, exact-index
contribution (§8.2), in-place patch closures (§6.2).

### 6.2 Who advances what

- **`visibleState`:** only the publish leader, under `snapshotGate` + fence.
  Monotonic by group generation. `Snapshot()` semantics are untouched: the
  gate still freezes lease acquisition, and one state root still selects the
  entire database view in O(1) (the hybrid spec's canonical-generation
  contract) — which is precisely why per-tablet visible roots are rejected
  (§12).
- **In-place patches** (the hot majority): the byte patch must happen inside
  the fence with the `safeFromReaders` veto (verified,
  `replaceBufferedPrimaryInplace`), so the deposit carries the patch
  arguments and the LEADER applies it via `ReplaceLeasedCanonicalDirty`. A
  veto (active reader could see the frame) falls back to the depositor's COW
  path, unchanged.
- **`durableState`:** only the checkpoint (committer `Flush` under the
  exclusive gate, §6.3). Never touched by per-mutation publication — already
  true today for both deferred-canonical lanes.
- **`primaryCheckpointBase`:** written only by materialize/flush-less
  publishers, which all run under the exclusive gate; its base-selection rule
  in `materializePrimaryParentsLocked` is unchanged. N writers never touch it
  (their publications move no real pages; only checkpoints and structurals
  do).

### 6.3 Checkpoints: one root publisher behind an exclusive gate

The writer population is guarded by a store-wide `writerGate`
(`sync.RWMutex`): tablet writers hold the read side for the duration of one
mutation; **checkpoint, structural transactions, `Update` (until phase 3),
snapshot-materialize, and Close take the write side** — a stop-the-world
writer drain. Checkpoint under the exclusive gate is today's
`checkpointBufferedLocked` with one addition: it materializes EVERY shard's
pending set in the one transaction (the pending-parent loops already batch by
shared page, so merging shard sets is concatenation before the existing
sort-by-bucket — verified, `materializePrimaryParentsLocked`). The order that
makes recycling safe is unchanged and load-bearing: materialize → `Flush` →
`MarkDurable` → `recycleRecoveryJournalLocked(durableGeneration)` — recycle
strictly after the durable fold, so a crash between fence and recycle replays
idempotently onto the newer root.

Escalation protocol: any writer hitting pressure (dirty budget, pending-set
budget, journal full) releases its token, takes the write side, re-checks the
pressure (another writer may have checkpointed first), runs the checkpoint,
downgrades, and retries its mutation — the same
ensure-then-reroute discipline `ensureBufferedPrimaryMutationCapacity`
implements today, lifted to the gate.

**Why a single root publisher and not a root-publication pipeline:** the
durable root advance costs one checkpoint (330–355 µs buffered) per 64+
mutations and is off every steady-state path; pipelining it buys back a cost
that is already amortized to ~5 µs/op, while a pipeline would need per-shard
durable cuts and a k-way `primaryCheckpointBase`. Rejected (§12).

## 7. Reader composition

- **Fence ownership:** the divert word nests (`divert.Add(1)`, verified), but
  the protocol requires the raiser to hold `snapshotGate`'s write side so
  diverted readers block instead of spinning — therefore fences serialize
  through the publish lock, and under N writers **only the publish leader and
  exclusive-gate holders ever raise a fence**. No new fence states, no
  concurrent fence raisers, and the documented store/load orderings on
  `ReadEpochs` hold verbatim because the writer side remains single-threaded
  at the moments that matter.
- **Reclaim floor:** `min(leases, epochs, oldestRecoveryGeneration)` is
  evaluated inside `AppendReusable`, which is called from
  `refreshReusableFor` — checkpoint/structural contexts only on the canonical
  lanes (per-mutation buffered staging opens no write transaction — verified,
  `cowBufferedPrimaryMutation`). Those contexts hold the exclusive gate, so
  the floor computation stays effectively single-threaded. N writers add no
  new reclaim sites.
- **Read path deltas: none.** Readers never learn that writers multiplied.
  The standing zero-read-path-change gate applies to every phase.

## 8. Structural transactions and posting installs

### 8.1 Structural work stays global

A split/merge/reclass takes the `writerGate` write side (drain all tablet
writers), then runs today's sequence unchanged: full checkpoint
(`flushPendingForStructural`), bounded structural transaction, post-publish
flush (the structural-transaction-is-a-checkpoint rule from the
double-retire fix — verified). Rationale: structurals are amortized rare (one
per full/empty leaf; capacity-relative gates landed), they are already
checkpoint-priced (two device flushes), and their correctness argument leans
on a quiesced writer population (`durableState` must not move under them).
The lock order (§10) makes the writer/structural interaction on the same
tablet trivial: a tablet writer holds `writerGate.R` + its token; the
structural holds `writerGate.W`, which excludes every token holder — there is
no same-tablet interleaving to reason about. Sharded structurals are a
deliberate non-goal until measurement shows structural stalls in p99 gates
(§12, §14).

The split-retry path composes: a writer whose stage hits
`ErrPrimaryLeafSplitRequired` releases its token, escalates to the gate,
runs the split, downgrades, retries — same shape as capacity escalation.

### 8.2 Exact-index resident install under N writers

Tiles partition by tablet (`TileID = BucketID<<2|q`, BucketID carries
TabletID), so per-tablet *contributions* are disjoint by construction. What
does not partition is the install: `preparePrimaryExactLeaf` snapshots the
whole live map minus one bucket, and two concurrent preparers would each miss
the other's tiles — a lost-update race — and the per-physical-index term leaf
(`encodeIndexTermLeaf` re-encodes the term set) is one shared object.
Decision, in two steps:

- **Phase 2 (unindexed parallelism):** indexed collections keep the exclusive
  gate for mutations — the honest analogue of the batch path's existing typed
  refusal. No stale postings are possible because nothing indexed runs
  concurrently.
- **Phase 4 (indexed parallelism):** deposits carry only the bucket's
  derived contribution (`bucketLive`, `byIndex` — the cheap tablet-local
  half, verified `deriveBucketExactContribution`); the publish LEADER merges
  contributions into the current snapshot and performs the single
  `installPrimaryExactResidentLocked` swap per group, rebasing on the live
  snapshot so no contribution is lost. The live map becomes per-tablet
  sub-maps keyed by TabletID (tile→tablet is derivable) so the leader's merge
  is per-tablet pointer swaps, not a full-map copy. The term-leaf re-encode —
  the only remaining O(global) per-group cost — is measured first; if it
  binds, term leaves shard per tablet with a k-way read-side merge, but that
  is a read-path change and needs its own gate **(projection: decided by a
  phase-4 microbench of `encodeIndexTermLeaf` at 8-writer mutation rates)**.

Structural posting repair (`structuralRepairPostingsHook`,
`prepareStructuralExactLocked`) runs under the exclusive gate and is
untouched.

## 9. Crash recovery

Nothing multi-stream, nothing new to fsck: **one journal, one sequenced
replay**, because the journal strategy (§5.1) preserved the single stream.

- Records are appended only by sequencer leaders in publish order; the batch
  record's generation is the group's published root generation, so generation
  monotonicity across the stream holds and `scanTail`'s torn-tail truncation
  argument is unchanged.
- Replay after root selection applies entries in order through the ordinary
  mutation path (`replayRecoveryJournalLocked` — single-threaded on open,
  stays single-threaded; replaying through N writers would buy startup
  parallelism at the price of replay determinism, rejected §12).
- Idempotence is inherited: a batch replays whole-or-none (one CRC), an
  absent-key delete is a no-op, replay onto a root newer than a record's
  generation is filtered by `Replay(rootGeneration)` — all verified today.
- New crash windows introduced by grouping, for the crash matrix:
  1. crash after group append, before sync — no member was acknowledged
     (buffered) / published (sync lane); replay may or may not see the
     record; either outcome is a correct un-acknowledged state.
  2. crash after sync, before publish (sync lane) — every member replays;
     the acknowledged-but-unpublished window test
     (`recoveryJournalPostSyncHook`) generalizes to the group.
  3. crash after publish, before the buffered group's ack sync — members are
     volatile exactly as today's buffered contract permits; no caller was
     acknowledged.
  4. checkpoint recycle racing a group in flight — the recycle epoch bump
     (§5.4) means waiters ack against the durable root; the crash sweep must
     cover kill-between-recycle-and-wake.

## 10. Canonical lock order

Total order; every path acquires only downward; no lock is held while
blocking on a later-numbered resource except where stated:

1. `writerGate` — read side per mutation; write side for checkpoint,
   structural, `Update` (until phase 3), Close.
2. Tablet write tokens, ascending TabletID (k of them for a batch).
3. Journal sequencer ring lock (deposit only; the sync wait happens after
   releasing it; sync-lane holders keep 1–2 across the wait by design, §5.4).
4. Publish lock = `snapshotGate.Lock()` + `beginReaderFence()`.

The buffered lane acquires 4 then later waits on 3's sequence with nothing
held; the sync lane waits on 3's sequence holding 1–2, then acquires 4.
Neither ever holds 4 while waiting on 3, so publish never blocks on a fence
and the reader-facing critical section stays sub-microsecond.

## 11. Phased implementation plan

Each phase is independently landable, gated, and leaves every earlier
guarantee intact (single-writer numbers, zero read-path deltas, crash sweeps).
Gates compare same-harness, same-client-count rows — competitor 8-client rows
are measured in phase 0, since RESULTS.md rows are single-client.

- **Phase 0 — concurrent harness lanes.** `mixedsuite`/`mixed` grow a
  `-clients=N` axis (1 and 8 first; 64 later); all five engines measured on
  both; repetition/isolation discipline unchanged. *Gates:* 10-rep medians
  with the existing cross-run drift bound; vibedb single-client rows within
  5 % of RESULTS.md (harness overhead audit).
- **Phase 1 — group-committed acknowledgements, existing single writer.**
  The journal sequencer (§5.2) behind `journalAckLocked`: append intent
  deposited under `c.writer`, sync shared by a leader outside it. No
  sharding, no batch-record requirement yet (individual records sharing one
  sync is sufficient and smallest). This attacks the ordinary-sync loss
  directly: apply stays ~4.7 µs serialized, sync amortizes 32.5/N µs.
  *Gates:* ordinary-sync ycsb-a ≥ **55 k ops/s at 8 clients** (stretch:
  ≥ 60 k, past Badger's single-client 59.7 k; the hard comparison is
  Badger's own 8-client row); single-client ordinary-sync ≥ 17 k (≤ 5 %
  regression); crash matrix extended with §9's windows 1/3/4, zero
  acknowledged loss.
- **Phase 1b — sync-lane group fence (the combiner front door).**
  DurabilitySync callers deposit whole mutation intents; a combiner drains:
  stage k, one batch record, one `Sync(powerSafe)`, publish k, wake k.
  Uses the batch-record framing and generalizes
  `journalBatchBeforePublishLocked` to independent entries. *Gates:*
  power-safe ycsb-a ≥ **1.5 k ops/s at 8 clients** (≥ 4× single-client 378;
  ceiling ~2 k at the 4.05 ms fence) and ≥ SQLite's 8-client row;
  visibility-follows-durability test: no reader observes any group member
  before the group record is durable; single-client power-safe not regressed.
- **Phase 2 — parallel staging (tablet tokens + grouped publish).**
  `writerGate` + tokens + shard frames (§4.2), publish leader (§6.1),
  checkpoint escalation (§6.3). Unindexed collections only; indexed and
  `Update` take the exclusive gate. *Gates:* buffered ycsb-a ≥ **300 k at 8
  clients**; churn ≥ **316 k at 8 clients** (take Badger's crown); ycsb-b
  ≥ 1.2 M at 8 clients; single-writer buffered within 3 % (ycsb-a ≥ 190 k);
  epoch stress test extended to N writers, race-detector clean; in-workload
  point-read p50 unchanged (0.33–0.38 µs); publish-section occupancy < 15 %
  at 8 writers (validates the serialized-publish bet).
- **Phase 3 — cross-tablet batches and structural composition under
  sharding.** Sorted token acquisition for `Update` (§4.3); structural
  escalation protocol (§8.1); ordinary-sync now also gains parallel apply.
  *Gates:* ordinary-sync ycsb-a ≥ **90 k at 8 clients** (sync amortized AND
  apply parallel); `Update` throughput at 8 clients ≥ 3× its single-client
  row; 56-point crash sweep + split-crash sweep re-run green with 8
  concurrent writers.
- **Phase 4 — indexed collections under parallelism.** Leader-merged posting
  contributions, per-tablet live sub-maps, measured term-leaf strategy
  (§8.2); indexed batches un-refused. *Gates:* indexed ycsb-a at 8 clients
  ≥ 3× indexed single-client; postings byte-identical to a from-scratch
  rebuild after a concurrent run (the existing equivalence gate, now under
  concurrency); no stale-posting reads in a paired mutate/query stress.

## 12. Rejected alternatives

- **Per-tablet journals with merge-by-generation recovery.** Rejected for
  cross-tablet batch atomicity (the one-CRC argument does not survive file
  splitting), device-global power-safe fences (parallel `F_FULLFSYNC` buys
  nothing), k-way recycle floors against one durable root, and because group
  commit on one stream already pushes the sync below the apply cost (§5.1).
- **Per-tablet writer goroutines behind a router dispatcher.** A mandatory
  goroutine hop on a 412 ns hot path, bounded queues that either allocate or
  add ring management, and no correctness benefit over caller-held tokens.
  Combining is only needed where sharing is (fence, publish) — so it lives
  there, flat-combining style, not in front of the tablet work.
- **PALM-style epoch publisher rewriting shared pages per epoch** (the
  previous draft's core). Overtaken by landed reality: the deferred-canonical
  lane already made per-mutation shared-page rewrites disappear (pending
  parents materialize once per checkpoint — verified), so the batched-descent
  publisher would batch work that no longer exists per mutation. What
  remains shared per mutation is pointer swaps, and §6.1 groups exactly
  those.
- **Per-tablet visible roots (sharded `visibleState`).** Breaks the O(1)
  whole-database snapshot invariant, `Snapshot`'s gate semantics, and the
  epoch entry's single pointer re-validation — a read-path rewrite with
  read-amplification consequences, against the standing zero-read-path-delta
  gate.
- **Root-publication pipeline / sharded checkpoints.** Adds k-way
  `primaryCheckpointBase` and per-shard durable cuts to save ~5 µs/op of
  already-amortized checkpoint cost (§6.3). Revisit only if checkpoint pauses
  fail a latency gate at 64 clients.
- **Sharded structural transactions.** Structurals are rare, checkpoint-priced
  and correctness-critical (double-retire history); stop-the-world is the
  simplest argument that stays true. Deferred until p99 evidence, not
  adopted preemptively.
- **Timed group-commit windows as the default.** Natural sync-in-flight
  windows self-tune with device latency; the committer's own measurements
  showed queue-at-pickup, not configured limits, governs group size. A
  bounded optional linger survives only for power-safe (§5.3).
- **Parallel journal replay on open.** Replay through the ordinary mutation
  path is the idempotence argument; parallelizing it would re-order
  application within a generation window for startup time nobody has asked
  for. Replay is bounded by journal capacity by construction.
- **Reseal-at-publish generation stamping** (patch the group generation into
  every prepared frame): an extra full-page CRC pass per mutation on the hot
  path; birth tickets + max-ticket group publication need none (§4.2).

## 13. Honest limits

- **Same-tablet writers serialize.** Per-key linearizability comes from the
  tablet token, so a workload hammering one tablet degrades to single-writer
  throughput plus group-committed acks — which is the correct floor, and
  phase 1 means that floor already beats today's number.
- **The sync lane's token-hold spans the fence.** At power-safe, a tablet is
  write-blocked for ~4 ms per group containing it. Unavoidable under
  visibility-follows-durability; the design's answer is that cross-tablet
  writers share that fence rather than queue behind it.
- **A uniform ref-changing flood** (bulk random inserts driving splits)
  degrades toward the exclusive-gate rate, since splits are stop-the-world.
  Bulk loading has its own builder path and is out of scope here.
- **The serialized publish section is a ceiling** — a deliberate one. If the
  phase-2 occupancy gate (< 15 % at 8 writers) fails, the fallback is
  batching depth (bigger publish groups), not sharding the section; the
  router seqlock and the epoch fence protocol both assume one publisher.

## 14. Open questions and the measurements that decide them

1. **PageCache concurrency envelope.** Readers already share the cache
   lock-free, but N writers concurrently calling `AdmitBufferedDirty` /
   `acquireFrameHinted` / `ReplaceLeasedCanonicalDirty` is unproven. Phase 2
   requires a cache-focused audit + stress before the tokens land; if
   admission needs a lock, it is per-frame, not global.
   *Decider:* race-detector stress + a phase-2 admission microbench.
2. **Publish-group state allocation.** One `fileStoreState` per group is
   better than today's one per mutation, but the zero-GC audit must confirm
   the deposit rings and shard frames stay allocation-free at steady state.
   *Decider:* `AllocsPerRun` gates extended to 8-writer loops.
3. **Term-leaf re-encode cost under load** (§8.2). Decides phase 4's shape
   (leader-merged single leaf vs per-tablet leaves with read-side merge).
   *Decider:* microbench of `encodeIndexTermLeaf` at 8-writer mutation rates.
4. **Does concurrent `fdatasync` scale on the target devices at all?** Not
   load-bearing for the chosen design; bounds how much the rejected
   per-tablet-journal alternative ever had to offer. *Decider:* optional
   phase-0 microbench (N files, N syncing goroutines).
5. **Competitor 8-client rows.** Badger's and SQLite's own concurrency
   scaling on this harness is unmeasured; every phase gate above that names a
   competitor binds against the phase-0 same-client-count row.
   *Decider:* phase 0.
6. **64-client behavior.** The token table, sequencer ring, and publish queue
   are sized for 8 first; 64-client lanes (the original harness ambition)
   may need wider rings and a second look at gate escalation fairness.
   *Decider:* phase 2/3 gates re-run at 64 clients, informational until then.
