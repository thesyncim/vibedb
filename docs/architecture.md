# Architecture

This is the front door to vibedb's storage design. The ordered-tablet primary
graph is the layout: every collection — freshly created, bulk-built, or
reopened — reads, mutates, checkpoints, and indexes against it. For public
APIs, see [store.md](store.md); for bytes on disk, see [format.md](format.md).

## The central rule: one bounded generation cut

Every published generation is semantically complete. A point read, ordered
scan, exact-index probe, and query begin from one immutable root and the bounded
immutable records that root selects. In deferred primary lanes, the selected
view is a canonical page graph plus a generation-stamped row overlay capped at
32,768 records; point reads select the newest applicable record and scans merge
the same records in key order, including deletes. An exact probe similarly
resolves its immutable spanned-leaf fold base plus bounded generation-stamped
posting and liveness records. Readers never consult the recovery journal or an
unbounded merge structure.

That rule expands into a family of invariants:

1. One root selects the document primary, indexes, allocator state, and catalog
   for a generation.
2. Hashes and compact certificates may reject candidates; exact keys and JSON
   values remain authoritative.
3. A successful delete removes the row and postings from the published logical
   view immediately. An eligible deferred lane may encode that absence as a
   bounded overlay delete record until the next foreground fold.
4. Snapshot-visible pages and overlay records are immutable. A writer either
   owns a frame exclusively, uses copy-on-write, or links a new immutable
   overlay record.
5. Every queue, cache, transaction, lease table, and retired-extent set is
   bounded at open.
6. Publication happens only after every query-visible structure for the new
   generation is complete.
7. One decision record in the database decision log (`txn.vtm`) selects the
   cross-collection cut for a multi-collection commit; recovery and read cuts
   never observe a torn participant subset.

The primary graph realizes this with a `StateRoot` selecting an ordered-tablet
catalog, its leaves, and an exact-index root whose per-index catalogs route to
ordered bounded term leaves.

## The ordered-tablet graph

The graph is:

```text
StateRoot
  ├─ TabletCatalog                 global lexical fences
  │    └─ TabletRoot               tablet-local identity and anchor routing
  │         └─ AnchorPage          stable leaf rows and current PageRefs
  │              └─ OrderedHashLeaf
  │                   ├─ exact keys and JSON values in lexical-rank order
  │                   └─ stable slots used by exact-term postings
  └─ PrimaryExactRoot              one record per physical exact index
       └─ ExactCatalog             level 0, or level 1 over level-0 pages
            └─ PrimaryExactLeaf    ordered term slices and giant-term stripes
                 └─ exact term → posting tiles
```

`StateRoot` selects the whole generation. `TabletCatalog` maps global lexical
fences to tablet identities and roots. A `TabletRoot` maps stable local leaf
identities to anchor rows and routes lexical fences to anchor pages. Each
anchor row holds the current reference for one ordered hash leaf. The leaf
uses bounded hash candidates for point lookup but stores records in lexical
order for lower bounds and scans.

The resulting identity is hierarchical:

```text
BucketID = (18-bit TabletID << 12) | 12-bit LocalLeafID
```

Exact-term postings name stable `(BucketID, slot)` locations, and the exact
secondary index that maps a JSON term to those postings is maintained in the
same publish as the document mutation that changes it. Ordinary mutations emit
absolute per-term and per-tile records; a slot-reassigning structural rewrite
emits a bounded rebase. At a fold, affected content-defined runs or giant-term
stripes are re-encoded through the deterministic cutter, while untouched term
leaves carry their durable page references forward. A complete key comparison
still decides every primary hit. The byte-level routing and leaf bounds are
specified in [format.md](format.md).

## Canonical frames and frame-native staging

The page cache is not merely a copy of durable bytes. In the deferred-canonical
lanes (buffered-visible and the journal-backed synchronous lane), an owned cache
frame can be the reader-visible base page for its stable reference. A qualifying
mutation may edit or replace complete frames; the unified primary lane may
instead link a bounded immutable row record over that base. Repeated changes
coalesce at a foreground checkpoint, which folds the visible cut into a new
canonical graph.

Two ownership questions must remain separate:

- **Reader-exclusive:** can a snapshot or sealed checkpoint still observe the
  old frame? If yes, the writer must COW it.
- **Durable-reachable:** can the last durable root still reach its physical
  extent? If yes, checkpointing must write a new extent before publishing the
  next durable root.

Frame-native staging means the committer refers directly to validated cache
frames rather than copying every page into a second per-commit buffer. The
queue owns bounded references and small publication records; the frames remain
pinned until the durability or checkpoint boundary releases them.

## Read path

A point read on the primary graph follows:

```text
read root
  → tablet catalog
  → tablet root
  → anchor page
  → ordered hash leaf
  → newest visible overlay record, if one exists
  → bounded hash candidates
  → exact key confirmation
```

The direct point read (`Collection.AppendRaw`) protects the read with one
epoch slot — no lock, no per-call generation lease. A reader announces the
generation it is about to read in a padded per-core slot; bounded publication
and retirement scans read those slots lock-free, and a retired extent is not
reused until no epoch or lease can still reach its generation. The read falls
back to the older
generation-lease path only when the epoch table declines the entry (full,
writer fence, or Close). A long-lived `Snapshot` still holds a generation lease
rather than an epoch slot.

An ordered scan follows the rooted tablet cursor for that snapshot and merges
the bounded overlay at the captured generation, suppressing rows whose newest
record is a delete. It never trusts a physical sibling pointer that a later COW
generation could make stale. Query execution takes an explicit heap or durable
snapshot and uses the same cut for indexes, scans, and document rechecks.

Collections may persist up to eight explicit scalar min/max summaries in each
primary stripe. A query with sound conjunctive scalar bounds compares canonical
ordered bytes and advances a rejected stripe before document reconstruction;
the full predicate still owns correctness for retained stripes. Summary space
is capped at 4 KiB per stripe and unsupported scalar shapes disable pruning
locally, so the optimization has bounded write/space cost and cannot create
false negatives.

## SQL and JSON documents

The relational boundary has one language: SQL. Embedded callers use
`database/sql`; PostgreSQL clients use the protocol-v3 `pgwire` front door.
Both adapters consume the same typed `sql/driver` catalog/session runtime for
DDL, DML, SELECT, prepared statements, and transactions. JSON is the row
representation, not a second request grammar. The `sql` package parses,
`query` lowers onto the shared compiled evaluator, and `sql/driver` supplies
catalog, storage, and transaction policy. This keeps SELECT and mutation
predicates on one implementation instead of duplicating them in an adapter.

Each SQL table is a durable collection plus catalog metadata for a declared
JSON schema, one scalar document-derived primary-key path, and exact index
definitions. Indexes can be added online and become visible with their durable
catalog entry in one publication. SQL-created tables use the ordered primary
graph; single writes maintain compound exact postings in the same publication,
and the driver batches through `Update` only where the collection supports it.
Catalog replacement and first table-file creation include the
platform namespace durability fence in addition to the durable file fence. The
catalog and collection are reopened together, so schema validation and index
maintenance do not disappear across process restarts.

A multi-table SELECT captures all participating durable collections while
holding their publication gates, then retains one generation lease per table.
The captured generations therefore genuinely coexisted. A declared-field inner
join that must emit matching pairs uses the heap executor after admitting the
complete captured input against the current fixed, conservative 64 MiB
working-set bound. The leases protect the cut while it is copied, then close;
the heap copy owns the same cut through result production. An oversized fallback
fails before execution. A join inside a transaction materializes its current
isolation cut plus the transaction overlay under the same bound.

The same global writer order that builds the coherent read cut also publishes
multi-collection commits. A SQL or native transaction may read and write
several tables: one dirty table still takes today's single-collection
`Update` path; two or more prepare conditional journal records and cross one
decision sync so every participant becomes visible together, or none do after
recovery. Autocommit point writes remain per-collection publications.

## Write path

A mutation is planned against one state:

1. Validate and copy the key and complete JSON value.
2. Resolve the existing row and affected exact-index tuples.
3. Construct either complete after-images for every touched primary leaf/page,
   or an eligible immutable primary overlay record, plus absolute exact-index
   term/tile records or a bounded structural rebase when stable slots move.
4. Recheck ownership and the source generation.
5. COW any snapshot-visible or durable-reachable bytes; otherwise patch an
   exclusively owned frame or link a bounded overlay record when every
   eligibility check passes.
6. Link the exact-index records or rebase for the rewritten bucket in the same
   publish.
7. Publish one new root after the selected acknowledgement boundary. On the
   journal-backed synchronous lane the redo record is appended and synced
   before that publish, so visibility follows durability.

`Update` batches distinct keys, rewrites each touched leaf once, descends each
directory once, and publishes one logical failure-atomic publication. It is the
single-collection transactional `WriteBatch` — a one-table SQL `COMMIT`, a
facade transaction that dirties one collection, and the group-commit primitive
all flow through it, and the logical batch is one journal record with one CRC
and one sync. A multi-collection commit stages the same per-collection batch
machinery, appends one conditional (kind-5) journal record per participant,
and treats the decision-log sync as the sole atomic commit point before
publishing every participant together. If a single-collection batch's final
rows do not fit the current leaf topology, a content-equivalent topology
generation may publish first and the logical batch publishes in the following
generation. A later failure exposes no subset of the batch, although
`Generation` may advance because the representation-only shape is retained.
Automatic combining applies the same machinery to overlapping `Put` and
`Delete` calls. The query-visible graph is complete at publication;
writer-only planning state is never published.

## Checkpoint path

Deferred-canonical mutations normally perform no root device write per mutation.
Buffered-visible does no device write at all on ordinary admission; the
journal-backed synchronous lane makes each acknowledgement durable with one
journal append plus sync and still defers its root. `Flush` captures the current
visible generation, materializes its reachable dirty/new primary frames and
newly encoded dirty exact leaves, carries clean exact leaves by `PageRef`, and
rebuilds the exact catalogs and exact root:

```text
seal visible root
  → fold the visible primary overlay into dirty/new leaves
  → walk dirty primary children and parents bottom-up
  → fold dirty exact runs/stripes; retain clean leaf refs
  → write complete COW page set plus fresh exact catalogs/root
  → data ordering barrier
  → write alternate inline root
  → final persistence barrier
  → advance durable root and release eligible staging
```

If bounded frame or retirement capacity would be exhausted, admission may
force this path early. A successful checkpoint always names one atomic
generation cut. Recovery chooses the previous complete root before the final
root boundary and the checkpointed root after it.

Automatically persisted modes use the same page/root protocol per generation
or commit group. See [durability.md](durability.md) for the exact boundary in
each mode.

## Snapshots and copy-on-write

Snapshot creation currently folds any pending primary overlay and unsealed
primary parents under the exclusive writer fence, then captures the resulting
physical file-store state, primary router, and bounded generation lease. This is
not a full-store copy or per-key scan, but creation can pay bounded foreground
work proportional to the dirty primary records being sealed. A resident
exact-index epoch may remain captured by the snapshot and is reconciled by its
exact-index reads. Later mutations may still share unchanged pages with the captured
cut; replaced pages and newly linked records leave the snapshot's view intact.

Retired extents remain unavailable for reuse until no snapshot can reach them.
A long-lived snapshot therefore consumes bounded retirement capacity and can
backpressure writers, but it does not make current readers traverse old
versions. Durable snapshots must be closed promptly.

## What the design deliberately excludes

vibedb is not an LSM tree. It excludes unbounded memtables, sorted-run levels,
background compaction, and offline maintenance. Its deferred primary and exact
index records are fixed-capacity, generation-stamped foreground structures;
pressure forces a synchronous fold or backpressures admission rather than
creating open-ended merge or space-amplification debt.

The trade is that reads perform an exact bounded reconciliation against the
captured generation until that foreground fold. Deletes disappear from the
logical view immediately but may remain as bounded delete records. Snapshots
currently seal pending primary records before pinning their logical cut; they
may retain a captured exact-index epoch, but do not retain a primary-overlay
cut. Isolated random writes can rewrite page paths and therefore pay bounded
foreground structural work. Writer batching, owned-frame in-place updates,
bounded overlays, and the recovery-only journal
remain predictable because their capacities and fold work are fixed at open.

## Design map

- [Hybrid mutations](design/hybrid-mutations.md): bounded foreground staging,
  batching, and checkpoints.
- [Canonical materialization](design/canonical-materialization.md): in-place
  frame updates and the gated async capsule path with recovery undo.
- [Recovery journal](design/recovery-journal.md): the recovery-only redo that
  now backs the synchronous lane.
- [Multi-table transactions](design/multi-table-transactions.md): conditional
  journal records, the `txn.vtm` decision log, and crash-atomic multi-collection
  commit.
- [Primary write concurrency](design/parallel-tablet-writers.md): the
  implemented 4,096 full-bucket stripe preparation lane and its coordinated
  fallbacks.
- [Distributed server boundary](design/distributed-sharding.md): the
  separate, server-only distributed tier. Its routed leader-only shard
  execution (`shardservice`) and stateless routing gateway (`gateway`) exist
  today, including an opt-in bounded row-batch transport primitive and
  owner-fenced retry-safe mailbox lifecycle commands that keep the routed
  one-frame lane unchanged but are not yet planner-orchestrated repartition;
  both are server-only and not part of the embedded API. They route on
  the frozen placement scalar (`distribution`), which also backs the opt-in
  single-shard `sql/driver` local-cluster facade and is therefore reachable from
  the embedded surface. It also documents that `autosplit` is a shadow-only
  unwired recommender and that serving replication, failover, and online
  movement are absent.
- [Distributed system target](design/distributed-system.md): the routed fast
  path plus distributed fallback, tenant-independent virtual buckets, global
  indexes, coherent snapshots, bounded exchange, serving replication, and
  online movement. Its bounded worker-mailbox state machine and shard-wire
  lifecycle commands exist, but direct producer routing, authenticated peer
  admission, and planner exchange orchestration remain unfinished. It is a delivery
  contract, not current capability.
- [SQL surface](design/sql-surface.md): the shared `database/sql` and `pgwire`
  contract over JSON documents, schemas, exact indexes, joins, and
  transactions.
