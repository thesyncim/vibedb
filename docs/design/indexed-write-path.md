# Indexed write path: O(delta) posting maintenance, spanned term leaves, online index add

**Status:** P0 (the generation-stamped posting overlay), P1 (spanned term
leaves), and P2 (online durable index creation) are implemented. P3 (indexed
batches and SQL-driver widening) remains. Historical measurements below are
retained as the baseline that motivated the work; unrun promotion gates remain
gates, not claimed results. The design covers the ordered primary graph's
exact secondary indexes only; the heap store's index machinery
(`store/store_index.go`) is source material, not a target.

**Idea:** stop paying for the whole index on every mutation. Keep the
canonical encoded term leaves as an immutable **fold base** that is rebuilt
only at checkpoints, and absorb per-mutation posting changes into a small
**generation-stamped overlay** (per-term posting records, per-tile live
records) that probes merge in O(that term's recent churn). The generation —
already the store's one MVCC axis — is what gives every Snapshot an exact
per-generation view without copying anything per mutation. Term leaves then
stop being one monolith per index: a deterministic content-defined split
spans an index (and a single term) across many bounded leaves, which removes
the 64 KiB fail-closed cap and makes the checkpoint fold O(dirty leaves).
P2's online index builder scans immutable primary leaves with optimistic
reconciliation and publishes only a complete Ready index. P3's indexed
batches will reuse the overlay's per-entry record shape.

---

## 1. The measured pre-P0/P1 problem (2026-07-29 baseline)

| metric | unindexed | indexed (1 exact index) | SQLite |
|---|---|---|---|
| churn, 10k corpus | 281,000 ops/s | **567 ops/s** | 38,921 ops/s |
| update p50 | 4.6 µs | **4,954 µs** | 10.5 → 19 µs |

Before P0/P1 this was a 69× loss to SQLite and a ~1000× regression against
our own unindexed path, with three distinct mechanisms verified against the
pre-P0 tree:

1. **O(index-size) mutation cost.** `preparePrimaryExactLeaf`
   (`store/durable/store_file_primary_exact_mutation.go:268`) runs on every
   indexed mutation and (a) copies the entire `primaryLive` map minus one
   bucket (`:281-299`, O(tiles) plus one heap allocation per tile), (b) per
   physical index calls `reconstructIndexTerms` (`:136` — decodes every term
   of the resident leaf into freshly allocated nested maps),
   `encodeIndexTermLeaf` (`:189` — sorts and re-encodes the entire term
   leaf, re-running `BuildTermPosting` per posting), and `OpenIndexTermLeaf`
   (full O(postings) re-admission of bytes we just produced). The file's own
   header comment names the price: "Cost is O(one leaf's live rows) …
   plus O(one physical index's postings) to re-encode each affected term
   leaf." At the 10k benchmark shape that is ~10,000 tile-postings decoded,
   re-encoded, and re-validated per Put.
2. **The former 64 KiB whole-index cap.** Exact-root version 0 mapped each physical
   index to exactly ONE `PagePrimaryExactLeaf`
   (`internal/storeio/primary_exact_index.go:96-125`), whose payload is one
   `IndexTermLeaf` bounded by `IndexTermLeafMaxBytes = 65535`
   (`index_term_leaf.go:23`) and by `MaxPageSize`
   (`primaryExactExtent`, fail-closed at
   `store_file_primary_exact.go:325-329` and the mutation-side staging at
   `store_file_primary_exact_mutation.go:580`). At 5.751 B/posting the cap
   lands near ~11k postings-per-KiB of corpus: the 10k benchmark encodes to
   ~57.5 KB — already at 90 % of the cap — and the 100k low-cardinality
   corpus fails closed at bulk build ("ordered-primary exact term leaf
   exceeds MaxPageSize") and on the mutation-path split ("common primary
   leaf class is full", surfaced when a split cannot stage its postings).
3. **The former lack of online add.** Before P2, durable indexes were frozen
   at Create:
   `Options.Indexes` compiles into `options.indexes` /
   `indexCatalogHash`, `createInitialState` stamps
   `IndexCount`/`IndexCatalogHash` into the state root
   (`store_file.go:2105`), and Open fails on any disagreement
   (`store_file_catalog.go:328-340`). P2 replaced that model with durable
   catalog authority and ready-only atomic `CreateIndex` publication over a
   live collection (§8).

## 2. Implemented inventory and remaining exclusions

1. **A mutation's index effect is confined to one bucket's four tiles.**
   `TileID = BucketID<<2 | quadrant`, 64 stable slots per quadrant, chunk 0
   only (`mask.Chunk != 0` is rejected on both the probe and reconstruct
   paths — `store_file_primary_exact.go:191`,
   `store_file_primary_exact_mutation.go:159`). Tiles partition by tablet
   because BucketID carries TabletID
   ([parallel-tablet-writers.md](parallel-tablet-writers.md) §8.2).
2. **Slot stability is the norm; reassignment is the exception.** An insert
   draws a fresh stable slot, a delete frees one, an in-place value change
   keeps its slot; only de-templating/reclass/split/merge rewrites can
   reassign a leaf's slots at once (header comment,
   `store_file_primary_exact_mutation.go:21-27`). Structural transactions
   already accumulate whole-bucket contributions separately
   (`accumulateStructuralLeafLocked`, `prepareStructuralExactLocked`).
3. **Prepare/install is split at the point of no return.** Every fallible
   posting step runs before `journalBeforePublishLocked`; the install is an
   infallible field swap under `snapshotGate` + reader fence
   (`store_file_primary_mutation.go:1235-1286`). The canonical-transaction
   lane stages exact pages durably inside the same per-mutation transaction
   (`:1585`); the deferred lanes fold at checkpoint
   (`stagePrimaryExactPagesLocked` from the checkpoint materialize,
   `store_file_primary_mutation.go:2281`) and at structural transactions
   (`store_file_primary_structural.go:667`).
4. **Snapshots pin one exact-index epoch by pointer.** The epoch contains
   the immutable spanned fold base, flat live table, and generation-stamped
   overlay. Probes are Snapshot-scoped and hold generation leases; epoch
   slots protect the direct point-read fast path. The reclaim floor is
   `min(leases, epochs, oldestRecoveryGeneration)` — the same floor this
   design reuses for overlay reclamation.
5. **The equivalence anchor:** incrementally folded spanned term leaves are
   byte-identical to a fresh `CreateFromPrimary` of the same final graph,
   including across cut boundaries (pinned by
   `TestFilePrimaryIndexedMutationMatchesRebuild`,
   `TestFilePrimaryIndexedCommitCrashBoundary`,
   `TestCreateFromPrimaryExactIndexDifferential`). This design keeps that
   anchor across packed leaves, giant-term stripe pieces, and online-added
   indexes.
6. **Batches refuse indexes.** `Update` fails with
   `ErrPrimaryBatchIndexedUnsupported`
   (`store_file_primary_batch.go:83`), and `SupportsUpdate` advertises the
   exclusion (`interfaces.go:21`), which is what keeps the SQL driver off
   indexed collections.
7. **Historical numbers to hold until the new gates are measured:** probe
   `BenchmarkPrimaryExactLookupIteration/lookup` = 1.22 µs, 0 allocs/op;
   space = 5.751 exact-B/posting (both measured on the 10k/100-term shape,
   `store_file_primary_exact_test.go:463`). Buffered checkpoint p50 is
   330–355 µs (parallel-writers doc §2). Zero-GC directive: steady-state
   mutation 0 allocs/op.

## 3. Architecture: fold base + generation-stamped overlay

The resident exact index becomes one immutable **index epoch** struct per
collection, swapped as a whole under `snapshotGate` only at fold points
(checkpoints, structural transactions, index catalog changes):

- **Fold base** (immutable between folds): per physical index, the encoded
  term leaves (the existing `IndexTermLeaf` codec, unchanged) plus a small
  sorted **leaf router** array mapping canonical-term ranges to
  leaves; and one flat open-addressed **live table** `tileID → uint64`
  replacing the `map[uint32]*[64]uint64` (the primary graph uses chunk 0
  only — **(verified)** — so one word per tile suffices resident;
  the durable `TermPosting` codec keeps its 64-chunk universe untouched).
- **Overlay** (append-only between folds, hash-keyed, generation-stamped):
  - **term records** `(indexID, term, tileID, gen, bits64)` — the tile's
    complete new posting bits for that term, absolute not delta;
  - **tile records** `(tileID, gen, live64, rebased flag)` — the tile's new
    live mask; `rebased` marks a slot-reassigning rewrite of the owning
    bucket.
  Records are immutable once linked; chains are newest-first with atomic
  head publication. All records for one mutation are stamped with its
  publishing generation and linked inside the same
  `snapshotGate` + reader-fence critical section that publishes the state,
  so "state at G visible" implies "every record ≤ G linked".

**Read rule (the whole consistency story).** A probe on Snapshot G resolves
term T per tile t as: newest term record for (T, t) with `gen ≤ G` if one
exists; otherwise the fold-base posting for (T, t) — unless a tile record
for t with `gen ≤ G` carries `rebased` at gen R and the base predates R (a
reassigned bucket voids its base postings; the rebase group wrote absolute
records for every term still present, so absence of a record ≥ R IS the
truth "no bits"). Term records older than a later rebase of their tile are
skipped by the same rule. Liveness recheck reads the newest tile record
≤ G, else the flat table. Every branch is a pure function of (fold base,
records ≤ G) — deterministic, and exactly generation G's postings.

**Write rule.** The common mutation path stops calling
`deriveBucketExactContribution` entirely:

- *update, indexed value unchanged* (the YCSB-dominant case): zero records.
  Slot is stable, term is identical — the index is untouched.
- *update, value changed:* per index, derive the old and new canonical
  terms (`appendPrimaryExactDocumentTerm` on the old and new raw values —
  the old value is already in hand from the pre-image leaf lookup the
  mutation performs for overflow accounting, **(verified)**
  `store_file_primary_mutation.go:1154-1183`), then emit ≤ 2 term records:
  old term's tile bits minus the slot, new term's tile bits plus the slot
  (writer resolves current bits with one probe-shaped read).
- *insert / delete:* one term record (bits ± slot) per index carrying a
  term, plus one tile record (live ± slot).
- *slot-reassigning leaf rewrite* (de-template, narrow→wide reclass —
  rare, once per leaf-class transition): fall back to
  `deriveBucketExactContribution` (O(leaf rows), as today) and emit a
  **rebase group**: 4 tile records with `rebased` + absolute term records
  for every term present in the bucket, all at one generation.
- *structural transactions* (split/merge/reclass): keep today's
  whole-bucket accumulation, but they are already checkpoint-shaped
  (`flushPendingForStructural`), so they fold first and rebuild the
  affected base leaves directly — the overlay never sees them.

Per-mutation cost: O(indexes × terms-per-document) canonicalizations plus a
handful of freelist records and atomic prepends — **(projection:
≤ 1 µs added over the unindexed 4.6 µs apply at one single-column index;
decided by the P0 gate)**.

**Fold.** At every point that advances `ExactIndexRoot`
(checkpoint, structural, canonical-lane per-mutation transaction), the
writer resolves base+overlay through the read rule at the fold generation,
re-encodes dirty term-leaf segments through the shared deterministic cutter,
carries untouched leaf pages by reference, stages changed pages and catalogs
in the same transaction (`stagePrimaryExactPagesLocked`), publishes a fresh
index epoch with an empty overlay, and
retires the consumed records/tables to a generation-keyed pending list
freed once the reclaim floor passes the fold generation. P0 moved the cost
out of each mutation; P1 bounds the fold to dirty leaves and giant-term
stripes.

**Zero-GC.** Records and interned term bytes come from a per-collection
arena with freelists; the overlay hash tables are sized for the churn
window and grow only by escalating to a checkpoint (the
`ensureBufferedPrimaryMutationCapacity` discipline, **(verified)** as the
standing pressure pattern) — never resized under readers. Probe-side
scratch (the seen-tile set for chain walks) lives in `IndexWorkspace`.
`AllocsPerRun` gates extend to the indexed mutation loop.

## 4. Design question 1 — the resident structure (chosen vs alternatives)

**Chosen: (c-refined) generation-stamped, term-keyed overlay over an
immutable fold base**, as specified in §3. Scored against the constraints:

- *Probe 1.22 µs / 0 alloc:* post-fold the base path is byte-identical to
  today's and the overlay check is one counter test plus one empty hash
  probe; the live recheck moves from a Go map to a flat open-addressed
  table, which is expected to buy back more than the overlay check costs
  **(projection: P0 gate holds the 1.22 µs bound; a new mid-window
  benchmark bounds the merged path)**.
- *Mutation O(delta) / 0 alloc:* records are O(touched terms); no map, no
  re-encode, no re-admission.
- *Snapshot pins exactly G:* generation filtering over immutable records;
  the epoch struct pointer capture replaces today's two-field capture —
  same gate discipline, one pointer.
- *Fold determinism:* resolution is a pure function of (base, records ≤
  fold gen); encoding reuses the canonical builder; the
  mutation-vs-rebuild identity gate stays byte-exact.
- *Memory:* records ≈ 40 B × (index-touching mutations per checkpoint
  window) × terms-touched; reclaimed at fold + floor. A held Snapshot
  delays reclamation exactly as it already delays extent reuse
  (`Options.MaxRetiredExtents` arithmetic, **(verified)** doc comment on
  `Snapshot`).

**Rejected (a): path-copied tile pages** (persistent radix over tileID,
COW tiles, readers pin a root). It fits the live map but inverts the probe:
equality is term-keyed, and the term side would need a persistent tree of
terms — abandoning `IndexTermLeafView`'s direct encodings, equality hash
table, and measured 1.22 µs/0-alloc probe for a node-walking structure with
per-term boxing. Per-mutation path copies also cost O(node fan-out) memory
traffic against a 4.6 µs total budget. It optimizes the structure we can
already rebuild lazily, at the price of the structure we cannot afford to
slow down.

**Rejected (b): mutable tiles + epoch-protected readers.** Epochs protect
brief entries against reclamation; they cannot give a long-lived Snapshot a
stable logical view of bits that mutate in place. Snapshots would need
version chains anyway — which is the chosen design — or copy-out at pin,
which taxes every Snapshot for the index's size. Riding the existing
divert/scan fence for in-place posting edits would also put multi-word tile
edits inside the reader-visible window with no seqlock, a torn-read class
the router only escapes by being single-word-per-row **(verified,
`resident_primary_router.go` seqlock notes in parallel-writers §1)**.

**Rejected (c-flat): a linear delta log consulted by probes.** Probe cost
O(window) — thousands of records between pressure-driven checkpoints —
against a 1.22 µs bound. Term-keying the log is exactly the chosen design.

## 5. Design question 3 — the live map

Before P0, `primaryLive` was rebuilt wholesale per mutation (full map copy
plus one allocation per tile, verified against the baseline tree). The
implemented replacement is
uniform with §3: the **flat fold-immutable live table** (open-addressed
`tileID → uint64`, rebuilt O(tiles) at fold — 2k tiles ≈ 16 KB at the 10k
shape, trivial against a 330 µs checkpoint) plus **per-tile overlay
records** for the ≤ 4 tiles a mutation touches. Readers get O(1) lookups
with at most one chain hop in the churn window; writers pay O(touched
tiles). The alternative — a persistent paged array with COW pages — costs a
page copy per mutation (KBs against a µs budget) and is rejected for the
same reason as 4(a); a plain mutable array is rejected because Snapshots
must see their generation's liveness (the recheck is a correctness fence
against stale postings, `store_file_primary_exact.go:190-195`).

## 6. Design question 2 — multi-leaf terms and the end of the cap

### 6.1 Sharding rule (deterministic, content-defined, order-preserving)

A physical index's sorted term sequence is cut into leaves by three pure
rules over content — never mutation history:

1. **Term-boundary cuts:** term T starts a new leaf iff the low 16 bits of
   `IndexTermRouteHash(storeID, T)` are below
   `IndexTermLeafCutThreshold = 1365`. That is approximately 1/48 and
   targets a mean 48 terms per run. Content-defined
   cuts make sharding local: adding or removing a term reshapes only its
   own run, never the whole file — the classic content-defined-chunking
   argument.
2. **Within-term stripe cuts** for giant terms: one term's postings are cut
   at fixed absolute tileID boundaries (every `R = 2048` tiles). A 64-slot
   tile's posting costs ≤ ~12 encoded bytes (sparse-masks: ≤ 2-byte varint
   + 8-byte mask; dense never wins for one chunk — **(verified)** codec
   selection in `term_posting.go:421-457`), so a stripe is ≤ ~24 KB plus
   leaf overhead — always under both `IndexTermLeafMaxBytes` and a 64 KiB
   `MaxPageSize`. Empty stripes emit nothing.
3. **Hard-cap cuts:** a run that would still exceed the byte bound (an
   adversarial run of huge terms that all miss rule 1) forces a cut at the
   last term that fits. A forced cut's position depends only on the run's
   own content and propagates only to the next rule-1 cut — still
   history-free, still local.

This replaced the fail-closed single-leaf `primaryExactExtent` limit: the builder
can always cut, so "exceeds MaxPageSize" becomes unreachable by
construction and is demoted to a corruption-class assertion.

### 6.2 Format

`PagePrimaryExactRoot` (version bumped) maps each physical index to a
**term-leaf catalog**: ordered entries `(firstTerm prefix, firstTileID,
PageRef)` in one power-of-two extent up to `MaxPageSize` (~1,300 leaves ≈
a 5 MB index), spilling to a two-level catalog tree above that (same shape
as the tablet catalog; depth ≤ 2 covers the multi-TB direction). Leaf
payloads remain exactly `AppendIndexTermLeaf` output — the leaf codec, its
direct encodings, and `TermPosting` are untouched.

### 6.3 Probe over a spanned term, zero-alloc

The resident leaf router is a sorted array captured with the index epoch.
Probe: binary search `(T, 0)` (≈ 8 short prefix compares at 140 leaves),
open that leaf's `IndexTermLeafView` (resident views are pre-admitted at
fold, so this is an array index, not a re-validation), `Lookup(T)`, stream
masks; while the next router entry continues the same term (stripe leaves),
advance and repeat. One nested iterator at a time, workspace-held cursor —
zero allocations, same shape as today's single-view iteration.

### 6.4 Bulk build

`buildPrimaryExactIndexes` keeps its derivation and then emits leaves
through the **same cutter** the fold uses (shared code path), so bulk
build, incremental fold, and journal replay produce byte-identical leaf
sets for identical final graphs — the identity gate survives sharding at
full strength, and the 100k corpus builds instead of failing closed.

### 6.5 Alternatives

- **Term → leaf-chain** (chain heads per term): per-term resident state and
  an extra indirection on every probe; ordering across terms lost, so
  `Range`/`Ordered` (used by the fold and future planners) needs a merge
  layer. The order-preserving cut subsumes it.
- **Index sharded by bucket/tile range globally:** a probe for one term
  must visit every shard (terms are scattered across all tile ranges) —
  probe O(shards), fails the 1.22 µs bound at scale. Adopted only *within*
  a term (stripes), where the probe wants every stripe anyway.
- **Tile-page tree keyed by tileID:** inverts the access pattern (equality
  is term-keyed); rejected with 4(a).

## 7. Fold, durability, crash identity

- **Checkpoint fold** (deferred lanes): unchanged call site
  (`store_file_primary_mutation.go:2281`); the input changes from "resident
  encoded bytes" to "resolve overlay onto base, re-encode **dirty leaves
  only** (P1), reuse untouched leaf pages by reference". Untouched leaves
  are *not* retired or rewritten — `stagePrimaryExactPagesLocked`'s
  current retire-everything loop becomes retire-dirty-only, shrinking
  checkpoint retirement pressure for large indexes.
- **Canonical per-mutation-transaction lane:** stages the fold inside each
  mutation's transaction as today (`:1585`). P0 leaves this lane's cost
  unchanged (whole-leaf re-encode, exactly today's behavior); P1 makes it
  O(dirty leaves ≈ 1–3 small encodes) — that lane already pays a device
  sync per mutation, so a few-µs encode is noise.
- **Journal replay identity:** replay drives the same mutation path →
  same records → same fold resolution → same cutter → identical pages.
  `TestFilePrimaryIndexedMutationMatchesRebuild`,
  `TestFilePrimaryIndexedCommitCrashBoundary`, and
  `TestCreateFromPrimaryExactIndexDifferential` stay green and are
  strengthened: the differential gains spanned-leaf and mid-window-crash
  cases, and a new test crashes between a rebase group and its fold.
- **Determinism:** `-count=2` byte-identical checkpoint files holds because
  fold output is a pure function of (final postings, deterministic cuts)
  and page allocation stays transaction-ordered as today.
- **Same-publish atomicity:** unchanged mechanism — records link and the
  state publishes inside one gate + fence section; the durable pages and
  `ExactIndexRoot` advance inside one transaction with the graph. A reader
  can never see a document without its posting or a posting without its
  document, in memory or after recovery.

## 8. Design question 4 — online CreateIndex

**Implemented catalog authority.** The frozen-at-Create model is inverted:
the durable canonical catalog (rehydrated at Open,
`store_file_catalog.go`) is the authority on index definitions;
`Options.Indexes` remains a Create input and optional Open assertion.
`CreateIndex(def)` compiles a prospective canonical catalog and publishes
its catalog identity, exact root, and ready posting leaf in one structural
transaction.

**Zero mutation-path tax.** The builder does not declare a Building index,
install a mutation log, or add a build-state branch to Put/Update/Delete.
Instead it scans one immutable primary leaf per bounded writer hold and
retains `(leaf ref, live masks, term contribution)`. A changed leaf has a
different immutable ref and only that contribution is rescanned. The
forward cursor makes the initial pass O(leaves), and the final full ref
vector is checked outside the writer against the resident router's
generation. Publication reacquires the writer and rechecks router identity
and generation; a concurrent mutation simply causes another bounded
reconciliation pass.

**Memory and disk shape.** Per-leaf term keys live in compact byte arenas.
The final encoder k-way merges sorted leaf contributions directly into the
canonical codec input rather than constructing a second whole-index map; one
flat posting arena and the ordered-primary chunk-0 codec path avoid per-term
payload allocations. Cutover installs a fresh empty-overlay epoch for the
O(delta) mutation engine and advances the resident router in place, avoiding
a second primary-graph walk.
Publication persistently reuses every existing immutable index leaf,
allocates only the new physical leaf and one new exact root, and treats a
second name for an identical path vector as a catalog-only alias. There is
no database rewrite or shadow file.

**Reads during the build:** existing snapshots and indexes remain fully
usable. The new name is absent until the ready index and catalog publish
together; a snapshot taken before the cutover retains the old catalog,
postings, and live masks, while a later snapshot sees all three new values.

**Crash during the build:** before publication there is no durable build
state to recover. The publication is ordinary copy-on-write: recovery
selects either the old root/catalog (no index) or the new root/catalog
(complete Ready index). Fault injection covers every data write, barrier,
root write, final sync, and torn-root boundary.

**Alternative rejected — freeze-and-rebuild** (build a new store with the
added index, swap files): O(store) unavailability-free but O(store) work
and double space per index add, and it forecloses the SQL layer's
`CREATE INDEX` on live multi-GB collections — the direction doc's whole
point. The in-file builder scans primary leaves once and never copies document
pages or existing on-disk index leaves; persistent growth is proportional to
the new index rather than to the whole database.

**Alternative rejected — long-lived snapshot backfill:** even a one-leaf
snapshot can pin retired volatile references during a burst and make writes
return capacity pressure. One bounded leaf visit under the writer prevents
that failure mode; the expensive final vector validation runs lock-free and
uses the generation recheck for stability.

## 9. Design question 5 — batches and the SQL driver

A `WriteBatch` entry's posting work is the same ≤ handful of records as a
single Put. The batch path (`updatePrimaryBatch`: prepare-all →
one-record-one-sync → publish-all, **(verified)**) gains: per-entry record
preparation into a batch-scoped list during prepare (fallible, before the
journal fence), then one publish links every record at the batch's single
generation inside the existing one-gate section. All-or-nothing follows
from the existing shape — nothing is linked or visible until the whole
batch has staged and fenced; the journal batch record already replays
whole-or-none (one CRC). `ErrPrimaryBatchIndexedUnsupported` is deleted;
`SupportsUpdate` drops the `len(c.options.indexes) == 0` clause, which
turns the SQL driver's update path on for indexed collections. A future
resumable Building-state variant would dual-write batch changes like
single mutations. This also
completes what parallel-tablet-writers §8.2 needs: deposits carry records
(tablet-local by tile partition), and the publish leader links them —
O(records) pointer work, no O(map) merge, so this design *removes* the
open question flagged there (§14.3) rather than adding to it.

## 10. Design question 6 — phases and promotion gates

Every phase is independently landable and keeps every earlier gate green.
"10k/100k corpus" = the measured shapes from §1; churn/update rows are the
mixed-workload harness lanes measured 2026-07-29.

### P0 — implemented: kill the O(N) re-encode (the 69×)

**Implemented:** overlay records + index-epoch capture + flat live table +
O(touched-terms) mutation path + rebase groups + checkpoint/structural fold
from overlay. **No durable format change** (single leaf per index kept;
canonical lane unchanged). `preparePrimaryExactLeaf`'s
reconstruct/encode/re-admit per mutation is deleted.

**Promotion gates (results must be recorded before claiming pass):**
- indexed churn, 10k corpus ≥ **100,000 ops/s** (baseline 567; SQLite 38.9k);
  indexed update p50 ≤ **15 µs** (baseline 4,954 µs).
- `BenchmarkPrimaryExactLookupIteration/lookup` ≤ **1.22 µs, 0 allocs/op**
  (post-fold path); new mid-window probe benchmark (probe with a warm
  overlay of 64 mutations) ≤ **1.5 µs, 0 allocs/op**.
- space: format untouched — **5.751 exact-B/posting exactly**.
- steady-state indexed mutation **0 allocs/op** (`AllocsPerRun` gate).
- identity/crash: the three §7 tests green, plus new de-template/reclass
  rebase test and crash-between-rebase-and-fold test; `-count=2`
  byte-identical checkpoints.
- indexed buffered checkpoint p50 ≤ **5 ms** at 10k (fold is O(index) until
  P1; bound states the accepted cost).

**Risk:** rebase-detection misses a slot-reassigning rewrite class →
caught by the identity tests, which compare against a from-scratch rebuild
byte-for-byte; overlay memory under a pinned Snapshot → bounded by the
existing floor discipline, stress-tested.

### P1 — implemented: spanned term leaves (the cap)

**Implemented:** content-defined term cuts + tile stripes + hard-cap cuts;
versioned exact root with per-index leaf catalogs (tree ≤ 2 levels); leaf
router in the index epoch; fold and canonical lane re-encode dirty leaves
only; bulk build shares the cutter; fail-closed check demoted to assertion.

**Promotion gates (results must be recorded before claiming pass):**
- **100k low-cardinality corpus builds, probes, and churns** (the pre-P1
  baseline failed closed at build).
- probe at 100k ≤ **2 µs, 0 allocs/op**; 10k probe still ≤ 1.22 µs.
- indexed churn ≥ **100k ops/s at both 10k and 100k** corpora (index cost
  is corpus-size-independent by construction).
- space ≤ **6.05 exact-B/posting** at the 10k gate shape (+5 % budget for
  per-leaf headers/restarts/lost cross-leaf dictionaries and catalog
  pages); 100k baseline recorded and pinned as the new gate.
- indexed checkpoint p50 back ≤ **1 ms** at 10k (dirty-leaf fold);
  canonical-lane indexed mutation no longer O(index).
- determinism: `-count=2`; differential test extended to spanned leaves;
  bulk-vs-mutation identity holds across cut boundaries.

**Residual measurement risk:** the fixed cut parameter may trade space
against fold latency; the benchmark sweep decides whether it stays.
Adversarial term-size distributions are bounded by the hard-cap cut and
locality tests.

### P2 — implemented: online CreateIndex

**Landed for the current exact-leaf format:** dynamic durable catalog
authority, O(leaves) optimistic reconciliation, bounded writer holds,
lock-free final validation, ready-only atomic cutover, immutable-leaf reuse,
alias deduplication, SQL catalog repair, and `CreateIndex` /
`CreateIndexContext` on the durable collection.

**Measured gates:**
- 8k documents build at a five-sample median of **~1.10M docs/s** for both
  20-value and 1,000-value corpora on Apple M4 Max, including atomic file
  publication;
- complete builds use about **606 allocations** (20 values) and **1,031
  allocations** (1,000 values), or 0.076 and 0.129 allocations/document;
- file growth is **5.12 B/document** and **9.22 B/document** respectively,
  including the new exact root and canonical catalog page;
- concurrent rewrites reconcile without a mutation hook or retired-capacity
  failure;
- online output is byte-identical to canonical map aggregation;
- crash at every publication boundary (data writes, barrier, root write,
  final sync, torn root) reopens as exactly old-or-new;
- old snapshots retain the old catalog while new snapshots see the Ready
  index.

**Remaining measurement gate:** rerun the online-add scale lanes against P1's
spanned exact leaves, including the 100k corpus. The implementation is current;
no 100k P2 result is claimed here.

**Risk:** sustained write churn can delay convergence, but cannot expose a
partial index or stop writers; bounded leaf holds, cancellation, paired
rewrite stress, canonical differential checks, and the final generation
proof cover the interleaving.

### P3 — indexed batches and the SQL driver

**Lands:** batch-scoped record staging, single-publish linking, refusal
deleted, and `SupportsUpdate` widened. A future resumable build-state
variant would make batches legal during that state as well.

**Gates:**
- 64-entry indexed batch amortized cost ≤ **2×** the unindexed batch
  (records are cheap; the bound is honest headroom).
- batch crash boundary: all-or-nothing including postings, verified by the
  exhaustive batch crash sweep extended with an indexed lane.
- SQL-driver update on an indexed collection passes the driver conformance
  suite; posting identity after a replayed batch run.

**Risk:** journal/record budget interplay on giant batches → the existing
`FitsBatch` escalation covers it; nothing new is invented.

## 11. Rejected alternatives (cross-cutting)

- **Path-copied tile pages as the primary structure** — §4(a).
- **Mutable tiles under epoch protection** — §4(b).
- **Flat delta log scanned per probe** — §4(c-flat).
- **Per-term leaf chains / global tile-range sharding / tileID trees** —
  §6.5.
- **Size-driven greedy sharding from the leaf head** (split on overflow
  like a B-tree): boundaries become history-dependent, which forfeits the
  bulk-vs-mutation byte-identity anchor — the one gate this codebase treats
  as the index's correctness proof. Content-defined cuts keep it.
- **Fold on every mutation for the deferred lanes** (just make the current
  code incremental leaf-by-leaf without an overlay): still O(dirty leaf)
  encode + page-cache traffic per mutation (~hundreds of µs at the 10k
  shape), misses the zero-record fast path for non-index-touching updates,
  and leaves Snapshot capture O(leaves). The overlay costs one mechanism
  and serves P0–P3 and parallel-writers phase 4 alike.
- **Freeze-and-rebuild index add; unlocked backfill-with-reconcile** — §8.

## 12. Honest limits

- **Mid-window probes pay for churn.** A term hammered between checkpoints
  grows its chain; the probe walks it. Bounded by the overlay-pressure
  escalation, and the mid-window gate pins the worst case we accept.
- **Giant-term probes stream every matching piece.** Routing is one binary
  search, but a low-cardinality term spanning many stripe pieces necessarily
  visits them all and returns all of their postings.
- **The exact catalog is bounded to two levels.** This covers the intended
  scale and fails closed if that explicit format bound is exceeded.
- **Rebase groups are O(leaf)** on slot-reassigning rewrites — inherent:
  the slot mapping changed for every row. Rare by construction (class
  transitions), and no worse than today's every-mutation cost.
- **A pinned Snapshot pins overlay memory** exactly as it pins retired
  extents; same documented remedy (short-lived snapshots).
- **`fileFormatMaxExactIndexes = 64` stands.** Nothing here needs more,
  and the exact root page budget assumes it.

## INVARIANTS

1. **Same-publish consistency.** A mutation's document change and its
   posting records become visible in one `snapshotGate` + reader-fence
   section; durable pages and `ExactIndexRoot` advance in one transaction
   with the graph. No reader — lease, epoch, or post-recovery — can
   observe one without the other.
2. **Exact per-generation views.** A Snapshot pinned at generation G
   resolves postings as (fold base + records with gen ≤ G) and sees
   exactly G's postings, indefinitely, regardless of later mutations,
   folds, or index adds.
3. **Record immutability and monotonic heads.** Overlay records never
   mutate after linking; chain heads only prepend; every record's
   generation ≤ the head's; records ≤ G are all linked before state G
   publishes.
4. **Fold purity.** Durable exact pages are a pure function of the final
   graph's postings and the deterministic cut rules — independent of
   mutation history, fold timing, replay, or build path. Bulk build,
   incremental maintenance, and journal replay of the same final graph
   produce byte-identical exact pages.
5. **Posting-slot agreement.** A row's posting bit is derived from the
   same (bucket, slot) the read path selects it by
   (`VisitPrimaryLeafPostingRows` on both sides); every posting bit is a
   subset of its tile's live mask at the same generation, and the probe's
   liveness recheck fails closed.
6. **Reclamation floor.** No overlay record, table, or fold base reachable
   by any lease or epoch at or below its generation is reclaimed; the
   floor is `min(leases, epochs, oldestRecoveryGeneration)` — the same
   floor as extents.
7. **Zero-GC steady state.** The indexed mutation and probe hot paths
   allocate nothing per operation; arenas/freelists grow only at
   fold/pressure boundaries under the exclusive writer.
8. **Crash/replay identity.** After any crash, journal replay plus the
   next fold yields exact pages byte-identical to an uninterrupted run of
   the same acknowledged operations.
9. **Cap-free by construction.** Every emitted term leaf fits
   `min(IndexTermLeafMaxBytes, MaxPageSize)` via the cut rules; no posting
   state can make a build or mutation fail closed on size.
10. **Reads never block on index maintenance.** Online build, fold, and
    catalog changes never suspend point reads, scans, or probes. The online
    name remains absent until the complete Ready index and catalog publish
    atomically.
11. **Concurrency headroom.** Posting contributions remain tablet-local
    (tile partition by TabletID) and install remains a serialized
    pointer/link section — the stage→fence→publish decomposition of the
    parallel-tablet-writers design is preserved, not foreclosed.

## MIGRATION

No backward compatibility is owed (pre-release; the standing
no-migration rule applies). On-disk changes, by phase:

- **P0:** none. Resident-only change; existing stores open unchanged.
- **P1 (current):** `PagePrimaryExactRoot` version 0 → 1: entries become per-index
  term-leaf catalogs `(firstTerm prefix, firstTileID, PageRef)` (single
  extent, spilling to a two-level tree), replacing the one-leaf-per-index
  table. `PagePrimaryExactLeaf` payloads are unchanged
  (`AppendIndexTermLeaf` output); `TermPosting` and `IndexTermLeaf` codecs
  are unchanged. Stores created before P1 are recreated, not migrated.
- **P2 (current):** the canonical catalog becomes the index-definition
  authority; `Options.Indexes` remains a Create input and optional Open
  assertion. A ready-only online cutover advances `IndexCount`,
  `IndexCatalogHash`, the catalog, and the v1 exact root atomically. There is
  no durable Building state or backfill cursor. Recreate, no migration.
- **P3:** no format change (journal batch records already carry what
  replay needs).
