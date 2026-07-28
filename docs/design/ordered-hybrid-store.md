# Ordered hybrid store

**Status:** promotion specification; candidate primitives are measured, the
complete primary is not the default.

**Idea:** route lexically to a tablet and leaf, use bounded hashes inside that
leaf, and keep exact key/value bytes in lexical order. Readers retain one
canonical representation; mutations publish complete generations.

**Decision rule:** every number below is measured, projected, or a gate. No
isolated primitive becomes the default until the complete store passes
[the promotion gates](#promotion-gates).

**Measured evidence:** in the [cited M4 Max runs](#ordered-hash-leaf), the
isolated ordered leaf measures 30.0–31.1 ns hits, 49.3–50.9 ns misses, and
5.14–5.17 ns/document lexical iteration; the combined resident route measures
185.8 ns with hashing included. These omit publication, snapshot COW,
secondary maintenance, and I/O. The complete store is therefore still judged
by the end-to-end gates, not these primitive results.

The target is not an LSM. Readers consult one immutable published generation,
with no memtable, delta, tombstone, version-chain, or merge cursor. Mutations
construct the next canonical pages directly and publish one root. Snapshots pin
old immutable roots; qualified in-place materialization is an optimization, and
copy-on-write is always the safe fallback.

## Why the primary cannot be globally hash-partitioned

Arbitrary hash partitioning and global lexical ranges cannot coexist without a
second ordered copy of every key or a sort at query time. Either choice violates
the read-amplification and space goals.

The winning interpretation of hash routing is therefore:

1. route by shortest distinguishing key prefixes to a lexical tablet and leaf;
2. use bounded hash candidates inside that leaf;
3. confirm every candidate against the complete key;
4. store key/value bytes physically in lexical-rank order.

This follows the useful division in Wormhole-style indexes: prefix hashing
keeps point routing independent of total key count, while ordered leaves retain
exact lower bounds and range scans. Swiss-table control groups are useful
inside a leaf, not as the global ordering structure.

## Canonical generation

```text
StateRoot
  |
  +-- TabletCatalog
  |     global lexical fences -> TabletID -> TabletRoot
  |
  +-- TabletRoot
  |     LocalLeafID -> stable (anchor page, row slot)
  |     lexical anchor-page fences -> current AnchorPageRef
  |
  +-- AnchorPage
  |     lexical leaf fences + stable row slots
  |     -> (BucketID, current PageRef, zone)
  |
  +-- OrderedHashLeaf
  |     stable hash slots -> lexical ranks -> exact keys/values
  |
  +-- ExactTermIndex
        exact term -> adaptive (BucketID, quadrant, mask) posting tiles
```

`BucketID` is hierarchical:

```text
BucketID = (18-bit TabletID << 12) | 12-bit LocalLeafID
```

The 30-bit namespace still addresses 1,073,741,824 leaves. A tablet's exact
8 KiB dense locator resolves one stable local ID to an anchor-page ID and row
slot; the anchor row stores the current compact leaf reference once. Primary
point reads route lexically to that row, while posting-driven reads decode
TabletID and LocalLeafID and resolve the same row. This unifies the ordered and
identity routes without a global `BucketMap`, its extra page walk, or a second
copy of every leaf mapping.

One state root selects the tablet catalog, tablet roots, anchor pages, primary
leaves, and exact-index roots. A snapshot therefore chooses the entire database
view in O(1), and all readers see either the old generation or the new
generation.

## Non-negotiable invariants

1. Readers consult exactly one published primary representation.
2. Point reads never consult a range cursor; scans never merge representations.
3. Global iteration, lower bounds, upper bounds, and prefix scans use bytewise
   lexical key order.
4. Hash tags only remove candidates. A hit always compares the complete key in
   the selected leaf.
5. `(BucketID, slot)` is stable across ordinary update and delete and is the
   secondary-posting identity.
6. Slot movement happens only during one bounded split, merge, or slot-class
   rewrite that atomically rebuilds all affected posting tiles.
7. Delete leaves no tombstone, probe-chain obligation, or later compaction work.
8. An active snapshot forces copy-on-write for any bytes it can still observe.
9. Every persistent router, leaf, posting, and allocator structure is
   pointer-free, checksummed, self-describing, and capacity-bounded.
10. False tag matches never scan another leaf, tablet, or table.

## Ordered hash leaf

The ordered-hash leaf candidate now combines the point and lexical paths:

- 256 stable slots for the normal class;
- one control byte per slot: keyed tag plus empty/live state;
- one byte per slot mapping slot to lexical rank, with `0xff` empty;
- compact common key lengths with a rare wide escape;
- restart-interval key-prefix truncation was measured and **rejected** for
  this leaf (`primary_leaf_prefix_lab.go`, M4 Max): it saves 42–54% of key
  bytes on dense sequential keys, 13–17% on UUIDv7, and loses on random
  keys — but document keys are a small fraction of the record, so the
  whole-file saving is roughly 1–3% while hits slow ~10 ns, iteration rises
  from 4.9 to 6.4–6.7 ns/doc (through the ≤6 ns gate), and lower bounds pay
  2–4.5x for the restart decode. Keys stay whole in the primary leaf; the
  exact-term index leaf, where key bytes dominate, already restart-compresses
  its terms;
- one overflow bit per live rank;
- succinct monotone record boundaries;
- key/value heap in lexical-rank order;
- small bounded insertion stash; no tombstones;
- adaptive 4–64 KiB page classes.

Measured on an M4 Max at commit `a11794f`:

| Isolated leaf operation | Result | Allocation |
| --- | ---: | ---: |
| keyed hash, 8-byte key | 12.1 ns | 0 |
| hit, hash included | 30.0–31.1 ns | 0 |
| miss, hash included | 49.3–50.9 ns | 0 |
| stable-slot exact hint | 13.4 ns | 0 |
| three false-tag exact confirmations | 60.3–60.7 ns | 0 |
| lexical iteration | 5.14–5.17 ns/document | 0 |

The measured structural envelope includes controls, slot-to-rank bytes, packed
key lengths, overflow state, Elias-Fano record boundaries and checkpoints, the
40-byte header, and the 16-byte segmented checksum trailer:

| Live rows | Occupancy | Structural B/live key | 8-byte key + 8-byte value physical B/live key |
| ---: | ---: | ---: | ---: |
| 218 | 85.2% | 4.954 | 37.58 |
| 225 | 87.9% | 4.876 | 36.41 |
| 230 | 89.8% | 4.996 | 35.62 |
| 244 | 95.3% | 4.848 | 33.57 |

The structural target is met in the intended occupancy band, but the final
column exposes a remaining discontinuity: these small records need roughly
4.8 KiB and therefore occupy an 8 KiB extent under the portable 4 KiB
allocation quantum. The whole-file space target is not met by calling that
padding "free."

The adaptive-capacity experiment finds a better aligned narrow class for the
common 8-byte key plus 8-byte value shape:

| Leaf class | Live rows | Structural B/key | Slack B/key | Physical B/key |
| --- | ---: | ---: | ---: | ---: |
| narrow: 217 slots, 192 normal + 25 stash | 195 | 4.887 | **0.118** | **21.005** |
| fixed 256-slot candidate | 218 | 4.954 | 16.62 | 37.58 |
| byte-packed cold 256-slot leaf | 218 | 4.954 | 0.122 | 21.08 |

The narrow class fits its exact 4,073-byte image in one aligned 4 KiB extent.
In the equivalent isolated loop its average hit is about 52.2 ns versus
52.4 ns for the fixed candidate, its miss is 14.3 ns versus 16.3 ns, and its
lexical scan is about 3.1 ns/document. Exact-key delete and restore reclaims the
same stable slot with no posting change.

Random replacement with unrelated new keys eventually fills the narrow stable
stash, so this is not yet a universal default. The safe upward reclass keeps
the same 192 normal slots and expands only the stash from slots 192–216 to
192–255 in an 8 KiB image. Every existing slot and posting bit survives; the
writer changes one canonical leaf image and its unified anchor handle. One
million simulated random replacements at 195 live rows did not exhaust the
wide class. Its current high-stash hit path is still too slow
(roughly 96–127 ns), so the wide codec needs a bounded hashed stash lookup
before promotion.

Byte-packed immutable runs remain a space-tier experiment for shapes that do
not fit an aligned class well. They use one direct 18-byte hot/cold handle:
48-bit eight-byte-granular offset, 48-bit generation, exact 16-bit length, and
four zone bytes. Readers still select one authoritative leaf, but cold random
misses can cross more device windows: the measured 4,568-byte leaf reads
1.052x the aligned 8 KiB window on average, and an arbitrary packed 4 KiB class
reads 2x. Packed runs therefore are not the default until device-backed random
misses pass the no-regression gate. Overflow references, allocator state,
roots, and value bytes also remain visible in the whole-file benchmark.

Same-length COW update cost scales with admitted extent size:

| Extent | COW | Owned, no observable snapshot |
| ---: | ---: | ---: |
| 4 KiB | 0.458 µs | 0.395 µs |
| 8 KiB | 0.886 µs | 0.780 µs |
| 16 KiB | 1.802 µs | 1.620 µs |
| 32 KiB | 3.608 µs | 3.001 µs |
| 64 KiB | 7.497 µs | 6.214 µs |

Owned same-length replacement copies only the value bytes, but the current
portable checksum scheme still hashes the intersected half-extent. Insert and
delete remain bounded O(leaf payload), and an owned growth that crosses an
extent boundary fails without mutation so the writer can use the canonical COW
resize path.

At 100 billion documents and 187–230 rows per leaf, the store has roughly
435–535 million primary leaves. With tablets cycling between roughly 2,048 and
4,096 leaves, an average of 3,072 produces about 142,000–174,000 tablets,
within the 262,144-tablet namespace. No Go object may exist per key or leaf;
resident navigation must use packed arrays and bounded page-cache frames.

The current monolithic router prototype proves the codec, not the production
write geometry:

| Isolated M4 Max route | 18/12 result |
| --- | ---: |
| combined routing bytes | 30.10 B/leaf |
| routing bytes at 187 rows | 0.1610 B/document |
| key route, hash included | 185.8 ns |
| key route, hash reused | 136.6 ns |
| resident posting-driven `BucketID` resolve | 25.1–26.4 ns |
| allocations | 0 |

The monolithic image is rejected for ordinary updates because it would rewrite
roughly 100 KiB to change one leaf reference. Production segmentation uses a
4 KiB tablet root, roughly 8 KiB anchor pages, stable anchor row slots, and the
8 KiB local-ID locator. An ordinary value COW then rewrites one anchor page and
its tablet root; the local-ID locator changes only during an anchor structural
rewrite.

### Point read

```text
snapshot root
 -> global tablet route with exact fence confirmation
 -> tablet root and lexical anchor page
 -> compact current leaf reference
 -> ordered-hash leaf, reusing the router's key hash
 -> two bounded control groups plus optional stash
 -> slot-to-rank
 -> boundary select
 -> exact key compare
 -> inline bytes or bounded overflow extents
```

An inline point read must stay within the complete 300 ns gate after tablet
catalog, tablet root, anchor page, and leaf-cache acquisition are included. An
overflow value adds the explicit extent reads reported by the benchmark.

### Ordered scan

```text
TabletCatalog lower bound
 -> snapshot-owned tablet/anchor cursor
 -> current leaf reference from each anchor row
 -> sequential lexical-rank decoder
 -> physically lexical key/value heap
```

There is no sort, tombstone subtraction, hash permutation, or version merge.
Physical sibling `PageRef`s cannot be the authoritative successor because COW
makes them stale in older generations; successor state comes from the
snapshot-owned rooted cursor.

## Updates, deletes, and structural work

Ordinary updates preserve `(BucketID, slot)`, rewrite one leaf plus one
segmented anchor path, and publish one root. The dense LocalLeafID locator is
unchanged because the anchor page and row slot are stable. A same-length,
projection-neutral update may use recovery-journaled canonical materialization
only when no snapshot can observe the old bytes. Otherwise it uses COW.

That per-mutation anchor-path rewrite is the synchronous and async-visible
contract. Buffered-visible mode uses the canonical-frame model from
[hybrid-mutations.md](hybrid-mutations.md), and the two documents share one
definition: an acknowledgement edits only the owned canonical leaf frame in
place (readerExclusive) and marks it dirty; anchor pages, tablet roots,
catalog nodes, and the state root are materialized once per checkpoint by the
bottom-up dirty-frame walk, not once per mutation. Repeated updates to one
leaf coalesce into its single after-image. This is the mechanism behind the
0.45 µs acknowledgement gate: route (~0.19 µs measured) plus one bounded
in-frame edit, with parent amplification amortized across the checkpoint
window. A snapshot or sealed checkpoint that can observe the frame forces the
ordinary COW path; the crash contract is unchanged because buffered
acknowledgements were never durable before their checkpoint.

The collection-local resident router records the newest state generation its
mutable leaf handles reflect. A snapshot selecting an older generation falls
back to the rooted catalog/tablet/anchor page walk, so a current-generation
handle rewrite cannot move that snapshot past its immutable graph.

Delete clears the live control byte and compacts lexical bytes immediately.
It writes no tombstone and leaves no probe-chain obligation. Empty leaves are
removed in the same generation. The phase-7 cutover path temporarily retains
and counts empty leaves until the split/merge structural transaction is wired.

Runtime inserts use empty candidate slots and then the bounded stash without
relocating published rows. Bulk build may use augmenting placement because no
posting identity exists yet.

Splits and merges are bounded structural transactions:

- a split keeps the old `BucketID` for the left leaf and allocates one for the
  right;
- rows moved right receive new posting tile identities in the same generation;
- adaptive slot classes avoid half-empty 256-slot leaves after ordinary splits;
- merge hysteresis prevents oscillation;
- every affected exact-index tile is rebuilt atomically;
- p50, p95, and p99 structural latency is reported separately.

The merge floor and the wide->narrow reclass gate are **capacity-relative, not
an absolute live count.** A leaf's achievable capacity is `min(slotCapacity,
byteCapacity)`: a leaf is slot-limited when its rows are tiny (an 8-byte value
narrow leaf holds ~195) and byte-limited when its rows are large (a 250-byte
document wide leaf holds ~28 before the 8 KiB heap is full). An absolute live
floor conflates the two: it declares every byte-full wide leaf permanently below
floor, so on a low-cardinality corpus (wide leaves at ~28 documents) every single
delete evaluates "warranted", flushes a full checkpoint, and then discovers there
is nothing to merge or reclaim -- a measured churn p50 of ~7.8 ms per delete.

The floor is therefore two conjuncts, expanded from `min(slotCap, byteCap)/4 < n`
so no division or per-row averaging is needed:

- **slot conjunct:** live count below ~25% of the narrow class's live slot
  budget (48 of 195), preserving the historical behaviour for the tiny-value
  geometry where a leaf really is slot-limited;
- **byte conjunct:** live key/value heap below 25% of the leaf's own byte
  capacity, so a leaf that is byte-full for its row size is never a merge
  candidate however few rows it holds.

The reclass gate is capacity-relative on the same two axes: a wide leaf is
reclaimed to narrow only when its rows fit the narrow class comfortably by
slots *and* by bytes, so a byte-full wide leaf is never re-encoded into a no-op
that falls back to wide yet still commits a checkpoint.

Merge/reclass evaluation is **read-only until a concrete transaction is viable.**
The routed leaf's own occupancy is peeked without a flush; for a below-floor
leaf a viable absorbing neighbour is confirmed by a read-only peek through the
resident router before any checkpoint. Only a selected, viable empty-removal,
merge, or reclass pays the pre-transaction flush (the structural commit already
flushes post-publish). Every other outcome -- ample leaf, byte-full leaf, no
viable neighbour -- returns in nanoseconds. Hysteresis is a per-leaf stamp of
the live count at the last no-viable-neighbour abort: the leaf is not
re-evaluated at that exact count until its own count changes (or a sibling's
own delete-side evaluation merges into it). The engagement counters
(evaluations, warranted, commits, aborts, hysteresis skips) make the
wasted-evaluation rate observable; only commits pay a flush.

The tradeoff is real: many secondary indexes make a slot-class rewrite more
expensive. Hiding that work behind an overlay would only move the cost to every
read and is not permitted.

A macro-tablet split is a larger, explicitly bounded workflow. The half that
keeps the old TabletID preserves its BucketIDs. The moved half receives the new
TabletID, so every affected posting tile must be repaired in the same
generation. Bulk build assigns final tablets bottom-up and pays none of this
rewrite. Runtime split latency, recovery capsule size, and multi-index
amplification are promotion gates; rarity is not permission to omit them.

## Exact indexes and deduplication

One canonical term leaf groups exact ordered scalar tuples. Posting identity is:

```text
TileID = (BucketID << 2) | quadrant
posting = (TileID, uint64 live-slot mask)
```

Four quadrants cover a normal 256-slot primary leaf. Singleton, few-mask, run,
sparse, and dense encodings are selected per posting. Repeated payloads share
leaf-local content-addressed bytes only when the dictionary entry is smaller
than the repeated payloads.

Index definitions are canonicalized by ordered paths and semantics. Aliases
with identical definitions share one physical index root; they do not build or
maintain duplicate postings.

The adaptive exact-term leaf at commit `649c7a3` removes repeated tile, row, and
mask structure without a read regression:

| Posting shape | B/posting, previous candidate → now | Exact lookup, previous candidate → now | Ordered iteration, previous candidate → now |
| --- | ---: | ---: | ---: |
| low-cardinality singleton | 5.094 → **1.156** | 231.5 → **25.4 ns** | 335 → **1.64 ns/posting** |
| low-cardinality one-mask | 13.09 → **1.266** | 271.6 → **25.9 ns** | 374 → **1.62 ns/posting** |
| high-cardinality singleton | 25.32 → **21.60** | 40.7 → **5.7 ns** | 3,262 → **1.63 ns/posting** |
| high-cardinality one-mask | 33.32 → **21.68** | 41.1 → **5.7 ns** | 3,328 → **1.67 ns/posting** |

All measured paths allocate zero. These are repeated physical-shape fixtures,
not a claim that every adaptive posting distribution compresses to the same
size. On the same four shapes, the new exact lookups are also 28–43% faster
than the equivalent packed-plus-exact baseline. Generic mixed distributions
and complete query execution remain promotion gates.

Posting order is stable identity order, not lexical key order. A query that
explicitly requests lexical index results intersects candidates while walking
the ordered primary cursor or sorts its bounded result. The API must state
which order it promises.

### Exact posting tiles on the ordered graph

Exact indexes are published beside the ordered primary graph, so a store that
declares indexes no longer needs the legacy chunk layout to carry the "indexed"
leg. Two self-describing page kinds carry them:

- `PagePrimaryExactLeaf` wraps exactly one canonical `IndexTermLeaf` byte stream
  (the adaptive posting codec above) in the common page envelope. One leaf holds
  the complete posting set for one physical index.
- `PagePrimaryExactRoot` is a `PageSize` reference catalog: an ordered record per
  physical index id, each either zero (an empty physical index) or a strictly
  ascending `PagePrimaryExactLeaf` reference. `StateRoot.ExactIndexRoot` names it.
  It is required exactly when the ordered primary declares indexes and keeps no
  legacy `IndexDirectory`, and is forbidden otherwise (`validateStateExactIndexRoot`).

**Build.** `CreateFromPrimary` builds the tiles inside the same transaction that
stages the graph. The bottom-up builder returns a posting-stable placement
(`BucketID`, slot) for every input row through `BuildPrimaryGraphPlaced`; the
exact build groups rows by canonical term, keys each posting by
`TileID = (BucketID << 2) | (slot >> 6)` with bit `slot & 63`, and encodes one
term leaf per non-empty index plus the root. The whole file image is a
deterministic function of the input and the canonical index catalog, so rebuilds
are byte-identical (`TestCreateFromPrimaryExactIndexDeterministic`), and the
catalog identity is folded into the reproducible `StoreID`.

**Slot model across leaf classes.** The posting-stable slot must mean the same
thing at build and at read. For the succinct narrow/wide leaves it is the leaf's
stable hash-directory slot; for the template-columnar class — which has no hash
directory — it is the lexical rank the template builder stamps as each row's
slot. `VisitPrimaryLeafPostingRows` is the single enumerator both the build
placement and the read path route through, so a row is always read under the
posting it was written under, regardless of class. Template ranks are bounded by
256, so they fit the four-quadrant tile model unchanged.

**Read.** A resident `IndexTermLeafView` per physical index answers exact
lookups; `AppendIndexMasksInto` canonicalizes the needle and returns one
`(tile, mask)` per live posting. Every posting bit is rechecked against a
live-slot map derived from the current graph, so a stale or grafted posting fails
closed. `RangeMasksRaw` materializes selected rows by resolving each tile's
bucket through the resident router and visiting the leaf's live rows in lexical
order.

**Mutation (build-and-read).** An indexed ordered-primary collection is
currently build-and-read: `Put`/`Delete` on it fail closed with
`ErrPrimaryExactIndexReadOnly` rather than publish a stale posting index. The
read path couples the live-slot map and the postings tightly — a delete that
left postings behind would make reads fail closed, an insert would make them
silently miss — so nothing between the two is acceptable. The honest maintenance
path is a bounded full rebuild of the exact tiles from the current graph at a
checkpoint that can afford the up-to-`fileStoreMaxPhysicalIndexes` term-leaf
reservation; it is not wired on the mutation path yet, so mutation refuses
instead of diverging. `structuralRepairPostingsHook` marks where the per-tablet
rebuild attaches once that lands, and is a no-op because the upstream gate keeps
structural transactions from ever firing on an indexed collection.

**Crash safety and verify.** Exact-index pages are staged and published through
the same committer as the graph in one atomic generation, so they add no new
commit-window shape. `cmd/vibedb-verify` walks `ExactIndexRoot` → every
`PagePrimaryExactLeaf`, admitting the reference catalog (bounds, ordering,
distinctness) and each leaf envelope, and records their extents for the
reachable-overlap check; recovery re-admits the exact root through
`validateRecoveredStateRef`.

## Allocator and locality

The free-space index uses a 64-way hierarchical maximum over the exact
offset-ordered free extent array. It preserves lowest-offset first-fit and adds
0.127 B/extent at 100M extents (12,698,432 bytes total). The exact 24-byte
extent table plus hierarchy costs 2,412,698,432 resident bytes at that scale.
The isolated late 64 KiB lookup remains logarithmic in the hierarchy depth.

Reopen must make every durable clean free extent eventually reusable, not just
the largest resident prefix. Cold extents remain packed and are promoted in
bounded chunks. Allocation, deletion, and rollback update the hierarchy without
allocations.

Allocator policy must also preserve lexical locality after random churn.
Logically ordered leaves scattered across the file keep correct scans but lose
sequential device behavior. Tablet-local extent classes, bounded clustering,
and explicit offline repack are part of the production benchmark, not deferred
cleanup. The gate metric is the mean absolute physical-offset distance between
lexically adjacent leaf extents after a fixed, seeded number of random churn
cycles; repack policy is intentionally not part of this allocator scale phase.

## Research decisions

Several modern indexes improve one axis by adding a second reader-visible
representation. They are useful evidence, but that trade is rejected here:

- Bf-Tree separates disk pages from variable-size cached mini-pages and reports
  strong point, scan, and update results. Its mini-pages may buffer recent
  updates, so a lookup must reconcile cached state with the disk page. We adopt
  the useful record/page-size decoupling as adaptive canonical leaf classes,
  but do not publish a mini-page delta above a base leaf.
- BzTree appends updates into unsorted leaf space and periodically consolidates.
  Reads inspect the unsorted region, and scans construct sorted response arrays.
  That is the exact read and maintenance debt this design forbids.
- PACTree's fingerprint and indirection arrays over sorted leaf data support
  the local ordered-hash shape. The next leaf persists the exact bounded route
  needed for cold reads rather than requiring recovery-time reconstruction.
- Current updatable learned indexes are not a default primary for arbitrary
  byte-string keys: published evaluations find that leaf key/position storage
  erodes the space advantage and that traditional indexes remain more robust
  across changing distributions. A learned router may be tested as an optional
  accelerator only after the exact prefix route exists.

Linux hardware atomic writes are a worthwhile capability lane, not a
portability assumption. Linux 6.13 exposes regular-file atomic-write limits
through `statx`; ext4 and XFS can accept aligned `O_DIRECT` `pwritev2` writes
with `RWF_ATOMIC` when the filesystem and device support them. This prevents a
torn target write, but it does not by itself solve the old-root/new-leaf crash
window or active snapshots. A future materialization lane may use it to shrink
the undo capsule from damage-granule images to the exact changed bytes, while
retaining the generation publication protocol and COW fallback. It must be
capability-probed and benchmarked; unsupported filesystems keep identical
semantics.

## Architecture-review closure ledger

The external review targeted commit `bc96a8a`, before the durability,
transaction, format, and retirement fixes now on `main`. Its findings remain
release gates even when the original bug is closed:

| Finding | Current disposition | Required proof before release |
| --- | --- | --- |
| unsafe zero-value durability | closed: `DurabilitySync` is zero and SQL path DSNs use it | data/root write and barrier failure matrix |
| failed synchronous commit remains visible | closed: applied, visible, and durable states are distinct; failure rolls readers back or fails closed | concurrent failure/reader tests plus reopen oracle |
| SQL uses statement snapshots and misses staged writes | closed: `BeginTx` retains one leased root and all reads use its overlay | insert/update/delete read-your-writes, repeatability, phantom, conflict, and lease-release tests |
| documented and implemented formats disagree | closed for the current inline-root format and locked by golden images | the vNext cutover updates the same authority and supplies copy migration |
| retired extents require full/quadratic scans | closed in the hot path: generation order, a moving head, bounded drains, and a fixed interval AVL remove pending-set scans | million-extent adversarial ordering, reopen, race, and allocation gates |
| reusable allocation is linear | closed for selection by the fixed 64-way maximum hierarchy | fragmentation, coalescing, locality, and long-churn whole-file gates |
| files are not self-describing | closed: exact canonical definitions, geometry, and admission bounds are durable; zero-option reopen rehydrates them before runtime resources | corrupt/missing/grafted catalog rejection, exact round trip, and current-device reassertion for in-place materialization |
| overlapping mutable engines and representations | open: vNext must become the only public mutable path; compact bulk cannot silently change performance class | API inventory plus deletion of obsolete paths after migration tests |
| compact bulk creates a read cliff | rejected as the default; paired point and all-byte benchmarks are mandatory | no promoted codec may regress point, random, lower-bound, or ordered scan gates |
| one collection is too large a 100 TB ownership unit | in progress: stable tablet/block identities and a shared-runtime-compatible catalog | split/merge, snapshot ownership, bounded resident metadata, and 100-billion-row simulations |
| backup, verify, salvage, and physical space return are missing | open | live-snapshot export, offline verify/salvage, and `vacuum-into` workflows |
| cross-tablet durability and snapshots are unspecified | intentionally after the local vNext format | leader epoch/sequence fencing, retained root history, safe-time reads, and GC watermark model |

Closing a row does not remove its tests. A rewrite that reintroduces one of
these failures is rejected even if its isolated codec benchmark is faster or
smaller.

## Promotion gates

The next format replaces the current primary only when the complete durable
store, not an isolated leaf, passes all gates on equivalent corpora:

| Metric | Gate |
| --- | ---: |
| warm random point | ≤300 ns, 0 alloc |
| lexical iteration | ≤6 ns/doc, 0 alloc |
| ordered all bytes | ≤60 ns/doc |
| local hit including hash | ≤45 ns |
| local miss including hash | ≤55 ns |
| async YCSB-B | ≥2.80 M ops/s |
| async YCSB-A | ≥2.15 M ops/s |
| async YCSB-F | ≥1.56 M ops/s |
| async churn | ≥2.23 M ops/s |
| async ordered-scan mix | ≥0.79 M ops/s |
| update p50 / p99 | ≤0.45 / 1.5 µs |
| delete + restore p50 / p99 | ≤0.75 / 1.8 µs |
| structural metadata | ≤5.0 B/live key after measured churn |
| whole-file disk | ≥15% below the best matched production-compressed competitor |
| sustained-churn live disk | flat live bytes under steady replace/delete while matched LSMs grow between compactions; measured as a first-class harness lane, not inferred |
| snapshot creation | O(1), no per-key work |
| snapshot-held read path | same page count and ≤1% latency change |
| post-delete debris | zero tombstones and version records |

The current async baseline is far below the mutation gates. The current Darwin
power-loss-safe pair also trails SQLite by 11–14% across YCSB-B/A/F, churn, and
the ordered-scan mix. Those are explicit open gaps, not projected wins.

## Integration sequence

1. Prove the ordered-hash leaf with exact differential, corruption, occupancy,
   split, stash, lower-bound, prefix, and scan tests.
2. Prove the persistent prefix `AnchorMap` under pathological shared prefixes,
   absent bounds, collisions, splits, and merges.
3. Add compact leaf envelopes and references; simulate 100-billion-document
   bounds before claiming 5 B/key.
4. Introduce one new root containing the tablet catalog, segmented tablet
   anchors and local-ID locators, primary leaves, and exact-term roots.
5. Integrate read-only point and lexical range paths first. Stop if either read
   gate fails.
6. Bulk-build final leaves and postings bottom-up. Unsorted input may use
   bounded unpublished scratch, never a runtime LSM.
7. Add ordinary update/delete with stable slots and COW publication.
8. Add split/reclass/merge with atomic posting repair and multi-index p99 tests.
9. Add qualified materialization and automatic combining only after crash-cut
   and snapshot proofs.
10. Run current point, scan, mixed, snapshot, overflow, index, memory, and disk
    matrices with matched production compression.
11. Make the winner the sole default and delete obsolete primary paths. The
    repository is unreleased, so there is no permanent two-engine tax.

## Known weaknesses

- Active snapshots force COW and can remove the in-place small-update win.
- A macro-tablet split rewrites moved leaves' secondary posting identities.
- Adaptive slot-class changes do bounded but potentially large multi-index work.
- Large incompressible documents lower occupancy or add overflow reads.
- Holding ≤5 B/key under churn requires measured occupancy and merge hygiene;
  a bulk-build number is insufficient.
- The current compact document codec is too slow to be a blanket default.
- Random churn can destroy physical scan locality without an allocator/repack
  policy.
- Isolated leaf numbers omit router, cache, checksum, durability, and index
  maintenance costs; they are evidence, not database-level victories.

## Harvested layout-qualification verdicts

These per-primitive verdicts were proven in a separate `internal/storeio/vnext`
qualification shelf. Each primitive has since graduated into the production
`storeio` package, so the shelf was retired; the conclusions that still bind the
production codecs are recorded here. The comparative harness was not an LSM and
never published a reader-visible journal, tombstone, forwarding page, or delta
overlay: one immutable root continues to name one canonical state.

- Keyed fingerprint directory: stores no key bytes and routes only; the complete
  key in the record block decides identity. A 4 KiB leaf holds 316 entries, each
  an implied eight-bit hash prefix, a 56-bit hash suffix, and a packed 26-bit
  stable block ID plus six-bit slot. Gate: at most 24 physical bytes per key at
  70% occupancy. An existing-key replacement never rewrites the directory unless
  its stable location changes.
- Stable block map: replaces the chunk radix and must match or improve its depth,
  cache footprint, and zone-summary behavior; it must never become a third
  lookup layer. Ordinary block references use a 12-byte physical locator (43-bit
  quantum offset, 48-bit generation, four-bit extent span, one-bit kind), or
  0.1875 bytes per full 64-row block; stable block IDs never encode physical
  offsets.
- Raw blocks: at most 64 stable slots, each a four-byte `(row start, key end)`
  record with JSON ending at the next live slot, ordered by a fixed 48-byte
  six-bit lexical permutation so a cross-block ordered tree needs only fences and
  block references, never one copied key per document. Gate: raw physical bytes
  at most 1.20 times key plus JSON on the representative small-document corpus.
- Packed blocks: one shared JSON prefix and suffix plus one contiguous
  independently decodable middle per row, retaining the raw four-byte stable-slot
  directory and permutation. A block stays raw unless packing saves at least one
  4 KiB quantum and 12.5% of the raw physical span, and packed promotion stays
  disabled until the integrated reader clears the read-latency gate. Gate: packed
  physical bytes at most 0.75 times key plus JSON on repetitive blocks;
  incompressible blocks keep exactly one canonical raw representation and readers
  never probe a packed alternative.
- Immutable route shard: the 50-bit collision-correct control (16-bit independent
  tag, 32-bit stable location, two state bits), not the final space target. A tag
  match always performs a complete-key comparison and never becomes identity, and
  at least 99.9% of random absent probes must acquire no document extent.
  Production scale instead needs shard-local row IDs and a separately accounted
  immutable block map so the resident route stays at or below 5.00 bytes per
  current key at 100 million, one billion, and modeled 100-billion-key scale;
  retained snapshot history is reported separately against an explicit cap.
- Posting tiles: one tile covers 64 stable chunks (4,096 rows) and
  deterministically picks the smallest of empty, all-live, dense, maximal runs,
  sparse chunk masks, and sparse row deltas; dense tiles are bounded at 512 bytes
  and payloads of at most 24 bytes stay inline in the term leaf, with no
  directory-to-posting layer. Against the 32-byte `(index, hash, chunk, mask)`
  record the per-tile space kill gates are all-live ≤1%, dense ≤30%, maximal runs
  ≤5%, one wide sparse mask ≤60%, one row per chunk ≤10%, and one inline
  singleton ≤30%. The all-live codec may be promoted only when the live mask is
  already co-resident with the term-leaf/manifest lookup.
- Manifest posting components: a typed 128-bit content identity where hash
  equality is only a lookup accelerator — type, length, and complete bytes must
  match before sharing. Components are immutable and reclaimed by root
  reachability and generation leases, never by an in-place refcount.
- Corruption boundary: the encoder deterministically emits maximal common JSON
  edges while the decoder accepts any structurally valid decomposition, so
  canonicality is a writer/publication invariant and the checksum plus structural
  validation are the sole corruption boundary. Data spans may be any 4 KiB
  multiple from 4 through 64 KiB; metadata stays fixed at one quantum until the
  cache and allocator support exact spans.

## Design references

- [Architecture overview](../architecture.md)
- [Performance baseline](../performance.md)
- [Wormhole: A Fast Ordered Index for In-memory Data Management](https://wuxb45.github.io/papers/wormhole.pdf)
- [Abseil Swiss Tables design notes](https://abseil.io/about/design/swisstables)
- [Faster Go maps with Swiss Tables](https://go.dev/blog/swisstable)
- [Bf-Tree: A Modern Read-Write-Optimized Concurrent Larger-Than-Memory Range Index](https://www.vldb.org/pvldb/vol17/p3442-hao.pdf)
- [BzTree: A High-Performance Latch-free Range Index for Non-Volatile Memory](https://www.vldb.org/pvldb/vol11/p553-arulraj.pdf)
- [Evaluating Persistent Memory Range Indexes, Part Two](https://www.vldb.org/pvldb/vol15/p2477-wang.pdf)
- [Are Updatable Learned Indexes Ready?](https://www.vldb.org/pvldb/vol15/p3004-wongkham.pdf)
- [Linux atomic block writes](https://www.kernel.org/doc/html/latest/filesystems/ext4/atomic_writes.html)
