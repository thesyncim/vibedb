package durable

import (
	"bytes"
	"math/bits"
	"slices"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// The resident engine of the O(delta) indexed write path.
//
// One immutable index epoch per collection: a fold base — the encoded exact
// term leaves, spanned across a deterministic content-defined cut set, plus a
// flat open-addressed tileID → live-tile table — under a
// generation-stamped, append-only overlay of per-term posting records and
// per-tile live records. A mutation appends O(touched terms) records instead
// of re-encoding the index; a probe on Snapshot G merges the newest records
// with gen ≤ G over the base; a fold (checkpoint, structural transaction,
// canonical-lane mutation transaction) resolves base+overlay through the same
// read rule and re-encodes with the unchanged leaf builder through the shared
// cutter, so fold output stays byte-identical to a from-scratch rebuild of
// the same final graph — the identity anchor the mutation-vs-rebuild tests
// pin, extended across cut boundaries.
//
// The read rule: a probe on Snapshot G
// resolves term T per tile t as the newest term record for (T, t) with
// gen ≤ G if one exists; otherwise the fold-base posting — unless a tile
// record for t with gen ≤ G carries rebased at gen R and the base predates R.
// The base generation is always the last fold's generation and every overlay
// record is stamped with a strictly later publishing generation, so "base
// predates R" holds for every rebase in the overlay; the branch tests only
// "does a rebase ≤ G exist". Term records older than a later rebase of their
// tile are skipped by the same rule (the rebase group wrote absolute records
// for every term still present, so absence of a record ≥ R IS the truth "no
// bits"). Liveness reads the newest tile record ≤ G, else the flat table.
// Every branch is a pure function of (fold base, records ≤ G).
//
// The fold is O(dirty leaves): the overlay's touched terms name exactly the
// content that can differ from the base, and the cutter's boundaries are
// local by construction — rule-1 cut terms delimit runs whose leaves are pure
// functions of the run's own content, and a giant term's stripe pieces are
// pure functions of their own stripe. The fold therefore re-cuts only the
// dirty runs (or, for a still-giant term, only its touched stripes), carries
// every other leaf forward by reference — encoded bytes, admitted view (live
// lookup rebound), and durable page ref — and the staging layer writes only
// the fresh leaves and retires only the superseded pages.
//
// Concurrency: records and entries are immutable once published. Entries are
// published into the open-addressed tables with atomic stores during the
// fallible prepare step (an entry with a nil chain head is indistinguishable
// from an absent one, so early publication is harmless if the mutation then
// fails); chain heads are linked only inside the same snapshotGate +
// reader-fence critical section that publishes the mutation's state, so
// "state at G visible" implies "every record ≤ G linked". The tables never
// resize and never delete between folds, so a lock-free reader probing to the
// first nil slot sees a consistent prefix of the single writer's publications.

// Overlay capacity. Sized for the pressure-driven churn window (records
// ≈ 32 B × record-emitting mutations per window); the mixed harness's
// 64-mutation checkpoint cadence uses ~130 records per window, and the
// capacity below covers ~2,000 record-emitting mutations before the
// ensure-step escalates to a checkpoint (the
// ensureBufferedPrimaryMutationCapacity discipline — fold and empty, never
// resize under readers). A single mutation that cannot fit even an empty
// overlay (a rebase group over a huge bucket, a run of maximum-size terms)
// escalates to a resident fold instead, which installs a fresh epoch with an
// empty overlay — correctness never depends on these sizes.
const (
	primaryExactOverlayTermRecordCap = 4096
	primaryExactOverlayTileRecordCap = 4096
	primaryExactOverlayTermEntryCap  = 2048
	primaryExactOverlayTileEntryCap  = 2048
	// Tables are 4× their entry caps so reader probes stay ~1 slot at the
	// worst admitted load factor (1/4).
	primaryExactOverlayTermTableSlots = 8192
	primaryExactOverlayTileTableSlots = 8192
	primaryExactOverlayTermBytesCap   = 128 << 10
)

// primaryExactTermRecord is one absolute posting overwrite: term (named by
// the owning chain's entry) in tile tileID holds exactly bits as of gen.
// Absolute, not delta — newest-wins needs no reconstruction walk.
type primaryExactTermRecord struct {
	next   *primaryExactTermRecord
	gen    uint64
	bits   uint64
	tileID uint32
}

// primaryExactTileRecord is one absolute liveness overwrite: tile tileID's
// live slot mask is live as of gen. rebased marks a slot-reassigning rewrite
// of the owning bucket (workspace or structural rewrite): it voids the fold base's
// postings for this tile and invalidates older term records.
type primaryExactTileRecord struct {
	next    *primaryExactTileRecord
	gen     uint64
	live    uint64
	tileID  uint32
	rebased bool
}

// primaryExactTermEntry is one distinct (physical index, canonical term)
// touched in the current fold window: the interned term bytes plus the
// newest-first record chain head. head is atomic because probes walk chains
// with no lock while the writer prepends under the publish fence.
type primaryExactTermEntry struct {
	head    atomic.Pointer[primaryExactTermRecord]
	hash    uint64
	termOff uint32
	termLen uint32
	indexID uint32
}

// primaryExactTileEntry is one distinct tile touched in the current window.
type primaryExactTileEntry struct {
	head   atomic.Pointer[primaryExactTileRecord]
	tileID uint32
}

// primaryLiveTable is a flat open-addressed live map: immutable between folds,
// cloned and
// patched O(touched tiles) at fold, one probe with linear stepping per
// lookup. Slots hold the full 64-chunk mask pointer because the term-leaf
// view retains per-posting live pointers for its lifetime; the primary graph
// populates chunk 0 only (slots are uint8: tile = bucket<<2 | slot>>6,
// bit = slot&63 — verified on the probe and reconstruct paths, which fail
// closed on any other chunk), so word-level rechecks read mask[0]. A tile
// whose rows all left points at the shared zero tile rather than clearing
// its slot: linear-probe chains stay intact under the patch-in-place fold,
// and a zero mask fails every posting containment check exactly as absence
// would.
type primaryLiveTable struct {
	slots []primaryLiveTableSlot
	count int
}

type primaryLiveTableSlot struct {
	mask   *[storeio.TermPostingTileChunks]uint64
	tileID uint32
}

// primaryExactZeroLiveTile is the shared all-dead tile mask patched-over
// slots point at. Immutable by convention; never written.
var primaryExactZeroLiveTile [storeio.TermPostingTileChunks]uint64

// primaryExactTileSlotHash spreads tile ids (dense per tablet, stride-4 per
// bucket) across the power-of-two table.
func primaryExactTileSlotHash(tileID uint32) uint32 {
	h := tileID
	h ^= h >> 16
	h *= 0x45d9f3b
	h ^= h >> 16
	return h
}

// primaryExactTermChainHash keys the overlay term table by (index, term)
// reusing the canonical route hash the leaf lookup already computes, so a
// probe hashes the needle exactly once.
func primaryExactTermChainHash(routeHash uint64, indexID uint32) uint64 {
	return routeHash ^ (uint64(indexID)+1)*0x9E3779B97F4A7C15
}

func newPrimaryLiveTable(
	live map[uint32]*[storeio.TermPostingTileChunks]uint64,
) *primaryLiveTable {
	size := 8
	for size < 2*len(live) {
		size <<= 1
	}
	table := &primaryLiveTable{
		slots: make([]primaryLiveTableSlot, size),
		count: len(live),
	}
	mask := uint32(size - 1)
	for tileID, tile := range live {
		at := primaryExactTileSlotHash(tileID) & mask
		for table.slots[at].mask != nil {
			at = (at + 1) & mask
		}
		table.slots[at] = primaryLiveTableSlot{mask: tile, tileID: tileID}
	}
	return table
}

// lookup returns the immutable live tile, or nil for an unoccupied tile. It
// is the IndexTermLeafLiveLookup the epoch's base views close over.
func (t *primaryLiveTable) lookup(
	tileID uint32,
) *[storeio.TermPostingTileChunks]uint64 {
	if t == nil || len(t.slots) == 0 {
		return nil
	}
	mask := uint32(len(t.slots) - 1)
	for at := primaryExactTileSlotHash(tileID) & mask; ; at = (at + 1) & mask {
		slot := &t.slots[at]
		if slot.mask == nil {
			return nil
		}
		if slot.tileID == tileID {
			return slot.mask
		}
	}
}

// word returns the tile's chunk-0 live word, 0 for an unoccupied tile.
func (t *primaryLiveTable) word(tileID uint32) uint64 {
	if tile := t.lookup(tileID); tile != nil {
		return tile[0]
	}
	return 0
}

func (t *primaryLiveTable) insert(
	tileID uint32, mask *[storeio.TermPostingTileChunks]uint64,
) {
	slotMask := uint32(len(t.slots) - 1)
	at := primaryExactTileSlotHash(tileID) & slotMask
	for t.slots[at].mask != nil {
		at = (at + 1) & slotMask
	}
	t.slots[at] = primaryLiveTableSlot{mask: mask, tileID: tileID}
	t.count++
}

// patch replaces or inserts one tile's mask in place. The caller has already
// guaranteed load-factor headroom for inserts. A nil-to-dead transition
// stores the shared zero tile so reader probe chains never break.
func (t *primaryLiveTable) patch(
	tileID uint32, mask *[storeio.TermPostingTileChunks]uint64,
) {
	slotMask := uint32(len(t.slots) - 1)
	at := primaryExactTileSlotHash(tileID) & slotMask
	for {
		slot := &t.slots[at]
		if slot.mask == nil {
			if mask != nil {
				*slot = primaryLiveTableSlot{mask: mask, tileID: tileID}
				t.count++
			}
			return
		}
		if slot.tileID == tileID {
			if mask == nil {
				slot.mask = &primaryExactZeroLiveTile
			} else {
				slot.mask = mask
			}
			return
		}
		at = (at + 1) & slotMask
	}
}

// primaryExactEpoch is the immutable-base + overlay pair. The struct
// pointer is swapped whole under snapshotGate at fold points only; a Snapshot
// pins (epoch pointer, generation) and resolves postings as
// (fold base + records with gen ≤ G) indefinitely.
type primaryExactEpoch struct {
	// Fold base — immutable between folds. exact holds each physical index's
	// spanned leaf set (the resident leaf router); views close over
	// live.lookup for their lifetime. Leaf encoded bytes, first-key copies,
	// and mask arrays are GC-owned precisely because folds share them across
	// epochs by reference; the epoch recycles only its own table storage and
	// slabs. baseGen is the generation of the fold that produced the base;
	// every overlay record's generation is strictly greater, which is what
	// collapses the read rule's "base predates the rebase" test to "a rebase
	// ≤ G exists".
	exact   []primaryExactResident
	live    *primaryLiveTable
	baseGen uint64

	// Overlay — append-only between folds. Slabs are bump-allocated by the
	// exclusive writer; entries/records are immutable once published;
	// termBytes interns each distinct term's canonical bytes exactly once
	// per window. Cursors are writer-private (readers navigate only through
	// the atomic tables and heads).
	termTable   []atomic.Pointer[primaryExactTermEntry]
	tileTable   []atomic.Pointer[primaryExactTileEntry]
	termEntries []primaryExactTermEntry
	tileEntries []primaryExactTileEntry
	termRecords []primaryExactTermRecord
	tileRecords []primaryExactTileRecord
	termBytes   []byte

	termEntryN  int
	tileEntryN  int
	termRecordN int
	tileRecordN int
	termBytesN  int

	// Record counters, published with the same release ordering as the chain
	// heads. A probe that loads 0 for both runs the byte-for-byte post-fold
	// base path with zero per-tile overlay lookups — this one load is the
	// whole overlay cost of the post-fold probe gate.
	termRecordCount atomic.Uint32
	tileRecordCount atomic.Uint32

	// liveStorage is the epoch-owned backing for the flat live table a dirty
	// fold clones into: the previous epoch's slots are copied verbatim and
	// only the window's touched tiles are patched, so the fold's live-table
	// cost is one memcpy plus O(touched tiles). Mask arrays are shared across
	// epochs by pointer and stay GC-owned for exactly that reason.
	liveStorage primaryLiveTable
}

func newPrimaryExactEpoch(indexCount int) *primaryExactEpoch {
	return &primaryExactEpoch{
		exact:       make([]primaryExactResident, indexCount),
		termTable:   make([]atomic.Pointer[primaryExactTermEntry], primaryExactOverlayTermTableSlots),
		tileTable:   make([]atomic.Pointer[primaryExactTileEntry], primaryExactOverlayTileTableSlots),
		termEntries: make([]primaryExactTermEntry, primaryExactOverlayTermEntryCap),
		tileEntries: make([]primaryExactTileEntry, primaryExactOverlayTileEntryCap),
		termRecords: make([]primaryExactTermRecord, primaryExactOverlayTermRecordCap),
		tileRecords: make([]primaryExactTileRecord, primaryExactOverlayTileRecordCap),
		termBytes:   make([]byte, primaryExactOverlayTermBytesCap),
	}
}

// reset prepares a retired epoch for reuse once the reclaim floor has passed
// its retirement generation. Base references drop (carried leaves are shared
// with newer epochs and GC-owned, so dropping the slice elements is the whole
// release); overlay tables clear; slabs, per-index leaf slice capacity, and
// the live-table slot backing are retained so a steady-state fold cycle
// allocates only what it re-encodes.
func (e *primaryExactEpoch) reset() {
	for i := range e.exact {
		resident := &e.exact[i]
		clear(resident.leaves)
		resident.leaves = resident.leaves[:0]
		resident.catalog = resident.catalog[:0]
	}
	e.live = nil
	e.baseGen = 0
	clear(e.termTable)
	clear(e.tileTable)
	clear(e.termEntries[:e.termEntryN])
	clear(e.tileEntries[:e.tileEntryN])
	e.termEntryN, e.tileEntryN = 0, 0
	e.termRecordN, e.tileRecordN = 0, 0
	e.termBytesN = 0
	e.termRecordCount.Store(0)
	e.tileRecordCount.Store(0)
	clear(e.liveStorage.slots)
	e.liveStorage.count = 0
}

func (e *primaryExactEpoch) overlayEmpty() bool {
	return e.termRecordCount.Load() == 0 && e.tileRecordCount.Load() == 0
}

func (e *primaryExactEpoch) termBytesFor(
	entry *primaryExactTermEntry,
) []byte {
	return e.termBytes[entry.termOff : entry.termOff+entry.termLen]
}

// lookupTermEntry is the reader-safe (index, term) probe: atomic loads,
// stopping at the first nil slot. An entry published after the reader's
// snapshot carries only records the generation filter skips, so missing it is
// indistinguishable from seeing it.
func (e *primaryExactEpoch) lookupTermEntry(
	hash uint64, indexID uint32, term []byte,
) *primaryExactTermEntry {
	mask := uint64(len(e.termTable) - 1)
	for at := hash & mask; ; at = (at + 1) & mask {
		entry := e.termTable[at].Load()
		if entry == nil {
			return nil
		}
		if entry.hash == hash && entry.indexID == indexID &&
			int(entry.termLen) == len(term) &&
			bytes.Equal(e.termBytesFor(entry), term) {
			return entry
		}
	}
}

func (e *primaryExactEpoch) lookupTileEntry(
	tileID uint32,
) *primaryExactTileEntry {
	mask := uint32(len(e.tileTable) - 1)
	for at := primaryExactTileSlotHash(tileID) & mask; ; at = (at + 1) & mask {
		entry := e.tileTable[at].Load()
		if entry == nil {
			return nil
		}
		if entry.tileID == tileID {
			return entry
		}
	}
}

// reserveTermEntry returns the entry for (indexID, term), publishing a fresh
// one (nil head) if the window has not touched the pair yet. ok is false on
// overlay pressure — entry slab, table load factor, or intern arena
// exhausted — and the caller escalates to a fold. Publishing during the
// fallible prepare step is safe: a nil-head entry is semantically absent.
func (e *primaryExactEpoch) reserveTermEntry(
	hash uint64, indexID uint32, term []byte,
) (entry *primaryExactTermEntry, ok bool) {
	if entry := e.lookupTermEntry(hash, indexID, term); entry != nil {
		return entry, true
	}
	if e.termEntryN >= len(e.termEntries) ||
		e.termEntryN >= len(e.termTable)/4 ||
		e.termBytesN+len(term) > len(e.termBytes) {
		return nil, false
	}
	entry = &e.termEntries[e.termEntryN]
	e.termEntryN++
	off := e.termBytesN
	copy(e.termBytes[off:], term)
	e.termBytesN += len(term)
	*entry = primaryExactTermEntry{
		hash: hash, termOff: uint32(off),
		termLen: uint32(len(term)), indexID: indexID,
	}
	mask := uint64(len(e.termTable) - 1)
	at := hash & mask
	for e.termTable[at].Load() != nil {
		at = (at + 1) & mask
	}
	e.termTable[at].Store(entry)
	return entry, true
}

func (e *primaryExactEpoch) reserveTileEntry(
	tileID uint32,
) (entry *primaryExactTileEntry, ok bool) {
	if entry := e.lookupTileEntry(tileID); entry != nil {
		return entry, true
	}
	if e.tileEntryN >= len(e.tileEntries) ||
		e.tileEntryN >= len(e.tileTable)/4 {
		return nil, false
	}
	entry = &e.tileEntries[e.tileEntryN]
	e.tileEntryN++
	*entry = primaryExactTileEntry{tileID: tileID}
	mask := uint32(len(e.tileTable) - 1)
	at := primaryExactTileSlotHash(tileID) & mask
	for e.tileTable[at].Load() != nil {
		at = (at + 1) & mask
	}
	e.tileTable[at].Store(entry)
	return entry, true
}

// allocTermRecord bump-allocates one record; false means overlay pressure.
// Records are unreachable until a chain head is stored, so a failed
// mutation's prepare unwinds by restoring the cursor.
func (e *primaryExactEpoch) allocTermRecord() (*primaryExactTermRecord, bool) {
	if e.termRecordN >= len(e.termRecords) {
		return nil, false
	}
	record := &e.termRecords[e.termRecordN]
	e.termRecordN++
	return record, true
}

func (e *primaryExactEpoch) allocTileRecord() (*primaryExactTileRecord, bool) {
	if e.tileRecordN >= len(e.tileRecords) {
		return nil, false
	}
	record := &e.tileRecords[e.tileRecordN]
	e.tileRecordN++
	return record, true
}

// rebaseFloor returns the generation of the newest rebase of tile ≤ atGen,
// or 0 when none: the threshold below which the fold base and older term
// records are voided for this tile.
func (e *primaryExactEpoch) rebaseFloor(tileID uint32, atGen uint64) uint64 {
	entry := e.lookupTileEntry(tileID)
	if entry == nil {
		return 0
	}
	for record := entry.head.Load(); record != nil; record = record.next {
		if record.gen <= atGen && record.rebased {
			return record.gen
		}
	}
	return 0
}

// resolveLiveWord is the read rule's liveness branch: the newest tile record
// ≤ atGen, else the flat table. overlayTiles lets the caller skip the chain
// lookup when it already knows no tile record exists in the window.
func (e *primaryExactEpoch) resolveLiveWord(
	tileID uint32, atGen uint64, overlayTiles bool,
) uint64 {
	if overlayTiles {
		if entry := e.lookupTileEntry(tileID); entry != nil {
			for record := entry.head.Load(); record != nil; record = record.next {
				if record.gen <= atGen {
					return record.live
				}
			}
		}
	}
	return e.live.word(tileID)
}

// liveTileBound is a conservative upper bound on the tiles a probe at any
// pinned generation can emit: the base's occupied tiles plus every tile a
// window record could have added. Records ≥ added tiles, and the atomic
// counter is safe to read from a snapshot.
func (e *primaryExactEpoch) liveTileBound() uint64 {
	if e == nil {
		return 0
	}
	count := uint64(0)
	if e.live != nil {
		count = uint64(e.live.count)
	}
	return count + uint64(e.tileRecordCount.Load())
}

// retiredPrimaryExactEpoch is one superseded epoch on the generation-keyed
// pending list: reachable by any Snapshot pinned below gen, recycled once
// the reclaim floor passes it.
type retiredPrimaryExactEpoch struct {
	epoch *primaryExactEpoch
	gen   uint64
}

// primaryExactReclaimFloor is the same floor extents reclaim under
// (invariant 6): min(active leases, active read epochs,
// oldestRecoveryGeneration). Epoch-slot point reads never touch postings,
// but including them costs one recycle deferral and keeps one floor story.
func (c *Collection) primaryExactReclaimFloor() uint64 {
	current := uint64(0)
	if state := c.state.Load(); state != nil {
		current = state.root.Generation
	}
	return min(
		c.leases.Minimum(current),
		c.readEpochs.Minimum(current),
		c.committer.FallbackGeneration(),
	)
}

// recyclePrimaryExactEpochsLocked moves retired epochs the floor has passed
// onto the reuse pool. Runs under the writer lock at fold points, so pool
// membership never races a fold's allocation.
func (c *Collection) recyclePrimaryExactEpochsLocked() {
	if len(c.primaryEpochRetired) == 0 {
		return
	}
	floor := c.primaryExactReclaimFloor()
	kept := c.primaryEpochRetired[:0]
	for _, retired := range c.primaryEpochRetired {
		if retired.gen < floor {
			retired.epoch.reset()
			c.primaryEpochPool = append(c.primaryEpochPool, retired.epoch)
			continue
		}
		kept = append(kept, retired)
	}
	for i := len(kept); i < len(c.primaryEpochRetired); i++ {
		c.primaryEpochRetired[i] = retiredPrimaryExactEpoch{}
	}
	c.primaryEpochRetired = kept
}

// takePrimaryExactEpochLocked returns a recycled epoch or allocates one.
// Steady-state fold cycles run off the pool (a retired epoch recycles as
// soon as the floor passes, one checkpoint later with no pinned Snapshot),
// so overlay storage reaches 0 allocs/fold once warm; a fresh allocation
// happens only under a long-pinned Snapshot, which already pins retired
// extents by the same documented remedy. Per-index leaf slice capacity is
// retained across recycles (reset truncated without freeing), so the carry
// of untouched leaves appends into warm storage.
func (c *Collection) takePrimaryExactEpochLocked(indexCount int) *primaryExactEpoch {
	c.recyclePrimaryExactEpochsLocked()
	if n := len(c.primaryEpochPool); n != 0 {
		epoch := c.primaryEpochPool[n-1]
		c.primaryEpochPool[n-1] = nil
		c.primaryEpochPool = c.primaryEpochPool[:n-1]
		if cap(epoch.exact) < indexCount {
			epoch.exact = make([]primaryExactResident, indexCount)
		} else {
			epoch.exact = epoch.exact[:indexCount]
		}
		return epoch
	}
	return newPrimaryExactEpoch(indexCount)
}

// primaryExactBucketOverride carries a freshly re-derived bucket
// contribution into a fold resolve: the bucket's tiles are dropped from
// base+overlay and replaced by the derivation from the rewritten leaf image
// (the canonical mutation lane's rebase fallback and pressure-escalated
// buffered mutations).
type primaryExactBucketOverride struct {
	bucket  storeio.BucketID
	live    map[uint32]uint64
	byIndex []map[string]map[uint32]uint64
}

// pendingTermHead resolves an entry's effective chain head when a prepared
// but not yet installed record set is in flight (the canonical mutation
// lane folds inside the same transaction that will publish the records, so
// its fold must see them before the install links them).
func pendingTermHead(
	pending *primaryExactPrepared, entry *primaryExactTermEntry,
) *primaryExactTermRecord {
	if pending != nil {
		for i := range pending.termLinks {
			if pending.termLinks[i].entry == entry {
				return pending.termLinks[i].head
			}
		}
	}
	return entry.head.Load()
}

func pendingTileHead(
	pending *primaryExactPrepared, entry *primaryExactTileEntry,
) *primaryExactTileRecord {
	if pending != nil {
		for i := range pending.tileLinks {
			if pending.tileLinks[i].entry == entry {
				return pending.tileLinks[i].head
			}
		}
	}
	return entry.head.Load()
}

// resolvePrimaryExactState resolves the epoch through the newest-wins read rule at
// generation atGen into the canonical (term → tile → bits) maps and live map
// the cutter-backed encoder consumes. drop excludes every tile of the
// buckets a caller is about to re-derive (structural rebuilds, the canonical
// mutation lane's rebase fallback); override merges one bucket's fresh
// derivation; pending exposes a prepared-but-unlinked record set. This is
// fold-side code: it may allocate (the per-mutation path never enters here
// on the buffered lane).
func (c *Collection) resolvePrimaryExactState(
	epoch *primaryExactEpoch,
	atGen uint64,
	drop func(storeio.BucketID) bool,
	override *primaryExactBucketOverride,
	structural map[storeio.BucketID]*structuralBucketContribution,
	pending *primaryExactPrepared,
) (
	byIndex []map[string]map[uint32]uint64,
	live map[uint32]*[storeio.TermPostingTileChunks]uint64,
	err error,
) {
	overlayTiles := epoch.tileRecordCount.Load() != 0 ||
		pending != nil && pending.tileRecordsAdded != 0
	dropTile := func(tileID uint32) bool {
		return drop != nil && drop(primaryExactTileBucket(tileID))
	}
	rebaseFloorAt := func(tileID uint32) uint64 {
		entry := epoch.lookupTileEntry(tileID)
		if entry == nil {
			return 0
		}
		for record := pendingTileHead(pending, entry); record != nil; record = record.next {
			if record.gen <= atGen && record.rebased {
				return record.gen
			}
		}
		return 0
	}

	// Liveness: flat table, minus dropped tiles, overlaid with the newest
	// tile record ≤ atGen, then the fresh derivations. Un-overridden tiles
	// share the base's immutable mask arrays by pointer, so a quiet fold
	// allocates no per-tile storage.
	live = make(
		map[uint32]*[storeio.TermPostingTileChunks]uint64,
		epoch.live.count+8,
	)
	for at := range epoch.live.slots {
		slot := &epoch.live.slots[at]
		if slot.mask == nil || slot.mask[0] == 0 && slot.mask == &primaryExactZeroLiveTile ||
			dropTile(slot.tileID) {
			continue
		}
		live[slot.tileID] = slot.mask
	}
	if overlayTiles {
		for i := 0; i < epoch.tileEntryN; i++ {
			entry := &epoch.tileEntries[i]
			if dropTile(entry.tileID) {
				continue
			}
			for record := pendingTileHead(pending, entry); record != nil; record = record.next {
				if record.gen > atGen {
					continue
				}
				if record.live == 0 {
					delete(live, entry.tileID)
				} else {
					tile := new([storeio.TermPostingTileChunks]uint64)
					tile[0] = record.live
					live[entry.tileID] = tile
				}
				break
			}
		}
	}
	applyLiveOverride := func(bucketLive map[uint32]uint64) {
		for tileID, bits := range bucketLive {
			if bits == 0 {
				delete(live, tileID)
				continue
			}
			tile := new([storeio.TermPostingTileChunks]uint64)
			tile[0] = bits
			live[tileID] = tile
		}
	}
	if override != nil {
		applyLiveOverride(override.live)
	}
	for _, contribution := range structural {
		applyLiveOverride(contribution.live)
	}

	// Postings, per physical index: base first (skipping dropped tiles and
	// tiles a rebase ≤ atGen voided), then overlay records applied with
	// newest-wins set/delete semantics, then the fresh derivations. The base
	// walks every spanned leaf in order; stripe pieces of one term merge in
	// the map through the same |= a bulk derivation uses.
	byIndex = make([]map[string]map[uint32]uint64, len(epoch.exact))
	for indexID := range epoch.exact {
		terms := make(map[string]map[uint32]uint64)
		byIndex[indexID] = terms
		for leafAt := range epoch.exact[indexID].leaves {
			it := epoch.exact[indexID].leaves[leafAt].view.Ordered()
			for {
				key, match, ok := it.Next()
				if !ok {
					break
				}
				tiles := terms[string(key)]
				emitted := false
				mi := match.MaskIterator()
				for {
					tileID, mask, more := mi.Next()
					if !more {
						break
					}
					if mask.Chunk != 0 {
						return nil, nil, storeio.ErrPrimaryExactIndexCorrupt
					}
					if dropTile(tileID) {
						continue
					}
					if overlayTiles && rebaseFloorAt(tileID) != 0 {
						// The bucket was slot-reassigned after this base was
						// folded; its rebase group re-recorded every surviving
						// term absolutely, so the base posting is void.
						continue
					}
					if tiles == nil {
						tiles = make(map[uint32]uint64)
					}
					tiles[tileID] |= mask.Bits
					emitted = true
				}
				if emitted {
					terms[string(key)] = tiles
				}
			}
		}
	}
	if epoch.termRecordCount.Load() != 0 ||
		pending != nil && pending.termRecordsAdded != 0 {
		// seenTiles is per-entry newest-wins scratch; entries are the exact
		// set of touched (index, term) pairs, so this pass is O(window).
		seenTiles := make(map[uint32]bool, 8)
		for i := 0; i < epoch.termEntryN; i++ {
			entry := &epoch.termEntries[i]
			terms := byIndex[entry.indexID]
			term := string(epoch.termBytesFor(entry))
			clear(seenTiles)
			for record := pendingTermHead(pending, entry); record != nil; record = record.next {
				if record.gen > atGen || seenTiles[record.tileID] {
					continue
				}
				seenTiles[record.tileID] = true
				if dropTile(record.tileID) {
					continue
				}
				bits := record.bits
				if record.gen < rebaseFloorAt(record.tileID) {
					bits = 0 // pre-rebase record: superseded slot assignment
				}
				tiles := terms[term]
				if bits == 0 {
					if tiles != nil {
						delete(tiles, record.tileID)
						if len(tiles) == 0 {
							delete(terms, term)
						}
					}
					continue
				}
				if tiles == nil {
					tiles = make(map[uint32]uint64)
					terms[term] = tiles
				}
				tiles[record.tileID] = bits
			}
		}
	}
	applyTermOverride := func(contribution []map[string]map[uint32]uint64) {
		for indexID := range contribution {
			terms := byIndex[indexID]
			for term, fresh := range contribution[indexID] {
				tiles := terms[term]
				if tiles == nil {
					tiles = make(map[uint32]uint64, len(fresh))
					terms[term] = tiles
				}
				for tileID, bits := range fresh {
					tiles[tileID] |= bits
				}
			}
		}
	}
	if override != nil {
		applyTermOverride(override.byIndex)
	}
	for _, contribution := range structural {
		applyTermOverride(contribution.byIndex)
	}
	for indexID := range byIndex {
		for term, tiles := range byIndex[indexID] {
			if len(tiles) == 0 {
				delete(byIndex[indexID], term)
			}
		}
	}
	return byIndex, live, nil
}

// prepareFoldedPrimaryExact resolves base+overlay at atGen (plus optional
// fresh bucket derivations) and encodes a complete fresh epoch with an empty
// overlay. The full fold is used by structural transactions, rebase
// windows, the canonical lane's slot-reassignment fallback, and overlay
// pressure escapes. baseGen stamps the new epoch; installing it under the
// publish gate is the epoch swap. The encode routes through the shared
// cutter, so the output leaf set is byte-identical to a from-scratch rebuild
// of the same final graph.
func (c *Collection) prepareFoldedPrimaryExact(
	atGen uint64,
	baseGen uint64,
	drop func(storeio.BucketID) bool,
	override *primaryExactBucketOverride,
	structural map[storeio.BucketID]*structuralBucketContribution,
	pending *primaryExactPrepared,
) (primaryExactPrepared, error) {
	epoch := c.primaryEpoch
	byIndex, live, err := c.resolvePrimaryExactState(
		epoch, atGen, drop, override, structural, pending,
	)
	if err != nil {
		return primaryExactPrepared{}, err
	}
	fresh := c.takePrimaryExactEpochLocked(len(epoch.exact))
	fresh.live = newPrimaryLiveTable(live)
	fresh.baseGen = baseGen
	liveLookup := fresh.live.lookup
	for indexID := range byIndex {
		leaves, encErr := c.encodePrimaryExactLeaves(
			byIndex[indexID], live, liveLookup,
		)
		if encErr != nil {
			return primaryExactPrepared{}, encErr
		}
		fresh.exact[indexID].leaves = append(
			fresh.exact[indexID].leaves[:0], leaves...,
		)
	}
	return primaryExactPrepared{active: true, epoch: fresh, gen: baseGen}, nil
}

// primaryExactFoldPlan is one touched (index, term)'s dirty classification
// inside a dirty fold: its resolved overlay tiles (a range in the fold's
// tile slab), where the term lives in the base leaf set, and whether its
// giant status lets the fold patch stripes instead of re-cutting a run.
type primaryExactFoldPlan struct {
	entry       *primaryExactTermEntry
	term        []byte
	routeHash   uint64
	overlayLo   int
	overlayHi   int
	homeLeaf    int // last leaf ≤ (term, 0); -1 before every leaf
	pieceLo     int // first leaf whose firstKey == term, -1 when none
	pieceHi     int
	giantBefore bool
	giantNow    bool
	presentNow  bool
	stripePatch bool // giantBefore && giantNow: touch only stripes
}

// prepareDirtyPrimaryExactFold is the checkpoint fold on its hot path:
// it resolves base+overlay through the read rule at atGen and re-encodes
// ONLY the leaves whose content the window's records can have changed —
// dirty rule-1 runs, or single touched stripes of a term that is giant on
// both sides of the window — carrying every other leaf forward by reference
// (bytes, admitted view with the live lookup rebound, durable page ref).
// Cut-boundary locality makes the splice byte-exact:
// cutting the whole final content and cutting only the dirty runs/stripes
// produce the same leaf set, and the rebuild-identity tests enforce it
// against the bulk-build oracle. pending exposes the canonical mutation
// lane's prepared-but-unlinked records; a rebase anywhere in the window
// escalates to the full fold (a rebase voids base postings of terms that
// received no record, which per-term dirty tracking cannot see).
func (c *Collection) prepareDirtyPrimaryExactFold(
	atGen, baseGen uint64, pending *primaryExactPrepared,
) (primaryExactPrepared, error) {
	epoch := c.primaryEpoch
	for i := 0; i < epoch.tileEntryN; i++ {
		entry := &epoch.tileEntries[i]
		for record := pendingTileHead(pending, entry); record != nil; record = record.next {
			if record.gen <= atGen && record.rebased {
				return c.prepareFoldedPrimaryExact(
					atGen, baseGen, nil, nil, nil, pending,
				)
			}
		}
	}
	fresh := c.takePrimaryExactEpochLocked(len(epoch.exact))
	fresh.baseGen = baseGen
	if err := c.foldLiveTable(epoch, fresh, atGen, pending); err != nil {
		return primaryExactPrepared{}, err
	}
	budget := storeio.IndexTermLeafCutBudget(uint32(c.options.MaxPageSize))

	for indexID := range epoch.exact {
		resident := &epoch.exact[indexID]
		entries := c.foldEntryScratch[:0]
		for i := 0; i < epoch.termEntryN; i++ {
			entry := &epoch.termEntries[i]
			if entry.indexID != uint32(indexID) {
				continue
			}
			has := false
			for record := pendingTermHead(pending, entry); record != nil; record = record.next {
				if record.gen <= atGen {
					has = true
					break
				}
			}
			if has {
				entries = append(entries, entry)
			}
		}
		c.foldEntryScratch = entries
		out := fresh.exact[indexID].leaves[:0]
		if len(entries) == 0 {
			// Quiet index: carry every leaf, rebinding the admitted views to
			// this epoch's live table (containment holds — no record means no
			// posting of these leaves changed liveness, see WithLive).
			for at := range resident.leaves {
				leaf := resident.leaves[at]
				leaf.view = leaf.view.WithLive(fresh.live.lookup)
				out = append(out, leaf)
			}
			fresh.exact[indexID].leaves = out
			continue
		}
		slices.SortFunc(entries, func(a, b *primaryExactTermEntry) int {
			return bytes.Compare(epoch.termBytesFor(a), epoch.termBytesFor(b))
		})
		var err error
		out, err = c.foldDirtyIndex(
			epoch, fresh, resident, entries, atGen, pending, budget, out,
		)
		if err != nil {
			return primaryExactPrepared{}, err
		}
		fresh.exact[indexID].leaves = out
	}
	return primaryExactPrepared{active: true, epoch: fresh, gen: baseGen}, nil
}

// foldLiveTable produces the fresh epoch's flat live table: the previous
// table's slots copied verbatim into epoch-owned storage, patched with the
// newest tile record ≤ atGen per touched tile. Mask arrays for changed tiles
// are freshly allocated (never mutated in place — older epochs share the old
// arrays by pointer). Growth pressure falls back to a full re-insert into a
// larger table.
func (c *Collection) foldLiveTable(
	epoch, fresh *primaryExactEpoch, atGen uint64,
	pending *primaryExactPrepared,
) error {
	changed := c.foldChangedTiles[:0]
	for i := 0; i < epoch.tileEntryN; i++ {
		entry := &epoch.tileEntries[i]
		for record := pendingTileHead(pending, entry); record != nil; record = record.next {
			if record.gen <= atGen {
				changed = append(changed, primaryExactProbeTile{
					tileID: entry.tileID, bits: record.live,
				})
				break
			}
		}
	}
	c.foldChangedTiles = changed
	old := epoch.live
	oldSlots := 0
	oldCount := 0
	if old != nil {
		oldSlots = len(old.slots)
		oldCount = old.count
	}
	need := oldSlots
	if need == 0 {
		need = 8
	}
	for 2*(oldCount+len(changed)) > need {
		need <<= 1
	}
	if cap(fresh.liveStorage.slots) < need {
		fresh.liveStorage.slots = make([]primaryLiveTableSlot, need)
	} else {
		fresh.liveStorage.slots = fresh.liveStorage.slots[:need]
	}
	fresh.liveStorage.count = 0
	fresh.live = &fresh.liveStorage
	if need == oldSlots {
		copy(fresh.liveStorage.slots, old.slots)
		fresh.liveStorage.count = oldCount
	} else {
		clear(fresh.liveStorage.slots)
		if old != nil {
			for at := range old.slots {
				slot := &old.slots[at]
				if slot.mask == nil {
					continue
				}
				fresh.liveStorage.insert(slot.tileID, slot.mask)
			}
		}
	}
	for i := range changed {
		if changed[i].bits == 0 {
			fresh.liveStorage.patch(changed[i].tileID, nil)
			continue
		}
		mask := new([storeio.TermPostingTileChunks]uint64)
		mask[0] = changed[i].bits
		fresh.liveStorage.patch(changed[i].tileID, mask)
	}
	return nil
}

// foldDirtyIndex splices one physical index: dirty rule-1 runs are re-cut
// from merged base+overlay content, still-giant terms with touched stripes
// are patched stripe by stripe, and everything else carries forward.
func (c *Collection) foldDirtyIndex(
	epoch, fresh *primaryExactEpoch,
	resident *primaryExactResident,
	entries []*primaryExactTermEntry,
	atGen uint64,
	pending *primaryExactPrepared,
	budget int,
	out []primaryExactLeaf,
) ([]primaryExactLeaf, error) {
	leaves := resident.leaves

	// Resolve every touched term's overlay tiles into one shared slab
	// (sorted ascending, newest record ≤ atGen wins; no rebase exists in a
	// dirty window) and classify the term against the base.
	overlaySlab := c.foldTileScratch[:0]
	plans := c.foldPlanScratch[:0]
	for _, entry := range entries {
		term := epoch.termBytesFor(entry)
		lo := len(overlaySlab)
		for record := pendingTermHead(pending, entry); record != nil; record = record.next {
			if record.gen > atGen {
				continue
			}
			at, hi := lo, len(overlaySlab)
			for at < hi {
				mid := int(uint(at+hi) >> 1)
				if overlaySlab[mid].tileID < record.tileID {
					at = mid + 1
				} else {
					hi = mid
				}
			}
			if at < len(overlaySlab) && overlaySlab[at].tileID == record.tileID {
				continue
			}
			overlaySlab = append(overlaySlab, primaryExactProbeTile{})
			copy(overlaySlab[at+1:], overlaySlab[at:])
			overlaySlab[at] = primaryExactProbeTile{
				tileID: record.tileID, bits: record.bits,
			}
		}
		plan := primaryExactFoldPlan{
			entry: entry, term: term,
			routeHash: storeio.IndexTermRouteHash(c.storeID, term),
			overlayLo: lo, overlayHi: len(overlaySlab),
			homeLeaf: -1, pieceLo: -1, pieceHi: -1,
		}
		if len(leaves) != 0 {
			plan.homeLeaf = primaryExactLeafAt(leaves, term, 0)
			scan := plan.homeLeaf
			if scan < 0 {
				scan = 0
			}
			for at := scan; at < len(leaves) && at <= scan+1; at++ {
				if bytes.Equal(leaves[at].firstKey, term) {
					plan.pieceLo = at
					break
				}
			}
			if plan.pieceLo >= 0 {
				plan.pieceHi = plan.pieceLo
				for plan.pieceHi < len(leaves) &&
					bytes.Equal(leaves[plan.pieceHi].firstKey, term) {
					plan.pieceHi++
				}
				plan.giantBefore = leaves[plan.pieceLo].piece
			}
		}
		baseCount, err := c.foldBaseTermPostingCount(leaves, &plan)
		if err != nil {
			return nil, err
		}
		mergedCount := baseCount
		for at := plan.overlayLo; at < plan.overlayHi; at++ {
			baseBits, bitsErr := c.foldBaseTermTileBits(
				leaves, &plan, overlaySlab[at].tileID,
			)
			if bitsErr != nil {
				return nil, bitsErr
			}
			if baseBits != 0 && overlaySlab[at].bits == 0 {
				mergedCount--
			} else if baseBits == 0 && overlaySlab[at].bits != 0 {
				mergedCount++
			}
		}
		plan.presentNow = mergedCount > 0
		plan.giantNow = storeio.IndexTermLeafGiant(
			storeio.IndexTermLeafEstimateChunk0TermBytes(
				len(term), mergedCount,
			), budget,
		)
		plan.stripePatch = plan.giantBefore && plan.giantNow
		plans = append(plans, plan)
	}
	c.foldTileScratch = overlaySlab
	c.foldPlanScratch = plans

	if len(leaves) == 0 {
		// Empty base: the whole index is one dirty run built from overlay
		// content alone.
		return c.foldRebuildRange(
			epoch, fresh, leaves, 0, 0, plans, overlaySlab, budget, out,
		)
	}

	// Run decomposition: runs[i] is the leaf position starting run i.
	runs := c.foldRunScratch[:0]
	for at := range leaves {
		if primaryExactRunHead(leaves, at) {
			runs = append(runs, at)
		}
	}
	c.foldRunScratch = runs
	runOfLeaf := func(leafAt int) int {
		lo, hi := 0, len(runs)
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			if runs[mid] <= leafAt {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		return lo - 1
	}

	// Mark dirty runs. A stripe-patchable term dirties nothing here; every
	// other touched term dirties the run containing its position, and a
	// deleted rule-1 cut term additionally dirties the previous run (its
	// wall falls, so the two runs' contents re-cut as one).
	dirty := c.foldDirtyRunScratch[:0]
	for range runs {
		dirty = append(dirty, false)
	}
	c.foldDirtyRunScratch = dirty
	for at := range plans {
		plan := &plans[at]
		if plan.stripePatch {
			continue
		}
		home := plan.homeLeaf
		if plan.pieceLo >= 0 {
			home = plan.pieceLo
		}
		if home < 0 {
			home = 0
		}
		run := runOfLeaf(home)
		dirty[run] = true
		if storeio.IndexTermLeafRunCut(plan.routeHash) && !plan.presentNow &&
			run > 0 {
			dirty[run-1] = true
		}
	}

	// Splice: walk runs in order; carry clean runs (patching still-giant
	// touched stripes in place), re-cut dirty ones.
	for runAt := range runs {
		runStart := runs[runAt]
		runEnd := len(leaves)
		if runAt+1 < len(runs) {
			runEnd = runs[runAt+1]
		}
		if !dirty[runAt] {
			var err error
			out, err = c.foldCarryRun(
				epoch, fresh, leaves, runStart, runEnd, plans, overlaySlab,
				budget, out,
			)
			if err != nil {
				return nil, err
			}
			continue
		}
		var err error
		out, err = c.foldRebuildRange(
			epoch, fresh, leaves, runStart, runEnd, plans, overlaySlab,
			budget, out,
		)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// foldBaseTermPostingCount counts the term's base tile-postings: the sum of
// its stripe pieces' posting counts (piece leaves are single-term, so their
// posting count is the term's), or its packed match length in the one leaf
// that can hold it.
func (c *Collection) foldBaseTermPostingCount(
	leaves []primaryExactLeaf, plan *primaryExactFoldPlan,
) (int, error) {
	if plan.giantBefore {
		count := 0
		for at := plan.pieceLo; at < plan.pieceHi; at++ {
			count += leaves[at].view.PostingLen()
		}
		return count, nil
	}
	at := plan.pieceLo
	if at < 0 {
		at = plan.homeLeaf
	}
	if at < 0 {
		return 0, nil
	}
	match, found := leaves[at].view.LookupRecord(
		storeio.IndexTermKeyRecord{
			RouteHash: plan.routeHash, Canonical: plan.term,
		},
	)
	if !found {
		return 0, nil
	}
	return match.Len(), nil
}

// foldBaseTermTileBits reads the term's base posting bits for one tile —
// the writer-side probe restricted to the fold base.
func (c *Collection) foldBaseTermTileBits(
	leaves []primaryExactLeaf, plan *primaryExactFoldPlan, tileID uint32,
) (uint64, error) {
	at := -1
	if plan.giantBefore {
		at = primaryExactLeafAt(
			leaves[plan.pieceLo:plan.pieceHi], plan.term, tileID,
		)
		if at >= 0 {
			at += plan.pieceLo
		}
	} else if plan.pieceLo >= 0 {
		at = plan.pieceLo
	} else if plan.homeLeaf >= 0 {
		at = plan.homeLeaf
	}
	if at < 0 {
		return 0, nil
	}
	match, found := leaves[at].view.LookupRecord(storeio.IndexTermKeyRecord{
		RouteHash: plan.routeHash, Canonical: plan.term,
	})
	if !found {
		return 0, nil
	}
	mi := match.MaskIterator()
	for {
		tile, mask, more := mi.Next()
		if !more || tile > tileID {
			return 0, nil
		}
		if tile == tileID {
			if mask.Chunk != 0 {
				return 0, storeio.ErrPrimaryExactIndexCorrupt
			}
			return mask.Bits, nil
		}
	}
}

// foldCarryRun copies one clean run's leaves into the fresh epoch, patching
// the stripe pieces of still-giant touched terms in place. Untouched leaves
// keep their encoded bytes and durable refs; only rebuilt stripe pieces are
// fresh.
func (c *Collection) foldCarryRun(
	epoch, fresh *primaryExactEpoch,
	leaves []primaryExactLeaf,
	runStart, runEnd int,
	plans []primaryExactFoldPlan,
	overlaySlab []primaryExactProbeTile,
	budget int,
	out []primaryExactLeaf,
) ([]primaryExactLeaf, error) {
	at := runStart
	for at < runEnd {
		leaf := &leaves[at]
		plan := foldStripePlanFor(plans, leaf, at)
		if plan == nil {
			carried := *leaf
			carried.view = carried.view.WithLive(fresh.live.lookup)
			out = append(out, carried)
			at++
			continue
		}
		var err error
		out, err = c.foldPatchGiantTerm(
			epoch, fresh, leaves, plan, overlaySlab, budget, out,
		)
		if err != nil {
			return nil, err
		}
		at = plan.pieceHi
	}
	return out, nil
}

// foldStripePlanFor returns the stripe-patch plan owning leaf at, if any.
func foldStripePlanFor(
	plans []primaryExactFoldPlan, leaf *primaryExactLeaf, at int,
) *primaryExactFoldPlan {
	if !leaf.piece {
		return nil
	}
	for i := range plans {
		if plans[i].stripePatch && at >= plans[i].pieceLo &&
			at < plans[i].pieceHi {
			return &plans[i]
		}
	}
	return nil
}

// foldPatchGiantTerm re-emits one still-giant term's pieces: untouched
// stripes carry their leaves forward; touched stripes resolve base+overlay
// and re-cut through the same rule-2/rule-3 path the full cutter uses, so
// the piece set matches a from-scratch cut of the merged term byte for byte.
func (c *Collection) foldPatchGiantTerm(
	epoch, fresh *primaryExactEpoch,
	leaves []primaryExactLeaf,
	plan *primaryExactFoldPlan,
	overlaySlab []primaryExactProbeTile,
	budget int,
	out []primaryExactLeaf,
) ([]primaryExactLeaf, error) {
	overlay := overlaySlab[plan.overlayLo:plan.overlayHi]
	pieceAt := plan.pieceLo
	oi := 0
	for pieceAt < plan.pieceLo || oi < len(overlay) || pieceAt < plan.pieceHi {
		// Select the next stripe present on either side.
		var stripe uint32
		haveBase := pieceAt < plan.pieceHi
		haveOverlay := oi < len(overlay)
		if !haveBase && !haveOverlay {
			break
		}
		baseStripe := uint32(0)
		if haveBase {
			baseStripe = storeio.IndexTermLeafStripe(leaves[pieceAt].firstTile)
		}
		overlayStripe := uint32(0)
		if haveOverlay {
			overlayStripe = storeio.IndexTermLeafStripe(overlay[oi].tileID)
		}
		switch {
		case haveBase && (!haveOverlay || baseStripe < overlayStripe):
			stripe = baseStripe
		case haveOverlay && (!haveBase || overlayStripe < baseStripe):
			stripe = overlayStripe
		default:
			stripe = baseStripe
		}
		basePieceEnd := pieceAt
		for basePieceEnd < plan.pieceHi &&
			storeio.IndexTermLeafStripe(leaves[basePieceEnd].firstTile) == stripe {
			basePieceEnd++
		}
		overlayEnd := oi
		for overlayEnd < len(overlay) &&
			storeio.IndexTermLeafStripe(overlay[overlayEnd].tileID) == stripe {
			overlayEnd++
		}
		if overlayEnd == oi {
			// Untouched stripe: carry its pieces byte-for-byte.
			for ; pieceAt < basePieceEnd; pieceAt++ {
				carried := leaves[pieceAt]
				carried.view = carried.view.WithLive(fresh.live.lookup)
				out = append(out, carried)
			}
			continue
		}
		// Touched stripe: merge base masks with the overlay's absolute tiles
		// and re-cut the stripe.
		postings := c.foldPostingScratch[:0]
		stripeOverlay := overlay[oi:overlayEnd]
		soi := 0
		emit := func(tileID uint32, maskBits uint64) error {
			liveMask := fresh.live.lookup(tileID)
			if liveMask == nil {
				return storeio.ErrPrimaryExactIndexCorrupt
			}
			postings = append(postings, storeio.IndexTermLeafPosting{
				Posting: storeio.TermPosting{
					TileID: tileID,
					Rows:   uint16(bits.OnesCount64(maskBits)),
				},
				Live:       liveMask,
				Chunk0Bits: maskBits, Chunk0Only: true,
			})
			return nil
		}
		for base := pieceAt; base < basePieceEnd; base++ {
			match, found := leaves[base].view.LookupRecord(
				storeio.IndexTermKeyRecord{
					RouteHash: plan.routeHash, Canonical: plan.term,
				},
			)
			if !found {
				return nil, storeio.ErrPrimaryExactIndexCorrupt
			}
			mi := match.MaskIterator()
			for {
				tileID, mask, more := mi.Next()
				if !more {
					break
				}
				if mask.Chunk != 0 {
					return nil, storeio.ErrPrimaryExactIndexCorrupt
				}
				for soi < len(stripeOverlay) &&
					stripeOverlay[soi].tileID < tileID {
					if stripeOverlay[soi].bits != 0 {
						if err := emit(
							stripeOverlay[soi].tileID, stripeOverlay[soi].bits,
						); err != nil {
							return nil, err
						}
					}
					soi++
				}
				if soi < len(stripeOverlay) &&
					stripeOverlay[soi].tileID == tileID {
					if stripeOverlay[soi].bits != 0 {
						if err := emit(
							stripeOverlay[soi].tileID, stripeOverlay[soi].bits,
						); err != nil {
							return nil, err
						}
					}
					soi++
					continue
				}
				if err := emit(tileID, mask.Bits); err != nil {
					return nil, err
				}
			}
		}
		for ; soi < len(stripeOverlay); soi++ {
			if stripeOverlay[soi].bits != 0 {
				if err := emit(
					stripeOverlay[soi].tileID, stripeOverlay[soi].bits,
				); err != nil {
					return nil, err
				}
			}
		}
		pieceAt = basePieceEnd
		oi = overlayEnd
		c.foldPostingScratch = postings[:0]
		if len(postings) == 0 {
			continue // stripe emptied; its pieces drop
		}
		record, ok := storeio.OpenIndexTermKeyRecord(c.storeID, plan.term)
		if !ok {
			return nil, storeio.ErrPrimaryExactIndexCorrupt
		}
		term := storeio.IndexTermLeafTerm{Key: record, Postings: postings}
		var cutErr error
		out, cutErr = c.foldEmitCutLeaves(
			fresh, []storeio.IndexTermLeafTerm{term}, budget, out, true,
		)
		if cutErr != nil {
			return nil, cutErr
		}
	}
	return out, nil
}

// foldRebuildRange re-cuts one dirty run: the base terms of leaves
// [runStart, runEnd) merged with every touched term in the run's key range,
// streamed through the shared cutter and encoded fresh.
func (c *Collection) foldRebuildRange(
	epoch, fresh *primaryExactEpoch,
	leaves []primaryExactLeaf,
	runStart, runEnd int,
	plans []primaryExactFoldPlan,
	overlaySlab []primaryExactProbeTile,
	budget int,
	out []primaryExactLeaf,
) ([]primaryExactLeaf, error) {
	// The run's touched terms: plans whose term falls inside
	// [firstKey(runStart), firstKey(runEnd)) — with open bounds at the
	// index's edges. Plans are entry-sorted, so this is one range scan.
	var lowKey, highKey []byte
	if runStart < len(leaves) && runStart > 0 {
		lowKey = leaves[runStart].firstKey
	}
	if runEnd < len(leaves) {
		highKey = leaves[runEnd].firstKey
	}
	planLo := 0
	for planLo < len(plans) && lowKey != nil &&
		bytes.Compare(plans[planLo].term, lowKey) < 0 {
		planLo++
	}
	planHi := planLo
	for planHi < len(plans) && (highKey == nil ||
		bytes.Compare(plans[planHi].term, highKey) < 0) {
		planHi++
	}
	runPlans := plans[planLo:planHi]

	// Scratch ceilings so the posting slab never reallocates mid-run (terms
	// hold sub-slices of it). The key arena MAY grow — prefix-compressed
	// keys can decompress past any cheap bound — so arena-sourced term keys
	// are rebound after the merge.
	maxPostings, maxTerms, maxKeyBytes := 0, 0, 0
	for at := runStart; at < runEnd; at++ {
		maxPostings += leaves[at].view.PostingLen()
		maxTerms += leaves[at].view.Len()
		maxKeyBytes += leaves[at].view.EncodedBytes()
	}
	for at := range runPlans {
		maxPostings += runPlans[at].overlayHi - runPlans[at].overlayLo
		maxTerms++
	}
	if cap(c.foldPostingScratch) < maxPostings {
		c.foldPostingScratch = make([]storeio.IndexTermLeafPosting, 0, maxPostings)
	}
	if cap(c.foldTermScratch) < maxTerms {
		c.foldTermScratch = make([]storeio.IndexTermLeafTerm, 0, maxTerms)
	}
	if cap(c.foldKeyArena) < maxKeyBytes {
		c.foldKeyArena = make([]byte, 0, maxKeyBytes)
	}
	if cap(c.foldKeyOffsets) < maxTerms {
		c.foldKeyOffsets = make([]int, 0, maxTerms)
	}
	terms := c.foldTermScratch[:0]
	postings := c.foldPostingScratch[:0]
	keyArena := c.foldKeyArena[:0]
	keyOffsets := c.foldKeyOffsets[:0]

	emit := func(tileID uint32, maskBits uint64) error {
		liveMask := fresh.live.lookup(tileID)
		if liveMask == nil {
			return storeio.ErrPrimaryExactIndexCorrupt
		}
		postings = append(postings, storeio.IndexTermLeafPosting{
			Posting: storeio.TermPosting{
				TileID: tileID,
				Rows:   uint16(bits.OnesCount64(maskBits)),
			},
			Live:       liveMask,
			Chunk0Bits: maskBits, Chunk0Only: true,
		})
		return nil
	}

	// Base stream across the run's leaves; pieces of one term appear as
	// consecutive equal keys and merge into one posting run.
	baseLeaf := runStart
	var baseIt storeio.IndexTermLeafIterator
	var baseKey []byte
	var baseMatch storeio.IndexTermLeafMatch
	baseOK := false
	advance := func() {
		for {
			if baseOK {
				return
			}
			if baseLeaf >= runEnd {
				return
			}
			baseIt = leaves[baseLeaf].view.Ordered()
			baseLeaf++
			baseKey, baseMatch, baseOK = baseIt.Next()
			if baseOK {
				return
			}
		}
	}
	nextBase := func() {
		baseKey, baseMatch, baseOK = baseIt.Next()
		if !baseOK {
			advance()
		}
	}
	advance()
	pi := 0
	for baseOK || pi < len(runPlans) {
		cmp := 0
		switch {
		case !baseOK:
			cmp = 1
		case pi >= len(runPlans):
			cmp = -1
		default:
			cmp = bytes.Compare(baseKey, runPlans[pi].term)
		}
		var termBytes []byte
		var plan *primaryExactFoldPlan
		keyOffset := -1
		useBase := cmp <= 0
		if useBase {
			// The iterator's key scratch is overwritten by its next advance;
			// the arena copy survives (rebound below if the arena
			// reallocated) until the builder copies it out.
			keyOffset = len(keyArena)
			keyArena = append(keyArena, baseKey...)
			termBytes = keyArena[keyOffset:]
		} else {
			termBytes = runPlans[pi].term
		}
		if cmp >= 0 {
			plan = &runPlans[pi]
			if !useBase {
				termBytes = plan.term
			}
			pi++
		}
		var overlay []primaryExactProbeTile
		if plan != nil {
			overlay = overlaySlab[plan.overlayLo:plan.overlayHi]
		}
		postingStart := len(postings)
		oi := 0
		for useBase {
			// Consume every consecutive base piece of this term, merging the
			// overlay's absolute tiles in ascending order.
			mi := baseMatch.MaskIterator()
			for {
				tileID, mask, more := mi.Next()
				if !more {
					break
				}
				if mask.Chunk != 0 {
					return nil, storeio.ErrPrimaryExactIndexCorrupt
				}
				for oi < len(overlay) && overlay[oi].tileID < tileID {
					if overlay[oi].bits != 0 {
						if err := emit(
							overlay[oi].tileID, overlay[oi].bits,
						); err != nil {
							return nil, err
						}
					}
					oi++
				}
				if oi < len(overlay) && overlay[oi].tileID == tileID {
					if overlay[oi].bits != 0 {
						if err := emit(
							overlay[oi].tileID, overlay[oi].bits,
						); err != nil {
							return nil, err
						}
					}
					oi++
					continue
				}
				if err := emit(tileID, mask.Bits); err != nil {
					return nil, err
				}
			}
			nextBase()
			if !baseOK || !bytes.Equal(baseKey, termBytes) {
				break
			}
		}
		for ; oi < len(overlay); oi++ {
			if overlay[oi].bits != 0 {
				if err := emit(overlay[oi].tileID, overlay[oi].bits); err != nil {
					return nil, err
				}
			}
		}
		if len(postings) != postingStart {
			record, ok := storeio.OpenIndexTermKeyRecord(c.storeID, termBytes)
			if !ok {
				return nil, storeio.ErrPrimaryExactIndexCorrupt
			}
			terms = append(terms, storeio.IndexTermLeafTerm{
				Key:      record,
				Postings: postings[postingStart:len(postings):len(postings)],
			})
			keyOffsets = append(keyOffsets, keyOffset)
		}
	}
	// Rebind arena-sourced keys: the arena may have reallocated while
	// growing, and the route hash is a pure function of the bytes, so only
	// the slice pointers need refreshing.
	for i := range terms {
		if keyOffsets[i] < 0 {
			continue
		}
		length := len(terms[i].Key.Canonical)
		terms[i].Key.Canonical = keyArena[keyOffsets[i] : keyOffsets[i]+length]
	}
	c.foldTermScratch = terms[:0]
	c.foldPostingScratch = postings[:0]
	c.foldKeyArena = keyArena[:0]
	c.foldKeyOffsets = keyOffsets[:0]
	if len(terms) == 0 {
		return out, nil
	}
	return c.foldEmitCutLeaves(fresh, terms, budget, out, false)
}

// foldEmitCutLeaves runs the shared cutter over a merged term sequence and
// encodes each emitted leaf into a fresh resident leaf (GC-owned bytes,
// admitted view, zero ref — the staging layer writes it durable).
// stripeOnly restricts the cutter to the giant-term stripe path so a patched
// stripe's pieces carry the piece flag exactly as a full cut would.
func (c *Collection) foldEmitCutLeaves(
	fresh *primaryExactEpoch,
	terms []storeio.IndexTermLeafTerm,
	budget int,
	out []primaryExactLeaf,
	stripeOnly bool,
) ([]primaryExactLeaf, error) {
	emit := func(leafTerms []storeio.IndexTermLeafTerm, piece bool) error {
		encoded, err := storeio.AppendIndexTermLeaf(nil, c.storeID, leafTerms)
		if err != nil {
			return err
		}
		view, err := storeio.OpenIndexTermLeaf(
			encoded, c.storeID, fresh.live.lookup,
		)
		if err != nil {
			return err
		}
		leaf, err := c.newPrimaryExactResidentLeaf(encoded, view, piece)
		if err != nil {
			return err
		}
		out = append(out, leaf)
		return nil
	}
	if stripeOnly {
		if err := storeio.CutGiantIndexTerm(&terms[0], budget, emit); err != nil {
			return nil, err
		}
		return out, nil
	}
	if err := storeio.CutIndexTermLeaves(terms, budget, emit); err != nil {
		return nil, err
	}
	return out, nil
}
