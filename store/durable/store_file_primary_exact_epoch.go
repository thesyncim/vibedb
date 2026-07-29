package durable

import (
	"bytes"
	"fmt"
	"math/bits"
	"slices"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// The resident engine of the O(delta) indexed write path
// (docs/design/indexed-write-path.md §3–§5, §7).
//
// One immutable index epoch per collection: a fold base — the encoded exact
// term leaves (codec unchanged) plus a flat open-addressed tileID → live-tile
// table — under a generation-stamped, append-only overlay of per-term posting
// records and per-tile live records. A mutation appends O(touched terms)
// records instead of re-encoding the whole index (the measured 4,954 µs /
// ~3,100-alloc per-Put cliff this replaces); a probe on Snapshot G merges the
// newest records with gen ≤ G over the base; a fold (checkpoint, structural
// transaction, canonical-lane mutation transaction) resolves base+overlay
// through the same read rule and re-encodes with the unchanged builder, so
// fold output stays byte-identical to a from-scratch rebuild of the same
// final graph — the identity anchor the mutation-vs-rebuild tests pin.
//
// The read rule (§3, the whole consistency story): a probe on Snapshot G
// resolves term T per tile t as the newest term record for (T, t) with
// gen ≤ G if one exists; otherwise the fold-base posting — unless a tile
// record for t with gen ≤ G carries rebased at gen R and the base predates R.
// In P0 the base generation is always the last fold's generation and every
// overlay record is stamped with a strictly later publishing generation, so
// "base predates R" holds for every rebase in the overlay; the branch tests
// only "does a rebase ≤ G exist". Term records older than a later rebase of
// their tile are skipped by the same rule (the rebase group wrote absolute
// records for every term still present, so absence of a record ≥ R IS the
// truth "no bits"). Liveness reads the newest tile record ≤ G, else the flat
// table. Every branch is a pure function of (fold base, records ≤ G).
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

// Overlay capacity. Sized for the pressure-driven churn window (§4: records
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
// of the owning bucket (de-template, reclass): it voids the fold base's
// postings for this tile and invalidates older term records (§3 read rule).
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

// primaryLiveTable is the flat open-addressed replacement for the old
// map[uint32]*[64]uint64 live map (§5): immutable between folds, rebuilt
// O(tiles) at fold, one probe with linear stepping per lookup. Slots hold the
// full 64-chunk mask pointer because the term-leaf view retains per-posting
// live pointers for its lifetime; the primary graph populates chunk 0 only
// (slots are uint8: tile = bucket<<2 | slot>>6, bit = slot&63 — verified on
// the probe and reconstruct paths, which fail closed on any other chunk), so
// word-level rechecks read mask[0].
type primaryLiveTable struct {
	slots []primaryLiveTableSlot
	count int
}

type primaryLiveTableSlot struct {
	mask   *[storeio.TermPostingTileChunks]uint64
	tileID uint32
}

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

// primaryExactEpoch is the immutable-base + overlay pair (§3). The struct
// pointer is swapped whole under snapshotGate at fold points only; a Snapshot
// pins (epoch pointer, generation) and resolves postings as
// (fold base + records with gen ≤ G) indefinitely.
type primaryExactEpoch struct {
	// Fold base — immutable between folds. exact reuses the pre-existing
	// resident shape (encoded canonical leaf bytes + admitted view); views
	// close over live.lookup for their lifetime. baseGen is the generation of
	// the fold that produced the base; every overlay record's generation is
	// strictly greater, which is what collapses the read rule's "base
	// predates the rebase" test to "a rebase ≤ G exists".
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
	// whole overlay cost of the 1.22 µs post-fold probe gate.
	termRecordCount atomic.Uint32
	tileRecordCount atomic.Uint32

	// Epoch-owned storage the streamed checkpoint fold reuses across this
	// epoch's recycled lives: the flat live table's slot backing and the
	// per-index encode output buffers. Both are referenced only by this
	// epoch (mask arrays, which ARE shared across epochs by pointer, stay
	// individually GC-owned for exactly that reason), so recycling them once
	// the reclaim floor passes the epoch is safe by construction.
	liveStorage   primaryLiveTable
	encodeScratch [][]byte
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
// its retirement generation. Base references drop (the next fold installs
// fresh ones); overlay tables clear; slabs, the live-table slot backing, and
// the encode output buffers are retained so a steady-state fold cycle
// allocates nothing for overlay or base storage.
func (e *primaryExactEpoch) reset() {
	if cap(e.encodeScratch) < len(e.exact) {
		e.encodeScratch = make([][]byte, len(e.exact))
	}
	e.encodeScratch = e.encodeScratch[:cap(e.encodeScratch)]
	for i := range e.exact {
		if cap(e.exact[i].encoded) > cap(e.encodeScratch[i]) {
			e.encodeScratch[i] = e.exact[i].encoded[:0]
		}
	}
	e.exact = e.exact[:0]
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
// records are voided for this tile (§3).
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
// extents by the same documented remedy.
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
			clear(epoch.exact)
		}
		return epoch
	}
	return newPrimaryExactEpoch(indexCount)
}

// primaryExactBucketOverride carries a freshly re-derived bucket
// contribution into a fold resolve: the bucket's tiles are dropped from
// base+overlay and replaced by the derivation from the rewritten leaf image
// (the canonical mutation lane and pressure-escalated buffered mutations).
type primaryExactBucketOverride struct {
	bucket  storeio.BucketID
	live    map[uint32]uint64
	byIndex []map[string]map[uint32]uint64
}

// resolvePrimaryExactState resolves the epoch through the §3 read rule at
// generation atGen into the canonical (term → tile → bits) maps and live map
// the unchanged encodeIndexTermLeaf builder consumes. drop excludes every
// tile of the buckets a caller is about to re-derive (structural rebuilds,
// the canonical mutation lane); override merges one bucket's fresh
// derivation. This is fold-side code: it may allocate (the per-mutation path
// never enters here on the buffered lane).
func (c *Collection) resolvePrimaryExactState(
	epoch *primaryExactEpoch,
	atGen uint64,
	drop func(storeio.BucketID) bool,
	override *primaryExactBucketOverride,
	structural map[storeio.BucketID]*structuralBucketContribution,
) (
	byIndex []map[string]map[uint32]uint64,
	live map[uint32]*[storeio.TermPostingTileChunks]uint64,
	err error,
) {
	overlayTiles := epoch.tileRecordCount.Load() != 0
	dropTile := func(tileID uint32) bool {
		return drop != nil && drop(primaryExactTileBucket(tileID))
	}

	// Liveness: flat table, minus dropped and rebased-away tiles, overlaid
	// with the newest tile record ≤ atGen, then the fresh derivations.
	// Un-overridden tiles share the base's immutable mask arrays by pointer,
	// so a quiet fold allocates no per-tile storage.
	live = make(
		map[uint32]*[storeio.TermPostingTileChunks]uint64,
		epoch.live.count+8,
	)
	for at := range epoch.live.slots {
		slot := &epoch.live.slots[at]
		if slot.mask == nil || dropTile(slot.tileID) {
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
			for record := entry.head.Load(); record != nil; record = record.next {
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
	// newest-wins set/delete semantics, then the fresh derivations.
	byIndex = make([]map[string]map[uint32]uint64, len(epoch.exact))
	for indexID := range epoch.exact {
		terms := make(map[string]map[uint32]uint64)
		byIndex[indexID] = terms
		if !epoch.exact[indexID].present {
			continue
		}
		it := epoch.exact[indexID].view.Ordered()
		for {
			key, match, ok := it.Next()
			if !ok {
				break
			}
			var tiles map[uint32]uint64
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
				if overlayTiles && epoch.rebaseFloor(tileID, atGen) != 0 {
					// The bucket was slot-reassigned after this base was
					// folded; its rebase group re-recorded every surviving
					// term absolutely, so the base posting is void.
					continue
				}
				if tiles == nil {
					tiles = make(map[uint32]uint64)
				}
				tiles[tileID] |= mask.Bits
			}
			if tiles != nil {
				terms[string(key)] = tiles
			}
		}
	}
	if epoch.termRecordCount.Load() != 0 {
		// seenTiles is per-entry newest-wins scratch; entries are the exact
		// set of touched (index, term) pairs, so this pass is O(window).
		seenTiles := make(map[uint32]bool, 8)
		for i := 0; i < epoch.termEntryN; i++ {
			entry := &epoch.termEntries[i]
			terms := byIndex[entry.indexID]
			term := string(epoch.termBytesFor(entry))
			clear(seenTiles)
			for record := entry.head.Load(); record != nil; record = record.next {
				if record.gen > atGen || seenTiles[record.tileID] {
					continue
				}
				seenTiles[record.tileID] = true
				if dropTile(record.tileID) {
					continue
				}
				bits := record.bits
				if record.gen < epoch.rebaseFloor(record.tileID, atGen) {
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

// ensureLiveTable readies the epoch-owned flat table for count occupied
// tiles, reusing the slot backing across the epoch's recycled lives.
func (e *primaryExactEpoch) ensureLiveTable(count int) {
	size := 8
	for size < 2*count {
		size <<= 1
	}
	if cap(e.liveStorage.slots) < size {
		e.liveStorage.slots = make([]primaryLiveTableSlot, size)
	} else {
		e.liveStorage.slots = e.liveStorage.slots[:size]
		clear(e.liveStorage.slots)
	}
	e.liveStorage.count = 0
	e.live = &e.liveStorage
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

// encodeBuf hands out the reusable encode output buffer for one physical
// index, empty and ready to append.
func (e *primaryExactEpoch) encodeBuf(indexID int) []byte {
	if indexID < len(e.encodeScratch) {
		return e.encodeScratch[indexID][:0]
	}
	return nil
}

// prepareStreamedPrimaryExactFold is the checkpoint fold (§7) on its hot
// path: it resolves base+overlay through the read rule at atGen and streams
// the result straight into the leaf builder's input shape — no intermediate
// maps, collection-owned scratch, epoch-owned output buffers — because the
// map-based resolve was measured at ~5.4 ms per dirty checkpoint at the
// 10k/card=low churn shape (fold garbage driving GC on top of ~0.8 ms of
// encode CPU), which capped the indexed churn workload at ~31k ops/s against
// its 100k gate. The map-based prepareFoldedPrimaryExact remains the
// override-carrying lane (canonical per-mutation transactions, structural
// rebuilds, pressure escapes) and the bulk builder remains the byte-identity
// oracle: this stream must produce exactly the bytes encodeIndexTermLeaf
// produces for the same resolved postings, and the rebuild-identity tests
// enforce it.
func (c *Collection) prepareStreamedPrimaryExactFold(
	atGen, baseGen uint64,
) (primaryExactPrepared, error) {
	epoch := c.primaryEpoch
	fresh := c.takePrimaryExactEpochLocked(len(epoch.exact))

	// Liveness: the newest tile record ≤ atGen per touched tile overrides
	// the flat table; untouched tiles share their immutable mask arrays by
	// pointer (mask arrays are GC-owned, never recycled, precisely so this
	// cross-epoch sharing cannot dangle).
	changed := c.foldChangedTiles[:0]
	for i := 0; i < epoch.tileEntryN; i++ {
		entry := &epoch.tileEntries[i]
		for record := entry.head.Load(); record != nil; record = record.next {
			if record.gen <= atGen {
				changed = append(changed, primaryExactProbeTile{
					tileID: entry.tileID, bits: record.live,
				})
				break
			}
		}
	}
	c.foldChangedTiles = changed
	liveCount := 0
	if epoch.live != nil {
		liveCount = epoch.live.count
	}
	fresh.ensureLiveTable(liveCount + len(changed))
	if epoch.live != nil {
		for at := range epoch.live.slots {
			slot := &epoch.live.slots[at]
			if slot.mask == nil {
				continue
			}
			if len(changed) != 0 && epoch.lookupTileEntry(slot.tileID) != nil {
				overridden := false
				for i := range changed {
					if changed[i].tileID == slot.tileID {
						overridden = true
						break
					}
				}
				if overridden {
					continue
				}
			}
			fresh.live.insert(slot.tileID, slot.mask)
		}
	}
	for i := range changed {
		if changed[i].bits == 0 {
			continue // every row left; the tile is dead
		}
		mask := new([storeio.TermPostingTileChunks]uint64)
		mask[0] = changed[i].bits
		fresh.live.insert(changed[i].tileID, mask)
	}
	fresh.baseGen = baseGen

	// Scratch ceilings, computed once so the posting slab never reallocates
	// mid-index (terms hold sub-slices of it). The key arena MAY grow —
	// prefix-compressed keys can decompress past any cheap bound — so
	// arena-sourced term keys are rebound after each index's merge instead
	// of relying on address stability.
	overlayTermRecords := epoch.termRecordN
	maxPostings, maxTerms, maxKeyBytes := 0, 0, 0
	for indexID := range epoch.exact {
		if !epoch.exact[indexID].present {
			continue
		}
		view := &epoch.exact[indexID].view
		maxPostings = max(maxPostings, view.PostingLen())
		maxTerms = max(maxTerms, view.Len())
		maxKeyBytes = max(maxKeyBytes, view.EncodedBytes())
	}
	maxPostings += overlayTermRecords
	maxTerms += epoch.termEntryN
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
	overlayTiles := epoch.tileRecordCount.Load() != 0

	for indexID := range epoch.exact {
		// The window's touched terms for this index, in canonical order —
		// the entry slab is exactly that set, already interned.
		entries := c.foldEntryScratch[:0]
		for i := 0; i < epoch.termEntryN; i++ {
			if epoch.termEntries[i].indexID == uint32(indexID) {
				entries = append(entries, &epoch.termEntries[i])
			}
		}
		c.foldEntryScratch = entries
		slices.SortFunc(entries, func(a, b *primaryExactTermEntry) int {
			return bytes.Compare(epoch.termBytesFor(a), epoch.termBytesFor(b))
		})
		if !epoch.exact[indexID].present && len(entries) == 0 {
			fresh.exact[indexID] = primaryExactResident{}
			continue
		}

		terms := c.foldTermScratch[:0]
		postings := c.foldPostingScratch[:0]
		keyArena := c.foldKeyArena[:0]
		// keyOffsets[i] is the arena start of terms[i]'s canonical bytes, or
		// -1 for overlay-interned keys (stable in the old epoch's arena for
		// the whole fold — the old epoch is still installed).
		keyOffsets := c.foldKeyOffsets[:0]
		var baseIt storeio.IndexTermLeafIterator
		var baseKey []byte
		var baseMatch storeio.IndexTermLeafMatch
		baseOK := false
		if epoch.exact[indexID].present {
			baseIt = epoch.exact[indexID].view.Ordered()
			baseKey, baseMatch, baseOK = baseIt.Next()
		}
		ei := 0
		for baseOK || ei < len(entries) {
			// Select the next canonical term across both streams; equal
			// terms merge (records override their tiles absolutely).
			cmp := 0
			switch {
			case !baseOK:
				cmp = 1
			case ei >= len(entries):
				cmp = -1
			default:
				cmp = bytes.Compare(
					baseKey, epoch.termBytesFor(entries[ei]),
				)
			}
			var termBytes []byte
			var entry *primaryExactTermEntry
			keyOffset := -1
			useBase := false
			if cmp <= 0 {
				// The iterator's key scratch is overwritten by its next
				// advance; the arena copy survives (rebound below if the
				// arena reallocated) until the builder copies it out.
				keyOffset = len(keyArena)
				keyArena = append(keyArena, baseKey...)
				termBytes = keyArena[keyOffset:]
				useBase = true
			} else {
				termBytes = epoch.termBytesFor(entries[ei])
			}
			if cmp >= 0 {
				entry = entries[ei]
				if !useBase {
					termBytes = epoch.termBytesFor(entry)
				}
				ei++
			}
			// Resolve the term's overlay tiles: newest record ≤ atGen per
			// tile, rebase-filtered, sorted ascending — the probe's collect,
			// on the writer.
			overlay := c.foldTileScratch[:0]
			if entry != nil {
				for record := entry.head.Load(); record != nil; record = record.next {
					if record.gen > atGen {
						continue
					}
					lo, hi := 0, len(overlay)
					for lo < hi {
						mid := int(uint(lo+hi) >> 1)
						if overlay[mid].tileID < record.tileID {
							lo = mid + 1
						} else {
							hi = mid
						}
					}
					if lo < len(overlay) && overlay[lo].tileID == record.tileID {
						continue
					}
					recordBits := record.bits
					if overlayTiles &&
						record.gen < epoch.rebaseFloor(record.tileID, atGen) {
						recordBits = 0
					}
					overlay = append(overlay, primaryExactProbeTile{})
					copy(overlay[lo+1:], overlay[lo:])
					overlay[lo] = primaryExactProbeTile{
						tileID: record.tileID, bits: recordBits,
					}
				}
				c.foldTileScratch = overlay
			}
			postingStart := len(postings)
			// A chunk-0-only posting always lands on the leaf's direct
			// representations (≤ 2 non-zero chunks by construction), so the
			// adaptive codec's record — payload, codec choice, placement —
			// is never consumed by the encoding. The synthetic record below
			// therefore produces byte-identical leaves while skipping
			// BuildTermPosting's codec selection, which was ~40% of the
			// fold's CPU at the 10k shape. The identity tests compare
			// against the bulk builder byte-for-byte, so any future codec
			// change that voids this reasoning fails loudly.
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
			oi := 0
			if useBase {
				mi := baseMatch.MaskIterator()
				for {
					tileID, mask, more := mi.Next()
					if !more {
						break
					}
					if mask.Chunk != 0 {
						return primaryExactPrepared{},
							storeio.ErrPrimaryExactIndexCorrupt
					}
					for oi < len(overlay) && overlay[oi].tileID < tileID {
						if overlay[oi].bits != 0 {
							if err := emit(
								overlay[oi].tileID, overlay[oi].bits,
							); err != nil {
								return primaryExactPrepared{}, err
							}
						}
						oi++
					}
					if oi < len(overlay) && overlay[oi].tileID == tileID {
						if overlay[oi].bits != 0 {
							if err := emit(
								overlay[oi].tileID, overlay[oi].bits,
							); err != nil {
								return primaryExactPrepared{}, err
							}
						}
						oi++
						continue
					}
					if overlayTiles && epoch.rebaseFloor(tileID, atGen) != 0 {
						continue // rebased bucket voids its base postings
					}
					if err := emit(tileID, mask.Bits); err != nil {
						return primaryExactPrepared{}, err
					}
				}
			}
			for ; oi < len(overlay); oi++ {
				if overlay[oi].bits != 0 {
					if err := emit(
						overlay[oi].tileID, overlay[oi].bits,
					); err != nil {
						return primaryExactPrepared{}, err
					}
				}
			}
			if len(postings) != postingStart {
				record, ok := storeio.OpenIndexTermKeyRecord(
					c.storeID, termBytes,
				)
				if !ok {
					return primaryExactPrepared{}, fmt.Errorf(
						"%w: canonical exact term", storeio.ErrInvalidWrite,
					)
				}
				terms = append(terms, storeio.IndexTermLeafTerm{
					Key: record, Postings: postings[postingStart:len(postings):len(postings)],
				})
				keyOffsets = append(keyOffsets, keyOffset)
			}
			if useBase {
				baseKey, baseMatch, baseOK = baseIt.Next()
			}
		}
		// Rebind arena-sourced keys: the arena may have reallocated while
		// growing, and the route hash is a pure function of the bytes, so
		// only the slice pointers need refreshing.
		for i := range terms {
			if keyOffsets[i] < 0 {
				continue
			}
			length := len(terms[i].Key.Canonical)
			terms[i].Key.Canonical =
				keyArena[keyOffsets[i] : keyOffsets[i]+length]
		}
		c.foldTermScratch = terms[:0]
		c.foldPostingScratch = postings[:0]
		c.foldKeyArena = keyArena[:0]
		c.foldKeyOffsets = keyOffsets[:0]
		if len(terms) == 0 {
			fresh.exact[indexID] = primaryExactResident{}
			continue
		}
		encoded, err := storeio.AppendIndexTermLeaf(
			fresh.encodeBuf(indexID), c.storeID, terms,
		)
		if err != nil {
			return primaryExactPrepared{}, err
		}
		view, err := storeio.OpenIndexTermLeaf(
			encoded, c.storeID, fresh.live.lookup,
		)
		if err != nil {
			return primaryExactPrepared{}, err
		}
		fresh.exact[indexID] = primaryExactResident{
			encoded: encoded, view: view, present: true,
		}
	}
	return primaryExactPrepared{active: true, epoch: fresh, gen: baseGen}, nil
}

// prepareFoldedPrimaryExact resolves base+overlay at atGen (plus optional
// fresh bucket derivations) and encodes a complete fresh epoch with an empty
// overlay: the fold of §7. baseGen stamps the new epoch; installing it under
// the publish gate is the epoch swap. The encode reuses the exact builder
// the bulk build uses, so the output is byte-identical to a from-scratch
// rebuild of the same final graph.
func (c *Collection) prepareFoldedPrimaryExact(
	atGen uint64,
	baseGen uint64,
	drop func(storeio.BucketID) bool,
	override *primaryExactBucketOverride,
	structural map[storeio.BucketID]*structuralBucketContribution,
) (primaryExactPrepared, error) {
	epoch := c.primaryEpoch
	byIndex, live, err := c.resolvePrimaryExactState(
		epoch, atGen, drop, override, structural,
	)
	if err != nil {
		return primaryExactPrepared{}, err
	}
	fresh := c.takePrimaryExactEpochLocked(len(epoch.exact))
	fresh.live = newPrimaryLiveTable(live)
	fresh.baseGen = baseGen
	liveLookup := fresh.live.lookup
	for indexID := range byIndex {
		encoded, encErr := c.encodeIndexTermLeaf(byIndex[indexID], live)
		if encErr != nil {
			return primaryExactPrepared{}, encErr
		}
		if encoded == nil {
			fresh.exact[indexID] = primaryExactResident{}
			continue
		}
		view, viewErr := storeio.OpenIndexTermLeaf(
			encoded, c.storeID, liveLookup,
		)
		if viewErr != nil {
			return primaryExactPrepared{}, viewErr
		}
		fresh.exact[indexID] = primaryExactResident{
			encoded: encoded, view: view, present: true,
		}
	}
	return primaryExactPrepared{active: true, epoch: fresh, gen: baseGen}, nil
}
