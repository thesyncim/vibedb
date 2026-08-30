package durable

import (
	"bytes"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// Exact-index posting maintenance for the ordered primary graph.
//
// A mutation rewrites exactly one leaf, and a leaf owns exactly the four
// quadrant posting tiles bucket<<2 | q (q in 0..3). Every posting bit for a
// row is a deterministic function of that row's stable slot in its leaf, so
// the whole exact-index effect of a mutation is confined to those tiles.
//
// The buffered write rule appends O(touched terms) overlay records instead of
// re-encoding the resident term leaves:
//
//   - update with the indexed value unchanged: zero records. The slot is
//     stable (UpdateTo/DeleteTo/PromoteCommonPrimaryLeaf* preserve published
//     slots by contract) and the term is identical, so the index is
//     untouched. This is the YCSB-dominant same-size replacement shape.
//   - update with the value changed: ≤ 2 absolute term records per physical
//     index — the old term's tile bits minus the slot, the new term's plus it
//     — using the pre-image the leaf lookup already has in hand.
//   - insert / delete: one term record per index carrying a term, plus one
//     tile record for the touched quadrant's live mask.
//   - slot-reassigning compact VCS1 workspace rewrite
//     (PlaceCommonPrimaryLeafRecords reassigns every slot): fall back
//     to deriveBucketExactContribution (O(leaf rows), exactly the old cost,
//     once per leaf-class transition) and emit a rebase group — 4 rebased
//     tile records plus absolute term records for every term present in the
//     bucket, all at one generation.
//
// The canonical per-mutation-transaction lane and structural transactions
// keep their fold-first shape: they resolve base+overlay through the
// read rule, re-encode with the unchanged builder, and install a fresh epoch,
// so an incrementally maintained index stays byte-identical to a fresh
// CreateFromPrimary of the same final graph — the anchor the
// mutation-vs-rebuild determinism tests pin. Re-derivation, not term-deltas,
// is still what makes the rebase group correct under every slot-reassignment
// class: both sides route through VisitPrimaryLeafPostingRows.

// primaryExactTileBucket recovers the BucketID that owns a posting tile. A leaf
// with BucketID b owns the four quadrant tiles b<<2 | q for q in 0..3.
func primaryExactTileBucket(tileID uint32) storeio.BucketID {
	return storeio.BucketID(tileID >> 2)
}

// primaryExactBounds are the publication bounds shared by the exact root and
// leaf envelopes for state.
func (c *Collection) primaryExactBounds(
	state *fileStoreState,
) storeio.PrimaryExactIndexBounds {
	return storeio.PrimaryExactIndexBounds{
		StoreID: c.storeID, Generation: state.root.Generation,
		FileEnd: state.fileEnd, NextLogicalID: state.root.NextLogicalID,
		AllocationQuantum: state.root.PageSize,
		MaxPageSize:       state.root.MaxPageSize, IndexCount: state.root.IndexCount,
	}
}

// deriveBucketExactContribution visits the live rows of the rewritten leaf image
// and returns, for the four tiles of bucket, the new live slot mask and, per
// physical index, the canonical term -> tile -> bits the bucket now contributes.
// It is fold-side and rebase-side code — the common buffered mutation never
// calls it on the ordinary buffered mutation path.
func (c *Collection) deriveBucketExactContribution(
	leafPage []byte, bucket storeio.BucketID,
	bounds storeio.CommonPrimaryLeafBounds,
) (
	bucketLive map[uint32]uint64,
	byIndex []map[string]map[uint32]uint64,
	err error,
) {
	indexes := c.options.indexes
	byIndex = make([]map[string]map[uint32]uint64, len(indexes))
	for i := range byIndex {
		byIndex[i] = make(map[string]map[uint32]uint64)
	}
	bucketLive = make(map[uint32]uint64, 4)
	var components [store.MaxIndexColumns]storeio.IndexTermComponent
	var canonical [storeio.IndexTermMaxKeyBytes]byte
	_, err = storeio.VisitPrimaryLeafPostingRows(
		leafPage, c.storeID, bucket, bounds, nil,
		func(slot uint8, _, raw []byte, overflow bool) error {
			if overflow {
				// The row stores its value out of line, so raw is the chain head
				// descriptor, not the document. Resolve the value through the shared
				// overflow reader and index the reassembled document exactly as an
				// inline row: term derivation is a pure function of the value bytes,
				// so an out-of-line value contributes the identical postings a fitting
				// one would. The chain is cache-resident here (volatile on the buffered
				// lane, durable after checkpoint), so resolution reads no device.
				resolved, resolveErr := c.appendPrimaryOverflowValue(
					c.overflowValueScratch[:0],
					storeio.DecodePrimaryOverflowRef(raw), bounds,
				)
				if resolveErr != nil {
					return resolveErr
				}
				c.overflowValueScratch = resolved
				raw = resolved
			}
			tileID := uint32(bucket)<<2 | uint32(slot>>6)
			bit := uint64(1) << uint(slot&63)
			bucketLive[tileID] |= bit
			for indexID, exact := range indexes {
				key, present, termErr := appendPrimaryExactDocumentTerm(
					canonical[:0], components[:], exact, raw,
				)
				if termErr != nil {
					return termErr
				}
				if !present {
					continue
				}
				identity := string(key)
				tiles := byIndex[indexID][identity]
				if tiles == nil {
					tiles = make(map[uint32]uint64)
					byIndex[indexID][identity] = tiles
				}
				tiles[tileID] |= bit
			}
			return nil
		},
	)
	if err != nil {
		return nil, nil, err
	}
	return bucketLive, byIndex, nil
}

// primaryExactTermLink is one prepared chain-head publication: head's
// next-chain was threaded onto entry's current chain during the fallible
// prepare, and the install stores head with one atomic write.
type primaryExactTermLink struct {
	entry *primaryExactTermEntry
	head  *primaryExactTermRecord
}

type primaryExactTileLink struct {
	entry *primaryExactTileEntry
	head  *primaryExactTileRecord
}

// primaryExactPrepared is a prepared exact-index effect not yet installed.
// Separating prepare from install lets every fallible step run before a
// mutation's point of no return and the install stay infallible under
// snapshotGate, exactly like the journal's fallible-append /
// infallible-publish split. Two shapes:
//
//   - epoch != nil: a complete fresh epoch (fold output, empty overlay) to
//     swap in — the canonical mutation lane, structural transactions,
//     checkpoint folds, and overlay-pressure escalations.
//   - epoch == nil: overlay chain heads to link into the current epoch — the
//     common buffered mutation. Link slices alias writer-owned scratch on the
//     Collection, so a steady-state mutation allocates nothing here.
type primaryExactPrepared struct {
	active bool
	gen    uint64
	epoch  *primaryExactEpoch

	termLinks        []primaryExactTermLink
	tileLinks        []primaryExactTileLink
	termRecordsAdded uint32
	tileRecordsAdded uint32
	// Unwind cursors: records are bump-allocated and unreachable until their
	// heads are stored, so a failed mutation restores the cursors. Entries
	// and interned term bytes published during prepare persist — a nil-head
	// entry is semantically absent, so they are harmless until the next fold
	// resets the overlay.
	termRecordMark int
	tileRecordMark int
}

// installPrimaryExactResidentLocked installs a prepared exact-index effect.
// It must run inside the same snapshotGate (+ reader fence on the buffered
// lane) section that publishes the mutation's state, so "state at G visible"
// implies "every record ≤ G linked" and a reader captures base and overlay
// as one epoch pointer. Infallible; a no-op for the unindexed path.
func (c *Collection) installPrimaryExactResidentLocked(
	prepared primaryExactPrepared,
) {
	if !prepared.active {
		return
	}
	if prepared.epoch != nil {
		old := c.primaryEpoch
		c.primaryEpoch = prepared.epoch
		if old != nil {
			// The generation-keyed pending list: a Snapshot pinned below
			// prepared.gen still resolves through the old epoch; it recycles
			// once the reclaim floor passes (invariant 6).
			c.primaryEpochRetired = append(
				c.primaryEpochRetired,
				retiredPrimaryExactEpoch{epoch: old, gen: prepared.gen},
			)
		}
		return
	}
	for i := range prepared.termLinks {
		link := &prepared.termLinks[i]
		link.entry.head.Store(link.head)
	}
	for i := range prepared.tileLinks {
		link := &prepared.tileLinks[i]
		link.entry.head.Store(link.head)
	}
	if prepared.termRecordsAdded != 0 {
		c.primaryEpoch.termRecordCount.Add(prepared.termRecordsAdded)
	}
	if prepared.tileRecordsAdded != 0 {
		c.primaryEpoch.tileRecordCount.Add(prepared.tileRecordsAdded)
	}
	c.exactTermLinkScratch = prepared.termLinks[:0]
	c.exactTileLinkScratch = prepared.tileLinks[:0]
}

// unwindPrimaryExactPrepared discards a prepared effect after a later
// prepare step failed (journal fence, admission): overlay records return to
// the bump allocator, a prepared fresh epoch returns to the pool. Nothing
// was linked or swapped, so no reader can hold any of it.
func (c *Collection) unwindPrimaryExactPrepared(
	prepared *primaryExactPrepared,
) {
	if !prepared.active {
		return
	}
	if prepared.epoch != nil {
		prepared.epoch.reset()
		c.primaryEpochPool = append(c.primaryEpochPool, prepared.epoch)
		prepared.epoch = nil
		return
	}
	epoch := c.primaryEpoch
	epoch.termRecordN = prepared.termRecordMark
	epoch.tileRecordN = prepared.tileRecordMark
	c.exactTermLinkScratch = prepared.termLinks[:0]
	c.exactTileLinkScratch = prepared.tileLinks[:0]
	prepared.termLinks, prepared.tileLinks = nil, nil
}

// emitPrimaryExactTermRecord threads one absolute term record onto the
// pending chain for (indexID, term). false reports overlay pressure; the
// caller unwinds and escalates to a fold. Multiple records for one entry in
// one mutation (a rebase group's tiles) chain through the pending head, so
// one atomic store at install publishes them all.
func (c *Collection) emitPrimaryExactTermRecord(
	epoch *primaryExactEpoch, prepared *primaryExactPrepared,
	chainHash uint64, indexID uint32, term []byte,
	tileID uint32, gen, bits uint64,
) bool {
	entry, ok := epoch.reserveTermEntry(chainHash, indexID, term)
	if !ok {
		return false
	}
	record, ok := epoch.allocTermRecord()
	if !ok {
		return false
	}
	next := entry.head.Load()
	for i := range prepared.termLinks {
		if prepared.termLinks[i].entry == entry {
			next = prepared.termLinks[i].head
			break
		}
	}
	*record = primaryExactTermRecord{
		next: next, gen: gen, bits: bits, tileID: tileID,
	}
	for i := range prepared.termLinks {
		if prepared.termLinks[i].entry == entry {
			prepared.termLinks[i].head = record
			prepared.termRecordsAdded++
			return true
		}
	}
	prepared.termLinks = append(prepared.termLinks, primaryExactTermLink{
		entry: entry, head: record,
	})
	prepared.termRecordsAdded++
	return true
}

func (c *Collection) emitPrimaryExactTileRecord(
	epoch *primaryExactEpoch, prepared *primaryExactPrepared,
	tileID uint32, gen, live uint64, rebased bool,
) bool {
	entry, ok := epoch.reserveTileEntry(tileID)
	if !ok {
		return false
	}
	record, ok := epoch.allocTileRecord()
	if !ok {
		return false
	}
	next := entry.head.Load()
	for i := range prepared.tileLinks {
		if prepared.tileLinks[i].entry == entry {
			next = prepared.tileLinks[i].head
			break
		}
	}
	*record = primaryExactTileRecord{
		next: next, gen: gen, live: live, tileID: tileID, rebased: rebased,
	}
	for i := range prepared.tileLinks {
		if prepared.tileLinks[i].entry == entry {
			prepared.tileLinks[i].head = record
			prepared.tileRecordsAdded++
			return true
		}
	}
	prepared.tileLinks = append(prepared.tileLinks, primaryExactTileLink{
		entry: entry, head: record,
	})
	prepared.tileRecordsAdded++
	return true
}

// resolvePrimaryExactTermTileBits is the writer's probe-shaped read of the
// current absolute bits for (index, term, tile): newest overlay record for
// the tile if any (a record older than the tile's newest rebase is void),
// else the fold base unless a rebase voided it. The spanned base routes by
// one router binary search on (term, tile) — for a striped giant term that
// lands directly on the stripe piece owning the tile — then seeks that
// leaf's ascending mask iterator, so its cost is O(one leaf's postings
// before the touched tile), corpus-size-independent by construction.
func (c *Collection) resolvePrimaryExactTermTileBits(
	epoch *primaryExactEpoch,
	indexID uint32,
	keyRecord storeio.IndexTermKeyRecord,
	chainHash uint64,
	tileID uint32,
) (uint64, error) {
	rebase := epoch.rebaseFloor(tileID, ^uint64(0))
	if entry := epoch.lookupTermEntry(
		chainHash, indexID, keyRecord.Canonical,
	); entry != nil {
		for record := entry.head.Load(); record != nil; record = record.next {
			if record.tileID != tileID {
				continue
			}
			if record.gen < rebase {
				return 0, nil
			}
			return record.bits, nil
		}
	}
	if rebase != 0 {
		// The rebase group re-recorded every surviving term absolutely; no
		// record at or above it means "no bits" and the base is void.
		return 0, nil
	}
	leaves := epoch.exact[indexID].leaves
	if len(leaves) == 0 {
		return 0, nil
	}
	at := primaryExactLeafAt(leaves, keyRecord.Canonical, tileID)
	if at < 0 {
		return 0, nil
	}
	match, found := leaves[at].view.LookupRecord(keyRecord)
	if !found {
		return 0, nil
	}
	iterator := match.MaskIterator()
	for {
		tile, mask, more := iterator.Next()
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

// preparePrimaryExactRebase emits the rebase group for a slot-reassigning
// leaf rewrite: 4 rebased tile records covering the bucket's quadrants
// (empty quadrants included — the rebase is what voids the base's postings
// and liveness for them) plus absolute term records for every term present
// in the rewritten bucket, all at one generation. ok=false reports overlay
// pressure with the prepared records already unwound by the caller.
func (c *Collection) preparePrimaryExactRebase(
	epoch *primaryExactEpoch, prepared *primaryExactPrepared,
	leafImage []byte, bucket storeio.BucketID,
	generation uint64,
	bounds storeio.CommonPrimaryLeafBounds,
) (ok bool, err error) {
	bucketLive, byIndex, err := c.deriveBucketExactContribution(
		leafImage, bucket, bounds,
	)
	if err != nil {
		return false, err
	}
	for quadrant := uint32(0); quadrant < 4; quadrant++ {
		tileID := uint32(bucket)<<2 | quadrant
		if !c.emitPrimaryExactTileRecord(
			epoch, prepared, tileID, generation, bucketLive[tileID], true,
		) {
			return false, nil
		}
	}
	for indexID := range byIndex {
		for term, tiles := range byIndex[indexID] {
			termBytes := []byte(term)
			chainHash := primaryExactTermChainHash(
				storeio.IndexTermRouteHash(c.storeID, termBytes),
				uint32(indexID),
			)
			for tileID, bits := range tiles {
				if !c.emitPrimaryExactTermRecord(
					epoch, prepared, chainHash, uint32(indexID), termBytes,
					tileID, generation, bits,
				) {
					return false, nil
				}
			}
		}
	}
	return true, nil
}

// preparePrimaryExactBufferedMutation rebases the exact contribution after the
// exceptional structural lane has placed an owned raw workspace back into
// compact VCS1. Steady-state VCS1 overlay mutations use
// preparePrimaryExactUnifiedMutation instead.
func (c *Collection) preparePrimaryExactBufferedMutation(
	leafImage []byte,
	resident storeio.ResidentPrimaryRoute,
	generation uint64,
	newBounds storeio.CommonPrimaryLeafBounds,
) (primaryExactPrepared, error) {
	epoch := c.primaryEpoch
	if epoch == nil {
		return primaryExactPrepared{}, nil
	}
	prepared := primaryExactPrepared{
		active:         true,
		gen:            generation,
		termLinks:      c.exactTermLinkScratch[:0],
		tileLinks:      c.exactTileLinkScratch[:0],
		termRecordMark: epoch.termRecordN,
		tileRecordMark: epoch.tileRecordN,
	}
	// The compact VCS1 leaf was rendered into an owned raw workspace and placed
	// wholesale, so no pre-image slot can be assumed to map to the new image.
	// Re-derive the bucket instead of attempting a slot delta.
	ok, err := c.preparePrimaryExactRebase(
		epoch, &prepared, leafImage, resident.Bucket, generation, newBounds,
	)
	if err != nil {
		c.unwindPrimaryExactPrepared(&prepared)
		return primaryExactPrepared{}, err
	}
	pressure := !ok
	if !pressure {
		return prepared, nil
	}
	// Overlay pressure: the window (or this one rebase group) does not fit
	// the arena. Escalate to a resident fold — resolve base+overlay through
	// the read rule with this bucket re-derived fresh, encode, and install a
	// fresh epoch whose empty overlay has full capacity. This is the
	// ensureBufferedPrimaryMutationCapacity escalation discipline applied at
	// the fold layer: never resize the tables readers are probing; swap the
	// whole epoch instead. O(index) once, then the window restarts.
	c.unwindPrimaryExactPrepared(&prepared)
	bucketLive, byIndex, err := c.deriveBucketExactContribution(
		leafImage, resident.Bucket, newBounds,
	)
	if err != nil {
		return primaryExactPrepared{}, err
	}
	bucket := resident.Bucket
	return c.prepareFoldedPrimaryExact(
		^uint64(0), generation,
		func(b storeio.BucketID) bool { return b == bucket },
		&primaryExactBucketOverride{
			bucket: bucket, live: bucketLive, byIndex: byIndex,
		},
		nil, nil,
	)
}

// preparePrimaryExactDeltaRaw is the representation-independent exact-index
// write rule. oldRaw is the logical pre-image already resolved by the primary
// mutation lane; compact VCS1 can therefore share the same slot-stable posting
// overlay without de-templating or synthesizing a raw leaf view.
func (c *Collection) preparePrimaryExactDeltaRaw(
	epoch *primaryExactEpoch, prepared *primaryExactPrepared,
	resident storeio.ResidentPrimaryRoute,
	oldRaw, src []byte,
	deleting, found bool,
	oldSlot, newSlot uint8,
	generation uint64,
) (ok bool, err error) {
	indexes := c.options.indexes
	var oldComponents [store.MaxIndexColumns]storeio.IndexTermComponent
	var newComponents [store.MaxIndexColumns]storeio.IndexTermComponent
	var oldCanonical [storeio.IndexTermMaxKeyBytes]byte
	var newCanonical [storeio.IndexTermMaxKeyBytes]byte

	slot := oldSlot
	if !found {
		slot = newSlot
	}
	tileID := uint32(resident.Bucket)<<2 | uint32(slot>>6)
	bit := uint64(1) << uint(slot&63)

	emitTerm := func(
		indexID int, term []byte, add bool,
	) (bool, error) {
		route := storeio.IndexTermRouteHash(c.storeID, term)
		chainHash := primaryExactTermChainHash(route, uint32(indexID))
		bits, resolveErr := c.resolvePrimaryExactTermTileBits(
			epoch, uint32(indexID),
			storeio.IndexTermKeyRecord{RouteHash: route, Canonical: term},
			chainHash, tileID,
		)
		if resolveErr != nil {
			return false, resolveErr
		}
		if add {
			bits |= bit
		} else {
			bits &^= bit
		}
		return c.emitPrimaryExactTermRecord(
			epoch, prepared, chainHash, uint32(indexID), term,
			tileID, generation, bits,
		), nil
	}

	for indexID, exact := range indexes {
		var oldTerm, newTerm []byte
		oldPresent, newPresent := false, false
		if found {
			oldTerm, oldPresent, err = appendPrimaryExactDocumentTerm(
				oldCanonical[:0], oldComponents[:], exact, oldRaw,
			)
			if err != nil {
				return false, err
			}
		}
		if !deleting {
			newTerm, newPresent, err = appendPrimaryExactDocumentTerm(
				newCanonical[:0], newComponents[:], exact, src,
			)
			if err != nil {
				return false, err
			}
		}
		if oldPresent && newPresent && bytes.Equal(oldTerm, newTerm) {
			// The dominant case: same slot, same term — the index is
			// untouched and this mutation writes nothing for it.
			continue
		}
		if oldPresent {
			ok, err = emitTerm(indexID, oldTerm, false)
			if err != nil || !ok {
				return ok, err
			}
		}
		if newPresent {
			ok, err = emitTerm(indexID, newTerm, true)
			if err != nil || !ok {
				return ok, err
			}
		}
	}
	if !found || deleting {
		// Insert and delete move a live bit; a slot-stable update does not.
		live := epoch.resolveLiveWord(tileID, ^uint64(0), true)
		if deleting {
			live &^= bit
		} else {
			live |= bit
		}
		if !c.emitPrimaryExactTileRecord(
			epoch, prepared, tileID, generation, live, false,
		) {
			return false, nil
		}
	}
	return true, nil
}

// preparePrimaryExactUnifiedMutation stages the exact-index half of one
// VCS1 overlay mutation. Both overlays install inside the same snapshot
// fence. pressure leaves neither half published and asks the caller to fold the
// current window before retrying.
func (c *Collection) preparePrimaryExactUnifiedMutation(
	resident storeio.ResidentPrimaryRoute,
	oldRaw, src []byte,
	deleting, found bool,
	oldSlot, newSlot uint8,
	generation uint64,
) (prepared primaryExactPrepared, pressure bool, err error) {
	epoch := c.primaryEpoch
	if epoch == nil {
		return primaryExactPrepared{}, false, nil
	}
	prepared = primaryExactPrepared{
		active:         true,
		gen:            generation,
		termLinks:      c.exactTermLinkScratch[:0],
		tileLinks:      c.exactTileLinkScratch[:0],
		termRecordMark: epoch.termRecordN,
		tileRecordMark: epoch.tileRecordN,
	}
	ok, err := c.preparePrimaryExactDeltaRaw(
		epoch, &prepared, resident, oldRaw, src,
		deleting, found, oldSlot, newSlot, generation,
	)
	if err != nil {
		c.unwindPrimaryExactPrepared(&prepared)
		return primaryExactPrepared{}, false, err
	}
	if !ok {
		c.unwindPrimaryExactPrepared(&prepared)
		return primaryExactPrepared{}, true, nil
	}
	return prepared, false, nil
}

// preparePrimaryExactLeaf is the fold-first exceptional structural lane. Its
// owned workspace placement may reassign every stable slot, so it derives the
// complete bucket contribution and publishes a fresh folded epoch atomically
// with the rewritten compact VCS1 leaf.
func (c *Collection) preparePrimaryExactLeaf(
	leafImage []byte,
	resident storeio.ResidentPrimaryRoute,
	generation uint64,
	bounds storeio.CommonPrimaryLeafBounds,
) (primaryExactPrepared, error) {
	if !c.primaryExactActive() {
		return primaryExactPrepared{}, nil
	}
	bucketLive, byIndex, err := c.deriveBucketExactContribution(
		leafImage, resident.Bucket, bounds,
	)
	if err != nil {
		return primaryExactPrepared{}, err
	}
	bucket := resident.Bucket
	return c.prepareFoldedPrimaryExact(
		^uint64(0), generation,
		func(b storeio.BucketID) bool { return b == bucket },
		&primaryExactBucketOverride{
			bucket: bucket, live: bucketLive, byIndex: byIndex,
		},
		nil, nil,
	)
}

// structuralBucketContribution is one leaf's exact-index contribution captured
// while a structural transaction re-encodes it: the bucket's new live tile masks
// and, per physical index, its new canonical term -> tile -> bits.
type structuralBucketContribution struct {
	live    map[uint32]uint64
	byIndex []map[string]map[uint32]uint64
}

// resetStructuralExactLocked clears the structural posting accumulators at the
// start of one bounded structural transaction.
func (c *Collection) resetStructuralExactLocked() {
	c.structuralExactReencoded = nil
	c.structuralExactRemoved = c.structuralExactRemoved[:0]
}

// accumulateStructuralLeafLocked records one re-encoded leaf's exact-index
// contribution so prepareStructuralExactLocked can rebuild the affected postings
// atomically with the tablet. leafPage is the freshly encoded leaf image.
func (c *Collection) accumulateStructuralLeafLocked(
	bucket storeio.BucketID, leafPage []byte,
	bounds storeio.CommonPrimaryLeafBounds,
) error {
	if !c.primaryExactActive() {
		return nil
	}
	bucketLive, byIndex, err := c.deriveBucketExactContribution(
		leafPage, bucket, bounds,
	)
	if err != nil {
		return err
	}
	if c.structuralExactReencoded == nil {
		c.structuralExactReencoded =
			make(map[storeio.BucketID]*structuralBucketContribution)
	}
	c.structuralExactReencoded[bucket] = &structuralBucketContribution{
		live: bucketLive, byIndex: byIndex,
	}
	return nil
}

// recordStructuralRemovedBucketLocked marks a bucket removed by a structural
// transaction (a merged-away or emptied leaf), so its postings are dropped.
func (c *Collection) recordStructuralRemovedBucketLocked(
	bucket storeio.BucketID,
) {
	c.structuralExactRemoved = append(c.structuralExactRemoved, bucket)
}

// prepareStructuralExactLocked folds the structural accumulators into a
// fresh epoch: it drops the tiles of every re-encoded or removed bucket,
// resolves every untouched tile through the read rule, and merges the
// re-encoded contributions, so it is correct whether a tablet rebuild
// reassigned slots, added a leaf, or removed one. Structural transactions
// fold first (flushPendingForStructural checkpoints the deferred lanes), so
// the overlay it resolves is empty in practice. Returns active=false for a
// collection without exact indexes.
func (c *Collection) prepareStructuralExactLocked(
	generation uint64,
) (primaryExactPrepared, error) {
	if !c.primaryExactActive() {
		return primaryExactPrepared{}, nil
	}
	affected := make(map[storeio.BucketID]bool)
	for bucket := range c.structuralExactReencoded {
		affected[bucket] = true
	}
	for _, bucket := range c.structuralExactRemoved {
		affected[bucket] = true
	}
	return c.prepareFoldedPrimaryExact(
		^uint64(0), generation,
		func(b storeio.BucketID) bool { return affected[b] },
		nil,
		c.structuralExactReencoded,
		nil,
	)
}

// stagePrimaryExactPagesLocked persists the exact indexes inside tx and
// returns the new PagePrimaryExactRoot ref: it writes a durable page for
// every staged leaf that does not already have one (carried leaves keep the
// page their ref names — the O(dirty leaves) half of the fold), rebuilds
// each index's ordered catalog and root (both small), and retires
// exactly the superseded pages: the old root, the old catalog pages, and
// the old pages of leaves the fold replaced or dropped. It stages nothing
// and returns a zero ref for a collection without exact indexes.
//
// exact is the resident term-leaf set to persist: a freshly folded epoch's
// base for a per-mutation transaction, structural transaction, or dirty
// checkpoint, or the current epoch's base for a quiet checkpoint (whose
// leaves all carry refs, so only root+catalog pages are written). Encoded
// bytes are already canonical; this only wraps them in page envelopes.
// Fresh page refs are recorded on the staged leaves, so the epoch installed
// at publish carries the durable identity of every leaf.
func (c *Collection) stagePrimaryExactPagesLocked(
	tx *storeio.WriteTransaction,
	state *fileStoreState,
	generation uint64,
	exact []primaryExactResident,
) (storeio.PageRef, error) {
	if len(exact) == 0 {
		return storeio.PageRef{}, nil
	}
	pageSize := uint32(c.options.PageSize)
	maxPageSize := uint32(c.options.MaxPageSize)

	// Retire superseded pages against the currently installed epoch (the
	// fold's input). Carried leaves preserve their relative order, so one
	// forward scan of the staged refs decides which old pages survive.
	if state.root.ExactIndexRoot != (storeio.PageRef{}) {
		old := c.primaryEpoch.exact
		for indexID := range old {
			var staged []primaryExactLeaf
			if indexID < len(exact) {
				staged = exact[indexID].leaves
			}
			// At this point fresh leaves still carry zero refs (their pages
			// are staged below), so a staged leaf with a ref IS a carried
			// leaf, and carried leaves preserve the old relative order: the
			// carried refs form an in-order subsequence of the old refs.
			si := 0
			for at := range old[indexID].leaves {
				ref := old[indexID].leaves[at].ref
				if ref == (storeio.PageRef{}) {
					continue
				}
				for si < len(staged) && staged[si].ref == (storeio.PageRef{}) {
					si++
				}
				if si < len(staged) && staged[si].ref == ref {
					si++
					continue // carried: the page survives
				}
				if err := c.appendPrimaryRetirement(state, ref); err != nil {
					return storeio.PageRef{}, err
				}
			}
			for _, ref := range old[indexID].catalog {
				if err := c.appendPrimaryRetirement(state, ref); err != nil {
					return storeio.PageRef{}, err
				}
			}
		}
		if err := c.appendPrimaryRetirement(
			state, state.root.ExactIndexRoot,
		); err != nil {
			return storeio.PageRef{}, err
		}
	}

	rootEntries := make([]storeio.PrimaryExactRootEntry, len(exact))
	stagedScratch := make([]primaryExactStagedLeaf, 0, 16)
	for indexID := range exact {
		resident := &exact[indexID]
		if !resident.present() {
			resident.catalog = resident.catalog[:0]
			continue
		}
		staged := stagedScratch[:0]
		for at := range resident.leaves {
			leaf := &resident.leaves[at]
			if leaf.ref == (storeio.PageRef{}) {
				ref, err := stagePrimaryExactLeafPage(
					tx, leaf.encoded, pageSize, maxPageSize,
				)
				if err != nil {
					return storeio.PageRef{}, err
				}
				leaf.ref = ref
			}
			staged = append(staged, primaryExactStagedLeaf{
				ref:       leaf.ref,
				firstKey:  leaf.firstKey,
				firstTile: leaf.firstTile,
				piece:     leaf.piece,
				runCut:    leaf.runCut,
			})
		}
		stagedScratch = staged[:0]
		catalogRef, pages, err := stagePrimaryExactCatalog(
			tx, pageSize, maxPageSize, staged, resident.catalog[:0],
		)
		if err != nil {
			return storeio.PageRef{}, err
		}
		resident.catalog = pages
		rootEntries[indexID] = storeio.PrimaryExactRootEntry{
			Catalog: catalogRef, LeafCount: uint32(len(staged)),
		}
	}
	rootPage, err := tx.Allocate(storeio.PagePrimaryExactRoot, pageSize, 0)
	if err != nil {
		return storeio.PageRef{}, err
	}
	if _, err := storeio.EncodePrimaryExactRootPage(
		rootPage.Bytes(), c.storeID, generation, rootPage.Ref().LogicalID,
		rootEntries,
	); err != nil {
		return storeio.PageRef{}, err
	}
	if err := rootPage.Stage(); err != nil {
		return storeio.PageRef{}, err
	}
	return rootPage.Ref(), nil
}

// clonePrimaryExactResidentsForStaging makes a metadata-private copy of an
// installed epoch's fold base. Encoded bytes, admitted views, and first keys
// remain immutable and may be shared; leaf refs and catalog slices must not be
// shared because staging replaces them before publication can still fail.
func clonePrimaryExactResidentsForStaging(
	src []primaryExactResident,
) []primaryExactResident {
	out := make([]primaryExactResident, len(src))
	for indexID := range src {
		out[indexID].leaves = append(
			out[indexID].leaves, src[indexID].leaves...,
		)
		out[indexID].catalog = append(
			out[indexID].catalog, src[indexID].catalog...,
		)
	}
	return out
}

// installPrimaryExactDurableMetadata copies page identities from a successfully
// published staging copy into the installed epoch. Readers use only the
// immutable encoded/view routing fields, so this writer-gated update changes no
// query-visible state.
func installPrimaryExactDurableMetadata(
	dst, src []primaryExactResident,
) {
	if len(dst) != len(src) {
		return
	}
	for indexID := range src {
		if len(dst[indexID].leaves) != len(src[indexID].leaves) {
			return
		}
		for leafAt := range src[indexID].leaves {
			dst[indexID].leaves[leafAt].ref =
				src[indexID].leaves[leafAt].ref
		}
		dst[indexID].catalog = append(
			dst[indexID].catalog[:0], src[indexID].catalog...,
		)
	}
}
