package durable

import (
	"bytes"
	"fmt"
	"math/bits"
	"slices"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// Exact-index posting maintenance for the ordered primary graph
// (docs/design/indexed-write-path.md §3).
//
// A mutation rewrites exactly one leaf, and a leaf owns exactly the four
// quadrant posting tiles bucket<<2 | q (q in 0..3). Every posting bit for a
// row is a deterministic function of that row's stable slot in its leaf, so
// the whole exact-index effect of a mutation is confined to those tiles.
//
// The buffered write rule appends O(touched terms) overlay records instead of
// re-encoding the resident term leaves (the removed per-mutation
// reconstruct/encode/re-admit was the measured 4,954 µs, ~3,100-alloc cliff —
// O(one physical index's postings) per Put at the 10k benchmark shape):
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
//   - slot-reassigning leaf rewrite (de-template of a template or compact
//     leaf — PlaceCommonPrimaryLeafRecords reassigns every slot): fall back
//     to deriveBucketExactContribution (O(leaf rows), exactly the old cost,
//     once per leaf-class transition) and emit a rebase group — 4 rebased
//     tile records plus absolute term records for every term present in the
//     bucket, all at one generation.
//
// The canonical per-mutation-transaction lane and structural transactions
// keep their fold-first shape (§7): they resolve base+overlay through the
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
		FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID,
		AllocationQuantum: state.root.PageSize,
		MaxPageSize:       state.root.MaxPageSize, IndexCount: state.root.IndexCount,
	}
}

// deriveBucketExactContribution visits the live rows of the rewritten leaf image
// and returns, for the four tiles of bucket, the new live slot mask and, per
// physical index, the canonical term -> tile -> bits the bucket now contributes.
// It is fold-side and rebase-side code — the common buffered mutation never
// calls it (§3's write rule).
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
	var scratch []byte
	scratch, err = storeio.VisitPrimaryLeafPostingRows(
		leafPage, c.storeID, bucket, bounds, scratch,
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

// encodeIndexTermLeaf encodes one physical index's canonical term map into an
// IndexTermLeaf byte stream, mirroring buildPrimaryExactIndexes exactly so the
// output is byte-identical to a bulk build of the same postings. It returns nil
// bytes for an empty physical index. It is the fold's identity anchor: every
// resident and durable term leaf in this file passes through it.
func (c *Collection) encodeIndexTermLeaf(
	terms map[string]map[uint32]uint64,
	live map[uint32]*[storeio.TermPostingTileChunks]uint64,
) ([]byte, error) {
	if len(terms) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(terms))
	for key := range terms {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	leafTerms := make([]storeio.IndexTermLeafTerm, len(keys))
	for termAt, key := range keys {
		record, ok := storeio.OpenIndexTermKeyRecord(c.storeID, []byte(key))
		if !ok {
			return nil, fmt.Errorf(
				"%w: canonical exact term", storeio.ErrInvalidWrite,
			)
		}
		tileMap := terms[key]
		tiles := make([]uint32, 0, len(tileMap))
		for tileID := range tileMap {
			tiles = append(tiles, tileID)
		}
		slices.Sort(tiles)
		postings := make([]storeio.IndexTermLeafPosting, len(tiles))
		for postingAt, tileID := range tiles {
			maskBits := tileMap[tileID]
			liveMask := live[tileID]
			if liveMask == nil {
				return nil, storeio.ErrPrimaryExactIndexCorrupt
			}
			// A chunk-0-only posting always encodes through the leaf's
			// direct representations, which never consume the adaptive
			// codec's record — the synthetic (tile, rows) record plus the
			// mask is the builder's complete input and skips codec
			// selection (see prepareStreamedPrimaryExactFold's emit).
			postings[postingAt] = storeio.IndexTermLeafPosting{
				Posting: storeio.TermPosting{
					TileID: tileID,
					Rows:   uint16(bits.OnesCount64(maskBits)),
				},
				Live:       liveMask,
				Chunk0Bits: maskBits, Chunk0Only: true,
			}
		}
		leafTerms[termAt] = storeio.IndexTermLeafTerm{
			Key: record, Postings: postings,
		}
	}
	return storeio.AppendIndexTermLeaf(nil, c.storeID, leafTerms)
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
// else the fold base unless a rebase voided it. The base branch seeks the
// term's ascending mask iterator and exits at the first tile past the
// target, so its cost is O(the term's postings before the touched tile) —
// the honest price of the unchanged monolithic leaf codec in P0, paid only
// on value-changed updates, inserts, and deletes of indexed terms.
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
	resident := &epoch.exact[indexID]
	if !resident.present {
		return 0, nil
	}
	match, found := resident.view.LookupRecord(keyRecord)
	if !found {
		return 0, nil
	}
	iterator := match.MaskIterator()
	for {
		at, mask, more := iterator.Next()
		if !more || at > tileID {
			return 0, nil
		}
		if at == tileID {
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

// preparePrimaryExactBufferedMutation is §3's write rule for the buffered
// canonical-frame lane: it prepares the overlay records for one mutation
// without touching the fold base. Zero heap allocations on the steady-state
// shapes (unchanged-value update, value change, insert, delete); the rebase
// and pressure escapes may allocate, bounded to leaf-class transitions and
// window exhaustion.
func (c *Collection) preparePrimaryExactBufferedMutation(
	state *fileStoreState,
	leaf *storeio.CommonPrimaryLeafView,
	leafImage []byte,
	resident storeio.ResidentPrimaryRoute,
	key, src []byte,
	deleting, found, detemplated bool,
	oldSlot, newSlot uint8,
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
	// A de-templated (template or compact) leaf was re-placed wholesale:
	// PlaceCommonPrimaryLeafRecords assigned every row a fresh slot, so no
	// pre-image slot maps to the new image. An update whose prepared slot
	// moved is the same condition caught defensively — the identity tests
	// compare against a from-scratch rebuild byte-for-byte, so a missed
	// reassignment class fails loudly there, and this branch fails safe by
	// re-deriving the whole bucket.
	rebase := detemplated || (found && !deleting && newSlot != oldSlot)
	pressure := false
	if rebase {
		ok, err := c.preparePrimaryExactRebase(
			epoch, &prepared, leafImage, resident.Bucket, generation, newBounds,
		)
		if err != nil {
			c.unwindPrimaryExactPrepared(&prepared)
			return primaryExactPrepared{}, err
		}
		pressure = !ok
	} else {
		ok, err := c.preparePrimaryExactDelta(
			state, epoch, &prepared, leaf, resident,
			key, src, deleting, found, oldSlot, newSlot, generation,
		)
		if err != nil {
			c.unwindPrimaryExactPrepared(&prepared)
			return primaryExactPrepared{}, err
		}
		pressure = !ok
	}
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
		nil,
	)
}

// preparePrimaryExactDelta emits the slot-stable record set: zero records
// for an unchanged indexed value, ≤ 2 term records per index for a changed
// one, term + tile records for insert and delete. ok=false is overlay
// pressure.
func (c *Collection) preparePrimaryExactDelta(
	state *fileStoreState,
	epoch *primaryExactEpoch, prepared *primaryExactPrepared,
	leaf *storeio.CommonPrimaryLeafView,
	resident storeio.ResidentPrimaryRoute,
	key, src []byte,
	deleting, found bool,
	oldSlot, newSlot uint8,
	generation uint64,
) (ok bool, err error) {
	indexes := c.options.indexes
	// The pre-image: the mutation's own leaf lookup already proved presence;
	// re-reading it here borrows the resident page for the duration of the
	// prepare only. An out-of-line pre-image resolves through the shared
	// overflow reader against the published bounds it was minted under.
	var oldRaw []byte
	if found {
		_, raw, overflow, present := leaf.LookupRawHashed(resident.Hash, key)
		if !present {
			return false, storeio.ErrCommonPrimaryLeafCorrupt
		}
		if overflow {
			resolved, resolveErr := c.appendPrimaryOverflowValue(
				c.overflowValueScratch[:0],
				storeio.DecodePrimaryOverflowRef(raw),
				c.primaryLeafBounds(state),
			)
			if resolveErr != nil {
				return false, resolveErr
			}
			c.overflowValueScratch = resolved
			raw = resolved
		}
		oldRaw = raw
	}

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

// preparePrimaryExactLeaf is the fold-first lane used by the canonical
// per-mutation-transaction path (§7): it re-derives bucket's contribution
// from the rewritten leaf image, resolves everything else through the read
// rule at the current head, and prepares a complete fresh epoch whose
// encoded leaves the same transaction stages durably. P0 keeps this lane's
// whole-leaf re-encode cost — that lane already pays a device sync per
// mutation. The overlay it resolves is empty in practice: every
// record-emitting buffered mutation registers a pending parent, and the
// transactional path materializes pending parents (a fold) before entering.
func (c *Collection) preparePrimaryExactLeaf(
	leafPage []byte, bucket storeio.BucketID,
	bounds storeio.CommonPrimaryLeafBounds,
	generation uint64,
) (primaryExactPrepared, error) {
	if !c.primaryExactActive() {
		return primaryExactPrepared{}, nil
	}
	bucketLive, byIndex, err := c.deriveBucketExactContribution(
		leafPage, bucket, bounds,
	)
	if err != nil {
		return primaryExactPrepared{}, err
	}
	return c.prepareFoldedPrimaryExact(
		^uint64(0), generation,
		func(b storeio.BucketID) bool { return b == bucket },
		&primaryExactBucketOverride{
			bucket: bucket, live: bucketLive, byIndex: byIndex,
		},
		nil,
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
	)
}

// stagePrimaryExactPagesLocked writes exact indexes as fresh durable pages
// inside tx and returns the new PagePrimaryExactRoot ref. Every present physical
// index is written in ascending index order (the reference catalog requires
// strictly ascending leaf logical ids), the superseded leaves and root named by
// state.root.ExactIndexRoot are retired through c.retireScratch, and an empty
// physical index keeps a zero leaf ref. It stages nothing and returns a zero ref
// for a collection without exact indexes.
//
// exact is the resident term-leaf set to persist: a freshly folded epoch's
// base for a per-mutation transaction, structural transaction, or dirty
// checkpoint, or the current epoch's base for a quiet checkpoint. Its
// encoded bytes are already canonical, so this only wraps them in page
// envelopes; it does not rebuild postings from the graph.
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
	if state.root.ExactIndexRoot != (storeio.PageRef{}) {
		lease, err := c.cache.Acquire(state.root.ExactIndexRoot)
		if err != nil {
			return storeio.PageRef{}, err
		}
		root, openErr := storeio.OpenPrimaryExactRootPage(
			lease.Page(), state.root.ExactIndexRoot, c.primaryExactBounds(state),
		)
		if openErr == nil {
			for indexID := 0; indexID < root.Len(); indexID++ {
				ref, ok := root.Leaf(uint32(indexID))
				if !ok {
					openErr = storeio.ErrPrimaryExactIndexCorrupt
					break
				}
				if ref != (storeio.PageRef{}) {
					if appendErr := c.appendPrimaryRetirement(
						state, ref,
					); appendErr != nil {
						openErr = appendErr
						break
					}
				}
			}
		}
		lease.Release()
		if openErr != nil {
			return storeio.PageRef{}, openErr
		}
	}
	leafRefs := make([]storeio.PageRef, len(exact))
	for indexID := range exact {
		if !exact[indexID].present {
			continue
		}
		encoded := exact[indexID].encoded
		extent, ok := primaryExactExtent(
			len(encoded)+storeio.PageHeaderSize+storeio.PageTrailerSize,
			pageSize, maxPageSize,
		)
		if !ok {
			return storeio.PageRef{}, fmt.Errorf(
				"%w: ordered-primary exact term leaf exceeds MaxPageSize",
				ErrPrimaryCutoverUnsupported,
			)
		}
		page, err := tx.Allocate(storeio.PagePrimaryExactLeaf, extent, 0)
		if err != nil {
			return storeio.PageRef{}, err
		}
		if _, err := storeio.EncodePrimaryExactLeafPage(
			page.Bytes(), c.storeID, generation, page.Ref().LogicalID, encoded,
		); err != nil {
			return storeio.PageRef{}, err
		}
		if err := page.Stage(); err != nil {
			return storeio.PageRef{}, err
		}
		leafRefs[indexID] = page.Ref()
	}
	rootPage, err := tx.Allocate(storeio.PagePrimaryExactRoot, pageSize, 0)
	if err != nil {
		return storeio.PageRef{}, err
	}
	if _, err := storeio.EncodePrimaryExactRootPage(
		rootPage.Bytes(), c.storeID, generation, rootPage.Ref().LogicalID,
		leafRefs,
	); err != nil {
		return storeio.PageRef{}, err
	}
	if err := rootPage.Stage(); err != nil {
		return storeio.PageRef{}, err
	}
	if state.root.ExactIndexRoot != (storeio.PageRef{}) {
		if err := c.appendPrimaryRetirement(
			state, state.root.ExactIndexRoot,
		); err != nil {
			return storeio.PageRef{}, err
		}
	}
	return rootPage.Ref(), nil
}
