package durable

import (
	"fmt"
	"math/bits"
	"slices"

	vibejson "github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// primaryExactTermPostings maps a posting tile ID to its live slot mask (chunk
// 0 only; each tile is one 64-slot bucket quadrant) while building one term.
type primaryExactTermPostings map[uint32]uint64

// primaryExactResident is one physical exact index resident for reads. encoded
// is the canonical IndexTermLeaf byte stream copied out of its page; view
// borrows encoded and the collection's live masks.
type primaryExactResident struct {
	encoded []byte
	view    storeio.IndexTermLeafView
	present bool
}

// primaryExactActive reports whether this collection carries ordered-primary
// exact indexes, i.e. postings live on the graph rather than the chunk store.
func (c *Collection) primaryExactActive() bool {
	return c.primaryEpoch != nil
}

// derivePrimaryLive rebuilds the posting live-slot map from the current resident
// primary graph: for every live row of every leaf, the tile ID is
// bucket<<2|(slot>>6) and the live bit is 1<<(slot&63) in chunk 0. It is the
// authority the read-side posting recheck validates against, so it is recomputed
// from the graph whenever the graph or the exact index is (re)opened.
func (c *Collection) derivePrimaryLive(
	state *fileStoreState,
) (map[uint32]*[storeio.TermPostingTileChunks]uint64, error) {
	router := c.primaryRouter.Load()
	if router == nil {
		return nil, storeio.ErrPrimaryExactIndexCorrupt
	}
	live := make(
		map[uint32]*[storeio.TermPostingTileChunks]uint64, router.Len()*4,
	)
	bounds := c.primaryLeafBounds(state)
	var scratch []byte
	for rank := 0; rank < router.Len(); rank++ {
		route, ok := router.RouteAtRank(rank)
		if !ok {
			return nil, storeio.ErrPrimaryExactIndexCorrupt
		}
		lease, err := router.AcquireLeaf(c.cache, route)
		if err != nil {
			return nil, err
		}
		scratch, err = storeio.VisitPrimaryLeafPostingRows(
			lease.Page(), state.root.StoreID, route.Bucket, bounds, scratch,
			func(slot uint8, _, _ []byte, _ bool) error {
				tileID := uint32(route.Bucket)<<2 | uint32(slot>>6)
				mask := live[tileID]
				if mask == nil {
					mask = new([storeio.TermPostingTileChunks]uint64)
					live[tileID] = mask
				}
				mask[0] |= uint64(1) << uint(slot&63)
				return nil
			},
		)
		lease.Release()
		if err != nil {
			return nil, err
		}
	}
	return live, nil
}

// openPrimaryExactIndexes materializes the resident index epoch from the
// published ExactIndexRoot: the fold base (encoded term leaves plus the flat
// live table derived from the graph) under an empty overlay. It is called
// once by Open for an indexed ordered-primary collection.
func (c *Collection) openPrimaryExactIndexes(state *fileStoreState) error {
	if state.root.ExactIndexRoot == (storeio.PageRef{}) {
		return nil
	}
	bounds := storeio.PrimaryExactIndexBounds{
		StoreID: state.root.StoreID, Generation: state.root.Generation,
		FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID,
		AllocationQuantum: state.root.PageSize,
		MaxPageSize:       state.root.MaxPageSize, IndexCount: state.root.IndexCount,
	}
	live, err := c.derivePrimaryLive(state)
	if err != nil {
		return err
	}
	rootLease, err := c.cache.Acquire(state.root.ExactIndexRoot)
	if err != nil {
		return err
	}
	root, err := storeio.OpenPrimaryExactRootPage(
		rootLease.Page(), state.root.ExactIndexRoot, bounds,
	)
	rootLease.Release()
	if err != nil {
		return err
	}
	epoch := newPrimaryExactEpoch(root.Len())
	epoch.live = newPrimaryLiveTable(live)
	epoch.baseGen = state.root.Generation
	// Views close over this epoch's flat table, not the collection field: a
	// fold installs a fresh epoch by swapping the pointer, and a snapshot
	// that captured this one must keep reading the table its views were
	// admitted against.
	liveLookup := epoch.live.lookup
	for indexID := 0; indexID < root.Len(); indexID++ {
		ref, ok := root.Leaf(uint32(indexID))
		if !ok {
			return storeio.ErrPrimaryExactIndexCorrupt
		}
		if ref == (storeio.PageRef{}) {
			continue
		}
		lease, acquireErr := c.cache.Acquire(ref)
		if acquireErr != nil {
			return acquireErr
		}
		payload, openErr := storeio.OpenPrimaryExactLeafPage(
			lease.Page(), ref, bounds,
		)
		if openErr == nil {
			epoch.exact[indexID].encoded = append([]byte(nil), payload...)
		}
		lease.Release()
		if openErr != nil {
			return openErr
		}
		view, viewErr := storeio.OpenIndexTermLeaf(
			epoch.exact[indexID].encoded, state.root.StoreID, liveLookup,
		)
		if viewErr != nil {
			return viewErr
		}
		epoch.exact[indexID].view = view
		epoch.exact[indexID].present = true
	}
	c.primaryEpoch = epoch
	// Retire/pool lists are bounded by fold cadence against the reclaim
	// floor; pre-sizing them keeps the publish-gate append allocation-free.
	c.primaryEpochRetired = make([]retiredPrimaryExactEpoch, 0, 8)
	c.primaryEpochPool = make([]*primaryExactEpoch, 0, 8)
	return nil
}

// appendPrimaryExactMasks is the exact-match read path for an ordered-primary
// index: it canonicalizes the needle term and resolves it through the pinned
// index epoch's read rule (docs/design/indexed-write-path.md §3) at the
// snapshot's generation G — the newest overlay term record ≤ G per tile,
// else the fold base unless a rebase ≤ G voided it — and appends one
// (tile, mask) per live posting in ascending tile order. Every posting bit
// is rechecked against the resolved liveness so a stale or grafted posting
// fails closed. Post-fold (both overlay counters zero, the state every
// Collection.Snapshot sees because pinning materializes pending parents)
// this is byte-for-byte the pre-overlay probe with the flat-table recheck.
func (s *Snapshot) appendPrimaryExactMasks(
	dst []store.Mask, workspace *IndexWorkspace,
	name string, values []vibejson.Index,
) ([]store.Mask, error) {
	indexID, ok := s.collection.options.indexNameIDs[name]
	if !ok {
		return dst, store.ErrIndexNotFound
	}
	exact := s.collection.options.indexes[indexID]
	var components [store.MaxIndexColumns]storeio.IndexTermComponent
	// The needle canonicalization scratch comes from the workspace: a stack
	// array of IndexTermMaxKeyBytes would be re-zeroed on every probe (a
	// measured ~2% of the mid-window gate), and the workspace already exists
	// to retain exactly this kind of high-water storage.
	needle := []byte(nil)
	if workspace != nil {
		needle = workspace.needle[:0]
	}
	key, err := appendPrimaryExactNeedleTerm(
		needle, components[:], exact, values,
	)
	if workspace != nil && cap(key) > cap(workspace.needle) {
		workspace.needle = key
	}
	if err != nil {
		return dst, err
	}
	if workspace != nil {
		workspace.lastProbe = IndexProbeStats{}
	}
	epoch := s.epoch
	if epoch == nil || int(indexID) >= len(epoch.exact) {
		return dst, nil
	}
	atGen := s.state.root.Generation
	keyRecord := storeio.IndexTermKeyRecord{
		RouteHash: storeio.IndexTermRouteHash(s.collection.storeID, key),
		Canonical: key,
	}
	var entry *primaryExactTermEntry
	if epoch.termRecordCount.Load() != 0 {
		entry = epoch.lookupTermEntry(
			primaryExactTermChainHash(keyRecord.RouteHash, uint32(indexID)),
			uint32(indexID), key,
		)
	}
	overlayTiles := epoch.tileRecordCount.Load() != 0
	resident := &epoch.exact[indexID]

	if entry == nil && !overlayTiles {
		// Post-fold fast path: no record can affect this probe, so the base
		// is the whole answer and the only overlay cost was the two counter
		// loads and one empty table probe above.
		if !resident.present {
			return dst, nil
		}
		match, found := resident.view.LookupRecord(keyRecord)
		if !found {
			return dst, nil
		}
		iterator := match.MaskIterator()
		for {
			tileID, mask, next := iterator.Next()
			if !next {
				break
			}
			if mask.Chunk != 0 || mask.Bits == 0 ||
				mask.Bits&^epoch.live.word(tileID) != 0 {
				return dst, storeio.ErrPrimaryExactIndexCorrupt
			}
			dst = append(dst, store.Mask{Chunk: tileID, Bits: mask.Bits})
			if workspace != nil {
				rows := uint64(bits.OnesCount64(mask.Bits))
				workspace.lastProbe.CandidateRows += rows
				workspace.lastProbe.CertificateRows += rows
				workspace.lastProbe.MatchedRows += rows
				workspace.lastProbe.CandidateChunks++
				workspace.lastProbe.PostingPages = 1
			}
		}
		return dst, nil
	}

	// Mid-window merged path. Overlay contributions collect into workspace
	// scratch as one sorted-insert-dedup pass: the chain is newest-first, so
	// the first record ≤ G per tile wins and a later hit on an already
	// collected tile is simply skipped; records older than the tile's newest
	// rebase ≤ G contribute "no bits". The sorted set then merges with the
	// base's ascending mask iterator so the output tile order matches the
	// post-fold path exactly.
	var overlay []primaryExactProbeTile
	if workspace != nil {
		overlay = workspace.overlayTiles[:0]
	}
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
				continue // a newer record for this tile is already collected
			}
			recordBits := record.bits
			if overlayTiles &&
				record.gen < epoch.rebaseFloor(record.tileID, atGen) {
				recordBits = 0 // pre-rebase record: superseded slot assignment
			}
			overlay = append(overlay, primaryExactProbeTile{})
			copy(overlay[lo+1:], overlay[lo:])
			overlay[lo] = primaryExactProbeTile{
				tileID: record.tileID, bits: recordBits,
			}
		}
	}
	// Decode the base postings into the second workspace scratch, applying
	// the rebase-void rule, so the emission below is one fused two-array
	// merge with a single inlined emission site and register-local stats —
	// the shape that keeps the 64-mutation worst case inside the mid-window
	// gate.
	var base []primaryExactProbeTile
	if workspace != nil {
		base = workspace.baseTiles[:0]
	}
	if resident.present {
		if match, found := resident.view.LookupRecord(keyRecord); found {
			iterator := match.MaskIterator()
			for {
				tileID, mask, next := iterator.Next()
				if !next {
					break
				}
				if mask.Chunk != 0 || mask.Bits == 0 {
					return dst, storeio.ErrPrimaryExactIndexCorrupt
				}
				if overlayTiles && epoch.rebaseFloor(tileID, atGen) != 0 {
					// Rebased bucket: base postings void; the rebase group's
					// absolute records are the only truth for its tiles.
					continue
				}
				base = append(base, primaryExactProbeTile{
					tileID: tileID, bits: mask.Bits,
				})
			}
		}
	}
	// Sentinel-terminated two-array merge: both sides end on an impossible
	// tile id, so the hot loop carries no bounds checks, and the flat-table
	// liveness probe is inlined (hoisted slot array + mask) — together these
	// are what hold the 64-mutation worst case inside the 1.5 µs gate. The
	// scratch write-back happens after the sentinels so their grown capacity
	// is what the workspace retains.
	overlay = append(overlay, primaryExactProbeTile{tileID: ^uint32(0)})
	base = append(base, primaryExactProbeTile{tileID: ^uint32(0)})
	if workspace != nil {
		workspace.overlayTiles = overlay
		workspace.baseTiles = base
	}
	liveSlots := epoch.live.slots
	liveMask := uint32(len(liveSlots) - 1)
	var candidateRows uint64
	var candidateChunks int
	at, bt := 0, 0
	for {
		tileID := overlay[at].tileID
		var maskBits uint64
		if tileID <= base[bt].tileID {
			if tileID == ^uint32(0) {
				break
			}
			// An overlay record ≤ G owns its tile absolutely; a base posting
			// for the same tile is superseded whether the record adds,
			// moves, or clears bits.
			maskBits = overlay[at].bits
			if base[bt].tileID == tileID {
				bt++
			}
			at++
			if maskBits == 0 {
				continue
			}
		} else {
			tileID, maskBits = base[bt].tileID, base[bt].bits
			bt++
		}
		var live uint64
		h := tileID
		h ^= h >> 16
		h *= 0x45d9f3b
		h ^= h >> 16
		for slot := h & liveMask; ; slot = (slot + 1) & liveMask {
			entry := &liveSlots[slot]
			if entry.mask == nil {
				break
			}
			if entry.tileID == tileID {
				live = entry.mask[0]
				break
			}
		}
		if overlayTiles {
			if entry := epoch.lookupTileEntry(tileID); entry != nil {
				for record := entry.head.Load(); record != nil; record = record.next {
					if record.gen <= atGen {
						live = record.live
						break
					}
				}
			}
		}
		if maskBits&^live != 0 {
			return dst, storeio.ErrPrimaryExactIndexCorrupt
		}
		dst = append(dst, store.Mask{Chunk: tileID, Bits: maskBits})
		candidateRows += uint64(bits.OnesCount64(maskBits))
		candidateChunks++
	}
	if workspace != nil && candidateChunks != 0 {
		workspace.lastProbe.CandidateRows += candidateRows
		workspace.lastProbe.CertificateRows += candidateRows
		workspace.lastProbe.MatchedRows += candidateRows
		workspace.lastProbe.CandidateChunks += candidateChunks
		workspace.lastProbe.PostingPages = 1
	}
	return dst, nil
}

// buildPrimaryExactIndexes derives every physical index's posting tiles from the
// placements the primary build assigned each row, encodes one canonical term
// leaf per non-empty index plus the reference catalog, and returns the root.
func buildPrimaryExactIndexes(
	tx *storeio.WriteTransaction,
	records []storeio.PrimaryGraphRecord,
	placements []storeio.PrimaryGraphPlacement,
	indexes []*store.ExactIndex,
	pageSize, maxPageSize uint32,
) (storeio.PageRef, error) {
	if len(indexes) == 0 {
		return storeio.PageRef{}, nil
	}
	live := make(map[uint32]*[storeio.TermPostingTileChunks]uint64)
	for _, placement := range placements {
		tileID := uint32(placement.Bucket)<<2 | uint32(placement.Slot>>6)
		mask := live[tileID]
		if mask == nil {
			mask = new([storeio.TermPostingTileChunks]uint64)
			live[tileID] = mask
		}
		mask[0] |= uint64(1) << uint(placement.Slot&63)
	}

	byIndex := make([]map[string]primaryExactTermPostings, len(indexes))
	for i := range byIndex {
		byIndex[i] = make(map[string]primaryExactTermPostings)
	}
	var components [store.MaxIndexColumns]storeio.IndexTermComponent
	var canonical [storeio.IndexTermMaxKeyBytes]byte
	for row, record := range records {
		placement := placements[row]
		tileID := uint32(placement.Bucket)<<2 | uint32(placement.Slot>>6)
		bit := uint64(1) << uint(placement.Slot&63)
		for indexID, exact := range indexes {
			key, present, err := appendPrimaryExactDocumentTerm(
				canonical[:0], components[:], exact, record.Value,
			)
			if err != nil {
				return storeio.PageRef{}, err
			}
			if !present {
				continue
			}
			identity := string(key)
			postings := byIndex[indexID][identity]
			if postings == nil {
				postings = make(primaryExactTermPostings)
				byIndex[indexID][identity] = postings
			}
			postings[tileID] |= bit
		}
	}

	leafRefs := make([]storeio.PageRef, len(indexes))
	for indexID := range indexes {
		if len(byIndex[indexID]) == 0 {
			continue
		}
		keys := make([]string, 0, len(byIndex[indexID]))
		for key := range byIndex[indexID] {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		terms := make([]storeio.IndexTermLeafTerm, len(keys))
		for termAt, key := range keys {
			keyBytes := []byte(key)
			record, ok := storeio.OpenIndexTermKeyRecord(tx.StoreID(), keyBytes)
			if !ok {
				return storeio.PageRef{}, fmt.Errorf(
					"%w: canonical exact term", storeio.ErrInvalidWrite,
				)
			}
			tiles := make([]uint32, 0, len(byIndex[indexID][key]))
			for tileID := range byIndex[indexID][key] {
				tiles = append(tiles, tileID)
			}
			slices.Sort(tiles)
			postings := make([]storeio.IndexTermLeafPosting, len(tiles))
			for postingAt, tileID := range tiles {
				maskBits := byIndex[indexID][key][tileID]
				liveMask := live[tileID]
				// Chunk-0-only postings encode through the leaf's direct
				// representations; the synthetic (tile, rows) record plus
				// the mask is the builder's complete input (see
				// encodeIndexTermLeaf).
				postings[postingAt] = storeio.IndexTermLeafPosting{
					Posting: storeio.TermPosting{
						TileID: tileID,
						Rows:   uint16(bits.OnesCount64(maskBits)),
					},
					Live:       liveMask,
					Chunk0Bits: maskBits, Chunk0Only: true,
				}
			}
			terms[termAt] = storeio.IndexTermLeafTerm{
				Key: record, Postings: postings,
			}
		}
		encoded, err := storeio.AppendIndexTermLeaf(nil, tx.StoreID(), terms)
		if err != nil {
			return storeio.PageRef{}, fmt.Errorf(
				"%w: ordered-primary exact index %d: %v",
				ErrPrimaryCutoverUnsupported, indexID, err,
			)
		}
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
			return storeio.PageRef{}, fmt.Errorf(
				"vibedb: allocate ordered-primary exact leaf %d (%d bytes): %w",
				indexID, extent, err,
			)
		}
		if _, err := storeio.EncodePrimaryExactLeafPage(
			page.Bytes(), tx.StoreID(), tx.Generation(),
			page.Ref().LogicalID, encoded,
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
		return storeio.PageRef{}, fmt.Errorf(
			"vibedb: allocate ordered-primary exact root: %w", err,
		)
	}
	if _, err := storeio.EncodePrimaryExactRootPage(
		rootPage.Bytes(), tx.StoreID(), tx.Generation(),
		rootPage.Ref().LogicalID, leafRefs,
	); err != nil {
		return storeio.PageRef{}, err
	}
	if err := rootPage.Stage(); err != nil {
		return storeio.PageRef{}, err
	}
	return rootPage.Ref(), nil
}

// appendPrimaryExactDocumentTerm canonicalizes one document's compound exact
// term. present is false when any component path is missing or non-scalar, in
// which case the document does not contribute to this index.
func appendPrimaryExactDocumentTerm(
	dst []byte,
	components []storeio.IndexTermComponent,
	exact *store.ExactIndex,
	raw []byte,
) ([]byte, bool, error) {
	for i := 0; i < int(exact.N); i++ {
		value, found, err := exact.Paths[i].GetRawTrusted(raw)
		if err != nil {
			return dst, false, err
		}
		if !found {
			return dst, false, nil
		}
		component, ok := primaryExactComponent(value)
		if !ok {
			return dst, false, nil
		}
		components[i] = component
	}
	key, ok := storeio.AppendIndexTermKey(dst, components[:exact.N])
	if !ok {
		return dst, false, fmt.Errorf(
			"%w: exact term exceeds ordered-primary bound",
			ErrPrimaryCutoverUnsupported,
		)
	}
	return key, true, nil
}

// appendPrimaryExactNeedleTerm canonicalizes a lookup needle from the caller's
// index values, matching the document-side canonicalization exactly.
func appendPrimaryExactNeedleTerm(
	dst []byte,
	components []storeio.IndexTermComponent,
	exact *store.ExactIndex,
	values []vibejson.Index,
) ([]byte, error) {
	if len(values) != int(exact.N) {
		return dst, store.ErrIndexArity
	}
	for i := range values {
		component, ok := primaryExactComponent(values[i].Root().Raw())
		if !ok {
			return dst, store.ErrIndexScalar
		}
		components[i] = component
	}
	key, ok := storeio.AppendIndexTermKey(dst, components[:exact.N])
	if !ok {
		return dst, store.ErrIndexScalar
	}
	return key, nil
}

func primaryExactComponent(raw vibejson.RawValue) (storeio.IndexTermComponent, bool) {
	kind := storeio.IndexTermInvalid
	switch raw.Kind() {
	case document.Null:
		kind = storeio.IndexTermNull
	case document.Bool:
		kind = storeio.IndexTermBool
	case document.Number:
		kind = storeio.IndexTermNumber
	case document.String:
		kind = storeio.IndexTermString
	default:
		return storeio.IndexTermComponent{}, false
	}
	return storeio.IndexTermComponent{
		Kind: kind, Direction: storeio.IndexTermAscending, JSON: raw.Bytes(),
	}, true
}

// primaryExactExtent rounds a byte need up to the smallest power-of-two page
// extent within [pageSize, maxPageSize].
func primaryExactExtent(
	need int, pageSize, maxPageSize uint32,
) (uint32, bool) {
	extent := pageSize
	for uint64(extent) < uint64(need) && extent < maxPageSize {
		extent <<= 1
	}
	return extent, uint64(extent) >= uint64(need) &&
		extent <= maxPageSize && bits.OnesCount32(extent) == 1
}
