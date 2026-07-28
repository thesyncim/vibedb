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
	return len(c.primaryExact) != 0 || c.primaryLive != nil
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

// openPrimaryExactIndexes materializes the resident exact indexes from the
// published ExactIndexRoot and derives the live-slot map they are validated
// against. It is called once by Open for an indexed ordered-primary collection.
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
	c.primaryLive = live
	liveLookup := func(tileID uint32) *[storeio.TermPostingTileChunks]uint64 {
		return c.primaryLive[tileID]
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
	resident := make([]primaryExactResident, root.Len())
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
			resident[indexID].encoded = append([]byte(nil), payload...)
		}
		lease.Release()
		if openErr != nil {
			return openErr
		}
		view, viewErr := storeio.OpenIndexTermLeaf(
			resident[indexID].encoded, state.root.StoreID, liveLookup,
		)
		if viewErr != nil {
			return viewErr
		}
		resident[indexID].view = view
		resident[indexID].present = true
	}
	c.primaryExact = resident
	return nil
}

// appendPrimaryExactMasks is the exact-match read path for an ordered-primary
// index: it canonicalizes the needle term, looks it up in the resident term
// leaf, and appends one (tile, mask) per live posting. Every posting bit is
// rechecked against the live map so a stale or grafted posting fails closed.
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
	var canonical [storeio.IndexTermMaxKeyBytes]byte
	key, err := appendPrimaryExactNeedleTerm(
		canonical[:0], components[:], exact, values,
	)
	if err != nil {
		return dst, err
	}
	if workspace != nil {
		workspace.lastProbe = IndexProbeStats{}
	}
	if int(indexID) >= len(s.collection.primaryExact) ||
		!s.collection.primaryExact[indexID].present {
		return dst, nil
	}
	match, found := s.collection.primaryExact[indexID].view.Lookup(key)
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
			s.collection.primaryLive[tileID] == nil ||
			mask.Bits&^s.collection.primaryLive[tileID][0] != 0 {
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
		var componentScratch [storeio.TermPostingMaxPayloadBytes]byte
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
				var posting [storeio.TermPostingTileChunks]uint64
				posting[0] = byIndex[indexID][key][tileID]
				liveMask := live[tileID]
				built, componentBytes, buildErr := storeio.BuildTermPosting(
					componentScratch[:], tileID, &posting, liveMask,
				)
				if buildErr != nil {
					return storeio.PageRef{}, buildErr
				}
				var component []byte
				if componentBytes != 0 {
					component = append(
						[]byte(nil), componentScratch[:componentBytes]...,
					)
				}
				postings[postingAt] = storeio.IndexTermLeafPosting{
					Posting: built, Component: component, Live: liveMask,
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
