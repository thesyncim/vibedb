package durable

import (
	"fmt"
	"slices"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// Incremental exact-index posting maintenance for the ordered primary graph.
//
// A mutation rewrites exactly one leaf, and a leaf owns exactly the four
// quadrant posting tiles bucket<<2 | q (q in 0..3). Every posting bit for a row
// is a deterministic function of that row's stable slot in its leaf, so the
// whole exact-index effect of a mutation is confined to those four tiles. The
// maintainer therefore re-derives just those tiles from the freshly rewritten
// leaf image and splices them into the resident term leaves, leaving every other
// tile's postings untouched.
//
// This is deliberately re-derivation, not a term-delta: an insert draws a fresh
// stable slot, a delete frees one, an in-place value change keeps the slot, and
// a de-templating or narrow->wide rewrite can reassign every slot in the leaf at
// once. Recomputing the bucket's tiles from the new leaf image is correct under
// all of them, because it reads each surviving row under exactly the posting the
// read path would select it by (both route through VisitPrimaryLeafPostingRows).
//
// Because the resident encoded term-leaf bytes are reconstructed from the same
// canonical (term -> tile -> mask) map a bulk build produces, an incrementally
// maintained leaf is byte-identical to a fresh CreateFromPrimary of the same
// final graph. That equivalence is the maintainer's correctness anchor and is
// pinned by the mutation-vs-rebuild determinism tests.
//
// Cost is O(one leaf's live rows) to re-derive the bucket tiles plus O(one
// physical index's postings) to re-encode each affected term leaf. The re-encode
// is the honest price of a monolithic term-leaf format; a mutable delta layer
// that avoids it on every write is a later optimization and is not required for
// correctness. The unindexed mutation path never enters here (primaryExactActive
// is false) and pays nothing.

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

// reconstructIndexTerms rebuilds one physical index's complete canonical
// term -> tile -> bits map by walking the current resident term leaf, dropping
// every posting in bucket's four tiles, and merging in the freshly derived
// bucket contribution. The result feeds encodeIndexTermLeaf and is a pure
// function of the final graph, so it matches a bulk build.
func reconstructIndexTerms(
	view storeio.IndexTermLeafView, present bool,
	bucket storeio.BucketID,
	newContribution map[string]map[uint32]uint64,
) (map[string]map[uint32]uint64, error) {
	terms := make(map[string]map[uint32]uint64)
	if present {
		it := view.Ordered()
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
				if primaryExactTileBucket(tileID) == bucket {
					continue // superseded by the fresh bucket contribution
				}
				if mask.Chunk != 0 {
					return nil, storeio.ErrPrimaryExactIndexCorrupt
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
	for identity, tiles := range newContribution {
		dst := terms[identity]
		if dst == nil {
			dst = make(map[uint32]uint64)
			terms[identity] = dst
		}
		for tileID, bits := range tiles {
			dst[tileID] |= bits
		}
	}
	return terms, nil
}

// encodeIndexTermLeaf encodes one physical index's canonical term map into an
// IndexTermLeaf byte stream, mirroring buildPrimaryExactIndexes exactly so the
// output is byte-identical to a bulk build of the same postings. It returns nil
// bytes for an empty physical index.
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
	var componentScratch [storeio.TermPostingMaxPayloadBytes]byte
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
			var posting [storeio.TermPostingTileChunks]uint64
			posting[0] = tileMap[tileID]
			liveMask := live[tileID]
			if liveMask == nil {
				return nil, storeio.ErrPrimaryExactIndexCorrupt
			}
			built, componentBytes, buildErr := storeio.BuildTermPosting(
				componentScratch[:], tileID, &posting, liveMask,
			)
			if buildErr != nil {
				return nil, buildErr
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
		leafTerms[termAt] = storeio.IndexTermLeafTerm{
			Key: record, Postings: postings,
		}
	}
	return storeio.AppendIndexTermLeaf(nil, c.storeID, leafTerms)
}

// primaryExactPrepared is a fresh resident exact-index posting snapshot computed
// from a rewritten leaf image but not yet installed. Separating prepare from
// install lets every fallible step run before a mutation's point of no return
// and the field swap stay infallible under snapshotGate, exactly like the
// journal's fallible-append / infallible-publish split.
type primaryExactPrepared struct {
	active bool
	exact  []primaryExactResident
	live   map[uint32]*[storeio.TermPostingTileChunks]uint64
}

// preparePrimaryExactLeaf re-derives bucket's exact-index contribution from the
// rewritten leaf image and builds a fresh resident posting snapshot (term leaves
// plus live map) without touching the collection. It is a no-op returning
// active=false for a collection without exact indexes, so it never allocates on
// the unindexed mutation path.
//
// The fresh snapshot shares every unchanged tile's immutable live array by
// pointer and only replaces the mutated bucket's quadrant tiles, so a snapshot
// that captured the previous pair keeps a consistent view and the cost is one
// small map header plus the re-encodes, not a deep copy of the graph.
func (c *Collection) preparePrimaryExactLeaf(
	leafPage []byte, bucket storeio.BucketID,
	bounds storeio.CommonPrimaryLeafBounds,
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
	newLive := make(
		map[uint32]*[storeio.TermPostingTileChunks]uint64, len(c.primaryLive)+4,
	)
	for tileID, mask := range c.primaryLive {
		if primaryExactTileBucket(tileID) == bucket {
			continue
		}
		newLive[tileID] = mask
	}
	for tileID, bits := range bucketLive {
		arr := new([storeio.TermPostingTileChunks]uint64)
		arr[0] = bits
		newLive[tileID] = arr
	}
	liveLookup := func(
		tileID uint32,
	) *[storeio.TermPostingTileChunks]uint64 {
		return newLive[tileID]
	}
	newExact := make([]primaryExactResident, len(c.primaryExact))
	for indexID := range c.primaryExact {
		terms, reconstructErr := reconstructIndexTerms(
			c.primaryExact[indexID].view, c.primaryExact[indexID].present,
			bucket, byIndex[indexID],
		)
		if reconstructErr != nil {
			return primaryExactPrepared{}, reconstructErr
		}
		encoded, encErr := c.encodeIndexTermLeaf(terms, newLive)
		if encErr != nil {
			return primaryExactPrepared{}, encErr
		}
		if encoded == nil {
			newExact[indexID] = primaryExactResident{}
			continue
		}
		view, viewErr := storeio.OpenIndexTermLeaf(
			encoded, c.storeID, liveLookup,
		)
		if viewErr != nil {
			return primaryExactPrepared{}, viewErr
		}
		newExact[indexID] = primaryExactResident{
			encoded: encoded, view: view, present: true,
		}
	}
	return primaryExactPrepared{active: true, exact: newExact, live: newLive}, nil
}

// installPrimaryExactResidentLocked swaps in a prepared posting snapshot. It must
// run with c.snapshotGate held for write so a reader captures either the old or
// the new pair whole. It is infallible and never runs for the unindexed path
// (prepared.active is false there).
func (c *Collection) installPrimaryExactResidentLocked(
	prepared primaryExactPrepared,
) {
	if !prepared.active {
		return
	}
	c.primaryLive = prepared.live
	c.primaryExact = prepared.exact
}

// structuralBucketContribution is one leaf's exact-index contribution captured
// while a structural transaction re-encodes it: the bucket's new live tile masks
// and, per physical index, its new canonical term -> tile -> bits.
type structuralBucketContribution struct {
	live   map[uint32]uint64
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

// reconstructIndexTermsStructural rebuilds one physical index's term map for a
// structural transaction: it drops every posting in an affected bucket's tiles
// and merges the re-encoded buckets' fresh contributions for this index.
func reconstructIndexTermsStructural(
	view storeio.IndexTermLeafView, present bool,
	affected map[storeio.BucketID]bool,
	reencoded map[storeio.BucketID]*structuralBucketContribution,
	indexID int,
) (map[string]map[uint32]uint64, error) {
	terms := make(map[string]map[uint32]uint64)
	if present {
		it := view.Ordered()
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
				if affected[primaryExactTileBucket(tileID)] {
					continue
				}
				if mask.Chunk != 0 {
					return nil, storeio.ErrPrimaryExactIndexCorrupt
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
	for _, contrib := range reencoded {
		for identity, contribTiles := range contrib.byIndex[indexID] {
			dst := terms[identity]
			if dst == nil {
				dst = make(map[uint32]uint64)
				terms[identity] = dst
			}
			for tileID, bits := range contribTiles {
				dst[tileID] |= bits
			}
		}
	}
	return terms, nil
}

// prepareStructuralExactLocked builds a fresh resident posting snapshot from the
// structural accumulators. It drops the tiles of every re-encoded or removed
// bucket and adds the re-encoded contributions, so it is correct whether a
// tablet rebuild reassigned slots, added a leaf, or removed one. Returns
// active=false for a collection without exact indexes.
func (c *Collection) prepareStructuralExactLocked() (primaryExactPrepared, error) {
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
	newLive := make(
		map[uint32]*[storeio.TermPostingTileChunks]uint64,
		len(c.primaryLive)+4*len(c.structuralExactReencoded),
	)
	for tileID, mask := range c.primaryLive {
		if affected[primaryExactTileBucket(tileID)] {
			continue
		}
		newLive[tileID] = mask
	}
	for _, contrib := range c.structuralExactReencoded {
		for tileID, bits := range contrib.live {
			arr := new([storeio.TermPostingTileChunks]uint64)
			arr[0] = bits
			newLive[tileID] = arr
		}
	}
	liveLookup := func(
		tileID uint32,
	) *[storeio.TermPostingTileChunks]uint64 {
		return newLive[tileID]
	}
	newExact := make([]primaryExactResident, len(c.primaryExact))
	for indexID := range c.primaryExact {
		terms, reconstructErr := reconstructIndexTermsStructural(
			c.primaryExact[indexID].view, c.primaryExact[indexID].present,
			affected, c.structuralExactReencoded, indexID,
		)
		if reconstructErr != nil {
			return primaryExactPrepared{}, reconstructErr
		}
		encoded, encErr := c.encodeIndexTermLeaf(terms, newLive)
		if encErr != nil {
			return primaryExactPrepared{}, encErr
		}
		if encoded == nil {
			newExact[indexID] = primaryExactResident{}
			continue
		}
		view, viewErr := storeio.OpenIndexTermLeaf(
			encoded, c.storeID, liveLookup,
		)
		if viewErr != nil {
			return primaryExactPrepared{}, viewErr
		}
		newExact[indexID] = primaryExactResident{
			encoded: encoded, view: view, present: true,
		}
	}
	return primaryExactPrepared{active: true, exact: newExact, live: newLive}, nil
}

// stagePrimaryExactPagesLocked writes exact indexes as fresh durable pages
// inside tx and returns the new PagePrimaryExactRoot ref. Every present physical
// index is written in ascending index order (the reference catalog requires
// strictly ascending leaf logical ids), the superseded leaves and root named by
// state.root.ExactIndexRoot are retired through c.retireScratch, and an empty
// physical index keeps a zero leaf ref. It stages nothing and returns a zero ref
// for a collection without exact indexes.
//
// exact is the resident term-leaf set to persist: the freshly prepared snapshot
// for a per-mutation transaction, or c.primaryExact for a checkpoint. Its
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
