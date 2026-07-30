package durable

import (
	"bytes"
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

// primaryExactLeaf is one resident spanned term leaf
// (docs/design/indexed-write-path.md §6): a bounded slice of one physical
// index's ordered term sequence produced by the deterministic content-defined
// cutter. encoded is the canonical IndexTermLeaf byte stream; it is GC-owned
// and shared by reference across index epochs when a fold carries the leaf
// forward untouched, which is what makes the checkpoint fold O(dirty leaves).
// ref is the durable page currently holding encoded (zero until staged);
// carrying it forward is what lets a checkpoint stage only dirty leaves and
// retire only their superseded pages. firstKey/firstTile order the leaves —
// stripe pieces of one giant term share firstKey and ascend by firstTile —
// and piece marks a rule-2 stripe piece (always a single-term leaf).
type primaryExactLeaf struct {
	encoded   []byte
	view      storeio.IndexTermLeafView
	ref       storeio.PageRef
	firstKey  []byte
	firstTile uint32
	piece     bool
	runCut    bool
}

// primaryExactResident is one physical exact index resident for reads: its
// ordered spanned leaves (the resident leaf router IS this slice — probes
// binary-search it) and the catalog pages currently describing them durably.
type primaryExactResident struct {
	leaves  []primaryExactLeaf
	catalog []storeio.PageRef
}

func (r *primaryExactResident) present() bool { return len(r.leaves) != 0 }

// primaryExactLeafAt returns the position of the last leaf whose
// (firstKey, firstTile) sorts at or before (term, tile), or -1 when the term
// sorts before every leaf. This is the resident router's probe: the returned
// leaf is the only packed leaf that can hold term, and pieces of a spanned
// term continue in the immediately following leaves.
func primaryExactLeafAt(
	leaves []primaryExactLeaf, term []byte, tile uint32,
) int {
	lo, hi := 0, len(leaves)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		leaf := &leaves[mid]
		cmp := bytes.Compare(leaf.firstKey, term)
		if cmp == 0 {
			if leaf.firstTile <= tile {
				cmp = -1
			} else {
				cmp = 1
			}
		}
		if cmp < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo - 1
}

// primaryExactRunHead reports whether leaf at starts a rule-1 run: its first
// term satisfies the content-defined cut predicate and the leaf is the first
// carrying that term (later stripe pieces of a cut giant term are interior).
// Position 0 always starts the index's leading run.
func primaryExactRunHead(leaves []primaryExactLeaf, at int) bool {
	if at == 0 {
		return true
	}
	leaf := &leaves[at]
	if !leaf.runCut {
		return false
	}
	return !bytes.Equal(leaves[at-1].firstKey, leaf.firstKey)
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

// openPrimaryExactLeafResident admits one durable term leaf into its resident
// form: copies the canonical bytes out of the page, admits the view against
// the epoch's live lookup, derives the leaf's first (term, tile) from the
// admitted content, and cross-checks it against the catalog entry the leaf
// was reached through — a grafted or reordered catalog fails closed here.
func (c *Collection) openPrimaryExactLeafResident(
	entry storeio.PrimaryExactCatalogEntry,
	bounds storeio.PrimaryExactIndexBounds,
	live storeio.IndexTermLeafLiveLookup,
) (primaryExactLeaf, error) {
	lease, err := c.cache.Acquire(entry.Leaf)
	if err != nil {
		return primaryExactLeaf{}, err
	}
	payload, openErr := storeio.OpenPrimaryExactLeafPage(
		lease.Page(), entry.Leaf, bounds,
	)
	var encoded []byte
	if openErr == nil {
		encoded = append([]byte(nil), payload...)
	}
	lease.Release()
	if openErr != nil {
		return primaryExactLeaf{}, openErr
	}
	view, err := storeio.OpenIndexTermLeaf(encoded, c.storeID, live)
	if err != nil {
		return primaryExactLeaf{}, err
	}
	leaf, err := c.newPrimaryExactResidentLeaf(
		encoded, view, entry.Flags&storeio.PrimaryExactCatalogPiece != 0,
	)
	if err != nil {
		return primaryExactLeaf{}, err
	}
	leaf.ref = entry.Leaf
	prefix := leaf.firstKey
	if len(prefix) > storeio.PrimaryExactCatalogPrefixBytes {
		prefix = prefix[:storeio.PrimaryExactCatalogPrefixBytes]
	}
	wantFlags := uint8(0)
	if leaf.piece {
		wantFlags |= storeio.PrimaryExactCatalogPiece
	}
	if leaf.runCut {
		wantFlags |= storeio.PrimaryExactCatalogRunCut
	}
	if !bytes.Equal(entry.Prefix, prefix) ||
		entry.FirstTile != leaf.firstTile || entry.Flags != wantFlags ||
		leaf.piece && view.Len() != 1 {
		return primaryExactLeaf{}, storeio.ErrPrimaryExactIndexCorrupt
	}
	return leaf, nil
}

// newPrimaryExactResidentLeaf derives the resident router fields from an
// admitted view: the first term's canonical bytes (copied — the iterator's
// key scratch does not survive), its first posting tile, and the rule-1
// predicate of the first term — a pure content function recomputed with the
// same route hash the cutter decided by.
func (c *Collection) newPrimaryExactResidentLeaf(
	encoded []byte, view storeio.IndexTermLeafView, piece bool,
) (primaryExactLeaf, error) {
	it := view.Ordered()
	key, match, ok := it.Next()
	if !ok {
		return primaryExactLeaf{}, storeio.ErrPrimaryExactIndexCorrupt
	}
	mi := match.MaskIterator()
	tileID, _, more := mi.Next()
	if !more {
		return primaryExactLeaf{}, storeio.ErrPrimaryExactIndexCorrupt
	}
	firstKey := append([]byte(nil), key...)
	return primaryExactLeaf{
		encoded:   encoded,
		view:      view,
		firstKey:  firstKey,
		firstTile: tileID,
		piece:     piece,
		runCut: storeio.IndexTermLeafRunCut(
			storeio.IndexTermRouteHash(c.storeID, firstKey),
		),
	}, nil
}

// openPrimaryExactIndexes materializes the resident index epoch from the
// published v1 ExactIndexRoot: per physical index, the ordered term-leaf
// catalog and its admitted leaves (the fold base) plus the flat live table
// derived from the graph, under an empty overlay. It is called once by Open
// for an indexed ordered-primary collection.
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
	var entries []storeio.PrimaryExactCatalogEntry
	var prefixArena []byte
	for indexID := 0; indexID < root.Len(); indexID++ {
		rootEntry, ok := root.Entry(uint32(indexID))
		if !ok {
			return storeio.ErrPrimaryExactIndexCorrupt
		}
		if rootEntry.LeafCount == 0 {
			continue
		}
		entries, prefixArena, err = c.collectPrimaryExactCatalog(
			rootEntry.Catalog, bounds, entries[:0], prefixArena[:0],
			&epoch.exact[indexID].catalog,
		)
		if err != nil {
			return err
		}
		if uint32(len(entries)) != rootEntry.LeafCount {
			return storeio.ErrPrimaryExactIndexCorrupt
		}
		resident := &epoch.exact[indexID]
		resident.leaves = make([]primaryExactLeaf, 0, len(entries))
		for at := range entries {
			leaf, leafErr := c.openPrimaryExactLeafResident(
				entries[at], bounds, liveLookup,
			)
			if leafErr != nil {
				return leafErr
			}
			if n := len(resident.leaves); n != 0 {
				previous := &resident.leaves[n-1]
				cmp := bytes.Compare(previous.firstKey, leaf.firstKey)
				if cmp > 0 || cmp == 0 && previous.firstTile >= leaf.firstTile {
					return storeio.ErrPrimaryExactIndexCorrupt
				}
			}
			resident.leaves = append(resident.leaves, leaf)
		}
	}
	c.primaryEpoch = epoch
	// Retire/pool lists are bounded by fold cadence against the reclaim
	// floor; pre-sizing them keeps the publish-gate append allocation-free.
	c.primaryEpochRetired = make([]retiredPrimaryExactEpoch, 0, 8)
	c.primaryEpochPool = make([]*primaryExactEpoch, 0, 8)
	return nil
}

// collectPrimaryExactCatalog walks one physical index's catalog (depth ≤ 2)
// and returns its ordered leaf entries with prefixes copied into arena (the
// source pages are released before return). catalogRefs collects every
// catalog page reached, in walk order, for the epoch's retirement tracking.
func (c *Collection) collectPrimaryExactCatalog(
	ref storeio.PageRef,
	bounds storeio.PrimaryExactIndexBounds,
	entries []storeio.PrimaryExactCatalogEntry,
	arena []byte,
	catalogRefs *[]storeio.PageRef,
) ([]storeio.PrimaryExactCatalogEntry, []byte, error) {
	lease, err := c.cache.Acquire(ref)
	if err != nil {
		return entries, arena, err
	}
	view, err := storeio.OpenPrimaryExactCatalogPage(lease.Page(), ref, bounds)
	if err != nil {
		lease.Release()
		return entries, arena, err
	}
	*catalogRefs = append(*catalogRefs, ref)
	if view.Level() == 1 {
		children := make([]storeio.PageRef, 0, view.Len())
		for i := 0; i < view.Len(); i++ {
			child, ok := view.Child(uint32(i))
			if !ok {
				lease.Release()
				return entries, arena, storeio.ErrPrimaryExactIndexCorrupt
			}
			children = append(children, child)
		}
		lease.Release()
		for _, child := range children {
			var childErr error
			entries, arena, childErr = c.collectPrimaryExactCatalogLeafPage(
				child, bounds, entries, arena, catalogRefs,
			)
			if childErr != nil {
				return entries, arena, childErr
			}
		}
		return entries, arena, nil
	}
	entries, arena, err = appendPrimaryExactCatalogEntries(view, entries, arena)
	lease.Release()
	return entries, arena, err
}

func (c *Collection) collectPrimaryExactCatalogLeafPage(
	ref storeio.PageRef,
	bounds storeio.PrimaryExactIndexBounds,
	entries []storeio.PrimaryExactCatalogEntry,
	arena []byte,
	catalogRefs *[]storeio.PageRef,
) ([]storeio.PrimaryExactCatalogEntry, []byte, error) {
	lease, err := c.cache.Acquire(ref)
	if err != nil {
		return entries, arena, err
	}
	view, err := storeio.OpenPrimaryExactCatalogPage(lease.Page(), ref, bounds)
	if err == nil && view.Level() != 0 {
		err = storeio.ErrPrimaryExactIndexCorrupt
	}
	if err != nil {
		lease.Release()
		return entries, arena, err
	}
	*catalogRefs = append(*catalogRefs, ref)
	entries, arena, err = appendPrimaryExactCatalogEntries(view, entries, arena)
	lease.Release()
	return entries, arena, err
}

// appendPrimaryExactCatalogEntries copies a level-0 page's entries out of the
// borrowed page: prefixes land in arena so the lease can be released. Arena
// growth may rebind earlier prefixes, so offsets are recorded and rebased.
func appendPrimaryExactCatalogEntries(
	view storeio.PrimaryExactCatalogView,
	entries []storeio.PrimaryExactCatalogEntry,
	arena []byte,
) ([]storeio.PrimaryExactCatalogEntry, []byte, error) {
	type span struct{ off, n int }
	base := len(entries)
	spans := make([]span, 0, view.Len())
	err := view.ForEachEntry(func(e storeio.PrimaryExactCatalogEntry) error {
		off := len(arena)
		arena = append(arena, e.Prefix...)
		spans = append(spans, span{off: off, n: len(e.Prefix)})
		e.Prefix = nil
		entries = append(entries, e)
		return nil
	})
	if err != nil {
		return entries, arena, err
	}
	for i := range spans {
		entries[base+i].Prefix = arena[spans[i].off : spans[i].off+spans[i].n]
	}
	return entries, arena, nil
}

// appendPrimaryExactMasks is the exact-match read path for an ordered-primary
// index: it canonicalizes the needle term and resolves it through the pinned
// index epoch's read rule (docs/design/indexed-write-path.md §3) at the
// snapshot's generation G — the newest overlay term record ≤ G per tile,
// else the fold base unless a rebase ≤ G voided it — and appends one
// (tile, mask) per live posting in ascending tile order. The base is spanned
// (§6.3): the probe binary-searches the resident leaf router once, opens the
// admitted view it lands on, and streams; while the following leaves continue
// the same term (stripe pieces of a giant term) it advances and repeats —
// sequential leaf views, no per-leaf search, zero allocations. Every posting
// bit is rechecked against the resolved liveness so a stale or grafted
// posting fails closed. Post-fold (both overlay counters zero) this is the
// pre-overlay probe plus one router binary search.
func (s *Snapshot) appendPrimaryExactMasks(
	dst []store.Mask, workspace *IndexWorkspace,
	name string, values []vibejson.Index,
) ([]store.Mask, error) {
	indexID, ok := s.indexNameIDs[name]
	if !ok {
		return dst, store.ErrIndexNotFound
	}
	exact := s.indexes[indexID]
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
	leaves := resident.leaves
	// Route to the first leaf that can hold the term. The predecessor search
	// on (term, 0) lands one leaf early when the term's own first leaf starts
	// at a non-zero tile — a stripe piece, or a packed leaf whose first term
	// is the needle — so a miss on the routed leaf falls through to the next
	// leaf exactly when that leaf's first term IS the needle.
	leafAt := -1
	if len(leaves) != 0 {
		leafAt = primaryExactLeafAt(leaves, key, 0)
		if leafAt < 0 && !bytes.Equal(leaves[0].firstKey, key) {
			leafAt = len(leaves) // sorts before every leaf: absent
		}
		if leafAt < 0 {
			leafAt = 0
		}
	} else {
		leafAt = 0
	}

	if entry == nil && !overlayTiles {
		// Post-fold fast path: no record can affect this probe, so the base
		// is the whole answer and the only overlay cost was the two counter
		// loads and one empty table probe above.
		pages := 0
		for at := leafAt; at < len(leaves); at++ {
			leaf := &leaves[at]
			if at != leafAt && !bytes.Equal(leaf.firstKey, key) {
				break // the term's stripe pieces, if any, are consecutive
			}
			match, found := leaf.view.LookupRecord(keyRecord)
			if !found {
				if at == leafAt && at+1 < len(leaves) &&
					bytes.Equal(leaves[at+1].firstKey, key) {
					continue // the term begins at the next leaf
				}
				break
			}
			pages++
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
				}
			}
		}
		if workspace != nil {
			workspace.lastProbe.PostingPages = pages
		}
		return dst, nil
	}

	// Mid-window merged path. Overlay contributions collect into workspace
	// scratch as one sorted-insert-dedup pass: the chain is newest-first, so
	// the first record ≤ G per tile wins and a later hit on an already
	// collected tile is simply skipped; records older than the tile's newest
	// rebase ≤ G contribute "no bits". The sorted set then merges with the
	// base's ascending mask stream so the output tile order matches the
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
	// gate. The spanned base streams pieces in leaf order, so tiles stay
	// ascending.
	var base []primaryExactProbeTile
	if workspace != nil {
		base = workspace.baseTiles[:0]
	}
	{
		for at := leafAt; at < len(leaves); at++ {
			leaf := &leaves[at]
			if at != leafAt && !bytes.Equal(leaf.firstKey, key) {
				break
			}
			match, found := leaf.view.LookupRecord(keyRecord)
			if !found {
				if at == leafAt && at+1 < len(leaves) &&
					bytes.Equal(leaves[at+1].firstKey, key) {
					continue // the term begins at the next leaf
				}
				break
			}
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

// primaryExactStagedLeaf is one leaf staged durable by the bulk build or a
// checkpoint: its page ref plus the catalog entry fields derived from its
// content.
type primaryExactStagedLeaf struct {
	ref       storeio.PageRef
	firstKey  []byte
	firstTile uint32
	piece     bool
	runCut    bool
}

// stagePrimaryExactCatalog writes the ordered catalog for one physical
// index's staged leaves: one level-0 page when the entries fit a single
// MaxPageSize extent, else level-0 children under one level-1 page (catalog
// tree depth ≤ 2). It returns the root-entry reference and appends every
// written catalog page to pages.
func stagePrimaryExactCatalog(
	tx *storeio.WriteTransaction,
	pageSize, maxPageSize uint32,
	staged []primaryExactStagedLeaf,
	pages []storeio.PageRef,
) (storeio.PageRef, []storeio.PageRef, error) {
	entries := make([]storeio.PrimaryExactCatalogEntry, len(staged))
	budget := int(maxPageSize) - storeio.PageHeaderSize - storeio.PageTrailerSize
	entryBytes := 0
	for i := range staged {
		prefix := staged[i].firstKey
		if len(prefix) > storeio.PrimaryExactCatalogPrefixBytes {
			prefix = prefix[:storeio.PrimaryExactCatalogPrefixBytes]
		}
		flags := uint8(0)
		if staged[i].piece {
			flags |= storeio.PrimaryExactCatalogPiece
		}
		if staged[i].runCut {
			flags |= storeio.PrimaryExactCatalogRunCut
		}
		entries[i] = storeio.PrimaryExactCatalogEntry{
			Leaf: staged[i].ref, FirstTile: staged[i].firstTile,
			Flags: flags, Prefix: prefix,
		}
		entryBytes += storeio.PrimaryExactCatalogEntryBytes(len(prefix))
	}
	stagePage := func(
		fill func(dst []byte, logicalID uint64) ([]byte, error),
		payloadBytes int,
	) (storeio.PageRef, error) {
		extent, ok := primaryExactExtent(
			payloadBytes+storeio.PageHeaderSize+storeio.PageTrailerSize,
			pageSize, maxPageSize,
		)
		if !ok {
			return storeio.PageRef{}, storeio.ErrPrimaryExactIndexCorrupt
		}
		page, err := tx.Allocate(storeio.PagePrimaryExactCatalog, extent, 0)
		if err != nil {
			return storeio.PageRef{}, err
		}
		if _, err := fill(page.Bytes(), page.Ref().LogicalID); err != nil {
			return storeio.PageRef{}, err
		}
		if err := page.Stage(); err != nil {
			return storeio.PageRef{}, err
		}
		return page.Ref(), nil
	}
	single := primaryExactCatalogHeaderReserve + entryBytes
	if single+storeio.PageHeaderSize+storeio.PageTrailerSize <= int(maxPageSize) {
		ref, err := stagePage(func(dst []byte, logicalID uint64) ([]byte, error) {
			return storeio.EncodePrimaryExactCatalogLeafPage(
				dst, tx.StoreID(), tx.Generation(), logicalID, entries,
			)
		}, single)
		if err != nil {
			return storeio.PageRef{}, pages, err
		}
		return ref, append(pages, ref), nil
	}
	// Two-level spill: fill level-0 children to the extent budget, then one
	// level-1 page naming them in order.
	var children []storeio.PageRef
	for start := 0; start < len(entries); {
		bytesUsed := primaryExactCatalogHeaderReserve
		end := start
		for end < len(entries) {
			next := storeio.PrimaryExactCatalogEntryBytes(
				len(entries[end].Prefix),
			)
			if bytesUsed+next > budget {
				break
			}
			bytesUsed += next
			end++
		}
		if end == start {
			return storeio.PageRef{}, pages, storeio.ErrPrimaryExactIndexCorrupt
		}
		child := entries[start:end]
		ref, err := stagePage(func(dst []byte, logicalID uint64) ([]byte, error) {
			return storeio.EncodePrimaryExactCatalogLeafPage(
				dst, tx.StoreID(), tx.Generation(), logicalID, child,
			)
		}, bytesUsed)
		if err != nil {
			return storeio.PageRef{}, pages, err
		}
		children = append(children, ref)
		pages = append(pages, ref)
		start = end
	}
	indexBytes := primaryExactCatalogHeaderReserve +
		len(children)*storeio.PageRefSize
	if indexBytes+storeio.PageHeaderSize+storeio.PageTrailerSize >
		int(maxPageSize) {
		return storeio.PageRef{}, pages, fmt.Errorf(
			"%w: exact catalog exceeds depth-2 bound",
			storeio.ErrPrimaryExactIndexCorrupt,
		)
	}
	ref, err := stagePage(func(dst []byte, logicalID uint64) ([]byte, error) {
		return storeio.EncodePrimaryExactCatalogIndexPage(
			dst, tx.StoreID(), tx.Generation(), logicalID, children,
		)
	}, indexBytes)
	if err != nil {
		return storeio.PageRef{}, pages, err
	}
	return ref, append(pages, ref), nil
}

// primaryExactCatalogHeaderReserve mirrors the storeio catalog header size
// for staging arithmetic.
const primaryExactCatalogHeaderReserve = 16

// buildPrimaryExactTerms materializes one physical index's sorted canonical
// term slice from the (term → tile → bits) map, in the exact input shape the
// cutter and leaf builder consume. Chunk-0-only postings encode through the
// leaf's direct representations; the synthetic (tile, rows) record plus the
// mask is the builder's complete input (see encode paths).
func buildPrimaryExactTerms(
	storeID [16]byte,
	terms map[string]map[uint32]uint64,
	live map[uint32]*[storeio.TermPostingTileChunks]uint64,
) ([]storeio.IndexTermLeafTerm, error) {
	if len(terms) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(terms))
	for key := range terms {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	out := make([]storeio.IndexTermLeafTerm, len(keys))
	for termAt, key := range keys {
		record, ok := storeio.OpenIndexTermKeyRecord(storeID, []byte(key))
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
			postings[postingAt] = storeio.IndexTermLeafPosting{
				Posting: storeio.TermPosting{
					TileID: tileID,
					Rows:   uint16(bits.OnesCount64(maskBits)),
				},
				Live:       liveMask,
				Chunk0Bits: maskBits, Chunk0Only: true,
			}
		}
		out[termAt] = storeio.IndexTermLeafTerm{Key: record, Postings: postings}
	}
	return out, nil
}

// encodePrimaryExactLeaves cuts and encodes one physical index's full term
// sequence into the resident spanned leaf set: the shared cutter decides the
// boundaries and AppendIndexTermLeaf encodes each leaf, so the output is
// byte-identical to a bulk build of the same postings — the invariant-4
// identity anchor, now spanning leaves.
func (c *Collection) encodePrimaryExactLeaves(
	terms map[string]map[uint32]uint64,
	live map[uint32]*[storeio.TermPostingTileChunks]uint64,
	liveLookup storeio.IndexTermLeafLiveLookup,
) ([]primaryExactLeaf, error) {
	ordered, err := buildPrimaryExactTerms(c.storeID, terms, live)
	if err != nil || len(ordered) == 0 {
		return nil, err
	}
	budget := storeio.IndexTermLeafCutBudget(uint32(c.options.MaxPageSize))
	var leaves []primaryExactLeaf
	err = storeio.CutIndexTermLeaves(
		ordered, budget,
		func(leafTerms []storeio.IndexTermLeafTerm, piece bool) error {
			encoded, encErr := storeio.AppendIndexTermLeaf(
				nil, c.storeID, leafTerms,
			)
			if encErr != nil {
				return encErr
			}
			view, viewErr := storeio.OpenIndexTermLeaf(
				encoded, c.storeID, liveLookup,
			)
			if viewErr != nil {
				return viewErr
			}
			leaf, leafErr := c.newPrimaryExactResidentLeaf(encoded, view, piece)
			if leafErr != nil {
				return leafErr
			}
			leaves = append(leaves, leaf)
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	return leaves, nil
}

// primaryExactIndexPageBound returns a safe upper bound on the pages
// buildPrimaryExactIndexes will stage for records, computed before the build
// transaction opens so the single bulk commit can reserve exactly one
// bounded batch. It plans the primary leaves (no staging), simulates each
// term's posting tiles — exact for ordinal-slot leaf classes, bounded by
// min(rows, 4) bucket quadrants for hash-directory classes — and runs the
// REAL cutter over the simulated content. Simulated per-term sizes dominate
// the actual ones tile-for-stripe, greedy cut counts are monotone in item
// sizes, and a term the simulation splits into stripe pieces costs at least
// the one leaf it would otherwise occupy, so the simulated leaf count never
// undercounts the staged one; catalog pages are bounded with the maximum
// entry width.
func primaryExactIndexPageBound(
	storeID [16]byte,
	records []storeio.PrimaryGraphRecord,
	indexes []*store.ExactIndex,
	maxPageSize uint32,
) (int, error) {
	if len(indexes) == 0 {
		return 0, nil
	}
	spans, err := storeio.PrimaryGraphLeafSpans(storeID, records)
	if err != nil {
		return 0, err
	}
	budget := storeio.IndexTermLeafCutBudget(maxPageSize)
	var components [store.MaxIndexColumns]storeio.IndexTermComponent
	var canonical [storeio.IndexTermMaxKeyBytes]byte
	total := 1 + 4 // the root page plus arithmetic headroom
	for _, exact := range indexes {
		tiles := make(map[string]map[uint32]bool)
		bucketRows := make(map[string]int)
		for _, span := range spans {
			clear(bucketRows)
			for row := span.First; row < span.Last; row++ {
				key, present, termErr := appendPrimaryExactDocumentTerm(
					canonical[:0], components[:], exact, records[row].Value,
				)
				if termErr != nil {
					return 0, termErr
				}
				if !present {
					continue
				}
				if span.Ordinal {
					tileID := uint32(span.Bucket)<<2 |
						uint32(row-span.First)>>6
					set := tiles[string(key)]
					if set == nil {
						set = make(map[uint32]bool, 4)
						tiles[string(key)] = set
					}
					set[tileID] = true
					continue
				}
				bucketRows[string(key)]++
			}
			for term, rows := range bucketRows {
				set := tiles[term]
				if set == nil {
					set = make(map[uint32]bool, 4)
					tiles[term] = set
				}
				for q := uint32(0); q < uint32(min(rows, 4)); q++ {
					set[uint32(span.Bucket)<<2|q] = true
				}
			}
		}
		if len(tiles) == 0 {
			continue
		}
		keys := make([]string, 0, len(tiles))
		for key := range tiles {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		terms := make([]storeio.IndexTermLeafTerm, len(keys))
		postingArena := make(
			[]storeio.IndexTermLeafPosting, 0, len(records)*4,
		)
		var live [storeio.TermPostingTileChunks]uint64
		live[0] = 1
		for at, key := range keys {
			record, ok := storeio.OpenIndexTermKeyRecord(storeID, []byte(key))
			if !ok {
				return 0, fmt.Errorf(
					"%w: canonical exact term", storeio.ErrInvalidWrite,
				)
			}
			sorted := make([]uint32, 0, len(tiles[key]))
			for tileID := range tiles[key] {
				sorted = append(sorted, tileID)
			}
			slices.Sort(sorted)
			start := len(postingArena)
			for _, tileID := range sorted {
				postingArena = append(postingArena, storeio.IndexTermLeafPosting{
					Posting:    storeio.TermPosting{TileID: tileID, Rows: 1},
					Live:       &live,
					Chunk0Bits: 1, Chunk0Only: true,
				})
			}
			terms[at] = storeio.IndexTermLeafTerm{
				Key: record, Postings: postingArena[start:len(postingArena):len(postingArena)],
			}
		}
		leaves := 0
		if err := storeio.CutIndexTermLeaves(
			terms, budget,
			func([]storeio.IndexTermLeafTerm, bool) error {
				leaves++
				return nil
			},
		); err != nil {
			return 0, err
		}
		entryMax := storeio.PrimaryExactCatalogEntryBytes(
			storeio.PrimaryExactCatalogPrefixBytes,
		)
		capacity := int(maxPageSize) - storeio.PageHeaderSize -
			storeio.PageTrailerSize - primaryExactCatalogHeaderReserve
		catalogPages := (leaves*entryMax + capacity - 1) / capacity
		if catalogPages == 0 {
			catalogPages = 1
		}
		if catalogPages > 1 {
			catalogPages++ // the level-1 page naming the children
		}
		total += leaves + catalogPages
	}
	return total, nil
}

// buildPrimaryExactIndexes derives every physical index's posting tiles from
// the placements the primary build assigned each row, cuts each index's term
// sequence into spanned leaves through the shared content-defined cutter,
// stages the leaves and their ordered catalogs, and returns the v1 exact
// root. Bulk build, checkpoint fold, and journal replay share the cutter, so
// identical final graphs produce byte-identical leaf sets regardless of path.
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

	budget := storeio.IndexTermLeafCutBudget(maxPageSize)
	rootEntries := make([]storeio.PrimaryExactRootEntry, len(indexes))
	var catalogPages []storeio.PageRef
	for indexID := range indexes {
		terms := make(map[string]map[uint32]uint64, len(byIndex[indexID]))
		for key, postings := range byIndex[indexID] {
			terms[key] = postings
		}
		ordered, err := buildPrimaryExactTerms(tx.StoreID(), terms, live)
		if err != nil {
			return storeio.PageRef{}, err
		}
		if len(ordered) == 0 {
			continue
		}
		var staged []primaryExactStagedLeaf
		err = storeio.CutIndexTermLeaves(
			ordered, budget,
			func(leafTerms []storeio.IndexTermLeafTerm, piece bool) error {
				encoded, encErr := storeio.AppendIndexTermLeaf(
					nil, tx.StoreID(), leafTerms,
				)
				if encErr != nil {
					return fmt.Errorf(
						"%w: ordered-primary exact index %d: %v",
						ErrPrimaryCutoverUnsupported, indexID, encErr,
					)
				}
				ref, stageErr := stagePrimaryExactLeafPage(
					tx, encoded, pageSize, maxPageSize,
				)
				if stageErr != nil {
					return stageErr
				}
				firstTile := leafTerms[0].Postings[0].Posting.TileID
				staged = append(staged, primaryExactStagedLeaf{
					ref:       ref,
					firstKey:  leafTerms[0].Key.Canonical,
					firstTile: firstTile,
					piece:     piece,
					runCut: storeio.IndexTermLeafRunCut(
						leafTerms[0].Key.RouteHash,
					),
				})
				return nil
			},
		)
		if err != nil {
			return storeio.PageRef{}, err
		}
		catalogRef, pages, err := stagePrimaryExactCatalog(
			tx, pageSize, maxPageSize, staged, catalogPages,
		)
		if err != nil {
			return storeio.PageRef{}, err
		}
		catalogPages = pages
		rootEntries[indexID] = storeio.PrimaryExactRootEntry{
			Catalog: catalogRef, LeafCount: uint32(len(staged)),
		}
	}
	rootPage, err := tx.Allocate(storeio.PagePrimaryExactRoot, pageSize, 0)
	if err != nil {
		return storeio.PageRef{}, fmt.Errorf(
			"vibedb: allocate ordered-primary exact root: %w", err,
		)
	}
	if _, err := storeio.EncodePrimaryExactRootPage(
		rootPage.Bytes(), tx.StoreID(), tx.Generation(),
		rootPage.Ref().LogicalID, rootEntries,
	); err != nil {
		return storeio.PageRef{}, err
	}
	if err := rootPage.Stage(); err != nil {
		return storeio.PageRef{}, err
	}
	return rootPage.Ref(), nil
}

// stagePrimaryExactLeafPage wraps one cutter-emitted canonical leaf in its
// durable page envelope. The extent always fits by construction (invariant
// 9: the cutter's budget is derived from MaxPageSize), so a failure here is
// corruption-class, not a capacity condition.
func stagePrimaryExactLeafPage(
	tx *storeio.WriteTransaction,
	encoded []byte,
	pageSize, maxPageSize uint32,
) (storeio.PageRef, error) {
	extent, ok := primaryExactExtent(
		len(encoded)+storeio.PageHeaderSize+storeio.PageTrailerSize,
		pageSize, maxPageSize,
	)
	if !ok {
		return storeio.PageRef{}, fmt.Errorf(
			"%w: cutter emitted an oversized exact term leaf",
			storeio.ErrPrimaryExactIndexCorrupt,
		)
	}
	page, err := tx.Allocate(storeio.PagePrimaryExactLeaf, extent, 0)
	if err != nil {
		return storeio.PageRef{}, err
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
	return page.Ref(), nil
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
