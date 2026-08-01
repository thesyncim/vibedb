package durable

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// The offline verify and salvage tools realise the "Verify and salvage, with
// catalog-loss recovery as a stated property" item from the next-wave plan.
//
// Both are strictly read-only against their input. They deliberately do NOT
// take the writer lock and do NOT apply the in-place materialization-journal
// rollback that Open performs, because that rollback writes to the file. They
// therefore want a quiescent file or a copy: a concurrent writer can retire and
// reuse the extents this walk is reading, and a file with an un-rolled-back
// materialization capsule is verified as written rather than as it would open.
// The primary-graph stores this package builds through CreateFromPrimary use no
// in-place materialization, so the durable-as-written image is the recovered
// image for them.

// VerifyFinding is one detected violation. Offset is the physical file offset of
// the offending page or free extent (zero when the finding is not tied to a
// single extent); LogicalID is its stable identity when known. Kind is a stable
// machine token so downstream scripts can classify without parsing Detail.
type VerifyFinding struct {
	Kind      string
	Offset    uint64
	LogicalID uint64
	Detail    string
}

// VerifyReport is the structured result of one full offline walk. A file is
// clean exactly when Findings is empty; every counter is still populated so a
// clean report can be diffed across runs.
type VerifyReport struct {
	// RootSlot is the selected inline superblock slot, or -1 when no valid root
	// could be selected (in which case Findings carries the reason).
	RootSlot    int
	Generation  uint64
	FileEnd     uint64
	Documents   int
	FreeExtents int
	// PageCounts records how many pages of each kind the walk validated.
	PageCounts map[string]int
	Findings   []VerifyFinding
}

// OK reports whether the walk found no violations.
func (r VerifyReport) OK() bool { return len(r.Findings) == 0 }

// Verify walks everything reachable from the newest valid root of a store file
// and returns a structured report. It selects the root exactly like recovery's
// first phase (newest structurally valid inline superblock whose top-level
// references resolve, with fallback to the preceding valid generation), then
// proves, for every reachable page: common-page checksum and identity; the
// catalog -> catalog-branch -> catalog-leaf -> tablet -> locator/anchor -> leaf
// graph shape; that leaf keys are globally lexically ordered across the whole
// traversal (which is exactly the fences-ordered / keys-within-fences invariant
// for an ordered graph); that no two reachable pages alias one extent; and that
// no durable free extent overlaps a reachable page.
//
// A structural error walking the file (an unreadable superblock, an
// unselectable root) is reported as a finding, not returned as err; err is
// reserved for an I/O failure that prevents the walk from running at all. The
// caller decides process exit status from report.OK().
func Verify(file *os.File) (VerifyReport, error) {
	report := VerifyReport{RootSlot: -1, PageCounts: map[string]int{}}
	if file == nil {
		return report, fmt.Errorf("vibedb: verify requires a non-nil file")
	}
	bootstrap, err := storeio.DiscoverMutableInlineBootstrap(file)
	if err != nil {
		report.Findings = append(report.Findings, VerifyFinding{
			Kind: "superblock", Detail: err.Error(),
		})
		return report, nil
	}
	scratch := make([]byte, bootstrap.MaxPageSize)
	inline, root, slot, _, err := storeio.RecoverInlineStateRootWithFallback(
		file, bootstrap.PageSize, scratch,
	)
	if err != nil {
		report.Findings = append(report.Findings, VerifyFinding{
			Kind: "root", Detail: err.Error(),
		})
		return report, nil
	}
	report.RootSlot = slot
	report.Generation = root.Generation
	report.FileEnd = inline.FileEnd

	w := &verifyWalker{
		file:        file,
		fileEnd:     inline.FileEnd,
		pageSize:    root.PageSize,
		maxPageSize: bootstrap.MaxPageSize,
		storeID:     root.StoreID,
		root:        root,
		report:      &report,
		catBounds: storeio.GlobalTabletCatalogBounds{
			StoreID:                root.StoreID,
			SelectedRootGeneration: root.Generation,
			FileEnd:                inline.FileEnd,
			NextLogicalID:          root.NextLogicalID,
		},
		leafBounds: storeio.CommonPrimaryLeafBounds{
			FileEnd:           inline.FileEnd,
			NextLogicalID:     root.NextLogicalID,
			AllocationQuantum: root.PageSize,
		},
	}

	w.walkStateRefs()
	if root.PrimaryRoot == (storeio.PageRef{}) {
		w.fail("root", 0, 0, "collection has no ordered-primary root")
	} else {
		w.walkPrimary()
	}
	if root.ExactIndexRoot != (storeio.PageRef{}) {
		w.walkExactIndexes()
	}
	w.checkReachableOverlap()
	w.walkFreeSet(&inline.FreeDelta)
	return report, nil
}

type verifyWalker struct {
	file        *os.File
	fileEnd     uint64
	pageSize    uint32
	maxPageSize uint32
	storeID     [16]byte
	root        storeio.StateRoot
	catBounds   storeio.GlobalTabletCatalogBounds
	leafBounds  storeio.CommonPrimaryLeafBounds
	report      *VerifyReport

	reachable []reachExtent
	haveLast  bool
	lastKey   []byte
}

type reachExtent struct {
	offset  uint64
	length  uint64
	logical uint64
	kind    string
}

func (w *verifyWalker) fail(
	kind string, offset, logical uint64, format string, args ...any,
) {
	w.report.Findings = append(w.report.Findings, VerifyFinding{
		Kind: kind, Offset: offset, LogicalID: logical,
		Detail: fmt.Sprintf(format, args...),
	})
}

func (w *verifyWalker) count(kind string) { w.report.PageCounts[kind]++ }

// walkExactIndexes proves the ordered-primary exact-index subtree: the
// PagePrimaryExactRoot reference catalog opens and admits (which validates that
// its leaf references are in-bounds, strictly ordered, and distinct from the
// root), and every non-empty PagePrimaryExactLeaf envelope opens with a matching
// identity. Each extent is recorded so the overlap check proves the posting
// tiles never alias a live routing page. The term-codec's semantic admission
// against the graph's live slot masks is proven by the online Open path.
func (w *verifyWalker) walkExactIndexes() {
	rootRef := w.root.ExactIndexRoot
	bounds := storeio.PrimaryExactIndexBounds{
		StoreID: w.storeID, Generation: w.root.Generation,
		FileEnd: w.fileEnd, NextLogicalID: w.root.NextLogicalID,
		AllocationQuantum: w.pageSize, MaxPageSize: w.maxPageSize,
		IndexCount: w.root.IndexCount,
	}
	w.record(rootRef, "primary-exact-root")
	page, ok := w.openAndCheckIdentity(rootRef, "primary-exact-root")
	if !ok {
		return
	}
	view, err := storeio.OpenPrimaryExactRootPage(page, rootRef, bounds)
	if err != nil {
		w.fail("primary-exact-root", rootRef.Offset, rootRef.LogicalID,
			"admit: %v", err)
		return
	}
	w.count("primary-exact-root")
	for indexID := 0; indexID < view.Len(); indexID++ {
		entry, ok := view.Entry(uint32(indexID))
		if !ok {
			w.fail("primary-exact-root", rootRef.Offset, rootRef.LogicalID,
				"root catalog entry %d", indexID)
			return
		}
		if entry.LeafCount == 0 {
			continue
		}
		state := verifyExactCatalogState{}
		w.walkExactCatalogPage(entry.Catalog, bounds, true, &state)
		if state.leaves != entry.LeafCount {
			w.fail("primary-exact-root", rootRef.Offset, rootRef.LogicalID,
				"index %d catalog leaves=%d want=%d",
				indexID, state.leaves, entry.LeafCount)
		}
	}
}

type verifyExactCatalogState struct {
	previousKey  []byte
	previousTile uint32
	leaves       uint32
}

// walkExactCatalogPage proves one PagePrimaryExactCatalog page and, for a
// level-1 page, its level-0 children; every referenced term leaf opens with a
// matching identity and is recorded for the overlap check.
func (w *verifyWalker) walkExactCatalogPage(
	ref storeio.PageRef, bounds storeio.PrimaryExactIndexBounds,
	allowIndex bool, state *verifyExactCatalogState,
) {
	w.record(ref, "primary-exact-catalog")
	page, ok := w.openAndCheckIdentity(ref, "primary-exact-catalog")
	if !ok {
		return
	}
	view, err := storeio.OpenPrimaryExactCatalogPage(page, ref, bounds)
	if err != nil {
		w.fail("primary-exact-catalog", ref.Offset, ref.LogicalID,
			"admit: %v", err)
		return
	}
	w.count("primary-exact-catalog")
	if view.Level() == 1 {
		if !allowIndex {
			w.fail("primary-exact-catalog", ref.Offset, ref.LogicalID,
				"catalog tree exceeds depth 2")
			return
		}
		for i := 0; i < view.Len(); i++ {
			child, ok := view.Child(uint32(i))
			if !ok {
				w.fail("primary-exact-catalog", ref.Offset, ref.LogicalID,
					"catalog child %d", i)
				return
			}
			w.walkExactCatalogPage(child, bounds, false, state)
		}
		return
	}
	if err := view.ForEachEntry(func(entry storeio.PrimaryExactCatalogEntry) error {
		w.record(entry.Leaf, "primary-exact-leaf")
		leaf, ok := w.openAndCheckIdentity(entry.Leaf, "primary-exact-leaf")
		if !ok {
			return nil
		}
		payload, err := storeio.OpenPrimaryExactLeafPage(
			leaf, entry.Leaf, bounds,
		)
		if err != nil {
			w.fail("primary-exact-leaf", entry.Leaf.Offset,
				entry.Leaf.LogicalID, "admit: %v", err)
			return nil
		}
		var allLive [storeio.TermPostingTileChunks]uint64
		for i := range allLive {
			allLive[i] = ^uint64(0)
		}
		view, err := storeio.OpenIndexTermLeaf(
			payload, w.storeID,
			func(uint32) *[storeio.TermPostingTileChunks]uint64 {
				return &allLive
			},
		)
		if err != nil {
			w.fail("primary-exact-leaf", entry.Leaf.Offset,
				entry.Leaf.LogicalID, "term codec: %v", err)
			return nil
		}
		it := view.Ordered()
		key, match, ok := it.Next()
		if !ok {
			w.fail("primary-exact-leaf", entry.Leaf.Offset,
				entry.Leaf.LogicalID, "empty term leaf")
			return nil
		}
		maskIt := match.MaskIterator()
		firstTile, _, ok := maskIt.Next()
		if !ok {
			w.fail("primary-exact-leaf", entry.Leaf.Offset,
				entry.Leaf.LogicalID, "first term has no postings")
			return nil
		}
		prefix := key
		if len(prefix) > storeio.PrimaryExactCatalogPrefixBytes {
			prefix = prefix[:storeio.PrimaryExactCatalogPrefixBytes]
		}
		flags := uint8(0)
		if entry.Flags&storeio.PrimaryExactCatalogPiece != 0 {
			flags |= storeio.PrimaryExactCatalogPiece
			if view.Len() != 1 {
				w.fail("primary-exact-leaf", entry.Leaf.Offset,
					entry.Leaf.LogicalID, "piece leaf has %d terms", view.Len())
			}
		}
		if storeio.IndexTermLeafRunCut(
			storeio.IndexTermRouteHash(w.storeID, key),
		) {
			flags |= storeio.PrimaryExactCatalogRunCut
		}
		if !bytes.Equal(entry.Prefix, prefix) ||
			entry.FirstTile != firstTile || entry.Flags != flags {
			w.fail("primary-exact-leaf", entry.Leaf.Offset,
				entry.Leaf.LogicalID, "catalog metadata mismatch")
			return nil
		}
		if state.leaves != 0 {
			cmp := bytes.Compare(state.previousKey, key)
			if cmp > 0 || cmp == 0 && state.previousTile >= firstTile {
				w.fail("primary-exact-leaf", entry.Leaf.Offset,
					entry.Leaf.LogicalID, "catalog term order")
				return nil
			}
		}
		state.previousKey = append(state.previousKey[:0], key...)
		state.previousTile = firstTile
		state.leaves++
		w.count("primary-exact-leaf")
		return nil
	}); err != nil {
		w.fail("primary-exact-catalog", ref.Offset, ref.LogicalID,
			"entries: %v", err)
	}
}

func (w *verifyWalker) record(ref storeio.PageRef, kind string) {
	// Record the extent claimed by the routing reference regardless of whether
	// the page itself later opens, so a corrupt page that is nonetheless referenced
	// still participates in aliasing and free-overlap detection.
	if uint64(ref.Length) == 0 ||
		ref.Offset+uint64(ref.Length) < ref.Offset ||
		ref.Offset+uint64(ref.Length) > w.fileEnd {
		return
	}
	w.reachable = append(w.reachable, reachExtent{
		offset: ref.Offset, length: uint64(ref.Length),
		logical: ref.LogicalID, kind: kind,
	})
}

func (w *verifyWalker) readExtent(offset, length uint64) ([]byte, error) {
	if length == 0 || offset+length < offset || offset+length > w.fileEnd {
		return nil, fmt.Errorf(
			"extent [%d,%d) out of file bounds (end %d)",
			offset, offset+length, w.fileEnd,
		)
	}
	buf := make([]byte, length)
	if _, err := w.file.ReadAt(buf, int64(offset)); err != nil {
		return nil, err
	}
	return buf, nil
}

// openAndCheckIdentity reads one common page and proves its checksum plus that
// the encoded identity matches the reference that reached it. Grafting a page
// from another file or generation, or aliasing the wrong logical page at a valid
// offset, fails here before any typed decoder trusts the payload.
func (w *verifyWalker) openAndCheckIdentity(
	ref storeio.PageRef, kind string,
) ([]byte, bool) {
	page, err := w.readExtent(ref.Offset, uint64(ref.Length))
	if err != nil {
		w.fail(kind, ref.Offset, ref.LogicalID, "read: %v", err)
		return nil, false
	}
	header, _, err := storeio.OpenPage(page)
	if err != nil {
		w.fail(kind, ref.Offset, ref.LogicalID, "open: %v", err)
		return nil, false
	}
	if header.Kind != ref.Kind {
		w.fail(kind, ref.Offset, ref.LogicalID,
			"page kind %d does not match reference kind %d",
			header.Kind, ref.Kind)
		return nil, false
	}
	if header.LogicalID != ref.LogicalID || header.Generation != ref.Generation {
		w.fail(kind, ref.Offset, ref.LogicalID,
			"page identity logical=%d generation=%d does not match reference logical=%d generation=%d",
			header.LogicalID, header.Generation, ref.LogicalID, ref.Generation)
		return nil, false
	}
	if header.StoreID != w.storeID {
		w.fail(kind, ref.Offset, ref.LogicalID, "grafted StoreID")
		return nil, false
	}
	return page, true
}

// walkStateRefs validates every non-primary top-level reference the state root
// publishes and the immutable page-catalog run. The primary root's subtree is
// walked separately by walkPrimary.
func (w *verifyWalker) walkStateRefs() {
	// The ordered-primary graph and its exact-index root are walked by
	// walkPrimary and walkExactIndexes.
	w.walkPageCatalog()
}

func (w *verifyWalker) walkPageCatalog() {
	extent, ok := storeio.StateRootPageCatalogExtent(w.root)
	if !ok || extent.Length == 0 {
		return
	}
	head := w.root.PageCatalogHead
	segments := extent.Length / uint64(w.pageSize)
	for i := range segments {
		ref := storeio.PageRef{
			Offset:     head.Offset + i*uint64(w.pageSize),
			LogicalID:  head.LogicalID + i,
			Generation: head.Generation,
			Length:     w.pageSize,
			Kind:       storeio.PageCatalogSegment,
		}
		w.record(ref, "page-catalog")
		page, err := w.readExtent(ref.Offset, uint64(ref.Length))
		if err != nil {
			w.fail("page-catalog", ref.Offset, ref.LogicalID, "read: %v", err)
			continue
		}
		header, _, err := storeio.OpenPage(page)
		if err != nil {
			w.fail("page-catalog", ref.Offset, ref.LogicalID, "open: %v", err)
			continue
		}
		if header.Kind != storeio.PageCatalogSegment ||
			header.LogicalID != ref.LogicalID || header.StoreID != w.storeID {
			w.fail("page-catalog", ref.Offset, ref.LogicalID,
				"catalog segment identity")
			continue
		}
		w.count("page-catalog")
	}
}

// walkPrimary exhaustively descends the ordered primary graph. Node shapes,
// levels, and stable IDs are proven exactly as validateOpenedPrimaryGraph proves
// them, and the walk then continues into anchors and leaves — which that opener
// deliberately skips — so every durable primary page is checksum-, identity-,
// and structure-validated, and every live key is ordering-checked.
func (w *verifyWalker) walkPrimary() {
	ref := w.root.PrimaryRoot
	w.record(ref, "primary-catalog")
	page, err := w.readExtent(ref.Offset, uint64(ref.Length))
	if err != nil {
		w.fail("primary-catalog", ref.Offset, ref.LogicalID, "read: %v", err)
		return
	}
	node, err := storeio.OpenGlobalTabletCatalogNode(page, ref, w.catBounds)
	if err != nil {
		w.fail("primary-catalog", ref.Offset, ref.LogicalID, "open: %v", err)
		return
	}
	if node.Level() != storeio.GlobalTabletCatalogRoot ||
		node.PageID() != 0 || node.Count() == 0 {
		w.fail("primary-catalog", ref.Offset, ref.LogicalID,
			"catalog root shape level=%d pageID=%d count=%d",
			node.Level(), node.PageID(), node.Count())
		return
	}
	w.count("primary-catalog")
	childLevel := node.ChildLevel()
	cursor := node.LowerBound(nil)
	for {
		route, ok := cursor.Route()
		if !ok {
			w.fail("primary-catalog", ref.Offset, ref.LogicalID,
				"catalog root cursor")
			return
		}
		switch childLevel {
		case storeio.GlobalTabletCatalogLeaf:
			w.walkCatalogLeaf(route)
		case storeio.GlobalTabletCatalogBranch:
			w.walkCatalogBranch(route)
		default:
			w.fail("primary-catalog", route.Ref.Offset, route.Ref.LogicalID,
				"catalog root child level %d", childLevel)
		}
		if !cursor.Next() {
			break
		}
	}
}

func (w *verifyWalker) walkCatalogBranch(route storeio.GlobalTabletCatalogNodeRoute) {
	w.record(route.Ref, "primary-catalog-branch")
	page, err := w.readExtent(route.Ref.Offset, uint64(route.Ref.Length))
	if err != nil {
		w.fail("primary-catalog-branch", route.Ref.Offset, route.Ref.LogicalID,
			"read: %v", err)
		return
	}
	branch, err := storeio.OpenGlobalTabletCatalogNode(page, route.Ref, w.catBounds)
	if err != nil {
		w.fail("primary-catalog-branch", route.Ref.Offset, route.Ref.LogicalID,
			"open: %v", err)
		return
	}
	if branch.Level() != storeio.GlobalTabletCatalogBranch ||
		branch.PageID() != route.ID ||
		branch.ChildLevel() != storeio.GlobalTabletCatalogLeaf ||
		branch.Count() == 0 {
		w.fail("primary-catalog-branch", route.Ref.Offset, route.Ref.LogicalID,
			"catalog branch shape")
		return
	}
	w.count("primary-catalog-branch")
	cursor := branch.LowerBound(nil)
	for {
		leafRoute, ok := cursor.Route()
		if !ok {
			w.fail("primary-catalog-branch", route.Ref.Offset, route.Ref.LogicalID,
				"catalog branch cursor")
			return
		}
		w.walkCatalogLeaf(leafRoute)
		if !cursor.Next() {
			break
		}
	}
}

func (w *verifyWalker) walkCatalogLeaf(route storeio.GlobalTabletCatalogNodeRoute) {
	w.record(route.Ref, "primary-catalog-leaf")
	page, err := w.readExtent(route.Ref.Offset, uint64(route.Ref.Length))
	if err != nil {
		w.fail("primary-catalog-leaf", route.Ref.Offset, route.Ref.LogicalID,
			"read: %v", err)
		return
	}
	leaf, err := storeio.OpenGlobalTabletCatalogNode(page, route.Ref, w.catBounds)
	if err != nil {
		w.fail("primary-catalog-leaf", route.Ref.Offset, route.Ref.LogicalID,
			"open: %v", err)
		return
	}
	if leaf.Level() != storeio.GlobalTabletCatalogLeaf ||
		leaf.PageID() != route.ID || leaf.Count() == 0 {
		w.fail("primary-catalog-leaf", route.Ref.Offset, route.Ref.LogicalID,
			"catalog leaf shape")
		return
	}
	w.count("primary-catalog-leaf")
	cursor := leaf.LowerBound(nil)
	for {
		tabletRoute, ok := cursor.Route()
		if !ok {
			w.fail("primary-catalog-leaf", route.Ref.Offset, route.Ref.LogicalID,
				"catalog leaf cursor")
			return
		}
		w.walkTablet(tabletRoute.Ref)
		if !cursor.Next() {
			break
		}
	}
}

func (w *verifyWalker) walkTablet(ref storeio.PageRef) {
	w.record(ref, "tablet-root")
	page, err := w.readExtent(ref.Offset, uint64(ref.Length))
	if err != nil {
		w.fail("tablet-root", ref.Offset, ref.LogicalID, "read: %v", err)
		return
	}
	tablet, err := storeio.OpenGlobalTabletCatalogTabletRoot(page, ref, w.catBounds)
	if err != nil {
		w.fail("tablet-root", ref.Offset, ref.LogicalID, "open: %v", err)
		return
	}
	w.count("tablet-root")
	locatorRef, ok := tablet.LocatorRef()
	if !ok {
		w.fail("tablet-root", ref.Offset, ref.LogicalID, "missing locator reference")
		return
	}
	w.walkLocator(locatorRef)
	for rank := 0; rank < tablet.AnchorCount(); rank++ {
		anchorRoute, ok := tablet.AnchorAt(rank)
		if !ok {
			w.fail("primary-anchor", ref.Offset, ref.LogicalID,
				"anchor iteration rank %d", rank)
			continue
		}
		w.walkAnchor(&tablet, anchorRoute)
	}
}

func (w *verifyWalker) walkLocator(ref storeio.PageRef) {
	w.record(ref, "primary-locator")
	page, err := w.readExtent(ref.Offset, uint64(ref.Length))
	if err != nil {
		w.fail("primary-locator", ref.Offset, ref.LogicalID, "read: %v", err)
		return
	}
	if _, err := storeio.OpenGlobalTabletCatalogLocator(page, ref, w.catBounds); err != nil {
		w.fail("primary-locator", ref.Offset, ref.LogicalID, "open: %v", err)
		return
	}
	w.count("primary-locator")
}

func (w *verifyWalker) walkAnchor(
	tablet *storeio.GlobalTabletCatalogTabletRootView,
	route storeio.GlobalTabletCatalogAnchorRoute,
) {
	w.record(route.Ref, "primary-anchor")
	page, err := w.readExtent(route.Ref.Offset, uint64(route.Ref.Length))
	if err != nil {
		w.fail("primary-anchor", route.Ref.Offset, route.Ref.LogicalID,
			"read: %v", err)
		return
	}
	// The context-free admission proof binds anchor identity, structure, leaf
	// handles, and snapshot bounds before the rooted iterator trusts any offset.
	if err := storeio.ValidateGlobalTabletCatalogAnchor(
		page, route.Ref, w.catBounds,
	); err != nil {
		w.fail("primary-anchor", route.Ref.Offset, route.Ref.LogicalID,
			"validate: %v", err)
		return
	}
	anchor, err := storeio.OpenGlobalTabletCatalogAnchor(page, tablet, route.PageID)
	if err != nil {
		w.fail("primary-anchor", route.Ref.Offset, route.Ref.LogicalID,
			"open: %v", err)
		return
	}
	w.count("primary-anchor")
	for rank := 0; rank < anchor.Count(); rank++ {
		leafRoute, ok := anchor.RouteAt(rank, 0)
		if !ok {
			w.fail("primary-leaf", route.Ref.Offset, route.Ref.LogicalID,
				"anchor leaf iteration rank %d", rank)
			continue
		}
		w.walkLeaf(leafRoute)
	}
}

func (w *verifyWalker) walkLeaf(route storeio.SegmentedTabletRouterRoute) {
	w.record(route.Ref, "primary-leaf")
	page, err := w.readExtent(route.Ref.Offset, uint64(route.Ref.Length))
	if err != nil {
		w.fail("primary-leaf", route.Ref.Offset, route.Ref.LogicalID,
			"read: %v", err)
		return
	}
	if storeio.PrimaryLeafClass(page) != storeio.CommonPrimaryLeafUnified {
		w.fail("primary-leaf", route.Ref.Offset, route.Ref.LogicalID,
			"non-unified leaf class %d", storeio.PrimaryLeafClass(page))
		return
	}
	uv, err := storeio.OpenCommonPrimaryUnifiedLeaf(
		page, w.storeID, route.Bucket, route.Ref,
		w.root.Generation, w.leafBounds,
	)
	if err != nil {
		w.fail("primary-leaf", route.Ref.Offset, route.Ref.LogicalID,
			"open unified: %v", err)
		return
	}
	w.count("primary-leaf")
	for rank := 0; rank < uv.Len(); rank++ {
		key, ok := uv.RowAt(rank)
		if !ok {
			w.fail("primary-leaf", route.Ref.Offset, route.Ref.LogicalID,
				"unified row %d", rank)
			return
		}
		w.checkLeafOrder(route, key)
	}
}

// checkLeafOrder proves globally non-decreasing keys across leaves. The catalog,
// tablet, anchor, and leaf iterators are each lexical, so a left-to-right
// traversal must yield increasing keys. A leaf holding a key outside its routed
// fence breaks this order; the check is a sound, cross-leaf proof of the
// fences-ordered / keys-within-fences invariant without re-deriving separators.
func (w *verifyWalker) checkLeafOrder(
	route storeio.SegmentedTabletRouterRoute, key []byte,
) {
	if w.haveLast && bytes.Compare(w.lastKey, key) >= 0 {
		w.fail("leaf-order", route.Ref.Offset, route.Ref.LogicalID,
			"key %q not greater than previous %q", key, w.lastKey)
	}
	w.lastKey = append(w.lastKey[:0], key...)
	w.haveLast = true
	w.report.Documents++
}

// checkReachableOverlap proves that no two reachable pages claim overlapping
// physical bytes. After sorting by offset, an overlap between any two extents
// implies an overlap between an adjacent pair, so the adjacent scan is complete.
func (w *verifyWalker) checkReachableOverlap() {
	sort.Slice(w.reachable, func(i, j int) bool {
		return w.reachable[i].offset < w.reachable[j].offset
	})
	for i := 1; i < len(w.reachable); i++ {
		prev := w.reachable[i-1]
		cur := w.reachable[i]
		if prev.offset+prev.length > cur.offset {
			w.fail("reachable-overlap", cur.offset, cur.logical,
				"%s page overlaps preceding %s page at offset %d",
				cur.kind, prev.kind, prev.offset)
		}
	}
}

// walkFreeSet reconstructs the durable free set exactly as the store does at
// open (inline delta over external delta chain over the segment-indexed image,
// every segment forced resident) and proves no free extent overlaps a reachable
// page. A store must never advertise a live page as reusable.
func (w *verifyWalker) walkFreeSet(inlineFree *storeio.InlineFreeDelta) {
	residentBudget := max(int64(w.maxPageSize)*64, 8<<20)
	cache, err := storeio.NewPageCache(w.file, storeio.PageCacheOptions{
		PageSize:      int(w.pageSize),
		MaxPageSize:   int(w.maxPageSize),
		ResidentBytes: residentBudget,
		StoreID:       w.storeID,
		Backend:       storeio.BackendPortable,
	})
	if err != nil {
		w.fail("free-log", 0, 0, "open free-set reader: %v", err)
		return
	}
	defer cache.Close()

	protected, _ := storeio.StateRootPageCatalogExtent(w.root)
	bounds := storeio.FreeLogBounds{
		FileEnd:         w.fileEnd,
		NextLogicalID:   w.root.NextLogicalID,
		ProtectedExtent: protected,
	}
	// A page-quantum extent is the smallest a free extent can be, so the number
	// of distinct free extents cannot exceed the file's page count. That is the
	// exact arena the replay API requires to be pre-sized.
	limit := int(w.fileEnd/uint64(w.pageSize)) + 1
	dst := make([]storeio.FreeExtent, 0, limit)
	extents, _, err := storeio.ReplayInlineFreeLog(
		cache, inlineFree, bounds, dst, limit, math.MaxInt32,
	)
	if err != nil {
		w.fail("free-log", 0, 0, "replay free set: %v", err)
		return
	}
	w.report.FreeExtents = len(extents)

	free := make([]storeio.FreeExtent, len(extents))
	copy(free, extents)
	sort.Slice(free, func(i, j int) bool { return free[i].Offset < free[j].Offset })
	// w.reachable is already offset-sorted by checkReachableOverlap.
	at := 0
	for _, e := range free {
		for at < len(w.reachable) &&
			w.reachable[at].offset+w.reachable[at].length <= e.Offset {
			at++
		}
		if at < len(w.reachable) && w.reachable[at].offset < e.Offset+e.Length {
			hit := w.reachable[at]
			w.fail("free-overlap", e.Offset, hit.logical,
				"free extent [%d,%d) overlaps reachable %s page at offset %d",
				e.Offset, e.Offset+e.Length, hit.kind, hit.offset)
		}
	}
}

// SalvageReport summarises one catalog-loss recovery. It is the salvage
// counterpart of VerifyReport: every counter is populated so a recovery can be
// audited even when it completed without error.
type SalvageReport struct {
	LeavesScanned    int
	BucketsKept      int
	Documents        int
	OverflowSkipped  int
	DuplicateSkipped int
	OutputFileEnd    int64
}

// salvageSelectingGeneration caps the leaf selecting-generation used while
// scanning by identity alone. It is the largest legal page generation, so every
// structurally valid leaf of any real generation opens against it.
const salvageSelectingGeneration = uint64(1)<<48 - 1

type salvageLeafHit struct {
	generation uint64
	offset     uint64
	length     uint64
}

// Salvage realises the catalog-loss recovery property: it finds every valid
// PagePrimaryLeaf extent by checksum and self-describing identity alone —
// ignoring the routing graph entirely — extracts every live inline (key, value)
// across those leaves in lexical order, and writes a fresh valid store through
// the ordinary bulk primary path (CreateFromPrimary) to out. Because leaves are
// self-describing (BucketID plus complete keys), a store whose catalog and
// tablet roots are destroyed is fully recoverable from its surviving leaves.
//
// src is read-only. out must be an empty file. When several generations of one
// leaf bucket survive, the highest generation wins, which is the live version in
// a store without in-flight splits. Overflow (non-inline) values are counted and
// skipped: CreateFromPrimary is inline-only, so a salvage of an inline store is
// exact and a salvage of an overflow-bearing store is reported as partial.
func Salvage(src, out *os.File, options Options) (SalvageReport, error) {
	var report SalvageReport
	if src == nil || out == nil {
		return report, fmt.Errorf(
			"vibedb: salvage requires non-nil source and output files",
		)
	}
	bootstrap, err := storeio.DiscoverMutableInlineBootstrap(src)
	if err != nil {
		return report, fmt.Errorf("vibedb: salvage cannot read geometry: %w", err)
	}
	pageSize := bootstrap.PageSize
	info, err := src.Stat()
	if err != nil {
		return report, err
	}
	if info.Size() < 0 {
		return report, fmt.Errorf("vibedb: salvage source has no content")
	}
	fileSize := uint64(info.Size())
	// Bounds are physical: floor the file to the page quantum so a torn tail
	// cannot make an otherwise valid leaf appear to run past the end.
	fileEnd := fileSize - fileSize%uint64(pageSize)
	layout, err := storeio.MutableStoreLayout(pageSize)
	if err != nil {
		return report, err
	}

	// The overflow-reference bound needs a NextLogicalID ceiling. Leaf identities
	// live below PrimaryFirstDynamicLogicalID, so that floor is always sufficient;
	// a recoverable root only widens it.
	nextLogical := storeio.PrimaryFirstDynamicLogicalID
	scratch := make([]byte, bootstrap.MaxPageSize)
	if _, rootRec, _, _, rerr := storeio.RecoverInlineStateRootWithFallback(
		src, pageSize, scratch,
	); rerr == nil && rootRec.NextLogicalID > nextLogical {
		nextLogical = rootRec.NextLogicalID
	}
	leafBounds := storeio.CommonPrimaryLeafBounds{
		FileEnd:           fileEnd,
		NextLogicalID:     nextLogical,
		AllocationQuantum: pageSize,
	}

	best := map[uint64]salvageLeafHit{}
	for offset := layout.DataStart; offset+uint64(pageSize) <= fileEnd; {
		readLen := uint64(bootstrap.MaxPageSize)
		if remaining := fileEnd - offset; remaining < readLen {
			readLen = remaining
		}
		buf := make([]byte, readLen)
		n, _ := src.ReadAt(buf, int64(offset))
		header, _, err := storeio.OpenPage(buf[:n])
		if err != nil {
			// No valid page begins here; step one quantum and keep scanning.
			offset += uint64(pageSize)
			continue
		}
		extent := uint64(header.PageSize)
		if header.Kind != storeio.PagePrimaryLeaf ||
			header.LogicalID < storeio.PrimaryLeafLogicalIDBase ||
			header.LogicalID >= storeio.PrimaryLeafLogicalIDLimit {
			offset += extent
			continue
		}
		bucket := storeio.BucketID(header.LogicalID - storeio.PrimaryLeafLogicalIDBase)
		expected := storeio.PageRef{
			Offset: offset, LogicalID: header.LogicalID,
			Generation: header.Generation, Length: header.PageSize,
			Kind: storeio.PagePrimaryLeaf,
		}
		_, leafErr := storeio.OpenCommonPrimaryUnifiedLeaf(
			buf[:extent], bootstrap.StoreID, bucket, expected,
			salvageSelectingGeneration, leafBounds,
		)
		leafValid := storeio.PrimaryLeafClass(buf[:extent]) ==
			storeio.CommonPrimaryLeafUnified && leafErr == nil
		if !leafValid {
			// Checksum-valid page in the leaf identity band but not a valid leaf.
			offset += extent
			continue
		}
		report.LeavesScanned++
		prev, seen := best[header.LogicalID]
		if !seen || header.Generation > prev.generation ||
			(header.Generation == prev.generation && offset > prev.offset) {
			best[header.LogicalID] = salvageLeafHit{
				generation: header.Generation, offset: offset, length: extent,
			}
		}
		offset += extent
	}

	type salvagedRecord struct{ key, value []byte }
	var records []salvagedRecord
	for logicalID, hit := range best {
		buf := make([]byte, hit.length)
		if _, err := src.ReadAt(buf, int64(hit.offset)); err != nil {
			return report, err
		}
		bucket := storeio.BucketID(logicalID - storeio.PrimaryLeafLogicalIDBase)
		expected := storeio.PageRef{
			Offset: hit.offset, LogicalID: logicalID,
			Generation: hit.generation, Length: uint32(hit.length),
			Kind: storeio.PagePrimaryLeaf,
		}
		uv, err := storeio.OpenCommonPrimaryUnifiedLeaf(
			buf, bootstrap.StoreID, bucket, expected,
			salvageSelectingGeneration, leafBounds,
		)
		if err != nil {
			return report, fmt.Errorf(
				"vibedb: salvage re-open unified leaf: %w", err,
			)
		}
		report.BucketsKept++
		for rank := 0; rank < uv.Len(); rank++ {
			key, body, overflow, ok := uv.RowRawAt(rank)
			if !ok {
				return report, fmt.Errorf(
					"vibedb: salvage unified row %d", rank,
				)
			}
			if overflow {
				report.OverflowSkipped++
				continue
			}
			value, rendered := uv.AppendRowBody(nil, body)
			if !rendered {
				return report, fmt.Errorf(
					"vibedb: salvage unified row body %d", rank,
				)
			}
			records = append(records, salvagedRecord{
				key: append([]byte(nil), key...), value: value,
			})
		}
	}
	sort.Slice(records, func(i, j int) bool {
		return bytes.Compare(records[i].key, records[j].key) < 0
	})

	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		return report, err
	}
	for _, r := range records {
		if err := builder.Append(string(r.key), r.value); err != nil {
			if errors.Is(err, store.ErrDuplicateKey) {
				report.DuplicateSkipped++
				continue
			}
			return report, err
		}
		report.Documents++
	}
	built, err := builder.Build()
	if err != nil {
		return report, err
	}
	fileEndOut, err := CreateFromPrimary(built, out, options)
	if err != nil {
		return report, err
	}
	report.OutputFileEnd = fileEndOut
	return report, nil
}
