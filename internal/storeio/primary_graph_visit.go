package storeio

import "fmt"

// AppendPrimaryTabletKeyBounds authenticates one tablet and appends the first
// and last document keys by opening only its boundary anchor and leaf pages.
// Caller-owned fixed key buffers keep final vector-to-catalog folding
// allocation-free.
func AppendPrimaryTabletKeyBounds(
	cache *PageCache,
	ref PageRef,
	bounds GlobalTabletCatalogBounds,
	firstDst, lastDst []byte,
) ([]byte, []byte, error) {
	if cache == nil || ref.Kind != PageTabletRoute || cap(firstDst)-len(firstDst) < CommonPrimaryLeafMaxKeyBytes || cap(lastDst)-len(lastDst) < CommonPrimaryLeafMaxKeyBytes {
		return firstDst, lastDst, fmt.Errorf("%w: tablet key bounds", ErrInvalidWrite)
	}
	tabletLease, err := cache.Acquire(ref)
	if err != nil {
		return firstDst, lastDst, err
	}
	tablet, err := OpenGlobalTabletCatalogTabletRoot(tabletLease.Page(), ref, bounds)
	if err != nil || tablet.AnchorCount() == 0 {
		tabletLease.Release()
		if err != nil {
			return firstDst, lastDst, err
		}
		return firstDst, lastDst, ErrGlobalTabletCatalogCorrupt
	}
	appendBoundary := func(anchorRank, leafRank, row int, dst []byte) ([]byte, error) {
		anchorRoute, ok := tablet.AnchorAt(anchorRank)
		if !ok {
			return dst, ErrGlobalTabletCatalogCorrupt
		}
		anchorLease, err := cache.Acquire(anchorRoute.Ref)
		if err != nil {
			return dst, err
		}
		if err := ValidateGlobalTabletCatalogAnchor(anchorLease.Page(), anchorRoute.Ref, bounds); err != nil {
			anchorLease.Release()
			return dst, err
		}
		anchor, err := OpenGlobalTabletCatalogAnchor(anchorLease.Page(), &tablet, anchorRoute.PageID)
		if err != nil {
			anchorLease.Release()
			return dst, err
		}
		if leafRank < 0 {
			leafRank = anchor.Count() - 1
		}
		leafRoute, ok := anchor.RouteAt(leafRank, 0)
		anchorLease.Release()
		if !ok {
			return dst, ErrGlobalTabletCatalogCorrupt
		}
		leafLease, err := cache.Acquire(leafRoute.Ref)
		if err != nil {
			return dst, err
		}
		leaf, err := OpenCompactPrimaryStripe(leafLease.Page(), bounds.StoreID, leafRoute.Bucket, leafRoute.Ref, bounds.SelectedRootGeneration, CommonPrimaryLeafBounds{FileEnd: bounds.FileEnd, NextLogicalID: bounds.NextLogicalID, AllocationQuantum: uint32(cache.options.PageSize)})
		if err != nil {
			leafLease.Release()
			return dst, err
		}
		if row < 0 {
			row = leaf.Len() - 1
		}
		dst, ok = leaf.AppendKey(dst, row)
		leafLease.Release()
		if !ok {
			return dst, ErrGlobalTabletCatalogCorrupt
		}
		return dst, nil
	}
	firstDst, err = appendBoundary(0, 0, 0, firstDst)
	if err == nil {
		lastDst, err = appendBoundary(tablet.AnchorCount()-1, -1, -1, lastDst)
	}
	tabletLease.Release()
	return firstDst, lastDst, err
}

// VisitPrimaryGraphRefs authenticates and visits every page reachable from one
// primary root, including overflow chains, in deterministic depth-first order.
// It retains at most the catalog depth plus one tablet/anchor/leaf lease, so a
// post-cutover retirement driver can persist a cursor and enqueue old extents
// without materializing the graph's reference set.
func VisitPrimaryGraphRefs(
	cache *PageCache,
	root PageRef,
	bounds GlobalTabletCatalogBounds,
	visit func(PageRef) error,
) error {
	if cache == nil || root == (PageRef{}) || visit == nil {
		return fmt.Errorf("%w: primary graph visitor", ErrInvalidWrite)
	}
	walker := primaryGraphRefWalker{
		cache: cache, bounds: bounds, visit: visit,
		leafBounds: CommonPrimaryLeafBounds{
			FileEnd: bounds.FileEnd, NextLogicalID: bounds.NextLogicalID,
			AllocationQuantum: uint32(cache.options.PageSize),
		},
	}
	return walker.catalog(root, GlobalTabletCatalogRoot)
}

type primaryGraphRefWalker struct {
	cache      *PageCache
	bounds     GlobalTabletCatalogBounds
	leafBounds CommonPrimaryLeafBounds
	visit      func(PageRef) error
}

func (w *primaryGraphRefWalker) catalog(
	ref PageRef,
	want GlobalTabletCatalogNodeLevel,
) error {
	lease, err := w.cache.Acquire(ref)
	if err != nil {
		return err
	}
	view, err := OpenGlobalTabletCatalogNode(lease.Page(), ref, w.bounds)
	if err != nil {
		lease.Release()
		return err
	}
	if view.Level() != want || view.Count() == 0 {
		lease.Release()
		return ErrGlobalTabletCatalogCorrupt
	}
	if err := w.visit(ref); err != nil {
		lease.Release()
		return err
	}
	cursor := view.LowerBound(nil)
	for {
		route, ok := cursor.Route()
		if !ok {
			lease.Release()
			return ErrGlobalTabletCatalogCorrupt
		}
		switch view.Level() {
		case GlobalTabletCatalogRoot:
			if view.ChildLevel() == GlobalTabletCatalogBranch {
				err = w.catalog(route.Ref, GlobalTabletCatalogBranch)
			} else {
				err = w.catalog(route.Ref, GlobalTabletCatalogLeaf)
			}
		case GlobalTabletCatalogBranch:
			err = w.catalog(route.Ref, GlobalTabletCatalogLeaf)
		case GlobalTabletCatalogLeaf:
			err = w.tablet(route.Ref)
		default:
			err = ErrGlobalTabletCatalogCorrupt
		}
		if err != nil {
			lease.Release()
			return err
		}
		if !cursor.Next() {
			break
		}
	}
	lease.Release()
	return nil
}

func (w *primaryGraphRefWalker) tablet(ref PageRef) error {
	lease, err := w.cache.Acquire(ref)
	if err != nil {
		return err
	}
	tablet, err := OpenGlobalTabletCatalogTabletRoot(lease.Page(), ref, w.bounds)
	if err != nil {
		lease.Release()
		return err
	}
	if err := w.visit(ref); err != nil {
		lease.Release()
		return err
	}
	locator, ok := tablet.LocatorRef()
	if !ok {
		lease.Release()
		return ErrGlobalTabletCatalogCorrupt
	}
	locatorLease, err := w.cache.Acquire(locator)
	if err != nil {
		lease.Release()
		return err
	}
	_, err = OpenGlobalTabletCatalogLocator(locatorLease.Page(), locator, w.bounds)
	locatorLease.Release()
	if err != nil {
		lease.Release()
		return err
	}
	if err := w.visit(locator); err != nil {
		lease.Release()
		return err
	}
	for rank := 0; rank < tablet.AnchorCount(); rank++ {
		route, ok := tablet.AnchorAt(rank)
		if !ok {
			lease.Release()
			return ErrGlobalTabletCatalogCorrupt
		}
		if err := w.anchor(&tablet, route); err != nil {
			lease.Release()
			return err
		}
	}
	lease.Release()
	return nil
}

func (w *primaryGraphRefWalker) anchor(
	tablet *GlobalTabletCatalogTabletRootView,
	route GlobalTabletCatalogAnchorRoute,
) error {
	lease, err := w.cache.Acquire(route.Ref)
	if err != nil {
		return err
	}
	if err := ValidateGlobalTabletCatalogAnchor(lease.Page(), route.Ref, w.bounds); err != nil {
		lease.Release()
		return err
	}
	anchor, err := OpenGlobalTabletCatalogAnchor(lease.Page(), tablet, route.PageID)
	if err != nil {
		lease.Release()
		return err
	}
	if err := w.visit(route.Ref); err != nil {
		lease.Release()
		return err
	}
	for rank := 0; rank < anchor.Count(); rank++ {
		leaf, ok := anchor.RouteAt(rank, 0)
		if !ok {
			lease.Release()
			return ErrGlobalTabletCatalogCorrupt
		}
		if err := w.leaf(leaf); err != nil {
			lease.Release()
			return err
		}
	}
	lease.Release()
	return nil
}

func (w *primaryGraphRefWalker) leaf(route SegmentedTabletRouterRoute) error {
	lease, err := w.cache.Acquire(route.Ref)
	if err != nil {
		return err
	}
	view, err := OpenCompactPrimaryStripe(
		lease.Page(), w.bounds.StoreID, route.Bucket, route.Ref,
		w.bounds.SelectedRootGeneration, w.leafBounds,
	)
	if err != nil {
		lease.Release()
		return err
	}
	if err := w.visit(route.Ref); err != nil {
		lease.Release()
		return err
	}
	for row := 0; row < view.Len(); row++ {
		overflow, ok := view.OverflowRef(row)
		if !ok {
			continue
		}
		if err := w.overflow(overflow); err != nil {
			lease.Release()
			return err
		}
	}
	lease.Release()
	return nil
}

func (w *primaryGraphRefWalker) overflow(ref PageRef) error {
	for ref != (PageRef{}) {
		lease, err := w.cache.Acquire(ref)
		if err != nil {
			return err
		}
		view, err := OpenOverflowPage(
			lease.Page(), w.bounds.FileEnd, w.bounds.NextLogicalID,
			w.leafBounds.AllocationQuantum,
		)
		if err != nil {
			lease.Release()
			return err
		}
		if err := w.visit(ref); err != nil {
			lease.Release()
			return err
		}
		ref = view.Header().Next
		lease.Release()
	}
	return nil
}
