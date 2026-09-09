package storeio

import "fmt"

// VisitPrimaryExactPackInventory validates and streams an authoritative pack
// inventory with bounded memory. Exact page and pack counts turn cycles,
// premature tails, and trailing links into corruption. Pack refs must remain
// globally sorted and nonoverlapping across inventory-page boundaries.
func VisitPrimaryExactPackInventory(cache *PageCache, head PageRef, packCount, pageCount uint32, bounds PrimaryExactIndexBounds, visit func(PageRef) error) error {
	if cache == nil || visit == nil || packCount == 0 != (head == (PageRef{})) || pageCount == 0 != (head == (PageRef{})) {
		return fmt.Errorf("%w: exact pack inventory", ErrInvalidWrite)
	}
	if head == (PageRef{}) {
		return nil
	}
	var decoder PrimaryExactPackDecoder
	if err := decoder.Prepare(PrimaryExactPackMaxPayloadBytes - PrimaryExactPackHeaderBytes); err != nil {
		return err
	}
	current := head
	var seenPacks uint32
	var previousEnd uint64
	for seenPages := uint32(0); seenPages < pageCount; seenPages++ {
		lease, err := cache.Acquire(current)
		if err != nil {
			return err
		}
		view, err := OpenPrimaryExactInventoryPage(lease.Page(), current, bounds)
		if err != nil {
			lease.Release()
			return err
		}
		remainingPages := pageCount - seenPages
		remainingPacks := packCount - seenPacks
		if uint32(view.Len()) > remainingPacks || remainingPages == 1 != (view.Next() == (PageRef{})) {
			lease.Release()
			return primaryExactCorrupt("inventory aggregate count")
		}
		if view.Len() != 0 {
			first, ok := view.Entry(0)
			if !ok || seenPacks != 0 && first.Offset < previousEnd {
				lease.Release()
				return primaryExactCorrupt("inventory aggregate order")
			}
		}
		if err = visit(current); err != nil {
			lease.Release()
			return err
		}
		for i := uint32(0); i < uint32(view.Len()); i++ {
			ref, ok := view.Entry(i)
			if !ok || seenPacks == packCount || seenPacks != 0 && ref.Offset < previousEnd {
				lease.Release()
				return primaryExactCorrupt("inventory aggregate order")
			}
			packLease, acquireErr := cache.Acquire(ref)
			if acquireErr != nil {
				lease.Release()
				return acquireErr
			}
			openErr := OpenPrimaryExactPackPage(packLease.Page(), ref, bounds, &decoder)
			packLease.Release()
			if openErr != nil {
				lease.Release()
				return openErr
			}
			if err := visit(ref); err != nil {
				lease.Release()
				return err
			}
			previousEnd = ref.Offset + uint64(ref.Length)
			seenPacks++
		}
		current = view.Next()
		lease.Release()
		if seenPages+1 < pageCount && current == (PageRef{}) {
			return primaryExactCorrupt("inventory premature tail")
		}
	}
	if current != (PageRef{}) || seenPacks != packCount {
		return primaryExactCorrupt("inventory aggregate count")
	}
	return nil
}

// VisitPrimaryExactIndexRefs authenticates and streams every page reachable
// from one exact-index root. The walk keeps at most the root and catalog depth
// leased, allowing a post-publication retirement driver to enqueue extents
// without retaining a graph-sized reference set.
func VisitPrimaryExactIndexRefs(
	cache *PageCache,
	root PageRef,
	bounds PrimaryExactIndexBounds,
	visit func(PageRef) error,
) error {
	if cache == nil || root == (PageRef{}) || visit == nil {
		return fmt.Errorf("%w: exact-index graph visitor", ErrInvalidWrite)
	}
	lease, err := cache.Acquire(root)
	if err != nil {
		return err
	}
	view, err := OpenPrimaryExactRootPage(lease.Page(), root, bounds)
	if err != nil {
		lease.Release()
		return err
	}
	if err := visit(root); err != nil {
		lease.Release()
		return err
	}
	walker := primaryExactRefWalker{
		cache: cache, bounds: bounds, visit: visit,
		seenPacks: make(map[PageRef]struct{}),
	}
	for index := uint32(0); index < uint32(view.Len()); index++ {
		entry, ok := view.Entry(index)
		if !ok {
			lease.Release()
			return ErrPrimaryExactIndexCorrupt
		}
		if entry.Catalog == (PageRef{}) {
			continue
		}
		seen, err := walker.catalog(entry.Catalog, index)
		if err != nil {
			lease.Release()
			return err
		}
		if seen != uint64(entry.LeafCount) {
			lease.Release()
			return primaryExactCorrupt("catalog leaf count")
		}
	}
	lease.Release()
	return nil
}

type primaryExactRefWalker struct {
	cache     *PageCache
	bounds    PrimaryExactIndexBounds
	visit     func(PageRef) error
	seenPacks map[PageRef]struct{}
}

func (w *primaryExactRefWalker) catalog(ref PageRef, indexID uint32) (uint64, error) {
	lease, err := w.cache.Acquire(ref)
	if err != nil {
		return 0, err
	}
	view, err := OpenPrimaryExactCatalogPage(lease.Page(), ref, w.bounds)
	if err != nil {
		lease.Release()
		return 0, err
	}
	if err := w.visit(ref); err != nil {
		lease.Release()
		return 0, err
	}
	var count uint64
	if view.Level() == 1 {
		for index := uint32(0); index < uint32(view.Len()); index++ {
			child, ok := view.Child(index)
			if !ok {
				lease.Release()
				return 0, ErrPrimaryExactIndexCorrupt
			}
			childCount, err := w.catalog(child, indexID)
			if err != nil || count > ^uint64(0)-childCount {
				lease.Release()
				if err != nil {
					return 0, err
				}
				return 0, primaryExactCorrupt("catalog leaf count")
			}
			count += childCount
		}
		lease.Release()
		return count, nil
	}
	err = view.ForEachEntry(func(entry PrimaryExactCatalogEntry) error {
		leafLease, err := w.cache.Acquire(entry.Leaf)
		if err != nil {
			return err
		}
		switch entry.Leaf.Kind {
		case PagePrimaryExactPack:
			var decoder PrimaryExactPackDecoder
			if err := decoder.Prepare(PrimaryExactPackMaxPayloadBytes - PrimaryExactPackHeaderBytes); err != nil {
				leafLease.Release()
				return err
			}
			if err := OpenPrimaryExactPackPage(leafLease.Page(), entry.Leaf, w.bounds, &decoder); err != nil {
				leafLease.Release()
				return err
			}
			if _, err := decoder.Member(int(entry.Member), indexID); err != nil {
				leafLease.Release()
				return err
			}
		default:
			_, err = OpenPrimaryExactLeafPage(leafLease.Page(), entry.Leaf, w.bounds)
			if err != nil {
				leafLease.Release()
				return err
			}
		}
		leafLease.Release()
		if _, seen := w.seenPacks[entry.Leaf]; !seen {
			w.seenPacks[entry.Leaf] = struct{}{}
			if err := w.visit(entry.Leaf); err != nil {
				return err
			}
		}
		count++
		return nil
	})
	lease.Release()
	return count, err
}
