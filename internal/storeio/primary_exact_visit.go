package storeio

import "fmt"

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
	walker := primaryExactRefWalker{cache: cache, bounds: bounds, visit: visit}
	for index := uint32(0); index < uint32(view.Len()); index++ {
		entry, ok := view.Entry(index)
		if !ok {
			lease.Release()
			return ErrPrimaryExactIndexCorrupt
		}
		if entry.Catalog == (PageRef{}) {
			continue
		}
		seen, err := walker.catalog(entry.Catalog)
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
	cache  *PageCache
	bounds PrimaryExactIndexBounds
	visit  func(PageRef) error
}

func (w *primaryExactRefWalker) catalog(ref PageRef) (uint64, error) {
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
			childCount, err := w.catalog(child)
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
		_, err = OpenPrimaryExactLeafPage(leafLease.Page(), entry.Leaf, w.bounds)
		leafLease.Release()
		if err != nil {
			return err
		}
		if err := w.visit(entry.Leaf); err != nil {
			return err
		}
		count++
		return nil
	})
	lease.Release()
	return count, err
}
