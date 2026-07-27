package storeio

import "fmt"

// Float64DirectoryBounds constrains every old-generation page admitted while
// traversing or replacing the ordered stripe directory.
type Float64DirectoryBounds struct {
	FileEnd       uint64
	NextLogicalID uint64
}

// Float64DirectoryMutation reports one bounded root-to-leaf replacement. The
// directory has fixed 64-way fanout and a hard level ceiling, so retirement
// metadata never grows with database size.
type Float64DirectoryMutation struct {
	Root         PageRef
	Retired      [Float64DirectoryMaxLevel + 1]PageRef
	RetiredCount uint8
	Found        bool
	Changed      bool
}

func (m *Float64DirectoryMutation) retire(ref PageRef) error {
	if int(m.RetiredCount) == len(m.Retired) {
		return ErrFloat64DirectoryDepth
	}
	m.Retired[m.RetiredCount] = ref
	m.RetiredCount++
	return nil
}

// LookupFloat64Directory resolves the stripe whose lower bound is the greatest
// FirstChunk not exceeding target. The caller verifies target against the
// stripe's exact ChunkCount, which keeps range metadata in one authoritative
// place.
func LookupFloat64Directory(
	cache *PageCache,
	root PageRef,
	target uint32,
	bounds Float64DirectoryBounds,
	allocationQuantum uint32,
) (Float64DirectoryEntry, bool, error) {
	if root == (PageRef{}) {
		return Float64DirectoryEntry{}, false, nil
	}
	if cache == nil {
		return Float64DirectoryEntry{}, false, fmt.Errorf(
			"%w: nil float64 directory cache", ErrInvalidWrite,
		)
	}
	ref := root
	expectedLower := uint32(0)
	expectedLevel := uint8(0)
	haveLevel := false
	for {
		lease, err := cache.Acquire(ref)
		if err != nil {
			return Float64DirectoryEntry{}, false, err
		}
		view, err := OpenAdmittedFloat64Directory(
			lease.Page(), bounds.FileEnd, bounds.NextLogicalID,
			allocationQuantum,
		)
		if err != nil {
			lease.Release()
			return Float64DirectoryEntry{}, false, err
		}
		header := view.Header()
		first, ok := view.EntryAt(0)
		if !ok || first.FirstChunk != expectedLower ||
			haveLevel && header.Level != expectedLevel {
			lease.Release()
			return Float64DirectoryEntry{}, false,
				ErrFloat64CatalogCorrupt
		}
		if !haveLevel {
			expectedLevel = header.Level
			haveLevel = true
		}
		entry, ok := view.Floor(target)
		lease.Release()
		if !ok {
			return Float64DirectoryEntry{}, false,
				ErrFloat64CatalogCorrupt
		}
		if expectedLevel == 0 {
			return entry, true, nil
		}
		expectedLevel--
		expectedLower = entry.FirstChunk
		ref = entry.Ref
	}
}

// WalkFloat64Directory visits stripes in strictly increasing FirstChunk order
// while pinning at most one directory node. The callback receives value-only
// entries, so a full scan does not retain a page-sized pointer graph.
func WalkFloat64Directory(
	cache *PageCache,
	root PageRef,
	bounds Float64DirectoryBounds,
	allocationQuantum uint32,
	fn func(Float64DirectoryEntry) error,
) error {
	if fn == nil {
		return fmt.Errorf("%w: float64 directory walk", ErrInvalidWrite)
	}
	return WalkFloat64DirectoryLeaves(
		cache, root, bounds, allocationQuantum,
		func(leaf Float64DirectoryView) error {
			for i := 0; i < leaf.Len(); i++ {
				entry, _ := leaf.EntryAt(i)
				if err := fn(entry); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

// WalkFloat64DirectoryLeaves visits one ordered leaf at a time. The borrowed
// view remains valid only for the callback and keeps that leaf pinned. It lets
// scan readers form a stack-resident prefetch vector without making the
// walker's fixed scratch escape to the heap.
func WalkFloat64DirectoryLeaves(
	cache *PageCache,
	root PageRef,
	bounds Float64DirectoryBounds,
	allocationQuantum uint32,
	fn func(Float64DirectoryView) error,
) error {
	if root == (PageRef{}) {
		return nil
	}
	if cache == nil || fn == nil {
		return fmt.Errorf("%w: float64 directory walk", ErrInvalidWrite)
	}
	lease, err := cache.Acquire(root)
	if err != nil {
		return err
	}
	view, err := OpenAdmittedFloat64Directory(
		lease.Page(), bounds.FileEnd, bounds.NextLogicalID,
		allocationQuantum,
	)
	if err != nil {
		lease.Release()
		return err
	}
	level := view.Header().Level
	first, ok := view.EntryAt(0)
	lease.Release()
	if !ok || first.FirstChunk != 0 {
		return ErrFloat64CatalogCorrupt
	}
	return walkFloat64DirectoryPage(
		cache, root, level, 0, bounds, allocationQuantum,
		fn,
	)
}

func walkFloat64DirectoryPage(
	cache *PageCache,
	ref PageRef,
	expectedLevel uint8,
	expectedLower uint32,
	bounds Float64DirectoryBounds,
	allocationQuantum uint32,
	fn func(Float64DirectoryView) error,
) error {
	lease, err := cache.Acquire(ref)
	if err != nil {
		return err
	}
	view, err := OpenAdmittedFloat64Directory(
		lease.Page(), bounds.FileEnd, bounds.NextLogicalID,
		allocationQuantum,
	)
	if err != nil {
		lease.Release()
		return err
	}
	if view.Header().Level != expectedLevel {
		lease.Release()
		return ErrFloat64CatalogCorrupt
	}
	if expectedLevel == 0 {
		first, ok := view.EntryAt(0)
		if !ok || first.FirstChunk != expectedLower {
			lease.Release()
			return ErrFloat64CatalogCorrupt
		}
		err := fn(view)
		lease.Release()
		return err
	}
	var entries [Float64DirectoryFanout]Float64DirectoryEntry
	count := view.Len()
	for i := 0; i < count; i++ {
		entries[i], _ = view.EntryAt(i)
	}
	lease.Release()
	if count == 0 || entries[0].FirstChunk != expectedLower {
		return ErrFloat64CatalogCorrupt
	}
	for i := 0; i < count; i++ {
		if err := walkFloat64DirectoryPage(
			cache, entries[i].Ref, expectedLevel-1,
			entries[i].FirstChunk, bounds, allocationQuantum, fn,
		); err != nil {
			return err
		}
	}
	return nil
}

// WalkFloat64DirectoryPages visits directory nodes in bulk-allocation order:
// leaves from left to right, then each successive branch level. A freshly
// built directory is therefore observed as one physically contiguous run,
// while copy-on-write outliers remain individually reclaimable. The extra
// bounded passes are used only when retiring the complete accelerator.
func WalkFloat64DirectoryPages(
	cache *PageCache,
	root PageRef,
	bounds Float64DirectoryBounds,
	allocationQuantum uint32,
	fn func(PageRef) error,
) error {
	if root == (PageRef{}) {
		return nil
	}
	if cache == nil || fn == nil {
		return fmt.Errorf(
			"%w: float64 directory page walk", ErrInvalidWrite,
		)
	}
	lease, err := cache.Acquire(root)
	if err != nil {
		return err
	}
	view, err := OpenAdmittedFloat64Directory(
		lease.Page(), bounds.FileEnd, bounds.NextLogicalID,
		allocationQuantum,
	)
	if err != nil {
		lease.Release()
		return err
	}
	rootLevel := view.Header().Level
	first, ok := view.EntryAt(0)
	lease.Release()
	if !ok || first.FirstChunk != 0 {
		return ErrFloat64CatalogCorrupt
	}
	for level := uint8(0); level <= rootLevel; level++ {
		if err := walkFloat64DirectoryLevel(
			cache, root, rootLevel, 0, level, bounds,
			allocationQuantum, fn,
		); err != nil {
			return err
		}
	}
	return nil
}

func walkFloat64DirectoryLevel(
	cache *PageCache,
	ref PageRef,
	expectedLevel uint8,
	expectedLower uint32,
	targetLevel uint8,
	bounds Float64DirectoryBounds,
	allocationQuantum uint32,
	fn func(PageRef) error,
) error {
	lease, err := cache.Acquire(ref)
	if err != nil {
		return err
	}
	view, err := OpenAdmittedFloat64Directory(
		lease.Page(), bounds.FileEnd, bounds.NextLogicalID,
		allocationQuantum,
	)
	if err != nil {
		lease.Release()
		return err
	}
	if view.Header().Level != expectedLevel {
		lease.Release()
		return ErrFloat64CatalogCorrupt
	}
	var entries [Float64DirectoryFanout]Float64DirectoryEntry
	count := view.Len()
	for i := 0; i < count; i++ {
		entries[i], _ = view.EntryAt(i)
	}
	lease.Release()
	if count == 0 || entries[0].FirstChunk != expectedLower {
		return ErrFloat64CatalogCorrupt
	}
	if expectedLevel == targetLevel {
		return fn(ref)
	}
	if expectedLevel < targetLevel {
		return ErrFloat64CatalogCorrupt
	}
	for i := 0; i < count; i++ {
		if err := walkFloat64DirectoryLevel(
			cache, entries[i].Ref, expectedLevel-1,
			entries[i].FirstChunk, targetLevel, bounds,
			allocationQuantum, fn,
		); err != nil {
			return err
		}
	}
	return nil
}

// ReplaceFloat64Directory swaps one exact FirstChunk mapping and path-copies
// only its ancestors. It cannot split or grow the tree and therefore performs
// no allocation beyond the transaction's fixed page arena.
func ReplaceFloat64Directory(
	cache *PageCache,
	tx *WriteTransaction,
	root PageRef,
	firstChunk uint32,
	stripe PageRef,
	bounds Float64DirectoryBounds,
) (Float64DirectoryMutation, error) {
	var mutation Float64DirectoryMutation
	if cache == nil || tx == nil || !tx.active ||
		root == (PageRef{}) || stripe.Kind != PageFloat64Stripe {
		return mutation, fmt.Errorf(
			"%w: float64 directory replacement", ErrInvalidWrite,
		)
	}
	ref, found, err := rewriteFloat64DirectoryPage(
		cache, tx, root, 0, firstChunk, stripe, bounds, &mutation,
	)
	if err != nil {
		return mutation, err
	}
	mutation.Root = ref
	mutation.Found = found
	mutation.Changed = found
	return mutation, nil
}

func rewriteFloat64DirectoryPage(
	cache *PageCache,
	tx *WriteTransaction,
	oldRef PageRef,
	expectedLower uint32,
	firstChunk uint32,
	stripe PageRef,
	bounds Float64DirectoryBounds,
	mutation *Float64DirectoryMutation,
) (PageRef, bool, error) {
	lease, err := cache.Acquire(oldRef)
	if err != nil {
		return PageRef{}, false, err
	}
	view, err := OpenAdmittedFloat64Directory(
		lease.Page(), bounds.FileEnd, bounds.NextLogicalID,
		tx.options.PageSize,
	)
	if err != nil {
		lease.Release()
		return PageRef{}, false, err
	}
	header := view.Header()
	var entries [Float64DirectoryFanout]Float64DirectoryEntry
	count := view.Len()
	for i := 0; i < count; i++ {
		entries[i], _ = view.EntryAt(i)
	}
	lease.Release()
	if count == 0 || entries[0].FirstChunk != expectedLower {
		return PageRef{}, false, ErrFloat64CatalogCorrupt
	}
	position := float64DirectoryFloor(entries[:count], firstChunk)
	if position < 0 {
		return oldRef, false, nil
	}
	if header.Level == 0 {
		if entries[position].FirstChunk != firstChunk {
			return oldRef, false, nil
		}
		entries[position].Ref = stripe
	} else {
		child, found, rewriteErr := rewriteFloat64DirectoryPage(
			cache, tx, entries[position].Ref,
			entries[position].FirstChunk, firstChunk, stripe, bounds,
			mutation,
		)
		if rewriteErr != nil || !found {
			return oldRef, found, rewriteErr
		}
		entries[position].Ref = child
	}
	if err := mutation.retire(oldRef); err != nil {
		return PageRef{}, false, err
	}
	page, err := encodeFloat64DirectoryTransactionPage(
		tx, oldRef.LogicalID, header.Level, entries[:count],
	)
	if err != nil {
		return PageRef{}, false, err
	}
	return page.Ref(), true, nil
}

func float64DirectoryFloor(
	entries []Float64DirectoryEntry,
	chunk uint32,
) int {
	low, high := 0, len(entries)
	for low < high {
		middle := int(uint(low+high) >> 1)
		if entries[middle].FirstChunk <= chunk {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low - 1
}

func encodeFloat64DirectoryTransactionPage(
	tx *WriteTransaction,
	logicalID uint64,
	level uint8,
	entries []Float64DirectoryEntry,
) (TransactionPage, error) {
	page, err := tx.Allocate(
		PageFloat64Catalog, tx.options.PageSize, logicalID,
	)
	if err != nil {
		return TransactionPage{}, err
	}
	header := Float64DirectoryHeader{
		StoreID: tx.options.StoreID, Generation: tx.options.Generation,
		LogicalID: page.Ref().LogicalID, PageSize: page.Ref().Length,
		Level: level,
	}
	if level == 0 {
		_, err = EncodeFloat64DirectoryLeaf(
			page.Bytes(), header, entries, tx.FileEnd(),
			tx.NextLogicalID(), tx.options.PageSize,
		)
	} else {
		_, err = EncodeFloat64DirectoryBranch(
			page.Bytes(), header, entries, tx.FileEnd(),
			tx.NextLogicalID(), tx.options.PageSize,
		)
	}
	if err != nil {
		return TransactionPage{}, err
	}
	if err := page.Stage(); err != nil {
		return TransactionPage{}, err
	}
	return page, nil
}
