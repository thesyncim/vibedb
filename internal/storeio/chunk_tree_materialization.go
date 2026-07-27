package storeio

import "fmt"

// TryAcquireResident returns a lease only when ref is already a clean,
// admitted resident frame. Canonical materialization uses this non-loading
// probe because a cache miss belongs on the ordinary copy-on-write path; it
// must not turn a write optimization into extra read I/O.
func (c *PageCache) TryAcquireResident(ref PageRef) (PageLease, bool, error) {
	var lease PageLease
	if c == nil {
		return lease, false, ErrPageCacheClosed
	}
	key, err := c.validateRef(ref)
	if err != nil {
		return lease, false, err
	}
	if !c.tryPinReady(cacheKeyHash(key), key, &lease) {
		return PageLease{}, false, nil
	}
	frame := &c.frames[lease.frame]
	frame.lock.Lock()
	clean := frame.state == pageCacheReady && frame.dirty == 0
	frame.lock.Unlock()
	if !clean {
		lease.Release()
		return PageLease{}, false, nil
	}
	return lease, true, nil
}

// LookupResidentChunkTreeLeaf resolves chunkID without admitting a page from
// storage and retains the exact leaf lease. The returned view aliases that
// lease and is valid only until the caller releases it.
//
// This is deliberately writer-only support for a bounded two-page canonical
// materialization. Point reads and scans keep using LookupChunkTree and do not
// gain another branch.
func LookupResidentChunkTreeLeaf(
	cache *PageCache,
	root PageRef,
	chunkID uint32,
	bounds ChunkTreeBounds,
) (
	leafRef PageRef,
	documentRef PageRef,
	view ChunkDirectoryView,
	lease PageLease,
	found bool,
	err error,
) {
	if root == (PageRef{}) {
		return PageRef{}, PageRef{}, ChunkDirectoryView{}, PageLease{},
			false, nil
	}
	if cache == nil {
		return PageRef{}, PageRef{}, ChunkDirectoryView{}, PageLease{},
			false, fmt.Errorf("%w: nil chunk-tree cache", ErrInvalidWrite)
	}
	admitted := cache.ValidatesOnAdmission()
	ref := root
	for expectedShift := chunkTreeRootShift; ; {
		current, resident, acquireErr := cache.TryAcquireResident(ref)
		if acquireErr != nil {
			return PageRef{}, PageRef{}, ChunkDirectoryView{}, PageLease{},
				false, acquireErr
		}
		if !resident {
			return PageRef{}, PageRef{}, ChunkDirectoryView{}, PageLease{},
				false, nil
		}
		if admitted {
			view = AdmittedChunkDirectoryPage(current.Page())
		} else {
			view, err = OpenChunkDirectoryPage(
				current.Page(), bounds.FileEnd, bounds.NextLogicalID,
			)
			if err != nil {
				current.Release()
				return PageRef{}, PageRef{}, ChunkDirectoryView{}, PageLease{},
					false, err
			}
		}
		header := view.Header()
		isRoot := expectedShift == chunkTreeRootShift
		if !isRoot && int(header.Shift) != expectedShift {
			current.Release()
			return PageRef{}, PageRef{}, ChunkDirectoryView{}, PageLease{},
				false, ErrChunkDirectoryCorrupt
		}
		if header.Prefix != chunkDirectoryPrefix(chunkID, header.Shift) {
			current.Release()
			if isRoot {
				return PageRef{}, PageRef{}, ChunkDirectoryView{}, PageLease{},
					false, nil
			}
			return PageRef{}, PageRef{}, ChunkDirectoryView{}, PageLease{},
				false, ErrChunkDirectoryCorrupt
		}
		next, ok := view.Lookup(chunkID)
		if !ok {
			current.Release()
			return PageRef{}, PageRef{}, ChunkDirectoryView{}, PageLease{},
				false, nil
		}
		if header.Shift == 0 {
			return ref, next, view, current, true, nil
		}
		expectedShift = int(header.Shift) - int(chunkDirectoryRadixBits)
		current.Release()
		ref = next
	}
}
