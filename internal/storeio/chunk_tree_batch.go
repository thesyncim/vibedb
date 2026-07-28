package storeio

import (
	"fmt"
	"math/bits"
)

// ChunkTreeEdit is one mutation inside a batch sorted by Chunk. Delete removes
// the mapping; otherwise Document replaces or inserts it.
type ChunkTreeEdit struct {
	Chunk    uint32
	Document PageRef
	Delete   bool
	// HasZone reports that Zone is a freshly computed summary. A false value
	// resets an updated lane to "no statistics"; an already-zoned leaf still
	// carries the zero summary for that lane so its untouched summaries remain
	// available.
	HasZone bool
	Zone    ChunkZone
}

// ChunkTreeBatchMutation reports one batched radix-tree replacement.
//
// Retired names every old directory page rewritten by the descent. The caller
// owns the slice and may reuse its capacity between transactions.
type ChunkTreeBatchMutation struct {
	Root    PageRef
	Retired []PageRef
	Changed bool
}

// MutateChunkTreeBatch applies edits in one copy-on-write radix descent. Edits
// must be sorted strictly by chunk id. Every old directory node reached by the
// batch is read and rewritten at most once, regardless of how many edited
// chunks share it.
//
// Sequential UpsertChunkTree calls are correct inside one transaction, but
// every call replaces its complete root-to-leaf path. Later calls then replace
// those newly staged pages again. A scattered batch consequently writes
// intermediate directory pages that no published root can reach. This descent
// partitions the edits at each radix node and emits only the final tree.
func MutateChunkTreeBatch(
	cache *PageCache,
	tx *WriteTransaction,
	root PageRef,
	edits []ChunkTreeEdit,
	bounds ChunkTreeBounds,
	retired []PageRef,
) (ChunkTreeBatchMutation, error) {
	mutation := ChunkTreeBatchMutation{Root: root, Retired: retired}
	if tx == nil || !tx.active || (cache == nil && root != (PageRef{})) {
		return mutation, fmt.Errorf("%w: chunk-tree batch", ErrInvalidWrite)
	}
	for i, edit := range edits {
		if i != 0 && edits[i-1].Chunk >= edit.Chunk {
			return mutation, fmt.Errorf("%w: unsorted chunk-tree batch", ErrInvalidWrite)
		}
		if edit.Delete {
			if edit.Document != (PageRef{}) || edit.HasZone ||
				edit.Zone != (ChunkZone{}) {
				return mutation, fmt.Errorf("%w: chunk-tree batch delete", ErrInvalidWrite)
			}
			continue
		}
		if !validChunkTreeDocumentRef(tx, edit.Document) ||
			!edit.HasZone && edit.Zone != (ChunkZone{}) {
			return mutation, fmt.Errorf("%w: chunk-tree batch document", ErrInvalidWrite)
		}
	}
	if len(edits) == 0 {
		return mutation, nil
	}

	batch := chunkTreeBatch{
		cache: cache, tx: tx, bounds: bounds, retired: retired,
	}
	if root == (PageRef{}) {
		effective := edits
		hasDelete := false
		for _, edit := range edits {
			if edit.Delete {
				hasDelete = true
				break
			}
		}
		if hasDelete {
			effective = make([]ChunkTreeEdit, 0, len(edits))
			for _, edit := range edits {
				if !edit.Delete {
					effective = append(effective, edit)
				}
			}
		}
		if len(effective) == 0 {
			return mutation, nil
		}
		targetShift := chunkTreeCoveringShift(
			effective[0].Chunk, effective[len(effective)-1].Chunk,
		)
		targetPrefix := chunkDirectoryPrefix(effective[0].Chunk, targetShift)
		next, changed, err := batch.rewriteVirtual(
			PageRef{}, 0, 0, targetShift, targetPrefix, effective,
		)
		mutation.Retired = batch.retired
		if err != nil {
			return mutation, err
		}
		mutation.Root, mutation.Changed = next, changed
		return mutation, nil
	}

	rootShift, rootPrefix, err := chunkTreeRootShape(cache, root, bounds)
	if err != nil {
		return mutation, err
	}
	targetShift := rootShift
	filtered := false
	for _, edit := range edits {
		inside := chunkDirectoryPrefix(edit.Chunk, rootShift) == rootPrefix
		if edit.Delete && !inside {
			filtered = true
			continue
		}
		if !inside {
			targetShift = max(
				targetShift, chunkTreeCoveringShift(rootPrefix, edit.Chunk),
			)
		}
	}
	effective := edits
	if filtered {
		effective = make([]ChunkTreeEdit, 0, len(edits))
		for _, edit := range edits {
			if edit.Delete &&
				chunkDirectoryPrefix(edit.Chunk, rootShift) != rootPrefix {
				continue
			}
			effective = append(effective, edit)
		}
	}
	if len(effective) == 0 {
		return mutation, nil
	}
	targetPrefix := chunkDirectoryPrefix(rootPrefix, targetShift)
	next, changed, err := batch.rewriteVirtual(
		root, rootShift, rootPrefix, targetShift, targetPrefix, effective,
	)
	mutation.Retired = batch.retired
	if err != nil {
		return mutation, err
	}
	mutation.Root, mutation.Changed = next, changed
	return mutation, nil
}

type chunkTreeBatch struct {
	cache   *PageCache
	tx      *WriteTransaction
	bounds  ChunkTreeBounds
	retired []PageRef
}

// rewriteVirtual rewrites the span at shift. oldRef may name an existing node
// at oldShift below this span; in that case the missing intermediate nodes are
// materialized while the tree grows. Once shift reaches oldShift, rewritePage
// merges the edits with the admitted node.
func (b *chunkTreeBatch) rewriteVirtual(
	oldRef PageRef,
	oldShift uint8,
	oldPrefix uint32,
	shift uint8,
	prefix uint32,
	edits []ChunkTreeEdit,
) (PageRef, bool, error) {
	if oldRef != (PageRef{}) && shift == oldShift {
		return b.rewritePage(oldRef, shift, prefix, edits)
	}
	if oldRef != (PageRef{}) && shift < oldShift {
		return PageRef{}, false, ErrChunkDirectoryCorrupt
	}
	if shift == 0 {
		var refs [64]PageRef
		var zones [64]ChunkZone
		var bitmap uint64
		hasZones := false
		for _, edit := range edits {
			if edit.Delete {
				continue
			}
			lane := uint8(edit.Chunk & 63)
			bitmap |= uint64(1) << lane
			refs[lane] = edit.Document
			zones[lane] = edit.Zone
			hasZones = hasZones || edit.HasZone
		}
		if bitmap == 0 {
			return PageRef{}, false, nil
		}
		packed, packedZones := packChunkTreeLanes(
			refs, zones, bitmap, hasZones,
		)
		page, err := encodeChunkTreeNode(
			b.tx, 0, prefix, bitmap, 0, packed, packedZones,
		)
		if err != nil {
			return PageRef{}, false, err
		}
		return page.Ref(), true, nil
	}

	nextShift := shift - chunkDirectoryRadixBits
	oldLane := uint8(0)
	hasOld := oldRef != (PageRef{})
	if hasOld {
		oldLane = uint8(oldPrefix >> shift & 63)
	}
	var refs [64]PageRef
	var bitmap uint64
	at := 0
	for lane := range uint8(64) {
		start := at
		for at < len(edits) && uint8(edits[at].Chunk>>shift&63) == lane {
			at++
		}
		if start == at && (!hasOld || lane != oldLane) {
			continue
		}
		childOld := PageRef{}
		if hasOld && lane == oldLane {
			childOld = oldRef
		}
		childPrefix := prefix | uint32(lane)<<shift
		child, _, err := b.rewriteVirtual(
			childOld, oldShift, oldPrefix, nextShift, childPrefix, edits[start:at],
		)
		if err != nil {
			return PageRef{}, false, err
		}
		if child == (PageRef{}) {
			continue
		}
		bitmap |= uint64(1) << lane
		refs[lane] = child
	}
	if at != len(edits) {
		return PageRef{}, false, ErrChunkDirectoryCorrupt
	}
	if bitmap == 0 {
		return PageRef{}, oldRef != (PageRef{}), nil
	}
	packed, _ := packChunkTreeLanes(
		refs, [64]ChunkZone{}, bitmap, false,
	)
	page, err := encodeChunkTreeNode(
		b.tx, 0, prefix, bitmap, shift, packed, nil,
	)
	if err != nil {
		return PageRef{}, false, err
	}
	return page.Ref(), true, nil
}

func (b *chunkTreeBatch) rewritePage(
	oldRef PageRef,
	shift uint8,
	prefix uint32,
	edits []ChunkTreeEdit,
) (PageRef, bool, error) {
	lease, err := b.cache.Acquire(oldRef)
	if err != nil {
		return PageRef{}, false, err
	}
	defer lease.Release()
	view, err := OpenChunkDirectoryPage(
		lease.Page(), b.bounds.FileEnd, b.bounds.NextLogicalID,
	)
	if err != nil {
		return PageRef{}, false, err
	}
	header := view.Header()
	if header.Shift != shift || header.Prefix != prefix {
		return PageRef{}, false, ErrChunkDirectoryCorrupt
	}

	var refs [64]PageRef
	var zones [64]ChunkZone
	bitmap := header.Bitmap
	hasZones := view.HasZones()
	left := bitmap
	for rank := 0; rank < view.Len(); rank++ {
		lane := uint8(bits.TrailingZeros64(left))
		refs[lane], _ = view.RefAt(rank)
		zones[lane] = view.ZoneAt(rank)
		left &= left - 1
	}

	changed := false
	if shift == 0 {
		for _, edit := range edits {
			lane := uint8(edit.Chunk & 63)
			bit := uint64(1) << lane
			found := bitmap&bit != 0
			if edit.Delete {
				if found {
					bitmap &^= bit
					refs[lane] = PageRef{}
					changed = true
				}
				continue
			}
			zoneChanged := zones[lane] != edit.Zone ||
				edit.HasZone && !hasZones
			if !found || refs[lane] != edit.Document || zoneChanged {
				bitmap |= bit
				refs[lane] = edit.Document
				changed = true
			}
			zones[lane] = edit.Zone
			hasZones = hasZones || edit.HasZone
		}
	} else {
		nextShift := shift - chunkDirectoryRadixBits
		at := 0
		for lane := range uint8(64) {
			start := at
			for at < len(edits) && uint8(edits[at].Chunk>>shift&63) == lane {
				at++
			}
			if start == at {
				continue
			}
			bit := uint64(1) << lane
			childPrefix := prefix | uint32(lane)<<shift
			var child PageRef
			var childChanged bool
			if bitmap&bit != 0 {
				child, childChanged, err = b.rewritePage(
					refs[lane], nextShift, childPrefix, edits[start:at],
				)
			} else {
				child, childChanged, err = b.rewriteVirtual(
					PageRef{}, 0, 0, nextShift, childPrefix, edits[start:at],
				)
			}
			if err != nil {
				return PageRef{}, false, err
			}
			if !childChanged {
				continue
			}
			changed = true
			if child == (PageRef{}) {
				bitmap &^= bit
				refs[lane] = PageRef{}
			} else {
				bitmap |= bit
				refs[lane] = child
			}
		}
		if at != len(edits) {
			return PageRef{}, false, ErrChunkDirectoryCorrupt
		}
	}
	if !changed {
		return oldRef, false, nil
	}
	b.retired = append(b.retired, oldRef)
	if bitmap == 0 {
		return PageRef{}, true, nil
	}
	packed, packedZones := packChunkTreeLanes(
		refs, zones, bitmap, hasZones && shift == 0,
	)
	page, err := encodeChunkTreeNode(
		b.tx, oldRef.LogicalID, prefix, bitmap, shift, packed, packedZones,
	)
	if err != nil {
		return PageRef{}, false, err
	}
	return page.Ref(), true, nil
}

func packChunkTreeLanes(
	refs [64]PageRef,
	zones [64]ChunkZone,
	bitmap uint64,
	hasZones bool,
) ([]PageRef, []ChunkZone) {
	packedRefs := refs[:0]
	var packedZones []ChunkZone
	if hasZones {
		packedZones = zones[:0]
	}
	for bitmap != 0 {
		lane := uint8(bits.TrailingZeros64(bitmap))
		packedRefs = append(packedRefs, refs[lane])
		if hasZones {
			packedZones = append(packedZones, zones[lane])
		}
		bitmap &= bitmap - 1
	}
	return packedRefs, packedZones
}

// ChunkTreeBatchPages bounds the directory pages a batch can publish. A
// 32-bit id has at most six six-bit radix nodes on its path. Treating the old
// root as one additional path covers an arbitrary height increase where the
// old tree and every edited chunk occupy different top-level spans.
func ChunkTreeBatchPages(edits int) int {
	if edits <= 0 {
		return 0
	}
	levels := 32/int(chunkDirectoryRadixBits) + 1
	return (edits + 1) * levels
}
