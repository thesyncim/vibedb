package storeio

import (
	"fmt"
	"math/bits"
)

// ChunkTreeBounds are copied from the selected durable roots when existing
// radix pages are admitted.
type ChunkTreeBounds struct {
	FileEnd       uint64
	NextLogicalID uint64
}

// ChunkTreeMutation reports one radix path replacement.
type ChunkTreeMutation struct {
	Root         PageRef
	Retired      [8]PageRef
	RetiredCount uint8
	Found        bool
	Changed      bool
}

// ChunkZoneMerger produces the summary a rewritten leaf lane should carry,
// given the summary that lane carries today.
//
// It is an interface rather than a value parameter because the old summary is
// only readable while the old leaf is resident, and the leaf is resident
// exactly once, inside the rewrite. Handing the merge in lets the caller fold
// its new document into the carried-forward summary without a second descent
// of the radix tree, and keeps this package from having to know what a summary
// means. Implementations are expected to be a pointer to caller-owned state,
// so the call allocates nothing.
type ChunkZoneMerger interface {
	MergeChunkZone(old ChunkZone) ChunkZone
}

// chunkTreeRootShift is the sentinel expected shift for the root node. The
// root's own header names the tree's height, because the tree is only as tall
// as the live chunk ids require: a store holding a few hundred chunks has a
// one- or two-level directory, and every copy-on-write path replacement
// rewrites exactly that many pages. Assuming the maximum height instead would
// charge every Put six page writes forever, which is the write amplification
// this sentinel exists to avoid.
const chunkTreeRootShift = -1

// WalkChunkTree visits live chunk mappings in ascending chunk ID without
// scanning holes in ChunkHighWater. The callback receives value-only refs;
// no directory lease remains pinned during the call.
func WalkChunkTree(cache *PageCache, root PageRef, bounds ChunkTreeBounds, fn func(uint32, PageRef) error) error {
	if root == (PageRef{}) {
		return nil
	}
	if cache == nil || fn == nil {
		return fmt.Errorf("%w: chunk-tree walk", ErrInvalidWrite)
	}
	return walkChunkTreePage(cache, root, bounds, chunkTreeRootShift, fn)
}

// WalkChunkTreeRuns coalesces consecutive chunk ids that name the same
// physical extent. Ordinary mutable document pages produce one-chunk runs;
// compact-generation groups produce bounded multi-chunk runs even when their
// mappings cross a radix-leaf boundary. Readers can therefore acquire and
// prefetch each physical extent once without materializing a chunk list.
func WalkChunkTreeRuns(
	cache *PageCache,
	root PageRef,
	bounds ChunkTreeBounds,
	fn func(first, count uint32, ref PageRef) error,
) error {
	if fn == nil {
		return fmt.Errorf("%w: chunk-tree run walk", ErrInvalidWrite)
	}
	var runRef PageRef
	var first, previous, count uint32
	flush := func() error {
		if count == 0 {
			return nil
		}
		err := fn(first, count, runRef)
		count = 0
		return err
	}
	err := WalkChunkTree(cache, root, bounds, func(chunk uint32, ref PageRef) error {
		if count != 0 && ref == runRef && uint64(chunk) == uint64(previous)+1 {
			if count == ^uint32(0) {
				return ErrChunkDirectoryCorrupt
			}
			previous = chunk
			count++
			return nil
		}
		if err := flush(); err != nil {
			return err
		}
		runRef, first, previous, count = ref, chunk, chunk, 1
		return nil
	})
	if err != nil {
		return err
	}
	return flush()
}

// WalkChunkTreeFloat64Runs extends ordinary physical run coalescing across
// adjacent document groups that derive the same detached typed extent. When
// detached is true ref names PageFloat64Group; otherwise it retains the
// ordinary document or document-group reference. This lets covering scans
// admit one shared typed page once without changing the general chunk tree.
func WalkChunkTreeFloat64Runs(
	cache *PageCache,
	root PageRef,
	bounds ChunkTreeBounds,
	allocationQuantum uint32,
	fn func(first, count uint32, ref PageRef, detached bool) error,
) error {
	if fn == nil || !validPhysicalPageSize(allocationQuantum) {
		return fmt.Errorf("%w: float64 chunk-tree run walk", ErrInvalidWrite)
	}
	var runRef PageRef
	var first, previous, count uint32
	runDetached := false
	flush := func() error {
		if count == 0 {
			return nil
		}
		err := fn(first, count, runRef, runDetached)
		count = 0
		return err
	}
	err := WalkChunkTreeRuns(cache, root, bounds, func(nextFirst, nextCount uint32, document PageRef) error {
		ref := document
		detached := false
		columns, found, deriveErr := DocumentGroupFloat64Sidecar(document, allocationQuantum)
		if deriveErr != nil {
			return deriveErr
		}
		if found {
			ref, detached = columns, true
		}
		if count != 0 && detached == runDetached && ref == runRef &&
			uint64(nextFirst) == uint64(previous)+1 {
			if uint64(count)+uint64(nextCount) > uint64(^uint32(0)) {
				return ErrChunkDirectoryCorrupt
			}
			count += nextCount
			previous = nextFirst + nextCount - 1
			return nil
		}
		if err := flush(); err != nil {
			return err
		}
		runRef, runDetached = ref, detached
		first, count = nextFirst, nextCount
		previous = nextFirst + nextCount - 1
		return nil
	})
	if err != nil {
		return err
	}
	return flush()
}

func walkChunkTreePage(cache *PageCache, ref PageRef, bounds ChunkTreeBounds, expectedShift int, fn func(uint32, PageRef) error) error {
	if ref.Kind != PageChunkDirectory {
		return fmt.Errorf("%w: chunk-tree reference kind", ErrChunkDirectoryCorrupt)
	}
	lease, err := cache.Acquire(ref)
	if err != nil {
		return err
	}
	var view ChunkDirectoryView
	if cache.ValidatesOnAdmission() {
		view = AdmittedChunkDirectoryPage(lease.Page())
	} else if view, err = OpenChunkDirectoryPage(lease.Page(), bounds.FileEnd, bounds.NextLogicalID); err != nil {
		lease.Release()
		return err
	}
	header := view.Header()
	if expectedShift != chunkTreeRootShift && int(header.Shift) != expectedShift {
		lease.Release()
		return ErrChunkDirectoryCorrupt
	}
	var refs [64]PageRef
	var lanes [64]uint8
	count := view.Len()
	bitmap := header.Bitmap
	for i := 0; i < count; i++ {
		refs[i], _ = view.RefAt(i)
		lanes[i] = uint8(bits.TrailingZeros64(bitmap))
		bitmap &= bitmap - 1
	}
	prefix := header.Prefix
	shift := header.Shift
	lease.Release()
	for i := 0; i < count; i++ {
		if shift == 0 {
			if err := fn(prefix|uint32(lanes[i]), refs[i]); err != nil {
				return err
			}
			continue
		}
		if err := walkChunkTreePage(cache, refs[i], bounds, int(shift-chunkDirectoryRadixBits), fn); err != nil {
			return err
		}
	}
	return nil
}

// WalkChunkTreeZones visits every live chunk's summary in ascending chunk id,
// reading directory pages only. It never acquires a document extent, which is
// the property that makes chunk-summary pruning worth having on a backend
// whose chunks live on disk: a chunk the caller rejects here is a page that was
// never faulted in.
//
// The callback receives value-only copies and no lease is held across it.
func WalkChunkTreeZones(cache *PageCache, root PageRef, bounds ChunkTreeBounds, fn func(chunkID uint32, zone ChunkZone) error) error {
	if root == (PageRef{}) {
		return nil
	}
	if cache == nil || fn == nil {
		return fmt.Errorf("%w: chunk-tree zone walk", ErrInvalidWrite)
	}
	return walkChunkTreeZonePage(cache, root, bounds, chunkTreeRootShift, fn)
}

func walkChunkTreeZonePage(cache *PageCache, ref PageRef, bounds ChunkTreeBounds, expectedShift int, fn func(uint32, ChunkZone) error) error {
	if ref.Kind != PageChunkDirectory {
		return fmt.Errorf("%w: chunk-tree reference kind", ErrChunkDirectoryCorrupt)
	}
	lease, err := cache.Acquire(ref)
	if err != nil {
		return err
	}
	var view ChunkDirectoryView
	if cache.ValidatesOnAdmission() {
		view = AdmittedChunkDirectoryPage(lease.Page())
	} else if view, err = OpenChunkDirectoryPage(lease.Page(), bounds.FileEnd, bounds.NextLogicalID); err != nil {
		lease.Release()
		return err
	}
	header := view.Header()
	if expectedShift != chunkTreeRootShift && int(header.Shift) != expectedShift {
		lease.Release()
		return ErrChunkDirectoryCorrupt
	}
	if header.Shift == 0 {
		// The leaf's summaries are copied out under the lease and the callback
		// runs after it is dropped, matching WalkChunkTree's contract that no
		// directory lease is pinned during a callback. The array is confined to
		// this frame so the branch recursion above never carries it.
		var zones [64]ChunkZone
		var lanes [64]uint8
		count := view.Len()
		bitmap := header.Bitmap
		for i := 0; i < count; i++ {
			zones[i] = view.ZoneAt(i)
			lanes[i] = uint8(bits.TrailingZeros64(bitmap))
			bitmap &= bitmap - 1
		}
		prefix := header.Prefix
		lease.Release()
		for i := 0; i < count; i++ {
			if err := fn(prefix|uint32(lanes[i]), zones[i]); err != nil {
				return err
			}
		}
		return nil
	}
	var refs [64]PageRef
	count := view.Len()
	for i := 0; i < count; i++ {
		refs[i], _ = view.RefAt(i)
	}
	shift := header.Shift
	lease.Release()
	for i := 0; i < count; i++ {
		if err := walkChunkTreeZonePage(cache, refs[i], bounds, int(shift-chunkDirectoryRadixBits), fn); err != nil {
			return err
		}
	}
	return nil
}

// LookupChunkTreeZone resolves one logical chunk's summary. It is the read a
// writer performs when it needs the summary it is about to carry forward but
// is not already inside a rewrite.
func LookupChunkTreeZone(cache *PageCache, root PageRef, chunkID uint32, bounds ChunkTreeBounds) (ChunkZone, error) {
	var zone ChunkZone
	if root == (PageRef{}) {
		return zone, nil
	}
	if cache == nil {
		return zone, fmt.Errorf("%w: nil chunk-tree cache", ErrInvalidWrite)
	}
	admitted := cache.ValidatesOnAdmission()
	ref := root
	for expectedShift := chunkTreeRootShift; ; {
		if ref.Kind != PageChunkDirectory {
			return zone, fmt.Errorf("%w: chunk-tree reference kind", ErrChunkDirectoryCorrupt)
		}
		lease, err := cache.Acquire(ref)
		if err != nil {
			return zone, err
		}
		var view ChunkDirectoryView
		if admitted {
			view = AdmittedChunkDirectoryPage(lease.Page())
		} else if view, err = OpenChunkDirectoryPage(lease.Page(), bounds.FileEnd, bounds.NextLogicalID); err != nil {
			lease.Release()
			return zone, err
		}
		header := view.Header()
		isRoot := expectedShift == chunkTreeRootShift
		if !isRoot && int(header.Shift) != expectedShift {
			lease.Release()
			return zone, ErrChunkDirectoryCorrupt
		}
		if header.Prefix != chunkDirectoryPrefix(chunkID, header.Shift) {
			lease.Release()
			if isRoot {
				return zone, nil
			}
			return zone, ErrChunkDirectoryCorrupt
		}
		if header.Shift == 0 {
			zone = view.Zone(chunkID)
			lease.Release()
			return zone, nil
		}
		next, ok := view.Lookup(chunkID)
		expectedShift = int(header.Shift) - int(chunkDirectoryRadixBits)
		lease.Release()
		if !ok {
			return zone, nil
		}
		ref = next
	}
}

func (m *ChunkTreeMutation) retire(ref PageRef) error {
	if int(m.RetiredCount) == len(m.Retired) {
		return ErrKeyTreeDepth
	}
	m.Retired[m.RetiredCount] = ref
	m.RetiredCount++
	return nil
}

// LookupChunkTree resolves one logical chunk to its immutable document or
// multi-chunk group extent.
func LookupChunkTree(cache *PageCache, root PageRef, chunkID uint32, bounds ChunkTreeBounds) (PageRef, bool, error) {
	if root == (PageRef{}) {
		return PageRef{}, false, nil
	}
	if cache == nil {
		return PageRef{}, false, fmt.Errorf("%w: nil chunk-tree cache", ErrInvalidWrite)
	}
	// See LookupKeyTree: admission validation is a cache property, so it is read
	// once instead of at every radix level.
	admitted := cache.ValidatesOnAdmission()
	ref := root
	for expectedShift := chunkTreeRootShift; ; {
		if ref.Kind != PageChunkDirectory {
			return PageRef{}, false, fmt.Errorf("%w: chunk-tree reference kind", ErrChunkDirectoryCorrupt)
		}
		lease, err := cache.Acquire(ref)
		if err != nil {
			return PageRef{}, false, err
		}
		var view ChunkDirectoryView
		if admitted {
			view = AdmittedChunkDirectoryPage(lease.Page())
		} else if view, err = OpenChunkDirectoryPage(lease.Page(), bounds.FileEnd, bounds.NextLogicalID); err != nil {
			lease.Release()
			return PageRef{}, false, err
		}
		header := view.Header()
		isRoot := expectedShift == chunkTreeRootShift
		if !isRoot && int(header.Shift) != expectedShift {
			lease.Release()
			return PageRef{}, false, ErrChunkDirectoryCorrupt
		}
		if header.Prefix != chunkDirectoryPrefix(chunkID, header.Shift) {
			lease.Release()
			// A root that does not cover chunkID means the tree has never been
			// grown that far, so the chunk is simply absent. Below the root the
			// lane was chosen from the parent's own bitmap, so a prefix that
			// disagrees there is a real inconsistency.
			if isRoot {
				return PageRef{}, false, nil
			}
			return PageRef{}, false, ErrChunkDirectoryCorrupt
		}
		next, ok := view.Lookup(chunkID)
		leaf := header.Shift == 0
		expectedShift = int(header.Shift) - int(chunkDirectoryRadixBits)
		lease.Release()
		if !ok {
			return PageRef{}, false, nil
		}
		if leaf {
			return next, true, nil
		}
		ref = next
	}
}

// LookupChunkTreeDocumentZone resolves one logical chunk to its immutable
// document extent and copies the leaf summary published beside that reference.
// It is the writer-side form of LookupChunkTree: mutation code needs the old
// summary while it still has the replacement rows available to fold, whereas
// ordinary point reads need only the document reference and keep using the
// narrower lookup above.
func LookupChunkTreeDocumentZone(
	cache *PageCache,
	root PageRef,
	chunkID uint32,
	bounds ChunkTreeBounds,
) (PageRef, ChunkZone, bool, error) {
	var zone ChunkZone
	if root == (PageRef{}) {
		return PageRef{}, zone, false, nil
	}
	if cache == nil {
		return PageRef{}, zone, false, fmt.Errorf(
			"%w: nil chunk-tree cache", ErrInvalidWrite,
		)
	}
	admitted := cache.ValidatesOnAdmission()
	ref := root
	for expectedShift := chunkTreeRootShift; ; {
		if ref.Kind != PageChunkDirectory {
			return PageRef{}, zone, false, fmt.Errorf(
				"%w: chunk-tree reference kind", ErrChunkDirectoryCorrupt,
			)
		}
		lease, err := cache.Acquire(ref)
		if err != nil {
			return PageRef{}, zone, false, err
		}
		var view ChunkDirectoryView
		if admitted {
			view = AdmittedChunkDirectoryPage(lease.Page())
		} else if view, err = OpenChunkDirectoryPage(
			lease.Page(), bounds.FileEnd, bounds.NextLogicalID,
		); err != nil {
			lease.Release()
			return PageRef{}, zone, false, err
		}
		header := view.Header()
		isRoot := expectedShift == chunkTreeRootShift
		if !isRoot && int(header.Shift) != expectedShift {
			lease.Release()
			return PageRef{}, zone, false, ErrChunkDirectoryCorrupt
		}
		if header.Prefix != chunkDirectoryPrefix(chunkID, header.Shift) {
			lease.Release()
			if isRoot {
				return PageRef{}, zone, false, nil
			}
			return PageRef{}, zone, false, ErrChunkDirectoryCorrupt
		}
		next, ok := view.Lookup(chunkID)
		leaf := header.Shift == 0
		expectedShift = int(header.Shift) - int(chunkDirectoryRadixBits)
		if leaf && ok {
			zone = view.Zone(chunkID)
		}
		lease.Release()
		if !ok {
			return PageRef{}, zone, false, nil
		}
		if leaf {
			return next, zone, true, nil
		}
		ref = next
	}
}

// ChunkTreeHasOtherReference reports whether any chunk in [first, first+count)
// except exclude still names want. It opens each covered 64-lane leaf once,
// rather than performing one full radix lookup per chunk. Compact document
// groups cover at most 128 rows, so their last-reference retirement touches at
// most three leaf paths regardless of database size.
func ChunkTreeHasOtherReference(
	cache *PageCache,
	root PageRef,
	first uint32,
	count uint16,
	exclude uint32,
	want PageRef,
	bounds ChunkTreeBounds,
) (bool, error) {
	return chunkTreeHasOtherReference(
		cache, root, first, count, exclude, want, 0, bounds,
	)
}

// ChunkTreeHasOtherFloat64Sidecar reports whether another document-group
// mapping in a typed extent's coverage derives want. Shared typed sidecars are
// retired only after both the touched document group and every other deriving
// group have disappeared from the old generation.
func ChunkTreeHasOtherFloat64Sidecar(
	cache *PageCache,
	root PageRef,
	first uint32,
	count uint16,
	exclude uint32,
	want PageRef,
	allocationQuantum uint32,
	bounds ChunkTreeBounds,
) (bool, error) {
	if want.Kind != PageFloat64Group || want.Flags != 0 || want.Aux != 0 ||
		!validPhysicalPageSize(allocationQuantum) {
		return false, fmt.Errorf("%w: float64 sidecar reference", ErrInvalidWrite)
	}
	return chunkTreeHasOtherReference(
		cache, root, first, count, exclude, want, allocationQuantum, bounds,
	)
}

func chunkTreeHasOtherReference(
	cache *PageCache,
	root PageRef,
	first uint32,
	count uint16,
	exclude uint32,
	want PageRef,
	float64Quantum uint32,
	bounds ChunkTreeBounds,
) (bool, error) {
	end := uint64(first) + uint64(count)
	if cache == nil || root == (PageRef{}) || count == 0 || end > uint64(^uint32(0))+1 {
		return false, fmt.Errorf("%w: chunk-tree reference range", ErrInvalidWrite)
	}
	for leaf := uint64(first) &^ uint64(63); leaf < end; leaf += 64 {
		found, err := chunkTreeLeafHasOtherReference(
			cache, root, uint32(leaf), first, end, exclude, want, float64Quantum, bounds,
		)
		if err != nil || found {
			return found, err
		}
	}
	return false, nil
}

func chunkTreeLeafHasOtherReference(
	cache *PageCache,
	root PageRef,
	leaf uint32,
	first uint32,
	end uint64,
	exclude uint32,
	want PageRef,
	float64Quantum uint32,
	bounds ChunkTreeBounds,
) (bool, error) {
	ref := root
	for expectedShift := chunkTreeRootShift; ; {
		lease, err := cache.Acquire(ref)
		if err != nil {
			return false, err
		}
		view, err := OpenChunkDirectoryPage(lease.Page(), bounds.FileEnd, bounds.NextLogicalID)
		if err != nil {
			lease.Release()
			return false, err
		}
		header := view.Header()
		isRoot := expectedShift == chunkTreeRootShift
		if !isRoot && int(header.Shift) != expectedShift {
			lease.Release()
			return false, ErrChunkDirectoryCorrupt
		}
		if header.Prefix != chunkDirectoryPrefix(leaf, header.Shift) {
			lease.Release()
			// Outside the root's coverage nothing can name want; below the root
			// the lane came from the parent's bitmap, so disagreement is damage.
			if isRoot {
				return false, nil
			}
			return false, ErrChunkDirectoryCorrupt
		}
		expectedShift = int(header.Shift) - int(chunkDirectoryRadixBits)
		if header.Shift == 0 {
			begin := max(uint64(first), uint64(leaf))
			limit := min(end, uint64(leaf)+64)
			for chunk := begin; chunk < limit; chunk++ {
				if uint32(chunk) == exclude {
					continue
				}
				candidate, ok := view.Lookup(uint32(chunk))
				if !ok {
					continue
				}
				if float64Quantum == 0 {
					if candidate == want {
						lease.Release()
						return true, nil
					}
					continue
				}
				if candidate.Kind == PageDocumentGroup {
					sidecar, detached, deriveErr := DocumentGroupFloat64Sidecar(
						candidate, float64Quantum,
					)
					if deriveErr != nil {
						lease.Release()
						return false, deriveErr
					}
					if detached && sidecar == want {
						lease.Release()
						return true, nil
					}
				}
			}
			lease.Release()
			return false, nil
		}
		next, ok := view.Lookup(leaf)
		lease.Release()
		if !ok {
			return false, nil
		}
		ref = next
	}
}

// UpsertChunkTree maps chunkID to one document extent, leaving the lane's
// chunk summary at "no statistics".
func UpsertChunkTree(cache *PageCache, tx *WriteTransaction, root PageRef, chunkID uint32, document PageRef, bounds ChunkTreeBounds) (ChunkTreeMutation, error) {
	return mutateChunkTree(cache, tx, root, chunkID, document, nil, false, bounds)
}

// UpsertChunkTreeZone maps chunkID to one document extent and asks merger for
// the summary the rewritten lane should carry. A nil merger is
// UpsertChunkTree.
func UpsertChunkTreeZone(cache *PageCache, tx *WriteTransaction, root PageRef, chunkID uint32, document PageRef, merger ChunkZoneMerger, bounds ChunkTreeBounds) (ChunkTreeMutation, error) {
	return mutateChunkTree(cache, tx, root, chunkID, document, merger, false, bounds)
}

// DeleteChunkTree removes one logical chunk mapping.
func DeleteChunkTree(cache *PageCache, tx *WriteTransaction, root PageRef, chunkID uint32, bounds ChunkTreeBounds) (ChunkTreeMutation, error) {
	return mutateChunkTree(cache, tx, root, chunkID, PageRef{}, nil, true, bounds)
}

type chunkTreeRewrite struct {
	ref     PageRef
	found   bool
	changed bool
	empty   bool
}

func mutateChunkTree(cache *PageCache, tx *WriteTransaction, root PageRef, chunkID uint32, document PageRef, merger ChunkZoneMerger, deleting bool, bounds ChunkTreeBounds) (ChunkTreeMutation, error) {
	var mutation ChunkTreeMutation
	if tx == nil || !tx.active || cache == nil && root != (PageRef{}) {
		return mutation, fmt.Errorf("%w: chunk-tree mutation", ErrInvalidWrite)
	}
	if !deleting && !validChunkTreeDocumentRef(tx, document) {
		return mutation, fmt.Errorf("%w: chunk document reference", ErrInvalidWrite)
	}
	if root == (PageRef{}) {
		if deleting {
			return mutation, nil
		}
		// The first chunk needs one leaf, not a six-level spine down to it.
		ref, err := buildChunkTreePath(tx, chunkID, document, merger, 0)
		if err != nil {
			return mutation, err
		}
		mutation.Root = ref
		mutation.Changed = true
		return mutation, nil
	}
	rootShift, rootPrefix, err := chunkTreeRootShape(cache, root, bounds)
	if err != nil {
		return mutation, err
	}
	if chunkDirectoryPrefix(chunkID, rootShift) != rootPrefix {
		// chunkID lies outside the current root's 64^(depth) span. Deleting an
		// absent mapping is a no-op; inserting one raises the tree just far
		// enough to cover both spans, which is why the store's height tracks
		// its chunk high-water mark instead of the uint32 chunk-id ceiling.
		if deleting {
			mutation.Root = root
			return mutation, nil
		}
		grown, err := growChunkTree(tx, root, rootShift, rootPrefix, chunkID, document, merger)
		if err != nil {
			return mutation, err
		}
		mutation.Root = grown
		mutation.Changed = true
		return mutation, nil
	}
	result, err := rewriteChunkTreePage(cache, tx, root, chunkID, document, merger, deleting, bounds, rootShift, &mutation)
	if err != nil {
		return mutation, err
	}
	mutation.Root = result.ref
	mutation.Found = result.found
	mutation.Changed = result.changed
	return mutation, nil
}

// chunkTreeRootShape reads the height and covered span a tree's root records
// for itself. The writer needs both before it can tell an ordinary path
// replacement from a height increase.
func chunkTreeRootShape(cache *PageCache, root PageRef, bounds ChunkTreeBounds) (uint8, uint32, error) {
	lease, err := cache.Acquire(root)
	if err != nil {
		return 0, 0, err
	}
	view, err := OpenChunkDirectoryPage(lease.Page(), bounds.FileEnd, bounds.NextLogicalID)
	if err != nil {
		lease.Release()
		return 0, 0, err
	}
	header := view.Header()
	lease.Release()
	return header.Shift, header.Prefix, nil
}

// growChunkTree raises an existing root until one node spans both its old
// prefix and chunkID, then hangs a fresh path to document beside it. The old
// root stays live as a child, so nothing is retired: a height increase is
// append-only and costs the new spine, not a rewrite of the whole tree.
func growChunkTree(tx *WriteTransaction, root PageRef, rootShift uint8, rootPrefix, chunkID uint32, document PageRef, merger ChunkZoneMerger) (PageRef, error) {
	target := chunkTreeCoveringShift(rootPrefix, chunkID)
	// Lift the old root to the level just under the new one by wrapping it in
	// single-child nodes; every level between must exist because lookup
	// descends exactly one radix step per page.
	lifted := root
	for shift := rootShift + chunkDirectoryRadixBits; shift < target; shift += chunkDirectoryRadixBits {
		refs := [1]PageRef{lifted}
		page, err := encodeChunkTreeNode(tx, 0, chunkDirectoryPrefix(rootPrefix, shift),
			uint64(1)<<(rootPrefix>>shift&63), shift, refs[:], nil)
		if err != nil {
			return PageRef{}, err
		}
		lifted = page.Ref()
	}
	fresh, err := buildChunkTreePath(tx, chunkID, document, merger, target-chunkDirectoryRadixBits)
	if err != nil {
		return PageRef{}, err
	}
	oldLane := uint8(rootPrefix >> target & 63)
	newLane := uint8(chunkID >> target & 63)
	refs := [2]PageRef{lifted, fresh}
	if newLane < oldLane {
		refs[0], refs[1] = fresh, lifted
	}
	bitmap := uint64(1)<<oldLane | uint64(1)<<newLane
	page, err := encodeChunkTreeNode(tx, 0, chunkDirectoryPrefix(chunkID, target), bitmap, target, refs[:], nil)
	if err != nil {
		return PageRef{}, err
	}
	return page.Ref(), nil
}

// chunkTreeCoveringShift is the lowest radix level whose 64-way span holds two
// chunk ids at once. It always terminates: at chunkDirectoryMaxShift the level
// covers more than 32 bits, so every uint32 shares the zero prefix there.
func chunkTreeCoveringShift(a, b uint32) uint8 {
	shift := uint8(0)
	for chunkDirectoryPrefix(a, shift) != chunkDirectoryPrefix(b, shift) {
		shift += chunkDirectoryRadixBits
	}
	return shift
}

func rewriteChunkTreePage(cache *PageCache, tx *WriteTransaction, oldRef PageRef, chunkID uint32, document PageRef, merger ChunkZoneMerger, deleting bool, bounds ChunkTreeBounds, expectedShift uint8, mutation *ChunkTreeMutation) (chunkTreeRewrite, error) {
	lease, err := cache.Acquire(oldRef)
	if err != nil {
		return chunkTreeRewrite{}, err
	}
	defer lease.Release()
	view, err := OpenChunkDirectoryPage(lease.Page(), bounds.FileEnd, bounds.NextLogicalID)
	if err != nil {
		return chunkTreeRewrite{}, err
	}
	header := view.Header()
	if header.Shift != expectedShift || header.Prefix != chunkDirectoryPrefix(chunkID, expectedShift) {
		return chunkTreeRewrite{}, ErrChunkDirectoryCorrupt
	}
	if expectedShift == 0 {
		// The leaf is split out because it is the only level that carries
		// summaries, and the parallel 65-entry summary array it needs would
		// otherwise sit in every frame of a six-deep recursion that has no use
		// for it.
		return rewriteChunkTreeLeaf(tx, oldRef, view, header, chunkID, document, merger, deleting, mutation)
	}
	lane := uint8(chunkID >> expectedShift & 63)
	bit := uint64(1) << lane
	rank := bits.OnesCount64(header.Bitmap & (bit - 1))
	var refs [65]PageRef
	count := view.Len()
	for i := 0; i < count; i++ {
		refs[i], _ = view.RefAt(i)
	}
	found := header.Bitmap&bit != 0
	if !found {
		if deleting {
			return chunkTreeRewrite{ref: oldRef}, nil
		}
		child, err := buildChunkTreePath(tx, chunkID, document, merger, expectedShift-chunkDirectoryRadixBits)
		if err != nil {
			return chunkTreeRewrite{}, err
		}
		copy(refs[rank+1:], refs[rank:count])
		refs[rank] = child
		count++
		header.Bitmap |= bit
	} else {
		child, err := rewriteChunkTreePage(cache, tx, refs[rank], chunkID, document, merger, deleting, bounds, expectedShift-chunkDirectoryRadixBits, mutation)
		if err != nil {
			return chunkTreeRewrite{}, err
		}
		if !child.changed {
			return chunkTreeRewrite{ref: oldRef, found: child.found}, nil
		}
		if child.empty {
			copy(refs[rank:], refs[rank+1:count])
			count--
			header.Bitmap &^= bit
		} else {
			refs[rank] = child.ref
		}
		found = child.found
	}
	if err := mutation.retire(oldRef); err != nil {
		return chunkTreeRewrite{}, err
	}
	if count == 0 {
		return chunkTreeRewrite{found: found, changed: true, empty: true}, nil
	}
	page, err := encodeChunkTreeNode(tx, oldRef.LogicalID, header.Prefix, header.Bitmap, expectedShift, refs[:count], nil)
	if err != nil {
		return chunkTreeRewrite{}, err
	}
	return chunkTreeRewrite{ref: page.Ref(), found: found, changed: true}, nil
}

// rewriteChunkTreeLeaf replaces one lane of a radix leaf, carrying every other
// lane's reference *and* summary forward by copy.
//
// Carrying the untouched summaries forward is not an optimization, it is the
// contract: a summary describes the chunk its neighbouring reference names, and
// the two are only ever published together. A rewrite that dropped the other
// 63 summaries would silently make the whole leaf unprunable on every commit,
// which is a correct answer and a useless one.
//
// A leaf gains summaries the first time a merger rewrites a lane in it, and
// never loses them: once the flag is set the untouched lanes carry their (zero,
// meaning "no statistics") records forward until a writer that holds their rows
// fills them in.
func rewriteChunkTreeLeaf(
	tx *WriteTransaction,
	oldRef PageRef,
	view ChunkDirectoryView,
	header ChunkDirectoryHeader,
	chunkID uint32,
	document PageRef,
	merger ChunkZoneMerger,
	deleting bool,
	mutation *ChunkTreeMutation,
) (chunkTreeRewrite, error) {
	lane := uint8(chunkID & 63)
	bit := uint64(1) << lane
	rank := bits.OnesCount64(header.Bitmap & (bit - 1))
	var refs [65]PageRef
	var zones [65]ChunkZone
	count := view.Len()
	hadZones := view.HasZones()
	for i := 0; i < count; i++ {
		refs[i], _ = view.RefAt(i)
		zones[i] = view.ZoneAt(i)
	}
	found := header.Bitmap&bit != 0
	switch {
	case deleting:
		if !found {
			return chunkTreeRewrite{ref: oldRef}, nil
		}
		copy(refs[rank:], refs[rank+1:count])
		copy(zones[rank:], zones[rank+1:count])
		count--
		header.Bitmap &^= bit
	case found:
		refs[rank] = document
		if merger != nil {
			zones[rank] = merger.MergeChunkZone(zones[rank])
		} else {
			zones[rank] = ChunkZone{}
		}
	default:
		copy(refs[rank+1:], refs[rank:count])
		copy(zones[rank+1:], zones[rank:count])
		refs[rank] = document
		zones[rank] = ChunkZone{}
		if merger != nil {
			zones[rank] = merger.MergeChunkZone(ChunkZone{})
		}
		count++
		header.Bitmap |= bit
	}
	if err := mutation.retire(oldRef); err != nil {
		return chunkTreeRewrite{}, err
	}
	if count == 0 {
		return chunkTreeRewrite{found: found, changed: true, empty: true}, nil
	}
	var written []ChunkZone
	if merger != nil || hadZones {
		written = zones[:count]
	}
	page, err := encodeChunkTreeNode(tx, oldRef.LogicalID, header.Prefix, header.Bitmap, 0, refs[:count], written)
	if err != nil {
		return chunkTreeRewrite{}, err
	}
	return chunkTreeRewrite{ref: page.Ref(), found: found, changed: true}, nil
}

func buildChunkTreePath(tx *WriteTransaction, chunkID uint32, document PageRef, merger ChunkZoneMerger, maxShift uint8) (PageRef, error) {
	shift := uint8(0)
	lane := uint8(chunkID >> shift & 63)
	child := document
	for {
		refs := [1]PageRef{child}
		var zones []ChunkZone
		if shift == 0 && merger != nil {
			leaf := [1]ChunkZone{merger.MergeChunkZone(ChunkZone{})}
			zones = leaf[:]
		}
		page, err := encodeChunkTreeNode(tx, 0, chunkDirectoryPrefix(chunkID, shift), uint64(1)<<lane, shift, refs[:], zones)
		if err != nil {
			return PageRef{}, err
		}
		child = page.Ref()
		if shift == maxShift {
			return child, nil
		}
		shift += chunkDirectoryRadixBits
		lane = uint8(chunkID >> shift & 63)
	}
}

func encodeChunkTreeNode(tx *WriteTransaction, logicalID uint64, prefix uint32, bitmap uint64, shift uint8, refs []PageRef, zones []ChunkZone) (TransactionPage, error) {
	page, err := tx.Allocate(PageChunkDirectory, tx.options.PageSize, logicalID)
	if err != nil {
		return TransactionPage{}, err
	}
	header := ChunkDirectoryHeader{
		StoreID: tx.options.StoreID, Generation: tx.options.Generation,
		LogicalID: page.Ref().LogicalID, PageSize: page.Ref().Length,
		Prefix: prefix, Bitmap: bitmap, Shift: shift,
	}
	if _, err := EncodeChunkDirectoryZonePage(page.Bytes(), header, refs, zones, tx.FileEnd(), tx.NextLogicalID()); err != nil {
		return TransactionPage{}, err
	}
	if err := page.Stage(); err != nil {
		return TransactionPage{}, err
	}
	return page, nil
}

func validChunkTreeDocumentRef(tx *WriteTransaction, ref PageRef) bool {
	quantum := uint64(tx.options.PageSize)
	length := uint64(ref.Length)
	layout, err := MutableStoreLayout(tx.options.PageSize)
	if err != nil {
		return false
	}
	return ref.Kind == PageDocument && ref.Flags == 0 && ref.Aux == 0 &&
		validPageExtentSize(PageDocument, ref.Length) &&
		ref.Length >= tx.options.PageSize && ref.Length%tx.options.PageSize == 0 &&
		ref.Generation != 0 && ref.Generation <= tx.options.Generation &&
		ref.LogicalID > StateRootLogicalID && ref.LogicalID < tx.NextLogicalID() &&
		ref.Offset >= layout.DataStart && ref.Offset%quantum == 0 &&
		length <= tx.FileEnd() && ref.Offset <= tx.FileEnd()-length
}

// ChunkTreePages bounds the pages one sequential chunk-directory upsert or
// delete can publish: one root-to-leaf copy over a radix tree whose depth is
// fixed by the 32-bit chunk identifier, plus one growth level.
func ChunkTreePages() int {
	return 32/int(chunkDirectoryRadixBits) + 2
}
