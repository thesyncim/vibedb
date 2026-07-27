package storeio

import (
	"fmt"
	"sort"
)

// PageKeyTreeEditOperation selects one exact fingerprint mutation.
type PageKeyTreeEditOperation uint8

const (
	PageKeyTreeInsert PageKeyTreeEditOperation = iota
	PageKeyTreeDelete
)

// PageKeyTreeEdit is one batch mutation. Location identifies the old entry.
// Insert stores Location; delete removes that exact identity.
type PageKeyTreeEdit struct {
	Location  PageKeyLocation
	Operation PageKeyTreeEditOperation
}

// PageKeyTreeBatchMutation is the result of one collision-aware batch rewrite.
// Retired is appended to caller-owned storage and contains every old page
// rewritten exactly once.
type PageKeyTreeBatchMutation struct {
	Root    PageRef
	Retired []PageRef
	Applied int
	Changed bool
}

type pageKeyTreeBatchPlan struct {
	edit  PageKeyTreeEdit
	path  [pageKeyDirectoryMaxLevel]pageKeyTreePathEntry
	depth int
	leaf  PageRef
}

type pageKeyTreeBatchChild struct {
	min uint64
	max uint64
	ref PageRef
}

type pageKeyTreeBatchPosition struct {
	path  [pageKeyDirectoryMaxLevel]pageKeyTreePathEntry
	depth int
	ref   PageRef
	level uint8
}

type pageKeyTreeBatchRun struct {
	positions []pageKeyTreeBatchPosition
}

type pageKeyTreeBatch struct {
	cache       *PageCache
	tx          *WriteTransaction
	bounds      PageKeyTreeBounds
	retired     []PageRef
	retiredSeen map[PageRef]struct{}
	applied     int
}

type pageKeyTreeBatchCandidate struct {
	location PageKeyLocation
	position pageKeyTreeBatchPosition
}

type pageKeyTreeBatchPlanningStats struct {
	HashGroups      int
	CandidateVisits int
	PageVisits      int
}

// MutatePageKeyTreeBatch applies strictly identity-sorted edits. It resolves
// every exact old location before allocating, then rewrites each touched leaf
// and ancestor once. Equal hashes may span any number of leaves.
func MutatePageKeyTreeBatch(
	cache *PageCache, tx *WriteTransaction, root PageRef,
	edits []PageKeyTreeEdit, bounds PageKeyTreeBounds, retired []PageRef,
) (PageKeyTreeBatchMutation, error) {
	mutation := PageKeyTreeBatchMutation{Root: root, Retired: retired}
	if tx == nil || !tx.active || cache == nil && root != (PageRef{}) ||
		root != (PageRef{}) && root.Kind != PageFingerprintDirectory ||
		bounds.ChunkHighWater == 0 || bounds.ChunkDocuments == 0 || bounds.ChunkDocuments > 64 {
		return mutation, fmt.Errorf("%w: fingerprint batch bounds", ErrInvalidWrite)
	}
	for index, edit := range edits {
		if edit.Operation > PageKeyTreeDelete ||
			edit.Location.Chunk >= bounds.ChunkHighWater ||
			uint32(edit.Location.Slot) >= bounds.ChunkDocuments ||
			index != 0 && comparePageKeyLocation(edits[index-1].Location, edit.Location) >= 0 {
			return mutation, fmt.Errorf("%w: fingerprint batch edit", ErrInvalidWrite)
		}
	}
	if len(edits) == 0 {
		return mutation, nil
	}
	retiredSeen := make(map[PageRef]struct{}, len(retired)+len(edits))
	for _, ref := range retired {
		retiredSeen[ref] = struct{}{}
	}
	batch := pageKeyTreeBatch{
		cache: cache, tx: tx, bounds: bounds, retired: retired,
		retiredSeen: retiredSeen,
	}
	if root == (PageRef{}) {
		entries := make([]PageKeyLocation, 0, len(edits))
		for _, edit := range edits {
			if edit.Operation == PageKeyTreeInsert {
				entries = append(entries, edit.Location)
			}
		}
		if len(entries) == 0 {
			return mutation, nil
		}
		batch.applied = len(entries)
		children, err := batch.encodeLeaves(nil, 0, PageRef{}, entries)
		if err != nil {
			return mutation, err
		}
		return batch.finish(children)
	}

	plans, applied, err := planPageKeyTreeBatch(
		cache, root, edits, bounds, nil,
	)
	if err != nil {
		return mutation, err
	}
	if len(plans) == 0 {
		return mutation, nil
	}
	batch.applied = applied
	children, err := batch.rewriteRuns(root, plans)
	if err != nil {
		mutation.Retired = batch.retired
		return mutation, err
	}
	return batch.finish(children)
}

func (b *pageKeyTreeBatch) retire(ref PageRef) {
	if _, exists := b.retiredSeen[ref]; exists {
		return
	}
	b.retiredSeen[ref] = struct{}{}
	b.retired = append(b.retired, ref)
}

// planPageKeyTreeBatch performs one current-root collision scan per distinct
// hash and merge-joins its identity-sorted edits with the leaf candidates.
// Work is O(E+C) for an edit group of E identities and C collision candidates;
// absent hashes retain the clamped insertion path from that same descent.
func planPageKeyTreeBatch(
	cache *PageCache,
	root PageRef,
	edits []PageKeyTreeEdit,
	bounds PageKeyTreeBounds,
	stats *pageKeyTreeBatchPlanningStats,
) ([]pageKeyTreeBatchPlan, int, error) {
	plans := make([]pageKeyTreeBatchPlan, 0, len(edits))
	for first := 0; first < len(edits); {
		last := first + 1
		for last < len(edits) && edits[last].Location.Hash == edits[first].Location.Hash {
			last++
		}
		if stats != nil {
			stats.HashGroups++
		}
		candidates, fallback, err := scanPageKeyTreeBatchHash(
			cache, root, edits[first].Location.Hash, bounds, stats,
		)
		if err != nil {
			return nil, 0, err
		}
		rank := 0
		for _, edit := range edits[first:last] {
			for rank < len(candidates) &&
				comparePageKeyLocation(candidates[rank].location, edit.Location) < 0 {
				rank++
			}
			found := rank < len(candidates) &&
				samePageKeyIdentity(candidates[rank].location, edit.Location)
			position := fallback
			if len(candidates) != 0 {
				if rank < len(candidates) {
					position = candidates[rank].position
				} else {
					position = candidates[len(candidates)-1].position
				}
			}
			switch edit.Operation {
			case PageKeyTreeInsert:
				if found {
					continue
				}
			case PageKeyTreeDelete:
				if !found {
					continue
				}
				position = candidates[rank].position
			default:
				return nil, 0, fmt.Errorf("%w: fingerprint batch operation", ErrInvalidWrite)
			}
			if position.ref == (PageRef{}) {
				return nil, 0, fmt.Errorf("%w: fingerprint batch insertion path", ErrKeyDirectoryCorrupt)
			}
			plans = append(plans, pageKeyTreeBatchPlan{
				edit: edit, path: position.path, depth: position.depth, leaf: position.ref,
			})
		}
		first = last
	}
	return plans, len(plans), nil
}

// scanPageKeyTreeBatchHash descends once with clamped branch routing, retaining
// the exact insertion leaf even when hash is absent. When present, it walks the
// current-root collision run by parent-derived successors and captures each
// candidate's immutable path before advancing.
func scanPageKeyTreeBatchHash(
	cache *PageCache,
	root PageRef,
	hash uint64,
	bounds PageKeyTreeBounds,
	stats *pageKeyTreeBatchPlanningStats,
) ([]pageKeyTreeBatchCandidate, pageKeyTreeBatchPosition, error) {
	if err := validatePageKeyTreeRead(cache, root, bounds); err != nil {
		return nil, pageKeyTreeBatchPosition{}, err
	}
	var path [pageKeyDirectoryMaxLevel]pageKeyTreePathEntry
	depth := 0
	ref := root
	expectedLevel := uint8(0)
	haveExpectedLevel := false
	expectedMax := uint64(0)
	haveExpectedMax := false
	for {
		lease, view, err := acquirePageFingerprintDirectory(cache, ref, bounds)
		if err != nil {
			return nil, pageKeyTreeBatchPosition{}, err
		}
		if stats != nil {
			stats.PageVisits++
		}
		header := view.Header()
		if haveExpectedLevel && header.Level != expectedLevel ||
			haveExpectedMax && header.MaxHash != expectedMax {
			lease.Release()
			return nil, pageKeyTreeBatchPosition{}, fmt.Errorf("%w: fingerprint batch scan path", ErrKeyDirectoryCorrupt)
		}
		if header.Level == 0 {
			position := pageKeyTreeBatchPosition{
				path: path, depth: depth, ref: ref, level: 0,
			}
			first, end, found := view.CandidateRange(hash)
			if !found {
				lease.Release()
				return nil, position, nil
			}
			candidates := make([]pageKeyTreeBatchCandidate, 0, end-first)
			var previous PageKeyLocation
			havePrevious := false
			for {
				for rank := first; rank < end; rank++ {
					location, ok := view.LocationAt(rank)
					if !ok || location.Hash != hash ||
						havePrevious && comparePageKeyLocation(previous, location) >= 0 {
						lease.Release()
						return nil, pageKeyTreeBatchPosition{}, fmt.Errorf("%w: fingerprint batch collision order", ErrKeyDirectoryCorrupt)
					}
					if stats != nil {
						stats.CandidateVisits++
					}
					candidates = append(candidates, pageKeyTreeBatchCandidate{
						location: location, position: position,
					})
					previous = location
					havePrevious = true
				}
				follow := view.Header().MaxHash == hash
				lease.Release()
				if !follow {
					return candidates, position, nil
				}
				nextRef, nextMax, ok, err := nextPageKeyTreeLeaf(
					cache, &path, &depth, bounds,
				)
				if err != nil {
					return nil, pageKeyTreeBatchPosition{}, err
				}
				if !ok {
					return candidates, position, nil
				}
				lease, view, err = acquirePageFingerprintDirectory(cache, nextRef, bounds)
				if err != nil {
					return nil, pageKeyTreeBatchPosition{}, err
				}
				if stats != nil {
					stats.PageVisits++
				}
				nextHeader := view.Header()
				if nextHeader.Level != 0 || nextHeader.MaxHash != nextMax {
					lease.Release()
					return nil, pageKeyTreeBatchPosition{}, fmt.Errorf("%w: fingerprint batch collision successor", ErrKeyDirectoryCorrupt)
				}
				if nextHeader.MinHash > hash {
					lease.Release()
					return candidates, position, nil
				}
				first, end, found = view.CandidateRange(hash)
				if !found {
					lease.Release()
					if nextHeader.MaxHash < hash {
						return nil, pageKeyTreeBatchPosition{}, fmt.Errorf("%w: fingerprint batch collision successor order", ErrKeyDirectoryCorrupt)
					}
					return candidates, position, nil
				}
				ref = nextRef
				position = pageKeyTreeBatchPosition{
					path: path, depth: depth, ref: ref, level: 0,
				}
			}
		}
		if depth == len(path) {
			lease.Release()
			return nil, pageKeyTreeBatchPosition{}, ErrKeyTreeDepth
		}
		rank := pageKeyBranchRank(view, hash)
		child, ok := view.BranchAt(rank)
		if !ok {
			lease.Release()
			return nil, pageKeyTreeBatchPosition{}, fmt.Errorf("%w: fingerprint batch scan child", ErrKeyDirectoryCorrupt)
		}
		path[depth] = pageKeyTreePathEntry{
			ref: ref, rank: uint16(rank), level: header.Level,
		}
		depth++
		lease.Release()
		expectedLevel = header.Level - 1
		haveExpectedLevel = true
		expectedMax = child.MaxHash
		haveExpectedMax = true
		ref = child.Child
	}
}

// rewriteRuns performs delete compaction bottom-up. At every level it rewrites
// maximal contiguous touched runs, admitting one current-root boundary sibling
// at a time only when the run cannot produce canonical half-full pages. The
// replacements produced at one level are therefore ordinary sorted children
// at the next level; no allocated page can fall outside the published result.
func (b *pageKeyTreeBatch) rewriteRuns(
	root PageRef, plans []pageKeyTreeBatchPlan,
) ([]pageKeyTreeBatchChild, error) {
	rootLease, rootView, err := acquirePageFingerprintDirectory(b.cache, root, b.bounds)
	if err != nil {
		return nil, err
	}
	rootLevel := rootView.Header().Level
	rootLease.Release()

	leafPositions := make(map[PageRef]pageKeyTreeBatchPosition, len(plans))
	plansByLeaf := make(map[PageRef][]pageKeyTreeBatchPlan, len(plans))
	for _, plan := range plans {
		position := pageKeyTreeBatchPosition{
			path: plan.path, depth: plan.depth, ref: plan.leaf, level: 0,
		}
		if previous, exists := leafPositions[plan.leaf]; exists &&
			!samePageKeyTreeBatchPosition(previous, position) {
			return nil, fmt.Errorf("%w: fingerprint batch duplicate leaf path", ErrKeyDirectoryCorrupt)
		}
		leafPositions[plan.leaf] = position
		plansByLeaf[plan.leaf] = append(plansByLeaf[plan.leaf], plan)
	}

	replacements, rewritten, err := b.rewriteLeafRuns(
		leafPositions, plansByLeaf, rootLevel == 0,
	)
	if err != nil {
		return nil, err
	}
	if rootLevel == 0 {
		return replacements[root], nil
	}

	active := pageKeyTreeBatchParents(rewritten)
	for level := uint8(1); level <= rootLevel; level++ {
		replacements, rewritten, err = b.rewriteBranchRuns(
			active, replacements, level, level == rootLevel,
		)
		if err != nil {
			return nil, err
		}
		if level == rootLevel {
			return replacements[root], nil
		}
		active = pageKeyTreeBatchParents(rewritten)
	}
	return nil, fmt.Errorf("%w: fingerprint batch root level", ErrKeyDirectoryCorrupt)
}

func (b *pageKeyTreeBatch) rewriteLeafRuns(
	active map[PageRef]pageKeyTreeBatchPosition,
	plans map[PageRef][]pageKeyTreeBatchPlan,
	root bool,
) (map[PageRef][]pageKeyTreeBatchChild, map[PageRef]pageKeyTreeBatchPosition, error) {
	for {
		runs, err := b.pageKeyTreeBatchRuns(active)
		if err != nil {
			return nil, nil, err
		}
		expanded := false
		for _, run := range runs {
			entries, err := b.pageKeyTreeBatchLeafEntries(run.positions, plans)
			if err != nil {
				return nil, nil, err
			}
			wholeLevel, err := b.pageKeyTreeBatchRunIsWholeLevel(run)
			if err != nil {
				return nil, nil, err
			}
			if _, ok := pageKeyTreeBatchLeafSpans(
				b.tx.options.PageSize, entries, !root && !wholeLevel,
			); ok {
				continue
			}
			if err := b.expandPageKeyTreeBatchRun(active, run); err != nil {
				return nil, nil, err
			}
			expanded = true
			break
		}
		if expanded {
			continue
		}

		replacements := make(map[PageRef][]pageKeyTreeBatchChild, len(active))
		for _, run := range runs {
			entries, err := b.pageKeyTreeBatchLeafEntries(run.positions, plans)
			if err != nil {
				return nil, nil, err
			}
			wholeLevel, err := b.pageKeyTreeBatchRunIsWholeLevel(run)
			if err != nil {
				return nil, nil, err
			}
			spans, ok := pageKeyTreeBatchLeafSpans(
				b.tx.options.PageSize, entries, !root && !wholeLevel,
			)
			if !ok {
				return nil, nil, fmt.Errorf("%w: fingerprint batch leaf occupancy", ErrInvalidWrite)
			}
			next, _, err := b.nextPageKeyTreeBatchPosition(run.positions[len(run.positions)-1])
			if err != nil {
				return nil, nil, err
			}
			oldNext := PageRef{}
			if next.ref != (PageRef{}) {
				oldNext = next.ref
			}
			children, err := b.encodePageKeyTreeBatchLeafSpans(
				run.positions, entries, spans, oldNext,
			)
			if err != nil {
				return nil, nil, err
			}
			replacements[run.positions[0].ref] = children
			for _, position := range run.positions {
				if _, exists := replacements[position.ref]; !exists {
					replacements[position.ref] = nil
				}
				b.retire(position.ref)
			}
		}
		return replacements, active, nil
	}
}

func (b *pageKeyTreeBatch) rewriteBranchRuns(
	active map[PageRef]pageKeyTreeBatchPosition,
	lower map[PageRef][]pageKeyTreeBatchChild,
	level uint8,
	root bool,
) (map[PageRef][]pageKeyTreeBatchChild, map[PageRef]pageKeyTreeBatchPosition, error) {
	for {
		runs, err := b.pageKeyTreeBatchRuns(active)
		if err != nil {
			return nil, nil, err
		}
		expanded := false
		for _, run := range runs {
			children, err := b.pageKeyTreeBatchBranchChildren(run.positions, lower, level)
			if err != nil {
				return nil, nil, err
			}
			wholeLevel, err := b.pageKeyTreeBatchRunIsWholeLevel(run)
			if err != nil {
				return nil, nil, err
			}
			if _, ok := pageKeyTreeBatchBranchSpans(
				b.tx.options.PageSize, len(children), !root && !wholeLevel,
			); ok {
				continue
			}
			if err := b.expandPageKeyTreeBatchRun(active, run); err != nil {
				return nil, nil, err
			}
			expanded = true
			break
		}
		if expanded {
			continue
		}

		replacements := make(map[PageRef][]pageKeyTreeBatchChild, len(active))
		for _, run := range runs {
			children, err := b.pageKeyTreeBatchBranchChildren(run.positions, lower, level)
			if err != nil {
				return nil, nil, err
			}
			if root && len(children) <= 1 {
				replacements[run.positions[0].ref] = children
			} else {
				wholeLevel, err := b.pageKeyTreeBatchRunIsWholeLevel(run)
				if err != nil {
					return nil, nil, err
				}
				spans, ok := pageKeyTreeBatchBranchSpans(
					b.tx.options.PageSize, len(children), !root && !wholeLevel,
				)
				if !ok {
					return nil, nil, fmt.Errorf("%w: fingerprint batch branch occupancy", ErrInvalidWrite)
				}
				encoded, err := b.encodePageKeyTreeBatchBranchSpans(
					run.positions, level, children, spans,
				)
				if err != nil {
					return nil, nil, err
				}
				replacements[run.positions[0].ref] = encoded
			}
			for _, position := range run.positions {
				if _, exists := replacements[position.ref]; !exists {
					replacements[position.ref] = nil
				}
				b.retire(position.ref)
			}
		}
		return replacements, active, nil
	}
}

func (b *pageKeyTreeBatch) pageKeyTreeBatchLeafEntries(
	positions []pageKeyTreeBatchPosition,
	plans map[PageRef][]pageKeyTreeBatchPlan,
) ([]PageKeyLocation, error) {
	var entries []PageKeyLocation
	for _, position := range positions {
		lease, view, err := acquirePageFingerprintDirectory(b.cache, position.ref, b.bounds)
		if err != nil {
			return nil, err
		}
		if view.Header().Level != 0 {
			lease.Release()
			return nil, fmt.Errorf("%w: fingerprint batch leaf level", ErrKeyDirectoryCorrupt)
		}
		part := make([]PageKeyLocation, view.Len())
		for rank := range part {
			var ok bool
			part[rank], ok = view.LocationAt(rank)
			if !ok {
				lease.Release()
				return nil, ErrKeyDirectoryCorrupt
			}
		}
		lease.Release()
		part, err = pageKeyTreeBatchApplyLeafPlans(part, plans[position.ref])
		if err != nil {
			return nil, err
		}
		if len(entries) != 0 && len(part) != 0 &&
			comparePageKeyLocation(entries[len(entries)-1], part[0]) >= 0 {
			return nil, fmt.Errorf("%w: fingerprint batch cross-leaf order", ErrKeyDirectoryCorrupt)
		}
		entries = append(entries, part...)
	}
	return entries, nil
}

func pageKeyTreeBatchApplyLeafPlans(
	entries []PageKeyLocation, plans []pageKeyTreeBatchPlan,
) ([]PageKeyLocation, error) {
	for _, plan := range plans {
		rank := pageKeyLocationRank(entries, plan.edit.Location)
		found := rank < len(entries) && samePageKeyIdentity(entries[rank], plan.edit.Location)
		switch plan.edit.Operation {
		case PageKeyTreeInsert:
			if found {
				return nil, fmt.Errorf("%w: duplicate fingerprint batch insertion", ErrKeyDirectoryCorrupt)
			}
			entries = append(entries, PageKeyLocation{})
			copy(entries[rank+1:], entries[rank:])
			entries[rank] = plan.edit.Location
		case PageKeyTreeDelete:
			if !found {
				return nil, fmt.Errorf("%w: missing fingerprint batch deletion", ErrKeyDirectoryCorrupt)
			}
			copy(entries[rank:], entries[rank+1:])
			entries = entries[:len(entries)-1]
		default:
			return nil, fmt.Errorf("%w: fingerprint batch operation", ErrInvalidWrite)
		}
	}
	return entries, nil
}

func (b *pageKeyTreeBatch) pageKeyTreeBatchBranchChildren(
	positions []pageKeyTreeBatchPosition,
	lower map[PageRef][]pageKeyTreeBatchChild,
	level uint8,
) ([]pageKeyTreeBatchChild, error) {
	var children []pageKeyTreeBatchChild
	for _, position := range positions {
		lease, view, err := acquirePageFingerprintDirectory(b.cache, position.ref, b.bounds)
		if err != nil {
			return nil, err
		}
		if view.Header().Level != level {
			lease.Release()
			return nil, fmt.Errorf("%w: fingerprint batch branch level", ErrKeyDirectoryCorrupt)
		}
		for rank := 0; rank < view.Len(); rank++ {
			entry, ok := view.BranchAt(rank)
			if !ok {
				lease.Release()
				return nil, ErrKeyDirectoryCorrupt
			}
			if replacement, changed := lower[entry.Child]; changed {
				childLease, childView, childErr := acquirePageFingerprintDirectory(
					b.cache, entry.Child, b.bounds,
				)
				if childErr != nil {
					lease.Release()
					return nil, childErr
				}
				childHeader := childView.Header()
				childLease.Release()
				if childHeader.Level+1 != level || childHeader.MaxHash != entry.MaxHash {
					lease.Release()
					return nil, fmt.Errorf("%w: fingerprint batch replaced child binding", ErrKeyDirectoryCorrupt)
				}
				children = append(children, replacement...)
				continue
			}
			childLease, childView, childErr := acquirePageFingerprintDirectory(
				b.cache, entry.Child, b.bounds,
			)
			if childErr != nil {
				lease.Release()
				return nil, childErr
			}
			childHeader := childView.Header()
			childLease.Release()
			if childHeader.Level+1 != level || childHeader.MaxHash != entry.MaxHash {
				lease.Release()
				return nil, fmt.Errorf("%w: fingerprint batch child level or maximum", ErrKeyDirectoryCorrupt)
			}
			children = append(children, pageKeyTreeBatchChild{
				min: childHeader.MinHash, max: childHeader.MaxHash, ref: entry.Child,
			})
		}
		lease.Release()
	}
	for index := 1; index < len(children); index++ {
		if children[index-1].max > children[index].min {
			return nil, fmt.Errorf("%w: fingerprint batch branch child order", ErrKeyDirectoryCorrupt)
		}
	}
	return children, nil
}

func pageKeyTreeBatchLeafSpans(
	pageSize uint32, entries []PageKeyLocation, requireMinimum bool,
) ([][2]int, bool) {
	if len(entries) == 0 {
		return nil, true
	}
	if !requireMinimum && pageKeyTreeLeafFits(pageSize, entries) {
		return [][2]int{{0, len(entries)}}, true
	}
	// A root exemption applies only while the result remains one leaf. Once a
	// split creates a parent, every output leaf is non-root and must satisfy the
	// same proof-backed body minimum as point compaction.
	requireMinimum = true
	minimum := pageKeyTreeLeafMinimumBodyBytes(pageSize)
	n := len(entries)
	best := make([]int, n+1)
	next := make([]int, n+1)
	for index := range best {
		best[index] = n + 1
	}
	best[n] = 0
	for first := n - 1; first >= 0; first-- {
		for last := first + 1; last <= n; last++ {
			part := entries[first:last]
			if !pageKeyTreeLeafFits(pageSize, part) {
				break
			}
			if requireMinimum && pageKeyTreeLeafBodyBytes(part) < minimum {
				continue
			}
			// For the same minimum page count, fill the left page as far as
			// possible. This is deterministic, leaves slack in the right edge
			// for monotonic inserts, and still proves every
			// non-root output against the exact body minimum.
			if best[last] != n+1 && best[last]+1 <= best[first] {
				best[first] = best[last] + 1
				next[first] = last
			}
		}
	}
	if best[0] == n+1 {
		return nil, false
	}
	spans := make([][2]int, 0, best[0])
	for first := 0; first < n; first = next[first] {
		if next[first] <= first {
			return nil, false
		}
		spans = append(spans, [2]int{first, next[first]})
	}
	return spans, true
}

func pageKeyTreeBatchBranchSpans(
	pageSize uint32, count int, requireMinimum bool,
) ([][2]int, bool) {
	if count == 0 {
		return nil, true
	}
	capacity := pageKeyTreeBranchCapacity(pageSize)
	if capacity == 0 {
		return nil, false
	}
	minimum := pageKeyTreeBranchMinimumChildren(pageSize)
	pages := (count + capacity - 1) / capacity
	if requireMinimum && count < minimum {
		return nil, false
	}
	if pages > 1 && count < pages*minimum {
		return nil, false
	}
	base, extra := count/pages, count%pages
	spans := make([][2]int, pages)
	first := 0
	for index := 0; index < pages; index++ {
		length := base
		if index < extra {
			length++
		}
		if length > capacity || requireMinimum && length < minimum {
			return nil, false
		}
		spans[index] = [2]int{first, first + length}
		first += length
	}
	return spans, true
}

func (b *pageKeyTreeBatch) encodePageKeyTreeBatchLeafSpans(
	positions []pageKeyTreeBatchPosition,
	entries []PageKeyLocation,
	spans [][2]int,
	oldNext PageRef,
) ([]pageKeyTreeBatchChild, error) {
	if len(spans) == 0 {
		return nil, nil
	}
	pages := make([]TransactionPage, len(spans))
	for index := range pages {
		logicalID := uint64(0)
		if index < len(positions) {
			logicalID = positions[index].ref.LogicalID
		}
		page, err := b.tx.Allocate(
			PageFingerprintDirectory, b.tx.options.PageSize, logicalID,
		)
		if err != nil {
			return nil, err
		}
		pages[index] = page
	}
	children := make([]pageKeyTreeBatchChild, len(spans))
	for index := len(spans) - 1; index >= 0; index-- {
		next := oldNext
		if index+1 < len(pages) {
			next = pages[index+1].Ref()
		}
		span := spans[index]
		part := entries[span[0]:span[1]]
		if err := encodePageKeyTreeLeafInto(b.tx, pages[index], part, next, b.bounds); err != nil {
			return nil, err
		}
		children[index] = pageKeyTreeBatchChild{
			min: part[0].Hash, max: part[len(part)-1].Hash, ref: pages[index].Ref(),
		}
	}
	return children, nil
}

func (b *pageKeyTreeBatch) encodePageKeyTreeBatchBranchSpans(
	positions []pageKeyTreeBatchPosition,
	level uint8,
	children []pageKeyTreeBatchChild,
	spans [][2]int,
) ([]pageKeyTreeBatchChild, error) {
	encoded := make([]pageKeyTreeBatchChild, 0, len(spans))
	for index, span := range spans {
		logicalID := uint64(0)
		if index < len(positions) {
			logicalID = positions[index].ref.LogicalID
		}
		part := children[span[0]:span[1]]
		branches := make([]PageKeyBranch, len(part))
		for rank, child := range part {
			branches[rank] = PageKeyBranch{MaxHash: child.max, Child: child.ref}
		}
		page, err := encodePageKeyTreeBranch(
			b.tx, logicalID, level, part[0].min, branches,
		)
		if err != nil {
			return nil, err
		}
		encoded = append(encoded, pageKeyTreeBatchChild{
			min: part[0].min, max: part[len(part)-1].max, ref: page.Ref(),
		})
	}
	return encoded, nil
}

func (b *pageKeyTreeBatch) pageKeyTreeBatchRuns(
	active map[PageRef]pageKeyTreeBatchPosition,
) ([]pageKeyTreeBatchRun, error) {
	positions := make([]pageKeyTreeBatchPosition, 0, len(active))
	for _, position := range active {
		positions = append(positions, position)
	}
	sort.Slice(positions, func(left, right int) bool {
		return comparePageKeyTreeBatchPosition(positions[left], positions[right]) < 0
	})
	runs := make([]pageKeyTreeBatchRun, 0, len(positions))
	for index := 0; index < len(positions); {
		run := pageKeyTreeBatchRun{positions: []pageKeyTreeBatchPosition{positions[index]}}
		index++
		for index < len(positions) {
			next, ok, err := b.nextPageKeyTreeBatchPosition(run.positions[len(run.positions)-1])
			if err != nil {
				return nil, err
			}
			if !ok || next.ref != positions[index].ref {
				break
			}
			run.positions = append(run.positions, positions[index])
			index++
		}
		runs = append(runs, run)
	}
	return runs, nil
}

func (b *pageKeyTreeBatch) expandPageKeyTreeBatchRun(
	active map[PageRef]pageKeyTreeBatchPosition,
	run pageKeyTreeBatchRun,
) error {
	previous, ok, err := b.previousPageKeyTreeBatchPosition(run.positions[0])
	if err != nil {
		return err
	}
	if ok {
		active[previous.ref] = previous
		return nil
	}
	next, ok, err := b.nextPageKeyTreeBatchPosition(run.positions[len(run.positions)-1])
	if err != nil {
		return err
	}
	if ok {
		active[next.ref] = next
		return nil
	}
	return fmt.Errorf("%w: fingerprint batch cannot satisfy occupancy", ErrInvalidWrite)
}

func (b *pageKeyTreeBatch) pageKeyTreeBatchRunIsWholeLevel(
	run pageKeyTreeBatchRun,
) (bool, error) {
	if len(run.positions) == 0 {
		return false, ErrKeyDirectoryCorrupt
	}
	_, previous, err := b.previousPageKeyTreeBatchPosition(run.positions[0])
	if err != nil || previous {
		return false, err
	}
	_, next, err := b.nextPageKeyTreeBatchPosition(run.positions[len(run.positions)-1])
	if err != nil {
		return false, err
	}
	return !next, nil
}

func (b *pageKeyTreeBatch) nextPageKeyTreeBatchPosition(
	position pageKeyTreeBatchPosition,
) (pageKeyTreeBatchPosition, bool, error) {
	return b.adjacentPageKeyTreeBatchPosition(position, true)
}

func (b *pageKeyTreeBatch) previousPageKeyTreeBatchPosition(
	position pageKeyTreeBatchPosition,
) (pageKeyTreeBatchPosition, bool, error) {
	return b.adjacentPageKeyTreeBatchPosition(position, false)
}

func (b *pageKeyTreeBatch) adjacentPageKeyTreeBatchPosition(
	position pageKeyTreeBatchPosition, forward bool,
) (pageKeyTreeBatchPosition, bool, error) {
	if err := b.validatePageKeyTreeBatchPosition(position); err != nil {
		return pageKeyTreeBatchPosition{}, false, err
	}
	for ancestor := position.depth - 1; ancestor >= 0; ancestor-- {
		pathEntry := position.path[ancestor]
		lease, view, err := acquirePageFingerprintDirectory(b.cache, pathEntry.ref, b.bounds)
		if err != nil {
			return pageKeyTreeBatchPosition{}, false, err
		}
		if view.Header().Level != pathEntry.level ||
			int(pathEntry.rank) >= view.Len() {
			lease.Release()
			return pageKeyTreeBatchPosition{}, false, fmt.Errorf("%w: fingerprint batch adjacent path", ErrKeyDirectoryCorrupt)
		}
		rank := int(pathEntry.rank)
		if forward {
			rank++
			if rank >= view.Len() {
				lease.Release()
				continue
			}
		} else {
			rank--
			if rank < 0 {
				lease.Release()
				continue
			}
		}
		child, ok := view.BranchAt(rank)
		lease.Release()
		if !ok {
			return pageKeyTreeBatchPosition{}, false, ErrKeyDirectoryCorrupt
		}
		path := position.path
		path[ancestor].rank = uint16(rank)
		return b.descendPageKeyTreeBatchPosition(
			path, ancestor+1, child.Child, pathEntry.level-1,
			child.MaxHash, position.level, forward,
		)
	}
	return pageKeyTreeBatchPosition{}, false, nil
}

func (b *pageKeyTreeBatch) descendPageKeyTreeBatchPosition(
	path [pageKeyDirectoryMaxLevel]pageKeyTreePathEntry,
	depth int,
	ref PageRef,
	expectedLevel uint8,
	expectedMax uint64,
	targetLevel uint8,
	left bool,
) (pageKeyTreeBatchPosition, bool, error) {
	for {
		lease, view, err := acquirePageFingerprintDirectory(b.cache, ref, b.bounds)
		if err != nil {
			return pageKeyTreeBatchPosition{}, false, err
		}
		header := view.Header()
		if header.Level != expectedLevel || header.MaxHash != expectedMax ||
			header.Level < targetLevel {
			lease.Release()
			return pageKeyTreeBatchPosition{}, false, fmt.Errorf("%w: fingerprint batch adjacent binding", ErrKeyDirectoryCorrupt)
		}
		if header.Level == targetLevel {
			lease.Release()
			return pageKeyTreeBatchPosition{
				path: path, depth: depth, ref: ref, level: targetLevel,
			}, true, nil
		}
		if depth == len(path) || view.Len() == 0 {
			lease.Release()
			return pageKeyTreeBatchPosition{}, false, ErrKeyTreeDepth
		}
		rank := 0
		if !left {
			rank = view.Len() - 1
		}
		child, ok := view.BranchAt(rank)
		path[depth] = pageKeyTreePathEntry{
			ref: ref, rank: uint16(rank), level: header.Level,
		}
		depth++
		lease.Release()
		if !ok {
			return pageKeyTreeBatchPosition{}, false, ErrKeyDirectoryCorrupt
		}
		expectedLevel = header.Level - 1
		expectedMax = child.MaxHash
		ref = child.Child
	}
}

func (b *pageKeyTreeBatch) validatePageKeyTreeBatchPosition(
	position pageKeyTreeBatchPosition,
) error {
	if position.depth < 0 || position.depth > len(position.path) ||
		position.ref == (PageRef{}) {
		return fmt.Errorf("%w: fingerprint batch position", ErrKeyDirectoryCorrupt)
	}
	for index := 0; index < position.depth; index++ {
		entry := position.path[index]
		lease, view, err := acquirePageFingerprintDirectory(b.cache, entry.ref, b.bounds)
		if err != nil {
			return err
		}
		if view.Header().Level != entry.level || entry.level == 0 ||
			int(entry.rank) >= view.Len() {
			lease.Release()
			return fmt.Errorf("%w: fingerprint batch position parent", ErrKeyDirectoryCorrupt)
		}
		child, ok := view.BranchAt(int(entry.rank))
		lease.Release()
		if !ok {
			return ErrKeyDirectoryCorrupt
		}
		want := position.ref
		if index+1 < position.depth {
			want = position.path[index+1].ref
			if position.path[index+1].level+1 != entry.level {
				return fmt.Errorf("%w: fingerprint batch position levels", ErrKeyDirectoryCorrupt)
			}
		} else if position.level+1 != entry.level {
			return fmt.Errorf("%w: fingerprint batch position target level", ErrKeyDirectoryCorrupt)
		}
		if child.Child != want {
			return fmt.Errorf("%w: fingerprint batch position child", ErrKeyDirectoryCorrupt)
		}
		childLease, childView, err := acquirePageFingerprintDirectory(b.cache, want, b.bounds)
		if err != nil {
			return err
		}
		childHeader := childView.Header()
		childLease.Release()
		if childHeader.Level+1 != entry.level || childHeader.MaxHash != child.MaxHash {
			return fmt.Errorf("%w: fingerprint batch position child binding", ErrKeyDirectoryCorrupt)
		}
	}
	if position.depth == 0 {
		lease, view, err := acquirePageFingerprintDirectory(b.cache, position.ref, b.bounds)
		if err != nil {
			return err
		}
		level := view.Header().Level
		lease.Release()
		if level != position.level {
			return fmt.Errorf("%w: fingerprint batch root position level", ErrKeyDirectoryCorrupt)
		}
	}
	return nil
}

func pageKeyTreeBatchParents(
	children map[PageRef]pageKeyTreeBatchPosition,
) map[PageRef]pageKeyTreeBatchPosition {
	parents := make(map[PageRef]pageKeyTreeBatchPosition, len(children))
	for _, child := range children {
		if child.depth == 0 {
			continue
		}
		entry := child.path[child.depth-1]
		parent := pageKeyTreeBatchPosition{
			path: child.path, depth: child.depth - 1, ref: entry.ref, level: entry.level,
		}
		parents[parent.ref] = parent
	}
	return parents
}

func comparePageKeyTreeBatchPosition(left, right pageKeyTreeBatchPosition) int {
	depth := min(left.depth, right.depth)
	for index := 0; index < depth; index++ {
		if left.path[index].rank < right.path[index].rank {
			return -1
		}
		if left.path[index].rank > right.path[index].rank {
			return 1
		}
	}
	if left.depth < right.depth {
		return -1
	}
	if left.depth > right.depth {
		return 1
	}
	return 0
}

func samePageKeyTreeBatchPosition(left, right pageKeyTreeBatchPosition) bool {
	return left.ref == right.ref && left.level == right.level &&
		left.depth == right.depth &&
		comparePageKeyTreeBatchPosition(left, right) == 0
}

func (b *pageKeyTreeBatch) finish(children []pageKeyTreeBatchChild) (PageKeyTreeBatchMutation, error) {
	mutation := PageKeyTreeBatchMutation{
		Retired: b.retired, Applied: b.applied, Changed: true,
	}
	level := uint8(0)
	if len(children) > 1 {
		var err error
		level, err = pageKeyTreePageLevel(b.cache, children[0].ref, b.tx, b.bounds)
		if err != nil {
			return mutation, err
		}
	}
	for len(children) > 1 {
		if level >= pageKeyDirectoryMaxLevel {
			return mutation, ErrKeyTreeDepth
		}
		level++
		var err error
		children, err = b.encodeBranches(nil, level, 0, children)
		if err != nil {
			return mutation, err
		}
	}
	if len(children) == 1 {
		mutation.Root = children[0].ref
	}
	return mutation, nil
}

func (b *pageKeyTreeBatch) encodeLeaves(
	dst []pageKeyTreeBatchChild, logicalID uint64, oldNext PageRef,
	entries []PageKeyLocation,
) ([]pageKeyTreeBatchChild, error) {
	spans, ok := pageKeyTreeBatchLeafSpans(b.tx.options.PageSize, entries, false)
	if !ok {
		return nil, fmt.Errorf("%w: fingerprint batch leaf does not fit", ErrInvalidWrite)
	}
	pages := make([]TransactionPage, len(spans))
	for index := range pages {
		id := uint64(0)
		if index == 0 {
			id = logicalID
		}
		page, err := b.tx.Allocate(PageFingerprintDirectory, b.tx.options.PageSize, id)
		if err != nil {
			return nil, err
		}
		pages[index] = page
	}
	for index, span := range spans {
		next := oldNext
		if index+1 < len(pages) {
			next = pages[index+1].Ref()
		}
		part := entries[span[0]:span[1]]
		if err := encodePageKeyTreeLeafInto(b.tx, pages[index], part, next, b.bounds); err != nil {
			return nil, err
		}
		dst = append(dst, pageKeyTreeBatchChild{
			min: part[0].Hash, max: part[len(part)-1].Hash, ref: pages[index].Ref(),
		})
	}
	return dst, nil
}

func (b *pageKeyTreeBatch) encodeBranches(
	dst []pageKeyTreeBatchChild, level uint8, logicalID uint64,
	children []pageKeyTreeBatchChild,
) ([]pageKeyTreeBatchChild, error) {
	spans, ok := pageKeyTreeBatchBranchSpans(
		b.tx.options.PageSize, len(children), false,
	)
	if !ok {
		return nil, fmt.Errorf("%w: fingerprint batch branch capacity", ErrInvalidWrite)
	}
	for _, span := range spans {
		part := children[span[0]:span[1]]
		branches := make([]PageKeyBranch, len(part))
		for index := range part {
			branches[index] = PageKeyBranch{
				MaxHash: part[index].max, Child: part[index].ref,
			}
		}
		page, err := encodePageKeyTreeBranch(
			b.tx, logicalID, level, part[0].min, branches,
		)
		if err != nil {
			return nil, err
		}
		dst = append(dst, pageKeyTreeBatchChild{
			min: part[0].min, max: part[len(part)-1].max, ref: page.Ref(),
		})
		logicalID = 0
	}
	return dst, nil
}

// PageKeyTreeBatchPages is a reservation upper bound for metadata pages a
// batch can publish, not an estimate of normal write amplification.
//
// At each of the fixed tree depths, E edits can select at most E old pages and
// can add at most E output pages through splits. Building any required levels
// above the rewritten root costs at most E more pages per depth (branch pages
// always hold at least two children). One fixed page per depth covers root
// promotion and the empty-tree boundary. Saturation preserves the upper-bound
// contract when the int multiplication would overflow. A MaxInt result cannot
// be increased: callers adding a publication-root page must reject it or use a
// checked/capped addition.
func PageKeyTreeBatchPages(edits int) int {
	if edits <= 0 {
		return 0
	}
	const depth = int(pageKeyDirectoryMaxLevel) + 1
	const perEdit = 3 * depth
	const fixed = depth
	maxInt := int(^uint(0) >> 1)
	if edits > (maxInt-fixed)/perEdit {
		return maxInt
	}
	return edits*perEdit + fixed
}
