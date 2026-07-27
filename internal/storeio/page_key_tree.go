package storeio

import (
	"fmt"
)

// PageKeyTreeBounds are copied from the state root and superblock selecting
// the immutable fingerprint tree. The chunk bounds validate stable row
// addresses; resolving those addresses remains the chunk tree's job.
//
// Leaves are globally ordered by (hash, chunk, slot). Branches intentionally
// store hash maxima only, so equal-hash insertion preserves the stronger
// identity order by walking the current-root collision run to its destination
// leaf rather than blindly choosing the first hash child.
type PageKeyTreeBounds struct {
	FileEnd        uint64
	NextLogicalID  uint64
	ChunkHighWater uint32
	ChunkDocuments uint32
}

// PageKeyTreeMutation is one copy-on-write fingerprint-tree mutation. Retired
// pages remain protected by the old generation until the caller publishes and
// advances its generation fence. Point compaction retires one selected page at
// every level plus at most one sibling below each parent: for the 16-page
// maximum path the exact bound is 2*16-1 = 31. Callers consume only the prefix
// selected by RetiredCount.
type PageKeyTreeMutation struct {
	Root         PageRef
	Retired      [2*pageKeyDirectoryMaxLevel + 1]PageRef
	RetiredCount uint8
	Found        bool
	Changed      bool
}

func (m *PageKeyTreeMutation) retire(ref PageRef) error {
	for index := uint8(0); index < m.RetiredCount; index++ {
		if m.Retired[index] == ref {
			return fmt.Errorf("%w: duplicate fingerprint retirement", ErrKeyDirectoryCorrupt)
		}
	}
	if int(m.RetiredCount) == len(m.Retired) {
		return ErrKeyTreeDepth
	}
	m.Retired[m.RetiredCount] = ref
	m.RetiredCount++
	return nil
}

type pageKeyTreePathEntry struct {
	ref   PageRef
	rank  uint16
	level uint8
}

// PageKeyTreeCursor enumerates every stable location carrying one hash.
//
// A hash is only a pruning key. Callers must resolve each returned location
// through the chunk tree and compare the complete key in the document page.
// The cursor is single-owner, must not be copied after first use, and should be
// closed when iteration stops early.
//
// Collision continuation is reconstructed through the current root's parent
// path. Header.Next is deliberately ignored because a physical leaf link may
// name an older immutable generation after copy-on-write.
type PageKeyTreeCursor struct {
	cache  *PageCache
	bounds PageKeyTreeBounds
	hash   uint64

	path  [pageKeyDirectoryMaxLevel]pageKeyTreePathEntry
	depth int

	leafRef  PageRef
	lease    PageLease
	view     PageKeyDirectoryView
	at       int
	end      int
	done     bool
	last     PageKeyLocation
	haveLast bool
}

// PageKeyTreeScanner enumerates every stable location in global
// (hash, chunk, slot) order.
//
// It is the allocation-free metadata image path used to build optional
// accelerators from the authoritative fingerprint tree. Successor leaves are
// reconstructed through the selected root's parent path: Header.Next is only
// a physical-locality hint and may name an older copy-on-write generation.
//
// The scanner retains at most one directory-page lease between calls. It is
// single-owner, must not be copied after first use, and should be closed when
// iteration stops early.
type PageKeyTreeScanner struct {
	cache  *PageCache
	bounds PageKeyTreeBounds

	path  [pageKeyDirectoryMaxLevel]pageKeyTreePathEntry
	depth int

	lease    PageLease
	view     PageKeyDirectoryView
	at       int
	done     bool
	last     PageKeyLocation
	haveLast bool
}

// OpenPageKeyTreeScanner positions an ordered scanner at the first location.
// An empty root returns an exhausted scanner without error.
func OpenPageKeyTreeScanner(
	cache *PageCache, root PageRef, bounds PageKeyTreeBounds,
) (PageKeyTreeScanner, error) {
	scanner := PageKeyTreeScanner{cache: cache, bounds: bounds, done: true}
	if root == (PageRef{}) {
		return scanner, nil
	}
	if err := validatePageKeyTreeRead(cache, root, bounds); err != nil {
		return PageKeyTreeScanner{}, err
	}
	ref := root
	expectedLevel := uint8(0)
	haveExpectedLevel := false
	expectedMax := uint64(0)
	haveExpectedMax := false
	for {
		lease, view, err := acquirePageFingerprintDirectory(cache, ref, bounds)
		if err != nil {
			return PageKeyTreeScanner{}, err
		}
		header := view.Header()
		if haveExpectedLevel && header.Level != expectedLevel {
			lease.Release()
			return PageKeyTreeScanner{}, fmt.Errorf(
				"%w: fingerprint-tree scan level", ErrKeyDirectoryCorrupt,
			)
		}
		if haveExpectedMax && header.MaxHash != expectedMax {
			lease.Release()
			return PageKeyTreeScanner{}, fmt.Errorf(
				"%w: fingerprint-tree scan maximum", ErrKeyDirectoryCorrupt,
			)
		}
		if header.Level == 0 {
			scanner.lease = lease
			scanner.view = view
			scanner.done = false
			return scanner, nil
		}
		if scanner.depth == len(scanner.path) {
			lease.Release()
			return PageKeyTreeScanner{}, ErrKeyTreeDepth
		}
		first, ok := view.BranchAt(0)
		if !ok {
			lease.Release()
			return PageKeyTreeScanner{}, fmt.Errorf(
				"%w: fingerprint-tree scan branch", ErrKeyDirectoryCorrupt,
			)
		}
		scanner.path[scanner.depth] = pageKeyTreePathEntry{
			ref: ref, rank: 0, level: header.Level,
		}
		scanner.depth++
		lease.Release()
		expectedLevel = header.Level - 1
		haveExpectedLevel = true
		expectedMax = first.MaxHash
		haveExpectedMax = true
		ref = first.Child
	}
}

// OpenPageKeyTreeCursor positions a collision iterator at the first leaf that
// can contain hash. An empty root or an out-of-range hash returns an exhausted
// cursor without error.
func OpenPageKeyTreeCursor(cache *PageCache, root PageRef, hash uint64, bounds PageKeyTreeBounds) (PageKeyTreeCursor, error) {
	cursor := PageKeyTreeCursor{cache: cache, bounds: bounds, hash: hash, done: true}
	if root == (PageRef{}) {
		return cursor, nil
	}
	if err := validatePageKeyTreeRead(cache, root, bounds); err != nil {
		return PageKeyTreeCursor{}, err
	}
	ref := root
	expectedLevel := uint8(0)
	haveExpectedLevel := false
	expectedMax := uint64(0)
	haveExpectedMax := false
	for {
		lease, view, err := acquirePageFingerprintDirectory(cache, ref, bounds)
		if err != nil {
			return PageKeyTreeCursor{}, err
		}
		header := view.Header()
		if haveExpectedLevel && header.Level != expectedLevel {
			lease.Release()
			return PageKeyTreeCursor{}, fmt.Errorf("%w: fingerprint-tree level", ErrKeyDirectoryCorrupt)
		}
		if haveExpectedMax && header.MaxHash != expectedMax {
			lease.Release()
			return PageKeyTreeCursor{}, fmt.Errorf("%w: fingerprint-tree child maximum", ErrKeyDirectoryCorrupt)
		}
		if hash < header.MinHash || hash > header.MaxHash {
			lease.Release()
			return cursor, nil
		}
		if header.Level == 0 {
			first, end, ok := view.CandidateRange(hash)
			if !ok {
				lease.Release()
				return cursor, nil
			}
			cursor.leafRef = ref
			cursor.lease = lease
			cursor.view = view
			cursor.at = first
			cursor.end = end
			cursor.done = false
			return cursor, nil
		}
		if cursor.depth == len(cursor.path) {
			lease.Release()
			return PageKeyTreeCursor{}, ErrKeyTreeDepth
		}
		branch, rank, ok := view.SelectBranch(hash)
		if !ok {
			lease.Release()
			return cursor, nil
		}
		cursor.path[cursor.depth] = pageKeyTreePathEntry{
			ref: ref, rank: uint16(rank), level: header.Level,
		}
		cursor.depth++
		lease.Release()
		expectedLevel = header.Level - 1
		haveExpectedLevel = true
		expectedMax = branch.MaxHash
		haveExpectedMax = true
		ref = branch.Child
	}
}

// FirstPageKeyTreeCandidate returns the first stable location carrying hash.
//
// It is the allocation-free common point-read path: unlike
// OpenPageKeyTreeCursor it does not retain a parent path for collision
// continuation. The hash remains only a pruning hint. Callers must resolve the
// returned location through the authoritative document block and compare the
// complete key; after a mismatch they must use OpenPageKeyTreeCursor to scan
// the collision run.
func FirstPageKeyTreeCandidate(
	cache *PageCache, root PageRef, hash uint64, bounds PageKeyTreeBounds,
) (PageKeyLocation, bool, error) {
	if root == (PageRef{}) {
		return PageKeyLocation{}, false, nil
	}
	if err := validatePageKeyTreeRead(cache, root, bounds); err != nil {
		return PageKeyLocation{}, false, err
	}
	ref := root
	expectedLevel := uint8(0)
	haveExpectedLevel := false
	expectedMax := uint64(0)
	haveExpectedMax := false
	for depth := 0; ; depth++ {
		if depth > int(pageKeyDirectoryMaxLevel) {
			return PageKeyLocation{}, false, ErrKeyTreeDepth
		}
		lease, view, err := acquirePageFingerprintDirectory(cache, ref, bounds)
		if err != nil {
			return PageKeyLocation{}, false, err
		}
		header := view.Header()
		if haveExpectedLevel && header.Level != expectedLevel {
			lease.Release()
			return PageKeyLocation{}, false, fmt.Errorf(
				"%w: fingerprint-tree candidate level", ErrKeyDirectoryCorrupt,
			)
		}
		if haveExpectedMax && header.MaxHash != expectedMax {
			lease.Release()
			return PageKeyLocation{}, false, fmt.Errorf(
				"%w: fingerprint-tree candidate maximum", ErrKeyDirectoryCorrupt,
			)
		}
		if hash < header.MinHash || hash > header.MaxHash {
			lease.Release()
			return PageKeyLocation{}, false, nil
		}
		if header.Level == 0 {
			first, _, ok := view.CandidateRange(hash)
			if !ok {
				lease.Release()
				return PageKeyLocation{}, false, nil
			}
			location, ok := view.LocationAt(first)
			lease.Release()
			if !ok || location.Hash != hash {
				return PageKeyLocation{}, false, fmt.Errorf(
					"%w: fingerprint-tree first candidate", ErrKeyDirectoryCorrupt,
				)
			}
			return location, true, nil
		}
		branch, _, ok := view.SelectBranch(hash)
		lease.Release()
		if !ok {
			return PageKeyLocation{}, false, nil
		}
		expectedLevel = header.Level - 1
		haveExpectedLevel = true
		expectedMax = branch.MaxHash
		haveExpectedMax = true
		ref = branch.Child
	}
}

// Next returns the next globally ordered location. Exhaustion closes the
// scanner. The returned value is detached from the page lease.
func (s *PageKeyTreeScanner) Next() (PageKeyLocation, bool, error) {
	if s == nil || s.done {
		return PageKeyLocation{}, false, nil
	}
	for {
		if s.at < s.view.Len() {
			location, ok := s.view.LocationAt(s.at)
			s.at++
			if !ok {
				s.Close()
				return PageKeyLocation{}, false, fmt.Errorf(
					"%w: fingerprint-tree scan location", ErrKeyDirectoryCorrupt,
				)
			}
			if s.haveLast && comparePageKeyLocation(s.last, location) >= 0 {
				s.Close()
				return PageKeyLocation{}, false, fmt.Errorf(
					"%w: fingerprint-tree scan order", ErrKeyDirectoryCorrupt,
				)
			}
			s.last = location
			s.haveLast = true
			return location, true, nil
		}
		s.lease.Release()
		s.view = PageKeyDirectoryView{}
		ref, expectedMax, ok, err := nextPageKeyTreeLeaf(
			s.cache, &s.path, &s.depth, s.bounds,
		)
		if err != nil || !ok {
			s.done = true
			return PageKeyLocation{}, false, err
		}
		lease, view, err := acquirePageFingerprintDirectory(s.cache, ref, s.bounds)
		if err != nil {
			s.done = true
			return PageKeyLocation{}, false, err
		}
		header := view.Header()
		if header.Level != 0 || header.MaxHash != expectedMax {
			lease.Release()
			s.done = true
			return PageKeyLocation{}, false, fmt.Errorf(
				"%w: fingerprint-tree scan successor", ErrKeyDirectoryCorrupt,
			)
		}
		s.lease = lease
		s.view = view
		s.at = 0
	}
}

// Close releases the scanner's current page lease. It is idempotent.
func (s *PageKeyTreeScanner) Close() {
	if s == nil {
		return
	}
	s.lease.Release()
	s.view = PageKeyDirectoryView{}
	s.done = true
}

// Next returns the next candidate. Exhaustion closes the cursor.
func (c *PageKeyTreeCursor) Next() (PageKeyLocation, bool, error) {
	if c == nil || c.done {
		return PageKeyLocation{}, false, nil
	}
	for {
		if c.at < c.end {
			location, ok := c.view.LocationAt(c.at)
			c.at++
			if !ok || location.Hash != c.hash {
				c.Close()
				return PageKeyLocation{}, false, fmt.Errorf("%w: fingerprint candidate", ErrKeyDirectoryCorrupt)
			}
			if c.haveLast && comparePageKeyLocation(c.last, location) >= 0 {
				c.Close()
				return PageKeyLocation{}, false, fmt.Errorf("%w: fingerprint collision order", ErrKeyDirectoryCorrupt)
			}
			c.last = location
			c.haveLast = true
			return location, true, nil
		}
		follow := c.view.Header().MaxHash == c.hash
		c.lease.Release()
		c.view = PageKeyDirectoryView{}
		c.leafRef = PageRef{}
		if !follow {
			c.done = true
			return PageKeyLocation{}, false, nil
		}
		ref, expectedMax, ok, err := nextPageKeyTreeLeaf(c.cache, &c.path, &c.depth, c.bounds)
		if err != nil || !ok {
			c.done = true
			return PageKeyLocation{}, false, err
		}
		lease, view, err := acquirePageFingerprintDirectory(c.cache, ref, c.bounds)
		if err != nil {
			c.done = true
			return PageKeyLocation{}, false, err
		}
		header := view.Header()
		if header.Level != 0 || header.MaxHash != expectedMax {
			lease.Release()
			c.done = true
			return PageKeyLocation{}, false, fmt.Errorf("%w: fingerprint successor level or maximum", ErrKeyDirectoryCorrupt)
		}
		if header.MinHash > c.hash {
			lease.Release()
			c.done = true
			return PageKeyLocation{}, false, nil
		}
		first, end, candidates := view.CandidateRange(c.hash)
		if !candidates {
			lease.Release()
			c.done = true
			if header.MaxHash < c.hash {
				return PageKeyLocation{}, false, fmt.Errorf("%w: fingerprint successor order", ErrKeyDirectoryCorrupt)
			}
			return PageKeyLocation{}, false, nil
		}
		c.leafRef = ref
		c.lease = lease
		c.view = view
		c.at = first
		c.end = end
	}
}

// Close releases the current leaf. It is idempotent.
func (c *PageKeyTreeCursor) Close() {
	if c == nil {
		return
	}
	c.lease.Release()
	c.view = PageKeyDirectoryView{}
	c.leafRef = PageRef{}
	c.done = true
}

// LookupPageKeyTree resolves one exact fingerprint-tree identity.
func LookupPageKeyTree(cache *PageCache, root PageRef, target PageKeyLocation, bounds PageKeyTreeBounds) (PageKeyLocation, bool, error) {
	cursor, err := OpenPageKeyTreeCursor(cache, root, target.Hash, bounds)
	if err != nil {
		return PageKeyLocation{}, false, err
	}
	defer cursor.Close()
	for {
		location, ok, err := cursor.Next()
		if err != nil || !ok {
			return PageKeyLocation{}, false, err
		}
		if samePageKeyIdentity(location, target) {
			return location, true, nil
		}
	}
}

// InsertPageKeyTree inserts entry if its (hash, chunk, slot) identity is
// absent. An existing identity returns Found=true and writes no pages.
func InsertPageKeyTree(cache *PageCache, tx *WriteTransaction, root PageRef, entry PageKeyLocation, bounds PageKeyTreeBounds) (PageKeyTreeMutation, error) {
	return mutatePageKeyTree(cache, tx, root, entry, pageKeyMutationInsert, bounds)
}

// DeletePageKeyTree removes expected.
func DeletePageKeyTree(cache *PageCache, tx *WriteTransaction, root PageRef, expected PageKeyLocation, bounds PageKeyTreeBounds) (PageKeyTreeMutation, error) {
	return mutatePageKeyTree(cache, tx, root, expected, pageKeyMutationDelete, bounds)
}

type pageKeyMutationOperation uint8

const (
	pageKeyMutationInsert pageKeyMutationOperation = iota
	pageKeyMutationDelete
)

type pageKeyTreeRewrite struct {
	ref      PageRef
	min      uint64
	max      uint64
	rightRef PageRef
	rightMin uint64
	rightMax uint64
	empty    bool
}

// pageKeyTreeCompactNode is deliberately unencoded while deletion propagates
// toward the root. A parent can therefore merge or redistribute it with one
// immutable sibling before either replacement page is staged. This is what
// prevents an underfull staged page from becoming unreachable.
type pageKeyTreeCompactNode struct {
	ref       PageRef
	level     uint8
	logicalID uint64
	min       uint64
	max       uint64
	next      PageRef
	entries   []PageKeyLocation
	children  []PageKeyBranch
	empty     bool
}

type pageKeyTreeCompactChild struct {
	ref PageRef
	min uint64
	max uint64
}

func compactPageKeyTreePath(
	cache *PageCache, tx *WriteTransaction, ref PageRef,
	path [pageKeyDirectoryMaxLevel]pageKeyTreePathEntry, pathDepth, depth int,
	leaf PageRef, expected PageKeyLocation,
	bounds PageKeyTreeBounds, root bool,
	expectedMax uint64, haveExpectedMax bool, mutation *PageKeyTreeMutation,
) (pageKeyTreeCompactNode, error) {
	lease, view, err := acquirePageFingerprintDirectory(cache, ref, bounds)
	if err != nil {
		return pageKeyTreeCompactNode{}, err
	}
	defer lease.Release()
	header := view.Header()
	if haveExpectedMax && header.MaxHash != expectedMax {
		return pageKeyTreeCompactNode{}, fmt.Errorf("%w: fingerprint compaction child maximum", ErrKeyDirectoryCorrupt)
	}
	if header.Level == 0 {
		if depth != pathDepth || ref != leaf {
			return pageKeyTreeCompactNode{}, fmt.Errorf("%w: fingerprint compaction leaf path", ErrKeyDirectoryCorrupt)
		}
		node, materializeErr := materializePageKeyTreeCompactNode(ref, view)
		if materializeErr != nil {
			return pageKeyTreeCompactNode{}, materializeErr
		}
		position := -1
		for index, entry := range node.entries {
			if samePageKeyIdentity(entry, expected) {
				position = index
				break
			}
		}
		if position < 0 {
			return pageKeyTreeCompactNode{}, fmt.Errorf("%w: fingerprint compaction target", ErrKeyDirectoryCorrupt)
		}
		copy(node.entries[position:], node.entries[position+1:])
		node.entries = node.entries[:len(node.entries)-1]
		if err := mutation.retire(ref); err != nil {
			return pageKeyTreeCompactNode{}, err
		}
		if len(node.entries) == 0 {
			return pageKeyTreeCompactNode{empty: true}, nil
		}
		node.min = node.entries[0].Hash
		node.max = node.entries[len(node.entries)-1].Hash
		return node, nil
	}

	if depth >= pathDepth {
		return pageKeyTreeCompactNode{}, fmt.Errorf("%w: fingerprint compaction branch path", ErrKeyDirectoryCorrupt)
	}
	pathEntry := path[depth]
	if pathEntry.ref != ref || pathEntry.level != header.Level ||
		int(pathEntry.rank) >= view.Len() {
		return pageKeyTreeCompactNode{}, fmt.Errorf("%w: fingerprint compaction path identity", ErrKeyDirectoryCorrupt)
	}
	children := make([]PageKeyBranch, view.Len())
	for rank := range children {
		child, ok := view.BranchAt(rank)
		if !ok {
			return pageKeyTreeCompactNode{}, ErrKeyDirectoryCorrupt
		}
		children[rank] = child
	}
	rank := int(pathEntry.rank)
	childNode, err := compactPageKeyTreePath(
		cache, tx, children[rank].Child, path, pathDepth, depth+1, leaf,
		expected, bounds, false,
		children[rank].MaxHash, true, mutation,
	)
	if err != nil {
		return pageKeyTreeCompactNode{}, err
	}

	minHash := header.MinHash
	switch {
	case childNode.empty:
		copy(children[rank:], children[rank+1:])
		children = children[:len(children)-1]
		if rank == 0 && len(children) != 0 {
			minHash, _, err = pageKeyTreePageRange(cache, children[0].Child, tx, bounds)
			if err != nil {
				return pageKeyTreeCompactNode{}, err
			}
		}
	case root && len(children) == 1:
		if err := mutation.retire(ref); err != nil {
			return pageKeyTreeCompactNode{}, err
		}
		return childNode, nil
	default:
		var replacements []pageKeyTreeCompactChild
		var first int
		var width int
		replacements, first, width, err = commitPageKeyTreeCompactChild(
			cache, tx, children, rank, childNode, bounds, mutation,
		)
		if err != nil {
			return pageKeyTreeCompactNode{}, err
		}
		updated := make([]PageKeyBranch, 0, len(children)-2+len(replacements))
		updated = append(updated, children[:first]...)
		for _, replacement := range replacements {
			updated = append(updated, PageKeyBranch{
				MaxHash: replacement.max, Child: replacement.ref,
			})
		}
		updated = append(updated, children[first+width:]...)
		children = updated
		if first == 0 {
			minHash = replacements[0].min
		}
	}

	if err := mutation.retire(ref); err != nil {
		return pageKeyTreeCompactNode{}, err
	}
	if len(children) == 0 {
		return pageKeyTreeCompactNode{empty: true}, nil
	}
	if root && len(children) == 1 {
		childMin, childMax, rangeErr := pageKeyTreePageRange(cache, children[0].Child, tx, bounds)
		if rangeErr != nil {
			return pageKeyTreeCompactNode{}, rangeErr
		}
		return pageKeyTreeCompactNode{
			ref: children[0].Child, level: header.Level - 1,
			min: childMin, max: childMax,
		}, nil
	}
	return pageKeyTreeCompactNode{
		level: header.Level, logicalID: ref.LogicalID,
		min: minHash, max: children[len(children)-1].MaxHash,
		children: children,
	}, nil
}

func materializePageKeyTreeCompactNode(
	ref PageRef, view PageKeyDirectoryView,
) (pageKeyTreeCompactNode, error) {
	header := view.Header()
	node := pageKeyTreeCompactNode{
		level: header.Level, logicalID: ref.LogicalID,
		min: header.MinHash, max: header.MaxHash, next: header.Next,
	}
	if header.Level == 0 {
		node.entries = make([]PageKeyLocation, view.Len())
		for rank := range node.entries {
			entry, ok := view.LocationAt(rank)
			if !ok {
				return pageKeyTreeCompactNode{}, ErrKeyDirectoryCorrupt
			}
			node.entries[rank] = entry
		}
		return node, nil
	}
	node.children = make([]PageKeyBranch, view.Len())
	for rank := range node.children {
		child, ok := view.BranchAt(rank)
		if !ok {
			return pageKeyTreeCompactNode{}, ErrKeyDirectoryCorrupt
		}
		node.children[rank] = child
	}
	return node, nil
}

func commitPageKeyTreeCompactChild(
	cache *PageCache, tx *WriteTransaction, children []PageKeyBranch, rank int,
	node pageKeyTreeCompactNode, bounds PageKeyTreeBounds,
	mutation *PageKeyTreeMutation,
) ([]pageKeyTreeCompactChild, int, int, error) {
	if !pageKeyTreeCompactNodeUnderfull(tx.options.PageSize, node) {
		page, err := encodePageKeyTreeCompactNode(tx, node, bounds)
		if err != nil {
			return nil, 0, 0, err
		}
		return []pageKeyTreeCompactChild{{
			ref: page.Ref(), min: node.min, max: node.max,
		}}, rank, 1, nil
	}
	if len(children) <= 1 {
		return nil, 0, 0, fmt.Errorf("%w: underfull fingerprint child without sibling", ErrKeyDirectoryCorrupt)
	}

	first := rank
	siblingRank := rank + 1
	if rank != 0 {
		first = rank - 1
		siblingRank = rank - 1
	}
	siblingRef := children[siblingRank].Child
	siblingLease, siblingView, err := acquirePageFingerprintDirectory(cache, siblingRef, bounds)
	if err != nil {
		return nil, 0, 0, err
	}
	sibling, err := materializePageKeyTreeCompactNode(siblingRef, siblingView)
	siblingLease.Release()
	if err != nil {
		return nil, 0, 0, err
	}
	if sibling.level != node.level || sibling.max != children[siblingRank].MaxHash {
		return nil, 0, 0, fmt.Errorf("%w: fingerprint compaction sibling", ErrKeyDirectoryCorrupt)
	}
	if err := mutation.retire(siblingRef); err != nil {
		return nil, 0, 0, err
	}
	left, right := sibling, node
	if rank == 0 {
		left, right = node, sibling
	}
	replacements, err := redistributePageKeyTreeCompactSiblings(
		cache, tx, left, right, bounds,
	)
	if err != nil {
		return nil, 0, 0, err
	}
	return replacements, first, 2, nil
}

func redistributePageKeyTreeCompactSiblings(
	cache *PageCache, tx *WriteTransaction,
	left, right pageKeyTreeCompactNode, bounds PageKeyTreeBounds,
) ([]pageKeyTreeCompactChild, error) {
	if left.level != right.level || left.empty || right.empty {
		return nil, fmt.Errorf("%w: fingerprint compaction sibling level", ErrKeyDirectoryCorrupt)
	}
	if left.level == 0 {
		if len(left.entries) == 0 || len(right.entries) == 0 ||
			comparePageKeyLocation(left.entries[len(left.entries)-1], right.entries[0]) >= 0 {
			return nil, fmt.Errorf("%w: fingerprint compaction leaf order", ErrKeyDirectoryCorrupt)
		}
		entries := make([]PageKeyLocation, 0, len(left.entries)+len(right.entries))
		entries = append(entries, left.entries...)
		entries = append(entries, right.entries...)
		if pageKeyTreeLeafFits(tx.options.PageSize, entries) {
			merged := pageKeyTreeCompactNode{
				level: 0, logicalID: left.logicalID, next: right.next,
				min: entries[0].Hash, max: entries[len(entries)-1].Hash,
				entries: entries,
			}
			page, err := encodePageKeyTreeCompactNode(tx, merged, bounds)
			if err != nil {
				return nil, err
			}
			return []pageKeyTreeCompactChild{{
				ref: page.Ref(), min: merged.min, max: merged.max,
			}}, nil
		}
		split := pageKeyTreeLeafBalancedSplit(tx.options.PageSize, entries)
		if split == 0 {
			return nil, fmt.Errorf("%w: fingerprint leaf redistribution", ErrInvalidWrite)
		}
		leftPage, err := tx.Allocate(
			PageFingerprintDirectory, tx.options.PageSize, left.logicalID,
		)
		if err != nil {
			return nil, err
		}
		rightPage, err := tx.Allocate(
			PageFingerprintDirectory, tx.options.PageSize, right.logicalID,
		)
		if err != nil {
			return nil, err
		}
		if err := encodePageKeyTreeLeafInto(
			tx, rightPage, entries[split:], right.next, bounds,
		); err != nil {
			return nil, err
		}
		if err := encodePageKeyTreeLeafInto(
			tx, leftPage, entries[:split], rightPage.Ref(), bounds,
		); err != nil {
			return nil, err
		}
		return []pageKeyTreeCompactChild{
			{
				ref: leftPage.Ref(), min: entries[0].Hash,
				max: entries[split-1].Hash,
			},
			{
				ref: rightPage.Ref(), min: entries[split].Hash,
				max: entries[len(entries)-1].Hash,
			},
		}, nil
	}

	if len(left.children) == 0 || len(right.children) == 0 ||
		left.max > right.min {
		return nil, fmt.Errorf("%w: fingerprint compaction branch order", ErrKeyDirectoryCorrupt)
	}
	if err := validatePageKeyTreeCompactBranchBindings(
		cache, tx, left, bounds,
	); err != nil {
		return nil, err
	}
	if err := validatePageKeyTreeCompactBranchBindings(
		cache, tx, right, bounds,
	); err != nil {
		return nil, err
	}
	children := make([]PageKeyBranch, 0, len(left.children)+len(right.children))
	children = append(children, left.children...)
	children = append(children, right.children...)
	if pageKeyTreeBranchFits(tx.options.PageSize, children) {
		merged := pageKeyTreeCompactNode{
			level: left.level, logicalID: left.logicalID,
			min: left.min, max: children[len(children)-1].MaxHash,
			children: children,
		}
		page, err := encodePageKeyTreeCompactNode(tx, merged, bounds)
		if err != nil {
			return nil, err
		}
		return []pageKeyTreeCompactChild{{
			ref: page.Ref(), min: merged.min, max: merged.max,
		}}, nil
	}
	split := pageKeyTreeBranchBalancedSplit(tx.options.PageSize, children)
	if split == 0 {
		return nil, fmt.Errorf("%w: fingerprint branch redistribution", ErrInvalidWrite)
	}
	rightMin, _, err := pageKeyTreePageRange(cache, children[split].Child, tx, bounds)
	if err != nil {
		return nil, err
	}
	leftPage, err := encodePageKeyTreeBranch(
		tx, left.logicalID, left.level, left.min, children[:split],
	)
	if err != nil {
		return nil, err
	}
	rightPage, err := encodePageKeyTreeBranch(
		tx, right.logicalID, right.level, rightMin, children[split:],
	)
	if err != nil {
		return nil, err
	}
	return []pageKeyTreeCompactChild{
		{
			ref: leftPage.Ref(), min: left.min,
			max: children[split-1].MaxHash,
		},
		{
			ref: rightPage.Ref(), min: rightMin,
			max: children[len(children)-1].MaxHash,
		},
	}, nil
}

func validatePageKeyTreeCompactBranchBindings(
	cache *PageCache, tx *WriteTransaction,
	node pageKeyTreeCompactNode, bounds PageKeyTreeBounds,
) error {
	if node.level == 0 || len(node.children) == 0 ||
		node.max != node.children[len(node.children)-1].MaxHash {
		return fmt.Errorf("%w: fingerprint compaction branch shape", ErrKeyDirectoryCorrupt)
	}
	var previousMax uint64
	for rank, child := range node.children {
		lease, err := cache.Acquire(child.Child)
		if err != nil {
			return err
		}
		view, openErr := OpenPageFingerprintDirectory(
			lease.Page(), tx.FileEnd(), tx.NextLogicalID(),
			bounds.ChunkHighWater, bounds.ChunkDocuments,
		)
		if openErr != nil {
			lease.Release()
			return openErr
		}
		header := view.Header()
		lease.Release()
		if header.Level+1 != node.level || header.MaxHash != child.MaxHash {
			return fmt.Errorf("%w: fingerprint compaction child binding", ErrKeyDirectoryCorrupt)
		}
		if rank == 0 {
			if header.MinHash != node.min {
				return fmt.Errorf("%w: fingerprint compaction first-child minimum", ErrKeyDirectoryCorrupt)
			}
		} else if previousMax > header.MinHash {
			return fmt.Errorf("%w: fingerprint compaction child overlap", ErrKeyDirectoryCorrupt)
		}
		previousMax = header.MaxHash
	}
	return nil
}

func encodePageKeyTreeCompactNode(
	tx *WriteTransaction, node pageKeyTreeCompactNode, bounds PageKeyTreeBounds,
) (TransactionPage, error) {
	if node.empty || node.ref != (PageRef{}) {
		return TransactionPage{}, fmt.Errorf("%w: fingerprint compact node state", ErrInvalidWrite)
	}
	if node.level == 0 {
		return encodePageKeyTreeLeaf(
			tx, node.logicalID, node.entries, node.next, bounds,
		)
	}
	return encodePageKeyTreeBranch(
		tx, node.logicalID, node.level, node.min, node.children,
	)
}

func pageKeyTreeCompactNodeUnderfull(
	pageSize uint32, node pageKeyTreeCompactNode,
) bool {
	if node.level == 0 {
		return pageKeyTreeLeafBodyBytes(node.entries) <
			pageKeyTreeLeafMinimumBodyBytes(pageSize)
	}
	return len(node.children) < pageKeyTreeBranchMinimumChildren(pageSize)
}

func pageKeyTreeLeafBodyBytes(entries []PageKeyLocation) int {
	return PageKeyLeafEncodedSize(entries) -
		PageHeaderSize - PageTrailerSize - PageKeyDirectoryPayloadHeaderSize
}

// pageKeyTreeLeafMinimumBodyBytes is the non-root half-full bound. Entries are
// fixed width, so count balance and byte balance are identical.
func pageKeyTreeLeafMinimumBodyBytes(pageSize uint32) int {
	usable := int(pageSize) -
		PageHeaderSize - PageTrailerSize - PageKeyDirectoryPayloadHeaderSize
	if usable <= 0 {
		return 0
	}
	capacity := usable / PageKeyLeafEntrySize
	return ((capacity + 1) / 2) * PageKeyLeafEntrySize
}

func pageKeyTreeLeafBalancedSplit(
	pageSize uint32, entries []PageKeyLocation,
) int {
	minimum := pageKeyTreeLeafMinimumBodyBytes(pageSize)
	best := 0
	bestDistance := int(^uint(0) >> 1)
	for split := 1; split < len(entries); split++ {
		if !pageKeyTreeLeafFits(pageSize, entries[:split]) ||
			!pageKeyTreeLeafFits(pageSize, entries[split:]) {
			continue
		}
		left := pageKeyTreeLeafBodyBytes(entries[:split])
		right := pageKeyTreeLeafBodyBytes(entries[split:])
		if left < minimum || right < minimum {
			continue
		}
		distance := left - right
		if distance < 0 {
			distance = -distance
		}
		if distance < bestDistance {
			best = split
			bestDistance = distance
		}
	}
	return best
}

func pageKeyTreeBranchCapacity(pageSize uint32) int {
	capacity := (int(pageSize) - PageHeaderSize - PageTrailerSize -
		PageKeyDirectoryPayloadHeaderSize) / PageKeyBranchEntrySize
	return min(capacity, int(^uint16(0)))
}

func pageKeyTreeBranchMinimumChildren(pageSize uint32) int {
	capacity := pageKeyTreeBranchCapacity(pageSize)
	return (capacity + 1) / 2
}

func pageKeyTreeBranchBalancedSplit(
	pageSize uint32, children []PageKeyBranch,
) int {
	capacity := pageKeyTreeBranchCapacity(pageSize)
	minimum := pageKeyTreeBranchMinimumChildren(pageSize)
	if capacity <= 0 || len(children) > 2*capacity {
		return 0
	}
	split := len(children) / 2
	if split < minimum {
		split = minimum
	}
	if len(children)-split < minimum {
		split = len(children) - minimum
	}
	if split < minimum || len(children)-split < minimum ||
		split > capacity || len(children)-split > capacity {
		return 0
	}
	return split
}

func mutatePageKeyTree(
	cache *PageCache, tx *WriteTransaction, root PageRef, expected PageKeyLocation,
	operation pageKeyMutationOperation, bounds PageKeyTreeBounds,
) (PageKeyTreeMutation, error) {
	mutation := PageKeyTreeMutation{Root: root}
	if err := validatePageKeyTreeMutation(cache, tx, root, expected, bounds); err != nil {
		return mutation, err
	}
	if root == (PageRef{}) {
		if operation != pageKeyMutationInsert {
			return mutation, nil
		}
		page, err := encodePageKeyTreeLeaf(tx, 0, []PageKeyLocation{expected}, PageRef{}, bounds)
		if err != nil {
			return mutation, err
		}
		mutation.Root = page.Ref()
		mutation.Changed = true
		return mutation, nil
	}

	found, _, path, depth, leaf, err := locatePageKeyTreeIdentity(cache, root, expected, bounds)
	if err != nil {
		return mutation, err
	}
	mutation.Found = found
	switch operation {
	case pageKeyMutationInsert:
		if found {
			return mutation, nil
		}
		path, depth, leaf, err = pageKeyTreeOrderedInsertionPath(cache, root, expected, bounds)
		if err != nil {
			return mutation, err
		}
	case pageKeyMutationDelete:
		if !found {
			return mutation, nil
		}
	default:
		return mutation, fmt.Errorf("%w: fingerprint mutation operation", ErrInvalidWrite)
	}

	if operation == pageKeyMutationDelete {
		result, compactErr := compactPageKeyTreePath(
			cache, tx, root, path, depth, 0, leaf, expected,
			bounds, true, 0, false, &mutation,
		)
		if compactErr != nil {
			return mutation, compactErr
		}
		mutation.Changed = true
		if result.empty {
			mutation.Root = PageRef{}
			return mutation, nil
		}
		if result.ref != (PageRef{}) {
			mutation.Root = result.ref
			return mutation, nil
		}
		page, encodeErr := encodePageKeyTreeCompactNode(tx, result, bounds)
		if encodeErr != nil {
			return mutation, encodeErr
		}
		mutation.Root = page.Ref()
		return mutation, nil
	}

	result, err := rewritePageKeyTreePath(
		cache, tx, root, path, depth, 0, leaf, expected,
		operation, bounds, true, 0, false, &mutation,
	)
	if err != nil {
		return mutation, err
	}
	mutation.Changed = true
	if result.empty {
		mutation.Root = PageRef{}
		return mutation, nil
	}
	mutation.Root = result.ref
	if result.rightRef == (PageRef{}) {
		return mutation, nil
	}
	level, err := pageKeyTreePageLevel(cache, result.ref, tx, bounds)
	if err != nil {
		return mutation, err
	}
	if level == pageKeyDirectoryMaxLevel {
		return mutation, ErrKeyTreeDepth
	}
	rootPage, err := encodePageKeyTreeBranch(tx, 0, level+1, result.min, []PageKeyBranch{
		{MaxHash: result.max, Child: result.ref},
		{MaxHash: result.rightMax, Child: result.rightRef},
	})
	if err != nil {
		return mutation, err
	}
	mutation.Root = rootPage.Ref()
	return mutation, nil
}

func locatePageKeyTreeIdentity(
	cache *PageCache, root PageRef, target PageKeyLocation, bounds PageKeyTreeBounds,
) (bool, PageKeyLocation, [pageKeyDirectoryMaxLevel]pageKeyTreePathEntry, int, PageRef, error) {
	var path [pageKeyDirectoryMaxLevel]pageKeyTreePathEntry
	cursor, err := OpenPageKeyTreeCursor(cache, root, target.Hash, bounds)
	if err != nil {
		return false, PageKeyLocation{}, path, 0, PageRef{}, err
	}
	defer cursor.Close()
	for {
		location, ok, nextErr := cursor.Next()
		if nextErr != nil || !ok {
			return false, PageKeyLocation{}, path, 0, PageRef{}, nextErr
		}
		if samePageKeyIdentity(location, target) {
			return true, location, cursor.path, cursor.depth, cursor.leafRef, nil
		}
	}
}

// pageKeyTreeOrderedInsertionPath preserves the complete
// (hash,chunk,slot) order across collision leaves even though branches carry
// hash maxima only. It chooses the first collision leaf whose next identity is
// greater than target, or the final collision leaf when target is the new
// maximum. A hash absent from the tree uses the ordinary clamped hash descent.
func pageKeyTreeOrderedInsertionPath(
	cache *PageCache, root PageRef, target PageKeyLocation, bounds PageKeyTreeBounds,
) ([pageKeyDirectoryMaxLevel]pageKeyTreePathEntry, int, PageRef, error) {
	var path [pageKeyDirectoryMaxLevel]pageKeyTreePathEntry
	cursor, err := OpenPageKeyTreeCursor(cache, root, target.Hash, bounds)
	if err != nil {
		return path, 0, PageRef{}, err
	}
	defer cursor.Close()
	haveCandidate := false
	lastDepth := 0
	lastLeaf := PageRef{}
	for {
		location, ok, nextErr := cursor.Next()
		if nextErr != nil {
			return path, 0, PageRef{}, nextErr
		}
		if !ok {
			break
		}
		haveCandidate = true
		path = cursor.path
		lastDepth = cursor.depth
		lastLeaf = cursor.leafRef
		if comparePageKeyLocation(target, location) < 0 {
			return path, lastDepth, lastLeaf, nil
		}
	}
	if haveCandidate {
		return path, lastDepth, lastLeaf, nil
	}
	return pageKeyTreeInsertionPath(cache, root, target.Hash, bounds)
}

func pageKeyTreeInsertionPath(
	cache *PageCache, root PageRef, hash uint64, bounds PageKeyTreeBounds,
) ([pageKeyDirectoryMaxLevel]pageKeyTreePathEntry, int, PageRef, error) {
	var path [pageKeyDirectoryMaxLevel]pageKeyTreePathEntry
	ref := root
	depth := 0
	expectedLevel := uint8(0)
	haveExpectedLevel := false
	expectedMax := uint64(0)
	haveExpectedMax := false
	for {
		lease, view, err := acquirePageFingerprintDirectory(cache, ref, bounds)
		if err != nil {
			return path, 0, PageRef{}, err
		}
		header := view.Header()
		if haveExpectedLevel && header.Level != expectedLevel {
			lease.Release()
			return path, 0, PageRef{}, fmt.Errorf("%w: fingerprint insertion level", ErrKeyDirectoryCorrupt)
		}
		if haveExpectedMax && header.MaxHash != expectedMax {
			lease.Release()
			return path, 0, PageRef{}, fmt.Errorf("%w: fingerprint insertion child maximum", ErrKeyDirectoryCorrupt)
		}
		if header.Level == 0 {
			lease.Release()
			return path, depth, ref, nil
		}
		if depth == len(path) {
			lease.Release()
			return path, 0, PageRef{}, ErrKeyTreeDepth
		}
		rank := pageKeyBranchRank(view, hash)
		child, ok := view.BranchAt(rank)
		if !ok {
			lease.Release()
			return path, 0, PageRef{}, fmt.Errorf("%w: fingerprint insertion child", ErrKeyDirectoryCorrupt)
		}
		path[depth] = pageKeyTreePathEntry{ref: ref, rank: uint16(rank), level: header.Level}
		depth++
		lease.Release()
		expectedLevel = header.Level - 1
		haveExpectedLevel = true
		expectedMax = child.MaxHash
		haveExpectedMax = true
		ref = child.Child
	}
}

func rewritePageKeyTreePath(
	cache *PageCache, tx *WriteTransaction, ref PageRef,
	path [pageKeyDirectoryMaxLevel]pageKeyTreePathEntry, pathDepth, depth int,
	leaf PageRef, expected PageKeyLocation,
	operation pageKeyMutationOperation, bounds PageKeyTreeBounds, root bool,
	expectedMax uint64, haveExpectedMax bool, mutation *PageKeyTreeMutation,
) (pageKeyTreeRewrite, error) {
	lease, view, err := acquirePageFingerprintDirectory(cache, ref, bounds)
	if err != nil {
		return pageKeyTreeRewrite{}, err
	}
	defer lease.Release()
	if haveExpectedMax && view.Header().MaxHash != expectedMax {
		return pageKeyTreeRewrite{}, fmt.Errorf("%w: fingerprint mutation child maximum", ErrKeyDirectoryCorrupt)
	}
	if view.Header().Level == 0 {
		if depth != pathDepth || ref != leaf {
			return pageKeyTreeRewrite{}, fmt.Errorf("%w: fingerprint mutation leaf path", ErrKeyDirectoryCorrupt)
		}
		return rewritePageKeyTreeLeaf(tx, ref, view, expected, operation, bounds, mutation)
	}
	if depth >= pathDepth {
		return pageKeyTreeRewrite{}, fmt.Errorf("%w: fingerprint mutation branch path", ErrKeyDirectoryCorrupt)
	}
	pathEntry := path[depth]
	if pathEntry.ref != ref || pathEntry.level != view.Header().Level ||
		int(pathEntry.rank) >= view.Len() {
		return pageKeyTreeRewrite{}, fmt.Errorf("%w: fingerprint mutation path identity", ErrKeyDirectoryCorrupt)
	}
	return rewritePageKeyTreeBranch(
		cache, tx, ref, view, path, pathDepth, depth, leaf, expected,
		operation, bounds, root, mutation,
	)
}

func rewritePageKeyTreeLeaf(
	tx *WriteTransaction, oldRef PageRef, view PageKeyDirectoryView,
	expected PageKeyLocation, operation pageKeyMutationOperation,
	bounds PageKeyTreeBounds, mutation *PageKeyTreeMutation,
) (pageKeyTreeRewrite, error) {
	entries := make([]PageKeyLocation, 0, view.Len()+1)
	position := -1
	for rank := 0; rank < view.Len(); rank++ {
		entry, ok := view.LocationAt(rank)
		if !ok {
			return pageKeyTreeRewrite{}, ErrKeyDirectoryCorrupt
		}
		if samePageKeyIdentity(entry, expected) {
			position = len(entries)
		}
		entries = append(entries, entry)
	}
	switch operation {
	case pageKeyMutationInsert:
		if position >= 0 {
			return pageKeyTreeRewrite{}, fmt.Errorf("%w: duplicate fingerprint insertion", ErrKeyDirectoryCorrupt)
		}
		position = pageKeyLocationRank(entries, expected)
		entries = append(entries, PageKeyLocation{})
		copy(entries[position+1:], entries[position:])
		entries[position] = expected
	case pageKeyMutationDelete:
		if position < 0 {
			return pageKeyTreeRewrite{}, fmt.Errorf("%w: missing fingerprint deletion", ErrKeyDirectoryCorrupt)
		}
		copy(entries[position:], entries[position+1:])
		entries = entries[:len(entries)-1]
	}
	if err := mutation.retire(oldRef); err != nil {
		return pageKeyTreeRewrite{}, err
	}
	if len(entries) == 0 {
		return pageKeyTreeRewrite{empty: true}, nil
	}
	if pageKeyTreeLeafFits(tx.options.PageSize, entries) {
		page, err := encodePageKeyTreeLeaf(tx, oldRef.LogicalID, entries, view.Header().Next, bounds)
		if err != nil {
			return pageKeyTreeRewrite{}, err
		}
		return pageKeyTreeRewrite{
			ref: page.Ref(), min: entries[0].Hash, max: entries[len(entries)-1].Hash,
		}, nil
	}
	split := pageKeyTreeLeafSplit(tx.options.PageSize, entries)
	if split == 0 {
		return pageKeyTreeRewrite{}, fmt.Errorf("%w: fingerprint leaf does not fit", ErrInvalidWrite)
	}
	leftPage, err := tx.Allocate(PageFingerprintDirectory, tx.options.PageSize, oldRef.LogicalID)
	if err != nil {
		return pageKeyTreeRewrite{}, err
	}
	rightPage, err := tx.Allocate(PageFingerprintDirectory, tx.options.PageSize, 0)
	if err != nil {
		return pageKeyTreeRewrite{}, err
	}
	if err := encodePageKeyTreeLeafInto(tx, rightPage, entries[split:], view.Header().Next, bounds); err != nil {
		return pageKeyTreeRewrite{}, err
	}
	if err := encodePageKeyTreeLeafInto(tx, leftPage, entries[:split], rightPage.Ref(), bounds); err != nil {
		return pageKeyTreeRewrite{}, err
	}
	return pageKeyTreeRewrite{
		ref: leftPage.Ref(), min: entries[0].Hash, max: entries[split-1].Hash,
		rightRef: rightPage.Ref(), rightMin: entries[split].Hash, rightMax: entries[len(entries)-1].Hash,
	}, nil
}

func rewritePageKeyTreeBranch(
	cache *PageCache, tx *WriteTransaction, oldRef PageRef, view PageKeyDirectoryView,
	path [pageKeyDirectoryMaxLevel]pageKeyTreePathEntry, pathDepth, depth int,
	leaf PageRef, expected PageKeyLocation,
	operation pageKeyMutationOperation, bounds PageKeyTreeBounds, root bool,
	mutation *PageKeyTreeMutation,
) (pageKeyTreeRewrite, error) {
	children := make([]PageKeyBranch, view.Len()+1)
	count := view.Len()
	for rank := 0; rank < count; rank++ {
		entry, ok := view.BranchAt(rank)
		if !ok {
			return pageKeyTreeRewrite{}, ErrKeyDirectoryCorrupt
		}
		children[rank] = entry
	}
	rank := int(path[depth].rank)
	child, err := rewritePageKeyTreePath(
		cache, tx, children[rank].Child, path, pathDepth, depth+1, leaf,
		expected, operation, bounds, false,
		children[rank].MaxHash, true, mutation,
	)
	if err != nil {
		return pageKeyTreeRewrite{}, err
	}
	minHash := view.Header().MinHash
	if child.empty {
		copy(children[rank:], children[rank+1:count])
		count--
		if rank == 0 && count != 0 {
			minHash, _, err = pageKeyTreePageRange(cache, children[0].Child, tx, bounds)
			if err != nil {
				return pageKeyTreeRewrite{}, err
			}
		}
	} else {
		children[rank] = PageKeyBranch{MaxHash: child.max, Child: child.ref}
		if rank == 0 {
			minHash = child.min
		}
		if child.rightRef != (PageRef{}) {
			copy(children[rank+2:], children[rank+1:count])
			children[rank+1] = PageKeyBranch{MaxHash: child.rightMax, Child: child.rightRef}
			count++
		}
	}
	children = children[:count]
	if err := mutation.retire(oldRef); err != nil {
		return pageKeyTreeRewrite{}, err
	}
	if count == 0 {
		return pageKeyTreeRewrite{empty: true}, nil
	}
	if root && count == 1 {
		childMin, childMax, rangeErr := pageKeyTreePageRange(cache, children[0].Child, tx, bounds)
		if rangeErr != nil {
			return pageKeyTreeRewrite{}, rangeErr
		}
		return pageKeyTreeRewrite{ref: children[0].Child, min: childMin, max: childMax}, nil
	}
	level := view.Header().Level
	if pageKeyTreeBranchFits(tx.options.PageSize, children) {
		page, encodeErr := encodePageKeyTreeBranch(tx, oldRef.LogicalID, level, minHash, children)
		if encodeErr != nil {
			return pageKeyTreeRewrite{}, encodeErr
		}
		return pageKeyTreeRewrite{
			ref: page.Ref(), min: minHash, max: children[len(children)-1].MaxHash,
		}, nil
	}
	split := pageKeyTreeBranchSplit(tx.options.PageSize, children)
	if split == 0 {
		return pageKeyTreeRewrite{}, fmt.Errorf("%w: fingerprint branch does not fit", ErrInvalidWrite)
	}
	left, err := encodePageKeyTreeBranch(tx, oldRef.LogicalID, level, minHash, children[:split])
	if err != nil {
		return pageKeyTreeRewrite{}, err
	}
	rightMin, _, err := pageKeyTreePageRange(cache, children[split].Child, tx, bounds)
	if err != nil {
		return pageKeyTreeRewrite{}, err
	}
	right, err := encodePageKeyTreeBranch(tx, 0, level, rightMin, children[split:])
	if err != nil {
		return pageKeyTreeRewrite{}, err
	}
	return pageKeyTreeRewrite{
		ref: left.Ref(), min: minHash, max: children[split-1].MaxHash,
		rightRef: right.Ref(), rightMin: rightMin, rightMax: children[len(children)-1].MaxHash,
	}, nil
}

func encodePageKeyTreeLeaf(
	tx *WriteTransaction, logicalID uint64, entries []PageKeyLocation,
	next PageRef, bounds PageKeyTreeBounds,
) (TransactionPage, error) {
	page, err := tx.Allocate(PageFingerprintDirectory, tx.options.PageSize, logicalID)
	if err != nil {
		return TransactionPage{}, err
	}
	if err := encodePageKeyTreeLeafInto(tx, page, entries, next, bounds); err != nil {
		return TransactionPage{}, err
	}
	return page, nil
}

func encodePageKeyTreeLeafInto(
	tx *WriteTransaction, page TransactionPage, entries []PageKeyLocation,
	next PageRef, bounds PageKeyTreeBounds,
) error {
	header := pageKeyTreeHeader(tx, page.Ref(), 0)
	header.MinHash = entries[0].Hash
	header.MaxHash = entries[len(entries)-1].Hash
	header.Next = next
	if _, err := EncodePageFingerprintLeaf(
		page.Bytes(), header, entries, tx.FileEnd(), tx.NextLogicalID(),
		bounds.ChunkHighWater, bounds.ChunkDocuments,
	); err != nil {
		return err
	}
	return page.Stage()
}

func encodePageKeyTreeBranch(
	tx *WriteTransaction, logicalID uint64, level uint8, minHash uint64,
	children []PageKeyBranch,
) (TransactionPage, error) {
	page, err := tx.Allocate(PageFingerprintDirectory, tx.options.PageSize, logicalID)
	if err != nil {
		return TransactionPage{}, err
	}
	header := pageKeyTreeHeader(tx, page.Ref(), level)
	header.MinHash = minHash
	header.MaxHash = children[len(children)-1].MaxHash
	if _, err := EncodePageFingerprintBranch(
		page.Bytes(), header, children, tx.FileEnd(), tx.NextLogicalID(),
	); err != nil {
		return TransactionPage{}, err
	}
	if err := page.Stage(); err != nil {
		return TransactionPage{}, err
	}
	return page, nil
}

func pageKeyTreeHeader(tx *WriteTransaction, ref PageRef, level uint8) PageKeyDirectoryHeader {
	return PageKeyDirectoryHeader{
		StoreID: tx.options.StoreID, Generation: tx.options.Generation,
		LogicalID: ref.LogicalID, PageSize: ref.Length, Level: level,
	}
}

func pageKeyTreePageLevel(
	cache *PageCache, ref PageRef, tx *WriteTransaction, bounds PageKeyTreeBounds,
) (uint8, error) {
	lease, err := cache.Acquire(ref)
	if err != nil {
		return 0, err
	}
	defer lease.Release()
	view, err := OpenPageFingerprintDirectory(
		lease.Page(), tx.FileEnd(), tx.NextLogicalID(),
		bounds.ChunkHighWater, bounds.ChunkDocuments,
	)
	if err != nil {
		return 0, err
	}
	return view.Header().Level, nil
}

func pageKeyTreePageRange(
	cache *PageCache, ref PageRef, tx *WriteTransaction, bounds PageKeyTreeBounds,
) (uint64, uint64, error) {
	lease, err := cache.Acquire(ref)
	if err != nil {
		return 0, 0, err
	}
	defer lease.Release()
	view, err := OpenPageFingerprintDirectory(
		lease.Page(), tx.FileEnd(), tx.NextLogicalID(),
		bounds.ChunkHighWater, bounds.ChunkDocuments,
	)
	if err != nil {
		return 0, 0, err
	}
	return view.Header().MinHash, view.Header().MaxHash, nil
}

func acquirePageFingerprintDirectory(
	cache *PageCache, ref PageRef, bounds PageKeyTreeBounds,
) (PageLease, PageKeyDirectoryView, error) {
	if ref.Kind != PageFingerprintDirectory {
		return PageLease{}, PageKeyDirectoryView{}, fmt.Errorf("%w: fingerprint reference kind", ErrKeyDirectoryCorrupt)
	}
	lease, err := cache.Acquire(ref)
	if err != nil {
		return PageLease{}, PageKeyDirectoryView{}, err
	}
	if cache.ValidatesOnAdmission() {
		return lease, AdmittedPageFingerprintDirectory(lease.Page()), nil
	}
	view, err := OpenPageFingerprintDirectory(
		lease.Page(), bounds.FileEnd, bounds.NextLogicalID,
		bounds.ChunkHighWater, bounds.ChunkDocuments,
	)
	if err != nil {
		lease.Release()
		return PageLease{}, PageKeyDirectoryView{}, err
	}
	return lease, view, nil
}

func nextPageKeyTreeLeaf(
	cache *PageCache, path *[pageKeyDirectoryMaxLevel]pageKeyTreePathEntry,
	depth *int, bounds PageKeyTreeBounds,
) (PageRef, uint64, bool, error) {
	for level := *depth - 1; level >= 0; level-- {
		node := &path[level]
		lease, view, err := acquirePageFingerprintDirectory(cache, node.ref, bounds)
		if err != nil {
			return PageRef{}, 0, false, err
		}
		if view.Header().Level != node.level || int(node.rank) >= view.Len() {
			lease.Release()
			return PageRef{}, 0, false, fmt.Errorf("%w: fingerprint successor path", ErrKeyDirectoryCorrupt)
		}
		if int(node.rank)+1 >= view.Len() {
			lease.Release()
			continue
		}
		node.rank++
		child, ok := view.BranchAt(int(node.rank))
		lease.Release()
		if !ok {
			return PageRef{}, 0, false, fmt.Errorf("%w: fingerprint successor child", ErrKeyDirectoryCorrupt)
		}
		ref := child.Child
		expectedMax := child.MaxHash
		*depth = level + 1
		expected := node.level - 1
		for expected != 0 {
			if *depth == len(*path) {
				return PageRef{}, 0, false, ErrKeyTreeDepth
			}
			childLease, childView, childErr := acquirePageFingerprintDirectory(cache, ref, bounds)
			if childErr != nil {
				return PageRef{}, 0, false, childErr
			}
			header := childView.Header()
			if header.Level != expected || header.MaxHash != expectedMax {
				childLease.Release()
				return PageRef{}, 0, false, fmt.Errorf("%w: fingerprint successor level or maximum", ErrKeyDirectoryCorrupt)
			}
			first, exists := childView.BranchAt(0)
			if !exists {
				childLease.Release()
				return PageRef{}, 0, false, fmt.Errorf("%w: fingerprint successor branch", ErrKeyDirectoryCorrupt)
			}
			(*path)[*depth] = pageKeyTreePathEntry{ref: ref, rank: 0, level: header.Level}
			*depth++
			childLease.Release()
			ref = first.Child
			expectedMax = first.MaxHash
			expected--
		}
		return ref, expectedMax, true, nil
	}
	return PageRef{}, 0, false, nil
}

func validatePageKeyTreeRead(cache *PageCache, root PageRef, bounds PageKeyTreeBounds) error {
	if cache == nil || root.Kind != PageFingerprintDirectory ||
		bounds.FileEnd == 0 || bounds.NextLogicalID <= StateRootLogicalID ||
		bounds.ChunkHighWater == 0 || bounds.ChunkDocuments == 0 || bounds.ChunkDocuments > 64 {
		return fmt.Errorf("%w: fingerprint-tree bounds", ErrInvalidWrite)
	}
	return nil
}

func validatePageKeyTreeMutation(
	cache *PageCache, tx *WriteTransaction, root PageRef,
	location PageKeyLocation, bounds PageKeyTreeBounds,
) error {
	if tx == nil || !tx.active || cache == nil && root != (PageRef{}) ||
		root != (PageRef{}) && root.Kind != PageFingerprintDirectory ||
		bounds.ChunkHighWater == 0 || bounds.ChunkDocuments == 0 || bounds.ChunkDocuments > 64 ||
		location.Chunk >= bounds.ChunkHighWater || uint32(location.Slot) >= bounds.ChunkDocuments {
		return fmt.Errorf("%w: fingerprint-tree mutation bounds", ErrInvalidWrite)
	}
	return nil
}

func samePageKeyIdentity(a, b PageKeyLocation) bool {
	return a.Hash == b.Hash && a.Chunk == b.Chunk && a.Slot == b.Slot
}

func comparePageKeyLocation(a, b PageKeyLocation) int {
	if a.Hash < b.Hash {
		return -1
	}
	if a.Hash > b.Hash {
		return 1
	}
	if a.Chunk < b.Chunk {
		return -1
	}
	if a.Chunk > b.Chunk {
		return 1
	}
	return int(a.Slot) - int(b.Slot)
}

func pageKeyLocationRank(entries []PageKeyLocation, target PageKeyLocation) int {
	low, high := 0, len(entries)
	for low < high {
		middle := int(uint(low+high) >> 1)
		if comparePageKeyLocation(entries[middle], target) < 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low
}

func pageKeyBranchRank(view PageKeyDirectoryView, hash uint64) int {
	low, high := 0, view.Len()
	for low < high {
		middle := int(uint(low+high) >> 1)
		entry, _ := view.BranchAt(middle)
		if entry.MaxHash < hash {
			low = middle + 1
		} else {
			high = middle
		}
	}
	if low == view.Len() {
		return view.Len() - 1
	}
	return low
}

func pageKeyTreeLeafFits(pageSize uint32, entries []PageKeyLocation) bool {
	if len(entries) == 0 || len(entries) > int(^uint16(0)) {
		return false
	}
	return uint64(PageKeyLeafEncodedSize(entries)) <= uint64(pageSize)
}

func pageKeyTreeLeafSplit(pageSize uint32, entries []PageKeyLocation) int {
	// Every leaf produced by a split becomes a non-root child, including the
	// two children created when a root leaf grows.
	return pageKeyTreeLeafBalancedSplit(pageSize, entries)
}

func pageKeyTreeBranchFits(pageSize uint32, children []PageKeyBranch) bool {
	if len(children) == 0 || len(children) > int(^uint16(0)) {
		return false
	}
	used := PageHeaderSize + PageTrailerSize + PageKeyDirectoryPayloadHeaderSize +
		len(children)*PageKeyBranchEntrySize
	return uint64(used) <= uint64(pageSize)
}

func pageKeyTreeBranchSplit(pageSize uint32, children []PageKeyBranch) int {
	capacity := (int(pageSize) - PageHeaderSize - PageTrailerSize -
		PageKeyDirectoryPayloadHeaderSize) / PageKeyBranchEntrySize
	capacity = min(capacity, int(^uint16(0)))
	if capacity <= 0 || len(children) <= capacity {
		return 0
	}
	split := len(children) / 2
	if split > capacity {
		split = capacity
	}
	if len(children)-split > capacity {
		split = len(children) - capacity
	}
	if split <= 0 || split >= len(children) {
		return 0
	}
	return split
}
