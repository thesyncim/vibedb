package storeio

import (
	"bytes"
	"fmt"
)

const primaryGraphCatalogDepth = 3

type primaryGraphCatalogPathEntry struct {
	ref        PageRef
	level      GlobalTabletCatalogNodeLevel
	childLevel GlobalTabletCatalogNodeLevel
	ordinal    uint16
}

// PrimaryGraphRow borrows its key and inline value from the cursor's current
// leaf lease. They remain valid only until the next cursor operation or Close.
type PrimaryGraphRow struct {
	Key      []byte
	Inline   []byte
	Overflow PageRef
}

// PrimaryGraphCursor walks one snapshot-selected ordered primary graph.
//
// Successors are reconstructed exclusively from catalog ordinals, tablet
// anchor ranks, and anchor row ranks rooted at the supplied catalog reference.
// Physical sibling references are never consulted. The cursor is
// single-owner, must not be copied after first use, and should be closed when
// iteration stops early.
type PrimaryGraphCursor struct {
	cache      *PageCache
	root       PageRef
	bounds     GlobalTabletCatalogBounds
	leafBounds CommonPrimaryLeafBounds
	lower      []byte
	upper      []byte
	prefix     []byte

	path  [primaryGraphCatalogDepth]primaryGraphCatalogPathEntry
	depth int

	tabletLease PageLease
	tablet      GlobalTabletCatalogTabletRootView
	anchorRank  int
	anchorLease PageLease
	anchor      GlobalTabletCatalogAnchorView
	rowRank     int
	leafLease   PageLease
	rows        CommonPrimaryLeafIterator

	// Template-columnar leaves reconstruct documents by splice, so the cursor
	// dispatches on the leaf class and, for the template class, walks ranks and
	// splices each surviving row into spliceScratch. The scratch is reused across
	// rows and leaves within one scan.
	leafClass     CommonPrimaryLeafClass
	tcLeaf        CommonPrimaryTemplateLeafView
	tcRank        int
	spliceScratch []byte

	done bool
}

// OpenPrimaryGraphCursor positions a cursor at the first key >= lower. A
// non-empty upper bound is exclusive.
func OpenPrimaryGraphCursor(
	cache *PageCache,
	root PageRef,
	bounds GlobalTabletCatalogBounds,
	leafBounds CommonPrimaryLeafBounds,
	lower, upper []byte,
) (PrimaryGraphCursor, error) {
	var cursor PrimaryGraphCursor
	if err := InitPrimaryGraphCursor(
		&cursor, cache, root, bounds, leafBounds, lower, upper,
	); err != nil {
		return PrimaryGraphCursor{}, err
	}
	return cursor, nil
}

// InitPrimaryGraphCursor is the zero-allocation initializer for callers that
// keep the cursor in their own stack frame. dst must not be copied until it
// has been closed because its page leases are single-owner values.
func InitPrimaryGraphCursor(
	dst *PrimaryGraphCursor,
	cache *PageCache,
	root PageRef,
	bounds GlobalTabletCatalogBounds,
	leafBounds CommonPrimaryLeafBounds,
	lower, upper []byte,
) error {
	if dst == nil {
		return fmt.Errorf("%w: nil primary graph cursor", ErrInvalidWrite)
	}
	*dst = PrimaryGraphCursor{
		cache: cache, root: root, bounds: bounds, leafBounds: leafBounds,
		lower: lower, upper: upper, done: true,
	}
	if err := dst.validate(); err != nil {
		return err
	}
	if len(upper) != 0 && bytes.Compare(lower, upper) >= 0 {
		return nil
	}
	tabletRef, err := dst.openCatalog(lower)
	if err != nil {
		dst.Close()
		return err
	}
	if err := dst.openTablet(tabletRef, lower, true); err != nil {
		dst.Close()
		return err
	}
	dst.done = false
	return nil
}

// OpenPrimaryGraphPrefixCursor positions a cursor at the first key carrying
// prefix. An empty prefix is equivalent to a full ordered scan.
func OpenPrimaryGraphPrefixCursor(
	cache *PageCache,
	root PageRef,
	bounds GlobalTabletCatalogBounds,
	leafBounds CommonPrimaryLeafBounds,
	prefix []byte,
) (PrimaryGraphCursor, error) {
	var cursor PrimaryGraphCursor
	if err := InitPrimaryGraphPrefixCursor(
		&cursor, cache, root, bounds, leafBounds, prefix,
	); err != nil {
		return PrimaryGraphCursor{}, err
	}
	return cursor, nil
}

// InitPrimaryGraphPrefixCursor is the zero-allocation prefix initializer.
func InitPrimaryGraphPrefixCursor(
	dst *PrimaryGraphCursor,
	cache *PageCache,
	root PageRef,
	bounds GlobalTabletCatalogBounds,
	leafBounds CommonPrimaryLeafBounds,
	prefix []byte,
) error {
	if err := InitPrimaryGraphCursor(
		dst, cache, root, bounds, leafBounds, prefix, nil,
	); err != nil {
		return err
	}
	dst.prefix = prefix
	return nil
}

func (c *PrimaryGraphCursor) validate() error {
	if c.cache == nil || c.root.Kind != PagePrimaryCatalog ||
		!c.bounds.valid() || c.leafBounds.FileEnd != c.bounds.FileEnd ||
		c.leafBounds.NextLogicalID != c.bounds.NextLogicalID ||
		c.leafBounds.AllocationQuantum == 0 {
		return fmt.Errorf("%w: primary graph cursor bounds", ErrInvalidWrite)
	}
	return nil
}

func (c *PrimaryGraphCursor) admittedCatalog(
	ref PageRef,
) (PageLease, GlobalTabletCatalogNodeView, error) {
	lease, err := c.cache.Acquire(ref)
	if err != nil {
		return PageLease{}, GlobalTabletCatalogNodeView{}, err
	}
	view := AdmittedGlobalTabletCatalogNode(lease.Page(), c.bounds)
	return lease, view, nil
}

func (c *PrimaryGraphCursor) openCatalog(key []byte) (PageRef, error) {
	ref := c.root
	for {
		lease, view, err := c.admittedCatalog(ref)
		if err != nil {
			return PageRef{}, err
		}
		if c.depth == 0 {
			if view.Level() != GlobalTabletCatalogRoot {
				lease.Release()
				return PageRef{}, fmt.Errorf(
					"%w: primary cursor catalog root",
					ErrGlobalTabletCatalogCorrupt,
				)
			}
		} else {
			parent := c.path[c.depth-1]
			if view.Level() != parent.childLevel {
				lease.Release()
				return PageRef{}, fmt.Errorf(
					"%w: primary cursor catalog level",
					ErrGlobalTabletCatalogCorrupt,
				)
			}
		}
		nodeCursor := view.LowerBound(key)
		route, routeOK := nodeCursor.Route()
		if !routeOK || route.Ref == (PageRef{}) || c.depth == len(c.path) {
			lease.Release()
			return PageRef{}, fmt.Errorf(
				"%w: primary cursor catalog route",
				ErrGlobalTabletCatalogCorrupt,
			)
		}
		c.path[c.depth] = primaryGraphCatalogPathEntry{
			ref: ref, level: view.Level(), childLevel: view.ChildLevel(),
			ordinal: route.Ordinal,
		}
		c.depth++
		level := view.Level()
		lease.Release()
		if level == GlobalTabletCatalogLeaf {
			return route.Ref, nil
		}
		ref = route.Ref
	}
}

func (c *PrimaryGraphCursor) openTablet(
	ref PageRef, key []byte, lowerBound bool,
) error {
	c.releaseTablet()
	lease, err := c.cache.Acquire(ref)
	if err != nil {
		return err
	}
	tablet := AdmittedGlobalTabletCatalogTabletRoot(lease.Page(), c.bounds)
	var anchorRoute GlobalTabletCatalogAnchorRoute
	var ok bool
	if lowerBound {
		anchorRoute, ok = tablet.RouteAnchor(key)
	} else {
		anchorRoute, ok = tablet.AnchorAt(0)
	}
	if !ok {
		lease.Release()
		return fmt.Errorf(
			"%w: primary cursor tablet anchor",
			ErrGlobalTabletCatalogCorrupt,
		)
	}
	anchorRank := 0
	if lowerBound {
		for ; anchorRank < tablet.AnchorCount(); anchorRank++ {
			candidate, candidateOK := tablet.AnchorAt(anchorRank)
			if candidateOK && candidate.PageID == anchorRoute.PageID {
				break
			}
		}
		if anchorRank == tablet.AnchorCount() {
			lease.Release()
			return fmt.Errorf(
				"%w: primary cursor anchor rank",
				ErrGlobalTabletCatalogCorrupt,
			)
		}
	}
	c.tabletLease = lease
	c.tablet = tablet
	c.anchorRank = anchorRank
	return c.openAnchor(anchorRoute, key, lowerBound)
}

func (c *PrimaryGraphCursor) openAnchor(
	route GlobalTabletCatalogAnchorRoute, key []byte, lowerBound bool,
) error {
	c.releaseAnchor()
	lease, err := c.cache.Acquire(route.Ref)
	if err != nil {
		return err
	}
	anchor := AdmittedGlobalTabletCatalogAnchor(
		lease.Page(), &c.tablet, route.PageID,
	)
	rowRank := 0
	var leafRoute SegmentedTabletRouterRoute
	var ok bool
	if lowerBound {
		rowRank, leafRoute, ok = anchor.LowerBound(key)
	} else {
		leafRoute, ok = anchor.RouteAt(0, 0)
	}
	if !ok {
		lease.Release()
		return fmt.Errorf(
			"%w: primary cursor anchor row",
			ErrGlobalTabletCatalogCorrupt,
		)
	}
	c.anchorLease = lease
	c.anchor = anchor
	c.rowRank = rowRank
	return c.openLeaf(leafRoute, lowerBound)
}

func (c *PrimaryGraphCursor) openLeaf(
	route SegmentedTabletRouterRoute, lowerBound bool,
) error {
	c.leafLease.Release()
	lease, err := c.cache.Acquire(route.Ref)
	if err != nil {
		return err
	}
	c.leafLease = lease
	c.leafClass = PrimaryLeafClass(lease.Page())
	if c.leafClass == CommonPrimaryLeafTemplate {
		tv, ok := AdmittedCommonPrimaryTemplateLeaf(
			lease.Page(), route.Bucket, c.leafBounds,
		)
		if !ok {
			return fmt.Errorf(
				"%w: template primary leaf", ErrCommonPrimaryLeafCorrupt,
			)
		}
		c.tcLeaf = tv
		c.rows = CommonPrimaryLeafIterator{}
		if lowerBound {
			c.tcRank = tv.FirstRankFrom(c.lower)
		} else {
			c.tcRank = 0
		}
		return nil
	}
	leaf := AdmittedCommonPrimaryLeaf(
		lease.Page(), c.bounds.StoreID, route.Bucket, c.leafBounds,
	)
	if lowerBound {
		c.rows = leaf.Range(c.lower, nil)
	} else {
		c.rows = leaf.AllRows()
	}
	return nil
}

// nextRawBorrowed yields the next row of the current leaf, dispatching on the
// leaf class. Template rows are spliced into the reused scratch; the returned
// value borrows that scratch and is valid only until the next call.
func (c *PrimaryGraphCursor) nextRawBorrowed() (
	key, raw []byte, overflow, ok bool,
) {
	if c.leafClass != CommonPrimaryLeafTemplate {
		return c.rows.NextRawBorrowed()
	}
	if c.tcRank >= c.tcLeaf.Len() {
		return nil, nil, false, false
	}
	k, ti, rowOK := c.tcLeaf.RowAt(c.tcRank)
	if !rowOK {
		return nil, nil, false, false
	}
	c.spliceScratch = c.tcLeaf.AppendRawRank(c.spliceScratch[:0], c.tcRank, ti)
	c.tcRank++
	return k, c.spliceScratch, false, true
}

func (c *PrimaryGraphCursor) nextBorrowed() (
	key, inline []byte, overflow PageRef, ok bool,
) {
	key, raw, isOverflow, ok := c.nextRawBorrowed()
	if !ok {
		return nil, nil, PageRef{}, false
	}
	if isOverflow {
		return key, nil, decodePageRef(raw), true
	}
	return key, raw, PageRef{}, true
}

// Next returns the next globally bytewise-lexical row. Exhaustion closes the
// cursor. Bounds and prefixes are checked over the decoded key, so no separator
// approximation can admit an out-of-range row.
func (c *PrimaryGraphCursor) Next() (PrimaryGraphRow, bool, error) {
	if c == nil || c.done {
		return PrimaryGraphRow{}, false, nil
	}
	for {
		key, inline, overflow, ok := c.nextBorrowed()
		if ok {
			if len(c.upper) != 0 && bytes.Compare(key, c.upper) >= 0 ||
				len(c.prefix) != 0 && !bytes.HasPrefix(key, c.prefix) {
				c.Close()
				return PrimaryGraphRow{}, false, nil
			}
			return PrimaryGraphRow{
				Key: key, Inline: inline, Overflow: overflow,
			}, true, nil
		}
		if err := c.advanceLeaf(); err != nil {
			c.Close()
			return PrimaryGraphRow{}, false, err
		}
		if c.done {
			return PrimaryGraphRow{}, false, nil
		}
	}
}

// VisitInline is the scan hot path for inline primary graphs. It keeps the
// sequential rank decoder inside the leaf loop instead of paying a
// row-at-a-time cursor call. If an overflow row is encountered, its reference
// is returned without invoking fn for that row.
func (c *PrimaryGraphCursor) VisitInline(
	fn func(key, value []byte) error,
) (PageRef, error) {
	if c == nil || c.done || fn == nil {
		return PageRef{}, nil
	}
	if len(c.upper) == 0 && len(c.prefix) == 0 {
		return c.visitAllInline(fn)
	}
	for {
		for {
			key, raw, overflow, ok := c.nextRawBorrowed()
			if !ok {
				break
			}
			if len(c.upper) != 0 && bytes.Compare(key, c.upper) >= 0 ||
				len(c.prefix) != 0 && !bytes.HasPrefix(key, c.prefix) {
				c.Close()
				return PageRef{}, nil
			}
			if overflow {
				return decodePageRef(raw), nil
			}
			if err := fn(key, raw); err != nil {
				return PageRef{}, err
			}
		}
		if err := c.advanceLeaf(); err != nil {
			c.Close()
			return PageRef{}, err
		}
		if c.done {
			return PageRef{}, nil
		}
	}
}

func (c *PrimaryGraphCursor) visitAllInline(
	fn func(key, value []byte) error,
) (PageRef, error) {
	for {
		for {
			key, raw, overflow, ok := c.nextRawBorrowed()
			if !ok {
				break
			}
			if overflow {
				return decodePageRef(raw), nil
			}
			if err := fn(key, raw); err != nil {
				return PageRef{}, err
			}
		}
		if err := c.advanceLeaf(); err != nil {
			c.Close()
			return PageRef{}, err
		}
		if c.done {
			return PageRef{}, nil
		}
	}
}

func (c *PrimaryGraphCursor) advanceLeaf() error {
	c.leafLease.Release()
	if c.rowRank+1 < c.anchor.Count() {
		c.rowRank++
		route, ok := c.anchor.RouteAt(c.rowRank, 0)
		if !ok {
			return fmt.Errorf(
				"%w: primary cursor leaf successor",
				ErrGlobalTabletCatalogCorrupt,
			)
		}
		return c.openLeaf(route, false)
	}
	c.releaseAnchor()
	if c.anchorRank+1 < c.tablet.AnchorCount() {
		c.anchorRank++
		route, ok := c.tablet.AnchorAt(c.anchorRank)
		if !ok {
			return fmt.Errorf(
				"%w: primary cursor anchor successor",
				ErrGlobalTabletCatalogCorrupt,
			)
		}
		return c.openAnchor(route, nil, false)
	}
	c.releaseTablet()
	tabletRef, ok, err := c.nextTablet()
	if err != nil {
		return err
	}
	if !ok {
		c.done = true
		return nil
	}
	return c.openTablet(tabletRef, nil, false)
}

func (c *PrimaryGraphCursor) nextTablet() (PageRef, bool, error) {
	for level := c.depth - 1; level >= 0; level-- {
		entry := &c.path[level]
		lease, view, err := c.admittedCatalog(entry.ref)
		if err != nil {
			return PageRef{}, false, err
		}
		if view.Level() != entry.level ||
			view.ChildLevel() != entry.childLevel ||
			int(entry.ordinal) >= view.Count() {
			lease.Release()
			return PageRef{}, false, fmt.Errorf(
				"%w: primary cursor successor path",
				ErrGlobalTabletCatalogCorrupt,
			)
		}
		if int(entry.ordinal)+1 >= view.Count() {
			lease.Release()
			continue
		}
		entry.ordinal++
		route, routeOK := view.RouteAt(int(entry.ordinal))
		nodeLevel := view.Level()
		childLevel := view.ChildLevel()
		lease.Release()
		if !routeOK {
			return PageRef{}, false, fmt.Errorf(
				"%w: primary cursor successor route",
				ErrGlobalTabletCatalogCorrupt,
			)
		}
		c.depth = level + 1
		if nodeLevel == GlobalTabletCatalogLeaf {
			return route.Ref, true, nil
		}
		ref := route.Ref
		expected := childLevel
		for {
			childLease, child, childErr := c.admittedCatalog(ref)
			if childErr != nil {
				return PageRef{}, false, childErr
			}
			if child.Level() != expected || c.depth == len(c.path) {
				childLease.Release()
				return PageRef{}, false, fmt.Errorf(
					"%w: primary cursor successor descent",
					ErrGlobalTabletCatalogCorrupt,
				)
			}
			first, firstOK := child.RouteAt(0)
			c.path[c.depth] = primaryGraphCatalogPathEntry{
				ref: ref, level: child.Level(),
				childLevel: child.ChildLevel(), ordinal: 0,
			}
			c.depth++
			childNodeLevel := child.Level()
			expected = child.ChildLevel()
			childLease.Release()
			if !firstOK {
				return PageRef{}, false, fmt.Errorf(
					"%w: primary cursor successor child",
					ErrGlobalTabletCatalogCorrupt,
				)
			}
			if childNodeLevel == GlobalTabletCatalogLeaf {
				return first.Ref, true, nil
			}
			ref = first.Ref
		}
	}
	return PageRef{}, false, nil
}

func (c *PrimaryGraphCursor) releaseAnchor() {
	c.leafLease.Release()
	c.rows = CommonPrimaryLeafIterator{}
	c.leafClass = 0
	c.tcLeaf = CommonPrimaryTemplateLeafView{}
	c.tcRank = 0
	c.anchorLease.Release()
	c.anchor = GlobalTabletCatalogAnchorView{}
	c.rowRank = 0
}

func (c *PrimaryGraphCursor) releaseTablet() {
	c.releaseAnchor()
	c.tabletLease.Release()
	c.tablet = GlobalTabletCatalogTabletRootView{}
	c.anchorRank = 0
}

// Close releases the current leaf, anchor, and tablet leases. It is
// idempotent.
func (c *PrimaryGraphCursor) Close() {
	if c == nil {
		return
	}
	c.releaseTablet()
	c.done = true
}
