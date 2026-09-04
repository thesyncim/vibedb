package storeio

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const primaryGraphCatalogDepth = 3

// One compact restart block contains at most this many distinct shapes. Most
// log-like leaves have one shape; a leaf with more shapes remains correct via
// the compact view's bounded random-rank fallback.
const primaryGraphSequentialShapeSlots = compactStreamRestart

type primaryGraphCatalogPathEntry struct {
	ref        PageRef
	level      GlobalTabletCatalogNodeLevel
	childLevel GlobalTabletCatalogNodeLevel
	ordinal    uint16
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
	leafBucket  BucketID
	leaf        CompactPrimaryStripeView
	row         int
	shapeBlock  int
	shapeSeen   [primaryGraphSequentialShapeSlots]uint8

	spliceScratch []byte

	done bool
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
		lower: lower, upper: upper, shapeBlock: -1, done: true,
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
	view, ok := AdmittedCachedCompactPrimaryStripe(
		lease.Header(), lease.Payload(), c.bounds.StoreID, route.Bucket,
	)
	if !ok {
		return fmt.Errorf(
			"%w: compact primary leaf", ErrCommonPrimaryLeafCorrupt,
		)
	}
	c.leaf = view
	c.leafBucket = route.Bucket
	c.shapeBlock = -1
	if lowerBound {
		c.row = view.FirstRankFrom(c.lower)
	} else {
		c.row = 0
	}
	return nil
}

// CurrentUnifiedLeaf returns the admitted compact leaf selected by the cursor.
// The old name remains temporarily while mutation callers are converted.
func (c *PrimaryGraphCursor) CurrentUnifiedLeaf() (
	BucketID, CompactPrimaryStripeView, bool,
) {
	if c == nil || c.done || c.leafLease.Page() == nil {
		return 0, CompactPrimaryStripeView{}, false
	}
	return c.leafBucket, c.leaf, true
}

// CurrentUnifiedLeafPage returns the admitted bytes selected by the cursor and
// their stable bucket identity. The page borrows the cursor's current lease and
// remains valid only until NextLeaf or Close. It is the rooted sparse-scan
// counterpart to CurrentUnifiedLeaf for helpers that already accept an
// admitted page image.
func (c *PrimaryGraphCursor) CurrentUnifiedLeafPage() (
	BucketID, []byte, bool,
) {
	if c == nil || c.done || c.leafLease.Page() == nil {
		return 0, nil, false
	}
	return c.leafBucket, c.leafLease.Page(), true
}

// VisitCurrentLeafInlineUntil drains the current unbounded leaf's base rows up
// to the exact lexical rank limit through the fused sequential decoder. An
// overflow row stops the drain and returns its borrowed key and chain head;
// re-enter with the same limit to continue. A zero ref means limit was reached.
// Overlay-aware scans apply one sorted edit at that boundary, then continue
// with the next untouched base span.
func (c *PrimaryGraphCursor) VisitCurrentLeafInlineUntil(
	limit int,
	fn func(key, value []byte) error,
) ([]byte, PageRef, error) {
	return c.VisitCurrentLeafInlineUntilDecoded(nil, limit, fn)
}

// VisitCurrentLeafInlineUntilDecoded is VisitCurrentLeafInlineUntil with
// caller-owned sequential scalar state for repeated ordered scans.
func (c *PrimaryGraphCursor) VisitCurrentLeafInlineUntilDecoded(
	decoder *CompactPrimaryScanDecoder,
	limit int,
	fn func(key, value []byte) error,
) ([]byte, PageRef, error) {
	if c == nil || c.done || fn == nil {
		return nil, PageRef{}, nil
	}
	if len(c.lower) != 0 || len(c.upper) != 0 || len(c.prefix) != 0 {
		return nil, PageRef{}, fmt.Errorf(
			"%w: bounded current-leaf drain", ErrInvalidWrite,
		)
	}
	return c.visitCurrentLeafInlineUntil(decoder, limit, fn)
}

// ConsumeCurrentLeafBase validates and advances exactly one base row without
// rendering its old value. It is the replacement/delete companion to
// VisitCurrentLeafInlineUntil and fails closed without advancing on a stale
// key certificate.
func (c *PrimaryGraphCursor) ConsumeCurrentLeafBase(expectedKey []byte) error {
	return c.consumeCurrentLeafBase(nil, expectedKey)
}

// ConsumeCurrentLeafBaseDecoded is ConsumeCurrentLeafBase with the ordered
// decoder kept in step with the consumed row. The old value is reconstructed
// only into cursor-owned splice scratch and is never exposed to a callback.
// This matters for overlay replacements and deletes: leaving one shape's
// scalar streams behind would force every later row of that shape in the leaf
// onto bounded random-rank decoding.
func (c *PrimaryGraphCursor) ConsumeCurrentLeafBaseDecoded(
	decoder *CompactPrimaryScanDecoder,
	expectedKey []byte,
) error {
	if decoder == nil {
		return ErrInvalidWrite
	}
	return c.consumeCurrentLeafBase(decoder, expectedKey)
}

func (c *PrimaryGraphCursor) consumeCurrentLeafBase(
	decoder *CompactPrimaryScanDecoder,
	expectedKey []byte,
) error {
	if c == nil || c.done || len(expectedKey) == 0 ||
		len(c.lower) != 0 || len(c.upper) != 0 || len(c.prefix) != 0 {
		return ErrInvalidWrite
	}
	if c.row >= c.leaf.Len() {
		return ErrCommonPrimaryLeafCorrupt
	}
	var keyScratch [CommonPrimaryLeafMaxKeyBytes]byte
	key, ok := c.leaf.AppendKey(keyScratch[:0], c.row)
	if !ok || !bytes.Equal(key, expectedKey) {
		return ErrCommonPrimaryLeafCorrupt
	}
	// Keep the stale-key certificate above independent from decoder progress:
	// failure must leave both the cursor and its caller-owned decoder at the
	// same base row.
	shape := c.leaf.rowShape(c.row)
	if shape < 0 || shape >= c.leaf.shapeCount {
		return ErrCommonPrimaryLeafCorrupt
	}
	ordinal := c.sequentialShapeOrdinal(c.row, shape)
	if ordinal < 0 {
		return ErrCommonPrimaryLeafCorrupt
	}
	if decoder != nil {
		// Once the row is fully certified, advance the sequential key state and
		// prove that it names the same admitted key before advancing this shape's
		// scalar streams.
		c.spliceScratch, ok = decoder.appendKey(
			c.spliceScratch[:0], &c.leaf, c.leafBucket, c.row,
		)
		if !ok || !bytes.Equal(c.spliceScratch, key) {
			return ErrCommonPrimaryLeafCorrupt
		}
		c.spliceScratch, ok = decoder.appendValue(
			c.spliceScratch[:0], &c.leaf, c.leafBucket,
			c.row, shape, ordinal,
		)
		if !ok {
			return ErrCommonPrimaryLeafCorrupt
		}
	}
	c.row++
	return nil
}

// sequentialShapeOrdinal returns this row's ordinal within its shape while a
// cursor advances in lexical order. For the common <=64-shape leaf, it scans
// the restart prefix only once and then advances one byte counter per row.
// More diverse leaves retain the bounded random-rank decoder.
func (c *PrimaryGraphCursor) sequentialShapeOrdinal(row, shape int) int {
	if c == nil || row < 0 || row >= c.leaf.rows ||
		shape < 0 || shape >= c.leaf.shapeCount {
		return -1
	}
	if c.leaf.shapeCount > len(c.shapeSeen) {
		return c.leaf.shapeOrdinal(row, shape)
	}
	block := row / compactStreamRestart
	if c.shapeBlock != block {
		clear(c.shapeSeen[:c.leaf.shapeCount])
		start := block * compactStreamRestart
		for at := start; at < row; at++ {
			prior := c.leaf.rowShape(at)
			if prior == c.leaf.shapeCount && c.leaf.IsOverflow(at) {
				continue
			}
			if prior < 0 || prior >= c.leaf.shapeCount ||
				c.shapeSeen[prior] == compactStreamRestart {
				return -1
			}
			c.shapeSeen[prior]++
		}
		c.shapeBlock = block
	}
	seen := c.shapeSeen[shape]
	if seen == compactStreamRestart {
		return -1
	}
	c.shapeSeen[shape]++
	base := int(binary.LittleEndian.Uint16(
		c.leaf.rankTable[(block*c.leaf.shapeCount+shape)*2:],
	))
	return base + int(seen)
}

// NextLeaf releases the current leaf and selects its rooted lexical successor.
// It reports nil at end; CurrentUnifiedLeaf then returns ok=false.
func (c *PrimaryGraphCursor) NextLeaf() error {
	if c == nil || c.done {
		return nil
	}
	if err := c.advanceLeaf(); err != nil {
		c.Close()
		return err
	}
	return nil
}

// VisitInline is the scan hot path for inline primary graphs. It keeps the
// sequential rank decoder inside the leaf loop instead of paying a
// row-at-a-time cursor call. When an overflow row is encountered the drain stops
// and returns that row's borrowed key and 32-byte descriptor without invoking fn
// for it; the caller resolves the out-of-line value and re-enters to continue.
// A nil key and zero PageRef mean the scan is complete.
func (c *PrimaryGraphCursor) VisitInline(
	fn func(key, value []byte) error,
) ([]byte, PageRef, error) {
	return c.VisitInlineDecoded(nil, fn)
}

// VisitInlineDecoded is VisitInline with a bounded sequential scalar decoder.
// It preserves the same overflow stop/re-entry contract.
func (c *PrimaryGraphCursor) VisitInlineDecoded(
	decoder *CompactPrimaryScanDecoder,
	fn func(key, value []byte) error,
) ([]byte, PageRef, error) {
	if c == nil || c.done || fn == nil {
		return nil, PageRef{}, nil
	}
	if len(c.upper) == 0 && len(c.prefix) == 0 {
		return c.visitAllInline(decoder, fn)
	}
	// Prefix scans are the half-open range [prefix, successor(prefix)).
	// Derive the upper fence once per drain; an all-0xff prefix has no upper
	// fence. Valid stored keys are bounded, so an oversized prefix is empty.
	upper := c.upper
	var prefixUpper [CommonPrimaryLeafMaxKeyBytes]byte
	if len(c.prefix) != 0 {
		if len(c.prefix) > len(prefixUpper) {
			c.Close()
			return nil, PageRef{}, nil
		}
		n := copy(prefixUpper[:], c.prefix)
		for n > 0 && prefixUpper[n-1] == 0xff {
			n--
		}
		if n > 0 {
			prefixUpper[n-1]++
			if len(upper) == 0 || bytes.Compare(prefixUpper[:n], upper) < 0 {
				upper = prefixUpper[:n]
			}
		}
	}
	for {
		limit := c.leaf.Len()
		if len(upper) != 0 {
			var err error
			limit, err = c.upperRankFromCurrent(upper)
			if err != nil {
				return nil, PageRef{}, err
			}
		}
		// Resolve the fence once per leaf and drain through the same sequential
		// decoder as a full scan. The previous per-row path restarted scalar
		// decoding for every bounded/prefix result, preventing range partitioning
		// from using the full scan's throughput.
		if limit <= c.row && limit < c.leaf.Len() {
			c.Close()
			return nil, PageRef{}, nil
		}
		// Short spans cannot amortize preparing every scalar stream in a leaf.
		// Use the bounded random-rank decoder for a restart block's worth of
		// rows, preserving the same values without the sequential setup cost.
		spanDecoder := decoder
		if limit-c.row <= compactStreamRestart {
			spanDecoder = nil
		}
		key, ref, err := c.visitCurrentLeafInlineUntil(spanDecoder, limit, fn)
		if err != nil || key != nil {
			return key, ref, err
		}
		if limit < c.leaf.Len() {
			c.Close()
			return nil, PageRef{}, nil
		}
		if err := c.advanceLeaf(); err != nil {
			c.Close()
			return nil, PageRef{}, err
		}
		if c.done {
			return nil, PageRef{}, nil
		}
	}
}

// upperRankFromCurrent brackets the upper fence from the current row before
// binary searching. A lookup-sized range should not search the entire leaf;
// a long range still needs only logarithmically many key decodes.
func (c *PrimaryGraphCursor) upperRankFromCurrent(upper []byte) (int, error) {
	rows := c.leaf.Len()
	if c.row >= rows {
		return rows, nil
	}
	var scratch [CommonPrimaryLeafMaxKeyBytes]byte
	lo, hi := c.row, rows
	for step := 1; ; step = min(step*2, rows-c.row) {
		probe := c.row + step - 1
		key, ok := c.leaf.AppendKey(scratch[:0], probe)
		if !ok {
			return 0, ErrCommonPrimaryLeafCorrupt
		}
		if bytes.Compare(key, upper) >= 0 {
			hi = probe
			break
		}
		lo = probe + 1
		if lo == rows {
			return rows, nil
		}
	}
	for lo < hi {
		mid := lo + (hi-lo)/2
		key, ok := c.leaf.AppendKey(scratch[:0], mid)
		if !ok {
			return 0, ErrCommonPrimaryLeafCorrupt
		}
		if bytes.Compare(key, upper) >= 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo, nil
}

func (c *PrimaryGraphCursor) visitAllInline(
	decoder *CompactPrimaryScanDecoder,
	fn func(key, value []byte) error,
) ([]byte, PageRef, error) {
	for {
		key, ref, err := c.visitCurrentLeafInlineUntil(
			decoder, c.leaf.Len(), fn,
		)
		if err != nil {
			return nil, PageRef{}, err
		}
		if key != nil {
			return key, ref, nil
		}
		if err := c.advanceLeaf(); err != nil {
			c.Close()
			return nil, PageRef{}, err
		}
		if c.done {
			return nil, PageRef{}, nil
		}
	}
}

// visitCurrentLeafInlineUntil reconstructs one exact compact base span.
func (c *PrimaryGraphCursor) visitCurrentLeafInlineUntil(
	decoder *CompactPrimaryScanDecoder,
	limit int,
	fn func(key, value []byte) error,
) (overflowKey []byte, overflow PageRef, err error) {
	if limit < c.row || limit > c.leaf.Len() {
		return nil, PageRef{}, ErrCommonPrimaryLeafCorrupt
	}
	for c.row < limit {
		if ref, overflow := c.leaf.OverflowRef(c.row); overflow {
			var ok bool
			if decoder == nil {
				c.spliceScratch, ok = c.leaf.AppendKey(c.spliceScratch[:0], c.row)
			} else {
				c.spliceScratch, ok = decoder.appendKey(
					c.spliceScratch[:0], &c.leaf, c.leafBucket, c.row,
				)
			}
			if !ok {
				return nil, PageRef{}, ErrCommonPrimaryLeafCorrupt
			}
			c.row++
			return c.spliceScratch, ref, nil
		}
		var ok bool
		if decoder == nil {
			c.spliceScratch, ok = c.leaf.AppendKey(c.spliceScratch[:0], c.row)
		} else {
			c.spliceScratch, ok = decoder.appendKey(
				c.spliceScratch[:0], &c.leaf, c.leafBucket, c.row,
			)
		}
		if !ok {
			return nil, PageRef{}, ErrCommonPrimaryLeafCorrupt
		}
		keyEnd := len(c.spliceScratch)
		shape := c.leaf.rowShape(c.row)
		ordinal := c.sequentialShapeOrdinal(c.row, shape)
		if decoder == nil {
			c.spliceScratch, ok = c.leaf.appendValueOrdinal(
				c.spliceScratch, c.row, shape, ordinal,
			)
		} else {
			c.spliceScratch, ok = decoder.appendValue(
				c.spliceScratch, &c.leaf, c.leafBucket,
				c.row, shape, ordinal,
			)
		}
		if !ok {
			return nil, PageRef{}, fmt.Errorf(
				"%w: compact value bucket=%d row=%d shape=%d ordinal=%d",
				ErrCommonPrimaryLeafCorrupt, c.leafBucket, c.row, shape, ordinal,
			)
		}
		c.row++
		if err := fn(c.spliceScratch[:keyEnd], c.spliceScratch[keyEnd:]); err != nil {
			return nil, PageRef{}, err
		}
	}
	return nil, PageRef{}, nil
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
	c.leafBucket = 0
	c.leaf = CompactPrimaryStripeView{}
	c.row = 0
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

// AdoptSpliceScratch seeds the cursor's document-reconstruction buffer with a
// caller-retained slice. A unified leaf reconstructs each row into this buffer,
// and a fresh cursor would grow it from nil on the first such row of the scan;
// seeding it from a buffer the caller keeps across scans makes a warm ordered
// scan render into retained capacity instead. Call it after
// InitPrimaryGraphCursor, which resets the cursor. ReleaseSpliceScratch returns
// the possibly-grown buffer for the caller to retain for the next scan.
func (c *PrimaryGraphCursor) AdoptSpliceScratch(buf []byte) {
	c.spliceScratch = buf[:0]
}

// ReleaseSpliceScratch returns the cursor's document-reconstruction buffer so
// the caller can retain its grown capacity across scans.
func (c *PrimaryGraphCursor) ReleaseSpliceScratch() []byte {
	return c.spliceScratch
}
