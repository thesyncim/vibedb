package storeio

import (
	"bytes"
	"fmt"
)

// PrimaryGraphRecord is one immutable key/value row supplied to
// BuildPrimaryGraph. Both slices are borrowed until the builder returns.
type PrimaryGraphRecord struct {
	Key   []byte
	Value []byte
}

// PrimaryGraphPlacement is the posting-stable location assigned to one input
// record by the bottom-up builder: the leaf's stable BucketID and the row's
// stable slot within it (see VisitPrimaryLeafPostingRows for the per-class slot
// model). Exact-index posting tiles are keyed by this placement.
type PrimaryGraphPlacement struct {
	Bucket BucketID
	Slot   uint8
}

type primaryLeafPlan struct {
	first   int
	last    int
	class   CommonPrimaryLeafClass
	records []CommonPrimaryLeafRecord
	// extent is the planner-selected physical page size.
	extent int
}

// PrimaryGraphPlan is one immutable, exact compact-leaf plan shared by bulk
// reservation, optional exact-index sizing, and graph construction. It borrows
// records and their byte slices until the build finishes; callers must not
// mutate them. Reusing the plan prevents each phase from re-encoding every
// candidate leaf solely to rediscover identical boundaries.
type PrimaryGraphPlan struct {
	storeID [16]byte
	records []PrimaryGraphRecord
	leaves  []primaryLeafPlan
	pages   int
	placed  bool
}

// PlanPrimaryGraph computes exact compact leaf boundaries and total graph page
// count. placed selects <=256-row leaves with stable uint8 posting slots for an
// exact secondary index; false permits the 4,096-row unindexed scan layout.
func PlanPrimaryGraph(
	storeID [16]byte,
	records []PrimaryGraphRecord,
	placed bool,
) (PrimaryGraphPlan, error) {
	return planPrimaryGraph(
		storeID, records, placed, CommonPrimaryLeafMaxExtentBytes,
	)
}

func planPrimaryGraph(
	storeID [16]byte,
	records []PrimaryGraphRecord,
	placed bool,
	maxExtent int,
) (PrimaryGraphPlan, error) {
	if err := validatePrimaryGraphRecords(storeID, records); err != nil {
		return PrimaryGraphPlan{}, err
	}
	maxRows := CompactPrimaryStripeMaxRows
	if placed {
		maxRows = CommonPrimaryLeafWideSlots
	}
	leaves, err := planCompactPrimaryLeaves(
		storeID, records, maxRows, maxExtent,
	)
	if err != nil {
		return PrimaryGraphPlan{}, err
	}
	pages, err := primaryGraphPageCountForLeaves(len(leaves))
	if err != nil {
		return PrimaryGraphPlan{}, err
	}
	return PrimaryGraphPlan{
		storeID: storeID, records: records, leaves: leaves,
		pages: pages, placed: placed,
	}, nil
}

// PageCount is the exact number of transaction pages Build will stage.
func (p *PrimaryGraphPlan) PageCount() int {
	if p == nil {
		return 0
	}
	return p.pages
}

// LeafSpans returns the planned ordered leaves for exact-index sizing.
func (p *PrimaryGraphPlan) LeafSpans() ([]PrimaryGraphLeafSpan, error) {
	if p == nil || !p.placed || len(p.leaves) == 0 {
		return nil, fmt.Errorf("%w: primary graph span plan", ErrInvalidWrite)
	}
	spans := make([]PrimaryGraphLeafSpan, len(p.leaves))
	for rank := range p.leaves {
		tabletID := uint32(rank / TabletLocalIdentityLocalCount)
		localID := uint32(rank % TabletLocalIdentityLocalCount)
		bucket, ok := MakeTabletLocalIdentityBucket(tabletID, localID)
		if !ok {
			return nil, fmt.Errorf("%w: primary leaf identity", ErrInvalidWrite)
		}
		spans[rank] = PrimaryGraphLeafSpan{
			Bucket: BucketID(bucket), First: p.leaves[rank].first,
			Last: p.leaves[rank].last, Ordinal: true,
		}
	}
	return spans, nil
}

type primaryBuiltLeaf struct {
	firstKey []byte
	lastKey  []byte
	ref      PageRef
}

type primaryCatalogChild struct {
	floor []byte
	id    uint32
	ref   PageRef
}

// ShortestPrimaryFence returns the shortest prefix of rightMin that is
// strictly greater than leftMax. The caller owns dst and the result aliases it.
func ShortestPrimaryFence(dst, leftMax, rightMin []byte) ([]byte, error) {
	if bytes.Compare(leftMax, rightMin) >= 0 {
		return nil, fmt.Errorf("%w: overlapping primary ranges", ErrInvalidWrite)
	}
	common := 0
	for common < len(leftMax) && common < len(rightMin) &&
		leftMax[common] == rightMin[common] {
		common++
	}
	length := common + 1
	if length > len(rightMin) || len(dst) < length {
		return nil, fmt.Errorf("%w: primary fence destination", ErrInvalidWrite)
	}
	copy(dst, rightMin[:length])
	return dst[:length], nil
}

// BuildPrimaryGraph deterministically stages one complete ordered primary
// graph in tx. Records must be strictly bytewise lexical and contain inline
// non-empty values. Every leaf uses the compact stripe grammar.
//
// The function stages leaves, segmented tablet pages, and catalog levels in
// bottom-up order. It does not publish tx or modify a StateRoot; the returned
// PagePrimaryCatalog reference is suitable for StateRoot.PrimaryRoot.
func BuildPrimaryGraph(
	tx *WriteTransaction,
	records []PrimaryGraphRecord,
) (PageRef, error) {
	return buildPrimaryGraphPlaced(tx, records, nil)
}

// BuildPrimaryGraphPlaced is BuildPrimaryGraph with caller-owned placement
// output: placements must have exactly one element per input record, and each
// receives the posting-stable location the builder assigned that row. It is the
// entry the ordered-primary exact-index build uses to key posting tiles.
func BuildPrimaryGraphPlaced(
	tx *WriteTransaction,
	records []PrimaryGraphRecord,
	placements []PrimaryGraphPlacement,
) (PageRef, error) {
	if len(placements) != len(records) {
		return PageRef{}, fmt.Errorf("%w: primary placement output", ErrInvalidWrite)
	}
	return buildPrimaryGraphPlaced(tx, records, placements)
}

// EmptyPrimaryGraphPageCount is the exact number of transaction pages
// BuildEmptyPrimaryGraph stages: one leaf, one tablet (anchor + locator +
// route), one catalog leaf, and the catalog root. A creation transaction
// reserves this plus its exact-index root when configured.
const EmptyPrimaryGraphPageCount = 1 + 3 + 1 + 1

// BuildEmptyPrimaryGraph stages a valid ordered primary graph that holds no
// documents: one empty compact stripe (tablet 0, local 0) spanning the entire key
// range, its single-anchor tablet, and a one-child catalog root. It is the
// creation-time counterpart of BuildPrimaryGraph — a freshly created collection
// is a primary-layout store from its first byte, and its first Put routes to
// this empty leaf and fills it exactly as a runtime insert fills a leaf a delete
// emptied. Both halves are already exercised in production (a single-document
// build produces one leaf/tablet/catalog; a delete of the last row produces an
// empty leaf), so this only composes them. The returned reference is suitable
// for StateRoot.PrimaryRoot.
func BuildEmptyPrimaryGraph(tx *WriteTransaction) (PageRef, error) {
	if tx == nil || !tx.active || tx.options.PageSize != physicalPageQuantum ||
		tx.options.StoreID == ([16]byte{}) || tx.options.Generation == 0 ||
		tx.nextID < PrimaryFirstDynamicLogicalID {
		return PageRef{}, fmt.Errorf("%w: empty primary graph transaction", ErrInvalidWrite)
	}
	bucket, ok := MakeTabletLocalIdentityBucket(0, 0)
	logicalID, logicalOK := CommonPrimaryLeafLogicalID(BucketID(bucket))
	if !ok || !logicalOK {
		return PageRef{}, fmt.Errorf("%w: empty primary leaf identity", ErrInvalidWrite)
	}
	page, err := tx.Allocate(
		PagePrimaryLeaf, CommonPrimaryLeafNarrowBytes, logicalID,
	)
	if err != nil {
		return PageRef{}, err
	}
	if _, err := EncodeCompactPrimaryStripe(
		page.Bytes(),
		CommonPrimaryLeafHeader{
			StoreID: tx.options.StoreID, Generation: tx.options.Generation,
			Bucket: BucketID(bucket), PageSize: CommonPrimaryLeafNarrowBytes,
		},
		nil,
		NewUnifiedPrimaryLeafBuilder(),
	); err != nil {
		return PageRef{}, err
	}
	if err := page.Stage(); err != nil {
		return PageRef{}, err
	}
	tablets, err := buildPrimaryTablets(tx, []primaryBuiltLeaf{{ref: page.Ref()}})
	if err != nil {
		return PageRef{}, err
	}
	return buildPrimaryCatalog(tx, tablets)
}

func buildPrimaryGraphPlaced(
	tx *WriteTransaction,
	records []PrimaryGraphRecord,
	placements []PrimaryGraphPlacement,
) (PageRef, error) {
	if tx == nil || !tx.active || tx.options.PageSize != physicalPageQuantum ||
		tx.options.StoreID == ([16]byte{}) ||
		tx.options.Generation == 0 ||
		tx.nextID < PrimaryFirstDynamicLogicalID ||
		len(records) == 0 {
		return PageRef{}, fmt.Errorf("%w: primary graph transaction or input", ErrInvalidWrite)
	}
	plan, err := planPrimaryGraph(
		tx.options.StoreID, records, placements != nil,
		min(tx.committer.bufferSize, CommonPrimaryLeafMaxExtentBytes),
	)
	if err != nil {
		return PageRef{}, err
	}
	return BuildPlannedPrimaryGraph(tx, &plan, placements)
}

// BuildPlannedPrimaryGraph stages a plan returned by PlanPrimaryGraph without
// repeating compact leaf planning. The transaction's store identity and page
// capacity must match the plan; placements must match its placed mode.
func BuildPlannedPrimaryGraph(
	tx *WriteTransaction,
	plan *PrimaryGraphPlan,
	placements []PrimaryGraphPlacement,
) (PageRef, error) {
	if tx == nil || !tx.active || plan == nil ||
		tx.options.PageSize != physicalPageQuantum ||
		tx.options.StoreID != plan.storeID || tx.options.Generation == 0 ||
		tx.nextID < PrimaryFirstDynamicLogicalID || len(plan.records) == 0 ||
		plan.pages == 0 || plan.placed != (placements != nil) ||
		placements != nil && len(placements) != len(plan.records) {
		return PageRef{}, fmt.Errorf("%w: planned primary graph input", ErrInvalidWrite)
	}
	for i := range plan.leaves {
		if plan.leaves[i].extent > tx.committer.bufferSize {
			return PageRef{}, fmt.Errorf("%w: planned primary graph extent", ErrInvalidWrite)
		}
	}
	built, err := buildPrimaryLeaves(
		tx, plan.records, plan.leaves, placements,
	)
	if err != nil {
		return PageRef{}, err
	}
	tablets, err := buildPrimaryTablets(tx, built)
	if err != nil {
		return PageRef{}, err
	}
	return buildPrimaryCatalog(tx, tablets)
}

// PrimaryGraphPageCount returns the exact number of transaction pages
// BuildPrimaryGraph will stage for records. Bulk callers use it to
// reserve one bounded commit without guessing from document count or average
// value width.
func PrimaryGraphPageCount(
	storeID [16]byte,
	records []PrimaryGraphRecord,
) (int, error) {
	plan, err := PlanPrimaryGraph(storeID, records, false)
	return plan.PageCount(), err
}

// PrimaryGraphPlacedPageCount is the exact graph-page count for a build that
// also emits uint8 ordinal placements for exact-index posting tiles.
func PrimaryGraphPlacedPageCount(
	storeID [16]byte,
	records []PrimaryGraphRecord,
) (int, error) {
	plan, err := PlanPrimaryGraph(storeID, records, true)
	return plan.PageCount(), err
}

func validatePrimaryGraphRecords(
	storeID [16]byte,
	records []PrimaryGraphRecord,
) error {
	if storeID == ([16]byte{}) || len(records) == 0 {
		return fmt.Errorf("%w: primary graph input", ErrInvalidWrite)
	}
	for at := range records {
		if len(records[at].Key) == 0 ||
			len(records[at].Key) > CommonPrimaryLeafMaxKeyBytes ||
			len(records[at].Value) == 0 ||
			at != 0 && bytes.Compare(records[at-1].Key, records[at].Key) >= 0 {
			return fmt.Errorf("%w: non-canonical primary records", ErrInvalidWrite)
		}
	}
	return nil
}

func primaryGraphPageCountForLeaves(leafCount int) (int, error) {
	if leafCount < 1 {
		return 0, fmt.Errorf("%w: primary leaf count", ErrInvalidWrite)
	}
	if leafCount > TabletLocalIdentityTabletCount*TabletLocalIdentityLocalCount {
		return 0, fmt.Errorf("%w: primary leaf namespace exhausted", ErrInvalidWrite)
	}
	tabletCount := (leafCount + TabletLocalIdentityLocalCount - 1) /
		TabletLocalIdentityLocalCount
	anchorCount := (leafCount + SegmentedTabletRouterRowsPerPage - 1) /
		SegmentedTabletRouterRowsPerPage
	leafFanout := GlobalTabletCatalogWorstCaseFanout(
		GlobalTabletCatalogNodeBytes, CommonPrimaryLeafMaxKeyBytes,
	)
	rootFanout := GlobalTabletCatalogWorstCaseFanout(
		GlobalTabletCatalogRootBytes, CommonPrimaryLeafMaxKeyBytes,
	)
	if leafFanout == 0 || rootFanout == 0 {
		return 0, fmt.Errorf("%w: primary catalog geometry", ErrInvalidWrite)
	}
	catalogLeaves := (tabletCount + leafFanout - 1) / leafFanout
	catalogBranches := 0
	rootChildren := catalogLeaves
	if catalogLeaves > rootFanout {
		catalogBranches = (catalogLeaves + leafFanout - 1) / leafFanout
		rootChildren = catalogBranches
	}
	if catalogLeaves > GlobalTabletCatalogMaxLeafPages ||
		catalogBranches > GlobalTabletCatalogMaxBranchPages ||
		rootChildren > rootFanout {
		return 0, fmt.Errorf("%w: primary catalog capacity", ErrInvalidWrite)
	}
	return leafCount + anchorCount + 2*tabletCount +
		catalogLeaves + catalogBranches + 1, nil
}

// PrimaryGraphLeafSpan describes one planned primary leaf without staging it:
// the input record range it will hold, the bucket it will own, and whether
// its compact stripe assigns posting slots by row ordinal. Bulk
// reservation uses it to bound the spanned exact-index page set before the
// build transaction opens.
type PrimaryGraphLeafSpan struct {
	Bucket  BucketID
	First   int
	Last    int
	Ordinal bool
}

// PrimaryGraphLeafSpans plans the leaves BuildPrimaryGraph will stage for
// records — identical planning, no staging.
func PrimaryGraphLeafSpans(
	storeID [16]byte,
	records []PrimaryGraphRecord,
) ([]PrimaryGraphLeafSpan, error) {
	plan, err := PlanPrimaryGraph(storeID, records, true)
	if err != nil {
		return nil, err
	}
	return plan.LeafSpans()
}

// planCompactPrimaryLeaves packs records into the largest compact stripe that
// fits the configured row and extent bounds. There is no fallback format.
func planCompactPrimaryLeaves(
	storeID [16]byte,
	records []PrimaryGraphRecord,
	maxRows, maxExtent int,
) ([]primaryLeafPlan, error) {
	if storeID == ([16]byte{}) || maxRows < 1 ||
		maxRows > CompactPrimaryStripeMaxRows ||
		maxExtent < int(physicalPageQuantum) || maxExtent > CommonPrimaryLeafMaxExtentBytes {
		return nil, fmt.Errorf("%w: compact primary plan geometry", ErrInvalidWrite)
	}
	var plans []primaryLeafPlan
	builder := NewUnifiedPrimaryLeafBuilder()
	window := make([]CommonPrimaryLeafRecord, 0, maxRows)
	for first := 0; first < len(records); {
		hi := min(maxRows, len(records)-first)
		window = window[:0]
		for at := range hi {
			record := CommonPrimaryLeafRecord{
				Key:   records[first+at].Key,
				Value: CommonPrimaryLeafValue{Inline: records[first+at].Value},
			}
			if maxRows <= CommonPrimaryLeafWideSlots {
				record.Slot = uint8(at)
			}
			window = append(window, record)
		}
		fits := func(count int) (int, bool, error) {
			payload, err := BuildCompactPrimaryStripePayload(window[:count], builder)
			if err == ErrCommonPrimaryLeafFull {
				return maxExtent + int(physicalPageQuantum), false, nil
			}
			if err != nil {
				return 0, false, err
			}
			need := PageHeaderSize + len(payload) + PageTrailerSize
			quantum := int(physicalPageQuantum)
			extent := (need + quantum - 1) &^ (quantum - 1)
			return extent, extent <= maxExtent, nil
		}
		count, extent := 0, 0
		// Log-like compact windows normally fit all 4,096 rows. Prove that
		// common case with one encoding before paying the bounded binary search;
		// oversized/high-cardinality windows still select the exact same largest
		// fitting prefix below.
		candidate, fullWindowFits, err := fits(hi)
		if err != nil {
			return nil, err
		}
		if fullWindowFits {
			count, extent = hi, candidate
		}
		for lo, high := 1, hi-1; count == 0 && lo <= high; {
			mid := (lo + high) / 2
			candidate, ok, err := fits(mid)
			if err != nil {
				return nil, err
			}
			if ok {
				count, extent = mid, candidate
				lo = mid + 1
			} else {
				high = mid - 1
			}
		}
		if count == 0 {
			return nil, fmt.Errorf("%w: compact primary record exceeds extent", ErrInvalidWrite)
		}
		plans = append(plans, primaryLeafPlan{
			first: first, last: first + count,
			class:   CommonPrimaryLeafCompact,
			records: append([]CommonPrimaryLeafRecord(nil), window[:count]...),
			extent:  extent,
		})
		first += count
	}
	return plans, nil
}

func buildPrimaryLeaves(
	tx *WriteTransaction,
	input []PrimaryGraphRecord,
	plans []primaryLeafPlan,
	placements []PrimaryGraphPlacement,
) ([]primaryBuiltLeaf, error) {
	built := make([]primaryBuiltLeaf, len(plans))
	unifiedBuilder := NewUnifiedPrimaryLeafBuilder()
	for rank := range plans {
		tabletID := uint32(rank / TabletLocalIdentityLocalCount)
		localID := uint32(rank % TabletLocalIdentityLocalCount)
		bucket, ok := MakeTabletLocalIdentityBucket(tabletID, localID)
		logicalID, logicalOK := CommonPrimaryLeafLogicalID(BucketID(bucket))
		if !ok || !logicalOK {
			return nil, fmt.Errorf("%w: primary leaf identity", ErrInvalidWrite)
		}
		if plans[rank].class != CommonPrimaryLeafCompact ||
			plans[rank].extent == 0 {
			return nil, fmt.Errorf("%w: non-unified primary plan", ErrInvalidWrite)
		}
		pageSize := uint32(plans[rank].extent)
		page, err := tx.Allocate(PagePrimaryLeaf, pageSize, logicalID)
		if err != nil {
			return nil, err
		}
		leafHeader := CommonPrimaryLeafHeader{
			StoreID: tx.options.StoreID, Generation: tx.options.Generation,
			Bucket: BucketID(bucket), PageSize: pageSize,
		}
		if _, err := EncodeCompactPrimaryStripe(
			page.Bytes(), leafHeader, plans[rank].records, unifiedBuilder,
		); err != nil {
			return nil, err
		}
		if placements != nil {
			if err := recordPrimaryPlacements(
				placements, input, plans[rank], BucketID(bucket),
			); err != nil {
				return nil, err
			}
		}
		if err := page.Stage(); err != nil {
			return nil, err
		}
		built[rank] = primaryBuiltLeaf{
			firstKey: input[plans[rank].first].Key,
			lastKey:  input[plans[rank].last-1].Key,
			ref:      page.Ref(),
		}
	}
	return built, nil
}

// recordPrimaryPlacements fills the compact stripe's posting-stable ordinal
// for every input row of one just-encoded leaf.
func recordPrimaryPlacements(
	placements []PrimaryGraphPlacement,
	input []PrimaryGraphRecord,
	plan primaryLeafPlan,
	bucket BucketID,
) error {
	if plan.class != CommonPrimaryLeafCompact {
		return fmt.Errorf("%w: non-unified placement plan", ErrInvalidWrite)
	}
	if plan.last-plan.first > CommonPrimaryLeafWideSlots {
		return fmt.Errorf("%w: compact ordinal placement width", ErrInvalidWrite)
	}
	for at := plan.first; at < plan.last; at++ {
		if !bytes.Equal(input[at].Key, plan.records[at-plan.first].Key) {
			return fmt.Errorf("%w: unified placement input", ErrInvalidWrite)
		}
		placements[at] = PrimaryGraphPlacement{
			Bucket: bucket, Slot: plan.records[at-plan.first].Slot,
		}
	}
	return nil
}

func buildPrimaryTablets(
	tx *WriteTransaction,
	leaves []primaryBuiltLeaf,
) ([]primaryCatalogChild, error) {
	tabletCount := (len(leaves) + TabletLocalIdentityLocalCount - 1) /
		TabletLocalIdentityLocalCount
	if tabletCount > TabletLocalIdentityTabletCount {
		return nil, fmt.Errorf("%w: tablet namespace exhausted", ErrInvalidWrite)
	}
	tablets := make([]primaryCatalogChild, tabletCount)
	var previousTabletMax []byte
	for tabletAt := range tabletCount {
		first := tabletAt * TabletLocalIdentityLocalCount
		last := min(first+TabletLocalIdentityLocalCount, len(leaves))
		tabletLeaves := leaves[first:last]
		tabletID := uint32(tabletAt)
		fences := make([][]byte, len(tabletLeaves))
		routerLeaves := make([]SegmentedTabletRouterLeaf, len(tabletLeaves))
		locatorEntries := make([]GlobalTabletCatalogLocatorEntry, len(tabletLeaves))
		for rank := range tabletLeaves {
			if rank != 0 {
				fence := make([]byte, len(tabletLeaves[rank].firstKey))
				var err error
				fences[rank], err = ShortestPrimaryFence(
					fence, tabletLeaves[rank-1].lastKey,
					tabletLeaves[rank].firstKey,
				)
				if err != nil {
					return nil, err
				}
			}
			localID := uint16(rank)
			routerLeaves[rank] = SegmentedTabletRouterLeaf{
				LocalID: localID, Fence: fences[rank],
				Ref: tabletLeaves[rank].ref,
			}
			locatorEntries[rank] = GlobalTabletCatalogLocatorEntry{
				LocalID: localID,
				PageID:  uint8(rank / SegmentedTabletRouterRowsPerPage),
				RowSlot: uint8(rank % SegmentedTabletRouterRowsPerPage),
				State:   GlobalTabletCatalogLocatorLive,
			}
		}

		pageCount := (len(tabletLeaves) + SegmentedTabletRouterRowsPerPage - 1) /
			SegmentedTabletRouterRowsPerPage
		anchorPages := make([]TransactionPage, pageCount)
		anchorRefs := make([]PageRef, pageCount)
		for pageID := range pageCount {
			logicalID, _ := GlobalTabletCatalogAnchorLogicalID(
				tabletID, uint8(pageID),
			)
			page, err := tx.Allocate(
				PagePrimaryAnchor, SegmentedTabletRouterAnchorPageBytes,
				logicalID,
			)
			if err != nil {
				return nil, err
			}
			anchorPages[pageID] = page
			anchorRefs[pageID] = page.Ref()
		}
		locatorLogical, _ := GlobalTabletCatalogLocatorLogicalID(tabletID)
		locatorPage, err := tx.Allocate(
			PagePrimaryLocator, GlobalTabletCatalogLocatorBytes, locatorLogical,
		)
		if err != nil {
			return nil, err
		}
		routeLogical, _ := GlobalTabletCatalogTabletRootLogicalID(tabletID)
		routePage, err := tx.Allocate(
			PageTabletRoute, GlobalTabletCatalogTabletBytes, routeLogical,
		)
		if err != nil {
			return nil, err
		}

		rawRoot := make([]byte, SegmentedTabletRouterRootBytes)
		rawLocator := make([]byte, SegmentedTabletRouterLocatorBytes)
		rawAnchors := make(
			[]byte, pageCount*SegmentedTabletRouterAnchorPageBytes,
		)
		header := SegmentedTabletRouterHeader{
			StoreID: tx.options.StoreID, TabletID: tabletID,
			Generation: tx.options.Generation,
			AnchorKind: PagePrimaryAnchor, LeafKind: PagePrimaryLeaf,
		}
		if _, _, _, _, err := EncodeSegmentedTabletRouter(
			rawRoot, rawLocator, rawAnchors, header, anchorRefs, routerLeaves,
		); err != nil {
			return nil, err
		}
		for pageID := range anchorPages {
			start := pageID * SegmentedTabletRouterAnchorPageBytes
			copy(
				anchorPages[pageID].Bytes(),
				rawAnchors[start:start+SegmentedTabletRouterAnchorPageBytes],
			)
			if err := anchorPages[pageID].Stage(); err != nil {
				return nil, err
			}
		}

		bounds := primaryCatalogBounds(tx)
		if _, err := EncodeGlobalTabletCatalogLocator(
			locatorPage.Bytes(),
			PageHeader{
				StoreID: tx.options.StoreID, Generation: tx.options.Generation,
				LogicalID: locatorLogical,
				PageSize:  GlobalTabletCatalogLocatorBytes,
				PayloadLength: GlobalTabletCatalogLocatorHeader +
					globalTabletCatalogPackedBytes,
				Kind: PagePrimaryLocator,
			},
			bounds, tabletID, tx.options.Generation, locatorEntries,
		); err != nil {
			return nil, err
		}
		if err := locatorPage.Stage(); err != nil {
			return nil, err
		}
		if _, err := EncodeGlobalTabletCatalogTabletRoot(
			routePage.Bytes(),
			PageHeader{
				StoreID: tx.options.StoreID, Generation: tx.options.Generation,
				LogicalID: routeLogical, PageSize: GlobalTabletCatalogTabletBytes,
				PayloadLength: GlobalTabletCatalogRootHeader +
					SegmentedTabletRouterRootBytes,
				Kind: PageTabletRoute,
			},
			bounds, locatorPage.Ref(), rawRoot,
		); err != nil {
			return nil, err
		}
		if err := routePage.Stage(); err != nil {
			return nil, err
		}

		var floor []byte
		if tabletAt != 0 {
			floor = make([]byte, len(tabletLeaves[0].firstKey))
			floor, err = ShortestPrimaryFence(
				floor, previousTabletMax, tabletLeaves[0].firstKey,
			)
			if err != nil {
				return nil, err
			}
		}
		tablets[tabletAt] = primaryCatalogChild{
			floor: floor, id: tabletID, ref: routePage.Ref(),
		}
		previousTabletMax = tabletLeaves[len(tabletLeaves)-1].lastKey
	}
	return tablets, nil
}

func buildPrimaryCatalog(
	tx *WriteTransaction,
	tablets []primaryCatalogChild,
) (PageRef, error) {
	leafFanout := GlobalTabletCatalogWorstCaseFanout(
		GlobalTabletCatalogNodeBytes, CommonPrimaryLeafMaxKeyBytes,
	)
	rootFanout := GlobalTabletCatalogWorstCaseFanout(
		GlobalTabletCatalogRootBytes, CommonPrimaryLeafMaxKeyBytes,
	)
	if leafFanout == 0 || rootFanout == 0 {
		return PageRef{}, fmt.Errorf("%w: primary catalog geometry", ErrInvalidWrite)
	}
	leaves, err := buildPrimaryCatalogLevel(
		tx, GlobalTabletCatalogLeaf, tablets, leafFanout,
	)
	if err != nil {
		return PageRef{}, err
	}
	rootChildren := leaves
	rootChildLevel := GlobalTabletCatalogLeaf
	if len(leaves) > rootFanout {
		rootChildren, err = buildPrimaryCatalogLevel(
			tx, GlobalTabletCatalogBranch, leaves, leafFanout,
		)
		if err != nil {
			return PageRef{}, err
		}
		rootChildLevel = GlobalTabletCatalogBranch
	}
	if len(rootChildren) > rootFanout {
		return PageRef{}, fmt.Errorf("%w: primary catalog root capacity", ErrInvalidWrite)
	}
	rootPage, err := tx.Allocate(
		PagePrimaryCatalog, GlobalTabletCatalogRootBytes,
		GlobalTabletCatalogRootLogicalID,
	)
	if err != nil {
		return PageRef{}, err
	}
	entries := primaryCatalogEntries(rootChildren)
	if _, err := EncodeGlobalTabletCatalogNode(
		rootPage.Bytes(),
		GlobalTabletCatalogNodeHeader{
			StoreID: tx.options.StoreID, Generation: tx.options.Generation,
			LogicalID: GlobalTabletCatalogRootLogicalID,
			Level:     GlobalTabletCatalogRoot, RootChildLevel: rootChildLevel,
			Kind: PagePrimaryCatalog, ChildKind: PagePrimaryCatalog,
			ChildLength: GlobalTabletCatalogNodeBytes,
			Bounds:      primaryCatalogBounds(tx),
		},
		entries,
	); err != nil {
		return PageRef{}, err
	}
	if err := rootPage.Stage(); err != nil {
		return PageRef{}, err
	}
	return rootPage.Ref(), nil
}

func buildPrimaryCatalogLevel(
	tx *WriteTransaction,
	level GlobalTabletCatalogNodeLevel,
	children []primaryCatalogChild,
	fanout int,
) ([]primaryCatalogChild, error) {
	count := (len(children) + fanout - 1) / fanout
	limit := GlobalTabletCatalogMaxLeafPages
	if level == GlobalTabletCatalogBranch {
		limit = GlobalTabletCatalogMaxBranchPages
	}
	if count > limit {
		return nil, fmt.Errorf("%w: primary catalog level capacity", ErrInvalidWrite)
	}
	result := make([]primaryCatalogChild, count)
	for pageID := range count {
		first := pageID * fanout
		last := min(first+fanout, len(children))
		logicalID, ok := GlobalTabletCatalogCatalogLeafLogicalID(uint32(pageID))
		if level == GlobalTabletCatalogBranch {
			logicalID, ok = GlobalTabletCatalogCatalogBranchLogicalID(uint32(pageID))
		}
		if !ok {
			return nil, fmt.Errorf("%w: primary catalog logical ID", ErrInvalidWrite)
		}
		page, err := tx.Allocate(
			PagePrimaryCatalog, GlobalTabletCatalogNodeBytes, logicalID,
		)
		if err != nil {
			return nil, err
		}
		childKind := PageTabletRoute
		childLength := uint32(GlobalTabletCatalogTabletBytes)
		if level == GlobalTabletCatalogBranch {
			childKind = PagePrimaryCatalog
			childLength = GlobalTabletCatalogNodeBytes
		}
		if _, err := EncodeGlobalTabletCatalogNode(
			page.Bytes(),
			GlobalTabletCatalogNodeHeader{
				StoreID: tx.options.StoreID, Generation: tx.options.Generation,
				LogicalID: logicalID, PageID: uint32(pageID), Level: level,
				Kind: PagePrimaryCatalog, ChildKind: childKind,
				ChildLength: childLength, Bounds: primaryCatalogBounds(tx),
			},
			primaryCatalogEntries(children[first:last]),
		); err != nil {
			return nil, err
		}
		if err := page.Stage(); err != nil {
			return nil, err
		}
		result[pageID] = primaryCatalogChild{
			floor: children[first].floor, id: uint32(pageID), ref: page.Ref(),
		}
	}
	return result, nil
}

func primaryCatalogEntries(
	children []primaryCatalogChild,
) []GlobalTabletCatalogNodeEntry {
	entries := make([]GlobalTabletCatalogNodeEntry, len(children))
	for at := range children {
		entries[at] = GlobalTabletCatalogNodeEntry{
			ID: children[at].id, Ref: children[at].ref,
		}
		if at != 0 {
			entries[at].Floor = children[at].floor
		}
	}
	return entries
}

func primaryCatalogBounds(tx *WriteTransaction) GlobalTabletCatalogBounds {
	// The catalog codec's admission context reserves room for the eventual
	// 64 KiB root. Tiny graphs can stage their tablet pages before physical
	// FileEnd reaches that size; the bottom-up build always allocates the root
	// before publication, so this is the exact prospective lower bound.
	fileEnd := max(tx.fileEnd, uint64(GlobalTabletCatalogRootBytes))
	return GlobalTabletCatalogBounds{
		StoreID:                tx.options.StoreID,
		SelectedRootGeneration: tx.options.Generation,
		FileEnd:                fileEnd,
		NextLogicalID:          tx.nextID,
	}
}
