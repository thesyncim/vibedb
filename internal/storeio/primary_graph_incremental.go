package storeio

import (
	"fmt"

	vibejson "github.com/thesyncim/vibejson"
)

// PrimaryGraphLeafWindowPlanner retains the canonical leaf builder scratch for
// bounded streaming compaction. Plan returns the largest prefix fitting one
// leaf; payload is borrowed until the next call.
type PrimaryGraphLeafWindowPlanner struct {
	builder *UnifiedPrimaryLeafBuilder
	placed  bool
}

// PrimaryGraphLeafEmission is the bounded routing witness returned after one
// planned window has been encoded and staged. Key views borrow the input
// window; the caller must copy only the fences it retains across source reads.
type PrimaryGraphLeafEmission struct {
	Bucket   BucketID
	Ref      PageRef
	Count    int
	FirstKey []byte
	LastKey  []byte
}

func NewPrimaryGraphLeafWindowPlanner(
	placed bool,
	summaries []vibejson.CompiledPointer,
) (*PrimaryGraphLeafWindowPlanner, error) {
	b := NewUnifiedPrimaryLeafBuilder()
	if err := b.SetCompactPrimarySummaries(summaries); err != nil {
		return nil, err
	}
	return &PrimaryGraphLeafWindowPlanner{builder: b, placed: placed}, nil
}

func (p *PrimaryGraphLeafWindowPlanner) Plan(
	records []PrimaryGraphRecord,
	maxExtent int,
) (count, extent int, payload []byte, err error) {
	maxRows := CompactPrimaryStripeMaxRows
	if p != nil && p.placed {
		maxRows = CommonPrimaryLeafWideSlots
	}
	if p == nil || p.builder == nil || len(records) == 0 ||
		len(records) > maxRows || maxExtent < int(physicalPageQuantum) ||
		maxExtent > CommonPrimaryLeafMaxExtentBytes {
		return 0, 0, nil, fmt.Errorf("%w: incremental primary window", ErrInvalidWrite)
	}
	if err = prepareCompactPrimaryGraphStripe(records, p.placed, p.builder); err != nil {
		return
	}
	fits := func(n int) (int, bool, error) {
		built, buildErr := buildPreparedCompactPrimaryGraphStripePayload(records[:n], p.builder)
		if buildErr == ErrCommonPrimaryLeafFull {
			return maxExtent + int(physicalPageQuantum), false, nil
		}
		if buildErr != nil {
			return 0, false, buildErr
		}
		need := PageHeaderSize + len(built) + PageTrailerSize
		quantum := int(physicalPageQuantum)
		candidate := (need + quantum - 1) &^ (quantum - 1)
		return candidate, candidate <= maxExtent, nil
	}
	for lo, hi := 1, len(records); lo <= hi; {
		mid := (lo + hi) / 2
		candidate, ok, fitErr := fits(mid)
		if fitErr != nil {
			return 0, 0, nil, fitErr
		}
		if ok {
			count, extent = mid, candidate
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	if count == 0 {
		return 0, 0, nil, fmt.Errorf(
			"%w: incremental primary record exceeds extent", ErrInvalidWrite,
		)
	}
	// The last failed binary-search probe may have reused the builder's payload
	// backing. Rebuild the selected prefix so the returned borrowed view is exact.
	payload, err = buildPreparedCompactPrimaryGraphStripePayload(records[:count], p.builder)
	return
}

// Stage plans and emits the largest prefix of records as one immutable leaf.
// It never builds a dataset-sized plan or record copy: the planner scratch and
// one sink page are the only mutable memory on this path.
func (p *PrimaryGraphLeafWindowPlanner) Stage(
	sink PrimaryGraphBuildSink,
	tabletID uint32,
	localID uint16,
	records []PrimaryGraphRecord,
	maxExtent int,
	placements []PrimaryGraphPlacement,
) (PrimaryGraphLeafEmission, error) {
	if sink == nil || tabletID >= TabletLocalIdentityTabletCount ||
		uint32(localID) >= TabletLocalIdentityLocalCount ||
		(placements != nil && len(placements) < len(records)) {
		return PrimaryGraphLeafEmission{}, fmt.Errorf(
			"%w: incremental primary stage", ErrInvalidWrite,
		)
	}
	count, extent, payload, err := p.Plan(records, maxExtent)
	if err != nil {
		return PrimaryGraphLeafEmission{}, err
	}
	bucketValue, ok := MakeTabletLocalIdentityBucket(tabletID, uint32(localID))
	if !ok {
		return PrimaryGraphLeafEmission{}, fmt.Errorf(
			"%w: incremental primary identity", ErrInvalidWrite,
		)
	}
	bucket := BucketID(bucketValue)
	logicalID, ok := CommonPrimaryLeafLogicalID(bucket)
	if !ok {
		return PrimaryGraphLeafEmission{}, fmt.Errorf(
			"%w: incremental primary logical ID", ErrInvalidWrite,
		)
	}
	page, err := sink.AllocatePage(PagePrimaryLeaf, uint32(extent), logicalID)
	if err != nil {
		return PrimaryGraphLeafEmission{}, err
	}
	if _, err := encodeCompactPrimaryStripePayload(
		page.Bytes(),
		CommonPrimaryLeafHeader{
			StoreID: sink.StoreIdentity(), Generation: sink.BuildGeneration(),
			Bucket: bucket, PageSize: uint32(extent),
		},
		payload,
	); err != nil {
		return PrimaryGraphLeafEmission{}, err
	}
	if placements != nil {
		if !p.placed || count > CommonPrimaryLeafWideSlots {
			return PrimaryGraphLeafEmission{}, fmt.Errorf(
				"%w: incremental primary placements", ErrInvalidWrite,
			)
		}
		for row := range count {
			placements[row] = PrimaryGraphPlacement{
				Bucket: bucket, Slot: uint8(row),
			}
		}
	}
	if err := page.Stage(); err != nil {
		return PrimaryGraphLeafEmission{}, err
	}
	return PrimaryGraphLeafEmission{
		Bucket: bucket, Ref: page.Ref(), Count: count,
		FirstKey: records[0].keyBytes(), LastKey: records[count-1].keyBytes(),
	}, nil
}

// stagePrimaryTabletWindow folds at most one format tablet of leaf witnesses.
// Its allocations are bounded by TabletLocalIdentityLocalCount and the fixed
// sixteen anchor pages, independent of collection cardinality.
func stagePrimaryTabletWindow(
	sink PrimaryGraphBuildSink,
	tabletID uint32,
	leaves []primaryBuiltLeaf,
	previousTabletMax []byte,
) (primaryCatalogChild, error) {
	if sink == nil || tabletID >= TabletLocalIdentityTabletCount ||
		len(leaves) == 0 || len(leaves) > TabletLocalIdentityLocalCount {
		return primaryCatalogChild{}, fmt.Errorf(
			"%w: incremental primary tablet", ErrInvalidWrite,
		)
	}
	fences := make([][]byte, len(leaves))
	routerLeaves := make([]SegmentedTabletRouterLeaf, len(leaves))
	locatorEntries := make([]GlobalTabletCatalogLocatorEntry, len(leaves))
	for rank := range leaves {
		if rank != 0 {
			fence := make([]byte, len(leaves[rank].firstKey))
			var err error
			fences[rank], err = ShortestPrimaryFence(
				fence, leaves[rank-1].lastKey, leaves[rank].firstKey,
			)
			if err != nil {
				return primaryCatalogChild{}, err
			}
		}
		localID := uint16(rank)
		routerLeaves[rank] = SegmentedTabletRouterLeaf{
			LocalID: localID, Fence: fences[rank], Ref: leaves[rank].ref,
		}
		locatorEntries[rank] = GlobalTabletCatalogLocatorEntry{
			LocalID: localID,
			PageID:  uint8(rank / SegmentedTabletRouterRowsPerPage),
			RowSlot: uint8(rank % SegmentedTabletRouterRowsPerPage),
			State:   GlobalTabletCatalogLocatorLive,
		}
	}

	pageCount := (len(leaves) + SegmentedTabletRouterRowsPerPage - 1) /
		SegmentedTabletRouterRowsPerPage
	anchorPages := make([]PrimaryGraphBuildPage, pageCount)
	anchorRefs := make([]PageRef, pageCount)
	for pageID := range pageCount {
		logicalID, ok := GlobalTabletCatalogAnchorLogicalID(tabletID, uint8(pageID))
		if !ok {
			return primaryCatalogChild{}, fmt.Errorf(
				"%w: incremental primary anchor ID", ErrInvalidWrite,
			)
		}
		page, err := sink.AllocatePage(
			PagePrimaryAnchor, SegmentedTabletRouterAnchorPageBytes, logicalID,
		)
		if err != nil {
			return primaryCatalogChild{}, err
		}
		anchorPages[pageID], anchorRefs[pageID] = page, page.Ref()
	}
	locatorLogical, ok := GlobalTabletCatalogLocatorLogicalID(tabletID)
	if !ok {
		return primaryCatalogChild{}, fmt.Errorf(
			"%w: incremental primary locator ID", ErrInvalidWrite,
		)
	}
	locatorPage, err := sink.AllocatePage(
		PagePrimaryLocator, GlobalTabletCatalogLocatorBytes, locatorLogical,
	)
	if err != nil {
		return primaryCatalogChild{}, err
	}
	routeLogical, ok := GlobalTabletCatalogTabletRootLogicalID(tabletID)
	if !ok {
		return primaryCatalogChild{}, fmt.Errorf(
			"%w: incremental primary route ID", ErrInvalidWrite,
		)
	}
	routePage, err := sink.AllocatePage(
		PageTabletRoute, GlobalTabletCatalogTabletBytes, routeLogical,
	)
	if err != nil {
		return primaryCatalogChild{}, err
	}

	rawRoot := make([]byte, SegmentedTabletRouterRootBytes)
	rawLocator := make([]byte, SegmentedTabletRouterLocatorBytes)
	rawAnchors := make([]byte, pageCount*SegmentedTabletRouterAnchorPageBytes)
	header := SegmentedTabletRouterHeader{
		StoreID: sink.StoreIdentity(), TabletID: tabletID,
		Generation: sink.BuildGeneration(),
		AnchorKind: PagePrimaryAnchor, LeafKind: PagePrimaryLeaf,
	}
	if _, _, _, _, err := EncodeSegmentedTabletRouter(
		rawRoot, rawLocator, rawAnchors, header, anchorRefs, routerLeaves,
	); err != nil {
		return primaryCatalogChild{}, err
	}
	for pageID := range anchorPages {
		start := pageID * SegmentedTabletRouterAnchorPageBytes
		copy(
			anchorPages[pageID].Bytes(),
			rawAnchors[start:start+SegmentedTabletRouterAnchorPageBytes],
		)
		if err := anchorPages[pageID].Stage(); err != nil {
			return primaryCatalogChild{}, err
		}
	}

	bounds := primaryCatalogBounds(sink)
	if _, err := EncodeGlobalTabletCatalogLocator(
		locatorPage.Bytes(),
		PageHeader{
			StoreID: sink.StoreIdentity(), Generation: sink.BuildGeneration(),
			LogicalID: locatorLogical, PageSize: GlobalTabletCatalogLocatorBytes,
			PayloadLength: GlobalTabletCatalogLocatorHeader +
				globalTabletCatalogPackedBytes,
			Kind: PagePrimaryLocator,
		},
		bounds, tabletID, sink.BuildGeneration(), locatorEntries,
	); err != nil {
		return primaryCatalogChild{}, err
	}
	if err := locatorPage.Stage(); err != nil {
		return primaryCatalogChild{}, err
	}
	if _, err := EncodeGlobalTabletCatalogTabletRoot(
		routePage.Bytes(),
		PageHeader{
			StoreID: sink.StoreIdentity(), Generation: sink.BuildGeneration(),
			LogicalID: routeLogical, PageSize: GlobalTabletCatalogTabletBytes,
			PayloadLength: GlobalTabletCatalogRootHeader +
				SegmentedTabletRouterRootBytes,
			Kind: PageTabletRoute,
		},
		bounds, locatorPage.Ref(), rawRoot,
	); err != nil {
		return primaryCatalogChild{}, err
	}
	if err := routePage.Stage(); err != nil {
		return primaryCatalogChild{}, err
	}

	var floor []byte
	if tabletID != 0 {
		floor = make([]byte, len(leaves[0].firstKey))
		floor, err = ShortestPrimaryFence(
			floor, previousTabletMax, leaves[0].firstKey,
		)
		if err != nil {
			return primaryCatalogChild{}, err
		}
	}
	return primaryCatalogChild{
		floor: floor, id: tabletID, ref: routePage.Ref(),
	}, nil
}
