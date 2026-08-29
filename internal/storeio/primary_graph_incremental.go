package storeio

import (
	"bytes"
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

	ends, pageCount, err := PlanSegmentedTabletRouterAnchors(routerLeaves)
	if err != nil {
		return primaryCatalogChild{}, err
	}
	for pageID, first := 0, 0; pageID < pageCount; pageID++ {
		for rank := first; rank < ends[pageID]; rank++ {
			locatorEntries[rank].PageID, locatorEntries[rank].RowSlot = uint8(pageID), uint8(rank-first)
		}
		first = ends[pageID]
	}
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

func stagePrimaryCatalogWindow(
	sink PrimaryGraphBuildSink,
	level GlobalTabletCatalogNodeLevel,
	pageID uint32,
	children []primaryCatalogChild,
) (primaryCatalogChild, error) {
	if sink == nil || len(children) == 0 ||
		(level != GlobalTabletCatalogLeaf && level != GlobalTabletCatalogBranch) {
		return primaryCatalogChild{}, fmt.Errorf(
			"%w: incremental primary catalog window", ErrInvalidWrite,
		)
	}
	logicalID, ok := GlobalTabletCatalogCatalogLeafLogicalID(pageID)
	childKind := PageTabletRoute
	childLength := uint32(GlobalTabletCatalogTabletBytes)
	if level == GlobalTabletCatalogBranch {
		logicalID, ok = GlobalTabletCatalogCatalogBranchLogicalID(pageID)
		childKind = PagePrimaryCatalog
		childLength = GlobalTabletCatalogNodeBytes
	}
	if !ok {
		return primaryCatalogChild{}, fmt.Errorf(
			"%w: incremental primary catalog ID", ErrInvalidWrite,
		)
	}
	page, err := sink.AllocatePage(
		PagePrimaryCatalog, GlobalTabletCatalogNodeBytes, logicalID,
	)
	if err != nil {
		return primaryCatalogChild{}, err
	}
	if _, err := EncodeGlobalTabletCatalogNode(
		page.Bytes(),
		GlobalTabletCatalogNodeHeader{
			StoreID: sink.StoreIdentity(), Generation: sink.BuildGeneration(),
			LogicalID: logicalID, PageID: pageID, Level: level,
			Kind: PagePrimaryCatalog, ChildKind: childKind,
			ChildLength: childLength, Bounds: primaryCatalogBounds(sink),
		},
		primaryCatalogEntries(children),
	); err != nil {
		return primaryCatalogChild{}, err
	}
	if err := page.Stage(); err != nil {
		return primaryCatalogChild{}, err
	}
	return primaryCatalogChild{
		floor: children[0].floor, id: pageID, ref: page.Ref(),
	}, nil
}

// PrimaryGraphCatalogFolder incrementally folds tablet roots into the fixed
// leaf/optional-branch/resident-root catalog. Before a branch is necessary it
// retains at most rootFanout+1 leaf witnesses; afterwards it retains one leaf
// fanout and at most rootFanout branch witnesses.
type PrimaryGraphCatalogFolder struct {
	sink                   PrimaryGraphBuildSink
	leafFanout, rootFanout int
	tablets                []primaryCatalogChild
	leaves                 []primaryCatalogChild
	branches               []primaryCatalogChild
	tabletKeyArena         []byte
	lastTabletMax          [CommonPrimaryLeafMaxKeyBytes]byte
	lastTabletMaxLen       int
	leafPageID             uint32
	branchPageID           uint32
	tabletCount            uint32
	lastTabletID           uint32
	branching              bool
	finished               bool
}

// PrimaryGraphStreamBuilder emits a complete primary graph one borrowed source
// window at a time. StageWindow consumes every record before returning, so an
// online reconciler may immediately release the immutable source-leaf pin.
// Only one format tablet of compact first/last-key witnesses is retained.
type PrimaryGraphStreamBuilder struct {
	sink              PrimaryGraphBuildSink
	planner           *PrimaryGraphLeafWindowPlanner
	catalog           *PrimaryGraphCatalogFolder
	placed            bool
	tabletID          uint32
	localID           uint16
	leaves            []primaryBuiltLeaf
	keyArena          []byte
	lastKey           [CommonPrimaryLeafMaxKeyBytes]byte
	lastKeyLen        int
	priorTabletMax    [CommonPrimaryLeafMaxKeyBytes]byte
	priorTabletMaxLen int
	records           uint64
	finished          bool
}

func NewPrimaryGraphStreamBuilder(
	sink PrimaryGraphBuildSink,
	placed bool,
	summaries []vibejson.CompiledPointer,
) (*PrimaryGraphStreamBuilder, error) {
	planner, err := NewPrimaryGraphLeafWindowPlanner(placed, summaries)
	if err != nil {
		return nil, err
	}
	catalog, err := NewPrimaryGraphCatalogFolder(sink)
	if err != nil {
		return nil, err
	}
	return &PrimaryGraphStreamBuilder{
		sink: sink, planner: planner, catalog: catalog, placed: placed,
		leaves: make([]primaryBuiltLeaf, 0, TabletLocalIdentityLocalCount),
		keyArena: make([]byte, 0,
			2*TabletLocalIdentityLocalCount*CommonPrimaryLeafMaxKeyBytes),
	}, nil
}

// StageWindow synchronously consumes one ordered borrowed source window. A
// non-nil placements slice receives posting-stable positions for exact-index
// emission and must have exactly one entry per record.
func (b *PrimaryGraphStreamBuilder) StageWindow(
	records []PrimaryGraphRecord,
	placements []PrimaryGraphPlacement,
) error {
	if b == nil || b.finished || len(records) == 0 ||
		(placements != nil && len(placements) != len(records)) ||
		(placements != nil && !b.placed) {
		return fmt.Errorf("%w: primary stream window", ErrInvalidWrite)
	}
	for consumed := 0; consumed < len(records); {
		maxRows := CompactPrimaryStripeMaxRows
		if b.placed {
			maxRows = CommonPrimaryLeafWideSlots
		}
		end := min(consumed+maxRows, len(records))
		window := records[consumed:end]
		if b.lastKeyLen != 0 &&
			bytes.Compare(b.lastKey[:b.lastKeyLen], window[0].keyBytes()) >= 0 {
			return fmt.Errorf("%w: primary stream order", ErrInvalidWrite)
		}
		var windowPlacements []PrimaryGraphPlacement
		if placements != nil {
			windowPlacements = placements[consumed:end]
		}
		emission, err := b.planner.Stage(
			b.sink, b.tabletID, b.localID, window,
			CommonPrimaryLeafMaxExtentBytes, windowPlacements,
		)
		if err != nil {
			return err
		}
		first := b.copyTabletKey(emission.FirstKey)
		last := b.copyTabletKey(emission.LastKey)
		b.leaves = append(b.leaves, primaryBuiltLeaf{
			firstKey: first, lastKey: last, ref: emission.Ref,
		})
		copy(b.lastKey[:], emission.LastKey)
		b.lastKeyLen = len(emission.LastKey)
		b.records += uint64(emission.Count)
		consumed += emission.Count
		b.localID++
		if int(b.localID) == TabletLocalIdentityLocalCount {
			if err := b.flushTablet(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *PrimaryGraphStreamBuilder) copyTabletKey(key []byte) []byte {
	start := len(b.keyArena)
	b.keyArena = append(b.keyArena, key...)
	return b.keyArena[start:len(b.keyArena):len(b.keyArena)]
}

func (b *PrimaryGraphStreamBuilder) flushTablet() error {
	if len(b.leaves) == 0 {
		return nil
	}
	child, err := stagePrimaryTabletWindow(
		b.sink, b.tabletID, b.leaves,
		b.priorTabletMax[:b.priorTabletMaxLen],
	)
	if err != nil {
		return err
	}
	if err := b.catalog.AddTablet(child); err != nil {
		return err
	}
	last := b.leaves[len(b.leaves)-1].lastKey
	copy(b.priorTabletMax[:], last)
	b.priorTabletMaxLen = len(last)
	b.tabletID++
	b.localID = 0
	b.leaves = b.leaves[:0]
	b.keyArena = b.keyArena[:0]
	return nil
}

func (b *PrimaryGraphStreamBuilder) Finish() (PageRef, error) {
	if b == nil || b.finished || b.records == 0 {
		return PageRef{}, ErrBatchState
	}
	b.finished = true
	if err := b.flushTablet(); err != nil {
		return PageRef{}, err
	}
	return b.catalog.Finish()
}

func NewPrimaryGraphCatalogFolder(
	sink PrimaryGraphBuildSink,
) (*PrimaryGraphCatalogFolder, error) {
	leafFanout := GlobalTabletCatalogWorstCaseFanout(
		GlobalTabletCatalogNodeBytes, CommonPrimaryLeafMaxKeyBytes,
	)
	rootFanout := GlobalTabletCatalogWorstCaseFanout(
		GlobalTabletCatalogRootBytes, CommonPrimaryLeafMaxKeyBytes,
	)
	if sink == nil || leafFanout == 0 || rootFanout == 0 {
		return nil, fmt.Errorf("%w: incremental primary catalog", ErrInvalidWrite)
	}
	return &PrimaryGraphCatalogFolder{
		sink: sink, leafFanout: leafFanout, rootFanout: rootFanout,
		tablets:        make([]primaryCatalogChild, 0, leafFanout),
		leaves:         make([]primaryCatalogChild, 0, rootFanout+1),
		branches:       make([]primaryCatalogChild, 0, rootFanout),
		tabletKeyArena: make([]byte, 0, leafFanout*CommonPrimaryLeafMaxKeyBytes),
	}, nil
}

func (f *PrimaryGraphCatalogFolder) AddTablet(child primaryCatalogChild) error {
	if f == nil || f.finished || child.ref.Kind != PageTabletRoute ||
		f.tabletCount != 0 && child.id <= f.lastTabletID {
		return fmt.Errorf("%w: incremental primary tablet child", ErrInvalidWrite)
	}
	f.tablets = append(f.tablets, child)
	f.tabletCount++
	f.lastTabletID = child.id
	if len(f.tablets) == f.leafFanout {
		return f.flushCatalogLeaf()
	}
	return nil
}

// AddTabletRef is the collection-level reconciliation entry point. A caller
// may replace one tablet repeatedly in its persistent migration vector, then
// feed only the final witnessed refs into this folder at cutover.
func (f *PrimaryGraphCatalogFolder) AddTabletRef(
	tabletID uint32, firstKey, lastKey []byte, ref PageRef,
) error {
	if len(firstKey) == 0 || len(lastKey) == 0 ||
		len(firstKey) > CommonPrimaryLeafMaxKeyBytes ||
		len(lastKey) > CommonPrimaryLeafMaxKeyBytes ||
		bytes.Compare(firstKey, lastKey) > 0 {
		return fmt.Errorf("%w: incremental primary tablet fence", ErrInvalidWrite)
	}
	var floorScratch [CommonPrimaryLeafMaxKeyBytes]byte
	var floor []byte
	if tabletID != 0 {
		var err error
		floor, err = ShortestPrimaryFence(floorScratch[:0], f.lastTabletMax[:f.lastTabletMaxLen], firstKey)
		if err != nil {
			return err
		}
	}
	if len(floor) > cap(f.tabletKeyArena)-len(f.tabletKeyArena) {
		return fmt.Errorf("%w: incremental primary tablet key bound", ErrInvalidWrite)
	}
	at := len(f.tabletKeyArena)
	f.tabletKeyArena = append(f.tabletKeyArena, floor...)
	err := f.AddTablet(primaryCatalogChild{floor: f.tabletKeyArena[at:len(f.tabletKeyArena):len(f.tabletKeyArena)], id: tabletID, ref: ref})
	if err == nil {
		f.lastTabletMaxLen = copy(f.lastTabletMax[:], lastKey)
	}
	return err
}

func (f *PrimaryGraphCatalogFolder) flushCatalogLeaf() error {
	if len(f.tablets) == 0 {
		return nil
	}
	child, err := stagePrimaryCatalogWindow(
		f.sink, GlobalTabletCatalogLeaf, f.leafPageID, f.tablets,
	)
	if err != nil {
		return err
	}
	f.leafPageID++
	f.tablets = f.tablets[:0]
	f.tabletKeyArena = f.tabletKeyArena[:0]
	f.leaves = append(f.leaves, child)
	if !f.branching && len(f.leaves) > f.rootFanout {
		f.branching = true
	}
	if f.branching {
		for len(f.leaves) >= f.leafFanout {
			if err := f.flushCatalogBranch(f.leafFanout); err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *PrimaryGraphCatalogFolder) flushCatalogBranch(count int) error {
	if count < 1 || count > len(f.leaves) {
		return fmt.Errorf("%w: incremental primary branch window", ErrInvalidWrite)
	}
	child, err := stagePrimaryCatalogWindow(
		f.sink, GlobalTabletCatalogBranch, f.branchPageID, f.leaves[:count],
	)
	if err != nil {
		return err
	}
	f.branchPageID++
	f.branches = append(f.branches, child)
	copy(f.leaves, f.leaves[count:])
	f.leaves = f.leaves[:len(f.leaves)-count]
	if len(f.branches) > f.rootFanout {
		return fmt.Errorf("%w: incremental primary root capacity", ErrInvalidWrite)
	}
	return nil
}

func (f *PrimaryGraphCatalogFolder) Finish() (PageRef, error) {
	if f == nil || f.finished {
		return PageRef{}, ErrBatchState
	}
	f.finished = true
	if err := f.flushCatalogLeaf(); err != nil {
		return PageRef{}, err
	}
	rootChildren := f.leaves
	rootChildLevel := GlobalTabletCatalogLeaf
	if f.branching {
		if len(f.leaves) != 0 {
			if err := f.flushCatalogBranch(len(f.leaves)); err != nil {
				return PageRef{}, err
			}
		}
		rootChildren = f.branches
		rootChildLevel = GlobalTabletCatalogBranch
	}
	if len(rootChildren) == 0 || len(rootChildren) > f.rootFanout {
		return PageRef{}, fmt.Errorf(
			"%w: incremental primary root children", ErrInvalidWrite,
		)
	}
	page, err := f.sink.AllocatePage(
		PagePrimaryCatalog, GlobalTabletCatalogRootBytes,
		GlobalTabletCatalogRootLogicalID,
	)
	if err != nil {
		return PageRef{}, err
	}
	if _, err := EncodeGlobalTabletCatalogNode(
		page.Bytes(),
		GlobalTabletCatalogNodeHeader{
			StoreID: f.sink.StoreIdentity(), Generation: f.sink.BuildGeneration(),
			LogicalID: GlobalTabletCatalogRootLogicalID,
			Level:     GlobalTabletCatalogRoot, RootChildLevel: rootChildLevel,
			Kind: PagePrimaryCatalog, ChildKind: PagePrimaryCatalog,
			ChildLength: GlobalTabletCatalogNodeBytes,
			Bounds:      primaryCatalogBounds(f.sink),
		},
		primaryCatalogEntries(rootChildren),
	); err != nil {
		return PageRef{}, err
	}
	if err := page.Stage(); err != nil {
		return PageRef{}, err
	}
	return page.Ref(), nil
}
