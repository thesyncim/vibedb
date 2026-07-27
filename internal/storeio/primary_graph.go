package storeio

import (
	"bytes"
	"errors"
	"fmt"
)

// PrimaryGraphRecord is one immutable key/value row supplied to
// BuildPrimaryGraph. Both slices are borrowed until the builder returns.
type PrimaryGraphRecord struct {
	Key   []byte
	Value []byte
}

// PrimaryRecord is the concise spelling used by store/durable bulk callers.
type PrimaryRecord = PrimaryGraphRecord

// PrimaryLeafClassPolicy controls the aligned class selected by a bottom-up
// build. Adaptive is the zero value: it first targets the 4 KiB narrow class
// and uses the 8 KiB wide class only when the same rows exceed that budget.
type PrimaryLeafClassPolicy uint8

const (
	PrimaryLeafAdaptive PrimaryLeafClassPolicy = iota
	PrimaryLeafNarrow
	PrimaryLeafWide
)

const (
	// PrimaryLeafDefault documents the zero-value production policy.
	PrimaryLeafDefault = PrimaryLeafAdaptive
	// PrimaryLeafNarrowOnly is an explicit synonym useful in option parsing.
	PrimaryLeafNarrowOnly = PrimaryLeafNarrow
	// PrimaryLeafWideOnly is an explicit synonym useful in option parsing.
	PrimaryLeafWideOnly = PrimaryLeafWide
)

type primaryLeafPlan struct {
	first   int
	last    int
	class   CommonPrimaryLeafClass
	records []CommonPrimaryLeafRecord
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
// non-empty values. Passing no policy selects PrimaryLeafAdaptive; at most one
// explicit policy is accepted.
//
// The function stages leaves, segmented tablet pages, and catalog levels in
// bottom-up order. It does not publish tx or modify a StateRoot; the returned
// PagePrimaryCatalog reference is suitable for StateRoot.PrimaryRoot.
func BuildPrimaryGraph(
	tx *WriteTransaction,
	records []PrimaryGraphRecord,
	policy ...PrimaryLeafClassPolicy,
) (PageRef, error) {
	if tx == nil || !tx.active || tx.options.PageSize != physicalPageQuantum ||
		tx.options.StoreID == ([16]byte{}) ||
		tx.options.Generation == 0 ||
		tx.nextID < PrimaryFirstDynamicLogicalID ||
		len(records) == 0 || len(policy) > 1 {
		return PageRef{}, fmt.Errorf("%w: primary graph transaction or input", ErrInvalidWrite)
	}
	selected := PrimaryLeafAdaptive
	if len(policy) == 1 {
		selected = policy[0]
	}
	if selected > PrimaryLeafWide {
		return PageRef{}, fmt.Errorf("%w: primary leaf policy", ErrInvalidWrite)
	}
	for at := range records {
		if len(records[at].Key) == 0 ||
			len(records[at].Key) > CommonPrimaryLeafMaxKeyBytes ||
			len(records[at].Value) == 0 ||
			at != 0 && bytes.Compare(records[at-1].Key, records[at].Key) >= 0 {
			return PageRef{}, fmt.Errorf("%w: non-canonical primary records", ErrInvalidWrite)
		}
	}

	plans, err := planPrimaryLeaves(tx, records, selected)
	if err != nil {
		return PageRef{}, err
	}
	if len(plans) > TabletLocalIdentityTabletCount*TabletLocalIdentityLocalCount {
		return PageRef{}, fmt.Errorf("%w: primary leaf namespace exhausted", ErrInvalidWrite)
	}
	built, err := buildPrimaryLeaves(tx, records, plans)
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
// BuildPrimaryGraph will stage for records and policy. Bulk callers use it to
// reserve one bounded commit without guessing from document count or average
// value width.
func PrimaryGraphPageCount(
	storeID [16]byte,
	records []PrimaryGraphRecord,
	policy ...PrimaryLeafClassPolicy,
) (int, error) {
	if storeID == ([16]byte{}) || len(records) == 0 || len(policy) > 1 {
		return 0, fmt.Errorf("%w: primary graph count input", ErrInvalidWrite)
	}
	selected := PrimaryLeafAdaptive
	if len(policy) == 1 {
		selected = policy[0]
	}
	if selected > PrimaryLeafWide {
		return 0, fmt.Errorf("%w: primary leaf policy", ErrInvalidWrite)
	}
	for at := range records {
		if len(records[at].Key) == 0 ||
			len(records[at].Key) > CommonPrimaryLeafMaxKeyBytes ||
			len(records[at].Value) == 0 ||
			at != 0 && bytes.Compare(records[at-1].Key, records[at].Key) >= 0 {
			return 0, fmt.Errorf("%w: non-canonical primary records", ErrInvalidWrite)
		}
	}
	layout, err := MutableStoreLayout(physicalPageQuantum)
	if err != nil {
		return 0, err
	}
	measurement := &WriteTransaction{
		options: WriteTransactionOptions{
			StoreID: storeID, Generation: 1,
			PageSize: physicalPageQuantum,
		},
		fileEnd: layout.DataStart,
		nextID:  PrimaryFirstDynamicLogicalID,
	}
	plans, err := planPrimaryLeaves(measurement, records, selected)
	if err != nil {
		return 0, err
	}
	leafCount := len(plans)
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

func planPrimaryLeaves(
	tx *WriteTransaction,
	records []PrimaryGraphRecord,
	policy PrimaryLeafClassPolicy,
) ([]primaryLeafPlan, error) {
	plans := make([]primaryLeafPlan, 0,
		(len(records)+CommonPrimaryLeafNarrowLive-1)/CommonPrimaryLeafNarrowLive)
	scratch := make([]byte, CommonPrimaryLeafWideBytes)
	bounds := CommonPrimaryLeafBounds{
		FileEnd:           tx.fileEnd,
		NextLogicalID:     tx.nextID,
		AllocationQuantum: tx.options.PageSize,
	}
	for first := 0; first < len(records); {
		count := min(CommonPrimaryLeafNarrowLive, len(records)-first)
		var chosen primaryLeafPlan
		for count > 0 {
			candidate := make([]CommonPrimaryLeafRecord, count)
			for at := range candidate {
				row := records[first+at]
				candidate[at] = CommonPrimaryLeafRecord{
					Key: row.Key, Value: CommonPrimaryLeafValue{Inline: row.Value},
				}
			}
			class, fitErr := fitPrimaryLeaf(
				scratch, tx, bounds, candidate, policy,
			)
			if fitErr == nil {
				chosen = primaryLeafPlan{
					first: first, last: first + count,
					class: class, records: candidate,
				}
				break
			}
			if !errors.Is(fitErr, ErrCommonPrimaryLeafFull) &&
				!errors.Is(fitErr, ErrCommonPrimaryLeafNeedsWide) {
				return nil, fitErr
			}
			count--
		}
		if chosen.last == 0 {
			return nil, fmt.Errorf("%w: primary record does not fit wide leaf", ErrInvalidWrite)
		}
		plans = append(plans, chosen)
		first = chosen.last
	}
	return plans, nil
}

func fitPrimaryLeaf(
	scratch []byte,
	tx *WriteTransaction,
	bounds CommonPrimaryLeafBounds,
	records []CommonPrimaryLeafRecord,
	policy PrimaryLeafClassPolicy,
) (CommonPrimaryLeafClass, error) {
	try := func(class CommonPrimaryLeafClass, pageSize uint32) error {
		if err := PlaceCommonPrimaryLeafRecords(
			class, tx.options.StoreID, records,
		); err != nil {
			return err
		}
		_, err := EncodeCommonPrimaryLeaf(
			scratch[:pageSize], class,
			CommonPrimaryLeafHeader{
				StoreID: tx.options.StoreID, Generation: tx.options.Generation,
				Bucket: 0, PageSize: pageSize,
			},
			tx.options.StoreID, records, bounds,
		)
		return err
	}
	switch policy {
	case PrimaryLeafNarrow:
		err := try(CommonPrimaryLeafNarrow, CommonPrimaryLeafNarrowBytes)
		return CommonPrimaryLeafNarrow, err
	case PrimaryLeafWide:
		err := try(CommonPrimaryLeafWide, CommonPrimaryLeafWideBytes)
		return CommonPrimaryLeafWide, err
	default:
		if err := try(
			CommonPrimaryLeafNarrow, CommonPrimaryLeafNarrowBytes,
		); err == nil {
			return CommonPrimaryLeafNarrow, nil
		} else if !errors.Is(err, ErrCommonPrimaryLeafNeedsWide) &&
			!errors.Is(err, ErrCommonPrimaryLeafFull) {
			return 0, err
		}
		err := try(CommonPrimaryLeafWide, CommonPrimaryLeafWideBytes)
		return CommonPrimaryLeafWide, err
	}
}

func buildPrimaryLeaves(
	tx *WriteTransaction,
	input []PrimaryGraphRecord,
	plans []primaryLeafPlan,
) ([]primaryBuiltLeaf, error) {
	built := make([]primaryBuiltLeaf, len(plans))
	bounds := CommonPrimaryLeafBounds{
		NextLogicalID:     tx.nextID,
		AllocationQuantum: tx.options.PageSize,
	}
	for rank := range plans {
		tabletID := uint32(rank / TabletLocalIdentityLocalCount)
		localID := uint32(rank % TabletLocalIdentityLocalCount)
		bucket, ok := MakeTabletLocalIdentityBucket(tabletID, localID)
		logicalID, logicalOK := CommonPrimaryLeafLogicalID(BucketID(bucket))
		if !ok || !logicalOK {
			return nil, fmt.Errorf("%w: primary leaf identity", ErrInvalidWrite)
		}
		pageSize := uint32(CommonPrimaryLeafNarrowBytes)
		if plans[rank].class == CommonPrimaryLeafWide {
			pageSize = CommonPrimaryLeafWideBytes
		}
		page, err := tx.Allocate(PagePrimaryLeaf, pageSize, logicalID)
		if err != nil {
			return nil, err
		}
		bounds.FileEnd = tx.fileEnd
		if _, err := EncodeCommonPrimaryLeaf(
			page.Bytes(), plans[rank].class,
			CommonPrimaryLeafHeader{
				StoreID: tx.options.StoreID, Generation: tx.options.Generation,
				Bucket: BucketID(bucket), PageSize: pageSize,
			},
			tx.options.StoreID, plans[rank].records, bounds,
		); err != nil {
			return nil, err
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
	for tabletAt := 0; tabletAt < tabletCount; tabletAt++ {
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
		for pageID := 0; pageID < pageCount; pageID++ {
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
	for pageID := 0; pageID < count; pageID++ {
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
