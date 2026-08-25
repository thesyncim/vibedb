package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"unsafe"
)

// The global tablet catalog closes the two durable routing links:
//
//   - an exact lexical catalog whose leaves name TabletRoot PageRefs; and
//   - independently cacheable tablet roots and 14-bit local-ID locators.
//
// The normal catalog has two levels. A rare third level is available for
// adversarial 256-byte separators. With the conservative no-prefix-sharing
// bound, an 8 KiB node admits at least 28 children and the 64 KiB root admits
// at least 235. Three levels therefore cover 235*28*28 = 184,240 tablets,
// above the 174,000-tablet 100-billion-document design bound. The exact writer
// still measures every real image and fails closed on overflow.
//
// Every view borrows one admitted image. There is no Go object per tablet:
// the resident object is one catalog root, while catalog branches/leaves,
// tablet roots, locators, and anchor pages are ordinary cache frames.
const (
	GlobalTabletCatalogRootBytes              = 64 << 10
	GlobalTabletCatalogNodeBytes              = 8 << 10
	GlobalTabletCatalogTabletBytes            = 8 << 10
	GlobalTabletCatalogLocatorBytes           = 8 << 10
	GlobalTabletCatalogHandleBytes            = 12
	GlobalTabletCatalogNodePayloadHeaderBytes = 32
	GlobalTabletCatalogLocatorHeader          = 32
	GlobalTabletCatalogRootHeader             = 64

	GlobalTabletCatalogMaxLeafPages   = 1 << 13
	GlobalTabletCatalogMaxBranchPages = 1 << 9

	GlobalTabletCatalogLeafLogicalIDBase        = PrimaryLeafLogicalIDBase
	GlobalTabletCatalogLeafLogicalIDLimit       = PrimaryLeafLogicalIDLimit
	GlobalTabletCatalogAnchorLogicalIDBase      = PrimaryAnchorLogicalIDBase
	GlobalTabletCatalogAnchorLogicalIDLimit     = PrimaryAnchorLogicalIDLimit
	GlobalTabletCatalogTabletRootLogicalIDBase  = PrimaryTabletRootLogicalIDBase
	GlobalTabletCatalogTabletRootLogicalIDLimit = PrimaryTabletRootLogicalIDLimit
	GlobalTabletCatalogLocatorLogicalIDBase     = PrimaryLocatorLogicalIDBase
	GlobalTabletCatalogLocatorLogicalIDLimit    = PrimaryLocatorLogicalIDLimit
	GlobalTabletCatalogLeafPageLogicalIDBase    = PrimaryCatalogLeafLogicalIDBase
	GlobalTabletCatalogLeafPageLogicalIDLimit   = PrimaryCatalogLeafLogicalIDLimit
	GlobalTabletCatalogBranchPageLogicalIDBase  = PrimaryCatalogBranchLogicalIDBase
	GlobalTabletCatalogBranchPageLogicalIDLimit = PrimaryCatalogBranchLogicalIDLimit
	GlobalTabletCatalogRootLogicalID            = PrimaryCatalogRootLogicalID
	GlobalTabletCatalogFirstDynamicLogicalID    = PrimaryFirstDynamicLogicalID

	globalTabletCatalogNodeVersion    = DevelopmentFormatVersion
	globalTabletCatalogLocatorVersion = DevelopmentFormatVersion
	globalTabletCatalogRootVersion    = DevelopmentFormatVersion
	globalTabletCatalogPackedBits     = 14
	globalTabletCatalogPackedBytes    = TabletLocalIdentityLocalCount * globalTabletCatalogPackedBits / 8
)

var (
	// ErrGlobalTabletCatalogCorrupt reports that a catalog page failed checksum
	// or structural admission and must not be routed against.
	ErrGlobalTabletCatalogCorrupt = errors.New(
		"vibedb: corrupt global tablet catalog page",
	)
	// ErrGlobalTabletCatalogNoSpace reports that a node cannot admit another
	// child; the builder must add a level or split.
	ErrGlobalTabletCatalogNoSpace = errors.New(
		"vibedb: global tablet catalog page has no space",
	)
)

// GlobalTabletCatalogNodeLevel identifies the exact child identity derived
// from a node row. A leaf names tablet roots; a branch names leaf pages; a root
// names either leaves (normal two-level form) or branches (rare three-level
// form).
type GlobalTabletCatalogNodeLevel uint8

const (
	GlobalTabletCatalogLeaf GlobalTabletCatalogNodeLevel = iota
	GlobalTabletCatalogBranch
	GlobalTabletCatalogRoot
)

// GlobalTabletCatalogNodeHeader is construction metadata. Page Kind is the
// outer common-page discriminator. Child Kind and fixed ChildLength make every
// packed child handle recoverable without storing duplicate logical IDs.
type GlobalTabletCatalogNodeHeader struct {
	StoreID    [16]byte
	Generation uint64
	LogicalID  uint64
	PageID     uint32
	Level      GlobalTabletCatalogNodeLevel
	Bounds     GlobalTabletCatalogBounds
	// RootChildLevel is Leaf in the normal two-level tree and Branch in the
	// rare adversarial three-level tree. It is ignored below the root.
	RootChildLevel GlobalTabletCatalogNodeLevel
	Kind           PageKind
	ChildKind      PageKind
	ChildLength    uint32
}

// GlobalTabletCatalogNodeEntry is one exact lexical floor and child. Floor
// zero must be empty. Subsequent floors are shortest exact separators.
type GlobalTabletCatalogNodeEntry struct {
	Floor []byte
	ID    uint32
	Ref   PageRef
}

// GlobalTabletCatalogNodeView borrows one common page, its exact front-coded
// floor map, and its packed physical handles.
type GlobalTabletCatalogNodeView struct {
	image       []byte
	payload     []byte
	handles     []byte
	heads       []byte
	floors      TabletAnchorMapView
	header      PageHeader
	level       GlobalTabletCatalogNodeLevel
	childLevel  GlobalTabletCatalogNodeLevel
	pageID      uint32
	childKind   PageKind
	childLength uint32
	headBytes   uint8
	bounds      GlobalTabletCatalogBounds
}

// GlobalTabletCatalogNodeRoute is the next cache acquisition. ID is a
// TabletID at a leaf, a leaf-page ID at a branch, or the selected child ID at
// a root.
type GlobalTabletCatalogNodeRoute struct {
	ID      uint32
	Ordinal uint16
	Ref     PageRef
}

// GlobalTabletCatalogNodeHandleRewrite is one child replacement in a batched
// immutable node rewrite. IDs have the same level-dependent meaning as
// GlobalTabletCatalogNodeRoute.ID.
type GlobalTabletCatalogNodeHandleRewrite struct {
	ID  uint32
	Ref PageRef
}

// GlobalTabletCatalogNodeCursor walks the tablet entries of one catalog node in
// lexical fence order, layering a TabletAnchorMapCursor over the node's borrowed
// view. It never follows child PageRefs.
type GlobalTabletCatalogNodeCursor struct {
	node   *GlobalTabletCatalogNodeView
	cursor TabletAnchorMapCursor
}

// GlobalTabletCatalogLocatorState occupies the high two bits of each packed
// 14-bit locator. The low twelve bits are pageID:4,rowSlot:8. Retired preserves
// the last location for validation/debugging; reuse remains conservatively
// gated by the selecting snapshot generation.
type GlobalTabletCatalogLocatorState uint8

const (
	GlobalTabletCatalogLocatorEmpty GlobalTabletCatalogLocatorState = iota
	GlobalTabletCatalogLocatorLive
	GlobalTabletCatalogLocatorRetired
	globalTabletCatalogLocatorReserved
)

// GlobalTabletCatalogLocatorEntry is one locator row: a LocalID and the anchor
// page and row slot it currently resolves to, plus the slot's lifecycle State.
type GlobalTabletCatalogLocatorEntry struct {
	LocalID uint16
	PageID  uint8
	RowSlot uint8
	State   GlobalTabletCatalogLocatorState
}

// GlobalTabletCatalogLocatorView is a borrowed, allocation-free read view over
// one admitted locator image. It resolves a tablet's LocalIDs to their current
// anchor page and slot and aliases the page bytes for the lease's lifetime.
type GlobalTabletCatalogLocatorView struct {
	image      []byte
	packed     []byte
	header     PageHeader
	ref        PageRef
	tabletID   uint32
	live       uint16
	retired    uint16
	reuseFloor uint64
	bounds     GlobalTabletCatalogBounds
}

// GlobalTabletCatalogTabletRootView is an independently admitted 8 KiB
// wrapper around the proven segmented-root codec. The wrapper's common-page
// checksum binds a complete, discoverable locator PageRef to that root.
type GlobalTabletCatalogTabletRootView struct {
	image   []byte
	payload []byte
	header  PageHeader
	inner   globalTabletCatalogSegmentedRootView
	locator PageRef
	bounds  GlobalTabletCatalogBounds
}

// This compact root-only view deliberately excludes the full segmented router's
// [16]anchor-view array. A cached tablet root therefore retains borrowed byte
// slices and scalars only; selected anchor views exist only for selected cache
// frames and cannot create a per-tablet heap/object graph.
type globalTabletCatalogSegmentedRootView struct {
	root        []byte
	rootRefs    []byte
	rootRanks   []byte
	rootOffsets []byte
	rootKeys    []byte
	storeID     [16]byte
	tabletID    uint32
	generation  uint64
	pageCount   uint8
	anchorKind  PageKind
	leafKind    PageKind
}

// GlobalTabletCatalogAnchorRoute pairs an anchor page's stable page ID within
// its tablet with that page's current physical PageRef.
type GlobalTabletCatalogAnchorRoute struct {
	PageID uint8
	Ref    PageRef
}

// GlobalTabletCatalogAnchorHandleRewrite is one stable anchor-row leaf
// replacement. A checkpoint may combine several rows from one anchor page
// into one immutable after-image.
type GlobalTabletCatalogAnchorHandleRewrite struct {
	Route SegmentedTabletRouterRoute
	Ref   PageRef
}

// GlobalTabletCatalogAnchorRefRewrite is one anchor-page replacement in a
// tablet root.
type GlobalTabletCatalogAnchorRefRewrite struct {
	PageID uint8
	Ref    PageRef
}

// GlobalTabletCatalogAnchorView is a borrowed view binding one tablet anchor
// page to its locator page and tablet identity, so a lexical route and its
// LocalID resolution share one admitted image.
type GlobalTabletCatalogAnchorView struct {
	page     segmentedTabletRouterAnchorView
	ref      PageRef
	locator  PageRef
	tabletID uint32
}

// GlobalTabletCatalogCatalogBounds is the computed geometry of a catalog tree
// for a given tablet count and worst-case fence width: fanouts, page and level
// counts, and the COW, disk, and resident byte budgets. The builder uses it to
// size the catalog and fail closed before it exceeds MaximumTablets.
type GlobalTabletCatalogCatalogBounds struct {
	Tablets        uint64
	MaxFenceBytes  int
	LeafFanout     int
	RootFanout     int
	LeafPages      uint64
	BranchPages    uint64
	Levels         int
	PointPages     int
	COWBytes       uint64
	DiskBytes      uint64
	ResidentBytes  uint64
	MaximumTablets uint64
}

// GlobalTabletCatalogBounds is snapshot-owned admission context. StoreID
// lets common-page admission reject checksum-valid cross-Store grafts,
// SelectedRootGeneration rejects references born after the selecting snapshot,
// FileEnd rejects out-of-file references, and NextLogicalID bounds the
// allocated logical namespace. PageRef itself has no StoreID, so raw
// non-common-page acquisition must retain this Store context until admission.
type GlobalTabletCatalogBounds struct {
	StoreID                [16]byte
	SelectedRootGeneration uint64
	FileEnd                uint64
	NextLogicalID          uint64
}

func GlobalTabletCatalogAnchorLogicalID(tabletID uint32, pageID uint8) (uint64, bool) {
	if tabletID >= TabletLocalIdentityTabletCount ||
		pageID >= SegmentedTabletRouterMaxPages {
		return 0, false
	}
	return GlobalTabletCatalogAnchorLogicalIDBase +
		uint64(tabletID)*SegmentedTabletRouterMaxPages + uint64(pageID), true
}

func GlobalTabletCatalogTabletRootLogicalID(tabletID uint32) (uint64, bool) {
	if tabletID >= TabletLocalIdentityTabletCount {
		return 0, false
	}
	return GlobalTabletCatalogTabletRootLogicalIDBase + uint64(tabletID), true
}

func GlobalTabletCatalogLocatorLogicalID(tabletID uint32) (uint64, bool) {
	if tabletID >= TabletLocalIdentityTabletCount {
		return 0, false
	}
	return GlobalTabletCatalogLocatorLogicalIDBase + uint64(tabletID), true
}

func GlobalTabletCatalogCatalogLeafLogicalID(pageID uint32) (uint64, bool) {
	if pageID >= GlobalTabletCatalogMaxLeafPages {
		return 0, false
	}
	return GlobalTabletCatalogLeafPageLogicalIDBase + uint64(pageID), true
}

func GlobalTabletCatalogCatalogBranchLogicalID(pageID uint32) (uint64, bool) {
	if pageID >= GlobalTabletCatalogMaxBranchPages {
		return 0, false
	}
	return GlobalTabletCatalogBranchPageLogicalIDBase + uint64(pageID), true
}

// GlobalTabletCatalogWorstCaseFanout returns a universal lower bound for
// valid floors no longer than maxFenceBytes. It charges two front-code bytes,
// the complete fence, offset/bucket metadata, one packed handle, both schema
// envelopes, and checks the real anchor-map geometry at every candidate count.
func GlobalTabletCatalogWorstCaseFanout(pageBytes, maxFenceBytes int) int {
	if pageBytes < GlobalTabletCatalogNodePayloadHeaderBytes+PageHeaderSize+PageTrailerSize ||
		maxFenceBytes <= 0 || maxFenceBytes > int(^uint16(0)) {
		return 0
	}
	for count := 1; count <= TabletAnchorMapMaxFences; count++ {
		fences := count - 1
		// A zero common prefix and no restart sharing is a valid upper bound
		// on the exact codec, independent of the corpus.
		keyBytes := fences * (2 + maxFenceBytes)
		if keyBytes > int(^uint16(0)) {
			return count - 1
		}
		mapBytes := tabletAnchorMapImageBytes(fences, 0, keyBytes)
		payload := GlobalTabletCatalogNodePayloadHeaderBytes + mapBytes +
			count*GlobalTabletCatalogHandleBytes
		if payload > pageBytes-PageHeaderSize-PageTrailerSize {
			return count - 1
		}
	}
	return TabletAnchorMapMaxFences
}

// GlobalTabletCatalogCatalogGeometry computes the guaranteed catalog shape
// at a fence-length bound. PointPages includes the resident root so callers can
// also read the number of cache misses as PointPages-1.
func GlobalTabletCatalogCatalogGeometry(
	tablets uint64, maxFenceBytes int,
) (GlobalTabletCatalogCatalogBounds, bool) {
	leafFanout := GlobalTabletCatalogWorstCaseFanout(
		GlobalTabletCatalogNodeBytes, maxFenceBytes,
	)
	rootFanout := GlobalTabletCatalogWorstCaseFanout(
		GlobalTabletCatalogRootBytes, maxFenceBytes,
	)
	if tablets == 0 || leafFanout == 0 || rootFanout == 0 {
		return GlobalTabletCatalogCatalogBounds{}, false
	}
	leafPages := (tablets + uint64(leafFanout) - 1) / uint64(leafFanout)
	bounds := GlobalTabletCatalogCatalogBounds{
		Tablets: tablets, MaxFenceBytes: maxFenceBytes,
		LeafFanout: leafFanout, RootFanout: rootFanout,
		LeafPages: leafPages, Levels: 2, PointPages: 2,
		COWBytes: GlobalTabletCatalogRootBytes +
			GlobalTabletCatalogNodeBytes,
		ResidentBytes: GlobalTabletCatalogRootBytes,
		MaximumTablets: uint64(rootFanout) * uint64(leafFanout) *
			uint64(leafFanout),
	}
	if leafPages > uint64(rootFanout) {
		bounds.BranchPages =
			(leafPages + uint64(leafFanout) - 1) / uint64(leafFanout)
		bounds.Levels = 3
		bounds.PointPages = 3
		bounds.COWBytes += GlobalTabletCatalogNodeBytes
		if bounds.BranchPages > uint64(rootFanout) {
			return GlobalTabletCatalogCatalogBounds{}, false
		}
	}
	bounds.DiskBytes = GlobalTabletCatalogRootBytes +
		(bounds.LeafPages+bounds.BranchPages)*GlobalTabletCatalogNodeBytes
	return bounds, true
}

func EncodeGlobalTabletCatalogNode(
	dst []byte,
	header GlobalTabletCatalogNodeHeader,
	entries []GlobalTabletCatalogNodeEntry,
) ([]byte, error) {
	pageBytes, err := globalTabletCatalogNodePageBytes(header.Level)
	if err != nil || len(dst) < pageBytes || len(entries) == 0 ||
		len(entries[0].Floor) != 0 ||
		header.StoreID == ([16]byte{}) ||
		header.Generation == 0 || header.Generation >= uint64(1)<<48 ||
		header.StoreID != header.Bounds.StoreID ||
		header.Generation > header.Bounds.SelectedRootGeneration ||
		!validPageKind(header.Kind) || !validPageKind(header.ChildKind) ||
		header.LogicalID == 0 || !header.Bounds.valid() {
		return nil, fmt.Errorf("%w: catalog node identity or geometry", ErrInvalidWrite)
	}
	childLevel, childLevelOK := globalTabletCatalogChildLevel(
		header.Level, header.RootChildLevel,
	)
	childKind := PagePrimaryCatalog
	if header.Level == GlobalTabletCatalogLeaf {
		childKind = PageTabletRoute
	}
	wantLogicalID, wantChildLength, ok := globalTabletCatalogNodeIdentity(
		header.Level, header.PageID,
	)
	if !ok || !childLevelOK || header.LogicalID != wantLogicalID ||
		header.Kind != PagePrimaryCatalog || header.ChildKind != childKind ||
		header.ChildLength != wantChildLength {
		return nil, fmt.Errorf("%w: catalog node namespace", ErrInvalidWrite)
	}
	anchors := make([]TabletAnchorMapAnchor, len(entries)-1)
	for at, entry := range entries {
		bucket, ok := globalTabletCatalogNodeBucket(
			header.Level, childLevel, entry.ID,
		)
		if !ok || at != 0 && len(entry.Floor) == 0 ||
			at != 0 && bytes.Compare(entries[at-1].Floor, entry.Floor) >= 0 {
			return nil, fmt.Errorf("%w: catalog node floor or child ID", ErrInvalidWrite)
		}
		for prior := range at {
			if entries[prior].ID == entry.ID {
				return nil, fmt.Errorf("%w: duplicate catalog child ID", ErrInvalidWrite)
			}
		}
		wantChild, ok := globalTabletCatalogChildLogicalID(
			header.Level, childLevel, entry.ID,
		)
		if !ok || globalTabletCatalogValidatePackedRef(
			entry.Ref, wantChild, header.ChildKind, header.ChildLength,
			header.Generation, header.Bounds,
		) != nil {
			return nil, fmt.Errorf("%w: catalog child reference", ErrInvalidWrite)
		}
		if at != 0 {
			anchors[at-1] = TabletAnchorMapAnchor{
				Fence: entry.Floor, Bucket: bucket,
			}
		}
	}
	common, _, keyBytes, err := tabletAnchorMapMeasure(anchors)
	if err != nil {
		return nil, err
	}
	mapBytes := tabletAnchorMapImageBytes(len(anchors), common, keyBytes)
	basePayloadBytes := GlobalTabletCatalogNodePayloadHeaderBytes + mapBytes +
		len(entries)*GlobalTabletCatalogHandleBytes
	headBytes := globalTabletCatalogChooseHeadBytes(
		entries, pageBytes-PageHeaderSize-PageTrailerSize-basePayloadBytes,
	)
	payloadBytes := basePayloadBytes + len(entries)*headBytes
	if payloadBytes > pageBytes-PageHeaderSize-PageTrailerSize {
		return nil, ErrGlobalTabletCatalogNoSpace
	}
	payload, err := InitPage(dst[:pageBytes], PageHeader{
		StoreID: header.StoreID, Generation: header.Generation,
		LogicalID: header.LogicalID, PageSize: uint32(pageBytes),
		PayloadLength: uint32(payloadBytes), Kind: header.Kind,
	})
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(payload[0:4], globalTabletCatalogNodeVersion)
	payload[4] = byte(header.Level)
	payload[5] = byte(header.ChildKind)
	payload[6] = byte(childLevel)
	binary.LittleEndian.PutUint32(payload[8:12], header.PageID)
	binary.LittleEndian.PutUint32(payload[12:16], uint32(mapBytes))
	binary.LittleEndian.PutUint32(payload[16:20], header.ChildLength)
	binary.LittleEndian.PutUint16(payload[20:22], GlobalTabletCatalogHandleBytes)
	binary.LittleEndian.PutUint16(payload[22:24], uint16(len(entries)))
	payload[24] = byte(headBytes)
	firstBucket, _ := globalTabletCatalogNodeBucket(
		header.Level, childLevel, entries[0].ID,
	)
	if _, err := EncodeTabletAnchorMap(
		payload[GlobalTabletCatalogNodePayloadHeaderBytes:GlobalTabletCatalogNodePayloadHeaderBytes+mapBytes],
		TabletAnchorMapHeader{
			TabletID: header.LogicalID, Generation: header.Generation,
		},
		firstBucket, anchors,
	); err != nil {
		return nil, err
	}
	handles := payload[GlobalTabletCatalogNodePayloadHeaderBytes+mapBytes : basePayloadBytes]
	for at, entry := range entries {
		globalTabletCatalogEncodePackedRef(
			handles[at*GlobalTabletCatalogHandleBytes:], entry.Ref,
		)
	}
	heads := payload[basePayloadBytes:]
	for at := 1; at < len(entries) && headBytes != 0; at++ {
		copy(heads[at*headBytes:], entries[at].Floor[:headBytes])
	}
	if _, err := sealInitializedPage(dst[:pageBytes]); err != nil {
		return nil, err
	}
	return dst[:pageBytes], nil
}

func OpenGlobalTabletCatalogNode(
	src []byte, expected PageRef, bounds GlobalTabletCatalogBounds,
) (GlobalTabletCatalogNodeView, error) {
	var view GlobalTabletCatalogNodeView
	header, payload, err := OpenPage(src)
	if err != nil || !bounds.valid() ||
		!globalTabletCatalogHeaderMatchesRef(header, expected, bounds) ||
		len(payload) < GlobalTabletCatalogNodePayloadHeaderBytes ||
		binary.LittleEndian.Uint32(payload[0:4]) != globalTabletCatalogNodeVersion ||
		payload[4] > byte(GlobalTabletCatalogRoot) ||
		payload[6] > byte(GlobalTabletCatalogBranch) ||
		!validPageKind(PageKind(payload[5])) ||
		binary.LittleEndian.Uint16(payload[20:22]) != GlobalTabletCatalogHandleBytes ||
		payload[7] != 0 ||
		payload[24] != 0 && payload[24] != 1 &&
			payload[24] != 2 && payload[24] != 4 ||
		!allZero(payload[25:GlobalTabletCatalogNodePayloadHeaderBytes]) {
		return view, globalTabletCatalogCorrupt("node header")
	}
	level := GlobalTabletCatalogNodeLevel(payload[4])
	childLevel, childLevelOK := globalTabletCatalogChildLevel(
		level, GlobalTabletCatalogNodeLevel(payload[6]),
	)
	childKind := PagePrimaryCatalog
	if level == GlobalTabletCatalogLeaf {
		childKind = PageTabletRoute
	}
	pageID := binary.LittleEndian.Uint32(payload[8:12])
	wantLogicalID, childLength, ok := globalTabletCatalogNodeIdentity(level, pageID)
	count := int(binary.LittleEndian.Uint16(payload[22:24]))
	headBytes := int(payload[24])
	mapBytes := int(binary.LittleEndian.Uint32(payload[12:16]))
	if !ok || !childLevelOK || header.Kind != PagePrimaryCatalog ||
		PageKind(payload[5]) != childKind ||
		header.LogicalID != wantLogicalID || count == 0 ||
		childLength != binary.LittleEndian.Uint32(payload[16:20]) ||
		mapBytes < TabletAnchorMapHeaderSize+TabletAnchorMapTrailerSize ||
		GlobalTabletCatalogNodePayloadHeaderBytes+mapBytes+
			count*(GlobalTabletCatalogHandleBytes+headBytes) != len(payload) {
		return GlobalTabletCatalogNodeView{},
			globalTabletCatalogCorrupt("node identity or sections")
	}
	floors, err := OpenTabletAnchorMap(
		payload[GlobalTabletCatalogNodePayloadHeaderBytes : GlobalTabletCatalogNodePayloadHeaderBytes+mapBytes],
	)
	if err != nil || floors.Header().TabletID != header.LogicalID ||
		floors.Header().Generation != header.Generation ||
		floors.BucketCount() != count {
		return GlobalTabletCatalogNodeView{},
			globalTabletCatalogCorrupt("node floor map")
	}
	handleAt := GlobalTabletCatalogNodePayloadHeaderBytes + mapBytes
	headAt := handleAt + count*GlobalTabletCatalogHandleBytes
	view = GlobalTabletCatalogNodeView{
		image: src[:header.PageSize], payload: payload,
		handles: payload[handleAt:headAt],
		heads:   payload[headAt:],
		floors:  floors, header: header, level: level, pageID: pageID,
		childLevel: childLevel, childKind: PageKind(payload[5]),
		childLength: childLength, headBytes: uint8(headBytes), bounds: bounds,
	}
	if headBytes != 0 && !allZero(view.heads[:headBytes]) {
		return GlobalTabletCatalogNodeView{},
			globalTabletCatalogCorrupt("node first head")
	}
	for ordinal := range count {
		bucket := floors.bucketAt(ordinal)
		id, ok := globalTabletCatalogNodeID(level, childLevel, bucket)
		wantChild, childOK := globalTabletCatalogChildLogicalID(
			level, childLevel, id,
		)
		ref, refOK := view.refAt(ordinal, id)
		if !ok || !childOK || !refOK ||
			ref.LogicalID != wantChild {
			return GlobalTabletCatalogNodeView{},
				globalTabletCatalogCorrupt("node child binding")
		}
		for prior := 0; prior < ordinal; prior++ {
			if floors.bucketAt(prior) == bucket {
				return GlobalTabletCatalogNodeView{},
					globalTabletCatalogCorrupt("duplicate node child ID")
			}
		}
		if ordinal != 0 && headBytes != 0 {
			common, restart, suffix, floorOK := floors.FenceAt(ordinal - 1)
			if !floorOK || len(common)+len(restart)+len(suffix) < headBytes ||
				!globalTabletCatalogHeadMatches(
					view.heads[ordinal*headBytes:], headBytes,
					common, restart, suffix,
				) {
				return GlobalTabletCatalogNodeView{},
					globalTabletCatalogCorrupt("node head accelerator")
			}
		}
	}
	return view, nil
}

// AdmittedGlobalTabletCatalogNode reconstructs a node whose common envelope,
// exact floor map, child handles, identities, and bounds were already checked
// by PageCache admission. Calling it on arbitrary bytes is invalid.
func AdmittedGlobalTabletCatalogNode(
	src []byte, bounds GlobalTabletCatalogBounds,
) GlobalTabletCatalogNodeView {
	header, _ := decodePageHeader(src)
	payloadEnd := PageHeaderSize + int(header.PayloadLength)
	payload := src[PageHeaderSize:payloadEnd:payloadEnd]
	level := GlobalTabletCatalogNodeLevel(payload[4])
	childLevel, _ := globalTabletCatalogChildLevel(
		level, GlobalTabletCatalogNodeLevel(payload[6]),
	)
	mapBytes := int(binary.LittleEndian.Uint32(payload[12:16]))
	count := int(binary.LittleEndian.Uint16(payload[22:24]))
	headBytes := int(payload[24])
	handleAt := GlobalTabletCatalogNodePayloadHeaderBytes + mapBytes
	headAt := handleAt + count*GlobalTabletCatalogHandleBytes
	return GlobalTabletCatalogNodeView{
		image: src[:header.PageSize], payload: payload,
		handles: payload[handleAt:headAt], heads: payload[headAt:],
		floors: AdmittedTabletAnchorMap(
			payload[GlobalTabletCatalogNodePayloadHeaderBytes:handleAt],
		),
		header: header, level: level, childLevel: childLevel,
		pageID:      binary.LittleEndian.Uint32(payload[8:12]),
		childKind:   PageKind(payload[5]),
		childLength: binary.LittleEndian.Uint32(payload[16:20]),
		headBytes:   uint8(headBytes),
		bounds:      bounds,
	}
}

func (v *GlobalTabletCatalogNodeView) Level() GlobalTabletCatalogNodeLevel {
	if v == nil {
		return GlobalTabletCatalogLeaf
	}
	return v.level
}

// ChildLevel reports whether this node routes to tablets, catalog leaves, or
// catalog branches. Leaf nodes report GlobalTabletCatalogLeaf because their
// children are tablet routes rather than another catalog node level.
func (v *GlobalTabletCatalogNodeView) ChildLevel() GlobalTabletCatalogNodeLevel {
	if v == nil {
		return GlobalTabletCatalogLeaf
	}
	return v.childLevel
}

// PageID returns the stable ID encoded by this catalog node.
func (v *GlobalTabletCatalogNodeView) PageID() uint32 {
	if v == nil {
		return 0
	}
	return v.pageID
}

// Header returns the admitted common-page identity.
func (v *GlobalTabletCatalogNodeView) Header() PageHeader {
	if v == nil {
		return PageHeader{}
	}
	return v.header
}

func (v *GlobalTabletCatalogNodeView) Count() int {
	if v == nil {
		return 0
	}
	return v.floors.BucketCount()
}

func (v *GlobalTabletCatalogNodeView) upperBound(key []byte) int {
	if v == nil {
		return 0
	}
	if v.headBytes == 0 || len(key) < int(v.headBytes) {
		return v.floors.upperBound(key)
	}
	headBytes := int(v.headBytes)
	low, high := 1, v.Count()
	for low < high {
		mid := int(uint(low+high) >> 1)
		order := bytes.Compare(
			v.heads[mid*headBytes:(mid+1)*headBytes],
			key[:headBytes],
		)
		if order == 0 {
			// Equal shortened heads are not identities. The exact admitted
			// floor map has a first-byte accelerator and is faster for the
			// remaining collision class than reconstructing every midpoint.
			return v.floors.upperBound(key)
		}
		if order < 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low - 1
}

func (v *GlobalTabletCatalogNodeView) Route(key []byte) GlobalTabletCatalogNodeRoute {
	if v == nil || len(v.image) == 0 {
		return GlobalTabletCatalogNodeRoute{}
	}
	ordinal := v.upperBound(key)
	bucket := v.floors.bucketAt(ordinal)
	id, ok := globalTabletCatalogNodeID(v.level, v.childLevel, bucket)
	ref, refOK := v.refAt(ordinal, id)
	if !ok || !refOK {
		return GlobalTabletCatalogNodeRoute{}
	}
	return GlobalTabletCatalogNodeRoute{ID: id, Ordinal: uint16(ordinal), Ref: ref}
}

// RouteAt returns one child in lexical floor order. It is the rooted-scan
// counterpart of Route: a cursor records the selected ordinal, then reacquires
// this immutable node to reconstruct successors without following physical
// sibling references.
func (v *GlobalTabletCatalogNodeView) RouteAt(
	ordinal int,
) (GlobalTabletCatalogNodeRoute, bool) {
	if v == nil || ordinal < 0 || ordinal >= v.Count() {
		return GlobalTabletCatalogNodeRoute{}, false
	}
	bucket := v.floors.bucketAt(ordinal)
	id, idOK := globalTabletCatalogNodeID(v.level, v.childLevel, bucket)
	ref, refOK := v.refAt(ordinal, id)
	if !idOK || !refOK {
		return GlobalTabletCatalogNodeRoute{}, false
	}
	return GlobalTabletCatalogNodeRoute{
		ID: id, Ordinal: uint16(ordinal), Ref: ref,
	}, true
}

func (v *GlobalTabletCatalogNodeView) LowerBound(
	key []byte,
) GlobalTabletCatalogNodeCursor {
	if v == nil {
		return GlobalTabletCatalogNodeCursor{}
	}
	return GlobalTabletCatalogNodeCursor{
		node: v, cursor: v.floors.LowerBound(key),
	}
}

func (c *GlobalTabletCatalogNodeCursor) Route() (GlobalTabletCatalogNodeRoute, bool) {
	if c == nil {
		return GlobalTabletCatalogNodeRoute{}, false
	}
	bucket, ok := c.cursor.Bucket()
	ordinal, ordinalOK := c.cursor.Ordinal()
	if c.node == nil {
		return GlobalTabletCatalogNodeRoute{}, false
	}
	id, idOK := globalTabletCatalogNodeID(
		c.node.level, c.node.childLevel, bucket,
	)
	ref, refOK := c.node.refAt(ordinal, id)
	if !ok || !ordinalOK || !idOK || !refOK {
		return GlobalTabletCatalogNodeRoute{}, false
	}
	return GlobalTabletCatalogNodeRoute{
		ID: id, Ordinal: uint16(ordinal), Ref: ref,
	}, true
}

func (c *GlobalTabletCatalogNodeCursor) Next() bool {
	return c != nil && c.cursor.Next()
}

// RewriteHandle performs an immutable non-structural catalog update. Bounds
// are the destination snapshot's allocation bounds and must monotonically
// extend the source view. A tablet root replacement rewrites its 8 KiB catalog
// leaf, every selected 8 KiB branch (if present), and the 64 KiB catalog root.
// Floors are unchanged.
func (v *GlobalTabletCatalogNodeView) RewriteHandle(
	dst []byte, generation uint64, bounds GlobalTabletCatalogBounds,
	id uint32, replacement PageRef,
) ([]byte, error) {
	return v.RewriteHandles(
		dst, generation, bounds,
		[]GlobalTabletCatalogNodeHandleRewrite{{
			ID: id, Ref: replacement,
		}},
	)
}

// RewriteHandles performs one immutable node rewrite containing every listed
// child replacement. It is the checkpoint counterpart of RewriteHandle:
// floors and all untouched handles are copied once, and duplicate child IDs
// are rejected rather than depending on caller order.
func (v *GlobalTabletCatalogNodeView) RewriteHandles(
	dst []byte,
	generation uint64,
	bounds GlobalTabletCatalogBounds,
	rewrites []GlobalTabletCatalogNodeHandleRewrite,
) ([]byte, error) {
	if v == nil || len(v.image) == 0 || generation <= v.header.Generation ||
		generation >= uint64(1)<<48 || len(dst) < len(v.image) ||
		!bounds.extends(v.bounds) || len(rewrites) == 0 ||
		len(rewrites) > v.Count() {
		return nil, fmt.Errorf("%w: catalog COW generation or destination", ErrInvalidWrite)
	}
	if globalTabletCatalogSlicesOverlap(dst[:len(v.image)], v.image) {
		return nil, fmt.Errorf("%w: catalog COW source/destination overlap", ErrInvalidWrite)
	}
	// Prove the complete batch before InitPage clears dst. Callers may reuse
	// that arena after an eligibility error, and the point RewriteHandle API
	// has always left it untouched on rejection.
	for rank, rewrite := range rewrites {
		for prior := range rank {
			if rewrites[prior].ID == rewrite.ID {
				return nil, fmt.Errorf(
					"%w: duplicate catalog COW child",
					ErrInvalidWrite,
				)
			}
		}
		wantChild, ok := globalTabletCatalogChildLogicalID(
			v.level, v.childLevel, rewrite.ID,
		)
		if !ok || globalTabletCatalogValidatePackedRef(
			rewrite.Ref, wantChild, v.childKind, v.childLength,
			generation, bounds,
		) != nil {
			return nil, fmt.Errorf(
				"%w: catalog COW child", ErrInvalidWrite,
			)
		}
		found := false
		for at := 0; at < v.Count(); at++ {
			candidate, valid := globalTabletCatalogNodeID(
				v.level, v.childLevel, v.floors.bucketAt(at),
			)
			if valid && candidate == rewrite.ID {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf(
				"%w: catalog COW child not found",
				ErrInvalidWrite,
			)
		}
	}
	payload, err := InitPage(dst[:len(v.image)], PageHeader{
		StoreID: v.header.StoreID, Generation: generation,
		LogicalID: v.header.LogicalID, PageSize: v.header.PageSize,
		PayloadLength: v.header.PayloadLength, Kind: v.header.Kind,
	})
	if err != nil {
		return nil, err
	}
	copy(payload, v.payload)
	mapBytes := int(binary.LittleEndian.Uint32(payload[12:16]))
	mapStart := GlobalTabletCatalogNodePayloadHeaderBytes
	mapEnd := mapStart + mapBytes
	if mapBytes < TabletAnchorMapHeaderSize+TabletAnchorMapTrailerSize ||
		mapEnd > len(payload) {
		return nil, globalTabletCatalogCorrupt("COW floor-map geometry")
	}
	// The floor map is embedded, so its identity is the enclosing node's
	// physical birth. Refreshing it makes equal COW nodes byte-identical even
	// when their unchanged floors came from different ancestors.
	binary.LittleEndian.PutUint64(
		payload[mapStart+24:mapStart+32], generation,
	)
	tabletAnchorMapSeal(payload[mapStart:mapEnd])
	for rank, rewrite := range rewrites {
		for prior := range rank {
			if rewrites[prior].ID == rewrite.ID {
				return nil, fmt.Errorf(
					"%w: duplicate catalog COW child",
					ErrInvalidWrite,
				)
			}
		}
		wantChild, ok := globalTabletCatalogChildLogicalID(
			v.level, v.childLevel, rewrite.ID,
		)
		if !ok || globalTabletCatalogValidatePackedRef(
			rewrite.Ref, wantChild, v.childKind, v.childLength,
			generation, bounds,
		) != nil {
			return nil, fmt.Errorf(
				"%w: catalog COW child", ErrInvalidWrite,
			)
		}
		ordinal := -1
		for at := 0; at < v.Count(); at++ {
			candidate, valid := globalTabletCatalogNodeID(
				v.level, v.childLevel, v.floors.bucketAt(at),
			)
			if valid && candidate == rewrite.ID {
				ordinal = at
				break
			}
		}
		if ordinal < 0 {
			return nil, fmt.Errorf(
				"%w: catalog COW child not found",
				ErrInvalidWrite,
			)
		}
		globalTabletCatalogEncodePackedRef(
			payload[GlobalTabletCatalogNodePayloadHeaderBytes+
				len(v.floors.image)+
				ordinal*GlobalTabletCatalogHandleBytes:],
			rewrite.Ref,
		)
	}
	if _, err := sealInitializedPage(dst[:len(v.image)]); err != nil {
		return nil, err
	}
	return dst[:len(v.image)], nil
}

func (v *GlobalTabletCatalogNodeView) refAt(
	ordinal int, id uint32,
) (PageRef, bool) {
	if v == nil || ordinal < 0 || ordinal >= v.Count() {
		return PageRef{}, false
	}
	logicalID, ok := globalTabletCatalogChildLogicalID(
		v.level, v.childLevel, id,
	)
	if !ok {
		return PageRef{}, false
	}
	src := v.handles[ordinal*GlobalTabletCatalogHandleBytes:]
	ref := PageRef{
		Offset:     segmentedTabletRouterGetUint48(src) << 3,
		LogicalID:  logicalID,
		Generation: segmentedTabletRouterGetUint48(src[6:]),
		Length:     v.childLength, Kind: v.childKind,
	}
	return ref, globalTabletCatalogValidatePackedRef(
		ref, logicalID, v.childKind, v.childLength,
		v.bounds.SelectedRootGeneration, v.bounds,
	) == nil
}

func EncodeGlobalTabletCatalogLocator(
	dst []byte, header PageHeader, bounds GlobalTabletCatalogBounds,
	tabletID uint32, reuseFloor uint64,
	entries []GlobalTabletCatalogLocatorEntry,
) ([]byte, error) {
	logicalID, ok := GlobalTabletCatalogLocatorLogicalID(tabletID)
	payloadLength := GlobalTabletCatalogLocatorHeader + globalTabletCatalogPackedBytes
	if !ok || len(dst) < GlobalTabletCatalogLocatorBytes ||
		header.LogicalID != logicalID ||
		header.Kind != PagePrimaryLocator ||
		header.PageSize != GlobalTabletCatalogLocatorBytes ||
		header.PayloadLength != uint32(payloadLength) ||
		header.Generation == 0 || header.Generation >= uint64(1)<<48 ||
		header.StoreID != bounds.StoreID ||
		header.Generation > bounds.SelectedRootGeneration ||
		reuseFloor > header.Generation || !bounds.valid() ||
		header.LogicalID >= bounds.NextLogicalID {
		return nil, fmt.Errorf("%w: compact locator identity", ErrInvalidWrite)
	}
	payload, err := InitPage(dst[:GlobalTabletCatalogLocatorBytes], header)
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(payload[0:4], globalTabletCatalogLocatorVersion)
	binary.LittleEndian.PutUint32(payload[4:8], tabletID)
	binary.LittleEndian.PutUint64(payload[16:24], reuseFloor)
	packed := payload[GlobalTabletCatalogLocatorHeader:]
	var live, retired uint16
	var previous uint16
	for at, entry := range entries {
		if entry.LocalID >= TabletLocalIdentityLocalCount ||
			at != 0 && entry.LocalID <= previous ||
			entry.PageID >= SegmentedTabletRouterMaxPages ||
			entry.State != GlobalTabletCatalogLocatorLive &&
				entry.State != GlobalTabletCatalogLocatorRetired {
			return nil, fmt.Errorf("%w: compact locator entry", ErrInvalidWrite)
		}
		code := uint16(entry.State)<<12 |
			uint16(entry.PageID)<<8 | uint16(entry.RowSlot)
		globalTabletCatalogPut14(packed, entry.LocalID, code)
		if entry.State == GlobalTabletCatalogLocatorLive {
			live++
		} else {
			retired++
		}
		previous = entry.LocalID
	}
	binary.LittleEndian.PutUint16(payload[8:10], live)
	binary.LittleEndian.PutUint16(payload[10:12], retired)
	if _, err := sealInitializedPage(dst[:GlobalTabletCatalogLocatorBytes]); err != nil {
		return nil, err
	}
	return dst[:GlobalTabletCatalogLocatorBytes], nil
}

// EncodeGlobalTabletCatalogLocatorPage is the durable-transaction entry point
// for a rebuilt locator page. It derives the exact page header (logical ID and
// payload length) for tabletID so the durable structural split/merge writer
// need not repeat the private packed-locator geometry, and encodes entries into
// dst, which must be one freshly allocated GlobalTabletCatalogLocatorBytes page.
func EncodeGlobalTabletCatalogLocatorPage(
	dst []byte, storeID [16]byte, generation uint64,
	tabletID uint32, reuseFloor uint64,
	bounds GlobalTabletCatalogBounds,
	entries []GlobalTabletCatalogLocatorEntry,
) ([]byte, error) {
	logicalID, ok := GlobalTabletCatalogLocatorLogicalID(tabletID)
	if !ok {
		return nil, fmt.Errorf(
			"%w: locator page tablet identity", ErrInvalidWrite,
		)
	}
	return EncodeGlobalTabletCatalogLocator(
		dst,
		PageHeader{
			StoreID: storeID, Generation: generation, LogicalID: logicalID,
			PageSize: GlobalTabletCatalogLocatorBytes,
			PayloadLength: GlobalTabletCatalogLocatorHeader +
				globalTabletCatalogPackedBytes,
			Kind: PagePrimaryLocator,
		},
		bounds, tabletID, reuseFloor, entries,
	)
}

func OpenGlobalTabletCatalogLocator(
	src []byte, expected PageRef, bounds GlobalTabletCatalogBounds,
) (GlobalTabletCatalogLocatorView, error) {
	var view GlobalTabletCatalogLocatorView
	header, payload, err := OpenPage(src)
	if err != nil ||
		!globalTabletCatalogHeaderMatchesRef(header, expected, bounds) ||
		header.Kind != PagePrimaryLocator ||
		len(payload) != GlobalTabletCatalogLocatorHeader+
			globalTabletCatalogPackedBytes ||
		binary.LittleEndian.Uint32(payload[0:4]) != globalTabletCatalogLocatorVersion ||
		!allZero(payload[12:16]) || !allZero(payload[24:GlobalTabletCatalogLocatorHeader]) {
		return view, globalTabletCatalogCorrupt("locator header")
	}
	tabletID := binary.LittleEndian.Uint32(payload[4:8])
	logicalID, ok := GlobalTabletCatalogLocatorLogicalID(tabletID)
	reuseFloor := binary.LittleEndian.Uint64(payload[16:24])
	if !ok || header.LogicalID != logicalID || reuseFloor > header.Generation {
		return GlobalTabletCatalogLocatorView{},
			globalTabletCatalogCorrupt("locator identity")
	}
	view = GlobalTabletCatalogLocatorView{
		image: src[:header.PageSize], packed: payload[GlobalTabletCatalogLocatorHeader:],
		header: header, ref: expected, tabletID: tabletID,
		live:       binary.LittleEndian.Uint16(payload[8:10]),
		retired:    binary.LittleEndian.Uint16(payload[10:12]),
		reuseFloor: reuseFloor,
		bounds:     bounds,
	}
	var live, retired uint16
	for localID := range uint16(TabletLocalIdentityLocalCount) {
		code := globalTabletCatalogGet14(view.packed, localID)
		state := GlobalTabletCatalogLocatorState(code >> 12)
		switch state {
		case GlobalTabletCatalogLocatorEmpty:
			if code&0x0fff != 0 {
				return GlobalTabletCatalogLocatorView{},
					globalTabletCatalogCorrupt("non-canonical empty locator")
			}
		case GlobalTabletCatalogLocatorLive:
			live++
		case GlobalTabletCatalogLocatorRetired:
			retired++
		default:
			return GlobalTabletCatalogLocatorView{},
				globalTabletCatalogCorrupt("reserved locator state")
		}
	}
	if live != view.live || retired != view.retired {
		return GlobalTabletCatalogLocatorView{},
			globalTabletCatalogCorrupt("locator cardinality")
	}
	return view, nil
}

func (v *GlobalTabletCatalogLocatorView) Resolve(
	localID uint16,
) (pageID, rowSlot uint8, state GlobalTabletCatalogLocatorState) {
	if v == nil || len(v.image) == 0 || localID >= TabletLocalIdentityLocalCount {
		return 0, 0, GlobalTabletCatalogLocatorEmpty
	}
	code := globalTabletCatalogGet14(v.packed, localID)
	return uint8(code >> 8 & 0x0f), uint8(code), GlobalTabletCatalogLocatorState(code >> 12)
}

// EncodeGlobalTabletCatalogTabletRoot wraps one validated segmented root.
// The complete locator reference is encoded, checksum-bound, and recoverable.
func EncodeGlobalTabletCatalogTabletRoot(
	dst []byte, header PageHeader, bounds GlobalTabletCatalogBounds,
	locator PageRef, segmentedRoot []byte,
) ([]byte, error) {
	inner, err := globalTabletCatalogOpenSegmentedRootOnly(segmentedRoot)
	logicalID, ok := GlobalTabletCatalogTabletRootLogicalID(inner.tabletID)
	locatorLogical, locatorOK := GlobalTabletCatalogLocatorLogicalID(inner.tabletID)
	payloadLength := GlobalTabletCatalogRootHeader + SegmentedTabletRouterRootBytes
	if err != nil || !ok || !locatorOK ||
		len(dst) < GlobalTabletCatalogTabletBytes ||
		header.LogicalID != logicalID ||
		header.Kind != PageTabletRoute ||
		header.Generation != inner.generation ||
		header.PageSize != GlobalTabletCatalogTabletBytes ||
		header.PayloadLength != uint32(payloadLength) ||
		header.StoreID != bounds.StoreID ||
		inner.storeID != header.StoreID ||
		header.Generation > bounds.SelectedRootGeneration ||
		globalTabletCatalogValidateFullRef(
			locator, locatorLogical, locator.Kind,
			GlobalTabletCatalogLocatorBytes, header.Generation, bounds,
		) != nil ||
		locator.Kind != PagePrimaryLocator || !bounds.valid() ||
		header.LogicalID >= bounds.NextLogicalID ||
		globalTabletCatalogRootRefsWithinBounds(inner, bounds) != nil {
		return nil, fmt.Errorf("%w: cacheable tablet-root identity", ErrInvalidWrite)
	}
	payload, err := InitPage(dst[:GlobalTabletCatalogTabletBytes], header)
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(payload[0:4], globalTabletCatalogRootVersion)
	binary.LittleEndian.PutUint32(payload[4:8], inner.tabletID)
	binary.LittleEndian.PutUint32(payload[8:12], SegmentedTabletRouterRootBytes)
	encodePageRef(payload[16:16+PageRefSize], locator)
	copy(payload[GlobalTabletCatalogRootHeader:], segmentedRoot)
	if _, err := sealInitializedPage(dst[:GlobalTabletCatalogTabletBytes]); err != nil {
		return nil, err
	}
	return dst[:GlobalTabletCatalogTabletBytes], nil
}

func OpenGlobalTabletCatalogTabletRoot(
	src []byte, expected PageRef, bounds GlobalTabletCatalogBounds,
) (GlobalTabletCatalogTabletRootView, error) {
	var view GlobalTabletCatalogTabletRootView
	header, payload, err := OpenPage(src)
	if err != nil ||
		!globalTabletCatalogHeaderMatchesRef(header, expected, bounds) ||
		header.Kind != PageTabletRoute ||
		len(payload) != GlobalTabletCatalogRootHeader+
			SegmentedTabletRouterRootBytes ||
		binary.LittleEndian.Uint32(payload[0:4]) != globalTabletCatalogRootVersion ||
		binary.LittleEndian.Uint32(payload[8:12]) != SegmentedTabletRouterRootBytes ||
		!allZero(payload[12:16]) ||
		!pageRefReservedZero(payload[16:16+PageRefSize]) ||
		!allZero(payload[16+PageRefSize:GlobalTabletCatalogRootHeader]) {
		return view, globalTabletCatalogCorrupt("tablet root wrapper")
	}
	inner, err := globalTabletCatalogOpenSegmentedRootOnly(
		payload[GlobalTabletCatalogRootHeader:],
	)
	tabletID := binary.LittleEndian.Uint32(payload[4:8])
	logicalID, ok := GlobalTabletCatalogTabletRootLogicalID(tabletID)
	locatorLogical, locatorOK := GlobalTabletCatalogLocatorLogicalID(tabletID)
	locator := decodePageRef(payload[16 : 16+PageRefSize])
	if err != nil || tabletID != inner.tabletID || !ok || !locatorOK ||
		inner.storeID != header.StoreID ||
		header.LogicalID != logicalID || header.Generation != inner.generation ||
		globalTabletCatalogValidateFullRef(
			locator, locatorLogical, locator.Kind,
			GlobalTabletCatalogLocatorBytes, header.Generation, bounds,
		) != nil ||
		locator.Kind != PagePrimaryLocator ||
		globalTabletCatalogRootRefsWithinBounds(inner, bounds) != nil {
		return GlobalTabletCatalogTabletRootView{},
			globalTabletCatalogCorrupt("tablet root binding")
	}
	return GlobalTabletCatalogTabletRootView{
		image: src[:header.PageSize], payload: payload, header: header,
		inner: inner, locator: locator, bounds: bounds,
	}, nil
}

// AdmittedGlobalTabletCatalogTabletRoot reconstructs a cacheable tablet root
// whose wrapper, segmented root, locator reference, and anchor references were
// already fully validated by PageCache admission. Calling it on arbitrary
// bytes is invalid.
func AdmittedGlobalTabletCatalogTabletRoot(
	src []byte, bounds GlobalTabletCatalogBounds,
) GlobalTabletCatalogTabletRootView {
	header, _ := decodePageHeader(src)
	payloadEnd := PageHeaderSize + int(header.PayloadLength)
	payload := src[PageHeaderSize:payloadEnd:payloadEnd]
	return GlobalTabletCatalogTabletRootView{
		image: src[:header.PageSize], payload: payload, header: header,
		inner: admittedGlobalTabletCatalogSegmentedRootOnly(
			payload[GlobalTabletCatalogRootHeader:],
		),
		locator: decodePageRef(payload[16 : 16+PageRefSize]),
		bounds:  bounds,
	}
}

func (v *GlobalTabletCatalogTabletRootView) LocatorRef() (PageRef, bool) {
	if v == nil {
		return PageRef{}, false
	}
	return v.locator, len(v.image) != 0
}

// TabletID returns the stable 18-bit identity of this tablet.
func (v *GlobalTabletCatalogTabletRootView) TabletID() uint32 {
	if v == nil {
		return 0
	}
	return v.inner.tabletID
}

// AnchorCount returns the number of independently cached anchor pages.
func (v *GlobalTabletCatalogTabletRootView) AnchorCount() int {
	if v == nil {
		return 0
	}
	return int(v.inner.pageCount)
}

// AnchorAt returns one anchor page in lexical rank order.
func (v *GlobalTabletCatalogTabletRootView) AnchorAt(
	rank int,
) (GlobalTabletCatalogAnchorRoute, bool) {
	if v == nil || rank < 0 || rank >= int(v.inner.pageCount) {
		return GlobalTabletCatalogAnchorRoute{}, false
	}
	pageID := v.inner.rootRanks[rank]
	ref, ok := v.inner.anchorRef(pageID)
	return GlobalTabletCatalogAnchorRoute{PageID: pageID, Ref: ref}, ok
}

// RouteAnchor completes the tablet-root half of a point lookup. The caller
// acquires exactly the returned anchor page; no locator is touched.
func (v *GlobalTabletCatalogTabletRootView) RouteAnchor(
	key []byte,
) (GlobalTabletCatalogAnchorRoute, bool) {
	if v == nil || len(v.image) == 0 {
		return GlobalTabletCatalogAnchorRoute{}, false
	}
	rank := v.inner.rootUpperBound(key) - 1
	pageID := v.inner.rootRanks[rank]
	ref, ok := v.inner.anchorRef(pageID)
	return GlobalTabletCatalogAnchorRoute{PageID: pageID, Ref: ref}, ok
}

// ValidateGlobalTabletCatalogAnchor performs the context-free admission proof
// for one independently cached anchor. The selected tablet root later proves
// that its exact PageRef chose this page; this validator proves the common
// identity, complete anchor structure, leaf handles, and snapshot bounds once
// before an admitted reconstruction can trust resident offsets.
func ValidateGlobalTabletCatalogAnchor(
	src []byte, expected PageRef, bounds GlobalTabletCatalogBounds,
) error {
	if expected.LogicalID < GlobalTabletCatalogAnchorLogicalIDBase ||
		expected.LogicalID >= GlobalTabletCatalogAnchorLogicalIDLimit {
		return globalTabletCatalogCorrupt("anchor logical identity")
	}
	delta := expected.LogicalID - GlobalTabletCatalogAnchorLogicalIDBase
	tabletID := uint32(delta / SegmentedTabletRouterMaxPages)
	pageID := uint8(delta % SegmentedTabletRouterMaxPages)
	logicalID, ok := GlobalTabletCatalogAnchorLogicalID(tabletID, pageID)
	header, _, err := OpenPage(src)
	if err != nil || !ok || logicalID != expected.LogicalID ||
		!globalTabletCatalogHeaderMatchesRef(header, expected, bounds) ||
		header.Kind != PagePrimaryAnchor ||
		header.PayloadLength != segmentedTabletRouterAnchorPayloadBytes {
		return globalTabletCatalogCorrupt("anchor admission envelope")
	}
	var ranks [SegmentedTabletRouterMaxPages]byte
	router := SegmentedTabletRouterView{
		rootRanks: ranks[:], storeID: bounds.StoreID,
		tabletID: tabletID, generation: bounds.SelectedRootGeneration,
		pageCount:  SegmentedTabletRouterMaxPages,
		anchorKind: PagePrimaryAnchor, leafKind: PagePrimaryLeaf,
	}
	page, err := segmentedTabletRouterOpenAnchor(
		src, router, pageID, expected,
	)
	if err != nil {
		return err
	}
	for rank := 0; rank < int(page.count); rank++ {
		slot := page.ranks[rank]
		localID := binary.LittleEndian.Uint16(page.localIDs[int(slot)*2:])
		bucket, bucketOK := MakeTabletLocalIdentityBucket(
			tabletID, uint32(localID),
		)
		leaf, _, leafOK := page.handleAt(slot, BucketID(bucket))
		if !bucketOK || !leafOK || !bounds.contains(leaf) {
			return globalTabletCatalogCorrupt("anchor leaf bounds")
		}
	}
	return nil
}

func OpenGlobalTabletCatalogAnchor(
	src []byte, root *GlobalTabletCatalogTabletRootView, pageID uint8,
) (GlobalTabletCatalogAnchorView, error) {
	// PageRef does not carry StoreID. The independently admitted tablet root
	// supplies the Store fence and the common anchor envelope must match it.
	if root == nil || len(root.image) == 0 || !root.bounds.valid() ||
		root.header.StoreID != root.bounds.StoreID ||
		root.header.Generation > root.bounds.SelectedRootGeneration {
		return GlobalTabletCatalogAnchorView{},
			globalTabletCatalogCorrupt("anchor without root")
	}
	ref, ok := root.inner.anchorRef(pageID)
	if !ok || globalTabletCatalogValidateFullRef(
		ref, ref.LogicalID, ref.Kind, SegmentedTabletRouterAnchorPageBytes,
		root.header.Generation, root.bounds,
	) != nil {
		return GlobalTabletCatalogAnchorView{},
			globalTabletCatalogCorrupt("anchor reference")
	}
	router := root.inner.segmentedView()
	page, err := segmentedTabletRouterOpenAnchor(src, router, pageID, ref)
	if err != nil {
		return GlobalTabletCatalogAnchorView{}, err
	}
	for rank := 0; rank < int(page.count); rank++ {
		slot := page.ranks[rank]
		localID := binary.LittleEndian.Uint16(page.localIDs[int(slot)*2:])
		bucket, bucketOK := MakeTabletLocalIdentityBucket(
			root.inner.tabletID, uint32(localID),
		)
		leaf, _, leafOK := page.handleAt(slot, BucketID(bucket))
		if !bucketOK || !leafOK || !root.bounds.contains(leaf) {
			return GlobalTabletCatalogAnchorView{},
				globalTabletCatalogCorrupt("anchor leaf bounds")
		}
	}
	return GlobalTabletCatalogAnchorView{
		page: page, ref: ref, locator: root.locator,
		tabletID: root.inner.tabletID,
	}, nil
}

// AdmittedGlobalTabletCatalogAnchor reconstructs an anchor whose complete
// context-free structure was validated by PageCache admission and whose exact
// reference was selected by root. Calling it on arbitrary bytes is invalid.
func AdmittedGlobalTabletCatalogAnchor(
	src []byte, root *GlobalTabletCatalogTabletRootView, pageID uint8,
) GlobalTabletCatalogAnchorView {
	router := root.inner.segmentedView()
	ref, _ := root.inner.anchorRef(pageID)
	return GlobalTabletCatalogAnchorView{
		page: segmentedTabletRouterAdmittedAnchor(
			src, router, pageID,
		),
		ref: ref, locator: root.locator, tabletID: root.inner.tabletID,
	}
}

// RewriteHandle performs the non-structural tablet COW selected by route. The
// compact global locator is unchanged: route already carries the stable
// page/row identity proven by the selected admitted anchor. The result is the
// raw segmented root plus its one rewritten anchor page; callers wrap Root in
// a new cacheable tablet-root page in the same transaction.
func (v *GlobalTabletCatalogTabletRootView) RewriteHandle(
	rootDst, pageDst []byte,
	generation uint64,
	route SegmentedTabletRouterRoute,
	leafRef PageRef,
	zone BucketZone,
	anchorRef PageRef,
	anchor *GlobalTabletCatalogAnchorView,
) (SegmentedTabletRouterCOWResult, error) {
	if v == nil || anchor == nil || len(v.image) == 0 ||
		len(anchor.page.image) == 0 ||
		anchor.tabletID != v.inner.tabletID ||
		anchor.locator != v.locator ||
		anchor.page.pageID != route.PageID {
		return SegmentedTabletRouterCOWResult{},
			fmt.Errorf("%w: global tablet COW selection", ErrInvalidWrite)
	}
	currentRef, _, ok := anchor.page.handleAt(route.RowSlot, route.Bucket)
	if !ok || currentRef != route.Ref {
		return SegmentedTabletRouterCOWResult{},
			fmt.Errorf("%w: global tablet COW route", ErrInvalidWrite)
	}
	router := v.inner.segmentedView()
	router.pages[route.PageID] = anchor.page
	return router.rewriteHandleAt(
		rootDst, pageDst, generation, route.Bucket,
		route.PageID, route.RowSlot, leafRef, zone, anchorRef,
	)
}

// InsertSplitLeaf performs the localized structural edit for one primary-leaf
// split. It preserves every unaffected anchor byte-for-byte, rewrites the
// locator and segmented root, and rewrites only the selected anchor plus one
// new anchor when the selected page was full. The caller publishes all
// returned images atomically through the surrounding tablet/catalog COW path.
func (v *GlobalTabletCatalogTabletRootView) InsertSplitLeaf(
	rootDst, locatorDst, leftDst, rightDst []byte,
	generation uint64,
	route SegmentedTabletRouterRoute,
	leftRef PageRef,
	rightLocalID uint16,
	rightFence []byte,
	rightRef PageRef,
	leftAnchorRef, rightAnchorRef PageRef,
	locator *GlobalTabletCatalogLocatorView,
	anchor *GlobalTabletCatalogAnchorView,
) (SegmentedTabletRouterLeafSplitResult, error) {
	var result SegmentedTabletRouterLeafSplitResult
	if v == nil || locator == nil || anchor == nil || len(v.image) == 0 ||
		len(locator.image) != GlobalTabletCatalogLocatorBytes ||
		len(anchor.page.image) != SegmentedTabletRouterAnchorPageBytes ||
		len(rootDst) < SegmentedTabletRouterRootBytes ||
		len(locatorDst) < GlobalTabletCatalogLocatorBytes ||
		len(leftDst) < SegmentedTabletRouterAnchorPageBytes ||
		generation <= v.inner.generation || generation >= uint64(1)<<48 ||
		locator.ref != v.locator || locator.tabletID != v.inner.tabletID ||
		anchor.tabletID != v.inner.tabletID || anchor.locator != v.locator ||
		anchor.page.pageID != route.PageID || rightLocalID >= TabletLocalIdentityLocalCount ||
		len(rightFence) == 0 || len(rightFence) > CommonPrimaryLeafMaxKeyBytes {
		return result, fmt.Errorf("%w: localized leaf split selection", ErrInvalidWrite)
	}
	currentRef, currentZone, ok := anchor.page.handleAt(route.RowSlot, route.Bucket)
	tabletID, leftLocalID, bucketOK := SplitTabletLocalIdentityBucket(uint32(route.Bucket))
	rightBucketU, rightBucketOK := MakeTabletLocalIdentityBucket(tabletID, uint32(rightLocalID))
	_, _, rightState := locator.Resolve(rightLocalID)
	if !ok || currentRef != route.Ref || !bucketOK || tabletID != v.inner.tabletID ||
		!rightBucketOK || rightLocalID == uint16(leftLocalID) ||
		rightState != GlobalTabletCatalogLocatorEmpty ||
		leftRef.Generation != generation || rightRef.Generation != generation ||
		segmentedTabletRouterValidateLeafRef(leftRef, route.Bucket, v.inner.leafKind, generation) != nil ||
		segmentedTabletRouterValidateLeafRef(rightRef, BucketID(rightBucketU), v.inner.leafKind, generation) != nil {
		return result, fmt.Errorf("%w: localized leaf split identity", ErrInvalidWrite)
	}
	sourceRank := -1
	for rank := 0; rank < int(anchor.page.count); rank++ {
		if anchor.page.ranks[rank] == route.RowSlot {
			sourceRank = rank
			break
		}
	}
	if sourceRank < 0 {
		return result, fmt.Errorf("%w: localized leaf split rank", ErrInvalidWrite)
	}
	leftFloor := anchor.page.fenceAt(sourceRank)
	if segmentedTabletRouterCompareFences(leftFloor, segmentedTabletRouterFence{a: rightFence}) >= 0 ||
		sourceRank+1 < int(anchor.page.count) &&
			segmentedTabletRouterCompareFences(segmentedTabletRouterFence{a: rightFence}, anchor.page.fenceAt(sourceRank+1)) >= 0 {
		return result, fmt.Errorf("%w: localized leaf split fence", ErrInvalidWrite)
	}

	count := int(anchor.page.count) + 1
	insertRank := sourceRank + 1
	type splitRow struct {
		fence segmentedTabletRouterFence
		local uint16
		ref   PageRef
		zone  BucketZone
	}
	rowAt := func(rank int) splitRow {
		if rank == insertRank {
			return splitRow{fence: segmentedTabletRouterFence{a: rightFence}, local: rightLocalID, ref: rightRef}
		}
		oldRank := rank
		if rank > insertRank {
			oldRank--
		}
		slot := anchor.page.ranks[oldRank]
		localID := binary.LittleEndian.Uint16(anchor.page.localIDs[int(slot)*2:])
		bucketU, _ := MakeTabletLocalIdentityBucket(tabletID, uint32(localID))
		ref, zone, _ := anchor.page.handleAt(slot, BucketID(bucketU))
		if oldRank == sourceRank {
			ref, zone = leftRef, currentZone
		}
		return splitRow{fence: anchor.page.fenceAt(oldRank), local: localID, ref: ref, zone: zone}
	}
	encodeRange := func(dst []byte, pageID uint8, first, last int) error {
		header := SegmentedTabletRouterHeader{
			StoreID: v.inner.storeID, TabletID: tabletID, Generation: generation,
			AnchorKind: v.inner.anchorKind, LeafKind: v.inner.leafKind,
		}
		_, err := segmentedTabletRouterEncodeAnchor(
			dst, header, pageID, last-first,
			func(rank int) segmentedTabletRouterFence { return rowAt(first + rank).fence },
			func(rank int) (uint8, uint16, PageRef, BucketZone) {
				row := rowAt(first + rank)
				return uint8(rank), row.local, row.ref, row.zone
			},
		)
		return err
	}

	leftPageID := route.PageID
	pageCount := v.inner.pageCount
	rightPageID := leftPageID
	splitRank := count
	if count > SegmentedTabletRouterRowsPerPage {
		if pageCount >= SegmentedTabletRouterMaxPages ||
			len(rightDst) < SegmentedTabletRouterAnchorPageBytes {
			return result, fmt.Errorf("%w: localized leaf split anchor capacity", ErrInvalidWrite)
		}
		rightPageID = pageCount
		pageCount++
		splitRank = count / 2
	}
	if leftAnchorRef.Generation != generation ||
		segmentedTabletRouterValidateAnchorRefIdentity(leftAnchorRef, tabletID, generation, leftPageID) != nil ||
		rightPageID != leftPageID && (rightAnchorRef.Generation != generation ||
			segmentedTabletRouterValidateAnchorRefIdentity(rightAnchorRef, tabletID, generation, rightPageID) != nil) {
		return result, fmt.Errorf("%w: localized leaf split anchor refs", ErrInvalidWrite)
	}
	if err := encodeRange(leftDst, leftPageID, 0, splitRank); err != nil {
		return result, err
	}
	if rightPageID != leftPageID {
		if err := encodeRange(rightDst, rightPageID, splitRank, count); err != nil {
			return result, err
		}
	}

	locatorImage := locatorDst[:GlobalTabletCatalogLocatorBytes]
	copy(locatorImage, locator.image)
	binary.LittleEndian.PutUint64(locatorImage[24:32], generation)
	payload := locatorImage[PageHeaderSize:]
	binary.LittleEndian.PutUint16(payload[8:10], locator.live+1)
	packed := payload[GlobalTabletCatalogLocatorHeader:]
	for rank := 0; rank < count; rank++ {
		row := rowAt(rank)
		pageID, slot := leftPageID, rank
		if rightPageID != leftPageID && rank >= splitRank {
			pageID, slot = rightPageID, rank-splitRank
		}
		globalTabletCatalogPut14(
			packed, row.local,
			uint16(GlobalTabletCatalogLocatorLive)<<12|uint16(pageID)<<8|uint16(slot),
		)
	}
	if _, err := sealInitializedPage(locatorImage); err != nil {
		return result, err
	}

	root := rootDst[:SegmentedTabletRouterRootBytes]
	if rightPageID == leftPageID {
		copy(root, v.inner.root)
		binary.LittleEndian.PutUint64(root[24:32], generation)
		binary.LittleEndian.PutUint32(root[36:40], PageChecksum(locatorImage))
		segmentedTabletRouterEncodeAnchorRef(
			root[segmentedTabletRouterRootRefsAt+int(leftPageID)*segmentedTabletRouterRootRefBytes:],
			leftAnchorRef,
		)
		segmentedTabletRouterSeal(root, segmentedTabletRouterRootTrailerAt)
	} else {
		router := v.inner.segmentedView()
		if err := router.encodeSplitRoot(
			root, locatorImage,
			SegmentedTabletRouterHeader{
				StoreID: v.inner.storeID, TabletID: tabletID, Generation: generation,
				AnchorKind: v.inner.anchorKind, LeafKind: v.inner.leafKind,
			},
			leftPageID, rightPageID, rowAt(splitRank).fence,
			leftAnchorRef, rightAnchorRef,
		); err != nil {
			return result, err
		}
	}
	bytes := SegmentedTabletRouterRootBytes + GlobalTabletCatalogLocatorBytes + SegmentedTabletRouterAnchorPageBytes
	result = SegmentedTabletRouterLeafSplitResult{
		Root: root, Locator: locatorImage,
		LeftPage:   leftDst[:SegmentedTabletRouterAnchorPageBytes],
		LeftPageID: leftPageID, RightPageID: rightPageID,
		PageCount: pageCount, Bytes: bytes,
	}
	if rightPageID != leftPageID {
		result.RightPage = rightDst[:SegmentedTabletRouterAnchorPageBytes]
		result.Bytes += SegmentedTabletRouterAnchorPageBytes
	}
	return result, nil
}

// RewriteAnchorHandles writes one anchor after-image containing every listed
// stable-row leaf replacement. All rewrites must select the supplied anchor;
// the tablet root itself is rewritten separately by RewriteAnchorRefs.
func (v *GlobalTabletCatalogTabletRootView) RewriteAnchorHandles(
	pageDst []byte,
	generation uint64,
	rewrites []GlobalTabletCatalogAnchorHandleRewrite,
	anchorRef PageRef,
	anchor *GlobalTabletCatalogAnchorView,
) ([]byte, error) {
	if v == nil || anchor == nil || len(v.image) == 0 ||
		len(anchor.page.image) == 0 || len(rewrites) == 0 ||
		anchor.tabletID != v.inner.tabletID ||
		anchor.locator != v.locator ||
		len(pageDst) < SegmentedTabletRouterAnchorPageBytes ||
		generation <= v.inner.generation ||
		generation >= uint64(1)<<48 {
		return nil, fmt.Errorf(
			"%w: global tablet batched anchor selection",
			ErrInvalidWrite,
		)
	}
	pageID := anchor.page.pageID
	if anchorRef.Generation != generation ||
		segmentedTabletRouterValidateAnchorRefIdentity(
			anchorRef, v.inner.tabletID, generation, pageID,
		) != nil {
		return nil, fmt.Errorf(
			"%w: global tablet batched anchor ref",
			ErrInvalidWrite,
		)
	}
	nextPage := pageDst[:SegmentedTabletRouterAnchorPageBytes]
	copy(nextPage, anchor.page.image)
	binary.LittleEndian.PutUint64(nextPage[24:32], generation)
	for rank, rewrite := range rewrites {
		route := rewrite.Route
		if route.PageID != pageID {
			return nil, fmt.Errorf(
				"%w: global tablet batched anchor page",
				ErrInvalidWrite,
			)
		}
		for prior := range rank {
			if rewrites[prior].Route.RowSlot == route.RowSlot {
				return nil, fmt.Errorf(
					"%w: duplicate global tablet anchor row",
					ErrInvalidWrite,
				)
			}
		}
		tabletID, localID, ok := SplitTabletLocalIdentityBucket(
			uint32(route.Bucket),
		)
		current, _, currentOK := anchor.page.handleAt(
			route.RowSlot, route.Bucket,
		)
		if !ok || tabletID != v.inner.tabletID ||
			binary.LittleEndian.Uint16(
				anchor.page.localIDs[int(route.RowSlot)*2:],
			) != localID ||
			!currentOK || current != route.Ref ||
			rewrite.Ref.Generation != generation ||
			segmentedTabletRouterValidateLeafRef(
				rewrite.Ref, route.Bucket, v.inner.leafKind,
				generation,
			) != nil {
			return nil, fmt.Errorf(
				"%w: global tablet batched anchor leaf",
				ErrInvalidWrite,
			)
		}
		segmentedTabletRouterEncodeLeafHandle(
			nextPage[segmentedTabletRouterAnchorHandlesAt+
				int(route.RowSlot)*SegmentedTabletRouterHandleBytes:],
			rewrite.Ref, route.Zone,
		)
	}
	segmentedTabletRouterSeal(
		nextPage, segmentedTabletRouterAnchorTrailerAt,
	)
	return nextPage, nil
}

// RewriteAnchorRefs writes one segmented tablet-root after-image containing
// every listed anchor-page replacement.
func (v *GlobalTabletCatalogTabletRootView) RewriteAnchorRefs(
	rootDst []byte,
	generation uint64,
	rewrites []GlobalTabletCatalogAnchorRefRewrite,
) ([]byte, error) {
	if v == nil || len(v.image) == 0 || len(rewrites) == 0 ||
		len(rootDst) < SegmentedTabletRouterRootBytes ||
		generation <= v.inner.generation ||
		generation >= uint64(1)<<48 {
		return nil, fmt.Errorf(
			"%w: global tablet batched root geometry",
			ErrInvalidWrite,
		)
	}
	nextRoot := rootDst[:SegmentedTabletRouterRootBytes]
	copy(nextRoot, v.inner.root)
	binary.LittleEndian.PutUint64(nextRoot[24:32], generation)
	for rank, rewrite := range rewrites {
		if rewrite.PageID >= SegmentedTabletRouterMaxPages ||
			rewrite.Ref.Generation != generation ||
			segmentedTabletRouterValidateAnchorRefIdentity(
				rewrite.Ref, v.inner.tabletID, generation,
				rewrite.PageID,
			) != nil {
			return nil, fmt.Errorf(
				"%w: global tablet batched root anchor",
				ErrInvalidWrite,
			)
		}
		if _, ok := v.inner.anchorRef(rewrite.PageID); !ok {
			return nil, fmt.Errorf(
				"%w: global tablet batched root page",
				ErrInvalidWrite,
			)
		}
		for prior := range rank {
			if rewrites[prior].PageID == rewrite.PageID {
				return nil, fmt.Errorf(
					"%w: duplicate global tablet root page",
					ErrInvalidWrite,
				)
			}
		}
		segmentedTabletRouterEncodeAnchorRef(
			nextRoot[segmentedTabletRouterRootRefsAt+
				int(rewrite.PageID)*segmentedTabletRouterRootRefBytes:],
			rewrite.Ref,
		)
	}
	segmentedTabletRouterSeal(
		nextRoot, segmentedTabletRouterRootTrailerAt,
	)
	return nextRoot, nil
}

func (v *GlobalTabletCatalogAnchorView) RouteHashed(
	hash uint64, key []byte,
) (SegmentedTabletRouterRoute, bool) {
	if v == nil || len(v.page.image) == 0 {
		return SegmentedTabletRouterRoute{}, false
	}
	return v.page.routeAt(v.page.upperBound(key)-1, hash), true
}

// LowerBound returns the current leaf interval and its lexical row rank.
// The selected leaf can end before key when a shortest separator leaves a
// keyless gap; callers continue with the next rooted anchor row in that case.
func (v *GlobalTabletCatalogAnchorView) LowerBound(
	key []byte,
) (int, SegmentedTabletRouterRoute, bool) {
	if v == nil || len(v.page.image) == 0 {
		return 0, SegmentedTabletRouterRoute{}, false
	}
	rank := v.page.upperBound(key) - 1
	return rank, v.page.routeAt(rank, 0), true
}

// Count returns the number of lexical leaf rows in this anchor page.
func (v *GlobalTabletCatalogAnchorView) Count() int {
	if v == nil {
		return 0
	}
	return int(v.page.count)
}

// RouteAt returns one leaf route in lexical rank order.
func (v *GlobalTabletCatalogAnchorView) RouteAt(
	rank int, hash uint64,
) (SegmentedTabletRouterRoute, bool) {
	if v == nil || rank < 0 || rank >= int(v.page.count) {
		return SegmentedTabletRouterRoute{}, false
	}
	return v.page.routeAt(rank, hash), true
}

// AppendFenceAt appends the lexical fence at rank to dst in flat form. Rank 0
// of the first anchor page is the empty tablet floor. It is the enumeration
// counterpart to RouteAt: a structural split/merge transaction rebuilds a
// tablet from its current leaf set, pairing each RouteAt handle with its fence
// to feed EncodeSegmentedTabletRouter. The appended bytes are owned by the
// caller because the source anchor page can retire in the same transaction.
// Supplying sufficient spare capacity makes the operation allocation-free.
func (v *GlobalTabletCatalogAnchorView) AppendFenceAt(
	dst []byte, rank int,
) ([]byte, bool) {
	if v == nil {
		return dst, false
	}
	fence, ok := v.page.fenceAtChecked(rank)
	if !ok {
		return dst, false
	}
	start := len(dst)
	dst = append(dst, make([]byte, fence.length())...)
	fence.copyTo(dst[start:], 0)
	return dst, true
}

// FenceAt returns a freshly allocated owned copy. Structural callers that
// enumerate more than one row should use AppendFenceAt with a shared arena.
func (v *GlobalTabletCatalogAnchorView) FenceAt(rank int) ([]byte, bool) {
	return v.AppendFenceAt(nil, rank)
}

// ResolveBucket is the posting path. It verifies tablet identity, live locator
// state, selected anchor PageRef, and the inverse LocalID row binding.
func (v *GlobalTabletCatalogAnchorView) ResolveBucket(
	locator *GlobalTabletCatalogLocatorView, bucket BucketID,
) (PageRef, BucketZone, bool) {
	if v == nil || locator == nil {
		return PageRef{}, BucketZone{}, false
	}
	tabletID, localID, ok := SplitTabletLocalIdentityBucket(uint32(bucket))
	if !ok || tabletID != v.tabletID || locator.tabletID != tabletID {
		return PageRef{}, BucketZone{}, false
	}
	if locator.ref != v.locator {
		return PageRef{}, BucketZone{}, false
	}
	pageID, rowSlot, state := locator.Resolve(localID)
	if state != GlobalTabletCatalogLocatorLive || pageID != v.page.pageID ||
		binary.LittleEndian.Uint16(v.page.localIDs[int(rowSlot)*2:]) != localID {
		return PageRef{}, BucketZone{}, false
	}
	return v.page.handleAt(rowSlot, bucket)
}

func globalTabletCatalogNodePageBytes(
	level GlobalTabletCatalogNodeLevel,
) (int, error) {
	switch level {
	case GlobalTabletCatalogLeaf, GlobalTabletCatalogBranch:
		return GlobalTabletCatalogNodeBytes, nil
	case GlobalTabletCatalogRoot:
		return GlobalTabletCatalogRootBytes, nil
	default:
		return 0, fmt.Errorf("%w: catalog node level", ErrInvalidWrite)
	}
}

func globalTabletCatalogChooseHeadBytes(
	entries []GlobalTabletCatalogNodeEntry, spare int,
) int {
	for _, width := range [...]int{4, 2, 1} {
		if spare < len(entries)*width {
			continue
		}
		valid := true
		for at := 1; at < len(entries); at++ {
			if len(entries[at].Floor) < width {
				valid = false
				break
			}
		}
		if valid {
			return width
		}
	}
	return 0
}

func globalTabletCatalogHeadMatches(
	head []byte, width int, parts ...[]byte,
) bool {
	at := 0
	for _, part := range parts {
		for _, value := range part {
			if at == width {
				return true
			}
			if head[at] != value {
				return false
			}
			at++
		}
	}
	return at == width
}

func globalTabletCatalogNodeIdentity(
	level GlobalTabletCatalogNodeLevel, pageID uint32,
) (uint64, uint32, bool) {
	switch level {
	case GlobalTabletCatalogLeaf:
		if pageID >= GlobalTabletCatalogMaxLeafPages {
			return 0, 0, false
		}
		id, _ := GlobalTabletCatalogCatalogLeafLogicalID(pageID)
		return id, GlobalTabletCatalogTabletBytes, true
	case GlobalTabletCatalogBranch:
		if pageID >= GlobalTabletCatalogMaxBranchPages {
			return 0, 0, false
		}
		id, _ := GlobalTabletCatalogCatalogBranchLogicalID(pageID)
		return id, GlobalTabletCatalogNodeBytes, true
	case GlobalTabletCatalogRoot:
		if pageID != 0 {
			return 0, 0, false
		}
		// The root may point directly to leaves or to rare branches. The
		// length is identical; child logical derivation uses the encoded level.
		return GlobalTabletCatalogRootLogicalID, GlobalTabletCatalogNodeBytes, true
	default:
		return 0, 0, false
	}
}

func globalTabletCatalogChildLevel(
	level, requested GlobalTabletCatalogNodeLevel,
) (GlobalTabletCatalogNodeLevel, bool) {
	switch level {
	case GlobalTabletCatalogLeaf, GlobalTabletCatalogBranch:
		return GlobalTabletCatalogLeaf, true
	case GlobalTabletCatalogRoot:
		if requested == GlobalTabletCatalogLeaf ||
			requested == GlobalTabletCatalogBranch {
			return requested, true
		}
		return 0, false
	default:
		return 0, false
	}
}

func globalTabletCatalogNodeBucket(
	level, childLevel GlobalTabletCatalogNodeLevel, id uint32,
) (BucketID, bool) {
	switch level {
	case GlobalTabletCatalogLeaf:
		bucket, ok := MakeTabletLocalIdentityBucket(id, 0)
		return BucketID(bucket), ok
	case GlobalTabletCatalogBranch:
		return BucketID(id), id < GlobalTabletCatalogMaxLeafPages
	case GlobalTabletCatalogRoot:
		if childLevel == GlobalTabletCatalogLeaf {
			return BucketID(id), id < GlobalTabletCatalogMaxLeafPages
		}
		return BucketID(id), childLevel == GlobalTabletCatalogBranch &&
			id < GlobalTabletCatalogMaxBranchPages
	default:
		return 0, false
	}
}

func globalTabletCatalogNodeID(
	level, childLevel GlobalTabletCatalogNodeLevel, bucket BucketID,
) (uint32, bool) {
	switch level {
	case GlobalTabletCatalogLeaf:
		tabletID, localID, ok := SplitTabletLocalIdentityBucket(uint32(bucket))
		return tabletID, ok && localID == 0
	case GlobalTabletCatalogBranch:
		id := uint32(bucket)
		return id, id < GlobalTabletCatalogMaxLeafPages
	case GlobalTabletCatalogRoot:
		id := uint32(bucket)
		if childLevel == GlobalTabletCatalogLeaf {
			return id, id < GlobalTabletCatalogMaxLeafPages
		}
		return id, childLevel == GlobalTabletCatalogBranch &&
			id < GlobalTabletCatalogMaxBranchPages
	default:
		return 0, false
	}
}

func globalTabletCatalogChildLogicalID(
	level, childLevel GlobalTabletCatalogNodeLevel, id uint32,
) (uint64, bool) {
	switch level {
	case GlobalTabletCatalogLeaf:
		return GlobalTabletCatalogTabletRootLogicalID(id)
	case GlobalTabletCatalogBranch:
		return GlobalTabletCatalogCatalogLeafLogicalID(id)
	case GlobalTabletCatalogRoot:
		if childLevel == GlobalTabletCatalogLeaf {
			return GlobalTabletCatalogCatalogLeafLogicalID(id)
		}
		if childLevel == GlobalTabletCatalogBranch {
			return GlobalTabletCatalogCatalogBranchLogicalID(id)
		}
		return 0, false
	default:
		return 0, false
	}
}

func globalTabletCatalogValidatePackedRef(
	ref PageRef, logicalID uint64, kind PageKind, length uint32,
	selectingGeneration uint64, bounds GlobalTabletCatalogBounds,
) error {
	if ref.Offset == 0 || ref.Offset&4095 != 0 ||
		ref.Offset>>3 >= uint64(1)<<48 ||
		ref.LogicalID != logicalID || ref.Generation == 0 ||
		ref.Generation >= uint64(1)<<48 ||
		ref.Generation > selectingGeneration || ref.Length != length ||
		ref.Kind != kind ||
		!bounds.contains(ref) {
		return fmt.Errorf("%w: packed catalog reference", ErrInvalidWrite)
	}
	return nil
}

func globalTabletCatalogValidateFullRef(
	ref PageRef, logicalID uint64, kind PageKind, length int,
	selectingGeneration uint64, bounds GlobalTabletCatalogBounds,
) error {
	if ref.Offset == 0 || ref.Offset&4095 != 0 ||
		ref.LogicalID != logicalID || ref.Generation == 0 ||
		ref.Generation >= uint64(1)<<48 ||
		ref.Generation > selectingGeneration ||
		ref.Length != uint32(length) || ref.Kind != kind || !bounds.contains(ref) {
		return fmt.Errorf("%w: full tablet reference", ErrInvalidWrite)
	}
	return nil
}

func globalTabletCatalogEncodePackedRef(dst []byte, ref PageRef) {
	segmentedTabletRouterPutUint48(dst, ref.Offset>>3)
	segmentedTabletRouterPutUint48(dst[6:], ref.Generation)
}

func globalTabletCatalogHeaderMatchesRef(
	header PageHeader, ref PageRef, bounds GlobalTabletCatalogBounds,
) bool {
	return ref.Offset != 0 && ref.Offset&4095 == 0 &&
		header.StoreID == bounds.StoreID &&
		header.LogicalID == ref.LogicalID &&
		header.Generation == ref.Generation &&
		header.PageSize == ref.Length && header.Kind == ref.Kind && bounds.contains(ref)
}

func (b GlobalTabletCatalogBounds) valid() bool {
	return b.StoreID != ([16]byte{}) &&
		b.SelectedRootGeneration != 0 &&
		b.SelectedRootGeneration < uint64(1)<<48 &&
		b.FileEnd >= GlobalTabletCatalogRootBytes &&
		b.NextLogicalID >= GlobalTabletCatalogFirstDynamicLogicalID
}

func (b GlobalTabletCatalogBounds) contains(ref PageRef) bool {
	length := uint64(ref.Length)
	return b.valid() && ref.LogicalID != 0 &&
		ref.LogicalID < b.NextLogicalID &&
		ref.Generation != 0 &&
		ref.Generation <= b.SelectedRootGeneration &&
		length != 0 && length <= b.FileEnd &&
		ref.Offset <= b.FileEnd-length
}

func (b GlobalTabletCatalogBounds) extends(previous GlobalTabletCatalogBounds) bool {
	return b.valid() && previous.valid() &&
		b.StoreID == previous.StoreID &&
		b.SelectedRootGeneration >= previous.SelectedRootGeneration &&
		b.FileEnd >= previous.FileEnd &&
		b.NextLogicalID >= previous.NextLogicalID
}

func globalTabletCatalogPut14(dst []byte, localID uint16, code uint16) {
	bit := int(localID) * globalTabletCatalogPackedBits
	at, shift := bit>>3, uint(bit&7)
	word := uint32(code&0x3fff) << shift
	dst[at] |= byte(word)
	dst[at+1] |= byte(word >> 8)
	if at+2 < len(dst) {
		dst[at+2] |= byte(word >> 16)
	}
}

func globalTabletCatalogGet14(src []byte, localID uint16) uint16 {
	bit := int(localID) * globalTabletCatalogPackedBits
	at, shift := bit>>3, uint(bit&7)
	word := uint32(src[at]) | uint32(src[at+1])<<8
	if at+2 < len(src) {
		word |= uint32(src[at+2]) << 16
	}
	return uint16(word>>shift) & 0x3fff
}

func (v *globalTabletCatalogSegmentedRootView) rootUpperBound(key []byte) int {
	low, high := 1, int(v.pageCount)
	for low < high {
		mid := int(uint(low+high) >> 1)
		fence := v.rootFence(mid).a
		if bytes.Compare(fence, key) <= 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low
}

func (v *globalTabletCatalogSegmentedRootView) rootFence(
	rank int,
) segmentedTabletRouterFence {
	start := int(binary.LittleEndian.Uint16(v.rootOffsets[rank*2:]))
	end := int(binary.LittleEndian.Uint16(v.rootOffsets[(rank+1)*2:]))
	return segmentedTabletRouterFence{a: v.rootKeys[start:end]}
}

func (v *globalTabletCatalogSegmentedRootView) anchorRef(
	pageID uint8,
) (PageRef, bool) {
	if pageID >= SegmentedTabletRouterMaxPages {
		return PageRef{}, false
	}
	start := int(pageID) * segmentedTabletRouterRootRefBytes
	src := v.rootRefs[start : start+segmentedTabletRouterRootRefBytes]
	if allZero(src) {
		return PageRef{}, false
	}
	logicalID, ok := GlobalTabletCatalogAnchorLogicalID(v.tabletID, pageID)
	if !ok {
		return PageRef{}, false
	}
	ref := PageRef{
		Offset:     segmentedTabletRouterGetUint48(src) << 12,
		LogicalID:  logicalID,
		Generation: segmentedTabletRouterGetUint48(src[6:]),
		Length:     uint32(4096) << src[12],
		Kind:       v.anchorKind,
	}
	return ref, segmentedTabletRouterValidateAnchorRef(
		ref,
		SegmentedTabletRouterHeader{
			StoreID: v.storeID, TabletID: v.tabletID,
			Generation: v.generation,
			AnchorKind: v.anchorKind, LeafKind: v.leafKind,
		},
		pageID,
	) == nil
}

func (v globalTabletCatalogSegmentedRootView) segmentedView() SegmentedTabletRouterView {
	return SegmentedTabletRouterView{
		root: v.root, rootRefs: v.rootRefs, rootRanks: v.rootRanks,
		rootOffsets: v.rootOffsets, rootKeys: v.rootKeys,
		storeID: v.storeID, tabletID: v.tabletID, generation: v.generation,
		pageCount: v.pageCount, anchorKind: v.anchorKind, leafKind: v.leafKind,
	}
}

func globalTabletCatalogRootRefsWithinBounds(
	root globalTabletCatalogSegmentedRootView,
	bounds GlobalTabletCatalogBounds,
) error {
	for rank := 0; rank < int(root.pageCount); rank++ {
		pageID := root.rootRanks[rank]
		ref, ok := root.anchorRef(pageID)
		if !ok || !bounds.contains(ref) {
			return fmt.Errorf("%w: tablet anchor bounds", ErrInvalidWrite)
		}
	}
	return nil
}

func globalTabletCatalogOpenSegmentedRootOnly(
	root []byte,
) (globalTabletCatalogSegmentedRootView, error) {
	var view globalTabletCatalogSegmentedRootView
	if len(root) != SegmentedTabletRouterRootBytes ||
		string(root[:8]) != segmentedTabletRouterRootMagic ||
		binary.LittleEndian.Uint32(root[8:12]) != segmentedTabletRouterVersion ||
		binary.LittleEndian.Uint16(root[12:14]) != segmentedTabletRouterRootHeaderBytes ||
		root[14] == 0 || root[14] > SegmentedTabletRouterMaxPages ||
		PageKind(root[15]) != PagePrimaryAnchor ||
		PageKind(root[16]) != PagePrimaryLeaf ||
		!allZero(root[17:20]) || allZero(root[44:60]) ||
		!allZero(root[60:segmentedTabletRouterRootHeaderBytes]) ||
		!segmentedTabletRouterChecksumOK(root, segmentedTabletRouterRootTrailerAt) {
		return view, globalTabletCatalogCorrupt("segmented root envelope")
	}
	pageCount := int(root[14])
	tabletID := binary.LittleEndian.Uint32(root[20:24])
	generation := binary.LittleEndian.Uint64(root[24:32])
	keyBytes := int(binary.LittleEndian.Uint16(root[32:34]))
	if tabletID >= TabletLocalIdentityTabletCount ||
		generation == 0 || generation >= uint64(1)<<48 ||
		int(binary.LittleEndian.Uint16(root[34:36])) != pageCount ||
		binary.LittleEndian.Uint32(root[40:44]) != SegmentedTabletRouterRootBytes ||
		keyBytes > segmentedTabletRouterRootTrailerAt-segmentedTabletRouterRootKeysAt {
		return globalTabletCatalogSegmentedRootView{},
			globalTabletCatalogCorrupt("segmented root identity")
	}
	view = globalTabletCatalogSegmentedRootView{
		root:        root,
		rootRefs:    root[segmentedTabletRouterRootRefsAt:segmentedTabletRouterRootRanksAt],
		rootRanks:   root[segmentedTabletRouterRootRanksAt:segmentedTabletRouterRootOffsetsAt],
		rootOffsets: root[segmentedTabletRouterRootOffsetsAt:segmentedTabletRouterRootKeysAt],
		rootKeys:    root[segmentedTabletRouterRootKeysAt : segmentedTabletRouterRootKeysAt+keyBytes],
		storeID:     [16]byte(root[44:60]),
		tabletID:    tabletID, generation: generation, pageCount: uint8(pageCount),
		anchorKind: PageKind(root[15]), leafKind: PageKind(root[16]),
	}
	if binary.LittleEndian.Uint16(view.rootOffsets[pageCount*2:]) != uint16(keyBytes) ||
		!allZero(root[segmentedTabletRouterRootRanksAt+pageCount:segmentedTabletRouterRootOffsetsAt]) ||
		!allZero(root[segmentedTabletRouterRootOffsetsAt+(pageCount+1)*2:segmentedTabletRouterRootKeysAt]) ||
		!allZero(root[segmentedTabletRouterRootKeysAt+keyBytes:segmentedTabletRouterRootTrailerAt]) {
		return globalTabletCatalogSegmentedRootView{},
			globalTabletCatalogCorrupt("segmented root sections")
	}
	var seen uint16
	var previous segmentedTabletRouterFence
	for rank := range pageCount {
		start := int(binary.LittleEndian.Uint16(view.rootOffsets[rank*2:]))
		end := int(binary.LittleEndian.Uint16(view.rootOffsets[(rank+1)*2:]))
		pageID := view.rootRanks[rank]
		if start > end || end > keyBytes ||
			pageID >= SegmentedTabletRouterMaxPages ||
			seen&(uint16(1)<<pageID) != 0 {
			return globalTabletCatalogSegmentedRootView{},
				globalTabletCatalogCorrupt("segmented root order")
		}
		seen |= uint16(1) << pageID
		fence := view.rootFence(rank)
		if rank == 0 && fence.length() != 0 ||
			rank != 0 && segmentedTabletRouterCompareFences(previous, fence) >= 0 {
			return globalTabletCatalogSegmentedRootView{},
				globalTabletCatalogCorrupt("segmented root floors")
		}
		if _, ok := view.anchorRef(pageID); !ok {
			return globalTabletCatalogSegmentedRootView{},
				globalTabletCatalogCorrupt("segmented anchor reference")
		}
		previous = fence
	}
	for pageID := range SegmentedTabletRouterMaxPages {
		start := pageID * segmentedTabletRouterRootRefBytes
		if seen&(uint16(1)<<pageID) == 0 &&
			!allZero(view.rootRefs[start:start+segmentedTabletRouterRootRefBytes]) {
			return globalTabletCatalogSegmentedRootView{},
				globalTabletCatalogCorrupt("segmented unused reference")
		}
	}
	return view, nil
}

func admittedGlobalTabletCatalogSegmentedRootOnly(
	root []byte,
) globalTabletCatalogSegmentedRootView {
	pageCount := int(root[14])
	keyBytes := int(binary.LittleEndian.Uint16(root[32:34]))
	return globalTabletCatalogSegmentedRootView{
		root:        root,
		rootRefs:    root[segmentedTabletRouterRootRefsAt:segmentedTabletRouterRootRanksAt],
		rootRanks:   root[segmentedTabletRouterRootRanksAt:segmentedTabletRouterRootOffsetsAt],
		rootOffsets: root[segmentedTabletRouterRootOffsetsAt:segmentedTabletRouterRootKeysAt],
		rootKeys:    root[segmentedTabletRouterRootKeysAt : segmentedTabletRouterRootKeysAt+keyBytes],
		storeID:     [16]byte(root[44:60]),
		tabletID:    binary.LittleEndian.Uint32(root[20:24]),
		generation:  binary.LittleEndian.Uint64(root[24:32]),
		pageCount:   uint8(pageCount),
		anchorKind:  PageKind(root[15]),
		leafKind:    PageKind(root[16]),
	}
}

func globalTabletCatalogCorrupt(detail string) error {
	return fmt.Errorf("%w: %s", ErrGlobalTabletCatalogCorrupt, detail)
}

func globalTabletCatalogSlicesOverlap(left, right []byte) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	leftStart := uintptr(unsafe.Pointer(unsafe.SliceData(left)))
	rightStart := uintptr(unsafe.Pointer(unsafe.SliceData(right)))
	return leftStart < rightStart+uintptr(len(right)) &&
		rightStart < leftStart+uintptr(len(left))
}
