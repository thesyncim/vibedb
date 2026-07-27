package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

// The fused tablet route block removes the independently cached TabletRoot
// from a point read. One checksum-bound 8 KiB block contains:
//
//   - the exact lexical floors for up to 256 stable, non-monotonic TabletIDs;
//   - one exact locator PageRef per tablet;
//   - each tablet's compact lexical anchor-root and exact anchor PageRefs; and
//   - an exact stable-slot inverse for the direct tablet directory.
//
// Stable slots are physical positions inside one route block and do not change
// when lexical ranks move. The direct directory is the only TabletID index:
// posting reads acquire the block from TabletID -> (BlockID, stable slot), then
// use the block's compact bitmap/rank inverse without hashing or searching.
//
// Generation is always physical birth. Admission validates the block and every
// reference against the selected snapshot-root generation; it deliberately
// does not require a child reference to be older than the block that names it.
const (
	FusedTabletRouteBlockLabBytes       = 8 << 10
	FusedTabletRouteBlockLabRootBytes   = 128 << 10
	FusedTabletRouteBlockLabBranchBytes = 64 << 10
	FusedTabletRouteBlockLabHeaderBytes = 64
	FusedTabletRouteBlockLabMaxTablets  = 256
	FusedTabletRouteBlockLabMaxFence    = 256

	fusedTabletRouteBlockLabPackedRefBytes       = 12
	fusedTabletRouteBlockLabStableBitmapBytes    = 32
	fusedTabletRouteBlockLabStableRankBytes      = 256
	fusedTabletRouteBlockLabStableDirectoryBytes = fusedTabletRouteBlockLabStableBitmapBytes +
		fusedTabletRouteBlockLabStableRankBytes
	fusedTabletRouteBlockLabDescriptorHeader   = 16
	fusedTabletRouteBlockLabDescriptorRefBytes = 13
	fusedTabletRouteBlockLabVersion            = uint32(2)

	FusedTabletRouteBlockLabLogicalIDBase  = PrimaryTabletRouteLogicalIDBase
	FusedTabletRouteBlockLabLogicalIDLimit = PrimaryTabletRouteLogicalIDLimit
	FusedTabletRouteBlockLabFirstDynamicID = PrimaryFirstDynamicLogicalID
)

var (
	ErrFusedTabletRouteBlockLabCorrupt = errors.New(
		"vibejson: corrupt fused tablet route block lab page",
	)
	ErrFusedTabletRouteBlockLabNoSpace = errors.New(
		"vibejson: fused tablet route block lab page has no space",
	)
)

type FusedTabletRouteBlockLabHeader struct {
	StoreID                [16]byte
	Generation             uint64
	SelectedRootGeneration uint64
	LogicalID              uint64
	BlockID                uint32
	Kind                   PageKind
	LocatorKind            PageKind
	AnchorKind             PageKind
	LeafKind               PageKind
	Bounds                 GlobalTabletCatalogBounds
}

type FusedTabletRouteBlockLabAnchor struct {
	Floor  []byte
	PageID uint8
	Ref    PageRef
}

type FusedTabletRouteBlockLabTablet struct {
	Floor      []byte
	TabletID   uint32
	StableSlot uint8
	Locator    PageRef
	Anchors    []FusedTabletRouteBlockLabAnchor
}

// FusedTabletRouteBlockLabView is one borrowed cache frame. It deliberately
// retains only byte slices and scalars; admission creates no per-tablet Go
// object graph.
type FusedTabletRouteBlockLabView struct {
	image                  []byte
	payload                []byte
	floors                 TabletAnchorMapLabView
	descriptorOffsets      []byte
	stablePresence         []byte
	stableRanks            []byte
	descriptors            []byte
	header                 PageHeader
	ref                    PageRef
	bounds                 GlobalTabletCatalogBounds
	selectedRootGeneration uint64
	blockID                uint32
	count                  uint16
	locatorKind            PageKind
	anchorKind             PageKind
	leafKind               PageKind
}

type FusedTabletRouteBlockLabTabletView struct {
	owner       *FusedTabletRouteBlockLabView
	descriptor  []byte
	anchorRows  []byte
	rootOffsets []byte
	rootCommon  []byte
	rootKeys    []byte
	locator     PageRef
	tabletID    uint32
	ordinal     uint16
	stableSlot  uint8
	pageCount   uint8
}

type FusedTabletRouteBlockLabRoute struct {
	BlockRef      PageRef
	TabletID      uint32
	TabletOrdinal uint16
	TabletSlot    uint8
	LocatorRef    PageRef
	PageID        uint8
	AnchorRef     PageRef

	owner      *FusedTabletRouteBlockLabView
	anchorRank uint8
}

type FusedTabletRouteBlockLabCursor struct {
	view    *FusedTabletRouteBlockLabView
	ordinal uint16
	valid   bool
}

type FusedTabletRouteBlockLabAnchorCursor struct {
	tablet *FusedTabletRouteBlockLabTabletView
	rank   uint8
	valid  bool
}

// FusedTabletRouteBlockLabOrderedCursor walks anchor pages directly across
// tablet boundaries. It is the scan hot path: locator metadata and point-route
// envelopes are never decoded while the cursor advances.
type FusedTabletRouteBlockLabOrderedCursor struct {
	view       *FusedTabletRouteBlockLabView
	anchorRows []byte
	tabletID   uint32
	ordinal    uint16
	rank       uint8
	pageCount  uint8
	valid      bool
}

type FusedTabletRouteBlockLabAnchorView struct {
	owner         *FusedTabletRouteBlockLabView
	tabletID      uint32
	tabletOrdinal uint16
	locator       PageRef
	ref           PageRef
	page          segmentedTabletRouterAnchorView
}

type FusedTabletRouteBlockLabRefMutation struct {
	TabletID       uint32
	ReplaceLocator bool
	Locator        PageRef
	ReplaceAnchor  bool
	AnchorPageID   uint8
	Anchor         PageRef
}

// FusedTabletRouteBlockLabStructuralScratch is a caller-owned, reusable arena
// for bounded route-block insert/remove COW. It deliberately retains all
// maximum-size storage inline: structural churn does not create a graph of
// tiny heap objects or depend on a garbage-collector compaction cycle.
type FusedTabletRouteBlockLabStructuralScratch struct {
	Tablets      [FusedTabletRouteBlockLabMaxTablets]FusedTabletRouteBlockLabTablet
	Anchors      [FusedTabletRouteBlockLabMaxTablets * SegmentedTabletRouterMaxPages]FusedTabletRouteBlockLabAnchor
	TabletFloors [FusedTabletRouteBlockLabMaxTablets][FusedTabletRouteBlockLabMaxFence]byte
	AnchorFloors [FusedTabletRouteBlockLabMaxTablets * SegmentedTabletRouterMaxPages][FusedTabletRouteBlockLabMaxFence]byte
}

type FusedTabletRouteBlockLabGeometry struct {
	Tablets                       uint64
	MaxFenceBytes                 int
	RouteBlockFanout              int
	BranchFanout                  int
	RootFanout                    int
	BranchLevels                  int
	RouteBlocks                   uint64
	Branches                      uint64
	MaximumTablets                uint64
	CatalogBytes                  uint64
	ResidentBytes                 uint64
	ColdPointCachePages           int
	CurrentColdPointPages         int
	FirstAnchorUpdateBytes        uint64
	CurrentFirstAnchorUpdateBytes uint64
	OwnedAnchorUpdateBytes        uint64
	CurrentOwnedAnchorUpdateBytes uint64
	SplitBytes                    uint64
	CurrentSplitBytes             uint64
}

type FusedTabletRouteBlockLabSpace struct {
	Documents        uint64
	Leaves           uint64
	Tablets          uint64
	AnchorsPerTablet uint64
	TabletsPerBlock  uint64
	RouteBlocks      uint64
	Branches         uint64
	CatalogBytes     uint64
	LocatorBytes     uint64
	AnchorBytes      uint64
	TotalBytes       uint64
	BytesPerDoc      float64
	ResidentBytes    uint64
	ColdPointPages   int
	ColdScanPages    uint64
}

func FusedTabletRouteBlockLabLogicalID(blockID uint32) (uint64, bool) {
	logicalID := FusedTabletRouteBlockLabLogicalIDBase + uint64(blockID)
	return logicalID, logicalID < FusedTabletRouteBlockLabLogicalIDLimit
}

func EncodeFusedTabletRouteBlockLab(
	dst []byte,
	header FusedTabletRouteBlockLabHeader,
	tablets []FusedTabletRouteBlockLabTablet,
) ([]byte, error) {
	wantLogicalID, logicalOK := FusedTabletRouteBlockLabLogicalID(header.BlockID)
	if len(dst) < FusedTabletRouteBlockLabBytes ||
		len(tablets) == 0 || len(tablets) > FusedTabletRouteBlockLabMaxTablets ||
		!logicalOK || header.LogicalID != wantLogicalID ||
		header.StoreID == ([16]byte{}) ||
		header.Generation == 0 || header.Generation >= uint64(1)<<48 ||
		header.SelectedRootGeneration == 0 ||
		header.SelectedRootGeneration >= uint64(1)<<48 ||
		header.Generation > header.SelectedRootGeneration ||
		header.StoreID != header.Bounds.StoreID ||
		header.SelectedRootGeneration !=
			header.Bounds.SelectedRootGeneration ||
		!header.Bounds.valid() || header.LogicalID >= header.Bounds.NextLogicalID ||
		!fusedTabletRouteBlockLabKindsValid(
			header.Kind, header.LocatorKind, header.AnchorKind, header.LeafKind,
		) {
		return nil, fmt.Errorf("%w: fused route-block identity", ErrInvalidWrite)
	}
	if len(tablets[0].Floor) != 0 {
		return nil, fmt.Errorf("%w: first fused tablet floor", ErrInvalidWrite)
	}

	outerAnchors := make([]TabletAnchorMapLabAnchor, len(tablets)-1)
	descriptorBytes := 0
	var stableSlots [4]uint64
	for at, tablet := range tablets {
		slotWord := tablet.StableSlot >> 6
		slotBit := uint64(1) << (tablet.StableSlot & 63)
		if tablet.TabletID >= TabletLocalIdentityTabletCount ||
			len(tablet.Floor) > FusedTabletRouteBlockLabMaxFence ||
			at != 0 && (len(tablet.Floor) == 0 ||
				bytes.Compare(tablets[at-1].Floor, tablet.Floor) >= 0) ||
			stableSlots[slotWord]&slotBit != 0 {
			return nil, fmt.Errorf("%w: fused tablet floor", ErrInvalidWrite)
		}
		stableSlots[slotWord] |= slotBit
		for prior := 0; prior < at; prior++ {
			if tablets[prior].TabletID == tablet.TabletID {
				return nil, fmt.Errorf("%w: duplicate fused TabletID", ErrInvalidWrite)
			}
		}
		if err := fusedTabletRouteBlockLabValidateTabletInput(
			tablet, header.LocatorKind, header.AnchorKind,
			header.SelectedRootGeneration, header.Bounds,
		); err != nil {
			return nil, err
		}
		descriptorBytes += fusedTabletRouteBlockLabDescriptorBytes(tablet.Anchors)
		if descriptorBytes > int(^uint16(0)) {
			return nil, ErrFusedTabletRouteBlockLabNoSpace
		}
		if at != 0 {
			bucket, _ := MakeTabletLocalIdentityBucket(tablet.TabletID, 0)
			outerAnchors[at-1] = TabletAnchorMapLabAnchor{
				Fence: tablet.Floor, Bucket: BucketID(bucket),
			}
		}
	}
	firstBucket, _ := MakeTabletLocalIdentityBucket(tablets[0].TabletID, 0)
	common, _, keyBytes, err := tabletAnchorMapLabMeasure(outerAnchors)
	if err != nil {
		return nil, err
	}
	mapBytes := tabletAnchorMapLabImageBytes(len(outerAnchors), common, keyBytes)
	offsetBytes := (len(tablets) + 1) * 2
	payloadBytes := FusedTabletRouteBlockLabHeaderBytes + mapBytes +
		offsetBytes + fusedTabletRouteBlockLabStableDirectoryBytes +
		descriptorBytes
	if payloadBytes > FusedTabletRouteBlockLabBytes-PageHeaderSize-PageTrailerSize ||
		mapBytes > int(^uint16(0)) {
		return nil, ErrFusedTabletRouteBlockLabNoSpace
	}

	payload, err := InitPage(dst[:FusedTabletRouteBlockLabBytes], PageHeader{
		StoreID: header.StoreID, Generation: header.Generation,
		LogicalID: header.LogicalID, PageSize: FusedTabletRouteBlockLabBytes,
		PayloadLength: uint32(payloadBytes), Kind: header.Kind,
	})
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(payload[0:4], fusedTabletRouteBlockLabVersion)
	binary.LittleEndian.PutUint32(payload[4:8], header.BlockID)
	binary.LittleEndian.PutUint16(payload[8:10], uint16(len(tablets)))
	binary.LittleEndian.PutUint16(payload[10:12], uint16(mapBytes))
	binary.LittleEndian.PutUint16(payload[12:14], uint16(descriptorBytes))
	payload[14] = byte(header.LocatorKind)
	payload[15] = byte(header.AnchorKind)
	payload[16] = byte(header.LeafKind)
	payload[17] = 1
	payload[18] = 2
	binary.LittleEndian.PutUint16(
		payload[19:21], fusedTabletRouteBlockLabStableDirectoryBytes,
	)

	mapAt := FusedTabletRouteBlockLabHeaderBytes
	if _, err := EncodeTabletAnchorMapLab(
		payload[mapAt:mapAt+mapBytes],
		TabletAnchorMapLabHeader{
			TabletID: header.LogicalID, Generation: header.Generation,
		},
		BucketID(firstBucket), outerAnchors,
	); err != nil {
		return nil, err
	}
	offsetAt := mapAt + mapBytes
	stableAt := offsetAt + offsetBytes
	descriptorAt := stableAt + fusedTabletRouteBlockLabStableDirectoryBytes
	stablePresence := payload[stableAt : stableAt+fusedTabletRouteBlockLabStableBitmapBytes]
	stableRanks := payload[stableAt+fusedTabletRouteBlockLabStableBitmapBytes : descriptorAt]
	running := 0
	for at, tablet := range tablets {
		binary.LittleEndian.PutUint16(
			payload[offsetAt+at*2:], uint16(running),
		)
		stablePresence[tablet.StableSlot>>3] |=
			byte(1) << (tablet.StableSlot & 7)
		stableRanks[tablet.StableSlot] = byte(at)
		wrote, err := fusedTabletRouteBlockLabEncodeDescriptor(
			payload[descriptorAt+running:], tablet, header.SelectedRootGeneration,
			header.Bounds, header.LocatorKind, header.AnchorKind,
		)
		if err != nil {
			return nil, err
		}
		running += wrote
	}
	binary.LittleEndian.PutUint16(
		payload[offsetAt+len(tablets)*2:], uint16(running),
	)
	if _, err := sealInitializedPage(dst[:FusedTabletRouteBlockLabBytes]); err != nil {
		return nil, err
	}
	return dst[:FusedTabletRouteBlockLabBytes], nil
}

func OpenFusedTabletRouteBlockLab(
	src []byte, expected PageRef, selectedRootGeneration uint64,
	bounds GlobalTabletCatalogBounds,
) (FusedTabletRouteBlockLabView, error) {
	var view FusedTabletRouteBlockLabView
	header, payload, err := OpenPage(src)
	if err != nil || selectedRootGeneration == 0 ||
		selectedRootGeneration >= uint64(1)<<48 ||
		selectedRootGeneration != bounds.SelectedRootGeneration ||
		expected.Generation > selectedRootGeneration || !bounds.valid() ||
		!globalTabletCatalogHeaderMatchesRef(header, expected, bounds) ||
		header.PageSize != FusedTabletRouteBlockLabBytes ||
		len(payload) < FusedTabletRouteBlockLabHeaderBytes ||
		binary.LittleEndian.Uint32(payload[0:4]) !=
			fusedTabletRouteBlockLabVersion ||
		payload[17] != 1 ||
		payload[18] != 2 ||
		binary.LittleEndian.Uint16(payload[19:21]) !=
			fusedTabletRouteBlockLabStableDirectoryBytes ||
		!allZero(payload[21:FusedTabletRouteBlockLabHeaderBytes]) ||
		!fusedTabletRouteBlockLabKindsValid(
			header.Kind, PageKind(payload[14]), PageKind(payload[15]),
			PageKind(payload[16]),
		) {
		return view, fusedTabletRouteBlockLabCorrupt("header")
	}
	blockID := binary.LittleEndian.Uint32(payload[4:8])
	logicalID, logicalOK := FusedTabletRouteBlockLabLogicalID(blockID)
	count := int(binary.LittleEndian.Uint16(payload[8:10]))
	mapBytes := int(binary.LittleEndian.Uint16(payload[10:12]))
	descriptorBytes := int(binary.LittleEndian.Uint16(payload[12:14]))
	offsetBytes := (count + 1) * 2
	if !logicalOK || logicalID != header.LogicalID ||
		count == 0 || count > FusedTabletRouteBlockLabMaxTablets ||
		mapBytes < TabletAnchorMapLabHeaderSize+TabletAnchorMapLabTrailerSize ||
		FusedTabletRouteBlockLabHeaderBytes+mapBytes+offsetBytes+
			fusedTabletRouteBlockLabStableDirectoryBytes+
			descriptorBytes != len(payload) {
		return FusedTabletRouteBlockLabView{},
			fusedTabletRouteBlockLabCorrupt("section geometry")
	}
	mapAt := FusedTabletRouteBlockLabHeaderBytes
	floors, err := OpenTabletAnchorMapLab(payload[mapAt : mapAt+mapBytes])
	if err != nil || floors.Header().TabletID != header.LogicalID ||
		// The map is an embedded section, not an independently shared page.
		// Its birth must therefore be exactly the enclosing block birth.
		// Accepting an ancestor generation makes equivalent COW results encode
		// differently depending on their history and defeats content dedup.
		floors.Header().Generation != header.Generation ||
		floors.BucketCount() != count {
		return FusedTabletRouteBlockLabView{},
			fusedTabletRouteBlockLabCorrupt("tablet floors")
	}
	offsetAt := mapAt + mapBytes
	stableAt := offsetAt + offsetBytes
	descriptorAt := stableAt + fusedTabletRouteBlockLabStableDirectoryBytes
	view = FusedTabletRouteBlockLabView{
		image: src[:header.PageSize], payload: payload, floors: floors,
		descriptorOffsets: payload[offsetAt:stableAt],
		stablePresence:    payload[stableAt : stableAt+fusedTabletRouteBlockLabStableBitmapBytes],
		stableRanks:       payload[stableAt+fusedTabletRouteBlockLabStableBitmapBytes : descriptorAt],
		descriptors:       payload[descriptorAt:],
		header:            header, ref: expected, bounds: bounds,
		selectedRootGeneration: selectedRootGeneration, blockID: blockID,
		count: uint16(count), locatorKind: PageKind(payload[14]),
		anchorKind: PageKind(payload[15]), leafKind: PageKind(payload[16]),
	}
	if binary.LittleEndian.Uint16(view.descriptorOffsets) != 0 ||
		int(binary.LittleEndian.Uint16(
			view.descriptorOffsets[count*2:],
		)) != descriptorBytes {
		return FusedTabletRouteBlockLabView{},
			fusedTabletRouteBlockLabCorrupt("descriptor terminal offsets")
	}
	var seenSlots [4]uint64
	for ordinal := 0; ordinal < count; ordinal++ {
		start := int(binary.LittleEndian.Uint16(
			view.descriptorOffsets[ordinal*2:],
		))
		end := int(binary.LittleEndian.Uint16(
			view.descriptorOffsets[(ordinal+1)*2:],
		))
		if start >= end || end > descriptorBytes {
			return FusedTabletRouteBlockLabView{},
				fusedTabletRouteBlockLabCorrupt("descriptor offsets")
		}
		tablet, ok := view.tabletAtOrdinal(ordinal)
		if !ok {
			return FusedTabletRouteBlockLabView{},
				fusedTabletRouteBlockLabCorrupt("tablet descriptor")
		}
		slot := tablet.stableSlot
		slotWord := slot >> 6
		slotBit := uint64(1) << (slot & 63)
		if seenSlots[slotWord]&slotBit != 0 ||
			view.stablePresence[slot>>3]&(byte(1)<<(slot&7)) == 0 ||
			int(view.stableRanks[slot]) != ordinal ||
			tablet.tabletID >= TabletLocalIdentityTabletCount {
			return FusedTabletRouteBlockLabView{},
				fusedTabletRouteBlockLabCorrupt("stable slot binding")
		}
		seenSlots[slotWord] |= slotBit
	}
	for slot := 0; slot < fusedTabletRouteBlockLabStableRankBytes; slot++ {
		word, bit := uint8(slot)>>6, uint64(1)<<(uint8(slot)&63)
		present := view.stablePresence[slot>>3]&(byte(1)<<uint(slot&7)) != 0
		seen := seenSlots[word]&bit != 0
		if present != seen || !present && view.stableRanks[slot] != 0 {
			return FusedTabletRouteBlockLabView{},
				fusedTabletRouteBlockLabCorrupt("stable slot directory")
		}
	}
	return view, nil
}

func (v *FusedTabletRouteBlockLabView) Count() int {
	if v == nil {
		return 0
	}
	return int(v.count)
}

func (v *FusedTabletRouteBlockLabView) RouteTablet(
	key []byte,
) (FusedTabletRouteBlockLabTabletView, bool) {
	if v == nil || len(v.image) == 0 {
		return FusedTabletRouteBlockLabTabletView{}, false
	}
	return v.admittedTabletAtOrdinal(v.floors.upperBound(key))
}

func (v *FusedTabletRouteBlockLabView) RouteAnchor(
	key []byte,
) (FusedTabletRouteBlockLabRoute, bool) {
	if v == nil || len(v.image) == 0 {
		return FusedTabletRouteBlockLabRoute{}, false
	}
	ordinal := v.floors.upperBound(key)
	bucket := v.floors.bucketAt(ordinal)
	tabletID, localID, ok := SplitTabletLocalIdentityBucket(uint32(bucket))
	start := int(binary.LittleEndian.Uint16(
		v.descriptorOffsets[ordinal*2:],
	))
	end := int(binary.LittleEndian.Uint16(
		v.descriptorOffsets[(ordinal+1)*2:],
	))
	if !ok || localID != 0 || start >= end || end > len(v.descriptors) {
		return FusedTabletRouteBlockLabRoute{}, false
	}
	src := v.descriptors[start:end]
	pageCount := int(src[0])
	rowsAt := fusedTabletRouteBlockLabDescriptorHeader
	offsetAt := rowsAt + pageCount*fusedTabletRouteBlockLabDescriptorRefBytes
	keysAt := offsetAt + (pageCount+1)*2
	commonBytes := int(binary.LittleEndian.Uint16(src[2:4]))
	common := src[keysAt : keysAt+commonBytes]
	keys := src[keysAt+commonBytes:]
	offsets := src[offsetAt:keysAt]
	rank := fusedTabletRouteBlockLabRootUpperBound(
		common, keys, offsets, pageCount, key,
	) - 1
	row := src[rowsAt+rank*fusedTabletRouteBlockLabDescriptorRefBytes : rowsAt+(rank+1)*fusedTabletRouteBlockLabDescriptorRefBytes]
	pageID := row[0]
	locatorLogical, _ := GlobalTabletCatalogLocatorLogicalID(tabletID)
	anchorLogical, anchorOK := GlobalTabletCatalogAnchorLogicalID(
		tabletID, pageID,
	)
	if !anchorOK {
		return FusedTabletRouteBlockLabRoute{}, false
	}
	return FusedTabletRouteBlockLabRoute{
		BlockRef: v.ref, TabletID: tabletID, TabletOrdinal: uint16(ordinal),
		TabletSlot: src[1],
		LocatorRef: fusedTabletRouteBlockLabDecodeRef(
			src[4:fusedTabletRouteBlockLabDescriptorHeader],
			locatorLogical, v.locatorKind, GlobalTabletCatalogLocatorBytes,
		),
		PageID: pageID,
		AnchorRef: fusedTabletRouteBlockLabDecodeRef(
			row[1:], anchorLogical, v.anchorKind,
			SegmentedTabletRouterAnchorPageBytes,
		),
		owner: v, anchorRank: uint8(rank),
	}, true
}

// ResolveStableSlot is the posting-driven block path. The direct tablet
// directory supplies stableSlot after its sole TabletID lookup; this exact
// inverse is one bitmap test plus one byte-indexed descriptor access.
func (v *FusedTabletRouteBlockLabView) ResolveStableSlot(
	stableSlot uint8,
) (FusedTabletRouteBlockLabTabletView, bool) {
	if v == nil || len(v.image) == 0 ||
		v.stablePresence[stableSlot>>3]&(byte(1)<<(stableSlot&7)) == 0 {
		return FusedTabletRouteBlockLabTabletView{}, false
	}
	return v.admittedTabletAtOrdinal(int(v.stableRanks[stableSlot]))
}

// TabletIDAt is the exact inverse callback used by
// DirectTabletDirectoryLabView.ValidateAcquiredBlock.
func (v *FusedTabletRouteBlockLabView) TabletIDAt(
	stableSlot uint8,
) (uint32, bool) {
	tablet, ok := v.ResolveStableSlot(stableSlot)
	if !ok || tablet.stableSlot != stableSlot {
		return 0, false
	}
	return tablet.tabletID, true
}

// ResolveTablet remains a bounded compatibility path for structural and
// diagnostic callers that have no direct-directory route. Posting reads must
// use ResolveStableSlot so this block never duplicates the global TabletID
// index.
func (v *FusedTabletRouteBlockLabView) ResolveTablet(
	bucket BucketID,
) (FusedTabletRouteBlockLabTabletView, bool) {
	if v == nil || len(v.image) == 0 {
		return FusedTabletRouteBlockLabTabletView{}, false
	}
	tabletID, _, ok := SplitTabletLocalIdentityBucket(uint32(bucket))
	if !ok {
		return FusedTabletRouteBlockLabTabletView{}, false
	}
	for ordinal := 0; ordinal < int(v.count); ordinal++ {
		tablet, valid := v.admittedTabletAtOrdinal(ordinal)
		if valid && tablet.tabletID == tabletID {
			return tablet, true
		}
	}
	return FusedTabletRouteBlockLabTabletView{}, false
}

func (v *FusedTabletRouteBlockLabView) LowerBound(
	key []byte,
) FusedTabletRouteBlockLabCursor {
	if v == nil || len(v.image) == 0 {
		return FusedTabletRouteBlockLabCursor{}
	}
	return FusedTabletRouteBlockLabCursor{
		view: v, ordinal: uint16(v.floors.upperBound(key)), valid: true,
	}
}

func (v *FusedTabletRouteBlockLabView) OrderedLowerBound(
	key []byte,
) FusedTabletRouteBlockLabOrderedCursor {
	if v == nil || len(v.image) == 0 {
		return FusedTabletRouteBlockLabOrderedCursor{}
	}
	var cursor FusedTabletRouteBlockLabOrderedCursor
	ordinal := v.floors.upperBound(key)
	if !cursor.load(v, ordinal, key) {
		return FusedTabletRouteBlockLabOrderedCursor{}
	}
	return cursor
}

func (c *FusedTabletRouteBlockLabCursor) Tablet() (FusedTabletRouteBlockLabTabletView, bool) {
	if c == nil || !c.valid || c.view == nil {
		return FusedTabletRouteBlockLabTabletView{}, false
	}
	return c.view.admittedTabletAtOrdinal(int(c.ordinal))
}

func (c *FusedTabletRouteBlockLabCursor) Next() bool {
	if c == nil || !c.valid || int(c.ordinal)+1 >= c.view.Count() {
		if c != nil {
			c.valid = false
		}
		return false
	}
	c.ordinal++
	return true
}

func (t *FusedTabletRouteBlockLabTabletView) TabletID() uint32 {
	if t == nil {
		return 0
	}
	return t.tabletID
}

func (t *FusedTabletRouteBlockLabTabletView) StableSlot() uint8 {
	if t == nil {
		return 0
	}
	return t.stableSlot
}

func (t *FusedTabletRouteBlockLabTabletView) LocatorRef() (PageRef, bool) {
	if t == nil || t.owner == nil || len(t.descriptor) == 0 {
		return PageRef{}, false
	}
	return t.locator, true
}

func (t *FusedTabletRouteBlockLabTabletView) RouteAnchor(
	key []byte,
) (FusedTabletRouteBlockLabRoute, bool) {
	if t == nil || t.owner == nil || len(t.descriptor) == 0 {
		return FusedTabletRouteBlockLabRoute{}, false
	}
	rank := t.rootUpperBound(key) - 1
	pageID, ref, ok := t.anchorAt(rank)
	if !ok {
		return FusedTabletRouteBlockLabRoute{}, false
	}
	return FusedTabletRouteBlockLabRoute{
		BlockRef: t.owner.ref, TabletID: t.tabletID,
		TabletOrdinal: t.ordinal, TabletSlot: t.stableSlot,
		LocatorRef: t.locator,
		PageID:     pageID, AnchorRef: ref, owner: t.owner,
		anchorRank: uint8(rank),
	}, true
}

func (t *FusedTabletRouteBlockLabTabletView) LowerBound(
	key []byte,
) FusedTabletRouteBlockLabAnchorCursor {
	if t == nil || t.owner == nil || len(t.descriptor) == 0 {
		return FusedTabletRouteBlockLabAnchorCursor{}
	}
	return FusedTabletRouteBlockLabAnchorCursor{
		tablet: t, rank: uint8(t.rootUpperBound(key) - 1), valid: true,
	}
}

func (c *FusedTabletRouteBlockLabAnchorCursor) Route() (FusedTabletRouteBlockLabRoute, bool) {
	if c == nil || !c.valid || c.tablet == nil {
		return FusedTabletRouteBlockLabRoute{}, false
	}
	pageID, ref, ok := c.tablet.anchorAt(int(c.rank))
	if !ok {
		return FusedTabletRouteBlockLabRoute{}, false
	}
	return FusedTabletRouteBlockLabRoute{
		BlockRef: c.tablet.owner.ref, TabletID: c.tablet.tabletID,
		TabletOrdinal: c.tablet.ordinal, TabletSlot: c.tablet.stableSlot,
		LocatorRef: c.tablet.locator,
		PageID:     pageID, AnchorRef: ref, owner: c.tablet.owner,
		anchorRank: c.rank,
	}, true
}

// AnchorRef is the ordered-scan hot path. Tablet identity and locator binding
// are already carried by the enclosing tablet cursor, so it avoids rebuilding
// the point-read route envelope for every anchor page.
func (c *FusedTabletRouteBlockLabAnchorCursor) AnchorRef() (uint8, PageRef, bool) {
	if c == nil || !c.valid || c.tablet == nil {
		return 0, PageRef{}, false
	}
	return c.tablet.admittedAnchorAt(int(c.rank))
}

func (c *FusedTabletRouteBlockLabAnchorCursor) Next() bool {
	if c == nil || !c.valid ||
		int(c.rank)+1 >= int(c.tablet.pageCount) {
		if c != nil {
			c.valid = false
		}
		return false
	}
	c.rank++
	return true
}

func (c *FusedTabletRouteBlockLabOrderedCursor) AnchorRef() (tabletID uint32, pageID uint8, ref PageRef, ok bool) {
	if c == nil || !c.valid || c.view == nil {
		return 0, 0, PageRef{}, false
	}
	row := c.anchorRows[int(c.rank)*fusedTabletRouteBlockLabDescriptorRefBytes : (int(c.rank)+1)*fusedTabletRouteBlockLabDescriptorRefBytes]
	pageID = row[0]
	logicalID, logicalOK := GlobalTabletCatalogAnchorLogicalID(
		c.tabletID, pageID,
	)
	if !logicalOK {
		return 0, 0, PageRef{}, false
	}
	return c.tabletID, pageID, fusedTabletRouteBlockLabDecodeRef(
		row[1:], logicalID, c.view.anchorKind,
		SegmentedTabletRouterAnchorPageBytes,
	), true
}

func (c *FusedTabletRouteBlockLabOrderedCursor) Next() bool {
	if c == nil || !c.valid || c.view == nil {
		return false
	}
	if int(c.rank)+1 < int(c.pageCount) {
		c.rank++
		return true
	}
	next := int(c.ordinal) + 1
	if next >= c.view.Count() || !c.load(c.view, next, nil) {
		c.valid = false
		return false
	}
	return true
}

func (c *FusedTabletRouteBlockLabOrderedCursor) load(
	view *FusedTabletRouteBlockLabView, ordinal int, key []byte,
) bool {
	if view == nil || ordinal < 0 || ordinal >= view.Count() {
		return false
	}
	bucket := view.floors.bucketAt(ordinal)
	tabletID, localID, ok := SplitTabletLocalIdentityBucket(uint32(bucket))
	start := int(binary.LittleEndian.Uint16(
		view.descriptorOffsets[ordinal*2:],
	))
	end := int(binary.LittleEndian.Uint16(
		view.descriptorOffsets[(ordinal+1)*2:],
	))
	if !ok || localID != 0 || start >= end || end > len(view.descriptors) {
		return false
	}
	src := view.descriptors[start:end]
	pageCount := int(src[0])
	rowsAt := fusedTabletRouteBlockLabDescriptorHeader
	offsetAt := rowsAt + pageCount*fusedTabletRouteBlockLabDescriptorRefBytes
	keysAt := offsetAt + (pageCount+1)*2
	commonBytes := int(binary.LittleEndian.Uint16(src[2:4]))
	rank := 0
	if key != nil {
		rank = fusedTabletRouteBlockLabRootUpperBound(
			src[keysAt:keysAt+commonBytes], src[keysAt+commonBytes:],
			src[offsetAt:keysAt], pageCount, key,
		) - 1
	}
	*c = FusedTabletRouteBlockLabOrderedCursor{
		view: view, anchorRows: src[rowsAt:offsetAt],
		tabletID: tabletID, ordinal: uint16(ordinal), rank: uint8(rank),
		pageCount: uint8(pageCount), valid: true,
	}
	return true
}

func OpenFusedTabletRouteBlockLabAnchor(
	src []byte, route FusedTabletRouteBlockLabRoute,
) (FusedTabletRouteBlockLabAnchorView, error) {
	if route.owner == nil || len(route.owner.image) == 0 ||
		route.BlockRef != route.owner.ref || !route.owner.bounds.valid() ||
		route.owner.header.StoreID != route.owner.bounds.StoreID ||
		route.owner.selectedRootGeneration !=
			route.owner.bounds.SelectedRootGeneration ||
		route.owner.header.Generation > route.owner.selectedRootGeneration {
		return FusedTabletRouteBlockLabAnchorView{},
			fusedTabletRouteBlockLabCorrupt("anchor route owner")
	}
	// PageRef does not carry StoreID. The admitted common-page owner supplies
	// the Store fence and the common anchor envelope must match it.
	tablet, ok := route.owner.admittedTabletAtOrdinal(int(route.TabletOrdinal))
	if !ok || tablet.tabletID != route.TabletID ||
		tablet.stableSlot != route.TabletSlot ||
		tablet.locator != route.LocatorRef {
		return FusedTabletRouteBlockLabAnchorView{},
			fusedTabletRouteBlockLabCorrupt("anchor tablet binding")
	}
	pageID, ref, ok := tablet.anchorAt(int(route.anchorRank))
	if !ok || pageID != route.PageID || ref != route.AnchorRef {
		return FusedTabletRouteBlockLabAnchorView{},
			fusedTabletRouteBlockLabCorrupt("anchor PageRef binding")
	}
	page, err := fusedTabletRouteBlockLabOpenAnchorPage(
		src, route.owner, tablet, pageID, ref,
	)
	if err != nil {
		return FusedTabletRouteBlockLabAnchorView{}, err
	}
	return FusedTabletRouteBlockLabAnchorView{
		owner: route.owner, tabletID: tablet.tabletID,
		tabletOrdinal: tablet.ordinal, locator: tablet.locator,
		ref: ref, page: page,
	}, nil
}

func (v *FusedTabletRouteBlockLabAnchorView) RouteHashed(
	hash uint64, key []byte,
) (SegmentedTabletRouterRoute, bool) {
	if v == nil || v.owner == nil || len(v.page.image) == 0 {
		return SegmentedTabletRouterRoute{}, false
	}
	return v.page.routeAt(v.page.upperBound(key)-1, hash), true
}

func (v *FusedTabletRouteBlockLabAnchorView) ResolveBucket(
	locator *GlobalTabletCatalogLocatorView, bucket BucketID,
) (PageRef, BucketZone, bool) {
	if v == nil || v.owner == nil || locator == nil {
		return PageRef{}, BucketZone{}, false
	}
	tabletID, localID, ok := SplitTabletLocalIdentityBucket(uint32(bucket))
	if !ok || tabletID != v.tabletID || locator.tabletID != tabletID ||
		locator.ref != v.locator {
		return PageRef{}, BucketZone{}, false
	}
	pageID, rowSlot, state := locator.Resolve(localID)
	if state != GlobalTabletCatalogLocatorLive ||
		pageID != v.page.pageID ||
		binary.LittleEndian.Uint16(v.page.localIDs[int(rowSlot)*2:]) != localID {
		return PageRef{}, BucketZone{}, false
	}
	return v.page.handleAt(rowSlot, bucket)
}

// RewriteReferences is the single-edit convenience wrapper around
// RewriteReferenceBatch.
func (v *FusedTabletRouteBlockLabView) RewriteReferences(
	dst []byte, birthGeneration, selectedRootGeneration uint64,
	bounds GlobalTabletCatalogBounds,
	mutation FusedTabletRouteBlockLabRefMutation,
) ([]byte, error) {
	one := [1]FusedTabletRouteBlockLabRefMutation{mutation}
	return v.RewriteReferenceBatch(
		dst, birthGeneration, selectedRootGeneration, bounds, one[:],
	)
}

// InsertTablet performs one bounded structural COW. StableSlot is physical
// identity and remains unchanged for every existing tablet even when the new
// lexical floor shifts all following ordinals.
func (v *FusedTabletRouteBlockLabView) InsertTablet(
	dst []byte, birthGeneration, selectedRootGeneration uint64,
	bounds GlobalTabletCatalogBounds,
	tablet FusedTabletRouteBlockLabTablet,
	scratch *FusedTabletRouteBlockLabStructuralScratch,
) ([]byte, error) {
	if v == nil || scratch == nil || len(v.image) == 0 ||
		v.Count() >= FusedTabletRouteBlockLabMaxTablets ||
		len(tablet.Floor) == 0 ||
		v.stablePresence[tablet.StableSlot>>3]&
			(byte(1)<<(tablet.StableSlot&7)) != 0 {
		return nil, fmt.Errorf("%w: fused structural insert", ErrInvalidWrite)
	}
	tablets, err := v.materializeStructuralScratch(scratch)
	if err != nil {
		return nil, err
	}
	insertAt := 1
	for insertAt < len(tablets) &&
		bytes.Compare(tablets[insertAt].Floor, tablet.Floor) < 0 {
		insertAt++
	}
	if insertAt < len(tablets) &&
		bytes.Equal(tablets[insertAt].Floor, tablet.Floor) {
		return nil, fmt.Errorf("%w: duplicate fused tablet floor", ErrInvalidWrite)
	}
	copy(scratch.Tablets[insertAt+1:len(tablets)+1],
		scratch.Tablets[insertAt:len(tablets)])
	scratch.Tablets[insertAt] = tablet
	return v.encodeStructuralRewrite(
		dst, birthGeneration, selectedRootGeneration, bounds,
		scratch.Tablets[:len(tablets)+1],
	)
}

// RemoveTablet performs one bounded structural COW by stable slot. Removing
// the first lexical tablet promotes its successor to the empty sentinel floor;
// its stable slot and every remaining physical identity are preserved.
func (v *FusedTabletRouteBlockLabView) RemoveTablet(
	dst []byte, birthGeneration, selectedRootGeneration uint64,
	bounds GlobalTabletCatalogBounds, stableSlot uint8,
	scratch *FusedTabletRouteBlockLabStructuralScratch,
) ([]byte, error) {
	if v == nil || scratch == nil || len(v.image) == 0 || v.Count() <= 1 ||
		v.stablePresence[stableSlot>>3]&(byte(1)<<(stableSlot&7)) == 0 {
		return nil, fmt.Errorf("%w: fused structural remove", ErrInvalidWrite)
	}
	tablets, err := v.materializeStructuralScratch(scratch)
	if err != nil {
		return nil, err
	}
	removeAt := -1
	for ordinal := range tablets {
		if tablets[ordinal].StableSlot == stableSlot {
			removeAt = ordinal
			break
		}
	}
	if removeAt < 0 {
		return nil, fusedTabletRouteBlockLabCorrupt(
			"structural stable-slot inverse",
		)
	}
	copy(scratch.Tablets[removeAt:len(tablets)-1],
		scratch.Tablets[removeAt+1:len(tablets)])
	next := scratch.Tablets[:len(tablets)-1]
	next[0].Floor = nil
	return v.encodeStructuralRewrite(
		dst, birthGeneration, selectedRootGeneration, bounds, next,
	)
}

func (v *FusedTabletRouteBlockLabView) encodeStructuralRewrite(
	dst []byte, birthGeneration, selectedRootGeneration uint64,
	bounds GlobalTabletCatalogBounds,
	tablets []FusedTabletRouteBlockLabTablet,
) ([]byte, error) {
	if v == nil || len(dst) < FusedTabletRouteBlockLabBytes ||
		birthGeneration <= v.header.Generation ||
		selectedRootGeneration < v.selectedRootGeneration ||
		!bounds.extends(v.bounds) ||
		globalTabletCatalogSlicesOverlap(
			dst[:FusedTabletRouteBlockLabBytes], v.image,
		) {
		return nil, fmt.Errorf("%w: fused structural COW", ErrInvalidWrite)
	}
	return EncodeFusedTabletRouteBlockLab(
		dst,
		FusedTabletRouteBlockLabHeader{
			StoreID: v.header.StoreID, Generation: birthGeneration,
			SelectedRootGeneration: selectedRootGeneration,
			LogicalID:              v.header.LogicalID, BlockID: v.blockID,
			Kind: v.header.Kind, LocatorKind: v.locatorKind,
			AnchorKind: v.anchorKind, LeafKind: v.leafKind, Bounds: bounds,
		},
		tablets,
	)
}

func (v *FusedTabletRouteBlockLabView) materializeStructuralScratch(
	scratch *FusedTabletRouteBlockLabStructuralScratch,
) ([]FusedTabletRouteBlockLabTablet, error) {
	if v == nil || scratch == nil || v.Count() == 0 {
		return nil, fmt.Errorf("%w: fused structural scratch", ErrInvalidWrite)
	}
	for ordinal := 0; ordinal < v.Count(); ordinal++ {
		view, ok := v.admittedTabletAtOrdinal(ordinal)
		if !ok {
			return nil, fusedTabletRouteBlockLabCorrupt(
				"structural tablet descriptor",
			)
		}
		tablet := &scratch.Tablets[ordinal]
		*tablet = FusedTabletRouteBlockLabTablet{
			TabletID: view.tabletID, StableSlot: view.stableSlot,
			Locator: view.locator,
			Anchors: scratch.Anchors[ordinal*SegmentedTabletRouterMaxPages : ordinal*SegmentedTabletRouterMaxPages+int(view.pageCount)],
		}
		if ordinal != 0 {
			common, restart, suffix, floorOK :=
				v.floors.FenceAt(ordinal - 1)
			floor := scratch.TabletFloors[ordinal][:]
			floorBytes := copyFenceParts(floor, common, restart, suffix)
			if !floorOK || floorBytes > FusedTabletRouteBlockLabMaxFence {
				return nil, fusedTabletRouteBlockLabCorrupt(
					"structural tablet floor",
				)
			}
			tablet.Floor = floor[:floorBytes]
		}
		for rank := 0; rank < int(view.pageCount); rank++ {
			pageID, ref, anchorOK := view.admittedAnchorAt(rank)
			if !anchorOK {
				return nil, fusedTabletRouteBlockLabCorrupt(
					"structural anchor reference",
				)
			}
			anchor := &tablet.Anchors[rank]
			*anchor = FusedTabletRouteBlockLabAnchor{
				PageID: pageID, Ref: ref,
			}
			if rank == 0 {
				continue
			}
			start := int(binary.LittleEndian.Uint16(
				view.rootOffsets[rank*2:],
			))
			end := int(binary.LittleEndian.Uint16(
				view.rootOffsets[(rank+1)*2:],
			))
			if start > end || end > len(view.rootKeys) {
				return nil, fusedTabletRouteBlockLabCorrupt(
					"structural anchor floor",
				)
			}
			floorIndex := ordinal*SegmentedTabletRouterMaxPages + rank
			floor := scratch.AnchorFloors[floorIndex][:]
			floorBytes := copy(floor, view.rootCommon)
			floorBytes += copy(floor[floorBytes:], view.rootKeys[start:end])
			if floorBytes > FusedTabletRouteBlockLabMaxFence {
				return nil, fusedTabletRouteBlockLabCorrupt(
					"structural anchor floor",
				)
			}
			anchor.Floor = floor[:floorBytes]
		}
	}
	return scratch.Tablets[:v.Count()], nil
}

func copyFenceParts(dst, common, restart, suffix []byte) int {
	wrote := copy(dst, common)
	wrote += copy(dst[wrote:], restart)
	wrote += copy(dst[wrote:], suffix)
	return wrote
}

// RewriteReferenceBatch performs the first snapshot-epoch COW of one shared
// route block. The selected root generation and destination allocation bounds
// must monotonically extend the admitted source snapshot, so untouched
// references remain valid while replacements may name newly appended pages.
//
// Mutations must be grouped by ascending TabletID. Within one tablet, locator
// replacement may appear once and anchor PageIDs must be strictly increasing.
// The complete batch is validated before dst changes, then copied and sealed
// once. This is the only supported way to accumulate edits: repeatedly
// rewriting the same admitted ancestor would create alternatives and could
// otherwise discard an earlier edit. Later materialization in the same
// ownership epoch amortizes this 16 KiB copy across every touched tablet.
func (v *FusedTabletRouteBlockLabView) RewriteReferenceBatch(
	dst []byte, birthGeneration, selectedRootGeneration uint64,
	bounds GlobalTabletCatalogBounds,
	mutations []FusedTabletRouteBlockLabRefMutation,
) ([]byte, error) {
	if v == nil || len(v.image) == 0 ||
		len(dst) < FusedTabletRouteBlockLabBytes ||
		birthGeneration <= v.header.Generation ||
		birthGeneration >= uint64(1)<<48 ||
		selectedRootGeneration == 0 ||
		selectedRootGeneration >= uint64(1)<<48 ||
		selectedRootGeneration < v.selectedRootGeneration ||
		selectedRootGeneration != bounds.SelectedRootGeneration ||
		birthGeneration > selectedRootGeneration ||
		!bounds.extends(v.bounds) ||
		len(mutations) == 0 ||
		len(mutations) > FusedTabletRouteBlockLabMaxTablets*
			(SegmentedTabletRouterMaxPages+1) ||
		globalTabletCatalogSlicesOverlap(
			dst[:FusedTabletRouteBlockLabBytes], v.image,
		) {
		return nil, fmt.Errorf("%w: fused route-block COW", ErrInvalidWrite)
	}
	var previousTabletID uint32
	var lastAnchorPageID uint8
	seenTablet := false
	seenLocator := false
	seenAnchor := false
	for index := range mutations {
		mutation := mutations[index]
		if !mutation.ReplaceLocator && !mutation.ReplaceAnchor ||
			seenTablet && mutation.TabletID < previousTabletID {
			return nil, fmt.Errorf(
				"%w: fused COW mutation order", ErrInvalidWrite,
			)
		}
		if !seenTablet || mutation.TabletID != previousTabletID {
			previousTabletID = mutation.TabletID
			seenTablet = true
			seenLocator = false
			seenAnchor = false
		}
		if mutation.ReplaceLocator {
			if seenLocator {
				return nil, fmt.Errorf(
					"%w: duplicate fused COW locator", ErrInvalidWrite,
				)
			}
			seenLocator = true
		}
		if mutation.ReplaceAnchor {
			if seenAnchor && mutation.AnchorPageID <= lastAnchorPageID {
				return nil, fmt.Errorf(
					"%w: duplicate or unordered fused COW anchor",
					ErrInvalidWrite,
				)
			}
			lastAnchorPageID = mutation.AnchorPageID
			seenAnchor = true
		}
		tablet, ok := v.resolveTabletID(mutation.TabletID)
		if !ok {
			return nil, fmt.Errorf(
				"%w: fused COW TabletID", ErrInvalidWrite,
			)
		}
		if mutation.ReplaceLocator {
			logicalID, _ :=
				GlobalTabletCatalogLocatorLogicalID(tablet.tabletID)
			if fusedTabletRouteBlockLabValidateRef(
				mutation.Locator, logicalID, v.locatorKind,
				GlobalTabletCatalogLocatorBytes,
				selectedRootGeneration, bounds,
			) != nil {
				return nil, fmt.Errorf(
					"%w: fused COW locator", ErrInvalidWrite,
				)
			}
		}
		if mutation.ReplaceAnchor {
			anchorRank := fusedTabletRouteBlockLabAnchorRank(
				tablet, mutation.AnchorPageID,
			)
			logicalID, logicalOK :=
				GlobalTabletCatalogAnchorLogicalID(
					tablet.tabletID, mutation.AnchorPageID,
				)
			if anchorRank < 0 || !logicalOK ||
				fusedTabletRouteBlockLabValidateRef(
					mutation.Anchor, logicalID, v.anchorKind,
					SegmentedTabletRouterAnchorPageBytes,
					selectedRootGeneration, bounds,
				) != nil {
				return nil, fmt.Errorf(
					"%w: fused COW anchor", ErrInvalidWrite,
				)
			}
		}
	}
	payload, err := InitPage(dst[:FusedTabletRouteBlockLabBytes], PageHeader{
		StoreID: v.header.StoreID, Generation: birthGeneration,
		LogicalID: v.header.LogicalID, PageSize: v.header.PageSize,
		PayloadLength: v.header.PayloadLength, Kind: v.header.Kind,
	})
	if err != nil {
		return nil, err
	}
	copy(payload, v.payload)
	mapBytes := int(binary.LittleEndian.Uint16(payload[10:12]))
	mapStart := FusedTabletRouteBlockLabHeaderBytes
	mapEnd := mapStart + mapBytes
	if mapBytes < TabletAnchorMapLabHeaderSize+TabletAnchorMapLabTrailerSize ||
		mapEnd > len(payload) {
		return nil, fusedTabletRouteBlockLabCorrupt("COW anchor-map geometry")
	}
	// Rewrite the embedded section's birth identity and checksum as part of
	// the outer COW. This makes the output depend only on the new birth and
	// logical contents, never on which ancestor supplied the unchanged map.
	binary.LittleEndian.PutUint64(
		payload[mapStart+24:mapStart+32], birthGeneration,
	)
	tabletAnchorMapLabSeal(payload[mapStart:mapEnd])
	descriptorBase := len(v.payload) - len(v.descriptors)
	for index := range mutations {
		mutation := mutations[index]
		tablet, ok := v.resolveTabletID(mutation.TabletID)
		if !ok {
			return nil, fusedTabletRouteBlockLabCorrupt(
				"COW prevalidated TabletID",
			)
		}
		descriptorStart := int(binary.LittleEndian.Uint16(
			v.descriptorOffsets[int(tablet.ordinal)*2:],
		))
		if mutation.ReplaceLocator {
			fusedTabletRouteBlockLabEncodeRef(
				payload[descriptorBase+descriptorStart+4:],
				mutation.Locator,
			)
		}
		if mutation.ReplaceAnchor {
			anchorRank := fusedTabletRouteBlockLabAnchorRank(
				tablet, mutation.AnchorPageID,
			)
			if anchorRank < 0 {
				return nil, fusedTabletRouteBlockLabCorrupt(
					"COW prevalidated anchor",
				)
			}
			at := descriptorBase + descriptorStart +
				fusedTabletRouteBlockLabDescriptorHeader +
				anchorRank*fusedTabletRouteBlockLabDescriptorRefBytes + 1
			fusedTabletRouteBlockLabEncodeRef(payload[at:], mutation.Anchor)
		}
	}
	if _, err := sealInitializedPage(
		dst[:FusedTabletRouteBlockLabBytes],
	); err != nil {
		return nil, err
	}
	return dst[:FusedTabletRouteBlockLabBytes], nil
}

func fusedTabletRouteBlockLabAnchorRank(
	tablet FusedTabletRouteBlockLabTabletView, pageID uint8,
) int {
	for rank := 0; rank < int(tablet.pageCount); rank++ {
		candidate, _, valid := tablet.anchorAt(rank)
		if valid && candidate == pageID {
			return rank
		}
	}
	return -1
}

func FusedTabletRouteBlockLabWorstCaseFanout(maxFenceBytes int) int {
	if maxFenceBytes <= 0 || maxFenceBytes > FusedTabletRouteBlockLabMaxFence {
		return 0
	}
	descriptorBytes := fusedTabletRouteBlockLabWorstDescriptorBytes(maxFenceBytes)
	for count := 1; count <= FusedTabletRouteBlockLabMaxTablets; count++ {
		fences := count - 1
		mapBytes := tabletAnchorMapLabImageBytes(
			fences, 0, fences*(2+maxFenceBytes),
		)
		payload := FusedTabletRouteBlockLabHeaderBytes + mapBytes +
			(count+1)*2 + fusedTabletRouteBlockLabStableDirectoryBytes +
			count*descriptorBytes
		if payload > FusedTabletRouteBlockLabBytes-PageHeaderSize-PageTrailerSize {
			return count - 1
		}
	}
	return FusedTabletRouteBlockLabMaxTablets
}

func FusedTabletRouteBlockLabCatalogGeometry(
	tablets uint64, maxFenceBytes int,
) (FusedTabletRouteBlockLabGeometry, bool) {
	routeFanout := FusedTabletRouteBlockLabWorstCaseFanout(maxFenceBytes)
	branchFanout := GlobalTabletCatalogWorstCaseFanout(
		FusedTabletRouteBlockLabBranchBytes, maxFenceBytes,
	)
	rootFanout := GlobalTabletCatalogWorstCaseFanout(
		FusedTabletRouteBlockLabRootBytes, maxFenceBytes,
	)
	if tablets == 0 || routeFanout == 0 ||
		branchFanout == 0 || rootFanout == 0 {
		return FusedTabletRouteBlockLabGeometry{}, false
	}
	routeBlocks := (tablets + uint64(routeFanout) - 1) /
		uint64(routeFanout)
	levelPages := (routeBlocks + uint64(branchFanout) - 1) /
		uint64(branchFanout)
	branches := levelPages
	branchLevels := 1
	maximum := uint64(rootFanout) * uint64(routeFanout)
	for level := 0; level < branchLevels; level++ {
		if maximum > ^uint64(0)/uint64(branchFanout) {
			maximum = ^uint64(0)
			break
		}
		maximum *= uint64(branchFanout)
	}
	for levelPages > uint64(rootFanout) {
		levelPages = (levelPages + uint64(branchFanout) - 1) /
			uint64(branchFanout)
		if branches > ^uint64(0)-levelPages {
			return FusedTabletRouteBlockLabGeometry{}, false
		}
		branches += levelPages
		branchLevels++
		if maximum <= ^uint64(0)/uint64(branchFanout) {
			maximum *= uint64(branchFanout)
		} else {
			maximum = ^uint64(0)
		}
	}
	if levelPages > uint64(rootFanout) {
		return FusedTabletRouteBlockLabGeometry{}, false
	}
	return FusedTabletRouteBlockLabGeometry{
		Tablets: tablets, MaxFenceBytes: maxFenceBytes,
		RouteBlockFanout: routeFanout, BranchFanout: branchFanout,
		RootFanout: rootFanout, BranchLevels: branchLevels,
		RouteBlocks: routeBlocks,
		Branches:    branches, MaximumTablets: maximum,
		CatalogBytes: FusedTabletRouteBlockLabRootBytes +
			branches*FusedTabletRouteBlockLabBranchBytes +
			routeBlocks*FusedTabletRouteBlockLabBytes,
		ResidentBytes:       FusedTabletRouteBlockLabRootBytes,
		ColdPointCachePages: branchLevels + 2, CurrentColdPointPages: 4,
		// First mutation of a snapshot-shared path includes every immutable
		// routing ancestor. Once the ownership layer has materialized that
		// path, both designs pay the same 24 KiB local anchor update; fusion
		// amortizes its one 16 KiB route block across all tablets it packs.
		FirstAnchorUpdateBytes: FusedTabletRouteBlockLabRootBytes +
			uint64(branchLevels)*FusedTabletRouteBlockLabBranchBytes +
			FusedTabletRouteBlockLabBytes +
			SegmentedTabletRouterAnchorPageBytes,
		CurrentFirstAnchorUpdateBytes: GlobalTabletCatalogRootBytes +
			GlobalTabletCatalogNodeBytes +
			GlobalTabletCatalogNodeBytes +
			GlobalTabletCatalogTabletBytes +
			SegmentedTabletRouterAnchorPageBytes,
		OwnedAnchorUpdateBytes: FusedTabletRouteBlockLabBytes +
			SegmentedTabletRouterAnchorPageBytes,
		CurrentOwnedAnchorUpdateBytes: GlobalTabletCatalogNodeBytes +
			GlobalTabletCatalogTabletBytes +
			SegmentedTabletRouterAnchorPageBytes,
		SplitBytes: FusedTabletRouteBlockLabRootBytes +
			uint64(branchLevels)*FusedTabletRouteBlockLabBranchBytes +
			FusedTabletRouteBlockLabBytes +
			GlobalTabletCatalogLocatorBytes +
			2*SegmentedTabletRouterAnchorPageBytes,
		CurrentSplitBytes: GlobalTabletCatalogRootBytes +
			GlobalTabletCatalogNodeBytes +
			GlobalTabletCatalogNodeBytes +
			GlobalTabletCatalogTabletBytes +
			GlobalTabletCatalogLocatorBytes +
			2*SegmentedTabletRouterAnchorPageBytes,
	}, true
}

func FusedTabletRouteBlockLabRoutingSpace(
	documents, rowsPerLeaf, occupiedLeavesPerTablet, tabletsPerBlock uint64,
) (FusedTabletRouteBlockLabSpace, bool) {
	if documents == 0 || rowsPerLeaf == 0 ||
		occupiedLeavesPerTablet == 0 ||
		occupiedLeavesPerTablet > TabletLocalIdentityLocalCount ||
		tabletsPerBlock == 0 ||
		tabletsPerBlock > FusedTabletRouteBlockLabMaxTablets {
		return FusedTabletRouteBlockLabSpace{}, false
	}
	leaves := (documents + rowsPerLeaf - 1) / rowsPerLeaf
	tablets := (leaves + occupiedLeavesPerTablet - 1) /
		occupiedLeavesPerTablet
	anchorsPerTablet := (occupiedLeavesPerTablet +
		SegmentedTabletRouterRowsPerPage - 1) /
		SegmentedTabletRouterRowsPerPage
	routeBlocks := (tablets + tabletsPerBlock - 1) / tabletsPerBlock
	branchFanout := uint64(GlobalTabletCatalogWorstCaseFanout(
		FusedTabletRouteBlockLabBranchBytes,
		FusedTabletRouteBlockLabMaxFence,
	))
	branches := (routeBlocks + branchFanout - 1) / branchFanout
	catalogBytes := uint64(FusedTabletRouteBlockLabRootBytes) +
		branches*FusedTabletRouteBlockLabBranchBytes +
		routeBlocks*FusedTabletRouteBlockLabBytes
	locatorBytes := tablets * GlobalTabletCatalogLocatorBytes
	anchorBytes := tablets * anchorsPerTablet *
		SegmentedTabletRouterAnchorPageBytes
	total := catalogBytes + locatorBytes + anchorBytes
	return FusedTabletRouteBlockLabSpace{
		Documents: documents, Leaves: leaves, Tablets: tablets,
		AnchorsPerTablet: anchorsPerTablet,
		TabletsPerBlock:  tabletsPerBlock, RouteBlocks: routeBlocks,
		Branches: branches, CatalogBytes: catalogBytes,
		LocatorBytes: locatorBytes, AnchorBytes: anchorBytes,
		TotalBytes: total, BytesPerDoc: float64(total) / float64(documents),
		ResidentBytes:  FusedTabletRouteBlockLabRootBytes,
		ColdPointPages: 3,
		ColdScanPages: branches + routeBlocks +
			tablets*anchorsPerTablet,
	}, true
}

func (v *FusedTabletRouteBlockLabView) resolveTabletID(
	tabletID uint32,
) (FusedTabletRouteBlockLabTabletView, bool) {
	if tabletID >= TabletLocalIdentityTabletCount {
		return FusedTabletRouteBlockLabTabletView{}, false
	}
	bucket, ok := MakeTabletLocalIdentityBucket(tabletID, 0)
	if !ok {
		return FusedTabletRouteBlockLabTabletView{}, false
	}
	return v.ResolveTablet(BucketID(bucket))
}

func (v *FusedTabletRouteBlockLabView) tabletAtOrdinal(
	ordinal int,
) (FusedTabletRouteBlockLabTabletView, bool) {
	if v == nil || ordinal < 0 || ordinal >= int(v.count) {
		return FusedTabletRouteBlockLabTabletView{}, false
	}
	bucket := v.floors.bucketAt(ordinal)
	tabletID, localID, ok := SplitTabletLocalIdentityBucket(uint32(bucket))
	start := int(binary.LittleEndian.Uint16(
		v.descriptorOffsets[ordinal*2:],
	))
	end := int(binary.LittleEndian.Uint16(
		v.descriptorOffsets[(ordinal+1)*2:],
	))
	if !ok || localID != 0 || start >= end || end > len(v.descriptors) {
		return FusedTabletRouteBlockLabTabletView{}, false
	}
	return fusedTabletRouteBlockLabOpenDescriptor(
		v, v.descriptors[start:end], tabletID, uint16(ordinal),
	)
}

func (v *FusedTabletRouteBlockLabView) admittedTabletAtOrdinal(
	ordinal int,
) (FusedTabletRouteBlockLabTabletView, bool) {
	if v == nil || ordinal < 0 || ordinal >= int(v.count) {
		return FusedTabletRouteBlockLabTabletView{}, false
	}
	bucket := v.floors.bucketAt(ordinal)
	tabletID, localID, ok := SplitTabletLocalIdentityBucket(uint32(bucket))
	start := int(binary.LittleEndian.Uint16(
		v.descriptorOffsets[ordinal*2:],
	))
	end := int(binary.LittleEndian.Uint16(
		v.descriptorOffsets[(ordinal+1)*2:],
	))
	if !ok || localID != 0 || start >= end || end > len(v.descriptors) {
		return FusedTabletRouteBlockLabTabletView{}, false
	}
	src := v.descriptors[start:end]
	pageCount := int(src[0])
	rowsAt := fusedTabletRouteBlockLabDescriptorHeader
	offsetAt := rowsAt + pageCount*fusedTabletRouteBlockLabDescriptorRefBytes
	keysAt := offsetAt + (pageCount+1)*2
	commonBytes := int(binary.LittleEndian.Uint16(src[2:4]))
	locatorLogical, _ := GlobalTabletCatalogLocatorLogicalID(tabletID)
	return FusedTabletRouteBlockLabTabletView{
		owner: v, descriptor: src, anchorRows: src[rowsAt:offsetAt],
		rootOffsets: src[offsetAt:keysAt],
		rootCommon:  src[keysAt : keysAt+commonBytes],
		rootKeys:    src[keysAt+commonBytes:],
		locator: fusedTabletRouteBlockLabDecodeRef(
			src[4:fusedTabletRouteBlockLabDescriptorHeader],
			locatorLogical, v.locatorKind, GlobalTabletCatalogLocatorBytes,
		),
		tabletID: tabletID, ordinal: uint16(ordinal), stableSlot: src[1],
		pageCount: uint8(pageCount),
	}, true
}

func (t *FusedTabletRouteBlockLabTabletView) rootUpperBound(key []byte) int {
	return fusedTabletRouteBlockLabRootUpperBound(
		t.rootCommon, t.rootKeys, t.rootOffsets, int(t.pageCount), key,
	)
}

func fusedTabletRouteBlockLabRootUpperBound(
	common, keys, offsets []byte, pageCount int, key []byte,
) int {
	compared := min(len(common), len(key))
	if order := bytes.Compare(common[:compared], key[:compared]); order != 0 {
		if order > 0 {
			return 1
		}
		return pageCount
	}
	if compared != len(common) {
		return 1
	}
	key = key[compared:]
	low, high := 1, pageCount
	for low < high {
		mid := int(uint(low+high) >> 1)
		start := int(binary.LittleEndian.Uint16(offsets[mid*2:]))
		end := int(binary.LittleEndian.Uint16(offsets[(mid+1)*2:]))
		if bytes.Compare(keys[start:end], key) <= 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low
}

func (t *FusedTabletRouteBlockLabTabletView) anchorAt(
	rank int,
) (uint8, PageRef, bool) {
	if t == nil || t.owner == nil || rank < 0 || rank >= int(t.pageCount) {
		return 0, PageRef{}, false
	}
	row := t.anchorRows[rank*fusedTabletRouteBlockLabDescriptorRefBytes : (rank+1)*fusedTabletRouteBlockLabDescriptorRefBytes]
	pageID := row[0]
	logicalID, ok := GlobalTabletCatalogAnchorLogicalID(t.tabletID, pageID)
	if !ok {
		return 0, PageRef{}, false
	}
	ref := fusedTabletRouteBlockLabDecodeRef(
		row[1:], logicalID, t.owner.anchorKind,
		SegmentedTabletRouterAnchorPageBytes,
	)
	return pageID, ref, fusedTabletRouteBlockLabValidateRef(
		ref, logicalID, t.owner.anchorKind,
		SegmentedTabletRouterAnchorPageBytes,
		t.owner.selectedRootGeneration, t.owner.bounds,
	) == nil
}

func (t *FusedTabletRouteBlockLabTabletView) admittedAnchorAt(
	rank int,
) (uint8, PageRef, bool) {
	if t == nil || t.owner == nil || rank < 0 || rank >= int(t.pageCount) {
		return 0, PageRef{}, false
	}
	row := t.anchorRows[rank*fusedTabletRouteBlockLabDescriptorRefBytes : (rank+1)*fusedTabletRouteBlockLabDescriptorRefBytes]
	pageID := row[0]
	logicalID, ok := GlobalTabletCatalogAnchorLogicalID(t.tabletID, pageID)
	if !ok {
		return 0, PageRef{}, false
	}
	return pageID, fusedTabletRouteBlockLabDecodeRef(
		row[1:], logicalID, t.owner.anchorKind,
		SegmentedTabletRouterAnchorPageBytes,
	), true
}

func fusedTabletRouteBlockLabOpenDescriptor(
	owner *FusedTabletRouteBlockLabView, src []byte,
	tabletID uint32, ordinal uint16,
) (FusedTabletRouteBlockLabTabletView, bool) {
	if owner == nil || len(src) < fusedTabletRouteBlockLabDescriptorHeader ||
		src[0] == 0 || src[0] > SegmentedTabletRouterMaxPages ||
		owner.stablePresence[src[1]>>3]&(byte(1)<<(src[1]&7)) == 0 {
		return FusedTabletRouteBlockLabTabletView{}, false
	}
	pageCount := int(src[0])
	rowsAt := fusedTabletRouteBlockLabDescriptorHeader
	offsetAt := rowsAt + pageCount*fusedTabletRouteBlockLabDescriptorRefBytes
	keysAt := offsetAt + (pageCount+1)*2
	commonBytes := int(binary.LittleEndian.Uint16(src[2:4]))
	if keysAt+commonBytes > len(src) {
		return FusedTabletRouteBlockLabTabletView{}, false
	}
	logicalID, ok := GlobalTabletCatalogLocatorLogicalID(tabletID)
	if !ok {
		return FusedTabletRouteBlockLabTabletView{}, false
	}
	locator := fusedTabletRouteBlockLabDecodeRef(
		src[4:fusedTabletRouteBlockLabDescriptorHeader],
		logicalID, owner.locatorKind, GlobalTabletCatalogLocatorBytes,
	)
	if fusedTabletRouteBlockLabValidateRef(
		locator, logicalID, owner.locatorKind,
		GlobalTabletCatalogLocatorBytes,
		owner.selectedRootGeneration, owner.bounds,
	) != nil {
		return FusedTabletRouteBlockLabTabletView{}, false
	}
	offsets := src[offsetAt:keysAt]
	common := src[keysAt : keysAt+commonBytes]
	keys := src[keysAt+commonBytes:]
	if binary.LittleEndian.Uint16(offsets) != 0 ||
		int(binary.LittleEndian.Uint16(offsets[pageCount*2:])) != len(keys) {
		return FusedTabletRouteBlockLabTabletView{}, false
	}
	var seen uint16
	var previous segmentedTabletRouterFence
	expectedCommon := -1
	rows := src[rowsAt:offsetAt]
	for rank := 0; rank < pageCount; rank++ {
		start := int(binary.LittleEndian.Uint16(offsets[rank*2:]))
		end := int(binary.LittleEndian.Uint16(offsets[(rank+1)*2:]))
		pageID := rows[rank*fusedTabletRouteBlockLabDescriptorRefBytes]
		fence := segmentedTabletRouterFence{
			a: common, b: keys[start:end],
		}
		if rank == 0 {
			fence.a = nil
		}
		if start > end || end > len(keys) ||
			rank == 0 && (start != end || len(common) != 0 && pageCount == 1) ||
			rank != 0 && (fence.length() == 0 ||
				segmentedTabletRouterCompareFences(previous, fence) >= 0) ||
			pageID >= SegmentedTabletRouterMaxPages ||
			seen&(uint16(1)<<pageID) != 0 {
			return FusedTabletRouteBlockLabTabletView{}, false
		}
		seen |= uint16(1) << pageID
		logicalID, logicalOK := GlobalTabletCatalogAnchorLogicalID(
			tabletID, pageID,
		)
		ref := fusedTabletRouteBlockLabDecodeRef(
			rows[rank*fusedTabletRouteBlockLabDescriptorRefBytes+1:],
			logicalID, owner.anchorKind,
			SegmentedTabletRouterAnchorPageBytes,
		)
		if !logicalOK || fusedTabletRouteBlockLabValidateRef(
			ref, logicalID, owner.anchorKind,
			SegmentedTabletRouterAnchorPageBytes,
			owner.selectedRootGeneration, owner.bounds,
		) != nil {
			return FusedTabletRouteBlockLabTabletView{}, false
		}
		if rank == 1 {
			expectedCommon = fence.length()
		} else if rank > 1 {
			expectedCommon = min(
				expectedCommon,
				segmentedTabletRouterFencePrefix(previous, fence),
			)
		}
		previous = fence
	}
	if pageCount == 1 {
		expectedCommon = 0
	}
	if expectedCommon != commonBytes {
		return FusedTabletRouteBlockLabTabletView{}, false
	}
	return FusedTabletRouteBlockLabTabletView{
		owner: owner, descriptor: src, anchorRows: rows,
		rootOffsets: offsets, rootCommon: common, rootKeys: keys,
		locator:  locator,
		tabletID: tabletID, ordinal: ordinal, stableSlot: src[1],
		pageCount: uint8(pageCount),
	}, true
}

// The fused owner supplies the snapshot selection context to the one typed
// segmented-anchor decoder. Physical page identity remains bound to ref while
// child generations are validated against the selected snapshot root.
func fusedTabletRouteBlockLabOpenAnchorPage(
	image []byte, owner *FusedTabletRouteBlockLabView,
	tablet FusedTabletRouteBlockLabTabletView, pageID uint8, ref PageRef,
) (segmentedTabletRouterAnchorView, error) {
	var view segmentedTabletRouterAnchorView
	if owner == nil || len(tablet.anchorRows) == 0 {
		return view, fusedTabletRouteBlockLabCorrupt("anchor without owner")
	}
	view, err := segmentedTabletRouterOpenAnchor(
		image,
		SegmentedTabletRouterView{
			storeID:    owner.header.StoreID,
			tabletID:   tablet.tabletID,
			generation: owner.selectedRootGeneration,
			rootRanks:  tablet.anchorRows[:1],
			anchorKind: owner.anchorKind,
			leafKind:   owner.leafKind,
		},
		pageID,
		ref,
	)
	if err != nil {
		return segmentedTabletRouterAnchorView{}, fmt.Errorf(
			"%w: typed anchor admission: %v",
			ErrFusedTabletRouteBlockLabCorrupt, err,
		)
	}
	for rank := 0; rank < int(view.count); rank++ {
		slot := view.ranks[rank]
		localID := binary.LittleEndian.Uint16(view.localIDs[int(slot)*2:])
		bucket, _ := MakeTabletLocalIdentityBucket(
			tablet.tabletID, uint32(localID),
		)
		leaf, _, ok := view.handleAt(slot, BucketID(bucket))
		if !ok || !owner.bounds.contains(leaf) {
			return segmentedTabletRouterAnchorView{},
				fusedTabletRouteBlockLabCorrupt("anchor leaf reference")
		}
	}
	return view, nil
}

func fusedTabletRouteBlockLabValidateTabletInput(
	tablet FusedTabletRouteBlockLabTablet,
	locatorKind, anchorKind PageKind,
	selectedRootGeneration uint64,
	bounds GlobalTabletCatalogBounds,
) error {
	if len(tablet.Anchors) == 0 ||
		len(tablet.Anchors) > SegmentedTabletRouterMaxPages ||
		len(tablet.Anchors[0].Floor) != 0 {
		return fmt.Errorf("%w: fused anchor-root geometry", ErrInvalidWrite)
	}
	locatorLogical, ok := GlobalTabletCatalogLocatorLogicalID(tablet.TabletID)
	if !ok || fusedTabletRouteBlockLabValidateRef(
		tablet.Locator, locatorLogical, locatorKind,
		GlobalTabletCatalogLocatorBytes,
		selectedRootGeneration, bounds,
	) != nil {
		return fmt.Errorf("%w: fused locator reference", ErrInvalidWrite)
	}
	var seen uint16
	for rank, anchor := range tablet.Anchors {
		if len(anchor.Floor) > FusedTabletRouteBlockLabMaxFence ||
			rank != 0 && (len(anchor.Floor) == 0 ||
				bytes.Compare(tablet.Anchors[rank-1].Floor, anchor.Floor) >= 0) ||
			anchor.PageID >= SegmentedTabletRouterMaxPages ||
			seen&(uint16(1)<<anchor.PageID) != 0 {
			return fmt.Errorf("%w: fused anchor floor or PageID", ErrInvalidWrite)
		}
		seen |= uint16(1) << anchor.PageID
		logicalID, logicalOK := GlobalTabletCatalogAnchorLogicalID(
			tablet.TabletID, anchor.PageID,
		)
		if !logicalOK || fusedTabletRouteBlockLabValidateRef(
			anchor.Ref, logicalID, anchorKind,
			SegmentedTabletRouterAnchorPageBytes,
			selectedRootGeneration, bounds,
		) != nil {
			return fmt.Errorf("%w: fused anchor reference", ErrInvalidWrite)
		}
	}
	return nil
}

func fusedTabletRouteBlockLabDescriptorBytes(
	anchors []FusedTabletRouteBlockLabAnchor,
) int {
	total := fusedTabletRouteBlockLabDescriptorHeader +
		len(anchors)*fusedTabletRouteBlockLabDescriptorRefBytes +
		(len(anchors)+1)*2
	common := fusedTabletRouteBlockLabAnchorCommon(anchors)
	total += common
	for rank, anchor := range anchors {
		if rank != 0 {
			total += len(anchor.Floor) - common
		}
	}
	return total
}

func fusedTabletRouteBlockLabWorstDescriptorBytes(maxFenceBytes int) int {
	return fusedTabletRouteBlockLabDescriptorHeader +
		SegmentedTabletRouterMaxPages*
			fusedTabletRouteBlockLabDescriptorRefBytes +
		(SegmentedTabletRouterMaxPages+1)*2 +
		(SegmentedTabletRouterMaxPages-1)*maxFenceBytes
}

func fusedTabletRouteBlockLabEncodeDescriptor(
	dst []byte, tablet FusedTabletRouteBlockLabTablet,
	selectedRootGeneration uint64, bounds GlobalTabletCatalogBounds,
	locatorKind, anchorKind PageKind,
) (int, error) {
	total := fusedTabletRouteBlockLabDescriptorBytes(tablet.Anchors)
	if len(dst) < total {
		return 0, ErrFusedTabletRouteBlockLabNoSpace
	}
	out := dst[:total]
	clear(out)
	out[0] = byte(len(tablet.Anchors))
	out[1] = tablet.StableSlot
	common := fusedTabletRouteBlockLabAnchorCommon(tablet.Anchors)
	binary.LittleEndian.PutUint16(out[2:4], uint16(common))
	fusedTabletRouteBlockLabEncodeRef(out[4:], tablet.Locator)
	rowsAt := fusedTabletRouteBlockLabDescriptorHeader
	offsetAt := rowsAt +
		len(tablet.Anchors)*fusedTabletRouteBlockLabDescriptorRefBytes
	keysAt := offsetAt + (len(tablet.Anchors)+1)*2
	if common != 0 {
		copy(out[keysAt:], tablet.Anchors[1].Floor[:common])
	}
	keyAt := 0
	for rank, anchor := range tablet.Anchors {
		row := out[rowsAt+rank*fusedTabletRouteBlockLabDescriptorRefBytes:]
		row[0] = anchor.PageID
		fusedTabletRouteBlockLabEncodeRef(row[1:], anchor.Ref)
		binary.LittleEndian.PutUint16(out[offsetAt+rank*2:], uint16(keyAt))
		if rank != 0 {
			copy(out[keysAt+common+keyAt:], anchor.Floor[common:])
			keyAt += len(anchor.Floor) - common
		}
	}
	binary.LittleEndian.PutUint16(
		out[offsetAt+len(tablet.Anchors)*2:], uint16(keyAt),
	)
	if err := fusedTabletRouteBlockLabValidateTabletInput(
		tablet, locatorKind, anchorKind,
		selectedRootGeneration, bounds,
	); err != nil {
		return 0, err
	}
	return total, nil
}

func fusedTabletRouteBlockLabAnchorCommon(
	anchors []FusedTabletRouteBlockLabAnchor,
) int {
	if len(anchors) <= 1 {
		return 0
	}
	common := len(anchors[1].Floor)
	for rank := 2; rank < len(anchors); rank++ {
		common = min(
			common,
			tabletAnchorMapLabPrefix(
				anchors[1].Floor, anchors[rank].Floor,
			),
		)
	}
	return common
}

func fusedTabletRouteBlockLabKindsValid(
	block, locator, anchor, leaf PageKind,
) bool {
	return block == PageTabletRoute &&
		locator == PagePrimaryLocator &&
		anchor == PagePrimaryAnchor &&
		leaf == PagePrimaryLeaf
}

func fusedTabletRouteBlockLabValidateRef(
	ref PageRef, logicalID uint64, kind PageKind, length int,
	selectedRootGeneration uint64, bounds GlobalTabletCatalogBounds,
) error {
	if ref.Offset == 0 || ref.Offset&4095 != 0 ||
		ref.Offset>>12 >= uint64(1)<<48 ||
		ref.LogicalID != logicalID ||
		ref.Generation == 0 || ref.Generation >= uint64(1)<<48 ||
		ref.Generation > selectedRootGeneration ||
		ref.Length != uint32(length) || ref.Kind != kind ||
		ref.Flags != 0 || ref.Aux != 0 || !bounds.contains(ref) {
		return fmt.Errorf("%w: fused child reference", ErrInvalidWrite)
	}
	return nil
}

func fusedTabletRouteBlockLabEncodeRef(dst []byte, ref PageRef) {
	segmentedTabletRouterPutUint48(dst, ref.Offset>>12)
	segmentedTabletRouterPutUint48(dst[6:], ref.Generation)
}

func fusedTabletRouteBlockLabDecodeRef(
	src []byte, logicalID uint64, kind PageKind, length int,
) PageRef {
	return PageRef{
		Offset:     segmentedTabletRouterGetUint48(src) << 12,
		LogicalID:  logicalID,
		Generation: segmentedTabletRouterGetUint48(src[6:]),
		Length:     uint32(length), Kind: kind,
	}
}

func fusedTabletRouteBlockLabCorrupt(detail string) error {
	return fmt.Errorf("%w: %s", ErrFusedTabletRouteBlockLabCorrupt, detail)
}
