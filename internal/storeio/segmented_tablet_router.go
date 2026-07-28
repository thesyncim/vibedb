package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

// The segmented tablet router is the production-candidate form of the 18/12
// primary routing experiment. One tablet has a 4 KiB root, an exact 8 KiB
// LocalID locator, and at most sixteen independently replaceable 8 KiB common
// PagePrimaryAnchor pages. A locator value is (stable page ID << 8) | stable
// row slot. Lexical ranks are a separate permutation inside each anchor page.
//
// The leaf handle is eighteen bytes:
//
//	u48 absolute offset in 8-byte units
//	u48 generation
//	u16 exact length minus one
//	4-byte zone
//
// Exact length is needed for byte-packed cold leaves. LogicalID is derived from
// BucketID and Kind is fixed by the tablet root, so neither is duplicated.
const (
	SegmentedTabletRouterRootBytes       = 4 << 10
	SegmentedTabletRouterLocatorBytes    = 8 << 10
	SegmentedTabletRouterAnchorPageBytes = 8 << 10
	SegmentedTabletRouterMaxPages        = 16
	SegmentedTabletRouterRowsPerPage     = 256
	SegmentedTabletRouterHandleBytes     = 18

	// Logical IDs form one explicit, disjoint durable namespace. StateRoot owns
	// 1. Every possible 30-bit leaf identity follows, then every possible
	// (18-bit tablet, 4-bit stable anchor-page) identity. The remaining hybrid
	// primary bands follow; dynamically allocated overflow, indexes, and
	// configuration pages start only after that complete shared namespace.
	SegmentedTabletRouterStateRootLogicalID    = StateRootLogicalID
	SegmentedTabletRouterLeafLogicalIDBase     = PrimaryLeafLogicalIDBase
	SegmentedTabletRouterLeafLogicalIDLimit    = PrimaryLeafLogicalIDLimit
	SegmentedTabletRouterAnchorLogicalIDBase   = PrimaryAnchorLogicalIDBase
	SegmentedTabletRouterAnchorLogicalIDLimit  = PrimaryAnchorLogicalIDLimit
	SegmentedTabletRouterFirstDynamicLogicalID = PrimaryFirstDynamicLogicalID

	segmentedTabletRouterRootHeaderBytes = 64
	segmentedTabletRouterRootRefBytes    = 13
	segmentedTabletRouterRootRefsAt      = segmentedTabletRouterRootHeaderBytes
	segmentedTabletRouterRootRanksAt     = segmentedTabletRouterRootRefsAt + SegmentedTabletRouterMaxPages*segmentedTabletRouterRootRefBytes
	segmentedTabletRouterRootOffsetsAt   = segmentedTabletRouterRootRanksAt + SegmentedTabletRouterMaxPages
	segmentedTabletRouterRootKeysAt      = segmentedTabletRouterRootOffsetsAt + (SegmentedTabletRouterMaxPages+1)*2
	segmentedTabletRouterRootTrailerAt   = SegmentedTabletRouterRootBytes - 8
	// The common page header owns StoreID, logical identity, generation,
	// physical size, kind, flags, and checksum. The anchor payload repeats
	// none of them: its eight-byte header is count, key bytes, common-prefix
	// width, head width, and two reserved zero bytes.
	segmentedTabletRouterAnchorPayloadHeaderBytes = 8
	segmentedTabletRouterAnchorRanksAt            = PageHeaderSize +
		segmentedTabletRouterAnchorPayloadHeaderBytes
	segmentedTabletRouterAnchorLocalIDsAt   = segmentedTabletRouterAnchorRanksAt + SegmentedTabletRouterRowsPerPage
	segmentedTabletRouterAnchorHandlesAt    = segmentedTabletRouterAnchorLocalIDsAt + SegmentedTabletRouterRowsPerPage*2
	segmentedTabletRouterAnchorOffsetsAt    = segmentedTabletRouterAnchorHandlesAt + SegmentedTabletRouterRowsPerPage*SegmentedTabletRouterHandleBytes
	segmentedTabletRouterAnchorKeysAt       = segmentedTabletRouterAnchorOffsetsAt + (SegmentedTabletRouterRowsPerPage+1)*2
	segmentedTabletRouterAnchorTrailerAt    = SegmentedTabletRouterAnchorPageBytes - PageTrailerSize
	segmentedTabletRouterAnchorPayloadBytes = SegmentedTabletRouterAnchorPageBytes -
		PageHeaderSize - PageTrailerSize
	segmentedTabletRouterAnchorKeyCapacity = segmentedTabletRouterAnchorTrailerAt - segmentedTabletRouterAnchorKeysAt

	segmentedTabletRouterRestart = 16
	segmentedTabletRouterEmpty   = uint16(0xffff)
	// Version 3 binds the private tablet root to StoreID and requires common
	// PagePrimaryAnchor children. Version 2 used the private STRPAGE1 anchor
	// envelope and must fail closed; no decoder fallback exists.
	segmentedTabletRouterVersion   = uint32(3)
	segmentedTabletRouterRootMagic = "STRROOT1"
)

var (
	// ErrSegmentedTabletRouterCorrupt reports that a router image failed
	// checksum or structural admission and must not be routed against.
	ErrSegmentedTabletRouterCorrupt = errors.New(
		"vibejson: corrupt segmented tablet router image",
	)
	// ErrSegmentedTabletRouterNoSpace reports that the image cannot admit
	// another route entry.
	ErrSegmentedTabletRouterNoSpace = errors.New(
		"vibejson: segmented tablet router image has no space",
	)
	// ErrSegmentedTabletRouterNotFound reports that no bucket routes the queried
	// key.
	ErrSegmentedTabletRouterNotFound = errors.New(
		"vibejson: segmented tablet router bucket not found",
	)
)

// SegmentedTabletRouterHeader is the root identity. AnchorKind identifies
// the fixed 8 KiB routing pages; LeafKind identifies the exact-length handles.
type SegmentedTabletRouterHeader struct {
	StoreID    [16]byte
	TabletID   uint32
	Generation uint64
	AnchorKind PageKind
	LeafKind   PageKind
}

// SegmentedTabletRouterLeaf is canonical encoder input. Leaves must be in
// strict lexical fence order. The first fence must be empty and denotes
// negative infinity. LocalID is stable and unique inside the tablet.
type SegmentedTabletRouterLeaf struct {
	LocalID uint16
	Fence   []byte
	Ref     PageRef
	Zone    BucketZone
}

// SegmentedTabletRouterRoute is a complete resident route. Hash is carried
// through to the ordered hash leaf, avoiding a second primary-key hash.
type SegmentedTabletRouterRoute struct {
	Bucket  BucketID
	Hash    uint64
	PageID  uint8
	RowSlot uint8
	Ref     PageRef
	Zone    BucketZone
}

type segmentedTabletRouterAnchorView struct {
	image      []byte
	ranks      []byte
	localIDs   []byte
	handles    []byte
	offsets    []byte
	keys       []byte
	heads      []byte
	headValid  []byte
	tabletID   uint32
	generation uint64
	pageID     uint8
	count      uint16
	common     uint8
	headBytes  uint8
	leafKind   PageKind
}

// SegmentedTabletRouterView borrows all checksum-admitted images. The fixed
// array avoids heap allocation and makes posting-driven page selection direct.
type SegmentedTabletRouterView struct {
	root        []byte
	locator     []byte
	pages       [SegmentedTabletRouterMaxPages]segmentedTabletRouterAnchorView
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

// SegmentedTabletRouterCursor walks current leaves in lexical order without
// following sibling pointers. Returned key-independent routes are exact.
type SegmentedTabletRouterCursor struct {
	view     *SegmentedTabletRouterView
	pageRank uint8
	rowRank  uint16
	valid    bool
}

// SegmentedTabletRouterCOWResult reports the exact immutable rewrite set.
type SegmentedTabletRouterCOWResult struct {
	Root       []byte
	AnchorPage []byte
	PageID     uint8
	Bytes      int
}

// SegmentedTabletRouterSplitResult reports one structural split. Locator,
// root, and both output anchor pages form one atomic publication unit.
type SegmentedTabletRouterSplitResult struct {
	Root        []byte
	Locator     []byte
	LeftPage    []byte
	RightPage   []byte
	LeftPageID  uint8
	RightPageID uint8
	Bytes       int
}

type segmentedTabletRouterFence struct {
	a, b, c []byte
}

func (f segmentedTabletRouterFence) length() int {
	return len(f.a) + len(f.b) + len(f.c)
}

func (f segmentedTabletRouterFence) at(index int) byte {
	if index < len(f.a) {
		return f.a[index]
	}
	index -= len(f.a)
	if index < len(f.b) {
		return f.b[index]
	}
	return f.c[index-len(f.b)]
}

func (f segmentedTabletRouterFence) copyTo(dst []byte, from int) int {
	n := 0
	for at := from; at < f.length(); at++ {
		dst[n] = f.at(at)
		n++
	}
	return n
}

func segmentedTabletRouterFencePrefix(
	left, right segmentedTabletRouterFence,
) int {
	limit := min(left.length(), right.length())
	for at := range limit {
		if left.at(at) != right.at(at) {
			return at
		}
	}
	return limit
}

func segmentedTabletRouterCompareFenceSuffixKey(
	restartPrefix, suffix, key []byte,
) int {
	compared := min(len(restartPrefix), len(key))
	if order := bytes.Compare(restartPrefix[:compared], key[:compared]); order != 0 {
		return order
	}
	if compared != len(restartPrefix) {
		return 1
	}
	key = key[compared:]
	compared = min(len(suffix), len(key))
	if order := bytes.Compare(suffix[:compared], key[:compared]); order != 0 {
		return order
	}
	if compared != len(suffix) {
		return 1
	}
	if compared < len(key) {
		return -1
	}
	return 0
}

func segmentedTabletRouterCompareFences(
	left, right segmentedTabletRouterFence,
) int {
	limit := min(left.length(), right.length())
	for at := range limit {
		if left.at(at) < right.at(at) {
			return -1
		}
		if left.at(at) > right.at(at) {
			return 1
		}
	}
	return left.length() - right.length()
}

// EncodeSegmentedTabletRouter builds the complete initial tablet. pageDst
// must contain one 8 KiB slice per required page, contiguously. anchorRefs are
// in lexical page order and become stable page IDs 0..pageCount-1.
func EncodeSegmentedTabletRouter(
	rootDst, locatorDst, pageDst []byte,
	header SegmentedTabletRouterHeader,
	anchorRefs []PageRef,
	leaves []SegmentedTabletRouterLeaf,
) (root, locator, pages []byte, pageCount int, err error) {
	if len(rootDst) < SegmentedTabletRouterRootBytes ||
		len(locatorDst) < SegmentedTabletRouterLocatorBytes ||
		header.StoreID == ([16]byte{}) ||
		header.TabletID >= TabletLocalIdentityTabletCount ||
		header.Generation == 0 || header.Generation >= uint64(1)<<48 ||
		header.AnchorKind != PagePrimaryAnchor ||
		header.LeafKind != PagePrimaryLeaf ||
		len(leaves) == 0 ||
		len(leaves) > TabletLocalIdentityLocalCount ||
		len(leaves) > SegmentedTabletRouterMaxPages*SegmentedTabletRouterRowsPerPage {
		return nil, nil, nil, 0, fmt.Errorf(
			"%w: segmented router identity or geometry", ErrInvalidWrite,
		)
	}
	pageCount = (len(leaves) + SegmentedTabletRouterRowsPerPage - 1) /
		SegmentedTabletRouterRowsPerPage
	if len(anchorRefs) != pageCount ||
		len(pageDst) < pageCount*SegmentedTabletRouterAnchorPageBytes {
		return nil, nil, nil, 0, fmt.Errorf(
			"%w: segmented router page destinations", ErrInvalidWrite,
		)
	}
	if len(leaves[0].Fence) != 0 {
		return nil, nil, nil, 0, fmt.Errorf(
			"%w: first segmented router fence is not empty", ErrInvalidWrite,
		)
	}
	locator = locatorDst[:SegmentedTabletRouterLocatorBytes]
	for at := range locator {
		locator[at] = 0xff
	}
	var previous []byte
	for rank, leaf := range leaves {
		if leaf.LocalID >= TabletLocalIdentityLocalCount ||
			rank != 0 && bytes.Compare(previous, leaf.Fence) >= 0 ||
			rank != 0 && len(leaf.Fence) == 0 {
			return nil, nil, nil, 0, fmt.Errorf(
				"%w: non-canonical leaf at rank %d", ErrInvalidWrite, rank,
			)
		}
		bucket, ok := MakeTabletLocalIdentityBucket(
			header.TabletID, uint32(leaf.LocalID),
		)
		refErr := segmentedTabletRouterValidateLeafRef(
			leaf.Ref, BucketID(bucket), header.LeafKind,
			header.Generation,
		)
		if !ok || refErr != nil {
			return nil, nil, nil, 0, fmt.Errorf(
				"%w: invalid leaf identity at rank %d: %v",
				ErrInvalidWrite, rank, refErr,
			)
		}
		if binary.LittleEndian.Uint16(locator[int(leaf.LocalID)*2:]) !=
			segmentedTabletRouterEmpty {
			return nil, nil, nil, 0, fmt.Errorf(
				"%w: duplicate LocalID", ErrInvalidWrite,
			)
		}
		pageID := rank / SegmentedTabletRouterRowsPerPage
		rowSlot := rank % SegmentedTabletRouterRowsPerPage
		binary.LittleEndian.PutUint16(
			locator[int(leaf.LocalID)*2:],
			uint16(pageID<<8|rowSlot),
		)
		previous = leaf.Fence
	}
	pages = pageDst[:pageCount*SegmentedTabletRouterAnchorPageBytes]
	for pageID := 0; pageID < pageCount; pageID++ {
		if anchorRefs[pageID].Generation != header.Generation {
			return nil, nil, nil, 0, fmt.Errorf(
				"%w: initial anchor generation", ErrInvalidWrite,
			)
		}
		if err := segmentedTabletRouterValidateAnchorRef(
			anchorRefs[pageID], header, uint8(pageID),
		); err != nil {
			return nil, nil, nil, 0, err
		}
		first := pageID * SegmentedTabletRouterRowsPerPage
		last := min(first+SegmentedTabletRouterRowsPerPage, len(leaves))
		if _, err := segmentedTabletRouterEncodeAnchorFromLeaves(
			pages[pageID*SegmentedTabletRouterAnchorPageBytes:],
			header, uint8(pageID), leaves[first:last],
		); err != nil {
			return nil, nil, nil, 0, err
		}
	}
	root = rootDst[:SegmentedTabletRouterRootBytes]
	if err := segmentedTabletRouterEncodeRootInitial(
		root, locator, header, anchorRefs, leaves,
	); err != nil {
		return nil, nil, nil, 0, err
	}
	return root, locator, pages, pageCount, nil
}

// OpenSegmentedTabletRouter admits one complete resident tablet. anchorPages
// is the concatenation of every live stable page ID from zero to pageCount-1.
// Unused stable IDs are represented by absent root refs, not dummy images.
func OpenSegmentedTabletRouter(
	root, locator, anchorPages []byte,
) (SegmentedTabletRouterView, error) {
	var view SegmentedTabletRouterView
	if len(root) != SegmentedTabletRouterRootBytes ||
		len(locator) != SegmentedTabletRouterLocatorBytes ||
		string(root[:8]) != segmentedTabletRouterRootMagic ||
		binary.LittleEndian.Uint32(root[8:12]) !=
			segmentedTabletRouterVersion ||
		binary.LittleEndian.Uint16(root[12:14]) !=
			segmentedTabletRouterRootHeaderBytes ||
		root[14] == 0 || root[14] > SegmentedTabletRouterMaxPages ||
		PageKind(root[15]) != PagePrimaryAnchor ||
		PageKind(root[16]) != PagePrimaryLeaf ||
		!allZero(root[17:20]) ||
		allZero(root[44:60]) ||
		!allZero(root[60:segmentedTabletRouterRootHeaderBytes]) {
		return view, segmentedTabletRouterCorrupt("root header")
	}
	pageCount := int(root[14])
	tabletID := binary.LittleEndian.Uint32(root[20:24])
	generation := binary.LittleEndian.Uint64(root[24:32])
	rootKeyBytes := int(binary.LittleEndian.Uint16(root[32:34]))
	if tabletID >= TabletLocalIdentityTabletCount || generation == 0 ||
		generation >= uint64(1)<<48 ||
		int(binary.LittleEndian.Uint16(root[34:36])) != pageCount ||
		binary.LittleEndian.Uint32(root[36:40]) != PageChecksum(locator) ||
		int(binary.LittleEndian.Uint32(root[40:44])) !=
			SegmentedTabletRouterRootBytes ||
		rootKeyBytes > segmentedTabletRouterRootTrailerAt-
			segmentedTabletRouterRootKeysAt ||
		len(anchorPages) != pageCount*SegmentedTabletRouterAnchorPageBytes ||
		!segmentedTabletRouterChecksumOK(
			root, segmentedTabletRouterRootTrailerAt,
		) {
		return view, segmentedTabletRouterCorrupt(
			"root identity, binding, geometry, or checksum",
		)
	}
	view = SegmentedTabletRouterView{
		root:        root,
		locator:     locator,
		rootRefs:    root[segmentedTabletRouterRootRefsAt:segmentedTabletRouterRootRanksAt],
		rootRanks:   root[segmentedTabletRouterRootRanksAt:segmentedTabletRouterRootOffsetsAt],
		rootOffsets: root[segmentedTabletRouterRootOffsetsAt:segmentedTabletRouterRootKeysAt],
		rootKeys:    root[segmentedTabletRouterRootKeysAt : segmentedTabletRouterRootKeysAt+rootKeyBytes],
		storeID:     [16]byte(root[44:60]),
		tabletID:    tabletID,
		generation:  generation,
		pageCount:   uint8(pageCount),
		anchorKind:  PageKind(root[15]),
		leafKind:    PageKind(root[16]),
	}
	if binary.LittleEndian.Uint16(view.rootOffsets[pageCount*2:]) !=
		uint16(rootKeyBytes) {
		return SegmentedTabletRouterView{},
			segmentedTabletRouterCorrupt("root terminal offset")
	}
	for rank := range pageCount {
		start := int(binary.LittleEndian.Uint16(view.rootOffsets[rank*2:]))
		end := int(binary.LittleEndian.Uint16(view.rootOffsets[(rank+1)*2:]))
		if start > end || end > rootKeyBytes {
			return SegmentedTabletRouterView{},
				segmentedTabletRouterCorrupt("root fence offsets")
		}
	}
	if !allZero(
		root[segmentedTabletRouterRootRanksAt+pageCount:segmentedTabletRouterRootOffsetsAt],
	) || !allZero(
		root[segmentedTabletRouterRootOffsetsAt+(pageCount+1)*2:segmentedTabletRouterRootKeysAt],
	) || !allZero(
		root[segmentedTabletRouterRootKeysAt+rootKeyBytes:segmentedTabletRouterRootTrailerAt],
	) {
		return SegmentedTabletRouterView{},
			segmentedTabletRouterCorrupt("root non-canonical padding")
	}
	var seenPages uint16
	for rank := range pageCount {
		pageID := view.rootRanks[rank]
		if int(pageID) >= pageCount ||
			seenPages&(uint16(1)<<pageID) != 0 {
			return SegmentedTabletRouterView{},
				segmentedTabletRouterCorrupt("root page permutation")
		}
		seenPages |= uint16(1) << pageID
		ref, ok := view.anchorRef(pageID)
		if !ok {
			return SegmentedTabletRouterView{},
				segmentedTabletRouterCorrupt("root anchor ref")
		}
		image := anchorPages[int(pageID)*SegmentedTabletRouterAnchorPageBytes : (int(pageID)+1)*SegmentedTabletRouterAnchorPageBytes]
		page, err := segmentedTabletRouterOpenAnchor(
			image, view, pageID, ref,
		)
		if err != nil {
			return SegmentedTabletRouterView{}, err
		}
		view.pages[pageID] = page
		if rank == 0 {
			fence := page.fenceAt(0)
			if fence.length() != 0 || view.rootFence(rank).length() != 0 {
				return SegmentedTabletRouterView{},
					segmentedTabletRouterCorrupt("non-empty first floor")
			}
		} else if segmentedTabletRouterCompareFences(
			view.rootFence(rank), page.fenceAt(0),
		) != 0 || segmentedTabletRouterCompareFences(
			view.rootFence(rank-1), view.rootFence(rank),
		) >= 0 {
			return SegmentedTabletRouterView{},
				segmentedTabletRouterCorrupt("root lexical floors")
		}
	}
	for pageID := range SegmentedTabletRouterMaxPages {
		start := pageID * segmentedTabletRouterRootRefBytes
		if seenPages&(uint16(1)<<pageID) == 0 &&
			!allZero(view.rootRefs[start:start+
				segmentedTabletRouterRootRefBytes]) {
			return SegmentedTabletRouterView{},
				segmentedTabletRouterCorrupt("unranked anchor ref")
		}
	}
	if err := view.validateLocator(); err != nil {
		return SegmentedTabletRouterView{}, err
	}
	return view, nil
}

func (v *SegmentedTabletRouterView) validateLocator() error {
	live := 0
	for localID := range TabletLocalIdentityLocalCount {
		code := binary.LittleEndian.Uint16(v.locator[localID*2:])
		if code == segmentedTabletRouterEmpty {
			continue
		}
		pageID, slot := uint8(code>>8), uint8(code)
		if pageID >= SegmentedTabletRouterMaxPages ||
			len(v.pages[pageID].image) == 0 ||
			binary.LittleEndian.Uint16(
				v.pages[pageID].localIDs[int(slot)*2:],
			) != uint16(localID) {
			return segmentedTabletRouterCorrupt("locator binding")
		}
		live++
	}
	rows := 0
	for pageID := range SegmentedTabletRouterMaxPages {
		page := &v.pages[pageID]
		if len(page.image) == 0 {
			continue
		}
		rows += int(page.count)
		for rank := 0; rank < int(page.count); rank++ {
			slot := page.ranks[rank]
			localID := binary.LittleEndian.Uint16(page.localIDs[int(slot)*2:])
			if localID >= TabletLocalIdentityLocalCount ||
				binary.LittleEndian.Uint16(v.locator[int(localID)*2:]) !=
					uint16(pageID<<8|int(slot)) {
				return segmentedTabletRouterCorrupt("row locator inverse")
			}
		}
	}
	if rows != live {
		return segmentedTabletRouterCorrupt("locator cardinality")
	}
	return nil
}

func segmentedTabletRouterOpenAnchor(
	image []byte,
	root SegmentedTabletRouterView,
	pageID uint8,
	ref PageRef,
) (segmentedTabletRouterAnchorView, error) {
	var view segmentedTabletRouterAnchorView
	if len(image) != SegmentedTabletRouterAnchorPageBytes {
		return view, segmentedTabletRouterCorrupt("anchor extent")
	}
	header, payload, err := OpenPage(image)
	if err != nil {
		return view, segmentedTabletRouterCorrupt("anchor common envelope")
	}
	logicalID, ok := SegmentedTabletRouterAnchorLogicalID(
		root.tabletID, pageID,
	)
	if !ok ||
		header.StoreID != root.storeID ||
		header.Generation != ref.Generation ||
		header.LogicalID != logicalID ||
		header.PageSize != SegmentedTabletRouterAnchorPageBytes ||
		header.PayloadLength != segmentedTabletRouterAnchorPayloadBytes ||
		header.Kind != PagePrimaryAnchor ||
		header.Flags != 0 ||
		len(payload) != segmentedTabletRouterAnchorPayloadBytes {
		return view, segmentedTabletRouterCorrupt(
			"anchor header, identity, or checksum",
		)
	}
	count := int(binary.LittleEndian.Uint16(payload[0:2]))
	keyBytes := int(binary.LittleEndian.Uint16(payload[2:4]))
	common := int(payload[4])
	headBytes := int(payload[5])
	if count == 0 || count > SegmentedTabletRouterRowsPerPage ||
		keyBytes < common || keyBytes > segmentedTabletRouterAnchorKeyCapacity ||
		headBytes != 0 && headBytes != 1 &&
			headBytes != 2 && headBytes != 4 ||
		!allZero(payload[6:segmentedTabletRouterAnchorPayloadHeaderBytes]) {
		return view, segmentedTabletRouterCorrupt("anchor geometry")
	}
	view = segmentedTabletRouterAnchorView{
		image:    image,
		ranks:    image[segmentedTabletRouterAnchorRanksAt:segmentedTabletRouterAnchorLocalIDsAt],
		localIDs: image[segmentedTabletRouterAnchorLocalIDsAt:segmentedTabletRouterAnchorHandlesAt],
		handles:  image[segmentedTabletRouterAnchorHandlesAt:segmentedTabletRouterAnchorOffsetsAt],
		offsets:  image[segmentedTabletRouterAnchorOffsetsAt:segmentedTabletRouterAnchorKeysAt],
		keys:     image[segmentedTabletRouterAnchorKeysAt : segmentedTabletRouterAnchorKeysAt+keyBytes],
		tabletID: root.tabletID,
		// The common header is bound to ref.Generation (the page's physical
		// birth). Child visibility is selected by the snapshot root.
		generation: root.generation,
		pageID:     pageID,
		count:      uint16(count),
		common:     uint8(common),
		headBytes:  uint8(headBytes),
		leafKind:   root.leafKind,
	}
	dataBytes := int(binary.LittleEndian.Uint16(view.offsets[count*2:]))
	headExtra := 0
	if headBytes != 0 {
		headExtra = count*headBytes + SegmentedTabletRouterRowsPerPage/8
	}
	if dataBytes < common || dataBytes+headExtra != keyBytes {
		return segmentedTabletRouterAnchorView{},
			segmentedTabletRouterCorrupt("anchor terminal offset")
	}
	keyRegion := view.keys
	if headBytes != 0 {
		view.heads = keyRegion[dataBytes : dataBytes+count*headBytes]
		view.headValid = keyRegion[dataBytes+count*headBytes:]
	}
	view.keys = keyRegion[:dataBytes]
	if !allZero(view.ranks[count:]) ||
		!allZero(view.offsets[(count+1)*2:]) ||
		!allZero(image[segmentedTabletRouterAnchorKeysAt+keyBytes:segmentedTabletRouterAnchorTrailerAt]) {
		return segmentedTabletRouterAnchorView{},
			segmentedTabletRouterCorrupt("anchor non-canonical padding")
	}
	var seen [4]uint64
	var previous segmentedTabletRouterFence
	var restart segmentedTabletRouterFence
	expectedCommon := -1
	for rank := range count {
		slot := view.ranks[rank]
		word, bit := slot>>6, uint64(1)<<(slot&63)
		if seen[word]&bit != 0 {
			return segmentedTabletRouterAnchorView{},
				segmentedTabletRouterCorrupt("duplicate row slot")
		}
		seen[word] |= bit
		localID := binary.LittleEndian.Uint16(view.localIDs[int(slot)*2:])
		if localID >= TabletLocalIdentityLocalCount {
			return segmentedTabletRouterAnchorView{},
				segmentedTabletRouterCorrupt("row LocalID")
		}
		bucket, _ := MakeTabletLocalIdentityBucket(
			root.tabletID, uint32(localID),
		)
		if _, _, ok := view.handleAt(slot, BucketID(bucket)); !ok {
			return segmentedTabletRouterAnchorView{},
				segmentedTabletRouterCorrupt("leaf handle")
		}
		fence, ok := view.fenceAtChecked(rank)
		if !ok || rank == 0 && pageID == root.rootRanks[0] &&
			fence.length() != 0 ||
			rank != 0 && segmentedTabletRouterCompareFences(
				previous, fence,
			) >= 0 {
			return segmentedTabletRouterAnchorView{},
				segmentedTabletRouterCorrupt("anchor fences")
		}
		if rank == 0 {
			expectedCommon = fence.length()
		} else {
			expectedCommon = min(
				expectedCommon,
				segmentedTabletRouterFencePrefix(
					view.fenceAt(0), fence,
				),
			)
		}
		start := int(binary.LittleEndian.Uint16(view.offsets[rank*2:]))
		if rank%segmentedTabletRouterRestart == 0 {
			restart = fence
			if view.keys[start] != 0 {
				return segmentedTabletRouterAnchorView{},
					segmentedTabletRouterCorrupt("restart sharing")
			}
		} else {
			wantShared := segmentedTabletRouterFencePrefix(
				restart, fence,
			) - common
			if wantShared < 0 || int(view.keys[start]) != wantShared {
				return segmentedTabletRouterAnchorView{},
					segmentedTabletRouterCorrupt("non-canonical sharing")
			}
		}
		previous = fence
	}
	if expectedCommon != common {
		return segmentedTabletRouterAnchorView{},
			segmentedTabletRouterCorrupt("non-canonical common prefix")
	}
	expectedHeadBytes := 0
	for _, candidate := range [...]int{4, 2, 1} {
		if dataBytes+count*candidate+
			SegmentedTabletRouterRowsPerPage/8 <=
			segmentedTabletRouterAnchorKeyCapacity {
			expectedHeadBytes = candidate
			break
		}
	}
	if expectedHeadBytes != headBytes {
		return segmentedTabletRouterAnchorView{},
			segmentedTabletRouterCorrupt("non-canonical head accelerator")
	}
	for rank := 0; rank < count && headBytes != 0; rank++ {
		fence := view.fenceAt(rank)
		valid := fence.length()-common >= headBytes
		bit := view.headValid[rank>>3]&(byte(1)<<uint(rank&7)) != 0
		if valid != bit {
			return segmentedTabletRouterAnchorView{},
				segmentedTabletRouterCorrupt("anchor head validity")
		}
		if valid {
			for at := range headBytes {
				if view.heads[rank*headBytes+at] != fence.at(common+at) {
					return segmentedTabletRouterAnchorView{},
						segmentedTabletRouterCorrupt("anchor head value")
				}
			}
		} else if !allZero(
			view.heads[rank*headBytes : (rank+1)*headBytes],
		) {
			return segmentedTabletRouterAnchorView{},
				segmentedTabletRouterCorrupt("anchor short head")
		}
	}
	for slot := range SegmentedTabletRouterRowsPerPage {
		word, bit := uint8(slot)>>6, uint64(1)<<(uint8(slot)&63)
		localID := binary.LittleEndian.Uint16(view.localIDs[slot*2:])
		handle := view.handles[slot*SegmentedTabletRouterHandleBytes : (slot+1)*SegmentedTabletRouterHandleBytes]
		if seen[word]&bit == 0 &&
			(localID != segmentedTabletRouterEmpty || !allZero(handle)) {
			return segmentedTabletRouterAnchorView{},
				segmentedTabletRouterCorrupt("unused row state")
		}
	}
	return view, nil
}

// segmentedTabletRouterAdmittedAnchor reconstructs the fixed slices and
// scalars of an anchor whose complete structure was already checked by cache
// admission. It deliberately performs no bounds or checksum work.
func segmentedTabletRouterAdmittedAnchor(
	image []byte,
	root SegmentedTabletRouterView,
	pageID uint8,
) segmentedTabletRouterAnchorView {
	payload := image[PageHeaderSize:segmentedTabletRouterAnchorTrailerAt]
	count := int(binary.LittleEndian.Uint16(payload[0:2]))
	keyBytes := int(binary.LittleEndian.Uint16(payload[2:4]))
	headBytes := int(payload[5])
	view := segmentedTabletRouterAnchorView{
		image:      image,
		ranks:      image[segmentedTabletRouterAnchorRanksAt:segmentedTabletRouterAnchorLocalIDsAt],
		localIDs:   image[segmentedTabletRouterAnchorLocalIDsAt:segmentedTabletRouterAnchorHandlesAt],
		handles:    image[segmentedTabletRouterAnchorHandlesAt:segmentedTabletRouterAnchorOffsetsAt],
		offsets:    image[segmentedTabletRouterAnchorOffsetsAt:segmentedTabletRouterAnchorKeysAt],
		keys:       image[segmentedTabletRouterAnchorKeysAt : segmentedTabletRouterAnchorKeysAt+keyBytes],
		tabletID:   root.tabletID,
		generation: root.generation,
		pageID:     pageID,
		count:      uint16(count),
		common:     payload[4],
		headBytes:  uint8(headBytes),
		leafKind:   root.leafKind,
	}
	dataBytes := int(binary.LittleEndian.Uint16(view.offsets[count*2:]))
	if headBytes != 0 {
		view.heads = view.keys[dataBytes : dataBytes+count*headBytes]
		view.headValid = view.keys[dataBytes+count*headBytes:]
	}
	view.keys = view.keys[:dataBytes]
	return view
}

// Route hashes once and completes the resident root + anchor-page route.
func (v *SegmentedTabletRouterView) Route(
	seed [16]byte, key []byte,
) SegmentedTabletRouterRoute {
	return v.RouteHashed(KeyHashBytes(seed, key), key)
}

// RouteHashed reuses a caller-owned hash. The route is allocation-free.
func (v *SegmentedTabletRouterView) RouteHashed(
	hash uint64, key []byte,
) SegmentedTabletRouterRoute {
	if v == nil || len(v.root) == 0 {
		return SegmentedTabletRouterRoute{Hash: hash}
	}
	pageRank := v.rootUpperBound(key) - 1
	pageID := v.rootRanks[pageRank]
	page := &v.pages[pageID]
	rowRank := page.upperBound(key) - 1
	return page.routeAt(rowRank, hash)
}

// ResolveBucketID is the posting-driven route through the exact 8 KiB dense
// locator. It performs no lexical comparisons.
func (v *SegmentedTabletRouterView) ResolveBucketID(
	bucket BucketID,
) (PageRef, BucketZone, bool) {
	if v == nil || uint32(bucket) >= PrimaryBucketIDLimit ||
		uint32(bucket)>>TabletLocalIdentityLocalBits != v.tabletID {
		return PageRef{}, BucketZone{}, false
	}
	localID := uint16(uint32(bucket) & (TabletLocalIdentityLocalCount - 1))
	code := binary.LittleEndian.Uint16(v.locator[int(localID)*2:])
	if code == segmentedTabletRouterEmpty {
		return PageRef{}, BucketZone{}, false
	}
	pageID, slot := uint8(code>>8), uint8(code)
	if pageID >= SegmentedTabletRouterMaxPages ||
		len(v.pages[pageID].image) == 0 ||
		binary.LittleEndian.Uint16(
			v.pages[pageID].localIDs[int(slot)*2:],
		) != localID {
		return PageRef{}, BucketZone{}, false
	}
	return v.pages[pageID].handleAt(slot, bucket)
}

// LowerBound returns the leaf interval that can contain target. Next walks
// anchor lexical ranks and then the root's page-rank permutation.
func (v *SegmentedTabletRouterView) LowerBound(
	target []byte,
) SegmentedTabletRouterCursor {
	if v == nil || len(v.root) == 0 {
		return SegmentedTabletRouterCursor{}
	}
	pageRank := v.rootUpperBound(target) - 1
	pageID := v.rootRanks[pageRank]
	rowRank := v.pages[pageID].upperBound(target) - 1
	return SegmentedTabletRouterCursor{
		view: v, pageRank: uint8(pageRank), rowRank: uint16(rowRank), valid: true,
	}
}

func (c *SegmentedTabletRouterCursor) Route(
	hash uint64,
) (SegmentedTabletRouterRoute, bool) {
	if c == nil || !c.valid {
		return SegmentedTabletRouterRoute{}, false
	}
	pageID := c.view.rootRanks[c.pageRank]
	return c.view.pages[pageID].routeAt(int(c.rowRank), hash), true
}

func (c *SegmentedTabletRouterCursor) Next() bool {
	if c == nil || !c.valid {
		return false
	}
	pageID := c.view.rootRanks[c.pageRank]
	if int(c.rowRank)+1 < int(c.view.pages[pageID].count) {
		c.rowRank++
		return true
	}
	if int(c.pageRank)+1 >= int(c.view.pageCount) {
		c.valid = false
		return false
	}
	c.pageRank++
	c.rowRank = 0
	return true
}

func (v *SegmentedTabletRouterView) rootUpperBound(key []byte) int {
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

func (v *SegmentedTabletRouterView) rootFence(rank int) segmentedTabletRouterFence {
	start := int(binary.LittleEndian.Uint16(v.rootOffsets[rank*2:]))
	end := int(binary.LittleEndian.Uint16(v.rootOffsets[(rank+1)*2:]))
	return segmentedTabletRouterFence{a: v.rootKeys[start:end]}
}

func (p *segmentedTabletRouterAnchorView) upperBound(key []byte) int {
	common := p.keys[:p.common]
	compared := min(len(common), len(key))
	if order := bytes.Compare(common[:compared], key[:compared]); order != 0 {
		if order > 0 {
			return 1
		}
		return int(p.count)
	}
	if compared != len(common) {
		return 1
	}
	key = key[compared:]
	switch p.headBytes {
	case 4:
		if len(key) >= 4 {
			want := binary.BigEndian.Uint32(key)
			low, high := 1, int(p.count)
			for low < high {
				mid := int(uint(low+high) >> 1)
				if p.headValid[mid>>3]&(byte(1)<<uint(mid&7)) != 0 {
					got := binary.BigEndian.Uint32(p.heads[mid*4:])
					if got < want {
						low = mid + 1
						continue
					}
					if got > want {
						high = mid
						continue
					}
				}
				fence := p.fenceAt(mid)
				if segmentedTabletRouterCompareFenceSuffixKey(
					fence.b, fence.c, key,
				) <= 0 {
					low = mid + 1
				} else {
					high = mid
				}
			}
			return low
		}
	case 2:
		if len(key) >= 2 {
			want := binary.BigEndian.Uint16(key)
			low, high := 1, int(p.count)
			for low < high {
				mid := int(uint(low+high) >> 1)
				if p.headValid[mid>>3]&(byte(1)<<uint(mid&7)) != 0 {
					got := binary.BigEndian.Uint16(p.heads[mid*2:])
					if got < want {
						low = mid + 1
						continue
					}
					if got > want {
						high = mid
						continue
					}
				}
				fence := p.fenceAt(mid)
				if segmentedTabletRouterCompareFenceSuffixKey(
					fence.b, fence.c, key,
				) <= 0 {
					low = mid + 1
				} else {
					high = mid
				}
			}
			return low
		}
	case 1:
		if len(key) >= 1 {
			want := key[0]
			low, high := 1, int(p.count)
			for low < high {
				mid := int(uint(low+high) >> 1)
				if p.headValid[mid>>3]&(byte(1)<<uint(mid&7)) != 0 {
					got := p.heads[mid]
					if got < want {
						low = mid + 1
						continue
					}
					if got > want {
						high = mid
						continue
					}
				}
				fence := p.fenceAt(mid)
				if segmentedTabletRouterCompareFenceSuffixKey(
					fence.b, fence.c, key,
				) <= 0 {
					low = mid + 1
				} else {
					high = mid
				}
			}
			return low
		}
	}
	low, high := 1, int(p.count)
	for low < high {
		mid := int(uint(low+high) >> 1)
		fence := p.fenceAt(mid)
		if segmentedTabletRouterCompareFenceSuffixKey(
			fence.b, fence.c, key,
		) <= 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low
}

func (p *segmentedTabletRouterAnchorView) fenceAt(
	rank int,
) segmentedTabletRouterFence {
	fence, _ := p.fenceAtChecked(rank)
	return fence
}

func (p *segmentedTabletRouterAnchorView) fenceAtChecked(
	rank int,
) (segmentedTabletRouterFence, bool) {
	if rank < 0 || rank >= int(p.count) || len(p.keys) < int(p.common) {
		return segmentedTabletRouterFence{}, false
	}
	start := int(binary.LittleEndian.Uint16(p.offsets[rank*2:]))
	end := int(binary.LittleEndian.Uint16(p.offsets[(rank+1)*2:]))
	if start < int(p.common) || start > end || end > len(p.keys) ||
		end-start < 1 {
		return segmentedTabletRouterFence{}, false
	}
	shared := int(p.keys[start])
	var restart []byte
	if rank%segmentedTabletRouterRestart != 0 {
		restartRank := rank - rank%segmentedTabletRouterRestart
		restartStart := int(binary.LittleEndian.Uint16(
			p.offsets[restartRank*2:],
		))
		restartEnd := int(binary.LittleEndian.Uint16(
			p.offsets[(restartRank+1)*2:],
		))
		if restartStart < int(p.common) || restartEnd-restartStart < 1 ||
			restartEnd > len(p.keys) || p.keys[restartStart] != 0 ||
			restartStart > restartEnd {
			return segmentedTabletRouterFence{}, false
		}
		restart = p.keys[restartStart+1 : restartEnd]
		if shared > len(restart) {
			return segmentedTabletRouterFence{}, false
		}
	} else if shared != 0 {
		return segmentedTabletRouterFence{}, false
	}
	return segmentedTabletRouterFence{
		a: p.keys[:p.common],
		b: restart[:shared],
		c: p.keys[start+1 : end],
	}, true
}

func (p *segmentedTabletRouterAnchorView) routeAt(
	rank int, hash uint64,
) SegmentedTabletRouterRoute {
	slot := p.ranks[rank]
	localID := binary.LittleEndian.Uint16(p.localIDs[int(slot)*2:])
	bucket, _ := MakeTabletLocalIdentityBucket(
		p.tabletID, uint32(localID),
	)
	ref, zone, _ := p.handleAt(slot, BucketID(bucket))
	return SegmentedTabletRouterRoute{
		Bucket: BucketID(bucket), Hash: hash, PageID: p.pageID,
		RowSlot: slot, Ref: ref, Zone: zone,
	}
}

func (p *segmentedTabletRouterAnchorView) handleAt(
	slot uint8, bucket BucketID,
) (PageRef, BucketZone, bool) {
	start := int(slot) * SegmentedTabletRouterHandleBytes
	src := p.handles[start : start+SegmentedTabletRouterHandleBytes]
	offsetUnits := segmentedTabletRouterGetUint48(src)
	generation := segmentedTabletRouterGetUint48(src[6:])
	length := uint32(binary.LittleEndian.Uint16(src[12:14])) + 1
	var zone BucketZone
	copy(zone[:], src[14:18])
	logicalID, ok := SegmentedTabletRouterLeafLogicalID(bucket)
	if !ok {
		return PageRef{}, BucketZone{}, false
	}
	ref := PageRef{
		Offset: offsetUnits << 3, LogicalID: logicalID,
		Generation: generation, Length: length, Kind: p.leafKind,
	}
	if segmentedTabletRouterValidateLeafRef(
		ref, bucket, p.leafKind, p.generation,
	) != nil {
		return PageRef{}, BucketZone{}, false
	}
	return ref, zone, true
}

func (v *SegmentedTabletRouterView) anchorRef(
	pageID uint8,
) (PageRef, bool) {
	start := int(pageID) * segmentedTabletRouterRootRefBytes
	src := v.rootRefs[start : start+segmentedTabletRouterRootRefBytes]
	if allZero(src) {
		return PageRef{}, false
	}
	logicalID, ok := SegmentedTabletRouterAnchorLogicalID(
		v.tabletID, pageID,
	)
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
	return ref, v.storeID != ([16]byte{}) &&
		v.anchorKind == PagePrimaryAnchor &&
		v.leafKind == PagePrimaryLeaf &&
		segmentedTabletRouterValidateAnchorRefIdentity(
			ref, v.tabletID, v.generation, pageID,
		) == nil
}

// RewriteHandle performs the ordinary COW path. It reads the locator to find
// the stable anchor row but never rewrites it. Exactly one 8 KiB anchor page
// and the 4 KiB root are produced.
func (v *SegmentedTabletRouterView) RewriteHandle(
	rootDst, pageDst []byte,
	generation uint64,
	bucket BucketID,
	leafRef PageRef,
	zone BucketZone,
	anchorRef PageRef,
) (SegmentedTabletRouterCOWResult, error) {
	var result SegmentedTabletRouterCOWResult
	if v == nil || len(rootDst) < SegmentedTabletRouterRootBytes ||
		len(pageDst) < SegmentedTabletRouterAnchorPageBytes ||
		generation <= v.generation || generation >= uint64(1)<<48 {
		return result, fmt.Errorf("%w: ordinary COW geometry", ErrInvalidWrite)
	}
	tabletID, localID, ok := SplitTabletLocalIdentityBucket(uint32(bucket))
	if !ok || tabletID != v.tabletID {
		return result, ErrSegmentedTabletRouterNotFound
	}
	code := binary.LittleEndian.Uint16(v.locator[int(localID)*2:])
	if code == segmentedTabletRouterEmpty {
		return result, ErrSegmentedTabletRouterNotFound
	}
	pageID, slot := uint8(code>>8), uint8(code)
	return v.rewriteHandleAt(
		rootDst, pageDst, generation, bucket, pageID, slot,
		leafRef, zone, anchorRef,
	)
}

func (v *SegmentedTabletRouterView) rewriteHandleAt(
	rootDst, pageDst []byte,
	generation uint64,
	bucket BucketID,
	pageID, slot uint8,
	leafRef PageRef,
	zone BucketZone,
	anchorRef PageRef,
) (SegmentedTabletRouterCOWResult, error) {
	var result SegmentedTabletRouterCOWResult
	if v == nil || len(rootDst) < SegmentedTabletRouterRootBytes ||
		len(pageDst) < SegmentedTabletRouterAnchorPageBytes ||
		generation <= v.generation || generation >= uint64(1)<<48 {
		return result, fmt.Errorf("%w: ordinary COW geometry", ErrInvalidWrite)
	}
	tabletID, localID, ok := SplitTabletLocalIdentityBucket(uint32(bucket))
	if !ok || tabletID != v.tabletID ||
		pageID >= SegmentedTabletRouterMaxPages {
		return result, ErrSegmentedTabletRouterNotFound
	}
	page := &v.pages[pageID]
	if len(page.image) == 0 ||
		binary.LittleEndian.Uint16(page.localIDs[int(slot)*2:]) != localID ||
		leafRef.Generation != generation ||
		segmentedTabletRouterValidateLeafRef(
			leafRef, bucket, v.leafKind, generation,
		) != nil {
		return result, fmt.Errorf("%w: ordinary COW leaf", ErrInvalidWrite)
	}
	if anchorRef.Generation != generation ||
		segmentedTabletRouterValidateAnchorRefIdentity(
			anchorRef, v.tabletID, generation, pageID,
		) != nil {
		return result, fmt.Errorf("%w: ordinary COW anchor ref", ErrInvalidWrite)
	}
	nextPage := pageDst[:SegmentedTabletRouterAnchorPageBytes]
	copy(nextPage, page.image)
	binary.LittleEndian.PutUint64(nextPage[24:32], generation)
	segmentedTabletRouterEncodeLeafHandle(
		nextPage[segmentedTabletRouterAnchorHandlesAt+
			int(slot)*SegmentedTabletRouterHandleBytes:],
		leafRef, zone,
	)
	// page.image was admitted through OpenPage and this path changes only the
	// common generation field plus one fixed-width exact handle. Recomputing
	// the common CRC directly avoids re-decoding an already-proven envelope on
	// the ordinary update path.
	segmentedTabletRouterSeal(
		nextPage, segmentedTabletRouterAnchorTrailerAt,
	)
	nextRoot := rootDst[:SegmentedTabletRouterRootBytes]
	copy(nextRoot, v.root)
	binary.LittleEndian.PutUint64(nextRoot[24:32], generation)
	segmentedTabletRouterEncodeAnchorRef(
		nextRoot[segmentedTabletRouterRootRefsAt+
			int(pageID)*segmentedTabletRouterRootRefBytes:],
		anchorRef,
	)
	segmentedTabletRouterSeal(
		nextRoot, segmentedTabletRouterRootTrailerAt,
	)
	return SegmentedTabletRouterCOWResult{
		Root: nextRoot, AnchorPage: nextPage, PageID: pageID,
		Bytes: SegmentedTabletRouterRootBytes +
			SegmentedTabletRouterAnchorPageBytes,
	}, nil
}

// SplitAnchorPage splits one lexical page at splitRank. Stable row slots are
// preserved; only moved LocalIDs change their high locator byte.
func (v *SegmentedTabletRouterView) SplitAnchorPage(
	rootDst, locatorDst, leftDst, rightDst []byte,
	generation uint64,
	pageID, newPageID uint8,
	splitRank int,
	leftRef, rightRef PageRef,
) (SegmentedTabletRouterSplitResult, error) {
	var result SegmentedTabletRouterSplitResult
	if v == nil || len(rootDst) < SegmentedTabletRouterRootBytes ||
		len(locatorDst) < SegmentedTabletRouterLocatorBytes ||
		len(leftDst) < SegmentedTabletRouterAnchorPageBytes ||
		len(rightDst) < SegmentedTabletRouterAnchorPageBytes ||
		generation <= v.generation || generation >= uint64(1)<<48 ||
		pageID >= SegmentedTabletRouterMaxPages ||
		newPageID >= SegmentedTabletRouterMaxPages ||
		pageID == newPageID || len(v.pages[pageID].image) == 0 ||
		len(v.pages[newPageID].image) != 0 ||
		newPageID != v.pageCount ||
		int(v.pageCount) >= SegmentedTabletRouterMaxPages {
		return result, fmt.Errorf("%w: structural split geometry", ErrInvalidWrite)
	}
	source := &v.pages[pageID]
	if splitRank <= 0 || splitRank >= int(source.count) {
		return result, fmt.Errorf("%w: structural split rank", ErrInvalidWrite)
	}
	nextHeader := SegmentedTabletRouterHeader{
		StoreID: v.storeID, TabletID: v.tabletID, Generation: generation,
		AnchorKind: v.anchorKind, LeafKind: v.leafKind,
	}
	if leftRef.Generation != generation || rightRef.Generation != generation ||
		segmentedTabletRouterValidateAnchorRefIdentity(
			leftRef, v.tabletID, generation, pageID,
		) != nil ||
		segmentedTabletRouterValidateAnchorRefIdentity(
			rightRef, v.tabletID, generation, newPageID,
		) != nil {
		return result, fmt.Errorf("%w: structural split refs", ErrInvalidWrite)
	}
	left := leftDst[:SegmentedTabletRouterAnchorPageBytes]
	right := rightDst[:SegmentedTabletRouterAnchorPageBytes]
	if _, err := segmentedTabletRouterEncodeAnchorSubset(
		left, nextHeader, pageID, source, 0, splitRank,
	); err != nil {
		return result, err
	}
	if _, err := segmentedTabletRouterEncodeAnchorSubset(
		right, nextHeader, newPageID, source, splitRank, int(source.count),
	); err != nil {
		return result, err
	}
	locator := locatorDst[:SegmentedTabletRouterLocatorBytes]
	copy(locator, v.locator)
	for rank := splitRank; rank < int(source.count); rank++ {
		slot := source.ranks[rank]
		localID := binary.LittleEndian.Uint16(source.localIDs[int(slot)*2:])
		binary.LittleEndian.PutUint16(
			locator[int(localID)*2:],
			uint16(newPageID)<<8|uint16(slot),
		)
	}
	root := rootDst[:SegmentedTabletRouterRootBytes]
	if err := v.encodeSplitRoot(
		root, locator, nextHeader, pageID, newPageID,
		source.fenceAt(splitRank), leftRef, rightRef,
	); err != nil {
		return result, err
	}
	return SegmentedTabletRouterSplitResult{
		Root: root, Locator: locator, LeftPage: left, RightPage: right,
		LeftPageID: pageID, RightPageID: newPageID,
		Bytes: SegmentedTabletRouterRootBytes +
			SegmentedTabletRouterLocatorBytes +
			2*SegmentedTabletRouterAnchorPageBytes,
	}, nil
}

// RoutingBytesPerDocument reports the exact fixed routing footprint at a
// specified leaf count and average leaf occupancy.
func SegmentedTabletRouterRoutingBytesPerDocument(
	leafCount, rowsPerLeaf int,
) float64 {
	if leafCount <= 0 || leafCount > TabletLocalIdentityLocalCount ||
		rowsPerLeaf <= 0 {
		return 0
	}
	pages := (leafCount + SegmentedTabletRouterRowsPerPage - 1) /
		SegmentedTabletRouterRowsPerPage
	bytes := SegmentedTabletRouterRootBytes +
		SegmentedTabletRouterLocatorBytes +
		pages*SegmentedTabletRouterAnchorPageBytes
	return float64(bytes) / float64(leafCount*rowsPerLeaf)
}

// SegmentedTabletRouterLeafLogicalID derives the collision-free durable
// logical ID for one posting-stable 30-bit BucketID.
func SegmentedTabletRouterLeafLogicalID(
	bucket BucketID,
) (uint64, bool) {
	if uint32(bucket) >= PrimaryBucketIDLimit {
		return 0, false
	}
	return SegmentedTabletRouterLeafLogicalIDBase + uint64(bucket), true
}

// SegmentedTabletRouterAnchorLogicalID derives the collision-free durable
// logical ID for one stable anchor-page identity.
func SegmentedTabletRouterAnchorLogicalID(
	tabletID uint32, pageID uint8,
) (uint64, bool) {
	if tabletID >= TabletLocalIdentityTabletCount ||
		pageID >= SegmentedTabletRouterMaxPages {
		return 0, false
	}
	ordinal := uint64(tabletID)*SegmentedTabletRouterMaxPages +
		uint64(pageID)
	return SegmentedTabletRouterAnchorLogicalIDBase + ordinal, true
}

// segmentedTabletRouterAnchorLogicalID is the terse internal form used only
// after geometry has already been validated.
func segmentedTabletRouterAnchorLogicalID(
	tabletID uint32, pageID uint8,
) uint64 {
	logicalID, _ := SegmentedTabletRouterAnchorLogicalID(tabletID, pageID)
	return logicalID
}

// SegmentedTabletRouterIsDynamicLogicalID reports whether id is available
// to tablet roots, catalogs, indexes, and other non-derived logical pages.
func SegmentedTabletRouterIsDynamicLogicalID(id uint64) bool {
	return id >= SegmentedTabletRouterFirstDynamicLogicalID
}

func segmentedTabletRouterEncodeAnchorFromLeaves(
	dst []byte,
	header SegmentedTabletRouterHeader,
	pageID uint8,
	leaves []SegmentedTabletRouterLeaf,
) ([]byte, error) {
	return segmentedTabletRouterEncodeAnchor(
		dst, header, pageID, len(leaves),
		func(rank int) segmentedTabletRouterFence {
			return segmentedTabletRouterFence{a: leaves[rank].Fence}
		},
		func(rank int) (uint8, uint16, PageRef, BucketZone) {
			return uint8(rank), leaves[rank].LocalID,
				leaves[rank].Ref, leaves[rank].Zone
		},
	)
}

func segmentedTabletRouterEncodeAnchorSubset(
	dst []byte,
	header SegmentedTabletRouterHeader,
	pageID uint8,
	source *segmentedTabletRouterAnchorView,
	first, last int,
) ([]byte, error) {
	return segmentedTabletRouterEncodeAnchor(
		dst, header, pageID, last-first,
		func(rank int) segmentedTabletRouterFence {
			return source.fenceAt(first + rank)
		},
		func(rank int) (uint8, uint16, PageRef, BucketZone) {
			slot := source.ranks[first+rank]
			localID := binary.LittleEndian.Uint16(
				source.localIDs[int(slot)*2:],
			)
			bucket, _ := MakeTabletLocalIdentityBucket(
				header.TabletID, uint32(localID),
			)
			ref, zone, _ := source.handleAt(slot, BucketID(bucket))
			return slot, localID, ref, zone
		},
	)
}

func segmentedTabletRouterEncodeAnchor(
	dst []byte,
	header SegmentedTabletRouterHeader,
	pageID uint8,
	count int,
	fenceAt func(int) segmentedTabletRouterFence,
	rowAt func(int) (uint8, uint16, PageRef, BucketZone),
) ([]byte, error) {
	if len(dst) < SegmentedTabletRouterAnchorPageBytes ||
		header.StoreID == ([16]byte{}) ||
		header.TabletID >= TabletLocalIdentityTabletCount ||
		header.Generation == 0 || header.Generation >= uint64(1)<<48 ||
		header.AnchorKind != PagePrimaryAnchor ||
		header.LeafKind != PagePrimaryLeaf ||
		pageID >= SegmentedTabletRouterMaxPages ||
		count <= 0 || count > SegmentedTabletRouterRowsPerPage {
		return nil, fmt.Errorf("%w: anchor encode geometry", ErrInvalidWrite)
	}
	image := dst[:SegmentedTabletRouterAnchorPageBytes]
	logicalID := segmentedTabletRouterAnchorLogicalID(
		header.TabletID, pageID,
	)
	payload, err := InitPage(image, PageHeader{
		StoreID:       header.StoreID,
		Generation:    header.Generation,
		LogicalID:     logicalID,
		PageSize:      SegmentedTabletRouterAnchorPageBytes,
		PayloadLength: segmentedTabletRouterAnchorPayloadBytes,
		Kind:          PagePrimaryAnchor,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: anchor common envelope", err)
	}
	binary.LittleEndian.PutUint16(payload[0:2], uint16(count))
	for at := segmentedTabletRouterAnchorLocalIDsAt; at < segmentedTabletRouterAnchorHandlesAt; at++ {
		image[at] = 0xff
	}
	firstFence := fenceAt(0)
	common := firstFence.length()
	for rank := 1; rank < count; rank++ {
		common = min(
			common,
			segmentedTabletRouterFencePrefix(firstFence, fenceAt(rank)),
		)
	}
	if common > 255 {
		return nil, fmt.Errorf("%w: anchor common prefix", ErrInvalidWrite)
	}
	payload[4] = byte(common)
	keys := image[segmentedTabletRouterAnchorKeysAt:segmentedTabletRouterAnchorTrailerAt]
	firstFence.copyTo(keys, 0)
	keyAt := common
	var seen [4]uint64
	var restart segmentedTabletRouterFence
	for rank := range count {
		fence := fenceAt(rank)
		slot, localID, ref, zone := rowAt(rank)
		word, bit := slot>>6, uint64(1)<<(slot&63)
		bucket, ok := MakeTabletLocalIdentityBucket(
			header.TabletID, uint32(localID),
		)
		if !ok || seen[word]&bit != 0 ||
			segmentedTabletRouterValidateLeafRef(
				ref, BucketID(bucket), header.LeafKind,
				header.Generation,
			) != nil {
			return nil, fmt.Errorf("%w: anchor row", ErrInvalidWrite)
		}
		seen[word] |= bit
		image[segmentedTabletRouterAnchorRanksAt+rank] = slot
		binary.LittleEndian.PutUint16(
			image[segmentedTabletRouterAnchorLocalIDsAt+int(slot)*2:],
			localID,
		)
		segmentedTabletRouterEncodeLeafHandle(
			image[segmentedTabletRouterAnchorHandlesAt+
				int(slot)*SegmentedTabletRouterHandleBytes:],
			ref, zone,
		)
		binary.LittleEndian.PutUint16(
			image[segmentedTabletRouterAnchorOffsetsAt+rank*2:],
			uint16(keyAt),
		)
		shared := 0
		if rank%segmentedTabletRouterRestart == 0 {
			restart = fence
		} else {
			shared = segmentedTabletRouterFencePrefix(restart, fence)
			shared = max(0, shared-common)
		}
		suffixAt := common + shared
		suffix := fence.length() - suffixAt
		if shared > 255 || suffix < 0 || suffix > 255 ||
			keyAt+1+suffix > len(keys) {
			return nil, fmt.Errorf(
				"%w: compressed anchor fence arena",
				ErrSegmentedTabletRouterNoSpace,
			)
		}
		keys[keyAt] = byte(shared)
		keyAt++
		keyAt += fence.copyTo(keys[keyAt:keyAt+suffix], suffixAt)
	}
	binary.LittleEndian.PutUint16(
		image[segmentedTabletRouterAnchorOffsetsAt+count*2:],
		uint16(keyAt),
	)
	headBytes := 0
	validBytes := SegmentedTabletRouterRowsPerPage / 8
	for _, candidate := range [...]int{4, 2, 1} {
		if keyAt+count*candidate+validBytes <= len(keys) {
			headBytes = candidate
			break
		}
	}
	if headBytes != 0 {
		heads := keys[keyAt : keyAt+count*headBytes]
		valid := keys[keyAt+count*headBytes : keyAt+count*headBytes+validBytes]
		for rank := range count {
			fence := fenceAt(rank)
			if fence.length()-common < headBytes {
				continue
			}
			valid[rank>>3] |= byte(1) << uint(rank&7)
			for at := 0; at < headBytes; at++ {
				heads[rank*headBytes+at] = fence.at(common + at)
			}
		}
		keyAt += count*headBytes + validBytes
	}
	payload[5] = byte(headBytes)
	binary.LittleEndian.PutUint16(payload[2:4], uint16(keyAt))
	if _, err := sealInitializedPage(image); err != nil {
		return nil, fmt.Errorf("%w: anchor common seal", err)
	}
	return image, nil
}

func segmentedTabletRouterEncodeRootInitial(
	root, locator []byte,
	header SegmentedTabletRouterHeader,
	anchorRefs []PageRef,
	leaves []SegmentedTabletRouterLeaf,
) error {
	clear(root)
	pageCount := len(anchorRefs)
	copy(root[:8], segmentedTabletRouterRootMagic)
	binary.LittleEndian.PutUint32(root[8:12], segmentedTabletRouterVersion)
	binary.LittleEndian.PutUint16(
		root[12:14], segmentedTabletRouterRootHeaderBytes,
	)
	root[14] = byte(pageCount)
	root[15] = byte(header.AnchorKind)
	root[16] = byte(header.LeafKind)
	binary.LittleEndian.PutUint32(root[20:24], header.TabletID)
	binary.LittleEndian.PutUint64(root[24:32], header.Generation)
	binary.LittleEndian.PutUint16(root[34:36], uint16(pageCount))
	binary.LittleEndian.PutUint32(root[36:40], PageChecksum(locator))
	binary.LittleEndian.PutUint32(
		root[40:44], SegmentedTabletRouterRootBytes,
	)
	copy(root[44:60], header.StoreID[:])
	keyAt := 0
	for rank := range pageCount {
		root[segmentedTabletRouterRootRanksAt+rank] = byte(rank)
		segmentedTabletRouterEncodeAnchorRef(
			root[segmentedTabletRouterRootRefsAt+
				rank*segmentedTabletRouterRootRefBytes:],
			anchorRefs[rank],
		)
		binary.LittleEndian.PutUint16(
			root[segmentedTabletRouterRootOffsetsAt+rank*2:],
			uint16(keyAt),
		)
		fence := leaves[rank*SegmentedTabletRouterRowsPerPage].Fence
		if keyAt+len(fence) >
			segmentedTabletRouterRootTrailerAt-
				segmentedTabletRouterRootKeysAt {
			return fmt.Errorf("%w: root fence arena", ErrSegmentedTabletRouterNoSpace)
		}
		copy(root[segmentedTabletRouterRootKeysAt+keyAt:], fence)
		keyAt += len(fence)
	}
	binary.LittleEndian.PutUint16(
		root[segmentedTabletRouterRootOffsetsAt+pageCount*2:],
		uint16(keyAt),
	)
	binary.LittleEndian.PutUint16(root[32:34], uint16(keyAt))
	segmentedTabletRouterSeal(root, segmentedTabletRouterRootTrailerAt)
	return nil
}

func (v *SegmentedTabletRouterView) encodeSplitRoot(
	dst, locator []byte,
	header SegmentedTabletRouterHeader,
	pageID, newPageID uint8,
	rightFloor segmentedTabletRouterFence,
	leftRef, rightRef PageRef,
) error {
	clear(dst[:SegmentedTabletRouterRootBytes])
	root := dst[:SegmentedTabletRouterRootBytes]
	copy(root[:segmentedTabletRouterRootHeaderBytes],
		v.root[:segmentedTabletRouterRootHeaderBytes])
	root[14] = v.pageCount + 1
	binary.LittleEndian.PutUint64(root[24:32], header.Generation)
	binary.LittleEndian.PutUint16(root[34:36], uint16(v.pageCount)+1)
	binary.LittleEndian.PutUint32(root[36:40], PageChecksum(locator))
	for stableID := range uint8(SegmentedTabletRouterMaxPages) {
		if stableID == pageID {
			segmentedTabletRouterEncodeAnchorRef(
				root[segmentedTabletRouterRootRefsAt+
					int(stableID)*segmentedTabletRouterRootRefBytes:],
				leftRef,
			)
		} else if stableID == newPageID {
			segmentedTabletRouterEncodeAnchorRef(
				root[segmentedTabletRouterRootRefsAt+
					int(stableID)*segmentedTabletRouterRootRefBytes:],
				rightRef,
			)
		} else {
			start := segmentedTabletRouterRootRefsAt +
				int(stableID)*segmentedTabletRouterRootRefBytes
			copy(root[start:start+segmentedTabletRouterRootRefBytes],
				v.root[start:start+segmentedTabletRouterRootRefBytes])
		}
	}
	insertRank := -1
	for rank := 0; rank < int(v.pageCount); rank++ {
		if v.rootRanks[rank] == pageID {
			insertRank = rank + 1
			break
		}
	}
	if insertRank < 1 {
		return fmt.Errorf("%w: split source rank", ErrInvalidWrite)
	}
	keyAt := 0
	outRank := 0
	for oldRank := 0; oldRank <= int(v.pageCount); oldRank++ {
		if outRank == insertRank {
			root[segmentedTabletRouterRootRanksAt+outRank] = newPageID
			binary.LittleEndian.PutUint16(
				root[segmentedTabletRouterRootOffsetsAt+outRank*2:],
				uint16(keyAt),
			)
			if keyAt+rightFloor.length() >
				segmentedTabletRouterRootTrailerAt-
					segmentedTabletRouterRootKeysAt {
				return ErrSegmentedTabletRouterNoSpace
			}
			keyAt += rightFloor.copyTo(
				root[segmentedTabletRouterRootKeysAt+keyAt:], 0,
			)
			outRank++
		}
		if oldRank == int(v.pageCount) {
			break
		}
		root[segmentedTabletRouterRootRanksAt+outRank] =
			v.rootRanks[oldRank]
		binary.LittleEndian.PutUint16(
			root[segmentedTabletRouterRootOffsetsAt+outRank*2:],
			uint16(keyAt),
		)
		oldFence := v.rootFence(oldRank)
		if keyAt+oldFence.length() >
			segmentedTabletRouterRootTrailerAt-
				segmentedTabletRouterRootKeysAt {
			return ErrSegmentedTabletRouterNoSpace
		}
		keyAt += oldFence.copyTo(
			root[segmentedTabletRouterRootKeysAt+keyAt:], 0,
		)
		outRank++
	}
	binary.LittleEndian.PutUint16(
		root[segmentedTabletRouterRootOffsetsAt+outRank*2:],
		uint16(keyAt),
	)
	binary.LittleEndian.PutUint16(root[32:34], uint16(keyAt))
	segmentedTabletRouterSeal(root, segmentedTabletRouterRootTrailerAt)
	return nil
}

func segmentedTabletRouterValidateLeafRef(
	ref PageRef, bucket BucketID, kind PageKind,
	selectingGeneration uint64,
) error {
	if kind != PagePrimaryLeaf {
		return fmt.Errorf("%w: exact leaf kind", ErrInvalidWrite)
	}
	logicalID, ok := SegmentedTabletRouterLeafLogicalID(bucket)
	if !ok {
		return fmt.Errorf("%w: exact leaf BucketID", ErrInvalidWrite)
	}
	switch {
	case ref.Offset == 0 || ref.Offset&7 != 0 ||
		ref.Offset>>3 >= uint64(1)<<48:
		return fmt.Errorf("%w: exact leaf offset", ErrInvalidWrite)
	case ref.Generation == 0 || ref.Generation >= uint64(1)<<48 ||
		ref.Generation > selectingGeneration:
		return fmt.Errorf("%w: exact leaf generation", ErrInvalidWrite)
	case ref.Length == 0 || ref.Length > 1<<16:
		return fmt.Errorf("%w: exact leaf length", ErrInvalidWrite)
	case ref.LogicalID != logicalID:
		return fmt.Errorf("%w: exact leaf logical ID", ErrInvalidWrite)
	case ref.Kind != PagePrimaryLeaf || ref.Flags != 0 || ref.Aux != 0:
		return fmt.Errorf("%w: exact leaf kind or flags", ErrInvalidWrite)
	}
	return nil
}

func segmentedTabletRouterValidateAnchorRef(
	ref PageRef,
	header SegmentedTabletRouterHeader,
	pageID uint8,
) error {
	if header.StoreID == ([16]byte{}) ||
		header.Generation == 0 || header.Generation >= uint64(1)<<48 ||
		header.AnchorKind != PagePrimaryAnchor ||
		header.LeafKind != PagePrimaryLeaf {
		return fmt.Errorf("%w: tablet-root generation", ErrInvalidWrite)
	}
	return segmentedTabletRouterValidateAnchorRefIdentity(
		ref, header.TabletID, header.Generation, pageID,
	)
}

func segmentedTabletRouterValidateAnchorRefIdentity(
	ref PageRef,
	tabletID uint32,
	selectingGeneration uint64,
	pageID uint8,
) error {
	logicalID, ok := SegmentedTabletRouterAnchorLogicalID(
		tabletID, pageID,
	)
	if !ok {
		return fmt.Errorf("%w: anchor identity", ErrInvalidWrite)
	}
	if ref.Offset == 0 || ref.Offset&4095 != 0 ||
		ref.Offset>>12 >= uint64(1)<<48 ||
		ref.Generation == 0 || ref.Generation >= uint64(1)<<48 ||
		ref.Generation > selectingGeneration ||
		ref.LogicalID != logicalID ||
		ref.Kind != PagePrimaryAnchor || ref.Flags != 0 || ref.Aux != 0 ||
		ref.Length != SegmentedTabletRouterAnchorPageBytes {
		return fmt.Errorf("%w: non-canonical anchor ref", ErrInvalidWrite)
	}
	return nil
}

func segmentedTabletRouterEncodeLeafHandle(
	dst []byte, ref PageRef, zone BucketZone,
) {
	segmentedTabletRouterPutUint48(dst, ref.Offset>>3)
	segmentedTabletRouterPutUint48(dst[6:], ref.Generation)
	binary.LittleEndian.PutUint16(dst[12:14], uint16(ref.Length-1))
	copy(dst[14:18], zone[:])
}

func segmentedTabletRouterEncodeAnchorRef(dst []byte, ref PageRef) {
	segmentedTabletRouterPutUint48(dst, ref.Offset>>12)
	segmentedTabletRouterPutUint48(dst[6:], ref.Generation)
	dst[12] = byte(tabletAnchorHandleExtentClass(ref.Length))
}

func segmentedTabletRouterGetUint48(src []byte) uint64 {
	return uint64(src[0]) |
		uint64(src[1])<<8 |
		uint64(src[2])<<16 |
		uint64(src[3])<<24 |
		uint64(src[4])<<32 |
		uint64(src[5])<<40
}

func segmentedTabletRouterPutUint48(dst []byte, value uint64) {
	dst[0] = byte(value)
	dst[1] = byte(value >> 8)
	dst[2] = byte(value >> 16)
	dst[3] = byte(value >> 24)
	dst[4] = byte(value >> 32)
	dst[5] = byte(value >> 40)
}

func segmentedTabletRouterSeal(image []byte, trailer int) {
	checksum := PageChecksum(image[:trailer])
	binary.LittleEndian.PutUint32(image[trailer:trailer+4], checksum)
	binary.LittleEndian.PutUint32(image[trailer+4:trailer+8], ^checksum)
}

func segmentedTabletRouterChecksumOK(image []byte, trailer int) bool {
	checksum := binary.LittleEndian.Uint32(image[trailer : trailer+4])
	return binary.LittleEndian.Uint32(image[trailer+4:trailer+8]) ==
		^checksum && PageChecksum(image[:trailer]) == checksum
}

func segmentedTabletRouterCorrupt(reason string) error {
	return fmt.Errorf("%w: %s", ErrSegmentedTabletRouterCorrupt, reason)
}
