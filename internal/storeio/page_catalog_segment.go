package storeio

import (
	"encoding/binary"
	"fmt"
	"io"
)

// PageCatalogBounds binds every catalog page and PageRef to the selected Store
// image. ExpectedDigest is optional; zero disables that early rejection.
type PageCatalogBounds struct {
	StoreID    [16]byte
	Generation uint64
	// PageSize is the Store allocation quantum and exact catalog segment
	// extent. Zero retains the minimum-quantum spelling for standalone codec
	// callers; durable integration always supplies the selected root value.
	PageSize      uint32
	DataStart     uint64
	FileEnd       uint64
	NextLogicalID uint64
	// TotalBytes is optional for standalone segment callers and exact when
	// non-zero. Streaming durable bootstrap requires it so the complete
	// contiguous extent is bounded before its first read.
	TotalBytes uint32
	// RequireDigest makes ExpectedDigest mandatory even when all sixteen bytes
	// are zero. Existing standalone callers retain the historical non-zero
	// fast-reject spelling when this flag is false.
	RequireDigest  bool
	ExpectedDigest [PageCatalogDigestSize]byte
}

// PageCatalogSegmentHeader identifies one immutable allocation-quantum chain
// page.
type PageCatalogSegmentHeader struct {
	StoreID    [16]byte
	Generation uint64
	LogicalID  uint64
	Ordinal    uint16
	Next       PageRef
}

// PageCatalogSegmentView borrows one checksummed page's canonical byte slice.
type PageCatalogSegmentView struct {
	header       PageCatalogSegmentHeader
	digest       [PageCatalogDigestSize]byte
	totalBytes   uint32
	segmentCount uint16
	dataCapacity int
	data         []byte
}

func (v PageCatalogSegmentView) Header() PageCatalogSegmentHeader {
	return v.header
}

func (v PageCatalogSegmentView) Digest() [PageCatalogDigestSize]byte {
	return v.digest
}

func (v PageCatalogSegmentView) TotalBytes() int {
	return int(v.totalBytes)
}

func (v PageCatalogSegmentView) SegmentCount() int {
	return int(v.segmentCount)
}

func (v PageCatalogSegmentView) DataOffset() int {
	return int(v.header.Ordinal) * v.dataCapacity
}

func (v PageCatalogSegmentView) Data() []byte {
	return v.data
}

// PageCatalogChainPage pairs a physical reference with its exact page image.
// OpenPageCatalogChain requires pages in Next order starting at the catalog
// head and validates that every link equals the next supplied reference.
type PageCatalogChainPage struct {
	Ref  PageRef
	Page []byte
}

// EncodePageCatalogSegment writes one deterministic slice of catalog into one
// complete common-format allocation-quantum page.
func EncodePageCatalogSegment(
	dst []byte,
	header PageCatalogSegmentHeader,
	catalog *CanonicalPageCatalog,
	bounds PageCatalogBounds,
) ([]byte, error) {
	pageSize, dataCapacity, geometryOK := pageCatalogSegmentGeometry(bounds)
	segmentCountInt, countOK := catalog.SegmentCountFor(pageSize)
	if catalog == nil || !geometryOK || !countOK ||
		segmentCountInt == 0 ||
		segmentCountInt > int(^uint16(0)) ||
		int(header.Ordinal) >= segmentCountInt {
		return nil, fmt.Errorf("%w: catalog segment ordinal", ErrInvalidWrite)
	}
	segmentCount := uint16(segmentCountInt)
	if err := validatePageCatalogSegmentHeader(
		header, bounds, segmentCount,
	); err != nil {
		return nil, err
	}
	offset := int(header.Ordinal) * dataCapacity
	length := min(dataCapacity, len(catalog.canonical)-offset)
	payloadLength := PageCatalogSegmentPayloadHeaderSize + length
	payload, err := InitPage(dst, PageHeader{
		StoreID: header.StoreID, Generation: header.Generation,
		LogicalID: header.LogicalID, PageSize: pageSize,
		PayloadLength: uint32(payloadLength), Kind: PageCatalogSegment,
	})
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(payload[0:4], pageCatalogSegmentVersion)
	binary.LittleEndian.PutUint16(payload[4:6], header.Ordinal)
	binary.LittleEndian.PutUint16(payload[6:8], segmentCount)
	binary.LittleEndian.PutUint32(payload[8:12], uint32(len(catalog.canonical)))
	binary.LittleEndian.PutUint32(payload[12:16], uint32(length))
	copy(payload[16:32], catalog.digest[:])
	encodePageRef(payload[32:64], header.Next)
	copy(
		payload[PageCatalogSegmentPayloadHeaderSize:],
		catalog.canonical[offset:offset+length],
	)
	page := dst[:pageSize]
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

// OpenPageCatalogSegment validates one complete common-format page. Chain-wide
// reference equality, byte continuity, and digest validation are performed by
// OpenPageCatalogChain.
func OpenPageCatalogSegment(
	src []byte, bounds PageCatalogBounds,
) (PageCatalogSegmentView, error) {
	pageHeader, payload, err := OpenPage(src)
	if err != nil {
		return PageCatalogSegmentView{}, fmt.Errorf(
			"%w: %w", ErrPageCatalogCorrupt, err,
		)
	}
	pageSize, dataCapacity, geometryOK := pageCatalogSegmentGeometry(bounds)
	if !geometryOK ||
		pageHeader.Kind != PageCatalogSegment ||
		pageHeader.Flags != 0 ||
		pageHeader.PageSize != pageSize ||
		len(payload) < PageCatalogSegmentPayloadHeaderSize ||
		pageHeader.StoreID != bounds.StoreID ||
		pageHeader.Generation == 0 ||
		pageHeader.Generation > bounds.Generation ||
		pageHeader.LogicalID <= StateRootLogicalID ||
		pageHeader.LogicalID >= bounds.NextLogicalID ||
		binary.LittleEndian.Uint32(payload[0:4]) != pageCatalogSegmentVersion ||
		!pageRefReservedZero(payload[32:64]) {
		return PageCatalogSegmentView{}, fmt.Errorf(
			"%w: segment header or reserved bytes", ErrPageCatalogCorrupt,
		)
	}
	ordinal := binary.LittleEndian.Uint16(payload[4:6])
	count := binary.LittleEndian.Uint16(payload[6:8])
	total := binary.LittleEndian.Uint32(payload[8:12])
	length := binary.LittleEndian.Uint32(payload[12:16])
	expectedCount := pageCatalogSegmentCountFor(total, pageSize)
	expectedOffset64 := uint64(ordinal) * uint64(dataCapacity)
	expectedLength := uint32(0)
	if expectedOffset64 < uint64(total) {
		expectedLength = uint32(min(
			dataCapacity,
			int(uint64(total)-expectedOffset64),
		))
	}
	if count == 0 || count != expectedCount || ordinal >= count ||
		total < PageCatalogCanonicalHeaderSize ||
		total > PageCatalogMaxCanonicalBytes ||
		bounds.TotalBytes != 0 && total != bounds.TotalBytes ||
		length != expectedLength ||
		uint64(PageCatalogSegmentPayloadHeaderSize)+uint64(length) !=
			uint64(len(payload)) {
		return PageCatalogSegmentView{}, fmt.Errorf(
			"%w: segment geometry", ErrPageCatalogCorrupt,
		)
	}
	var digest [PageCatalogDigestSize]byte
	copy(digest[:], payload[16:32])
	if (bounds.RequireDigest ||
		bounds.ExpectedDigest != ([PageCatalogDigestSize]byte{})) &&
		digest != bounds.ExpectedDigest {
		return PageCatalogSegmentView{}, fmt.Errorf(
			"%w: catalog digest", ErrPageCatalogCorrupt,
		)
	}
	header := PageCatalogSegmentHeader{
		StoreID: pageHeader.StoreID, Generation: pageHeader.Generation,
		LogicalID: pageHeader.LogicalID, Ordinal: ordinal,
		Next: decodePageRef(payload[32:64]),
	}
	if err := validatePageCatalogSegmentHeader(header, bounds, count); err != nil {
		return PageCatalogSegmentView{}, fmt.Errorf(
			"%w: segment link", ErrPageCatalogCorrupt,
		)
	}
	data := payload[PageCatalogSegmentPayloadHeaderSize:]
	return PageCatalogSegmentView{
		header: header, digest: digest, totalBytes: total,
		segmentCount: count, dataCapacity: dataCapacity, data: data,
	}, nil
}

// OpenPageCatalogChainAt streams one immutable, physically contiguous catalog
// chain from reader. scratch is caller-owned and reused for every exact
// allocation-quantum page. The function allocates one authoritative canonical
// byte image, derives every physical and logical reference from head and the
// ordinal, validates each encoded Next against that derivation, and returns a
// catalog that owns the image.
//
// Durable callers must supply exact TotalBytes and set RequireDigest. Requiring
// the flag separately from the digest preserves an all-zero digest as a valid
// checked value instead of overloading it as "not supplied".
func OpenPageCatalogChainAt(
	reader io.ReaderAt,
	head PageRef,
	bounds PageCatalogBounds,
	scratch []byte,
) (*CanonicalPageCatalog, error) {
	pageSize, dataCapacity, geometryOK := pageCatalogSegmentGeometry(bounds)
	if reader == nil || !geometryOK ||
		bounds.TotalBytes < PageCatalogCanonicalHeaderSize ||
		bounds.TotalBytes > PageCatalogMaxCanonicalBytes ||
		!bounds.RequireDigest ||
		uint64(len(scratch)) < uint64(pageSize) {
		return nil, fmt.Errorf(
			"%w: streaming catalog contract", ErrInvalidWrite,
		)
	}
	count := pageCatalogSegmentCountFor(bounds.TotalBytes, pageSize)
	if count == 0 {
		return nil, fmt.Errorf(
			"%w: streaming catalog geometry", ErrInvalidWrite,
		)
	}
	if err := validatePageCatalogRef(head, bounds); err != nil {
		return nil, fmt.Errorf(
			"%w: catalog head reference", ErrPageCatalogCorrupt,
		)
	}
	lastOrdinal := uint64(count - 1)
	lastOffsetDelta := lastOrdinal * uint64(pageSize)
	if head.Offset > maxSuperblockFileOffset-lastOffsetDelta ||
		head.LogicalID > ^uint64(0)-lastOrdinal {
		return nil, fmt.Errorf(
			"%w: contiguous catalog extent overflow",
			ErrPageCatalogCorrupt,
		)
	}
	last := PageRef{
		Offset:     head.Offset + lastOffsetDelta,
		LogicalID:  head.LogicalID + lastOrdinal,
		Generation: head.Generation, Length: pageSize,
		Kind: PageCatalogSegment,
	}
	if err := validatePageCatalogRef(last, bounds); err != nil {
		return nil, fmt.Errorf(
			"%w: contiguous catalog extent bounds",
			ErrPageCatalogCorrupt,
		)
	}

	canonical := make([]byte, int(bounds.TotalBytes))
	page := scratch[:int(pageSize):int(pageSize)]
	for ordinal := range count {
		ordinal64 := uint64(ordinal)
		ref := PageRef{
			Offset:     head.Offset + ordinal64*uint64(pageSize),
			LogicalID:  head.LogicalID + ordinal64,
			Generation: head.Generation, Length: pageSize,
			Kind: PageCatalogSegment,
		}
		clear(page)
		n, readErr := reader.ReadAt(page, int64(ref.Offset))
		if readErr != nil && !(readErr == io.EOF && n == len(page)) {
			return nil, fmt.Errorf(
				"%w: catalog segment %d read: %w",
				ErrPageCatalogCorrupt, ordinal, readErr,
			)
		}
		if n != len(page) {
			return nil, fmt.Errorf(
				"%w: catalog segment %d short read",
				ErrPageCatalogCorrupt, ordinal,
			)
		}
		view, err := OpenPageCatalogSegment(page, bounds)
		if err != nil {
			return nil, err
		}
		header := view.Header()
		if header.StoreID != bounds.StoreID ||
			header.Generation != ref.Generation ||
			header.LogicalID != ref.LogicalID ||
			header.Ordinal != ordinal ||
			view.SegmentCount() != int(count) ||
			view.TotalBytes() != int(bounds.TotalBytes) ||
			view.Digest() != bounds.ExpectedDigest ||
			view.DataOffset() != int(ordinal)*dataCapacity {
			return nil, fmt.Errorf(
				"%w: catalog segment %d identity or geometry",
				ErrPageCatalogCorrupt, ordinal,
			)
		}
		var next PageRef
		if ordinal+1 < count {
			nextOrdinal := ordinal64 + 1
			next = PageRef{
				Offset:     head.Offset + nextOrdinal*uint64(pageSize),
				LogicalID:  head.LogicalID + nextOrdinal,
				Generation: head.Generation, Length: pageSize,
				Kind: PageCatalogSegment,
			}
		}
		if header.Next != next {
			return nil, fmt.Errorf(
				"%w: catalog segment %d contiguous link",
				ErrPageCatalogCorrupt, ordinal,
			)
		}
		offset := view.DataOffset()
		data := view.Data()
		if offset < 0 || offset > len(canonical) ||
			len(data) > len(canonical)-offset {
			return nil, fmt.Errorf(
				"%w: catalog segment %d canonical bounds",
				ErrPageCatalogCorrupt, ordinal,
			)
		}
		copy(canonical[offset:offset+len(data)], data)
	}
	if pageCatalogDigest(canonical) != bounds.ExpectedDigest {
		return nil, fmt.Errorf(
			"%w: canonical digest", ErrPageCatalogCorrupt,
		)
	}
	catalog, err := openOwnedCanonicalPageCatalog(canonical)
	if err != nil {
		return nil, err
	}
	if catalog.digest != bounds.ExpectedDigest {
		return nil, fmt.Errorf(
			"%w: rebuilt digest", ErrPageCatalogCorrupt,
		)
	}
	return catalog, nil
}

// OpenPageCatalogChain reconstructs, authenticates, and canonically parses one
// complete catalog chain. A zero-page chain is the canonical empty catalog and
// requires a completely empty bounds contract.
func OpenPageCatalogChain(
	pages []PageCatalogChainPage, bounds PageCatalogBounds,
) (*CanonicalPageCatalog, error) {
	if len(pages) == 0 {
		if bounds != (PageCatalogBounds{}) {
			return nil, fmt.Errorf(
				"%w: empty chain has non-empty bounds", ErrPageCatalogCorrupt,
			)
		}
		return &CanonicalPageCatalog{}, nil
	}
	if len(pages) > int(^uint16(0)) {
		return nil, fmt.Errorf("%w: chain length", ErrPageCatalogCorrupt)
	}
	first, err := OpenPageCatalogSegment(pages[0].Page, bounds)
	if err != nil {
		return nil, err
	}
	if first.header.Ordinal != 0 ||
		first.segmentCount != uint16(len(pages)) {
		return nil, fmt.Errorf(
			"%w: chain length", ErrPageCatalogCorrupt,
		)
	}
	canonical := make([]byte, int(first.totalBytes))
	digest := first.digest
	generation := first.header.Generation
	var previousLogical uint64
	seenOffsets := make(map[uint64]struct{}, len(pages))
	for i, chainPage := range pages {
		if err := validatePageCatalogRef(chainPage.Ref, bounds); err != nil {
			return nil, fmt.Errorf(
				"%w: chain reference %d", ErrPageCatalogCorrupt, i,
			)
		}
		if _, exists := seenOffsets[chainPage.Ref.Offset]; exists {
			return nil, fmt.Errorf(
				"%w: duplicate chain extent", ErrPageCatalogCorrupt,
			)
		}
		seenOffsets[chainPage.Ref.Offset] = struct{}{}
		if len(chainPage.Page) != int(chainPage.Ref.Length) {
			return nil, fmt.Errorf(
				"%w: chain page length", ErrPageCatalogCorrupt,
			)
		}
		view, openErr := OpenPageCatalogSegment(chainPage.Page, bounds)
		if openErr != nil {
			return nil, openErr
		}
		pageHeader, _ := decodePageHeader(chainPage.Page)
		if pageHeader.StoreID != bounds.StoreID ||
			pageHeader.Generation != chainPage.Ref.Generation ||
			pageHeader.LogicalID != chainPage.Ref.LogicalID ||
			pageHeader.PageSize != chainPage.Ref.Length ||
			pageHeader.Kind != chainPage.Ref.Kind ||
			pageHeader.Flags != chainPage.Ref.Flags ||
			view.header.Ordinal != uint16(i) ||
			view.segmentCount != uint16(len(pages)) ||
			view.totalBytes != first.totalBytes ||
			view.digest != digest ||
			view.header.Generation != generation ||
			i != 0 && view.header.LogicalID <= previousLogical {
			return nil, fmt.Errorf(
				"%w: chain identity, order, or digest", ErrPageCatalogCorrupt,
			)
		}
		copy(canonical[view.DataOffset():], view.data)
		previousLogical = view.header.LogicalID
		if i+1 == len(pages) {
			if view.header.Next != (PageRef{}) {
				return nil, fmt.Errorf(
					"%w: non-zero chain tail", ErrPageCatalogCorrupt,
				)
			}
		} else if view.header.Next != pages[i+1].Ref {
			return nil, fmt.Errorf(
				"%w: chain link mismatch", ErrPageCatalogCorrupt,
			)
		}
	}
	if pageCatalogDigest(canonical) != digest {
		return nil, fmt.Errorf(
			"%w: canonical digest", ErrPageCatalogCorrupt,
		)
	}
	catalog, err := openOwnedCanonicalPageCatalog(canonical)
	if err != nil {
		return nil, err
	}
	if catalog.digest != digest {
		return nil, fmt.Errorf(
			"%w: rebuilt digest", ErrPageCatalogCorrupt,
		)
	}
	return catalog, nil
}

func pageCatalogSegmentCount(total uint32) uint16 {
	return pageCatalogSegmentCountFor(total, PageCatalogSegmentPageSize)
}

func pageCatalogSegmentCountFor(total, pageSize uint32) uint16 {
	if total == 0 || total > PageCatalogMaxCanonicalBytes {
		return 0
	}
	capacity, ok := pageCatalogSegmentDataCapacity(pageSize)
	if !ok {
		return 0
	}
	count := (uint64(total) + uint64(capacity) - 1) /
		uint64(capacity)
	if count > uint64(^uint16(0)) {
		return 0
	}
	return uint16(count)
}

func pageCatalogSegmentGeometry(
	bounds PageCatalogBounds,
) (uint32, int, bool) {
	pageSize := bounds.PageSize
	if pageSize == 0 {
		pageSize = PageCatalogSegmentPageSize
	}
	capacity, ok := pageCatalogSegmentDataCapacity(pageSize)
	return pageSize, capacity, ok
}

func pageCatalogSegmentDataCapacity(pageSize uint32) (int, bool) {
	const overhead = PageHeaderSize + PageTrailerSize +
		PageCatalogSegmentPayloadHeaderSize
	if !validPhysicalPageSize(pageSize) ||
		uint64(pageSize) <= uint64(overhead) ||
		uint64(pageSize) > uint64(^uint(0)>>1) {
		return 0, false
	}
	return int(pageSize) - overhead, true
}

func validatePageCatalogSegmentHeader(
	header PageCatalogSegmentHeader,
	bounds PageCatalogBounds,
	segmentCount uint16,
) error {
	pageSize, _, geometryOK := pageCatalogSegmentGeometry(bounds)
	if bounds.StoreID == ([16]byte{}) || bounds.Generation == 0 ||
		!geometryOK ||
		bounds.DataStart == 0 ||
		bounds.DataStart%uint64(pageSize) != 0 ||
		bounds.FileEnd < bounds.DataStart ||
		bounds.FileEnd > maxSuperblockFileOffset ||
		bounds.NextLogicalID <= StateRootLogicalID+1 ||
		header.StoreID != bounds.StoreID || header.Generation == 0 ||
		header.Generation > bounds.Generation ||
		header.LogicalID <= StateRootLogicalID ||
		header.LogicalID >= bounds.NextLogicalID ||
		segmentCount == 0 || header.Ordinal >= segmentCount {
		return fmt.Errorf("%w: catalog segment identity", ErrInvalidWrite)
	}
	last := uint32(header.Ordinal)+1 == uint32(segmentCount)
	if last {
		if header.Next != (PageRef{}) {
			return fmt.Errorf("%w: catalog segment tail", ErrInvalidWrite)
		}
		return nil
	}
	if err := validatePageCatalogRef(header.Next, bounds); err != nil ||
		header.Next.Generation != header.Generation ||
		header.Next.LogicalID <= header.LogicalID {
		return fmt.Errorf("%w: catalog segment next", ErrInvalidWrite)
	}
	return nil
}

func validatePageCatalogRef(ref PageRef, bounds PageCatalogBounds) error {
	pageSize, _, geometryOK := pageCatalogSegmentGeometry(bounds)
	length := uint64(ref.Length)
	if !geometryOK ||
		bounds.FileEnd > maxSuperblockFileOffset ||
		ref.Kind != PageCatalogSegment ||
		ref.Flags != 0 || ref.Aux != 0 ||
		ref.Generation == 0 || ref.Generation > bounds.Generation ||
		ref.LogicalID <= StateRootLogicalID ||
		ref.LogicalID >= bounds.NextLogicalID ||
		ref.Length != pageSize ||
		ref.Offset < bounds.DataStart ||
		ref.Offset%uint64(pageSize) != 0 ||
		ref.Offset > maxSuperblockFileOffset ||
		length > bounds.FileEnd ||
		ref.Offset > bounds.FileEnd-length {
		return fmt.Errorf("%w: catalog page reference", ErrInvalidWrite)
	}
	return nil
}
