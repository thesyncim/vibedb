package storeio

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	OverflowPagePayloadHeaderSize = 60
	overflowPageVersion           = DevelopmentFormatVersion
)

// ErrOverflowPageCorrupt reports a malformed separately checksummed large-
// value extent.
var ErrOverflowPageCorrupt = errors.New("vibedb: corrupt Store overflow page")

// OverflowPageHeader describes one ordered piece of a JSON value. Offset is
// the byte position of Data in the complete value. Next is zero only for the
// final piece.
type OverflowPageHeader struct {
	StoreID    [16]byte
	Generation uint64
	LogicalID  uint64
	PageSize   uint32
	Total      uint64
	Offset     uint64
	Next       PageRef
}

// OverflowPageView is one checksum-verified borrowed value piece.
type OverflowPageView struct {
	header OverflowPageHeader
	data   []byte
}

// EncodeOverflowPage writes one complete value piece. allocationQuantum is
// the Store's base page size; overflow extents may be any whole number of
// allocation quanta and every physical offset remains quantum-aligned.
func EncodeOverflowPage(dst []byte, header OverflowPageHeader, data []byte, fileEnd, nextLogicalID uint64, allocationQuantum uint32) ([]byte, error) {
	if err := validateOverflowPage(header, len(data), fileEnd, nextLogicalID, allocationQuantum); err != nil {
		return nil, err
	}
	payloadLength := OverflowPagePayloadHeaderSize + len(data)
	payload, err := InitPage(dst, PageHeader{
		StoreID: header.StoreID, Generation: header.Generation, LogicalID: header.LogicalID,
		PageSize: header.PageSize, PayloadLength: uint32(payloadLength), Kind: PageOverflow,
	})
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(payload[0:4], overflowPageVersion)
	encodePageRef(payload[8:8+PageRefSize], header.Next)
	binary.LittleEndian.PutUint64(payload[40:48], header.Total)
	binary.LittleEndian.PutUint64(payload[48:56], header.Offset)
	binary.LittleEndian.PutUint32(payload[56:60], uint32(len(data)))
	copy(payload[OverflowPagePayloadHeaderSize:], data)
	page := dst[:int(header.PageSize)]
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

// OpenOverflowPage validates one complete value piece against the selecting
// state-root bounds.
func OpenOverflowPage(src []byte, fileEnd, nextLogicalID uint64, allocationQuantum uint32) (OverflowPageView, error) {
	pageHeader, payload, err := OpenPage(src)
	if err != nil {
		return OverflowPageView{}, fmt.Errorf("%w: %w", ErrOverflowPageCorrupt, err)
	}
	if pageHeader.Kind != PageOverflow || len(payload) < OverflowPagePayloadHeaderSize ||
		binary.LittleEndian.Uint32(payload[0:4]) != overflowPageVersion ||
		!allZero(payload[4:8]) || !pageRefReservedZero(payload[8:8+PageRefSize]) {
		return OverflowPageView{}, fmt.Errorf("%w: header, version, or reserved bytes", ErrOverflowPageCorrupt)
	}
	header := OverflowPageHeader{
		StoreID: pageHeader.StoreID, Generation: pageHeader.Generation,
		LogicalID: pageHeader.LogicalID, PageSize: pageHeader.PageSize,
		Next:   decodePageRef(payload[8 : 8+PageRefSize]),
		Total:  binary.LittleEndian.Uint64(payload[40:48]),
		Offset: binary.LittleEndian.Uint64(payload[48:56]),
	}
	dataLength := uint64(binary.LittleEndian.Uint32(payload[56:60]))
	if dataLength != uint64(len(payload)-OverflowPagePayloadHeaderSize) ||
		validateOverflowPage(header, int(dataLength), fileEnd, nextLogicalID, allocationQuantum) != nil {
		return OverflowPageView{}, fmt.Errorf("%w: value bounds, link, or location", ErrOverflowPageCorrupt)
	}
	data := payload[OverflowPagePayloadHeaderSize:]
	return OverflowPageView{header: header, data: data[:len(data):len(data)]}, nil
}

// Header returns value-only piece metadata.
func (v OverflowPageView) Header() OverflowPageHeader { return v.header }

// Data returns the capacity-clipped piece bytes, valid while the page remains
// leased.
func (v OverflowPageView) Data() []byte { return v.data }

func validateOverflowPage(header OverflowPageHeader, dataLength int, fileEnd, nextLogicalID uint64, allocationQuantum uint32) error {
	if header.StoreID == ([16]byte{}) || header.Generation == 0 ||
		header.LogicalID == 0 || header.LogicalID >= nextLogicalID ||
		!validPageExtentSize(PageOverflow, header.PageSize) ||
		!validPhysicalPageSize(allocationQuantum) ||
		header.PageSize < allocationQuantum || header.PageSize%allocationQuantum != 0 ||
		dataLength <= 0 || uint64(dataLength) > uint64(header.PageSize)-PageHeaderSize-PageTrailerSize-OverflowPagePayloadHeaderSize ||
		header.Total == 0 || header.Offset >= header.Total || uint64(dataLength) > header.Total-header.Offset {
		return fmt.Errorf("%w: overflow identity, bounds, or location", ErrInvalidWrite)
	}
	end := header.Offset + uint64(dataLength)
	if header.Next == (PageRef{}) {
		if end != header.Total {
			return fmt.Errorf("%w: truncated final overflow piece", ErrInvalidWrite)
		}
		return nil
	}
	if end >= header.Total || !pageRefWithinFile(header.Next, PageOverflow, header, fileEnd, nextLogicalID, allocationQuantum) ||
		header.Next.LogicalID <= header.LogicalID {
		return fmt.Errorf("%w: overflow continuation", ErrInvalidWrite)
	}
	return nil
}

func pageRefWithinFile(ref PageRef, kind PageKind, header OverflowPageHeader, fileEnd, nextLogicalID uint64, allocationQuantum uint32) bool {
	quantum := uint64(allocationQuantum)
	length := uint64(ref.Length)
	layout, err := MutableStoreLayout(allocationQuantum)
	if err != nil {
		return false
	}
	return fileEnd >= layout.DataStart && fileEnd <= maxSuperblockFileOffset && fileEnd%quantum == 0 &&
		ref.Kind == kind &&
		validPageExtentSize(PageOverflow, ref.Length) &&
		ref.Length >= allocationQuantum && ref.Length%allocationQuantum == 0 &&
		ref.Generation != 0 && ref.Generation <= header.Generation &&
		ref.LogicalID != 0 && ref.LogicalID < nextLogicalID && ref.LogicalID != header.LogicalID &&
		ref.Offset >= layout.DataStart && ref.Offset%quantum == 0 &&
		ref.Offset <= maxSuperblockFileOffset && length <= fileEnd && ref.Offset <= fileEnd-length
}
