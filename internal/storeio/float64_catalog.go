package storeio

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	Float64DirectoryPayloadHeaderSize = 32
	Float64DirectoryRecordSize        = 40
	Float64DirectoryFanout            = 64
	Float64DirectoryMaxLevel          = 6
	float64DirectoryVersion           = DevelopmentFormatVersion
)

// ErrFloat64CatalogCorrupt reports a checksum-valid numeric stripe directory
// with malformed ordering, bounds, or page references. The historical error
// name remains internal so callers do not need two corruption paths.
var ErrFloat64CatalogCorrupt = errors.New("vibejson: corrupt Store float64 stripe directory")

// ErrFloat64DirectoryDepth reports an impossible path beyond the format's
// fixed fanout/level contract.
var ErrFloat64DirectoryDepth = errors.New("vibejson: Store float64 stripe directory is too deep")

// Float64DirectoryEntry is one ordered lower bound. Leaves map FirstChunk to
// a packed value stripe; branches map it to a directory child. Keeping both
// node forms identical makes traversal compact and bounds mutation scratch to
// one fixed array without interfaces or heap allocation.
type Float64DirectoryEntry struct {
	FirstChunk uint32
	Ref        PageRef
}

// Float64DirectoryHeader identifies one immutable node in the ordered stripe
// directory. Level zero is a leaf; higher levels contain child references.
type Float64DirectoryHeader struct {
	StoreID    [16]byte
	Generation uint64
	LogicalID  uint64
	PageSize   uint32
	Level      uint8
}

// Float64DirectoryView borrows one admitted directory node.
type Float64DirectoryView struct {
	header  Float64DirectoryHeader
	payload []byte
	count   uint8
}

// EncodeFloat64DirectoryLeaf writes one ordered stripe-directory leaf without
// allocating. Directory nodes are fixed PageSize metadata pages even when the
// referenced stripes use larger packed extents.
func EncodeFloat64DirectoryLeaf(
	dst []byte,
	header Float64DirectoryHeader,
	entries []Float64DirectoryEntry,
	fileEnd, nextLogicalID uint64,
	allocationQuantum uint32,
) ([]byte, error) {
	if header.Level != 0 {
		return nil, fmt.Errorf("%w: float64 directory leaf level", ErrInvalidWrite)
	}
	return encodeFloat64Directory(
		dst, header, entries, fileEnd, nextLogicalID, allocationQuantum,
	)
}

// EncodeFloat64DirectoryBranch writes one ordered directory branch without
// allocating. Child generations may be older than the branch because an
// incremental copy-on-write mutation shares every untouched subtree.
func EncodeFloat64DirectoryBranch(
	dst []byte,
	header Float64DirectoryHeader,
	entries []Float64DirectoryEntry,
	fileEnd, nextLogicalID uint64,
	allocationQuantum uint32,
) ([]byte, error) {
	if header.Level == 0 {
		return nil, fmt.Errorf("%w: float64 directory branch level", ErrInvalidWrite)
	}
	return encodeFloat64Directory(
		dst, header, entries, fileEnd, nextLogicalID, allocationQuantum,
	)
}

func encodeFloat64Directory(
	dst []byte,
	header Float64DirectoryHeader,
	entries []Float64DirectoryEntry,
	fileEnd, nextLogicalID uint64,
	allocationQuantum uint32,
) ([]byte, error) {
	if err := validateFloat64DirectoryHeader(
		header, len(entries), nextLogicalID, allocationQuantum,
	); err != nil {
		return nil, err
	}
	var seen chunkDirectoryRefSet
	for i, entry := range entries {
		if i != 0 && entries[i-1].FirstChunk >= entry.FirstChunk {
			return nil, fmt.Errorf("%w: float64 directory key order", ErrInvalidWrite)
		}
		if err := validateFloat64DirectoryRef(
			header, entry.Ref, fileEnd, nextLogicalID, allocationQuantum,
		); err != nil || !seen.add(entry.Ref) {
			return nil, fmt.Errorf("%w: float64 directory reference", ErrInvalidWrite)
		}
	}
	payloadLength := Float64DirectoryPayloadHeaderSize +
		len(entries)*Float64DirectoryRecordSize
	payload, err := InitPage(dst, PageHeader{
		StoreID: header.StoreID, Generation: header.Generation,
		LogicalID: header.LogicalID, PageSize: header.PageSize,
		PayloadLength: uint32(payloadLength), Kind: PageFloat64Catalog,
	})
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(payload[0:4], float64DirectoryVersion)
	payload[4] = header.Level
	payload[6] = uint8(len(entries))
	for i, entry := range entries {
		record := payload[Float64DirectoryPayloadHeaderSize+i*Float64DirectoryRecordSize:]
		binary.LittleEndian.PutUint32(record[0:4], entry.FirstChunk)
		encodePageRef(record[8:8+PageRefSize], entry.Ref)
	}
	page := dst[:int(header.PageSize)]
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

// OpenFloat64Directory verifies one complete stripe-directory node.
func OpenFloat64Directory(
	src []byte,
	fileEnd, nextLogicalID uint64,
	allocationQuantum uint32,
) (Float64DirectoryView, error) {
	pageHeader, payload, err := OpenPage(src)
	if err != nil {
		return Float64DirectoryView{}, fmt.Errorf(
			"%w: %w", ErrFloat64CatalogCorrupt, err,
		)
	}
	return openFloat64DirectoryPayload(
		pageHeader, payload, fileEnd, nextLogicalID, allocationQuantum,
	)
}

// OpenAdmittedFloat64Directory validates a directory node after common CRC
// admission.
func OpenAdmittedFloat64Directory(
	src []byte,
	fileEnd, nextLogicalID uint64,
	allocationQuantum uint32,
) (Float64DirectoryView, error) {
	pageHeader, ok := decodePageHeader(src)
	if !ok || len(src) != int(pageHeader.PageSize) {
		return Float64DirectoryView{}, ErrFloat64CatalogCorrupt
	}
	end := PageHeaderSize + int(pageHeader.PayloadLength)
	return openFloat64DirectoryPayload(
		pageHeader, src[PageHeaderSize:end:end],
		fileEnd, nextLogicalID, allocationQuantum,
	)
}

func openFloat64DirectoryPayload(
	pageHeader PageHeader,
	payload []byte,
	fileEnd, nextLogicalID uint64,
	allocationQuantum uint32,
) (Float64DirectoryView, error) {
	if pageHeader.Kind != PageFloat64Catalog || pageHeader.Flags != 0 ||
		len(payload) < Float64DirectoryPayloadHeaderSize ||
		binary.LittleEndian.Uint32(payload[0:4]) != float64DirectoryVersion ||
		payload[5] != 0 || payload[7] != 0 ||
		!allZero(payload[8:Float64DirectoryPayloadHeaderSize]) {
		return Float64DirectoryView{}, fmt.Errorf(
			"%w: header or reserved bytes", ErrFloat64CatalogCorrupt,
		)
	}
	count := int(payload[6])
	header := Float64DirectoryHeader{
		StoreID: pageHeader.StoreID, Generation: pageHeader.Generation,
		LogicalID: pageHeader.LogicalID, PageSize: pageHeader.PageSize,
		Level: payload[4],
	}
	if err := validateFloat64DirectoryHeader(
		header, count, nextLogicalID, allocationQuantum,
	); err != nil ||
		len(payload) != Float64DirectoryPayloadHeaderSize+
			count*Float64DirectoryRecordSize {
		return Float64DirectoryView{}, fmt.Errorf(
			"%w: node bounds", ErrFloat64CatalogCorrupt,
		)
	}
	var previous uint32
	var seen chunkDirectoryRefSet
	for i := range count {
		record := payload[Float64DirectoryPayloadHeaderSize+i*Float64DirectoryRecordSize:]
		first := binary.LittleEndian.Uint32(record[0:4])
		ref := decodePageRef(record[8 : 8+PageRefSize])
		if !allZero(record[4:8]) ||
			i != 0 && previous >= first ||
			!pageRefReservedZero(record[8:8+PageRefSize]) {
			return Float64DirectoryView{}, fmt.Errorf(
				"%w: record order or reserved bytes",
				ErrFloat64CatalogCorrupt,
			)
		}
		if err := validateFloat64DirectoryRef(
			header, ref, fileEnd, nextLogicalID, allocationQuantum,
		); err != nil || !seen.add(ref) {
			return Float64DirectoryView{}, fmt.Errorf(
				"%w: record reference", ErrFloat64CatalogCorrupt,
			)
		}
		previous = first
	}
	return Float64DirectoryView{
		header: header, payload: payload, count: uint8(count),
	}, nil
}

func validateFloat64DirectoryHeader(
	header Float64DirectoryHeader,
	count int,
	nextLogicalID uint64,
	allocationQuantum uint32,
) error {
	payloadLength := Float64DirectoryPayloadHeaderSize +
		count*Float64DirectoryRecordSize
	if header.StoreID == ([16]byte{}) || header.Generation == 0 ||
		header.LogicalID <= StateRootLogicalID ||
		header.LogicalID >= nextLogicalID ||
		header.PageSize != allocationQuantum ||
		!validPhysicalPageSize(allocationQuantum) ||
		header.Level > Float64DirectoryMaxLevel ||
		count < 1 || count > Float64DirectoryFanout ||
		payloadLength >
			int(header.PageSize)-PageHeaderSize-PageTrailerSize {
		return fmt.Errorf("%w: float64 directory header", ErrInvalidWrite)
	}
	return nil
}

func validateFloat64DirectoryRef(
	header Float64DirectoryHeader,
	ref PageRef,
	fileEnd, nextLogicalID uint64,
	allocationQuantum uint32,
) error {
	length := uint64(ref.Length)
	layout, err := MutableStoreLayout(allocationQuantum)
	if err != nil {
		return err
	}
	wantKind := PageFloat64Catalog
	exactLength := true
	if header.Level == 0 {
		wantKind = PageFloat64Stripe
		exactLength = false
	}
	if ref.Kind != wantKind || ref.Flags != 0 || ref.Aux != 0 ||
		ref.Generation == 0 || ref.Generation > header.Generation ||
		ref.LogicalID <= StateRootLogicalID ||
		ref.LogicalID >= nextLogicalID ||
		ref.LogicalID == header.LogicalID ||
		!validPhysicalPageSize(ref.Length) ||
		ref.Length < allocationQuantum ||
		exactLength && ref.Length != allocationQuantum ||
		ref.Length%allocationQuantum != 0 ||
		ref.Offset%uint64(allocationQuantum) != 0 ||
		ref.Offset < layout.DataStart ||
		length > fileEnd || ref.Offset > fileEnd-length {
		return fmt.Errorf("%w: float64 directory ref", ErrInvalidWrite)
	}
	return nil
}

// AdmittedFloat64Directory reconstructs a previously validated directory
// node. Calling it on unvalidated bytes is invalid.
func AdmittedFloat64Directory(src []byte) Float64DirectoryView {
	pageHeader, _ := decodePageHeader(src)
	end := PageHeaderSize + int(pageHeader.PayloadLength)
	payload := src[PageHeaderSize:end:end]
	return Float64DirectoryView{
		header: Float64DirectoryHeader{
			StoreID: pageHeader.StoreID, Generation: pageHeader.Generation,
			LogicalID: pageHeader.LogicalID, PageSize: pageHeader.PageSize,
			Level: payload[4],
		},
		payload: payload, count: payload[6],
	}
}

func (v Float64DirectoryView) Header() Float64DirectoryHeader {
	return v.header
}

func (v Float64DirectoryView) Len() int { return int(v.count) }

// EntryAt returns one ordered lower-bound mapping.
func (v Float64DirectoryView) EntryAt(
	index int,
) (Float64DirectoryEntry, bool) {
	if index < 0 || index >= int(v.count) {
		return Float64DirectoryEntry{}, false
	}
	record := v.payload[Float64DirectoryPayloadHeaderSize+index*Float64DirectoryRecordSize:]
	return Float64DirectoryEntry{
		FirstChunk: binary.LittleEndian.Uint32(record[0:4]),
		Ref:        decodePageRef(record[8 : 8+PageRefSize]),
	}, true
}

// Floor returns the mapping with the greatest FirstChunk not exceeding chunk.
func (v Float64DirectoryView) Floor(
	chunk uint32,
) (Float64DirectoryEntry, bool) {
	low, high := 0, int(v.count)
	for low < high {
		middle := int(uint(low+high) >> 1)
		record := v.payload[Float64DirectoryPayloadHeaderSize+
			middle*Float64DirectoryRecordSize:]
		if binary.LittleEndian.Uint32(record[0:4]) <= chunk {
			low = middle + 1
		} else {
			high = middle
		}
	}
	if low == 0 {
		return Float64DirectoryEntry{}, false
	}
	return v.EntryAt(low - 1)
}
