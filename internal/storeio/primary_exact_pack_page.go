package storeio

import (
	"encoding/binary"
	"fmt"
)

const (
	primaryExactInventoryHeaderBytes = 48
	primaryExactInventoryEntryBytes  = PageRefSize
)

// EncodePrimaryExactPackPage seals one raw pack payload in the common page
// envelope. LZ4 emission is deliberately excluded from this integration stage.
func EncodePrimaryExactPackPage(dst []byte, storeID [16]byte, generation, logicalID uint64, pack []byte) ([]byte, error) {
	var decoded PrimaryExactPackDecoder
	if err := decoded.Open(pack); err != nil || decoded.Codec() != "raw" {
		return nil, fmt.Errorf("%w: exact pack payload", ErrInvalidWrite)
	}
	if len(dst) > PrimaryExactPackMaxDecodedBytes || len(pack) > len(dst)-PageHeaderSize-PageTrailerSize {
		return nil, fmt.Errorf("%w: exact pack extent", ErrInvalidWrite)
	}
	payload, err := InitPage(dst, PageHeader{StoreID: storeID, Generation: generation, LogicalID: logicalID, PageSize: uint32(len(dst)), PayloadLength: uint32(len(pack)), Kind: PagePrimaryExactPack})
	if err != nil {
		return nil, err
	}
	copy(payload, pack)
	if _, err := sealInitializedPage(dst); err != nil {
		return nil, err
	}
	return dst, nil
}

// OpenPrimaryExactPackPage validates the envelope and opens the pack using the
// caller's prepared decoder. Member views follow the decoder's borrow lifetime.
func OpenPrimaryExactPackPage(src []byte, expected PageRef, bounds PrimaryExactIndexBounds, decoder *PrimaryExactPackDecoder) error {
	if decoder == nil || expected.Length > PrimaryExactPackMaxDecodedBytes || !validPrimaryExactRef(expected, PagePrimaryExactPack, bounds) {
		return primaryExactCorrupt("pack reference")
	}
	header, payload, err := OpenPage(src)
	if err != nil || header.StoreID != bounds.StoreID || header.Generation != expected.Generation || header.LogicalID != expected.LogicalID || header.PageSize != expected.Length || header.Kind != PagePrimaryExactPack {
		return primaryExactCorrupt("pack envelope")
	}
	if err := decoder.Open(payload); err != nil {
		return primaryExactCorrupt("pack payload")
	}
	return nil
}

// PrimaryExactInventoryView is one page in the authoritative sorted physical
// pack inventory. The chain and exact aggregate counts are checked by its root
// consumer; each page independently proves sorted, nonoverlapping pack refs.
type PrimaryExactInventoryView struct {
	payload []byte
	next    PageRef
	count   uint32
}

func PrimaryExactInventoryPageCapacity(pageBytes int) int {
	usable := pageBytes - PageHeaderSize - PageTrailerSize - primaryExactInventoryHeaderBytes
	if usable < primaryExactInventoryEntryBytes {
		return 0
	}
	return usable / primaryExactInventoryEntryBytes
}

func EncodePrimaryExactInventoryPage(dst []byte, storeID [16]byte, generation, logicalID uint64, next PageRef, refs []PageRef) ([]byte, error) {
	payloadBytes := primaryExactInventoryHeaderBytes + len(refs)*primaryExactInventoryEntryBytes
	if len(refs) == 0 || payloadBytes > len(dst)-PageHeaderSize-PageTrailerSize || next != (PageRef{}) && next.Kind != PagePrimaryExactInventory {
		return nil, fmt.Errorf("%w: exact inventory size", ErrInvalidWrite)
	}
	var previousEnd uint64
	for i, ref := range refs {
		if ref.Kind != PagePrimaryExactPack || i != 0 && ref.Offset < previousEnd || uint64(ref.Length) > ^uint64(0)-ref.Offset {
			return nil, fmt.Errorf("%w: exact inventory order", ErrInvalidWrite)
		}
		previousEnd = ref.Offset + uint64(ref.Length)
	}
	payload, err := InitPage(dst, PageHeader{StoreID: storeID, Generation: generation, LogicalID: logicalID, PageSize: uint32(len(dst)), PayloadLength: uint32(payloadBytes), Kind: PagePrimaryExactInventory})
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(payload[0:], primaryExactVersion)
	binary.LittleEndian.PutUint32(payload[4:], uint32(len(refs)))
	encodePageRef(payload[8:8+PageRefSize], next)
	clear(payload[8+PageRefSize : primaryExactInventoryHeaderBytes])
	for i, ref := range refs {
		encodePageRef(payload[primaryExactInventoryHeaderBytes+i*PageRefSize:], ref)
	}
	if _, err := sealInitializedPage(dst); err != nil {
		return nil, err
	}
	return dst, nil
}

func OpenPrimaryExactInventoryPage(src []byte, expected PageRef, bounds PrimaryExactIndexBounds) (PrimaryExactInventoryView, error) {
	if !validPrimaryExactRef(expected, PagePrimaryExactInventory, bounds) {
		return PrimaryExactInventoryView{}, primaryExactCorrupt("inventory reference")
	}
	header, payload, err := OpenPage(src)
	if err != nil || header.StoreID != bounds.StoreID || header.Generation != expected.Generation || header.LogicalID != expected.LogicalID || header.PageSize != expected.Length || header.Kind != PagePrimaryExactInventory || len(payload) < primaryExactInventoryHeaderBytes || binary.LittleEndian.Uint32(payload) != primaryExactVersion || !allZero(payload[8+PageRefSize:primaryExactInventoryHeaderBytes]) {
		return PrimaryExactInventoryView{}, primaryExactCorrupt("inventory envelope")
	}
	count := binary.LittleEndian.Uint32(payload[4:])
	if count == 0 || uint64(primaryExactInventoryHeaderBytes)+uint64(count)*PageRefSize != uint64(len(payload)) {
		return PrimaryExactInventoryView{}, primaryExactCorrupt("inventory count")
	}
	if !pageRefReservedZero(payload[8 : 8+PageRefSize]) {
		return PrimaryExactInventoryView{}, primaryExactCorrupt("inventory next")
	}
	next := decodePageRef(payload[8 : 8+PageRefSize])
	if next != (PageRef{}) && (!validPrimaryExactRef(next, PagePrimaryExactInventory, bounds) || next.Offset == expected.Offset || next.LogicalID == expected.LogicalID) {
		return PrimaryExactInventoryView{}, primaryExactCorrupt("inventory next")
	}
	view := PrimaryExactInventoryView{payload: payload, next: next, count: count}
	var previousEnd uint64
	for i := uint32(0); i < count; i++ {
		ref, ok := view.Entry(i)
		if !ok || !validPrimaryExactRef(ref, PagePrimaryExactPack, bounds) || i != 0 && ref.Offset < previousEnd {
			return PrimaryExactInventoryView{}, primaryExactCorrupt("inventory order")
		}
		previousEnd = ref.Offset + uint64(ref.Length)
	}
	return view, nil
}

func (v PrimaryExactInventoryView) Len() int      { return int(v.count) }
func (v PrimaryExactInventoryView) Next() PageRef { return v.next }
func (v PrimaryExactInventoryView) Entry(i uint32) (PageRef, bool) {
	if i >= v.count {
		return PageRef{}, false
	}
	refBytes := v.payload[primaryExactInventoryHeaderBytes+int(i)*PageRefSize:]
	if !pageRefReservedZero(refBytes[:PageRefSize]) {
		return PageRef{}, false
	}
	ref := decodePageRef(refBytes[:PageRefSize])
	if ref.Kind != PagePrimaryExactPack || ref.Length == 0 || uint64(ref.Length) > ^uint64(0)-ref.Offset {
		return PageRef{}, false
	}
	return ref, true
}
