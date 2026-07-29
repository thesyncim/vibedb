package storeio

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	primaryExactVersion         = uint32(0)
	primaryExactRootHeaderBytes = 16
	// fileFormatMaxExactIndexes bounds the physical exact-index count so one
	// PageSize root always carries every leaf reference.
	fileFormatMaxExactIndexes = 64
)

// ErrPrimaryExactIndexCorrupt reports a malformed ordered-primary exact index
// page, reference catalog, or posting that fails self-describing admission.
var ErrPrimaryExactIndexCorrupt = errors.New("vibedb: corrupt ordered-primary exact index")

// PrimaryExactIndexBounds are the publication bounds shared by the root and
// leaf envelopes. IndexCount is the canonical physical index count.
type PrimaryExactIndexBounds struct {
	StoreID           [16]byte
	Generation        uint64
	FileEnd           uint64
	NextLogicalID     uint64
	AllocationQuantum uint32
	MaxPageSize       uint32
	IndexCount        uint32
}

func (b PrimaryExactIndexBounds) valid() bool {
	return b.StoreID != ([16]byte{}) && b.Generation != 0 &&
		b.FileEnd != 0 && b.NextLogicalID > StateRootLogicalID &&
		b.AllocationQuantum >= physicalPageQuantum &&
		b.MaxPageSize >= b.AllocationQuantum && b.IndexCount != 0
}

// EncodePrimaryExactLeafPage wraps one already canonical IndexTermLeaf in the
// common page envelope. encoded is exactly AppendIndexTermLeaf output.
func EncodePrimaryExactLeafPage(
	dst []byte, storeID [16]byte, generation, logicalID uint64,
	encoded []byte,
) ([]byte, error) {
	if len(encoded) < indexTermLeafHeaderBytes ||
		len(encoded) > IndexTermLeafMaxBytes {
		return nil, fmt.Errorf("%w: term leaf length", ErrInvalidWrite)
	}
	header := PageHeader{
		StoreID: storeID, Generation: generation, LogicalID: logicalID,
		PageSize: uint32(len(dst)), PayloadLength: uint32(len(encoded)),
		Kind: PagePrimaryExactLeaf,
	}
	payload, err := InitPage(dst, header)
	if err != nil {
		return nil, err
	}
	copy(payload, encoded)
	page := dst[:len(dst)]
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

// OpenPrimaryExactLeafPage validates the common envelope and returns the exact
// capacity-clipped IndexTermLeaf bytes. The term codec performs semantic
// admission separately because it requires the selected primary live masks.
func OpenPrimaryExactLeafPage(
	src []byte, expected PageRef, bounds PrimaryExactIndexBounds,
) ([]byte, error) {
	if !validPrimaryExactRef(expected, PagePrimaryExactLeaf, bounds) {
		return nil, primaryExactCorrupt("leaf reference")
	}
	header, payload, err := OpenPage(src)
	if err != nil || header.StoreID != bounds.StoreID ||
		header.Generation != expected.Generation ||
		header.LogicalID != expected.LogicalID ||
		header.PageSize != expected.Length ||
		header.Kind != PagePrimaryExactLeaf || header.Flags != 0 ||
		len(payload) < indexTermLeafHeaderBytes ||
		len(payload) > IndexTermLeafMaxBytes {
		return nil, primaryExactCorrupt("leaf envelope")
	}
	return payload[:len(payload):len(payload)], nil
}

// EncodePrimaryExactRootPage writes the canonical physical-index-id to leaf
// mapping. Record order is the physical ID, so aliases never duplicate bytes.
func EncodePrimaryExactRootPage(
	dst []byte, storeID [16]byte, generation, logicalID uint64,
	leaves []PageRef,
) ([]byte, error) {
	payloadBytes := primaryExactRootHeaderBytes + len(leaves)*PageRefSize
	if len(leaves) == 0 || len(leaves) > fileFormatMaxExactIndexes ||
		payloadBytes > len(dst)-PageHeaderSize-PageTrailerSize {
		return nil, fmt.Errorf("%w: exact root size", ErrInvalidWrite)
	}
	header := PageHeader{
		StoreID: storeID, Generation: generation, LogicalID: logicalID,
		PageSize: uint32(len(dst)), PayloadLength: uint32(payloadBytes),
		Kind: PagePrimaryExactRoot,
	}
	payload, err := InitPage(dst, header)
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(payload[0:4], primaryExactVersion)
	binary.LittleEndian.PutUint32(payload[4:8], uint32(len(leaves)))
	for i, ref := range leaves {
		if ref != (PageRef{}) && ref.Kind != PagePrimaryExactLeaf {
			return nil, fmt.Errorf("%w: exact leaf kind", ErrInvalidWrite)
		}
		encodePageRef(
			payload[primaryExactRootHeaderBytes+i*PageRefSize:primaryExactRootHeaderBytes+(i+1)*PageRefSize],
			ref,
		)
	}
	page := dst[:len(dst)]
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

// PrimaryExactRootView is a validated exact-index reference catalog. Non-zero
// leaf refs are unique; their order is the canonical physical index ID, not
// allocation order. A zero ref names an empty physical index.
type PrimaryExactRootView struct {
	payload []byte
	count   uint32
}

func OpenPrimaryExactRootPage(
	src []byte, expected PageRef, bounds PrimaryExactIndexBounds,
) (PrimaryExactRootView, error) {
	if !bounds.valid() || bounds.IndexCount > fileFormatMaxExactIndexes ||
		!validPrimaryExactRef(expected, PagePrimaryExactRoot, bounds) {
		return PrimaryExactRootView{}, primaryExactCorrupt("root reference")
	}
	header, payload, err := OpenPage(src)
	if err != nil || header.StoreID != bounds.StoreID ||
		header.Generation != expected.Generation ||
		header.LogicalID != expected.LogicalID ||
		header.PageSize != expected.Length ||
		header.Kind != PagePrimaryExactRoot || header.Flags != 0 ||
		len(payload) != primaryExactRootHeaderBytes+
			int(bounds.IndexCount)*PageRefSize ||
		binary.LittleEndian.Uint32(payload[0:4]) != primaryExactVersion ||
		binary.LittleEndian.Uint32(payload[4:8]) != bounds.IndexCount ||
		!allZero(payload[8:primaryExactRootHeaderBytes]) {
		return PrimaryExactRootView{}, primaryExactCorrupt("root envelope")
	}
	view := PrimaryExactRootView{payload: payload, count: bounds.IndexCount}
	for i := uint32(0); i < bounds.IndexCount; i++ {
		ref, ok := view.Leaf(i)
		if !ok {
			return PrimaryExactRootView{}, primaryExactCorrupt("leaf catalog")
		}
		if ref == (PageRef{}) {
			continue
		}
		if !validPrimaryExactRef(ref, PagePrimaryExactLeaf, bounds) ||
			ref.LogicalID == expected.LogicalID ||
			ref.Offset == expected.Offset {
			return PrimaryExactRootView{}, primaryExactCorrupt("leaf catalog")
		}
		for previousID := uint32(0); previousID < i; previousID++ {
			previous, ok := view.Leaf(previousID)
			if !ok {
				return PrimaryExactRootView{}, primaryExactCorrupt("leaf catalog")
			}
			if previous != (PageRef{}) &&
				(previous.LogicalID == ref.LogicalID ||
					previous.Offset == ref.Offset) {
				return PrimaryExactRootView{}, primaryExactCorrupt("leaf catalog")
			}
		}
	}
	return view, nil
}

func (v PrimaryExactRootView) Len() int { return int(v.count) }

func (v PrimaryExactRootView) Leaf(index uint32) (PageRef, bool) {
	if index >= v.count {
		return PageRef{}, false
	}
	at := primaryExactRootHeaderBytes + int(index)*PageRefSize
	ref := decodePageRef(v.payload[at : at+PageRefSize])
	if !pageRefReservedZero(v.payload[at : at+PageRefSize]) {
		return PageRef{}, false
	}
	return ref, true
}

func validPrimaryExactRef(
	ref PageRef, kind PageKind, bounds PrimaryExactIndexBounds,
) bool {
	if !bounds.valid() || ref.Kind != kind || ref.Flags != 0 || ref.Aux != 0 ||
		!validPhysicalPageSize(ref.Length) ||
		ref.Length < bounds.AllocationQuantum ||
		ref.Length > bounds.MaxPageSize ||
		ref.Length%bounds.AllocationQuantum != 0 ||
		ref.Generation == 0 || ref.Generation > bounds.Generation ||
		ref.LogicalID <= StateRootLogicalID ||
		ref.LogicalID >= bounds.NextLogicalID {
		return false
	}
	layout, err := MutableStoreLayout(bounds.AllocationQuantum)
	length := uint64(ref.Length)
	return err == nil && ref.Offset >= layout.DataStart &&
		ref.Offset%uint64(bounds.AllocationQuantum) == 0 &&
		length <= bounds.FileEnd && ref.Offset <= bounds.FileEnd-length
}

func primaryExactCorrupt(what string) error {
	return fmt.Errorf("%w: %s", ErrPrimaryExactIndexCorrupt, what)
}

// VisitPrimaryLeafPostingRows enumerates the live rows of one admitted primary
// leaf, of any class, in lexical key order, with the posting-stable slot each
// row occupies. For the succinct narrow/wide classes the slot is the leaf's
// stable hash-directory slot; for the template-columnar class it is the lexical
// rank, which is exactly the Slot the template builder assigned each row.
//
// A template leaf's slot space differs from the narrow/wide hash space, but
// build placement and read enumeration both route through this one function, so
// the posting a row is written under is the posting a lookup selects it by,
// regardless of class. That is the invariant the read-side liveness recheck
// depends on. raw borrows the leaf page for the succinct classes and the
// returned scratch, spliced fresh for each row, for the template class.
func VisitPrimaryLeafPostingRows(
	page []byte, storeID [16]byte, bucket BucketID,
	bounds CommonPrimaryLeafBounds, scratch []byte,
	fn func(slot uint8, key, raw []byte, overflow bool) error,
) ([]byte, error) {
	switch PrimaryLeafClass(page) {
	case CommonPrimaryLeafTemplate:
		tv, ok := AdmittedCommonPrimaryTemplateLeaf(page, bucket, bounds)
		if !ok {
			return scratch, primaryExactCorrupt("template leaf")
		}
		for rank := 0; rank < tv.Len(); rank++ {
			key, ti, ok := tv.RowAt(rank)
			if !ok {
				return scratch, primaryExactCorrupt("template row")
			}
			scratch = tv.AppendRawRank(scratch[:0], rank, ti)
			if err := fn(uint8(rank), key, scratch, false); err != nil {
				return scratch, err
			}
		}
		return scratch, nil
	case CommonPrimaryLeafCompact:
		cv, ok := AdmittedCommonPrimaryCompactLeaf(page, bucket, bounds)
		if !ok {
			return scratch, primaryExactCorrupt("compact leaf")
		}
		for rank := 0; rank < cv.Len(); rank++ {
			key, ti, ok := cv.RowAt(rank)
			if !ok {
				return scratch, primaryExactCorrupt("compact row")
			}
			scratch = cv.AppendRawRank(scratch[:0], rank, ti)
			if err := fn(uint8(rank), key, scratch, false); err != nil {
				return scratch, err
			}
		}
		return scratch, nil
	case CommonPrimaryLeafUnified:
		// The unified class keeps the envelope's stable hash slots (the one
		// slot discipline of the unified-leaf design §3.1), so posting slots
		// resolve from the encoded slot tables rather than the lexical rank;
		// row bodies render into the reused scratch exactly as templates do.
		uv, ok := AdmittedCommonPrimaryUnifiedLeaf(page, storeID, bucket, bounds)
		if !ok {
			return scratch, primaryExactCorrupt("unified leaf")
		}
		slots, slotsOK := uv.env.rankSlots()
		if !slotsOK {
			return scratch, primaryExactCorrupt("unified slots")
		}
		it := uv.env.AllRows()
		for rank := 0; ; rank++ {
			key, raw, overflow, ok := it.NextRawBorrowed()
			if !ok {
				break
			}
			if overflow {
				if err := fn(slots[rank], key, raw, true); err != nil {
					return scratch, err
				}
				continue
			}
			var rendered bool
			scratch, rendered = uv.AppendRowBody(scratch[:0], raw)
			if !rendered {
				return scratch, primaryExactCorrupt("unified row body")
			}
			if err := fn(slots[rank], key, scratch, false); err != nil {
				return scratch, err
			}
		}
		return scratch, nil
	}
	view := AdmittedCommonPrimaryLeaf(page, storeID, bucket, bounds)
	it := view.AllRows()
	for {
		key, raw, overflow, ok := it.NextRawBorrowed()
		if !ok {
			break
		}
		slot, _, _, found := view.LookupRawHashed(
			KeyHashBytes(storeID, key), key,
		)
		if !found {
			return scratch, primaryExactCorrupt("row slot")
		}
		if err := fn(slot, key, raw, overflow); err != nil {
			return scratch, err
		}
	}
	return scratch, nil
}
