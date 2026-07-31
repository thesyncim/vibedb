package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
)

const (
	primaryExactVersion         = DevelopmentFormatVersion
	primaryExactRootHeaderBytes = 16
	primaryExactRootEntryBytes  = 8 + PageRefSize
	// fileFormatMaxExactIndexes bounds the physical exact-index count so one
	// PageSize root always carries every per-index catalog reference.
	fileFormatMaxExactIndexes = 64

	primaryExactCatalogHeaderBytes = 16
	// PrimaryExactCatalogPrefixBytes caps the routing prefix of a leaf's
	// first term stored in its catalog entry. The prefix routes multi-TB
	// probes without opening leaves; resident probes route through the
	// admitted views, so a truncated prefix costs nothing today.
	PrimaryExactCatalogPrefixBytes = 40

	// PrimaryExactCatalogPiece marks a leaf that is one stripe piece of a
	// giant term (a single-term leaf produced by the rule-2 cutter path);
	// PrimaryExactCatalogRunCut marks a leaf whose first term satisfies the
	// rule-1 run-cut predicate. Both are pure functions of leaf content,
	// re-derivable and cross-checked at Open; they are stored so segment
	// boundaries need no trial estimation on the open path.
	PrimaryExactCatalogPiece  = uint8(1)
	PrimaryExactCatalogRunCut = uint8(2)
	primaryExactCatalogFlags  = PrimaryExactCatalogPiece | PrimaryExactCatalogRunCut
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
		b.FileEnd != 0 && b.NextLogicalID != 0 &&
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
	page := dst
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
		header.Kind != PagePrimaryExactLeaf ||
		len(payload) < indexTermLeafHeaderBytes ||
		len(payload) > IndexTermLeafMaxBytes {
		return nil, primaryExactCorrupt("leaf envelope")
	}
	return payload[:len(payload):len(payload)], nil
}

// PrimaryExactRootEntry is one physical index's row in the exact root:
// how many term leaves the index spans and the reference of its ordered
// catalog. LeafCount == 0 names an empty physical index (zero Catalog).
type PrimaryExactRootEntry struct {
	Catalog   PageRef
	LeafCount uint32
}

// EncodePrimaryExactRootPage writes the canonical physical-index-id to
// term-leaf-catalog mapping. Record order is the physical ID, so aliases
// never duplicate bytes.
func EncodePrimaryExactRootPage(
	dst []byte, storeID [16]byte, generation, logicalID uint64,
	entries []PrimaryExactRootEntry,
) ([]byte, error) {
	payloadBytes := primaryExactRootHeaderBytes +
		len(entries)*primaryExactRootEntryBytes
	if len(entries) == 0 || len(entries) > fileFormatMaxExactIndexes ||
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
	binary.LittleEndian.PutUint32(payload[4:8], uint32(len(entries)))
	for i, entry := range entries {
		if entry.LeafCount == 0 != (entry.Catalog == (PageRef{})) ||
			entry.Catalog != (PageRef{}) &&
				entry.Catalog.Kind != PagePrimaryExactCatalog {
			return nil, fmt.Errorf("%w: exact catalog entry", ErrInvalidWrite)
		}
		at := primaryExactRootHeaderBytes + i*primaryExactRootEntryBytes
		binary.LittleEndian.PutUint32(payload[at:at+4], entry.LeafCount)
		encodePageRef(payload[at+8:at+8+PageRefSize], entry.Catalog)
	}
	page := dst
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

// PrimaryExactRootView is a validated exact root: per physical index, the
// leaf count and catalog reference. Non-zero catalog refs are unique; their
// order is the canonical physical index ID, not allocation order.
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
		header.Kind != PagePrimaryExactRoot ||
		len(payload) != primaryExactRootHeaderBytes+
			int(bounds.IndexCount)*primaryExactRootEntryBytes ||
		binary.LittleEndian.Uint32(payload[0:4]) != primaryExactVersion ||
		binary.LittleEndian.Uint32(payload[4:8]) != bounds.IndexCount ||
		!allZero(payload[8:primaryExactRootHeaderBytes]) {
		return PrimaryExactRootView{}, primaryExactCorrupt("root envelope")
	}
	view := PrimaryExactRootView{payload: payload, count: bounds.IndexCount}
	for i := uint32(0); i < bounds.IndexCount; i++ {
		entry, ok := view.Entry(i)
		if !ok {
			return PrimaryExactRootView{}, primaryExactCorrupt("root catalog")
		}
		if entry.LeafCount == 0 {
			continue
		}
		if !validPrimaryExactRef(
			entry.Catalog, PagePrimaryExactCatalog, bounds,
		) || entry.Catalog.LogicalID == expected.LogicalID ||
			entry.Catalog.Offset == expected.Offset {
			return PrimaryExactRootView{}, primaryExactCorrupt("root catalog")
		}
		for previousID := uint32(0); previousID < i; previousID++ {
			previous, ok := view.Entry(previousID)
			if !ok {
				return PrimaryExactRootView{}, primaryExactCorrupt("root catalog")
			}
			if previous.Catalog != (PageRef{}) &&
				(previous.Catalog.LogicalID == entry.Catalog.LogicalID ||
					previous.Catalog.Offset == entry.Catalog.Offset) {
				return PrimaryExactRootView{}, primaryExactCorrupt("root catalog")
			}
		}
	}
	return view, nil
}

func (v PrimaryExactRootView) Len() int { return int(v.count) }

// Entry returns one physical index's catalog row. ok is false for an
// out-of-range index or a non-canonical record.
func (v PrimaryExactRootView) Entry(index uint32) (PrimaryExactRootEntry, bool) {
	if index >= v.count {
		return PrimaryExactRootEntry{}, false
	}
	at := primaryExactRootHeaderBytes + int(index)*primaryExactRootEntryBytes
	record := v.payload[at : at+primaryExactRootEntryBytes]
	leafCount := binary.LittleEndian.Uint32(record[0:4])
	if !allZero(record[4:8]) || !pageRefReservedZero(record[8:8+PageRefSize]) {
		return PrimaryExactRootEntry{}, false
	}
	catalog := decodePageRef(record[8 : 8+PageRefSize])
	if leafCount == 0 != (catalog == (PageRef{})) {
		return PrimaryExactRootEntry{}, false
	}
	return PrimaryExactRootEntry{Catalog: catalog, LeafCount: leafCount}, true
}

// PrimaryExactCatalogEntry is one ordered term-leaf reference in a level-0
// catalog page: the leaf, the first posting tile of its first term (pieces of
// one giant term share the term and ascend by tile), the content-derived
// piece/run-cut flags, and a routing prefix of the first term's canonical
// bytes (truncated to PrimaryExactCatalogPrefixBytes).
type PrimaryExactCatalogEntry struct {
	Leaf      PageRef
	FirstTile uint32
	Flags     uint8
	Prefix    []byte
}

// PrimaryExactCatalogEntryBytes is the encoded size of one level-0 entry.
func PrimaryExactCatalogEntryBytes(prefixLen int) int {
	return PageRefSize + 4 + 1 + 1 + prefixLen
}

// EncodePrimaryExactCatalogLeafPage writes one level-0 catalog page: ordered
// term-leaf entries for one physical index (or one child's slice of them).
func EncodePrimaryExactCatalogLeafPage(
	dst []byte, storeID [16]byte, generation, logicalID uint64,
	entries []PrimaryExactCatalogEntry,
) ([]byte, error) {
	payloadBytes := primaryExactCatalogHeaderBytes
	for i := range entries {
		if len(entries[i].Prefix) > PrimaryExactCatalogPrefixBytes ||
			entries[i].Flags&^primaryExactCatalogFlags != 0 ||
			entries[i].Leaf.Kind != PagePrimaryExactLeaf {
			return nil, fmt.Errorf("%w: exact catalog entry", ErrInvalidWrite)
		}
		payloadBytes += PrimaryExactCatalogEntryBytes(len(entries[i].Prefix))
	}
	if len(entries) == 0 ||
		payloadBytes > len(dst)-PageHeaderSize-PageTrailerSize {
		return nil, fmt.Errorf("%w: exact catalog size", ErrInvalidWrite)
	}
	header := PageHeader{
		StoreID: storeID, Generation: generation, LogicalID: logicalID,
		PageSize: uint32(len(dst)), PayloadLength: uint32(payloadBytes),
		Kind: PagePrimaryExactCatalog,
	}
	payload, err := InitPage(dst, header)
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(payload[0:4], primaryExactVersion)
	payload[4] = 0 // level 0
	binary.LittleEndian.PutUint32(payload[8:12], uint32(len(entries)))
	at := primaryExactCatalogHeaderBytes
	for i := range entries {
		entry := &entries[i]
		encodePageRef(payload[at:at+PageRefSize], entry.Leaf)
		at += PageRefSize
		binary.LittleEndian.PutUint32(payload[at:at+4], entry.FirstTile)
		payload[at+4] = entry.Flags
		payload[at+5] = uint8(len(entry.Prefix))
		copy(payload[at+6:], entry.Prefix)
		at += 6 + len(entry.Prefix)
	}
	page := dst
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

// EncodePrimaryExactCatalogIndexPage writes one level-1 catalog page: the
// ordered level-0 children of a physical index too large for one extent.
func EncodePrimaryExactCatalogIndexPage(
	dst []byte, storeID [16]byte, generation, logicalID uint64,
	children []PageRef,
) ([]byte, error) {
	payloadBytes := primaryExactCatalogHeaderBytes + len(children)*PageRefSize
	if len(children) < 2 ||
		payloadBytes > len(dst)-PageHeaderSize-PageTrailerSize {
		return nil, fmt.Errorf("%w: exact catalog size", ErrInvalidWrite)
	}
	header := PageHeader{
		StoreID: storeID, Generation: generation, LogicalID: logicalID,
		PageSize: uint32(len(dst)), PayloadLength: uint32(payloadBytes),
		Kind: PagePrimaryExactCatalog,
	}
	payload, err := InitPage(dst, header)
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(payload[0:4], primaryExactVersion)
	payload[4] = 1 // level 1
	binary.LittleEndian.PutUint32(payload[8:12], uint32(len(children)))
	for i, child := range children {
		if child.Kind != PagePrimaryExactCatalog {
			return nil, fmt.Errorf("%w: exact catalog child", ErrInvalidWrite)
		}
		encodePageRef(
			payload[primaryExactCatalogHeaderBytes+i*PageRefSize:], child,
		)
	}
	page := dst
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

// PrimaryExactCatalogView is one validated catalog page of either level.
type PrimaryExactCatalogView struct {
	payload []byte
	count   uint32
	level   uint8
}

func OpenPrimaryExactCatalogPage(
	src []byte, expected PageRef, bounds PrimaryExactIndexBounds,
) (PrimaryExactCatalogView, error) {
	if !bounds.valid() ||
		!validPrimaryExactRef(expected, PagePrimaryExactCatalog, bounds) {
		return PrimaryExactCatalogView{}, primaryExactCorrupt("catalog reference")
	}
	header, payload, err := OpenPage(src)
	if err != nil || header.StoreID != bounds.StoreID ||
		header.Generation != expected.Generation ||
		header.LogicalID != expected.LogicalID ||
		header.PageSize != expected.Length ||
		header.Kind != PagePrimaryExactCatalog ||
		len(payload) < primaryExactCatalogHeaderBytes ||
		binary.LittleEndian.Uint32(payload[0:4]) != primaryExactVersion ||
		payload[4] > 1 || !allZero(payload[5:8]) ||
		!allZero(payload[12:primaryExactCatalogHeaderBytes]) {
		return PrimaryExactCatalogView{}, primaryExactCorrupt("catalog envelope")
	}
	view := PrimaryExactCatalogView{
		payload: payload,
		count:   binary.LittleEndian.Uint32(payload[8:12]),
		level:   payload[4],
	}
	if view.count == 0 {
		return PrimaryExactCatalogView{}, primaryExactCorrupt("catalog empty")
	}
	if view.level == 1 {
		if len(payload) != primaryExactCatalogHeaderBytes+
			int(view.count)*PageRefSize || view.count < 2 {
			return PrimaryExactCatalogView{}, primaryExactCorrupt("catalog children")
		}
		for i := uint32(0); i < view.count; i++ {
			child, ok := view.Child(i)
			if !ok || !validPrimaryExactRef(
				child, PagePrimaryExactCatalog, bounds,
			) || child.LogicalID == expected.LogicalID {
				return PrimaryExactCatalogView{}, primaryExactCorrupt("catalog children")
			}
		}
		return view, nil
	}
	// Level 0: sequential variable-length entries; validate shape, leaf
	// references, and prefix ordering. Prefixes are truncated, so ordering is
	// enforced as non-decreasing by prefix bytes with strictly increasing
	// first tiles between full-key (untruncated) duplicates — one giant
	// term's stripe pieces. The online Open path cross-checks every prefix,
	// tile, and flag against the admitted leaf content itself.
	at := primaryExactCatalogHeaderBytes
	var previousPrefix []byte
	previousTile := uint32(0)
	previousFull := false
	for i := uint32(0); i < view.count; i++ {
		if at+PageRefSize+6 > len(payload) {
			return PrimaryExactCatalogView{}, primaryExactCorrupt("catalog entry")
		}
		record := payload[at:]
		if !pageRefReservedZero(record[:PageRefSize]) {
			return PrimaryExactCatalogView{}, primaryExactCorrupt("catalog entry")
		}
		leaf := decodePageRef(record[:PageRefSize])
		tile := binary.LittleEndian.Uint32(record[PageRefSize : PageRefSize+4])
		flags := record[PageRefSize+4]
		prefixLen := int(record[PageRefSize+5])
		if flags&^primaryExactCatalogFlags != 0 ||
			prefixLen > PrimaryExactCatalogPrefixBytes ||
			at+PageRefSize+6+prefixLen > len(payload) ||
			!validPrimaryExactRef(leaf, PagePrimaryExactLeaf, bounds) {
			return PrimaryExactCatalogView{}, primaryExactCorrupt("catalog entry")
		}
		prefix := record[PageRefSize+6 : PageRefSize+6+prefixLen]
		full := prefixLen < PrimaryExactCatalogPrefixBytes
		if i != 0 {
			switch bytes.Compare(previousPrefix, prefix) {
			case 1:
				return PrimaryExactCatalogView{}, primaryExactCorrupt("catalog order")
			case 0:
				if previousFull && full && tile <= previousTile {
					return PrimaryExactCatalogView{}, primaryExactCorrupt("catalog order")
				}
			}
		}
		previousPrefix, previousTile, previousFull = prefix, tile, full
		at += PageRefSize + 6 + prefixLen
	}
	if at != len(payload) {
		return PrimaryExactCatalogView{}, primaryExactCorrupt("catalog length")
	}
	return view, nil
}

func (v PrimaryExactCatalogView) Level() uint8 { return v.level }
func (v PrimaryExactCatalogView) Len() int     { return int(v.count) }

// Child returns one level-1 child reference.
func (v PrimaryExactCatalogView) Child(index uint32) (PageRef, bool) {
	if v.level != 1 || index >= v.count {
		return PageRef{}, false
	}
	at := primaryExactCatalogHeaderBytes + int(index)*PageRefSize
	if !pageRefReservedZero(v.payload[at : at+PageRefSize]) {
		return PageRef{}, false
	}
	return decodePageRef(v.payload[at : at+PageRefSize]), true
}

// ForEachEntry streams a validated level-0 page's entries in order. The
// entry's Prefix borrows the page for the duration of the callback.
func (v PrimaryExactCatalogView) ForEachEntry(
	fn func(entry PrimaryExactCatalogEntry) error,
) error {
	if v.level != 0 {
		return primaryExactCorrupt("catalog level")
	}
	at := primaryExactCatalogHeaderBytes
	for i := uint32(0); i < v.count; i++ {
		record := v.payload[at:]
		prefixLen := int(record[PageRefSize+5])
		entry := PrimaryExactCatalogEntry{
			Leaf: decodePageRef(record[:PageRefSize]),
			FirstTile: binary.LittleEndian.Uint32(
				record[PageRefSize : PageRefSize+4],
			),
			Flags:  record[PageRefSize+4],
			Prefix: record[PageRefSize+6 : PageRefSize+6+prefixLen],
		}
		if err := fn(entry); err != nil {
			return err
		}
		at += PageRefSize + 6 + prefixLen
	}
	return nil
}

func validPrimaryExactRef(
	ref PageRef, kind PageKind, bounds PrimaryExactIndexBounds,
) bool {
	if !bounds.valid() || ref.Kind != kind ||
		!validPhysicalPageSize(ref.Length) ||
		ref.Length < bounds.AllocationQuantum ||
		ref.Length > bounds.MaxPageSize ||
		ref.Length%bounds.AllocationQuantum != 0 ||
		ref.Generation == 0 || ref.Generation > bounds.Generation ||
		ref.LogicalID == 0 ||
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

// VisitPrimaryLeafPostingRows enumerates one unified leaf's live rows in
// lexical key order with the stable hash-directory slot each row occupies.
// Inline rows render into scratch; overflow descriptors borrow the page.
func VisitPrimaryLeafPostingRows(
	page []byte, storeID [16]byte, bucket BucketID,
	bounds CommonPrimaryLeafBounds, scratch []byte,
	fn func(slot uint8, key, raw []byte, overflow bool) error,
) ([]byte, error) {
	if PrimaryLeafClass(page) != CommonPrimaryLeafUnified {
		return scratch, primaryExactCorrupt("non-unified leaf")
	}
	uv, ok := AdmittedCommonPrimaryUnifiedLeaf(page, storeID, bucket, bounds)
	if !ok {
		return scratch, primaryExactCorrupt("unified leaf")
	}
	slots, slotsOK := uv.env.rankSlots()
	if !slotsOK {
		return scratch, primaryExactCorrupt("unified slots")
	}
	it := uv.env.AllRows()
	var renderer unifiedPrimaryRowRenderer
	renderer.Reset(uv)
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
		scratch = renderer.Append(scratch[:0], raw)
		if err := fn(slots[rank], key, scratch, false); err != nil {
			return scratch, err
		}
	}
	return scratch, nil
}

// VisitPrimaryLeafSelectedPostingRows materializes only the stable slots named
// by selected. It first maps occupied hash slots to lexical ranks, then visits
// those ranks in order. Sparse exact-index scans therefore do O(matches) row
// decoding and rendering instead of reconstructing every document in each
// touched bucket. Dead and absent bits are ignored.
func VisitPrimaryLeafSelectedPostingRows(
	page []byte, storeID [16]byte, bucket BucketID,
	bounds CommonPrimaryLeafBounds, selected [4]uint64, scratch []byte,
	fn func(slot uint8, key, raw []byte, overflow bool) error,
) ([]byte, error) {
	if PrimaryLeafClass(page) != CommonPrimaryLeafUnified {
		return scratch, primaryExactCorrupt("non-unified leaf")
	}
	uv, ok := AdmittedCommonPrimaryUnifiedLeaf(page, storeID, bucket, bounds)
	if !ok {
		return scratch, primaryExactCorrupt("unified leaf")
	}
	requested := 0
	for _, word := range selected {
		requested += bits.OnesCount64(word)
	}
	if requested == 0 {
		return scratch, nil
	}
	// Above three-quarters density, a sequential leaf walk is cheaper than
	// slot-to-rank inversion plus random row-directory probes. This branch also
	// handles masks containing many dead bits efficiently; only occupied slots
	// are delivered.
	if requested*4 >= uv.Len()*3 {
		slots, slotsOK := uv.env.rankSlots()
		if !slotsOK {
			return scratch, primaryExactCorrupt("unified slots")
		}
		it := uv.env.AllRows()
		var renderer unifiedPrimaryRowRenderer
		rendererReady := false
		for rank := 0; ; rank++ {
			key, raw, overflow, rowOK := it.NextRawBorrowed()
			if !rowOK {
				break
			}
			slot := slots[rank]
			if selected[slot>>6]&(uint64(1)<<uint(slot&63)) == 0 {
				continue
			}
			if !overflow {
				if !rendererReady {
					renderer.Reset(uv)
					rendererReady = true
				}
				scratch = renderer.Append(scratch[:0], raw)
				raw = scratch
			}
			if err := fn(slot, key, raw, overflow); err != nil {
				return scratch, err
			}
		}
		return scratch, nil
	}
	var (
		selectedRanks [4]uint64
		slotsByRank   [CommonPrimaryLeafWideSlots]uint8
		anySelected   bool
	)
	for quadrant, word := range selected {
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			word &= word - 1
			slot := uint8(quadrant*64 + bit)
			rank, occupied := uv.env.slotRank(slot)
			if !occupied {
				continue
			}
			anySelected = true
			selectedRanks[rank>>6] |= uint64(1) << uint(rank&63)
			slotsByRank[rank] = slot
		}
	}
	if !anySelected {
		return scratch, nil
	}
	var renderer unifiedPrimaryRowRenderer
	rendererReady := false
	for rankWord, word := range selectedRanks {
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			word &= word - 1
			rank := rankWord*64 + bit
			key, raw, overflow, rowOK := uv.RowRawAt(rank)
			if !rowOK {
				return scratch, primaryExactCorrupt("unified selected row")
			}
			if !overflow {
				if !rendererReady {
					renderer.Reset(uv)
					rendererReady = true
				}
				scratch = renderer.Append(scratch[:0], raw)
				raw = scratch
			}
			if err := fn(slotsByRank[rank], key, raw, overflow); err != nil {
				return scratch, err
			}
		}
	}
	return scratch, nil
}
