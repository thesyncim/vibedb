package storeio

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	// PageHeaderSize is the fixed, architecture-independent prefix shared by
	// every attached-Store page.
	PageHeaderSize = 64
	// PageTrailerSize stores CRC32C and its complement at the end of the
	// physical page. Keeping the trailer fixed makes the payload naturally
	// aligned and lets checksum code consume one contiguous prefix.
	PageTrailerSize = 8

	pageMagic = "SJPAGE01"
	// pageVersion is 4 because the hybrid primary received dedicated durable
	// page kinds. Earlier experiments deliberately reused unrelated kinds,
	// which made a valid checksum insufficient to select the only legal decoder.
	// The store has never been released; there is deliberately no migration
	// path.
	pageVersion = uint16(4)
)

// ErrPageCorrupt reports a malformed, truncated, or checksum-invalid common
// Store page.
var ErrPageCorrupt = errors.New("vibejson: corrupt Store page")

// DevelopmentFormatVersion is the schema version every on-disk structure in
// this package carries while the format is under development.
//
// Nothing is released, so no file written by anyone else exists and there is no
// compatibility ladder to climb. A layout change therefore edits its schema in
// place instead of adding a version, and every file written before the change
// simply stops opening — which is the intended outcome, not a regression.
//
// Sharing one constant is deliberate. Independent per-schema numbers invite
// exactly the wasted care they imply: decode paths that branch on versions no
// file can hold, and reviewers preserving compatibility with nothing. The first
// release replaces this with a real number per schema and starts the ladder for
// real.
const DevelopmentFormatVersion = uint32(0)

// PageKind identifies the pointer-free payload schema inside a common page.
// Values are durable format identifiers, not Go type ordinals.
type PageKind uint8

const (
	// PageStateRoot is the published generation root; it names every current
	// durable structure and is the page recovery selects an image from.
	PageStateRoot PageKind = iota + 1
	// PageDocument is one verbatim JSON document extent: a chunk's contiguous
	// bytes handed to a scan callback without decoding.
	PageDocument
	// PageOverflow is a continuation extent for a document or value too large
	// for one home page; the home page holds a PageRef into this chain.
	PageOverflow
	// PageChunkDirectory is a chunk-radix directory node mapping chunk IDs to
	// their document pages.
	PageChunkDirectory
	// PageKeyDirectory is the pre-hybrid primary-key directory (branch or leaf).
	// It remains a distinct discriminator so a reader never mistakes it for the
	// fingerprint directory or a primary-graph node.
	PageKeyDirectory
	// PageIndexDirectory is one exact secondary-index directory node.
	PageIndexDirectory
	// PageIndexPosting is a packed posting-list page for a secondary index.
	PageIndexPosting
	// PageDocumentGroup is a compact-format group page: one shape template and
	// value dictionary plus per-row token streams packing several chunks.
	PageDocumentGroup
	// PageFloat64Group is one columnar float64 group page.
	PageFloat64Group
	// PageFloat64Catalog names the float64 groups and stripes of a column.
	PageFloat64Catalog
	// PageFloat64Stripe is one float64 column stripe.
	PageFloat64Stripe
	// PageIndexGroupCatalog names the durable index groups.
	PageIndexGroupCatalog
	// PageFreeImage and PageFreeDelta carry the free set as a base image plus a
	// chain of per-commit diffs. They replaced a B+tree of PageFreeDirectory
	// nodes, whose identifier is deliberately gone rather than reserved: the
	// store has never been released, so renumbering costs nothing, and leaving a
	// hole would invite a reader to treat an old node as a valid page of some
	// other kind instead of failing the way a removed format should.
	PageFreeImage
	// PageFreeDelta is one per-commit free-set diff in the image-plus-delta
	// chain described above.
	PageFreeDelta
	// PageFreeIndex names the image's segments. It exists so a fold rewrites the
	// segments a commit touched instead of the whole image; see free_index.go
	// for why the image stopped being a linked list.
	PageFreeIndex
	// PageFingerprintDirectory is the hash-routed primary-key directory. It is
	// intentionally distinct from the legacy PageKeyDirectory kind, which has
	// carried more than one primary-directory schema during development. The
	// durable kind is the format discriminator: readers must never guess a
	// decoder from payload bytes.
	//
	// Keep this value after the existing kinds. Adding it here preserves every
	// durable identifier already assigned above.
	PageFingerprintDirectory
	// PageCatalogSegment carries the self-describing, canonical Store catalog.
	// It is deliberately distinct from every query accelerator: reopening a
	// file must select this decoder from the durable kind, never by guessing
	// from payload bytes.
	PageCatalogSegment
	// The hybrid primary is one durable graph, but each independently cached
	// schema has its own kind. Readers therefore select exactly one decoder from
	// the common header; no byte probing or legacy fallback is permitted.
	//
	// PagePrimaryCatalog is the graph root: the exact lexical catalog whose
	// leaves name each macro-tablet's root PageRef, suitable for
	// StateRoot.PrimaryRoot.
	PagePrimaryCatalog
	// PageTabletDirectory is a tablet-directory node of that catalog, routing a
	// key to the tablet that owns it.
	PageTabletDirectory
	// PagePrimaryLocator is a tablet's local-ID locator page, mapping a stable
	// BucketID to the current leaf page and slot holding it.
	PagePrimaryLocator
	// PageTabletRoute is a tablet's segmented route block over its leaves.
	PageTabletRoute
	// PagePrimaryAnchor is a tablet's lexical anchor map: immutable interval
	// fences that route to stable BucketIDs.
	PagePrimaryAnchor
	// PagePrimaryLeaf is a primary leaf holding the ordered key/value records.
	PagePrimaryLeaf
	// PagePrimaryExactRoot names the immutable physical exact-index leaves built
	// beside an ordered primary graph: a canonical physical-index-id to
	// PagePrimaryExactLeaf reference catalog. PagePrimaryExactLeaf wraps one
	// canonical IndexTermLeaf byte stream in the common page envelope.
	PagePrimaryExactRoot
	PagePrimaryExactLeaf
)

// PageHeader is the decoded identity of one immutable physical page. StoreID
// prevents cross-file grafting, LogicalID remains stable across copy-on-write
// replacement, and Generation identifies the version of that logical page.
// PayloadLength excludes the fixed header, zero padding, and checksum trailer.
type PageHeader struct {
	StoreID       [16]byte
	Generation    uint64
	LogicalID     uint64
	PageSize      uint32
	PayloadLength uint32
	Kind          PageKind
	Flags         uint8
}

// InitPage clears one caller-owned physical page, writes its canonical header,
// and returns the exact payload window for the caller to fill. The returned
// slice aliases dst. Call SealPage only after filling it. No allocation is
// performed.
func InitPage(dst []byte, header PageHeader) ([]byte, error) {
	if err := validatePageHeader(header); err != nil {
		return nil, err
	}
	if uint64(len(dst)) < uint64(header.PageSize) {
		return nil, fmt.Errorf("%w: page buffer has %d bytes, need %d", ErrInvalidWrite, len(dst), header.PageSize)
	}
	page := dst[:int(header.PageSize)]
	clear(page)
	copy(page[0:8], pageMagic)
	binary.LittleEndian.PutUint16(page[8:10], pageVersion)
	binary.LittleEndian.PutUint16(page[10:12], PageHeaderSize)
	page[12] = byte(header.Kind)
	page[13] = header.Flags
	binary.LittleEndian.PutUint32(page[16:20], header.PageSize)
	binary.LittleEndian.PutUint32(page[20:24], header.PayloadLength)
	binary.LittleEndian.PutUint64(page[24:32], header.Generation)
	binary.LittleEndian.PutUint64(page[32:40], header.LogicalID)
	copy(page[40:56], header.StoreID[:])
	end := PageHeaderSize + int(header.PayloadLength)
	return page[PageHeaderSize:end:end], nil
}

// SealPage validates a page initialized by InitPage and writes its CRC32C
// trailer. Bytes outside the declared payload must remain zero, preventing
// stale buffer contents from becoming durable or leaking into deterministic
// images. No allocation is performed.
func SealPage(page []byte) (uint32, error) {
	return sealPage(page, true)
}

// sealInitializedPage is the internal fast path for encoders that call
// InitPage and write only through its capacity-clipped payload. InitPage has
// already cleared the padding, so rescanning it would add a second full-page
// pass without strengthening that construction path.
func sealInitializedPage(page []byte) (uint32, error) {
	return sealPage(page, false)
}

func sealPage(page []byte, validatePadding bool) (uint32, error) {
	header, ok := decodePageHeader(page)
	if !ok || uint64(len(page)) < uint64(header.PageSize) {
		return 0, fmt.Errorf("%w: invalid page header", ErrInvalidWrite)
	}
	page = page[:int(header.PageSize)]
	payloadEnd := PageHeaderSize + int(header.PayloadLength)
	trailer := len(page) - PageTrailerSize
	if !allZero(page[14:16]) || !allZero(page[56:64]) ||
		validatePadding && !allZero(page[payloadEnd:trailer]) {
		return 0, fmt.Errorf("%w: non-zero page reserved bytes or padding", ErrInvalidWrite)
	}
	checksum := PageChecksum(page[:trailer])
	binary.LittleEndian.PutUint32(page[trailer:trailer+4], checksum)
	binary.LittleEndian.PutUint32(page[trailer+4:], ^checksum)
	return checksum, nil
}

// OpenPage verifies and decodes one physical page, returning a capacity-clipped
// payload view that borrows src. Unknown kinds or flags, non-canonical reserved
// bytes, impossible lengths, and checksum failures are rejected before a
// payload byte is trusted. Padding is checksum-covered but deliberately not
// scanned a second time; InitPage and SealPage enforce zero padding on writers,
// and the clipped view makes padding inaccessible to readers. No allocation is
// performed on success.
func OpenPage(src []byte) (PageHeader, []byte, error) {
	header, ok := decodePageHeader(src)
	if !ok || uint64(len(src)) < uint64(header.PageSize) {
		return PageHeader{}, nil, fmt.Errorf("%w: header", ErrPageCorrupt)
	}
	page := src[:int(header.PageSize)]
	payloadEnd := PageHeaderSize + int(header.PayloadLength)
	trailer := len(page) - PageTrailerSize
	checksum := binary.LittleEndian.Uint32(page[trailer : trailer+4])
	if binary.LittleEndian.Uint32(page[trailer+4:]) != ^checksum ||
		PageChecksum(page[:trailer]) != checksum {
		return PageHeader{}, nil, fmt.Errorf("%w: checksum", ErrPageCorrupt)
	}
	if !allZero(page[14:16]) || !allZero(page[56:64]) {
		return PageHeader{}, nil, fmt.Errorf("%w: reserved bytes", ErrPageCorrupt)
	}
	return header, page[PageHeaderSize:payloadEnd:payloadEnd], nil
}

func decodePageHeader(src []byte) (PageHeader, bool) {
	if len(src) < PageHeaderSize || string(src[0:8]) != pageMagic ||
		binary.LittleEndian.Uint16(src[8:10]) != pageVersion ||
		binary.LittleEndian.Uint16(src[10:12]) != PageHeaderSize {
		return PageHeader{}, false
	}
	header := PageHeader{
		Kind:          PageKind(src[12]),
		Flags:         src[13],
		PageSize:      binary.LittleEndian.Uint32(src[16:20]),
		PayloadLength: binary.LittleEndian.Uint32(src[20:24]),
		Generation:    binary.LittleEndian.Uint64(src[24:32]),
		LogicalID:     binary.LittleEndian.Uint64(src[32:40]),
	}
	copy(header.StoreID[:], src[40:56])
	return header, validatePageHeader(header) == nil
}

func validatePageHeader(header PageHeader) error {
	if header.StoreID == ([16]byte{}) || header.Generation == 0 || header.LogicalID == 0 {
		return fmt.Errorf("%w: zero page identity", ErrInvalidWrite)
	}
	if !validPageKind(header.Kind) || !validPageFlags(header.Kind, header.Flags) {
		return fmt.Errorf("%w: page kind or flags", ErrInvalidWrite)
	}
	if !validPageExtentSize(header.Kind, header.PageSize) ||
		uint64(header.PayloadLength) > uint64(header.PageSize)-PageHeaderSize-PageTrailerSize {
		return fmt.Errorf("%w: page or payload size", ErrInvalidWrite)
	}
	return nil
}

// validPageExtentSize keeps metadata and value formats without complete typed
// support on the power-of-two allocation geometry. Primary document and
// overflow extents have end-to-end exact-quantum allocation, validation,
// caching, retirement, and recovery support.
func validPageExtentSize(kind PageKind, size uint32) bool {
	if validPhysicalPageSize(size) {
		return true
	}
	switch kind {
	case PageDocument, PageOverflow:
		return size >= physicalPageQuantum && size%physicalPageQuantum == 0
	default:
		return false
	}
}

func validPageKind(kind PageKind) bool {
	return kind >= PageStateRoot && kind <= PagePrimaryExactLeaf
}

func validPageFlags(kind PageKind, flags uint8) bool {
	if kind == PageDocumentGroup {
		encoded := uint16(flags)
		return encoded&^documentGroupKnownFlags == 0 &&
			(encoded&DocumentGroupFlagFloat64Sidecar != 0 || encoded == 0)
	}
	return flags == 0
}
