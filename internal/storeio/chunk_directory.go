package storeio

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
)

const (
	ChunkDirectoryPayloadHeaderSize = 32
	// chunkDirectoryVersion is 2 because a tree is now only as tall as its live
	// chunk ids require, instead of always reaching from chunk 0 to the uint32
	// ceiling. A version 1 root is a structurally valid version 2 page — it is
	// just a six-level spine of single-child nodes — so it would open cleanly
	// and then silently charge every Put six page writes forever. Rejecting it
	// turns a permanent write-amplification regression into an open error.
	chunkDirectoryVersion    = DevelopmentFormatVersion
	chunkDirectoryRadixBits  = uint8(6)
	chunkDirectoryMaxShift   = uint8(30)
	chunkDirectoryRefSetSize = 128

	// ChunkZoneSize is the width of one lane's opaque chunk summary. The value
	// is set by what a full 64-lane leaf has left over in the smallest page a
	// Store may use: 4096 - 64 header - 8 trailer - 32 payload header - 64*32
	// references = 1944, or 30 bytes a lane. This package deliberately does not
	// know what the bytes mean; store/store_zone_compact.go owns their schema,
	// and keeping them opaque here is what stops a low-level page format from
	// growing a dependency on JSON value ordering.
	ChunkZoneSize = 30

	// ChunkDirectoryFlagZones marks a leaf that carries one ChunkZone per
	// packed reference, in the same rank order. It is a per-page flag rather
	// than a format-wide rule because a builder that has no summaries to write
	// (a bulk load that declines to compute them) must still be able to publish
	// an ordinary leaf, and because a leaf without the flag has to keep reading
	// as exactly the bytes it read as before.
	ChunkDirectoryFlagZones = uint16(1)

	chunkDirectoryKnownFlags = ChunkDirectoryFlagZones
)

// ChunkZone is one chunk's opaque summary as it appears inside a directory
// leaf. The zero value means "no statistics", which is how every lane of a
// leaf written without summaries reads.
type ChunkZone [ChunkZoneSize]byte

// ErrChunkDirectoryCorrupt reports a common page whose packed chunk-directory
// payload is malformed or contains an invalid physical reference.
var ErrChunkDirectoryCorrupt = errors.New("vibejson: corrupt Store chunk directory")

// ChunkDirectoryHeader describes one immutable packed-radix node. Shift is a
// multiple of six; zero identifies a leaf whose entries point to document
// pages or immutable multi-chunk document groups. Several leaf lanes may name
// one group extent. Higher levels point to unique chunk-directory pages.
// Bitmap lane order defines the packed reference order, so sparse nodes store
// no empty slots.
//
// A tree's height is not fixed: the root's own Shift names it, and readers
// must take it from the page rather than assume chunkDirectoryMaxShift. A root
// may also carry a non-zero Prefix, meaning it spans only part of the chunk-id
// space; ids outside that span are absent, not corrupt.
type ChunkDirectoryHeader struct {
	StoreID    [16]byte
	Generation uint64
	LogicalID  uint64
	PageSize   uint32
	Prefix     uint32
	Bitmap     uint64
	Shift      uint8
	Flags      uint16
}

// ChunkDirectoryView is an admitted, checksum-verified directory page. It
// retains only a borrowed view of one resident page, so pointer count scales
// with the bounded frame cache rather than with keys or chunks.
type ChunkDirectoryView struct {
	header  ChunkDirectoryHeader
	payload []byte
}

// EncodeChunkDirectoryPage writes one complete pointer-free directory node.
// refs must be ordered by increasing set-bit lane. fileEnd and nextLogicalID
// come from the state root and bound every child before publication. No
// allocation is performed.
func EncodeChunkDirectoryPage(dst []byte, header ChunkDirectoryHeader, refs []PageRef, fileEnd, nextLogicalID uint64) ([]byte, error) {
	return EncodeChunkDirectoryZonePage(dst, header, refs, nil, fileEnd, nextLogicalID)
}

// EncodeChunkDirectoryZonePage is EncodeChunkDirectoryPage with one opaque
// chunk summary per reference. zones may be nil, in which case the page is
// byte-identical to what EncodeChunkDirectoryPage writes; otherwise it must
// have exactly one entry per reference, in the same packed rank order, and the
// node must be a leaf — a branch names other directory pages, not chunks, so a
// summary there would describe nothing.
//
// The summaries share the page, the generation, and the checksum with the
// references they describe. That is the entire crash-safety argument for
// durable zone maps: there is no window in which a commit has published a new
// chunk extent and not its summary, because they are the same write.
func EncodeChunkDirectoryZonePage(dst []byte, header ChunkDirectoryHeader, refs []PageRef, zones []ChunkZone, fileEnd, nextLogicalID uint64) ([]byte, error) {
	if len(zones) != 0 && (header.Shift != 0 || len(zones) != len(refs)) {
		return nil, fmt.Errorf("%w: directory zone array shape", ErrInvalidWrite)
	}
	// The flag is derived from the argument, never taken from the caller: a
	// header claiming summaries a call did not supply would encode a payload
	// length no reader could parse, so it is rejected rather than corrected.
	if header.Flags&ChunkDirectoryFlagZones != 0 && len(zones) == 0 {
		return nil, fmt.Errorf("%w: directory zone flag without summaries", ErrInvalidWrite)
	}
	if len(zones) != 0 {
		header.Flags |= ChunkDirectoryFlagZones
	}
	if err := validateChunkDirectoryHeader(header, len(refs), fileEnd, nextLogicalID); err != nil {
		return nil, err
	}
	if err := validateChunkDirectoryRefs(header, refs, fileEnd, nextLogicalID); err != nil {
		return nil, err
	}
	payloadLength := ChunkDirectoryPayloadHeaderSize + len(refs)*PageRefSize + len(zones)*ChunkZoneSize
	payload, err := InitPage(dst, PageHeader{
		StoreID:       header.StoreID,
		Generation:    header.Generation,
		LogicalID:     header.LogicalID,
		PageSize:      header.PageSize,
		PayloadLength: uint32(payloadLength),
		Kind:          PageChunkDirectory,
	})
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(payload[0:4], chunkDirectoryVersion)
	binary.LittleEndian.PutUint32(payload[4:8], header.Prefix)
	binary.LittleEndian.PutUint64(payload[8:16], header.Bitmap)
	payload[16] = header.Shift
	payload[17] = uint8(len(refs))
	binary.LittleEndian.PutUint16(payload[18:20], header.Flags)
	for i, ref := range refs {
		start := ChunkDirectoryPayloadHeaderSize + i*PageRefSize
		encodePageRef(payload[start:start+PageRefSize], ref)
	}
	zoneBase := ChunkDirectoryPayloadHeaderSize + len(refs)*PageRefSize
	for i := range zones {
		copy(payload[zoneBase+i*ChunkZoneSize:], zones[i][:])
	}
	page := dst[:int(header.PageSize)]
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

// OpenChunkDirectoryPage verifies a common page and its packed-radix payload
// once, returning a borrowed view for repeated allocation-free lookups.
func OpenChunkDirectoryPage(src []byte, fileEnd, nextLogicalID uint64) (ChunkDirectoryView, error) {
	pageHeader, payload, err := OpenPage(src)
	if err != nil {
		return ChunkDirectoryView{}, fmt.Errorf("%w: %w", ErrChunkDirectoryCorrupt, err)
	}
	if pageHeader.Kind != PageChunkDirectory || len(payload) < ChunkDirectoryPayloadHeaderSize ||
		binary.LittleEndian.Uint32(payload[0:4]) != chunkDirectoryVersion ||
		!allZero(payload[20:ChunkDirectoryPayloadHeaderSize]) {
		return ChunkDirectoryView{}, fmt.Errorf("%w: header, version, or reserved bytes", ErrChunkDirectoryCorrupt)
	}
	count := int(payload[17])
	header := ChunkDirectoryHeader{
		StoreID:    pageHeader.StoreID,
		Generation: pageHeader.Generation,
		LogicalID:  pageHeader.LogicalID,
		PageSize:   pageHeader.PageSize,
		Prefix:     binary.LittleEndian.Uint32(payload[4:8]),
		Bitmap:     binary.LittleEndian.Uint64(payload[8:16]),
		Shift:      payload[16],
		Flags:      binary.LittleEndian.Uint16(payload[18:20]),
	}
	zoneCount := 0
	if header.Flags&ChunkDirectoryFlagZones != 0 {
		if header.Shift != 0 {
			return ChunkDirectoryView{}, fmt.Errorf("%w: zones on a branch node", ErrChunkDirectoryCorrupt)
		}
		zoneCount = count
	}
	if len(payload) != ChunkDirectoryPayloadHeaderSize+count*PageRefSize+zoneCount*ChunkZoneSize {
		return ChunkDirectoryView{}, fmt.Errorf("%w: payload length", ErrChunkDirectoryCorrupt)
	}
	if err := validateChunkDirectoryHeader(header, count, fileEnd, nextLogicalID); err != nil {
		return ChunkDirectoryView{}, fmt.Errorf("%w: %v", ErrChunkDirectoryCorrupt, err)
	}
	var seen chunkDirectoryRefSet
	for i := 0; i < count; i++ {
		start := ChunkDirectoryPayloadHeaderSize + i*PageRefSize
		encoded := payload[start : start+PageRefSize]
		ref := decodePageRef(encoded)
		if err := validateChunkDirectoryRef(header, ref, fileEnd, nextLogicalID); err != nil {
			return ChunkDirectoryView{}, fmt.Errorf("%w: %v", ErrChunkDirectoryCorrupt, err)
		}
		if (header.Shift != 0 || ref.Kind != PageDocumentGroup) && !seen.add(ref) {
			return ChunkDirectoryView{}, fmt.Errorf("%w: duplicate child reference", ErrChunkDirectoryCorrupt)
		}
	}
	return ChunkDirectoryView{header: header, payload: payload}, nil
}

// AdmittedChunkDirectoryPage reconstructs a view of a page already validated
// by PageCache admission. Calling it on arbitrary bytes is invalid. It avoids
// repeating CRC and whole-node validation on every resident lookup.
func AdmittedChunkDirectoryPage(src []byte) ChunkDirectoryView {
	pageHeader, _ := decodePageHeader(src)
	payloadEnd := PageHeaderSize + int(pageHeader.PayloadLength)
	payload := src[PageHeaderSize:payloadEnd:payloadEnd]
	return ChunkDirectoryView{
		header: ChunkDirectoryHeader{
			StoreID: pageHeader.StoreID, Generation: pageHeader.Generation,
			LogicalID: pageHeader.LogicalID, PageSize: pageHeader.PageSize,
			Prefix: binary.LittleEndian.Uint32(payload[4:8]),
			Bitmap: binary.LittleEndian.Uint64(payload[8:16]), Shift: payload[16],
			Flags: binary.LittleEndian.Uint16(payload[18:20]),
		},
		payload: payload,
	}
}

// Header returns the value-only identity and radix metadata of the view.
func (v ChunkDirectoryView) Header() ChunkDirectoryHeader { return v.header }

// Len returns the number of packed child references.
func (v ChunkDirectoryView) Len() int { return bits.OnesCount64(v.header.Bitmap) }

// RefAt returns the reference at packed rank. It performs no checksum or heap
// work because OpenChunkDirectoryPage validated the complete payload once.
func (v ChunkDirectoryView) RefAt(rank int) (PageRef, bool) {
	if rank < 0 || rank >= v.Len() {
		return PageRef{}, false
	}
	return chunkDirectoryRefAt(v.payload, rank)
}

// ChunkIDAt returns the logical chunk id represented by a leaf rank. It is
// false for branch nodes. Rank follows the increasing set-bit order used by
// RefAt, so callers can enumerate a sparse leaf without scanning empty ids.
func (v ChunkDirectoryView) ChunkIDAt(rank int) (uint32, bool) {
	if v.header.Shift != 0 || rank < 0 || rank >= v.Len() {
		return 0, false
	}
	bitmap := v.header.Bitmap
	for range rank {
		bitmap &= bitmap - 1
	}
	lane := uint32(bits.TrailingZeros64(bitmap))
	return v.header.Prefix | lane, true
}

// Lookup resolves one logical chunk id with a prefix check, bitmap probe, and
// popcount rank. A branch result names another chunk-directory page; a leaf
// result names the immutable document page for that chunk.
func (v ChunkDirectoryView) Lookup(chunkID uint32) (PageRef, bool) {
	if chunkDirectoryPrefix(chunkID, v.header.Shift) != v.header.Prefix {
		return PageRef{}, false
	}
	lane := uint8(chunkID >> v.header.Shift & 63)
	bit := uint64(1) << lane
	if v.header.Bitmap&bit == 0 {
		return PageRef{}, false
	}
	rank := bits.OnesCount64(v.header.Bitmap & (bit - 1))
	return chunkDirectoryRefAt(v.payload, rank)
}

// HasZones reports whether this node carries per-lane chunk summaries.
func (v ChunkDirectoryView) HasZones() bool {
	return v.header.Flags&ChunkDirectoryFlagZones != 0
}

// ZoneAt returns the opaque summary at packed rank, or the zero summary — "no
// statistics" — for a node that carries none. Reading a summary costs one
// bounds check and one 30-byte copy out of the already-resident leaf; it never
// touches the chunk the summary describes, which is the entire point of
// storing it here.
func (v ChunkDirectoryView) ZoneAt(rank int) ChunkZone {
	var zone ChunkZone
	if !v.HasZones() || rank < 0 || rank >= v.Len() {
		return zone
	}
	start := ChunkDirectoryPayloadHeaderSize + v.Len()*PageRefSize + rank*ChunkZoneSize
	if start+ChunkZoneSize > len(v.payload) {
		return zone
	}
	copy(zone[:], v.payload[start:start+ChunkZoneSize])
	return zone
}

// Zone resolves one logical chunk id's summary with the same prefix, bitmap,
// and popcount probe Lookup uses.
func (v ChunkDirectoryView) Zone(chunkID uint32) ChunkZone {
	var zone ChunkZone
	if chunkDirectoryPrefix(chunkID, v.header.Shift) != v.header.Prefix {
		return zone
	}
	lane := uint8(chunkID >> v.header.Shift & 63)
	bit := uint64(1) << lane
	if v.header.Bitmap&bit == 0 {
		return zone
	}
	return v.ZoneAt(bits.OnesCount64(v.header.Bitmap & (bit - 1)))
}

func chunkDirectoryRefAt(payload []byte, rank int) (PageRef, bool) {
	start := ChunkDirectoryPayloadHeaderSize + rank*PageRefSize
	if rank < 0 || start < ChunkDirectoryPayloadHeaderSize || start+PageRefSize > len(payload) {
		return PageRef{}, false
	}
	return decodePageRef(payload[start : start+PageRefSize]), true
}

func validateChunkDirectoryHeader(header ChunkDirectoryHeader, count int, fileEnd, nextLogicalID uint64) error {
	if header.StoreID == ([16]byte{}) || header.Generation == 0 || header.LogicalID <= StateRootLogicalID ||
		header.LogicalID >= nextLogicalID || !validPhysicalPageSize(header.PageSize) ||
		header.Flags&^chunkDirectoryKnownFlags != 0 {
		return fmt.Errorf("%w: directory identity, page size, or flags", ErrInvalidWrite)
	}
	if header.Bitmap == 0 || count != bits.OnesCount64(header.Bitmap) || count > 64 ||
		header.Shift > chunkDirectoryMaxShift || header.Shift%chunkDirectoryRadixBits != 0 ||
		chunkDirectoryPrefix(header.Prefix, header.Shift) != header.Prefix {
		return fmt.Errorf("%w: directory radix metadata", ErrInvalidWrite)
	}
	if header.Shift == chunkDirectoryMaxShift && header.Bitmap&^uint64(0xf) != 0 {
		return fmt.Errorf("%w: high directory lanes exceed uint32", ErrInvalidWrite)
	}
	pageSize := uint64(header.PageSize)
	layout, err := MutableStoreLayout(header.PageSize)
	if err != nil {
		return err
	}
	if fileEnd < layout.DataStart || fileEnd > maxSuperblockFileOffset || fileEnd%pageSize != 0 ||
		nextLogicalID <= StateRootLogicalID {
		return fmt.Errorf("%w: directory bounds", ErrInvalidWrite)
	}
	zoneLength := 0
	if header.Flags&ChunkDirectoryFlagZones != 0 {
		if header.Shift != 0 {
			return fmt.Errorf("%w: directory zones on a branch node", ErrInvalidWrite)
		}
		zoneLength = count * ChunkZoneSize
	}
	payloadLength := uint64(ChunkDirectoryPayloadHeaderSize + count*PageRefSize + zoneLength)
	if payloadLength > pageSize-PageHeaderSize-PageTrailerSize {
		return fmt.Errorf("%w: directory payload does not fit", ErrInvalidWrite)
	}
	return nil
}

func validateChunkDirectoryRefs(header ChunkDirectoryHeader, refs []PageRef, fileEnd, nextLogicalID uint64) error {
	var seen chunkDirectoryRefSet
	for _, ref := range refs {
		if err := validateChunkDirectoryRef(header, ref, fileEnd, nextLogicalID); err != nil {
			return err
		}
		if (header.Shift != 0 || ref.Kind != PageDocumentGroup) && !seen.add(ref) {
			return fmt.Errorf("%w: duplicate child reference", ErrInvalidWrite)
		}
	}
	return nil
}

// chunkDirectoryRefSet is a bounded, stack-resident uniqueness filter for one
// 64-way node. Two independent tables avoid conflating logical and physical
// namespaces. Valid references never contain zero, which is the empty marker.
// A 2:1 table-to-entry ratio bounds probes without allocating or retaining a
// Go pointer per reference.
type chunkDirectoryRefSet struct {
	logical [chunkDirectoryRefSetSize]uint64
	offset  [chunkDirectoryRefSetSize]uint64
}

func (set *chunkDirectoryRefSet) add(ref PageRef) bool {
	if !chunkDirectoryRefSetInsert(&set.logical, ref.LogicalID) {
		return false
	}
	if !chunkDirectoryRefSetInsert(&set.offset, ref.Offset) {
		return false
	}
	return true
}

func chunkDirectoryRefSetInsert(table *[chunkDirectoryRefSetSize]uint64, value uint64) bool {
	// Fibonacci hashing distributes both consecutive logical ids and aligned
	// physical offsets across the power-of-two table.
	slot := value * 0x9e3779b97f4a7c15 >> 57
	for {
		current := table[slot]
		if current == 0 {
			table[slot] = value
			return true
		}
		if current == value {
			return false
		}
		slot = (slot + 1) & (chunkDirectoryRefSetSize - 1)
	}
}

func validateChunkDirectoryRef(header ChunkDirectoryHeader, ref PageRef, fileEnd, nextLogicalID uint64) error {
	wantKind := PageChunkDirectory
	validLength := ref.Length == header.PageSize
	if header.Shift == 0 {
		wantKind = ref.Kind
		// Directory and metadata nodes use the Store's allocation quantum.
		// Primary documents may occupy any whole number of quanta. Immutable
		// document groups retain their power-of-two packed format.
		switch ref.Kind {
		case PageDocument:
			validLength = validPageExtentSize(PageDocument, ref.Length) &&
				ref.Length >= header.PageSize && ref.Length%header.PageSize == 0
		case PageDocumentGroup:
			validLength = ref.Length >= header.PageSize &&
				validPhysicalPageSize(ref.Length)
		default:
			validLength = false
		}
	}
	pageSize := uint64(header.PageSize)
	length := uint64(ref.Length)
	layout, err := MutableStoreLayout(header.PageSize)
	if err != nil {
		return err
	}
	validFlags := ref.Flags == 0 && ref.Aux == 0
	if header.Shift == 0 && ref.Kind == PageDocumentGroup {
		flags := uint16(ref.Flags)
		validFlags = flags&^documentGroupKnownFlags == 0 &&
			(flags&DocumentGroupFlagFloat64Sidecar != 0 || flags == 0)
		if validFlags && flags == 0 {
			validFlags = ref.Aux == 0
		}
		if validFlags && flags != 0 {
			sidecar, found, err := DocumentGroupFloat64Sidecar(ref, header.PageSize)
			validFlags = err == nil && found && sidecar.LogicalID < nextLogicalID &&
				uint64(sidecar.Length) <= fileEnd && sidecar.Offset <= fileEnd-uint64(sidecar.Length)
		}
	}
	if ref.Kind != wantKind || !validFlags || !validLength ||
		ref.Generation == 0 || ref.Generation > header.Generation ||
		ref.LogicalID <= StateRootLogicalID || ref.LogicalID >= nextLogicalID ||
		ref.Offset < layout.DataStart || ref.Offset%pageSize != 0 ||
		ref.Offset > maxSuperblockFileOffset || length > fileEnd || ref.Offset > fileEnd-length {
		return fmt.Errorf("%w: invalid chunk-directory child", ErrInvalidWrite)
	}
	return nil
}

func chunkDirectoryPrefix(chunkID uint32, shift uint8) uint32 {
	covered := shift + chunkDirectoryRadixBits
	if covered >= 32 {
		return 0
	}
	return chunkID &^ (uint32(1)<<covered - 1)
}
