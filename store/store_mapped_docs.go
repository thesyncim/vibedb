package store

import (
	"encoding/binary"
	"math"
	"runtime"
	"unsafe"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibedb/internal/storemem"
)

// storeMappedDocRef is the pointer-free description of one document in a
// validated or collection-owned packed source. sourceOff addresses exact JSON;
// the aligned tape immediately follows it. A ref never escapes; docAt
// reconstructs ordinary borrowed slices while collection state pins the bytes.
type storeMappedDocRef struct {
	sourceOff  uint64
	srcLen     uint32
	entryCount uint32
	start      uint32
	end        uint32
	shapeID    uint32
	kind       uint8
	enriched   bool
	_          [2]byte
}

// storeOwnedDocRef is the middle Builder control-plane descriptor. Its
// 16-bit shape and root coordinates cover wide shaped rows when a classic
// tape count cannot use the union field in storeCompactDocRef. An exceptional
// wider shaped row selects the general 32-byte descriptor for the publication.
type storeOwnedDocRef struct {
	sourceOff  uint64
	srcLen     uint32
	entryCount uint32
	shapeID    uint16
	start      uint16
	end        uint16
	kind       uint8
	flags      uint8
}

// storeCompactDocRef is the common 16-byte Builder descriptor. meta is
// an entry count for a classic row and a shape/template ID for every compact
// row kind; those kinds derive their entry count from immutable shared
// metadata. Root coordinates are likewise recovered only by the cold
// navigation/checkpoint paths. Keeping the first 12 bytes identical to the
// wider descriptors makes exact-source reads direct and branch-free.
type storeCompactDocRef struct {
	sourceOff uint64
	srcLen    uint32
	meta      uint16
	kind      uint8
	flags     uint8
}

const storeMappedNoShape = ^uint32(0)
const storeOwnedNoShape = ^uint16(0)

// Builder scalar tapes exploit the flat-shape proof: next is one and
// count is zero, so only source coordinates and kind/flag bits remain. Three-
// and four-byte rows cover common bounds; the five-byte form is exact fallback.
const (
	storeOwnedDocNarrow              = uint8(3)
	storeOwnedDocTemplate            = uint8(4) // uint16 start + uint16 end
	storeOwnedDocTemplate8           = uint8(5) // uint8 start + uint8 end
	storeOwnedDocTemplateLength8     = uint8(6) // uint16 start + uint8 length
	storeOwnedDocNarrowLength8       = uint8(7) // uint16 start + uint8 length + info
	storeOwnedDocNarrow8             = uint8(8) // uint8 start + uint8 end + info
	storeOwnedDocNarrow9             = uint8(9) // 9-bit start + uint8 length + 7-bit info
	storeOwnedNarrowValueLen         = 5
	storeOwnedNarrowLength8ValueLen  = 4
	storeOwnedNarrow8ValueLen        = 3
	storeOwnedNarrow9ValueLen        = 3
	storeOwnedTemplateSpanLen        = 4
	storeOwnedTemplate8SpanLen       = 2
	storeOwnedTemplateLength8SpanLen = 3
)

func storeOwnedDocIsTemplate(kind uint8) bool {
	return kind == storeOwnedDocTemplate || kind == storeOwnedDocTemplate8 || kind == storeOwnedDocTemplateLength8
}

func storeOwnedDocIsNarrow(kind uint8) bool {
	return kind == storeOwnedDocNarrow || kind == storeOwnedDocNarrowLength8 ||
		kind == storeOwnedDocNarrow8 || kind == storeOwnedDocNarrow9
}

func storeOwnedTemplateSpanWidth(kind uint8) uint64 {
	switch kind {
	case storeOwnedDocTemplate8:
		return storeOwnedTemplate8SpanLen
	case storeOwnedDocTemplateLength8:
		return storeOwnedTemplateLength8SpanLen
	default:
		return storeOwnedTemplateSpanLen
	}
}

func storeMappedTapeOffset(sourceEnd uint64, kind uint8) uint64 {
	if storeOwnedDocIsNarrow(kind) || storeOwnedDocIsTemplate(kind) {
		return (sourceEnd + 1) &^ 1
	}
	return persistAlign8(sourceEnd)
}

// The layout is an on-host control-plane ABI. Keep it compact and pointer-free:
// one descriptor per row must not become one GC-scanned object per row.
const storeMappedDocRefBytes = 32
const storeOwnedDocRefBytes = 24
const storeCompactDocRefBytes = 16

var _ [storeMappedDocRefBytes - unsafe.Sizeof(storeMappedDocRef{})]byte
var _ [unsafe.Sizeof(storeMappedDocRef{}) - storeMappedDocRefBytes]byte
var _ [storeOwnedDocRefBytes - unsafe.Sizeof(storeOwnedDocRef{})]byte
var _ [unsafe.Sizeof(storeOwnedDocRef{}) - storeOwnedDocRefBytes]byte
var _ [storeCompactDocRefBytes - unsafe.Sizeof(storeCompactDocRef{})]byte
var _ [unsafe.Sizeof(storeCompactDocRef{}) - storeCompactDocRefBytes]byte

const storeDocRefSourceLengthOffset = unsafe.Offsetof(storeMappedDocRef{}.srcLen)

// rawAt reads the common descriptor prefix directly. Make layout drift a
// compile error in both directions rather than a latent source-span bug.
var _ [storeDocRefSourceLengthOffset - unsafe.Offsetof(storeOwnedDocRef{}.srcLen)]byte
var _ [unsafe.Offsetof(storeOwnedDocRef{}.srcLen) - storeDocRefSourceLengthOffset]byte
var _ [storeDocRefSourceLengthOffset - unsafe.Offsetof(storeCompactDocRef{}.srcLen)]byte
var _ [unsafe.Offsetof(storeCompactDocRef{}.srcLen) - storeDocRefSourceLengthOffset]byte

// storeMappedDocs owns every row descriptor in one pointer-free anonymous
// region. Open borrows source/tape bytes from its caller-owned image;
// Builder attaches a second owned block. Chunks retain only this owner
// and a base ordinal, so Go pointer count is bounded by chunks and distinct
// shapes rather than documents.
type storeMappedDocs struct {
	refs        []storeMappedDocRef
	ownedRefs   []storeOwnedDocRef
	compactRefs []storeCompactDocRef
	// refData and refStride give source-only reads one common descriptor
	// calculation. Every layout begins with the exact uint64 source offset and
	// uint32 length, keeping rawAt below the compiler's inline budget without
	// a representation branch.
	refData     unsafe.Pointer
	refStride   uintptr
	block       *storemem.Block
	sourceBlock *storemem.Block
	templates   []*DocumentTemplate
	shapes      []*ShapeRecord
}

func newStoreMappedDocs(count int) (*storeMappedDocs, error) {
	size := int(unsafe.Sizeof(storeMappedDocRef{}))
	if count < 0 || count > math.MaxInt/size {
		return nil, ErrCheckpointTooLarge
	}
	block, err := storemem.Allocate(count * size)
	if err != nil {
		return nil, err
	}
	var refs []storeMappedDocRef
	if count != 0 {
		refs = unsafe.Slice((*storeMappedDocRef)(unsafe.Pointer(unsafe.SliceData(block.Bytes()))), count)
	}
	m := &storeMappedDocs{
		refs: refs, refData: unsafe.Pointer(unsafe.SliceData(block.Bytes())),
		refStride: unsafe.Sizeof(storeMappedDocRef{}), block: block,
	}
	runtime.SetFinalizer(m, (*storeMappedDocs).release)
	return m, nil
}

func (m *storeMappedDocs) release() {
	if m == nil {
		return
	}
	if m.block != nil {
		_ = m.block.Close()
	}
	if m.sourceBlock != nil {
		_ = m.sourceBlock.Close()
	}
	m.block = nil
	m.sourceBlock = nil
	m.refs = nil
	m.ownedRefs = nil
	m.compactRefs = nil
	m.refData = nil
	m.refStride = 0
	m.templates = nil
	m.shapes = nil
}

func (m *storeMappedDocs) refAt(index uint64) storeMappedDocRef {
	if m.compactRefs != nil {
		r := m.compactRefs[index]
		shapeID, entryCount := storeMappedNoShape, uint32(r.meta)
		switch {
		case storeOwnedDocIsTemplate(r.kind):
			shapeID = uint32(r.meta)
			if int(shapeID) < len(m.templates) {
				entryCount = uint32(len(m.templates[shapeID].Index.Entries))
			}
		case r.kind != persistDocClassic:
			shapeID = uint32(r.meta)
			if int(shapeID) < len(m.shapes) {
				entryCount = uint32(len(m.shapes[shapeID].Fields))
			}
		}
		return storeMappedDocRef{
			sourceOff: r.sourceOff, srcLen: r.srcLen, entryCount: entryCount,
			shapeID: shapeID,
			kind:    r.kind, enriched: r.flags&1 != 0,
		}
	}
	if m.ownedRefs == nil {
		return m.refs[index]
	}
	r := m.ownedRefs[index]
	shapeID := uint32(r.shapeID)
	if r.shapeID == storeOwnedNoShape {
		shapeID = storeMappedNoShape
	}
	return storeMappedDocRef{
		sourceOff: r.sourceOff, srcLen: r.srcLen, entryCount: r.entryCount,
		start: uint32(r.start), end: uint32(r.end), shapeID: shapeID,
		kind: r.kind, enriched: r.flags&1 != 0,
	}
}

func (m *storeMappedDocs) setRef(index uint64, r storeMappedDocRef) {
	if m.compactRefs != nil {
		meta := uint16(r.entryCount)
		if r.kind != persistDocClassic {
			meta = uint16(r.shapeID)
		}
		var flags uint8
		if r.enriched {
			flags = 1
		}
		m.compactRefs[index] = storeCompactDocRef{
			sourceOff: r.sourceOff, srcLen: r.srcLen, meta: meta,
			kind: r.kind, flags: flags,
		}
		return
	}
	if m.ownedRefs == nil {
		m.refs[index] = r
		return
	}
	shapeID := uint16(r.shapeID)
	if r.shapeID == storeMappedNoShape {
		shapeID = storeOwnedNoShape
	}
	var flags uint8
	if r.enriched {
		flags = 1
	}
	m.ownedRefs[index] = storeOwnedDocRef{
		sourceOff: r.sourceOff, srcLen: r.srcLen, entryCount: r.entryCount,
		shapeID: shapeID, start: uint16(r.start), end: uint16(r.end),
		kind: r.kind, flags: flags,
	}
}

func (m *storeMappedDocs) externalBytes() uint64 {
	if m == nil {
		return 0
	}
	var bytes uint64
	if m.block != nil && m.block.OutsideHeap() {
		bytes += uint64(m.block.Len())
	}
	if m.sourceBlock != nil && m.sourceBlock.OutsideHeap() {
		bytes += uint64(m.sourceBlock.Len())
	}
	return bytes
}

func (s *Segment) DocAt(i int) vibejson.Index {
	if s.mappedDocs == nil {
		return s.docs[i]
	}
	ref := s.mappedBase + uint64(i)
	var srcOff uint64
	var srcLen, count uint32
	var kind uint8
	if s.mappedDocs.compactRefs != nil {
		r := &s.mappedDocs.compactRefs[ref]
		srcOff, srcLen, kind = r.sourceOff, r.srcLen, r.kind
		switch {
		case kind == persistDocClassic:
			count = uint32(r.meta)
		case !storeOwnedDocIsTemplate(kind) && int(r.meta) < len(s.mappedShapes):
			count = uint32(len(s.mappedShapes[r.meta].Fields))
		}
	} else if s.mappedDocs.ownedRefs != nil {
		r := &s.mappedDocs.ownedRefs[ref]
		srcOff, srcLen, count, kind = r.sourceOff, r.srcLen, r.entryCount, r.kind
	} else {
		r := &s.mappedDocs.refs[ref]
		srcOff, srcLen, count, kind = r.sourceOff, r.srcLen, r.entryCount, r.kind
	}
	srcEnd := srcOff + uint64(srcLen)
	entriesOff := storeMappedTapeOffset(srcEnd, kind)
	src := s.source[srcOff:srcEnd:srcEnd]
	if kind == persistDocNarrow || storeOwnedDocIsNarrow(kind) || storeOwnedDocIsTemplate(kind) {
		runtime.KeepAlive(s.mappedDocs)
		return vibejson.Index{Src: src}
	}
	index := vibejson.Index{Src: src, Entries: openEntries(s.source, entriesOff, uint64(count))}
	runtime.KeepAlive(s.mappedDocs)
	return index
}

// RawAt is the point-read form of docAt. It reconstructs only the source view,
// avoiding entry-tape work on Collection.GetRaw and compiled key lookups.
func (s *Segment) RawAt(i int) []byte {
	if s.mappedDocs == nil {
		return s.docs[i].Src
	}
	m := s.mappedDocs
	ref := unsafe.Add(m.refData, uintptr(s.mappedBase+uint64(i))*m.refStride)
	start := *(*uint64)(ref)
	length := *(*uint32)(unsafe.Add(ref, storeDocRefSourceLengthOffset))
	end := start + uint64(length)
	raw := s.source[start:end:end]
	runtime.KeepAlive(m)
	return raw
}

// storeRootSpan recovers the parser's root coordinates from already-validated
// source. Compact rows omit these cold coordinates: field scans use stored
// child spans, while navigation and checkpoint expansion pay this bounded
// leading/trailing whitespace scan only when they need the root entry.
func storeRootSpan(src []byte) (uint32, uint32) {
	start, end := 0, len(src)
	for start < end && vibejson.IsJSONWhitespace(src[start]) {
		start++
	}
	for end > start && vibejson.IsJSONWhitespace(src[end-1]) {
		end--
	}
	return uint32(start), uint32(end)
}

func (s *Segment) NarrowAt(i int, ref ShapeTapeRef, ordinal int) ShapeNarrowValue {
	if s.mappedDocs == nil {
		return s.narrow[int(ref.off)+ordinal]
	}
	index := s.mappedBase + uint64(i)
	var srcOff uint64
	var srcLen uint32
	var kind uint8
	if s.mappedDocs.compactRefs != nil {
		r := &s.mappedDocs.compactRefs[index]
		srcOff, srcLen, kind = r.sourceOff, r.srcLen, r.kind
	} else if s.mappedDocs.ownedRefs != nil {
		r := &s.mappedDocs.ownedRefs[index]
		srcOff, srcLen, kind = r.sourceOff, r.srcLen, r.kind
	} else {
		r := &s.mappedDocs.refs[index]
		srcOff, srcLen, kind = r.sourceOff, r.srcLen, r.kind
	}
	srcEnd := srcOff + uint64(srcLen)
	off := storeMappedTapeOffset(srcEnd, kind)
	var value ShapeNarrowValue
	switch kind {
	case storeOwnedDocNarrow8:
		off += uint64(ordinal) * storeOwnedNarrow8ValueLen
		value.Span = uint32(s.source[off]) | uint32(s.source[off+1])<<16
		value.Info = uint32(s.source[off+2]) << vibejson.InfoKindShift
	case storeOwnedDocNarrow9:
		off += uint64(ordinal) * storeOwnedNarrow9ValueLen
		packed := uint32(s.source[off]) | uint32(s.source[off+1])<<8 | uint32(s.source[off+2])<<16
		start := packed & 0x1ff
		value.Span = start | (start+((packed>>9)&0xff))<<16
		value.Info = packed >> 17 << vibejson.InfoKindShift
	case storeOwnedDocNarrowLength8:
		off += uint64(ordinal) * storeOwnedNarrowLength8ValueLen
		start := uint32(binary.LittleEndian.Uint16(s.source[off : off+2]))
		value.Span = start | (start+uint32(s.source[off+2]))<<16
		value.Info = uint32(s.source[off+3]) << vibejson.InfoKindShift
	case storeOwnedDocNarrow:
		off += uint64(ordinal) * storeOwnedNarrowValueLen
		value.Span = binary.LittleEndian.Uint32(s.source[off : off+4])
		value.Info = uint32(s.source[off+4]) << vibejson.InfoKindShift
	default:
		off += uint64(ordinal) * uint64(unsafe.Sizeof(ShapeNarrowValue{}))
		value = *(*ShapeNarrowValue)(unsafe.Pointer(&s.source[off]))
	}
	runtime.KeepAlive(s.mappedDocs)
	return value
}

func (s *Segment) narrowLen() int {
	if s.mappedDocs != nil {
		return s.mappedNarrow
	}
	return len(s.narrow)
}
