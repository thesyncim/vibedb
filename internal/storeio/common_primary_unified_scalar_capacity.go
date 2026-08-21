package storeio

import (
	"unsafe"

	"github.com/thesyncim/vibejson"
)

// NewUnifiedPrimaryScalarPatchBuilder returns a builder whose complete strict
// scalar-patch working set is touched and fixed at construction. The strict
// planner never builds a JSON tape, canonical workspace, dictionary census, or
// generic shape plan, so only its encoded-body heap and one row descriptor per
// stable slot need backing storage.
func NewUnifiedPrimaryScalarPatchBuilder() *UnifiedPrimaryLeafBuilder {
	return &UnifiedPrimaryLeafBuilder{
		heap: make([]byte, 0, CommonPrimaryLeafMaxExtentBytes),
		rows: make([]unifiedPrimaryLeafRow, 0, CommonPrimaryLeafWideSlots),
	}
}

// ScalarPatchCapacityBytes reports every retained byte reachable from a
// builder used exclusively by PatchPlanScalarReplacements. That strict path
// touches only heap and rows. In particular it cannot initialize the builder's
// maps, tape, canonical workspace, spans, shapes, dictionary, or generic
// planner slices.
func (b *UnifiedPrimaryLeafBuilder) ScalarPatchCapacityBytes() uint64 {
	if b == nil {
		return 0
	}
	return uint64(unsafe.Sizeof(*b)) +
		uint64(cap(b.heap)) +
		uint64(cap(b.rows))*uint64(unsafe.Sizeof(unifiedPrimaryLeafRow{}))
}

// NewCompactPrimaryPatchBuilder returns an initially modest builder for the
// compact column-patch lane. Unlike the retired scalar-body patcher, this lane
// parses replacement JSON and replans complete scalar streams, so it must use
// the general builder rather than claiming the strict two-slice working set.
func NewCompactPrimaryPatchBuilder() *UnifiedPrimaryLeafBuilder {
	return NewUnifiedPrimaryLeafBuilder()
}

// CompactPatchCapacityBytes reports the retained backing storage reachable
// from a builder after compact column patching. Borrowed records, replacement
// values, and stream dictionary spellings are represented by slice descriptors
// below but their external byte storage is deliberately not double-counted.
func (b *UnifiedPrimaryLeafBuilder) CompactPatchCapacityBytes() uint64 {
	if b == nil {
		return 0
	}
	bytes := uint64(unsafe.Sizeof(*b)) + b.ws.CapacityBytes()
	bytes += uint64(cap(b.indexStore)) * uint64(unsafe.Sizeof(vibejson.IndexEntry{}))
	bytes += uint64(cap(b.heap))
	bytes += uint64(cap(b.spans)) * uint64(unsafe.Sizeof(UnifiedTokenSpan{}))
	bytes += uint64(cap(b.rows)) * uint64(unsafe.Sizeof(unifiedPrimaryLeafRow{}))
	bytes += uint64(cap(b.shapes)) * uint64(unsafe.Sizeof(unifiedPrimaryLeafShape{}))
	bytes += uint64(cap(b.shapeRows)) * uint64(unsafe.Sizeof(int32(0)))
	bytes += uint64(cap(b.shapeSavings)) * uint64(unsafe.Sizeof(int64(0)))
	bytes += uint64(cap(b.dictionary)) * uint64(unsafe.Sizeof(unifiedDictionaryCandidate{}))
	bytes += uint64(cap(b.patchValues)) * uint64(unsafe.Sizeof(unifiedPrimaryPatchValueDelta{}))
	bytes += uint64(cap(b.compactSummaryPointers)) *
		uint64(unsafe.Sizeof(vibejson.CompiledPointer{}))
	bytes += uint64(cap(b.compactSummaryMin)+cap(b.compactSummaryMax)) *
		uint64(unsafe.Sizeof([]byte{}))
	for i := range b.compactSummaryMin {
		bytes += uint64(cap(b.compactSummaryMin[i]))
	}
	for i := range b.compactSummaryMax {
		bytes += uint64(cap(b.compactSummaryMax[i]))
	}
	bytes += uint64(cap(b.compactSummaryValid)) * uint64(unsafe.Sizeof(bool(false)))
	bytes += uint64(cap(b.compactSummaryProbe))
	bytes += b.compact.capacityBytes()
	return bytes
}

func (s *compactPrimaryBuildScratch) capacityBytes() uint64 {
	if s == nil {
		return 0
	}
	bytes := uint64(cap(s.payload)) + uint64(cap(s.shapeCodes)) +
		uint64(cap(s.overflow)) + uint64(cap(s.patchHeap)) +
		uint64(cap(s.patchStreams))
	bytes += uint64(cap(s.shapeOrder)) * uint64(unsafe.Sizeof(uint16(0)))
	bytes += uint64(cap(s.shapeEnds)) * uint64(unsafe.Sizeof(uint16(0)))
	bytes += uint64(cap(s.counts)) * uint64(unsafe.Sizeof(uint16(0)))
	bytes += uint64(cap(s.streamValues)) * uint64(unsafe.Sizeof([]byte{}))
	bytes += uint64(cap(s.patchEnds)) * uint64(unsafe.Sizeof(uint32(0)))
	bytes += uint64(cap(s.patchValues)) * uint64(unsafe.Sizeof([]byte{}))
	bytes += uint64(cap(s.patchMods)) *
		uint64(unsafe.Sizeof(compactPrimaryReplacementPatch{}))
	bytes += uint64(cap(s.patchGroups)) *
		uint64(unsafe.Sizeof(compactPrimaryStreamPatch{}))
	bytes += uint64(cap(s.shapeDeltas)) * uint64(unsafe.Sizeof(int(0)))
	bytes += s.stream.capacityBytes()
	return bytes
}

func (s *compactStreamScratch) capacityBytes() uint64 {
	if s == nil {
		return 0
	}
	var bytes uint64
	for index := range s.data {
		bytes += uint64(cap(s.data[index]))
		bytes += uint64(cap(s.dict[index])) * uint64(unsafe.Sizeof([]byte{}))
		bytes += uint64(cap(s.alphabet[index]))
	}
	bytes += uint64(cap(s.integers)) * uint64(unsafe.Sizeof(int64(0)))
	bytes += uint64(cap(s.dates)) * uint64(unsafe.Sizeof(int32(0)))
	bytes += uint64(cap(s.parsed)) * uint64(unsafe.Sizeof(uint64(0)))
	bytes += uint64(cap(s.dictionaryTable)) * uint64(unsafe.Sizeof(uint32(0)))
	bytes += uint64(cap(s.dictionaryStamp)) * uint64(unsafe.Sizeof(uint32(0)))
	return bytes
}
