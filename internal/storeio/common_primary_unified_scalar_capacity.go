package storeio

import (
	"unsafe"
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
