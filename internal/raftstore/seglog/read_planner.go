package seglog

import "math"

const MaxPlannedExtents = 256

// PhysicalExtent is one independently authenticated contiguous batch blob.
// First/Last are inclusive group-local Raft indexes.
type PhysicalExtent struct {
	SegmentID, Offset, Bytes uint64
	First, Last              uint64
}

type ReadSpan struct {
	SegmentID, Offset, Bytes uint64
	FirstExtent, Extents     uint16
}

// ReadPlanWorkspace is caller-owned fixed routing scratch. It charges all
// scatter/coalescing state independently of retained entry count.
type ReadPlanWorkspace struct {
	spans [MaxPlannedExtents]ReadSpan
}

// Plan coalesces physically ordered extents without crossing segments. The
// sum of bytes read between live extents never exceeds maxExtraBytes.
func (w *ReadPlanWorkspace) Plan(extents []PhysicalExtent, maxExtraBytes uint64) ([]ReadSpan, uint64, error) {
	spans, extra, next, err := w.PlanPage(extents, maxExtraBytes, 0)
	if err == nil && next != len(extents) {
		return nil, 0, ErrBounds
	}
	return spans, extra, err
}

// PlanPage bounds scratch for arbitrarily long catch-up reads. next is the
// first unplanned extent and lets the caller submit this page before continuing.
func (w *ReadPlanWorkspace) PlanPage(extents []PhysicalExtent, maxExtraBytes uint64, start int) ([]ReadSpan, uint64, int, error) {
	if start < 0 || start > len(extents) {
		return nil, 0, start, ErrBounds
	}
	allExtents := extents
	extents = extents[start:]
	if len(extents) == 0 {
		return w.spans[:0], 0, start, nil
	}
	if start != 0 {
		previous, extent := allExtents[start-1], extents[0]
		if previous.Last == math.MaxUint64 || extent.First != previous.Last+1 || previous.SegmentID == extent.SegmentID && (previous.Offset > math.MaxUint64-previous.Bytes || extent.Offset < previous.Offset+previous.Bytes) {
			return nil, 0, start, ErrCorrupt
		}
	}
	spans := w.spans[:0]
	extra := uint64(0)
	for i, extent := range extents {
		if i == MaxPlannedExtents {
			return spans, extra, start + i, nil
		}
		if len(spans) == len(w.spans) {
			return spans, extra, start + i, nil
		}
		if extent.SegmentID == 0 || extent.Bytes == 0 || extent.First == 0 || extent.Last < extent.First || extent.Offset > math.MaxUint64-extent.Bytes {
			return nil, 0, start + i, ErrCorrupt
		}
		if i != 0 {
			previous := extents[i-1]
			if previous.Last == math.MaxUint64 || extent.First != previous.Last+1 {
				return nil, 0, start + i, ErrCorrupt
			}
			if previous.SegmentID == extent.SegmentID && extent.Offset < previous.Offset+previous.Bytes {
				return nil, 0, start + i, ErrCorrupt
			}
		}
		if len(spans) != 0 {
			last := &spans[len(spans)-1]
			end := last.Offset + last.Bytes
			if last.SegmentID == extent.SegmentID && extent.Offset >= end {
				gap := extent.Offset - end
				if extra <= maxExtraBytes && gap <= maxExtraBytes-extra && gap <= math.MaxUint64-extent.Bytes && last.Bytes <= math.MaxUint64-gap-extent.Bytes {
					last.Bytes += gap + extent.Bytes
					last.Extents++
					extra += gap
					continue
				}
			}
		}
		spans = append(spans, ReadSpan{SegmentID: extent.SegmentID, Offset: extent.Offset, Bytes: extent.Bytes, FirstExtent: uint16(i), Extents: 1})
	}
	return spans, extra, len(extents) + start, nil
}
