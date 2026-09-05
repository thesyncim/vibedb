package durable

import (
	"bytes"
	"fmt"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// ProjectionFilter is reusable state for direct scalar paths in a compact
// primary stripe. It is intentionally separate from Snapshot so a prepared
// query can warm its path resolvers across snapshot generations.
type ProjectionFilter struct {
	inner *storeio.UnifiedProjectionFilter
	paths []string
}

// ProjectedRangeResult reports one strict projected range scan. Supported is
// false when the complete prefix needed by the caller cannot be answered from
// compact scalar fields; Scanned is then zero and any callback progress must
// be discarded before falling back to the same snapshot range.
type ProjectedRangeResult struct {
	Scanned   int
	Supported bool
	Stopped   bool
}

// NewProjectionFilter builds a reusable direct scalar projection filter. Paths
// use the query/compiler's canonical RFC 6901 spelling.
func NewProjectionFilter(paths []string) (*ProjectionFilter, error) {
	if len(paths) == 0 {
		return nil, fmt.Errorf("vibedb: empty primary projection")
	}
	encoded := make([][]byte, len(paths))
	for i := range paths {
		encoded[i] = []byte(paths[i])
	}
	inner, err := storeio.NewUnifiedProjectionFilter(encoded)
	if err != nil {
		return nil, err
	}
	return &ProjectionFilter{inner: inner, paths: append([]string(nil), paths...)}, nil
}

// FieldCount reports the number of scalar values the callback receives.
func (f *ProjectionFilter) FieldCount() int {
	if f == nil || f.inner == nil {
		return 0
	}
	return len(f.paths)
}

// FilterProjectedRangeWithScratch scans a snapshot-rooted primary range and
// decodes only the selected scalar fields. Borrowed callback values are valid
// until visit returns. All shape and stream views are cleared before return so
// caller-owned capacity never retains page-cache frames.
func (s *Snapshot) FilterProjectedRangeWithScratch(
	f *ProjectionFilter,
	lower, upper []byte,
	lowerExclusive bool,
	shapeSeen []int,
	shapeWork []storeio.UnifiedProjectionShapeWorkspace,
	streamWork []storeio.UnifiedProjectionStreamWorkspace,
	fields []storeio.UnifiedProjectionField,
	valueScratch []byte,
	limit int,
	visit func(row uint64, fields []storeio.UnifiedProjectionField) error,
) (result ProjectedRangeResult, scratch []byte, err error) {
	if s == nil || s.collection == nil || s.state == nil {
		return ProjectedRangeResult{}, valueScratch, ErrClosed
	}
	if f == nil || f.inner == nil || f.FieldCount() == 0 || visit == nil {
		return ProjectedRangeResult{}, valueScratch,
			fmt.Errorf("vibedb: nil primary projection filter")
	}
	if len(lower) != 0 && len(upper) != 0 && bytes.Compare(lower, upper) >= 0 {
		return ProjectedRangeResult{Supported: true}, valueScratch, nil
	}
	if limit == 0 {
		return ProjectedRangeResult{Supported: true, Stopped: true}, valueScratch, nil
	}
	if lowerExclusive && len(lower) != 0 {
		if len(lower) > storeio.CommonPrimaryLeafMaxKeyBytes || len(lower) >= cap(valueScratch) {
			return ProjectedRangeResult{}, valueScratch, nil
		}
		// Reserve the fence at the end of caller-owned storage for the cursor's
		// lifetime. Restore the full arena capacity on every return path.
		original := valueScratch
		defer func() { scratch = original[:len(scratch)] }()
		fenceStart := cap(valueScratch) - len(lower) - 1
		fence := valueScratch[fenceStart:cap(valueScratch)]
		copy(fence, lower)
		fence[len(lower)] = 0
		lower = fence
		valueScratch = valueScratch[:0:fenceStart]
	}
	state := s.state
	catalogBounds := storeio.GlobalTabletCatalogBounds{
		StoreID:                state.root.StoreID,
		SelectedRootGeneration: state.root.Generation,
		FileEnd:                state.fileEnd,
		NextLogicalID:          state.root.NextLogicalID,
	}
	leafBounds := storeio.CommonPrimaryLeafBounds{
		FileEnd:           state.fileEnd,
		NextLogicalID:     state.root.NextLogicalID,
		AllocationQuantum: state.root.PageSize,
	}
	var cursor storeio.PrimaryGraphCursor
	if err := storeio.InitPrimaryGraphCursor(
		&cursor, s.collection.cache, state.root.PrimaryRoot,
		catalogBounds, leafBounds, lower, upper,
	); err != nil {
		return ProjectedRangeResult{}, valueScratch, err
	}
	defer cursor.Close()
	const maxShapes = storeio.UnifiedProjectionMaxShapes
	fieldCount := f.FieldCount()
	if cap(fields) < fieldCount || fieldCount == 0 {
		// Projection scratch is explicitly caller-owned. A declined attempt
		// must not hide an unbounded durable allocation behind this wrapper;
		// the query layer will rerun the same snapshot range generically.
		return ProjectedRangeResult{}, valueScratch, nil
	}
	shapeCap := min(maxShapes, cap(shapeSeen), cap(shapeWork))
	if streamShapes := cap(streamWork) / fieldCount; streamShapes < shapeCap {
		shapeCap = streamShapes
	}
	if shapeCap <= 0 {
		return ProjectedRangeResult{}, valueScratch, nil
	}
	shapeSeen = shapeSeen[:shapeCap]
	shapeWork = shapeWork[:shapeCap]
	streamCount := shapeCap * fieldCount
	streamWork = streamWork[:streamCount]
	fields = fields[:fieldCount]
	defer func() {
		clear(shapeSeen)
		clear(shapeWork)
		clear(streamWork)
		clear(fields)
	}()
	var progress storeio.UnifiedProjectionProgress
	supported, stopped, valueScratch, err := cursor.VisitProjected(
		f.inner, &progress, shapeSeen, shapeWork, streamWork, fields,
		valueScratch, limit,
		func(row uint64, values []storeio.UnifiedProjectionField) error {
			return visit(row, values)
		},
	)
	if err != nil {
		return ProjectedRangeResult{}, valueScratch, err
	}
	if !supported {
		return ProjectedRangeResult{}, valueScratch, nil
	}
	return ProjectedRangeResult{
		Scanned: progress.Scanned, Supported: true, Stopped: stopped,
	}, valueScratch, nil
}
