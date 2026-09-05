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
	Scanned       int
	Matched       int
	NativeMatched int
	Supported     bool
	Stopped       bool
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
	fieldCount := 0
	if f != nil {
		fieldCount = f.FieldCount()
	}
	return s.FilterProjectedRangeWithMatchScratch(
		f, fieldCount, lower, upper, lowerExclusive,
		shapeSeen, shapeWork, streamWork, fields, valueScratch, nil, nil, limit,
		nil, visit, nil,
	)
}

// FilterProjectedRangeWithMatchScratch is the late-materializing form of
// FilterProjectedRangeWithScratch. The first filterCount fields are decoded
// for every row and passed to match. Remaining fields are decoded only after a
// match, so filter-only paths never consume late output work. If a compact row
// cannot answer the selected paths, fallback receives its inline JSON or an
// overflow value reassembled for this snapshot; the cursor has already retained
// the row position and continues with the next row after the callback returns.
func (s *Snapshot) FilterProjectedRangeWithMatchScratch(
	f *ProjectionFilter,
	filterCount int,
	lower, upper []byte,
	lowerExclusive bool,
	shapeSeen []int,
	shapeWork []storeio.UnifiedProjectionShapeWorkspace,
	streamWork []storeio.UnifiedProjectionStreamWorkspace,
	fields []storeio.UnifiedProjectionField,
	valueScratch []byte,
	fallbackScratch *[]byte,
	fallbackReserve func(required int64) ([]byte, error),
	limit int,
	match func(row uint64, fields []storeio.UnifiedProjectionField) (bool, error),
	visit func(row uint64, fields []storeio.UnifiedProjectionField) error,
	fallback func(row uint64, raw []byte) (bool, error),
) (result ProjectedRangeResult, scratch []byte, err error) {
	if s == nil || s.collection == nil || s.state == nil {
		return ProjectedRangeResult{}, valueScratch, ErrClosed
	}
	if f == nil || f.inner == nil || f.FieldCount() == 0 || visit == nil ||
		filterCount < 0 || filterCount > f.FieldCount() {
		return ProjectedRangeResult{}, valueScratch,
			fmt.Errorf("vibedb: invalid primary projection match filter")
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
	var fallbackBuffer []byte
	if fallbackScratch != nil {
		fallbackBuffer = *fallbackScratch
	}
	cursor.AdoptSpliceScratch(fallbackBuffer)
	defer func() {
		if fallbackScratch != nil {
			buffer := cursor.ReleaseSpliceScratch()
			*fallbackScratch = buffer[:0]
		}
		cursor.Close()
	}()
	fieldCount := f.FieldCount()
	if cap(fields) < fieldCount || fieldCount == 0 {
		return ProjectedRangeResult{}, valueScratch, nil
	}
	shapeCap := min(storeio.UnifiedProjectionMaxShapes, min(cap(shapeSeen), cap(shapeWork)))
	if streamShapes := cap(streamWork) / fieldCount; streamShapes < shapeCap {
		shapeCap = streamShapes
	}
	if shapeCap <= 0 {
		return ProjectedRangeResult{}, valueScratch, nil
	}
	shapeSeen = shapeSeen[:shapeCap]
	shapeWork = shapeWork[:shapeCap]
	streamWork = streamWork[:shapeCap*fieldCount]
	fields = fields[:fieldCount]
	defer func() {
		clear(shapeSeen)
		clear(shapeWork)
		clear(streamWork)
		clear(fields)
	}()
	var progress storeio.UnifiedProjectionProgress
	var reserve func(required int) ([]byte, error)
	if fallbackReserve != nil {
		reserve = func(required int) ([]byte, error) {
			if required < 0 {
				return nil, storeio.ErrUnifiedProjectionFallbackUnsupported
			}
			buffer, reserveErr := fallbackReserve(int64(required))
			if reserveErr == nil && fallbackScratch != nil {
				*fallbackScratch = buffer[:0]
			}
			return buffer, reserveErr
		}
	}
	var wrappedFallback func(uint64, []byte, storeio.PageRef) (bool, error)
	if fallback != nil {
		wrappedFallback = func(row uint64, raw []byte, overflow storeio.PageRef) (bool, error) {
			if overflow != (storeio.PageRef{}) {
				var buffer []byte
				if fallbackScratch != nil {
					buffer = *fallbackScratch
				}
				var overflowReserve func(int) ([]byte, error)
				if fallbackReserve != nil {
					overflowReserve = reserve
				} else {
					// Let the unified decoder use an already-admitted buffer, but
					// decline before it allocates when the legacy API supplied no
					// admission callback.
					overflowReserve = func(int) ([]byte, error) {
						return nil, storeio.ErrUnifiedProjectionFallbackUnsupported
					}
				}
				resolved, bounded, overflowErr := s.collection.appendPrimaryOverflowValueWithReserve(
					buffer[:0], overflow, leafBounds, overflowReserve,
				)
				if overflowErr != nil {
					return false, overflowErr
				}
				if !bounded {
					return false, storeio.ErrUnifiedProjectionFallbackUnsupported
				}
				if fallbackScratch != nil {
					*fallbackScratch = resolved[:0]
				}
				// An overflow row may have admitted a larger buffer than the
				// cursor started with. Adopt it so a later inline fallback and
				// ReleaseSpliceScratch observe the same retained capacity.
				cursor.AdoptSpliceScratch(resolved[:0])
				raw = resolved
			}
			return fallback(row, raw)
		}
	}
	supported, stopped, valueScratch, err := cursor.VisitProjectedMatchWithReserve(
		f.inner, &progress, filterCount, shapeSeen, shapeWork, streamWork,
		fields, valueScratch, limit, match,
		func(row uint64, values []storeio.UnifiedProjectionField) error {
			return visit(row, values)
		}, wrappedFallback, reserve,
	)
	if err != nil {
		return ProjectedRangeResult{}, valueScratch, err
	}
	if !supported {
		return ProjectedRangeResult{}, valueScratch, nil
	}
	return ProjectedRangeResult{
		Scanned: progress.Scanned, Matched: progress.Matched,
		NativeMatched: progress.NativeMatched,
		Supported:     true, Stopped: stopped,
	}, valueScratch, nil
}
