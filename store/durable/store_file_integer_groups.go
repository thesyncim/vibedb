package durable

import (
	"fmt"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// IntegerGroupFilter is reusable state for one strict compact integer GROUP
// BY scan. A nil sum path selects COUNT(*) groups; otherwise the group and sum
// paths must both resolve to exact compact integer streams in every present
// row. Wide FOR streams are the primary representation, while compact
// dictionary/alphabet integer forms cover low-cardinality columns.
type IntegerGroupFilter struct {
	inner *storeio.UnifiedIntegerGroupFilter
}

// FilterIntegerGroupsResult reports one strict grouped scan. Supported is false
// when the complete snapshot cannot be answered from compact integer FOR data;
// Scanned is then deliberately zero and any callback progress must be dropped.
type FilterIntegerGroupsResult struct {
	Scanned   int
	Supported bool
}

// NewIntegerGroupFilter builds a strict integer GROUP BY filter. sumPath may
// be empty for COUNT(*) groups.
func NewIntegerGroupFilter(groupPath, sumPath string) (*IntegerGroupFilter, error) {
	var sum []byte
	if sumPath != "" {
		sum = []byte(sumPath)
	}
	inner, err := storeio.NewUnifiedIntegerGroupFilter(
		[]byte(groupPath), sum,
	)
	if err != nil {
		return nil, err
	}
	return &IntegerGroupFilter{inner: inner}, nil
}

// FilterIntegerGroups scans the snapshot selected by this durable Snapshot.
// The callback is invoked in snapshot physical order with the row ordinal,
// group key, and optional SUM value. The snapshot's captured state root and
// generation are used throughout; the live collection/router is never read.
func (s *Snapshot) FilterIntegerGroups(
	f *IntegerGroupFilter,
	visit func(row uint64, group, sum int64) error,
) (FilterIntegerGroupsResult, error) {
	return s.FilterIntegerGroupsWithScratch(f, nil, nil, visit)
}

// FilterIntegerGroupsWithScratch is FilterIntegerGroups with caller-owned
// per-shape scratch. The storage view contains slices into page-cache frames,
// so every view is cleared before this method returns; callers may retain the
// backing arrays for a warm execution without pinning an old snapshot page.
// Passing nil scratch is allowed and allocates one bounded stripe-sized set.
func (s *Snapshot) FilterIntegerGroupsWithScratch(
	f *IntegerGroupFilter,
	shapeSeen []int,
	shapeWork []storeio.IntegerGroupShapeWorkspace,
	visit func(row uint64, group, sum int64) error,
) (FilterIntegerGroupsResult, error) {
	if s == nil || s.collection == nil || s.state == nil {
		return FilterIntegerGroupsResult{}, ErrClosed
	}
	if f == nil || f.inner == nil || visit == nil {
		return FilterIntegerGroupsResult{}, fmt.Errorf("vibedb: nil unified integer group filter")
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
		catalogBounds, leafBounds, nil, nil,
	); err != nil {
		return FilterIntegerGroupsResult{}, err
	}
	defer cursor.Close()
	if cap(shapeSeen) < storeio.CompactPrimaryStripeMaxRows {
		shapeSeen = make([]int, storeio.CompactPrimaryStripeMaxRows)
	}
	shapeSeen = shapeSeen[:storeio.CompactPrimaryStripeMaxRows]
	if cap(shapeWork) < storeio.CompactPrimaryStripeMaxRows {
		shapeWork = make(
			[]storeio.IntegerGroupShapeWorkspace,
			storeio.CompactPrimaryStripeMaxRows,
		)
	}
	shapeWork = shapeWork[:storeio.CompactPrimaryStripeMaxRows]
	defer func() {
		clear(shapeSeen)
		// compactStreamView retains page-backed slices. Zeroing the whole
		// reusable array is required even when the caller keeps its capacity.
		clear(shapeWork)
	}()
	var progress storeio.UnifiedIntegerGroupProgress
	supported, err := cursor.FilterIntegerGroups(
		f.inner, &progress, shapeSeen, shapeWork, visit,
	)
	if err != nil {
		return FilterIntegerGroupsResult{}, err
	}
	if !supported {
		return FilterIntegerGroupsResult{}, nil
	}
	return FilterIntegerGroupsResult{
		Scanned: progress.Scanned, Supported: true,
	}, nil
}
