package durable

import (
	"fmt"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// RangeDataSkippingRawBuffer is RangeRawBuffer with a compiled conservative
// compact-stripe filter. Rejected stripes are advanced without decoding keys,
// reconstructing values, or touching overflow chains.
func (s *Snapshot) RangeDataSkippingRawBuffer(
	filter *DataSkippingFilter,
	scratch []byte,
	fn func(key, value []byte) error,
) ([]byte, error) {
	if s == nil || s.collection == nil || s.state == nil {
		return scratch, ErrClosed
	}
	if filter == nil || filter.count == 0 {
		return s.RangeRawBuffer(scratch, fn)
	}
	if fn == nil {
		return scratch, nil
	}
	filter.skippedRows = 0
	filter.skippedStripes = 0
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
		return scratch, err
	}
	defer cursor.Close()
	cursor.AdoptSpliceScratch(s.scanSpliceScratch)
	defer func() {
		s.scanSpliceScratch = cursor.ReleaseSpliceScratch()
	}()
	var decoder storeio.CompactPrimaryScanDecoder
	for {
		_, stripe, ok := cursor.CurrentUnifiedLeaf()
		if !ok {
			return scratch, nil
		}
		keep, err := filter.mayContain(&stripe)
		if err != nil {
			return scratch, err
		}
		if !keep {
			filter.skippedRows += uint64(stripe.Len())
			filter.skippedStripes++
			if err := cursor.NextLeaf(); err != nil {
				return scratch, err
			}
			continue
		}
		for {
			key, ref, err := cursor.VisitCurrentLeafInlineUntilDecoded(
				&decoder, stripe.Len(), fn,
			)
			if err != nil {
				return scratch, err
			}
			if ref == (storeio.PageRef{}) {
				break
			}
			scratch, err = s.collection.appendPrimaryOverflowValue(
				scratch[:0], ref, leafBounds,
			)
			if err != nil {
				return scratch, fmt.Errorf("data-skipping overflow: %w", err)
			}
			if err := fn(key, scratch); err != nil {
				return scratch, err
			}
		}
		if err := cursor.NextLeaf(); err != nil {
			return scratch, err
		}
	}
}
