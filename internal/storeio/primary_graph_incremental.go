package storeio

import (
	"fmt"

	vibejson "github.com/thesyncim/vibejson"
)

// PrimaryGraphLeafWindowPlanner retains the canonical leaf builder scratch for
// bounded streaming compaction. Plan returns the largest prefix fitting one
// leaf; payload is borrowed until the next call.
type PrimaryGraphLeafWindowPlanner struct {
	builder *UnifiedPrimaryLeafBuilder
	placed  bool
}

func NewPrimaryGraphLeafWindowPlanner(
	placed bool,
	summaries []vibejson.CompiledPointer,
) (*PrimaryGraphLeafWindowPlanner, error) {
	b := NewUnifiedPrimaryLeafBuilder()
	if err := b.SetCompactPrimarySummaries(summaries); err != nil {
		return nil, err
	}
	return &PrimaryGraphLeafWindowPlanner{builder: b, placed: placed}, nil
}

func (p *PrimaryGraphLeafWindowPlanner) Plan(
	records []PrimaryGraphRecord,
	maxExtent int,
) (count, extent int, payload []byte, err error) {
	maxRows := CompactPrimaryStripeMaxRows
	if p != nil && p.placed {
		maxRows = CommonPrimaryLeafWideSlots
	}
	if p == nil || p.builder == nil || len(records) == 0 ||
		len(records) > maxRows || maxExtent < int(physicalPageQuantum) ||
		maxExtent > CommonPrimaryLeafMaxExtentBytes {
		return 0, 0, nil, fmt.Errorf("%w: incremental primary window", ErrInvalidWrite)
	}
	if err = prepareCompactPrimaryGraphStripe(records, p.placed, p.builder); err != nil {
		return
	}
	fits := func(n int) (int, bool, error) {
		built, buildErr := buildPreparedCompactPrimaryGraphStripePayload(records[:n], p.builder)
		if buildErr == ErrCommonPrimaryLeafFull {
			return maxExtent + int(physicalPageQuantum), false, nil
		}
		if buildErr != nil {
			return 0, false, buildErr
		}
		need := PageHeaderSize + len(built) + PageTrailerSize
		quantum := int(physicalPageQuantum)
		candidate := (need + quantum - 1) &^ (quantum - 1)
		return candidate, candidate <= maxExtent, nil
	}
	for lo, hi := 1, len(records); lo <= hi; {
		mid := (lo + hi) / 2
		candidate, ok, fitErr := fits(mid)
		if fitErr != nil {
			return 0, 0, nil, fitErr
		}
		if ok {
			count, extent = mid, candidate
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	if count == 0 {
		return 0, 0, nil, fmt.Errorf(
			"%w: incremental primary record exceeds extent", ErrInvalidWrite,
		)
	}
	// The last failed binary-search probe may have reused the builder's payload
	// backing. Rebuild the selected prefix so the returned borrowed view is exact.
	payload, err = buildPreparedCompactPrimaryGraphStripePayload(records[:count], p.builder)
	return
}
