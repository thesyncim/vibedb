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

// PrimaryGraphLeafEmission is the bounded routing witness returned after one
// planned window has been encoded and staged. Key views borrow the input
// window; the caller must copy only the fences it retains across source reads.
type PrimaryGraphLeafEmission struct {
	Bucket   BucketID
	Ref      PageRef
	Count    int
	FirstKey []byte
	LastKey  []byte
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

// Stage plans and emits the largest prefix of records as one immutable leaf.
// It never builds a dataset-sized plan or record copy: the planner scratch and
// one sink page are the only mutable memory on this path.
func (p *PrimaryGraphLeafWindowPlanner) Stage(
	sink PrimaryGraphBuildSink,
	tabletID uint32,
	localID uint16,
	records []PrimaryGraphRecord,
	maxExtent int,
	placements []PrimaryGraphPlacement,
) (PrimaryGraphLeafEmission, error) {
	if sink == nil || tabletID >= TabletLocalIdentityTabletCount ||
		uint32(localID) >= TabletLocalIdentityLocalCount ||
		(placements != nil && len(placements) < len(records)) {
		return PrimaryGraphLeafEmission{}, fmt.Errorf(
			"%w: incremental primary stage", ErrInvalidWrite,
		)
	}
	count, extent, payload, err := p.Plan(records, maxExtent)
	if err != nil {
		return PrimaryGraphLeafEmission{}, err
	}
	bucketValue, ok := MakeTabletLocalIdentityBucket(tabletID, uint32(localID))
	if !ok {
		return PrimaryGraphLeafEmission{}, fmt.Errorf(
			"%w: incremental primary identity", ErrInvalidWrite,
		)
	}
	bucket := BucketID(bucketValue)
	logicalID, ok := CommonPrimaryLeafLogicalID(bucket)
	if !ok {
		return PrimaryGraphLeafEmission{}, fmt.Errorf(
			"%w: incremental primary logical ID", ErrInvalidWrite,
		)
	}
	page, err := sink.AllocatePage(PagePrimaryLeaf, uint32(extent), logicalID)
	if err != nil {
		return PrimaryGraphLeafEmission{}, err
	}
	if _, err := encodeCompactPrimaryStripePayload(
		page.Bytes(),
		CommonPrimaryLeafHeader{
			StoreID: sink.StoreIdentity(), Generation: sink.BuildGeneration(),
			Bucket: bucket, PageSize: uint32(extent),
		},
		payload,
	); err != nil {
		return PrimaryGraphLeafEmission{}, err
	}
	if placements != nil {
		if !p.placed || count > CommonPrimaryLeafWideSlots {
			return PrimaryGraphLeafEmission{}, fmt.Errorf(
				"%w: incremental primary placements", ErrInvalidWrite,
			)
		}
		for row := range count {
			placements[row] = PrimaryGraphPlacement{
				Bucket: bucket, Slot: uint8(row),
			}
		}
	}
	if err := page.Stage(); err != nil {
		return PrimaryGraphLeafEmission{}, err
	}
	return PrimaryGraphLeafEmission{
		Bucket: bucket, Ref: page.Ref(), Count: count,
		FirstKey: records[0].keyBytes(), LastKey: records[count-1].keyBytes(),
	}, nil
}
