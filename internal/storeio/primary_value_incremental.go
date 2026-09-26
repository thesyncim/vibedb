package storeio

import (
	"fmt"

	vibejson "github.com/thesyncim/vibejson"
)

// PrimaryValueLeafWindowPlanner is the mixed inline/overflow counterpart of
// PrimaryGraphLeafWindowPlanner. It retains one canonical builder and emits
// stable-slot leaves directly into a staged generation.
type PrimaryValueLeafWindowPlanner struct {
	builder *UnifiedPrimaryLeafBuilder
}

func NewPrimaryValueLeafWindowPlanner(
	summaries []vibejson.CompiledPointer,
) (*PrimaryValueLeafWindowPlanner, error) {
	return NewPrimaryValueLeafWindowPlannerOptions(summaries, false)
}

// NewPrimaryValueLeafWindowPlannerOptions configures JSON summaries and the
// collection's immutable opaque-value policy on the one reusable builder.
func NewPrimaryValueLeafWindowPlannerOptions(
	summaries []vibejson.CompiledPointer, opaque bool,
) (*PrimaryValueLeafWindowPlanner, error) {
	builder := NewUnifiedPrimaryLeafBuilder()
	if err := builder.SetOpaqueValues(opaque); err != nil {
		return nil, err
	}
	if err := builder.SetCompactPrimarySummaries(summaries); err != nil {
		return nil, err
	}
	return &PrimaryValueLeafWindowPlanner{builder: builder}, nil
}

func (p *PrimaryValueLeafWindowPlanner) Plan(
	records []CommonPrimaryLeafRecord,
	maxExtent int,
) (count, extent int, payload []byte, err error) {
	return p.plan(records, maxExtent, CommonPrimaryLeafWideSlots, false)
}

// PlanUnplaced plans the largest compact-stripe prefix without 256-slot
// placement or dependence on input Slot fields. It is used by scan-oriented
// leaves, whose compact stripe grammar supports wider windows while indexed
// and tin-maintained leaves retain the stable 256-slot shape through Plan.
// For prefixes within the slot geometry it reserves a worst-case slot-map byte
// per row, so its extent remains a safe bound when runtime mutation placement
// later emits an explicit map.
func (p *PrimaryValueLeafWindowPlanner) PlanUnplaced(
	records []CommonPrimaryLeafRecord,
	maxExtent int,
) (count, extent int, payload []byte, err error) {
	return p.plan(records, maxExtent, CompactPrimaryStripeMaxRows, true)
}

func (p *PrimaryValueLeafWindowPlanner) plan(
	records []CommonPrimaryLeafRecord,
	maxExtent, maxRows int,
	unplaced bool,
) (count, extent int, payload []byte, err error) {
	if p == nil || p.builder == nil || len(records) == 0 ||
		len(records) > maxRows ||
		maxExtent < int(physicalPageQuantum) ||
		maxExtent > CommonPrimaryLeafMaxExtentBytes {
		return 0, 0, nil, fmt.Errorf("%w: incremental primary values", ErrInvalidWrite)
	}
	if unplaced {
		err = prepareCompactPrimaryStripeUnplaced(records, p.builder)
	} else {
		err = prepareCompactPrimaryStripe(records, p.builder)
	}
	if err != nil {
		return
	}
	fits := func(n int) (int, []byte, bool, error) {
		built, buildErr := buildPreparedCompactPrimaryStripePayload(records[:n], p.builder)
		if buildErr == ErrCommonPrimaryLeafFull {
			return maxExtent + int(physicalPageQuantum), nil, false, nil
		}
		if buildErr != nil {
			return 0, nil, false, buildErr
		}
		need := PageHeaderSize + len(built) + PageTrailerSize
		if unplaced && n <= CommonPrimaryLeafWideSlots {
			need += n
		}
		quantum := int(physicalPageQuantum)
		candidate := (need + quantum - 1) &^ (quantum - 1)
		return candidate, built, candidate <= maxExtent, nil
	}
	fullExtent, fullPayload, fullFits, fullErr := fits(len(records))
	if fullErr != nil {
		return 0, 0, nil, fullErr
	}
	if fullFits {
		return len(records), fullExtent, fullPayload, nil
	}
	for low, high := 1, len(records); low <= high; {
		middle := (low + high) / 2
		candidate, _, ok, fitErr := fits(middle)
		if fitErr != nil {
			return 0, 0, nil, fitErr
		}
		if ok {
			count, extent = middle, candidate
			low = middle + 1
		} else {
			high = middle - 1
		}
	}
	if count == 0 {
		return 0, 0, nil, fmt.Errorf(
			"%w: incremental primary value exceeds extent", ErrInvalidWrite,
		)
	}
	payload, err = buildPreparedCompactPrimaryStripePayload(records[:count], p.builder)
	return
}

func (p *PrimaryValueLeafWindowPlanner) Stage(
	sink PrimaryGraphBuildSink,
	tabletID uint32,
	localID uint16,
	records []CommonPrimaryLeafRecord,
	maxExtent int,
) (PrimaryGraphLeafEmission, error) {
	if sink == nil || tabletID >= TabletLocalIdentityTabletCount ||
		uint32(localID) >= TabletLocalIdentityLocalCount {
		return PrimaryGraphLeafEmission{}, fmt.Errorf(
			"%w: incremental primary value stage", ErrInvalidWrite,
		)
	}
	count, extent, payload, err := p.Plan(records, maxExtent)
	if err != nil {
		return PrimaryGraphLeafEmission{}, err
	}
	bucketValue, ok := MakeTabletLocalIdentityBucket(tabletID, uint32(localID))
	if !ok {
		return PrimaryGraphLeafEmission{}, ErrInvalidWrite
	}
	bucket := BucketID(bucketValue)
	logicalID, ok := CommonPrimaryLeafLogicalID(bucket)
	if !ok {
		return PrimaryGraphLeafEmission{}, ErrInvalidWrite
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
	if err := page.Stage(); err != nil {
		return PrimaryGraphLeafEmission{}, err
	}
	return PrimaryGraphLeafEmission{
		Bucket: bucket, Ref: page.Ref(), Count: count,
		FirstKey: records[0].Key, LastKey: records[count-1].Key,
	}, nil
}
