package storeio

import "fmt"

type StagedPageCatalog struct {
	Head   PageRef
	Digest [PageCatalogDigestSize]byte
	Bytes  uint32
	Pages  uint16
}

// StageCanonicalPageCatalog emits a schema/index catalog directly into a
// migration sink one allocation-quantum page at a time. It retains no encoded
// catalog copy beyond the canonical catalog's existing owned image. The sink
// must provide physically/logically sequential allocation for this immutable
// chain; any deviation fails before publication.
func StageCanonicalPageCatalog(
	sink PrimaryGraphBuildSink,
	catalog *CanonicalPageCatalog,
	pageSize uint32,
) (StagedPageCatalog, error) {
	var staged StagedPageCatalog
	if sink == nil || catalog == nil || pageSize == 0 ||
		int(pageSize) > sink.MaxBuildPageBytes() {
		return staged, fmt.Errorf("%w: staged page catalog", ErrInvalidWrite)
	}
	if catalog.CanonicalSize() == 0 {
		return staged, nil
	}
	count, ok := catalog.SegmentCountFor(pageSize)
	if !ok || count < 1 || count > int(^uint16(0)) {
		return staged, fmt.Errorf("%w: staged page catalog geometry", ErrInvalidWrite)
	}
	initialNextID := sink.BuildNextLogicalID()
	var head PageRef
	for ordinal := 0; ordinal < count; ordinal++ {
		page, err := sink.AllocatePage(PageCatalogSegment, pageSize, 0)
		if err != nil {
			return staged, err
		}
		ref := page.Ref()
		if ordinal == 0 {
			head = ref
		} else {
			want := PageRef{
				Offset:     head.Offset + uint64(ordinal)*uint64(pageSize),
				LogicalID:  head.LogicalID + uint64(ordinal),
				Generation: head.Generation, Length: pageSize,
				Kind: PageCatalogSegment,
			}
			if ref != want {
				return staged, fmt.Errorf(
					"%w: non-contiguous staged page catalog", ErrInvalidWrite,
				)
			}
		}
		var next PageRef
		if ordinal+1 < count {
			next = PageRef{
				Offset:     head.Offset + uint64(ordinal+1)*uint64(pageSize),
				LogicalID:  head.LogicalID + uint64(ordinal+1),
				Generation: head.Generation, Length: pageSize,
				Kind: PageCatalogSegment,
			}
		}
		finalFileEnd := max(
			sink.BuildFileEnd(), head.Offset+uint64(count)*uint64(pageSize),
		)
		finalNextID := max(
			sink.BuildNextLogicalID()+uint64(count-ordinal-1),
			initialNextID+uint64(count),
		)
		bounds := PageCatalogBounds{
			StoreID: sink.StoreIdentity(), Generation: sink.BuildGeneration(),
			PageSize: pageSize, DataStart: head.Offset,
			FileEnd: finalFileEnd, NextLogicalID: finalNextID,
			TotalBytes:     uint32(catalog.CanonicalSize()),
			ExpectedDigest: catalog.Digest(),
		}
		if _, err := EncodePageCatalogSegment(
			page.Bytes(),
			PageCatalogSegmentHeader{
				StoreID: sink.StoreIdentity(), Generation: sink.BuildGeneration(),
				LogicalID: ref.LogicalID, Ordinal: uint16(ordinal), Next: next,
			},
			catalog, bounds,
		); err != nil {
			return staged, err
		}
		if err := page.Stage(); err != nil {
			return staged, err
		}
	}
	return StagedPageCatalog{
		Head: head, Digest: catalog.Digest(),
		Bytes: uint32(catalog.CanonicalSize()), Pages: uint16(count),
	}, nil
}
