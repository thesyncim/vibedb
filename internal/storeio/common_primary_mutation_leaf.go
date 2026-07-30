package storeio

import (
	"errors"
	"fmt"
)

// PrimaryLeafMutationScratch owns the bounded temporary storage used by the
// exceptional class-5 mutation bridge. The durable image is always class 5;
// this workspace renders it into the proven raw envelope API used by the
// structural mutation code until that code is converted to a native row view.
type PrimaryLeafMutationScratch struct {
	records      []CommonPrimaryLeafRecord
	heap         []byte
	spans        [][2]int
	overflowRefs []PageRef
	page         []byte
}

func NewPrimaryLeafMutationScratch(maxExtent int) *PrimaryLeafMutationScratch {
	if maxExtent < CommonPrimaryLeafWideBytes {
		maxExtent = CommonPrimaryLeafWideBytes
	}
	return &PrimaryLeafMutationScratch{
		records:      make([]CommonPrimaryLeafRecord, 0, CommonPrimaryLeafWideSlots),
		heap:         make([]byte, 0, maxExtent),
		spans:        make([][2]int, CommonPrimaryLeafWideSlots),
		overflowRefs: make([]PageRef, CommonPrimaryLeafWideSlots),
		page:         make([]byte, maxExtent),
	}
}

func (s *PrimaryLeafMutationScratch) reset(count int) bool {
	if s == nil || count < 0 || count > cap(s.records) ||
		count > len(s.spans) || count > len(s.overflowRefs) {
		return false
	}
	s.records = s.records[:0]
	s.heap = s.heap[:0]
	clear(s.spans[:count])
	clear(s.overflowRefs[:count])
	return true
}

// PrimaryLeafClass reports the leaf class byte from a checksum-admitted
// PagePrimaryLeaf. Format version 5 accepts only CommonPrimaryLeafUnified.
func PrimaryLeafClass(page []byte) CommonPrimaryLeafClass {
	header, ok := decodePageHeader(page)
	if !ok {
		return 0
	}
	payloadEnd := PageHeaderSize + int(header.PayloadLength)
	if payloadEnd > len(page) ||
		int(header.PayloadLength) < commonPrimaryLeafPayloadHeader {
		return 0
	}
	return CommonPrimaryLeafClass(page[PageHeaderSize+2] & 0x7f)
}

func AdmittedPrimaryLeafForMutation(
	page []byte,
	seed [16]byte,
	bucket BucketID,
	bounds CommonPrimaryLeafBounds,
) (CommonPrimaryLeafView, error) {
	return AdmittedPrimaryLeafForMutationWithScratch(
		page, seed, bucket, bounds, nil,
	)
}

// AdmittedPrimaryLeafForMutationWithScratch renders a class-5 leaf into an
// owned raw mutation workspace. The returned bytes never alias the admitted
// durable page, so callers must copy-on-write.
func AdmittedPrimaryLeafForMutationWithScratch(
	page []byte,
	seed [16]byte,
	bucket BucketID,
	bounds CommonPrimaryLeafBounds,
	scratch *PrimaryLeafMutationScratch,
) (CommonPrimaryLeafView, error) {
	if PrimaryLeafClass(page) != CommonPrimaryLeafUnified {
		return CommonPrimaryLeafView{}, fmt.Errorf(
			"%w: non-unified primary leaf", ErrCommonPrimaryLeafCorrupt,
		)
	}
	uv, ok := AdmittedCommonPrimaryUnifiedLeaf(page, seed, bucket, bounds)
	if !ok {
		return CommonPrimaryLeafView{}, fmt.Errorf(
			"%w: unified primary leaf", ErrCommonPrimaryLeafCorrupt,
		)
	}
	var records []CommonPrimaryLeafRecord
	if scratch != nil {
		if err := uv.renderRecordsInto(scratch); err != nil {
			return CommonPrimaryLeafView{}, err
		}
		records = scratch.records
	} else {
		var err error
		records, _, err = uv.RenderRecords(nil, nil)
		if err != nil {
			return CommonPrimaryLeafView{}, err
		}
	}
	for _, attempt := range [...]struct {
		class    CommonPrimaryLeafClass
		pageSize uint32
	}{
		{CommonPrimaryLeafNarrow, CommonPrimaryLeafNarrowBytes},
		{CommonPrimaryLeafWide, CommonPrimaryLeafWideBytes},
		{CommonPrimaryLeafWide, 16 << 10},
		{CommonPrimaryLeafWide, 32 << 10},
		{CommonPrimaryLeafWide, 64 << 10},
	} {
		if len(records) > attempt.class.slots() {
			continue
		}
		if err := PlaceCommonPrimaryLeafRecords(
			attempt.class, seed, records,
		); err != nil {
			if errors.Is(err, ErrCommonPrimaryLeafNeedsWide) ||
				errors.Is(err, ErrCommonPrimaryLeafFull) {
				continue
			}
			return CommonPrimaryLeafView{}, err
		}
		var dst []byte
		if scratch == nil {
			dst = make([]byte, attempt.pageSize)
		} else {
			if int(attempt.pageSize) > len(scratch.page) {
				return CommonPrimaryLeafView{}, ErrCommonPrimaryLeafFull
			}
			dst = scratch.page[:int(attempt.pageSize)]
		}
		if _, err := EncodeCommonPrimaryLeaf(
			dst, attempt.class,
			CommonPrimaryLeafHeader{
				StoreID: seed, Generation: uv.Header().Generation,
				Bucket: bucket, PageSize: attempt.pageSize,
			},
			seed, records, bounds,
		); err != nil {
			if errors.Is(err, ErrCommonPrimaryLeafNeedsWide) ||
				errors.Is(err, ErrCommonPrimaryLeafFull) {
				continue
			}
			return CommonPrimaryLeafView{}, err
		}
		return AdmittedCommonPrimaryLeaf(dst, seed, bucket, bounds), nil
	}
	return CommonPrimaryLeafView{}, ErrCommonPrimaryLeafFull
}
