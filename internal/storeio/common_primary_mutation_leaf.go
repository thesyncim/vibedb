package storeio

import (
	"errors"
	"fmt"
)

// PrimaryLeafMutationScratch owns the bounded temporary storage used to render
// a compact stripe into the existing in-memory mutation envelope.
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
		records:      make([]CommonPrimaryLeafRecord, 0, CompactPrimaryStripeMaxRows),
		heap:         make([]byte, 0, maxExtent),
		spans:        make([][2]int, CompactPrimaryStripeMaxRows),
		overflowRefs: make([]PageRef, CompactPrimaryStripeMaxRows),
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

// PrimaryLeafClass reports the durable leaf grammar from a checksum-admitted
// PagePrimaryLeaf.
func PrimaryLeafClass(page []byte) CommonPrimaryLeafClass {
	header, ok := decodePageHeader(page)
	if !ok {
		return 0
	}
	payloadEnd := PageHeaderSize + int(header.PayloadLength)
	if payloadEnd > len(page) || int(header.PayloadLength) < 4 {
		return 0
	}
	if string(page[PageHeaderSize:PageHeaderSize+4]) == compactPrimaryMagic {
		return CommonPrimaryLeafCompact
	}
	if int(header.PayloadLength) < commonPrimaryLeafPayloadHeader {
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

// AdmittedPrimaryLeafForMutationWithScratch renders a compact stripe into an
// owned raw mutation workspace. The returned bytes never alias the admitted
// durable page, so callers must copy-on-write.
func AdmittedPrimaryLeafForMutationWithScratch(
	page []byte,
	seed [16]byte,
	bucket BucketID,
	bounds CommonPrimaryLeafBounds,
	scratch *PrimaryLeafMutationScratch,
) (CommonPrimaryLeafView, error) {
	if PrimaryLeafClass(page) != CommonPrimaryLeafCompact {
		return CommonPrimaryLeafView{}, fmt.Errorf(
			"%w: non-compact primary leaf", ErrCommonPrimaryLeafCorrupt,
		)
	}
	stripe, ok := AdmittedCompactPrimaryStripe(page, seed, bucket)
	if !ok {
		return CommonPrimaryLeafView{}, fmt.Errorf(
			"%w: compact primary leaf", ErrCommonPrimaryLeafCorrupt,
		)
	}
	var records []CommonPrimaryLeafRecord
	if scratch != nil {
		var err error
		records, err = stripe.RenderRecordsWithScratch(scratch)
		if err != nil {
			return CommonPrimaryLeafView{}, err
		}
	} else {
		records = make([]CommonPrimaryLeafRecord, stripe.Len())
		for row := range records {
			key, decoded := stripe.AppendKey(nil, row)
			if !decoded {
				return CommonPrimaryLeafView{}, ErrCommonPrimaryLeafCorrupt
			}
			value := CommonPrimaryLeafValue{}
			if ref, overflow := stripe.OverflowRef(row); overflow {
				value.Overflow = ref
			} else {
				value.Inline, decoded = stripe.AppendValue(nil, row)
				if !decoded {
					return CommonPrimaryLeafView{}, ErrCommonPrimaryLeafCorrupt
				}
			}
			records[row] = CommonPrimaryLeafRecord{
				Key: key, Value: value,
			}
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
				StoreID: seed, Generation: stripe.Header().Generation,
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
