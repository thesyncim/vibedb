package storeio

import (
	"bytes"
	"errors"
	"fmt"
	"sync/atomic"
)

// Template-columnar mutation diagnostics. detemplateEvents counts how many times
// a resident template-columnar leaf was spliced back into a raw envelope for a
// mutation (the "de-template on first write" tax); replanEvents counts full
// EncodeTemplateColumnarLeaf dictionary re-dedup passes. Both are process-global
// and cheap; a harness resets them after bulk load to attribute runtime cost.
var (
	detemplateEvents atomic.Uint64
	replanEvents     atomic.Uint64
)

// TemplateColumnarDetemplateEvents reports how many times a template leaf was
// de-templated for a mutation since the last reset.
func TemplateColumnarDetemplateEvents() uint64 { return detemplateEvents.Load() }

// TemplateColumnarReplanEvents reports how many full template re-plan (dictionary
// re-dedup) encodes have run since the last reset.
func TemplateColumnarReplanEvents() uint64 { return replanEvents.Load() }

// ResetTemplateColumnarDiag zeroes the template mutation diagnostics.
func ResetTemplateColumnarDiag() {
	detemplateEvents.Store(0)
	replanEvents.Store(0)
}

// A template-columnar primary leaf is a PagePrimaryLeaf physical page whose
// payload carries the class discriminator in payload[2] (the same byte the
// narrow/wide envelope uses) followed by a template-columnar image. Keeping the
// PagePrimaryLeaf kind and the leaf's stable BucketID/LogicalID means the
// segmented tablet router, catalog, COW, and snapshots route to it exactly as
// they route to a raw leaf: only the payload codec differs, and readers select
// it from the class byte after the common-page checksum has already proven the
// payload. The image lives at payload[commonPrimaryTemplateLeafImageOffset:].
const commonPrimaryTemplateLeafImageOffset = 3

// PrimaryLeafClass reports the leaf class recorded in an admitted PagePrimaryLeaf
// page without probing the payload codec. The common page checksum has already
// been verified by PageCache admission, so payload[2] is trustworthy.
func PrimaryLeafClass(page []byte) CommonPrimaryLeafClass {
	header, ok := decodePageHeader(page)
	if !ok {
		return 0
	}
	payloadEnd := PageHeaderSize + int(header.PayloadLength)
	if payloadEnd > len(page) || int(header.PayloadLength) < commonPrimaryLeafPayloadHeader {
		return 0
	}
	return CommonPrimaryLeafClass(page[PageHeaderSize+2] & 0x7f)
}

// TemplateColumnarLeafImagePayloadBytes returns the payload byte count a
// template-columnar leaf would occupy for records, or an error if the records
// cannot be templated (a non-inline value, or the row-count ceiling). Bulk
// class selection uses it to compare the template class against the raw leaf
// without materialising a page.
func TemplateColumnarLeafImagePayloadBytes(records []CommonPrimaryLeafRecord) (int, error) {
	image, err := buildTemplateColumnarImageForRecords(records)
	if err != nil {
		return 0, err
	}
	return commonPrimaryTemplateLeafImageOffset + len(image), nil
}

func buildTemplateColumnarImageForRecords(records []CommonPrimaryLeafRecord) ([]byte, error) {
	if len(records) == 0 || len(records) >= templateColumnarLeafSlots {
		return nil, fmt.Errorf("%w: template leaf row count", ErrInvalidWrite)
	}
	rows := make([]TemplateColumnarLeafRow, len(records))
	for i := range records {
		if records[i].Value.IsOverflow() || len(records[i].Value.Inline) == 0 {
			return nil, fmt.Errorf(
				"%w: template leaf requires inline values", ErrInvalidWrite,
			)
		}
		rows[i] = TemplateColumnarLeafRow{
			Slot: uint8(i), Key: records[i].Key,
			JSON: records[i].Value.Inline,
		}
	}
	return EncodeTemplateColumnarLeaf(rows)
}

// EncodeCommonPrimaryTemplateLeaf writes one template-columnar primary leaf into
// dst. Records must be strictly lexical with non-empty inline values. The page
// carries the standard PagePrimaryLeaf common framing plus the template class
// byte, so the resulting bytes are a drop-in leaf for the ordered graph.
func EncodeCommonPrimaryTemplateLeaf(
	dst []byte,
	header CommonPrimaryLeafHeader,
	records []CommonPrimaryLeafRecord,
	bounds CommonPrimaryLeafBounds,
) ([]byte, error) {
	extent := int(header.PageSize)
	logicalID, logicalOK := CommonPrimaryLeafLogicalID(header.Bucket)
	if !validPhysicalPageSize(header.PageSize) ||
		extent < CommonPrimaryLeafNarrowBytes || extent > 64<<10 ||
		len(dst) < extent || header.StoreID == ([16]byte{}) ||
		header.Generation == 0 || header.Generation >= uint64(1)<<48 ||
		!logicalOK || !commonPrimaryLeafValidateBounds(bounds) {
		return nil, fmt.Errorf(
			"%w: template leaf identity/bounds", ErrInvalidWrite,
		)
	}
	for rank := range records {
		if len(records[rank].Key) == 0 ||
			len(records[rank].Key) > CommonPrimaryLeafMaxKeyBytes ||
			rank != 0 && bytes.Compare(
				records[rank-1].Key, records[rank].Key,
			) >= 0 {
			return nil, fmt.Errorf(
				"%w: template leaf keys not lexical", ErrInvalidWrite,
			)
		}
	}
	image, err := buildTemplateColumnarImageForRecords(records)
	if err != nil {
		return nil, err
	}
	payloadLength := commonPrimaryTemplateLeafImageOffset + len(image)
	if payloadLength > extent-PageHeaderSize-PageTrailerSize {
		return nil, ErrCommonPrimaryLeafFull
	}
	payload, err := InitPage(dst, PageHeader{
		StoreID: header.StoreID, Generation: header.Generation,
		LogicalID: logicalID, PageSize: uint32(extent),
		PayloadLength: uint32(payloadLength), Kind: commonPrimaryLeafPageKind,
	})
	if err != nil {
		return nil, err
	}
	payload[0] = 0
	payload[1] = 0
	payload[2] = byte(CommonPrimaryLeafTemplate)
	copy(payload[commonPrimaryTemplateLeafImageOffset:], image)
	page := dst[:extent]
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

// CommonPrimaryTemplateLeafView is the read view over a template-columnar
// primary leaf. It borrows the admitted page and allocates nothing.
type CommonPrimaryTemplateLeafView struct {
	header CommonPrimaryLeafHeader
	bounds CommonPrimaryLeafBounds
	tv     TemplateColumnarLeafView
}

// OpenCommonPrimaryTemplateLeaf validates common framing, the selecting PageRef,
// the template class byte, and the full template-columnar image (region
// checksums, lexical rows, and splice bounds). It is the admission entry.
func OpenCommonPrimaryTemplateLeaf(
	src []byte,
	seed [16]byte,
	bucket BucketID,
	expected PageRef,
	selectingGeneration uint64,
	bounds CommonPrimaryLeafBounds,
) (CommonPrimaryTemplateLeafView, error) {
	if seed == ([16]byte{}) ||
		!commonPrimaryLeafValidateExpectedRef(
			expected, bucket, selectingGeneration, bounds,
		) {
		return CommonPrimaryTemplateLeafView{}, fmt.Errorf(
			"%w: selector", ErrCommonPrimaryLeafCorrupt,
		)
	}
	pageHeader, payload, err := OpenPage(src)
	if err != nil {
		return CommonPrimaryTemplateLeafView{}, fmt.Errorf(
			"%w: %w", ErrCommonPrimaryLeafCorrupt, err,
		)
	}
	logicalID, _ := CommonPrimaryLeafLogicalID(bucket)
	if len(payload) < commonPrimaryTemplateLeafImageOffset ||
		payload[2]&0x7f != byte(CommonPrimaryLeafTemplate) ||
		pageHeader.LogicalID != logicalID ||
		pageHeader.LogicalID != expected.LogicalID ||
		pageHeader.Generation != expected.Generation ||
		pageHeader.PageSize != expected.Length ||
		pageHeader.Kind != expected.Kind || pageHeader.Flags != 0 {
		return CommonPrimaryTemplateLeafView{}, fmt.Errorf(
			"%w: common identity or template header",
			ErrCommonPrimaryLeafCorrupt,
		)
	}
	tv, err := OpenTemplateColumnarLeaf(
		payload[commonPrimaryTemplateLeafImageOffset:],
	)
	if err != nil {
		return CommonPrimaryTemplateLeafView{}, fmt.Errorf(
			"%w: %w", ErrCommonPrimaryLeafCorrupt, err,
		)
	}
	return CommonPrimaryTemplateLeafView{
		header: CommonPrimaryLeafHeader{
			StoreID: pageHeader.StoreID, Generation: pageHeader.Generation,
			Bucket: bucket, PageSize: pageHeader.PageSize,
		},
		bounds: bounds, tv: tv,
	}, nil
}

// AdmittedCommonPrimaryTemplateLeaf reconstructs a template leaf whose framing
// and image were already validated by PageCache admission. It allocates nothing.
func AdmittedCommonPrimaryTemplateLeaf(
	src []byte,
	bucket BucketID,
	bounds CommonPrimaryLeafBounds,
) (CommonPrimaryTemplateLeafView, bool) {
	pageHeader, ok := decodePageHeader(src)
	if !ok {
		return CommonPrimaryTemplateLeafView{}, false
	}
	payloadEnd := PageHeaderSize + int(pageHeader.PayloadLength)
	if payloadEnd > len(src) ||
		int(pageHeader.PayloadLength) < commonPrimaryTemplateLeafImageOffset {
		return CommonPrimaryTemplateLeafView{}, false
	}
	payload := src[PageHeaderSize:payloadEnd:payloadEnd]
	if payload[2]&0x7f != byte(CommonPrimaryLeafTemplate) {
		return CommonPrimaryTemplateLeafView{}, false
	}
	tv, ok := parseTemplateColumnarLeafDirectories(
		payload[commonPrimaryTemplateLeafImageOffset:],
	)
	if !ok {
		return CommonPrimaryTemplateLeafView{}, false
	}
	return CommonPrimaryTemplateLeafView{
		header: CommonPrimaryLeafHeader{
			StoreID: pageHeader.StoreID, Generation: pageHeader.Generation,
			Bucket: bucket, PageSize: pageHeader.PageSize,
		},
		bounds: bounds, tv: tv,
	}, true
}

// AdmittedPrimaryLeafForMutation returns a raw CommonPrimaryLeafView for an
// admitted primary leaf. A narrow or wide leaf is returned directly and
// detemplated is false. A template leaf is de-templated: every row is spliced
// back into the smallest raw envelope that fits and returned as an owned view,
// with detemplated true so the caller forces a copy-on-write rewrite instead of
// an in-place patch. The rewrite that follows therefore replaces the template
// page with an ordinary raw leaf, which is the "de-template on first write"
// contract. seed is the collection StoreID.
func AdmittedPrimaryLeafForMutation(
	page []byte,
	seed [16]byte,
	bucket BucketID,
	bounds CommonPrimaryLeafBounds,
) (CommonPrimaryLeafView, bool, error) {
	if PrimaryLeafClass(page) != CommonPrimaryLeafTemplate {
		return AdmittedCommonPrimaryLeaf(page, seed, bucket, bounds), false, nil
	}
	tv, ok := AdmittedCommonPrimaryTemplateLeaf(page, bucket, bounds)
	if !ok {
		return CommonPrimaryLeafView{}, false, fmt.Errorf(
			"%w: template primary leaf", ErrCommonPrimaryLeafCorrupt,
		)
	}
	detemplateEvents.Add(1)
	records, _, err := tv.DetemplateRecords(nil, nil)
	if err != nil {
		return CommonPrimaryLeafView{}, false, err
	}
	generation := tv.Header().Generation
	for _, class := range [...]CommonPrimaryLeafClass{
		CommonPrimaryLeafNarrow, CommonPrimaryLeafWide,
	} {
		pageSize := uint32(CommonPrimaryLeafNarrowBytes)
		if class == CommonPrimaryLeafWide {
			pageSize = CommonPrimaryLeafWideBytes
		}
		if placeErr := PlaceCommonPrimaryLeafRecords(
			class, seed, records,
		); placeErr != nil {
			if errors.Is(placeErr, ErrCommonPrimaryLeafNeedsWide) {
				continue
			}
			return CommonPrimaryLeafView{}, false, placeErr
		}
		buf := make([]byte, pageSize)
		if _, encErr := EncodeCommonPrimaryLeaf(
			buf, class,
			CommonPrimaryLeafHeader{
				StoreID: seed, Generation: generation,
				Bucket: bucket, PageSize: pageSize,
			},
			seed, records, bounds,
		); encErr != nil {
			if errors.Is(encErr, ErrCommonPrimaryLeafNeedsWide) ||
				errors.Is(encErr, ErrCommonPrimaryLeafFull) {
				continue
			}
			return CommonPrimaryLeafView{}, false, encErr
		}
		return AdmittedCommonPrimaryLeaf(buf, seed, bucket, bounds), true, nil
	}
	return CommonPrimaryLeafView{}, false, ErrCommonPrimaryLeafFull
}

func (v *CommonPrimaryTemplateLeafView) Len() int {
	if v == nil {
		return 0
	}
	return int(v.tv.count)
}

func (v *CommonPrimaryTemplateLeafView) Header() CommonPrimaryLeafHeader {
	if v == nil {
		return CommonPrimaryLeafHeader{}
	}
	return v.header
}

// lowerBoundRank returns the first rank whose key is >= key.
func (v *CommonPrimaryTemplateLeafView) lowerBoundRank(key []byte) int {
	low, high := 0, int(v.tv.count)
	for low < high {
		middle := int(uint(low+high) >> 1)
		got, _, ok := v.tv.row(middle)
		if !ok || bytes.Compare(got, key) < 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low
}

func (v *CommonPrimaryTemplateLeafView) rankByKey(key []byte) (int, uint16, bool) {
	low, high := 0, int(v.tv.count)
	for low < high {
		middle := int(uint(low+high) >> 1)
		got, ti, ok := v.tv.row(middle)
		if !ok {
			return 0, 0, false
		}
		switch bytes.Compare(got, key) {
		case 0:
			return middle, ti, true
		case -1:
			low = middle + 1
		default:
			high = middle
		}
	}
	return 0, 0, false
}

// AppendRawByKey splices the exact original JSON for key into dst. The template
// splice reconstructs the document byte for byte; a miss leaves dst unchanged.
func (v *CommonPrimaryTemplateLeafView) AppendRawByKey(
	dst []byte, key []byte,
) ([]byte, bool) {
	if v == nil || len(key) == 0 {
		return dst, false
	}
	rank, ti, ok := v.rankByKey(key)
	if !ok {
		return dst, false
	}
	return v.tv.appendRawDirect(dst, rank, ti), true
}

// RowAt returns the key and template index at rank. Keys borrow the admitted
// page.
func (v *CommonPrimaryTemplateLeafView) RowAt(rank int) ([]byte, uint16, bool) {
	return v.tv.row(rank)
}

// AppendRawRank splices the document at rank into dst.
func (v *CommonPrimaryTemplateLeafView) AppendRawRank(
	dst []byte, rank int, ti uint16,
) []byte {
	return v.tv.appendRawDirect(dst, rank, ti)
}

// FirstRankFrom returns the first rank whose key is >= lower (0 for a nil
// lower), for ordered scans.
func (v *CommonPrimaryTemplateLeafView) FirstRankFrom(lower []byte) int {
	if v == nil || len(lower) == 0 {
		return 0
	}
	return v.lowerBoundRank(lower)
}

// DetemplateRecords reconstructs every row as a raw CommonPrimaryLeafRecord for
// mutation de-templating. Spliced JSON is written contiguously into heap (grown
// as needed and returned); record values borrow that heap and record keys
// borrow the admitted page, so both must outlive the returned records.
func (v *CommonPrimaryTemplateLeafView) DetemplateRecords(
	records []CommonPrimaryLeafRecord, heap []byte,
) ([]CommonPrimaryLeafRecord, []byte, error) {
	if v == nil {
		return records, heap, ErrCommonPrimaryLeafCorrupt
	}
	records = records[:0]
	heap = heap[:0]
	spans := make([][2]int, int(v.tv.count))
	for rank := 0; rank < int(v.tv.count); rank++ {
		_, ti, ok := v.tv.row(rank)
		if !ok {
			return records, heap, ErrCommonPrimaryLeafCorrupt
		}
		start := len(heap)
		heap = v.tv.appendRawDirect(heap, rank, ti)
		spans[rank] = [2]int{start, len(heap)}
	}
	for rank := 0; rank < int(v.tv.count); rank++ {
		key, _, ok := v.tv.row(rank)
		if !ok {
			return records, heap, ErrCommonPrimaryLeafCorrupt
		}
		records = append(records, CommonPrimaryLeafRecord{
			Key: key,
			Value: CommonPrimaryLeafValue{
				Inline: heap[spans[rank][0]:spans[rank][1]:spans[rank][1]],
			},
		})
	}
	return records, heap, nil
}
