package storeio

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// A compact primary leaf is a PagePrimaryLeaf physical page whose payload carries
// the class discriminator in payload[2] (the same byte the narrow/wide envelope
// and the template-columnar class use) followed by an embedded document-group
// image. Keeping the PagePrimaryLeaf kind and the leaf's stable BucketID/
// LogicalID means the segmented tablet router, catalog, COW, snapshots, and the
// exact-index maintainer route to it exactly as they route to any other leaf:
// only the payload codec differs, and readers select it from the class byte
// after the common-page checksum has already proven the payload.
//
// The embedded image is the container-independent grouped payload written by
// writeDocumentGroupPayload — one shared shape-template table and value
// dictionary per leaf, each row reduced to a dictionary/literal token stream —
// so a document stored compact on the ordered primary graph reconstructs byte
// for byte identically to the same document stored compact in the legacy chunk
// layout. The image lives at payload[commonPrimaryCompactLeafImageOffset:].
const commonPrimaryCompactLeafImageOffset = 3

// CommonPrimaryCompactLeafMaxRows caps the rows one compact leaf may hold. The
// exact-index posting-stable slot for a compact leaf is the row's lexical rank
// (as for the template class), which is a uint8, and a leaf never holds more
// than 256 rows regardless of class.
const CommonPrimaryCompactLeafMaxRows = 256

// compactPrimaryLeafSpanChunk is the synthetic document-group chunk width. The
// embedded encoder requires at least two chunks whose live masks are dense from
// slot zero; a compact leaf's lexical rows are packed into consecutive chunks of
// at most 64 rows, so global lexical rank r maps to chunk r/width, slot r%width.
const compactPrimaryLeafSpanChunk = 64

// CompactPrimaryLeafBuilder holds the reusable scratch a bulk build needs to
// stage many compact leaves without per-leaf allocation: the document-group
// workspace (template dedup and dictionary), the JSON index storage, and the
// span/record/chunk buffers handed to the embedded encoder.
type CompactPrimaryLeafBuilder struct {
	workspace  DocumentGroupWorkspace
	indexStore []vibejson.IndexEntry
	spans      []DocumentGroupSpan
	records    []DocumentGroupRecord
	chunks     []DocumentGroupChunk
}

// NewCompactPrimaryLeafBuilder returns a builder with modest starting scratch.
func NewCompactPrimaryLeafBuilder() *CompactPrimaryLeafBuilder {
	return &CompactPrimaryLeafBuilder{
		indexStore: make([]vibejson.IndexEntry, 0, 64),
	}
}

// appendCompactLeafSpans extracts the ordered scalar-leaf value byte spans of
// one document in a single index pass. The bytes between spans are the shape
// template; the spans are the dictionary or literal values. It uses the same
// leaf-scalar criterion (a non-key tape leaf, Next == 1) the chunk compact path
// uses, so a document's template/value decomposition — and therefore its
// grouped bytes — is identical across both containers.
func (b *CompactPrimaryLeafBuilder) appendCompactLeafSpans(dst []DocumentGroupSpan, src []byte) ([]DocumentGroupSpan, error) {
	var index vibejson.Index
	for {
		var err error
		index, err = vibejson.BuildIndex(src, b.indexStore[:cap(b.indexStore)])
		if err == nil {
			break
		}
		if !errors.Is(err, document.ErrIndexFull) {
			return dst, err
		}
		grown := cap(b.indexStore) * 2
		if grown < 64 {
			grown = 64
		}
		b.indexStore = make([]vibejson.IndexEntry, 0, grown)
	}
	b.indexStore = index.Entries
	start := len(dst)
	for i := range index.Entries {
		e := &index.Entries[i]
		if e.Flags()&vibejson.TapeFlagKey != 0 || e.Next != 1 {
			continue
		}
		dst = append(dst, DocumentGroupSpan{Start: e.Start, End: e.End})
	}
	previous := uint32(0)
	for i := start; i < len(dst); i++ {
		if dst[i].Start < previous || dst[i].Start >= dst[i].End ||
			uint64(dst[i].End) > uint64(len(src)) {
			return dst, fmt.Errorf("%w: compact leaf spans", ErrInvalidWrite)
		}
		previous = dst[i].End
	}
	return dst, nil
}

// extractRecords fills b.records with one document-group record per input row,
// its scalar-value spans already computed in a single index pass. Slot and chunk
// assignment are deferred to chunkPrefix, so a packing search can size any prefix
// of the same extracted records without re-indexing a document per trial.
func (b *CompactPrimaryLeafBuilder) extractRecords(records []CommonPrimaryLeafRecord) error {
	b.spans = b.spans[:0]
	b.records = b.records[:0]
	for row := range records {
		record := &records[row]
		if record.Value.IsOverflow() || len(record.Value.Inline) == 0 {
			return fmt.Errorf("%w: compact leaf requires inline values", ErrInvalidWrite)
		}
		if len(record.Key) == 0 || len(record.Key) > CommonPrimaryLeafMaxKeyBytes {
			return fmt.Errorf("%w: compact leaf key", ErrInvalidWrite)
		}
		if row != 0 && bytes.Compare(records[row-1].Key, record.Key) >= 0 {
			return fmt.Errorf("%w: compact leaf keys not lexical", ErrInvalidWrite)
		}
		spanStart := len(b.spans)
		var err error
		b.spans, err = b.appendCompactLeafSpans(b.spans, record.Value.Inline)
		if err != nil {
			return err
		}
		b.records = append(b.records, DocumentGroupRecord{
			Key: record.Key, JSON: record.Value.Inline,
			Spans: b.spans[spanStart:len(b.spans)],
		})
	}
	return nil
}

// chunkPrefix packs the first count extracted records into synthetic
// document-group chunks (dense live masks, at most 64 rows each, at least two
// chunks), assigning each row its chunk-local slot. The returned chunks alias
// b.records, whose Slot fields it (re)writes for this count.
func (b *CompactPrimaryLeafBuilder) chunkPrefix(count int) ([]DocumentGroupChunk, error) {
	if count < 2 || count > CommonPrimaryCompactLeafMaxRows || count > len(b.records) {
		return nil, fmt.Errorf("%w: compact leaf row count", ErrInvalidWrite)
	}
	numChunks := (count + compactPrimaryLeafSpanChunk - 1) / compactPrimaryLeafSpanChunk
	if numChunks < 2 {
		numChunks = 2
	}
	if cap(b.chunks) < numChunks {
		b.chunks = make([]DocumentGroupChunk, numChunks)
	} else {
		b.chunks = b.chunks[:numChunks]
		clear(b.chunks)
	}
	base := count / numChunks
	extra := count % numChunks
	row := 0
	for chunkOrdinal := range numChunks {
		rows := base
		if chunkOrdinal < extra {
			rows++
		}
		if rows == 0 || rows > compactPrimaryLeafSpanChunk {
			return nil, fmt.Errorf("%w: compact leaf chunk shape", ErrInvalidWrite)
		}
		for slot := range rows {
			b.records[row+slot].Slot = uint8(slot)
		}
		b.chunks[chunkOrdinal] = DocumentGroupChunk{
			ChunkID: uint32(chunkOrdinal),
			Live:    denseLiveMask(rows),
			Rows:    b.records[row : row+rows],
		}
		row += rows
	}
	return b.chunks, nil
}

// chunksFor extracts records and packs all of them into one leaf's chunks.
func (b *CompactPrimaryLeafBuilder) chunksFor(records []CommonPrimaryLeafRecord) ([]DocumentGroupChunk, error) {
	if err := b.extractRecords(records); err != nil {
		return nil, err
	}
	return b.chunkPrefix(len(records))
}

// prefixImageBytes returns the compact leaf payload byte count (class byte plus
// embedded grouped image) for the first count already-extracted records.
func (b *CompactPrimaryLeafBuilder) prefixImageBytes(count int) (int, error) {
	chunks, err := b.chunkPrefix(count)
	if err != nil {
		return 0, err
	}
	layout, err := planDocumentGroup(chunks, &b.workspace)
	if err != nil {
		return 0, err
	}
	return commonPrimaryCompactLeafImageOffset + layout.payloadBytes, nil
}

func denseLiveMask(count int) uint64 {
	if count >= 64 {
		return ^uint64(0)
	}
	return uint64(1)<<uint(count) - 1
}

// CompactPrimaryLeafImagePayloadBytes returns the leaf payload byte count a
// compact leaf holding records would occupy (the class byte plus the embedded
// grouped image), or an error if the records cannot be grouped. Bulk class
// selection uses it to pack a leaf without materialising a page.
func CompactPrimaryLeafImagePayloadBytes(records []CommonPrimaryLeafRecord, b *CompactPrimaryLeafBuilder) (int, error) {
	if b == nil {
		return 0, fmt.Errorf("%w: nil compact leaf builder", ErrInvalidWrite)
	}
	chunks, err := b.chunksFor(records)
	if err != nil {
		return 0, err
	}
	layout, err := planDocumentGroup(chunks, &b.workspace)
	if err != nil {
		return 0, err
	}
	return commonPrimaryCompactLeafImageOffset + layout.payloadBytes, nil
}

// EncodeCommonPrimaryCompactLeaf writes one compact primary leaf into dst.
// Records must be strictly lexical with non-empty inline values. The page
// carries the standard PagePrimaryLeaf common framing plus the compact class
// byte and the embedded grouped image, so the resulting bytes are a drop-in leaf
// for the ordered graph.
func EncodeCommonPrimaryCompactLeaf(
	dst []byte,
	header CommonPrimaryLeafHeader,
	records []CommonPrimaryLeafRecord,
	bounds CommonPrimaryLeafBounds,
	b *CompactPrimaryLeafBuilder,
) ([]byte, error) {
	extent := int(header.PageSize)
	logicalID, logicalOK := CommonPrimaryLeafLogicalID(header.Bucket)
	if b == nil || !validPhysicalPageSize(header.PageSize) ||
		extent < CommonPrimaryLeafNarrowBytes || extent > 64<<10 ||
		len(dst) < extent || header.StoreID == ([16]byte{}) ||
		header.Generation == 0 || header.Generation >= uint64(1)<<48 ||
		!logicalOK || !commonPrimaryLeafValidateBounds(bounds) {
		return nil, fmt.Errorf("%w: compact leaf identity/bounds", ErrInvalidWrite)
	}
	chunks, err := b.chunksFor(records)
	if err != nil {
		return nil, err
	}
	layout, err := planDocumentGroup(chunks, &b.workspace)
	if err != nil {
		return nil, err
	}
	payloadLength := commonPrimaryCompactLeafImageOffset + layout.payloadBytes
	if payloadLength > extent-PageHeaderSize-PageTrailerSize {
		return nil, ErrCommonPrimaryLeafFull
	}
	dgHeader := DocumentGroupHeader{
		StoreID: header.StoreID, Generation: header.Generation, LogicalID: logicalID,
		PageSize: uint32(extent), FirstChunk: 0, ChunkCount: uint16(len(chunks)),
		RowCount: uint16(len(records)), ColumnCount: 0, Flags: 0,
	}
	if err := validateDocumentGroupHeader(dgHeader, layout, chunks, bounds.NextLogicalID); err != nil {
		return nil, err
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
	payload[2] = byte(CommonPrimaryLeafCompact)
	if err := writeDocumentGroupPayload(
		payload[commonPrimaryCompactLeafImageOffset:payloadLength],
		dgHeader, chunks, &b.workspace, layout,
	); err != nil {
		return nil, err
	}
	page := dst[:extent]
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

// CommonPrimaryCompactLeafView is the read view over a compact primary leaf. It
// borrows the admitted page and allocates nothing.
type CommonPrimaryCompactLeafView struct {
	header CommonPrimaryLeafHeader
	bounds CommonPrimaryLeafBounds
	group  DocumentGroupView
}

// OpenCommonPrimaryCompactLeaf validates common framing, the selecting PageRef,
// the compact class byte, and the full embedded grouped image (chunk directory,
// row directory, token bounds, template and dictionary directories). It is the
// admission entry used by the walker and verifier.
func OpenCommonPrimaryCompactLeaf(
	src []byte,
	seed [16]byte,
	bucket BucketID,
	expected PageRef,
	selectingGeneration uint64,
	bounds CommonPrimaryLeafBounds,
) (CommonPrimaryCompactLeafView, error) {
	if seed == ([16]byte{}) ||
		!commonPrimaryLeafValidateExpectedRef(
			expected, bucket, selectingGeneration, bounds,
		) {
		return CommonPrimaryCompactLeafView{}, fmt.Errorf(
			"%w: selector", ErrCommonPrimaryLeafCorrupt,
		)
	}
	pageHeader, payload, err := OpenPage(src)
	if err != nil {
		return CommonPrimaryCompactLeafView{}, fmt.Errorf(
			"%w: %w", ErrCommonPrimaryLeafCorrupt, err,
		)
	}
	logicalID, _ := CommonPrimaryLeafLogicalID(bucket)
	if len(payload) < commonPrimaryCompactLeafImageOffset ||
		payload[2]&0x7f != byte(CommonPrimaryLeafCompact) ||
		pageHeader.LogicalID != logicalID ||
		pageHeader.LogicalID != expected.LogicalID ||
		pageHeader.Generation != expected.Generation ||
		pageHeader.PageSize != expected.Length ||
		pageHeader.Kind != expected.Kind || pageHeader.Flags != 0 {
		return CommonPrimaryCompactLeafView{}, fmt.Errorf(
			"%w: common identity or compact header",
			ErrCommonPrimaryLeafCorrupt,
		)
	}
	group, err := openDocumentGroupEmbeddedPayload(
		payload[commonPrimaryCompactLeafImageOffset:],
		pageHeader.StoreID, pageHeader.Generation, pageHeader.LogicalID,
	)
	if err != nil {
		return CommonPrimaryCompactLeafView{}, fmt.Errorf(
			"%w: %w", ErrCommonPrimaryLeafCorrupt, err,
		)
	}
	return CommonPrimaryCompactLeafView{
		header: CommonPrimaryLeafHeader{
			StoreID: pageHeader.StoreID, Generation: pageHeader.Generation,
			Bucket: bucket, PageSize: pageHeader.PageSize,
		},
		bounds: bounds, group: group,
	}, nil
}

// AdmittedCommonPrimaryCompactLeaf reconstructs a compact leaf whose framing and
// image were already validated by PageCache admission. It allocates nothing.
func AdmittedCommonPrimaryCompactLeaf(
	src []byte,
	bucket BucketID,
	bounds CommonPrimaryLeafBounds,
) (CommonPrimaryCompactLeafView, bool) {
	pageHeader, ok := decodePageHeader(src)
	if !ok {
		return CommonPrimaryCompactLeafView{}, false
	}
	payloadEnd := PageHeaderSize + int(pageHeader.PayloadLength)
	if payloadEnd > len(src) ||
		int(pageHeader.PayloadLength) <= commonPrimaryCompactLeafImageOffset {
		return CommonPrimaryCompactLeafView{}, false
	}
	payload := src[PageHeaderSize:payloadEnd:payloadEnd]
	if payload[2]&0x7f != byte(CommonPrimaryLeafCompact) {
		return CommonPrimaryCompactLeafView{}, false
	}
	return CommonPrimaryCompactLeafView{
		header: CommonPrimaryLeafHeader{
			StoreID: pageHeader.StoreID, Generation: pageHeader.Generation,
			Bucket: bucket, PageSize: pageHeader.PageSize,
		},
		bounds: bounds,
		group:  documentGroupViewFromPayload(payload[commonPrimaryCompactLeafImageOffset:]),
	}, true
}

// Len returns the live row count.
func (v *CommonPrimaryCompactLeafView) Len() int {
	if v == nil {
		return 0
	}
	return v.group.RowCount()
}

// Header returns the leaf's stable identity.
func (v *CommonPrimaryCompactLeafView) Header() CommonPrimaryLeafHeader {
	if v == nil {
		return CommonPrimaryLeafHeader{}
	}
	return v.header
}

// RowAt returns the borrowed key at lexical rank. The second result mirrors the
// template leaf's template-index return so posting/scan dispatch is uniform; the
// compact decoder resolves its own template, so callers pass it back unread.
func (v *CommonPrimaryCompactLeafView) RowAt(rank int) ([]byte, uint16, bool) {
	if v == nil {
		return nil, 0, false
	}
	key, ok := v.group.RowKeyAt(rank)
	return key, 0, ok
}

// AppendRawRank splices the exact original JSON at lexical rank onto dst.
func (v *CommonPrimaryCompactLeafView) AppendRawRank(dst []byte, rank int, _ uint16) []byte {
	if v == nil {
		return dst
	}
	out, _ := v.group.AppendRowRawAt(dst, rank)
	return out
}

// AppendRawByKey splices the exact original JSON for key into dst. A miss leaves
// dst unchanged.
func (v *CommonPrimaryCompactLeafView) AppendRawByKey(dst []byte, key []byte) ([]byte, bool) {
	if v == nil || len(key) == 0 {
		return dst, false
	}
	rank, ok := v.rankByKey(key)
	if !ok {
		return dst, false
	}
	out, decoded := v.group.AppendRowRawAt(dst, rank)
	return out, decoded
}

// FirstRankFrom returns the first lexical rank whose key is >= lower (0 for a
// nil lower), for ordered scans.
func (v *CommonPrimaryCompactLeafView) FirstRankFrom(lower []byte) int {
	if v == nil || len(lower) == 0 {
		return 0
	}
	return v.lowerBoundRank(lower)
}

func (v *CommonPrimaryCompactLeafView) lowerBoundRank(key []byte) int {
	low, high := 0, v.group.RowCount()
	for low < high {
		middle := int(uint(low+high) >> 1)
		got, ok := v.group.RowKeyAt(middle)
		if !ok || bytes.Compare(got, key) < 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low
}

func (v *CommonPrimaryCompactLeafView) rankByKey(key []byte) (int, bool) {
	low, high := 0, v.group.RowCount()
	for low < high {
		middle := int(uint(low+high) >> 1)
		got, ok := v.group.RowKeyAt(middle)
		if !ok {
			return 0, false
		}
		switch bytes.Compare(got, key) {
		case 0:
			return middle, true
		case -1:
			low = middle + 1
		default:
			high = middle
		}
	}
	return 0, false
}

// DetemplateRecords reconstructs every row as a raw CommonPrimaryLeafRecord for
// mutation de-compaction. Reconstructed JSON is written contiguously into heap
// (grown as needed and returned); record values borrow that heap and record keys
// borrow the admitted page, so both must outlive the returned records.
func (v *CommonPrimaryCompactLeafView) DetemplateRecords(
	records []CommonPrimaryLeafRecord, heap []byte,
) ([]CommonPrimaryLeafRecord, []byte, error) {
	if v == nil {
		return records, heap, ErrCommonPrimaryLeafCorrupt
	}
	records = records[:0]
	heap = heap[:0]
	n := v.group.RowCount()
	spans := make([][2]int, n)
	for rank := 0; rank < n; rank++ {
		start := len(heap)
		out, ok := v.group.AppendRowRawAt(heap, rank)
		if !ok {
			return records, heap, ErrCommonPrimaryLeafCorrupt
		}
		heap = out
		spans[rank] = [2]int{start, len(heap)}
	}
	for rank := 0; rank < n; rank++ {
		key, ok := v.group.RowKeyAt(rank)
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
