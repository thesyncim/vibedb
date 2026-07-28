package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"slices"
	"strings"

	"github.com/thesyncim/vibejson/x/byteview"
)

const (
	DocumentGroupPayloadHeaderSize = 64
	DocumentGroupChunkSize         = 16
	DocumentGroupRecordSize        = 16

	documentGroupVersion    = DevelopmentFormatVersion
	documentGroupKnownFlags = DocumentGroupFlagFloat64Sidecar |
		documentGroupFloat64OrderMask | documentGroupFloat64LogicalHighMask
	documentGroupShortLiteral  = byte(0x80)
	documentGroupLongLiteral   = byte(0xff)
	documentGroupMaxDictionary = int(documentGroupShortLiteral)
	documentGroupMaxShortBytes = int(documentGroupLongLiteral - documentGroupShortLiteral)
)

// ErrDocumentGroupCorrupt reports a checksum-valid common page whose grouped
// document payload violates its chunk, template, dictionary, or column
// invariants.
var ErrDocumentGroupCorrupt = errors.New("vibejson: corrupt Store document group")

// DocumentGroupSpan names one exact JSON leaf spelling in a record. Spans are
// ordered, non-overlapping byte ranges in JSON. The bytes between them are the
// structural template; the spans themselves are dictionary or literal values.
type DocumentGroupSpan struct {
	Start uint32
	End   uint32
}

// DocumentGroupRecord is one transient row supplied to the compact-generation
// encoder. Key, JSON, and Spans are borrowed only for the call.
type DocumentGroupRecord struct {
	Key   []byte
	JSON  []byte
	Spans []DocumentGroupSpan
	Slot  uint8
}

// DocumentGroupChunk is one ordinary stable-slot chunk inside a larger
// immutable physical extent. Columns uses the same chunk-local, column-major
// representation as DocumentPage.
type DocumentGroupChunk struct {
	ChunkID uint32
	Live    uint64
	Rows    []DocumentGroupRecord
	Columns DocumentFloat64Columns
}

// DocumentGroupHeader identifies one immutable compact-generation group.
// FirstChunk and ChunkCount describe consecutive logical chunks; readers still
// address exactly one logical chunk at a time through the chunk radix tree.
type DocumentGroupHeader struct {
	StoreID     [16]byte
	Generation  uint64
	LogicalID   uint64
	PageSize    uint32
	FirstChunk  uint32
	ChunkCount  uint16
	RowCount    uint16
	ColumnCount uint16
	Flags       uint16
}

type documentGroupTemplatePlan struct {
	record int
	bytes  int
}

type documentGroupDictionaryCandidate struct {
	value  string
	count  int
	saving int
}

// DocumentGroupWorkspace is reusable bulk-construction state. It is not
// retained by an encoded page and does not participate in reads. Reusing one
// workspace avoids a map and several slices per 128-row group.
type DocumentGroupWorkspace struct {
	templates      []documentGroupTemplatePlan
	recordTemplate []uint16
	counts         map[string]int
	dictionary     []documentGroupDictionaryCandidate
	dictionaryID   map[string]uint8
}

type documentGroupLayout struct {
	rows, chunks, columns int
	keyBytes, bodyBytes   int
	templateBytes         int
	dictionaryBytes       int
	columnBytes           int
	decodedBytes          uint64
	payloadBytes          int
}

// DocumentGroupSize returns the exact physical bytes required for chunks in a
// page of the requested allocation quantum. It returns ok=false when grouping
// is invalid or cannot beat representational bounds. The result is rounded to
// a power-of-two physical extent; callers may compare it with independent
// document pages before choosing the grouped representation.
func DocumentGroupSize(chunks []DocumentGroupChunk, allocationQuantum uint32, workspace *DocumentGroupWorkspace) (size uint32, ok bool) {
	layout, err := planDocumentGroup(chunks, workspace)
	if err != nil || !validPhysicalPageSize(allocationQuantum) {
		return 0, false
	}
	required := uint64(PageHeaderSize + layout.payloadBytes + PageTrailerSize)
	size64 := uint64(allocationQuantum)
	for size64 < required {
		size64 <<= 1
		if size64 > math.MaxUint32 {
			return 0, false
		}
	}
	if !validPhysicalPageSize(uint32(size64)) {
		return 0, false
	}
	return uint32(size64), true
}

// EncodeDocumentGroup writes consecutive stable-slot chunks into one exact,
// independently checksummed extent. Repeated structural bytes are stored once
// per page template; repeated complete leaf spellings use a bounded dictionary.
// Literal leaves remain byte-for-byte exact. Keys and any co-located typed
// columns stay directly addressable. A compact bulk generation may instead
// set DocumentGroupFlagFloat64Sidecar and store columns in the adjacent
// column-major extent derived from the group PageRef.
func EncodeDocumentGroup(dst []byte, header DocumentGroupHeader, chunks []DocumentGroupChunk, nextLogicalID uint64, workspace *DocumentGroupWorkspace) ([]byte, error) {
	layout, err := planDocumentGroup(chunks, workspace)
	if err != nil {
		return nil, err
	}
	if workspace == nil {
		return nil, fmt.Errorf("%w: nil document-group workspace", ErrInvalidWrite)
	}
	if err := validateDocumentGroupHeader(header, layout, chunks, nextLogicalID); err != nil {
		return nil, err
	}
	payload, err := InitPage(dst, PageHeader{
		StoreID: header.StoreID, Generation: header.Generation, LogicalID: header.LogicalID,
		PageSize: header.PageSize, PayloadLength: uint32(layout.payloadBytes), Kind: PageDocumentGroup,
		Flags: uint8(header.Flags),
	})
	if err != nil {
		return nil, err
	}
	if err := writeDocumentGroupPayload(payload, header, chunks, workspace, layout); err != nil {
		return nil, err
	}
	page := dst[:int(header.PageSize)]
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

// writeDocumentGroupPayload writes the exact grouped payload — directory,
// row records, keys, token bodies, templates, dictionary, and any typed
// columns — into payload, which must be exactly layout.payloadBytes long and
// zeroed. It is the container-independent half of EncodeDocumentGroup: the
// legacy chunk page frames these bytes with a common PageHeader and CRC, while
// the ordered-primary compact leaf embeds the identical bytes behind its class
// discriminator. Both paths therefore share one encoder, which is why a compact
// document is byte-for-byte the same regardless of which container carried it.
func writeDocumentGroupPayload(payload []byte, header DocumentGroupHeader, chunks []DocumentGroupChunk, workspace *DocumentGroupWorkspace, layout documentGroupLayout) error {
	binary.LittleEndian.PutUint32(payload[0:4], documentGroupVersion)
	binary.LittleEndian.PutUint32(payload[4:8], header.FirstChunk)
	binary.LittleEndian.PutUint16(payload[8:10], uint16(layout.chunks))
	binary.LittleEndian.PutUint16(payload[10:12], uint16(layout.rows))
	binary.LittleEndian.PutUint16(payload[12:14], uint16(len(workspace.templates)))
	binary.LittleEndian.PutUint16(payload[14:16], uint16(len(workspace.dictionary)))
	binary.LittleEndian.PutUint32(payload[16:20], uint32(layout.chunks*DocumentGroupChunkSize))
	binary.LittleEndian.PutUint32(payload[20:24], uint32(layout.rows*DocumentGroupRecordSize))
	binary.LittleEndian.PutUint32(payload[24:28], uint32(layout.keyBytes))
	binary.LittleEndian.PutUint32(payload[28:32], uint32(layout.bodyBytes))
	binary.LittleEndian.PutUint32(payload[32:36], uint32(layout.templateBytes))
	binary.LittleEndian.PutUint32(payload[36:40], uint32(layout.dictionaryBytes))
	binary.LittleEndian.PutUint32(payload[40:44], uint32(layout.columnBytes))
	binary.LittleEndian.PutUint16(payload[44:46], uint16(layout.columns))
	binary.LittleEndian.PutUint16(payload[46:48], header.Flags)
	binary.LittleEndian.PutUint64(payload[48:56], layout.decodedBytes)
	binary.LittleEndian.PutUint16(payload[56:58], DocumentGroupRecordSize)
	binary.LittleEndian.PutUint16(payload[58:60], DocumentGroupChunkSize)

	chunkStart := DocumentGroupPayloadHeaderSize
	rowStart := chunkStart + layout.chunks*DocumentGroupChunkSize
	keyStart := rowStart + layout.rows*DocumentGroupRecordSize
	bodyStart := keyStart + layout.keyBytes
	templateStart := bodyStart + layout.bodyBytes
	dictionaryStart := templateStart + layout.templateBytes
	columnStart := dictionaryStart + layout.dictionaryBytes

	row, keyEnd, bodyEnd := 0, 0, 0
	for chunkOrdinal, chunk := range chunks {
		descriptor := chunkStart + chunkOrdinal*DocumentGroupChunkSize
		binary.LittleEndian.PutUint32(payload[descriptor:descriptor+4], chunk.ChunkID)
		binary.LittleEndian.PutUint64(payload[descriptor+4:descriptor+12], chunk.Live)
		binary.LittleEndian.PutUint16(payload[descriptor+12:descriptor+14], uint16(row))
		payload[descriptor+14] = uint8(len(chunk.Rows))
		for _, record := range chunk.Rows {
			keyEnd += copy(payload[keyStart+keyEnd:], record.Key)
			templateID := workspace.recordTemplate[row]
			for _, span := range record.Spans {
				value := record.JSON[span.Start:span.End]
				if id, found := workspace.dictionaryID[byteview.String(value)]; found {
					payload[bodyStart+bodyEnd] = id
					bodyEnd++
					continue
				}
				if len(value) <= documentGroupMaxShortBytes {
					payload[bodyStart+bodyEnd] = documentGroupShortLiteral + byte(len(value)-1)
					bodyEnd++
				} else {
					payload[bodyStart+bodyEnd] = documentGroupLongLiteral
					bodyEnd++
					bodyEnd += putDocumentGroupUvarint(payload[bodyStart+bodyEnd:], uint32(len(value)))
				}
				bodyEnd += copy(payload[bodyStart+bodyEnd:], value)
			}
			rd := rowStart + row*DocumentGroupRecordSize
			binary.LittleEndian.PutUint32(payload[rd:rd+4], uint32(keyEnd))
			binary.LittleEndian.PutUint32(payload[rd+4:rd+8], uint32(bodyEnd))
			binary.LittleEndian.PutUint32(payload[rd+8:rd+12], uint32(len(record.JSON)))
			binary.LittleEndian.PutUint16(payload[rd+12:rd+14], templateID)
			payload[rd+14] = record.Slot
			row++
		}
	}
	if keyEnd != layout.keyBytes || bodyEnd != layout.bodyBytes {
		return fmt.Errorf("%w: document-group encoding plan drift", ErrInvalidWrite)
	}

	entryCursor := templateStart + len(workspace.templates)*4
	for id, template := range workspace.templates {
		record := documentGroupRecordAt(chunks, template.record)
		entryStart := entryCursor
		binary.LittleEndian.PutUint16(payload[entryCursor:entryCursor+2], uint16(len(record.Spans)))
		entryCursor += 4
		staticBytes := len(record.JSON)
		for _, span := range record.Spans {
			staticBytes -= int(span.End - span.Start)
		}
		binary.LittleEndian.PutUint32(payload[entryCursor:entryCursor+4], uint32(staticBytes))
		entryCursor += 4
		ends := entryCursor
		entryCursor += (len(record.Spans) + 1) * 4
		previous, written := uint32(0), 0
		for segment := 0; segment <= len(record.Spans); segment++ {
			end := uint32(len(record.JSON))
			if segment < len(record.Spans) {
				end = record.Spans[segment].Start
			}
			written += copy(payload[entryCursor+written:], record.JSON[previous:end])
			binary.LittleEndian.PutUint32(payload[ends+segment*4:ends+(segment+1)*4], uint32(written))
			if segment < len(record.Spans) {
				previous = record.Spans[segment].End
			}
		}
		entryCursor += written
		binary.LittleEndian.PutUint32(payload[templateStart+id*4:templateStart+(id+1)*4], uint32(entryCursor-templateStart-len(workspace.templates)*4))
		if entryCursor-entryStart != template.bytes {
			return fmt.Errorf("%w: template encoding plan drift", ErrInvalidWrite)
		}
	}
	if entryCursor != dictionaryStart {
		return fmt.Errorf("%w: template section length drift", ErrInvalidWrite)
	}

	dictionaryData := dictionaryStart + len(workspace.dictionary)*4
	cursor := dictionaryData
	for id, candidate := range workspace.dictionary {
		cursor += copy(payload[cursor:], candidate.value)
		binary.LittleEndian.PutUint32(
			payload[dictionaryStart+id*4:dictionaryStart+(id+1)*4],
			uint32(cursor-dictionaryData),
		)
	}
	if cursor != columnStart {
		return fmt.Errorf("%w: dictionary section length drift", ErrInvalidWrite)
	}
	for _, chunk := range chunks {
		for column, mask := range chunk.Columns.Masks {
			binary.LittleEndian.PutUint64(payload[cursor:cursor+8], mask)
			cursor += 8
			base := column * 64
			for slots := mask; slots != 0; slots &= slots - 1 {
				slot := bits.TrailingZeros64(slots)
				binary.LittleEndian.PutUint64(payload[cursor:cursor+8], math.Float64bits(chunk.Columns.Values[base+slot]))
				cursor += 8
			}
		}
	}
	if cursor != len(payload) {
		return fmt.Errorf("%w: column section length drift", ErrInvalidWrite)
	}
	return nil
}

func planDocumentGroup(chunks []DocumentGroupChunk, workspace *DocumentGroupWorkspace) (documentGroupLayout, error) {
	var layout documentGroupLayout
	if workspace == nil || len(chunks) < 2 || len(chunks) > math.MaxUint16 {
		return layout, fmt.Errorf("%w: document-group chunk count or workspace", ErrInvalidWrite)
	}
	workspace.templates = workspace.templates[:0]
	workspace.recordTemplate = workspace.recordTemplate[:0]
	if workspace.counts == nil {
		workspace.counts = make(map[string]int, 128)
	} else {
		clear(workspace.counts)
	}
	if workspace.dictionaryID == nil {
		workspace.dictionaryID = make(map[string]uint8, documentGroupMaxDictionary)
	} else {
		clear(workspace.dictionaryID)
	}
	workspace.dictionary = workspace.dictionary[:0]

	firstChunk := chunks[0].ChunkID
	columnCount := len(chunks[0].Columns.Masks)
	recordIndex := 0
	for chunkOrdinal, chunk := range chunks {
		if chunk.ChunkID != firstChunk+uint32(chunkOrdinal) || chunk.Live == 0 ||
			len(chunk.Rows) != bits.OnesCount64(chunk.Live) ||
			len(chunk.Columns.Masks) != columnCount ||
			columnCount == 0 && len(chunk.Columns.Values) != 0 ||
			columnCount != 0 && len(chunk.Columns.Values) != columnCount*64 {
			return layout, fmt.Errorf("%w: document-group chunk shape", ErrInvalidWrite)
		}
		live := chunk.Live
		for _, record := range chunk.Rows {
			slot := uint8(bits.TrailingZeros64(live))
			if record.Slot != slot || len(record.JSON) == 0 || uint64(len(record.JSON)) > math.MaxUint32 {
				return layout, fmt.Errorf("%w: document-group row", ErrInvalidWrite)
			}
			if !validDocumentGroupSpans(record.JSON, record.Spans) {
				return layout, fmt.Errorf("%w: document-group spans", ErrInvalidWrite)
			}
			templateID := -1
			for id, template := range workspace.templates {
				representative := documentGroupRecordAt(chunks, template.record)
				if documentGroupStaticEqual(representative, record) {
					templateID = id
					break
				}
			}
			if templateID < 0 {
				if len(workspace.templates) == math.MaxUint16 {
					return layout, fmt.Errorf("%w: too many document-group templates", ErrInvalidWrite)
				}
				staticBytes := len(record.JSON)
				for _, span := range record.Spans {
					staticBytes -= int(span.End - span.Start)
				}
				templateBytes := 8 + (len(record.Spans)+1)*4 + staticBytes
				workspace.templates = append(workspace.templates, documentGroupTemplatePlan{
					record: recordIndex, bytes: templateBytes,
				})
				templateID = len(workspace.templates) - 1
			}
			workspace.recordTemplate = append(workspace.recordTemplate, uint16(templateID))
			layout.keyBytes += len(record.Key)
			layout.decodedBytes += uint64(len(record.JSON))
			for _, span := range record.Spans {
				// The workspace is cleared before its borrowed input can change.
				// A read-only view avoids copying every scalar merely to count
				// page-local dictionary candidates.
				value := byteview.String(record.JSON[span.Start:span.End])
				workspace.counts[value]++
			}
			recordIndex++
			live &= live - 1
		}
		for column, mask := range chunk.Columns.Masks {
			if mask&^chunk.Live != 0 {
				return layout, fmt.Errorf("%w: document-group column mask", ErrInvalidWrite)
			}
			layout.columnBytes += 8 + bits.OnesCount64(mask)*8
			base := column * 64
			for slots := mask; slots != 0; slots &= slots - 1 {
				value := chunk.Columns.Values[base+bits.TrailingZeros64(slots)]
				if math.IsNaN(value) || math.IsInf(value, 0) {
					return layout, fmt.Errorf("%w: document-group non-finite column", ErrInvalidWrite)
				}
			}
		}
	}
	if recordIndex == 0 || recordIndex > math.MaxUint16 || columnCount > math.MaxUint16 {
		return layout, fmt.Errorf("%w: document-group row or column count", ErrInvalidWrite)
	}

	for value, count := range workspace.counts {
		n := len(value)
		literalBytes := documentGroupLiteralBytes(n)
		saving := count*literalBytes - (count + 4 + n)
		if saving > 0 {
			workspace.dictionary = append(workspace.dictionary, documentGroupDictionaryCandidate{
				value: value, count: count, saving: saving,
			})
		}
	}
	slices.SortFunc(workspace.dictionary, func(a, b documentGroupDictionaryCandidate) int {
		if a.saving != b.saving {
			return b.saving - a.saving
		}
		return strings.Compare(a.value, b.value)
	})
	if len(workspace.dictionary) > documentGroupMaxDictionary {
		workspace.dictionary = workspace.dictionary[:documentGroupMaxDictionary]
	}
	for id, candidate := range workspace.dictionary {
		workspace.dictionaryID[candidate.value] = uint8(id)
		layout.dictionaryBytes += 4 + len(candidate.value)
	}
	for _, chunk := range chunks {
		for _, record := range chunk.Rows {
			for _, span := range record.Spans {
				value := record.JSON[span.Start:span.End]
				if _, found := workspace.dictionaryID[byteview.String(value)]; found {
					layout.bodyBytes++
				} else {
					layout.bodyBytes += documentGroupLiteralBytes(len(value))
				}
			}
		}
	}
	layout.templateBytes = len(workspace.templates) * 4
	for _, template := range workspace.templates {
		layout.templateBytes += template.bytes
	}
	layout.chunks = len(chunks)
	layout.rows = recordIndex
	layout.columns = columnCount
	layout.payloadBytes = DocumentGroupPayloadHeaderSize +
		layout.chunks*DocumentGroupChunkSize + layout.rows*DocumentGroupRecordSize +
		layout.keyBytes + layout.bodyBytes + layout.templateBytes +
		layout.dictionaryBytes + layout.columnBytes
	if layout.payloadBytes < 0 || uint64(layout.payloadBytes) > math.MaxUint32 {
		return documentGroupLayout{}, fmt.Errorf("%w: document-group payload overflow", ErrInvalidWrite)
	}
	return layout, nil
}

func validateDocumentGroupHeader(header DocumentGroupHeader, layout documentGroupLayout, chunks []DocumentGroupChunk, nextLogicalID uint64) error {
	if header.StoreID == ([16]byte{}) || header.Generation == 0 ||
		header.LogicalID <= StateRootLogicalID || header.LogicalID >= nextLogicalID ||
		!validPhysicalPageSize(header.PageSize) || header.Flags&^documentGroupKnownFlags != 0 ||
		header.Flags&DocumentGroupFlagFloat64Sidecar != 0 && layout.columns != 0 ||
		header.FirstChunk != chunks[0].ChunkID ||
		int(header.ChunkCount) != layout.chunks || int(header.RowCount) != layout.rows ||
		int(header.ColumnCount) != layout.columns ||
		uint64(layout.payloadBytes) > uint64(header.PageSize)-PageHeaderSize-PageTrailerSize {
		return fmt.Errorf("%w: document-group header", ErrInvalidWrite)
	}
	return nil
}

func validDocumentGroupSpans(src []byte, spans []DocumentGroupSpan) bool {
	previous := uint32(0)
	for _, span := range spans {
		if span.Start < previous || span.Start >= span.End || uint64(span.End) > uint64(len(src)) {
			return false
		}
		previous = span.End
	}
	return true
}

func documentGroupStaticEqual(a, b DocumentGroupRecord) bool {
	if len(a.Spans) != len(b.Spans) {
		return false
	}
	ap, bp := uint32(0), uint32(0)
	for i := 0; i <= len(a.Spans); i++ {
		ae, be := uint32(len(a.JSON)), uint32(len(b.JSON))
		if i < len(a.Spans) {
			ae, be = a.Spans[i].Start, b.Spans[i].Start
		}
		if !bytes.Equal(a.JSON[ap:ae], b.JSON[bp:be]) {
			return false
		}
		if i < len(a.Spans) {
			ap, bp = a.Spans[i].End, b.Spans[i].End
		}
	}
	return true
}

func documentGroupRecordAt(chunks []DocumentGroupChunk, ordinal int) DocumentGroupRecord {
	for _, chunk := range chunks {
		if ordinal < len(chunk.Rows) {
			return chunk.Rows[ordinal]
		}
		ordinal -= len(chunk.Rows)
	}
	return DocumentGroupRecord{}
}

func documentGroupUvarintLen(value uint32) int {
	switch {
	case value < 1<<7:
		return 1
	case value < 1<<14:
		return 2
	case value < 1<<21:
		return 3
	case value < 1<<28:
		return 4
	default:
		return 5
	}
}

func documentGroupLiteralBytes(length int) int {
	if length <= documentGroupMaxShortBytes {
		return 1 + length
	}
	return 1 + documentGroupUvarintLen(uint32(length)) + length
}

func putDocumentGroupUvarint(dst []byte, value uint32) int {
	n := 0
	for value >= 0x80 {
		dst[n] = byte(value) | 0x80
		value >>= 7
		n++
	}
	dst[n] = byte(value)
	return n + 1
}

func readDocumentGroupUvarint(src []byte) (uint32, int, bool) {
	var value uint32
	for i := 0; i < 5 && i < len(src); i++ {
		b := src[i]
		if i == 4 && b > 0x0f {
			return 0, 0, false
		}
		value |= uint32(b&0x7f) << (7 * i)
		if b&0x80 == 0 {
			if i != 0 && b == 0 {
				return 0, 0, false
			}
			return value, i + 1, true
		}
	}
	return 0, 0, false
}

// DocumentGroupView is an admitted grouped extent. Its slices borrow one page;
// the view retains no pointer per row, template, value, or chunk.
type DocumentGroupView struct {
	header  DocumentGroupHeader
	payload []byte

	chunkStart, rowStart           int
	keyStart, bodyStart            int
	templateStart, dictionaryStart int
	columnStart                    int
	templateCount, dictionaryCount int
}

// OpenDocumentGroup verifies a complete common page and its grouped payload.
func OpenDocumentGroup(src []byte, chunkHighWater uint32, nextLogicalID uint64) (DocumentGroupView, error) {
	pageHeader, payload, err := OpenPage(src)
	if err != nil {
		return DocumentGroupView{}, fmt.Errorf("%w: %w", ErrDocumentGroupCorrupt, err)
	}
	return openDocumentGroupPayload(pageHeader, payload, chunkHighWater, nextLogicalID)
}

// OpenAdmittedDocumentGroup validates the typed payload of a common page whose
// identity and CRC32C were already admitted by PageCache.
func OpenAdmittedDocumentGroup(src []byte, chunkHighWater uint32, nextLogicalID uint64) (DocumentGroupView, error) {
	pageHeader, ok := decodePageHeader(src)
	if !ok || len(src) != int(pageHeader.PageSize) {
		return DocumentGroupView{}, fmt.Errorf("%w: admitted common header", ErrDocumentGroupCorrupt)
	}
	payloadEnd := PageHeaderSize + int(pageHeader.PayloadLength)
	return openDocumentGroupPayload(pageHeader, src[PageHeaderSize:payloadEnd:payloadEnd], chunkHighWater, nextLogicalID)
}

// AdmittedDocumentGroup reconstructs a grouped view after PageCache has
// validated the common page and complete typed payload once. Calling it on
// arbitrary bytes is invalid.
func AdmittedDocumentGroup(src []byte) DocumentGroupView {
	pageHeader, _ := decodePageHeader(src)
	payloadEnd := PageHeaderSize + int(pageHeader.PayloadLength)
	payload := src[PageHeaderSize:payloadEnd:payloadEnd]
	lengths := [7]int{}
	for i := range lengths {
		lengths[i] = int(binary.LittleEndian.Uint32(payload[16+i*4 : 20+i*4]))
	}
	offsets := [8]int{DocumentGroupPayloadHeaderSize}
	for i, length := range lengths {
		offsets[i+1] = offsets[i] + length
	}
	return DocumentGroupView{
		header: DocumentGroupHeader{
			StoreID: pageHeader.StoreID, Generation: pageHeader.Generation, LogicalID: pageHeader.LogicalID,
			PageSize: pageHeader.PageSize, FirstChunk: binary.LittleEndian.Uint32(payload[4:8]),
			ChunkCount:  binary.LittleEndian.Uint16(payload[8:10]),
			RowCount:    binary.LittleEndian.Uint16(payload[10:12]),
			ColumnCount: binary.LittleEndian.Uint16(payload[44:46]),
			Flags:       binary.LittleEndian.Uint16(payload[46:48]),
		},
		payload: payload, chunkStart: offsets[0], rowStart: offsets[1],
		keyStart: offsets[2], bodyStart: offsets[3], templateStart: offsets[4],
		dictionaryStart: offsets[5], columnStart: offsets[6],
		templateCount:   int(binary.LittleEndian.Uint16(payload[12:14])),
		dictionaryCount: int(binary.LittleEndian.Uint16(payload[14:16])),
	}
}

func openDocumentGroupPayload(pageHeader PageHeader, payload []byte, chunkHighWater uint32, nextLogicalID uint64) (DocumentGroupView, error) {
	if pageHeader.Kind != PageDocumentGroup || len(payload) < DocumentGroupPayloadHeaderSize ||
		binary.LittleEndian.Uint32(payload[0:4]) != documentGroupVersion ||
		binary.LittleEndian.Uint16(payload[56:58]) != DocumentGroupRecordSize ||
		binary.LittleEndian.Uint16(payload[58:60]) != DocumentGroupChunkSize ||
		!allZero(payload[60:DocumentGroupPayloadHeaderSize]) {
		return DocumentGroupView{}, fmt.Errorf("%w: header, version, or reserved bytes", ErrDocumentGroupCorrupt)
	}
	chunkCount := int(binary.LittleEndian.Uint16(payload[8:10]))
	rowCount := int(binary.LittleEndian.Uint16(payload[10:12]))
	templateCount := int(binary.LittleEndian.Uint16(payload[12:14]))
	dictionaryCount := int(binary.LittleEndian.Uint16(payload[14:16]))
	firstChunk := binary.LittleEndian.Uint32(payload[4:8])
	flags := binary.LittleEndian.Uint16(payload[46:48])
	if chunkCount < 2 || rowCount == 0 || templateCount == 0 ||
		dictionaryCount > documentGroupMaxDictionary || flags&^documentGroupKnownFlags != 0 ||
		uint16(pageHeader.Flags) != flags ||
		flags&DocumentGroupFlagFloat64Sidecar != 0 &&
			binary.LittleEndian.Uint16(payload[44:46]) != 0 ||
		pageHeader.LogicalID <= StateRootLogicalID || pageHeader.LogicalID >= nextLogicalID ||
		uint64(firstChunk)+uint64(chunkCount) > uint64(chunkHighWater) {
		return DocumentGroupView{}, fmt.Errorf("%w: identity or counts", ErrDocumentGroupCorrupt)
	}
	lengths := [7]uint64{}
	for i := range lengths {
		lengths[i] = uint64(binary.LittleEndian.Uint32(payload[16+i*4 : 20+i*4]))
	}
	if lengths[0] != uint64(chunkCount*DocumentGroupChunkSize) ||
		lengths[1] != uint64(rowCount*DocumentGroupRecordSize) {
		return DocumentGroupView{}, fmt.Errorf("%w: directory lengths", ErrDocumentGroupCorrupt)
	}
	offsets := [8]uint64{DocumentGroupPayloadHeaderSize}
	for i, length := range lengths {
		if length > uint64(len(payload))-offsets[i] {
			return DocumentGroupView{}, fmt.Errorf("%w: section bounds", ErrDocumentGroupCorrupt)
		}
		offsets[i+1] = offsets[i] + length
	}
	if offsets[len(offsets)-1] != uint64(len(payload)) {
		return DocumentGroupView{}, fmt.Errorf("%w: payload length", ErrDocumentGroupCorrupt)
	}
	view := DocumentGroupView{
		header: DocumentGroupHeader{
			StoreID: pageHeader.StoreID, Generation: pageHeader.Generation, LogicalID: pageHeader.LogicalID,
			PageSize: pageHeader.PageSize, FirstChunk: firstChunk, ChunkCount: uint16(chunkCount),
			RowCount: uint16(rowCount), ColumnCount: binary.LittleEndian.Uint16(payload[44:46]), Flags: flags,
		},
		payload: payload, chunkStart: int(offsets[0]), rowStart: int(offsets[1]),
		keyStart: int(offsets[2]), bodyStart: int(offsets[3]),
		templateStart: int(offsets[4]), dictionaryStart: int(offsets[5]),
		columnStart: int(offsets[6]), templateCount: templateCount, dictionaryCount: dictionaryCount,
	}
	if err := view.validate(binary.LittleEndian.Uint64(payload[48:56])); err != nil {
		return DocumentGroupView{}, err
	}
	return view, nil
}

func (v DocumentGroupView) validate(wantDecoded uint64) error {
	rowBase, keyEnd, bodyEnd := 0, uint32(0), uint32(0)
	decoded := uint64(0)
	for chunk := 0; chunk < int(v.header.ChunkCount); chunk++ {
		descriptor := v.chunkStart + chunk*DocumentGroupChunkSize
		chunkID := binary.LittleEndian.Uint32(v.payload[descriptor : descriptor+4])
		live := binary.LittleEndian.Uint64(v.payload[descriptor+4 : descriptor+12])
		firstRow := int(binary.LittleEndian.Uint16(v.payload[descriptor+12 : descriptor+14]))
		count := int(v.payload[descriptor+14])
		if chunkID != v.header.FirstChunk+uint32(chunk) || live == 0 ||
			firstRow != rowBase || count != bits.OnesCount64(live) || v.payload[descriptor+15] != 0 {
			return fmt.Errorf("%w: chunk directory", ErrDocumentGroupCorrupt)
		}
		slots := live
		for range count {
			rd := v.rowStart + rowBase*DocumentGroupRecordSize
			nextKey := binary.LittleEndian.Uint32(v.payload[rd : rd+4])
			nextBody := binary.LittleEndian.Uint32(v.payload[rd+4 : rd+8])
			jsonLength := binary.LittleEndian.Uint32(v.payload[rd+8 : rd+12])
			templateID := int(binary.LittleEndian.Uint16(v.payload[rd+12 : rd+14]))
			slot := uint8(bits.TrailingZeros64(slots))
			if nextKey < keyEnd || uint64(nextKey) > uint64(v.bodyStart-v.keyStart) ||
				nextBody < bodyEnd || uint64(nextBody) > uint64(v.templateStart-v.bodyStart) ||
				jsonLength == 0 || templateID >= v.templateCount ||
				v.payload[rd+14] != slot || v.payload[rd+15] != 0 {
				return fmt.Errorf("%w: row directory", ErrDocumentGroupCorrupt)
			}
			template, ok := v.template(templateID)
			if !ok {
				return fmt.Errorf("%w: template directory", ErrDocumentGroupCorrupt)
			}
			cursor := v.bodyStart + int(bodyEnd)
			end := v.bodyStart + int(nextBody)
			valueBytes := uint64(0)
			for range template.values {
				if cursor >= end {
					return fmt.Errorf("%w: truncated row body", ErrDocumentGroupCorrupt)
				}
				token := v.payload[cursor]
				cursor++
				if int(token) < v.dictionaryCount {
					value, found := v.dictionary(int(token))
					if !found {
						return fmt.Errorf("%w: dictionary reference", ErrDocumentGroupCorrupt)
					}
					valueBytes += uint64(len(value))
					continue
				}
				if token >= documentGroupShortLiteral && token < documentGroupLongLiteral {
					length := int(token-documentGroupShortLiteral) + 1
					if length > end-cursor {
						return fmt.Errorf("%w: short literal length", ErrDocumentGroupCorrupt)
					}
					cursor += length
					valueBytes += uint64(length)
					continue
				}
				if token != documentGroupLongLiteral {
					return fmt.Errorf("%w: unknown row token", ErrDocumentGroupCorrupt)
				}
				length, n, found := readDocumentGroupUvarint(v.payload[cursor:end])
				if !found || length == 0 || uint64(length) > uint64(end-cursor-n) {
					return fmt.Errorf("%w: literal length", ErrDocumentGroupCorrupt)
				}
				cursor += n + int(length)
				valueBytes += uint64(length)
			}
			if cursor != end || valueBytes+uint64(template.staticBytes) != uint64(jsonLength) {
				return fmt.Errorf("%w: row decoded length", ErrDocumentGroupCorrupt)
			}
			decoded += uint64(jsonLength)
			keyEnd, bodyEnd = nextKey, nextBody
			rowBase++
			slots &= slots - 1
		}
	}
	if rowBase != int(v.header.RowCount) || int(keyEnd) != v.bodyStart-v.keyStart ||
		int(bodyEnd) != v.templateStart-v.bodyStart || decoded != wantDecoded {
		return fmt.Errorf("%w: unreferenced row data", ErrDocumentGroupCorrupt)
	}
	if !v.validTemplateDirectory() || !v.validDictionaryDirectory() || !v.validColumns() {
		return fmt.Errorf("%w: template, dictionary, or column section", ErrDocumentGroupCorrupt)
	}
	return nil
}

type documentGroupTemplateView struct {
	values      int
	staticBytes uint32
	ends        []byte
	data        []byte
}

func (v DocumentGroupView) template(id int) (documentGroupTemplateView, bool) {
	if id < 0 || id >= v.templateCount {
		return documentGroupTemplateView{}, false
	}
	directoryBytes := v.templateCount * 4
	previous := uint32(0)
	if id != 0 {
		previous = binary.LittleEndian.Uint32(v.payload[v.templateStart+(id-1)*4 : v.templateStart+id*4])
	}
	end := binary.LittleEndian.Uint32(v.payload[v.templateStart+id*4 : v.templateStart+(id+1)*4])
	dataStart := v.templateStart + directoryBytes
	if end <= previous || uint64(end) > uint64(v.dictionaryStart-dataStart) {
		return documentGroupTemplateView{}, false
	}
	entry := v.payload[dataStart+int(previous) : dataStart+int(end)]
	if len(entry) < 8 || !allZero(entry[2:4]) {
		return documentGroupTemplateView{}, false
	}
	values := int(binary.LittleEndian.Uint16(entry[0:2]))
	staticBytes := binary.LittleEndian.Uint32(entry[4:8])
	endsBytes := (values + 1) * 4
	if endsBytes < 4 || 8+endsBytes > len(entry) || int(staticBytes) != len(entry)-8-endsBytes {
		return documentGroupTemplateView{}, false
	}
	return documentGroupTemplateView{
		values: values, staticBytes: staticBytes,
		ends: entry[8 : 8+endsBytes], data: entry[8+endsBytes:],
	}, true
}

func (v DocumentGroupView) validTemplateDirectory() bool {
	for id := 0; id < v.templateCount; id++ {
		template, ok := v.template(id)
		if !ok {
			return false
		}
		previous := uint32(0)
		for segment := 0; segment <= template.values; segment++ {
			end := binary.LittleEndian.Uint32(template.ends[segment*4 : (segment+1)*4])
			if end < previous || end > template.staticBytes {
				return false
			}
			previous = end
		}
		if previous != template.staticBytes {
			return false
		}
	}
	return true
}

func (v DocumentGroupView) dictionary(id int) ([]byte, bool) {
	if id < 0 || id >= v.dictionaryCount {
		return nil, false
	}
	previous := uint32(0)
	if id != 0 {
		previous = binary.LittleEndian.Uint32(v.payload[v.dictionaryStart+(id-1)*4 : v.dictionaryStart+id*4])
	}
	end := binary.LittleEndian.Uint32(v.payload[v.dictionaryStart+id*4 : v.dictionaryStart+(id+1)*4])
	dataStart := v.dictionaryStart + v.dictionaryCount*4
	if end <= previous || uint64(end) > uint64(v.columnStart-dataStart) {
		return nil, false
	}
	return v.payload[dataStart+int(previous) : dataStart+int(end) : dataStart+int(end)], true
}

func (v DocumentGroupView) validDictionaryDirectory() bool {
	if v.dictionaryCount == 0 {
		return v.dictionaryStart == v.columnStart
	}
	for id := 0; id < v.dictionaryCount; id++ {
		if _, ok := v.dictionary(id); !ok {
			return false
		}
	}
	lastStart := v.dictionaryStart + (v.dictionaryCount-1)*4
	last := binary.LittleEndian.Uint32(v.payload[lastStart : lastStart+4])
	return int(last) == v.columnStart-v.dictionaryStart-v.dictionaryCount*4
}

func (v DocumentGroupView) validColumns() bool {
	cursor := v.columnStart
	for chunk := 0; chunk < int(v.header.ChunkCount); chunk++ {
		descriptor := v.chunkStart + chunk*DocumentGroupChunkSize
		live := binary.LittleEndian.Uint64(v.payload[descriptor+4 : descriptor+12])
		for range int(v.header.ColumnCount) {
			if cursor+8 > len(v.payload) {
				return false
			}
			mask := binary.LittleEndian.Uint64(v.payload[cursor : cursor+8])
			cursor += 8
			if mask&^live != 0 {
				return false
			}
			valueBytes := bits.OnesCount64(mask) * 8
			if cursor+valueBytes > len(v.payload) {
				return false
			}
			for end := cursor + valueBytes; cursor < end; cursor += 8 {
				value := math.Float64frombits(binary.LittleEndian.Uint64(v.payload[cursor : cursor+8]))
				if math.IsNaN(value) || math.IsInf(value, 0) {
					return false
				}
			}
		}
	}
	return cursor == len(v.payload)
}

// Header returns the group identity and logical coverage.
func (v DocumentGroupView) Header() DocumentGroupHeader { return v.header }

// Chunk returns one allocation-free logical chunk view.
func (v DocumentGroupView) Chunk(chunkID uint32) (DocumentGroupChunkView, bool) {
	if chunkID < v.header.FirstChunk || uint64(chunkID-v.header.FirstChunk) >= uint64(v.header.ChunkCount) {
		return DocumentGroupChunkView{}, false
	}
	ordinal := int(chunkID - v.header.FirstChunk)
	descriptor := v.chunkStart + ordinal*DocumentGroupChunkSize
	firstRow := int(binary.LittleEndian.Uint16(v.payload[descriptor+12 : descriptor+14]))
	count := int(v.payload[descriptor+14])
	columnOffset := v.columnStart
	for chunk := range ordinal {
		cd := v.chunkStart + chunk*DocumentGroupChunkSize
		live := binary.LittleEndian.Uint64(v.payload[cd+4 : cd+12])
		for range int(v.header.ColumnCount) {
			mask := binary.LittleEndian.Uint64(v.payload[columnOffset : columnOffset+8])
			columnOffset += 8 + bits.OnesCount64(mask&live)*8
		}
	}
	return DocumentGroupChunkView{
		group: v, chunkID: chunkID,
		live:     binary.LittleEndian.Uint64(v.payload[descriptor+4 : descriptor+12]),
		firstRow: firstRow, count: count, columnStart: columnOffset,
	}, true
}

// DocumentGroupChunkView addresses one stable-slot chunk inside a group.
type DocumentGroupChunkView struct {
	group       DocumentGroupView
	chunkID     uint32
	live        uint64
	firstRow    int
	count       int
	columnStart int
}

// ChunkID returns the logical chunk selected from the physical group.
func (v DocumentGroupChunkView) ChunkID() uint32 { return v.chunkID }

// GroupHeader returns the immutable physical group's identity and logical
// coverage. Writers use the coverage to retire the extent only after its last
// chunk mapping has been peeled away.
func (v DocumentGroupChunkView) GroupHeader() DocumentGroupHeader { return v.group.header }

// Live returns the exact stable-slot occupancy mask.
func (v DocumentGroupChunkView) Live() uint64 { return v.live }

// Len returns the live row count.
func (v DocumentGroupChunkView) Len() int { return v.count }

// Float64ColumnCount returns the number of group-local typed covers.
func (v DocumentGroupChunkView) Float64ColumnCount() int {
	return int(v.group.header.ColumnCount)
}

// DocumentGroupRecordView is a borrowed key plus exact decoded-length
// descriptor. AppendJSON materializes its JSON into caller storage.
type DocumentGroupRecordView struct {
	Key        []byte
	JSONLength uint32
	Slot       uint8
	rank       int
}

// Lookup returns the record at a stable slot without decoding JSON.
func (v DocumentGroupChunkView) Lookup(slot uint8) (DocumentGroupRecordView, bool) {
	if slot >= 64 {
		return DocumentGroupRecordView{}, false
	}
	bit := uint64(1) << slot
	if v.live&bit == 0 {
		return DocumentGroupRecordView{}, false
	}
	rank := bits.OnesCount64(v.live & (bit - 1))
	return v.recordAt(rank, slot)
}

// RecordAt returns the row at packed rank.
func (v DocumentGroupChunkView) RecordAt(rank int) (DocumentGroupRecordView, bool) {
	if rank < 0 || rank >= v.count {
		return DocumentGroupRecordView{}, false
	}
	live := v.live
	for range rank {
		live &= live - 1
	}
	return v.recordAt(rank, uint8(bits.TrailingZeros64(live)))
}

func (v DocumentGroupChunkView) recordAt(rank int, slot uint8) (DocumentGroupRecordView, bool) {
	if rank < 0 || rank >= v.count {
		return DocumentGroupRecordView{}, false
	}
	row := v.firstRow + rank
	rd := v.group.rowStart + row*DocumentGroupRecordSize
	keyEnd := int(binary.LittleEndian.Uint32(v.group.payload[rd : rd+4]))
	keyBegin := 0
	if row != 0 {
		keyBegin = int(binary.LittleEndian.Uint32(v.group.payload[rd-DocumentGroupRecordSize : rd-DocumentGroupRecordSize+4]))
	}
	if keyBegin > keyEnd || v.group.keyStart+keyEnd > v.group.bodyStart {
		return DocumentGroupRecordView{}, false
	}
	return DocumentGroupRecordView{
		Key:        v.group.payload[v.group.keyStart+keyBegin : v.group.keyStart+keyEnd : v.group.keyStart+keyEnd],
		JSONLength: binary.LittleEndian.Uint32(v.group.payload[rd+8 : rd+12]),
		Slot:       slot, rank: row,
	}, true
}

// AppendJSON appends the exact JSON spelling for slot. With enough dst
// capacity it allocates nothing and touches only that row's token stream, one
// page-local template, and referenced dictionary values.
func (v DocumentGroupChunkView) AppendJSON(dst []byte, slot uint8) ([]byte, bool) {
	record, ok := v.Lookup(slot)
	if !ok {
		return dst, false
	}
	return v.appendRecordJSON(dst, record)
}

// AppendRowJSON decodes slot's JSON and also returns its borrowed key.
//
// A scan needs both, and reaching them through Lookup and then AppendJSON made
// it derive the same packed rank twice per row: once for the key and once more
// inside AppendJSON, each time with a popcount over the live mask and a second
// descriptor load. Handing the already-resolved record straight to the decoder
// removes that duplicate work from every row of a compact-generation scan.
func (v DocumentGroupChunkView) AppendRowJSON(dst []byte, slot uint8) ([]byte, []byte, bool) {
	record, ok := v.Lookup(slot)
	if !ok {
		return dst, nil, false
	}
	out, decoded := v.appendRecordJSON(dst, record)
	return out, record.Key, decoded
}

// documentGroupRowDecoder holds the page-level offsets a row decode needs, so
// the token loop reads them from locals instead of rediscovering them.
//
// The old loop called DocumentGroupView.dictionary once per token. That is a
// non-inlinable call taking a 144-byte DocumentGroupView by value, and it
// recomputed the dictionary data origin and re-ran its bounds test on every
// token even though both are fixed for the whole page. It was 9.2% of scan
// samples on its own. Resolving the directory once per row and keeping the
// per-token work to two little-endian loads and one slice removes that without
// removing a single check: the same ordering and range test still guards every
// entry, it just runs against hoisted operands.
type documentGroupRowDecoder struct {
	payload         []byte
	dictionaryEnds  []byte
	dictionaryData  []byte
	dictionaryCount int
}

func (v *DocumentGroupView) rowDecoder() documentGroupRowDecoder {
	dataStart := v.dictionaryStart + v.dictionaryCount*4
	return documentGroupRowDecoder{
		payload:         v.payload,
		dictionaryEnds:  v.payload[v.dictionaryStart:dataStart:dataStart],
		dictionaryData:  v.payload[dataStart:v.columnStart:v.columnStart],
		dictionaryCount: v.dictionaryCount,
	}
}

// value returns dictionary entry id. It repeats dictionary's ordering and
// containment checks against the hoisted directory rather than trusting id.
func (d *documentGroupRowDecoder) value(id int) ([]byte, bool) {
	if id < 0 || id >= d.dictionaryCount {
		return nil, false
	}
	previous := uint32(0)
	if id != 0 {
		previous = binary.LittleEndian.Uint32(d.dictionaryEnds[(id-1)*4 : id*4])
	}
	end := binary.LittleEndian.Uint32(d.dictionaryEnds[id*4 : (id+1)*4])
	if end <= previous || uint64(end) > uint64(len(d.dictionaryData)) {
		return nil, false
	}
	return d.dictionaryData[previous:end:end], true
}

func (v *DocumentGroupChunkView) appendRecordJSON(dst []byte, record DocumentGroupRecordView) ([]byte, bool) {
	decoder := v.group.rowDecoder()
	template, ok := v.group.template(int(binary.LittleEndian.Uint16(
		decoder.payload[v.group.rowStart+record.rank*DocumentGroupRecordSize+12 : v.group.rowStart+record.rank*DocumentGroupRecordSize+14],
	)))
	if !ok {
		return dst, false
	}
	return v.appendDecodedRow(dst, record, &decoder, template)
}

// appendDecodedRow is the token-stream loop. It is split out so a scan can
// resolve the dictionary directory and a shared template once per chunk and
// then decode every row on that chunk without repeating either.
func (v *DocumentGroupChunkView) appendDecodedRow(
	dst []byte,
	record DocumentGroupRecordView,
	decoder *documentGroupRowDecoder,
	template documentGroupTemplateView,
) ([]byte, bool) {
	payload := decoder.payload
	rd := v.group.rowStart + record.rank*DocumentGroupRecordSize
	bodyEnd := int(binary.LittleEndian.Uint32(payload[rd+4 : rd+8]))
	bodyBegin := 0
	if record.rank != 0 {
		previous := rd - DocumentGroupRecordSize
		bodyBegin = int(binary.LittleEndian.Uint32(payload[previous+4 : previous+8]))
	}
	cursor := v.group.bodyStart + bodyBegin
	end := v.group.bodyStart + bodyEnd
	start := len(dst)
	staticData := template.data
	staticEnds := template.ends
	staticPrevious := uint32(0)
	for value := 0; value < template.values; value++ {
		staticEnd := binary.LittleEndian.Uint32(staticEnds[value*4 : (value+1)*4])
		dst = append(dst, staticData[staticPrevious:staticEnd]...)
		staticPrevious = staticEnd
		if cursor >= end {
			return dst, false
		}
		token := payload[cursor]
		cursor++
		if int(token) < decoder.dictionaryCount {
			entry, valid := decoder.value(int(token))
			if !valid {
				return dst, false
			}
			dst = append(dst, entry...)
			continue
		}
		length, n := uint32(0), 0
		if token >= documentGroupShortLiteral && token < documentGroupLongLiteral {
			length = uint32(token-documentGroupShortLiteral) + 1
		} else {
			var okLength bool
			if length, n, okLength = readDocumentGroupUvarint(payload[cursor:end]); !okLength {
				return dst, false
			}
			cursor += n
		}
		if uint64(cursor)+uint64(length) > uint64(end) {
			return dst, false
		}
		dst = append(dst, payload[cursor:cursor+int(length)]...)
		cursor += int(length)
	}
	dst = append(dst, staticData[staticPrevious:]...)
	return dst, cursor == end && len(dst)-start == int(record.JSONLength)
}

// DocumentGroupRows walks the selected live rows of one logical chunk inside a
// compact generation, in ascending stable-slot order.
//
// It is the grouped twin of DocumentPageRows and exists for the same reason:
// the random-access primitives rediscover per row what a sequential walk
// already knows. Lookup converts a stable slot back to a packed rank with a
// popcount; the decoder resolved the dictionary directory and the row's
// template from scratch on every row. Rows in one chunk overwhelmingly share a
// template, so the template is re-resolved only when the identifier actually
// changes, and the dictionary directory is resolved once for the whole walk.
//
// Every key, JSON, and dictionary slice borrows the admitted page.
type DocumentGroupRows struct {
	view     DocumentGroupChunkView
	decoder  documentGroupRowDecoder
	template documentGroupTemplateView
	selected uint64
	pending  uint64
	rank     int
	// templateID is the identifier the cached template was resolved from.
	// Negative means nothing is cached yet.
	templateID int
	corrupt    bool
}

// Rows returns an iterator over the live rows selected by mask. Pass
// ^uint64(0) to visit every live row.
func (v DocumentGroupChunkView) Rows(mask uint64) DocumentGroupRows {
	return DocumentGroupRows{
		view: v, decoder: v.group.rowDecoder(),
		selected: v.live & mask, pending: v.live, templateID: -1,
	}
}

// Next decodes the next selected row's JSON onto dst and returns its borrowed
// key. ok is false at the end of the chunk and also when a row is structurally
// inconsistent, which Err distinguishes.
func (it *DocumentGroupRows) Next(dst []byte) (slot uint8, key, json []byte, ok bool) {
	if it.selected == 0 {
		return 0, nil, dst, false
	}
	bit := it.selected & -it.selected
	it.selected &^= bit
	// Live rows the mask skipped still occupy packed ranks. For a full walk the
	// skipped set is always empty and this folds away to a plain increment.
	skipped := it.pending & (bit - 1)
	it.rank += bits.OnesCount64(skipped)
	it.pending &^= skipped | bit
	rank := it.rank
	it.rank++

	record, found := it.view.recordAt(rank, uint8(bits.TrailingZeros64(bit)))
	if !found {
		it.corrupt = true
		return 0, nil, dst, false
	}
	descriptor := it.view.group.rowStart + record.rank*DocumentGroupRecordSize
	id := int(binary.LittleEndian.Uint16(it.decoder.payload[descriptor+12 : descriptor+14]))
	if id != it.templateID {
		template, valid := it.view.group.template(id)
		if !valid {
			it.corrupt = true
			return 0, nil, dst, false
		}
		it.template, it.templateID = template, id
	}
	out, decoded := it.view.appendDecodedRow(dst, record, &it.decoder, it.template)
	if !decoded {
		it.corrupt = true
		return 0, nil, out, false
	}
	return record.Slot, record.Key, out, true
}

// Err reports whether iteration stopped on a structurally inconsistent row
// rather than at the end of the chunk.
func (it *DocumentGroupRows) Err() bool { return it.corrupt }

// LookupString verifies a complete key without decoding JSON.
func (v DocumentGroupChunkView) LookupString(slot uint8, key string) (DocumentGroupRecordView, bool) {
	record, ok := v.Lookup(slot)
	return record, ok && string(record.Key) == key
}

// Float64Column returns one borrowed typed covering column for this logical
// chunk. It shares DocumentPage's iterator representation.
func (v DocumentGroupChunkView) Float64Column(column int) (DocumentFloat64ColumnView, bool) {
	if column < 0 || column >= int(v.group.header.ColumnCount) {
		return DocumentFloat64ColumnView{}, false
	}
	cursor := v.columnStart
	for current := 0; current < int(v.group.header.ColumnCount); current++ {
		mask := binary.LittleEndian.Uint64(v.group.payload[cursor : cursor+8])
		cursor += 8
		valueBytes := bits.OnesCount64(mask) * 8
		if current == column {
			return DocumentFloat64ColumnView{
				mask: mask, values: v.group.payload[cursor : cursor+valueBytes : cursor+valueBytes],
			}, true
		}
		cursor += valueBytes
	}
	return DocumentFloat64ColumnView{}, false
}

// --- Ordered-primary compact-leaf embedding support ---
//
// A compact primary leaf embeds a document-group payload (produced by
// writeDocumentGroupPayload) behind its class byte, addressed by global row
// rank rather than by chunk/slot. The helpers below let that leaf reuse the
// group's template/dictionary/token decoder without materialising a chunk view
// per row. They read the same flat row directory, key section, token bodies,
// templates, and dictionary that the chunk path writes, so a compact document
// reconstructs byte-for-byte identically in either container.

// documentGroupViewFromPayload reconstructs a group view over an embedded
// payload whose enclosing common page (the compact leaf) has already been
// checksum-admitted. It is the embedded twin of AdmittedDocumentGroup and reads
// no PageHeader identity, because a leaf's reads never consult the group's
// StoreID/Generation/LogicalID.
func documentGroupViewFromPayload(payload []byte) DocumentGroupView {
	lengths := [7]int{}
	for i := range lengths {
		lengths[i] = int(binary.LittleEndian.Uint32(payload[16+i*4 : 20+i*4]))
	}
	offsets := [8]int{DocumentGroupPayloadHeaderSize}
	for i, length := range lengths {
		offsets[i+1] = offsets[i] + length
	}
	return DocumentGroupView{
		header: DocumentGroupHeader{
			PageSize:    0,
			FirstChunk:  binary.LittleEndian.Uint32(payload[4:8]),
			ChunkCount:  binary.LittleEndian.Uint16(payload[8:10]),
			RowCount:    binary.LittleEndian.Uint16(payload[10:12]),
			ColumnCount: binary.LittleEndian.Uint16(payload[44:46]),
			Flags:       binary.LittleEndian.Uint16(payload[46:48]),
		},
		payload: payload, chunkStart: offsets[0], rowStart: offsets[1],
		keyStart: offsets[2], bodyStart: offsets[3], templateStart: offsets[4],
		dictionaryStart: offsets[5], columnStart: offsets[6],
		templateCount:   int(binary.LittleEndian.Uint16(payload[12:14])),
		dictionaryCount: int(binary.LittleEndian.Uint16(payload[14:16])),
	}
}

// openDocumentGroupEmbeddedPayload fully validates an embedded group payload
// using identity synthesized from the enclosing leaf. chunkHighWater is set to
// the leaf's own chunk coverage and nextLogicalID one past the leaf logical ID,
// so the group's self-consistency checks (consecutive chunk IDs, row directory,
// token bounds, template and dictionary directories) run exactly as they do for
// a standalone grouped extent.
func openDocumentGroupEmbeddedPayload(payload []byte, storeID [16]byte, generation, logicalID uint64) (DocumentGroupView, error) {
	if len(payload) < DocumentGroupPayloadHeaderSize {
		return DocumentGroupView{}, fmt.Errorf("%w: embedded payload length", ErrDocumentGroupCorrupt)
	}
	chunkCount := uint64(binary.LittleEndian.Uint16(payload[8:10]))
	firstChunk := binary.LittleEndian.Uint32(payload[4:8])
	header := PageHeader{
		StoreID: storeID, Generation: generation, LogicalID: logicalID,
		PageSize: uint32(len(payload)) + PageHeaderSize + PageTrailerSize,
		Kind:     PageDocumentGroup, PayloadLength: uint32(len(payload)),
		Flags: uint8(binary.LittleEndian.Uint16(payload[46:48])),
	}
	highWater := uint64(firstChunk) + chunkCount
	if highWater > uint64(^uint32(0)) {
		return DocumentGroupView{}, fmt.Errorf("%w: embedded chunk coverage", ErrDocumentGroupCorrupt)
	}
	return openDocumentGroupPayload(header, payload, uint32(highWater), logicalID+1)
}

// RowCount returns the total number of live rows across every chunk, which is
// the compact leaf's live row count.
func (v DocumentGroupView) RowCount() int { return int(v.header.RowCount) }

// rowDirectory reads the global row record at rank: its key span, token-body
// span, exact decoded JSON length, and template identifier.
func (v DocumentGroupView) rowDirectory(rank int) (keyBegin, keyEnd, bodyBegin, bodyEnd int, jsonLength uint32, templateID int, ok bool) {
	if rank < 0 || rank >= int(v.header.RowCount) {
		return 0, 0, 0, 0, 0, 0, false
	}
	rd := v.rowStart + rank*DocumentGroupRecordSize
	keyEnd = int(binary.LittleEndian.Uint32(v.payload[rd : rd+4]))
	bodyEnd = int(binary.LittleEndian.Uint32(v.payload[rd+4 : rd+8]))
	jsonLength = binary.LittleEndian.Uint32(v.payload[rd+8 : rd+12])
	templateID = int(binary.LittleEndian.Uint16(v.payload[rd+12 : rd+14]))
	if rank != 0 {
		keyBegin = int(binary.LittleEndian.Uint32(v.payload[rd-DocumentGroupRecordSize : rd-DocumentGroupRecordSize+4]))
		bodyBegin = int(binary.LittleEndian.Uint32(v.payload[rd-DocumentGroupRecordSize+4 : rd-DocumentGroupRecordSize+8]))
	}
	if keyBegin > keyEnd || v.keyStart+keyEnd > v.bodyStart ||
		bodyBegin > bodyEnd || v.bodyStart+bodyEnd > v.templateStart {
		return 0, 0, 0, 0, 0, 0, false
	}
	return keyBegin, keyEnd, bodyBegin, bodyEnd, jsonLength, templateID, true
}

// RowKeyAt returns the borrowed primary key of the global row at rank.
func (v DocumentGroupView) RowKeyAt(rank int) ([]byte, bool) {
	keyBegin, keyEnd, _, _, _, _, ok := v.rowDirectory(rank)
	if !ok {
		return nil, false
	}
	return v.payload[v.keyStart+keyBegin : v.keyStart+keyEnd : v.keyStart+keyEnd], true
}

// AppendRowRawAt splices the exact original JSON of the global row at rank onto
// dst, interleaving its shared template's static segments with its per-row
// dictionary/literal tokens. It reproduces the document byte for byte and, with
// enough dst capacity, allocates nothing.
func (v DocumentGroupView) AppendRowRawAt(dst []byte, rank int) ([]byte, bool) {
	_, _, bodyBegin, bodyEnd, jsonLength, templateID, ok := v.rowDirectory(rank)
	if !ok {
		return dst, false
	}
	template, ok := v.template(templateID)
	if !ok {
		return dst, false
	}
	decoder := (&v).rowDecoder()
	cursor := v.bodyStart + bodyBegin
	end := v.bodyStart + bodyEnd
	start := len(dst)
	staticData := template.data
	staticEnds := template.ends
	staticPrevious := uint32(0)
	for value := 0; value < template.values; value++ {
		staticEnd := binary.LittleEndian.Uint32(staticEnds[value*4 : (value+1)*4])
		dst = append(dst, staticData[staticPrevious:staticEnd]...)
		staticPrevious = staticEnd
		if cursor >= end {
			return dst, false
		}
		token := decoder.payload[cursor]
		cursor++
		if int(token) < decoder.dictionaryCount {
			entry, valid := decoder.value(int(token))
			if !valid {
				return dst, false
			}
			dst = append(dst, entry...)
			continue
		}
		length, n := uint32(0), 0
		if token >= documentGroupShortLiteral && token < documentGroupLongLiteral {
			length = uint32(token-documentGroupShortLiteral) + 1
		} else {
			var okLength bool
			if length, n, okLength = readDocumentGroupUvarint(decoder.payload[cursor:end]); !okLength {
				return dst, false
			}
			cursor += n
		}
		if uint64(cursor)+uint64(length) > uint64(end) {
			return dst, false
		}
		dst = append(dst, decoder.payload[cursor:cursor+int(length)]...)
		cursor += int(length)
	}
	dst = append(dst, staticData[staticPrevious:]...)
	return dst, cursor == end && len(dst)-start == int(jsonLength)
}
