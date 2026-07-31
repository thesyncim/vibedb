package storeio

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
)

const (
	PostingPagePayloadHeaderSize = 32
	PostingSegmentHeaderSize     = 48
	postingPageVersion           = DevelopmentFormatVersion
	// PostingSegmentCollision marks a certificate whose hash stream contains
	// more than one exact scalar or compound tuple. Readers must recheck its
	// documents.
	PostingSegmentCollision  = uint16(1 << 0)
	postingSegmentKnownFlags = PostingSegmentCollision
)

// ErrPostingPageCorrupt reports a checksum-valid common page whose packed
// stable-slot posting streams are malformed or non-canonical.
var ErrPostingPageCorrupt = errors.New("vibejson: corrupt Store index posting page")

// PostingEntry is one logical chunk's exact stable-slot result. Entries in a
// stream are strictly ordered by Chunk and Bits is never zero.
type PostingEntry struct {
	Chunk uint32
	Bits  uint64
}

// PostingLink identifies a continuation segment without embedding a physical
// pointer. The state-root directory resolves LogicalID for the selected
// generation; Segment is its packed rank inside that page.
type PostingLink struct {
	LogicalID uint64
	Segment   uint16
}

// PostingSegment is one value stream supplied to EncodePostingPage. StreamID
// comes from the exact value dictionary after full canonical tuple comparison;
// TupleHash is an accelerator and is never an equality boundary.
type PostingSegment struct {
	StreamID    uint32
	TupleHash   uint64
	Flags       uint16
	Next        PostingLink
	Certificate []byte
	Entries     []PostingEntry
}

// PostingPageHeader identifies one immutable physical page that packs several
// independent posting micro-streams for the same declared index.
type PostingPageHeader struct {
	StoreID    [16]byte
	Generation uint64
	LogicalID  uint64
	PageSize   uint32
	IndexID    uint32
}

// PostingSegmentHeader is the admitted value-only metadata for one stream.
type PostingSegmentHeader struct {
	StreamID   uint32
	TupleHash  uint64
	FirstChunk uint32
	LastChunk  uint32
	Rows       uint32
	Flags      uint16
	Next       PostingLink
}

// PostingPageView retains one borrowed page payload. Segment metadata is read
// directly from its packed directory, so pointer count is bounded by resident
// frames rather than by indexed values.
type PostingPageView struct {
	header  PostingPageHeader
	payload []byte
	count   uint16
}

// PostingSegmentView is one admitted segment inside a PostingPageView.
type PostingSegmentView struct {
	header      PostingSegmentHeader
	certificate []byte
	entries     []byte
	count       uint16
}

// PostingIterator decodes an already-admitted segment without allocation.
// Copying an iterator creates an independent cursor over the same immutable
// page.
type PostingIterator struct {
	entries   []byte
	position  int
	remaining uint16
	chunk     uint32
	first     bool
}

// EncodePostingPage packs ordered, uniquely identified micro-streams into one
// physical page. Singleton masks encode (chunk delta, slot) in one uvarint;
// multi-slot masks encode a tagged delta followed by a native uint64 word.
// No allocation is performed. Input slices must not overlap dst because
// InitPage clears the complete destination extent.
func EncodePostingPage(dst []byte, header PostingPageHeader, segments []PostingSegment, nextLogicalID uint64, indexHighWater uint32) ([]byte, error) {
	encodedBytes, err := validatePostingPageWrite(header, segments, nextLogicalID, indexHighWater)
	if err != nil {
		return nil, err
	}
	payloadLength := PostingPagePayloadHeaderSize + len(segments)*PostingSegmentHeaderSize + encodedBytes
	payload, err := InitPage(dst, PageHeader{
		StoreID:       header.StoreID,
		Generation:    header.Generation,
		LogicalID:     header.LogicalID,
		PageSize:      header.PageSize,
		PayloadLength: uint32(payloadLength),
		Kind:          PageIndexPosting,
	})
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(payload[0:4], postingPageVersion)
	binary.LittleEndian.PutUint32(payload[4:8], header.IndexID)
	binary.LittleEndian.PutUint16(payload[8:10], uint16(len(segments)))
	binary.LittleEndian.PutUint32(payload[12:16], uint32(len(segments)*PostingSegmentHeaderSize))
	binary.LittleEndian.PutUint32(payload[16:20], uint32(encodedBytes))

	dataPosition := PostingPagePayloadHeaderSize + len(segments)*PostingSegmentHeaderSize
	for i, segment := range segments {
		start := PostingPagePayloadHeaderSize + i*PostingSegmentHeaderSize
		record := payload[start : start+PostingSegmentHeaderSize]
		rows := uint32(0)
		for _, entry := range segment.Entries {
			rows += uint32(bits.OnesCount64(entry.Bits))
		}
		entriesLength, _ := PostingEntriesEncodedSize(segment.Entries)
		encodedLength := len(segment.Certificate) + entriesLength
		binary.LittleEndian.PutUint32(record[0:4], segment.StreamID)
		binary.LittleEndian.PutUint32(record[4:8], segment.Entries[0].Chunk)
		binary.LittleEndian.PutUint32(record[8:12], segment.Entries[len(segment.Entries)-1].Chunk)
		binary.LittleEndian.PutUint32(record[12:16], rows)
		binary.LittleEndian.PutUint64(record[16:24], segment.TupleHash)
		binary.LittleEndian.PutUint64(record[24:32], segment.Next.LogicalID)
		binary.LittleEndian.PutUint32(record[32:36], uint32(dataPosition))
		binary.LittleEndian.PutUint32(record[36:40], uint32(encodedLength))
		binary.LittleEndian.PutUint16(record[40:42], uint16(len(segment.Entries)))
		binary.LittleEndian.PutUint16(record[42:44], segment.Next.Segment)
		binary.LittleEndian.PutUint16(record[44:46], segment.Flags)
		binary.LittleEndian.PutUint16(record[46:48], uint16(len(segment.Certificate)))

		position := dataPosition
		copy(payload[position:position+len(segment.Certificate)], segment.Certificate)
		position += len(segment.Certificate)
		previous := segment.Entries[0].Chunk
		for entryIndex, entry := range segment.Entries {
			delta := entry.Chunk - previous
			if entryIndex == 0 {
				delta = 0
			}
			if entry.Bits&(entry.Bits-1) == 0 {
				slot := uint64(bits.TrailingZeros64(entry.Bits))
				token := uint64(delta)<<7 | slot<<1
				position += binary.PutUvarint(payload[position:], token)
			} else {
				token := uint64(delta)<<1 | 1
				position += binary.PutUvarint(payload[position:], token)
				binary.LittleEndian.PutUint64(payload[position:position+8], entry.Bits)
				position += 8
			}
			previous = entry.Chunk
		}
		dataPosition += encodedLength
	}
	page := dst[:int(header.PageSize)]
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

// OpenPostingPage verifies the complete packed directory and every compressed
// segment once. Subsequent lookup and iteration need no checksum, heap object,
// or per-entry pointer.
func OpenPostingPage(src []byte, nextLogicalID uint64, indexHighWater uint32) (PostingPageView, error) {
	pageHeader, payload, err := OpenPage(src)
	if err != nil {
		return PostingPageView{}, fmt.Errorf("%w: %w", ErrPostingPageCorrupt, err)
	}
	if pageHeader.Kind != PageIndexPosting || len(payload) < PostingPagePayloadHeaderSize ||
		!allZero(payload[10:12]) ||
		!allZero(payload[20:PostingPagePayloadHeaderSize]) {
		return PostingPageView{}, fmt.Errorf("%w: header, version, or reserved bytes", ErrPostingPageCorrupt)
	}
	if binary.LittleEndian.Uint32(payload[0:4]) != postingPageVersion {
		return PostingPageView{}, fmt.Errorf("%w: header, version, or reserved bytes", ErrPostingPageCorrupt)
	}
	count := binary.LittleEndian.Uint16(payload[8:10])
	header := PostingPageHeader{
		StoreID:    pageHeader.StoreID,
		Generation: pageHeader.Generation,
		LogicalID:  pageHeader.LogicalID,
		PageSize:   pageHeader.PageSize,
		IndexID:    binary.LittleEndian.Uint32(payload[4:8]),
	}
	directoryBytes := binary.LittleEndian.Uint32(payload[12:16])
	dataBytes := binary.LittleEndian.Uint32(payload[16:20])
	dataStart := PostingPagePayloadHeaderSize + int(directoryBytes)
	if err := validatePostingPageHeader(header, int(count), nextLogicalID, indexHighWater); err != nil ||
		directoryBytes != uint32(count)*PostingSegmentHeaderSize ||
		uint64(dataStart)+uint64(dataBytes) != uint64(len(payload)) {
		return PostingPageView{}, fmt.Errorf("%w: page identity, directory, or data bounds", ErrPostingPageCorrupt)
	}

	previousStream := uint32(0)
	dataPosition := dataStart
	for i := 0; i < int(count); i++ {
		segment, _, entries, entryCount, decodeErr := decodePostingSegment(payload, i, dataStart)
		if decodeErr != nil || segment.StreamID <= previousStream ||
			segment.Flags&^postingSegmentKnownFlags != 0 || segment.Rows == 0 ||
			dataPosition != int(binary.LittleEndian.Uint32(postingSegmentRecord(payload, i)[32:36])) ||
			!validPostingLink(segment.Next, header.LogicalID, nextLogicalID) {
			return PostingPageView{}, fmt.Errorf("%w: segment directory", ErrPostingPageCorrupt)
		}
		position := 0
		previousChunk := segment.FirstChunk
		rows := uint64(0)
		for entryIndex := 0; entryIndex < int(entryCount); entryIndex++ {
			entry, next, ok := decodePostingEntry(entries, position, previousChunk, entryIndex == 0)
			if !ok {
				return PostingPageView{}, fmt.Errorf("%w: non-canonical posting entry", ErrPostingPageCorrupt)
			}
			position = next
			previousChunk = entry.Chunk
			rows += uint64(bits.OnesCount64(entry.Bits))
		}
		if position != len(entries) || previousChunk != segment.LastChunk || rows != uint64(segment.Rows) {
			return PostingPageView{}, fmt.Errorf("%w: posting count, rows, or tail", ErrPostingPageCorrupt)
		}
		previousStream = segment.StreamID
		dataPosition += int(binary.LittleEndian.Uint32(postingSegmentRecord(payload, i)[36:40]))
	}
	if dataPosition != len(payload) {
		return PostingPageView{}, fmt.Errorf("%w: non-canonical data packing", ErrPostingPageCorrupt)
	}
	return PostingPageView{header: header, payload: payload, count: count}, nil
}

// Header returns the value-only page identity.
func (v PostingPageView) Header() PostingPageHeader { return v.header }

// Len returns the number of packed posting segments.
func (v PostingPageView) Len() int { return int(v.count) }

// SegmentAt returns the admitted segment at packed rank.
func (v PostingPageView) SegmentAt(rank int) (PostingSegmentView, bool) {
	if rank < 0 || rank >= int(v.count) {
		return PostingSegmentView{}, false
	}
	header, certificate, entries, count, err := decodePostingSegment(
		v.payload, rank,
		PostingPagePayloadHeaderSize+int(v.count)*PostingSegmentHeaderSize,
	)
	if err != nil {
		return PostingSegmentView{}, false
	}
	return PostingSegmentView{
		header: header, certificate: certificate, entries: entries, count: count,
	}, true
}

// Lookup resolves one exact-value stream id with binary search over the packed
// segment directory.
func (v PostingPageView) Lookup(streamID uint32) (PostingSegmentView, bool) {
	low, high := 0, int(v.count)
	for low < high {
		middle := int(uint(low+high) >> 1)
		got := binary.LittleEndian.Uint32(postingSegmentRecord(v.payload, middle)[0:4])
		if got < streamID {
			low = middle + 1
		} else {
			high = middle
		}
	}
	if low >= int(v.count) || binary.LittleEndian.Uint32(postingSegmentRecord(v.payload, low)[0:4]) != streamID {
		return PostingSegmentView{}, false
	}
	return v.SegmentAt(low)
}

// Header returns the value-only segment metadata.
func (v PostingSegmentView) Header() PostingSegmentHeader { return v.header }

// Len returns the number of chunk masks in this segment.
func (v PostingSegmentView) Len() int { return int(v.count) }

// Certificate returns the capacity-clipped exact scalar or compound-tuple
// representative for this hash stream. Empty means the writer could not
// encode a certificate and readers must recheck documents. The slice borrows
// the posting page.
func (v PostingSegmentView) Certificate() []byte { return v.certificate }

// Iterator returns an independent zero-allocation decoder.
func (v PostingSegmentView) Iterator() PostingIterator {
	return PostingIterator{entries: v.entries, remaining: v.count, chunk: v.header.FirstChunk, first: true}
}

// Next returns the next ordered chunk mask.
func (it *PostingIterator) Next() (PostingEntry, bool) {
	if it == nil || it.remaining == 0 {
		return PostingEntry{}, false
	}
	entry, next, ok := decodePostingEntry(it.entries, it.position, it.chunk, it.first)
	if !ok {
		it.remaining = 0
		return PostingEntry{}, false
	}
	it.position = next
	it.remaining--
	it.chunk = entry.Chunk
	it.first = false
	return entry, true
}

// PostingEntriesEncodedSize returns the canonical byte count for one ordered
// segment, excluding page and segment headers.
func PostingEntriesEncodedSize(entries []PostingEntry) (int, error) {
	if len(entries) == 0 || len(entries) > int(^uint16(0)) {
		return 0, fmt.Errorf("%w: posting entry count", ErrInvalidWrite)
	}
	encodedLength := 0
	previous := entries[0].Chunk
	for i, entry := range entries {
		entrySize, err := PostingEntryEncodedSize(previous, entry, i == 0)
		if err != nil {
			return 0, err
		}
		encodedLength += entrySize
		previous = entry.Chunk
	}
	return encodedLength, nil
}

// PostingPagePrefix returns the longest non-empty prefix whose canonical
// posting bytes fit encodedCapacity. The first entry of every segment resets
// its chunk delta to zero. Callers can therefore partition a long stream
// without trial encodes or allocation.
func PostingPagePrefix(entries []PostingEntry, encodedCapacity int) (count, encodedSize int, err error) {
	if len(entries) == 0 || encodedCapacity <= 0 {
		return 0, 0, fmt.Errorf("%w: posting prefix input", ErrInvalidWrite)
	}
	previous := entries[0].Chunk
	limit := min(len(entries), int(^uint16(0)))
	for i := range limit {
		entry := entries[i]
		entrySize, entryErr := PostingEntryEncodedSize(previous, entry, i == 0)
		if entryErr != nil {
			return 0, 0, entryErr
		}
		if encodedSize+entrySize > encodedCapacity {
			break
		}
		encodedSize += entrySize
		count++
		previous = entry.Chunk
	}
	if count == 0 {
		return 0, 0, fmt.Errorf("%w: posting entry does not fit", ErrInvalidWrite)
	}
	return count, encodedSize, nil
}

// PostingEntryEncodedSize returns the canonical byte width of entry relative
// to previous. first resets the page-local delta to zero. It is the
// allocation-free sizing primitive used by packers before they borrow an
// output page.
func PostingEntryEncodedSize(previous uint32, entry PostingEntry, first bool) (int, error) {
	if entry.Bits == 0 || !first && entry.Chunk <= previous {
		return 0, fmt.Errorf("%w: unordered or empty posting", ErrInvalidWrite)
	}
	delta := entry.Chunk - previous
	if first {
		delta = 0
	}
	return postingEntryEncodedSize(delta, entry.Bits), nil
}

func validatePostingPageWrite(header PostingPageHeader, segments []PostingSegment, nextLogicalID uint64, indexHighWater uint32) (int, error) {
	if err := validatePostingPageHeader(header, len(segments), nextLogicalID, indexHighWater); err != nil {
		return 0, err
	}
	encodedBytes := 0
	previousStream := uint32(0)
	for _, segment := range segments {
		if segment.StreamID == 0 || segment.StreamID <= previousStream ||
			segment.Flags&^postingSegmentKnownFlags != 0 ||
			len(segment.Certificate) > int(^uint16(0)) ||
			segment.Flags&PostingSegmentCollision != 0 && len(segment.Certificate) == 0 ||
			!validPostingLink(segment.Next, header.LogicalID, nextLogicalID) {
			return 0, fmt.Errorf("%w: posting segment identity or flags", ErrInvalidWrite)
		}
		length, err := PostingEntriesEncodedSize(segment.Entries)
		if err != nil {
			return 0, err
		}
		encodedBytes += len(segment.Certificate) + length
		previousStream = segment.StreamID
	}
	payloadLength := uint64(PostingPagePayloadHeaderSize) + uint64(len(segments))*PostingSegmentHeaderSize + uint64(encodedBytes)
	if payloadLength > uint64(header.PageSize)-PageHeaderSize-PageTrailerSize {
		return 0, fmt.Errorf("%w: posting payload does not fit", ErrInvalidWrite)
	}
	return encodedBytes, nil
}

func validatePostingPageHeader(header PostingPageHeader, count int, nextLogicalID uint64, indexHighWater uint32) error {
	if header.StoreID == ([16]byte{}) || header.Generation == 0 ||
		header.LogicalID == 0 || header.LogicalID >= nextLogicalID ||
		!validPhysicalPageSize(header.PageSize) ||
		count <= 0 || count > int(^uint16(0)) || indexHighWater == 0 || header.IndexID >= indexHighWater {
		return fmt.Errorf("%w: posting page identity, index, or count", ErrInvalidWrite)
	}
	return nil
}

func validPostingLink(link PostingLink, currentLogicalID, nextLogicalID uint64) bool {
	if link == (PostingLink{}) {
		return true
	}
	return link.LogicalID != 0 && link.LogicalID < nextLogicalID && link.LogicalID != currentLogicalID
}

func postingSegmentRecord(payload []byte, rank int) []byte {
	start := PostingPagePayloadHeaderSize + rank*PostingSegmentHeaderSize
	return payload[start : start+PostingSegmentHeaderSize]
}

func decodePostingSegment(payload []byte, rank, dataStart int) (PostingSegmentHeader, []byte, []byte, uint16, error) {
	record := postingSegmentRecord(payload, rank)
	certificateLength := binary.LittleEndian.Uint16(record[46:48])
	offset := binary.LittleEndian.Uint32(record[32:36])
	length := binary.LittleEndian.Uint32(record[36:40])
	if offset < uint32(dataStart) || uint64(offset)+uint64(length) > uint64(len(payload)) || length == 0 {
		return PostingSegmentHeader{}, nil, nil, 0, ErrPostingPageCorrupt
	}
	header := PostingSegmentHeader{
		StreamID:   binary.LittleEndian.Uint32(record[0:4]),
		FirstChunk: binary.LittleEndian.Uint32(record[4:8]),
		LastChunk:  binary.LittleEndian.Uint32(record[8:12]),
		Rows:       binary.LittleEndian.Uint32(record[12:16]),
		TupleHash:  binary.LittleEndian.Uint64(record[16:24]),
		Next: PostingLink{
			LogicalID: binary.LittleEndian.Uint64(record[24:32]),
			Segment:   binary.LittleEndian.Uint16(record[42:44]),
		},
		Flags: binary.LittleEndian.Uint16(record[44:46]),
	}
	count := binary.LittleEndian.Uint16(record[40:42])
	if header.StreamID == 0 || count == 0 || header.LastChunk < header.FirstChunk ||
		uint32(certificateLength) >= length ||
		header.Flags&PostingSegmentCollision != 0 && certificateLength == 0 {
		return PostingSegmentHeader{}, nil, nil, 0, ErrPostingPageCorrupt
	}
	end := int(uint64(offset) + uint64(length))
	certificateEnd := int(offset) + int(certificateLength)
	return header,
		payload[int(offset):certificateEnd:certificateEnd],
		payload[certificateEnd:end:end],
		count, nil
}

func postingEntryEncodedSize(delta uint32, mask uint64) int {
	if mask&(mask-1) == 0 {
		slot := uint64(bits.TrailingZeros64(mask))
		return postingUvarintLen(uint64(delta)<<7 | slot<<1)
	}
	return postingUvarintLen(uint64(delta)<<1|1) + 8
}

func decodePostingEntry(src []byte, position int, previous uint32, first bool) (PostingEntry, int, bool) {
	if position < 0 || position >= len(src) {
		return PostingEntry{}, position, false
	}
	token, n := binary.Uvarint(src[position:])
	if n <= 0 || n != postingUvarintLen(token) {
		return PostingEntry{}, position, false
	}
	position += n
	var delta uint64
	var mask uint64
	if token&1 == 0 {
		delta = token >> 7
		slot := token >> 1 & 63
		mask = uint64(1) << slot
	} else {
		delta = token >> 1
		if position+8 > len(src) {
			return PostingEntry{}, position, false
		}
		mask = binary.LittleEndian.Uint64(src[position : position+8])
		position += 8
		if bits.OnesCount64(mask) < 2 {
			return PostingEntry{}, position, false
		}
	}
	if first {
		if delta != 0 {
			return PostingEntry{}, position, false
		}
		return PostingEntry{Chunk: previous, Bits: mask}, position, true
	}
	if delta == 0 || delta > uint64(^uint32(0))-uint64(previous) {
		return PostingEntry{}, position, false
	}
	return PostingEntry{Chunk: previous + uint32(delta), Bits: mask}, position, true
}

func postingUvarintLen(value uint64) int {
	length := 1
	for value >= 0x80 {
		value >>= 7
		length++
	}
	return length
}
