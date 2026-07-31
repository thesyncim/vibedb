package storeio

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
)

const (
	// TermPostingTileChunks and TermPostingTileRows define the stable universe
	// of one independently replaceable posting component. The tile preserves
	// the Store's 64-row chunk currency while bounding a dense representation
	// to 512 bytes.
	TermPostingTileChunks = 64
	TermPostingTileRows   = TermPostingTileChunks * 64

	// TermPostingInlineBytes is the maximum encoded payload owned directly by
	// a term leaf. Larger payloads are named by the leaf's manifest entry and
	// supplied to OpenTermPosting as one immutable content-addressed component.
	TermPostingInlineBytes     = 24
	TermPostingDenseBytes      = TermPostingTileRows / 8
	TermPostingMaxPayloadBytes = TermPostingDenseBytes

	// These are packed-format accounting constants for comparison and layout
	// planning. An inline owner is tile/rows/codec/length plus its variable
	// bytes. A manifest entry additionally stores a 128-bit ComponentID plus
	// compact flags/reserved bytes around the bounded 16-bit component length.
	TermPostingInlineHeaderBytes  = 8
	TermPostingManifestEntryBytes = 28

	// termPostingComponentDomain binds content identities to this component
	// type and format version. A future incompatible payload cannot alias a
	// component admitted by this decoder even when its trailing bytes match.
	termPostingComponentDomain = "VJTP\x00"
)

// TermPostingCodec is the canonical physical encoding of one term in one
// 4,096-row tile. BuildTermPosting chooses the smallest encoded payload.
// Equal-size encodings use a fixed read-cost tie break.
type TermPostingCodec uint8

const (
	TermPostingEmpty TermPostingCodec = iota
	TermPostingAllLive
	TermPostingDense
	TermPostingRuns
	TermPostingSparseMasks
	TermPostingSparseRows
)

// TermPostingPlacement says whether the term leaf owns the encoded bytes or
// owns a direct manifest entry for one immutable component. It deliberately
// does not name a posting directory or reader-visible delta layer.
type TermPostingPlacement uint8

const (
	TermPostingInline TermPostingPlacement = iota
	TermPostingManifest
)

// TermPostingComponentID is a deterministic content identity. A persistent
// interner must still compare type, length, and complete bytes after a hash
// match; the ID alone is never proof of equality.
type TermPostingComponentID [16]byte

// TermPosting is the value stored by a term leaf for one tile. Inline contains
// the complete payload when Placement is TermPostingInline. Otherwise the
// leaf stores ComponentID directly and the immutable component consists of
// EncodedBytes bytes supplied by the caller.
type TermPosting struct {
	TileID       uint32
	Rows         uint16
	EncodedBytes uint16
	Codec        TermPostingCodec
	Placement    TermPostingPlacement
	Inline       [TermPostingInlineBytes]byte
	ComponentID  TermPostingComponentID
}

// TermPostingMask is one non-empty 64-row chunk relative to a posting tile.
type TermPostingMask struct {
	Chunk uint8
	Bits  uint64
}

// TermPostingView is an admitted immutable posting. It borrows component and
// live for its lifetime. Inline bytes remain value-owned by posting.
type TermPostingView struct {
	posting   TermPosting
	component []byte
	live      *[TermPostingTileChunks]uint64
}

// TermPostingIterator streams ordered non-empty chunk masks without
// allocation or expansion into an intermediate bitmap.
type TermPostingIterator struct {
	posting   TermPosting
	component []byte
	live      *[TermPostingTileChunks]uint64

	position    int
	chunk       int
	previousRow int
	pendingRow  int
	previousEnd int
	runStart    int
	runEnd      int
	haveRun     bool
}

// ErrTermPostingCorrupt reports an invalid or non-canonical term posting.
var ErrTermPostingCorrupt = errors.New("vibejson: corrupt Store term posting")

func (c TermPostingCodec) String() string {
	switch c {
	case TermPostingEmpty:
		return "empty"
	case TermPostingAllLive:
		return "all-live"
	case TermPostingDense:
		return "dense"
	case TermPostingRuns:
		return "runs"
	case TermPostingSparseMasks:
		return "sparse-masks"
	case TermPostingSparseRows:
		return "sparse-rows"
	default:
		return fmt.Sprintf("term-posting-codec(%d)", uint8(c))
	}
}

// BuildTermPosting constructs the one canonical representation for posting.
// posting must be a subset of live. When the selected payload fits inline,
// componentBytes is zero and dst is untouched. Otherwise exactly
// componentBytes bytes at the start of dst contain the immutable component.
// The successful path performs no allocation.
func BuildTermPosting(
	dst []byte,
	tileID uint32,
	posting, live *[TermPostingTileChunks]uint64,
) (record TermPosting, componentBytes int, err error) {
	if posting == nil || live == nil {
		return TermPosting{}, 0, fmt.Errorf("%w: nil term posting tile", ErrInvalidWrite)
	}
	for chunk := range TermPostingTileChunks {
		if posting[chunk]&^live[chunk] != 0 {
			return TermPosting{}, 0, fmt.Errorf("%w: posting contains a dead stable slot", ErrInvalidWrite)
		}
	}
	codec, encodedBytes, rows := selectTermPostingCodec(posting, live)
	record = TermPosting{
		TileID: tileID, Rows: rows, EncodedBytes: uint16(encodedBytes),
		Codec: codec, Placement: TermPostingInline,
	}
	if encodedBytes <= TermPostingInlineBytes {
		if !encodeTermPostingPayload(record.Inline[:encodedBytes], codec, posting) {
			return TermPosting{}, 0, fmt.Errorf("%w: encode inline term posting", ErrInvalidWrite)
		}
		return record, 0, nil
	}
	if encodedBytes > len(dst) {
		return TermPosting{}, 0, fmt.Errorf("%w: term posting component capacity", ErrInvalidWrite)
	}
	record.Placement = TermPostingManifest
	component := dst[:encodedBytes:encodedBytes]
	if !encodeTermPostingPayload(component, codec, posting) {
		return TermPosting{}, 0, fmt.Errorf("%w: encode term posting component", ErrInvalidWrite)
	}
	record.ComponentID = termPostingComponentID(codec, rows, component)
	return record, encodedBytes, nil
}

// OpenTermPosting validates placement, content identity, canonical codec,
// canonical varints/runs, row count, and live-slot containment once. It does
// not allocate and does not retain any temporary expanded bitmap.
func OpenTermPosting(
	posting TermPosting,
	component []byte,
	live *[TermPostingTileChunks]uint64,
) (TermPostingView, error) {
	if live == nil || posting.Codec > TermPostingSparseRows ||
		posting.Placement > TermPostingManifest ||
		posting.Rows > TermPostingTileRows ||
		posting.EncodedBytes > TermPostingMaxPayloadBytes {
		return TermPostingView{}, termPostingCorrupt("record header")
	}
	encodedBytes := int(posting.EncodedBytes)
	var payload []byte
	switch posting.Placement {
	case TermPostingInline:
		if encodedBytes > TermPostingInlineBytes ||
			len(component) != 0 ||
			posting.ComponentID != (TermPostingComponentID{}) ||
			!zeroTermPostingTail(posting.Inline[encodedBytes:]) {
			return TermPostingView{}, termPostingCorrupt("inline placement")
		}
		payload = posting.Inline[:encodedBytes:encodedBytes]
	case TermPostingManifest:
		if encodedBytes <= TermPostingInlineBytes ||
			len(component) != encodedBytes ||
			posting.ComponentID == (TermPostingComponentID{}) ||
			!zeroTermPostingTail(posting.Inline[:]) ||
			termPostingComponentID(posting.Codec, posting.Rows, component) != posting.ComponentID {
			return TermPostingView{}, termPostingCorrupt("manifest placement or identity")
		}
		payload = component
	}

	var decoded [TermPostingTileChunks]uint64
	rows, ok := decodeTermPostingPayload(&decoded, posting.Codec, payload, live)
	if !ok || rows != posting.Rows {
		return TermPostingView{}, termPostingCorrupt("payload or row count")
	}
	for chunk := range TermPostingTileChunks {
		if decoded[chunk]&^live[chunk] != 0 {
			return TermPostingView{}, termPostingCorrupt("dead stable slot")
		}
	}
	codec, size, selectedRows := selectTermPostingCodec(&decoded, live)
	if codec != posting.Codec || size != encodedBytes || selectedRows != posting.Rows {
		return TermPostingView{}, termPostingCorrupt("non-canonical codec")
	}
	return TermPostingView{
		posting: posting, component: component, live: live,
	}, nil
}

// Iterator returns an independent, zero-allocation cursor.
func (v TermPostingView) Iterator() TermPostingIterator {
	return TermPostingIterator{
		posting: v.posting, component: v.component, live: v.live,
		chunk: -1, previousRow: -1, pendingRow: -1,
	}
}

// MasksInto expands the admitted posting into caller-owned mutation scratch.
// It is the COW update/delete bridge: callers toggle stable-slot bits in dst
// and pass the result back to BuildTermPosting, while the old view remains
// immutable for snapshots. The successful path performs no allocation.
func (v TermPostingView) MasksInto(
	dst *[TermPostingTileChunks]uint64,
) bool {
	if dst == nil || v.live == nil {
		return false
	}
	var payload []byte
	if v.posting.Placement == TermPostingInline {
		n := int(v.posting.EncodedBytes)
		payload = v.posting.Inline[:n:n]
	} else {
		payload = v.component
	}
	rows, ok := decodeTermPostingPayload(
		dst, v.posting.Codec, payload, v.live,
	)
	return ok && rows == v.posting.Rows
}

// Next returns the next ordered non-empty chunk mask.
func (it *TermPostingIterator) Next() (TermPostingMask, bool) {
	if it == nil {
		return TermPostingMask{}, false
	}
	switch it.posting.Codec {
	case TermPostingEmpty:
		return TermPostingMask{}, false
	case TermPostingAllLive:
		for it.chunk++; it.chunk < TermPostingTileChunks; it.chunk++ {
			if mask := it.live[it.chunk]; mask != 0 {
				return TermPostingMask{Chunk: uint8(it.chunk), Bits: mask}, true
			}
		}
		return TermPostingMask{}, false
	case TermPostingDense:
		payload := it.payload()
		for it.chunk++; it.chunk < TermPostingTileChunks; it.chunk++ {
			start := it.chunk * 8
			if mask := binary.LittleEndian.Uint64(payload[start : start+8]); mask != 0 {
				return TermPostingMask{Chunk: uint8(it.chunk), Bits: mask}, true
			}
		}
		return TermPostingMask{}, false
	case TermPostingSparseMasks:
		return it.nextSparseMask()
	case TermPostingSparseRows:
		return it.nextSparseRowsMask()
	case TermPostingRuns:
		return it.nextRunsMask()
	default:
		return TermPostingMask{}, false
	}
}

func (it *TermPostingIterator) payload() []byte {
	if it.posting.Placement == TermPostingInline {
		n := int(it.posting.EncodedBytes)
		return it.posting.Inline[:n:n]
	}
	return it.component
}

func (it *TermPostingIterator) nextSparseMask() (TermPostingMask, bool) {
	payload := it.payload()
	if it.position >= len(payload) {
		return TermPostingMask{}, false
	}
	token, n := trustedTermPostingUvarint(payload[it.position:])
	it.position += n
	delta := uint64(0)
	mask := uint64(0)
	if token&1 == 0 {
		delta = token >> 7
		mask = uint64(1) << ((token >> 1) & 63)
	} else {
		delta = token >> 1
		mask = binary.LittleEndian.Uint64(payload[it.position : it.position+8])
		it.position += 8
	}
	if it.chunk < 0 {
		it.chunk = int(delta)
	} else {
		it.chunk += int(delta)
	}
	return TermPostingMask{Chunk: uint8(it.chunk), Bits: mask}, true
}

func (it *TermPostingIterator) nextSparseRowsMask() (TermPostingMask, bool) {
	row, ok := it.nextSparseRow()
	if !ok {
		return TermPostingMask{}, false
	}
	chunk := row >> 6
	mask := uint64(1) << (row & 63)
	for {
		row, ok = it.readSparseRow()
		if !ok {
			break
		}
		if row>>6 != chunk {
			it.pendingRow = row
			break
		}
		mask |= uint64(1) << (row & 63)
	}
	return TermPostingMask{Chunk: uint8(chunk), Bits: mask}, true
}

func (it *TermPostingIterator) nextSparseRow() (int, bool) {
	if it.pendingRow >= 0 {
		row := it.pendingRow
		it.pendingRow = -1
		return row, true
	}
	return it.readSparseRow()
}

func (it *TermPostingIterator) readSparseRow() (int, bool) {
	payload := it.payload()
	if it.position >= len(payload) {
		return 0, false
	}
	delta, n := trustedTermPostingUvarint(payload[it.position:])
	it.position += n
	it.previousRow += int(delta)
	return it.previousRow, true
}

func (it *TermPostingIterator) nextRunsMask() (TermPostingMask, bool) {
	if !it.haveRun && !it.loadRun() {
		return TermPostingMask{}, false
	}
	chunk := it.runStart >> 6
	mask := uint64(0)
	for {
		chunkEnd := min((chunk+1)<<6, it.runEnd)
		mask |= termPostingRangeMask(it.runStart&63, chunkEnd-(chunk<<6))
		it.runStart = chunkEnd
		if it.runStart < it.runEnd {
			break
		}
		it.haveRun = false
		if !it.loadRun() || it.runStart>>6 != chunk {
			break
		}
	}
	return TermPostingMask{Chunk: uint8(chunk), Bits: mask}, true
}

func (it *TermPostingIterator) loadRun() bool {
	payload := it.payload()
	if it.position >= len(payload) {
		return false
	}
	gap, n := trustedTermPostingUvarint(payload[it.position:])
	it.position += n
	lengthMinusOne, n := trustedTermPostingUvarint(payload[it.position:])
	it.position += n
	it.runStart = it.previousEnd + int(gap)
	it.runEnd = it.runStart + int(lengthMinusOne) + 1
	it.previousEnd = it.runEnd
	it.haveRun = true
	return true
}

func selectTermPostingCodec(
	posting, live *[TermPostingTileChunks]uint64,
) (TermPostingCodec, int, uint16) {
	rows := 0
	fullLive := true
	for chunk := range TermPostingTileChunks {
		rows += bits.OnesCount64(posting[chunk])
		fullLive = fullLive &&
			posting[chunk] == ^uint64(0) &&
			live[chunk] == ^uint64(0)
	}
	if rows == 0 {
		return TermPostingEmpty, 0, 0
	}
	// A zero-byte posting must be independent of mutable liveness. Restrict it
	// to a completely occupied tile: a partial tile equal to today's live mask
	// could otherwise gain a row when a deleted stable slot is reused without
	// rewriting this term. Partial tiles always carry self-contained bits.
	if fullLive {
		return TermPostingAllLive, 0, uint16(rows)
	}

	// Start with dense so it wins exact byte ties: it has the cheapest seek and
	// intersection path. Strictly smaller candidates replace it in deterministic
	// read-cost order.
	codec, size := TermPostingDense, TermPostingDenseBytes
	if candidate := termPostingRunsSize(posting); candidate < size {
		codec, size = TermPostingRuns, candidate
	}
	if candidate := termPostingSparseMasksSize(posting); candidate < size {
		codec, size = TermPostingSparseMasks, candidate
	}
	if candidate := termPostingSparseRowsSize(posting); candidate < size {
		codec, size = TermPostingSparseRows, candidate
	}
	return codec, size, uint16(rows)
}

func encodeTermPostingPayload(
	dst []byte,
	codec TermPostingCodec,
	posting *[TermPostingTileChunks]uint64,
) bool {
	position := 0
	switch codec {
	case TermPostingEmpty, TermPostingAllLive:
		return len(dst) == 0
	case TermPostingDense:
		if len(dst) != TermPostingDenseBytes {
			return false
		}
		for chunk, mask := range posting {
			binary.LittleEndian.PutUint64(dst[chunk*8:chunk*8+8], mask)
		}
		position = len(dst)
	case TermPostingSparseRows:
		previous := -1
		for chunk, mask := range posting {
			for mask != 0 {
				slot := bits.TrailingZeros64(mask)
				row := chunk*64 + slot
				position += binary.PutUvarint(dst[position:], uint64(row-previous))
				previous = row
				mask &= mask - 1
			}
		}
	case TermPostingSparseMasks:
		previous, first := 0, true
		for chunk, mask := range posting {
			if mask == 0 {
				continue
			}
			delta := chunk - previous
			if first {
				delta = chunk
				first = false
			}
			if mask&(mask-1) == 0 {
				token := uint64(delta)<<7 | uint64(bits.TrailingZeros64(mask))<<1
				position += binary.PutUvarint(dst[position:], token)
			} else {
				position += binary.PutUvarint(dst[position:], uint64(delta)<<1|1)
				binary.LittleEndian.PutUint64(dst[position:position+8], mask)
				position += 8
			}
			previous = chunk
		}
	case TermPostingRuns:
		previousEnd := 0
		for row := 0; row < TermPostingTileRows; {
			start, ok := nextTermPostingSetRow(posting, row)
			if !ok {
				break
			}
			end := start + 1
			for end < TermPostingTileRows && termPostingRowSet(posting, end) {
				end++
			}
			position += binary.PutUvarint(dst[position:], uint64(start-previousEnd))
			position += binary.PutUvarint(dst[position:], uint64(end-start-1))
			previousEnd = end
			row = end
		}
	default:
		return false
	}
	return position == len(dst)
}

func decodeTermPostingPayload(
	dst *[TermPostingTileChunks]uint64,
	codec TermPostingCodec,
	payload []byte,
	live *[TermPostingTileChunks]uint64,
) (uint16, bool) {
	clear(dst[:])
	rows := 0
	switch codec {
	case TermPostingEmpty:
		return 0, len(payload) == 0
	case TermPostingAllLive:
		if len(payload) != 0 {
			return 0, false
		}
		for chunk, mask := range live {
			dst[chunk] = mask
			rows += bits.OnesCount64(mask)
		}
		return uint16(rows), rows != 0
	case TermPostingDense:
		if len(payload) != TermPostingDenseBytes {
			return 0, false
		}
		for chunk := range TermPostingTileChunks {
			mask := binary.LittleEndian.Uint64(payload[chunk*8 : chunk*8+8])
			dst[chunk] = mask
			rows += bits.OnesCount64(mask)
		}
	case TermPostingSparseRows:
		position, previous := 0, -1
		for position < len(payload) {
			delta, n := canonicalTermPostingUvarint(payload[position:])
			if n == 0 || delta == 0 ||
				delta > uint64(TermPostingTileRows-1-previous) {
				return 0, false
			}
			position += n
			row := previous + int(delta)
			if row < 0 || row >= TermPostingTileRows {
				return 0, false
			}
			dst[row>>6] |= uint64(1) << (row & 63)
			previous = row
			rows++
		}
	case TermPostingSparseMasks:
		position, previous, entries := 0, 0, 0
		for position < len(payload) {
			token, n := canonicalTermPostingUvarint(payload[position:])
			if n == 0 {
				return 0, false
			}
			position += n
			delta := uint64(0)
			mask := uint64(0)
			if token&1 == 0 {
				delta = token >> 7
				mask = uint64(1) << ((token >> 1) & 63)
			} else {
				delta = token >> 1
				if len(payload)-position < 8 {
					return 0, false
				}
				mask = binary.LittleEndian.Uint64(payload[position : position+8])
				position += 8
				if bits.OnesCount64(mask) < 2 {
					return 0, false
				}
			}
			if entries == 0 && delta >= TermPostingTileChunks ||
				entries != 0 &&
					(delta == 0 ||
						delta > uint64(TermPostingTileChunks-1-previous)) {
				return 0, false
			}
			chunk := int(delta)
			if entries != 0 {
				chunk += previous
			}
			if chunk < 0 || chunk >= TermPostingTileChunks || mask == 0 {
				return 0, false
			}
			dst[chunk] = mask
			rows += bits.OnesCount64(mask)
			previous = chunk
			entries++
		}
		if entries == 0 {
			return 0, false
		}
	case TermPostingRuns:
		position, previousEnd, runCount := 0, 0, 0
		for position < len(payload) {
			gap, n := canonicalTermPostingUvarint(payload[position:])
			if n == 0 {
				return 0, false
			}
			position += n
			lengthMinusOne, n := canonicalTermPostingUvarint(payload[position:])
			if n == 0 {
				return 0, false
			}
			position += n
			if runCount != 0 && gap == 0 {
				return 0, false
			}
			if gap > uint64(TermPostingTileRows-previousEnd) {
				return 0, false
			}
			start := previousEnd + int(gap)
			length := lengthMinusOne + 1
			if length == 0 ||
				length > uint64(TermPostingTileRows-start) {
				return 0, false
			}
			end := start + int(length)
			setTermPostingRange(dst, start, end)
			rows += int(length)
			previousEnd = end
			runCount++
		}
		if runCount == 0 {
			return 0, false
		}
	default:
		return 0, false
	}
	return uint16(rows), rows != 0
}

func termPostingSparseRowsSize(posting *[TermPostingTileChunks]uint64) int {
	size, previous := 0, -1
	for chunk, mask := range posting {
		for mask != 0 {
			row := chunk*64 + bits.TrailingZeros64(mask)
			size += termPostingUvarintSize(uint64(row - previous))
			previous = row
			mask &= mask - 1
		}
	}
	return size
}

func termPostingSparseMasksSize(posting *[TermPostingTileChunks]uint64) int {
	size, previous, first := 0, 0, true
	for chunk, mask := range posting {
		if mask == 0 {
			continue
		}
		delta := chunk - previous
		if first {
			delta = chunk
			first = false
		}
		if mask&(mask-1) == 0 {
			token := uint64(delta)<<7 | uint64(bits.TrailingZeros64(mask))<<1
			size += termPostingUvarintSize(token)
		} else {
			size += termPostingUvarintSize(uint64(delta)<<1|1) + 8
		}
		previous = chunk
	}
	return size
}

func termPostingRunsSize(posting *[TermPostingTileChunks]uint64) int {
	size, previousEnd := 0, 0
	for row := 0; row < TermPostingTileRows; {
		start, ok := nextTermPostingSetRow(posting, row)
		if !ok {
			break
		}
		end := start + 1
		for end < TermPostingTileRows && termPostingRowSet(posting, end) {
			end++
		}
		size += termPostingUvarintSize(uint64(start - previousEnd))
		size += termPostingUvarintSize(uint64(end - start - 1))
		previousEnd = end
		row = end
	}
	return size
}

func canonicalTermPostingUvarint(src []byte) (uint64, int) {
	value, n := binary.Uvarint(src)
	if n <= 0 || n != termPostingUvarintSize(value) {
		return 0, 0
	}
	return value, n
}

// trustedTermPostingUvarint is used only after OpenTermPosting has admitted
// the complete payload. Posting deltas overwhelmingly fit one byte; keeping
// that path inline avoids paying the general decoder on every emitted mask.
func trustedTermPostingUvarint(src []byte) (uint64, int) {
	if src[0] < 0x80 {
		return uint64(src[0]), 1
	}
	return binary.Uvarint(src)
}

func termPostingUvarintSize(value uint64) int {
	size := 1
	for value >= 0x80 {
		value >>= 7
		size++
	}
	return size
}

func termPostingComponentID(
	codec TermPostingCodec,
	rows uint16,
	payload []byte,
) TermPostingComponentID {
	const headerBytes = len(termPostingComponentDomain) + 1 + 2 + 2
	var material [headerBytes + TermPostingMaxPayloadBytes]byte
	copy(material[:], termPostingComponentDomain)
	offset := len(termPostingComponentDomain)
	material[offset] = byte(codec)
	binary.LittleEndian.PutUint16(material[offset+1:offset+3], rows)
	binary.LittleEndian.PutUint16(material[offset+3:offset+5], uint16(len(payload)))
	copy(material[headerBytes:], payload)
	sum := sha256.Sum256(material[:headerBytes+len(payload)])
	var id TermPostingComponentID
	copy(id[:], sum[:len(id)])
	return id
}

func nextTermPostingSetRow(
	posting *[TermPostingTileChunks]uint64,
	from int,
) (int, bool) {
	if from < 0 {
		from = 0
	}
	if from >= TermPostingTileRows {
		return 0, false
	}
	chunk := from >> 6
	mask := posting[chunk] & (^uint64(0) << (from & 63))
	for {
		if mask != 0 {
			return chunk*64 + bits.TrailingZeros64(mask), true
		}
		chunk++
		if chunk == TermPostingTileChunks {
			return 0, false
		}
		mask = posting[chunk]
	}
}

func termPostingRowSet(posting *[TermPostingTileChunks]uint64, row int) bool {
	return posting[row>>6]&(uint64(1)<<(row&63)) != 0
}

func setTermPostingRange(
	posting *[TermPostingTileChunks]uint64,
	start, end int,
) {
	for start < end {
		chunk := start >> 6
		chunkEnd := min((chunk+1)<<6, end)
		posting[chunk] |= termPostingRangeMask(start&63, chunkEnd-(chunk<<6))
		start = chunkEnd
	}
}

func termPostingRangeMask(start, end int) uint64 {
	if start < 0 || end <= start || end > 64 {
		return 0
	}
	upper := ^uint64(0)
	if end != 64 {
		upper = uint64(1)<<end - 1
	}
	lower := uint64(0)
	if start != 0 {
		lower = uint64(1)<<start - 1
	}
	return upper &^ lower
}

func zeroTermPostingTail(src []byte) bool {
	var combined byte
	for _, value := range src {
		combined |= value
	}
	return combined == 0
}

func termPostingCorrupt(what string) error {
	return fmt.Errorf("%w: %s", ErrTermPostingCorrupt, what)
}
