package storeio

import (
	"encoding/binary"
	"strconv"
)

const (
	compactPrimaryScanShapes  = compactStreamRestart
	compactPrimaryScanStreams = 128
)

type compactPrimaryScanShape struct {
	first uint16
	holes uint16
}

type compactStreamSequentialState struct {
	next   int
	cursor int
	value  int64
}

// CompactPrimaryScanDecoder retains bounded sequential scalar state for one
// ordered compact leaf. It is caller-owned, contains no heap pointers of its
// own, and falls back to bounded random-rank decoding when a structurally
// unusual leaf has more shapes or scalar streams than this common scan lane.
// The on-disk representation and point-read restart bound are unchanged.
type CompactPrimaryScanDecoder struct {
	bucket     BucketID
	generation uint64
	lastRow    int
	prepared   bool
	supported  bool
	shapes     [compactPrimaryScanShapes]compactPrimaryScanShape
	streams    [compactPrimaryScanStreams]compactStreamSequentialState
}

func (d *CompactPrimaryScanDecoder) prepare(
	v *CompactPrimaryStripeView,
	bucket BucketID,
) {
	if d == nil || v == nil {
		return
	}
	if d.prepared && d.bucket == bucket && d.generation == v.header.Generation {
		return
	}
	d.bucket = bucket
	d.generation = v.header.Generation
	d.lastRow = -1
	d.prepared = true
	d.supported = false
	if v.shapeCount > len(d.shapes) {
		return
	}
	streamCount := 0
	for shape := 0; shape < v.shapeCount; shape++ {
		entry, ok := v.shapeEntry(shape)
		if !ok || entry.template.holes > len(d.streams)-streamCount {
			return
		}
		d.shapes[shape] = compactPrimaryScanShape{
			first: uint16(streamCount), holes: uint16(entry.template.holes),
		}
		streamCount += entry.template.holes
	}
	clear(d.streams[:streamCount])
	d.supported = true
}

func (d *CompactPrimaryScanDecoder) appendValue(
	dst []byte,
	v *CompactPrimaryStripeView,
	bucket BucketID,
	row, shape, ordinal int,
) ([]byte, bool) {
	if d == nil || v == nil {
		return v.appendValueOrdinal(dst, row, shape, ordinal)
	}
	if d.prepared && d.bucket == bucket &&
		d.generation == v.header.Generation && row <= d.lastRow {
		// A decoder may be retained and reused for another traversal of the
		// same immutable leaf. A non-increasing lexical row starts a new pass.
		d.prepared = false
	}
	d.prepare(v, bucket)
	d.lastRow = row
	if !d.supported || shape < 0 || shape >= v.shapeCount {
		return v.appendValueOrdinal(dst, row, shape, ordinal)
	}
	entry, ok := v.shapeEntry(shape)
	meta := d.shapes[shape]
	if !ok || entry.template.holes != int(meta.holes) {
		return dst, false
	}
	start := len(dst)
	streamRaw := entry.streamRaw
	previous := uint32(0)
	for hole := 0; hole < entry.template.holes; hole++ {
		end := binary.LittleEndian.Uint32(entry.template.ends[hole*4:])
		dst = append(dst, entry.template.static[previous:end]...)
		previous = end
		stream, admitted := admittedCompactStream(streamRaw)
		if !admitted {
			return dst[:start], false
		}
		state := &d.streams[int(meta.first)+hole]
		dst, ok = state.appendValue(dst, stream, ordinal)
		if !ok {
			return dst[:start], false
		}
		streamRaw = streamRaw[stream.encoded:]
	}
	return append(dst, entry.template.static[previous:]...), true
}

func (s *compactStreamSequentialState) appendValue(
	dst []byte,
	v compactStreamView,
	row int,
) ([]byte, bool) {
	prefix := 0
	switch v.kind {
	case compactStreamDelta:
	case compactStreamPrefixInt:
		if len(v.data) < 2 || v.data[0]&2 != 0 {
			return v.appendValue(dst, row)
		}
		prefix = 2
	default:
		return v.appendValue(dst, row)
	}
	if row < 0 || row >= v.count || row != s.next {
		return v.appendValue(dst, row)
	}
	if row%compactStreamRestart == 0 {
		block := row / compactStreamRestart
		at := prefix + block*4
		if len(v.data)-at < 4 {
			return dst, false
		}
		s.cursor = int(binary.LittleEndian.Uint32(v.data[at:]))
		if len(v.data)-s.cursor < 8 {
			return dst, false
		}
		s.value = int64(binary.LittleEndian.Uint64(v.data[s.cursor:]))
		s.cursor += 8
	} else {
		u, n, ok := readCompactUvarint(v.data[s.cursor:])
		if !ok {
			return dst, false
		}
		s.cursor += n
		s.value += int64(u>>1) ^ -int64(u&1)
	}
	s.next++
	if v.kind == compactStreamDelta {
		return AppendCanonicalInt(dst, s.value), true
	}
	if s.value < 0 {
		return dst, false
	}
	prefixValue, _ := v.dictionaryEntry(0)
	suffixValue, _ := v.dictionaryEntry(1)
	dst = append(dst, prefixValue...)
	digits := len(dst)
	dst = strconv.AppendUint(dst, uint64(s.value), 10)
	if v.data[0]&1 != 0 {
		width := int(v.data[1])
		n := len(dst) - digits
		if n > width {
			return dst, false
		}
		if n < width {
			gap := width - n
			for range gap {
				dst = append(dst, 0)
			}
			copy(dst[digits+gap:], dst[digits:digits+n])
			for at := digits; at < digits+gap; at++ {
				dst[at] = '0'
			}
		}
	}
	return append(dst, suffixValue...), true
}
