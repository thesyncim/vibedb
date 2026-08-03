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
	next        int
	cursor      int
	bit         int
	lengthBit   int
	value       int64
	width       int
	alphabetEnd int
	prefixEnd   int
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
	if v.kind == compactStreamAlphabet {
		if row < 0 || row >= v.count || row != s.next {
			return v.appendValue(dst, row)
		}
		if row%compactStreamRestart == 0 {
			block := row / compactStreamRestart
			s.cursor = int(binary.LittleEndian.Uint32(v.data[block*4:]))
			base, n, ok := readCompactUvarint(v.data[s.cursor:])
			if !ok {
				return dst, false
			}
			s.value = int64(base)
			s.cursor += n
			if s.cursor >= len(v.data) {
				return dst, false
			}
			s.width = int(v.data[s.cursor])
			s.cursor++
			rows := min(compactStreamRestart, v.count-block*compactStreamRestart)
			lengthBytes := (rows*s.width + 7) / 8
			if lengthBytes > len(v.data)-s.cursor {
				return dst, false
			}
			s.lengthBit = s.cursor * 8
			s.bit = (s.cursor + lengthBytes) * 8
		}
		if s.next == 0 {
			s.alphabetEnd = int(binary.LittleEndian.Uint16(v.dictDir))
			s.prefixEnd = s.alphabetEnd
			if v.dictCount == 3 {
				s.prefixEnd = int(binary.LittleEndian.Uint16(v.dictDir[2:]))
			}
		}
		alphabet := v.dictData[:s.alphabetEnd]
		prefix := v.dictData[s.alphabetEnd:s.prefixEnd]
		suffix := v.dictData[s.prefixEnd:]
		length := uint64(s.value) + compactReadBits(v.data, s.lengthBit, s.width)
		s.lengthBit += s.width
		if length > CommonPrimaryLeafMaxExtentBytes {
			return dst, false
		}
		endBit := s.bit + int(length)*int(v.width)
		if endBit > len(v.data)*8 {
			return dst, false
		}
		start := len(dst)
		dst = append(dst, prefix...)
		middle := len(dst)
		dst = append(dst, make([]byte, int(length))...)
		if v.width == 0 {
			for char := range int(length) {
				dst[middle+char] = alphabet[0]
			}
			s.next++
			return append(dst, suffix...), true
		}
		mask := uint16(1<<v.width) - 1
		for char := 0; char < int(length); char++ {
			byteAt := s.bit >> 3
			shift := s.bit & 7
			word := uint16(v.data[byteAt])
			if shift+int(v.width) > 8 {
				word |= uint16(v.data[byteAt+1]) << 8
			}
			code := int(word>>shift) & int(mask)
			if code >= len(alphabet) {
				return dst[:start], false
			}
			dst[middle+char] = alphabet[code]
			s.bit += int(v.width)
		}
		if s.bit != endBit {
			return dst[:start], false
		}
		s.next++
		return append(dst, suffix...), true
	}
	prefix := 0
	switch v.kind {
	case compactStreamDelta:
	case compactStreamDeltaPack:
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
	packedDelta := v.kind == compactStreamDeltaPack ||
		v.kind == compactStreamPrefixInt && v.data[0]&4 != 0
	if packedDelta {
		if row%compactStreamRestart == 0 {
			block := row / compactStreamRestart
			s.cursor = int(binary.LittleEndian.Uint32(v.data[prefix+block*4:]))
			s.value = int64(binary.LittleEndian.Uint64(v.data[s.cursor:]))
			s.width = int(v.data[s.cursor+8])
			s.cursor += 9
		} else {
			within := row%compactStreamRestart - 1
			u := compactReadBits(v.data[s.cursor:], within*s.width, s.width)
			s.value += int64(u>>1) ^ -int64(u&1)
		}
	} else if row%compactStreamRestart == 0 {
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
	if v.kind == compactStreamDelta || v.kind == compactStreamDeltaPack {
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
