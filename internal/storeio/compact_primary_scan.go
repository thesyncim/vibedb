package storeio

import (
	"encoding/binary"
	"strconv"
)

const (
	compactPrimaryScanShapes  = compactStreamRestart
	compactPrimaryScanStreams = 128
	compactPrimaryScanHoles   = 16
)

type compactPrimaryScanShape struct {
	first  uint16
	holes  uint16
	ends   [compactPrimaryScanHoles + 1]uint32
	static []byte
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
	previousLen int
}

// CompactPrimaryScanDecoder retains bounded sequential scalar state for one
// ordered compact leaf. It is caller-owned and allocates no heap storage; its
// prepared slice views borrow the immutable leaf lease and must not outlive it.
// Structurally unusual leaves with more shapes or scalar streams fall back to
// bounded random-rank decoding. The on-disk representation and point-read
// restart bound are unchanged.
type CompactPrimaryScanDecoder struct {
	bucket     BucketID
	generation uint64
	lastRow    int
	prepared   bool
	supported  bool
	shapes     [compactPrimaryScanShapes]compactPrimaryScanShape
	streamView [compactPrimaryScanStreams]compactStreamView
	streams    [compactPrimaryScanStreams]compactStreamSequentialState
	key        compactStreamView
	keyState   compactStreamSequentialState
	keyPrior   [CommonPrimaryLeafMaxKeyBytes]byte
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
	d.key = v.key
	d.keyState = compactStreamSequentialState{}
	if v.shapeCount > len(d.shapes) {
		return
	}
	streamCount := 0
	for shape := 0; shape < v.shapeCount; shape++ {
		entry, ok := v.shapeEntry(shape)
		if !ok || entry.template.holes > compactPrimaryScanHoles ||
			entry.template.holes > len(d.streams)-streamCount {
			return
		}
		meta := compactPrimaryScanShape{
			first: uint16(streamCount), holes: uint16(entry.template.holes),
			static: entry.template.static,
		}
		for segment := 0; segment <= entry.template.holes; segment++ {
			meta.ends[segment] = binary.LittleEndian.Uint32(
				entry.template.ends[segment*4:],
			)
		}
		d.shapes[shape] = meta
		streamRaw := entry.streamRaw
		for hole := 0; hole < entry.template.holes; hole++ {
			stream, admitted := admittedCompactStream(streamRaw)
			if !admitted || stream.count != entry.rows {
				return
			}
			d.streamView[streamCount+hole] = stream
			streamRaw = streamRaw[stream.encoded:]
		}
		if len(streamRaw) != 0 {
			return
		}
		streamCount += entry.template.holes
	}
	clear(d.streams[:streamCount])
	d.supported = true
}

// appendKey reconstructs the ordered key stream without restarting its front
// decoder from the beginning of the 64-row block for every row. A bounded
// private prefix copy keeps the next key independent from callback mutations.
// Non-front streams and non-sequential callers retain the admitted random-rank
// decoder.
func (d *CompactPrimaryScanDecoder) appendKey(
	dst []byte,
	v *CompactPrimaryStripeView,
	bucket BucketID,
	row int,
) ([]byte, bool) {
	if d == nil || v == nil {
		return v.AppendKey(dst, row)
	}
	if d.prepared && d.bucket == bucket &&
		d.generation == v.header.Generation && row <= d.lastRow {
		// A retained decoder started another traversal of this immutable leaf.
		d.prepared = false
	}
	d.prepare(v, bucket)
	out, ok := d.keyState.appendFrontKey(
		dst, d.key, row, d.keyPrior[:d.keyState.previousLen],
	)
	if !ok || len(out) > len(d.keyPrior) {
		return dst, false
	}
	copy(d.keyPrior[:], out)
	return out, true
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
	meta := d.shapes[shape]
	if int(meta.holes) > compactPrimaryScanHoles {
		return dst, false
	}
	start := len(dst)
	previous := uint32(0)
	for hole := 0; hole < int(meta.holes); hole++ {
		end := meta.ends[hole]
		dst = append(dst, meta.static[previous:end]...)
		previous = end
		streamAt := int(meta.first) + hole
		stream := d.streamView[streamAt]
		state := &d.streams[streamAt]
		var ok bool
		dst, ok = state.appendValue(dst, stream, ordinal)
		if !ok {
			return dst[:start], false
		}
	}
	end := meta.ends[meta.holes]
	return append(dst, meta.static[previous:end]...), true
}

func (s *compactStreamSequentialState) appendFrontKey(
	dst []byte,
	v compactStreamView,
	row int,
	previous []byte,
) ([]byte, bool) {
	if v.kind != compactStreamFront || row < 0 || row >= v.count ||
		row != s.next {
		return v.appendValue(dst, row)
	}
	start := len(dst)
	if row%compactStreamRestart == 0 {
		block := row / compactStreamRestart
		if block*4+4 > len(v.data) {
			return dst, false
		}
		s.cursor = int(binary.LittleEndian.Uint32(v.data[block*4:]))
		length, n, ok := readCompactUvarint(v.data[s.cursor:])
		if !ok || length > uint64(len(v.data)) {
			return dst, false
		}
		s.cursor += n
		if int(length) > len(v.data)-s.cursor {
			return dst, false
		}
		dst = append(dst, v.data[s.cursor:s.cursor+int(length)]...)
		s.cursor += int(length)
	} else {
		if s.cursor >= len(v.data) {
			return dst, false
		}
		packed := v.data[s.cursor]
		s.cursor++
		prefix, suffix := int(packed>>4), int(packed&15)
		if packed == 0xff {
			p, n, ok := readCompactUvarint(v.data[s.cursor:])
			if !ok {
				return dst, false
			}
			s.cursor += n
			suf, n, ok := readCompactUvarint(v.data[s.cursor:])
			if !ok {
				return dst, false
			}
			s.cursor += n
			if p > uint64(^uint(0)>>1) || suf > uint64(^uint(0)>>1) {
				return dst, false
			}
			prefix, suffix = int(p), int(suf)
		}
		if prefix > len(previous) ||
			suffix > len(v.data)-s.cursor {
			return dst, false
		}
		dst = append(dst, previous[:prefix]...)
		dst = append(dst, v.data[s.cursor:s.cursor+suffix]...)
		s.cursor += suffix
	}
	s.previousLen = len(dst) - start
	s.next++
	return dst, true
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
