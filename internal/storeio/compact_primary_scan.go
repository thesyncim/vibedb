package storeio

import (
	"encoding/binary"
	"strconv"
)

const (
	compactPrimaryScanShapes  = compactStreamRestart
	compactPrimaryScanStreams = 128
	compactPrimaryScanHoles   = 16
	// Dictionary boundaries are decoded once per leaf into one shared bounded
	// pool. A stream that would overflow the pool simply keeps the admitted
	// random-rank decoder; other streams in the leaf still specialize.
	compactPrimaryScanDictionaryBounds = 512
	// Dictionary fragments combine the static bytes preceding a hole with each
	// dictionary spelling. The common 4K-row shape set needs 6,717 bytes; keep a
	// bounded power-of-two pool and let unusual leaves fall back per stream.
	compactPrimaryScanDictionaryFragmentBytes = 8 << 10
)

type compactPrimaryScanShape struct {
	first  uint16
	holes  uint16
	ends   [compactPrimaryScanHoles + 1]uint32
	static []byte
}

type compactPrimaryScanStream struct {
	dictionaryFirst uint16
	dictionaryCount uint16
}

type compactStreamSequentialState struct {
	next         int
	cursor       int
	bit          int
	lengthBit    int
	value        int64
	width        int
	alphabetEnd  int
	prefixEnd    int
	previousLen  int32
	alphabetBase uint16 // Base+1 for a contiguous alphabet; zero keeps scalar lookup.
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
	streamPlan [compactPrimaryScanStreams]compactPrimaryScanStream
	streams    [compactPrimaryScanStreams]compactStreamSequentialState
	dictionary [compactPrimaryScanDictionaryBounds]uint16
	fragments  [compactPrimaryScanDictionaryFragmentBytes]byte
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
	clear(d.streamPlan[:])
	streamCount := 0
	dictionaryCount := 0
	fragmentCount := 0
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
			if !admitted || !stream.matchesShapeRows(entry.rows, v.rows) {
				return
			}
			d.streamView[streamCount+hole] = stream
			prefixStart := uint32(0)
			if hole != 0 {
				prefixStart = meta.ends[hole-1]
			}
			prefixEnd := meta.ends[hole]
			prefixBytes := int(prefixEnd - prefixStart)
			fragmentBytes := prefixBytes*stream.dictCount + len(stream.dictData)
			if stream.kind == compactStreamDictionary &&
				stream.dictCount > 0 &&
				stream.dictCount+1 <= len(d.dictionary)-dictionaryCount &&
				fragmentBytes <= len(d.fragments)-fragmentCount {
				plan := &d.streamPlan[streamCount+hole]
				plan.dictionaryFirst = uint16(dictionaryCount)
				plan.dictionaryCount = uint16(stream.dictCount)
				d.dictionary[dictionaryCount] = uint16(fragmentCount)
				dictionaryStart := 0
				for id := 0; id < stream.dictCount; id++ {
					dictionaryEnd := int(binary.LittleEndian.Uint16(stream.dictDir[id*2:]))
					fragmentCount += copy(
						d.fragments[fragmentCount:], meta.static[prefixStart:prefixEnd],
					)
					fragmentCount += copy(
						d.fragments[fragmentCount:], stream.dictData[dictionaryStart:dictionaryEnd],
					)
					d.dictionary[dictionaryCount+id+1] = uint16(fragmentCount)
					dictionaryStart = dictionaryEnd
				}
				dictionaryCount += stream.dictCount + 1
			}
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
// Bounded starts prime the nearest restart once; numeric keys use the scalar
// sequential decoder. Other representations retain admitted random-rank decoding.
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
	var out []byte
	var ok bool
	if d.key.kind == compactStreamFront {
		if row >= 0 && row < d.key.count && row != d.keyState.next {
			d.keyState = compactStreamSequentialState{next: row / compactStreamRestart * compactStreamRestart}
			for d.keyState.next < row {
				prior, valid := d.keyState.appendFrontKey(d.keyPrior[:0], &d.key,
					d.keyState.next, d.keyPrior[:d.keyState.previousLen])
				if !valid || len(prior) > len(d.keyPrior) {
					return dst, false
				}
			}
		}
		out, ok = d.keyState.appendFrontKey(dst, &d.key, row, d.keyPrior[:d.keyState.previousLen])
	} else if d.key.kind == compactStreamPrefixInt &&
		len(d.key.data) == 18 && d.key.data[0]&2 != 0 {
		out, ok = d.keyState.appendArithmeticPrefixKey(dst, &d.key, row)
	} else {
		if row != d.keyState.next {
			d.keyState.seek(&d.key, row)
		}
		out, ok = d.keyState.appendValue(dst, &d.key, row)
	}
	if !ok || len(out) > len(d.keyPrior) {
		return dst, false
	}
	copy(d.keyPrior[:], out)
	return out, true
}

func (s *compactStreamSequentialState) appendArithmeticPrefixKey(
	dst []byte,
	v *compactStreamView,
	row int,
) ([]byte, bool) {
	if row < 0 || row >= v.count || row != s.next {
		return v.appendValue(dst, row)
	}
	first := int64(binary.LittleEndian.Uint64(v.data[2:]))
	delta := int64(binary.LittleEndian.Uint64(v.data[10:]))
	value := first + int64(row)*delta
	out, ok := appendCompactPrefixUint(dst, v, value)
	if !ok {
		return dst, false
	}
	s.value = value
	s.next++
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
	// Borrow decoder-owned metadata for this synchronous render. On 64-bit
	// targets the shape is 96 bytes and each stream view is 104 bytes, so copying
	// either inside the row/hole loop would turn metadata into scan bandwidth.
	meta := &d.shapes[shape]
	if int(meta.holes) > compactPrimaryScanHoles {
		return dst, false
	}
	start := len(dst)
	previous := uint32(0)
	for hole := 0; hole < int(meta.holes); hole++ {
		end := meta.ends[hole]
		streamAt := int(meta.first) + hole
		stream := &d.streamView[streamAt]
		var ok bool
		plan := d.streamPlan[streamAt]
		if plan.dictionaryCount != 0 {
			dst, ok = d.appendDictionaryFragment(dst, streamAt, ordinal)
		} else {
			dst = append(dst, meta.static[previous:end]...)
			state := &d.streams[streamAt]
			coordinate := stream.shapeCoordinate(row, ordinal)
			if stream.kind != compactStreamRankAffine && coordinate != state.next {
				state.seek(stream, coordinate)
			}
			dst, ok = state.appendValue(dst, stream, coordinate)
		}
		previous = end
		if !ok {
			return dst[:start], false
		}
	}
	end := meta.ends[meta.holes]
	return append(dst, meta.static[previous:end]...), true
}

func (d *CompactPrimaryScanDecoder) appendDictionaryFragment(
	dst []byte,
	streamAt int,
	row int,
) ([]byte, bool) {
	v := &d.streamView[streamAt]
	s := &d.streams[streamAt]
	if row != s.next {
		s.seek(v, row)
		if row != s.next {
			return dst, false
		}
	}
	// prepare admitted the complete immutable stream, decoded every dictionary
	// boundary, and installed this plan only when both bounded pools fit. Keep
	// the row loop to state synchronization and packed-ID consumption; repeating
	// the stream grammar and fragment geometry checks for every value made those
	// already-proven branches a material fraction of scan time.
	plan := d.streamPlan[streamAt]
	first := int(plan.dictionaryFirst)
	width := int(v.width)
	reservoir := uint64(s.value)
	available := s.bit
	cursor := s.cursor
	for available < width {
		reservoir |= uint64(v.data[cursor]) << uint(available)
		cursor++
		available += 8
	}
	id := 0
	if width != 0 {
		id = int(reservoir & (uint64(1)<<uint(width) - 1))
	}
	start, end := int(d.dictionary[first+id]), int(d.dictionary[first+id+1])
	reservoir >>= uint(width)
	available -= width
	s.value = int64(reservoir)
	s.bit = available
	s.cursor = cursor
	s.next++
	fragmentLen := end - start
	// The admitted corpus is dominated by 15-byte fragments. A fixed copy
	// lowers to one unaligned vector load/store instead of runtime.memmove.
	if fragmentLen <= 16 && cap(dst)-len(dst) >= 16 {
		at := len(dst)
		dst = dst[:at+16]
		*(*[16]byte)(dst[at:]) = *(*[16]byte)(d.fragments[start:])
		return dst[:at+fragmentLen], true
	}
	return append(dst, d.fragments[start:end]...), true
}

func (s *compactStreamSequentialState) appendDictionary(
	dst []byte,
	v *compactStreamView,
	row int,
	bounds []uint16,
) ([]byte, bool) {
	if v.kind != compactStreamDictionary || row < 0 || row >= v.count ||
		row != s.next || len(bounds) != v.dictCount+1 {
		return v.appendValue(dst, row)
	}
	width := int(v.width)
	// Dictionary geometry admits widths 0..16. The stream has already passed
	// the complete compact grammar, but keep this local gate so a malformed
	// direct view cannot overflow the reservoir or index beyond its data.
	if v.dictCount <= 0 || width > 16 {
		return dst, false
	}
	// Carry unused little-endian ID bits between sequential rows instead of
	// recomputing an absolute bit offset and crossing bytes from scratch. These
	// fields are otherwise unused by dictionary streams, so the scan decoder's
	// bounded footprint remains unchanged: value is the low-bit reservoir, bit
	// is its available-bit count, and cursor is the next source byte.
	reservoir := uint64(s.value)
	available := s.bit
	cursor := s.cursor
	for available < width {
		if cursor >= len(v.data) {
			return dst, false
		}
		reservoir |= uint64(v.data[cursor]) << uint(available)
		cursor++
		available += 8
	}
	id := 0
	if width != 0 {
		id = int(reservoir & (uint64(1)<<uint(width) - 1))
	}
	if id < 0 || id >= v.dictCount {
		return dst, false
	}
	start, end := int(bounds[id]), int(bounds[id+1])
	if end < start || end > len(v.dictData) {
		return dst, false
	}
	reservoir >>= uint(width)
	available -= width
	s.value = int64(reservoir)
	s.bit = available
	s.cursor = cursor
	s.next++
	return append(dst, v.dictData[start:end]...), true
}

func (s *compactStreamSequentialState) appendFrontKey(
	dst []byte,
	v *compactStreamView,
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
	s.previousLen = int32(len(dst) - start)
	s.next++
	return dst, true
}

func (s *compactStreamSequentialState) appendValue(
	dst []byte,
	v *compactStreamView,
	row int,
) ([]byte, bool) {
	if v.kind == compactStreamAlphabet {
		if row < 0 || row >= v.count || row != s.next {
			return v.appendValue(dst, row)
		}
		return s.appendAlphabet(dst, v, row)
	}
	prefix := 0
	switch v.kind {
	case compactStreamFOR:
		if row < 0 || row >= v.count || row != s.next {
			return v.appendValue(dst, row)
		}
		if len(v.data) < 8 {
			return dst, false
		}
		if v.width > 56 {
			return v.appendValue(dst, row)
		}
		width := int(v.width)
		data := v.data[8:]
		reservoir := uint64(s.value)
		available := s.bit
		cursor := s.cursor
		for available < width {
			if cursor >= len(data) {
				return dst, false
			}
			reservoir |= uint64(data[cursor]) << uint(available)
			cursor++
			available += 8
		}
		offset := reservoir & (uint64(1)<<uint(width) - 1)
		reservoir >>= uint(width)
		available -= width
		s.value = int64(reservoir)
		s.bit = available
		s.cursor = cursor
		s.next++
		base := int64(binary.LittleEndian.Uint64(v.data))
		return AppendCanonicalInt(dst, base+int64(offset)), true
	case compactStreamDate:
		if row < 0 || row >= v.count || row != s.next {
			return v.appendValue(dst, row)
		}
		if len(v.data) < 4 {
			return dst, false
		}
		if v.width > 56 {
			return v.appendValue(dst, row)
		}
		width := int(v.width)
		data := v.data[4:]
		reservoir := uint64(s.value)
		available := s.bit
		cursor := s.cursor
		for available < width {
			if cursor >= len(data) {
				return dst, false
			}
			reservoir |= uint64(data[cursor]) << uint(available)
			cursor++
			available += 8
		}
		offset := reservoir & (uint64(1)<<uint(width) - 1)
		reservoir >>= uint(width)
		available -= width
		s.value = int64(reservoir)
		s.bit = available
		s.cursor = cursor
		s.next++
		base := int32(binary.LittleEndian.Uint32(v.data))
		return appendCompactDate(dst, base+int32(offset)), true
	case compactStreamRankAffine:
		return s.appendRankAffine(dst, v, row)
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
	return appendCompactPrefixUint(dst, v, s.value)
}

// appendRankAffine evaluates the admitted physical-rank arithmetic directly.
// Rank-affine streams are complete leaf domains, so the row is already the
// physical rank. Keeping this path pointer-based avoids copying the 104-byte
// stream view and renders the shared prefix/suffix without the generic
// random-rank decoder.
func (s *compactStreamSequentialState) appendRankAffine(
	dst []byte,
	v *compactStreamView,
	row int,
) ([]byte, bool) {
	if v.kind != compactStreamRankAffine || row < 0 || row >= v.count ||
		len(v.data) != 18 {
		return v.appendValue(dst, row)
	}
	base := int64(binary.LittleEndian.Uint64(v.data[2:]))
	step := int64(binary.LittleEndian.Uint64(v.data[10:]))
	value := base + step*int64(row)
	out, ok := appendCompactPrefixUint(dst, v, value)
	if !ok {
		return dst, false
	}
	s.value = value
	// Physical ranks contain shape gaps, so this stream cannot use the
	// ordinal-style next-row synchronization. Retain the last rank only for
	// diagnostics; every admitted rank is independently arithmetic.
	s.next = row + 1
	return out, true
}

// appendCompactPrefixUint renders an admitted nonnegative prefix-integer
// spelling. The small-value path is intentionally shared by sequential
// prefix and rank-affine scans; strconv remains only for larger values.
func appendCompactPrefixUint(dst []byte, v *compactStreamView, value int64) ([]byte, bool) {
	if value < 0 {
		return dst, false
	}
	start := len(dst)
	prefix, prefixOK := v.dictionaryEntry(0)
	suffix, suffixOK := v.dictionaryEntry(1)
	if !prefixOK || !suffixOK {
		return dst, false
	}
	dst = append(dst, prefix...)
	digits := len(dst)
	width := 0
	if v.data[0]&1 != 0 {
		width = int(v.data[1])
	}
	if width == 8 && value < 100_000_000 {
		dst = appendFixedUint8(dst, uint32(value))
	} else if value < 1_000_000 {
		dst = appendCanonicalUint6(dst, uint64(value))
	} else {
		dst = strconv.AppendUint(dst, uint64(value), 10)
	}
	if width != 0 {
		n := len(dst) - digits
		if n > width {
			return dst[:start], false
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
	return append(dst, suffix...), true
}

// compactSpreadAlphabet5 expands eight packed five-bit codes into byte lanes.
// Mask before shifting: the source and destination groups overlap.
func compactSpreadAlphabet5(packed uint64) uint64 {
	x := packed&0xfffff | ((packed>>20)&0xfffff)<<32
	x = x&0x000003ff000003ff | (x&0x000ffc00000ffc00)<<6
	return x&0x001f001f001f001f | (x&0x03e003e003e003e0)<<3
}

// appendAlphabet renders an admitted alphabet at the state's current ordinal.
// Both callers check kind, row bounds, and synchronization before entering.
// Keeping this renderer independent of random-decode fallback avoids a recursive
// call graph that would force caller-owned scan state onto the heap.
func (s *compactStreamSequentialState) appendAlphabet(dst []byte, v *compactStreamView, row int) ([]byte, bool) {
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
	if s.alphabetEnd == 0 {
		s.alphabetEnd = int(binary.LittleEndian.Uint16(v.dictDir))
		s.prefixEnd = s.alphabetEnd
		if v.dictCount == 3 {
			s.prefixEnd = int(binary.LittleEndian.Uint16(v.dictDir[2:]))
		}
		// Admission requires strictly increasing symbols. Equal endpoint span
		// and cardinality therefore prove every symbol is contiguous.
		if int(v.dictData[s.alphabetEnd-1])-int(v.dictData[0]) == s.alphabetEnd-1 {
			s.alphabetBase = uint16(v.dictData[0]) + 1
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
	middleLen := int(length)
	// The decoder overwrites every middle byte, so a warm scan can extend
	// its retained buffer without paying to clear the same bytes first.
	if middleLen <= cap(dst)-len(dst) {
		dst = dst[:len(dst)+middleLen]
	} else {
		dst = append(dst, make([]byte, middleLen)...)
	}
	if v.width == 0 {
		for char := range middleLen {
			dst[middle+char] = alphabet[0]
		}
		s.next++
		return append(dst, suffix...), true
	}
	width := int(v.width)
	mask := uint64(1<<v.width) - 1
	char := 0
	bit := s.bit
	if middleLen >= 32 && s.alphabetBase != 0 {
		var ok bool
		char, ok = compactScanAlphabetSIMD(dst[middle:middle+middleLen], v.data, bit, width, len(alphabet), byte(s.alphabetBase-1))
		if !ok {
			return dst[:start], false
		}
		bit += char * width
	}
	// Eight symbols occupy at most 48 bits. One bounded load replaces
	// eight byte-reservoir refills; a scalar tail handles the page edge.
	for char+8 <= middleLen && bit/8+8 <= len(v.data) {
		packed := binary.LittleEndian.Uint64(v.data[bit/8:]) >> uint(bit&7)
		if width == 5 && s.alphabetBase != 0 {
			codes := compactSpreadAlphabet5(packed)
			if (codes+uint64(128-len(alphabet))*0x0101010101010101)&0x8080808080808080 != 0 {
				return dst[:start], false
			}
			binary.LittleEndian.PutUint64(dst[middle+char:], codes+uint64(s.alphabetBase-1)*0x0101010101010101)
			char += 8
			bit += 40
			continue
		}
		a := int(packed & mask)
		b := int(packed >> uint(width) & mask)
		c := int(packed >> uint(2*width) & mask)
		d := int(packed >> uint(3*width) & mask)
		e := int(packed >> uint(4*width) & mask)
		f := int(packed >> uint(5*width) & mask)
		g := int(packed >> uint(6*width) & mask)
		h := int(packed >> uint(7*width) & mask)
		if max(a, b, c, d, e, f, g, h) >= len(alphabet) {
			return dst[:start], false
		}
		out := dst[middle+char : middle+char+8]
		out[0], out[1], out[2], out[3] = alphabet[a], alphabet[b], alphabet[c], alphabet[d]
		out[4], out[5], out[6], out[7] = alphabet[e], alphabet[f], alphabet[g], alphabet[h]
		char += 8
		bit += 8 * width
	}
	byteAt := bit >> 3
	shift := bit & 7
	if char < middleLen && byteAt+8 <= len(v.data) {
		packed := binary.LittleEndian.Uint64(v.data[byteAt:]) >> uint(shift)
		tail := dst[middle+char : middle+middleLen]
		if width == 5 && s.alphabetBase != 0 {
			codes := compactSpreadAlphabet5(packed & (uint64(1)<<uint(len(tail)*5) - 1))
			if (codes+uint64(128-len(alphabet))*0x0101010101010101)&0x8080808080808080 != 0 {
				return dst[:start], false
			}
			var rendered [8]byte
			binary.LittleEndian.PutUint64(rendered[:], codes+uint64(s.alphabetBase-1)*0x0101010101010101)
			copy(tail, rendered[:len(tail)])
			s.bit = endBit
			s.next++
			return append(dst, suffix...), true
		}
		for i := range tail {
			code := int(packed & mask)
			if code >= len(alphabet) {
				return dst[:start], false
			}
			tail[i] = alphabet[code]
			packed >>= uint(width)
		}
		s.bit = endBit
		s.next++
		return append(dst, suffix...), true
	}
	var packed uint64
	available := 0
	if char < middleLen && shift != 0 {
		packed = uint64(v.data[byteAt]) >> shift
		available = 8 - shift
		byteAt++
	}
	for ; char < middleLen; char++ {
		for available < width {
			packed |= uint64(v.data[byteAt]) << available
			available += 8
			byteAt++
		}
		code := int(packed & mask)
		if code >= len(alphabet) {
			return dst[:start], false
		}
		dst[middle+char] = alphabet[code]
		packed >>= width
		available -= width
	}
	s.bit = endBit
	s.next++
	return append(dst, suffix...), true
}
