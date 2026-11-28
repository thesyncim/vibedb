package storeio

import (
	"encoding/binary"
	"strconv"
	"unsafe"
)

// unifiedIntegerFORValue reads one value from an admitted FOR stream. The
// grouped lane deliberately keeps the same admission contract as the integer
// extrema lane: full-width and wrapping streams are exact JSON data, but their
// packed unsigned deltas do not preserve the signed ordering that the native
// path relies on, so the complete query declines to the ordinary executor.
func unifiedIntegerFORValue(v compactStreamView, row int) (int64, bool) {
	if !v.validIntegerFORData() || v.width == 64 || v.dictCount != 0 ||
		len(v.dictDir) != 0 || len(v.dictData) != 0 || row < 0 || row >= v.count {
		return 0, false
	}
	base := int64(binary.LittleEndian.Uint64(v.data))
	if v.width > 0 {
		maxDelta := (uint64(1) << v.width) - 1
		const maxInt64Value = uint64(1<<63 - 1)
		if base >= 0 && maxDelta > maxInt64Value-uint64(base) {
			return 0, false
		}
	}
	delta := compactReadBits(v.data[8:], row*int(v.width), int(v.width))
	return base + int64(delta), true
}

// unifiedCanonicalInt64Value admits only the canonical JSON integer spelling,
// while covering the full signed int64 range (the token fast path's narrower
// 18-digit admission is intentionally not reused here). Dictionary, alphabet,
// and front-coded compact streams retain their original spellings, so this is
// the exact numeric proof for those representations.
func unifiedCanonicalInt64Value(src []byte) (int64, bool) {
	if len(src) == 0 || len(src) > 20 {
		return 0, false
	}
	at := 0
	negative := src[0] == '-'
	if negative {
		at++
	}
	if at == len(src) || src[at] < '0' || src[at] > '9' {
		return 0, false
	}
	if src[at] == '0' && at+1 != len(src) {
		return 0, false
	}
	if negative && src[at] == '0' {
		return 0, false
	}
	limit := uint64(1<<63 - 1)
	if negative {
		limit++
	}
	var value uint64
	for ; at < len(src); at++ {
		if src[at] < '0' || src[at] > '9' {
			return 0, false
		}
		digit := uint64(src[at] - '0')
		if value > (limit-digit)/10 {
			return 0, false
		}
		value = value*10 + digit
	}
	if negative {
		if value == uint64(1)<<63 {
			return -1 << 63, true
		}
		return -int64(value), true
	}
	return int64(value), true
}

func unifiedCheckedIntegerAdd(a, b int64) (int64, bool) {
	const minInt64 = -1 << 63
	const maxInt64 = 1<<63 - 1
	if b > 0 && a > maxInt64-b || b < 0 && a < minInt64-b {
		return 0, false
	}
	return a + b, true
}

func unifiedDeltaIntegerValue(v compactStreamView, row int) (int64, bool) {
	if v.kind != compactStreamDelta || row < 0 || row >= v.count {
		return 0, false
	}
	blocks := (v.count + compactStreamRestart - 1) / compactStreamRestart
	block := row / compactStreamRestart
	if block < 0 || block >= blocks || len(v.data) < 4*blocks {
		return 0, false
	}
	cursor := int(binary.LittleEndian.Uint32(v.data[block*4:]))
	if cursor < 0 || cursor > len(v.data)-8 {
		return 0, false
	}
	value := int64(binary.LittleEndian.Uint64(v.data[cursor:]))
	cursor += 8
	for at := block * compactStreamRestart; at < row; at++ {
		u, n, ok := readCompactUvarint(v.data[cursor:])
		if !ok || n <= 0 || n > len(v.data)-cursor {
			return 0, false
		}
		cursor += n
		delta := int64(u>>1) ^ -int64(u&1)
		value, ok = unifiedCheckedIntegerAdd(value, delta)
		if !ok {
			return 0, false
		}
	}
	return value, true
}

func unifiedPackedDeltaIntegerValue(v compactStreamView, row int) (int64, bool) {
	if v.kind != compactStreamDeltaPack || row < 0 || row >= v.count {
		return 0, false
	}
	blocks := (v.count + compactStreamRestart - 1) / compactStreamRestart
	block := row / compactStreamRestart
	if block < 0 || block >= blocks || len(v.data) < 4*blocks {
		return 0, false
	}
	cursor := int(binary.LittleEndian.Uint32(v.data[block*4:]))
	if cursor < 0 || cursor > len(v.data)-9 {
		return 0, false
	}
	value := int64(binary.LittleEndian.Uint64(v.data[cursor:]))
	width := int(v.data[cursor+8])
	packed := v.data[cursor+9:]
	for at := block * compactStreamRestart; at < row; at++ {
		u := compactReadBits(packed, (at-block*compactStreamRestart)*width, width)
		delta := int64(u>>1) ^ -int64(u&1)
		var ok bool
		value, ok = unifiedCheckedIntegerAdd(value, delta)
		if !ok {
			return 0, false
		}
	}
	return value, true
}

func unifiedCompactIntegerValue(v compactStreamView, row int) (int64, bool) {
	if row < 0 || row >= v.count {
		return 0, false
	}
	switch v.kind {
	case compactStreamRankAffine:
		if v.rankAffineIsNumber() {
			return v.rankAffineInteger(row)
		}
		return 0, false
	case compactStreamFOR:
		return unifiedIntegerFORValue(v, row)
	case compactStreamDelta:
		return unifiedDeltaIntegerValue(v, row)
	case compactStreamDeltaPack:
		return unifiedPackedDeltaIntegerValue(v, row)
	case compactStreamDictionary:
		id := int(compactReadBits(v.data, row*int(v.width), int(v.width)))
		raw, ok := v.dictionaryEntry(id)
		if !ok {
			return 0, false
		}
		return unifiedCanonicalInt64Value(raw)
	case compactStreamFront:
		return unifiedFrontIntegerValue(v, row)
	case compactStreamPrefixInt, compactStreamAlphabet:
		// Reject long spellings BEFORE appendValue can grow its destination.
		// Every int64 needs at most twenty bytes including its sign.
		var scratch [20]byte
		if v.kind == compactStreamPrefixInt {
			value, ok := v.prefixInteger(row)
			if !ok || value < 0 {
				return 0, false
			}
			prefix, _ := v.dictionaryEntry(0)
			suffix, _ := v.dictionaryEntry(1)
			digits := len(strconv.AppendInt(scratch[:0], value, 10))
			if v.data[0]&1 != 0 {
				digits = max(digits, int(v.data[1]))
			}
			if len(prefix)+len(suffix)+digits > len(scratch) {
				return 0, false
			}
		} else {
			_, prefix, suffix, ok := v.alphabetParts()
			if !ok || len(prefix)+len(suffix) > len(scratch) {
				return 0, false
			}
			base, lengths, _, _, width := v.alphabetBlock(row / compactStreamRestart)
			delta := compactReadBits(lengths, (row%compactStreamRestart)*width, width)
			remaining := uint64(len(scratch) - len(prefix) - len(suffix))
			if base > remaining || delta > remaining-base {
				return 0, false
			}
		}
		raw, ok := v.appendValue(scratch[:0], row)
		if !ok {
			return 0, false
		}
		return unifiedCanonicalInt64Value(raw)
	default:
		return 0, false
	}
}

// Front decoding reconstructs at most twenty bytes, including intermediate
// restart values. A long intermediate spelling declines even when a later
// value is short; the generic scan remains authoritative for that stream.
func unifiedFrontIntegerValue(v compactStreamView, row int) (int64, bool) {
	var scratch [20]byte
	block := row / compactStreamRestart
	cursor := int(binary.LittleEndian.Uint32(v.data[block*4:]))
	length, n, ok := readCompactUvarint(v.data[cursor:])
	if !ok || length > uint64(len(scratch)) {
		return 0, false
	}
	cursor += n
	raw := scratch[:int(length)]
	copy(raw, v.data[cursor:cursor+int(length)])
	cursor += int(length)
	for at := block * compactStreamRestart; at < row; at++ {
		packed := v.data[cursor]
		cursor++
		prefix, suffix := uint64(packed>>4), uint64(packed&15)
		if packed == 0xff {
			prefix, n, ok = readCompactUvarint(v.data[cursor:])
			if !ok {
				return 0, false
			}
			cursor += n
			suffix, n, ok = readCompactUvarint(v.data[cursor:])
			if !ok {
				return 0, false
			}
			cursor += n
		}
		if prefix > uint64(len(raw)) || suffix > uint64(len(scratch))-prefix {
			return 0, false
		}
		raw = scratch[:int(prefix+suffix)]
		copy(raw[int(prefix):], v.data[cursor:cursor+int(suffix)])
		cursor += int(suffix)
	}
	return unifiedCanonicalInt64Value(raw)
}

func compactPacked4MaxBelowScalar(data []byte, count, limit int) bool {
	if count <= 0 {
		return true
	}
	if limit <= 0 {
		return false
	}
	if limit >= 16 {
		return true
	}
	lim := uint8(limit)
	pairs := count >> 1
	for i := range pairs {
		b := data[i]
		if b&0x0f >= lim || b>>4 >= lim {
			return false
		}
	}
	if count&1 != 0 && data[pairs]&0x0f >= lim {
		return false
	}
	return true
}

func compactUnpackPacked4x32Scalar(dst *[32]uint8, src *[16]uint8) {
	for i := range 16 {
		b := src[i]
		dst[2*i] = b & 0x0f
		dst[2*i+1] = b >> 4
	}
}

func compactUnpackPacked7x16Scalar(dst *[16]uint16, src *[16]uint8) {
	p0 := uint64(src[0]) | uint64(src[1])<<8 | uint64(src[2])<<16 | uint64(src[3])<<24 |
		uint64(src[4])<<32 | uint64(src[5])<<40 | uint64(src[6])<<48
	for i := range 8 {
		dst[i] = uint16((p0 >> uint(i*7)) & 127)
	}
	p1 := uint64(src[7]) | uint64(src[8])<<8 | uint64(src[9])<<16 | uint64(src[10])<<24 |
		uint64(src[11])<<32 | uint64(src[12])<<40 | uint64(src[13])<<48
	for i := range 8 {
		dst[8+i] = uint16((p1 >> uint(i*7)) & 127)
	}
}

func compactUnpackPacked10x32Scalar(dst *[32]uint16, src *[40]uint8) {
	for g := range 8 {
		c := g * 5
		p := uint64(src[c]) | uint64(src[c+1])<<8 | uint64(src[c+2])<<16 | uint64(src[c+3])<<24 | uint64(src[c+4])<<32
		o := g * 4
		dst[o] = uint16(p & 1023)
		dst[o+1] = uint16((p >> 10) & 1023)
		dst[o+2] = uint16((p >> 20) & 1023)
		dst[o+3] = uint16((p >> 30) & 1023)
	}
}

func compactPackedMaxBelow(data []byte, count, width, limit int) bool {
	if count <= 0 {
		return true
	}
	if width < 0 || width > 64 || limit <= 0 {
		return false
	}
	if uint64(len(data)) != (uint64(count)*uint64(width)+7)/8 {
		return false
	}
	if width == 0 {
		return true
	}
	if width <= 16 && limit >= 1<<width {
		return true
	}
	if width == 4 {
		return compactPacked4MaxBelowSIMD(data, count, limit)
	}
	if width == 7 || width == 8 || width == 10 || width == 16 {
		_, maxCode, found, supported := countCompactPackedExtrema(data, count, width)
		return supported && (!found || maxCode < uint64(limit))
	}
	lim := uint64(limit)
	if width > 56 {
		for row := range count {
			if compactReadBits(data, row*width, width) >= lim {
				return false
			}
		}
		return true
	}
	mask := uint64(1)<<uint(width) - 1
	var reservoir uint64
	available, cursor := 0, 0
	for range count {
		for available < width {
			reservoir |= uint64(data[cursor]) << uint(available)
			cursor++
			available += 8
		}
		if reservoir&mask >= lim {
			return false
		}
		reservoir >>= uint(width)
		available -= width
	}
	return true
}

func unifiedCompactIntegerStreamExactDelta(v compactStreamView) bool {
	if v.count == 0 {
		return true
	}
	blocks := (v.count + compactStreamRestart - 1) / compactStreamRestart
	if len(v.data) < 4*blocks {
		return false
	}
	for block := range blocks {
		cursor := int(binary.LittleEndian.Uint32(v.data[block*4:]))
		if cursor < 0 || cursor > len(v.data)-8 {
			return false
		}
		value := int64(binary.LittleEndian.Uint64(v.data[cursor:]))
		cursor += 8
		end := min((block+1)*compactStreamRestart, v.count)
		for at := block*compactStreamRestart + 1; at < end; at++ {
			u, n, ok := readCompactUvarint(v.data[cursor:])
			if !ok || n <= 0 || n > len(v.data)-cursor {
				return false
			}
			cursor += n
			delta := int64(u>>1) ^ -int64(u&1)
			value, ok = unifiedCheckedIntegerAdd(value, delta)
			if !ok {
				return false
			}
		}
	}
	return true
}

func unifiedCompactIntegerStreamExactDeltaPack(v compactStreamView) bool {
	if v.count == 0 {
		return true
	}
	blocks := (v.count + compactStreamRestart - 1) / compactStreamRestart
	if len(v.data) < 4*blocks {
		return false
	}
	for block := range blocks {
		cursor := int(binary.LittleEndian.Uint32(v.data[block*4:]))
		if cursor < 0 || cursor > len(v.data)-9 {
			return false
		}
		value := int64(binary.LittleEndian.Uint64(v.data[cursor:]))
		width := int(v.data[cursor+8])
		packed := v.data[cursor+9:]
		end := min((block+1)*compactStreamRestart, v.count)
		start := block * compactStreamRestart
		if width > 64 {
			return false
		}
		deltas := end - start - 1
		if uint64(len(packed)) < (uint64(deltas)*uint64(width)+7)/8 {
			return false
		}
		for i := range deltas {
			u := compactReadBits(packed, i*width, width)
			delta := int64(u>>1) ^ -int64(u&1)
			var ok bool
			value, ok = unifiedCheckedIntegerAdd(value, delta)
			if !ok {
				return false
			}
		}
	}
	return true
}

func unifiedCompactIntegerStreamExactFront(v compactStreamView) bool {
	if v.count == 0 {
		return true
	}
	blocks := (v.count + compactStreamRestart - 1) / compactStreamRestart
	if len(v.data) < 4*blocks {
		return false
	}
	var scratch [20]byte
	for block := range blocks {
		cursor := int(binary.LittleEndian.Uint32(v.data[block*4:]))
		if cursor < 0 || cursor >= len(v.data) {
			return false
		}
		length, n, ok := readCompactUvarint(v.data[cursor:])
		if !ok || length > uint64(len(scratch)) || length > uint64(len(v.data)-cursor-n) {
			return false
		}
		cursor += n
		raw := scratch[:int(length)]
		copy(raw, v.data[cursor:cursor+int(length)])
		cursor += int(length)
		if _, ok := unifiedCanonicalInt64Value(raw); !ok {
			return false
		}
		end := min((block+1)*compactStreamRestart, v.count)
		for at := block*compactStreamRestart + 1; at < end; at++ {
			if cursor >= len(v.data) {
				return false
			}
			packed := v.data[cursor]
			cursor++
			prefix, suffix := uint64(packed>>4), uint64(packed&15)
			if packed == 0xff {
				prefix, n, ok = readCompactUvarint(v.data[cursor:])
				if !ok {
					return false
				}
				cursor += n
				suffix, n, ok = readCompactUvarint(v.data[cursor:])
				if !ok {
					return false
				}
				cursor += n
			}
			if prefix > uint64(len(raw)) || suffix > uint64(len(scratch))-prefix || suffix > uint64(len(v.data)-cursor) {
				return false
			}
			raw = scratch[:int(prefix+suffix)]
			copy(raw[int(prefix):], v.data[cursor:cursor+int(suffix)])
			cursor += int(suffix)
			if _, ok := unifiedCanonicalInt64Value(raw); !ok {
				return false
			}
		}
	}
	return true
}

func unifiedCompactIntegerStreamExactSlow(v compactStreamView) bool {
	for row := 0; row < v.count; row++ {
		if _, ok := unifiedCompactIntegerValue(v, row); !ok {
			return false
		}
	}
	return true
}

func unifiedCompactIntegerStreamExact(v compactStreamView) bool {
	if v.kind == compactStreamRankAffine {
		return v.rankAffineIsNumber()
	}
	if v.count < 0 {
		return false
	}
	if v.count == 0 {
		return true
	}
	switch v.kind {
	case compactStreamFOR:
		if !v.validIntegerFORData() || v.width == 64 || v.dictCount != 0 ||
			len(v.dictDir) != 0 || len(v.dictData) != 0 {
			return false
		}
		base := int64(binary.LittleEndian.Uint64(v.data))
		if v.width > 0 {
			maxDelta := (uint64(1) << v.width) - 1
			const maxInt64Value = uint64(1<<63 - 1)
			if base >= 0 && maxDelta > maxInt64Value-uint64(base) {
				return false
			}
		}
		return true
	case compactStreamDictionary:
		if v.dictCount <= 0 || !compactPackedMaxBelow(v.data, v.count, int(v.width), v.dictCount) {
			return false
		}
		for id := range v.dictCount {
			raw, ok := v.dictionaryEntry(id)
			if !ok {
				return false
			}
			if _, ok := unifiedCanonicalInt64Value(raw); !ok {
				// Preserve exact semantics if an unreferenced dictionary entry
				// carries a non-integer spelling.
				return unifiedCompactIntegerStreamExactSlow(v)
			}
		}
		return true
	case compactStreamDelta:
		return unifiedCompactIntegerStreamExactDelta(v)
	case compactStreamDeltaPack:
		return unifiedCompactIntegerStreamExactDeltaPack(v)
	case compactStreamFront:
		return unifiedCompactIntegerStreamExactFront(v)
	default:
		return unifiedCompactIntegerStreamExactSlow(v)
	}
}

type integerStreamFastKind uint8

const (
	intFastGeneric integerStreamFastKind = iota
	intFastZero
	intFastRankAffine
	intFastFOR
	intFastDict
	intFastDelta
	intFastDeltaPack
)

// integerStreamFastDecoder is stack-allocated for single-shape compact leaves
// (the common table layout), keeping IntegerGroupShapeWorkspace small while
// pre-decoding dictionary spellings once per leaf and unpacking packed bit
// widths in 32-row SIMD/reservoir batches.
type integerStreamFastDecoder struct {
	kind      integerStreamFastKind
	width     uint8
	base      int64
	step      int64
	data      []byte
	stream    compactStreamView
	dictCount int
	dict      [256]int64
	// Sequential cursor state for compactStreamDelta / compactStreamDeltaPack
	// (restart block size is 64 = two 32-row batches).
	deltaRow    int
	deltaCursor int
	deltaValue  int64
	deltaPacked []byte
	deltaWidth  int
	deltaBlock  int
}

func prepareIntegerStreamFast(v compactStreamView, dec *integerStreamFastDecoder) bool {
	dec.stream = v
	if v.kind == compactStreamRankAffine {
		if !v.rankAffineIsNumber() {
			return false
		}
		dec.kind = intFastRankAffine
		dec.base = int64(binary.LittleEndian.Uint64(v.data[2:]))
		dec.step = int64(binary.LittleEndian.Uint64(v.data[10:]))
		return true
	}
	if v.count < 0 {
		return false
	}
	if v.count == 0 {
		dec.kind = intFastZero
		return true
	}
	switch v.kind {
	case compactStreamFOR:
		if !v.validIntegerFORData() || v.width == 64 || v.dictCount != 0 ||
			len(v.dictDir) != 0 || len(v.dictData) != 0 {
			return false
		}
		base := int64(binary.LittleEndian.Uint64(v.data))
		if v.width > 0 {
			maxDelta := (uint64(1) << v.width) - 1
			const maxInt64Value = uint64(1<<63 - 1)
			if base >= 0 && maxDelta > maxInt64Value-uint64(base) {
				return false
			}
		}
		dec.kind = intFastFOR
		dec.width = v.width
		dec.base = base
		dec.data = v.data[8:]
		return true
	case compactStreamDictionary:
		if v.dictCount <= 0 || !compactPackedMaxBelow(v.data, v.count, int(v.width), v.dictCount) {
			return false
		}
		if v.dictCount <= len(dec.dict) {
			allExact := true
			for id := range v.dictCount {
				raw, ok := v.dictionaryEntry(id)
				if !ok {
					return false
				}
				val, ok := unifiedCanonicalInt64Value(raw)
				if !ok {
					allExact = false
					break
				}
				dec.dict[id] = val
			}
			if allExact {
				dec.kind = intFastDict
				dec.width = v.width
				dec.data = v.data
				dec.dictCount = v.dictCount
				return true
			}
			if !unifiedCompactIntegerStreamExactSlow(v) {
				return false
			}
			dec.kind = intFastGeneric
			return true
		}
		for id := range v.dictCount {
			raw, ok := v.dictionaryEntry(id)
			if !ok {
				return false
			}
			if _, ok := unifiedCanonicalInt64Value(raw); !ok {
				if !unifiedCompactIntegerStreamExactSlow(v) {
					return false
				}
				dec.kind = intFastGeneric
				return true
			}
		}
		dec.kind = intFastGeneric
		return true
	case compactStreamDelta:
		if !unifiedCompactIntegerStreamExactDelta(v) {
			return false
		}
		dec.kind = intFastDelta
		dec.data = v.data
		dec.deltaRow = -1
		return true
	case compactStreamDeltaPack:
		if !unifiedCompactIntegerStreamExactDeltaPack(v) {
			return false
		}
		dec.kind = intFastDeltaPack
		dec.data = v.data
		dec.deltaRow = -1
		return true
	default:
		if !unifiedCompactIntegerStreamExact(v) {
			return false
		}
		dec.kind = intFastGeneric
		return true
	}
}

// decodeBatch fills dst[:n] for rows [row, row+n). row must be a multiple of
// 32 (guaranteeing byte alignment for every packed bit width) and 1 <= n <= 32.
func (d *integerStreamFastDecoder) decodeBatch(row, n int, dst *[32]int64) bool {
	switch d.kind {
	case intFastZero:
		return n == 0
	case intFastRankAffine:
		val := d.base + d.step*int64(row)
		step := d.step
		for i := range n {
			dst[i] = val
			val += step
		}
		return true
	case intFastDict:
		dict := &d.dict
		w := int(d.width)
		switch w {
		case 0:
			val := dict[0]
			for i := range n {
				dst[i] = val
			}
			return true
		case 4:
			if n == 32 {
				var codes [32]uint8
				compactUnpackPacked4x32SIMD(&codes, (*[16]uint8)(d.data[row>>1:]))
				for i := 0; i < 32; i += 4 {
					dst[i] = dict[codes[i]]
					dst[i+1] = dict[codes[i+1]]
					dst[i+2] = dict[codes[i+2]]
					dst[i+3] = dict[codes[i+3]]
				}
				return true
			}
			src := d.data[row>>1:]
			pairs := n >> 1
			for i := range pairs {
				b := src[i]
				dst[2*i] = dict[b&0x0f]
				dst[2*i+1] = dict[b>>4]
			}
			if n&1 != 0 {
				dst[n-1] = dict[src[pairs]&0x0f]
			}
			return true
		case 7:
			byteOff := (row >> 3) * 7
			if n == 32 && byteOff+30 <= len(d.data) {
				var u0, u1 [16]uint16
				compactUnpackPacked7x16SIMD(&u0, (*[16]uint8)(d.data[byteOff:]))
				compactUnpackPacked7x16SIMD(&u1, (*[16]uint8)(d.data[byteOff+14:]))
				for i := range 16 {
					dst[i] = dict[u0[i]]
					dst[16+i] = dict[u1[i]]
				}
				return true
			}
			mask := uint64(127)
			var reservoir uint64
			available, cursor := 0, byteOff
			for i := range n {
				for available < 7 {
					reservoir |= uint64(d.data[cursor]) << uint(available)
					cursor++
					available += 8
				}
				dst[i] = dict[reservoir&mask]
				reservoir >>= 7
				available -= 7
			}
			return true
		case 8:
			src := d.data[row : row+n]
			i := 0
			for ; i+4 <= n; i += 4 {
				dst[i] = dict[src[i]]
				dst[i+1] = dict[src[i+1]]
				dst[i+2] = dict[src[i+2]]
				dst[i+3] = dict[src[i+3]]
			}
			for ; i < n; i++ {
				dst[i] = dict[src[i]]
			}
			return true
		default:
			mask := uint64(1)<<uint(w) - 1
			var reservoir uint64
			available, cursor := 0, (row*w)>>3
			for i := range n {
				for available < w {
					reservoir |= uint64(d.data[cursor]) << uint(available)
					cursor++
					available += 8
				}
				dst[i] = dict[reservoir&mask]
				reservoir >>= uint(w)
				available -= w
			}
			return true
		}
	case intFastFOR:
		base := d.base
		w := int(d.width)
		switch w {
		case 0:
			for i := range n {
				dst[i] = base
			}
			return true
		case 4:
			if n == 32 {
				var codes [32]uint8
				compactUnpackPacked4x32SIMD(&codes, (*[16]uint8)(d.data[row>>1:]))
				for i := 0; i < 32; i += 4 {
					dst[i] = base + int64(codes[i])
					dst[i+1] = base + int64(codes[i+1])
					dst[i+2] = base + int64(codes[i+2])
					dst[i+3] = base + int64(codes[i+3])
				}
				return true
			}
			src := d.data[row>>1:]
			pairs := n >> 1
			for i := range pairs {
				b := src[i]
				dst[2*i] = base + int64(b&0x0f)
				dst[2*i+1] = base + int64(b>>4)
			}
			if n&1 != 0 {
				dst[n-1] = base + int64(src[pairs]&0x0f)
			}
			return true
		case 7:
			byteOff := (row >> 3) * 7
			if n == 32 && byteOff+30 <= len(d.data) {
				var u0, u1 [16]uint16
				compactUnpackPacked7x16SIMD(&u0, (*[16]uint8)(d.data[byteOff:]))
				compactUnpackPacked7x16SIMD(&u1, (*[16]uint8)(d.data[byteOff+14:]))
				for i := range 16 {
					dst[i] = base + int64(u0[i])
					dst[16+i] = base + int64(u1[i])
				}
				return true
			}
			var reservoir uint64
			available, cursor := 0, byteOff
			for i := range n {
				for available < 7 {
					reservoir |= uint64(d.data[cursor]) << uint(available)
					cursor++
					available += 8
				}
				dst[i] = base + int64(reservoir&127)
				reservoir >>= 7
				available -= 7
			}
			return true
		case 8:
			src := d.data[row : row+n]
			i := 0
			for ; i+4 <= n; i += 4 {
				dst[i] = base + int64(src[i])
				dst[i+1] = base + int64(src[i+1])
				dst[i+2] = base + int64(src[i+2])
				dst[i+3] = base + int64(src[i+3])
			}
			for ; i < n; i++ {
				dst[i] = base + int64(src[i])
			}
			return true
		case 10:
			byteOff := (row >> 2) * 5
			if n == 32 {
				var u [32]uint16
				compactUnpackPacked10x32SIMD(&u, (*[40]uint8)(d.data[byteOff:]))
				for i := 0; i < 32; i += 4 {
					dst[i] = base + int64(u[i])
					dst[i+1] = base + int64(u[i+1])
					dst[i+2] = base + int64(u[i+2])
					dst[i+3] = base + int64(u[i+3])
				}
				return true
			}
			var reservoir uint64
			available, cursor := 0, byteOff
			for i := range n {
				for available < 10 {
					reservoir |= uint64(d.data[cursor]) << uint(available)
					cursor++
					available += 8
				}
				dst[i] = base + int64(reservoir&1023)
				reservoir >>= 10
				available -= 10
			}
			return true
		case 16:
			src := d.data[row*2 : (row+n)*2]
			i := 0
			for ; i+4 <= n; i += 4 {
				dst[i] = base + int64(binary.LittleEndian.Uint16(src[i*2:]))
				dst[i+1] = base + int64(binary.LittleEndian.Uint16(src[i*2+2:]))
				dst[i+2] = base + int64(binary.LittleEndian.Uint16(src[i*2+4:]))
				dst[i+3] = base + int64(binary.LittleEndian.Uint16(src[i*2+6:]))
			}
			for ; i < n; i++ {
				dst[i] = base + int64(binary.LittleEndian.Uint16(src[i*2:]))
			}
			return true
		case 32:
			src := d.data[row*4 : (row+n)*4]
			i := 0
			for ; i+4 <= n; i += 4 {
				dst[i] = base + int64(binary.LittleEndian.Uint32(src[i*4:]))
				dst[i+1] = base + int64(binary.LittleEndian.Uint32(src[i*4+4:]))
				dst[i+2] = base + int64(binary.LittleEndian.Uint32(src[i*4+8:]))
				dst[i+3] = base + int64(binary.LittleEndian.Uint32(src[i*4+12:]))
			}
			for ; i < n; i++ {
				dst[i] = base + int64(binary.LittleEndian.Uint32(src[i*4:]))
			}
			return true
		default:
			if w <= 56 {
				mask := uint64(1)<<uint(w) - 1
				var reservoir uint64
				available, cursor := 0, (row*w)>>3
				for i := range n {
					for available < w {
						reservoir |= uint64(d.data[cursor]) << uint(available)
						cursor++
						available += 8
					}
					dst[i] = base + int64(reservoir&mask)
					reservoir >>= uint(w)
					available -= w
				}
				return true
			}
			for i := range n {
				dst[i] = base + int64(compactReadBits(d.data, (row+i)*w, w))
			}
			return true
		}
	case intFastDelta:
		// compactStreamRestart == 64, and row is a multiple of 32.
		// Even batches (row&63 == 0) begin at a restart block header;
		// odd batches (row&63 == 32) continue directly from d.deltaCursor.
		if row&63 == 0 || d.deltaRow != row {
			block := row / compactStreamRestart
			cursor := int(binary.LittleEndian.Uint32(d.data[block*4:]))
			value := int64(binary.LittleEndian.Uint64(d.data[cursor:]))
			cursor += 8
			at := block * compactStreamRestart
			for at < row {
				u, k, ok := readCompactUvarint(d.data[cursor:])
				if !ok || k <= 0 {
					return false
				}
				cursor += k
				value += int64(u>>1) ^ -int64(u&1)
				at++
			}
			dst[0] = value
			for i := 1; i < n; i++ {
				u, k, ok := readCompactUvarint(d.data[cursor:])
				if !ok || k <= 0 {
					return false
				}
				cursor += k
				value += int64(u>>1) ^ -int64(u&1)
				dst[i] = value
			}
			d.deltaRow = row + n
			d.deltaCursor = cursor
			d.deltaValue = value
			return true
		}
		cursor := d.deltaCursor
		value := d.deltaValue
		for i := range n {
			u, k, ok := readCompactUvarint(d.data[cursor:])
			if !ok || k <= 0 {
				return false
			}
			cursor += k
			value += int64(u>>1) ^ -int64(u&1)
			dst[i] = value
		}
		d.deltaRow = row + n
		d.deltaCursor = cursor
		d.deltaValue = value
		return true
	case intFastDeltaPack:
		if row&63 == 0 || d.deltaRow != row {
			block := row / compactStreamRestart
			cursor := int(binary.LittleEndian.Uint32(d.data[block*4:]))
			value := int64(binary.LittleEndian.Uint64(d.data[cursor:]))
			width := int(d.data[cursor+8])
			packed := d.data[cursor+9:]
			start := block * compactStreamRestart
			at := start
			for at < row {
				u := compactReadBits(packed, (at-start)*width, width)
				value += int64(u>>1) ^ -int64(u&1)
				at++
			}
			dst[0] = value
			for i := 1; i < n; i++ {
				u := compactReadBits(packed, (row+i-start-1)*width, width)
				value += int64(u>>1) ^ -int64(u&1)
				dst[i] = value
			}
			d.deltaRow = row + n
			d.deltaValue = value
			d.deltaPacked = packed
			d.deltaWidth = width
			d.deltaBlock = start
			return true
		}
		value := d.deltaValue
		packed := d.deltaPacked
		width := d.deltaWidth
		start := d.deltaBlock
		for i := range n {
			u := compactReadBits(packed, (row+i-start-1)*width, width)
			value += int64(u>>1) ^ -int64(u&1)
			dst[i] = value
		}
		d.deltaRow = row + n
		d.deltaValue = value
		return true
	default:
		for i := range n {
			val, ok := unifiedCompactIntegerValue(d.stream, row+i)
			if !ok {
				return false
			}
			dst[i] = val
		}
		return true
	}
}

// compactIntegerStreamRawAt locates the stream view at hole without running
// integer-exactness validation, allowing callers that pre-decode into a fast
// stack decoder to validate and build lookup tables in one pass.
func compactIntegerStreamRawAt(
	entry compactPrimaryShapeView, hole, leafRows int,
) (compactStreamView, bool) {
	if hole < 0 || hole >= entry.template.holes {
		return compactStreamView{}, false
	}
	raw := entry.streamRaw
	for at := 0; at <= hole; at++ {
		stream, admitted := admittedCompactStream(raw)
		if !admitted || !stream.matchesShapeRows(entry.rows, leafRows) {
			return compactStreamView{}, false
		}
		if at == hole {
			return stream, true
		}
		raw = raw[stream.encoded:]
	}
	return compactStreamView{}, false
}

// compactIntegerStreamAt resolves one scalar hole in a compact shape and
// admits it only when the selected stream contains exact canonical integers.
// FOR is the common wide-column representation; dictionary, alphabet, and
// front-coded integer streams are also safe when every spelling proves to be
// an int64, which covers low-cardinality SQL grouping columns without JSON
// reconstruction. The compact stripe validator has already checked every
// stream's envelope; this helper repeats the bounded stream walk needed to
// reach the target hole.
func compactIntegerStreamAt(
	entry compactPrimaryShapeView, hole, leafRows int,
) (compactStreamView, bool) {
	stream, ok := compactIntegerStreamRawAt(entry, hole, leafRows)
	if !ok || !unifiedCompactIntegerStreamExact(stream) {
		return compactStreamView{}, false
	}
	return stream, true
}

// IntegerGroupShapeWorkspace is caller-owned per-shape scratch for one
// compact grouped scan. Its fields intentionally remain private; the type is
// exported only so durable Snapshot can retain and reuse the bounded table.
type IntegerGroupShapeWorkspace struct {
	rows  int
	group compactStreamView
	sum   compactStreamView
}

// IntegerGroupScratchBytes reports the retained caller-owned bytes needed for
// shape-local ordinals and compact stream views. It intentionally excludes the
// page-backed byte slices those views point into: the durable cursor owns those
// frames, and clears every view before returning to avoid retaining them.
func IntegerGroupScratchBytes(shapeCount int) int64 {
	if shapeCount <= 0 {
		return 0
	}
	per := uint64(unsafe.Sizeof(int(0)) + unsafe.Sizeof(IntegerGroupShapeWorkspace{}))
	if uint64(shapeCount) > uint64(^uint64(0)>>1)/per {
		return int64(^uint64(0) >> 1)
	}
	return int64(uint64(shapeCount) * per)
}

// VisitResolvedIntegerGroups visits one compact stripe in physical row order,
// yielding the resolved integer group key and, when sum is non-nil, the
// resolved integer SUM value. shapeSeen is caller-owned reusable storage; it
// carries one ordinal per shape so a multi-shape stripe never reconstructs a
// row or performs a random rank search for every value.
//
// The operation is strict and atomic at stripe granularity. Any overflow row,
// absent/container target, non-integer compact spelling, malformed stream, or
// wrapping FOR stream returns supported=false after discarding local progress.
// The caller may then run the authoritative generic executor over the complete
// snapshot.
// IntegerGroupBatch holds caller-owned 32-row decode buffers for
// VisitResolvedIntegerGroupsBatch so slice arguments passed to the batch
// callback never force per-leaf heap allocations.
type IntegerGroupBatch struct {
	Groups [32]int64
	Sums   [32]int64
}

// SupportsIntegerGroups reports whether this compact stripe's templates and
// streams pass strict integer-group admission without requiring caller-owned
// shape workspace buffers. keyScratch is caller-owned scratch for escaped
// template key decoding (pass a >=1024-byte slice to avoid heap allocation).
func (v *CompactPrimaryStripeView) SupportsIntegerGroups(
	groupResolver, sumResolver *UnifiedHoleResolver,
	keyScratch []byte,
) bool {
	if v == nil || groupResolver == nil || len(v.overflow) != 0 || v.shapeCount < 0 {
		return false
	}
	if v.shapeCount == 0 {
		return v.rows == 0
	}
	if v.shapeCount == 1 {
		entry, ok := v.shapeEntry(0)
		if !ok || entry.rows != v.rows {
			return false
		}
		groupHole := resolveCompactProjectionTemplate(groupResolver, entry.template, keyScratch)
		if groupHole < 0 || groupHole >= entry.template.holes {
			return false
		}
		groupStream, ok := compactIntegerStreamRawAt(entry, groupHole, v.rows)
		if !ok {
			return false
		}
		var groupDec integerStreamFastDecoder
		if !prepareIntegerStreamFast(groupStream, &groupDec) {
			return false
		}
		if sumResolver != nil {
			sumHole := resolveCompactProjectionTemplate(sumResolver, entry.template, keyScratch)
			if sumHole < 0 || sumHole >= entry.template.holes {
				return false
			}
			if sumHole != groupHole {
				sumStream, ok := compactIntegerStreamRawAt(entry, sumHole, v.rows)
				if !ok {
					return false
				}
				var sumDec integerStreamFastDecoder
				if !prepareIntegerStreamFast(sumStream, &sumDec) {
					return false
				}
			}
		}
		return true
	}
	for shape := 0; shape < v.shapeCount; shape++ {
		entry, ok := v.shapeEntry(shape)
		if !ok || entry.rows < 0 {
			return false
		}
		groupHole := resolveCompactProjectionTemplate(groupResolver, entry.template, keyScratch)
		if groupHole < 0 || groupHole >= entry.template.holes {
			return false
		}
		if _, ok := compactIntegerStreamAt(entry, groupHole, v.rows); !ok {
			return false
		}
		if sumResolver != nil {
			sumHole := resolveCompactProjectionTemplate(sumResolver, entry.template, keyScratch)
			if sumHole < 0 || sumHole >= entry.template.holes {
				return false
			}
			if sumHole != groupHole {
				if _, ok := compactIntegerStreamAt(entry, sumHole, v.rows); !ok {
					return false
				}
			}
		}
	}
	return true
}

func (v *CompactPrimaryStripeView) VisitResolvedIntegerGroups(
	groupResolver, sumResolver *UnifiedHoleResolver,
	shapeSeen []int, shapeWork []IntegerGroupShapeWorkspace,
	visit func(row int, group, sum int64) error,
) (supported bool, err error) {
	if visit == nil {
		return false, nil
	}
	return v.visitResolvedIntegerGroups(groupResolver, sumResolver, shapeSeen, shapeWork, nil, visit, nil)
}

// VisitResolvedIntegerGroupsBatch is the batched counterpart of
// VisitResolvedIntegerGroups, delivering up to 32 physical rows per callback
// in exact physical row order. sums is nil when sumResolver is nil; otherwise
// len(sums) == len(groups). batch may be nil (allocating one per call) or
// caller-owned for zero-allocation scanning across leaves.
func (v *CompactPrimaryStripeView) VisitResolvedIntegerGroupsBatch(
	groupResolver, sumResolver *UnifiedHoleResolver,
	shapeSeen []int, shapeWork []IntegerGroupShapeWorkspace,
	batch *IntegerGroupBatch,
	visitBatch func(startRow int, groups, sums []int64) error,
) (supported bool, err error) {
	if visitBatch == nil {
		return false, nil
	}
	if batch == nil {
		batch = new(IntegerGroupBatch)
	}
	return v.visitResolvedIntegerGroups(groupResolver, sumResolver, shapeSeen, shapeWork, batch, nil, visitBatch)
}

func (v *CompactPrimaryStripeView) visitResolvedIntegerGroups(
	groupResolver, sumResolver *UnifiedHoleResolver,
	shapeSeen []int, shapeWork []IntegerGroupShapeWorkspace,
	batch *IntegerGroupBatch,
	visit func(row int, group, sum int64) error,
	visitBatch func(startRow int, groups, sums []int64) error,
) (supported bool, err error) {
	if v == nil || groupResolver == nil ||
		len(v.overflow) != 0 || len(shapeSeen) < v.shapeCount ||
		len(shapeWork) < v.shapeCount {
		return false, nil
	}
	// Admission precedes every ordinal read, so its temporary decoded keys
	// can borrow the already charged integer arena. The memory contains no
	// pointers, and the byte view stays within the supplied slice's bounds.
	keySlots := min(len(shapeSeen), 1024/int(unsafe.Sizeof(int(0))))
	keyScratch := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(shapeSeen))),
		keySlots*int(unsafe.Sizeof(int(0))))
	usedOrdinals := shapeSeen[:max(v.shapeCount, keySlots)]
	defer func() {
		if !supported {
			clear(usedOrdinals)
		}
	}()
	// Fast path: uniform single-shape stripes (the common table layout) decode
	// dictionary spellings once into stack tables and unpack 32-row batches via
	// SIMD/reservoir kernels without per-row shape lookups or ASCII reparsing.
	if v.shapeCount == 1 {
		entry, ok := v.shapeEntry(0)
		if !ok || entry.rows != v.rows {
			return false, nil
		}
		groupHole := resolveCompactProjectionTemplate(groupResolver, entry.template, keyScratch)
		if groupHole < 0 || groupHole >= entry.template.holes {
			return false, nil
		}
		groupStream, ok := compactIntegerStreamRawAt(entry, groupHole, v.rows)
		if !ok {
			return false, nil
		}
		var groupDec integerStreamFastDecoder
		if !prepareIntegerStreamFast(groupStream, &groupDec) {
			return false, nil
		}
		var (
			sumDec  integerStreamFastDecoder
			sameCol bool
		)
		if sumResolver != nil {
			sumHole := resolveCompactProjectionTemplate(sumResolver, entry.template, keyScratch)
			if sumHole < 0 || sumHole >= entry.template.holes {
				return false, nil
			}
			if sumHole == groupHole {
				sameCol = true
			} else {
				sumStream, ok := compactIntegerStreamRawAt(entry, sumHole, v.rows)
				if !ok || !prepareIntegerStreamFast(sumStream, &sumDec) {
					return false, nil
				}
			}
		}
		clear(usedOrdinals)
		if visitBatch != nil {
			for row := 0; row < v.rows; row += 32 {
				n := min(32, v.rows-row)
				if !groupDec.decodeBatch(row, n, &batch.Groups) {
					return false, nil
				}
				var sums []int64
				if sumResolver != nil {
					if sameCol {
						sums = batch.Groups[:n]
					} else {
						if !sumDec.decodeBatch(row, n, &batch.Sums) {
							return false, nil
						}
						sums = batch.Sums[:n]
					}
				}
				if err := visitBatch(row, batch.Groups[:n], sums); err != nil {
					return false, err
				}
			}
		} else {
			var groupBuf, sumBuf [32]int64
			for row := 0; row < v.rows; row += 32 {
				n := min(32, v.rows-row)
				if !groupDec.decodeBatch(row, n, &groupBuf) {
					return false, nil
				}
				if sumResolver != nil {
					if sameCol {
						for i := range n {
							if err := visit(row+i, groupBuf[i], groupBuf[i]); err != nil {
								return false, err
							}
						}
					} else {
						if !sumDec.decodeBatch(row, n, &sumBuf) {
							return false, nil
						}
						for i := range n {
							if err := visit(row+i, groupBuf[i], sumBuf[i]); err != nil {
								return false, err
							}
						}
					}
				} else {
					for i := range n {
						if err := visit(row+i, groupBuf[i], 0); err != nil {
							return false, err
						}
					}
				}
			}
		}
		shapeSeen[0] = v.rows
		return true, nil
	}
	defer clear(shapeWork[:v.shapeCount])
	for shape := 0; shape < v.shapeCount; shape++ {
		entry, ok := v.shapeEntry(shape)
		if !ok || entry.rows < 0 {
			return false, nil
		}
		groupHole := resolveCompactProjectionTemplate(groupResolver, entry.template, keyScratch)
		if groupHole < 0 || groupHole >= entry.template.holes {
			// UnifiedHoleAbsent and UnifiedHoleContainer are both negative;
			// neither has SQL's all-integer semantics required by this lane.
			return false, nil
		}
		groupStream, ok := compactIntegerStreamAt(entry, groupHole, v.rows)
		if !ok {
			return false, nil
		}
		var sumStream compactStreamView
		if sumResolver != nil {
			sumHole := resolveCompactProjectionTemplate(sumResolver, entry.template, keyScratch)
			if sumHole < 0 || sumHole >= entry.template.holes {
				return false, nil
			}
			if sumHole == groupHole {
				sumStream = groupStream
			} else {
				sumStream, ok = compactIntegerStreamAt(entry, sumHole, v.rows)
				if !ok {
					return false, nil
				}
			}
		}
		shapeWork[shape] = IntegerGroupShapeWorkspace{
			rows: entry.rows, group: groupStream, sum: sumStream,
		}
	}
	// Prepare every target stream before invoking the first callback. The row
	// loop then follows physical order, while shapeSeen carries each stream's
	// local ordinal. A shape-major callback order would alter first-seen group
	// ordering for unordered output whenever shapes are interleaved.
	clear(usedOrdinals)
	if visitBatch != nil {
		for base := 0; base < v.rows; base += 32 {
			n := min(32, v.rows-base)
			for i := range n {
				row := base + i
				shape := v.rowShape(row)
				if shape < 0 || shape >= v.shapeCount {
					return false, nil
				}
				ordinal := shapeSeen[shape]
				shapeSeen[shape]++
				entry := &shapeWork[shape]
				if ordinal >= entry.rows {
					return false, nil
				}
				group, ok := unifiedCompactIntegerValue(entry.group, entry.group.shapeCoordinate(row, ordinal))
				if !ok {
					return false, nil
				}
				batch.Groups[i] = group
				if sumResolver != nil {
					sum, ok := unifiedCompactIntegerValue(entry.sum, entry.sum.shapeCoordinate(row, ordinal))
					if !ok {
						return false, nil
					}
					batch.Sums[i] = sum
				}
			}
			var sums []int64
			if sumResolver != nil {
				sums = batch.Sums[:n]
			}
			if err := visitBatch(base, batch.Groups[:n], sums); err != nil {
				return false, err
			}
		}
	} else {
		for row := 0; row < v.rows; row++ {
			shape := v.rowShape(row)
			if shape < 0 || shape >= v.shapeCount {
				return false, nil
			}
			ordinal := shapeSeen[shape]
			shapeSeen[shape]++
			entry := &shapeWork[shape]
			if ordinal >= entry.rows {
				return false, nil
			}
			group, ok := unifiedCompactIntegerValue(entry.group, entry.group.shapeCoordinate(row, ordinal))
			if !ok {
				return false, nil
			}
			var sum int64
			if sumResolver != nil {
				sum, ok = unifiedCompactIntegerValue(entry.sum, entry.sum.shapeCoordinate(row, ordinal))
				if !ok {
					return false, nil
				}
			}
			if err := visit(row, group, sum); err != nil {
				return false, err
			}
		}
	}
	for shape := 0; shape < v.shapeCount; shape++ {
		if shapeSeen[shape] != shapeWork[shape].rows {
			return false, nil
		}
	}
	return true, nil
}
