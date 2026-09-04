package storeio

import "encoding/binary"

// seek primes an admitted scalar stream at an arbitrary ordinal. Fixed-width
// streams seek directly; delta and alphabet streams replay only metadata from
// their nearest 64-row restart. Unsupported representations keep random-rank
// decoding. No rendered values or additional retained storage are needed.
func (s *compactStreamSequentialState) seek(v *compactStreamView, row int) {
	if row < 0 || row >= v.count {
		return
	}
	var next compactStreamSequentialState
	switch v.kind {
	case compactStreamDictionary, compactStreamFOR, compactStreamDate:
		data := v.data
		width := int(v.width)
		switch v.kind {
		case compactStreamDictionary:
			if width > 16 {
				return
			}
		case compactStreamFOR:
			if len(data) < 8 || width > 56 {
				return
			}
			data = data[8:]
		case compactStreamDate:
			if len(data) < 4 || width > 56 {
				return
			}
			data = data[4:]
		}
		bit := row * width
		if bit+width > len(data)*8 {
			return
		}
		next.cursor = bit / 8
		if shift := bit & 7; shift != 0 {
			next.value = int64(data[next.cursor] >> shift)
			next.bit = 8 - shift
			next.cursor++
		}
	case compactStreamDelta, compactStreamDeltaPack, compactStreamPrefixInt:
		prefix := 0
		if v.kind == compactStreamPrefixInt {
			if len(v.data) < 2 || v.data[0]&2 != 0 {
				return
			}
			prefix = 2
		}
		within := row % compactStreamRestart
		if within != 0 {
			at := prefix + row/compactStreamRestart*4
			if at+4 > len(v.data) {
				return
			}
			next.cursor = int(binary.LittleEndian.Uint32(v.data[at:]))
			if next.cursor > len(v.data)-8 {
				return
			}
			next.value = int64(binary.LittleEndian.Uint64(v.data[next.cursor:]))
			next.cursor += 8
			packed := v.kind == compactStreamDeltaPack ||
				v.kind == compactStreamPrefixInt && v.data[0]&4 != 0
			if packed {
				if next.cursor >= len(v.data) {
					return
				}
				next.width = int(v.data[next.cursor])
				next.cursor++
				if next.width > 64 || within*next.width > (len(v.data)-next.cursor)*8 {
					return
				}
			}
			for i := 1; i < within; i++ {
				var u uint64
				if packed {
					u = compactReadBits(v.data[next.cursor:], (i-1)*next.width, next.width)
				} else {
					value, n, ok := readCompactUvarint(v.data[next.cursor:])
					if !ok {
						return
					}
					u = value
					next.cursor += n
				}
				next.value += int64(u>>1) ^ -int64(u&1)
			}
		}
	case compactStreamAlphabet:
		at := row / compactStreamRestart * 4
		if at+4 > len(v.data) || len(v.dictDir) < 2 {
			return
		}
		next.cursor = int(binary.LittleEndian.Uint32(v.data[at:]))
		if next.cursor >= len(v.data) {
			return
		}
		base, n, ok := readCompactUvarint(v.data[next.cursor:])
		if !ok || base > CommonPrimaryLeafMaxExtentBytes {
			return
		}
		next.value = int64(base)
		next.cursor += n
		if next.cursor >= len(v.data) {
			return
		}
		next.width = int(v.data[next.cursor])
		next.cursor++
		rows := min(compactStreamRestart, v.count-row/compactStreamRestart*compactStreamRestart)
		lengthBytes := (rows*next.width + 7) / 8
		if next.width > 64 || lengthBytes > len(v.data)-next.cursor {
			return
		}
		next.lengthBit = next.cursor * 8
		next.bit = (next.cursor + lengthBytes) * 8
		for i := 0; i < row%compactStreamRestart; i++ {
			length := base + compactReadBits(v.data, next.lengthBit, next.width)
			if length > CommonPrimaryLeafMaxExtentBytes {
				return
			}
			next.lengthBit += next.width
			next.bit += int(length) * int(v.width)
			if next.bit > len(v.data)*8 {
				return
			}
		}
	default:
		return
	}
	next.next = row
	*s = next
}
