package storeio

import "encoding/binary"

// countIntegerExtrema returns exact extrema for a checked bare PrefixInt or
// signed integer FOR stream. A width-64 stream, or a sub-64 stream whose base
// plus the maximum representable delta would cross MaxInt64, is declined:
// unsigned lane ordering is then no longer signed ordering and the native
// aggregate must leave the complete query to the generic executor.
func (v compactStreamView) countIntegerExtrema() (
	minimum, maximum int64, found, supported bool,
) {
	if v.kind == compactStreamPrefixInt {
		first, step, supported := v.barePrefixIntegerArithmetic()
		if !supported {
			return 0, 0, false, false
		}
		if v.count == 0 {
			return 0, 0, false, true
		}
		last := first
		if v.count > 1 {
			last += step * int64(v.count-1)
		}
		if last < first {
			return last, first, true, true
		}
		return first, last, true, true
	}
	if !v.validIntegerFORData() || v.width == 64 || v.dictCount != 0 ||
		len(v.dictDir) != 0 || len(v.dictData) != 0 {
		return 0, 0, false, false
	}
	base := int64(binary.LittleEndian.Uint64(v.data))
	if v.width > 0 {
		maxDelta := (uint64(1) << v.width) - 1
		const maxInt64Value = uint64(1<<63 - 1)
		if base >= 0 && maxDelta > maxInt64Value-uint64(base) {
			return 0, 0, false, false
		}
	}
	minDelta, maxDelta, packedFound, packedSupported :=
		countCompactPackedExtrema(v.data[8:], v.count, int(v.width))
	if !packedSupported || !packedFound {
		return 0, 0, packedFound, packedSupported
	}
	return int64(uint64(base) + minDelta),
		int64(uint64(base) + maxDelta), true, true
}

// countCompactPackedExtrema returns the unsigned minimum and maximum packed
// lane values. The caller adds the FOR base after proving that reconstruction
// cannot wrap. found is false only for an empty input; malformed geometry is
// also reported as unsupported so a storage-native aggregate can decline
// atomically and leave the generic executor authoritative.
func countCompactPackedExtrema(
	data []byte, count, width int,
) (minimum, maximum uint64, found, supported bool) {
	if count < 0 || width < 0 || width > 64 {
		return 0, 0, false, false
	}
	packedBytes := (uint64(count)*uint64(width) + 7) / 8
	if uint64(len(data)) != packedBytes {
		return 0, 0, false, false
	}
	if count == 0 {
		return 0, 0, false, true
	}
	if width == 0 {
		return 0, 0, true, true
	}
	if width == 7 {
		return countCompactPacked7ExtremaImpl(data, count)
	}
	if width == 8 {
		return countCompactPacked8ExtremaImpl(data, count)
	}
	if width == 10 {
		return countCompactPacked10ExtremaImpl(data, count)
	}
	if width == 16 {
		return countCompactPacked16ExtremaImpl(data, count)
	}
	if width > 56 {
		minimum = ^uint64(0)
		for row := 0; row < count; row++ {
			value := compactReadBits(data, row*width, width)
			if value < minimum {
				minimum = value
			}
			if value > maximum {
				maximum = value
			}
		}
		return minimum, maximum, true, true
	}
	mask := uint64(1)<<uint(width) - 1
	minimum = mask
	var reservoir uint64
	available := 0
	cursor := 0
	for range count {
		for available < width {
			reservoir |= uint64(data[cursor]) << uint(available)
			cursor++
			available += 8
		}
		value := reservoir & mask
		if value < minimum {
			minimum = value
		}
		if value > maximum {
			maximum = value
		}
		reservoir >>= uint(width)
		available -= width
	}
	return minimum, maximum, true, true
}

func countCompactPacked8ExtremaScalar(
	data []byte, count int,
) (minimum, maximum uint64, found, supported bool) {
	if count <= 0 {
		return 0, 0, false, true
	}
	minimum = uint64(data[0])
	maximum = minimum
	for row := 1; row < count; row++ {
		value := uint64(data[row])
		if value < minimum {
			minimum = value
		}
		if value > maximum {
			maximum = value
		}
	}
	return minimum, maximum, true, true
}

func countCompactPacked16ExtremaScalar(
	data []byte, count int,
) (minimum, maximum uint64, found, supported bool) {
	if count <= 0 {
		return 0, 0, false, true
	}
	minimum = uint64(binary.LittleEndian.Uint16(data))
	maximum = minimum
	for row := 1; row < count; row++ {
		value := uint64(binary.LittleEndian.Uint16(data[row*2:]))
		if value < minimum {
			minimum = value
		}
		if value > maximum {
			maximum = value
		}
	}
	return minimum, maximum, true, true
}

func countCompactPacked7ExtremaScalar(
	data []byte, count int,
) (minimum, maximum uint64, found, supported bool) {
	if count <= 0 {
		return 0, 0, false, true
	}
	row, cursor := 0, 0
	minimum = 127
	for ; row+8 <= count; row, cursor = row+8, cursor+7 {
		packed := uint64(data[cursor]) |
			uint64(data[cursor+1])<<8 |
			uint64(data[cursor+2])<<16 |
			uint64(data[cursor+3])<<24 |
			uint64(data[cursor+4])<<32 |
			uint64(data[cursor+5])<<40 |
			uint64(data[cursor+6])<<48
		for at := 0; at < 8; at++ {
			value := packed >> uint(at*7) & 127
			if value < minimum {
				minimum = value
			}
			if value > maximum {
				maximum = value
			}
		}
	}
	for ; row < count; row++ {
		value := compactReadBits(data, row*7, 7)
		if value < minimum {
			minimum = value
		}
		if value > maximum {
			maximum = value
		}
	}
	return minimum, maximum, true, true
}

func countCompactPacked10ExtremaScalar(
	data []byte, count int,
) (minimum, maximum uint64, found, supported bool) {
	if count <= 0 {
		return 0, 0, false, true
	}
	row, cursor := 0, 0
	minimum = 1023
	for ; row+4 <= count; row, cursor = row+4, cursor+5 {
		packed := uint64(data[cursor]) |
			uint64(data[cursor+1])<<8 |
			uint64(data[cursor+2])<<16 |
			uint64(data[cursor+3])<<24 |
			uint64(data[cursor+4])<<32
		for at := 0; at < 4; at++ {
			value := packed >> uint(at*10) & 1023
			if value < minimum {
				minimum = value
			}
			if value > maximum {
				maximum = value
			}
		}
	}
	for ; row < count; row++ {
		value := compactReadBits(data, row*10, 10)
		if value < minimum {
			minimum = value
		}
		if value > maximum {
			maximum = value
		}
	}
	return minimum, maximum, true, true
}
