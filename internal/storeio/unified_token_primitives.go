package storeio

// UnifiedTokenSpan names one exact canonical JSON scalar spelling in a
// unified-leaf row. Spans are ordered, non-overlapping byte ranges. The bytes
// between them form the row's structural template; the spans themselves are
// encoded as typed, dictionary, or literal tokens.
type UnifiedTokenSpan struct {
	Start uint32
	End   uint32
}

// unifiedDictionaryCandidate is the unified leaf planner's transient
// dictionary census record.
type unifiedDictionaryCandidate struct {
	value  string
	count  int
	saving int
}

func unifiedTokenUvarintLen(value uint32) int {
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

func putUnifiedTokenUvarint(dst []byte, value uint32) int {
	n := 0
	for value >= 0x80 {
		dst[n] = byte(value) | 0x80
		value >>= 7
		n++
	}
	dst[n] = byte(value)
	return n + 1
}

func readUnifiedTokenUvarint(src []byte) (uint32, int, bool) {
	var value uint32
	for i := 0; i < 5 && i < len(src); i++ {
		b := src[i]
		if i == 4 && b > 0x0f {
			return 0, 0, false
		}
		value |= uint32(b&0x7f) << (7 * i)
		if b&0x80 == 0 {
			// Reject non-minimal encodings so one value has one wire spelling.
			if i != 0 && b == 0 {
				return 0, 0, false
			}
			return value, i + 1, true
		}
	}
	return 0, 0, false
}
