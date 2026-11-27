package query

import (
	"encoding/binary"

	vibejson "github.com/thesyncim/vibejson"
)

// JSONGroupKeyEncoder appends the query engine's canonical GROUP BY identity
// for validated JSON cells. It retains only reusable decoded-string scratch;
// returned keys belong to the caller's destination. The zero value is ready.
// An encoder is not safe for concurrent use.
type JSONGroupKeyEncoder struct {
	text []byte
}

// Append validates value as exactly one JSON value and appends its canonical
// GROUP BY identity to dst. Numeric spelling variants with the same exact
// value and escaped/unescaped spellings of the same string receive identical
// keys. Containers retain the query engine's exact-source identity.
func (e *JSONGroupKeyEncoder) Append(dst, value []byte) ([]byte, bool) {
	if e == nil || len(value) == 0 {
		return dst, false
	}
	if value[0] <= ' ' || value[len(value)-1] <= ' ' {
		value = trimJSONGroupSpace(value)
		if len(value) == 0 {
			return dst, false
		}
	}
	// Fast path for the dominant canonical wire tokens: plain JSON integers,
	// unescaped ASCII JSON strings, null, and booleans. Each branch validates
	// exact RFC 8259 grammar in one pass and emits the bit-for-bit identical
	// encoding produced by appendGroupKey. Any complex token (decimals,
	// exponents, escaped or non-ASCII strings, containers, invalid input) falls
	// through to the general vibejson validator and classifier.
	switch value[0] {
	case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-':
		if out, ok := appendFastPlainIntegerGroupKey(dst, value); ok {
			return out, true
		}
	case '"':
		if out, ok := appendFastPlainASCIIStringGroupKey(dst, value); ok {
			return out, true
		}
	case 'n':
		if len(value) == 4 && value[1] == 'u' && value[2] == 'l' && value[3] == 'l' {
			return append(dst, gkNull), true
		}
	case 't':
		if len(value) == 4 && value[1] == 'r' && value[2] == 'u' && value[3] == 'e' {
			return append(dst, gkBool, 1), true
		}
	case 'f':
		if len(value) == 5 && value[1] == 'a' && value[2] == 'l' && value[3] == 's' && value[4] == 'e' {
			return append(dst, gkBool, 0), true
		}
	}
	if !vibejson.Valid(value) {
		return dst, false
	}
	e.text = e.text[:0]
	scalar := classifyRawInto(vibejson.RawValue{Src: value}, &e.text)
	return appendGroupKey(dst, scalar), true
}

func appendFastPlainIntegerGroupKey(dst, value []byte) ([]byte, bool) {
	neg := byte(0)
	digits := value
	if digits[0] == '-' {
		neg = 1
		digits = digits[1:]
		if len(digits) == 0 {
			return dst, false
		}
	}
	if digits[0] == '0' {
		if len(digits) == 1 {
			return append(dst, gkNumber, 0x00), true
		}
		return dst, false
	}
	if digits[0] < '1' || digits[0] > '9' {
		return dst, false
	}
	for i := 1; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return dst, false
		}
	}
	weight := len(digits) - 1
	sigEnd := len(digits)
	for sigEnd > 1 && digits[sigEnd-1] == '0' {
		sigEnd--
	}
	sig := digits[:sigEnd]
	dst = append(dst, gkNumber, 0x01, neg)
	switch {
	case weight == 0:
		dst = append(dst, 0x00)
	case weight < 10:
		dst = append(dst, 0x01, 0x00, 0x01, byte('0'+weight))
	case weight < 100:
		dst = append(dst, 0x01, 0x00, 0x02, byte('0'+weight/10), byte('0'+weight%10))
	default:
		dst = appendWeightKey(dst, decimalWeight{compact: int64(weight)})
	}
	if len(sig) < 0x80 {
		dst = append(dst, byte(len(sig)))
	} else {
		dst = binary.AppendUvarint(dst, uint64(len(sig)))
	}
	return append(dst, sig...), true
}

func appendFastPlainASCIIStringGroupKey(dst, value []byte) ([]byte, bool) {
	if len(value) < 2 || value[len(value)-1] != '"' {
		return dst, false
	}
	inner := value[1 : len(value)-1]
	for i := 0; i < len(inner); i++ {
		c := inner[i]
		if c < 0x20 || c >= 0x80 || c == '"' || c == '\\' {
			return dst, false
		}
	}
	dst = append(dst, gkString)
	if len(inner) < 0x80 {
		dst = append(dst, byte(len(inner)))
	} else {
		dst = binary.AppendUvarint(dst, uint64(len(inner)))
	}
	return append(dst, inner...), true
}

func trimJSONGroupSpace(value []byte) []byte {
	start, end := 0, len(value)
	for start < end && isJSONGroupSpace(value[start]) {
		start++
	}
	for end > start && isJSONGroupSpace(value[end-1]) {
		end--
	}
	return value[start:end]
}

func isJSONGroupSpace(value byte) bool {
	return value == ' ' || value == '\t' || value == '\r' || value == '\n'
}
