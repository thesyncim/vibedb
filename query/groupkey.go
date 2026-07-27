package query

import "encoding/binary"

// GROUP BY key encoding.
//
// A group key is a self-delimiting byte string with one property: two scalars
// encode to equal bytes exactly when compareScalar reports them equal. GROUP
// BY can then intern the encoded key through the core's KeyInterner and treat
// interner-identity as group-identity, one hash-and-probe per row, while the
// reference executor partitions by compareScalar directly — the differential
// checks the two partitions match, which is only meaningful because the
// encoding and the order agree by construction.
//
// The tricky case is numbers: equal *values* with different spellings (1 and
// 1.0, 100 and 1e2, 0 and -0) must land in one group. The key therefore
// encodes a number's exact decimal decomposition — sign, weight, and trimmed
// significant digits — never its spelling and never a float64, matching the
// value equality compareScalar uses.
//
// Composite keys (GROUP BY over several paths) concatenate each component's
// encoding; every component is self-delimiting, so the concatenation is
// unambiguous.

const (
	gkNull      = 0x00
	gkBool      = 0x01
	gkNumber    = 0x02
	gkString    = 0x03
	gkContainer = 0x04
)

// appendGroupKey appends s's group-key encoding to dst and returns it.
func appendGroupKey(dst []byte, s scalar) []byte {
	switch s.kind {
	case kindNull:
		return append(dst, gkNull)
	case kindBool:
		b := byte(0)
		if s.bval {
			b = 1
		}
		return append(dst, gkBool, b)
	case kindNumber:
		return appendNumberKey(dst, s.num)
	case kindString:
		dst = append(dst, gkString)
		dst = binary.AppendUvarint(dst, uint64(len(s.sval)))
		return append(dst, s.sval...)
	default:
		dst = append(dst, gkContainer)
		dst = binary.AppendUvarint(dst, uint64(len(s.raw)))
		return append(dst, s.raw...)
	}
}

// appendNumberKey encodes a JSON number by its exact decimal decomposition so
// that all spellings of one value share a key. It encodes the sign, the
// weight, and the *concatenated* significant-digit sequence — the same
// sequence compareMagnitude walks — never the integer/fraction split, since
// one value can place its digits either side of the point (1 and 0.1e1 both
// reduce to the sequence "1" at weight 0). A zero carries no sign (−0 and 0 are
// one value); a wide exponent (beyond int64 weight) folds its exponent literal
// into the key, matching compareWideExp's equivalence.
func appendNumberKey(dst []byte, num []byte) []byte {
	d := parseDecimal(num)
	dst = append(dst, gkNumber)
	if d.zero {
		return append(dst, 0x00) // zero marker, unsigned
	}
	neg := byte(0)
	if d.neg {
		neg = 1
	}
	dst = append(dst, 0x01, neg)
	if d.expWide {
		en := byte(0)
		if d.expNeg {
			en = 1
		}
		dst = append(dst, 0x01, en)
		dst = binary.AppendUvarint(dst, uint64(len(d.expDigit)))
		dst = append(dst, d.expDigit...)
	} else {
		dst = append(dst, 0x00)
		dst = binary.BigEndian.AppendUint64(dst, uint64(d.weight))
	}
	dst = binary.AppendUvarint(dst, uint64(len(d.intDigits)+len(d.fracDigits)))
	dst = append(dst, d.intDigits...)
	return append(dst, d.fracDigits...)
}
