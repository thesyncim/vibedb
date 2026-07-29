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
// encodes a number's exact decimal decomposition — sign, arbitrary-precision
// weight, and trimmed significant digits — never its spelling and never a
// float64, matching the value equality compareScalar uses.
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

func scalarGroupKeyBytes(s scalar) int64 {
	switch s.kind {
	case kindNull:
		return 1
	case kindBool:
		return 2
	case kindNumber:
		d := parseDecimal(s.num)
		if d.zero {
			return 2
		}
		weight := d.weight
		if !weight.wide {
			weight = wideWeightFromInt64(weight.compact)
		}
		magnitude := weightMagnitudeLen(&weight)
		weightBytes := int64(1)
		if magnitude != 0 {
			weightBytes = saturatedBytes(
				2+groupKeyUvarintBytes(uint64(magnitude)),
				int64(magnitude),
			)
		}
		digits := saturatedBytes(int64(len(d.intDigits)), int64(len(d.fracDigits)))
		return saturatedBytes(
			3,
			saturatedBytes(
				weightBytes,
				saturatedBytes(groupKeyUvarintBytes(uint64(digits)), digits),
			),
		)
	case kindString:
		n := int64(len(s.sval))
		return saturatedBytes(
			1+groupKeyUvarintBytes(uint64(n)),
			n,
		)
	default:
		n := int64(len(s.raw))
		return saturatedBytes(
			1+groupKeyUvarintBytes(uint64(n)),
			n,
		)
	}
}

func groupKeyUvarintBytes(n uint64) int64 {
	bytes := int64(1)
	for n >= 0x80 {
		bytes++
		n >>= 7
	}
	return bytes
}

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
// one value). The weight is emitted as its canonical signed decimal integer,
// including the mantissa-position adjustment. Thus exponent spellings on
// opposite sides of an int64 boundary, and spellings that induce an
// arbitrarily long carry or borrow, still share a key exactly when their
// adjusted weights are equal.
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
	dst = appendWeightKey(dst, d.weight)
	dst = binary.AppendUvarint(dst, uint64(len(d.intDigits)+len(d.fracDigits)))
	dst = append(dst, d.intDigits...)
	return append(dst, d.fracDigits...)
}

// appendWeightKey emits one canonical adjusted decimal weight. It appends
// directly from the weight's source view and fixed tail; no exponent-sized
// intermediate is allocated.
func appendWeightKey(dst []byte, w decimalWeight) []byte {
	if !w.wide {
		w = wideWeightFromInt64(w.compact)
	}
	n := weightMagnitudeLen(&w)
	if n == 0 {
		return append(dst, 0x00)
	}
	neg := byte(0)
	if w.neg {
		neg = 1
	}
	dst = append(dst, 0x01, neg)
	dst = binary.AppendUvarint(dst, uint64(n))
	dst = append(dst, w.prefix...)
	if w.hasMiddle {
		dst = append(dst, w.middle)
	}
	for i := 0; i < w.fillLen; i++ {
		dst = append(dst, w.fill)
	}
	start := int(w.tailStart)
	return append(dst, w.tail[start:start+int(w.tailLen)]...)
}
