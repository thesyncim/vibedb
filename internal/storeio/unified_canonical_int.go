package storeio

import "strconv"

// A number spelling is admitted to the typed integer token only when it
// matches -?(0|[1-9][0-9]{0,17}) and is not -0. Such a spelling is the
// unique minimal decimal spelling of its int64 value, so
// strconv.AppendInt regenerates it byte-identically: no leading zeros, no
// sign on zero, no plus, no fraction, no exponent, and the 18-digit cap
// guarantees the value fits int64 with no range check (the largest admitted
// magnitude, 999999999999999999, is below 2^63-1). This is the entire
// number-rewriting policy: rewrite only where identity is provable; every
// other spelling stays a verbatim literal. The differential test in
// unified_canonical_int_test.go pins admitted ⇒ byte-identical regeneration
// and rejected ⇒ stored-literal round-trip.

// CanonicalIntMaxDigits is the digit cap that makes the int64 fit proof
// one-directional.
const CanonicalIntMaxDigits = 18

const canonicalDecimalPairs = "" +
	"00010203040506070809" +
	"10111213141516171819" +
	"20212223242526272829" +
	"30313233343536373839" +
	"40414243444546474849" +
	"50515253545556575859" +
	"60616263646566676869" +
	"70717273747576777879" +
	"80818283848586878889" +
	"90919293949596979899"

// CanonicalIntValue is the admission predicate: it reports whether spelling
// is a canonical integer token and, when it is, the int64 value whose
// AppendCanonicalInt spelling is byte-identical to the input.
func CanonicalIntValue(spelling []byte) (int64, bool) {
	digits := spelling
	neg := false
	if len(digits) > 0 && digits[0] == '-' {
		neg = true
		digits = digits[1:]
	}
	if len(digits) == 0 || len(digits) > CanonicalIntMaxDigits {
		return 0, false
	}
	if digits[0] == '0' {
		// "0" alone is canonical; a leading zero before more digits is not,
		// and -0 is excluded because AppendInt(0) spells "0".
		if len(digits) != 1 || neg {
			return 0, false
		}
		return 0, true
	}
	var v int64
	for _, c := range digits {
		if c < '0' || c > '9' {
			return 0, false
		}
		// No overflow possible: 18 digits max stays under 2^63.
		v = v*10 + int64(c-'0')
	}
	if neg {
		v = -v
	}
	return v, true
}

// exactDecimalInt64Value reports whether a validated JSON number denotes an
// exact int64 value, regardless of spelling. Unlike CanonicalIntValue it
// accepts fractions and exponents when their mathematical value is integral,
// so 1.0, 10e-1, and 1e0 all return 1. It is the compiled-needle fast path for
// comparing a decimal query literal against the leaf grammar's typed integers.
func exactDecimalInt64Value(spelling []byte) (int64, bool) {
	i := 0
	neg := false
	if spelling[i] == '-' {
		neg = true
		i++
	}
	mantissaStart := i
	mantissaEnd := len(spelling)
	dot := -1
	totalDigits := 0
	fractionDigits := 0
	nonzero := false
	for i < len(spelling) && spelling[i] != 'e' && spelling[i] != 'E' {
		if spelling[i] == '.' {
			dot = i
		} else {
			totalDigits++
			if dot >= 0 {
				fractionDigits++
			}
			nonzero = nonzero || spelling[i] != '0'
		}
		i++
	}
	if i < len(spelling) {
		mantissaEnd = i
	}
	if !nonzero {
		return 0, true
	}

	var exponent int64
	if i < len(spelling) {
		i++
		exponentNeg := false
		if spelling[i] == '+' || spelling[i] == '-' {
			exponentNeg = spelling[i] == '-'
			i++
		}
		for i < len(spelling) && spelling[i] == '0' {
			i++
		}
		if len(spelling)-i > 18 {
			return 0, false
		}
		for ; i < len(spelling); i++ {
			exponent = exponent*10 + int64(spelling[i]-'0')
		}
		if exponentNeg {
			exponent = -exponent
		}
	}
	scale := exponent - int64(fractionDigits)
	keepDigits := totalDigits
	if scale < 0 {
		drop := -scale
		if drop > int64(totalDigits) {
			return 0, false
		}
		trailing := int64(0)
		for at := mantissaEnd - 1; at >= mantissaStart; at-- {
			if spelling[at] == '.' {
				continue
			}
			if spelling[at] != '0' {
				break
			}
			trailing++
		}
		if trailing < drop {
			return 0, false
		}
		keepDigits -= int(drop)
		scale = 0
	}
	if scale > 19 {
		return 0, false
	}

	limit := uint64(^uint64(0) >> 1)
	if neg {
		limit++
	}
	var magnitude uint64
	seen := 0
	for at := mantissaStart; at < mantissaEnd && seen < keepDigits; at++ {
		if spelling[at] == '.' {
			continue
		}
		digit := uint64(spelling[at] - '0')
		if magnitude > (limit-digit)/10 {
			return 0, false
		}
		magnitude = magnitude*10 + digit
		seen++
	}
	for ; scale > 0; scale-- {
		if magnitude > limit/10 {
			return 0, false
		}
		magnitude *= 10
	}
	if neg {
		if magnitude == uint64(1)<<63 {
			return -1 << 63, true
		}
		return -int64(magnitude), true
	}
	return int64(magnitude), true
}

// AppendCanonicalInt regenerates the canonical spelling of an admitted
// value. For any (v, true) from CanonicalIntValue the output is
// byte-identical to the admitted spelling — the identity the
// differential test pins.
func AppendCanonicalInt(dst []byte, v int64) []byte {
	if v >= 0 && v < 1_000_000 {
		return appendCanonicalUint6(dst, uint64(v))
	}
	if v < 0 && v > -1_000_000 {
		dst = append(dst, '-')
		return appendCanonicalUint6(dst, uint64(-v))
	}
	return strconv.AppendInt(dst, v, 10)
}

// appendCanonicalUint6 is the scan-dominant integer renderer. IDs, counters,
// ordinals, and small measurements account for nearly all integer tokens; a
// fixed digit-pair table avoids strconv's general base-10 setup on that lane.
// Larger magnitudes retain strconv's heavily tuned generic path above.
func appendCanonicalUint6(dst []byte, v uint64) []byte {
	pair := func(value uint64) (byte, byte) {
		at := value * 2
		return canonicalDecimalPairs[at], canonicalDecimalPairs[at+1]
	}
	switch {
	case v < 10:
		return append(dst, byte(v)+'0')
	case v < 100:
		a, b := pair(v)
		return append(dst, a, b)
	case v < 1_000:
		rest := v % 100
		a, b := pair(rest)
		return append(dst, byte(v/100)+'0', a, b)
	case v < 10_000:
		leading, rest := v/100, v%100
		a, b := pair(leading)
		c, d := pair(rest)
		return append(dst, a, b, c, d)
	case v < 100_000:
		leading, rest := v/10_000, v%10_000
		middle, trailing := rest/100, rest%100
		a, b := pair(middle)
		c, d := pair(trailing)
		return append(dst, byte(leading)+'0', a, b, c, d)
	default:
		leading, rest := v/10_000, v%10_000
		middle, trailing := rest/100, rest%100
		a, b := pair(leading)
		c, d := pair(middle)
		e, f := pair(trailing)
		return append(dst, a, b, c, d, e, f)
	}
}

// appendFixedUint8 renders a known sub-100M value with all eight digits,
// including leading zeroes. Arithmetic prefix keys use this fixed width and
// otherwise paid to render a minimal integer before shifting it for padding.
func appendFixedUint8(dst []byte, v uint32) []byte {
	pair := func(value uint32) (byte, byte) {
		at := value * 2
		return canonicalDecimalPairs[at], canonicalDecimalPairs[at+1]
	}
	hi := v / 10_000
	lo := v - hi*10_000
	a := hi / 100
	b := hi - a*100
	c := lo / 100
	d := lo - c*100
	a0, a1 := pair(a)
	b0, b1 := pair(b)
	c0, c1 := pair(c)
	d0, d1 := pair(d)
	return append(dst, a0, a1, b0, b1, c0, c1, d0, d1)
}

// AppendZigzagVarint appends the canonical-integer token payload: zigzag
// mapping (small magnitudes of either sign become small unsigned values)
// followed by base-128 varint, little-endian groups, high bit as
// continuation. At most 10 bytes; the corpus-typical 5-digit id takes 3.
func AppendZigzagVarint(dst []byte, v int64) []byte {
	u := uint64(v)<<1 ^ uint64(v>>63)
	for u >= 0x80 {
		dst = append(dst, byte(u)|0x80)
		u >>= 7
	}
	return append(dst, byte(u))
}

// DecodeZigzagVarint decodes one zigzag varint from the front of b,
// returning the value and the number of bytes consumed. A truncated,
// oversized (more than 10 bytes), or non-minimal-length final-byte overflow
// input returns n == 0: the decoder fails closed rather than guessing,
// because a token stream is trusted only as far as its checksummed page and
// a bad length here would misframe every following token.
func DecodeZigzagVarint(b []byte) (v int64, n int) {
	var u uint64
	var shift uint
	for i := 0; i < len(b); i++ {
		c := b[i]
		if shift == 63 && c > 1 {
			// The tenth byte may only contribute the top bit of a uint64.
			return 0, 0
		}
		u |= uint64(c&0x7f) << shift
		if c < 0x80 {
			return int64(u>>1) ^ -int64(u&1), i + 1
		}
		shift += 7
		if shift > 63 {
			return 0, 0
		}
	}
	return 0, 0
}
