package storeio

import "strconv"

// The canonical-int token (unified-leaf design §3.4).
//
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
// and rejected ⇒ stored-literal round-trip, as §3.4 requires.

// CanonicalIntMaxDigits is the §3.4 digit cap that makes the int64 fit
// proof one-directional.
const CanonicalIntMaxDigits = 18

// CanonicalIntValue is the admission predicate: it reports whether spelling
// is a canonical int token (§3.4) and, when it is, the int64 value whose
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
		// No overflow possible: 18 digits max stays under 2^63 (§3.4).
		v = v*10 + int64(c-'0')
	}
	if neg {
		v = -v
	}
	return v, true
}

// AppendCanonicalInt regenerates the canonical spelling of an admitted
// value. For any (v, true) from CanonicalIntValue the output is
// byte-identical to the admitted spelling — the §3.4 identity the
// differential test pins.
func AppendCanonicalInt(dst []byte, v int64) []byte {
	return strconv.AppendInt(dst, v, 10)
}

// AppendZigzagVarint appends the token payload encoding of §3.4: zigzag
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
