// Derived from Vitess (github.com/vitessio/vitess,
// go/vt/vtgate/vindexes/xxhash.go), Apache License 2.0. The differential harness
// pins upstream v0.24.2; this xxhash keyspace-id algorithm is byte-identical
// across v0.22.0..v0.24.2 (v0.22.0 itself does not compile under this repo's Go
// 1.26 toolchain). See LICENSE-VITESS and docs/provenance.md
// (ALGO-VITESS-XXHASH-001).

package vitessroute

import (
	"encoding/binary"

	"github.com/cespare/xxhash/v2"

	"github.com/thesyncim/vibedb/distribution"
)

// vindexBytes reproduces sqltypes.Value.ToBytes for the closed placement scalar
// set, which is exactly the byte sequence a Vitess Vindex hashes:
//
//   - a String contributes its raw bytes verbatim;
//   - a Number is admitted only when it is an exact integer within the upstream
//     signed/unsigned 64-bit domain [-2^63, 2^64-1] and contributes its minimal
//     decimal ASCII, the same rendering Vitess produces for an INT64 or UINT64
//     value. Fractional and out-of-range numbers are rejected.
//
// The decimal-ASCII-for-integers rule is the load-bearing subtlety of xxhash
// compatibility: an integer never hashes as its little-endian binary form.
//
// Provenance: ALGO-VITESS-XXHASH-001.
func vindexBytes(v distribution.Scalar) ([]byte, error) {
	switch v.Kind() {
	case distribution.KindString:
		s, _ := v.StringValue()
		return []byte(s), nil
	case distribution.KindNumber:
		spelling, _ := v.NumberSpelling()
		dec, err := integerDecimalASCII(spelling)
		if err != nil {
			return nil, err
		}
		return []byte(dec), nil
	default:
		return nil, &distribution.ShardValueError{Reason: "value is not a supported placement scalar"}
	}
}

// xxhashPoint computes the xxhash Vindex keyspace id for a single value: the
// little-endian 8-byte encoding of xxhash Sum64 over the value's upstream input
// serialization, seed 0. It is byte-for-byte identical to Vitess's vXXHash over
// sqltypes.Value.ToBytes().
func xxhashPoint(v distribution.Scalar) (distribution.KeyspacePoint, error) {
	b, err := vindexBytes(v)
	if err != nil {
		return distribution.KeyspacePoint{}, err
	}
	return xxhashPointFromBytes(b), nil
}

// xxhashPointFromBytes frames the raw hash of already-serialized input bytes.
//
// Provenance: ALGO-VITESS-XXHASH-001.
func xxhashPointFromBytes(b []byte) distribution.KeyspacePoint {
	var p distribution.KeyspacePoint
	binary.LittleEndian.PutUint64(p[:], xxhash.Sum64(b))
	return p
}

// uint64 domain bounds as canonical decimal magnitudes: the largest admitted
// unsigned value (2^64-1) and the largest admitted negative magnitude (2^63,
// i.e. the magnitude of the smallest int64).
const (
	maxUnsignedMagnitude = "18446744073709551615"
	maxNegativeMagnitude = "9223372036854775808"
)

// expCap saturates a parsed exponent so an enormous but well-formed exponent
// literal (which can only denote an out-of-domain or zero value) never overflows
// int arithmetic or materializes an exponent-sized buffer.
const expCap = 1 << 30

// integerDecimalASCII validates that spelling denotes an exact integer within
// the admitted domain [-2^63, 2^64-1] and returns its minimal decimal ASCII
// form: no sign for zero (negative zero included), no leading zero, no leading
// plus. Fractional and out-of-range numbers are rejected with a typed
// *distribution.ShardValueError. spelling is a JSON number literal already
// validated by distribution.NewNumber, but this function re-parses defensively
// and trusts no external invariant.
func integerDecimalASCII(spelling string) (string, error) {
	s := spelling
	n := len(s)
	i := 0

	neg := false
	if i < n && s[i] == '-' {
		neg = true
		i++
	}

	intStart := i
	for i < n && isDigit(s[i]) {
		i++
	}
	intEnd := i
	if intEnd == intStart {
		return "", rejectNumber("number has no integer digits")
	}

	fracStart, fracEnd := i, i
	if i < n && s[i] == '.' {
		i++
		fracStart = i
		for i < n && isDigit(s[i]) {
			i++
		}
		fracEnd = i
		if fracEnd == fracStart {
			return "", rejectNumber("number has an empty fraction")
		}
	}

	expNeg := false
	var expDigits string
	if i < n && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < n && (s[i] == '+' || s[i] == '-') {
			expNeg = s[i] == '-'
			i++
		}
		expStart := i
		for i < n && isDigit(s[i]) {
			i++
		}
		if i == expStart {
			return "", rejectNumber("number has an empty exponent")
		}
		expDigits = s[expStart:i]
	}

	if i != n {
		return "", rejectNumber("number has trailing characters")
	}

	// value = combined * 10^(expVal - len(frac)), where combined is the integer
	// formed by the concatenated integer and fraction digit runs.
	fracLen := fracEnd - fracStart
	combined := s[intStart:intEnd] + s[fracStart:fracEnd]

	// Strip leading zeros; an all-zero significand is exactly zero regardless of
	// sign or exponent.
	lead := 0
	for lead < len(combined) && combined[lead] == '0' {
		lead++
	}
	core := combined[lead:]
	if core == "" {
		return "0", nil
	}

	expVal := saturatedExponent(expDigits, expNeg)
	shift := expVal - fracLen

	// Fold trailing zeros of the significand into the power of ten. Afterwards
	// core has neither a leading nor a trailing zero, so it is not divisible by
	// ten: a negative residual shift then proves the value is not an integer.
	for len(core) > 1 && core[len(core)-1] == '0' {
		core = core[:len(core)-1]
		shift++
	}
	if shift < 0 {
		return "", rejectNumber("number is not an exact integer")
	}

	// The integer magnitude is core followed by shift zeros. Its digit count
	// bounds the domain check; anything past the 20-digit unsigned maximum
	// overflows before it is ever materialized.
	digitCount := len(core) + shift
	if digitCount > len(maxUnsignedMagnitude) {
		return "", rejectNumber("number is outside the supported 64-bit domain")
	}

	buf := make([]byte, 0, digitCount)
	buf = append(buf, core...)
	for j := 0; j < shift; j++ {
		buf = append(buf, '0')
	}
	magnitude := string(buf)

	bound := maxUnsignedMagnitude
	if neg {
		bound = maxNegativeMagnitude
	}
	if compareMagnitude(magnitude, bound) > 0 {
		return "", rejectNumber("number is outside the supported 64-bit domain")
	}

	if neg {
		return "-" + magnitude, nil
	}
	return magnitude, nil
}

// saturatedExponent parses a leading-zero-tolerant exponent literal into a
// signed int, clamping the magnitude to expCap so an astronomically large
// exponent stays representable and still forces the domain check to reject.
func saturatedExponent(digits string, neg bool) int {
	v := 0
	for k := 0; k < len(digits); k++ {
		v = v*10 + int(digits[k]-'0')
		if v > expCap {
			v = expCap
			break
		}
	}
	if neg {
		return -v
	}
	return v
}

// compareMagnitude orders two canonical decimal magnitudes (no leading zero)
// numerically: the longer is larger, and equal lengths compare lexically.
func compareMagnitude(a, b string) int {
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	}
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func rejectNumber(reason string) error {
	return &distribution.ShardValueError{Reason: reason}
}
