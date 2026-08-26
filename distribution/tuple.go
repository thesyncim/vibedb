package distribution

import "encoding/binary"

// TupleVersion identifies the current tuple codec. Column order, spelling,
// type, mapper version, mapper parameters, and tuple version
// together form placement identity.
type TupleVersion uint32

// CurrentTupleVersion identifies the only tuple codec this unreleased build
// accepts. Its byte format is placement identity and must not change without
// regenerating every dependent placement artifact in the same change.
const CurrentTupleVersion TupleVersion = 1

// TupleCodec appends canonical scalar and tuple encodings. Two scalars (or
// tuples) compare equal for placement purposes
// exactly when their appended bytes are equal; no Go equality, source
// spelling, float conversion, or textual concatenation participates.
type TupleCodec interface {
	// AppendScalar appends value's canonical encoding to dst and returns the
	// extended slice. It reports a typed error and returns dst unmodified for
	// a Scalar outside the closed placement scalar set.
	AppendScalar(dst []byte, value Scalar) ([]byte, error)
	// AppendTuple appends each value's canonical encoding to dst in order and
	// returns the extended slice. Every scalar's encoding is self-delimiting,
	// so concatenation is unambiguous: no scalar's bytes can be mistaken for
	// a prefix of another's across a tuple boundary. On error dst holds
	// whatever prefix of values was already appended.
	AppendTuple(dst []byte, values []Scalar) ([]byte, error)
	// Version reports the current codec identifier.
	Version() TupleVersion
}

// CurrentTupleCodec is the only supported placement tuple codec.
var CurrentTupleCodec TupleCodec = tupleCodec{}

type tupleCodec struct{}

func (tupleCodec) AppendScalar(dst []byte, value Scalar) ([]byte, error) {
	return appendScalar(dst, value)
}

func (tupleCodec) AppendTuple(dst []byte, values []Scalar) ([]byte, error) {
	return appendTuple(dst, values)
}

func (tupleCodec) Version() TupleVersion {
	return CurrentTupleVersion
}

// Every scalar encoding is self-delimiting: a leading tag byte selects String
// or Number, and every variable-length field that follows carries an explicit
// uvarint length, so concatenated scalars can never be misread across a
// boundary. Tag 0x00 is reserved and never emitted, so a zeroed buffer can
// never be mistaken for a valid scalar.
const (
	tagString byte = 0x01
	tagNumber byte = 0x02
)

// numberForm follows tagNumber: zero folds every zero spelling (including
// negative zero) into one canonical form, sign otherwise follows.
const (
	numberFormZero     byte = 0x00
	numberFormPositive byte = 0x01
	numberFormNegative byte = 0x02
)

// weightForm precedes a nonzero number's weight magnitude: zero means the
// adjusted decimal weight is exactly 0 and no further weight bytes follow.
const (
	weightFormZero     byte = 0x00
	weightFormPositive byte = 0x01
	weightFormNegative byte = 0x02
)

func appendScalar(dst []byte, value Scalar) ([]byte, error) {
	switch value.kind {
	case KindString:
		return appendStringScalar(dst, value.data), nil
	case KindNumber:
		return appendNumberScalar(dst, value.data)
	default:
		return dst, &UnsupportedScalarError{Kind: value.kind}
	}
}

func appendTuple(dst []byte, values []Scalar) ([]byte, error) {
	for i := range values {
		var err error
		dst, err = appendScalar(dst, values[i])
		if err != nil {
			return dst, err
		}
	}
	return dst, nil
}

// CanonicalTuplePrefixLen validates the first arity scalar frames in raw and
// returns their exact byte length. It does not decode strings or decimal
// spellings and allocates nothing. Callers use it when a storage key appends a
// second tuple after the placement tuple (for example, a non-unique global
// index key followed by its base-row locator).
//
// The result is false for a truncated, overlong-varint, non-canonical, or
// unsupported scalar frame. Bytes after the requested prefix are deliberately
// not inspected.
func CanonicalTuplePrefixLen(raw []byte, arity int) (int, bool) {
	if arity < 1 || arity > KeyspaceWidth {
		return 0, false
	}
	at := 0
	for scalar := 0; scalar < arity; scalar++ {
		next, ok := canonicalScalarEnd(raw, at)
		if !ok {
			return 0, false
		}
		at = next
	}
	return at, true
}

func canonicalScalarEnd(raw []byte, at int) (int, bool) {
	if at < 0 || at >= len(raw) {
		return 0, false
	}
	switch raw[at] {
	case tagString:
		length, next, ok := canonicalTupleUvarint(raw, at+1)
		if !ok || length > uint64(len(raw)-next) {
			return 0, false
		}
		return next + int(length), true
	case tagNumber:
		if at+1 >= len(raw) {
			return 0, false
		}
		form := raw[at+1]
		if form == numberFormZero {
			return at + 2, true
		}
		if form != numberFormPositive && form != numberFormNegative {
			return 0, false
		}
		at += 2
		if at >= len(raw) {
			return 0, false
		}
		weightForm := raw[at]
		at++
		switch weightForm {
		case weightFormZero:
		case weightFormPositive, weightFormNegative:
			length, next, ok := canonicalTupleUvarint(raw, at)
			if !ok || length == 0 || length > uint64(len(raw)-next) {
				return 0, false
			}
			end := next + int(length)
			if raw[next] == '0' || !canonicalTupleDigits(raw[next:end]) {
				return 0, false
			}
			at = end
		default:
			return 0, false
		}
		length, next, ok := canonicalTupleUvarint(raw, at)
		if !ok || length == 0 || length > uint64(len(raw)-next) {
			return 0, false
		}
		end := next + int(length)
		// Significant digits are stripped at both ends by AppendScalar.
		if raw[next] == '0' || raw[end-1] == '0' ||
			!canonicalTupleDigits(raw[next:end]) {
			return 0, false
		}
		return end, true
	default:
		return 0, false
	}
}

func canonicalTupleUvarint(raw []byte, at int) (uint64, int, bool) {
	if at < 0 || at >= len(raw) {
		return 0, 0, false
	}
	value, width := binary.Uvarint(raw[at:])
	if width <= 0 {
		return 0, 0, false
	}
	var canonical [binary.MaxVarintLen64]byte
	if binary.PutUvarint(canonical[:], value) != width {
		return 0, 0, false
	}
	return value, at + width, true
}

func canonicalTupleDigits(raw []byte) bool {
	for _, digit := range raw {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	return true
}

// appendStringScalar appends tag, uvarint(len(s)), and s's raw bytes
// verbatim: no UTF-8 revalidation, escaping, or NUL handling, since the
// explicit length prefix already makes the field self-delimiting.
func appendStringScalar(dst []byte, s string) []byte {
	dst = append(dst, tagString)
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

// appendNumberScalar appends tag, sign/zero form, and — for a nonzero
// value — the canonical adjusted weight followed by uvarint(digit count) and
// the concatenated trimmed significant digits (integer run then fraction
// run). Spelling never participates: every equal-valued spelling reduces to
// identical bytes through parseNumberSpelling's decomposition.
func appendNumberScalar(dst []byte, spelling string) ([]byte, error) {
	var d numberDecimal
	if err := parseNumberSpelling(spelling, &d); err != nil {
		return dst, err
	}
	dst = append(dst, tagNumber)
	if d.zero {
		return append(dst, numberFormZero), nil
	}
	if d.neg {
		dst = append(dst, numberFormNegative)
	} else {
		dst = append(dst, numberFormPositive)
	}
	dst = appendWeightField(dst, &d.weight)
	dst = binary.AppendUvarint(dst, uint64(len(d.intDigits)+len(d.fracDigits)))
	dst = append(dst, d.intDigits...)
	return append(dst, d.fracDigits...), nil
}

func appendWeightField(dst []byte, w *numberWeight) []byte {
	n := weightMagnitudeLen(w)
	if n == 0 {
		return append(dst, weightFormZero)
	}
	if w.neg {
		dst = append(dst, weightFormNegative)
	} else {
		dst = append(dst, weightFormPositive)
	}
	dst = binary.AppendUvarint(dst, uint64(n))
	return appendWeightMagnitude(dst, w)
}
