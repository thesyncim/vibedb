package distribution

import "encoding/binary"

// TupleVersion identifies a frozen tuple codec revision. Column order,
// spelling, type, mapper version, mapper parameters, and tuple version
// together form placement identity.
type TupleVersion uint32

// TupleVersion1 is the first frozen tuple codec revision. Its byte format is
// an immutable compatibility artifact: it must never change in place.
const TupleVersion1 TupleVersion = 1

// TupleCodec appends canonical scalar and tuple encodings for one frozen
// version. Two scalars (or tuples) compare equal for placement purposes
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
	// Version reports the frozen revision this codec implements.
	Version() TupleVersion
}

// V1 is the frozen tuple codec version 1.
var V1 TupleCodec = tupleCodecV1{}

type tupleCodecV1 struct{}

func (tupleCodecV1) AppendScalar(dst []byte, value Scalar) ([]byte, error) {
	return appendScalarV1(dst, value)
}

func (tupleCodecV1) AppendTuple(dst []byte, values []Scalar) ([]byte, error) {
	return appendTupleV1(dst, values)
}

func (tupleCodecV1) Version() TupleVersion {
	return TupleVersion1
}

// Frozen version 1 wire tags and forms. Every scalar encoding is
// self-delimiting: a leading tag byte selects String or Number, and every
// variable-length field that follows carries an explicit uvarint length, so
// concatenated scalars can never be misread across a boundary. Tag 0x00 is
// reserved and never emitted, so a zeroed buffer can never be mistaken for a
// valid version 1 scalar.
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

func appendScalarV1(dst []byte, value Scalar) ([]byte, error) {
	switch value.kind {
	case KindString:
		return appendStringScalarV1(dst, value.data), nil
	case KindNumber:
		return appendNumberScalarV1(dst, value.data)
	default:
		return dst, &UnsupportedScalarError{Kind: value.kind}
	}
}

func appendTupleV1(dst []byte, values []Scalar) ([]byte, error) {
	for i := range values {
		var err error
		dst, err = appendScalarV1(dst, values[i])
		if err != nil {
			return dst, err
		}
	}
	return dst, nil
}

// appendStringScalarV1 appends tag, uvarint(len(s)), and s's raw bytes
// verbatim: no UTF-8 revalidation, escaping, or NUL handling, since the
// explicit length prefix already makes the field self-delimiting.
func appendStringScalarV1(dst []byte, s string) []byte {
	dst = append(dst, tagString)
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

// appendNumberScalarV1 appends tag, sign/zero form, and — for a nonzero
// value — the canonical adjusted weight followed by uvarint(digit count) and
// the concatenated trimmed significant digits (integer run then fraction
// run). Spelling never participates: every equal-valued spelling reduces to
// identical bytes through parseNumberSpelling's decomposition.
func appendNumberScalarV1(dst []byte, spelling string) ([]byte, error) {
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
	dst = appendWeightFieldV1(dst, &d.weight)
	dst = binary.AppendUvarint(dst, uint64(len(d.intDigits)+len(d.fracDigits)))
	dst = append(dst, d.intDigits...)
	return append(dst, d.fracDigits...), nil
}

func appendWeightFieldV1(dst []byte, w *numberWeight) []byte {
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
