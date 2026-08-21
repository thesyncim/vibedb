// Package distribution implements cross-shard placement identity: a closed
// String/exact-Number scalar set, canonical tuple framing, tenant-independent
// virtual buckets, immutable range manifests, and fenced routing targets.
package distribution

// ScalarKind identifies which member of the closed placement scalar set a
// Scalar holds.
type ScalarKind uint8

const (
	// KindInvalid is the zero value of ScalarKind; no exported constructor
	// ever returns a Scalar carrying it, but a zero-value Scalar{} does.
	KindInvalid ScalarKind = iota
	// KindString identifies a binary UTF-8 string scalar.
	KindString
	// KindNumber identifies an exact, canonically decomposed number scalar.
	KindNumber
)

// String renders the kind name for diagnostics.
func (k ScalarKind) String() string {
	switch k {
	case KindString:
		return "String"
	case KindNumber:
		return "Number"
	default:
		return "Invalid"
	}
}

// Scalar is a closed placement value: exactly a String or an exact Number.
// There is no exported way to construct one carrying Bool, timestamp, Any,
// object, array, or nested values; the zero value is KindInvalid and is
// rejected wherever a Scalar is encoded.
type Scalar struct {
	kind ScalarKind
	data string
}

// NewString returns a String scalar over s's raw bytes. Any byte sequence is
// accepted; no UTF-8 well-formedness validation is performed, and the bytes
// participate in placement identity exactly as given, with no collation.
func NewString(s string) Scalar {
	return Scalar{kind: KindString, data: s}
}

// NewNumber returns an exact Number scalar for spelling, a JSON number
// literal (optional '-', integer part, optional fraction, optional
// exponent). It reports a typed *InvalidNumberError for any spelling that is
// not a well-formed JSON number. Equal values under exact decimal equality —
// including spelling variants such as "5", "5.0", "5e0", "50e-1", and the
// negative-zero/zero pair — canonicalize identically at encoding time.
func NewNumber(spelling string) (Scalar, error) {
	var d numberDecimal
	if err := parseNumberSpelling(spelling, &d); err != nil {
		return Scalar{}, err
	}
	return Scalar{kind: KindNumber, data: spelling}, nil
}

// Kind reports which closed placement scalar variant the value holds.
func (s Scalar) Kind() ScalarKind {
	return s.kind
}

// StringValue returns s's raw bytes and reports whether s is a String
// scalar.
func (s Scalar) StringValue() (string, bool) {
	if s.kind != KindString {
		return "", false
	}
	return s.data, true
}

// NumberSpelling returns s's original validated JSON number spelling and
// reports whether s is a Number scalar.
func (s Scalar) NumberSpelling() (string, bool) {
	if s.kind != KindNumber {
		return "", false
	}
	return s.data, true
}
