package query

import (
	"strconv"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
)

// The scalar value model and the total order the executor compares, groups,
// and sorts by. Every WHERE comparison, GROUP BY key, and ORDER BY key funnels
// through one classification (classifyRaw / classifyLiteral) and one total
// order (compareScalar), so the three operations can never disagree about
// which values are equal — a property the exhaustive differential leans on
// when it checks that GROUP BY's partition matches the reference's.
//
// Numbers are compared by exact decimal value, never by rounding through
// float64: 1, 1.0, and 1e0 are one value, and two integers that differ past
// float64's 53-bit mantissa (9007199254740992 and 9007199254740993) are two.
// This mirrors the exact-decimal contract the core's containment kernel
// (number_equal.go) applies to `@>`; that kernel is package-private, so the
// query package reproduces the minimum decomposition it needs to *order* as
// well as compare, and the differential cross-checks it against math/big.

// scalarKind is a value's place in the query's total order over JSON values.
// The constant order is the cross-type order used when a comparison, group, or
// sort mixes kinds: null sorts first, then bools, numbers, strings, and
// finally containers.
type scalarKind uint8

const (
	// kindNull is JSON null or an absent path. The two are one value: "the
	// value at an absent path is null" is the query's defining rule.
	kindNull scalarKind = iota
	kindBool
	kindNumber
	kindString
	// kindContainer is a JSON array or object. Containers are not compared by
	// structure here; they sort and group by exact source bytes, a defined —
	// if not semantic — total order sufficient for a basic surface.
	kindContainer
)

// A scalar is one classified JSON value: a WHERE literal, or the value a path
// resolves to in one document. It borrows the bytes it was built from (the
// document arena for a cell, the compiled literal for a literal) and stays
// valid for the extraction pass that produced it.
type scalar struct {
	kind scalarKind
	bval bool // kindBool

	// num holds a number's exact source spelling; isInt/ival cache the plain
	// integer fast path so the common comparison never decomposes digits.
	num   []byte
	isInt bool
	ival  int64

	sval string // kindString: decoded content (escapes resolved)
	raw  []byte // original JSON bytes, for projection passthrough and containers
}

// classifyRaw classifies one extracted cell. An invalid RawValue (the absence
// convention of AppendField / AppendPointer) and an explicit JSON null both
// classify as kindNull, so absent and null paths are indistinguishable to
// every downstream operation, as documented.
func classifyRaw(r vibejson.RawValue) scalar {
	var scratch []byte
	return classifyRawInto(r, &scratch)
}

// classifyRawInto is classifyRaw with caller-owned decoded-string storage.
// The caller must reserve enough capacity before the first call if previously
// returned escaped-string views must survive later calls.
func classifyRawInto(r vibejson.RawValue, scratch *[]byte) scalar {
	b := r.Bytes()
	if len(b) == 0 {
		return scalar{kind: kindNull}
	}
	switch r.Kind() {
	case document.Null:
		return scalar{kind: kindNull, raw: b}
	case document.Bool:
		v, _ := r.Bool()
		return scalar{kind: kindBool, bval: v, raw: b}
	case document.Number:
		s := scalar{kind: kindNumber, num: b, raw: b}
		if i, ok := r.Int64(); ok {
			s.isInt, s.ival = true, i
		}
		return s
	case document.String:
		if content, clean := r.StringBytes(); clean {
			return scalar{kind: kindString, sval: byteview.String(content), raw: b}
		}
		mark := len(*scratch)
		decoded, ok, err := r.AppendText(*scratch)
		if ok && err == nil {
			*scratch = decoded
			return scalar{kind: kindString, sval: byteview.String(decoded[mark:]), raw: b}
		}
		// Stored documents are validated, so this is defensive only. Preserve
		// a total classification without retaining a transient error.
		return scalar{kind: kindString, sval: byteview.String(b), raw: b}
	default:
		return scalar{kind: kindContainer, raw: b}
	}
}

// classifyLiteral builds the scalar for a typed WHERE literal. A numeric
// literal carries a canonical spelling so it compares exactly against document
// numbers of any spelling.
func classifyLiteral(l literal) scalar {
	switch l.kind {
	case kindBool:
		return scalar{kind: kindBool, bval: l.bval}
	case kindNumber:
		s := scalar{kind: kindNumber, num: l.num, raw: l.num}
		s.isInt, s.ival = l.isInt, l.ival
		return s
	case kindString:
		return scalar{kind: kindString, sval: l.sval}
	default:
		return scalar{kind: kindNull}
	}
}

// compareScalar returns the sign of a - b under the query's total order over
// JSON values: null < bool < number < string < container across kinds, and
// within a kind by value — bools false < true, numbers by exact decimal value,
// strings by decoded byte content, containers by exact source bytes. It is a
// genuine total order (reflexive, antisymmetric, transitive), which is what
// lets GROUP BY key by "compareScalar == 0" and ORDER BY sort stably.
func compareScalar(a, b scalar) int {
	if a.kind != b.kind {
		if a.kind < b.kind {
			return -1
		}
		return 1
	}
	switch a.kind {
	case kindNull:
		return 0
	case kindBool:
		switch {
		case a.bval == b.bval:
			return 0
		case !a.bval:
			return -1
		default:
			return 1
		}
	case kindNumber:
		if a.isInt && b.isInt {
			switch {
			case a.ival < b.ival:
				return -1
			case a.ival > b.ival:
				return 1
			default:
				return 0
			}
		}
		return compareNumberBytes(a.num, b.num)
	case kindString:
		switch {
		case a.sval < b.sval:
			return -1
		case a.sval > b.sval:
			return 1
		default:
			return 0
		}
	default:
		return bytesCompare(a.raw, b.raw)
	}
}

// bytesCompare is bytes.Compare, inlined to avoid importing bytes for one use.
func bytesCompare(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	default:
		return 0
	}
}

// float64OfNumber returns a number cell's float64 value for aggregation, and
// false for a non-number scalar or a magnitude float64 cannot represent.
func (s scalar) float64OfNumber() (float64, bool) {
	if s.kind != kindNumber {
		return 0, false
	}
	if s.isInt {
		return float64(s.ival), true
	}
	f, err := strconv.ParseFloat(string(s.num), 64)
	if err != nil {
		return 0, false
	}
	return f, true
}
