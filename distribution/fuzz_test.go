package distribution

// FuzzScalarEncoding drives the current codec with arbitrary strings used
// two ways: as raw String scalar bytes (which NewString always accepts) and
// as a candidate Number spelling (which NewNumber may reject). There is no
// decoder to round-trip through — the design deliberately ships no Decode
// API, so the invariants checked here are the ones that
// actually hold without one:
//
//   - neither constructor nor AppendScalar/AppendTuple ever panics;
//   - a rejected Number spelling always carries a typed *InvalidNumberError;
//   - encoding the same Scalar twice gives bit-for-bit identical bytes;
//   - a String and a Number never share a leading tag byte;
//   - AppendTuple of two scalars is exactly the concatenation of their
//     individual AppendScalar outputs (the self-delimiting-framing
//     guarantee the whole tuple format depends on), for arbitrary fuzzed
//     inputs, not just the curated golden vectors.

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func FuzzScalarEncoding(f *testing.F) {
	type seed struct{ raw, num string }
	seeds := []seed{
		{"", "0"},
		{"a", "-0"},
		{"a\x00b", "5e0"},
		{"\x00\x00", "50e-1"},
		{"héllo", "-42.0"},
		{"日本語", "9223372036854775807"},
		{"😀", "9223372036854775808"},
		{"\x80\xff", "-9223372036854775808"},
		{"\xc3\xa9", "18446744073709551615"},
		{string([]byte{0xc3}), "0.1e9223372036854775808"},
		{"quote\"inside", "1e" + strings.Repeat("9", 100)},
		{`back\slash`, "-1.5e-300"},
		{"tab\ttab", "01"},    // NewNumber must reject this
		{"line\nbreak", "1."}, // NewNumber must reject this
		{"", ""},              // NewNumber must reject this
		{"x", "NaN"},          // NewNumber must reject this
		{strings.Repeat("y", 512), "Infinity"},
	}
	for _, s := range seeds {
		f.Add(s.raw, s.num)
	}

	f.Fuzz(func(t *testing.T, raw, numCandidate string) {
		// String: infallible construction, deterministic, self-consistent
		// tag, and never panics regardless of byte content (valid UTF-8,
		// invalid UTF-8, or embedded NULs all pass through verbatim).
		str := NewString(raw)
		if str.Kind() != KindString {
			t.Fatalf("NewString(%q).Kind() = %v, want KindString", raw, str.Kind())
		}
		strBytes1, err := CurrentTupleCodec.AppendScalar(nil, str)
		if err != nil {
			t.Fatalf("AppendScalar(String(%q)) returned an error: %v", raw, err)
		}
		strBytes2, err := CurrentTupleCodec.AppendScalar(nil, str)
		if err != nil || !bytes.Equal(strBytes1, strBytes2) {
			t.Fatalf("AppendScalar(String(%q)) is nondeterministic: %x vs %x (err=%v)", raw, strBytes1, strBytes2, err)
		}
		if len(strBytes1) == 0 || strBytes1[0] != tagString {
			t.Fatalf("String(%q) encoding %x does not start with tagString", raw, strBytes1)
		}

		// Number: may be rejected, but only with the typed error, and any
		// spelling that is accepted must encode deterministically and carry
		// tagNumber.
		num, err := NewNumber(numCandidate)
		if err != nil {
			var invalid *InvalidNumberError
			if !errors.As(err, &invalid) {
				t.Fatalf("NewNumber(%q) returned %T, want *InvalidNumberError", numCandidate, err)
			}
			return
		}
		numBytes1, err := CurrentTupleCodec.AppendScalar(nil, num)
		if err != nil {
			t.Fatalf("AppendScalar(Number(%q)) returned an error for a validated number: %v", numCandidate, err)
		}
		numBytes2, err := CurrentTupleCodec.AppendScalar(nil, num)
		if err != nil || !bytes.Equal(numBytes1, numBytes2) {
			t.Fatalf("AppendScalar(Number(%q)) is nondeterministic: %x vs %x (err=%v)", numCandidate, numBytes1, numBytes2, err)
		}
		if len(numBytes1) == 0 || numBytes1[0] != tagNumber {
			t.Fatalf("Number(%q) encoding %x does not start with tagNumber", numCandidate, numBytes1)
		}

		// Self-delimiting framing: a 2-scalar tuple must be exactly the
		// concatenation of each scalar's own encoding, in both orders.
		tupleAB, err := CurrentTupleCodec.AppendTuple(nil, []Scalar{str, num})
		if err != nil {
			t.Fatalf("AppendTuple([String, Number]): %v", err)
		}
		wantAB := append(append([]byte(nil), strBytes1...), numBytes1...)
		if !bytes.Equal(tupleAB, wantAB) {
			t.Fatalf("AppendTuple([String(%q), Number(%q)]) = %x, want concatenation %x", raw, numCandidate, tupleAB, wantAB)
		}

		tupleBA, err := CurrentTupleCodec.AppendTuple(nil, []Scalar{num, str})
		if err != nil {
			t.Fatalf("AppendTuple([Number, String]): %v", err)
		}
		wantBA := append(append([]byte(nil), numBytes1...), strBytes1...)
		if !bytes.Equal(tupleBA, wantBA) {
			t.Fatalf("AppendTuple([Number(%q), String(%q)]) = %x, want concatenation %x", numCandidate, raw, tupleBA, wantBA)
		}
	})
}
