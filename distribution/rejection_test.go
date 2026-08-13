package distribution

// This package's closed placement scalar set is enforced two ways, and both
// need direct coverage:
//
//  1. Bool, timestamp, Any, object, array, and nested values have no
//     exported constructor at all: Scalar's fields are unexported and
//     NewString/NewNumber are the only two exported functions that produce
//     one, so those kinds simply cannot be named through the public API.
//     TestScalarHasNoExportedFields guards that shape with reflection so an
//     accidental future exported field or constructor is caught here first.
//  2. The one residual way to reach a non-String/Number Scalar is Go's own
//     zero value, var s Scalar. AppendScalar/AppendTuple must reject it with
//     a typed error rather than panicking or silently emitting bytes; that
//     is what TestUnsupportedScalarRejection and friends check.
//
// Malformed Number spellings are rejected before a Scalar even exists, by
// NewNumber; TestInvalidNumberSpellingRejection enumerates the JSON number
// grammar's failure modes.

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

// TestScalarHasNoExportedFields pins the closure mechanism the codec
// depends on: if Scalar ever grows an exported field, some caller outside
// this package could construct a Scalar carrying an arbitrary kind, and the
// "closed placement scalar set" guarantee this package documents would
// silently stop holding.
func TestScalarHasNoExportedFields(t *testing.T) {
	typ := reflect.TypeFor[Scalar]()
	if typ.NumField() == 0 {
		t.Fatal("Scalar has no fields at all; test needs updating, not deleting")
	}
	for f := range typ.Fields() {
		if f.IsExported() {
			t.Fatalf("Scalar.%s is exported; this breaks the closed scalar set (only NewString/NewNumber may construct one)", f.Name)
		}
	}
}

// TestInvalidNumberSpellingRejection enumerates the JSON number grammar
// failure modes NewNumber must reject with a typed *InvalidNumberError,
// including the exact non-numeric literals ("NaN", "Infinity") that make
// Number an *exact* numeric domain rather than a float64 domain.
func TestInvalidNumberSpellingRejection(t *testing.T) {
	bad := []string{
		"",
		"-",
		"+1",
		"01",
		"-01",
		"1.",
		".1",
		"1.e1",
		"1e",
		"1e+",
		"1e-",
		"1ee1",
		"1..1",
		"1.1.1",
		"1e1e1",
		"NaN",
		"nan",
		"Infinity",
		"-Infinity",
		"Inf",
		"1,5",
		"1_000",
		"0x1",
		" 1",
		"1 ",
		"1\n",
		"true",
		"null",
		"\"1\"",
	}
	for _, spelling := range bad {
		t.Run(spelling, func(t *testing.T) {
			s, err := NewNumber(spelling)
			if err == nil {
				t.Fatalf("NewNumber(%q) succeeded, want rejection", spelling)
			}
			var invalid *InvalidNumberError
			if !errors.As(err, &invalid) {
				t.Fatalf("NewNumber(%q) returned %T, want *InvalidNumberError", spelling, err)
			}
			if invalid.Spelling != spelling {
				t.Fatalf("InvalidNumberError.Spelling = %q, want %q", invalid.Spelling, spelling)
			}
			if s.Kind() != KindInvalid {
				t.Fatalf("NewNumber(%q) returned a non-zero Scalar alongside an error", spelling)
			}
		})
	}
}

// TestValidNumberSpellingAcceptance is the positive complement: spellings a
// strict JSON number grammar must accept, so the malformed-input list above
// is not accidentally rejecting well-formed values too.
func TestValidNumberSpellingAcceptance(t *testing.T) {
	good := []string{
		"0", "-0", "1", "-1", "0.0", "-0.0",
		"123", "1.5", "1e10", "1E10", "1e+10", "1e-10",
		"0.001", "100000", "1.0000", "9223372036854775807",
	}
	for _, spelling := range good {
		if _, err := NewNumber(spelling); err != nil {
			t.Fatalf("NewNumber(%q) rejected a well-formed spelling: %v", spelling, err)
		}
	}
}

// TestUnsupportedScalarRejection covers the one residual way to reach a
// Scalar outside {String, Number}: the zero value. Bool, timestamp, Any,
// object, array, and nested values have no constructor to test rejection
// of; the zero value is the only unconstructed-but-reachable case, and it
// must fail closed at the encoding boundary rather than panic or emit bytes.
func TestUnsupportedScalarRejection(t *testing.T) {
	var zero Scalar
	if zero.Kind() != KindInvalid {
		t.Fatalf("zero-value Scalar has Kind() = %v, want KindInvalid", zero.Kind())
	}

	prefix := []byte("existing-prefix")
	dst := append([]byte(nil), prefix...)
	got, err := CurrentTupleCodec.AppendScalar(dst, zero)
	if err == nil {
		t.Fatal("AppendScalar(zero Scalar) succeeded, want *UnsupportedScalarError")
	}
	var unsupported *UnsupportedScalarError
	if !errors.As(err, &unsupported) {
		t.Fatalf("AppendScalar(zero Scalar) returned %T, want *UnsupportedScalarError", err)
	}
	if unsupported.Kind != KindInvalid {
		t.Fatalf("UnsupportedScalarError.Kind = %v, want KindInvalid", unsupported.Kind)
	}
	if !bytes.Equal(got, dst) {
		t.Fatalf("AppendScalar(zero Scalar) modified dst: got %x, want unchanged %x", got, dst)
	}

	if CurrentTupleCodec.Version() != CurrentTupleVersion {
		t.Fatalf("CurrentTupleCodec.Version() = %d, want %d", CurrentTupleCodec.Version(), CurrentTupleVersion)
	}
}

// TestUnsupportedScalarInTupleStopsAtFirstFailure pins the documented
// AppendTuple partial-progress contract: on error, dst holds whatever
// prefix of values was already appended, and no bytes for the failing or
// later elements appear.
func TestUnsupportedScalarInTupleStopsAtFirstFailure(t *testing.T) {
	ok1 := NewString("before")
	var bad Scalar // zero value: KindInvalid
	ok2 := NewString("after")

	wantPrefix, err := CurrentTupleCodec.AppendScalar(nil, ok1)
	if err != nil {
		t.Fatalf("AppendScalar(ok1): %v", err)
	}

	got, err := CurrentTupleCodec.AppendTuple(nil, []Scalar{ok1, bad, ok2})
	if err == nil {
		t.Fatal("AppendTuple with an unsupported element succeeded, want error")
	}
	var unsupported *UnsupportedScalarError
	if !errors.As(err, &unsupported) {
		t.Fatalf("AppendTuple error is %T, want *UnsupportedScalarError", err)
	}
	if !bytes.Equal(got, wantPrefix) {
		t.Fatalf("AppendTuple on failure returned %x, want exactly the prefix already appended %x", got, wantPrefix)
	}
}

// TestNewStringAcceptsAnyBytes documents (and locks in) that String has no
// rejection path at all: unlike Number, every byte sequence — including
// invalid UTF-8 and embedded NULs — is a valid String scalar.
func TestNewStringAcceptsAnyBytes(t *testing.T) {
	for _, raw := range []string{"", "\x00", "\xff\xfe", "\x80", string([]byte{0xc3})} {
		s := NewString(raw)
		if s.Kind() != KindString {
			t.Fatalf("NewString(%q).Kind() = %v, want KindString", raw, s.Kind())
		}
		got, ok := s.StringValue()
		if !ok || got != raw {
			t.Fatalf("NewString(%q).StringValue() = %q, %v", raw, got, ok)
		}
		if _, err := CurrentTupleCodec.AppendScalar(nil, s); err != nil {
			t.Fatalf("AppendScalar(String(%q)): %v", raw, err)
		}
	}
}
