package distribution

// Deterministic repeated encoding is the property every other guarantee in
// this package rests on: two callers computing the same logical value's
// placement bytes, on different processes, different Go versions of the
// same frozen tuple version, or the same process a million calls apart,
// must land on the identical byte string every time. These tests call each
// encoder many times — across fresh, reused, and non-empty starting
// buffers — and require bit-for-bit agreement on every call, not just the
// first two.

import (
	"bytes"
	"strings"
	"testing"
)

func TestDeterministicRepeatedNumberEncoding(t *testing.T) {
	spellings := []string{
		"0", "-0", "0.0", "-0e999",
		"5", "5.0", "5e0", "50e-1",
		"-42", "-42.0", "-4.2e1",
		"9223372036854775807", "9223372036854775808",
		"-9223372036854775808",
		"18446744073709551615",
		"0.1e9223372036854775808",
		"1e" + strings.Repeat("9", 120),
		"-1.23456789e-300",
	}
	for _, spelling := range spellings {
		t.Run(spelling, func(t *testing.T) {
			s, err := NewNumber(spelling)
			if err != nil {
				t.Fatalf("NewNumber(%q): %v", spelling, err)
			}
			var want []byte
			for i := range 500 {
				// Alternate the starting buffer shape: nil, empty-with-
				// capacity, and a nonempty prefix that must be preserved
				// and not read back into the result.
				var dst []byte
				switch i % 3 {
				case 0:
					dst = nil
				case 1:
					dst = make([]byte, 0, 256)
				case 2:
					dst = append([]byte(nil), "prefix-noise"...)
				}
				prefixLen := len(dst)
				got, err := V1.AppendScalar(dst, s)
				if err != nil {
					t.Fatalf("iteration %d: AppendScalar: %v", i, err)
				}
				if !bytes.Equal(dst[:prefixLen], got[:prefixLen]) {
					t.Fatalf("iteration %d: AppendScalar corrupted the caller's existing prefix", i)
				}
				encoded := append([]byte(nil), got[prefixLen:]...)
				if want == nil {
					want = encoded
				} else if !bytes.Equal(encoded, want) {
					t.Fatalf("iteration %d: %q encoded to %x, first call gave %x", i, spelling, encoded, want)
				}
			}
		})
	}
}

func TestDeterministicRepeatedStringEncoding(t *testing.T) {
	raws := []string{
		"", "a", "hello world", "\x00\x00\x00",
		"héllo", "日本語", "😀", "\x80\xff",
		strings.Repeat("x", 4096),
	}
	for _, raw := range raws {
		t.Run(raw, func(t *testing.T) {
			s := NewString(raw)
			var want []byte
			for i := range 500 {
				got, err := V1.AppendScalar(nil, s)
				if err != nil {
					t.Fatalf("iteration %d: AppendScalar: %v", i, err)
				}
				if want == nil {
					want = append([]byte(nil), got...)
				} else if !bytes.Equal(got, want) {
					t.Fatalf("iteration %d: %q encoded to %x, first call gave %x", i, raw, got, want)
				}
			}
		})
	}
}

func TestDeterministicRepeatedTupleEncoding(t *testing.T) {
	five, err := NewNumber("5.0")
	if err != nil {
		t.Fatal(err)
	}
	values := []Scalar{NewString("tenant"), five, NewString("")}
	var want []byte
	for i := range 500 {
		got, err := V1.AppendTuple(nil, values)
		if err != nil {
			t.Fatalf("iteration %d: AppendTuple: %v", i, err)
		}
		if want == nil {
			want = append([]byte(nil), got...)
		} else if !bytes.Equal(got, want) {
			t.Fatalf("iteration %d: tuple encoded to %x, first call gave %x", i, got, want)
		}
	}
}
