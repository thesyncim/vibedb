package storeio

import (
	"bytes"
	"math"
	"math/rand/v2"
	"strconv"
	"testing"
)

func TestExactDecimalInt64Value(t *testing.T) {
	tests := []struct {
		spelling string
		want     int64
		ok       bool
	}{
		{"0", 0, true}, {"-0.0e999999999999999999999", 0, true},
		{"1", 1, true}, {"1.0", 1, true}, {"10e-1", 1, true},
		{"0.001e3", 1, true}, {"120e-1", 12, true},
		{"1.2", 0, false}, {"1e-1", 0, false},
		{"9223372036854775807", 9223372036854775807, true},
		{"9223372036854775808", 0, false},
		{"-9223372036854775808", -9223372036854775808, true},
		{"-9223372036854775809", 0, false},
		{"1e999999999999999999999", 0, false},
	}
	for _, test := range tests {
		got, ok := exactDecimalInt64Value([]byte(test.spelling))
		if got != test.want || ok != test.ok {
			t.Errorf("exactDecimalInt64Value(%s) = (%d,%v), want (%d,%v)",
				test.spelling, got, ok, test.want, test.ok)
		}
	}
}

// checkCanonicalIntRoundTrip pins the canonical-integer contract for one
// spelling: admitted implies byte-identical regeneration through the zigzag
// varint payload; rejected implies the spelling stays a stored literal: the
// predicate refuses it, so the token encoder copies it verbatim and the
// round-trip is the identity by construction.
func checkCanonicalIntRoundTrip(t *testing.T, spelling []byte, wantAdmit bool) {
	t.Helper()
	v, ok := CanonicalIntValue(spelling)
	if ok != wantAdmit {
		t.Fatalf("CanonicalIntValue(%q) admitted=%v, want %v", spelling, ok, wantAdmit)
	}
	if !ok {
		return
	}
	payload := AppendZigzagVarint(nil, v)
	decoded, n := DecodeZigzagVarint(payload)
	if n != len(payload) {
		t.Fatalf("DecodeZigzagVarint(%q payload % x) consumed %d of %d", spelling, payload, n, len(payload))
	}
	if decoded != v {
		t.Fatalf("zigzag round trip of %q: %d != %d", spelling, decoded, v)
	}
	regen := AppendCanonicalInt(nil, decoded)
	if !bytes.Equal(regen, spelling) {
		t.Fatalf("regeneration of %q produced %q", spelling, regen)
	}
}

func TestTokenCanonicalIntHandcrafted(t *testing.T) {
	admitted := []string{
		"0", "1", "-1", "9", "10", "-10", "42", "100000",
		"999999999999999999", "-999999999999999999", // the 18-digit cap
		"123456789012345678", "-123456789012345678",
	}
	for _, s := range admitted {
		checkCanonicalIntRoundTrip(t, []byte(s), true)
	}
	rejected := []string{
		// -0 and leading zeros: not the minimal spelling of any int64.
		"-0", "00", "01", "007", "-01", "0123456789",
		// 19 digits and beyond: outside the one-directional int64 proof.
		"1000000000000000000", "-1000000000000000000",
		"9223372036854775807", "-9223372036854775808",
		"99999999999999999999",
		// Plus sign, exponent, and fraction forms (valid JSON numbers or
		// not, the token admits none of them).
		"+5", "1e3", "1E3", "1e+3", "5e-1", "1.0", "0.5", "-0.0", "1.",
		// Junk that must fail closed.
		"", "-", " 5", "5 ", "0x10", "٥", "1_000", "--1", "1-",
	}
	for _, s := range rejected {
		checkCanonicalIntRoundTrip(t, []byte(s), false)
	}
}

// TestTokenCanonicalIntGenerative sweeps the admission predicate
// requires: random in-range values must admit and regenerate identically;
// systematic near-miss mutations of each admitted spelling must reject.
func TestTokenCanonicalIntGenerative(t *testing.T) {
	rng := rand.New(rand.NewPCG(0x7040, 0xE45))
	const maxAdmitted = int64(999999999999999999)
	for i := 0; i < 200000; i++ {
		// Bias across magnitudes so short spellings are as covered as long
		// ones: pick a digit count, then a value within it.
		digits := 1 + rng.IntN(18)
		lo := int64(1)
		for d := 1; d < digits; d++ {
			lo *= 10
		}
		hi := lo*10 - 1
		if hi > maxAdmitted || hi < 0 {
			hi = maxAdmitted
		}
		v := lo + rng.Int64N(hi-lo+1)
		if digits == 1 {
			v = rng.Int64N(10) // include 0
		}
		if rng.IntN(2) == 0 && v != 0 {
			v = -v
		}
		spelling := strconv.AppendInt(nil, v, 10)
		checkCanonicalIntRoundTrip(t, spelling, true)

		// Near-miss mutations. Every mutation below breaks canonicality by
		// construction: a leading zero, a plus, minus-zero, a fraction or
		// exponent suffix, or a 19th digit.
		neg := spelling[0] == '-'
		body := spelling
		if neg {
			body = spelling[1:]
		}
		withPrefix := func(p string) []byte {
			m := []byte{}
			if neg {
				m = append(m, '-')
			}
			m = append(m, p...)
			return append(m, body...)
		}
		checkCanonicalIntRoundTrip(t, withPrefix("0"), false)
		checkCanonicalIntRoundTrip(t, append([]byte("+"), spelling...), false)
		checkCanonicalIntRoundTrip(t, append(append([]byte{}, spelling...), ".0"...), false)
		checkCanonicalIntRoundTrip(t, append(append([]byte{}, spelling...), 'e', '1'), false)
		if len(body) == 18 {
			checkCanonicalIntRoundTrip(t, append(append([]byte{}, spelling...), '1'), false)
		}
	}
	checkCanonicalIntRoundTrip(t, []byte("-0"), false)
}

// TestTokenZigzagVarintEdges pins the payload codec at the integer extremes
// and its fail-closed behavior on malformed input: a token stream misframes
// everything after a wrongly-sized varint, so truncation and overflow must
// return n == 0, never a guess.
func TestTokenZigzagVarintEdges(t *testing.T) {
	values := []int64{
		0, 1, -1, 63, 64, -64, -65, math.MaxInt64, math.MinInt64,
		999999999999999999, -999999999999999999,
	}
	for _, v := range values {
		payload := AppendZigzagVarint(nil, v)
		if len(payload) > 10 {
			t.Fatalf("varint of %d is %d bytes", v, len(payload))
		}
		decoded, n := DecodeZigzagVarint(payload)
		if n != len(payload) || decoded != v {
			t.Fatalf("round trip of %d: got %d (n=%d, len=%d)", v, decoded, n, len(payload))
		}
		// Truncations of a multi-byte payload must fail closed.
		for cut := 0; cut < len(payload)-1; cut++ {
			if payload[cut] < 0x80 {
				continue // a shorter valid varint happens to be a prefix
			}
			if _, n := DecodeZigzagVarint(payload[:cut+1]); n != 0 {
				t.Fatalf("truncated varint of %d (cut %d) decoded with n=%d", v, cut, n)
			}
		}
	}
	if _, n := DecodeZigzagVarint(nil); n != 0 {
		t.Fatal("empty input decoded")
	}
	// Eleven continuation bytes: past any uint64.
	over := bytes.Repeat([]byte{0xff}, 11)
	if _, n := DecodeZigzagVarint(over); n != 0 {
		t.Fatal("11-byte varint decoded")
	}
	// Ten bytes whose final byte carries more than the one remaining bit.
	over = append(bytes.Repeat([]byte{0x80}, 9), 0x02)
	if _, n := DecodeZigzagVarint(over); n != 0 {
		t.Fatal("overflowing tenth byte decoded")
	}
}
