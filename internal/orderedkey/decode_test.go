package orderedkey

import (
	"bytes"
	"math/big"
	"testing"
)

// decodeOne decodes the single component a test just encoded.
func decodeOne(t *testing.T, key []byte) (Component, []byte, int) {
	t.Helper()
	c, out, next, err := DecodeComponent(nil, key, 0)
	if err != nil {
		t.Fatalf("decode %x: %v", key, err)
	}
	return c, out, next
}

// Round-trip is by value, not by spelling: the encoder canonicalizes numbers,
// so "1.0" decodes to "1". The invariant that matters is that the decoded text
// names the same number and re-encodes to the identical key, which is what lets
// a caller return a key column without keeping a second copy of the source.
func TestRoundTripPreservesValue(t *testing.T) {
	values := corpus(t)
	for _, d := range []Direction{Ascending, Descending} {
		for _, v := range values {
			key := encodeValue(t, nil, v, d)
			c, out, next := decodeOne(t, key)
			if next != len(key) {
				t.Fatalf("%s: next=%d len=%d", v, next, len(key))
			}
			if c.Kind != v.kind {
				t.Fatalf("%s: kind %v want %v", v, c.Kind, v.kind)
			}
			if (c.Descending) != (d == Descending) {
				t.Fatalf("%s: descending %v want %v", v, c.Descending, d == Descending)
			}
			payload := out[c.PayloadStart:c.PayloadEnd]
			switch v.kind {
			case KindBool:
				if c.Bool != v.b {
					t.Fatalf("%s: bool %v", v, c.Bool)
				}
			case KindNumber:
				got, ok := new(big.Rat).SetString(string(payload))
				if !ok || got.Cmp(v.rat) != 0 {
					t.Fatalf("%s: decoded %q is a different value", v, payload)
				}
				again, ok := AppendNumber(nil, payload, d)
				if !ok || !bytes.Equal(again, key) {
					t.Fatalf("%s: re-encoding %q gave\n%x\nwant\n%x", v, payload, again, key)
				}
			case KindString:
				if string(payload) != v.str {
					t.Fatalf("%s: decoded %q", v, payload)
				}
			default:
				if len(payload) != 0 {
					t.Fatalf("%s: unexpected payload %q", v, payload)
				}
			}
		}
	}
}

// Negative zero is not a distinct key. JSON's -0 and 0 are the same number under
// exact-decimal comparison, so they must land on one key or an equality lookup
// for 0 would miss rows stored as -0. NaN and infinities have no JSON number
// spelling at all, so they are outside this encoder's domain and are rejected
// rather than given an arbitrary position in the order.
func TestNegativeZeroAndNonFiniteDomain(t *testing.T) {
	zero, ok := AppendNumber(nil, []byte("0"), Ascending)
	if !ok {
		t.Fatal("0")
	}
	for _, text := range []string{"-0", "0.0", "-0.000", "0e10", "-0e-10"} {
		got, ok := AppendNumber(nil, []byte(text), Ascending)
		if !ok || !bytes.Equal(got, zero) {
			t.Fatalf("%q did not encode as zero: %x", text, got)
		}
		c, out, _ := decodeOne(t, got)
		if string(out[c.PayloadStart:c.PayloadEnd]) != "0" {
			t.Fatalf("%q decoded to %q", text, out)
		}
	}
	for _, text := range []string{"NaN", "nan", "Infinity", "-Infinity", "Inf", "+1"} {
		if _, ok := AppendNumber(nil, []byte(text), Ascending); ok {
			t.Fatalf("%q was accepted", text)
		}
	}
}

func TestDecodeRejectsMalformedKeys(t *testing.T) {
	valid, _ := AppendString(nil, []byte("abc"), Ascending)
	number, _ := AppendNumber(nil, []byte("-12.5"), Ascending)
	cases := [][]byte{
		nil,
		{},
		{0x00},                                  // no such tag
		{0xff},                                  // no such tag
		{tagString},                             // unterminated
		{tagString, 'a'},                        // unterminated
		{tagString, 0x00},                       // truncated terminator
		{tagString, 0x00, 0x01},                 // 0x00 followed by neither terminator nor escape
		{tagNumber},                             // no sign byte
		{tagNumber, 0x99},                       // bad sign byte
		{tagNumber, tagNumberPositive, 1, 2, 3}, // truncated exponent
		{tagNumber, tagNumberPositive, 0, 0, 0, 0, 0, 0, 0, 0}, // no digit terminator
		valid[:len(valid)-1],
		number[:len(number)-1],
	}
	for _, key := range cases {
		if _, _, _, err := DecodeComponent(nil, key, 0); err == nil {
			t.Fatalf("accepted malformed key %x", key)
		}
	}
}

// Regression: decode once accepted string content that is not valid UTF-8,
// which AppendString can never produce. Such a key decoded to a payload that
// failed to re-encode, breaking the round-trip identity the storage layer
// relies on. Found by FuzzDecodeKey.
func TestDecodeRejectsInvalidUTF8StringContent(t *testing.T) {
	for _, content := range [][]byte{
		{0xfa}, {0xff}, {0x80}, {'a', 0xc3}, {0xc3, 0x28}, {0xed, 0xa0, 0x80},
	} {
		key := []byte{tagString}
		for _, b := range content {
			key = appendStringByte(key, b)
		}
		key = append(key, 0, 0)
		if _, _, _, err := DecodeComponent(nil, key, 0); err == nil {
			t.Fatalf("accepted invalid UTF-8 content %x", content)
		}
	}
}

// Regression: at the extreme of the exponent range the encoder and decoder
// disagreed. "0.9e-9223372036854775807" encoded fine but decoded to text naming
// exponent MinInt64, which has no representable magnitude and so could not be
// re-encoded; "0.09e-9223372036854775807" encoded fine but could not be decoded
// at all. The canonical spelling now writes the adjusted exponent directly
// rather than adjusted-1, and the encoder refuses the one exponent that has no
// spelling, so the encodable and decodable sets coincide. Found by FuzzDecodeKey.
func TestExtremeExponentRoundTripsOrIsRefused(t *testing.T) {
	for _, text := range []string{
		"0.9e-9223372036854775807", "0.09e-9223372036854775807",
		"9e9223372036854775806", "0.9e9223372036854775807",
		"1e-9223372036854775807", "1e9223372036854775807",
	} {
		key, ok := AppendNumber(nil, []byte(text), Ascending)
		if !ok {
			continue // refused up front, which is a valid outcome
		}
		c, out, _, err := DecodeComponent(nil, key, 0)
		if err != nil {
			t.Fatalf("%s: encoded but did not decode: %v", text, err)
		}
		payload := out[c.PayloadStart:c.PayloadEnd]
		// Value equality is asserted by re-encoding to identical bytes rather
		// than through math/big: exponents of this magnitude would require
		// materializing 10^(9e18), which big.Rat cannot represent at all. Since
		// equal values encode to equal keys, byte equality is the stronger
		// statement here anyway.
		again, ok := AppendNumber(nil, payload, Ascending)
		if !ok || !bytes.Equal(again, key) {
			t.Fatalf("%s: decoded %q did not re-encode (ok=%v)", text, payload, ok)
		}
	}
}

// Digits with a leading or trailing zero are not producible by the encoder, so
// accepting them would let two different byte strings decode to one value and
// break the "equal values have equal keys" invariant callers index on.
func TestDecodeRejectsNonCanonicalDigits(t *testing.T) {
	build := func(digits ...byte) []byte {
		key := []byte{tagNumber, tagNumberPositive}
		key = appendExponent(key, int64(len(digits)), false)
		key = append(key, digits...)
		return append(key, 0)
	}
	for _, digits := range [][]byte{{1}, {1, 2}, {2, 1}, {1, 1}} {
		if _, _, _, err := DecodeComponent(nil, build(digits...), 0); err == nil {
			t.Fatalf("accepted non-canonical digits %v", digits)
		}
	}
	// A canonical digit run (no leading or trailing encoded 1, i.e. no '0').
	if _, _, _, err := DecodeComponent(nil, build(2, 3), 0); err != nil {
		t.Fatalf("rejected canonical digits: %v", err)
	}
}

// Decoding is a storage boundary that can be handed corrupted pages, so it must
// be total: never panic, never read out of bounds, and reject what it cannot
// canonically reproduce. Anything it does accept must re-encode to the same
// bytes, which is what keeps "equal values have equal keys" true on disk.
func FuzzDecodeKey(f *testing.F) {
	seeds := [][]byte{
		nil, {}, {0x10}, {0x20}, {0x21}, {0xef},
		{0x40, 'a', 0, 0},
		{0x40, 0, 0xff, 0, 0},
		{0x30, 0x20},
	}
	for _, s := range seeds {
		f.Add(s)
	}
	key, _ := AppendString(nil, []byte("tenant"), Ascending)
	key, _ = AppendNumber(key, []byte("-12.5e3"), Descending)
	key, _ = AppendBool(key, true, Ascending)
	f.Add(key)

	f.Fuzz(func(t *testing.T, data []byte) {
		var buf []byte
		off := 0
		for off < len(data) {
			c, out, next, err := DecodeComponent(buf[:0], data, off)
			if err != nil {
				return
			}
			if next <= off || next > len(data) {
				t.Fatalf("decode did not advance: off=%d next=%d", off, next)
			}
			d := Ascending
			if c.Descending {
				d = Descending
			}
			payload := out[c.PayloadStart:c.PayloadEnd]
			var again []byte
			var ok bool
			switch c.Kind {
			case KindNull:
				again, ok = AppendNull(nil, d)
			case KindBool:
				again, ok = AppendBool(nil, c.Bool, d)
			case KindNumber:
				again, ok = AppendNumber(nil, payload, d)
			default:
				again, ok = AppendString(nil, payload, d)
			}
			if !ok {
				t.Fatalf("re-encode of decoded component failed: kind=%v payload=%q", c.Kind, payload)
			}
			if !bytes.Equal(again, data[off:next]) {
				t.Fatalf("re-encode mismatch:\n got %x\nwant %x", again, data[off:next])
			}
			buf = out
			off = next
		}
	})
}
