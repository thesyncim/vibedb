package storeio

import (
	"strconv"
	"strings"
	"testing"
)

func TestCompactIntegerSpellingDecodersBoundMemory(t *testing.T) {
	for _, kind := range []string{"dictionary", "front", "alphabet", "prefix"} {
		t.Run(kind, func(t *testing.T) {
			encode := func(values [][]byte) compactStreamEncoding {
				switch kind {
				case "dictionary":
					return encodeCompactDictionary(values)
				case "front":
					return encodeCompactFront(values)
				case "alphabet":
					var scratch compactStreamScratch
					out, ok := scratch.encodeAlphabet(0, values, 0)
					if !ok {
						t.Fatal("alphabet encoding declined")
					}
					return out
				default:
					out, ok := encodeCompactPrefixInt(values)
					if !ok {
						t.Fatal("prefix encoding declined")
					}
					return out
				}
			}
			values := [][]byte{[]byte("123401"), []byte("123402"), []byte("123403")}
			view := compactCodecRoundTrip(t, encode(values), values)
			for row, raw := range values {
				want, _ := strconv.ParseInt(string(raw), 10, 64)
				got, ok := unifiedCompactIntegerValue(view, row)
				if !ok || got != want {
					t.Fatalf("row %d = %d/%t, want %d", row, got, ok, want)
				}
			}
			long := strings.Repeat("x", 4096)
			values = [][]byte{[]byte(long + "1"), []byte(long + "2"), []byte(long + "3")}
			view = compactCodecRoundTrip(t, encode(values), values)
			if allocs := testing.AllocsPerRun(10, func() {
				if _, ok := unifiedCompactIntegerValue(view, 2); ok {
					t.Fatal("long noninteger accepted")
				}
			}); allocs != 0 {
				t.Fatalf("long token rejection allocated %g times", allocs)
			}
		})
	}
}

func TestUnifiedCanonicalIntegerBoundaries(t *testing.T) {
	for _, raw := range []string{"9223372036854775807", "-9223372036854775808", "0", "-1"} {
		want, _ := strconv.ParseInt(raw, 10, 64)
		if got, ok := unifiedCanonicalInt64Value([]byte(raw)); !ok || got != want {
			t.Fatalf("%s = %d/%t", raw, got, ok)
		}
	}
	for _, raw := range []string{"9223372036854775808", "-9223372036854775809", "-0", "1.0", "1e0", `"1"`, "01"} {
		if _, ok := unifiedCanonicalInt64Value([]byte(raw)); ok {
			t.Fatalf("accepted %q", raw)
		}
	}
}
