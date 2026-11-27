package query

import (
	"bytes"
	"strings"
	"testing"

	vibejson "github.com/thesyncim/vibejson"
)

func TestJSONGroupKeyEncoderMatchesExactScalarIdentity(t *testing.T) {
	classes := [][]string{
		{"0", "-0", "0.0", "0e1000000000"},
		{"1", "1.0", "10e-1", "0.1e1", " \n1.00\t"},
		{`"café"`, `"caf\u00e9"`},
		{"null"}, {"false"}, {"true"}, {`{"x":1}`},
	}
	var encoder JSONGroupKeyEncoder
	keys := make([][]byte, len(classes))
	for class := range classes {
		for _, spelling := range classes[class] {
			key, ok := encoder.Append(nil, []byte(spelling))
			if !ok {
				t.Fatalf("Append(%q) rejected valid JSON", spelling)
			}
			if keys[class] == nil {
				keys[class] = key
			} else if !bytes.Equal(key, keys[class]) {
				t.Fatalf("equivalent spellings %q and %q have different keys", classes[class][0], spelling)
			}
		}
	}
	for left := range keys {
		for right := left + 1; right < len(keys); right++ {
			if bytes.Equal(keys[left], keys[right]) {
				t.Fatalf("distinct classes %q and %q have equal keys", classes[left][0], classes[right][0])
			}
		}
	}
	prefix := []byte("keep:")
	if got, ok := encoder.Append(prefix, []byte(`"\ud800"`)); ok || string(got) != "keep:" {
		t.Fatalf("invalid JSON changed destination: %q, ok=%v", got, ok)
	}

	source := []byte(`"escaped \ud83d\ude00 group"`)
	dst := make([]byte, 0, 128)
	dst, _ = encoder.Append(dst, source) // warm decoded-string scratch
	if allocs := testing.AllocsPerRun(1000, func() {
		var ok bool
		dst, ok = encoder.Append(dst[:0], source)
		if !ok {
			panic("valid group key rejected")
		}
	}); allocs != 0 {
		t.Fatalf("warmed JSON group-key allocations = %v, want 0", allocs)
	}
}

func FuzzJSONGroupKeyEncoder(f *testing.F) {
	for _, seed := range [][]byte{
		[]byte("null"), []byte("1.0"), []byte(`"caf\u00e9"`),
		[]byte(`{"x":1}`), []byte(`"\ud800"`), {0xff},
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value []byte) {
		var encoder JSONGroupKeyEncoder
		prefix := []byte("keep:")
		first, ok := encoder.Append(prefix, value)
		if !ok {
			if string(first) != "keep:" {
				t.Fatalf("rejected value changed destination: %x", first)
			}
			return
		}
		second, secondOK := encoder.Append(nil, value)
		if !secondOK || !bytes.Equal(first[len(prefix):], second) {
			t.Fatalf("group-key encoding is not deterministic for %x", value)
		}
	})
}

func TestJSONGroupKeyEncoderFastPathDifferential(t *testing.T) {
	cases := []string{
		"0", "-0", "0.0", "-0.0", "0e0",
		"1", "-1", "9", "-9", "10", "-10", "100", "1e2", "1000", "-1000",
		"1234500", "1.2345e6",
		"9223372036854775807", "-9223372036854775808",
		"9999999999", "10000000000",
		strings.Repeat("9", 100),
		"1" + strings.Repeat("0", 120),
		"-1" + strings.Repeat("0", 150) + "5",
		`""`, `"a"`, `"hello"`, `"a\nb"`, `"\"quoted\""`, `"\u0041"`, `"A"`,
		`"café"`, `"🎉"`, `"` + strings.Repeat("x", 200) + `"`,
		"null", "true", "false",
		"  42  ", "\t\"ok\"\n", " null ", " true\r\n",
		"[]", "[1,2]", "{}", `{"a":1}`,
		// Invalid JSON inputs that must be rejected identically.
		"", "   ", "-", "01", "-01", "00", "+1", "1.", "1e", "1x",
		`"unterminated`, "\"\x00\"", "\"\x1f\"", `"a"x`, `"a""b"`,
		"nul", "tru", "fals", "true1", "nullx", "NaN", "Infinity",
	}
	var encoder JSONGroupKeyEncoder
	for _, tc := range cases {
		raw := []byte(tc)
		got, gotOK := encoder.Append(nil, raw)

		trimmed := trimJSONGroupSpace(raw)
		wantOK := len(trimmed) > 0 && vibejson.Valid(trimmed)
		var want []byte
		if wantOK {
			var scratch []byte
			scalar := classifyRawInto(vibejson.RawValue{Src: trimmed}, &scratch)
			want = appendGroupKey(nil, scalar)
		}
		if gotOK != wantOK {
			t.Fatalf("Append(%q): ok = %v, want %v", tc, gotOK, wantOK)
		}
		if gotOK && !bytes.Equal(got, want) {
			t.Fatalf("Append(%q): got %x, want %x", tc, got, want)
		}
	}
}
