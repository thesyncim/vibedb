package orderedkey

import (
	"bytes"
	"math"
	"math/big"
	"slices"
	"strconv"
	"testing"
)

func testScalar(t *testing.T, text string, direction Direction) []byte {
	t.Helper()
	var (
		key []byte
		ok  bool
	)
	switch text[0] {
	case 'n':
		key, ok = AppendNull(nil, direction)
	case 'f':
		key, ok = AppendBool(nil, false, direction)
	case 't':
		key, ok = AppendBool(nil, true, direction)
	case '"':
		key, ok = AppendJSONString(nil, []byte(text), direction)
	default:
		key, ok = AppendNumber(nil, []byte(text), direction)
	}
	if !ok {
		t.Fatalf("encode %q", text)
	}
	return key
}

func TestNumberCanonicalEquality(t *testing.T) {
	equivalent := [][]string{
		{"0", "-0", "0.0", "0e999999999999999999999999"},
		{"1", "1.0", "1.00e0", "0.1e1", "10e-1"},
		{"100", "1e2", "100.000", "0.001e5"},
		{"-12.5", "-12.50", "-125e-1", "-0.125e2"},
		{
			"1e9223372036854775808",
			"0.1e9223372036854775809",
			"10e9223372036854775807",
		},
		{
			"1e-9223372036854775809",
			"0.1e-9223372036854775808",
			"10e-9223372036854775810",
		},
	}
	for _, group := range equivalent {
		want := testScalar(t, group[0], Ascending)
		for _, text := range group[1:] {
			if got := testScalar(t, text, Ascending); !bytes.Equal(got, want) {
				t.Fatalf("%q and %q differ:\n%x\n%x", group[0], text, want, got)
			}
		}
	}
}

func TestCompoundPrefixAndDirection(t *testing.T) {
	prefix, ok := AppendString(nil, []byte("PT"), Ascending)
	if !ok {
		t.Fatal("prefix")
	}
	key := append([]byte(nil), prefix...)
	key, ok = AppendNumber(key, []byte("42"), Descending)
	if !ok || !bytes.HasPrefix(key, prefix) {
		t.Fatalf("compound prefix: %x, %x", prefix, key)
	}
}

func TestRejectsMalformedValues(t *testing.T) {
	dst := []byte{1, 2, 3}
	numbers := [][]byte{
		{}, []byte("-"), []byte("01"), []byte("1."), []byte("1e"),
	}
	for _, number := range numbers {
		got, ok := AppendNumber(dst, number, Ascending)
		if ok || !slices.Equal(got, dst) {
			t.Fatalf("%q: got %x, %v", number, got, ok)
		}
	}
	strings := [][]byte{
		[]byte(`"`), []byte(`"\x"`), []byte(`"\ud800"`),
		[]byte(`"a"b"`),
		{'"', 0x01, '"'},
	}
	for _, value := range strings {
		got, ok := AppendJSONString(dst, value, Ascending)
		if ok || !slices.Equal(got, dst) {
			t.Fatalf("%q: got %x, %v", value, got, ok)
		}
	}
}

func TestEncodedSizeMatchesAppend(t *testing.T) {
	for _, value := range [][]byte{
		[]byte("0"),
		[]byte("-0.000"),
		[]byte("1"),
		[]byte("-123456.7500e-2"),
		[]byte("1e9223372036854775806"),
		[]byte("1e9223372036854775807"),
		[]byte("1e9223372036854775808"),
		[]byte("1e-9223372036854775808"),
		[]byte("1e999999999999999999999999"),
		[]byte("0.01e9223372036854775808"),
		[]byte("1000000000000000000000000000000000000000"),
	} {
		size, ok := NumberEncodedSize(value)
		key, appended := AppendNumber(nil, value, Ascending)
		if ok != appended || ok && size != len(key) {
			t.Fatalf(
				"NumberEncodedSize(%q) = (%d, %t), append = (%d, %t)",
				value, size, ok, len(key), appended,
			)
		}
	}
	for _, value := range [][]byte{
		[]byte(""),
		[]byte("plain"),
		[]byte("embedded\x00nul"),
		[]byte("Olá"),
		{0xff},
	} {
		size, ok := StringEncodedSize(value)
		key, appended := AppendString(nil, value, Ascending)
		if ok != appended || ok && size != len(key) {
			t.Fatalf(
				"StringEncodedSize(%q) = (%d, %t), append = (%d, %t)",
				value, size, ok, len(key), appended,
			)
		}
	}
	for _, raw := range [][]byte{
		[]byte(`""`),
		[]byte(`"plain"`),
		[]byte(`"embedded\u0000nul"`),
		[]byte(`"\ud83d\ude00"`),
		[]byte(`"\ud800"`),
		[]byte(`"\x"`),
	} {
		size, ok := JSONStringEncodedSize(raw)
		key, appended := AppendJSONString(nil, raw, Ascending)
		if ok != appended || ok && size != len(key) {
			t.Fatalf(
				"JSONStringEncodedSize(%q) = (%d, %t), append = (%d, %t)",
				raw, size, ok, len(key), appended,
			)
		}
	}
}

func TestAdjustedExponentArithmeticMatchesBigInt(t *testing.T) {
	explicit := []string{
		"0",
		"1",
		"-1",
		"9223372036854775806",
		"9223372036854775807",
		"9223372036854775808",
		"-9223372036854775807",
		"-9223372036854775808",
		"-9223372036854775809",
		"18446744073709551616",
		"-18446744073709551616",
		"999999999999999999999999999999999999999999",
		"-999999999999999999999999999999999999999999",
	}
	deltas := []int64{
		math.MinInt64, math.MinInt64 + 1, -101, -2, -1, 0, 1, 2, 101,
		math.MaxInt64 - 1, math.MaxInt64,
	}
	for _, text := range explicit {
		_, _, _, _, parsed, ok := numberParts([]byte("1e" + text))
		if !ok {
			t.Fatalf("parse exponent %s", text)
		}
		for _, delta := range deltas {
			adjusted := makeAdjustedExponent(parsed, delta)
			var got string
			if adjusted.compact {
				got = strconv.FormatInt(adjusted.value, 10)
			} else {
				magnitude := appendMagnitude(nil, adjusted)
				if adjusted.negative {
					got = "-"
				}
				got += string(magnitude)
			}
			want, parsedOK := new(big.Int).SetString(text, 10)
			if !parsedOK {
				t.Fatalf("big.Int parse %s", text)
			}
			want.Add(want, big.NewInt(delta))
			if got != want.String() {
				t.Fatalf("%s + %d = %s, want %s", text, delta, got, want)
			}
		}
	}
}

func TestZeroAllocation(t *testing.T) {
	dst := make([]byte, 0, 256)
	if allocations := testing.AllocsPerRun(1000, func() {
		var ok bool
		dst, ok = AppendJSONString(dst[:0], []byte(`"customer\u0000name"`), Ascending)
		if !ok {
			panic("string")
		}
		if size, ok := JSONStringEncodedSize(
			[]byte(`"customer\u0000name"`),
		); !ok || size == 0 {
			panic("string size")
		}
		dst, ok = AppendNumber(dst, []byte("-123456.7500e-2"), Descending)
		if !ok {
			panic("number")
		}
		dst, ok = AppendNumber(dst, []byte("0.1e999999999999999999999999"), Ascending)
		if !ok {
			panic("wide number")
		}
		dst, ok = AppendBool(dst, true, Ascending)
		if !ok {
			panic("bool")
		}
	}); allocations != 0 {
		t.Fatalf("allocations = %v", allocations)
	}
}
