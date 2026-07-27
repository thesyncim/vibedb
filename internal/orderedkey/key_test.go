package orderedkey

import (
	"bytes"
	"slices"
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
		{"0", "-0", "0.0", "0e999"},
		{"1", "1.0", "1.00e0", "0.1e1", "10e-1"},
		{"100", "1e2", "100.000", "0.001e5"},
		{"-12.5", "-12.50", "-125e-1", "-0.125e2"},
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
	end, ok := AppendPrefixEnd(nil, prefix)
	if !ok || bytes.Compare(prefix, key) > 0 || bytes.Compare(key, end) >= 0 {
		t.Fatalf("prefix bounds: prefix=%x key=%x end=%x", prefix, key, end)
	}
}

func TestRejectsMalformedAndHugeValues(t *testing.T) {
	dst := []byte{1, 2, 3}
	numbers := [][]byte{
		{}, []byte("-"), []byte("01"), []byte("1."), []byte("1e"),
		[]byte("1e999999999999999999999999"),
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
		[]byte{'"', 0x01, '"'},
	}
	for _, value := range strings {
		got, ok := AppendJSONString(dst, value, Ascending)
		if ok || !slices.Equal(got, dst) {
			t.Fatalf("%q: got %x, %v", value, got, ok)
		}
	}
}

func TestZeroAllocation(t *testing.T) {
	dst := make([]byte, 0, 128)
	if allocations := testing.AllocsPerRun(1000, func() {
		var ok bool
		dst, ok = AppendJSONString(dst[:0], []byte(`"customer\u0000name"`), Ascending)
		if !ok {
			panic("string")
		}
		dst, ok = AppendNumber(dst, []byte("-123456.7500e-2"), Descending)
		if !ok {
			panic("number")
		}
		dst, ok = AppendBool(dst, true, Ascending)
		if !ok {
			panic("bool")
		}
	}); allocations != 0 {
		t.Fatalf("allocations = %v", allocations)
	}
}
