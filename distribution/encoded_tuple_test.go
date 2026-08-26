package distribution

import (
	"bytes"
	"testing"
)

func TestNativePointForEncodedTuplePrefixMatchesNativeMapper(t *testing.T) {
	number, err := NewNumber("50e-1")
	if err != nil {
		t.Fatal(err)
	}
	values := []Scalar{NewString("tenant-a"), number}
	prefix, err := CurrentTupleCodec.AppendTuple(nil, values)
	if err != nil {
		t.Fatal(err)
	}
	locator, err := CurrentTupleCodec.AppendTuple(nil, []Scalar{NewString("row-7")})
	if err != nil {
		t.Fatal(err)
	}
	raw := append(append([]byte(nil), prefix...), locator...)
	want, err := NewNativeMapperWithBucketBits(2, 17).PointFor(values)
	if err != nil {
		t.Fatal(err)
	}
	got, consumed, ok := NativePointForEncodedTuplePrefix(raw, 2, 17)
	if !ok || got != want || consumed != len(prefix) ||
		!bytes.Equal(raw[consumed:], locator) {
		t.Fatalf("encoded route = %x,%d,%v, want %x,%d", got, consumed, ok, want, len(prefix))
	}
}

func TestCanonicalTuplePrefixLenFailsClosed(t *testing.T) {
	valid, err := CurrentTupleCodec.AppendTuple(nil, []Scalar{NewString("x")})
	if err != nil {
		t.Fatal(err)
	}
	cases := [][]byte{
		nil,
		{tagString},
		{tagString, 0x81, 0x00, 'x'}, // overlong uvarint
		{tagNumber, numberFormPositive, weightFormZero, 1, '0'},
		{tagNumber, numberFormPositive, weightFormPositive, 1, '0', 1, '1'},
		{tagNumber, numberFormPositive, weightFormZero, 2, '1'},
		append(append([]byte(nil), valid...), byte(0xff)),
	}
	for index, raw := range cases {
		arity := 1
		if index == len(cases)-1 {
			// Trailing bytes are legal for prefix parsing; requesting the next
			// scalar must still reject them.
			arity = 2
		}
		if _, ok := CanonicalTuplePrefixLen(raw, arity); ok {
			t.Fatalf("case %d accepted malformed tuple %x", index, raw)
		}
	}
	if _, _, ok := NativePointForEncodedTuplePrefix(valid, 1, 7); ok {
		t.Fatal("accepted unsupported bucket width")
	}
}

func TestNativePointForEncodedTuplePrefixAllocations(t *testing.T) {
	raw, err := CurrentTupleCodec.AppendTuple(nil, []Scalar{NewString("tenant-a"), NewString("email")})
	if err != nil {
		t.Fatal(err)
	}
	if got := testing.AllocsPerRun(1000, func() {
		_, _, _ = NativePointForEncodedTuplePrefix(raw, 2, DefaultVirtualBucketBits)
	}); got != 0 {
		t.Fatalf("encoded tuple routing allocations = %v", got)
	}
}
