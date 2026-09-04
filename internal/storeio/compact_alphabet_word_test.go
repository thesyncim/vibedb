package storeio

import (
	"bytes"
	"testing"
)

func TestCompactSpreadAlphabet5(t *testing.T) {
	for lane := range 8 {
		for code := uint64(0); code < 32; code++ {
			for _, background := range []uint64{0, ^uint64(0), 0x8b153794c632ad57} {
				packed := background & ^(uint64(31)<<uint(lane*5)) | code<<uint(lane*5)
				got := compactSpreadAlphabet5(packed)
				for i := range 8 {
					if byte(got>>uint(i*8)) != byte(packed>>uint(i*5)&31) {
						t.Fatalf("lane=%d code=%d input=%x output=%x", lane, code, packed, got)
					}
				}
			}
		}
	}
}

func TestCompactAlphabetWordRejectsInvalidCodes(t *testing.T) {
	for length := 1; length <= 31; length++ {
		values := make([][]byte, 129)
		values[0] = []byte("abcdefghijklmnopqrstuvwxyz")
		for row := 1; row < len(values); row++ {
			values[row] = bytes.Repeat([]byte{'c'}, 17)
		}
		values[1] = bytes.Repeat([]byte{'a'}, length)
		var scratch compactStreamScratch
		encoded, ok := scratch.encodeAlphabet(0, values, 0)
		if !ok || encoded.width != 5 {
			t.Fatal("expected five-bit alphabet")
		}
		view := compactCodecRoundTrip(t, encoded, values)
		for at := range length {
			bad := view
			bad.data = bytes.Clone(view.data)
			var state compactStreamSequentialState
			state.seek(&bad, 1)
			compactPutBits(bad.data, state.bit+at*5, 5, 31)
			dst := append(make([]byte, 0, 128), "keep"...)
			got, valid := state.appendValue(dst, &bad, 1)
			if valid || !bytes.Equal(got, []byte("keep")) {
				t.Fatalf("length=%d at=%d: got=%q valid=%v", length, at, got, valid)
			}
		}
	}
}

func TestCompactAlphabetPointFailedSeekDoesNotReenter(t *testing.T) {
	values := [][]byte{[]byte("abcdefghijklmnopqrstuvwx"), []byte("zyxwvutsrqponmlkjihgfedcba")}
	var scratch compactStreamScratch
	encoded, ok := scratch.encodeAlphabet(0, values, 0)
	if !ok {
		t.Fatal("expected alphabet")
	}
	view := compactCodecRoundTrip(t, encoded, values)
	view.data = nil
	for _, row := range []int{0, 1} {
		got, valid := view.appendAlphabetValue([]byte("keep"), row)
		if valid || !bytes.Equal(got, []byte("keep")) {
			t.Fatalf("row=%d got=%q valid=%v", row, got, valid)
		}
	}
}
