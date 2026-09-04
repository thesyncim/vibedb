package storeio

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

func checkCompactSequentialSeeks(t *testing.T, view compactStreamView) {
	t.Helper()
	if view.kind == compactStreamFront {
		return // Front key seeks also require the decoder's private key buffer.
	}
	bounds := make([]uint16, view.dictCount+1)
	if view.kind == compactStreamDictionary {
		for id := 0; id < view.dictCount; id++ {
			bounds[id+1] = binary.LittleEndian.Uint16(view.dictDir[id*2:])
		}
	}
	got, want := make([]byte, 0, 512), make([]byte, 0, 512)
	var state compactStreamSequentialState
	// Every possible bit alignment and restart offset; seek both forwards and
	// backwards with reused state, then drain across the following restart.
	for start := min(view.count-1, 129); start >= 0; start-- {
		state.seek(&view, start)
		for row := start; row < min(view.count, start+67); row++ {
			var ok, wantOK bool
			if view.kind == compactStreamDictionary {
				got, ok = state.appendDictionary(got[:0], &view, row, bounds)
			} else {
				got, ok = state.appendValue(got[:0], &view, row)
			}
			want, wantOK = view.appendValue(want[:0], row)
			if !ok || !wantOK || !bytes.Equal(got, want) {
				t.Fatalf("start=%d row=%d kind=%d got=%q,%v want=%q,%v", start, row, view.kind, got, ok, want, wantOK)
			}
		}
	}
	if allocs := testing.AllocsPerRun(100, func() {
		for _, row := range []int{1, 63, 64, 65, 127, 128} {
			state.seek(&view, row)
			if view.kind == compactStreamDictionary {
				got, _ = state.appendDictionary(got[:0], &view, row, bounds)
			} else {
				got, _ = state.appendValue(got[:0], &view, row)
			}
		}
	}); allocs != 0 {
		t.Fatalf("seek allocations=%v", allocs)
	}
}

func TestCompactPrimaryScanDecoderSeeks(t *testing.T) {
	for _, high := range []bool{false, true} {
		t.Run(fmt.Sprintf("high=%v", high), func(t *testing.T) {
			_, view, _ := compactPrimaryTestPage(t, 900, high)
			// Exercise both front and prefix-integer key representations.
			front := make([][]byte, view.Len())
			for row := range front {
				front[row], _ = view.AppendKey(nil, row)
			}
			for _, key := range []compactStreamView{view.key, compactCodecRoundTrip(t, encodeCompactFront(front), front)} {
				view.key = key
				var decoder CompactPrimaryScanDecoder
				keyGot, keyWant := make([]byte, 0, 256), make([]byte, 0, 256)
				got, want := make([]byte, 0, 2048), make([]byte, 0, 2048)
				for _, start := range []int{17, 63, 64, 65, 257, 511, 0, 899} {
					for row := start; row < min(view.Len(), start+130); row++ {
						var ok bool
						keyGot, ok = decoder.appendKey(keyGot[:0], &view, view.header.Bucket, row)
						keyWant, _ = view.AppendKey(keyWant[:0], row)
						if !ok || !bytes.Equal(keyGot, keyWant) {
							t.Fatalf("key start=%d row=%d got=%q want=%q", start, row, keyGot, keyWant)
						}
						clear(keyGot) // Borrowed callbacks may mutate both key and value.
						shape := view.rowShape(row)
						got, ok = decoder.appendValue(got[:0], &view, view.header.Bucket, row, shape, view.shapeOrdinal(row, shape))
						want, _ = view.AppendValue(want[:0], row)
						if !ok || !bytes.Equal(got, want) {
							t.Fatalf("value start=%d row=%d got=%q want=%q", start, row, got, want)
						}
						clear(got)
					}
				}
			}
		})
	}
}

func TestCompactPrimaryScanAlphabetBatchRejectsInvalidSymbols(t *testing.T) {
	values := make([][]byte, 129)
	for row := range values {
		values[row] = make([]byte, 33+row%3)
		for i := range values[row] {
			values[row][i] = byte('A' + (row+i)%3)
		}
	}
	var scratch compactStreamScratch
	encoded, ok := scratch.encodeAlphabet(0, values, 0)
	if !ok || encoded.width != 2 {
		t.Fatal("expected two-bit alphabet")
	}
	view := compactCodecRoundTrip(t, encoded, values)
	_, _, bit, _, _ := view.alphabetBlock(0)
	for _, char := range []int{0, 7, 8, 15, 31, 32} {
		bad := view
		bad.data = append([]byte(nil), view.data...)
		compactPutBits(bad.data, bit+char*2, 2, 3)
		var state compactStreamSequentialState
		dst := append(make([]byte, 0, 128), "keep"...)
		out, valid := state.appendValue(dst, &bad, 0)
		if valid || !bytes.Equal(out, []byte("keep")) {
			t.Fatalf("invalid symbol %d accepted or lost caller prefix: %q,%v", char, out, valid)
		}
	}
}
