package storeio

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func compactPackedExtremaExpected(data []byte, count, width int) (uint64, uint64, bool) {
	if count == 0 {
		return 0, 0, false
	}
	minimum := uint64(1)<<uint(width) - 1
	var maximum uint64
	for row := 0; row < count; row++ {
		value := compactReadBits(data, row*width, width)
		if value < minimum {
			minimum = value
		}
		if value > maximum {
			maximum = value
		}
	}
	return minimum, maximum, true
}

func TestCompactPackedExtremaParityAndCanaries(t *testing.T) {
	counts := []int{0, 1, 2, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256, 511, 512, 1023, 1024, 4095, 4096, 4097}
	for _, width := range []int{1, 2, 7, 8, 10, 16, 32, 56, 57, 64} {
		for _, count := range counts {
			packed := compactPackedEqualPatternData(count, width, compactPackedPatternRandom)
			for _, offset := range []int{0, 1, 17, 31} {
				backing := bytes.Repeat([]byte{0xa5}, offset+len(packed)+16)
				copy(backing[offset:], packed)
				input := backing[offset : offset+len(packed) : offset+len(packed)]
				wantMin, wantMax, wantFound := compactPackedExtremaExpected(input, count, width)
				gotMin, gotMax, gotFound, supported := countCompactPackedExtrema(input, count, width)
				if !supported || gotMin != wantMin || gotMax != wantMax || gotFound != wantFound {
					t.Fatalf("width=%d count=%d offset=%d got=(%d,%d,%t,%t) want=(%d,%d,%t)", width, count, offset, gotMin, gotMax, gotFound, supported, wantMin, wantMax, wantFound)
				}
				if !bytes.Equal(input, packed) {
					t.Fatalf("width=%d count=%d offset=%d mutated input", width, count, offset)
				}
				for at := 0; at < offset; at++ {
					if backing[at] != 0xa5 {
						t.Fatalf("width=%d count=%d prefix canary at %d", width, count, at)
					}
				}
				for at := offset + len(packed); at < len(backing); at++ {
					if backing[at] != 0xa5 {
						t.Fatalf("width=%d count=%d suffix canary at %d", width, count, at)
					}
				}
			}
		}
	}
}

func TestCompactPackedExtremaZeroWidthAndDispatch(t *testing.T) {
	minimum, maximum, found, supported := countCompactPackedExtrema(nil, 37, 0)
	if !supported || !found || minimum != 0 || maximum != 0 {
		t.Fatalf("zero-width extrema=(%d,%d,%t,%t), want (0,0,true,true)", minimum, maximum, found, supported)
	}
	minimum, maximum, found, supported = countCompactPackedExtrema(nil, 0, 0)
	if !supported || found || minimum != 0 || maximum != 0 {
		t.Fatalf("empty extrema=(%d,%d,%t,%t), want (0,0,false,true)", minimum, maximum, found, supported)
	}
	for _, tc := range []struct {
		name         string
		data         []byte
		count, width int
	}{
		{name: "empty-width0-trailing", data: []byte{0}, count: 0, width: 0},
		{name: "empty-width7-trailing", data: []byte{0}, count: 0, width: 7},
		{name: "zero-width-trailing", data: []byte{0}, count: 37, width: 0},
		{name: "short", data: nil, count: 1, width: 7},
		{name: "trailing", data: []byte{0, 0}, count: 1, width: 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			minimum, maximum, found, supported := countCompactPackedExtrema(tc.data, tc.count, tc.width)
			if supported || found || minimum != 0 || maximum != 0 {
				t.Fatalf("malformed extrema=(%d,%d,%t,%t), want (0,0,false,false)", minimum, maximum, found, supported)
			}
		})
	}
	for width, fn := range map[int]func([]byte, int) (uint64, uint64, bool, bool){
		7: countCompactPacked7ExtremaImpl, 8: countCompactPacked8ExtremaImpl,
		10: countCompactPacked10ExtremaImpl, 16: countCompactPacked16ExtremaImpl,
	} {
		name := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
		if !strings.HasSuffix(name, "ExtremaScalar") &&
			!strings.HasSuffix(name, "ExtremaNEON") &&
			!strings.HasSuffix(name, "ExtremaAVX2") {
			t.Fatalf("width=%d unexpected extrema dispatch=%q", width, name)
		}
	}
}

func TestCompactIntegerExtremaFORStrictAdmission(t *testing.T) {
	const (
		minInt64Value = int64(-1 << 63)
		maxInt64Value = int64(1<<63 - 1)
	)
	fixtures := []struct {
		name   string
		values []int64
	}{
		{name: "width0", values: []int64{17}},
		{name: "width7", values: compactOrderedFORFixture(-64, 127)},
		{name: "width8", values: compactOrderedFORFixture(-128, 255)},
		{name: "width10", values: compactOrderedFORFixture(-512, 1023)},
		{name: "width16", values: compactOrderedFORFixture(-32768, 65535)},
		{name: "width56-signed", values: []int64{-7, (int64(1) << 56) - 8}},
		{name: "width57-signed", values: []int64{-8, (int64(1) << 57) - 9}},
		{name: "width61-signed", values: []int64{-11, (int64(1) << 61) - 12}},
	}
	for _, fixture := range fixtures {
		fixture := fixture
		t.Run(fixture.name, func(t *testing.T) {
			encoding := encodeCompactFOR(fixture.values)
			raw, err := encoding.appendBinary(nil)
			if err != nil {
				t.Fatal(err)
			}
			stream, err := openCompactStream(raw)
			if err != nil {
				t.Fatal(err)
			}
			wantMin, wantMax := fixture.values[0], fixture.values[0]
			for _, value := range fixture.values[1:] {
				if value < wantMin {
					wantMin = value
				}
				if value > wantMax {
					wantMax = value
				}
			}
			gotMin, gotMax, found, supported := stream.countIntegerExtrema()
			if !supported || !found || gotMin != wantMin || gotMax != wantMax {
				t.Fatalf("got=(%d,%d,%t,%t), want=(%d,%d,true,true), width=%d", gotMin, gotMax, found, supported, wantMin, wantMax, stream.width)
			}
		})
	}

	width64 := encodeCompactFOR([]int64{minInt64Value, -1, 0, 1, maxInt64Value})
	raw, err := width64.appendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := openCompactStream(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, found, supported := stream.countIntegerExtrema(); found || supported {
		t.Fatalf("width64 extrema=(found=%t supported=%t), want false,false", found, supported)
	}

	data := make([]byte, 9)
	binary.LittleEndian.PutUint64(data, uint64(maxInt64Value))
	data[8] = 0b10
	wrapped := compactStreamEncoding{kind: compactStreamFOR, width: 1, count: 2, data: data}
	raw, err = wrapped.appendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	stream, err = openCompactStream(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, found, supported := stream.countIntegerExtrema(); found || supported {
		t.Fatalf("wrapped extrema=(found=%t supported=%t), want false,false", found, supported)
	}

	delta := encodeCompactDelta([]int64{1, 2, 3})
	raw, err = delta.appendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	stream, err = openCompactStream(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, found, supported := stream.countIntegerExtrema(); found || supported {
		t.Fatalf("delta extrema=(found=%t supported=%t), want false,false", found, supported)
	}
}

func TestCompactPackedExtremaZeroAlloc(t *testing.T) {
	for _, width := range []int{7, 8, 10, 16} {
		const count = 4096
		data := compactPackedEqualPatternData(count, width, compactPackedPatternRandom)
		wantMin, wantMax, wantFound := compactPackedExtremaExpected(data, count, width)
		fn := func() {
			gotMin, gotMax, gotFound, supported := countCompactPackedExtrema(data, count, width)
			if !supported || !gotFound || gotMin != wantMin || gotMax != wantMax || gotFound != wantFound {
				panic("packed extrema changed during allocation check")
			}
		}
		fn()
		if allocs := testing.AllocsPerRun(1000, fn); allocs != 0 {
			t.Fatalf("width=%d allocations=%v want 0", width, allocs)
		}
	}
}
