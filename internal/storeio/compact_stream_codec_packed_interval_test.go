package storeio

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func compactPackedBetweenExpected(
	data []byte, count, width int, lower, upper uint64,
) int {
	matched := 0
	for row := 0; row < count; row++ {
		value := compactReadBits(data, row*width, width)
		if value >= lower && value < upper {
			matched++
		}
	}
	return matched
}

func TestCompactPackedBetweenParityAndCanaries(t *testing.T) {
	counts := []int{
		0, 1, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128,
		255, 256, 511, 512, 1023, 1024, 4095, 4096, 4097,
	}
	for _, width := range []int{7, 8, 10, 16} {
		mask := uint64(1)<<uint(width) - 1
		intervals := [][2]uint64{
			{0, 1},
			{1, 2},
			{mask / 3, mask/3 + 1},
			{mask / 2, mask},
			{mask, mask + 1},
			{0, mask + 1},
			{mask + 1, mask + 2},
		}
		for _, count := range counts {
			packed := compactPackedEqualPatternData(
				count, width, compactPackedPatternRandom,
			)
			for _, offset := range []int{0, 1, 17, 31} {
				backing := bytes.Repeat([]byte{0xa5}, offset+len(packed)+16)
				copy(backing[offset:], packed)
				input := backing[offset : offset+len(packed) : offset+len(packed)]
				for _, interval := range intervals {
					lower, upper := interval[0], interval[1]
					expected := compactPackedBetweenExpected(input, count, width, lower, upper)
					got := countCompactPackedBetween(input, count, width, lower, upper)
					var scalar int
					switch width {
					case 7:
						scalar = countCompactPacked7BetweenScalar(input, count, lower, upper)
					case 8:
						scalar = countCompactPacked8BetweenScalar(input, count, lower, upper)
					case 10:
						scalar = countCompactPacked10BetweenScalar(input, count, lower, upper)
					case 16:
						scalar = countCompactPacked16BetweenScalar(input, count, lower, upper)
					}
					if got != expected || scalar != expected {
						t.Fatalf("width=%d count=%d offset=%d interval=[%d,%d) scalar=%d dispatch=%d expected=%d", width, count, offset, lower, upper, scalar, got, expected)
					}
				}
				if !bytes.Equal(input, packed) {
					t.Fatalf("width=%d count=%d offset=%d mutated input", width, count, offset)
				}
				for at := 0; at < offset; at++ {
					if backing[at] != 0xa5 {
						t.Fatalf("width=%d count=%d offset=%d prefix canary at %d", width, count, offset, at)
					}
				}
				for at := offset + len(packed); at < len(backing); at++ {
					if backing[at] != 0xa5 {
						t.Fatalf("width=%d count=%d offset=%d suffix canary at %d", width, count, offset, at)
					}
				}
			}
		}
	}
}

func TestCompactPackedBetweenZeroWidthAndBounds(t *testing.T) {
	if got := countCompactPackedBetween(nil, 37, 0, 0, 1); got != 37 {
		t.Fatalf("zero-width full interval=%d, want 37", got)
	}
	if got := countCompactPackedBetween(nil, 37, 0, 1, 2); got != 0 {
		t.Fatalf("zero-width excluded interval=%d, want 0", got)
	}
	data := make([]byte, 7)
	if got := countCompactPackedBetween(data, 8, 7, 128, 129); got != 0 {
		t.Fatalf("out-of-range lower=%d, want 0", got)
	}
	if got := countCompactPackedBetween(data, 8, 7, 0, 128); got != 8 {
		t.Fatalf("full width interval=%d, want 8", got)
	}
	if got := countCompactPackedBetween(data, 8, 7, 2, 128); got != 0 {
		t.Fatalf("dirty all-zero logical lanes=%d, want 0", got)
	}
}

func TestCompactPackedBetweenDispatch(t *testing.T) {
	for width, fn := range map[int]func([]byte, int, uint64, uint64) int{
		7:  countCompactPacked7BetweenImpl,
		8:  countCompactPacked8BetweenImpl,
		10: countCompactPacked10BetweenImpl,
		16: countCompactPacked16BetweenImpl,
	} {
		name := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
		if !strings.HasSuffix(name, "BetweenScalar") &&
			!strings.HasSuffix(name, "BetweenNEON") &&
			!strings.HasSuffix(name, "BetweenAVX2") {
			t.Fatalf("width=%d unexpected interval dispatch=%q", width, name)
		}
	}
}

func TestCompactIntegerIntervalFORParity(t *testing.T) {
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
		{name: "width64", values: []int64{minInt64Value, -1, 0, 1, maxInt64Value}},
	}
	intervals := []UnifiedIntegerInterval{
		{Lower: minInt64Value, Upper: -1},
		{Lower: -1, Upper: 1},
		{Lower: 0, Upper: maxInt64Value},
		{Lower: maxInt64Value, Upper: maxInt64Value},
		{Lower: 17, Upper: 0},
		{Lower: 1, Upper: maxInt64Value, UpperUnbounded: true},
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
			for _, interval := range intervals {
				want := 0
				for _, value := range fixture.values {
					if value >= interval.Lower &&
						(interval.UpperUnbounded || value < interval.Upper) {
						want++
					}
				}
				got, supported := stream.countIntegerInterval(interval)
				if !supported || got != want {
					t.Fatalf("interval=%+v got=%d supported=%t want=%d width=%d", interval, got, supported, want, stream.width)
				}
			}
		})
	}

	delta := encodeCompactDelta([]int64{1, 2, 3})
	raw, err := delta.appendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := openCompactStream(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, supported := stream.countIntegerInterval(
		UnifiedIntegerInterval{Lower: 1, Upper: 3},
	); supported {
		t.Fatal("delta stream unexpectedly admitted by interval FOR lane")
	}
	malformed := compactStreamView{
		kind: compactStreamFOR, width: 16, count: 2, data: make([]byte, 9),
	}
	if _, supported := malformed.countIntegerInterval(
		UnifiedIntegerInterval{Lower: 0, Upper: 1},
	); supported {
		t.Fatal("malformed FOR stream unexpectedly admitted by interval lane")
	}
}

func TestCompactIntegerIntervalFORWrappedBase(t *testing.T) {
	const maxInt64Value = int64(1<<63 - 1)
	data := make([]byte, 9)
	binary.LittleEndian.PutUint64(data, uint64(maxInt64Value))
	data[8] = 0b10 // MaxInt64 + 0, MaxInt64 + 1 (wrapping to MinInt64).
	encoding := compactStreamEncoding{
		kind: compactStreamFOR, width: 1, count: 2, data: data,
	}
	raw, err := encoding.appendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := openCompactStream(raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		interval UnifiedIntegerInterval
		want     int
	}{
		{interval: UnifiedIntegerInterval{Lower: -1, Upper: 1}, want: 0},
		{interval: UnifiedIntegerInterval{Lower: 0, Upper: maxInt64Value, UpperUnbounded: true}, want: 1},
		{interval: UnifiedIntegerInterval{Lower: maxInt64Value, Upper: maxInt64Value, UpperUnbounded: true}, want: 1},
		{interval: UnifiedIntegerInterval{Lower: minInt64ValueForTest(), Upper: 0}, want: 1},
	} {
		got, supported := stream.countIntegerInterval(test.interval)
		if !supported || got != test.want {
			t.Fatalf("interval=%+v got=%d supported=%t want=%d", test.interval, got, supported, test.want)
		}
	}
}

func minInt64ValueForTest() int64 { return -1 << 63 }

func TestCompactPackedBetweenZeroAlloc(t *testing.T) {
	for _, width := range []int{7, 8, 10, 16} {
		const count = 4096
		data := compactPackedEqualPatternData(count, width, compactPackedPatternRandom)
		lower := uint64(1) << uint(width-2)
		upper := uint64(1) << uint(width-1)
		want := compactPackedBetweenExpected(data, count, width, lower, upper)
		fn := func() {
			if countCompactPackedBetween(data, count, width, lower, upper) != want {
				panic("packed interval count changed during allocation check")
			}
		}
		fn()
		if allocs := testing.AllocsPerRun(1_000, fn); allocs != 0 {
			t.Fatalf("width=%d allocations=%v want 0", width, allocs)
		}
	}
}
