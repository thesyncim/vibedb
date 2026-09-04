package storeio

import (
	"encoding/binary"
	"strings"
	"testing"
)

func compactPackedLessExpected(data []byte, count, width int, threshold uint64) int {
	matched := 0
	for row := 0; row < count; row++ {
		if compactReadBits(data, row*width, width) < threshold {
			matched++
		}
	}
	return matched
}

func TestCompactPackedLessParity(t *testing.T) {
	counts := make([]int, 0, 120)
	for count := 0; count <= 80; count++ {
		counts = append(counts, count)
	}
	counts = append(counts,
		95, 96, 97,
		127, 128, 129,
		255, 256, 257,
		511, 512, 513,
		4095, 4096, 4097,
	)
	for count := 65535; count <= 65536; count++ {
		counts = append(counts, count)
	}
	for _, width := range []int{7, 8, 10, 16, 32, 56, 57, 64} {
		mask := uint64(1)<<uint(width) - 1
		thresholds := []uint64{0, 1, mask / 3, mask, mask + 1, ^uint64(0)}
		for _, count := range counts {
			packed := compactPackedEqualPatternData(
				count, width, compactPackedPatternRandom,
			)
			for _, offset := range []int{0, 17, 31} {
				backing := make([]byte, offset+len(packed)+16)
				for at := range backing {
					backing[at] = 0xa5
				}
				copy(backing[offset:], packed)
				input := backing[offset : offset+len(packed) : offset+len(packed)]
				for _, threshold := range thresholds {
					expected := compactPackedLessExpected(input, count, width, threshold)
					got := countCompactPackedLess(input, count, width, threshold)
					var scalar int
					switch width {
					case 7:
						scalar = countCompactPacked7LessScalar(input, count, threshold)
					case 8:
						scalar = countCompactPacked8LessScalar(input, count, threshold)
					case 10:
						scalar = countCompactPacked10LessScalar(input, count, threshold)
					case 16:
						scalar = countCompactPacked16LessScalar(input, count, threshold)
					}
					if got != expected || (width == 7 || width == 8 || width == 10 || width == 16) && scalar != expected {
						t.Fatalf(
							"width=%d count=%d offset=%d threshold=%d scalar=%d dispatch=%d expected=%d",
							width, count, offset, threshold, scalar, got, expected,
						)
					}
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

func TestCompactPackedLessZeroWidth(t *testing.T) {
	if got := countCompactPackedLess(nil, 37, 0, 0); got != 0 {
		t.Fatalf("zero width threshold zero = %d, want 0", got)
	}
	if got := countCompactPackedLess(nil, 37, 0, 1); got != 37 {
		t.Fatalf("zero width positive threshold = %d, want 37", got)
	}
}

func TestCountCompactPackedLessDispatch(t *testing.T) {
	for width, fn := range map[int]func([]byte, int, uint64) int{
		7:  countCompactPacked7LessImpl,
		8:  countCompactPacked8LessImpl,
		10: countCompactPacked10LessImpl,
		16: countCompactPacked16LessImpl,
	} {
		name := compactPackedEqualDispatchName(fn)
		if !strings.HasSuffix(name, "LessScalar") &&
			!strings.HasSuffix(name, "LessNEON") &&
			!strings.HasSuffix(name, "LessAVX2") {
			t.Fatalf("width=%d unexpected ordered dispatch=%q", width, name)
		}
	}
}

func TestCompactIntegerOrderedFORParity(t *testing.T) {
	const minInt64Value = int64(-1 << 63)
	const maxInt64Value = int64(1<<63 - 1)
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
	needles := []int64{minInt64Value, -32768, -1, 0, 1, 32767, maxInt64Value}
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
			for _, needle := range needles {
				for _, op := range []UnifiedIntegerOrder{
					UnifiedIntegerLess,
					UnifiedIntegerLessEqual,
					UnifiedIntegerGreater,
					UnifiedIntegerGreaterEqual,
				} {
					want := 0
					for _, value := range fixture.values {
						match := false
						switch op {
						case UnifiedIntegerLess:
							match = value < needle
						case UnifiedIntegerLessEqual:
							match = value <= needle
						case UnifiedIntegerGreater:
							match = value > needle
						case UnifiedIntegerGreaterEqual:
							match = value >= needle
						}
						if match {
							want++
						}
					}
					got, supported := stream.countIntegerOrdered(needle, op)
					if !supported || got != want {
						t.Fatalf("needle=%d op=%d got=%d supported=%t want=%d width=%d", needle, op, got, supported, want, stream.width)
					}
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
	if _, supported := stream.countIntegerOrdered(2, UnifiedIntegerLess); supported {
		t.Fatal("delta stream unexpectedly admitted by ordered FOR lane")
	}
}

func TestCompactIntegerOrderedFORWrappedBase(t *testing.T) {
	const (
		minInt64Value = int64(-1 << 63)
		maxInt64Value = int64(1<<63 - 1)
	)
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
		needle int64
		op     UnifiedIntegerOrder
		want   int
	}{
		{needle: minInt64Value, op: UnifiedIntegerLessEqual, want: 1},
		{needle: 0, op: UnifiedIntegerLess, want: 1},
		{needle: 0, op: UnifiedIntegerGreater, want: 1},
		{needle: maxInt64Value, op: UnifiedIntegerGreaterEqual, want: 1},
	} {
		got, supported := stream.countIntegerOrdered(test.needle, test.op)
		if !supported || got != test.want {
			t.Fatalf("needle=%d op=%d got=%d supported=%t want=%d", test.needle, test.op, got, supported, test.want)
		}
	}

	malformed := compactStreamView{
		kind: compactStreamFOR, width: 16, count: 2, data: make([]byte, 9),
	}
	for _, op := range []UnifiedIntegerOrder{
		UnifiedIntegerLess,
		UnifiedIntegerLessEqual,
		UnifiedIntegerGreater,
		UnifiedIntegerGreaterEqual,
	} {
		if _, supported := malformed.countIntegerOrdered(maxInt64Value, op); supported {
			t.Fatalf("short FOR data admitted for op=%d", op)
		}
	}
}

func TestCompactIntegerOrderedStripeGating(t *testing.T) {
	view, labelResolver, numberResolver, _, _, numberNeedle, _ :=
		compactPackedBenchmarkStripeWide(t)
	for _, op := range []UnifiedIntegerOrder{
		UnifiedIntegerLess,
		UnifiedIntegerLessEqual,
		UnifiedIntegerGreater,
		UnifiedIntegerGreaterEqual,
	} {
		want := 0
		for row := 0; row < view.Len(); row++ {
			value := compactPackedBenchmarkNumber16Value(row)
			match := false
			switch op {
			case UnifiedIntegerLess:
				match = value < numberNeedle
			case UnifiedIntegerLessEqual:
				match = value <= numberNeedle
			case UnifiedIntegerGreater:
				match = value > numberNeedle
			case UnifiedIntegerGreaterEqual:
				match = value >= numberNeedle
			}
			if match {
				want++
			}
		}
		got, ok := view.CountResolvedIntegerOrdered(&numberResolver, numberNeedle, op)
		if !ok || got != want {
			t.Fatalf("FOR16 op=%d got=%d ok=%t want=%d", op, got, ok, want)
		}
	}
	if _, ok := view.CountResolvedIntegerOrdered(
		&labelResolver, 0, UnifiedIntegerLess,
	); ok {
		t.Fatal("dictionary8 target unexpectedly admitted by ordered FOR lane")
	}
}

func compactOrderedFORFixture(base int64, span int64) []int64 {
	values := make([]int64, 65)
	values[0] = base
	values[1] = base + span
	for row := 2; row < len(values); row++ {
		values[row] = base + int64((row*37)%int(span+1))
	}
	return values
}

func TestCompactPackedLessZeroAlloc(t *testing.T) {
	for _, width := range []int{7, 8, 10, 16} {
		const count = 4096
		data := compactPackedEqualPatternData(count, width, compactPackedPatternRandom)
		threshold := uint64(1) << uint(width-1)
		want := compactPackedLessExpected(data, count, width, threshold)
		fn := func() {
			if countCompactPackedLess(data, count, width, threshold) != want {
				panic("packed ordered count changed during allocation check")
			}
		}
		fn()
		if allocs := testing.AllocsPerRun(1_000, fn); allocs != 0 {
			t.Fatalf("width=%d allocations=%v want 0", width, allocs)
		}
	}
}
