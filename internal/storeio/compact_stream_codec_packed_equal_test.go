package storeio

import (
	"bytes"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

const (
	compactPackedPatternZero = iota
	compactPackedPatternMax
	compactPackedPatternAlternating
	compactPackedPatternRandom
)

func compactPackedEqualPatternData(count, width, pattern int) []byte {
	data := make([]byte, (count*width+7)/8)
	mask := uint64(1)<<uint(width) - 1
	state := uint64(0x8f2a4c6d91e7b305)
	for row := 0; row < count; row++ {
		var value uint64
		switch pattern {
		case compactPackedPatternMax:
			value = mask
		case compactPackedPatternAlternating:
			if row&1 != 0 {
				value = mask
			}
		case compactPackedPatternRandom:
			state = state*6364136223846793005 + 1442695040888963407
			value = state >> uint(64-width)
		}
		compactPutBits(data, row*width, width, value)
	}
	// Make every unused high bit dirty. The equality scan must only consume
	// count logical rows and never treat padding as another packed value.
	if len(data) != 0 {
		used := count * width & 7
		if used != 0 {
			data[len(data)-1] |= ^byte((1 << uint(used)) - 1)
		}
	}
	return data
}

func compactPackedEqualExpected(data []byte, count, width int, want uint64) int {
	matched := 0
	for row := 0; row < count; row++ {
		if compactReadBits(data, row*width, width) == want {
			matched++
		}
	}
	return matched
}

func TestCompactPackedEqualWidth7Through16Parity(t *testing.T) {
	counts := make([]int, 0, 120)
	for count := 0; count <= 80; count++ {
		counts = append(counts, count)
	}
	counts = append(counts,
		95, 96, 97,
		127, 128, 129,
		255, 256, 257,
		511, 512, 513,
		65535, 65536,
	)
	// Sweep the 4096-row reduction boundary and nearby tails. Width7 needs
	// lookahead before its first flush; widths 10 and 16 consume exact blocks.
	for count := 4094; count <= 4112; count++ {
		counts = append(counts, count)
	}
	patterns := []int{
		compactPackedPatternZero,
		compactPackedPatternMax,
		compactPackedPatternAlternating,
		compactPackedPatternRandom,
	}
	for _, width := range []int{7, 8, 10, 16} {
		mask := uint64(1)<<uint(width) - 1
		wants := []uint64{0, 1, mask, mask / 3, mask + 1, ^uint64(0)}
		for _, count := range counts {
			for _, pattern := range patterns {
				packed := compactPackedEqualPatternData(count, width, pattern)
				for offset := 0; offset <= 31; offset++ {
					backing := make([]byte, offset+len(packed)+16)
					for at := range backing {
						backing[at] = 0xa5
					}
					copy(backing[offset:], packed)
					input := backing[offset : offset+len(packed) : offset+len(packed)]
					for _, want := range wants {
						expected := compactPackedEqualExpected(input, count, width, want)
						gotScalar := 0
						switch width {
						case 7:
							gotScalar = countCompactPacked7EqualScalar(input, count, want)
						case 8:
							gotScalar = countCompactPacked8EqualScalar(input, count, want)
						case 10:
							gotScalar = countCompactPacked10EqualScalar(input, count, want)
						case 16:
							gotScalar = countCompactPacked16EqualScalar(input, count, want)
						}
						got := countCompactPackedEqual(input, count, width, want)
						if gotScalar != expected || got != expected {
							t.Fatalf(
								"width=%d count=%d pattern=%d offset=%d want=%d scalar=%d dispatch=%d expected=%d",
								width, count, pattern, offset, want,
								gotScalar, got, expected,
							)
						}
					}
					if !bytes.Equal(input, packed) {
						t.Fatalf("width=%d count=%d pattern=%d offset=%d mutated input", width, count, pattern, offset)
					}
					for at := 0; at < offset; at++ {
						if backing[at] != 0xa5 {
							t.Fatalf("width=%d count=%d pattern=%d offset=%d prefix canary at %d", width, count, pattern, offset, at)
						}
					}
					for at := offset + len(packed); at < len(backing); at++ {
						if backing[at] != 0xa5 {
							t.Fatalf("width=%d count=%d pattern=%d offset=%d suffix canary at %d", width, count, pattern, offset, at)
						}
					}
				}
			}
		}
	}
}

func TestCompactPackedEqualFirstRowsExerciseEveryLane(t *testing.T) {
	const count = 4096
	for _, width := range []int{7, 8, 10, 16} {
		mask := uint64(1)<<uint(width) - 1
		firstRows := 64
		if width == 8 {
			firstRows = 128
		}
		data := make([]byte, (count*width+7)/8)
		const background = uint64(0)
		for row := 0; row < count; row++ {
			value := background
			if row < firstRows {
				value = uint64(row*37+1) & mask
				if width == 8 {
					value = uint64(row + 1)
				}
			}
			compactPutBits(data, row*width, width, value)
		}
		for row := 0; row < firstRows; row++ {
			want := compactReadBits(data, row*width, width)
			// Distinct nonzero values cover every lane, including all four
			// 32-byte AMD64 accumulators, with exactly one match per needle.
			if got := countCompactPackedEqual(data, count, width, want); got != 1 {
				t.Fatalf("width=%d first-row=%d got=%d expected=1", width, row, got)
			}
		}
	}
}

func compactPackedEqualDispatchName(fn func([]byte, int, uint64) int) string {
	pc := reflect.ValueOf(fn).Pointer()
	if pc == 0 {
		return ""
	}
	function := runtime.FuncForPC(pc)
	if function == nil {
		return ""
	}
	return function.Name()
}

func TestCountCompactPackedEqualDispatch(t *testing.T) {
	name7 := compactPackedEqualDispatchName(countCompactPacked7EqualImpl)
	name8 := compactPackedEqualDispatchName(countCompactPacked8EqualImpl)
	name10 := compactPackedEqualDispatchName(countCompactPacked10EqualImpl)
	name16 := compactPackedEqualDispatchName(countCompactPacked16EqualImpl)
	if name7 == "" || name8 == "" || name10 == "" || name16 == "" {
		t.Fatalf("missing packed dispatch function names: width7=%q width8=%q width10=%q width16=%q", name7, name8, name10, name16)
	}
	if !strings.HasSuffix(name7, "countCompactPacked7EqualScalar") &&
		!strings.HasSuffix(name7, "countCompactPacked7EqualNEON") &&
		!strings.HasSuffix(name7, "countCompactPacked7EqualAVX2") {
		t.Fatalf("unexpected width7 dispatch=%q", name7)
	}
	if !strings.HasSuffix(name8, "countCompactPacked8EqualScalar") &&
		!strings.HasSuffix(name8, "countCompactPacked8EqualNEON") &&
		!strings.HasSuffix(name8, "countCompactPacked8EqualAVX2") {
		t.Fatalf("unexpected width8 dispatch=%q", name8)
	}
	if !strings.HasSuffix(name10, "countCompactPacked10EqualScalar") &&
		!strings.HasSuffix(name10, "countCompactPacked10EqualNEON") &&
		!strings.HasSuffix(name10, "countCompactPacked10EqualAVX2") {
		t.Fatalf("unexpected width10 dispatch=%q", name10)
	}
	if !strings.HasSuffix(name16, "countCompactPacked16EqualScalar") &&
		!strings.HasSuffix(name16, "countCompactPacked16EqualNEON") &&
		!strings.HasSuffix(name16, "countCompactPacked16EqualAVX2") {
		t.Fatalf("unexpected width16 dispatch=%q", name16)
	}
}

func TestCountCompactPackedEqualZeroAlloc(t *testing.T) {
	for _, width := range []int{7, 8, 10, 16} {
		count := 4096
		data := compactPackedEqualPatternData(count, width, compactPackedPatternRandom)
		want := uint64(1) << uint(width-1)
		expected := compactPackedEqualExpected(data, count, width, want)
		fn := func() {
			if countCompactPackedEqual(data, count, width, want) != expected {
				panic("packed count changed during allocation check")
			}
		}
		fn()
		if allocs := testing.AllocsPerRun(1_000, fn); allocs != 0 {
			t.Fatalf("width=%d allocations=%v want 0", width, allocs)
		}
	}
}
