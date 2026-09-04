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

func TestCompactPackedEqualWidth7And10Parity(t *testing.T) {
	counts := make([]int, 0, 120)
	for count := 0; count <= 80; count++ {
		counts = append(counts, count)
	}
	counts = append(counts,
		255, 256, 257,
		65535, 65536,
	)
	// The vector's lookahead leaves a scalar tail at the flush boundary;
	// sweep both sides so each packed width tests its first actual flush.
	for count := 4094; count <= 4112; count++ {
		counts = append(counts, count)
	}
	patterns := []int{
		compactPackedPatternZero,
		compactPackedPatternMax,
		compactPackedPatternAlternating,
		compactPackedPatternRandom,
	}
	for _, width := range []int{7, 10} {
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
						if width == 7 {
							gotScalar = countCompactPacked7EqualScalar(input, count, want)
						} else {
							gotScalar = countCompactPacked10EqualScalar(input, count, want)
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
	for _, width := range []int{7, 10} {
		mask := uint64(1)<<uint(width) - 1
		data := make([]byte, (count*width+7)/8)
		background := mask / 3
		for row := 0; row < count; row++ {
			value := background
			if row < 32 {
				value = uint64(row*37+1) & mask
			}
			compactPutBits(data, row*width, width, value)
		}
		for row := 0; row < 32; row++ {
			want := compactReadBits(data, row*width, width)
			// These first 32 values are distinct and exclude background, so
			// each lane must contribute exactly once without an oracle scan.
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
	name10 := compactPackedEqualDispatchName(countCompactPacked10EqualImpl)
	if name7 == "" || name10 == "" {
		t.Fatalf("missing packed dispatch function names: width7=%q width10=%q", name7, name10)
	}
	if !strings.HasSuffix(name7, "countCompactPacked7EqualScalar") &&
		!strings.HasSuffix(name7, "countCompactPacked7EqualNEON") {
		t.Fatalf("unexpected width7 dispatch=%q", name7)
	}
	if !strings.HasSuffix(name10, "countCompactPacked10EqualScalar") &&
		!strings.HasSuffix(name10, "countCompactPacked10EqualNEON") {
		t.Fatalf("unexpected width10 dispatch=%q", name10)
	}
}

func TestCountCompactPackedEqualZeroAlloc(t *testing.T) {
	for _, width := range []int{7, 10} {
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
