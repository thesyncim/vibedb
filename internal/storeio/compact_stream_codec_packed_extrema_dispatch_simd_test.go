//go:build go1.27 && !go1.28 && goexperiment.simd && arm64

package storeio

import (
	"bytes"
	"strings"
	"testing"
)

func TestCountCompactPackedExtremaDispatchNEON(t *testing.T) {
	for width, fn := range map[int]func([]byte, int) (uint64, uint64, bool, bool){
		7: countCompactPacked7ExtremaImpl, 8: countCompactPacked8ExtremaImpl,
		10: countCompactPacked10ExtremaImpl, 16: countCompactPacked16ExtremaImpl,
	} {
		name := compactPackedExtremaDispatchName(fn)
		if !strings.HasSuffix(name, "ExtremaNEON") {
			t.Fatalf("width=%d dispatch=%q, want NEON", width, name)
		}
	}
}

func TestCountCompactPackedExtremaNEONDirectParity(t *testing.T) {
	candidates := []struct {
		width int
		fn    func([]byte, int) (uint64, uint64, bool, bool)
	}{
		{width: 7, fn: countCompactPacked7ExtremaNEON},
		{width: 8, fn: countCompactPacked8ExtremaNEON},
		{width: 10, fn: countCompactPacked10ExtremaNEON},
		{width: 16, fn: countCompactPacked16ExtremaNEON},
	}
	for _, candidate := range candidates {
		for _, count := range []int{32, 33, 64, 65, 4095, 4096, 4097} {
			for _, pattern := range []int{
				compactPackedPatternZero,
				compactPackedPatternMax,
				compactPackedPatternRandom,
			} {
				packed := compactPackedEqualPatternData(count, candidate.width, pattern)
				for _, offset := range []int{0, 1, 17, 31} {
					backing := bytes.Repeat([]byte{0xa5}, offset+len(packed)+16)
					copy(backing[offset:], packed)
					input := backing[offset : offset+len(packed) : offset+len(packed)]
					wantMin, wantMax, wantFound := compactPackedExtremaExpected(input, count, candidate.width)
					gotMin, gotMax, gotFound, supported := candidate.fn(input, count)
					if !supported || gotMin != wantMin || gotMax != wantMax || gotFound != wantFound {
						t.Fatalf("width=%d count=%d pattern=%d offset=%d got=(%d,%d,%t,%t) want=(%d,%d,%t,true)", candidate.width, count, pattern, offset, gotMin, gotMax, gotFound, supported, wantMin, wantMax, wantFound)
					}
					if !bytes.Equal(input, packed) {
						t.Fatalf("width=%d count=%d pattern=%d offset=%d mutated input", candidate.width, count, pattern, offset)
					}
				}
			}
		}
	}
}
