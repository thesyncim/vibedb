//go:build go1.27 && !go1.28 && goexperiment.simd && amd64

package storeio

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"simd/archsimd"
)

func TestCountCompactPackedExtremaDispatchAVX2(t *testing.T) {
	wantAVX2 := archsimd.X86.AVX2()
	t.Logf("AVX2=%t GODEBUG=%q", wantAVX2, os.Getenv("GODEBUG"))
	if os.Getenv("GODEBUG") == "cpu.avx2=off" && wantAVX2 {
		t.Fatal("AVX2 remained enabled with GODEBUG=cpu.avx2=off")
	}
	if os.Getenv("VIBEDB_TEST_REQUIRE_AVX2") == "1" && !wantAVX2 {
		t.Fatal("native AVX2 qualification requires AVX2 to be enabled")
	}
	for width, fn := range map[int]func([]byte, int) (uint64, uint64, bool, bool){
		7: countCompactPacked7ExtremaImpl, 8: countCompactPacked8ExtremaImpl,
		10: countCompactPacked10ExtremaImpl, 16: countCompactPacked16ExtremaImpl,
	} {
		name := compactPackedExtremaDispatchName(fn)
		if name == "" {
			t.Fatalf("width=%d missing dispatch function name", width)
		}
		if wantAVX2 {
			if !strings.HasSuffix(name, "ExtremaAVX2") {
				t.Fatalf("width=%d dispatch=%q, want AVX2", width, name)
			}
		} else if !strings.HasSuffix(name, "ExtremaScalar") {
			t.Fatalf("width=%d dispatch=%q, want scalar with AVX2 disabled", width, name)
		}
	}
}

func TestCountCompactPackedExtremaAVX2DirectParity(t *testing.T) {
	if !archsimd.X86.AVX2() {
		t.Skip("AVX2 is unavailable or disabled")
	}
	candidates := []struct {
		width int
		fn    func([]byte, int) (uint64, uint64, bool, bool)
	}{
		{width: 7, fn: countCompactPacked7ExtremaAVX2},
		{width: 8, fn: countCompactPacked8ExtremaAVX2},
		{width: 10, fn: countCompactPacked10ExtremaAVX2},
		{width: 16, fn: countCompactPacked16ExtremaAVX2},
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
