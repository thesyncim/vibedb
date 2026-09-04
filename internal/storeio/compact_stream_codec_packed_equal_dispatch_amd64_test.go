//go:build go1.27 && !go1.28 && goexperiment.simd && amd64

package storeio

import (
	"os"
	"strings"
	"testing"

	"simd/archsimd"
)

func TestCountCompactPackedEqualDispatchAVX2(t *testing.T) {
	wantAVX2 := archsimd.X86.AVX2()
	t.Logf("AVX2=%t GODEBUG=%q", wantAVX2, os.Getenv("GODEBUG"))
	// CI uses this exact runtime override with GOAMD64=v1. Check the feature
	// bit itself so an ignored override cannot masquerade as fallback parity.
	if os.Getenv("GODEBUG") == "cpu.avx2=off" && wantAVX2 {
		t.Fatal("AVX2 remained enabled with GODEBUG=cpu.avx2=off")
	}
	// Native benchmark runners must execute the candidates before timing;
	// ordinary tests still support machines where AVX2 is unavailable.
	if os.Getenv("VIBEDB_TEST_REQUIRE_AVX2") == "1" && !wantAVX2 {
		t.Fatal("native AVX2 qualification requires AVX2 to be enabled")
	}
	for _, name := range []string{
		compactPackedEqualDispatchName(countCompactPacked7EqualImpl),
		compactPackedEqualDispatchName(countCompactPacked8EqualImpl),
		compactPackedEqualDispatchName(countCompactPacked10EqualImpl),
		compactPackedEqualDispatchName(countCompactPacked16EqualImpl),
		compactPackedEqualDispatchName(countCompactPacked7LessImpl),
		compactPackedEqualDispatchName(countCompactPacked8LessImpl),
		compactPackedEqualDispatchName(countCompactPacked10LessImpl),
		compactPackedEqualDispatchName(countCompactPacked16LessImpl),
	} {
		if name == "" {
			t.Fatal("missing amd64 packed dispatch function name")
		}
		if wantAVX2 {
			if !strings.HasSuffix(name, "EqualAVX2") && !strings.HasSuffix(name, "LessAVX2") {
				t.Fatalf("AVX2 dispatch=%q, want packed AVX2", name)
			}
		} else if !strings.HasSuffix(name, "EqualScalar") && !strings.HasSuffix(name, "LessScalar") {
			t.Fatalf("AVX2-disabled dispatch=%q, want packed scalar", name)
		}
	}
}

func TestCountCompactPackedLessAVX2DirectParity(t *testing.T) {
	if !archsimd.X86.AVX2() {
		t.Skip("AVX2 is unavailable or disabled")
	}
	candidates := []struct {
		width int
		fn    func([]byte, int, uint64) int
	}{
		{width: 7, fn: countCompactPacked7LessAVX2},
		{width: 8, fn: countCompactPacked8LessAVX2},
		{width: 10, fn: countCompactPacked10LessAVX2},
		{width: 16, fn: countCompactPacked16LessAVX2},
	}
	for _, candidate := range candidates {
		mask := uint64(1)<<uint(candidate.width) - 1
		for _, count := range []int{32, 64, 65, 4095, 4096, 4097} {
			packed := compactPackedEqualPatternData(
				count, candidate.width, compactPackedPatternRandom,
			)
			for _, offset := range []int{0, 17, 31} {
				backing := make([]byte, offset+len(packed)+16)
				for at := range backing {
					backing[at] = 0xa5
				}
				copy(backing[offset:], packed)
				input := backing[offset : offset+len(packed) : offset+len(packed)]
				for _, threshold := range []uint64{0, 1, mask / 3, mask, mask + 1, ^uint64(0)} {
					expected := compactPackedLessExpected(input, count, candidate.width, threshold)
					if got := candidate.fn(input, count, threshold); got != expected {
						t.Fatalf("width=%d count=%d offset=%d threshold=%d got=%d expected=%d", candidate.width, count, offset, threshold, got, expected)
					}
				}
			}
		}
	}
}

func TestCountCompactPackedEqualAVX2DirectParity(t *testing.T) {
	if !archsimd.X86.AVX2() {
		t.Skip("AVX2 is unavailable or disabled")
	}
	candidates := []struct {
		width int
		fn    func([]byte, int, uint64) int
	}{
		{width: 7, fn: countCompactPacked7EqualAVX2},
		{width: 8, fn: countCompactPacked8EqualAVX2},
		{width: 10, fn: countCompactPacked10EqualAVX2},
		{width: 16, fn: countCompactPacked16EqualAVX2},
	}
	counts := []int{32, 64, 65, 4095, 4096, 4097}
	patterns := []int{
		compactPackedPatternZero,
		compactPackedPatternAlternating,
		compactPackedPatternRandom,
	}
	for _, candidate := range candidates {
		mask := uint64(1)<<uint(candidate.width) - 1
		for _, count := range counts {
			for _, pattern := range patterns {
				packed := compactPackedEqualPatternData(count, candidate.width, pattern)
				for _, offset := range []int{0, 17, 31} {
					backing := make([]byte, offset+len(packed)+16)
					for at := range backing {
						backing[at] = 0xa5
					}
					copy(backing[offset:], packed)
					input := backing[offset : offset+len(packed) : offset+len(packed)]
					for _, want := range []uint64{0, 1, mask, mask + 1, ^uint64(0)} {
						expected := compactPackedEqualExpected(input, count, candidate.width, want)
						if got := candidate.fn(input, count, want); got != expected {
							t.Fatalf("width=%d count=%d pattern=%d offset=%d want=%d got=%d expected=%d", candidate.width, count, pattern, offset, want, got, expected)
						}
					}
				}
			}
		}
	}
}
