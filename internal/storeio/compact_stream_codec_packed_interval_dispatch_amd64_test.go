//go:build go1.27 && !go1.28 && goexperiment.simd && amd64

package storeio

import (
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"simd/archsimd"
)

func TestCountCompactPackedBetweenDispatchAVX2(t *testing.T) {
	wantAVX2 := archsimd.X86.AVX2()
	t.Logf("AVX2=%t GODEBUG=%q", wantAVX2, os.Getenv("GODEBUG"))
	if os.Getenv("GODEBUG") == "cpu.avx2=off" && wantAVX2 {
		t.Fatal("AVX2 remained enabled with GODEBUG=cpu.avx2=off")
	}
	if os.Getenv("VIBEDB_TEST_REQUIRE_AVX2") == "1" && !wantAVX2 {
		t.Fatal("native AVX2 qualification requires AVX2 to be enabled")
	}
	for width, fn := range map[int]func([]byte, int, uint64, uint64) int{
		7:  countCompactPacked7BetweenImpl,
		8:  countCompactPacked8BetweenImpl,
		10: countCompactPacked10BetweenImpl,
		16: countCompactPacked16BetweenImpl,
	} {
		name := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
		if wantAVX2 {
			if !strings.HasSuffix(name, "BetweenAVX2") {
				t.Fatalf("width=%d AVX2 dispatch=%q, want packed AVX2", width, name)
			}
		} else if !strings.HasSuffix(name, "BetweenScalar") {
			t.Fatalf("width=%d AVX2-disabled dispatch=%q, want packed scalar", width, name)
		}
	}
}

func TestCountCompactPackedBetweenAVX2DirectParity(t *testing.T) {
	if !archsimd.X86.AVX2() {
		t.Skip("AVX2 is unavailable or disabled")
	}
	candidates := []struct {
		width int
		fn    func([]byte, int, uint64, uint64) int
	}{
		{width: 7, fn: countCompactPacked7BetweenAVX2},
		{width: 8, fn: countCompactPacked8BetweenAVX2},
		{width: 10, fn: countCompactPacked10BetweenAVX2},
		{width: 16, fn: countCompactPacked16BetweenAVX2},
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
				for _, interval := range [][2]uint64{
					{0, 1}, {1, 2}, {mask / 3, mask/3 + 1},
					{mask / 2, mask}, {0, mask + 1},
				} {
					expected := compactPackedBetweenExpected(
						input, count, candidate.width, interval[0], interval[1],
					)
					if got := candidate.fn(
						input, count, interval[0], interval[1],
					); got != expected {
						t.Fatalf(
							"width=%d count=%d offset=%d interval=[%d,%d) got=%d expected=%d",
							candidate.width, count, offset, interval[0], interval[1], got, expected,
						)
					}
				}
				for at := 0; at < offset; at++ {
					if backing[at] != 0xa5 {
						t.Fatalf("width=%d count=%d offset=%d prefix canary at %d", candidate.width, count, offset, at)
					}
				}
				for at := offset + len(packed); at < len(backing); at++ {
					if backing[at] != 0xa5 {
						t.Fatalf("width=%d count=%d offset=%d suffix canary at %d", candidate.width, count, offset, at)
					}
				}
			}
		}
	}
}
