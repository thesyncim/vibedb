//go:build go1.27 && !go1.28 && goexperiment.simd && amd64 && !amd64.v3

package bitset

import (
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

// TestAVX2DisabledFallback starts a clean process because internal/cpu resolves
// GODEBUG before package initialization. This proves a v1/v2 binary neither
// advertises nor executes the AVX2 body when the capability is unavailable.
func TestAVX2DisabledFallback(t *testing.T) {
	if os.Getenv("VIBEJSON_BITSET_AVX2_OFF_CHILD") == "1" {
		if Accelerated() {
			t.Fatal("AVX2 bitmap backend selected with cpu.avx2=off")
		}
		a := []uint64{1, 3, 7, 15, 31, 63, 127, 255, 511}
		b := []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8}
		if got, want := And(nil, a, b), refAnd(nil, a, b); !slices.Equal(got, want) {
			t.Fatalf("portable fallback = %v, want %v", got, want)
		}
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestAVX2DisabledFallback$")
	env := make([]string, 0, len(os.Environ())+2)
	for _, item := range os.Environ() {
		if !strings.HasPrefix(item, "GODEBUG=") &&
			!strings.HasPrefix(item, "VIBEJSON_BITSET_AVX2_OFF_CHILD=") {
			env = append(env, item)
		}
	}
	cmd.Env = append(env, "GODEBUG=cpu.avx2=off", "VIBEJSON_BITSET_AVX2_OFF_CHILD=1")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("AVX2-disabled child: %v\n%s", err, output)
	}
}
