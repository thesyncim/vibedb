//go:build go1.27 && !go1.28 && goexperiment.simd && arm64 && darwin

package storeio

import (
	"bytes"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func TestCompactAlphabetSIMDDispatchBoundsAndParity(t *testing.T) {
	pc := reflect.ValueOf(compactScanAlphabetSIMD).Pointer()
	file, _ := runtime.FuncForPC(pc).FileLine(pc)
	if !strings.HasSuffix(file, "compact_primary_scan_alphabet_arm64.go") {
		t.Fatalf("SIMD build dispatched %s", file)
	}
	for width := 1; width <= 6; width++ {
		for _, cardinality := range []int{1 << width, 1<<width - 1} {
			for bit := range 8 {
				for _, size := range []int{0, 1, 7, 15, 16, 17, 31, 32, 33, 47, 48, 63, 64, 79, 80, 97, 127} {
					data := make([]byte, (bit+size*width+7)/8)
					for i := range size {
						compactPutBits(data, bit+i*width, width, uint64((i*17+i/3)%cardinality))
					}
					for _, base := range []byte{0, byte(256 - cardinality)} {
						backing := bytes.Repeat([]byte{0xa5}, size+32)
						n, ok := compactScanAlphabetSIMD(backing[7:7+size], data, bit, width, cardinality, base)
						if !ok || n%16 != 0 || n > size {
							t.Fatalf("width=%d bit=%d size=%d decoded=%d,%v", width, bit, size, n, ok)
						}
						if size >= 32 && len(data) >= 16 && n == 0 {
							t.Fatal("SIMD path did not execute")
						}
						for i := range n {
							want := base + byte(compactReadBits(data, bit+i*width, width))
							if backing[7+i] != want {
								t.Fatalf("width=%d bit=%d size=%d char=%d got=%d want=%d", width, bit, size, i, backing[7+i], want)
							}
						}
						if !bytes.Equal(backing[:7], bytes.Repeat([]byte{0xa5}, 7)) || !bytes.Equal(backing[7+n:], bytes.Repeat([]byte{0xa5}, len(backing)-7-n)) {
							t.Fatal("SIMD wrote beyond the decoded range")
						}
					}
				}
			}
		}
	}
}

func TestCompactAlphabetSIMDInvalidCodesAndAllocations(t *testing.T) {
	var data [64]byte
	var dst [64]byte
	for _, char := range []int{0, 15, 16, 31, 32, 47, 63} {
		clear(data[:])
		compactPutBits(data[:], 3+char*5, 5, 31)
		if _, ok := compactScanAlphabetSIMD(dst[:], data[:], 3, 5, 26, 'a'); ok {
			t.Fatalf("accepted invalid code at %d", char)
		}
	}
	clear(data[:])
	if allocs := testing.AllocsPerRun(1000, func() {
		if n, ok := compactScanAlphabetSIMD(dst[:], data[:], 3, 5, 26, 'a'); !ok || n != len(dst) {
			panic("SIMD dispatch")
		}
	}); allocs != 0 {
		t.Fatalf("SIMD allocations=%v", allocs)
	}
}
