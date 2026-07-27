//go:build go1.27 && !go1.28 && goexperiment.simd && amd64

package storeio

import (
	"hash/crc32"
	"simd/archsimd"
	"testing"

	xsyscpu "golang.org/x/sys/cpu"
)

func TestPageChecksumPCLMUL8MatchesStandardLibrary(t *testing.T) {
	if !archsimd.X86.AVX() || !xsyscpu.X86.HasPCLMULQDQ {
		t.Skip("AVX plus PCLMULQDQ unavailable")
	}
	table := crc32.MakeTable(crc32.Castagnoli)
	data := make([]byte, (128<<10)+31)
	for i := range data {
		data[i] = byte(i*197 + i>>5)
	}
	for alignment := 0; alignment < 16; alignment++ {
		for _, size := range []int{128, 129, 255, 256, 257, 1023, 1024, 4095, 4096, 4097, 64 << 10, 128 << 10} {
			input := data[alignment : alignment+size]
			if got, want := pageChecksumPCLMUL8(input), crc32.Checksum(input, table); got != want {
				t.Fatalf("alignment=%d size=%d: checksum=%08x, want %08x", alignment, size, got, want)
			}
		}
	}
}

func TestPageChecksumAVX512MatchesStandardLibrary(t *testing.T) {
	if !archsimd.X86.AVX512() || !archsimd.X86.AVX512VPCLMULQDQ() {
		t.Skip("AVX-512 plus VPCLMULQDQ unavailable")
	}
	table := crc32.MakeTable(crc32.Castagnoli)
	data := make([]byte, (128<<10)+63)
	for i := range data {
		data[i] = byte(i*193 + i>>3)
	}
	for alignment := 0; alignment < 64; alignment++ {
		for _, size := range []int{256, 257, 511, 512, 1023, 1024, 4095, 4096, 4097, 64 << 10, 128 << 10} {
			input := data[alignment : alignment+size]
			if got, want := pageChecksumAVX512(input), crc32.Checksum(input, table); got != want {
				t.Fatalf("alignment=%d size=%d: checksum=%08x, want %08x", alignment, size, got, want)
			}
		}
	}
}
