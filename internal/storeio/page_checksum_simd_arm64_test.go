//go:build go1.27 && !go1.28 && goexperiment.simd && arm64 && darwin

package storeio

import (
	"hash/crc32"
	"simd/archsimd"
	"testing"
)

func TestPageChecksumPMULLMatchesStandardLibrary(t *testing.T) {
	if !archsimd.ARM64.PMULL() {
		t.Skip("PMULL unavailable")
	}
	input := make([]byte, 64*1024+143)
	for i := range input {
		input[i] = byte(i*131 + i>>4 + 17)
	}
	for _, size := range []int{256, 257, 511, 1023} {
		if got, want := pageChecksumPMULL4(input[:size]), crc32.Checksum(input[:size], pageChecksumTable); got != want {
			t.Fatalf("four-stream size %d = %08x, want %08x", size, got, want)
		}
	}
	for _, size := range []int{1024, 1025, 4096, 4096 + 63, 64 * 1024, 64*1024 + 143} {
		if got, want := pageChecksumPMULL9(input[:size]), crc32.Checksum(input[:size], pageChecksumTable); got != want {
			t.Fatalf("nine-stream size %d = %08x, want %08x", size, got, want)
		}
	}
}
