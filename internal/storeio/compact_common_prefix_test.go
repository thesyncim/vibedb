package storeio

import (
	"fmt"
	"testing"
)

func TestCompactCommonPrefixBoundaries(t *testing.T) {
	var left, right [288]byte
	for i := range left {
		left[i] = byte(i*37 + 19)
	}
	for offset := 0; offset < 8; offset++ {
		for length := 0; length <= 257; length++ {
			a := left[offset : offset+length]
			b := right[7-offset : 7-offset+length]
			copy(b, a)
			if got := compactCommonPrefix(a, b); got != length {
				t.Fatalf("equal offset=%d length=%d: %d", offset, length, got)
			}
			for mismatch := range length {
				for _, bit := range []byte{1, 0x80} {
					b[mismatch] ^= bit
					if got := compactCommonPrefix(a, b); got != mismatch {
						t.Fatalf("offset=%d length=%d mismatch=%d bit=%x: %d", offset, length, mismatch, bit, got)
					}
					b[mismatch] ^= bit
				}
				if compactCommonPrefix(a, b[:mismatch]) != mismatch || compactCommonPrefix(a[:mismatch], b) != mismatch {
					t.Fatal("unequal length prefix crossed its bound")
				}
			}
		}
	}
}

var compactPrefixBenchmarkSink int

func BenchmarkCompactCommonPrefix(b *testing.B) {
	for _, size := range []int{12, 32, 256, 4096} {
		for _, mismatch := range []int{0, size / 2, size} {
			b.Run(fmt.Sprintf("bytes=%d/prefix=%d", size, mismatch), func(b *testing.B) {
				left, right := make([]byte, size), make([]byte, size)
				if mismatch < size {
					right[mismatch] = 1
				}
				b.ReportAllocs()
				b.SetBytes(int64(mismatch + 1))
				b.ResetTimer()
				var sum int
				for i := 0; i < b.N; i++ {
					sum += compactCommonPrefix(left, right)
				}
				compactPrefixBenchmarkSink = sum
			})
		}
	}
}
