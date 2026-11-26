//go:build go1.27 && !go1.28 && goexperiment.simd && arm64

package storeio

import (
	"math/bits"
	"simd/archsimd"
)

func compactCommonPrefixEqualHead(a, b []byte) int {
	n := min(len(a), len(b))
	if n < 32 {
		return compactCommonPrefixEqualHeadScalar(a, b)
	}
	return compactCommonPrefixEqualHeadNEON(a, b, n)
}

func compactCommonPrefixEqualHeadNEON(a, b []byte, n int) int {
	i := 0
	for n-i >= 64 {
		d0 := archsimd.LoadUint8x16Array((*[16]uint8)(a[i:])).Xor(archsimd.LoadUint8x16Array((*[16]uint8)(b[i:])))
		d1 := archsimd.LoadUint8x16Array((*[16]uint8)(a[i+16:])).Xor(archsimd.LoadUint8x16Array((*[16]uint8)(b[i+16:])))
		d2 := archsimd.LoadUint8x16Array((*[16]uint8)(a[i+32:])).Xor(archsimd.LoadUint8x16Array((*[16]uint8)(b[i+32:])))
		d3 := archsimd.LoadUint8x16Array((*[16]uint8)(a[i+48:])).Xor(archsimd.LoadUint8x16Array((*[16]uint8)(b[i+48:])))
		if d0.Or(d1).Or(d2.Or(d3)).ReduceMax() != 0 {
			if off := firstDiffByte16NEON(d0); off >= 0 {
				return i + off
			}
			if off := firstDiffByte16NEON(d1); off >= 0 {
				return i + 16 + off
			}
			if off := firstDiffByte16NEON(d2); off >= 0 {
				return i + 32 + off
			}
			return i + 48 + firstDiffByte16NEON(d3)
		}
		i += 64
	}
	for n-i >= 32 {
		d0 := archsimd.LoadUint8x16Array((*[16]uint8)(a[i:])).Xor(archsimd.LoadUint8x16Array((*[16]uint8)(b[i:])))
		d1 := archsimd.LoadUint8x16Array((*[16]uint8)(a[i+16:])).Xor(archsimd.LoadUint8x16Array((*[16]uint8)(b[i+16:])))
		if d0.Or(d1).ReduceMax() != 0 {
			if off := firstDiffByte16NEON(d0); off >= 0 {
				return i + off
			}
			return i + 16 + firstDiffByte16NEON(d1)
		}
		i += 32
	}
	if i < n {
		return i + compactCommonPrefixEqualHeadScalar(a[i:n], b[i:n])
	}
	return n
}

func firstDiffByte16NEON(diff archsimd.Uint8x16) int {
	w := diff.ReshapeToUint64s()
	if lo := w.GetElem(0); lo != 0 {
		return bits.TrailingZeros64(lo) / 8
	}
	if hi := w.GetElem(1); hi != 0 {
		return 8 + bits.TrailingZeros64(hi)/8
	}
	return -1
}
