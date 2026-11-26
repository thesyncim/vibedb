//go:build go1.27 && !go1.28 && goexperiment.simd && amd64

package storeio

import (
	"math/bits"
	"simd/archsimd"
)

var compactCommonPrefixUseAVX2 = archsimd.X86.AVX2()

func compactCommonPrefixEqualHead(a, b []byte) int {
	n := min(len(a), len(b))
	if !compactCommonPrefixUseAVX2 || n < 32 {
		return compactCommonPrefixEqualHeadScalar(a, b)
	}
	return compactCommonPrefixEqualHeadAVX2(a, b, n)
}

func compactCommonPrefixEqualHeadAVX2(a, b []byte, n int) int {
	i := 0
	for n-i >= 64 {
		eq0 := archsimd.LoadUint8x32Array((*[32]uint8)(a[i:])).Equal(archsimd.LoadUint8x32Array((*[32]uint8)(b[i:])))
		eq1 := archsimd.LoadUint8x32Array((*[32]uint8)(a[i+32:])).Equal(archsimd.LoadUint8x32Array((*[32]uint8)(b[i+32:])))
		if eq0.And(eq1).ToBits() != ^uint32(0) {
			if m0 := ^eq0.ToBits(); m0 != 0 {
				return i + bits.TrailingZeros32(m0)
			}
			return i + 32 + bits.TrailingZeros32(^eq1.ToBits())
		}
		i += 64
	}
	for n-i >= 32 {
		eq := archsimd.LoadUint8x32Array((*[32]uint8)(a[i:])).Equal(archsimd.LoadUint8x32Array((*[32]uint8)(b[i:])))
		if m := ^eq.ToBits(); m != 0 {
			return i + bits.TrailingZeros32(m)
		}
		i += 32
	}
	if i < n {
		return i + compactCommonPrefixEqualHeadScalar(a[i:n], b[i:n])
	}
	return n
}
