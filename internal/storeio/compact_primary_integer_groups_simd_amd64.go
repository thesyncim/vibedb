//go:build go1.27 && !go1.28 && goexperiment.simd && amd64

package storeio

import "simd/archsimd"

var compactIntegerGroupsUseAVX2 = archsimd.X86.AVX2()

func compactPacked4MaxBelowSIMD(data []byte, count, limit int) bool {
	if count <= 0 {
		return true
	}
	if limit <= 0 {
		return false
	}
	if limit >= 16 {
		return true
	}
	if !compactIntegerGroupsUseAVX2 || count < 64 {
		return compactPacked4MaxBelowScalar(data, count, limit)
	}
	mask := archsimd.BroadcastUint8x32(0x0f)
	var maxVec archsimd.Uint8x32
	row := 0
	rem := data
	for ; row+64 <= count && len(rem) >= 32; row += 64 {
		v := archsimd.LoadUint8x32Array((*[32]uint8)(rem))
		lo := v.And(mask)
		hi := v.ReshapeToUint16s().ShiftAllRight(4).ReshapeToUint8s().And(mask)
		maxVec = maxVec.Max(lo).Max(hi)
		rem = rem[32:]
	}
	if int(reduceUint8x32MaxAVX2(maxVec)) >= limit {
		return false
	}
	if row < count {
		return compactPacked4MaxBelowScalar(rem, count-row, limit)
	}
	return true
}

func compactUnpackPacked4x32SIMD(dst *[32]uint8, src *[16]uint8) {
	if !compactIntegerGroupsUseAVX2 {
		compactUnpackPacked4x32Scalar(dst, src)
		return
	}
	w := archsimd.LoadUint8x16Array(src).ExtendToUint16()
	lo := w.And(archsimd.BroadcastUint16x16(0x000f))
	hi := w.And(archsimd.BroadcastUint16x16(0x00f0)).ShiftAllLeft(4)
	lo.Or(hi).ReshapeToUint8s().StoreArray(dst)
}

func compactUnpackPacked7x16SIMD(dst *[16]uint16, src *[16]uint8) {
	if !compactIntegerGroupsUseAVX2 {
		compactUnpackPacked7x16Scalar(dst, src)
		return
	}
	indices0 := archsimd.LoadInt8x16Array(&compactPacked7AVX2Indices0)
	indices1 := archsimd.LoadInt8x16Array(&compactPacked7AVX2Indices1)
	factors := archsimd.LoadUint16x8Array(&compactPacked7AVX2Factors)
	loaded := archsimd.LoadUint8x16Array(src)
	loaded.PermuteOrZero(indices0).ReshapeToUint16s().Mul(factors).ShiftAllRight(9).StoreArray((*[8]uint16)(dst[:8]))
	loaded.PermuteOrZero(indices1).ReshapeToUint16s().Mul(factors).ShiftAllRight(9).StoreArray((*[8]uint16)(dst[8:16]))
}

func compactUnpackPacked10x32SIMD(dst *[32]uint16, src *[40]uint8) {
	if !compactIntegerGroupsUseAVX2 {
		compactUnpackPacked10x32Scalar(dst, src)
		return
	}
	indices := archsimd.LoadInt8x16Array(&compactPacked10AVX2Indices)
	indicesOffset6 := archsimd.LoadInt8x16Array(&compactPacked10AVX2IndicesOffset6)
	factors := archsimd.LoadUint16x8Array(&compactPacked10AVX2Factors)
	l0 := archsimd.LoadUint8x16Array((*[16]uint8)(src[0:16]))
	l1 := archsimd.LoadUint8x16Array((*[16]uint8)(src[10:26]))
	l2 := archsimd.LoadUint8x16Array((*[16]uint8)(src[20:36]))
	l3 := archsimd.LoadUint8x16Array((*[16]uint8)(src[24:40]))
	l0.PermuteOrZero(indices).ReshapeToUint16s().Mul(factors).ShiftAllRight(6).StoreArray((*[8]uint16)(dst[0:8]))
	l1.PermuteOrZero(indices).ReshapeToUint16s().Mul(factors).ShiftAllRight(6).StoreArray((*[8]uint16)(dst[8:16]))
	l2.PermuteOrZero(indices).ReshapeToUint16s().Mul(factors).ShiftAllRight(6).StoreArray((*[8]uint16)(dst[16:24]))
	l3.PermuteOrZero(indicesOffset6).ReshapeToUint16s().Mul(factors).ShiftAllRight(6).StoreArray((*[8]uint16)(dst[24:32]))
}
