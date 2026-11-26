//go:build go1.27 && !go1.28 && goexperiment.simd && arm64

package storeio

import "simd/archsimd"

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
	if count < 32 {
		return compactPacked4MaxBelowScalar(data, count, limit)
	}
	mask := archsimd.BroadcastUint8x16(0x0f)
	var maxVec archsimd.Uint8x16
	row := 0
	rem := data
	for ; row+32 <= count && len(rem) >= 16; row += 32 {
		v := archsimd.LoadUint8x16Array((*[16]uint8)(rem))
		maxVec = maxVec.Max(v.And(mask)).Max(v.ShiftAllRight(4))
		rem = rem[16:]
	}
	if int(maxVec.ReduceMax()) >= limit {
		return false
	}
	if row < count {
		return compactPacked4MaxBelowScalar(rem, count-row, limit)
	}
	return true
}

func compactUnpackPacked4x32SIMD(dst *[32]uint8, src *[16]uint8) {
	v := archsimd.LoadUint8x16Array(src)
	lo := v.And(archsimd.BroadcastUint8x16(0x0f))
	hi := v.ShiftAllRight(4)
	lo.InterleaveLo(hi).StoreArray((*[16]uint8)(dst[:16]))
	lo.InterleaveHi(hi).StoreArray((*[16]uint8)(dst[16:32]))
}

func compactUnpackPacked7x16SIMD(dst *[16]uint16, src *[16]uint8) {
	indices0 := archsimd.LoadUint8x16Array(&compactPacked7NEONIndices0)
	indices1 := archsimd.LoadUint8x16Array(&compactPacked7NEONIndices1)
	shifts := archsimd.LoadInt16x8Array(&compactPacked7NEONShifts)
	mask := archsimd.BroadcastUint16x8(127)
	loaded := archsimd.LoadUint8x16Array(src)
	loaded.LookupOrZero(indices0).ReshapeToUint16s().Shift(shifts).And(mask).StoreArray((*[8]uint16)(dst[:8]))
	loaded.LookupOrZero(indices1).ReshapeToUint16s().Shift(shifts).And(mask).StoreArray((*[8]uint16)(dst[8:16]))
}

func compactUnpackPacked10x32SIMD(dst *[32]uint16, src *[40]uint8) {
	indices := archsimd.LoadUint8x16Array(&compactPacked10NEONIndices)
	indicesOffset6 := archsimd.LoadUint8x16Array(&compactPacked10NEONIndicesOffset6)
	shifts := archsimd.LoadInt16x8Array(&compactPacked10NEONShifts)
	mask := archsimd.BroadcastUint16x8(1023)
	l0 := archsimd.LoadUint8x16Array((*[16]uint8)(src[0:16]))
	l1 := archsimd.LoadUint8x16Array((*[16]uint8)(src[10:26]))
	l2 := archsimd.LoadUint8x16Array((*[16]uint8)(src[20:36]))
	l3 := archsimd.LoadUint8x16Array((*[16]uint8)(src[24:40]))
	l0.LookupOrZero(indices).ReshapeToUint16s().Shift(shifts).And(mask).StoreArray((*[8]uint16)(dst[0:8]))
	l1.LookupOrZero(indices).ReshapeToUint16s().Shift(shifts).And(mask).StoreArray((*[8]uint16)(dst[8:16]))
	l2.LookupOrZero(indices).ReshapeToUint16s().Shift(shifts).And(mask).StoreArray((*[8]uint16)(dst[16:24]))
	l3.LookupOrZero(indicesOffset6).ReshapeToUint16s().Shift(shifts).And(mask).StoreArray((*[8]uint16)(dst[24:32]))
}
