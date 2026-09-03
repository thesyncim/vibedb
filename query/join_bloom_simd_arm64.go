//go:build goexperiment.simd && arm64

package query

import "simd/archsimd"

// This Go 1.27 toolchain lowers ShiftAllRight through a runtime shift vector.
// Load one shared shift and one-bit vector instead of rebuilding them per half.
var joinBloomNEONOnes = [4]uint32{1, 1, 1, 1}
var joinBloomNEONRight = [4]int32{-27, -27, -27, -27}

// Keep both halves in registers: materializing the signature on the stack
// costs more than the eight-lane arithmetic. NEON is baseline on Go arm64.
func joinBloomInsertBlock(block *joinBloomBlock, low uint32) {
	h := archsimd.BroadcastUint32x4(low)
	one := archsimd.LoadUint32x4Array(&joinBloomNEONOnes)
	right := archsimd.LoadInt32x4Array(&joinBloomNEONRight)
	w0 := one.Shift(h.Mul(archsimd.LoadUint32x4Array((*[4]uint32)(joinBloomSalt[:4]))).Shift(right).BitsToInt32())
	w1 := one.Shift(h.Mul(archsimd.LoadUint32x4Array((*[4]uint32)(joinBloomSalt[4:]))).Shift(right).BitsToInt32())
	b0 := (*[4]uint32)(block[:4])
	b1 := (*[4]uint32)(block[4:])
	archsimd.LoadUint32x4Array(b0).Or(w0).StoreArray(b0)
	archsimd.LoadUint32x4Array(b1).Or(w1).StoreArray(b1)
}
