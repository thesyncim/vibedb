//go:build goexperiment.simd && amd64

package query

import "simd/archsimd"

func joinBloomInsertBlock(block *joinBloomBlock, low uint32) {
	if !archsimd.X86.AVX2() {
		joinBloomInsertScalar(block, low)
		return
	}
	h := archsimd.BroadcastUint32x8(low)
	positions := h.Mul(archsimd.LoadUint32x8Array(&joinBloomSalt)).ShiftAllRight(27)
	w := archsimd.BroadcastUint32x8(1).ShiftLeft(positions)
	archsimd.LoadUint32x8Array((*[8]uint32)(block)).Or(w).StoreArray((*[8]uint32)(block))
}
