//go:build go1.27 && !go1.28 && goexperiment.simd && amd64

package bitset

import (
	"simd/archsimd"
	"unsafe"
)

// The vector bodies are deliberately textual and process two independent
// 256-bit vectors per loop: no helper call can force live vectors through the
// stack ABI, and array-pointer loads remove loop bounds checks. The scalar
// tails are shared only after every vector is stored.

// The dispatch wrappers inline these compact loops below the first complete
// two-vector block. Keeping them separate from the unrolled scalar fallback
// avoids a second call on tiny bitmaps without bloating every wrapper.
func andWordsSmall(dst, a, b []uint64) {
	a = a[:len(dst)]
	b = b[:len(dst)]
	for i := range dst {
		dst[i] = a[i] & b[i]
	}
}

func and3WordsSmall(dst, a, b, c []uint64) {
	a = a[:len(dst)]
	b = b[:len(dst)]
	c = c[:len(dst)]
	for i := range dst {
		dst[i] = a[i] & b[i] & c[i]
	}
}

func orWordsSmall(dst, a, b []uint64) {
	a = a[:len(dst)]
	b = b[:len(dst)]
	for i := range dst {
		dst[i] = a[i] | b[i]
	}
}

func andNotWordsSmall(dst, a, b []uint64) {
	a = a[:len(dst)]
	b = b[:len(dst)]
	for i := range dst {
		dst[i] = a[i] &^ b[i]
	}
}

func andWordsAVX2(dst, a, b []uint64) {
	i := 0
	dp := unsafe.Pointer(unsafe.SliceData(dst))
	ap := unsafe.Pointer(unsafe.SliceData(a))
	bp := unsafe.Pointer(unsafe.SliceData(b))
	for ; i+8 <= len(dst); i += 8 {
		off := uintptr(i) * 8
		a0 := archsimd.LoadUint64x4Array((*[4]uint64)(unsafe.Add(ap, off+0)))
		a1 := archsimd.LoadUint64x4Array((*[4]uint64)(unsafe.Add(ap, off+32)))
		b0 := archsimd.LoadUint64x4Array((*[4]uint64)(unsafe.Add(bp, off+0)))
		b1 := archsimd.LoadUint64x4Array((*[4]uint64)(unsafe.Add(bp, off+32)))
		a0.And(b0).StoreArray((*[4]uint64)(unsafe.Add(dp, off+0)))
		a1.And(b1).StoreArray((*[4]uint64)(unsafe.Add(dp, off+32)))
	}
	archsimd.ClearAVXUpperBits()
	for ; i < len(dst); i++ {
		dst[i] = a[i] & b[i]
	}
}

func and3WordsAVX2(dst, a, b, c []uint64) {
	i := 0
	dp := unsafe.Pointer(unsafe.SliceData(dst))
	ap := unsafe.Pointer(unsafe.SliceData(a))
	bp := unsafe.Pointer(unsafe.SliceData(b))
	cp := unsafe.Pointer(unsafe.SliceData(c))
	for ; i+8 <= len(dst); i += 8 {
		off := uintptr(i) * 8
		a0 := archsimd.LoadUint64x4Array((*[4]uint64)(unsafe.Add(ap, off+0)))
		a1 := archsimd.LoadUint64x4Array((*[4]uint64)(unsafe.Add(ap, off+32)))
		b0 := archsimd.LoadUint64x4Array((*[4]uint64)(unsafe.Add(bp, off+0)))
		b1 := archsimd.LoadUint64x4Array((*[4]uint64)(unsafe.Add(bp, off+32)))
		c0 := archsimd.LoadUint64x4Array((*[4]uint64)(unsafe.Add(cp, off+0)))
		c1 := archsimd.LoadUint64x4Array((*[4]uint64)(unsafe.Add(cp, off+32)))
		a0.And(b0).And(c0).StoreArray((*[4]uint64)(unsafe.Add(dp, off+0)))
		a1.And(b1).And(c1).StoreArray((*[4]uint64)(unsafe.Add(dp, off+32)))
	}
	archsimd.ClearAVXUpperBits()
	for ; i < len(dst); i++ {
		dst[i] = a[i] & b[i] & c[i]
	}
}

func orWordsAVX2(dst, a, b []uint64) {
	i := 0
	dp := unsafe.Pointer(unsafe.SliceData(dst))
	ap := unsafe.Pointer(unsafe.SliceData(a))
	bp := unsafe.Pointer(unsafe.SliceData(b))
	for ; i+8 <= len(dst); i += 8 {
		off := uintptr(i) * 8
		a0 := archsimd.LoadUint64x4Array((*[4]uint64)(unsafe.Add(ap, off+0)))
		a1 := archsimd.LoadUint64x4Array((*[4]uint64)(unsafe.Add(ap, off+32)))
		b0 := archsimd.LoadUint64x4Array((*[4]uint64)(unsafe.Add(bp, off+0)))
		b1 := archsimd.LoadUint64x4Array((*[4]uint64)(unsafe.Add(bp, off+32)))
		a0.Or(b0).StoreArray((*[4]uint64)(unsafe.Add(dp, off+0)))
		a1.Or(b1).StoreArray((*[4]uint64)(unsafe.Add(dp, off+32)))
	}
	archsimd.ClearAVXUpperBits()
	for ; i < len(dst); i++ {
		dst[i] = a[i] | b[i]
	}
}

func andNotWordsAVX2(dst, a, b []uint64) {
	i := 0
	dp := unsafe.Pointer(unsafe.SliceData(dst))
	ap := unsafe.Pointer(unsafe.SliceData(a))
	bp := unsafe.Pointer(unsafe.SliceData(b))
	for ; i+8 <= len(dst); i += 8 {
		off := uintptr(i) * 8
		a0 := archsimd.LoadUint64x4Array((*[4]uint64)(unsafe.Add(ap, off+0)))
		a1 := archsimd.LoadUint64x4Array((*[4]uint64)(unsafe.Add(ap, off+32)))
		b0 := archsimd.LoadUint64x4Array((*[4]uint64)(unsafe.Add(bp, off+0)))
		b1 := archsimd.LoadUint64x4Array((*[4]uint64)(unsafe.Add(bp, off+32)))
		a0.AndNot(b0).StoreArray((*[4]uint64)(unsafe.Add(dp, off+0)))
		a1.AndNot(b1).StoreArray((*[4]uint64)(unsafe.Add(dp, off+32)))
	}
	archsimd.ClearAVXUpperBits()
	for ; i < len(dst); i++ {
		dst[i] = a[i] &^ b[i]
	}
}
