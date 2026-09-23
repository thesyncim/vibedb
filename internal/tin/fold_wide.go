//go:build go1.27 && !go1.28 && goexperiment.simd && (amd64 || arm64)

package tin

import (
	"simd/archsimd"
	"unsafe"
)

// Pre-broadcast fold constants live in rodata and load with one vector load
// each; they are never rebuilt per iteration.
var (
	foldWideA     = archsimd.BroadcastUint8x16('A')
	foldWideZ     = archsimd.BroadcastUint8x16('Z')
	foldWideDelta = archsimd.BroadcastUint8x16('a' - 'A')
)

// foldASCIIWide folds 16 bytes per iteration with byte-compare, mask, and
// add — all fixed 128-bit ops, so the one spelling serves NEON and AVX2.
// Unsigned comparison keeps bytes >= 0x80 out of the [A,Z] range by value,
// which is exactly the passthrough the rune path requires. The tail stays
// scalar.
func foldASCIIWide(dst []byte, src string) {
	// Each window derives from the base pointer: slice indexing would
	// bounds-check every vector, while carrying the pointer forward trips
	// checkptr under -race. The window borrows the string body without
	// copying or allocating and never escapes the load.
	base := unsafe.Pointer(unsafe.StringData(src))
	i := 0
	for ; i+16 <= len(src); i += 16 {
		v := archsimd.LoadUint8x16(unsafe.Slice((*byte)(unsafe.Add(base, uintptr(i))), 16))
		upper := v.GreaterEqual(foldWideA).And(v.LessEqual(foldWideZ))
		folded := v.Add(foldWideDelta.Masked(upper))
		folded.Store(dst[i : i+16])
	}
	for ; i < len(src); i++ {
		dst[i] = foldByte(src[i])
	}
}

// foldASCIIWideBytes is foldASCIIWide over a caller-owned buffer: same
// 16-byte windows, same tail. The window borrows the slice body the same
// way the string lane borrows the string body.
func foldASCIIWideBytes(dst, src []byte) {
	if len(src) == 0 {
		return
	}
	base := unsafe.Pointer(unsafe.SliceData(src))
	i := 0
	for ; i+16 <= len(src); i += 16 {
		v := archsimd.LoadUint8x16(unsafe.Slice((*byte)(unsafe.Add(base, uintptr(i))), 16))
		upper := v.GreaterEqual(foldWideA).And(v.LessEqual(foldWideZ))
		folded := v.Add(foldWideDelta.Masked(upper))
		folded.Store(dst[i : i+16])
	}
	for ; i < len(src); i++ {
		dst[i] = foldByte(src[i])
	}
}
