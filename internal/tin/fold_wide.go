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
	// Walk a raw pointer: slice indexing would bounds-check every vector.
	ptr := unsafe.StringData(src)
	i := 0
	for ; i+16 <= len(src); i += 16 {
		// The window borrows the string body without copying or allocating
		// and never escapes the load.
		v := archsimd.LoadUint8x16(unsafe.Slice(ptr, 16))
		ptr = (*byte)(unsafe.Add(unsafe.Pointer(ptr), 16))
		upper := v.GreaterEqual(foldWideA).And(v.LessEqual(foldWideZ))
		folded := v.Add(foldWideDelta.Masked(upper))
		folded.Store(dst[i : i+16])
	}
	for ; i < len(src); i++ {
		dst[i] = foldByte(src[i])
	}
}
