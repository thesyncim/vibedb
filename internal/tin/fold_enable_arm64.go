//go:build go1.27 && !go1.28 && goexperiment.simd && arm64

package tin

// NEON byte and float ops are baseline arm64, so the wide kernels need no
// runtime gate.
func init() {
	foldASCIIImpl = foldASCIIWide
	foldASCIIBytesImpl = foldASCIIWideBytes
	bm25Impl = bm25Wide
}
