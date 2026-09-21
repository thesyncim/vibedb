//go:build go1.27 && !go1.28 && goexperiment.simd && arm64

package tin

// NEON byte ops are baseline arm64, so the wide fold needs no runtime gate.
func init() {
	foldASCIIImpl = foldASCIIWide
}
