//go:build go1.27 && !go1.28 && goexperiment.simd && amd64

package tin

import "simd/archsimd"

// The wide fold is selected only behind the AVX2 feature bit, mirroring
// storeio: a binary built with GOAMD64=v1 stays safe on older machines.
func init() {
	if !archsimd.X86.AVX2() {
		return
	}
	foldASCIIImpl = foldASCIIWide
}
