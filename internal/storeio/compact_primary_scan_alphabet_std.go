//go:build !go1.27 || go1.28 || !goexperiment.simd || !arm64 || !darwin

package storeio

func compactScanAlphabetSIMD(dst, data []byte, bit, width, cardinality int, base byte) (int, bool) {
	return 0, true
}
