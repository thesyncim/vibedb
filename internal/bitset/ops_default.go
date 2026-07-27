//go:build !go1.27 || go1.28 || !goexperiment.simd || !amd64

package bitset

// Accelerated reports whether this build selected the SIMD word kernels.
func Accelerated() bool               { return false }
func andWords(dst, a, b []uint64)     { andWordsScalar(dst, a, b) }
func and3Words(dst, a, b, c []uint64) { and3WordsScalar(dst, a, b, c) }
func orWords(dst, a, b []uint64)      { orWordsScalar(dst, a, b) }
func andNotWords(dst, a, b []uint64)  { andNotWordsScalar(dst, a, b) }
