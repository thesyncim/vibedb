//go:build go1.27 && !go1.28 && goexperiment.simd && amd64 && amd64.v3

package bitset

// GOAMD64=v3 and newer binaries require AVX2-capable processors. Direct calls
// remove even the process-constant capability branch from bitmap operations.
// Inputs shorter than two 256-bit vectors stay scalar because they cannot enter
// the unrolled vector body.

// Accelerated reports whether this process selected the AVX2 word kernels.
func Accelerated() bool { return true }

func andWords(dst, a, b []uint64) {
	if len(dst) < 8 {
		andWordsSmall(dst, a, b)
		return
	}
	andWordsAVX2(dst, a, b)
}

func and3Words(dst, a, b, c []uint64) {
	if len(dst) < 8 {
		and3WordsSmall(dst, a, b, c)
		return
	}
	and3WordsAVX2(dst, a, b, c)
}

func orWords(dst, a, b []uint64) {
	if len(dst) < 8 {
		orWordsSmall(dst, a, b)
		return
	}
	orWordsAVX2(dst, a, b)
}

func andNotWords(dst, a, b []uint64) {
	if len(dst) < 8 {
		andNotWordsSmall(dst, a, b)
		return
	}
	andNotWordsAVX2(dst, a, b)
}
