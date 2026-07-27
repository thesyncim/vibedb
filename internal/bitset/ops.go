// Package bitset provides allocation-free boolean kernels over dense posting
// words. Callers retain representation policy: sparse ordinal lists should not
// be converted merely to reach these kernels unless reuse amortizes the cost.
package bitset

// And appends the word-wise intersection of a and b to dst. The shorter input
// fixes the result length. dst may be exactly a[:0] or b[:0] for in-place
// operation; otherwise its writable capacity must not overlap either input.
func And(dst, a, b []uint64) []uint64 {
	n := min(len(a), len(b))
	mark := len(dst)
	dst = extend(dst, n)
	andWords(dst[mark:], a[:n], b[:n])
	return dst
}

// And3 appends the fused intersection of a, b, and c. Fusing three predicates
// avoids one intermediate write/read pass; the shortest input fixes the result
// length. Exact in-place operation with any input is supported.
func And3(dst, a, b, c []uint64) []uint64 {
	n := min(len(a), len(b), len(c))
	mark := len(dst)
	dst = extend(dst, n)
	and3Words(dst[mark:], a[:n], b[:n], c[:n])
	return dst
}

// Or appends the word-wise union of a and b to dst, treating missing words in
// the shorter input as zero. dst may be exactly a[:0] or b[:0] for in-place
// operation; otherwise its writable capacity must not overlap either input.
func Or(dst, a, b []uint64) []uint64 {
	n := max(len(a), len(b))
	mark := len(dst)
	dst = extend(dst, n)
	common := min(len(a), len(b))
	orWords(dst[mark:mark+common], a[:common], b[:common])
	if len(a) > common {
		copy(dst[mark+common:], a[common:])
	} else {
		copy(dst[mark+common:], b[common:])
	}
	return dst
}

// AndNot appends a &^ b to dst. Its length is len(a); absent words of b are
// zero. dst may be exactly a[:0] or b[:0] for in-place operation; otherwise its
// writable capacity must not overlap either input.
func AndNot(dst, a, b []uint64) []uint64 {
	mark := len(dst)
	dst = extend(dst, len(a))
	common := min(len(a), len(b))
	andNotWords(dst[mark:mark+common], a[:common], b[:common])
	copy(dst[mark+common:], a[common:])
	return dst
}

func extend[T any](dst []T, n int) []T {
	if n <= cap(dst)-len(dst) {
		return dst[:len(dst)+n]
	}
	return append(dst, make([]T, n)...)
}

func andWordsScalar(dst, a, b []uint64) {
	i := 0
	for ; i+4 <= len(dst); i += 4 {
		dst[i+0] = a[i+0] & b[i+0]
		dst[i+1] = a[i+1] & b[i+1]
		dst[i+2] = a[i+2] & b[i+2]
		dst[i+3] = a[i+3] & b[i+3]
	}
	for ; i < len(dst); i++ {
		dst[i] = a[i] & b[i]
	}
}

func and3WordsScalar(dst, a, b, c []uint64) {
	i := 0
	for ; i+4 <= len(dst); i += 4 {
		dst[i+0] = a[i+0] & b[i+0] & c[i+0]
		dst[i+1] = a[i+1] & b[i+1] & c[i+1]
		dst[i+2] = a[i+2] & b[i+2] & c[i+2]
		dst[i+3] = a[i+3] & b[i+3] & c[i+3]
	}
	for ; i < len(dst); i++ {
		dst[i] = a[i] & b[i] & c[i]
	}
}

func orWordsScalar(dst, a, b []uint64) {
	i := 0
	for ; i+4 <= len(dst); i += 4 {
		dst[i+0] = a[i+0] | b[i+0]
		dst[i+1] = a[i+1] | b[i+1]
		dst[i+2] = a[i+2] | b[i+2]
		dst[i+3] = a[i+3] | b[i+3]
	}
	for ; i < len(dst); i++ {
		dst[i] = a[i] | b[i]
	}
}

func andNotWordsScalar(dst, a, b []uint64) {
	i := 0
	for ; i+4 <= len(dst); i += 4 {
		dst[i+0] = a[i+0] &^ b[i+0]
		dst[i+1] = a[i+1] &^ b[i+1]
		dst[i+2] = a[i+2] &^ b[i+2]
		dst[i+3] = a[i+3] &^ b[i+3]
	}
	for ; i < len(dst); i++ {
		dst[i] = a[i] &^ b[i]
	}
}
