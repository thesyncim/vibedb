//go:build !go1.27 || go1.28 || !goexperiment.simd || (!arm64 && !amd64)

package storeio

func compactCommonPrefixEqualHead(a, b []byte) int {
	return compactCommonPrefixEqualHeadScalar(a, b)
}
