//go:build !go1.27 || go1.28 || !goexperiment.simd || (!arm64 && !amd64.v3)

package store

func reducePackedFloat64LE(values []byte) packedFloat64Summary {
	return reducePackedFloat64LEReference(values)
}
