//go:build !goexperiment.simd || (!amd64 && !arm64)

package query

func joinBloomInsertBlock(block *joinBloomBlock, low uint32) {
	joinBloomInsertScalar(block, low)
}
