//go:build !(go1.27 && !go1.28 && goexperiment.simd && (arm64 || amd64))

package storeio

func compactPacked4MaxBelowSIMD(data []byte, count, limit int) bool {
	return compactPacked4MaxBelowScalar(data, count, limit)
}

func compactUnpackPacked4x32SIMD(dst *[32]uint8, src *[16]uint8) {
	compactUnpackPacked4x32Scalar(dst, src)
}

func compactUnpackPacked7x16SIMD(dst *[16]uint16, src *[16]uint8) {
	compactUnpackPacked7x16Scalar(dst, src)
}

func compactUnpackPacked10x32SIMD(dst *[32]uint16, src *[40]uint8) {
	compactUnpackPacked10x32Scalar(dst, src)
}
