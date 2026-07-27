//go:build go1.27 && !go1.28 && goexperiment.simd && amd64.v3

package store

import (
	"simd/archsimd"
	"unsafe"
)

// reducePackedFloat64LE keeps the four portable accumulation lanes in one
// AVX2 register. Min/max uses validated raw scalar loads to preserve the
// stable-slot signed-zero behavior shared with the portable implementation.
func reducePackedFloat64LE(values []byte) packedFloat64Summary {
	if len(values) < 32 {
		return reducePackedFloat64LEReference(values)
	}
	base := unsafe.Pointer(unsafe.SliceData(values))
	sum := archsimd.LoadFloat64x4Array((*[4]float64)(base))
	offset := 32
	for ; offset+32 <= len(values); offset += 32 {
		sum = sum.Add(archsimd.LoadFloat64x4Array(
			(*[4]float64)(unsafe.Add(base, offset)),
		))
	}
	var lanes [4]float64
	sum.StoreArray(&lanes)
	summary := packedFloat64Summary{
		count: len(values) / 8,
		sum:   (lanes[0] + lanes[1]) + (lanes[2] + lanes[3]),
	}
	for ; offset < len(values); offset += 8 {
		summary.sum += *(*float64)(unsafe.Add(base, offset))
	}
	first := *(*float64)(base)
	summary.min, summary.max = first, first
	for offset = 8; offset < len(values); offset += 8 {
		value := *(*float64)(unsafe.Add(base, offset))
		if value < summary.min {
			summary.min = value
		}
		if value > summary.max {
			summary.max = value
		}
	}
	archsimd.ClearAVXUpperBits()
	return summary
}
