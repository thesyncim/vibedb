//go:build go1.27 && !go1.28 && goexperiment.simd && arm64

package store

import (
	"simd/archsimd"
	"unsafe"
)

// reducePackedFloat64LE uses two independent NEON accumulators. The four
// scalar lanes are folded in the same order as the portable reference; the
// scalar min/max pass preserves the established stable-slot and signed-zero
// semantics exactly.
func reducePackedFloat64LE(values []byte) packedFloat64Summary {
	if len(values) < 32 {
		return reducePackedFloat64LEReference(values)
	}
	base := unsafe.Pointer(unsafe.SliceData(values))
	sum0 := archsimd.LoadFloat64x2Array((*[2]float64)(base))
	sum1 := archsimd.LoadFloat64x2Array((*[2]float64)(unsafe.Add(base, 16)))
	offset := 32
	for ; offset+32 <= len(values); offset += 32 {
		sum0 = sum0.Add(archsimd.LoadFloat64x2Array(
			(*[2]float64)(unsafe.Add(base, offset)),
		))
		sum1 = sum1.Add(archsimd.LoadFloat64x2Array(
			(*[2]float64)(unsafe.Add(base, offset+16)),
		))
	}
	var lanes0, lanes1 [2]float64
	sum0.StoreArray(&lanes0)
	sum1.StoreArray(&lanes1)
	summary := packedFloat64Summary{
		count: len(values) / 8,
		sum:   (lanes0[0] + lanes0[1]) + (lanes1[0] + lanes1[1]),
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
	return summary
}
