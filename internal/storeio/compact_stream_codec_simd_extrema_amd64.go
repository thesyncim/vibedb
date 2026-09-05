//go:build go1.27 && !go1.28 && goexperiment.simd && amd64

package storeio

import "simd/archsimd"

// The amd64 archsimd API exposes lane-wise Min/Max but not a horizontal
// reduction. These reductions run once after the vector scan, outside the
// packed load/min/max loop.
func reduceUint8x16MinAVX2(x archsimd.Uint8x16) uint8 {
	minimum := x.GetElem(0)
	for i := uint8(1); i < 16; i++ {
		value := x.GetElem(i)
		if value < minimum {
			minimum = value
		}
	}
	return minimum
}

func reduceUint8x16MaxAVX2(x archsimd.Uint8x16) uint8 {
	maximum := x.GetElem(0)
	for i := uint8(1); i < 16; i++ {
		value := x.GetElem(i)
		if value > maximum {
			maximum = value
		}
	}
	return maximum
}

func reduceUint16x8MinAVX2(x archsimd.Uint16x8) uint16 {
	minimum := x.GetElem(0)
	for i := uint8(1); i < 8; i++ {
		value := x.GetElem(i)
		if value < minimum {
			minimum = value
		}
	}
	return minimum
}

func reduceUint16x8MaxAVX2(x archsimd.Uint16x8) uint16 {
	maximum := x.GetElem(0)
	for i := uint8(1); i < 8; i++ {
		value := x.GetElem(i)
		if value > maximum {
			maximum = value
		}
	}
	return maximum
}

func reduceUint8x32MinAVX2(x archsimd.Uint8x32) uint8 {
	minimum := reduceUint8x16MinAVX2(x.GetLo())
	if value := reduceUint8x16MinAVX2(x.GetHi()); value < minimum {
		minimum = value
	}
	return minimum
}

func reduceUint8x32MaxAVX2(x archsimd.Uint8x32) uint8 {
	maximum := reduceUint8x16MaxAVX2(x.GetLo())
	if value := reduceUint8x16MaxAVX2(x.GetHi()); value > maximum {
		maximum = value
	}
	return maximum
}

func reduceUint16x16MinAVX2(x archsimd.Uint16x16) uint16 {
	minimum := reduceUint16x8MinAVX2(x.GetLo())
	if value := reduceUint16x8MinAVX2(x.GetHi()); value < minimum {
		minimum = value
	}
	return minimum
}

func reduceUint16x16MaxAVX2(x archsimd.Uint16x16) uint16 {
	maximum := reduceUint16x8MaxAVX2(x.GetLo())
	if value := reduceUint16x8MaxAVX2(x.GetHi()); value > maximum {
		maximum = value
	}
	return maximum
}

// countCompactPacked7ExtremaAVX2 extracts two eight-lane groups from each
// admitted 16-byte load and reduces unsigned word minima/maxima. The load
// consumes 14 logical bytes; scalar rows finish the exact packed tail.
func countCompactPacked7ExtremaAVX2(
	data []byte, count int,
) (minimum, maximum uint64, found, supported bool) {
	if count <= 0 {
		return 0, 0, false, true
	}
	if count < 32 {
		return countCompactPacked7ExtremaScalar(data, count)
	}
	indices0 := archsimd.LoadInt8x16Array(&compactPacked7AVX2Indices0)
	indices1 := archsimd.LoadInt8x16Array(&compactPacked7AVX2Indices1)
	factors := archsimd.LoadUint16x8Array(&compactPacked7AVX2Factors)
	minimumVector := archsimd.BroadcastUint16x8(127)
	maximumVector := archsimd.Uint16x8{}
	row := 0
	remaining := data
	for ; row+16 <= count && len(remaining) >= 16; row += 16 {
		loaded := archsimd.LoadUint8x16Array((*[16]uint8)(remaining))
		lanes0 := loaded.PermuteOrZero(indices0).ReshapeToUint16s().Mul(factors).ShiftAllRight(9)
		lanes1 := loaded.PermuteOrZero(indices1).ReshapeToUint16s().Mul(factors).ShiftAllRight(9)
		minimumVector = minimumVector.Min(lanes0).Min(lanes1)
		maximumVector = maximumVector.Max(lanes0).Max(lanes1)
		remaining = remaining[14:]
	}
	minimum = uint64(reduceUint16x8MinAVX2(minimumVector))
	maximum = uint64(reduceUint16x8MaxAVX2(maximumVector))
	found, supported = true, true
	if row < count {
		tailMinimum, tailMaximum, tailFound, tailSupported :=
			countCompactPacked7ExtremaScalar(remaining, count-row)
		if !tailSupported {
			return 0, 0, false, false
		}
		if tailFound {
			if tailMinimum < minimum {
				minimum = tailMinimum
			}
			if tailMaximum > maximum {
				maximum = tailMaximum
			}
		}
	}
	return minimum, maximum, found, supported
}

// countCompactPacked8ExtremaAVX2 scans byte-aligned values in 256-bit
// vectors. Min/Max are unsigned byte operations, and the scalar tail keeps
// every load within the exact logical input.
func countCompactPacked8ExtremaAVX2(
	data []byte, count int,
) (minimum, maximum uint64, found, supported bool) {
	if count <= 0 {
		return 0, 0, false, true
	}
	if count < 32 {
		return countCompactPacked8ExtremaScalar(data, count)
	}
	minimumVector := archsimd.BroadcastUint8x32(255)
	maximumVector := archsimd.Uint8x32{}
	row := 0
	remaining := data
	for ; row+32 <= count && len(remaining) >= 32; row += 32 {
		loaded := archsimd.LoadUint8x32Array((*[32]uint8)(remaining))
		minimumVector = minimumVector.Min(loaded)
		maximumVector = maximumVector.Max(loaded)
		remaining = remaining[32:]
	}
	minimum = uint64(reduceUint8x32MinAVX2(minimumVector))
	maximum = uint64(reduceUint8x32MaxAVX2(maximumVector))
	found, supported = true, true
	if row < count {
		tailMinimum, tailMaximum, tailFound, tailSupported :=
			countCompactPacked8ExtremaScalar(remaining, count-row)
		if !tailSupported {
			return 0, 0, false, false
		}
		if tailFound {
			if tailMinimum < minimum {
				minimum = tailMinimum
			}
			if tailMaximum > maximum {
				maximum = tailMaximum
			}
		}
	}
	return minimum, maximum, found, supported
}

// countCompactPacked10ExtremaAVX2 extracts four eight-lane groups from the
// same bounded four-load schedule as equality, reducing unsigned words after
// each packed block. Exactly 40 bytes are consumed per 32 logical rows.
func countCompactPacked10ExtremaAVX2(
	data []byte, count int,
) (minimum, maximum uint64, found, supported bool) {
	if count <= 0 {
		return 0, 0, false, true
	}
	if count < 32 {
		return countCompactPacked10ExtremaScalar(data, count)
	}
	indices := archsimd.LoadInt8x16Array(&compactPacked10AVX2Indices)
	indicesOffset6 := archsimd.LoadInt8x16Array(&compactPacked10AVX2IndicesOffset6)
	factors := archsimd.LoadUint16x8Array(&compactPacked10AVX2Factors)
	minimumVector := archsimd.BroadcastUint16x8(1023)
	maximumVector := archsimd.Uint16x8{}
	row := 0
	remaining := data
	for ; row+32 <= count && len(remaining) >= 40; row += 32 {
		loaded0 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining))
		loaded1 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[10:]))
		loaded2 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[20:]))
		loaded3 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[24:]))
		lanes0 := loaded0.PermuteOrZero(indices).ReshapeToUint16s().Mul(factors).ShiftAllRight(6)
		lanes1 := loaded1.PermuteOrZero(indices).ReshapeToUint16s().Mul(factors).ShiftAllRight(6)
		lanes2 := loaded2.PermuteOrZero(indices).ReshapeToUint16s().Mul(factors).ShiftAllRight(6)
		lanes3 := loaded3.PermuteOrZero(indicesOffset6).ReshapeToUint16s().Mul(factors).ShiftAllRight(6)
		minimumVector = minimumVector.Min(lanes0).Min(lanes1).Min(lanes2).Min(lanes3)
		maximumVector = maximumVector.Max(lanes0).Max(lanes1).Max(lanes2).Max(lanes3)
		remaining = remaining[40:]
	}
	minimum = uint64(reduceUint16x8MinAVX2(minimumVector))
	maximum = uint64(reduceUint16x8MaxAVX2(maximumVector))
	found, supported = true, true
	if row < count {
		tailMinimum, tailMaximum, tailFound, tailSupported :=
			countCompactPacked10ExtremaScalar(remaining, count-row)
		if !tailSupported {
			return 0, 0, false, false
		}
		if tailFound {
			if tailMinimum < minimum {
				minimum = tailMinimum
			}
			if tailMaximum > maximum {
				maximum = tailMaximum
			}
		}
	}
	return minimum, maximum, found, supported
}

// countCompactPacked16ExtremaAVX2 scans little-endian uint16 values with
// unsigned word min/max operations. A 32-byte load covers 16 rows and tails
// are decoded scalar without reading padding.
func countCompactPacked16ExtremaAVX2(
	data []byte, count int,
) (minimum, maximum uint64, found, supported bool) {
	if count <= 0 {
		return 0, 0, false, true
	}
	if count < 32 {
		return countCompactPacked16ExtremaScalar(data, count)
	}
	minimumVector := archsimd.BroadcastUint16x16(65535)
	maximumVector := archsimd.Uint16x16{}
	row := 0
	remaining := data
	for ; row+16 <= count && len(remaining) >= 32; row += 16 {
		loaded := archsimd.LoadUint8x32Array((*[32]uint8)(remaining)).ReshapeToUint16s()
		minimumVector = minimumVector.Min(loaded)
		maximumVector = maximumVector.Max(loaded)
		remaining = remaining[32:]
	}
	minimum = uint64(reduceUint16x16MinAVX2(minimumVector))
	maximum = uint64(reduceUint16x16MaxAVX2(maximumVector))
	found, supported = true, true
	if row < count {
		tailMinimum, tailMaximum, tailFound, tailSupported :=
			countCompactPacked16ExtremaScalar(remaining, count-row)
		if !tailSupported {
			return 0, 0, false, false
		}
		if tailFound {
			if tailMinimum < minimum {
				minimum = tailMinimum
			}
			if tailMaximum > maximum {
				maximum = tailMaximum
			}
		}
	}
	return minimum, maximum, found, supported
}
