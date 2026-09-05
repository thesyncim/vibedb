//go:build go1.27 && !go1.28 && goexperiment.simd && arm64

package storeio

import "simd/archsimd"

// countCompactPacked7ExtremaNEON extracts two eight-lane groups from each
// admitted 16-byte load and keeps unsigned lane minima/maxima in registers.
// Seven-byte groups consume 16 logical rows; the final partial group uses the
// scalar oracle so no load reads beyond the exact packed extent.
func countCompactPacked7ExtremaNEON(
	data []byte, count int,
) (minimum, maximum uint64, found, supported bool) {
	if count <= 0 {
		return 0, 0, false, true
	}
	if count < 32 {
		return countCompactPacked7ExtremaScalar(data, count)
	}
	indices0 := archsimd.LoadUint8x16Array(&compactPacked7NEONIndices0)
	indices1 := archsimd.LoadUint8x16Array(&compactPacked7NEONIndices1)
	shifts := archsimd.LoadInt16x8Array(&compactPacked7NEONShifts)
	mask := archsimd.BroadcastUint16x8(127)
	minimumVector := mask
	maximumVector := archsimd.Uint16x8{}
	row := 0
	remaining := data
	for ; row+16 <= count && len(remaining) >= 16; row += 16 {
		loaded := archsimd.LoadUint8x16Array((*[16]uint8)(remaining))
		lanes0 := loaded.LookupOrZero(indices0).ReshapeToUint16s().Shift(shifts).And(mask)
		lanes1 := loaded.LookupOrZero(indices1).ReshapeToUint16s().Shift(shifts).And(mask)
		minimumVector = minimumVector.Min(lanes0).Min(lanes1)
		maximumVector = maximumVector.Max(lanes0).Max(lanes1)
		remaining = remaining[14:]
	}
	minimum = uint64(minimumVector.ReduceMin())
	maximum = uint64(maximumVector.ReduceMax())
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

// countCompactPacked8ExtremaNEON scans byte-aligned lanes with direct loads.
// Keeping the extrema in Uint8x16 registers maps the reductions to UMIN/UMAX
// while the exact-length tail remains on the scalar implementation.
func countCompactPacked8ExtremaNEON(
	data []byte, count int,
) (minimum, maximum uint64, found, supported bool) {
	if count <= 0 {
		return 0, 0, false, true
	}
	if count < 32 {
		return countCompactPacked8ExtremaScalar(data, count)
	}
	minimumVector := archsimd.BroadcastUint8x16(255)
	maximumVector := archsimd.Uint8x16{}
	row := 0
	remaining := data
	for ; row+16 <= count && len(remaining) >= 16; row += 16 {
		loaded := archsimd.LoadUint8x16Array((*[16]uint8)(remaining))
		minimumVector = minimumVector.Min(loaded)
		maximumVector = maximumVector.Max(loaded)
		remaining = remaining[16:]
	}
	minimum = uint64(minimumVector.ReduceMin())
	maximum = uint64(maximumVector.ReduceMax())
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

// countCompactPacked10ExtremaNEON extracts four eight-lane groups from four
// overlapping loads (exactly 40 bytes for 32 rows), then reduces unsigned
// words in-register. The scalar tail handles non-multiple-of-32 row counts.
func countCompactPacked10ExtremaNEON(
	data []byte, count int,
) (minimum, maximum uint64, found, supported bool) {
	if count <= 0 {
		return 0, 0, false, true
	}
	if count < 32 {
		return countCompactPacked10ExtremaScalar(data, count)
	}
	indices := archsimd.LoadUint8x16Array(&compactPacked10NEONIndices)
	indicesOffset6 := archsimd.LoadUint8x16Array(&compactPacked10NEONIndicesOffset6)
	shifts := archsimd.LoadInt16x8Array(&compactPacked10NEONShifts)
	mask := archsimd.BroadcastUint16x8(1023)
	minimumVector := mask
	maximumVector := archsimd.Uint16x8{}
	row := 0
	remaining := data
	for ; row+32 <= count && len(remaining) >= 40; row += 32 {
		loaded0 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining))
		loaded1 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[10:]))
		loaded2 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[20:]))
		loaded3 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[24:]))
		lanes0 := loaded0.LookupOrZero(indices).ReshapeToUint16s().Shift(shifts).And(mask)
		lanes1 := loaded1.LookupOrZero(indices).ReshapeToUint16s().Shift(shifts).And(mask)
		lanes2 := loaded2.LookupOrZero(indices).ReshapeToUint16s().Shift(shifts).And(mask)
		lanes3 := loaded3.LookupOrZero(indicesOffset6).ReshapeToUint16s().Shift(shifts).And(mask)
		minimumVector = minimumVector.Min(lanes0).Min(lanes1).Min(lanes2).Min(lanes3)
		maximumVector = maximumVector.Max(lanes0).Max(lanes1).Max(lanes2).Max(lanes3)
		remaining = remaining[40:]
	}
	minimum = uint64(minimumVector.ReduceMin())
	maximum = uint64(maximumVector.ReduceMax())
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

// countCompactPacked16ExtremaNEON handles little-endian byte-aligned words;
// each 16-byte load covers eight packed lanes and never crosses data's exact
// bound. Scalar rows after the final complete vector are merged afterward.
func countCompactPacked16ExtremaNEON(
	data []byte, count int,
) (minimum, maximum uint64, found, supported bool) {
	if count <= 0 {
		return 0, 0, false, true
	}
	if count < 32 {
		return countCompactPacked16ExtremaScalar(data, count)
	}
	minimumVector := archsimd.BroadcastUint16x8(65535)
	maximumVector := archsimd.Uint16x8{}
	row := 0
	remaining := data
	for ; row+8 <= count && len(remaining) >= 16; row += 8 {
		loaded := archsimd.LoadUint8x16Array((*[16]uint8)(remaining)).ReshapeToUint16s()
		minimumVector = minimumVector.Min(loaded)
		maximumVector = maximumVector.Max(loaded)
		remaining = remaining[16:]
	}
	minimum = uint64(minimumVector.ReduceMin())
	maximum = uint64(maximumVector.ReduceMax())
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
