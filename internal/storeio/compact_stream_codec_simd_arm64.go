//go:build go1.27 && !go1.28 && goexperiment.simd && arm64

package storeio

import "simd/archsimd"

var (
	countCompactPacked7EqualImpl  = countCompactPacked7EqualNEON
	countCompactPacked10EqualImpl = countCompactPacked10EqualNEON
)

// The lookup indices interleave the two bytes needed by each little-endian
// packed lane. A 16-byte load covers two eight-lane groups of 7-bit values;
// the second table starts seven bytes later and therefore consumes 14 bytes.
var (
	compactPacked7NEONIndices0 = [16]uint8{
		0, 1, 0, 1, 1, 2, 2, 3,
		3, 4, 4, 5, 5, 6, 6, 7,
	}
	compactPacked7NEONIndices1 = [16]uint8{
		7, 8, 7, 8, 8, 9, 9, 10,
		10, 11, 11, 12, 12, 13, 13, 14,
	}
	compactPacked7NEONShifts = [8]int16{0, -7, -6, -5, -4, -3, -2, -1}

	compactPacked10NEONIndices = [16]uint8{
		0, 1, 1, 2, 2, 3, 3, 4,
		5, 6, 6, 7, 7, 8, 8, 9,
	}
	compactPacked10NEONShifts = [8]int16{0, -2, -4, -6, 0, -2, -4, -6}
)

// countCompactPacked7EqualNEON scans two eight-lane groups per 16-byte load.
// A load is admitted only when all 16 bytes are inside the encoded logical
// input. The load may include later rows or final padding, but only 14 bytes
// and 16 rows are consumed. No byte outside data is touched. Rows left after
// the guarded vector loop use the scalar oracle.
func countCompactPacked7EqualNEON(data []byte, count int, want uint64) (matched int) {
	if count <= 0 {
		return 0
	}
	if count < 32 {
		return countCompactPacked7EqualScalar(data, count, want)
	}
	if want > 127 {
		// Keep an out-of-range needle impossible while still scanning every row.
		want = 128
	}
	indices0 := archsimd.LoadUint8x16Array(&compactPacked7NEONIndices0)
	indices1 := archsimd.LoadUint8x16Array(&compactPacked7NEONIndices1)
	shifts := archsimd.LoadInt16x8Array(&compactPacked7NEONShifts)
	mask := archsimd.BroadcastUint16x8(127)
	needle := archsimd.BroadcastUint16x8(uint16(want))
	// ReduceSum returns uint16. Flushing every 4096 rows keeps even an
	// all-matching input below that limit, regardless of total input length.
	var sums0, sums1 archsimd.Uint16x8
	row := 0
	remaining := data
	for ; row+16 <= count && len(remaining) >= 16; row += 16 {
		loaded := archsimd.LoadUint8x16Array((*[16]uint8)(remaining))
		lanes0 := loaded.LookupOrZero(indices0).ReshapeToUint16s().Shift(shifts)
		lanes1 := loaded.LookupOrZero(indices1).ReshapeToUint16s().Shift(shifts)
		sums0 = sums0.Sub(lanes0.And(mask).Equal(needle).ToInt16x8().ToBits())
		sums1 = sums1.Sub(lanes1.And(mask).Equal(needle).ToInt16x8().ToBits())
		remaining = remaining[14:]
		if row&4095 == 4080 {
			matched += int(sums0.ReduceSum()) + int(sums1.ReduceSum())
			sums0, sums1 = archsimd.Uint16x8{}, archsimd.Uint16x8{}
		}
	}
	matched += int(sums0.ReduceSum()) + int(sums1.ReduceSum())
	if row < count {
		matched += countCompactPacked7EqualScalar(remaining, count-row, want)
	}
	return matched
}

// countCompactPacked10EqualNEON scans eight 10-bit lanes per 16-byte load.
// Ten bytes are consumed from each load; the remaining rows and any load
// whose final six bytes would cross the logical input use the scalar oracle.
func countCompactPacked10EqualNEON(data []byte, count int, want uint64) (matched int) {
	if count <= 0 {
		return 0
	}
	if count < 32 {
		return countCompactPacked10EqualScalar(data, count, want)
	}
	if want > 1023 {
		want = 1024
	}
	indices := archsimd.LoadUint8x16Array(&compactPacked10NEONIndices)
	shifts := archsimd.LoadInt16x8Array(&compactPacked10NEONShifts)
	mask := archsimd.BroadcastUint16x8(1023)
	needle := archsimd.BroadcastUint16x8(uint16(want))
	// Bound each uint16 reduction independently of the total input length.
	var sums archsimd.Uint16x8
	row := 0
	remaining := data
	for ; row+8 <= count && len(remaining) >= 16; row += 8 {
		loaded := archsimd.LoadUint8x16Array((*[16]uint8)(remaining))
		lanes := loaded.LookupOrZero(indices).ReshapeToUint16s().Shift(shifts)
		sums = sums.Sub(lanes.And(mask).Equal(needle).ToInt16x8().ToBits())
		remaining = remaining[10:]
		if row&4095 == 4088 {
			matched += int(sums.ReduceSum())
			sums = archsimd.Uint16x8{}
		}
	}
	matched += int(sums.ReduceSum())
	if row < count {
		matched += countCompactPacked10EqualScalar(remaining, count-row, want)
	}
	return matched
}
