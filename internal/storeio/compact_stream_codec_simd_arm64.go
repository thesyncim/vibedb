//go:build go1.27 && !go1.28 && goexperiment.simd && arm64

package storeio

import "simd/archsimd"

var (
	countCompactPacked7EqualImpl    = countCompactPacked7EqualNEON
	countCompactPacked8EqualImpl    = countCompactPacked8EqualNEON
	countCompactPacked10EqualImpl   = countCompactPacked10EqualNEON
	countCompactPacked16EqualImpl   = countCompactPacked16EqualNEON
	countCompactPacked7LessImpl     = countCompactPacked7LessNEON
	countCompactPacked8LessImpl     = countCompactPacked8LessNEON
	countCompactPacked10LessImpl    = countCompactPacked10LessNEON
	countCompactPacked16LessImpl    = countCompactPacked16LessNEON
	countCompactPacked7BetweenImpl  = countCompactPacked7BetweenNEON
	countCompactPacked8BetweenImpl  = countCompactPacked8BetweenNEON
	countCompactPacked10BetweenImpl = countCompactPacked10BetweenNEON
	countCompactPacked16BetweenImpl = countCompactPacked16BetweenNEON
	countCompactPacked7ExtremaImpl  = countCompactPacked7ExtremaScalar
	countCompactPacked8ExtremaImpl  = countCompactPacked8ExtremaScalar
	countCompactPacked10ExtremaImpl = countCompactPacked10ExtremaScalar
	countCompactPacked16ExtremaImpl = countCompactPacked16ExtremaScalar
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
	// The final 32-row width10 load starts at byte 24 and looks up bytes
	// 30..39. It overlaps the preceding load by 12 bytes and starts six bytes
	// before its logical byte-30 input, staying inside the exact 40-byte input
	// without a 46-byte lookahead.
	compactPacked10NEONIndicesOffset6 = [16]uint8{
		6, 7, 7, 8, 8, 9, 9, 10,
		11, 12, 12, 13, 13, 14, 14, 15,
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

// countCompactPacked10EqualNEON scans 32 10-bit lanes per four loads. The
// final load starts at byte 24 and uses indices 6..15, so four loads consume
// exactly 40 bytes while the last two loads overlap 12 bytes. Keeping four
// independent accumulators removes the old per-eight-row flush branch; each
// chunk is bounded to 4096 rows so uint16 reduction cannot overflow.
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
	indicesOffset6 := archsimd.LoadUint8x16Array(&compactPacked10NEONIndicesOffset6)
	shifts := archsimd.LoadInt16x8Array(&compactPacked10NEONShifts)
	mask := archsimd.BroadcastUint16x8(1023)
	needle := archsimd.BroadcastUint16x8(uint16(want))
	var sums0, sums1, sums2, sums3 archsimd.Uint16x8
	row := 0
	remaining := data
	for row < count {
		chunkEnd := count
		if remainingRows := 4096; chunkEnd-row > remainingRows {
			chunkEnd = row + remainingRows
		}
		vectorEnd := row + (chunkEnd-row)/32*32
		for ; row < vectorEnd && len(remaining) >= 40; row += 32 {
			loaded0 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining))
			loaded1 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[10:]))
			loaded2 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[20:]))
			loaded3 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[24:]))
			lanes0 := loaded0.LookupOrZero(indices).ReshapeToUint16s().Shift(shifts)
			lanes1 := loaded1.LookupOrZero(indices).ReshapeToUint16s().Shift(shifts)
			lanes2 := loaded2.LookupOrZero(indices).ReshapeToUint16s().Shift(shifts)
			lanes3 := loaded3.LookupOrZero(indicesOffset6).ReshapeToUint16s().Shift(shifts)
			sums0 = sums0.Sub(lanes0.And(mask).Equal(needle).ToInt16x8().ToBits())
			sums1 = sums1.Sub(lanes1.And(mask).Equal(needle).ToInt16x8().ToBits())
			sums2 = sums2.Sub(lanes2.And(mask).Equal(needle).ToInt16x8().ToBits())
			sums3 = sums3.Sub(lanes3.And(mask).Equal(needle).ToInt16x8().ToBits())
			remaining = remaining[40:]
		}
		matched += int(sums0.ReduceSum()) + int(sums1.ReduceSum()) +
			int(sums2.ReduceSum()) + int(sums3.ReduceSum())
		sums0, sums1, sums2, sums3 = archsimd.Uint16x8{}, archsimd.Uint16x8{}, archsimd.Uint16x8{}, archsimd.Uint16x8{}
		if row < chunkEnd {
			tail := chunkEnd - row
			matched += countCompactPacked10EqualScalar(remaining, tail, want)
			remaining = remaining[(tail*10+7)/8:]
			row = chunkEnd
		}
	}
	return matched
}

// countCompactPacked8EqualNEON scans byte-aligned eight-bit values in four
// accumulators. Each 512-row chunk gives every accumulator at most eight
// vectors, and the final partial group adds at most three more, so every
// uint8 reduction stays below 256. Values left after complete vectors use the
// scalar oracle.
func countCompactPacked8EqualNEON(data []byte, count int, want uint64) (matched int) {
	if count <= 0 {
		return 0
	}
	if count < 32 || want > 255 {
		return countCompactPacked8EqualScalar(data, count, want)
	}
	needle := archsimd.BroadcastUint8x16(uint8(want))
	row := 0
	remaining := data
	for row < count {
		chunkEnd := count
		if chunkEnd-row > 512 {
			chunkEnd = row + 512
		}
		vectorEnd := row + (chunkEnd-row)/64*64
		var sums0, sums1, sums2, sums3 archsimd.Uint8x16
		for ; row < vectorEnd && len(remaining) >= 64; row += 64 {
			loaded0 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining))
			loaded1 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[16:]))
			loaded2 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[32:]))
			loaded3 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[48:]))
			sums0 = sums0.Sub(loaded0.Equal(needle).ToInt8x16().ToBits())
			sums1 = sums1.Sub(loaded1.Equal(needle).ToInt8x16().ToBits())
			sums2 = sums2.Sub(loaded2.Equal(needle).ToInt8x16().ToBits())
			sums3 = sums3.Sub(loaded3.Equal(needle).ToInt8x16().ToBits())
			remaining = remaining[64:]
		}
		// A final partial group has at most three vectors. Adding it to sums0
		// remains bounded by (8+3)*16 = 176 per reduction.
		for ; row+16 <= chunkEnd && len(remaining) >= 16; row += 16 {
			loaded := archsimd.LoadUint8x16Array((*[16]uint8)(remaining))
			sums0 = sums0.Sub(loaded.Equal(needle).ToInt8x16().ToBits())
			remaining = remaining[16:]
		}
		matched += int(sums0.ReduceSum()) + int(sums1.ReduceSum()) +
			int(sums2.ReduceSum()) + int(sums3.ReduceSum())
		if row < chunkEnd {
			tail := chunkEnd - row
			matched += countCompactPacked8EqualScalar(remaining, tail, want)
			remaining = remaining[tail:]
			row = chunkEnd
		}
	}
	return matched
}

// countCompactPacked16EqualNEON scans eight little-endian uint16 values per
// load. A uint16 accumulator is reduced at most once per 4096-row chunk,
// keeping each lane's sum at 512 or less. Impossible needles take the scalar
// path so a uint64 value is never truncated into a false uint16 match.
func countCompactPacked16EqualNEON(data []byte, count int, want uint64) (matched int) {
	if count <= 0 {
		return 0
	}
	if count < 32 || want > 65535 {
		return countCompactPacked16EqualScalar(data, count, want)
	}
	needle := archsimd.BroadcastUint16x8(uint16(want))
	row := 0
	remaining := data
	for row < count {
		chunkEnd := count
		if chunkEnd-row > 4096 {
			chunkEnd = row + 4096
		}
		vectorEnd := row + (chunkEnd-row)/8*8
		var sums archsimd.Uint16x8
		for ; row < vectorEnd && len(remaining) >= 16; row += 8 {
			loaded := archsimd.LoadUint8x16Array((*[16]uint8)(remaining)).ReshapeToUint16s()
			sums = sums.Sub(loaded.Equal(needle).ToInt16x8().ToBits())
			remaining = remaining[16:]
		}
		matched += int(sums.ReduceSum())
		if row < chunkEnd {
			tail := chunkEnd - row
			matched += countCompactPacked16EqualScalar(remaining, tail, want)
			remaining = remaining[tail*2:]
			row = chunkEnd
		}
	}
	return matched
}

// The ordered kernels share the equality extraction and reduction schedules;
// only the lane predicate changes. Thresholds are bounded by the packed width
// by countCompactPackedLess before dispatch, while these guards keep the
// architecture entry points safe for direct differential tests as well.
func countCompactPacked7LessNEON(data []byte, count int, threshold uint64) (matched int) {
	if count <= 0 || threshold == 0 {
		return 0
	}
	if threshold > 127 {
		return count
	}
	if count < 32 {
		return countCompactPacked7LessScalar(data, count, threshold)
	}
	indices0 := archsimd.LoadUint8x16Array(&compactPacked7NEONIndices0)
	indices1 := archsimd.LoadUint8x16Array(&compactPacked7NEONIndices1)
	shifts := archsimd.LoadInt16x8Array(&compactPacked7NEONShifts)
	mask := archsimd.BroadcastUint16x8(127)
	needle := archsimd.BroadcastUint16x8(uint16(threshold))
	var sums0, sums1 archsimd.Uint16x8
	row := 0
	remaining := data
	for ; row+16 <= count && len(remaining) >= 16; row += 16 {
		loaded := archsimd.LoadUint8x16Array((*[16]uint8)(remaining))
		lanes0 := loaded.LookupOrZero(indices0).ReshapeToUint16s().Shift(shifts)
		lanes1 := loaded.LookupOrZero(indices1).ReshapeToUint16s().Shift(shifts)
		sums0 = sums0.Sub(lanes0.And(mask).Less(needle).ToInt16x8().ToBits())
		sums1 = sums1.Sub(lanes1.And(mask).Less(needle).ToInt16x8().ToBits())
		remaining = remaining[14:]
		if row&4095 == 4080 {
			matched += int(sums0.ReduceSum()) + int(sums1.ReduceSum())
			sums0, sums1 = archsimd.Uint16x8{}, archsimd.Uint16x8{}
		}
	}
	matched += int(sums0.ReduceSum()) + int(sums1.ReduceSum())
	if row < count {
		matched += countCompactPacked7LessScalar(remaining, count-row, threshold)
	}
	return matched
}

func countCompactPacked10LessNEON(data []byte, count int, threshold uint64) (matched int) {
	if count <= 0 || threshold == 0 {
		return 0
	}
	if threshold > 1023 {
		return count
	}
	if count < 32 {
		return countCompactPacked10LessScalar(data, count, threshold)
	}
	indices := archsimd.LoadUint8x16Array(&compactPacked10NEONIndices)
	indicesOffset6 := archsimd.LoadUint8x16Array(&compactPacked10NEONIndicesOffset6)
	shifts := archsimd.LoadInt16x8Array(&compactPacked10NEONShifts)
	mask := archsimd.BroadcastUint16x8(1023)
	needle := archsimd.BroadcastUint16x8(uint16(threshold))
	var sums0, sums1, sums2, sums3 archsimd.Uint16x8
	row := 0
	remaining := data
	for row < count {
		chunkEnd := count
		if remainingRows := 4096; chunkEnd-row > remainingRows {
			chunkEnd = row + remainingRows
		}
		vectorEnd := row + (chunkEnd-row)/32*32
		for ; row < vectorEnd && len(remaining) >= 40; row += 32 {
			loaded0 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining))
			loaded1 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[10:]))
			loaded2 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[20:]))
			loaded3 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[24:]))
			lanes0 := loaded0.LookupOrZero(indices).ReshapeToUint16s().Shift(shifts)
			lanes1 := loaded1.LookupOrZero(indices).ReshapeToUint16s().Shift(shifts)
			lanes2 := loaded2.LookupOrZero(indices).ReshapeToUint16s().Shift(shifts)
			lanes3 := loaded3.LookupOrZero(indicesOffset6).ReshapeToUint16s().Shift(shifts)
			sums0 = sums0.Sub(lanes0.And(mask).Less(needle).ToInt16x8().ToBits())
			sums1 = sums1.Sub(lanes1.And(mask).Less(needle).ToInt16x8().ToBits())
			sums2 = sums2.Sub(lanes2.And(mask).Less(needle).ToInt16x8().ToBits())
			sums3 = sums3.Sub(lanes3.And(mask).Less(needle).ToInt16x8().ToBits())
			remaining = remaining[40:]
		}
		matched += int(sums0.ReduceSum()) + int(sums1.ReduceSum()) +
			int(sums2.ReduceSum()) + int(sums3.ReduceSum())
		sums0, sums1, sums2, sums3 = archsimd.Uint16x8{}, archsimd.Uint16x8{}, archsimd.Uint16x8{}, archsimd.Uint16x8{}
		if row < chunkEnd {
			tail := chunkEnd - row
			matched += countCompactPacked10LessScalar(remaining, tail, threshold)
			remaining = remaining[(tail*10+7)/8:]
			row = chunkEnd
		}
	}
	return matched
}

func countCompactPacked8LessNEON(data []byte, count int, threshold uint64) (matched int) {
	if count <= 0 || threshold == 0 {
		return 0
	}
	if threshold > 255 {
		return count
	}
	if count < 32 {
		return countCompactPacked8LessScalar(data, count, threshold)
	}
	needle := archsimd.BroadcastUint8x16(uint8(threshold))
	row := 0
	remaining := data
	for row < count {
		chunkEnd := count
		if chunkEnd-row > 512 {
			chunkEnd = row + 512
		}
		vectorEnd := row + (chunkEnd-row)/64*64
		var sums0, sums1, sums2, sums3 archsimd.Uint8x16
		for ; row < vectorEnd && len(remaining) >= 64; row += 64 {
			loaded0 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining))
			loaded1 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[16:]))
			loaded2 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[32:]))
			loaded3 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[48:]))
			sums0 = sums0.Sub(loaded0.Less(needle).ToInt8x16().ToBits())
			sums1 = sums1.Sub(loaded1.Less(needle).ToInt8x16().ToBits())
			sums2 = sums2.Sub(loaded2.Less(needle).ToInt8x16().ToBits())
			sums3 = sums3.Sub(loaded3.Less(needle).ToInt8x16().ToBits())
			remaining = remaining[64:]
		}
		for ; row+16 <= chunkEnd && len(remaining) >= 16; row += 16 {
			loaded := archsimd.LoadUint8x16Array((*[16]uint8)(remaining))
			sums0 = sums0.Sub(loaded.Less(needle).ToInt8x16().ToBits())
			remaining = remaining[16:]
		}
		matched += int(sums0.ReduceSum()) + int(sums1.ReduceSum()) +
			int(sums2.ReduceSum()) + int(sums3.ReduceSum())
		if row < chunkEnd {
			tail := chunkEnd - row
			matched += countCompactPacked8LessScalar(remaining, tail, threshold)
			remaining = remaining[tail:]
			row = chunkEnd
		}
	}
	return matched
}

func countCompactPacked16LessNEON(data []byte, count int, threshold uint64) (matched int) {
	if count <= 0 || threshold == 0 {
		return 0
	}
	if threshold > 65535 {
		return count
	}
	if count < 32 {
		return countCompactPacked16LessScalar(data, count, threshold)
	}
	needle := archsimd.BroadcastUint16x8(uint16(threshold))
	row := 0
	remaining := data
	for row < count {
		chunkEnd := count
		if chunkEnd-row > 4096 {
			chunkEnd = row + 4096
		}
		vectorEnd := row + (chunkEnd-row)/8*8
		var sums archsimd.Uint16x8
		for ; row < vectorEnd && len(remaining) >= 16; row += 8 {
			loaded := archsimd.LoadUint8x16Array((*[16]uint8)(remaining)).ReshapeToUint16s()
			sums = sums.Sub(loaded.Less(needle).ToInt16x8().ToBits())
			remaining = remaining[16:]
		}
		matched += int(sums.ReduceSum())
		if row < chunkEnd {
			tail := chunkEnd - row
			matched += countCompactPacked16LessScalar(remaining, tail, threshold)
			remaining = remaining[tail*2:]
			row = chunkEnd
		}
	}
	return matched
}

// The fused interval kernels below apply both finite packed-lane bounds to
// the values extracted from each load. countCompactPackedBetween handles
// zero/full-domain and one-sided intervals before dispatch, so these entry
// points only receive representable lower < upper endpoints.
func countCompactPacked7BetweenNEON(
	data []byte, count int, lower, upper uint64,
) (matched int) {
	if count <= 0 || upper <= lower {
		return 0
	}
	if lower > 127 || upper > 128 {
		return countCompactPacked7BetweenScalar(data, count, lower, upper)
	}
	if count < 32 {
		return countCompactPacked7BetweenScalar(data, count, lower, upper)
	}
	indices0 := archsimd.LoadUint8x16Array(&compactPacked7NEONIndices0)
	indices1 := archsimd.LoadUint8x16Array(&compactPacked7NEONIndices1)
	shifts := archsimd.LoadInt16x8Array(&compactPacked7NEONShifts)
	mask := archsimd.BroadcastUint16x8(127)
	lowerNeedle := archsimd.BroadcastUint16x8(uint16(lower))
	upperNeedle := archsimd.BroadcastUint16x8(uint16(upper))
	var sums0, sums1 archsimd.Uint16x8
	row := 0
	remaining := data
	for ; row+16 <= count && len(remaining) >= 16; row += 16 {
		loaded := archsimd.LoadUint8x16Array((*[16]uint8)(remaining))
		lanes0 := loaded.LookupOrZero(indices0).ReshapeToUint16s().Shift(shifts).And(mask)
		lanes1 := loaded.LookupOrZero(indices1).ReshapeToUint16s().Shift(shifts).And(mask)
		lower0 := lowerNeedle.LessEqual(lanes0)
		lower1 := lowerNeedle.LessEqual(lanes1)
		sums0 = sums0.Sub(lower0.And(lanes0.Less(upperNeedle)).ToInt16x8().ToBits())
		sums1 = sums1.Sub(lower1.And(lanes1.Less(upperNeedle)).ToInt16x8().ToBits())
		remaining = remaining[14:]
		if row&4095 == 4080 {
			matched += int(sums0.ReduceSum()) + int(sums1.ReduceSum())
			sums0, sums1 = archsimd.Uint16x8{}, archsimd.Uint16x8{}
		}
	}
	matched += int(sums0.ReduceSum()) + int(sums1.ReduceSum())
	if row < count {
		matched += countCompactPacked7BetweenScalar(remaining, count-row, lower, upper)
	}
	return matched
}

func countCompactPacked10BetweenNEON(
	data []byte, count int, lower, upper uint64,
) (matched int) {
	if count <= 0 || upper <= lower {
		return 0
	}
	if lower > 1023 || upper > 1024 {
		return countCompactPacked10BetweenScalar(data, count, lower, upper)
	}
	if count < 32 {
		return countCompactPacked10BetweenScalar(data, count, lower, upper)
	}
	indices := archsimd.LoadUint8x16Array(&compactPacked10NEONIndices)
	indicesOffset6 := archsimd.LoadUint8x16Array(&compactPacked10NEONIndicesOffset6)
	shifts := archsimd.LoadInt16x8Array(&compactPacked10NEONShifts)
	mask := archsimd.BroadcastUint16x8(1023)
	lowerNeedle := archsimd.BroadcastUint16x8(uint16(lower))
	upperNeedle := archsimd.BroadcastUint16x8(uint16(upper))
	var sums0, sums1, sums2, sums3 archsimd.Uint16x8
	row := 0
	remaining := data
	for row < count {
		chunkEnd := count
		if remainingRows := 4096; chunkEnd-row > remainingRows {
			chunkEnd = row + remainingRows
		}
		vectorEnd := row + (chunkEnd-row)/32*32
		for ; row < vectorEnd && len(remaining) >= 40; row += 32 {
			loaded0 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining))
			loaded1 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[10:]))
			loaded2 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[20:]))
			loaded3 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[24:]))
			lanes0 := loaded0.LookupOrZero(indices).ReshapeToUint16s().Shift(shifts).And(mask)
			lanes1 := loaded1.LookupOrZero(indices).ReshapeToUint16s().Shift(shifts).And(mask)
			lanes2 := loaded2.LookupOrZero(indices).ReshapeToUint16s().Shift(shifts).And(mask)
			lanes3 := loaded3.LookupOrZero(indicesOffset6).ReshapeToUint16s().Shift(shifts).And(mask)
			sums0 = sums0.Sub(lowerNeedle.LessEqual(lanes0).And(lanes0.Less(upperNeedle)).ToInt16x8().ToBits())
			sums1 = sums1.Sub(lowerNeedle.LessEqual(lanes1).And(lanes1.Less(upperNeedle)).ToInt16x8().ToBits())
			sums2 = sums2.Sub(lowerNeedle.LessEqual(lanes2).And(lanes2.Less(upperNeedle)).ToInt16x8().ToBits())
			sums3 = sums3.Sub(lowerNeedle.LessEqual(lanes3).And(lanes3.Less(upperNeedle)).ToInt16x8().ToBits())
			remaining = remaining[40:]
		}
		matched += int(sums0.ReduceSum()) + int(sums1.ReduceSum()) +
			int(sums2.ReduceSum()) + int(sums3.ReduceSum())
		sums0, sums1, sums2, sums3 = archsimd.Uint16x8{}, archsimd.Uint16x8{}, archsimd.Uint16x8{}, archsimd.Uint16x8{}
		if row < chunkEnd {
			tail := chunkEnd - row
			matched += countCompactPacked10BetweenScalar(remaining, tail, lower, upper)
			remaining = remaining[(tail*10+7)/8:]
			row = chunkEnd
		}
	}
	return matched
}

func countCompactPacked8BetweenNEON(
	data []byte, count int, lower, upper uint64,
) (matched int) {
	if count <= 0 || upper <= lower {
		return 0
	}
	if lower > 255 || upper > 255 {
		return countCompactPacked8BetweenScalar(data, count, lower, upper)
	}
	if count < 32 {
		return countCompactPacked8BetweenScalar(data, count, lower, upper)
	}
	lowerNeedle := archsimd.BroadcastUint8x16(uint8(lower))
	upperNeedle := archsimd.BroadcastUint8x16(uint8(upper))
	row := 0
	remaining := data
	for row < count {
		chunkEnd := count
		if chunkEnd-row > 512 {
			chunkEnd = row + 512
		}
		vectorEnd := row + (chunkEnd-row)/64*64
		var sums0, sums1, sums2, sums3 archsimd.Uint8x16
		for ; row < vectorEnd && len(remaining) >= 64; row += 64 {
			loaded0 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining))
			loaded1 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[16:]))
			loaded2 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[32:]))
			loaded3 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[48:]))
			sums0 = sums0.Sub(lowerNeedle.LessEqual(loaded0).And(loaded0.Less(upperNeedle)).ToInt8x16().ToBits())
			sums1 = sums1.Sub(lowerNeedle.LessEqual(loaded1).And(loaded1.Less(upperNeedle)).ToInt8x16().ToBits())
			sums2 = sums2.Sub(lowerNeedle.LessEqual(loaded2).And(loaded2.Less(upperNeedle)).ToInt8x16().ToBits())
			sums3 = sums3.Sub(lowerNeedle.LessEqual(loaded3).And(loaded3.Less(upperNeedle)).ToInt8x16().ToBits())
			remaining = remaining[64:]
		}
		for ; chunkEnd-row >= 16 && len(remaining) >= 16; row += 16 {
			loaded := archsimd.LoadUint8x16Array((*[16]uint8)(remaining))
			sums0 = sums0.Sub(lowerNeedle.LessEqual(loaded).And(loaded.Less(upperNeedle)).ToInt8x16().ToBits())
			remaining = remaining[16:]
		}
		matched += int(sums0.ReduceSum()) + int(sums1.ReduceSum()) +
			int(sums2.ReduceSum()) + int(sums3.ReduceSum())
		if row < chunkEnd {
			tail := chunkEnd - row
			matched += countCompactPacked8BetweenScalar(remaining, tail, lower, upper)
			remaining = remaining[tail:]
			row = chunkEnd
		}
	}
	return matched
}

func countCompactPacked16BetweenNEON(
	data []byte, count int, lower, upper uint64,
) (matched int) {
	if count <= 0 || upper <= lower {
		return 0
	}
	if lower > 65535 || upper > 65535 {
		return countCompactPacked16BetweenScalar(data, count, lower, upper)
	}
	if count < 32 {
		return countCompactPacked16BetweenScalar(data, count, lower, upper)
	}
	lowerNeedle := archsimd.BroadcastUint16x8(uint16(lower))
	upperNeedle := archsimd.BroadcastUint16x8(uint16(upper))
	row := 0
	remaining := data
	for row < count {
		chunkEnd := count
		if chunkEnd-row > 4096 {
			chunkEnd = row + 4096
		}
		vectorEnd := row + (chunkEnd-row)/8*8
		var sums archsimd.Uint16x8
		for ; row < vectorEnd && len(remaining) >= 16; row += 8 {
			loaded := archsimd.LoadUint8x16Array((*[16]uint8)(remaining)).ReshapeToUint16s()
			sums = sums.Sub(lowerNeedle.LessEqual(loaded).And(loaded.Less(upperNeedle)).ToInt16x8().ToBits())
			remaining = remaining[16:]
		}
		matched += int(sums.ReduceSum())
		if row < chunkEnd {
			tail := chunkEnd - row
			matched += countCompactPacked16BetweenScalar(remaining, tail, lower, upper)
			remaining = remaining[tail*2:]
			row = chunkEnd
		}
	}
	return matched
}
