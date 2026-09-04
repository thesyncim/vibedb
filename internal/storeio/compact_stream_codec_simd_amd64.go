//go:build go1.27 && !go1.28 && goexperiment.simd && amd64

package storeio

import "simd/archsimd"

// The SIMD entry points are guarded by the runtime AVX2 feature bit. The
// file is still built on every amd64 SIMD target so a binary built with
// GOAMD64=v1 remains safe on machines without AVX2.
var (
	countCompactPacked7EqualImpl    = countCompactPacked7EqualScalar
	countCompactPacked8EqualImpl    = countCompactPacked8EqualScalar
	countCompactPacked10EqualImpl   = countCompactPacked10EqualScalar
	countCompactPacked16EqualImpl   = countCompactPacked16EqualScalar
	countCompactPacked7LessImpl     = countCompactPacked7LessScalar
	countCompactPacked8LessImpl     = countCompactPacked8LessScalar
	countCompactPacked10LessImpl    = countCompactPacked10LessScalar
	countCompactPacked16LessImpl    = countCompactPacked16LessScalar
	countCompactPacked7BetweenImpl  = countCompactPacked7BetweenScalar
	countCompactPacked8BetweenImpl  = countCompactPacked8BetweenScalar
	countCompactPacked10BetweenImpl = countCompactPacked10BetweenScalar
	countCompactPacked16BetweenImpl = countCompactPacked16BetweenScalar
)

func init() {
	if !archsimd.X86.AVX2() {
		return
	}
	countCompactPacked7EqualImpl = countCompactPacked7EqualAVX2
	countCompactPacked8EqualImpl = countCompactPacked8EqualAVX2
	countCompactPacked10EqualImpl = countCompactPacked10EqualAVX2
	countCompactPacked16EqualImpl = countCompactPacked16EqualAVX2
	countCompactPacked7LessImpl = countCompactPacked7LessAVX2
	countCompactPacked8LessImpl = countCompactPacked8LessAVX2
	countCompactPacked10LessImpl = countCompactPacked10LessAVX2
	countCompactPacked16LessImpl = countCompactPacked16LessAVX2
	countCompactPacked7BetweenImpl = countCompactPacked7BetweenAVX2
	countCompactPacked8BetweenImpl = countCompactPacked8BetweenAVX2
	countCompactPacked10BetweenImpl = countCompactPacked10BetweenAVX2
	countCompactPacked16BetweenImpl = countCompactPacked16BetweenAVX2
}

// PermuteOrZero works on bytes while ReshapeToUint16s forms little-endian
// words. Multiplication before the literal shift drops the high bits modulo
// 2^16 and leaves the requested packed lane in the low word bits. This avoids
// AVX-512-only variable word shifts.
var (
	compactPacked7AVX2Indices0 = [16]int8{
		0, 1, 0, 1, 1, 2, 2, 3,
		3, 4, 4, 5, 5, 6, 6, 7,
	}
	compactPacked7AVX2Indices1 = [16]int8{
		7, 8, 7, 8, 8, 9, 9, 10,
		10, 11, 11, 12, 12, 13, 13, 14,
	}
	compactPacked7AVX2Factors = [8]uint16{512, 4, 8, 16, 32, 64, 128, 256}

	compactPacked10AVX2Indices = [16]int8{
		0, 1, 1, 2, 2, 3, 3, 4,
		5, 6, 6, 7, 7, 8, 8, 9,
	}
	// The last of four 16-byte loads starts at byte 24 and extracts bytes
	// 30..39. It overlaps the preceding load by 12 bytes and consumes exactly
	// the 40 bytes occupied by 32 ten-bit lanes.
	compactPacked10AVX2IndicesOffset6 = [16]int8{
		6, 7, 7, 8, 8, 9, 9, 10,
		11, 12, 12, 13, 13, 14, 14, 15,
	}
	compactPacked10AVX2Factors = [8]uint16{64, 16, 4, 1, 64, 16, 4, 1}
)

// reduceUint16x8AVX2 horizontally sums an unsigned word vector. Values are
// bounded well below 65535 by the caller's chunk limits, so wrapping pairwise
// adds cannot affect the result.
func reduceUint16x8AVX2(x archsimd.Uint16x8) uint16 {
	x = x.ConcatAddPairs(x)
	x = x.ConcatAddPairs(x)
	x = x.ConcatAddPairs(x)
	return x.GetElem(0)
}

// countCompactPacked7EqualAVX2 scans two eight-lane groups per 16-byte load.
// A load consumes 14 bytes and is admitted only while all 16 loaded bytes are
// within the logical packed input. The scalar tail therefore also handles
// short inputs and the final partial group without reading padding.
func countCompactPacked7EqualAVX2(data []byte, count int, want uint64) (matched int) {
	if count <= 0 {
		return 0
	}
	if count < 32 {
		return countCompactPacked7EqualScalar(data, count, want)
	}
	if want > 127 {
		// Keep an out-of-range needle impossible while still scanning all rows.
		want = 128
	}
	indices0 := archsimd.LoadInt8x16Array(&compactPacked7AVX2Indices0)
	indices1 := archsimd.LoadInt8x16Array(&compactPacked7AVX2Indices1)
	factors := archsimd.LoadUint16x8Array(&compactPacked7AVX2Factors)
	needle := archsimd.BroadcastUint16x8(uint16(want))
	row := 0
	remaining := data
	for row < count {
		chunkEnd := count
		if chunkEnd-row > 4096 {
			chunkEnd = row + 4096
		}
		vectorEnd := row + (chunkEnd-row)/16*16
		var sums0, sums1 archsimd.Uint16x8
		for ; row < vectorEnd && len(remaining) >= 16; row += 16 {
			loaded := archsimd.LoadUint8x16Array((*[16]uint8)(remaining))
			lanes0 := loaded.PermuteOrZero(indices0).ReshapeToUint16s().Mul(factors).ShiftAllRight(9)
			lanes1 := loaded.PermuteOrZero(indices1).ReshapeToUint16s().Mul(factors).ShiftAllRight(9)
			sums0 = sums0.Sub(lanes0.Equal(needle).ToInt16x8().ToBits())
			sums1 = sums1.Sub(lanes1.Equal(needle).ToInt16x8().ToBits())
			remaining = remaining[14:]
		}
		matched += int(reduceUint16x8AVX2(sums0)) + int(reduceUint16x8AVX2(sums1))
		if row < chunkEnd {
			tail := chunkEnd - row
			matched += countCompactPacked7EqualScalar(remaining, tail, want)
			remaining = remaining[(tail*7+7)/8:]
			row = chunkEnd
		}
	}
	return matched
}

// countCompactPacked10EqualAVX2 scans 32 ten-bit lanes per four loads. The
// four independent word accumulators are reduced once per <=4096-row chunk,
// and the scalar tail handles rows that cannot form a complete 32-lane group.
func countCompactPacked10EqualAVX2(data []byte, count int, want uint64) (matched int) {
	if count <= 0 {
		return 0
	}
	if count < 32 {
		return countCompactPacked10EqualScalar(data, count, want)
	}
	if want > 1023 {
		want = 1024
	}
	indices := archsimd.LoadInt8x16Array(&compactPacked10AVX2Indices)
	indicesOffset6 := archsimd.LoadInt8x16Array(&compactPacked10AVX2IndicesOffset6)
	factors := archsimd.LoadUint16x8Array(&compactPacked10AVX2Factors)
	needle := archsimd.BroadcastUint16x8(uint16(want))
	row := 0
	remaining := data
	for row < count {
		chunkEnd := count
		if chunkEnd-row > 4096 {
			chunkEnd = row + 4096
		}
		vectorEnd := row + (chunkEnd-row)/32*32
		var sums0, sums1, sums2, sums3 archsimd.Uint16x8
		for ; row < vectorEnd && len(remaining) >= 40; row += 32 {
			loaded0 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining))
			loaded1 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[10:]))
			loaded2 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[20:]))
			loaded3 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[24:]))
			lanes0 := loaded0.PermuteOrZero(indices).ReshapeToUint16s().Mul(factors).ShiftAllRight(6)
			lanes1 := loaded1.PermuteOrZero(indices).ReshapeToUint16s().Mul(factors).ShiftAllRight(6)
			lanes2 := loaded2.PermuteOrZero(indices).ReshapeToUint16s().Mul(factors).ShiftAllRight(6)
			lanes3 := loaded3.PermuteOrZero(indicesOffset6).ReshapeToUint16s().Mul(factors).ShiftAllRight(6)
			sums0 = sums0.Sub(lanes0.Equal(needle).ToInt16x8().ToBits())
			sums1 = sums1.Sub(lanes1.Equal(needle).ToInt16x8().ToBits())
			sums2 = sums2.Sub(lanes2.Equal(needle).ToInt16x8().ToBits())
			sums3 = sums3.Sub(lanes3.Equal(needle).ToInt16x8().ToBits())
			remaining = remaining[40:]
		}
		matched += int(reduceUint16x8AVX2(sums0)) + int(reduceUint16x8AVX2(sums1)) +
			int(reduceUint16x8AVX2(sums2)) + int(reduceUint16x8AVX2(sums3))
		if row < chunkEnd {
			tail := chunkEnd - row
			matched += countCompactPacked10EqualScalar(remaining, tail, want)
			remaining = remaining[(tail*10+7)/8:]
			row = chunkEnd
		}
	}
	return matched
}

// reduceUint8x32AVX2 combines four bounded byte accumulators and horizontally
// sums their 32 lanes with VPSADBW. The two 64-bit halves are added before
// extraction, leaving just two scalar extracts in the reduction.
func reduceUint8x32AVX2(
	sums0, sums1, sums2, sums3 archsimd.Uint8x32,
) int {
	combined := sums0.Add(sums1).Add(sums2).Add(sums3)
	groups := combined.SumOf8AbsDiff(archsimd.Uint8x32{})
	halves := groups.GetLo().Add(groups.GetHi())
	return int(halves.GetElem(0) + halves.GetElem(1))
}

// countCompactPacked8EqualAVX2 scans four 32-byte vectors per 128 rows. A
// compare mask is all zeroes or all ones; subtracting it from a zero byte
// accumulator turns each match into one. Chunks are <=4096 rows, so combining
// the four accumulators cannot overflow a byte before VPSADBW reduces it.
func countCompactPacked8EqualAVX2(data []byte, count int, want uint64) (matched int) {
	if count <= 0 {
		return 0
	}
	if count < 32 || want > 255 {
		return countCompactPacked8EqualScalar(data, count, want)
	}
	needle := archsimd.BroadcastUint8x32(uint8(want))
	row := 0
	remaining := data
	for row < count {
		chunkEnd := count
		if chunkEnd-row > 4096 {
			chunkEnd = row + 4096
		}
		vectorEnd := row + (chunkEnd-row)/128*128
		var sums0, sums1, sums2, sums3 archsimd.Uint8x32
		for ; row < vectorEnd && len(remaining) >= 128; row += 128 {
			loaded0 := archsimd.LoadUint8x32Array((*[32]uint8)(remaining))
			loaded1 := archsimd.LoadUint8x32Array((*[32]uint8)(remaining[32:]))
			loaded2 := archsimd.LoadUint8x32Array((*[32]uint8)(remaining[64:]))
			loaded3 := archsimd.LoadUint8x32Array((*[32]uint8)(remaining[96:]))
			sums0 = sums0.Sub(loaded0.Equal(needle).ToInt8x32().ToBits())
			sums1 = sums1.Sub(loaded1.Equal(needle).ToInt8x32().ToBits())
			sums2 = sums2.Sub(loaded2.Equal(needle).ToInt8x32().ToBits())
			sums3 = sums3.Sub(loaded3.Equal(needle).ToInt8x32().ToBits())
			remaining = remaining[128:]
		}
		// Consume complete 32-row vectors before falling back to the scalar
		// tail. At most three vectors enter sums0 after the four-vector loop.
		for ; chunkEnd-row >= 32 && len(remaining) >= 32; row += 32 {
			loaded := archsimd.LoadUint8x32Array((*[32]uint8)(remaining))
			sums0 = sums0.Sub(loaded.Equal(needle).ToInt8x32().ToBits())
			remaining = remaining[32:]
		}
		matched += reduceUint8x32AVX2(sums0, sums1, sums2, sums3)
		if row < chunkEnd {
			tail := chunkEnd - row
			matched += countCompactPacked8EqualScalar(remaining, tail, want)
			remaining = remaining[tail:]
			row = chunkEnd
		}
	}
	return matched
}

// reduceUint16x16AVX2 sums a pair of 256-bit word accumulators. Pairwise addition
// of their low and high halves forms eight partial sums; three more reductions
// leave the total in lane zero. No sum exceeds 4096 within a chunk.
func reduceUint16x16AVX2(sums0, sums1 archsimd.Uint16x16) uint16 {
	lo := sums0.GetLo().Add(sums1.GetLo())
	hi := sums0.GetHi().Add(sums1.GetHi())
	x := lo.ConcatAddPairs(hi)
	x = x.ConcatAddPairs(x)
	x = x.ConcatAddPairs(x)
	x = x.ConcatAddPairs(x)
	return x.GetElem(0)
}

// countCompactPacked16EqualAVX2 scans two 32-byte vectors per 32 rows. The
// scalar path handles short and impossible-needle calls so a uint64 needle is
// never truncated into an accidental uint16 match.
func countCompactPacked16EqualAVX2(data []byte, count int, want uint64) (matched int) {
	if count <= 0 {
		return 0
	}
	if count < 32 || want > 65535 {
		return countCompactPacked16EqualScalar(data, count, want)
	}
	needle := archsimd.BroadcastUint16x16(uint16(want))
	row := 0
	remaining := data
	for row < count {
		chunkEnd := count
		if chunkEnd-row > 4096 {
			chunkEnd = row + 4096
		}
		vectorEnd := row + (chunkEnd-row)/32*32
		var sums0, sums1 archsimd.Uint16x16
		for ; row < vectorEnd && len(remaining) >= 64; row += 32 {
			loaded0 := archsimd.LoadUint8x32Array((*[32]uint8)(remaining)).ReshapeToUint16s()
			loaded1 := archsimd.LoadUint8x32Array((*[32]uint8)(remaining[32:])).ReshapeToUint16s()
			sums0 = sums0.Sub(loaded0.Equal(needle).ToInt16x16().ToBits())
			sums1 = sums1.Sub(loaded1.Equal(needle).ToInt16x16().ToBits())
			remaining = remaining[64:]
		}
		// A final 16-row vector avoids sending a full word-aligned group
		// through the scalar counter when the chunk leaves one behind.
		for ; chunkEnd-row >= 16 && len(remaining) >= 32; row += 16 {
			loaded := archsimd.LoadUint8x32Array((*[32]uint8)(remaining)).ReshapeToUint16s()
			sums0 = sums0.Sub(loaded.Equal(needle).ToInt16x16().ToBits())
			remaining = remaining[32:]
		}
		matched += int(reduceUint16x16AVX2(sums0, sums1))
		if row < chunkEnd {
			tail := chunkEnd - row
			matched += countCompactPacked16EqualScalar(remaining, tail, want)
			remaining = remaining[tail*2:]
			row = chunkEnd
		}
	}
	return matched
}

// Ordered packed counters reuse the equality extraction and bounded
// reductions. The front-end bounds thresholds to each lane width; guards are
// repeated here so direct architecture tests remain total and cannot truncate
// a uint64 threshold into a false comparison.
func countCompactPacked7LessAVX2(data []byte, count int, threshold uint64) (matched int) {
	if count <= 0 || threshold == 0 {
		return 0
	}
	if threshold > 127 {
		return count
	}
	if count < 32 {
		return countCompactPacked7LessScalar(data, count, threshold)
	}
	indices0 := archsimd.LoadInt8x16Array(&compactPacked7AVX2Indices0)
	indices1 := archsimd.LoadInt8x16Array(&compactPacked7AVX2Indices1)
	factors := archsimd.LoadUint16x8Array(&compactPacked7AVX2Factors)
	needle := archsimd.BroadcastUint16x8(uint16(threshold))
	row := 0
	remaining := data
	for row < count {
		chunkEnd := count
		if chunkEnd-row > 4096 {
			chunkEnd = row + 4096
		}
		vectorEnd := row + (chunkEnd-row)/16*16
		var sums0, sums1 archsimd.Uint16x8
		for ; row < vectorEnd && len(remaining) >= 16; row += 16 {
			loaded := archsimd.LoadUint8x16Array((*[16]uint8)(remaining))
			lanes0 := loaded.PermuteOrZero(indices0).ReshapeToUint16s().Mul(factors).ShiftAllRight(9)
			lanes1 := loaded.PermuteOrZero(indices1).ReshapeToUint16s().Mul(factors).ShiftAllRight(9)
			sums0 = sums0.Sub(lanes0.Less(needle).ToInt16x8().ToBits())
			sums1 = sums1.Sub(lanes1.Less(needle).ToInt16x8().ToBits())
			remaining = remaining[14:]
		}
		matched += int(reduceUint16x8AVX2(sums0)) + int(reduceUint16x8AVX2(sums1))
		if row < chunkEnd {
			tail := chunkEnd - row
			matched += countCompactPacked7LessScalar(remaining, tail, threshold)
			remaining = remaining[(tail*7+7)/8:]
			row = chunkEnd
		}
	}
	return matched
}

func countCompactPacked10LessAVX2(data []byte, count int, threshold uint64) (matched int) {
	if count <= 0 || threshold == 0 {
		return 0
	}
	if threshold > 1023 {
		return count
	}
	if count < 32 {
		return countCompactPacked10LessScalar(data, count, threshold)
	}
	indices := archsimd.LoadInt8x16Array(&compactPacked10AVX2Indices)
	indicesOffset6 := archsimd.LoadInt8x16Array(&compactPacked10AVX2IndicesOffset6)
	factors := archsimd.LoadUint16x8Array(&compactPacked10AVX2Factors)
	needle := archsimd.BroadcastUint16x8(uint16(threshold))
	row := 0
	remaining := data
	for row < count {
		chunkEnd := count
		if chunkEnd-row > 4096 {
			chunkEnd = row + 4096
		}
		vectorEnd := row + (chunkEnd-row)/32*32
		var sums0, sums1, sums2, sums3 archsimd.Uint16x8
		for ; row < vectorEnd && len(remaining) >= 40; row += 32 {
			loaded0 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining))
			loaded1 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[10:]))
			loaded2 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[20:]))
			loaded3 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[24:]))
			lanes0 := loaded0.PermuteOrZero(indices).ReshapeToUint16s().Mul(factors).ShiftAllRight(6)
			lanes1 := loaded1.PermuteOrZero(indices).ReshapeToUint16s().Mul(factors).ShiftAllRight(6)
			lanes2 := loaded2.PermuteOrZero(indices).ReshapeToUint16s().Mul(factors).ShiftAllRight(6)
			lanes3 := loaded3.PermuteOrZero(indicesOffset6).ReshapeToUint16s().Mul(factors).ShiftAllRight(6)
			sums0 = sums0.Sub(lanes0.Less(needle).ToInt16x8().ToBits())
			sums1 = sums1.Sub(lanes1.Less(needle).ToInt16x8().ToBits())
			sums2 = sums2.Sub(lanes2.Less(needle).ToInt16x8().ToBits())
			sums3 = sums3.Sub(lanes3.Less(needle).ToInt16x8().ToBits())
			remaining = remaining[40:]
		}
		matched += int(reduceUint16x8AVX2(sums0)) + int(reduceUint16x8AVX2(sums1)) +
			int(reduceUint16x8AVX2(sums2)) + int(reduceUint16x8AVX2(sums3))
		if row < chunkEnd {
			tail := chunkEnd - row
			matched += countCompactPacked10LessScalar(remaining, tail, threshold)
			remaining = remaining[(tail*10+7)/8:]
			row = chunkEnd
		}
	}
	return matched
}

func countCompactPacked8LessAVX2(data []byte, count int, threshold uint64) (matched int) {
	if count <= 0 || threshold == 0 {
		return 0
	}
	if threshold > 255 {
		return count
	}
	if count < 32 {
		return countCompactPacked8LessScalar(data, count, threshold)
	}
	needle := archsimd.BroadcastUint8x32(uint8(threshold))
	row := 0
	remaining := data
	for row < count {
		chunkEnd := count
		if chunkEnd-row > 4096 {
			chunkEnd = row + 4096
		}
		vectorEnd := row + (chunkEnd-row)/128*128
		var sums0, sums1, sums2, sums3 archsimd.Uint8x32
		for ; row < vectorEnd && len(remaining) >= 128; row += 128 {
			loaded0 := archsimd.LoadUint8x32Array((*[32]uint8)(remaining))
			loaded1 := archsimd.LoadUint8x32Array((*[32]uint8)(remaining[32:]))
			loaded2 := archsimd.LoadUint8x32Array((*[32]uint8)(remaining[64:]))
			loaded3 := archsimd.LoadUint8x32Array((*[32]uint8)(remaining[96:]))
			sums0 = sums0.Sub(loaded0.Less(needle).ToInt8x32().ToBits())
			sums1 = sums1.Sub(loaded1.Less(needle).ToInt8x32().ToBits())
			sums2 = sums2.Sub(loaded2.Less(needle).ToInt8x32().ToBits())
			sums3 = sums3.Sub(loaded3.Less(needle).ToInt8x32().ToBits())
			remaining = remaining[128:]
		}
		for ; chunkEnd-row >= 32 && len(remaining) >= 32; row += 32 {
			loaded := archsimd.LoadUint8x32Array((*[32]uint8)(remaining))
			sums0 = sums0.Sub(loaded.Less(needle).ToInt8x32().ToBits())
			remaining = remaining[32:]
		}
		matched += reduceUint8x32AVX2(sums0, sums1, sums2, sums3)
		if row < chunkEnd {
			tail := chunkEnd - row
			matched += countCompactPacked8LessScalar(remaining, tail, threshold)
			remaining = remaining[tail:]
			row = chunkEnd
		}
	}
	return matched
}

func countCompactPacked16LessAVX2(data []byte, count int, threshold uint64) (matched int) {
	if count <= 0 || threshold == 0 {
		return 0
	}
	if threshold > 65535 {
		return count
	}
	if count < 32 {
		return countCompactPacked16LessScalar(data, count, threshold)
	}
	needle := archsimd.BroadcastUint16x16(uint16(threshold))
	row := 0
	remaining := data
	for row < count {
		chunkEnd := count
		if chunkEnd-row > 4096 {
			chunkEnd = row + 4096
		}
		vectorEnd := row + (chunkEnd-row)/16*16
		var sums0, sums1 archsimd.Uint16x16
		for ; row < vectorEnd && len(remaining) >= 32; row += 16 {
			loaded := archsimd.LoadUint8x32Array((*[32]uint8)(remaining)).ReshapeToUint16s()
			sums0 = sums0.Sub(loaded.Less(needle).ToInt16x16().ToBits())
			remaining = remaining[32:]
		}
		matched += int(reduceUint16x16AVX2(sums0, sums1))
		if row < chunkEnd {
			tail := chunkEnd - row
			matched += countCompactPacked16LessScalar(remaining, tail, threshold)
			remaining = remaining[tail*2:]
			row = chunkEnd
		}
	}
	return matched
}

// The fused interval kernels decode each packed lane once and intersect both
// finite bounds in AVX2 masks. One-sided/full-domain intervals are handled by
// countCompactPackedBetween before dispatch, keeping these functions on the
// finite representable lane path.
func countCompactPacked7BetweenAVX2(
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
	indices0 := archsimd.LoadInt8x16Array(&compactPacked7AVX2Indices0)
	indices1 := archsimd.LoadInt8x16Array(&compactPacked7AVX2Indices1)
	factors := archsimd.LoadUint16x8Array(&compactPacked7AVX2Factors)
	lowerNeedle := archsimd.BroadcastUint16x8(uint16(lower))
	upperNeedle := archsimd.BroadcastUint16x8(uint16(upper))
	row := 0
	remaining := data
	for row < count {
		chunkEnd := count
		if chunkEnd-row > 4096 {
			chunkEnd = row + 4096
		}
		vectorEnd := row + (chunkEnd-row)/16*16
		var sums0, sums1 archsimd.Uint16x8
		for ; row < vectorEnd && len(remaining) >= 16; row += 16 {
			loaded := archsimd.LoadUint8x16Array((*[16]uint8)(remaining))
			lanes0 := loaded.PermuteOrZero(indices0).ReshapeToUint16s().Mul(factors).ShiftAllRight(9)
			lanes1 := loaded.PermuteOrZero(indices1).ReshapeToUint16s().Mul(factors).ShiftAllRight(9)
			sums0 = sums0.Sub(lowerNeedle.LessEqual(lanes0).And(lanes0.Less(upperNeedle)).ToInt16x8().ToBits())
			sums1 = sums1.Sub(lowerNeedle.LessEqual(lanes1).And(lanes1.Less(upperNeedle)).ToInt16x8().ToBits())
			remaining = remaining[14:]
		}
		matched += int(reduceUint16x8AVX2(sums0)) + int(reduceUint16x8AVX2(sums1))
		if row < chunkEnd {
			tail := chunkEnd - row
			matched += countCompactPacked7BetweenScalar(remaining, tail, lower, upper)
			remaining = remaining[(tail*7+7)/8:]
			row = chunkEnd
		}
	}
	return matched
}

func countCompactPacked10BetweenAVX2(
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
	indices := archsimd.LoadInt8x16Array(&compactPacked10AVX2Indices)
	indicesOffset6 := archsimd.LoadInt8x16Array(&compactPacked10AVX2IndicesOffset6)
	factors := archsimd.LoadUint16x8Array(&compactPacked10AVX2Factors)
	lowerNeedle := archsimd.BroadcastUint16x8(uint16(lower))
	upperNeedle := archsimd.BroadcastUint16x8(uint16(upper))
	row := 0
	remaining := data
	for row < count {
		chunkEnd := count
		if chunkEnd-row > 4096 {
			chunkEnd = row + 4096
		}
		vectorEnd := row + (chunkEnd-row)/32*32
		var sums0, sums1, sums2, sums3 archsimd.Uint16x8
		for ; row < vectorEnd && len(remaining) >= 40; row += 32 {
			loaded0 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining))
			loaded1 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[10:]))
			loaded2 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[20:]))
			loaded3 := archsimd.LoadUint8x16Array((*[16]uint8)(remaining[24:]))
			lanes0 := loaded0.PermuteOrZero(indices).ReshapeToUint16s().Mul(factors).ShiftAllRight(6)
			lanes1 := loaded1.PermuteOrZero(indices).ReshapeToUint16s().Mul(factors).ShiftAllRight(6)
			lanes2 := loaded2.PermuteOrZero(indices).ReshapeToUint16s().Mul(factors).ShiftAllRight(6)
			lanes3 := loaded3.PermuteOrZero(indicesOffset6).ReshapeToUint16s().Mul(factors).ShiftAllRight(6)
			sums0 = sums0.Sub(lowerNeedle.LessEqual(lanes0).And(lanes0.Less(upperNeedle)).ToInt16x8().ToBits())
			sums1 = sums1.Sub(lowerNeedle.LessEqual(lanes1).And(lanes1.Less(upperNeedle)).ToInt16x8().ToBits())
			sums2 = sums2.Sub(lowerNeedle.LessEqual(lanes2).And(lanes2.Less(upperNeedle)).ToInt16x8().ToBits())
			sums3 = sums3.Sub(lowerNeedle.LessEqual(lanes3).And(lanes3.Less(upperNeedle)).ToInt16x8().ToBits())
			remaining = remaining[40:]
		}
		matched += int(reduceUint16x8AVX2(sums0)) + int(reduceUint16x8AVX2(sums1)) +
			int(reduceUint16x8AVX2(sums2)) + int(reduceUint16x8AVX2(sums3))
		if row < chunkEnd {
			tail := chunkEnd - row
			matched += countCompactPacked10BetweenScalar(remaining, tail, lower, upper)
			remaining = remaining[(tail*10+7)/8:]
			row = chunkEnd
		}
	}
	return matched
}

func countCompactPacked8BetweenAVX2(
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
	lowerNeedle := archsimd.BroadcastUint8x32(uint8(lower))
	upperNeedle := archsimd.BroadcastUint8x32(uint8(upper))
	row := 0
	remaining := data
	for row < count {
		chunkEnd := count
		if chunkEnd-row > 4096 {
			chunkEnd = row + 4096
		}
		vectorEnd := row + (chunkEnd-row)/128*128
		var sums0, sums1, sums2, sums3 archsimd.Uint8x32
		for ; row < vectorEnd && len(remaining) >= 128; row += 128 {
			loaded0 := archsimd.LoadUint8x32Array((*[32]uint8)(remaining))
			loaded1 := archsimd.LoadUint8x32Array((*[32]uint8)(remaining[32:]))
			loaded2 := archsimd.LoadUint8x32Array((*[32]uint8)(remaining[64:]))
			loaded3 := archsimd.LoadUint8x32Array((*[32]uint8)(remaining[96:]))
			sums0 = sums0.Sub(lowerNeedle.LessEqual(loaded0).And(loaded0.Less(upperNeedle)).ToInt8x32().ToBits())
			sums1 = sums1.Sub(lowerNeedle.LessEqual(loaded1).And(loaded1.Less(upperNeedle)).ToInt8x32().ToBits())
			sums2 = sums2.Sub(lowerNeedle.LessEqual(loaded2).And(loaded2.Less(upperNeedle)).ToInt8x32().ToBits())
			sums3 = sums3.Sub(lowerNeedle.LessEqual(loaded3).And(loaded3.Less(upperNeedle)).ToInt8x32().ToBits())
			remaining = remaining[128:]
		}
		for ; chunkEnd-row >= 32 && len(remaining) >= 32; row += 32 {
			loaded := archsimd.LoadUint8x32Array((*[32]uint8)(remaining))
			sums0 = sums0.Sub(lowerNeedle.LessEqual(loaded).And(loaded.Less(upperNeedle)).ToInt8x32().ToBits())
			remaining = remaining[32:]
		}
		matched += reduceUint8x32AVX2(sums0, sums1, sums2, sums3)
		if row < chunkEnd {
			tail := chunkEnd - row
			matched += countCompactPacked8BetweenScalar(remaining, tail, lower, upper)
			remaining = remaining[tail:]
			row = chunkEnd
		}
	}
	return matched
}

func countCompactPacked16BetweenAVX2(
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
	lowerNeedle := archsimd.BroadcastUint16x16(uint16(lower))
	upperNeedle := archsimd.BroadcastUint16x16(uint16(upper))
	row := 0
	remaining := data
	for row < count {
		chunkEnd := count
		if chunkEnd-row > 4096 {
			chunkEnd = row + 4096
		}
		vectorEnd := row + (chunkEnd-row)/16*16
		var sums0, sums1 archsimd.Uint16x16
		for ; row < vectorEnd && len(remaining) >= 32; row += 16 {
			loaded := archsimd.LoadUint8x32Array((*[32]uint8)(remaining)).ReshapeToUint16s()
			sums0 = sums0.Sub(lowerNeedle.LessEqual(loaded).And(loaded.Less(upperNeedle)).ToInt16x16().ToBits())
			remaining = remaining[32:]
		}
		matched += int(reduceUint16x16AVX2(sums0, sums1))
		if row < chunkEnd {
			tail := chunkEnd - row
			matched += countCompactPacked16BetweenScalar(remaining, tail, lower, upper)
			remaining = remaining[tail*2:]
			row = chunkEnd
		}
	}
	return matched
}
