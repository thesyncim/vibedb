package storeio

import (
	"bytes"
	"encoding/binary"
	"math"
	"slices"
	"strconv"
)

// Rank-affine streams use physical leaf rank, unlike ordinary streams, which
// use the ordinal within one JSON shape. Their count certifies the full leaf
// domain. Keeping the coordinate choice explicit prevents shape gaps from
// changing a stored value or causing a predicate to count another shape.
func (v *compactStreamView) shapeCoordinate(rank, ordinal int) int {
	if v.kind == compactStreamRankAffine {
		return rank
	}
	return ordinal
}

func (v *compactStreamView) matchesShapeRows(shapeRows, leafRows int) bool {
	if v.kind == compactStreamRankAffine {
		return v.count == leafRows
	}
	return v.count == shapeRows
}

func (v compactStreamView) validRankAffine() bool {
	if v.kind != compactStreamRankAffine || v.width != 0 || v.count < 2 ||
		v.count > CompactPrimaryStripeMaxRows || v.dictCount != 2 ||
		len(v.dictDir) != 4 || len(v.data) != 18 ||
		(v.data[0] != 2 && v.data[0] != 3) ||
		(v.data[0] == 2 && v.data[1] != 0) ||
		(v.data[0] == 3 && v.data[1] == 0) {
		return false
	}
	prefix, pOK := v.dictionaryEntry(0)
	suffix, sOK := v.dictionaryEntry(1)
	if !pOK || !sOK {
		return false
	}
	// The numeric component is the one digit run selected by the writer.
	for _, part := range [][]byte{prefix, suffix} {
		for _, b := range part {
			if b >= '0' && b <= '9' {
				return false
			}
		}
	}
	first := int64(binary.LittleEndian.Uint64(v.data[2:]))
	step := int64(binary.LittleEndian.Uint64(v.data[10:]))
	if !compactRankAffineDomain(first, step, v.count) {
		return false
	}
	steps := int64(v.count - 1)
	last := first + step*steps
	if v.data[0]&1 != 0 {
		var scratch [20]byte
		width := int(v.data[1])
		if len(strconv.AppendInt(scratch[:0], first, 10)) > width ||
			len(strconv.AppendInt(scratch[:0], last, 10)) > width {
			return false
		}
	}
	return true
}

// rankAffineInteger returns the already-admitted monotone value in O(1).
// The full-domain endpoint check proves that multiplication and addition
// cannot overflow for any legal rank, including ranks of other shapes.
func (v compactStreamView) rankAffineInteger(rank int) (int64, bool) {
	if v.kind != compactStreamRankAffine || rank < 0 || rank >= v.count || len(v.data) != 18 {
		return 0, false
	}
	first := int64(binary.LittleEndian.Uint64(v.data[2:]))
	step := int64(binary.LittleEndian.Uint64(v.data[10:]))
	return first + step*int64(rank), true
}

// Numeric fast paths accept only the bare canonical nonnegative-integer
// spelling. Affixed strings retain their exact bytes without a numeric cast.
func (v compactStreamView) rankAffineIsNumber() bool {
	if v.kind != compactStreamRankAffine || len(v.data) != 18 || v.data[0] != 2 {
		return false
	}
	prefix, pOK := v.dictionaryEntry(0)
	suffix, sOK := v.dictionaryEntry(1)
	return pOK && sOK && len(prefix) == 0 && len(suffix) == 0
}

func (v compactStreamView) rankAffineFindInteger(value int64) (int, bool) {
	if v.kind != compactStreamRankAffine || value < 0 || len(v.data) != 18 {
		return 0, false
	}
	base := int64(binary.LittleEndian.Uint64(v.data[2:]))
	step := int64(binary.LittleEndian.Uint64(v.data[10:]))
	if step == 0 {
		return 0, false
	}
	// Both value and base are nonnegative signed integers, so subtraction
	// stays inside the signed domain; MinInt64/-1 is impossible.
	delta := value - base
	if delta%step != 0 {
		return 0, false
	}
	rank := delta / step
	return int(rank), rank >= 0 && rank < int64(v.count)
}

func (v compactStreamView) rankAffineFindSpelling(needle []byte) (int, bool) {
	parsed, ok := parseCompactPrefixInt(needle)
	if !ok || v.kind != compactStreamRankAffine || len(v.data) != 18 {
		return 0, false
	}
	prefix, pOK := v.dictionaryEntry(0)
	suffix, sOK := v.dictionaryEntry(1)
	if !pOK || !sOK || !bytes.Equal(prefix, parsed.prefix) || !bytes.Equal(suffix, parsed.suffix) ||
		v.data[0]&1 != 0 && parsed.width != int(v.data[1]) ||
		v.data[0]&1 == 0 && !parsed.canonical {
		return 0, false
	}
	return v.rankAffineFindInteger(int64(parsed.value))
}

// countShapeBefore uses the existing rank checkpoints. Only the final
// 64-row block is decoded; no affine value stream is expanded for a count.
func (v *CompactPrimaryStripeView) countShapeBefore(shape, end int) int {
	if end <= 0 {
		return 0
	}
	if end >= v.rows {
		entry, ok := v.shapeEntry(shape)
		if !ok {
			return 0
		}
		return entry.rows
	}
	return v.shapeOrdinal(end, shape)
}

func (v *CompactPrimaryStripeView) rankAffineShapeEqual(stream compactStreamView, shape int, value int64) int {
	rank, found := stream.rankAffineFindInteger(value)
	if found && v.rowShape(rank) == shape {
		return 1
	}
	return 0
}

func (v *CompactPrimaryStripeView) rankAffineShapeSpelling(stream compactStreamView, shape int, value []byte) int {
	rank, found := stream.rankAffineFindSpelling(value)
	if found && v.rowShape(rank) == shape {
		return 1
	}
	return 0
}

// A monotone arithmetic sequence maps every ordered predicate to a physical
// rank prefix or suffix. Binary search avoids signed division corner cases;
// shape rank checkpoints intersect it with this stream's actual shape.
func (v *CompactPrimaryStripeView) rankAffineShapeOrdered(stream compactStreamView, shape int, needle int64, op UnifiedIntegerOrder) (int, bool) {
	if !stream.rankAffineIsNumber() || !validUnifiedIntegerOrder(op) {
		return 0, false
	}
	predicate := func(rank int) bool {
		value, _ := stream.rankAffineInteger(rank)
		switch op {
		case UnifiedIntegerLess:
			return value < needle
		case UnifiedIntegerLessEqual:
			return value <= needle
		case UnifiedIntegerGreater:
			return value > needle
		default:
			return value >= needle
		}
	}
	first, last := predicate(0), predicate(v.rows-1)
	total := v.countShapeBefore(shape, v.rows)
	if first == last {
		if first {
			return total, true
		}
		return 0, true
	}
	lo, hi := 0, v.rows
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if predicate(mid) == first {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	prefix := v.countShapeBefore(shape, lo)
	if first {
		return prefix, true
	}
	return total - prefix, true
}

func (v *CompactPrimaryStripeView) rankAffineShapeInterval(stream compactStreamView, shape int, interval UnifiedIntegerInterval) (int, bool) {
	if !stream.rankAffineIsNumber() {
		return 0, false
	}
	if !interval.UpperUnbounded && interval.Upper <= interval.Lower {
		return 0, true
	}
	lower, _ := v.rankAffineShapeOrdered(stream, shape, interval.Lower, UnifiedIntegerGreaterEqual)
	if interval.UpperUnbounded {
		return lower, true
	}
	upper, _ := v.rankAffineShapeOrdered(stream, shape, interval.Upper, UnifiedIntegerGreaterEqual)
	return lower - upper, true
}

// Select a shape ordinal using the existing restart table, then inspect only
// one 64-row block. Sparse shapes do not require a whole-leaf search.
func (v *CompactPrimaryStripeView) shapeRank(shape, ordinal int) (int, bool) {
	total := v.countShapeBefore(shape, v.rows)
	if ordinal < 0 || ordinal >= total {
		return 0, false
	}
	lo, hi := 0, (v.rows+compactStreamRestart-1)/compactStreamRestart
	for lo < hi {
		mid := (lo + hi) / 2
		if v.countShapeBefore(shape, mid*compactStreamRestart) <= ordinal {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	start := (lo - 1) * compactStreamRestart
	seen := v.countShapeBefore(shape, start)
	for rank := start; rank < min(start+compactStreamRestart, v.rows); rank++ {
		if v.rowShape(rank) == shape {
			if seen == ordinal {
				return rank, true
			}
			seen++
		}
	}
	return 0, false
}

func (v *CompactPrimaryStripeView) rankAffineShapeExtrema(stream compactStreamView, shape int) (minimum, maximum int64, found, supported bool) {
	if !stream.rankAffineIsNumber() {
		return 0, 0, false, false
	}
	total := v.countShapeBefore(shape, v.rows)
	if total == 0 {
		return 0, 0, false, true
	}
	first, firstOK := v.shapeRank(shape, 0)
	last, lastOK := v.shapeRank(shape, total-1)
	if !firstOK || !lastOK {
		return 0, 0, false, false
	}
	a, _ := stream.rankAffineInteger(first)
	b, _ := stream.rankAffineInteger(last)
	return min(a, b), max(a, b), true, true
}

// compactRankAffineDomain proves every intermediate value using the two
// endpoints. Division occurs before multiplication, including descending data.
func compactRankAffineDomain(first, step int64, count int) bool {
	if first < 0 || step == 0 || count < 2 || count > CompactPrimaryStripeMaxRows {
		return false
	}
	steps := int64(count - 1)
	if step > 0 {
		return step <= (math.MaxInt64-first)/steps
	}
	return step != math.MinInt64 && -step <= first/steps
}

// The ordinary prefix parser already proved every spelling and populated
// parsed. This candidate reuses those values and avoids building a packed
// delta stream when physical ranks account exactly for the shape gaps.
func (s *compactStreamScratch) encodeRankAffineParsed(slot int, first compactPrefixIntValue, allCanonical, fixedWidth bool, ranks []uint16, leafRows int) (compactStreamEncoding, bool) {
	// Signed canonical numbers retain the existing signed integer codecs. A
	// shared minus affix is not the bare numeric certificate used below.
	if len(first.prefix) == 1 && first.prefix[0] == '-' && len(first.suffix) == 0 {
		return compactStreamEncoding{}, false
	}
	parsed := s.parsed
	if len(parsed) < compactStreamRestart || len(ranks) != len(parsed) ||
		leafRows <= len(parsed) || leafRows > CompactPrimaryStripeMaxRows {
		return compactStreamEncoding{}, false
	}
	gap := int64(ranks[1]) - int64(ranks[0])
	delta := int64(parsed[1]) - int64(parsed[0])
	if gap <= 0 || delta == 0 || delta%gap != 0 {
		return compactStreamEncoding{}, false
	}
	step := delta / gap
	base := int64(first.value)
	rank0 := int64(ranks[0])
	if rank0 != 0 {
		if step > 0 {
			if step > base/rank0 {
				return compactStreamEncoding{}, false
			}
		} else if -step > (math.MaxInt64-base)/rank0 {
			return compactStreamEncoding{}, false
		}
		base -= step * rank0
	}
	if !compactRankAffineDomain(base, step, leafRows) {
		return compactStreamEncoding{}, false
	}
	for row, rank := range ranks {
		if int(rank) >= leafRows || row != 0 && rank <= ranks[row-1] ||
			base+step*int64(rank) != int64(parsed[row]) {
			return compactStreamEncoding{}, false
		}
	}
	// Bare canonical numbers must remain admitted by every native integer
	// query. Quoted/affixed fixed-width values keep the optimized renderer.
	fixedWidth = fixedWidth && !(allCanonical && len(first.prefix) == 0 && len(first.suffix) == 0)
	if fixedWidth {
		var scratch [20]byte
		last := base + step*int64(leafRows-1)
		if first.width > math.MaxUint8 ||
			len(strconv.AppendInt(scratch[:0], base, 10)) > first.width ||
			len(strconv.AppendInt(scratch[:0], last, 10)) > first.width {
			return compactStreamEncoding{}, false
		}
	} else if !allCanonical {
		return compactStreamEncoding{}, false
	}
	data := slices.Grow(s.data[slot][:0], 18)[:18]
	clear(data)
	data[0] = 2
	if fixedWidth {
		data[0], data[1] = 3, byte(first.width)
	}
	binary.LittleEndian.PutUint64(data[2:], uint64(base))
	binary.LittleEndian.PutUint64(data[10:], uint64(step))
	dictionary := slices.Grow(s.dict[slot][:0], 2)[:2]
	dictionary[0], dictionary[1] = first.prefix, first.suffix
	s.data[slot], s.dict[slot] = data, dictionary
	return compactStreamEncoding{kind: compactStreamRankAffine, count: leafRows, data: data, dict: dictionary}, true
}
