package storeio

import (
	"encoding/binary"
	"strconv"
	"unsafe"
)

// unifiedIntegerFORValue reads one value from an admitted FOR stream. The
// grouped lane deliberately keeps the same admission contract as the integer
// extrema lane: full-width and wrapping streams are exact JSON data, but their
// packed unsigned deltas do not preserve the signed ordering that the native
// path relies on, so the complete query declines to the ordinary executor.
func unifiedIntegerFORValue(v compactStreamView, row int) (int64, bool) {
	if !v.validIntegerFORData() || v.width == 64 || v.dictCount != 0 ||
		len(v.dictDir) != 0 || len(v.dictData) != 0 || row < 0 || row >= v.count {
		return 0, false
	}
	base := int64(binary.LittleEndian.Uint64(v.data))
	if v.width > 0 {
		maxDelta := (uint64(1) << v.width) - 1
		const maxInt64Value = uint64(1<<63 - 1)
		if base >= 0 && maxDelta > maxInt64Value-uint64(base) {
			return 0, false
		}
	}
	delta := compactReadBits(v.data[8:], row*int(v.width), int(v.width))
	return base + int64(delta), true
}

// unifiedCanonicalInt64Value admits only the canonical JSON integer spelling,
// while covering the full signed int64 range (the token fast path's narrower
// 18-digit admission is intentionally not reused here). Dictionary, alphabet,
// and front-coded compact streams retain their original spellings, so this is
// the exact numeric proof for those representations.
func unifiedCanonicalInt64Value(src []byte) (int64, bool) {
	if len(src) == 0 || len(src) > 20 {
		return 0, false
	}
	at := 0
	negative := src[0] == '-'
	if negative {
		at++
	}
	if at == len(src) || src[at] < '0' || src[at] > '9' {
		return 0, false
	}
	if src[at] == '0' && at+1 != len(src) {
		return 0, false
	}
	if negative && src[at] == '0' {
		return 0, false
	}
	limit := uint64(1<<63 - 1)
	if negative {
		limit++
	}
	var value uint64
	for ; at < len(src); at++ {
		if src[at] < '0' || src[at] > '9' {
			return 0, false
		}
		digit := uint64(src[at] - '0')
		if value > (limit-digit)/10 {
			return 0, false
		}
		value = value*10 + digit
	}
	if negative {
		if value == uint64(1)<<63 {
			return -1 << 63, true
		}
		return -int64(value), true
	}
	return int64(value), true
}

func unifiedCheckedIntegerAdd(a, b int64) (int64, bool) {
	const minInt64 = -1 << 63
	const maxInt64 = 1<<63 - 1
	if b > 0 && a > maxInt64-b || b < 0 && a < minInt64-b {
		return 0, false
	}
	return a + b, true
}

func unifiedDeltaIntegerValue(v compactStreamView, row int) (int64, bool) {
	if v.kind != compactStreamDelta || row < 0 || row >= v.count {
		return 0, false
	}
	blocks := (v.count + compactStreamRestart - 1) / compactStreamRestart
	block := row / compactStreamRestart
	if block < 0 || block >= blocks || len(v.data) < 4*blocks {
		return 0, false
	}
	cursor := int(binary.LittleEndian.Uint32(v.data[block*4:]))
	if cursor < 0 || cursor > len(v.data)-8 {
		return 0, false
	}
	value := int64(binary.LittleEndian.Uint64(v.data[cursor:]))
	cursor += 8
	for at := block * compactStreamRestart; at < row; at++ {
		u, n, ok := readCompactUvarint(v.data[cursor:])
		if !ok || n <= 0 || n > len(v.data)-cursor {
			return 0, false
		}
		cursor += n
		delta := int64(u>>1) ^ -int64(u&1)
		value, ok = unifiedCheckedIntegerAdd(value, delta)
		if !ok {
			return 0, false
		}
	}
	return value, true
}

func unifiedPackedDeltaIntegerValue(v compactStreamView, row int) (int64, bool) {
	if v.kind != compactStreamDeltaPack || row < 0 || row >= v.count {
		return 0, false
	}
	blocks := (v.count + compactStreamRestart - 1) / compactStreamRestart
	block := row / compactStreamRestart
	if block < 0 || block >= blocks || len(v.data) < 4*blocks {
		return 0, false
	}
	cursor := int(binary.LittleEndian.Uint32(v.data[block*4:]))
	if cursor < 0 || cursor > len(v.data)-9 {
		return 0, false
	}
	value := int64(binary.LittleEndian.Uint64(v.data[cursor:]))
	width := int(v.data[cursor+8])
	packed := v.data[cursor+9:]
	for at := block * compactStreamRestart; at < row; at++ {
		u := compactReadBits(packed, (at-block*compactStreamRestart)*width, width)
		delta := int64(u>>1) ^ -int64(u&1)
		var ok bool
		value, ok = unifiedCheckedIntegerAdd(value, delta)
		if !ok {
			return 0, false
		}
	}
	return value, true
}

func unifiedCompactIntegerValue(v compactStreamView, row int) (int64, bool) {
	if row < 0 || row >= v.count {
		return 0, false
	}
	switch v.kind {
	case compactStreamRankAffine:
		if v.rankAffineIsNumber() {
			return v.rankAffineInteger(row)
		}
		return 0, false
	case compactStreamFOR:
		return unifiedIntegerFORValue(v, row)
	case compactStreamDelta:
		return unifiedDeltaIntegerValue(v, row)
	case compactStreamDeltaPack:
		return unifiedPackedDeltaIntegerValue(v, row)
	case compactStreamDictionary:
		id := int(compactReadBits(v.data, row*int(v.width), int(v.width)))
		raw, ok := v.dictionaryEntry(id)
		if !ok {
			return 0, false
		}
		return unifiedCanonicalInt64Value(raw)
	case compactStreamFront:
		return unifiedFrontIntegerValue(v, row)
	case compactStreamPrefixInt, compactStreamAlphabet:
		// Reject long spellings BEFORE appendValue can grow its destination.
		// Every int64 needs at most twenty bytes including its sign.
		var scratch [20]byte
		if v.kind == compactStreamPrefixInt {
			value, ok := v.prefixInteger(row)
			if !ok || value < 0 {
				return 0, false
			}
			prefix, _ := v.dictionaryEntry(0)
			suffix, _ := v.dictionaryEntry(1)
			digits := len(strconv.AppendInt(scratch[:0], value, 10))
			if v.data[0]&1 != 0 {
				digits = max(digits, int(v.data[1]))
			}
			if len(prefix)+len(suffix)+digits > len(scratch) {
				return 0, false
			}
		} else {
			_, prefix, suffix, ok := v.alphabetParts()
			if !ok || len(prefix)+len(suffix) > len(scratch) {
				return 0, false
			}
			base, lengths, _, _, width := v.alphabetBlock(row / compactStreamRestart)
			delta := compactReadBits(lengths, (row%compactStreamRestart)*width, width)
			remaining := uint64(len(scratch) - len(prefix) - len(suffix))
			if base > remaining || delta > remaining-base {
				return 0, false
			}
		}
		raw, ok := v.appendValue(scratch[:0], row)
		if !ok {
			return 0, false
		}
		return unifiedCanonicalInt64Value(raw)
	default:
		return 0, false
	}
}

// Front decoding reconstructs at most twenty bytes, including intermediate
// restart values. A long intermediate spelling declines even when a later
// value is short; the generic scan remains authoritative for that stream.
func unifiedFrontIntegerValue(v compactStreamView, row int) (int64, bool) {
	var scratch [20]byte
	block := row / compactStreamRestart
	cursor := int(binary.LittleEndian.Uint32(v.data[block*4:]))
	length, n, ok := readCompactUvarint(v.data[cursor:])
	if !ok || length > uint64(len(scratch)) {
		return 0, false
	}
	cursor += n
	raw := scratch[:int(length)]
	copy(raw, v.data[cursor:cursor+int(length)])
	cursor += int(length)
	for at := block * compactStreamRestart; at < row; at++ {
		packed := v.data[cursor]
		cursor++
		prefix, suffix := uint64(packed>>4), uint64(packed&15)
		if packed == 0xff {
			prefix, n, ok = readCompactUvarint(v.data[cursor:])
			if !ok {
				return 0, false
			}
			cursor += n
			suffix, n, ok = readCompactUvarint(v.data[cursor:])
			if !ok {
				return 0, false
			}
			cursor += n
		}
		if prefix > uint64(len(raw)) || suffix > uint64(len(scratch))-prefix {
			return 0, false
		}
		raw = scratch[:int(prefix+suffix)]
		copy(raw[int(prefix):], v.data[cursor:cursor+int(suffix)])
		cursor += int(suffix)
	}
	return unifiedCanonicalInt64Value(raw)
}

func unifiedCompactIntegerStreamExact(v compactStreamView) bool {
	if v.kind == compactStreamRankAffine {
		return v.rankAffineIsNumber()
	}
	if v.count < 0 {
		return false
	}
	for row := 0; row < v.count; row++ {
		if _, ok := unifiedCompactIntegerValue(v, row); !ok {
			return false
		}
	}
	return true
}

// compactIntegerStreamAt resolves one scalar hole in a compact shape and
// admits it only when the selected stream contains exact canonical integers.
// FOR is the common wide-column representation; dictionary, alphabet, and
// front-coded integer streams are also safe when every spelling proves to be
// an int64, which covers low-cardinality SQL grouping columns without JSON
// reconstruction. The compact stripe validator has already checked every
// stream's envelope; this helper repeats the bounded stream walk needed to
// reach the target hole.
func compactIntegerStreamAt(
	entry compactPrimaryShapeView, hole, leafRows int,
) (compactStreamView, bool) {
	if hole < 0 || hole >= entry.template.holes {
		return compactStreamView{}, false
	}
	raw := entry.streamRaw
	for at := 0; at <= hole; at++ {
		stream, admitted := admittedCompactStream(raw)
		if !admitted || !stream.matchesShapeRows(entry.rows, leafRows) {
			return compactStreamView{}, false
		}
		if at == hole {
			// Validate every retained spelling before the caller publishes any
			// row of this shape. FOR remains the fastest representation; the
			// exact integer dictionary/alphabet forms used by low-cardinality
			// SQL columns are admitted without rebuilding JSON or Segments.
			if !unifiedCompactIntegerStreamExact(stream) {
				return compactStreamView{}, false
			}
			return stream, true
		}
		raw = raw[stream.encoded:]
	}
	return compactStreamView{}, false
}

// IntegerGroupShapeWorkspace is caller-owned per-shape scratch for one
// compact grouped scan. Its fields intentionally remain private; the type is
// exported only so durable Snapshot can retain and reuse the bounded table.
type IntegerGroupShapeWorkspace struct {
	rows  int
	group compactStreamView
	sum   compactStreamView
}

// IntegerGroupScratchBytes reports the retained caller-owned bytes needed for
// shape-local ordinals and compact stream views. It intentionally excludes the
// page-backed byte slices those views point into: the durable cursor owns those
// frames, and clears every view before returning to avoid retaining them.
func IntegerGroupScratchBytes(shapeCount int) int64 {
	if shapeCount <= 0 {
		return 0
	}
	per := uint64(unsafe.Sizeof(int(0)) + unsafe.Sizeof(IntegerGroupShapeWorkspace{}))
	if uint64(shapeCount) > uint64(^uint64(0)>>1)/per {
		return int64(^uint64(0) >> 1)
	}
	return int64(uint64(shapeCount) * per)
}

// VisitResolvedIntegerGroups visits one compact stripe in physical row order,
// yielding the resolved integer group key and, when sum is non-nil, the
// resolved integer SUM value. shapeSeen is caller-owned reusable storage; it
// carries one ordinal per shape so a multi-shape stripe never reconstructs a
// row or performs a random rank search for every value.
//
// The operation is strict and atomic at stripe granularity. Any overflow row,
// absent/container target, non-integer compact spelling, malformed stream, or
// wrapping FOR stream returns supported=false after discarding local progress.
// The caller may then run the authoritative generic executor over the complete
// snapshot.
func (v *CompactPrimaryStripeView) VisitResolvedIntegerGroups(
	groupResolver, sumResolver *UnifiedHoleResolver,
	shapeSeen []int, shapeWork []IntegerGroupShapeWorkspace,
	visit func(row int, group, sum int64) error,
) (supported bool, err error) {
	if v == nil || groupResolver == nil || visit == nil ||
		len(v.overflow) != 0 || len(shapeSeen) < v.shapeCount ||
		len(shapeWork) < v.shapeCount {
		return false, nil
	}
	// Admission precedes every ordinal read, so its temporary decoded keys
	// can borrow the already charged integer arena. The memory contains no
	// pointers, and the byte view stays within the supplied slice's bounds.
	keySlots := min(len(shapeSeen), 1024/int(unsafe.Sizeof(int(0))))
	keyScratch := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(shapeSeen))),
		keySlots*int(unsafe.Sizeof(int(0))))
	usedOrdinals := shapeSeen[:max(v.shapeCount, keySlots)]
	defer func() {
		if !supported {
			clear(usedOrdinals)
		}
	}()
	for shape := 0; shape < v.shapeCount; shape++ {
		entry, ok := v.shapeEntry(shape)
		if !ok || entry.rows < 0 {
			return false, nil
		}
		groupHole := resolveCompactProjectionTemplate(groupResolver, entry.template, keyScratch)
		if groupHole < 0 || groupHole >= entry.template.holes {
			// UnifiedHoleAbsent and UnifiedHoleContainer are both negative;
			// neither has SQL's all-integer semantics required by this lane.
			return false, nil
		}
		groupStream, ok := compactIntegerStreamAt(entry, groupHole, v.rows)
		if !ok {
			return false, nil
		}
		var sumStream compactStreamView
		if sumResolver != nil {
			sumHole := resolveCompactProjectionTemplate(sumResolver, entry.template, keyScratch)
			if sumHole < 0 || sumHole >= entry.template.holes {
				return false, nil
			}
			if sumHole == groupHole {
				sumStream = groupStream
			} else {
				sumStream, ok = compactIntegerStreamAt(entry, sumHole, v.rows)
				if !ok {
					return false, nil
				}
			}
		}
		shapeWork[shape] = IntegerGroupShapeWorkspace{
			rows: entry.rows, group: groupStream, sum: sumStream,
		}
	}
	// Prepare every target stream before invoking the first callback. The row
	// loop then follows physical order, while shapeSeen carries each stream's
	// local ordinal. A shape-major callback order would alter first-seen group
	// ordering for unordered output whenever shapes are interleaved.
	clear(usedOrdinals)
	for row := 0; row < v.rows; row++ {
		shape := v.rowShape(row)
		if shape < 0 || shape >= v.shapeCount {
			return false, nil
		}
		ordinal := shapeSeen[shape]
		shapeSeen[shape]++
		entry := &shapeWork[shape]
		if ordinal >= entry.rows {
			return false, nil
		}
		group, ok := unifiedCompactIntegerValue(entry.group, entry.group.shapeCoordinate(row, ordinal))
		if !ok {
			return false, nil
		}
		var sum int64
		if sumResolver != nil {
			sum, ok = unifiedCompactIntegerValue(entry.sum, entry.sum.shapeCoordinate(row, ordinal))
			if !ok {
				return false, nil
			}
		}
		if err := visit(row, group, sum); err != nil {
			return false, err
		}
	}
	for shape := 0; shape < v.shapeCount; shape++ {
		if shapeSeen[shape] != shapeWork[shape].rows {
			return false, nil
		}
	}
	return true, nil
}
