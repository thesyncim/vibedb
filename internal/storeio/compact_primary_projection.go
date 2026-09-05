package storeio

import (
	"bytes"
	"encoding/binary"
	"math"
	"strconv"
	"unsafe"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// UnifiedProjectionMaxShapes is the largest shape set admitted by the
// bounded projection lane.  The query layer owns arrays of this size and the
// durable wrapper never replaces undersized caller storage with hidden
// allocations.
const UnifiedProjectionMaxShapes = compactStreamRestart

// UnifiedProjectionField is a scalar JSON spelling borrowed for one callback.
// The bytes are backed by the caller's projection scratch and must be copied
// before the callback returns.
type UnifiedProjectionField struct {
	JSON []byte
}

// UnifiedProjectionShapeWorkspace is caller-owned metadata for one compact
// shape. The fields are private so page-backed stream views cannot escape the
// storage callback through a durable wrapper.
type UnifiedProjectionShapeWorkspace struct {
	rows        int
	start       int
	prepared    bool
	unsupported bool
}

// UnifiedProjectionStreamWorkspace is one caller-owned slot for a shape's
// selected scalar stream. It is exported as a storage-neutral type so query
// execution can preallocate a flat stream arena without knowing its layout.
type UnifiedProjectionStreamWorkspace struct {
	hole int
	view compactStreamView
}

// UnifiedProjectionScratchBytes reports the fixed metadata bytes needed for a
// projected scan. The value excludes page-backed stream data and the caller's
// JSON value scratch, both of which are bounded independently by the caller.
func UnifiedProjectionScratchBytes(shapeCount, fieldCount int) int64 {
	if shapeCount <= 0 || fieldCount <= 0 {
		return 0
	}
	shapeBytes := uint64(unsafe.Sizeof(UnifiedProjectionShapeWorkspace{})) +
		uint64(unsafe.Sizeof(int(0)))
	streamBytes := uint64(unsafe.Sizeof(UnifiedProjectionStreamWorkspace{}))
	fieldBytes := uint64(unsafe.Sizeof(UnifiedProjectionField{}))
	if uint64(shapeCount) > uint64(^uint64(0)>>1)/shapeBytes {
		return int64(^uint64(0) >> 1)
	}
	shapes := uint64(shapeCount) * shapeBytes
	if uint64(shapeCount) > uint64(^uint64(0)>>1)/uint64(fieldCount) {
		return int64(^uint64(0) >> 1)
	}
	streamCount := uint64(shapeCount) * uint64(fieldCount)
	if streamCount > (uint64(^uint64(0)>>1)-shapes)/streamBytes {
		return int64(^uint64(0) >> 1)
	}
	metadata := shapes + streamCount*streamBytes
	if uint64(fieldCount) > (uint64(^uint64(0)>>1)-metadata)/fieldBytes {
		return int64(^uint64(0) >> 1)
	}
	metadata += uint64(fieldCount) * fieldBytes
	return int64(metadata)
}

// compactProjectionValueLen computes the exact canonical spelling length
// without appending to the caller's value scratch.  This is deliberately
// separate from appendValue: a long dictionary/front/alphabet string must
// decline before appendValue can grow a borrowed destination and invalidate a
// previous field view in the same row.
func compactProjectionValueLen(v compactStreamView, row int) (length, peak int, ok bool) {
	if row < 0 || row >= v.count {
		return 0, 0, false
	}
	safeAdd := func(parts ...int) (int, bool) {
		n := 0
		for _, part := range parts {
			if part < 0 || n > math.MaxInt-part {
				return 0, false
			}
			n += part
		}
		return n, true
	}
	switch v.kind {
	case compactStreamDictionary:
		id := int(compactReadBits(v.data, row*int(v.width), int(v.width)))
		value, ok := v.dictionaryEntry(id)
		if !ok {
			return 0, 0, false
		}
		return len(value), len(value), true
	case compactStreamFront:
		block := row / compactStreamRestart
		if block < 0 || block*4+4 > len(v.data) {
			return 0, 0, false
		}
		cursor := int(binary.LittleEndian.Uint32(v.data[block*4:]))
		if cursor < 0 || cursor > len(v.data) {
			return 0, 0, false
		}
		length, n, ok := readCompactUvarint(v.data[cursor:])
		if !ok || n <= 0 || n > len(v.data)-cursor ||
			length > uint64(len(v.data)-cursor-n) {
			return 0, 0, false
		}
		cursor += n + int(length)
		current := int(length)
		peak := current
		for at := block*compactStreamRestart + 1; at <= row; at++ {
			if cursor >= len(v.data) {
				return 0, 0, false
			}
			packed := v.data[cursor]
			cursor++
			prefix, suffix := int(packed>>4), int(packed&15)
			if packed == 0xff {
				var n int
				var ok bool
				var p, s uint64
				p, n, ok = readCompactUvarint(v.data[cursor:])
				if !ok || n <= 0 || n > len(v.data)-cursor || p > uint64(math.MaxInt) {
					return 0, 0, false
				}
				cursor += n
				s, n, ok = readCompactUvarint(v.data[cursor:])
				if !ok || n <= 0 || n > len(v.data)-cursor || s > uint64(math.MaxInt) {
					return 0, 0, false
				}
				cursor += n
				prefix, suffix = int(p), int(s)
			}
			if prefix < 0 || prefix > current || suffix > len(v.data)-cursor {
				return 0, 0, false
			}
			var next int
			next, ok = safeAdd(prefix, suffix)
			if !ok {
				return 0, 0, false
			}
			current = next
			if current > peak {
				peak = current
			}
			cursor += suffix
		}
		return current, peak, true
	case compactStreamFOR:
		if v.width == 64 {
			return 0, 0, false
		}
		value, ok := unifiedIntegerFORValue(v, row)
		if !ok {
			return 0, 0, false
		}
		var digits [20]byte
		nDigits := len(strconv.AppendInt(digits[:0], value, 10))
		return nDigits, nDigits, true
	case compactStreamDelta:
		value, ok := unifiedDeltaIntegerValue(v, row)
		if !ok {
			return 0, 0, false
		}
		var digits [20]byte
		nDigits := len(strconv.AppendInt(digits[:0], value, 10))
		return nDigits, nDigits, true
	case compactStreamDeltaPack:
		value, ok := unifiedPackedDeltaIntegerValue(v, row)
		if !ok {
			return 0, 0, false
		}
		var digits [20]byte
		nDigits := len(strconv.AppendInt(digits[:0], value, 10))
		return nDigits, nDigits, true
	case compactStreamDate:
		if v.width > 32 || len(v.data) < 4 {
			return 0, 0, false
		}
		return 12, 12, true // quoted YYYY-MM-DD
	case compactStreamPrefixInt:
		if len(v.data) < 2 {
			return 0, 0, false
		}
		value, ok := v.prefixInteger(row)
		if !ok || value < 0 {
			return 0, 0, false
		}
		prefix, okPrefix := v.dictionaryEntry(0)
		suffix, okSuffix := v.dictionaryEntry(1)
		if !okPrefix || !okSuffix {
			return 0, 0, false
		}
		var digits [20]byte
		width := len(strconv.AppendUint(digits[:0], uint64(value), 10))
		if v.data[0]&1 != 0 {
			if len(v.data) < 2 || int(v.data[1]) < width {
				return 0, 0, false
			}
			width = int(v.data[1])
		}
		length, ok := safeAdd(len(prefix), width, len(suffix))
		return length, length, ok
	case compactStreamAlphabet:
		_, prefix, suffix, ok := v.alphabetParts()
		if !ok {
			return 0, 0, false
		}
		block := row / compactStreamRestart
		base, lengths, _, rows, width := v.alphabetBlock(block)
		if row%compactStreamRestart >= rows {
			return 0, 0, false
		}
		delta := compactReadBits(lengths, (row%compactStreamRestart)*width, width)
		if delta > math.MaxUint64-base {
			return 0, 0, false
		}
		length := base + delta
		if length > uint64(math.MaxInt) {
			return 0, 0, false
		}
		lengthInt, ok := safeAdd(len(prefix), int(length), len(suffix))
		return lengthInt, lengthInt, ok
	}
	return 0, 0, false
}

// compactProjectionStreamAt resolves one scalar hole in a compact shape.
// CompactPrimaryStripe admission has already validated the enclosing stream;
// this helper repeats only the bounded stream walk needed to reach the hole.
func compactProjectionStreamAt(
	entry compactPrimaryShapeView, hole int,
) (compactStreamView, bool) {
	if hole < 0 || hole >= entry.template.holes {
		return compactStreamView{}, false
	}
	streamRaw := entry.streamRaw
	for at := 0; at <= hole; at++ {
		stream, admitted := admittedCompactStream(streamRaw)
		if !admitted || stream.count != entry.rows {
			return compactStreamView{}, false
		}
		if at == hole {
			return stream, true
		}
		streamRaw = streamRaw[stream.encoded:]
	}
	return compactStreamView{}, false
}

// prepareUnifiedProjectionShape resolves all requested paths and records their
// compact streams before a row callback can publish a result. A path must name
// exactly one scalar hole in every admitted shape; absent and container paths
// decline so the generic executor remains authoritative for NULL/structure
// semantics.
func prepareUnifiedProjectionShape(
	v *CompactPrimaryStripeView,
	shape int,
	resolvers []UnifiedHoleResolver,
	meta *UnifiedProjectionShapeWorkspace,
	streams []UnifiedProjectionStreamWorkspace,
	keyScratch []byte,
) bool {
	if v == nil || meta == nil || shape < 0 || shape >= v.shapeCount ||
		len(streams) < len(resolvers) {
		return false
	}
	entry, ok := v.shapeEntry(shape)
	if !ok || entry.rows < 0 {
		meta.unsupported = true
		return false
	}
	meta.rows = entry.rows
	meta.start = 0
	meta.unsupported = false
	var skeleton [1024]byte
	var tape [128]vibejson.IndexEntry
	filled, index, ok := compactProjectionTemplateIndex(
		entry.template, skeleton[:], tape[:],
	)
	if !ok {
		meta.unsupported = true
		return false
	}
	for _, e := range index.Entries {
		if e.Flags()&vibejson.TapeFlagEscaped != 0 &&
			uint64(e.End-e.Start) > uint64(cap(keyScratch)) {
			meta.unsupported = true
			return false
		}
	}
	if !resolveCompactProjectionFieldsLast(
		filled, index, resolvers, keyScratch, streams,
	) {
		meta.unsupported = true
		return false
	}
	for field := range resolvers {
		hole := streams[field].hole
		if hole < 0 {
			meta.unsupported = true
			return false
		}
		stream, ok := compactProjectionStreamAt(entry, hole)
		if !ok {
			meta.unsupported = true
			return false
		}
		streams[field].view = stream
	}
	return true
}

// Projection admission must not grow the reusable filter when a later
// snapshot contains a wider template. Use fixed stack arenas and the existing
// path walker; shapes exceeding either arena use the generic reader.
func resolveCompactProjectionTemplate(resolver *UnifiedHoleResolver, entry compactPrimaryTemplateView, keyScratch []byte) int {
	var skeleton [1024]byte
	var tape [128]vibejson.IndexEntry
	filled, index, ok := compactProjectionTemplateIndex(
		entry, skeleton[:], tape[:],
	)
	if !ok {
		return UnifiedHoleContainer
	}
	for _, e := range index.Entries {
		if e.Flags()&vibejson.TapeFlagEscaped != 0 && uint64(e.End-e.Start) > uint64(cap(keyScratch)) {
			return UnifiedHoleContainer
		}
	}
	local := *resolver
	// Decoded keys cannot exceed their admitted spellings. Reuse the caller's
	// value arena during shape preparation so keyEquals cannot allocate.
	local.keyScratch = keyScratch[:0]
	_, result, matched, ok := local.resolveWalkLast(
		filled, index.Entries, 0, 0, 0,
	)
	if !ok || !matched {
		return UnifiedHoleAbsent
	}
	return result
}

// compactProjectionTemplateIndex fills one compact shape's holes with null
// and builds a single bounded tape. Callers keep skeleton and tape storage on
// their stack or in caller-owned workspace, so this helper performs no heap
// growth while preparing a shape.
func compactProjectionTemplateIndex(
	entry compactPrimaryTemplateView,
	skeleton []byte,
	tape []vibejson.IndexEntry,
) (filled []byte, index vibejson.Index, ok bool) {
	if entry.holes < 0 || len(entry.static) > len(skeleton) ||
		entry.holes > (len(skeleton)-len(entry.static))/4 ||
		entry.holes >= len(entry.ends)/4 {
		return nil, vibejson.Index{}, false
	}
	filled = skeleton[:0]
	previous := 0
	for segment := 0; segment <= entry.holes; segment++ {
		end := int(readUint32(entry.ends[segment*4:]))
		if end < previous || end > len(entry.static) {
			return nil, vibejson.Index{}, false
		}
		if segment > 0 {
			filled = append(filled, "null"...)
		}
		filled = append(filled, entry.static[previous:end]...)
		previous = end
	}
	index, err := vibejson.BuildIndex(filled, tape)
	if err != nil {
		return nil, vibejson.Index{}, false
	}
	return filled, index, true
}

// resolveCompactProjectionFieldsLast resolves every selected path against
// one already-built shape tape. Top-level paths use one object-member walk,
// which both shares duplicate-key comparisons and computes every selected
// hole ordinal from the same running count. Nested paths use the same tape and
// the SQL-LAST walker independently; they still avoid rebuilding the shape
// skeleton and index once per field.
func resolveCompactProjectionFieldsLast(
	src []byte,
	index vibejson.Index,
	resolvers []UnifiedHoleResolver,
	keyScratch []byte,
	streams []UnifiedProjectionStreamWorkspace,
) bool {
	if len(resolvers) == 0 || len(streams) < len(resolvers) ||
		len(index.Entries) == 0 {
		return false
	}
	for field := range resolvers {
		streams[field].hole = UnifiedHoleAbsent
		streams[field].view = compactStreamView{}
	}
	allTopLevel := true
	for field := range resolvers {
		if _, ok := resolvers[field].topLevelSegment(); !ok {
			allTopLevel = false
			break
		}
	}
	if allTopLevel {
		return resolveCompactProjectionTopLevelLast(
			src, index.Entries, resolvers, keyScratch, streams,
		)
	}
	for field := range resolvers {
		local := resolvers[field]
		local.keyScratch = keyScratch[:0]
		_, result, matched, ok := local.resolveWalkLast(
			src, index.Entries, 0, 0, 0,
		)
		if !ok || !matched {
			return false
		}
		streams[field].hole = result
	}
	return true
}

// resolveCompactProjectionTopLevelLast handles the common flat-object case
// with one pass over object members. It overwrites a field on each matching
// member, so a duplicate container, scalar, or explicit null follows the
// query resolver's last-member semantics. Unselected subtrees are skipped by
// their validated hole counts rather than recursively parsed per field.
func resolveCompactProjectionTopLevelLast(
	src []byte,
	entries []vibejson.IndexEntry,
	resolvers []UnifiedHoleResolver,
	keyScratch []byte,
	streams []UnifiedProjectionStreamWorkspace,
) bool {
	if len(entries) == 0 || entries[0].Kind() != document.Object {
		return false
	}
	root := &entries[0]
	child := 1
	hole := 0
	for range int(root.Count()) {
		if child < 0 || child+1 >= len(entries) {
			return false
		}
		key := &entries[child]
		value := child + 1
		if key.Flags()&vibejson.TapeFlagKey == 0 {
			return false
		}
		keyBytes, ok := compactProjectionKeyBytes(src, key, keyScratch)
		if !ok {
			return false
		}
		valueHoles, ok := compactProjectionSubtreeHoles(entries, value)
		if !ok {
			return false
		}
		candidate := hole
		if entries[value].Kind() == document.Array ||
			entries[value].Kind() == document.Object {
			candidate = UnifiedHoleContainer
		}
		for field := range resolvers {
			name, nameOK := resolvers[field].topLevelSegment()
			if nameOK && bytes.Equal(keyBytes, name) {
				streams[field].hole = candidate
			}
		}
		hole += valueHoles
		step := int(entries[value].Next)
		if step <= 0 || step > len(entries)-value {
			return false
		}
		child = value + step
	}
	return true
}

// compactProjectionKeyBytes returns one borrowed decoded key spelling for the
// shared top-level walk. The key scratch is caller-owned and bounded; escaped
// keys that cannot fit decline before vibejson is asked to append into it.
func compactProjectionKeyBytes(
	src []byte,
	key *vibejson.IndexEntry,
	scratch []byte,
) ([]byte, bool) {
	if key == nil || key.Start >= key.End || uint64(key.End) > uint64(len(src)) {
		return nil, false
	}
	raw := src[key.Start:key.End]
	if key.Flags()&vibejson.TapeFlagEscaped == 0 {
		if len(raw) < 2 {
			return nil, false
		}
		return raw[1 : len(raw)-1], true
	}
	if len(raw) > cap(scratch) {
		return nil, false
	}
	scratch = scratch[:0]
	node := vibejson.Node{Src: &src[0], Entry: key}
	scratch, _ = node.AppendText(scratch)
	return scratch, true
}

// VisitResolvedProjection visits selected scalar fields in physical row order.
// Values are appended to valueScratch and are valid only until visit returns.
// A positive limit stops before any later row is inspected; zero is an empty
// successful result and a negative limit means scan all rows.
func (v *CompactPrimaryStripeView) VisitResolvedProjection(
	resolvers []UnifiedHoleResolver,
	shapeSeen []int,
	shapeWork []UnifiedProjectionShapeWorkspace,
	streamWork []UnifiedProjectionStreamWorkspace,
	fields []UnifiedProjectionField,
	valueScratch []byte,
	limit int,
	visit func(row int, fields []UnifiedProjectionField) error,
) (supported, stopped bool, scratch []byte, err error) {
	if v == nil || len(resolvers) == 0 || len(fields) < len(resolvers) ||
		len(shapeSeen) < v.shapeCount || len(shapeWork) < v.shapeCount ||
		len(streamWork) < v.shapeCount*len(resolvers) || visit == nil {
		return false, false, valueScratch, nil
	}
	if limit == 0 || v.rows == 0 {
		return true, limit == 0, valueScratch, nil
	}
	clear(shapeSeen[:v.shapeCount])
	clear(shapeWork[:v.shapeCount])
	clear(streamWork[:v.shapeCount*len(resolvers)])
	for row := 0; row < v.rows; row++ {
		if limit > 0 && row >= limit {
			return true, true, valueScratch, nil
		}
		if v.IsOverflow(row) {
			return false, false, valueScratch, nil
		}
		shape := v.rowShape(row)
		if shape < 0 || shape >= v.shapeCount {
			return false, false, valueScratch, nil
		}
		if !shapeWork[shape].prepared {
			base := shape * len(resolvers)
			shapeWork[shape].prepared = true
			if !prepareUnifiedProjectionShape(
				v, shape, resolvers, &shapeWork[shape],
				streamWork[base:base+len(resolvers)], valueScratch,
			) {
				shapeWork[shape].unsupported = true
				return false, false, valueScratch, nil
			}
		}
		if shapeWork[shape].unsupported {
			return false, false, valueScratch, nil
		}
		ordinal := v.shapeOrdinal(row, shape)
		if ordinal < 0 || ordinal >= shapeWork[shape].rows {
			return false, false, valueScratch, nil
		}
		base := shape * len(resolvers)
		valueScratch = valueScratch[:0]
		for field := range resolvers {
			start := len(valueScratch)
			required, peak, ok := compactProjectionValueLen(
				streamWork[base+field].view, ordinal,
			)
			if !ok || peak > cap(valueScratch)-len(valueScratch) {
				return false, false, valueScratch, nil
			}
			beforeCap := cap(valueScratch)
			valueScratch, ok = streamWork[base+field].view.appendValue(
				valueScratch, ordinal,
			)
			if !ok || cap(valueScratch) != beforeCap ||
				len(valueScratch)-start != required {
				return false, false, valueScratch, nil
			}
			fields[field].JSON = valueScratch[start:]
		}
		if err := visit(row, fields[:len(resolvers)]); err != nil {
			return false, false, valueScratch, err
		}
		shapeSeen[shape]++
	}
	for shape := 0; shape < v.shapeCount; shape++ {
		if shapeSeen[shape] > shapeWork[shape].rows {
			return false, false, valueScratch, nil
		}
	}
	return true, false, valueScratch, nil
}
