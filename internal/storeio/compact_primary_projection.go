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

// UnifiedProjectionField is one selected scalar borrowed for one callback.
// JSON is either backed by the caller's projection scratch or borrowed directly
// from a compact dictionary entry, as indicated by Kind. Integer values carry
// their decoded int64 directly and therefore have no JSON spelling until a
// caller asks AppendJSON to format one. Missing fields have neither.
type UnifiedProjectionFieldKind uint8

const (
	// UnifiedProjectionFieldJSON is a value rendered into the caller's bounded
	// projection scratch. It is the zero value so old raw-field construction
	// remains a valid fallback representation.
	UnifiedProjectionFieldJSON UnifiedProjectionFieldKind = iota
	// UnifiedProjectionFieldBorrowedJSON borrows the exact bytes of a stored
	// dictionary entry. The bytes remain valid only until the callback returns.
	UnifiedProjectionFieldBorrowedJSON
	// UnifiedProjectionFieldInteger carries a native compact integer without
	// rendering its canonical JSON spelling.
	UnifiedProjectionFieldInteger
	// UnifiedProjectionFieldMissing represents an absent path. JSON is nil so
	// query materialization can preserve the missing/null distinction.
	UnifiedProjectionFieldMissing
)

type UnifiedProjectionField struct {
	JSON    []byte
	Integer int64
	Kind    UnifiedProjectionFieldKind
}

// AppendJSON appends the field's JSON representation to dst. Integer fields
// are formatted into caller-owned storage; borrowed and scratch-backed JSON is
// copied as-is. A missing field appends nothing, matching its nil JSON view.
func (f UnifiedProjectionField) AppendJSON(dst []byte) []byte {
	switch f.Kind {
	case UnifiedProjectionFieldInteger:
		return strconv.AppendInt(dst, f.Integer, 10)
	case UnifiedProjectionFieldMissing:
		return dst
	default:
		return append(dst, f.JSON...)
	}
}

// ScratchBytes reports the bytes that live in the caller's projection value
// scratch. Dictionary entries borrow page bytes and native/missing fields use
// no value scratch at all.
func (f UnifiedProjectionField) ScratchBytes() int {
	if f.Kind != UnifiedProjectionFieldJSON {
		return 0
	}
	return len(f.JSON)
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
type compactProjectionSequentialState struct {
	next   int
	cursor int
	value  int64
	width  int
}

type UnifiedProjectionStreamWorkspace struct {
	hole  int
	view  compactStreamView
	state compactProjectionSequentialState
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

// compactProjectionDictionaryEntry returns the exact JSON bytes stored in one
// dictionary stream. The entry is page-backed and remains valid for the leaf
// callback's lifetime, so callers can publish it without copying through the
// projection value scratch.
func compactProjectionDictionaryEntry(
	v *compactStreamView, row int,
) ([]byte, bool) {
	if v == nil || v.kind != compactStreamDictionary || row < 0 || row >= v.count {
		return nil, false
	}
	id := int(compactReadBits(v.data, row*int(v.width), int(v.width)))
	return v.dictionaryEntry(id)
}

// compactProjectionSequentialIntegerState advances one restart-coded integer
// stream in row order. The state is deliberately narrower than the general
// scan decoder: projection needs only the next row, its packed cursor, the
// current value, and the packed delta width.
func (s *compactProjectionSequentialState) seed(
	v *compactStreamView, row int,
) bool {
	if s == nil || v == nil || row < 0 || row >= v.count {
		return false
	}
	prefix := 0
	packed := false
	switch v.kind {
	case compactStreamDelta:
	case compactStreamDeltaPack:
		packed = true
	case compactStreamPrefixInt:
		if len(v.data) < 2 || v.data[0]&2 != 0 {
			return false
		}
		prefix = 2
		packed = v.data[0]&4 != 0
	default:
		return false
	}
	blocks := (v.count + compactStreamRestart - 1) / compactStreamRestart
	block := row / compactStreamRestart
	if block < 0 || block >= blocks || blocks > (len(v.data)-prefix)/4 {
		return false
	}
	at := prefix + block*4
	if at < prefix || at+4 > len(v.data) {
		return false
	}
	cursor := int(binary.LittleEndian.Uint32(v.data[at:]))
	header := prefix + blocks*4
	if cursor < header || cursor > len(v.data)-8 {
		return false
	}
	value := int64(binary.LittleEndian.Uint64(v.data[cursor:]))
	cursor += 8
	width := 0
	if packed {
		if cursor >= len(v.data) {
			return false
		}
		width = int(v.data[cursor])
		if width > 64 {
			return false
		}
		cursor++
		rows := min(compactStreamRestart, v.count-block*compactStreamRestart)
		bits := (rows - 1) * width
		packedBytes := (bits + 7) / 8
		if packedBytes < 0 || packedBytes > len(v.data)-cursor {
			return false
		}
	}
	s.next = block * compactStreamRestart
	s.cursor = cursor
	s.value = value
	s.width = width
	return true
}

func (s *compactProjectionSequentialState) advance(
	v *compactStreamView,
) bool {
	if s == nil || v == nil || s.next < 0 || s.next+1 >= v.count {
		return false
	}
	block := s.next / compactStreamRestart
	if (s.next+1)/compactStreamRestart != block {
		return false
	}
	switch v.kind {
	case compactStreamDeltaPack,
		compactStreamPrefixInt:
		if v.kind == compactStreamPrefixInt && len(v.data) < 2 {
			return false
		}
		packed := v.kind == compactStreamDeltaPack || v.data[0]&4 != 0
		if packed {
			within := s.next - block*compactStreamRestart
			if s.width < 0 || s.width > 64 || within < 0 ||
				(s.width != 0 && within > math.MaxInt/s.width) {
				return false
			}
			bit := within * s.width
			available := len(v.data) - s.cursor
			if available < 0 || bit > math.MaxInt-s.width ||
				uint64(bit+s.width) > uint64(available)*8 {
				return false
			}
			u := compactReadBits(v.data[s.cursor:], bit, s.width)
			delta := int64(u>>1) ^ -int64(u&1)
			value, ok := unifiedCheckedIntegerAdd(s.value, delta)
			if !ok {
				return false
			}
			s.value = value
			s.next++
			return true
		}
	case compactStreamDelta:
	default:
		return false
	}
	if s.cursor < 0 || s.cursor >= len(v.data) {
		return false
	}
	u, n, ok := readCompactUvarint(v.data[s.cursor:])
	if !ok || n <= 0 || n > len(v.data)-s.cursor {
		return false
	}
	delta := int64(u>>1) ^ -int64(u&1)
	value, ok := unifiedCheckedIntegerAdd(s.value, delta)
	if !ok {
		return false
	}
	s.cursor += n
	s.value = value
	s.next++
	return true
}

func (s *compactProjectionSequentialState) integerAt(
	v *compactStreamView, row int,
) (int64, bool) {
	if s == nil || v == nil || row < 0 || row >= v.count {
		return 0, false
	}
	block := row / compactStreamRestart
	blockStart := block * compactStreamRestart
	if s.next < blockStart || s.next > row || s.next < 0 {
		if !s.seed(v, row) {
			return 0, false
		}
	}
	for s.next < row {
		if !s.advance(v) {
			return 0, false
		}
	}
	return s.value, s.next == row
}

// compactProjectionIntegerValue recognizes only the integer streams whose
// validated compact representation can be consumed as an int64 directly.
// Width-64 FOR remains on the generic render path because its packed deltas
// do not preserve signed ordering for this bounded lane.
func compactProjectionIntegerValue(
	v *compactStreamView, row int, state *compactProjectionSequentialState,
) (int64, bool) {
	if v == nil {
		return 0, false
	}
	switch v.kind {
	case compactStreamFOR:
		if v.width == 64 {
			return 0, false
		}
		return unifiedIntegerFORValue(*v, row)
	case compactStreamDelta:
		return state.integerAt(v, row)
	case compactStreamDeltaPack:
		return state.integerAt(v, row)
	default:
		return 0, false
	}
}

// compactProjectionPrefixFieldAt renders a PrefixInt row after its restart
// state has decoded the numeric component. Prefix and suffix bytes are copied
// exactly once into bounded value scratch; the random value-length walk is no
// longer repeated by appendValue.
func compactProjectionPrefixFieldAt(
	v *compactStreamView, row int, valueScratch []byte,
	field *UnifiedProjectionField, state *compactProjectionSequentialState,
) ([]byte, bool) {
	var value int64
	var ok bool
	if len(v.data) < 2 {
		return valueScratch, false
	}
	if v.data[0]&2 != 0 {
		value, ok = v.prefixInteger(row)
	} else {
		value, ok = state.integerAt(v, row)
	}
	if !ok || value < 0 {
		return valueScratch, false
	}
	prefix, prefixOK := v.dictionaryEntry(0)
	suffix, suffixOK := v.dictionaryEntry(1)
	if !prefixOK || !suffixOK {
		return valueScratch, false
	}
	if len(prefix) == 0 && len(suffix) == 0 && v.data[0]&1 == 0 {
		*field = UnifiedProjectionField{
			Integer: value,
			Kind:    UnifiedProjectionFieldInteger,
		}
		return valueScratch, true
	}
	digits := canonicalIntRenderedLen(value)
	width := digits
	if v.data[0]&1 != 0 {
		width = int(v.data[1])
		if width < digits {
			return valueScratch, false
		}
	}
	if len(prefix) > math.MaxInt-width ||
		len(prefix)+width > math.MaxInt-len(suffix) {
		return valueScratch, false
	}
	total := len(prefix) + width + len(suffix)
	start := len(valueScratch)
	if total > cap(valueScratch)-start {
		return valueScratch, false
	}
	valueScratch = append(valueScratch, prefix...)
	digitsStart := len(valueScratch)
	if width == 8 && v.data[0]&1 != 0 && value < 100_000_000 {
		valueScratch = appendFixedUint8(valueScratch, uint32(value))
	} else if value < 1_000_000 {
		valueScratch = appendCanonicalUint6(valueScratch, uint64(value))
	} else {
		valueScratch = strconv.AppendUint(valueScratch, uint64(value), 10)
	}
	n := len(valueScratch) - digitsStart
	if n > width {
		return valueScratch, false
	}
	if n < width {
		gap := width - n
		for range gap {
			valueScratch = append(valueScratch, 0)
		}
		copy(valueScratch[digitsStart+gap:], valueScratch[digitsStart:digitsStart+n])
		for at := digitsStart; at < digitsStart+gap; at++ {
			valueScratch[at] = '0'
		}
	}
	valueScratch = append(valueScratch, suffix...)
	if len(valueScratch)-start != total {
		return valueScratch, false
	}
	*field = UnifiedProjectionField{
		JSON: valueScratch[start:],
		Kind: UnifiedProjectionFieldJSON,
	}
	return valueScratch, true
}

// compactProjectionFieldAt resolves one selected stream row. Native integers
// and dictionary entries bypass both formatting and projection scratch. The
// remaining codecs retain the existing bounded render path, including the
// restart peak admission that protects earlier fields in the same callback.
func compactProjectionFieldAt(
	v *compactStreamView, row int, valueScratch []byte,
	field *UnifiedProjectionField, state *compactProjectionSequentialState,
) (scratch []byte, ok bool) {
	if v == nil || field == nil || row < 0 || row >= v.count {
		return valueScratch, false
	}
	if value, native := compactProjectionIntegerValue(v, row, state); native {
		*field = UnifiedProjectionField{
			Integer: value,
			Kind:    UnifiedProjectionFieldInteger,
		}
		return valueScratch, true
	}
	if value, dictionary := compactProjectionDictionaryEntry(v, row); dictionary {
		if integer, canonical := CanonicalIntValue(value); canonical {
			*field = UnifiedProjectionField{
				Integer: integer,
				Kind:    UnifiedProjectionFieldInteger,
			}
			return valueScratch, true
		}
		*field = UnifiedProjectionField{
			JSON: value,
			Kind: UnifiedProjectionFieldBorrowedJSON,
		}
		return valueScratch, true
	}
	if v.kind == compactStreamPrefixInt {
		return compactProjectionPrefixFieldAt(
			v, row, valueScratch, field, state,
		)
	}
	required, peak, bounded := compactProjectionValueLen(*v, row)
	if !bounded || peak > cap(valueScratch)-len(valueScratch) {
		return valueScratch, false
	}
	start := len(valueScratch)
	beforeCap := cap(valueScratch)
	valueScratch, bounded = v.appendValue(valueScratch, row)
	if !bounded || cap(valueScratch) != beforeCap ||
		len(valueScratch)-start != required {
		return valueScratch, false
	}
	*field = UnifiedProjectionField{
		JSON: valueScratch[start:],
		Kind: UnifiedProjectionFieldJSON,
	}
	return valueScratch, true
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
// exactly one scalar hole in every admitted shape. Container paths decline so
// the generic executor remains authoritative for structure semantics; absent
// paths are retained as explicit missing fields and require no stream.
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
		if hole == UnifiedHoleAbsent {
			streams[field].view = compactStreamView{}
			continue
		}
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
		streams[field].state = compactProjectionSequentialState{next: -1}
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
			stream := &streamWork[base+field]
			if stream.hole == UnifiedHoleAbsent {
				fields[field] = UnifiedProjectionField{
					Kind: UnifiedProjectionFieldMissing,
				}
				continue
			}
			var ok bool
			valueScratch, ok = compactProjectionFieldAt(
				&stream.view, ordinal, valueScratch, &fields[field], &stream.state,
			)
			if !ok {
				return false, false, valueScratch, nil
			}
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
