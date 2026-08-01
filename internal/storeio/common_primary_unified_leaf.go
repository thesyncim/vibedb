package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
)

// The unified primary leaf (class 5) stores every document as canonical
// template rows inside the succinct ordered envelope. The envelope machinery
// — stable cuckoo hash slots, control
// bytes, lexical rank permutation, 7-bit key lengths, overflow bitmap, and
// succinct record boundaries — is reused byte for byte at the wide-class
// geometry (192 normal + 64 stash slots); only the record heap's *value*
// content changes meaning. Between the envelope's checkpoint table and the
// record heap sit the two per-leaf unified sections:
//
//	unifiedStart (the envelope's layout.heapStart for this live count):
//	  u16 templateCount            (≤ 255: row tag 0xFF is the trivial escape)
//	  u16 dictionaryCount          (≤ 128: dict ids occupy token tags 0..127)
//	  u32 templateSectionBytes     (directory + entries)
//	  u32 dictionarySectionBytes   (directory + value spellings)
//	  u32 trivialContentBytes      (exact no-compression fold envelope)
//	template section: cumulative u32 ends (relative to the entry data start),
//	  then per template the document_group entry layout:
//	  u16 holeCount | u16 zero | u32 staticBytes |
//	  (holeCount+1)×u32 cumulative static-segment ends | static bytes
//	dictionary section: cumulative u32 ends + raw canonical value spellings
//	record heap (physically lexical, succinct boundaries as today):
//	  per record: [key bytes][row body]
//	  row body = templateID u8 | token stream         (templated row)
//	           | 0xFF          | canonical JSON bytes (trivial row)
//	           | 32-byte PageRef                      (overflow bitmap set)
//
// The stored logical value of every row is its vibejson canonical form: the
// encoder canonicalizes each input document (already-canonical input is
// detected in the same walk and aliased), and
// every render reproduces exactly those canonical bytes.
const (
	// CommonPrimaryLeafUnified is the class-5 unified canonical template-row
	// leaf. Like classes 3/4 its payload is not the raw narrow/wide value heap,
	// so CommonPrimaryLeafClass.slots() reports zero for it and readers must
	// dispatch on the class byte; unlike classes 3/4 its posting-stable slot is
	// the envelope's stable cuckoo hash slot (one-slot discipline).
	CommonPrimaryLeafUnified CommonPrimaryLeafClass = 5

	// commonPrimaryUnifiedHeaderBytes frames the two unified sections. The
	// counts are duplicated from the section directories so an open can bound
	// every section arithmetic check before touching section bytes.
	commonPrimaryUnifiedHeaderBytes = 16

	// Token tags. Dictionary ids and short-literal ranges are disjoint,
	// following the document-group scheme, extended with the typed tags whose
	// losslessness the design proves from the JSON grammar.
	unifiedTokenDictLimit   = 128  // dict ids occupy tags 0x00..0x7F
	unifiedTokenShortBase   = 0x80 // short literal lengths 1..120 → 0x80..0xF7
	unifiedTokenShortMax    = 120
	unifiedTokenLongLiteral = 0xF8 // uvarint length + spelling bytes
	unifiedTokenTrue        = 0xF9 // regeneration is identity by grammar
	unifiedTokenFalse       = 0xFA
	unifiedTokenNull        = 0xFB
	unifiedTokenInt         = 0xFC // canonical-integer admission, zigzag varint
	// Tags 0xFD/0xFE are unassigned and fail closed; 0xFF is never a token tag
	// because it is the row-level trivial escape, which keeps the row tag space
	// unambiguous when a decoder resynchronizes at a row boundary.

	// unifiedRowTrivial marks a row stored as its whole canonical spelling:
	// the escape hatch inside the one grammar, not a class.
	unifiedRowTrivial = 0xFF

	// unifiedMaxTemplates caps template ids at the values a row's leading byte
	// can name without colliding with the trivial tag.
	unifiedMaxTemplates = 255
	// unifiedNarrowExtentMaxRows caps rows in a 4 KiB extent. The envelope's
	// one-byte select checkpoints (commonPrimaryLeafLayoutFor keys the width on
	// extent == 4 KiB) encode a bitmap position of (offset>>8) + boundaryIndex;
	// at 4 KiB the offset contributes at most 15, so boundaries beyond 240 rows
	// could overflow the byte and panic the checkpoint builder. The wide class
	// never met this before because wide leaves were always ≥ 8 KiB.
	unifiedNarrowExtentMaxRows = 240
)

// unifiedPrimaryLeafRow is the per-row plan the builder extracts once per
// window: where the canonical spelling lives (aliased from the input when the
// input is already canonical, otherwise in the builder heap), the row's hole
// spans within that spelling, its shape, and the typed token-stream cost
// without dictionary participation (the trivial-row predicate operand).
type unifiedPrimaryLeafRow struct {
	heapOff      int32 // -1: canonical bytes alias the input record
	length       int32
	spanStart    int32
	spanEnd      int32
	shape        int32 // -1 for overflow rows, which carry no canonical bytes
	tokensNoDict int32
}

// unifiedPrimaryLeafShape is one distinct skeleton in the current window,
// content-addressed by its static segments (hash-routed candidates are byte
// compared, never trusted by fingerprint — the shapeTapeConforms discipline).
type unifiedPrimaryLeafShape struct {
	firstRow    int
	holes       int
	staticBytes int
	// entryBytes is the template-entry cost 8 + (holes+1)·4 + staticBytes,
	// exactly the document_group entry layout this class reuses.
	entryBytes int
}

// unifiedPrefixPlan is the deterministic derivation for one row prefix: which
// shapes template, in which id order, with which dictionary, and how many
// bytes each section and the heap take. It is a pure function of the prefix's
// rows, which is what makes bulk build and the later fold
// share one encoder.
type unifiedPrefixPlan struct {
	count           int
	templated       []int32 // shape → template id, -1 when trivial/absent
	templateShapes  []int   // template id → shape
	templateBytes   int     // directory + entries
	dictionaryBytes int     // directory + spellings
	heapBytes       int     // keys (with escape bytes) + row bodies
	contentBytes    int     // header + sections + heap
	trivialRows     int
	liveRows        int
}

// UnifiedPrimaryLeafBuilder holds the reusable scratch one bulk build (or,
// later, one fold) needs to stage unified leaves without per-leaf allocation
// churn: tape storage, the canonical workspace, the canonical-bytes heap for
// rewritten documents, hole spans, shape and dictionary derivation state.
type UnifiedPrimaryLeafBuilder struct {
	indexStore   []vibejson.IndexEntry
	ws           CanonicalWorkspace
	heap         []byte
	spans        []UnifiedTokenSpan
	rows         []unifiedPrimaryLeafRow
	shapes       []unifiedPrimaryLeafShape
	records      []CommonPrimaryLeafRecord
	plan         unifiedPrefixPlan
	shapeRows    []int32
	shapeSavings []int64
	counts       map[string]int
	dictionary   []unifiedDictionaryCandidate
	dictionaryID map[string]uint8
	patchValues  []unifiedPrimaryPatchValueDelta
	patchInteger [24]byte
}

// NewUnifiedPrimaryLeafBuilder returns a builder with modest starting scratch.
func NewUnifiedPrimaryLeafBuilder() *UnifiedPrimaryLeafBuilder {
	return &UnifiedPrimaryLeafBuilder{
		indexStore: make([]vibejson.IndexEntry, 0, 64),
	}
}

// canonicalOf resolves row i's canonical spelling against the builder heap or
// the borrowed input record it aliases.
func (b *UnifiedPrimaryLeafBuilder) canonicalOf(i int) []byte {
	row := &b.rows[i]
	if row.heapOff < 0 {
		return b.records[i].Value.Inline[:row.length]
	}
	return b.heap[row.heapOff : int(row.heapOff)+int(row.length)]
}

// buildIndex builds a tape over src, growing the reusable entry storage on
// ErrIndexFull exactly as the compact builder does.
func (b *UnifiedPrimaryLeafBuilder) buildIndex(src []byte) (vibejson.Index, error) {
	for {
		index, err := vibejson.BuildIndex(src, b.indexStore[:cap(b.indexStore)])
		if err == nil {
			b.indexStore = index.Entries
			return index, nil
		}
		if !errors.Is(err, document.ErrIndexFull) {
			return vibejson.Index{}, err
		}
		grown := cap(b.indexStore) * 2
		if grown < 64 {
			grown = 64
		}
		b.indexStore = make([]vibejson.IndexEntry, 0, grown)
	}
}

// appendHoleSpans extracts the ordered scalar-leaf spans of one canonical
// document from its tape. A hole is a non-key tape leaf with Next == 1, which
// places holes at scalar leaves of arbitrary depth and makes empty containers
// holes too — nesting never disqualifies.
func appendHoleSpans(dst []UnifiedTokenSpan, index vibejson.Index) []UnifiedTokenSpan {
	for i := range index.Entries {
		e := &index.Entries[i]
		if e.Flags()&vibejson.TapeFlagKey != 0 || e.Next != 1 {
			continue
		}
		dst = append(dst, UnifiedTokenSpan{Start: e.Start, End: e.End})
	}
	return dst
}

// zigzagVarintLen is the encoded byte length of AppendZigzagVarint(v).
func zigzagVarintLen(v int64) int {
	u := uint64(v)<<1 ^ uint64(v>>63)
	n := 1
	for u >= 0x80 {
		u >>= 7
		n++
	}
	return n
}

// unifiedTypedTokenCost is the token byte cost of one hole spelling without
// dictionary participation: typed tags where identity is provable
// from the grammar, a verbatim literal everywhere else.
func unifiedTypedTokenCost(v []byte) int {
	switch len(v) {
	case 4:
		if string(v) == "true" || string(v) == "null" {
			return 1
		}
	case 5:
		if string(v) == "false" {
			return 1
		}
	}
	if value, ok := CanonicalIntValue(v); ok {
		return 1 + zigzagVarintLen(value)
	}
	if len(v) <= unifiedTokenShortMax {
		return 1 + len(v)
	}
	return 1 + unifiedTokenUvarintLen(uint32(len(v))) + len(v)
}

// unifiedDictionaryEligible is the single policy gate shared by the full
// planner and the checkpoint patch certificate. Canonical integers are already
// represented by a typed zigzag varint, so dictionarying them buys marginal
// bytes at the cost of making every integer replacement depend on a leaf-wide
// frequency census. Keeping integers out of the dictionary makes an
// equal-token-cost integer replacement locally plan-stable. The one-byte typed
// spellings are excluded for the simpler reason that a dictionary reference
// cannot improve on them.
func unifiedDictionaryEligible(v []byte) bool {
	if unifiedTypedTokenCost(v) == 1 {
		return false
	}
	_, integer := CanonicalIntValue(v)
	return !integer
}

// shapeEqual reports whether rows a and b share one skeleton: identical hole
// counts and identical static bytes between (and around) the holes. This is
// static template equality restated over the builder's span storage.
func (b *UnifiedPrimaryLeafBuilder) shapeEqual(a, bi int) bool {
	ra, rb := &b.rows[a], &b.rows[bi]
	if ra.spanEnd-ra.spanStart != rb.spanEnd-rb.spanStart {
		return false
	}
	ca, cb := b.canonicalOf(a), b.canonicalOf(bi)
	sa := b.spans[ra.spanStart:ra.spanEnd]
	sb := b.spans[rb.spanStart:rb.spanEnd]
	pa, pb := uint32(0), uint32(0)
	for i := 0; i <= len(sa); i++ {
		ea, eb := uint32(len(ca)), uint32(len(cb))
		if i < len(sa) {
			ea, eb = sa[i].Start, sb[i].Start
		}
		if !bytes.Equal(ca[pa:ea], cb[pb:eb]) {
			return false
		}
		if i < len(sa) {
			pa, pb = sa[i].End, sb[i].End
		}
	}
	return true
}

// extract canonicalizes every record of one window and derives its holes and
// shape. Records are borrowed until the next extract call. Already-canonical
// input skips the render (stored and rendered bytes are
// canonical by construction); everything else renders once into the builder
// heap, whose offsets — not slices — the rows keep, so heap growth during
// later rows cannot invalidate earlier references.
func (b *UnifiedPrimaryLeafBuilder) extract(records []CommonPrimaryLeafRecord) error {
	b.records = records
	b.heap = b.heap[:0]
	b.spans = b.spans[:0]
	b.rows = b.rows[:0]
	b.shapes = b.shapes[:0]
	for i := range records {
		record := &records[i]
		if len(record.Key) == 0 || len(record.Key) > CommonPrimaryLeafMaxKeyBytes {
			return fmt.Errorf("%w: unified leaf key", ErrInvalidWrite)
		}
		if i != 0 && bytes.Compare(records[i-1].Key, record.Key) >= 0 {
			return fmt.Errorf("%w: unified leaf keys not lexical", ErrInvalidWrite)
		}
		if record.Value.IsOverflow() {
			// Overflow rows carry a chain reference, never canonical bytes; the
			// chain itself holds the canonical spelling and the row takes
			// no part in the template census.
			b.rows = append(b.rows, unifiedPrimaryLeafRow{heapOff: -1, shape: -1})
			continue
		}
		if len(record.Value.Inline) == 0 {
			return fmt.Errorf("%w: unified leaf empty value", ErrInvalidWrite)
		}
		index, err := b.buildIndex(record.Value.Inline)
		if err != nil {
			return err
		}
		row := unifiedPrimaryLeafRow{heapOff: -1}
		if IndexIsCanonical(index, &b.ws) {
			row.length = int32(len(record.Value.Inline))
		} else {
			off := len(b.heap)
			b.heap, err = AppendCanonicalIndexed(b.heap, index, &b.ws)
			if err != nil {
				return err
			}
			row.heapOff = int32(off)
			row.length = int32(len(b.heap) - off)
			// The hole spans must address the canonical spelling, so the
			// rewritten document is re-indexed over its canonical bytes.
			index, err = b.buildIndex(b.heap[off:len(b.heap):len(b.heap)])
			if err != nil {
				return err
			}
		}
		row.spanStart = int32(len(b.spans))
		b.spans = appendHoleSpans(b.spans, index)
		row.spanEnd = int32(len(b.spans))
		canonical := record.Value.Inline
		if row.heapOff >= 0 {
			canonical = b.heap[row.heapOff : int(row.heapOff)+int(row.length)]
		}
		tokens := 0
		staticBytes := int(row.length)
		for _, span := range b.spans[row.spanStart:row.spanEnd] {
			tokens += unifiedTypedTokenCost(canonical[span.Start:span.End])
			staticBytes -= int(span.End - span.Start)
		}
		row.tokensNoDict = int32(tokens)
		b.rows = append(b.rows, row)
		at := len(b.rows) - 1
		shape := -1
		for s := range b.shapes {
			if b.shapeEqual(b.shapes[s].firstRow, at) {
				shape = s
				break
			}
		}
		if shape < 0 {
			holes := int(row.spanEnd - row.spanStart)
			b.shapes = append(b.shapes, unifiedPrimaryLeafShape{
				firstRow: at, holes: holes, staticBytes: staticBytes,
				// The template-entry cost, verified against the reused
				// document_group entry layout: 8 header bytes, one u32 per
				// segment end (holes+1), and the static bytes.
				entryBytes: 8 + (holes+1)*4 + staticBytes,
			})
			shape = len(b.shapes) - 1
		}
		b.rows[at].shape = int32(shape)
	}
	return nil
}

// planPrefix derives the deterministic template/dictionary/token plan for the
// first count extracted rows: template census first, then dictionary
// selection, then token sizing. The amortization predicate is evaluated with
// dictionary-free typed token costs. tokenStreamLen cannot include dictionary
// references without a circular dependency (which shapes template decides
// which rows feed the dictionary), so the predicate uses the conservative
// dictionary-free cost — a shape that templates under it always saves at
// least what the predicate charged, and the choice is a pure function of the
// prefix's rows. This deviation is flagged in the stage report.
func (b *UnifiedPrimaryLeafBuilder) planPrefix(count int) (*unifiedPrefixPlan, error) {
	if count < 0 || count > len(b.rows) {
		return nil, fmt.Errorf("%w: unified prefix count", ErrInvalidWrite)
	}
	plan := &b.plan
	plan.count = count
	plan.templated = slices.Grow(plan.templated[:0], len(b.shapes))[:len(b.shapes)]
	plan.templateShapes = plan.templateShapes[:0]
	plan.templateBytes = 0
	plan.dictionaryBytes = 0
	plan.heapBytes = 0
	plan.trivialRows = 0
	plan.liveRows = 0
	b.shapeRows = slices.Grow(b.shapeRows[:0], len(b.shapes))[:len(b.shapes)]
	b.shapeSavings = slices.Grow(b.shapeSavings[:0], len(b.shapes))[:len(b.shapes)]
	for s := range b.shapes {
		plan.templated[s] = -1
		b.shapeRows[s] = 0
		b.shapeSavings[s] = 0
	}
	for i := 0; i < count; i++ {
		row := &b.rows[i]
		if row.shape < 0 {
			continue
		}
		b.shapeRows[row.shape]++
		// Each templated row saves canonicalLen − tokenStreamLen − 1,
		// the −1 charging the row its templateID byte.
		b.shapeSavings[row.shape] += int64(row.length) - int64(row.tokensNoDict) - 1
	}
	// Shapes are admitted in first-use lexical-rank order, which is the
	// order b.shapes already holds. The template-id budget (≤ 255, the row tag
	// space minus the trivial escape) is enforced in the same order so the
	// choice stays a pure function of the row set; in practice the predicate
	// makes an overflow unreachable because every admitted shape must be
	// net-byte-positive across the ≤ 256 rows of one leaf.
	for s := range b.shapes {
		if b.shapeRows[s] == 0 || len(plan.templateShapes) >= unifiedMaxTemplates {
			continue
		}
		// Ties template: equality is admission, fixed so that two
		// implementations cannot disagree.
		if int64(b.shapes[s].entryBytes)+4 <= b.shapeSavings[s] {
			plan.templated[s] = int32(len(plan.templateShapes))
			plan.templateShapes = append(plan.templateShapes, s)
			plan.templateBytes += 4 + b.shapes[s].entryBytes
		}
	}

	// Dictionary selection over the templated rows' hole spellings, with the
	// deterministic document_group derivation the design cites: candidates
	// whose saving is positive, ordered saving-descending with a bytewise
	// ascending tie-break, truncated at 128. The saving is computed against
	// the typed token cost the value would otherwise take, so spellings the
	// typed tags already store in one byte can never buy a dictionary slot.
	if b.counts == nil {
		b.counts = make(map[string]int, 128)
	} else {
		clear(b.counts)
	}
	for i := 0; i < count; i++ {
		row := &b.rows[i]
		if row.shape < 0 || plan.templated[row.shape] < 0 {
			continue
		}
		canonical := b.canonicalOf(i)
		for _, span := range b.spans[row.spanStart:row.spanEnd] {
			value := canonical[span.Start:span.End]
			if !unifiedDictionaryEligible(value) {
				continue
			}
			// The builder heap and the borrowed records are stable for the
			// whole plan cycle, so a read-only string view avoids copying
			// every scalar merely to count candidates.
			b.counts[byteview.String(value)]++
		}
	}
	plan.dictionaryBytes = b.selectUnifiedDictionary()

	for i := 0; i < count; i++ {
		record := &b.records[i]
		keyBytes := len(record.Key)
		if keyBytes >= commonPrimaryLeafEscapeLength {
			keyBytes++
		}
		plan.heapBytes += keyBytes + b.rowBodyBytes(i, plan)
		plan.liveRows++
	}
	plan.contentBytes = commonPrimaryUnifiedHeaderBytes +
		plan.templateBytes + plan.dictionaryBytes + plan.heapBytes
	return plan, nil
}

// selectUnifiedDictionary derives the deterministic dictionary from b.counts
// and returns its encoded section bytes. The checkpoint patch certificate uses
// this exact routine after applying its changed-value deltas, preventing the
// fast path and full planner from developing subtly different ranking rules.
func (b *UnifiedPrimaryLeafBuilder) selectUnifiedDictionary() int {
	if b.dictionaryID == nil {
		b.dictionaryID = make(map[string]uint8, unifiedTokenDictLimit)
	} else {
		clear(b.dictionaryID)
	}
	b.dictionary = b.dictionary[:0]
	for value, n := range b.counts {
		cost := unifiedTypedTokenCost(byteview.Bytes(value))
		saving := n*cost - (n + 4 + len(value))
		if saving > 0 {
			b.dictionary = append(b.dictionary, unifiedDictionaryCandidate{
				value: value, count: n, saving: saving,
			})
		}
	}
	slices.SortFunc(b.dictionary, func(x, y unifiedDictionaryCandidate) int {
		if x.saving != y.saving {
			return y.saving - x.saving
		}
		return strings.Compare(x.value, y.value)
	})
	if len(b.dictionary) > unifiedTokenDictLimit {
		b.dictionary = b.dictionary[:unifiedTokenDictLimit]
	}
	dictionaryBytes := 0
	for id, candidate := range b.dictionary {
		b.dictionaryID[candidate.value] = uint8(id)
		dictionaryBytes += 4 + len(candidate.value)
	}
	return dictionaryBytes
}

// rowBodyBytes is the encoded row-body cost of row i under plan, counting
// trivial rows for the census as a side effect of the shared classification.
func (b *UnifiedPrimaryLeafBuilder) rowBodyBytes(i int, plan *unifiedPrefixPlan) int {
	row := &b.rows[i]
	if row.shape < 0 {
		return CommonPrimaryLeafOverflowBytes
	}
	if plan.templated[row.shape] < 0 {
		plan.trivialRows++
		return 1 + int(row.length)
	}
	canonical := b.canonicalOf(i)
	body := 1
	for _, span := range b.spans[row.spanStart:row.spanEnd] {
		value := canonical[span.Start:span.End]
		if _, found := b.dictionaryID[byteview.String(value)]; found {
			body++
			continue
		}
		body += unifiedTypedTokenCost(value)
	}
	return body
}

// appendRowBody emits row i's body under plan. It must agree byte for byte
// with rowBodyBytes; the encoder cross-checks the two with a plan-drift error.
func (b *UnifiedPrimaryLeafBuilder) appendRowBody(dst []byte, i int, plan *unifiedPrefixPlan) []byte {
	row := &b.rows[i]
	if plan.templated[row.shape] < 0 {
		dst = append(dst, unifiedRowTrivial)
		return append(dst, b.canonicalOf(i)...)
	}
	dst = append(dst, byte(plan.templated[row.shape]))
	canonical := b.canonicalOf(i)
	for _, span := range b.spans[row.spanStart:row.spanEnd] {
		value := canonical[span.Start:span.End]
		if id, found := b.dictionaryID[byteview.String(value)]; found {
			dst = append(dst, id)
			continue
		}
		switch len(value) {
		case 4:
			if string(value) == "true" {
				dst = append(dst, unifiedTokenTrue)
				continue
			}
			if string(value) == "null" {
				dst = append(dst, unifiedTokenNull)
				continue
			}
		case 5:
			if string(value) == "false" {
				dst = append(dst, unifiedTokenFalse)
				continue
			}
		}
		if v, ok := CanonicalIntValue(value); ok {
			dst = append(dst, unifiedTokenInt)
			dst = AppendZigzagVarint(dst, v)
			continue
		}
		if len(value) <= unifiedTokenShortMax {
			dst = append(dst, unifiedTokenShortBase+byte(len(value)-1))
		} else {
			dst = append(dst, unifiedTokenLongLiteral)
			var lengthBytes [5]byte
			n := putUnifiedTokenUvarint(lengthBytes[:], uint32(len(value)))
			dst = append(dst, lengthBytes[:n]...)
		}
		dst = append(dst, value...)
	}
	return dst
}

// rawRenderCapacity returns the largest row prefix whose canonical renders
// fit one raw wide envelope of extentBytes. Structural mutations can render a
// class-5 leaf into a raw wide envelope of at most 64 KiB before re-fitting,
// so a unified leaf must never hold more rows than that envelope can carry.
func (b *UnifiedPrimaryLeafBuilder) rawRenderCapacity(extentBytes int) int {
	capacity := extentBytes - PageHeaderSize - PageTrailerSize
	contentBytes := commonPrimaryUnifiedHeaderBytes
	count := 0
	for count < CommonPrimaryLeafWideSlots && count < len(b.rows) {
		grown := contentBytes + len(b.records[count].Key) +
			b.trivialValueBytes(count)
		if len(b.records[count].Key) >= commonPrimaryLeafEscapeLength {
			grown++
		}
		layout := commonPrimaryLeafLayoutFor(
			CommonPrimaryLeafWide, count+1, extentBytes,
		)
		if layout.heapStart+grown > capacity {
			break
		}
		contentBytes = grown
		count++
	}
	return count
}

func (b *UnifiedPrimaryLeafBuilder) trivialValueBytes(row int) int {
	if b.rows[row].shape < 0 {
		return CommonPrimaryLeafOverflowBytes
	}
	return 1 + int(b.rows[row].length)
}

func (b *UnifiedPrimaryLeafBuilder) trivialContentBytes(count int) int {
	total := commonPrimaryUnifiedHeaderBytes
	for row := 0; row < count; row++ {
		total += len(b.records[row].Key) + b.trivialValueBytes(row)
		if len(b.records[row].Key) >= commonPrimaryLeafEscapeLength {
			total++
		}
	}
	return total
}

// CommonPrimaryUnifiedTrivialFits reports whether count rows with the supplied
// exact no-compression content byte total fit the maximum class-5 extent. The
// total includes the unified header, encoded keys, row tags/values, and
// overflow descriptors, but not the succinct envelope tables.
func CommonPrimaryUnifiedTrivialFits(count, contentBytes int) bool {
	if count < 0 || count > CommonPrimaryLeafWideSlots ||
		contentBytes < commonPrimaryUnifiedHeaderBytes {
		return false
	}
	capacity := CommonPrimaryLeafMaxExtentBytes -
		PageHeaderSize - PageTrailerSize
	layout := commonPrimaryLeafLayoutFor(
		CommonPrimaryLeafWide, count, CommonPrimaryLeafMaxExtentBytes,
	)
	return layout.heapStart != 0 &&
		layout.heapStart+contentBytes <= capacity
}

// CommonPrimaryUnifiedInsertedTrivialBytes reports the exact increase to a
// class-5 leaf's no-compression content total for one new inline row. It
// includes the encoded key, the trivial row tag, and value bytes.
func CommonPrimaryUnifiedInsertedTrivialBytes(key []byte, valueBytes int) int {
	if len(key) == 0 || len(key) > CommonPrimaryLeafMaxKeyBytes ||
		valueBytes <= 0 {
		return 0
	}
	total := len(key) + 1 + valueBytes
	if len(key) >= commonPrimaryLeafEscapeLength {
		total++
	}
	return total
}

// unifiedLeafExtents are the physical extents the packing search tries,
// smallest first so an equal bytes-per-row tie keeps the smaller extent.
var unifiedLeafExtents = [...]int{
	CommonPrimaryLeafNarrowBytes, CommonPrimaryLeafWideBytes,
	16 << 10, 32 << 10, 64 << 10,
}

// planUnifiedLeaf runs the single packing planner over the front of
// window: for each extent in {4..64 KiB}, binary-search the largest lexical
// row prefix that encodes within the extent, places within the 256-slot wide
// class, and holds ≤ 256 rows; choose the extent minimizing bytes per row.
// Placement failure below 256 rows (stash exhaustion) is treated as "does not
// fit" — deterministic under the seeded hashing. The design notes both
// searched quantities are monotone in the prefix; the amortization predicate
// can flip a shape between trivial and templated as rows are added, which can
// locally shrink the image, but the search remains deterministic (its probe
// path is a pure function of the memoized sizes) and the identity gates
// compare like against like.
func planUnifiedLeaf(
	b *UnifiedPrimaryLeafBuilder, seed [16]byte, window []CommonPrimaryLeafRecord,
) (int, int, error) {
	if err := b.extract(window); err != nil {
		return 0, 0, err
	}
	hi := b.rawRenderCapacity(CommonPrimaryLeafMaxExtentBytes)
	if hi < 1 {
		return 0, 0, fmt.Errorf(
			"%w: unified record does not fit the mutation render envelope",
			ErrInvalidWrite,
		)
	}
	place := func(n int) (bool, error) {
		err := PlaceCommonPrimaryLeafRecords(CommonPrimaryLeafWide, seed, window[:n])
		if err == nil {
			return true, nil
		}
		if errors.Is(err, ErrCommonPrimaryLeafFull) ||
			errors.Is(err, ErrCommonPrimaryLeafNeedsWide) {
			return false, nil
		}
		return false, err
	}
	// A cuckoo placement valid for n rows restricts to a valid placement for
	// n−1, so the largest placeable prefix binary-searches.
	maxPlace := 0
	for lo, high := 1, hi; lo <= high; {
		mid := (lo + high) / 2
		ok, err := place(mid)
		if err != nil {
			return 0, 0, err
		}
		if ok {
			maxPlace = mid
			lo = mid + 1
		} else {
			high = mid - 1
		}
	}
	if maxPlace < 1 {
		return 0, 0, fmt.Errorf("%w: unified record does not place", ErrInvalidWrite)
	}
	memo := make(map[int]int, maxPlace)
	content := func(n int) (int, error) {
		if cached, ok := memo[n]; ok {
			return cached, nil
		}
		plan, err := b.planPrefix(n)
		if err != nil {
			return 0, err
		}
		memo[n] = plan.contentBytes
		return plan.contentBytes, nil
	}
	bestCount, bestExtent := 0, 0
	bestRatio := math.MaxFloat64
	for _, extent := range unifiedLeafExtents {
		hiExtent := maxPlace
		if extent == CommonPrimaryLeafNarrowBytes && hiExtent > unifiedNarrowExtentMaxRows {
			hiExtent = unifiedNarrowExtentMaxRows
		}
		capacity := extent - PageHeaderSize - PageTrailerSize
		fits := func(n int) (bool, error) {
			bytesNeeded, err := content(n)
			if err != nil {
				return false, err
			}
			layout := commonPrimaryLeafLayoutFor(CommonPrimaryLeafWide, n, extent)
			return layout.heapStart+bytesNeeded <= capacity, nil
		}
		count := 0
		for lo, high := 1, hiExtent; lo <= high; {
			mid := (lo + high) / 2
			ok, err := fits(mid)
			if err != nil {
				return 0, 0, err
			}
			if ok {
				count = mid
				lo = mid + 1
			} else {
				high = mid - 1
			}
		}
		if count < 1 {
			continue
		}
		ratio := float64(extent) / float64(count)
		if ratio < bestRatio {
			bestCount, bestExtent, bestRatio = count, extent, ratio
		}
	}
	if bestCount == 0 {
		return 0, 0, fmt.Errorf(
			"%w: unified record does not fit any extent", ErrInvalidWrite,
		)
	}
	return bestCount, bestExtent, nil
}

// EncodeCommonPrimaryUnifiedLeaf writes one immutable class-5 unified leaf
// into dst. Records must be strictly lexical; inline values are canonicalized
// (already-canonical input is stored as given), overflow records keep their
// chain reference. Placement runs internally against the wide-class slot
// geometry, so callers need not pre-place. The leaf image is a pure function
// of (records, header, seed, extent) — the determinism INVARIANT 2 depends on.
func EncodeCommonPrimaryUnifiedLeaf(
	dst []byte,
	header CommonPrimaryLeafHeader,
	seed [16]byte,
	records []CommonPrimaryLeafRecord,
	bounds CommonPrimaryLeafBounds,
	b *UnifiedPrimaryLeafBuilder,
) ([]byte, error) {
	return encodeCommonPrimaryUnifiedLeaf(
		dst, header, seed, records, bounds, b, true,
	)
}

func encodeCommonPrimaryUnifiedLeaf(
	dst []byte,
	header CommonPrimaryLeafHeader,
	seed [16]byte,
	records []CommonPrimaryLeafRecord,
	bounds CommonPrimaryLeafBounds,
	b *UnifiedPrimaryLeafBuilder,
	place bool,
) ([]byte, error) {
	extent := int(header.PageSize)
	logicalID, logicalOK := CommonPrimaryLeafLogicalID(header.Bucket)
	if b == nil || !validPhysicalPageSize(header.PageSize) ||
		extent < CommonPrimaryLeafNarrowBytes ||
		extent > CommonPrimaryLeafMaxExtentBytes ||
		len(dst) < extent || header.StoreID == ([16]byte{}) ||
		header.Generation == 0 || header.Generation >= uint64(1)<<48 ||
		seed == ([16]byte{}) ||
		len(records) > CommonPrimaryLeafWideSlots ||
		!logicalOK || !commonPrimaryLeafValidateBounds(bounds) {
		return nil, fmt.Errorf("%w: unified leaf identity/class/bounds", ErrInvalidWrite)
	}
	if extent == CommonPrimaryLeafNarrowBytes &&
		len(records) > unifiedNarrowExtentMaxRows {
		return nil, fmt.Errorf("%w: unified 4 KiB checkpoint bound", ErrInvalidWrite)
	}
	for rank := range records {
		record := &records[rank]
		if record.Value.IsOverflow() &&
			!commonPrimaryLeafValidateOverflow(
				record.Value.Overflow, logicalID, header.Generation, bounds,
			) {
			return nil, fmt.Errorf("%w: unified leaf overflow ref", ErrInvalidWrite)
		}
	}
	if err := b.extract(records); err != nil {
		return nil, err
	}
	plan, err := b.planPrefix(len(records))
	if err != nil {
		return nil, err
	}
	if place {
		if err := PlaceCommonPrimaryLeafRecords(
			CommonPrimaryLeafWide, seed, records,
		); err != nil {
			return nil, err
		}
	}
	layout := commonPrimaryLeafLayoutFor(
		CommonPrimaryLeafWide, len(records), extent,
	)
	if layout.heapStart == 0 {
		return nil, fmt.Errorf("%w: unified leaf layout", ErrInvalidWrite)
	}
	unifiedStart := layout.heapStart
	heapStart := unifiedStart + commonPrimaryUnifiedHeaderBytes +
		plan.templateBytes + plan.dictionaryBytes
	payloadLength := heapStart + plan.heapBytes
	if payloadLength > extent-PageHeaderSize-PageTrailerSize {
		return nil, ErrCommonPrimaryLeafFull
	}
	payload, err := InitPage(dst, PageHeader{
		StoreID: header.StoreID, Generation: header.Generation,
		LogicalID: logicalID, PageSize: uint32(extent),
		PayloadLength: uint32(payloadLength), Kind: commonPrimaryLeafPageKind,
	})
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		payload[2] = byte(CommonPrimaryLeafUnified) | 0x80
	} else {
		payload[0] = byte(len(records) - 1)
		payload[2] = byte(CommonPrimaryLeafUnified)
	}

	// Envelope tables: identical machinery and byte meaning to the wide raw
	// class; the slot, lookup, and boundary machinery is reused byte
	// for byte; only the record heap's value content changes meaning).
	stashCount := 0
	var occupied [4]uint64
	var wideHashes [CommonPrimaryLeafWideSlots - CommonPrimaryLeafNormalSlots]uint64
	for rank := range records {
		record := &records[rank]
		slot := int(record.Slot)
		if slot >= CommonPrimaryLeafWideSlots {
			return nil, fmt.Errorf("%w: unified leaf slot", ErrInvalidWrite)
		}
		bit := uint64(1) << uint(slot&63)
		if occupied[slot>>6]&bit != 0 {
			return nil, fmt.Errorf("%w: duplicate unified leaf slot", ErrInvalidWrite)
		}
		occupied[slot>>6] |= bit
		hash := commonPrimaryLeafHash(seed, record.Key)
		if slot < CommonPrimaryLeafNormalSlots {
			if !commonPrimaryLeafNormalCandidate(hash, record.Slot) {
				return nil, fmt.Errorf("%w: unified leaf candidate slot", ErrInvalidWrite)
			}
			payload[layout.controlStart+slot] =
				commonPrimaryLeafControlLive | byte(hash>>57)
			payload[layout.normalRankStart+slot] = byte(rank)
		} else {
			stash := slot - CommonPrimaryLeafNormalSlots
			payload[layout.stashBitmap+stash/8] |= byte(1) << uint(stash&7)
			payload[layout.stashRankStart+stash] = byte(rank)
			wideHashes[stash] = hash
			stashCount++
		}
		commonPrimaryLeafPutKeyLength(payload, &layout, rank, len(record.Key))
		if record.Value.IsOverflow() {
			payload[layout.overflowStart+rank/8] |= byte(1) << uint(rank&7)
		}
	}
	for stash, hash := range wideHashes {
		if payload[layout.stashBitmap+stash/8]&(byte(1)<<uint(stash&7)) == 0 {
			continue
		}
		commonPrimaryLeafInsertWideHash(payload, layout, hash, stash)
	}
	payload[1] = byte(stashCount)

	// Unified sections.
	binary.LittleEndian.PutUint16(payload[unifiedStart:], uint16(len(plan.templateShapes)))
	binary.LittleEndian.PutUint16(payload[unifiedStart+2:], uint16(len(b.dictionary)))
	binary.LittleEndian.PutUint32(payload[unifiedStart+4:], uint32(plan.templateBytes))
	binary.LittleEndian.PutUint32(payload[unifiedStart+8:], uint32(plan.dictionaryBytes))
	binary.LittleEndian.PutUint32(
		payload[unifiedStart+12:], uint32(b.trivialContentBytes(len(records))),
	)
	templateDir := unifiedStart + commonPrimaryUnifiedHeaderBytes
	templateData := templateDir + 4*len(plan.templateShapes)
	cursor := templateData
	for id, shape := range plan.templateShapes {
		entryStart := cursor
		s := &b.shapes[shape]
		row := s.firstRow
		canonical := b.canonicalOf(row)
		spans := b.spans[b.rows[row].spanStart:b.rows[row].spanEnd]
		binary.LittleEndian.PutUint16(payload[cursor:], uint16(s.holes))
		cursor += 4 // u16 holes + u16 reserved zero
		binary.LittleEndian.PutUint32(payload[cursor:], uint32(s.staticBytes))
		cursor += 4
		ends := cursor
		cursor += (s.holes + 1) * 4
		previous, written := uint32(0), 0
		for segment := 0; segment <= len(spans); segment++ {
			end := uint32(len(canonical))
			if segment < len(spans) {
				end = spans[segment].Start
			}
			written += copy(payload[cursor+written:], canonical[previous:end])
			binary.LittleEndian.PutUint32(
				payload[ends+segment*4:], uint32(written),
			)
			if segment < len(spans) {
				previous = spans[segment].End
			}
		}
		cursor += written
		binary.LittleEndian.PutUint32(
			payload[templateDir+id*4:], uint32(cursor-templateData),
		)
		if cursor-entryStart != s.entryBytes {
			return nil, fmt.Errorf("%w: unified template plan drift", ErrInvalidWrite)
		}
	}
	dictionaryDir := templateDir + plan.templateBytes
	if cursor != dictionaryDir {
		return nil, fmt.Errorf("%w: unified template section drift", ErrInvalidWrite)
	}
	dictionaryData := dictionaryDir + 4*len(b.dictionary)
	cursor = dictionaryData
	for id, candidate := range b.dictionary {
		cursor += copy(payload[cursor:], candidate.value)
		binary.LittleEndian.PutUint32(
			payload[dictionaryDir+id*4:], uint32(cursor-dictionaryData),
		)
	}
	if cursor != heapStart {
		return nil, fmt.Errorf("%w: unified dictionary section drift", ErrInvalidWrite)
	}

	// Record heap with succinct boundaries, exactly the envelope's spelling.
	for rank := range records {
		record := &records[rank]
		commonPrimaryLeafPutBoundary(payload, &layout, rank, uint16(cursor))
		if len(record.Key) >= commonPrimaryLeafEscapeLength {
			payload[cursor] = byte(len(record.Key) - 1)
			cursor++
		}
		cursor += copy(payload[cursor:], record.Key)
		if record.Value.IsOverflow() {
			encodePageRef(payload[cursor:cursor+PageRefSize], record.Value.Overflow)
			cursor += PageRefSize
			continue
		}
		body := b.appendRowBody(payload[cursor:cursor:len(payload)], rank, plan)
		cursor += len(body)
	}
	if cursor != payloadLength {
		return nil, fmt.Errorf("%w: unified heap plan drift", ErrInvalidWrite)
	}
	commonPrimaryLeafPutBoundary(payload, &layout, len(records), uint16(cursor))
	commonPrimaryLeafBuildCheckpoints(payload, &layout, len(records)+1)
	page := dst[:extent]
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

// EncodeBestCommonPrimaryUnifiedLeaf encodes the complete row set through the
// class-5 planner, choosing its smallest winning extent. Unlike the bulk
// planner it does not accept a prefix: every supplied row must fit one leaf.
// Checkpoint folds use this entry so bulk and mutation history share one
// deterministic encoder.
func EncodeBestCommonPrimaryUnifiedLeaf(
	dst []byte,
	header CommonPrimaryLeafHeader,
	seed [16]byte,
	records []CommonPrimaryLeafRecord,
	bounds CommonPrimaryLeafBounds,
	builder *UnifiedPrimaryLeafBuilder,
) ([]byte, error) {
	if builder == nil {
		return nil, fmt.Errorf("%w: unified fold input", ErrInvalidWrite)
	}
	if err := builder.extract(records); err != nil {
		return nil, err
	}
	if len(records) > builder.rawRenderCapacity(
		CommonPrimaryLeafMaxExtentBytes,
	) {
		return nil, ErrCommonPrimaryLeafFull
	}
	plan, err := builder.planPrefix(len(records))
	if err != nil {
		return nil, err
	}
	extent := 0
	for _, candidate := range unifiedLeafExtents {
		if candidate == CommonPrimaryLeafNarrowBytes &&
			len(records) > unifiedNarrowExtentMaxRows {
			continue
		}
		capacity := candidate - PageHeaderSize - PageTrailerSize
		layout := commonPrimaryLeafLayoutFor(
			CommonPrimaryLeafWide, len(records), candidate,
		)
		if layout.heapStart != 0 &&
			layout.heapStart+plan.contentBytes <= capacity {
			extent = candidate
			break
		}
	}
	if extent == 0 {
		return nil, ErrCommonPrimaryLeafFull
	}
	if extent > len(dst) {
		return nil, fmt.Errorf("%w: unified fold destination", ErrInvalidWrite)
	}
	header.PageSize = uint32(extent)
	return encodeCommonPrimaryUnifiedLeaf(
		dst[:extent], header, seed, records, bounds, builder, false,
	)
}

// CommonPrimaryUnifiedLeafView is the borrowed, allocation-free read view over
// one class-5 leaf. env is the succinct envelope view constructed at the wide
// slot geometry with its heap start advanced past the unified sections, so
// every slot lookup, rank walk, and boundary select is the proven envelope
// machinery verbatim; the unified fields resolve templates and dictionary
// entries for the row-body splice.
type CommonPrimaryUnifiedLeafView struct {
	env             CommonPrimaryLeafView
	templateCount   int
	dictionaryCount int
	templateDir     int
	templateData    int
	dictionaryDir   int
	dictionaryData  int
	heapStart       int
	trivialBytes    int
}

// unifiedSectionOffsets derives the section offsets from an already
// bounds-checked unified header. It fails when any section falls outside the
// payload or the arithmetic is inconsistent.
func unifiedSectionOffsets(
	payload []byte, unifiedStart int,
) (v CommonPrimaryUnifiedLeafView, ok bool) {
	if unifiedStart <= 0 ||
		unifiedStart+commonPrimaryUnifiedHeaderBytes > len(payload) {
		return v, false
	}
	templateCount := int(binary.LittleEndian.Uint16(payload[unifiedStart:]))
	dictionaryCount := int(binary.LittleEndian.Uint16(payload[unifiedStart+2:]))
	templateBytes := int(binary.LittleEndian.Uint32(payload[unifiedStart+4:]))
	dictionaryBytes := int(binary.LittleEndian.Uint32(payload[unifiedStart+8:]))
	trivialBytes := int(binary.LittleEndian.Uint32(payload[unifiedStart+12:]))
	if templateCount > unifiedMaxTemplates ||
		dictionaryCount > unifiedTokenDictLimit ||
		trivialBytes < commonPrimaryUnifiedHeaderBytes ||
		templateBytes < 4*templateCount || dictionaryBytes < 4*dictionaryCount ||
		templateBytes > len(payload) || dictionaryBytes > len(payload) {
		return v, false
	}
	v.templateCount = templateCount
	v.dictionaryCount = dictionaryCount
	v.templateDir = unifiedStart + commonPrimaryUnifiedHeaderBytes
	v.templateData = v.templateDir + 4*templateCount
	v.dictionaryDir = v.templateDir + templateBytes
	v.dictionaryData = v.dictionaryDir + 4*dictionaryCount
	v.heapStart = v.dictionaryDir + dictionaryBytes
	v.trivialBytes = trivialBytes
	if v.heapStart > len(payload) {
		return v, false
	}
	return v, true
}

// OpenCommonPrimaryUnifiedLeaf validates common framing, the selecting
// PageRef, the full envelope (slots, ranks, stash directory, key lengths,
// lexical order, boundaries, checkpoints, overflow refs), and every unified
// section — template directory and entries, dictionary directory, and each
// row body's complete token walk. Every section is independently fail-closed:
// a violation anywhere rejects the leaf.
func OpenCommonPrimaryUnifiedLeaf(
	src []byte,
	seed [16]byte,
	bucket BucketID,
	expected PageRef,
	selectingGeneration uint64,
	bounds CommonPrimaryLeafBounds,
) (CommonPrimaryUnifiedLeafView, error) {
	if seed == ([16]byte{}) ||
		!commonPrimaryLeafValidateExpectedRef(
			expected, bucket, selectingGeneration, bounds,
		) {
		return CommonPrimaryUnifiedLeafView{}, fmt.Errorf(
			"%w: selector", ErrCommonPrimaryLeafCorrupt,
		)
	}
	pageHeader, payload, err := OpenPage(src)
	if err != nil {
		return CommonPrimaryUnifiedLeafView{}, fmt.Errorf(
			"%w: %w", ErrCommonPrimaryLeafCorrupt, err,
		)
	}
	logicalID, _ := CommonPrimaryLeafLogicalID(bucket)
	if len(payload) < commonPrimaryLeafPayloadHeader ||
		payload[2]&0x7f != byte(CommonPrimaryLeafUnified) ||
		len(src) < int(pageHeader.PageSize) ||
		pageHeader.LogicalID != logicalID ||
		pageHeader.LogicalID != expected.LogicalID ||
		pageHeader.Generation != expected.Generation ||
		pageHeader.PageSize != expected.Length ||
		pageHeader.Kind != expected.Kind {
		return CommonPrimaryUnifiedLeafView{}, fmt.Errorf(
			"%w: common identity or unified header", ErrCommonPrimaryLeafCorrupt,
		)
	}
	count := 0
	if payload[2]&0x80 == 0 {
		count = int(payload[0]) + 1
	} else if payload[0] != 0 {
		return CommonPrimaryUnifiedLeafView{}, fmt.Errorf(
			"%w: unified empty count encoding", ErrCommonPrimaryLeafCorrupt,
		)
	}
	stashCount := int(payload[1])
	layout := commonPrimaryLeafLayoutFor(
		CommonPrimaryLeafWide, count, int(pageHeader.PageSize),
	)
	if layout.heapStart == 0 ||
		stashCount > CommonPrimaryLeafWideSlots-CommonPrimaryLeafNormalSlots {
		return CommonPrimaryUnifiedLeafView{}, fmt.Errorf(
			"%w: unified layout", ErrCommonPrimaryLeafCorrupt,
		)
	}
	view, ok := unifiedSectionOffsets(payload, layout.heapStart)
	if !ok {
		return CommonPrimaryUnifiedLeafView{}, fmt.Errorf(
			"%w: unified sections", ErrCommonPrimaryLeafCorrupt,
		)
	}
	// The envelope validator checks the terminal boundary against
	// layout.heapStart, which for class 5 is the record heap after the unified
	// sections — patch it before handing the layout to the envelope machinery.
	layout.heapStart = view.heapStart
	view.env = CommonPrimaryLeafView{
		header: CommonPrimaryLeafHeader{
			StoreID: pageHeader.StoreID, Generation: pageHeader.Generation,
			Bucket: bucket, PageSize: pageHeader.PageSize,
		},
		// The envelope machinery keys its slot arithmetic on the class field;
		// the unified class shares the wide slot geometry exactly.
		class: CommonPrimaryLeafWide, seed: seed,
		page:    src[:int(pageHeader.PageSize):int(pageHeader.PageSize)],
		payload: payload, count: uint16(count), stashCount: uint8(stashCount),
		layout: layout, bounds: bounds,
	}
	if err := view.env.validate(logicalID); err != nil {
		return CommonPrimaryUnifiedLeafView{}, err
	}
	if err := view.validateSections(); err != nil {
		return CommonPrimaryUnifiedLeafView{}, err
	}
	return view, nil
}

// AdmittedCommonPrimaryUnifiedLeaf reconstructs a class-5 leaf whose framing
// and sections were already fully validated by PageCache admission. It only
// re-derives offsets (never trusting them past arithmetic bounds) and
// allocates nothing; calling it on arbitrary bytes is invalid.
func AdmittedCommonPrimaryUnifiedLeaf(
	src []byte,
	seed [16]byte,
	bucket BucketID,
	bounds CommonPrimaryLeafBounds,
) (CommonPrimaryUnifiedLeafView, bool) {
	pageHeader, ok := decodePageHeader(src)
	if !ok {
		return CommonPrimaryUnifiedLeafView{}, false
	}
	payloadEnd := PageHeaderSize + int(pageHeader.PayloadLength)
	if payloadEnd > len(src) ||
		int(pageHeader.PayloadLength) < commonPrimaryLeafPayloadHeader {
		return CommonPrimaryUnifiedLeafView{}, false
	}
	payload := src[PageHeaderSize:payloadEnd:payloadEnd]
	if payload[2]&0x7f != byte(CommonPrimaryLeafUnified) {
		return CommonPrimaryUnifiedLeafView{}, false
	}
	count := 0
	if payload[2]&0x80 == 0 {
		count = int(payload[0]) + 1
	}
	layout := commonPrimaryLeafLayoutFor(
		CommonPrimaryLeafWide, count, int(pageHeader.PageSize),
	)
	if layout.heapStart == 0 {
		return CommonPrimaryUnifiedLeafView{}, false
	}
	view, ok := unifiedSectionOffsets(payload, layout.heapStart)
	if !ok {
		return CommonPrimaryUnifiedLeafView{}, false
	}
	layout.heapStart = view.heapStart
	view.env = CommonPrimaryLeafView{
		header: CommonPrimaryLeafHeader{
			StoreID: pageHeader.StoreID, Generation: pageHeader.Generation,
			Bucket: bucket, PageSize: pageHeader.PageSize,
		},
		class: CommonPrimaryLeafWide, seed: seed,
		page:    src[:int(pageHeader.PageSize):int(pageHeader.PageSize)],
		payload: payload, count: uint16(count), stashCount: payload[1],
		layout: layout, bounds: bounds,
	}
	return view, true
}

// unifiedTemplateView resolves one template entry: hole count, the cumulative
// static-segment end table, and the static bytes.
type unifiedTemplateView struct {
	holes  int
	ends   []byte
	static []byte
}

func (v *CommonPrimaryUnifiedLeafView) templateEntry(id int) (unifiedTemplateView, bool) {
	if id < 0 || id >= v.templateCount {
		return unifiedTemplateView{}, false
	}
	payload := v.env.payload
	previous := uint32(0)
	if id != 0 {
		previous = binary.LittleEndian.Uint32(payload[v.templateDir+(id-1)*4:])
	}
	end := binary.LittleEndian.Uint32(payload[v.templateDir+id*4:])
	if end <= previous || uint64(end) > uint64(v.dictionaryDir-v.templateData) {
		return unifiedTemplateView{}, false
	}
	entry := payload[v.templateData+int(previous) : v.templateData+int(end)]
	if len(entry) < 8 || entry[2] != 0 || entry[3] != 0 {
		return unifiedTemplateView{}, false
	}
	holes := int(binary.LittleEndian.Uint16(entry[0:2]))
	staticBytes := int(binary.LittleEndian.Uint32(entry[4:8]))
	endsBytes := (holes + 1) * 4
	if 8+endsBytes > len(entry) || staticBytes != len(entry)-8-endsBytes {
		return unifiedTemplateView{}, false
	}
	return unifiedTemplateView{
		holes: holes, ends: entry[8 : 8+endsBytes], static: entry[8+endsBytes:],
	}, true
}

func (v *CommonPrimaryUnifiedLeafView) dictionaryEntry(id int) ([]byte, bool) {
	if id < 0 || id >= v.dictionaryCount {
		return nil, false
	}
	payload := v.env.payload
	previous := uint32(0)
	if id != 0 {
		previous = binary.LittleEndian.Uint32(payload[v.dictionaryDir+(id-1)*4:])
	}
	end := binary.LittleEndian.Uint32(payload[v.dictionaryDir+id*4:])
	if end <= previous || uint64(end) > uint64(v.heapStart-v.dictionaryData) {
		return nil, false
	}
	at := v.dictionaryData
	return payload[at+int(previous) : at+int(end) : at+int(end)], true
}

// admittedTemplateEntry is the PageCache-admitted counterpart of
// templateEntry. OpenCommonPrimaryUnifiedLeaf already proved the directory,
// entry header, hole table, and static byte bounds before the frame became
// reader-visible, so a lease over that immutable frame does not need to repeat
// those branches for every row it renders.
func (v *CommonPrimaryUnifiedLeafView) admittedTemplateEntry(id int) unifiedTemplateView {
	payload := v.env.payload
	previous := uint32(0)
	if id != 0 {
		previous = binary.LittleEndian.Uint32(payload[v.templateDir+(id-1)*4:])
	}
	end := binary.LittleEndian.Uint32(payload[v.templateDir+id*4:])
	entry := payload[v.templateData+int(previous) : v.templateData+int(end)]
	holes := int(binary.LittleEndian.Uint16(entry[0:2]))
	endsBytes := (holes + 1) * 4
	return unifiedTemplateView{
		holes: holes, ends: entry[8 : 8+endsBytes], static: entry[8+endsBytes:],
	}
}

// admittedDictionaryEntry is the PageCache-admitted counterpart of
// dictionaryEntry. The id and cumulative directory were validated together
// with every row token at frame admission.
func (v *CommonPrimaryUnifiedLeafView) admittedDictionaryEntry(id int) []byte {
	payload := v.env.payload
	previous := uint32(0)
	if id != 0 {
		previous = binary.LittleEndian.Uint32(payload[v.dictionaryDir+(id-1)*4:])
	}
	end := binary.LittleEndian.Uint32(payload[v.dictionaryDir+id*4:])
	at := v.dictionaryData
	return payload[at+int(previous) : at+int(end) : at+int(end)]
}

// validateSections proves every unified section independently: the template
// directory and each entry's segment table, the dictionary directory, and —
// per row — the body classification and a complete bounds-checked token walk
// that must consume the body exactly. Every violation fails closed.
func (v *CommonPrimaryUnifiedLeafView) validateSections() error {
	corrupt := func(what string) error {
		return fmt.Errorf("%w: unified %s", ErrCommonPrimaryLeafCorrupt, what)
	}
	previous := uint32(0)
	for id := 0; id < v.templateCount; id++ {
		entry, ok := v.templateEntry(id)
		if !ok {
			return corrupt("template entry")
		}
		segPrevious := uint32(0)
		for segment := 0; segment <= entry.holes; segment++ {
			end := binary.LittleEndian.Uint32(entry.ends[segment*4:])
			if end < segPrevious || int(end) > len(entry.static) {
				return corrupt("template segment table")
			}
			segPrevious = end
		}
		if int(segPrevious) != len(entry.static) {
			return corrupt("template static length")
		}
		end := binary.LittleEndian.Uint32(v.env.payload[v.templateDir+id*4:])
		if end <= previous {
			return corrupt("template directory order")
		}
		previous = end
	}
	if v.templateCount != 0 &&
		v.templateData+int(previous) != v.dictionaryDir {
		return corrupt("template section length")
	}
	if v.templateCount == 0 && v.templateData != v.dictionaryDir {
		return corrupt("empty template section length")
	}
	previous = 0
	for id := 0; id < v.dictionaryCount; id++ {
		if _, ok := v.dictionaryEntry(id); !ok {
			return corrupt("dictionary entry")
		}
		end := binary.LittleEndian.Uint32(v.env.payload[v.dictionaryDir+id*4:])
		if end <= previous {
			return corrupt("dictionary directory order")
		}
		previous = end
	}
	if v.dictionaryData+int(previous) != v.heapStart {
		return corrupt("dictionary section length")
	}
	trivialBytes := commonPrimaryUnifiedHeaderBytes
	for rank := 0; rank < v.env.Len(); rank++ {
		key, valueStart, end, ok := v.env.keyBounds(rank)
		if !ok {
			return corrupt("row bounds")
		}
		trivialBytes += len(key)
		if len(key) >= commonPrimaryLeafEscapeLength {
			trivialBytes++
		}
		if v.env.rankOverflow(rank) {
			trivialBytes += CommonPrimaryLeafOverflowBytes
			continue // the envelope validator already proved the PageRef
		}
		body := v.env.payload[valueStart:end]
		if len(body) == 0 {
			return corrupt("empty row body")
		}
		if body[0] == unifiedRowTrivial {
			if len(body) < 2 {
				return corrupt("trivial row body")
			}
			trivialBytes += len(body)
			continue
		}
		entry, ok := v.templateEntry(int(body[0]))
		if !ok {
			return corrupt("row template id")
		}
		renderBytes, valid := v.tokenStreamRenderBytes(
			body[1:], entry.holes,
		)
		if !valid {
			return corrupt("row token stream")
		}
		trivialBytes += 1 + len(entry.static) + renderBytes
	}
	if trivialBytes != v.trivialBytes ||
		!CommonPrimaryUnifiedTrivialFits(v.env.Len(), trivialBytes) {
		return corrupt("trivial content length")
	}
	return nil
}

// tokenStreamRenderBytes walks the hole tokens over body and reports their
// exact canonical render length while proving bounds and exact termination.
func (v *CommonPrimaryUnifiedLeafView) tokenStreamRenderBytes(
	body []byte, holes int,
) (int, bool) {
	cursor := 0
	renderBytes := 0
	for range holes {
		if cursor >= len(body) {
			return 0, false
		}
		tag := body[cursor]
		cursor++
		switch {
		case tag < unifiedTokenDictLimit:
			if int(tag) >= v.dictionaryCount {
				return 0, false
			}
			entry, ok := v.dictionaryEntry(int(tag))
			if !ok {
				return 0, false
			}
			renderBytes += len(entry)
		case tag >= unifiedTokenShortBase && tag < unifiedTokenShortBase+unifiedTokenShortMax:
			length := int(tag-unifiedTokenShortBase) + 1
			if length > len(body)-cursor {
				return 0, false
			}
			cursor += length
			renderBytes += length
		case tag == unifiedTokenLongLiteral:
			length, n, ok := readUnifiedTokenUvarint(body[cursor:])
			if !ok || length == 0 || uint64(length) > uint64(len(body)-cursor-n) {
				return 0, false
			}
			cursor += n + int(length)
			renderBytes += int(length)
		case tag == unifiedTokenTrue, tag == unifiedTokenNull:
			renderBytes += 4
		case tag == unifiedTokenFalse:
			renderBytes += 5
		case tag == unifiedTokenInt:
			value, n := DecodeZigzagVarint(body[cursor:])
			if n == 0 {
				return 0, false
			}
			cursor += n
			var integer [24]byte
			renderBytes += len(AppendCanonicalInt(integer[:0], value))
		default:
			// 0xFD/0xFE unassigned; 0xFF never appears inside a token stream.
			return 0, false
		}
	}
	return renderBytes, cursor == len(body)
}

// Len returns the number of live rows.
func (v *CommonPrimaryUnifiedLeafView) Len() int {
	if v == nil {
		return 0
	}
	return v.env.Len()
}

// Header returns the leaf's stable identity.
func (v *CommonPrimaryUnifiedLeafView) Header() CommonPrimaryLeafHeader {
	if v == nil {
		return CommonPrimaryLeafHeader{}
	}
	return v.env.Header()
}

// TrivialContentBytes returns the exact content bytes needed if every inline
// row in this leaf lost all compression and used the trivial row spelling.
func (v *CommonPrimaryUnifiedLeafView) TrivialContentBytes() int {
	if v == nil {
		return 0
	}
	return v.trivialBytes
}

// TemplateCount reports the leaf's template table size (census deliverable).
func (v *CommonPrimaryUnifiedLeafView) TemplateCount() int {
	if v == nil {
		return 0
	}
	return v.templateCount
}

// DictionaryCount reports the leaf's dictionary size.
func (v *CommonPrimaryUnifiedLeafView) DictionaryCount() int {
	if v == nil {
		return 0
	}
	return v.dictionaryCount
}

// TrivialRowCount scans the row directory and reports how many live rows are
// stored in trivial form — the census's trivial fraction.
func (v *CommonPrimaryUnifiedLeafView) TrivialRowCount() int {
	if v == nil {
		return 0
	}
	trivial := 0
	for rank := 0; rank < v.env.Len(); rank++ {
		_, valueStart, _, ok := v.env.keyBounds(rank)
		if !ok || v.env.rankOverflow(rank) {
			continue
		}
		if v.env.payload[valueStart] == unifiedRowTrivial {
			trivial++
		}
	}
	return trivial
}

// RowAt returns the borrowed key at lexical rank.
func (v *CommonPrimaryUnifiedLeafView) RowAt(rank int) ([]byte, bool) {
	if v == nil {
		return nil, false
	}
	key, _, ok := v.env.keyAt(rank)
	return key, ok
}

// RowRawAt returns the borrowed key and raw row body at lexical rank;
// overflow reports whether the body is the encoded 32-byte chain descriptor
// rather than a templated/trivial body.
func (v *CommonPrimaryUnifiedLeafView) RowRawAt(rank int) (key, body []byte, overflow, ok bool) {
	if v == nil {
		return nil, nil, false, false
	}
	key, valueStart, end, ok := v.env.keyBounds(rank)
	if !ok || valueStart < 0 || end < valueStart || end > len(v.env.payload) {
		return nil, nil, false, false
	}
	return key, v.env.payload[valueStart:end:end], v.env.rankOverflow(rank), true
}

// AllRows returns a sequential iterator over the admitted leaf's borrowed raw
// rows. Inline bodies remain encoded for
// [CommonPrimaryUnifiedLeafView.AppendAdmittedRowBody]; overflow bodies are
// fixed-size chain descriptors. The iterator and every slice it yields borrow
// the page lease that admitted v and must not outlive it.
func (v *CommonPrimaryUnifiedLeafView) AllRows() CommonPrimaryLeafIterator {
	if v == nil {
		return CommonPrimaryLeafIterator{}
	}
	return v.env.AllRows()
}

// PostingSlots returns the stable posting slot for every lexical row rank.
// Callers that walk a whole leaf compute the directory inversion once and
// index the returned fixed array by rank.
func (v *CommonPrimaryUnifiedLeafView) PostingSlots() (
	[CommonPrimaryLeafWideSlots]uint8, bool,
) {
	if v == nil {
		return [CommonPrimaryLeafWideSlots]uint8{}, false
	}
	return v.env.rankSlots()
}

// AppendRowBody splices the canonical document a templated or trivial row
// body encodes onto dst: trivial rows are one memcpy, templated rows
// interleave the template's static segments with the token renders. It
// fails closed (dst unchanged, false) on any structural violation rather than
// emitting partial bytes, and never reads outside the admitted payload.
func (v *CommonPrimaryUnifiedLeafView) AppendRowBody(dst, body []byte) ([]byte, bool) {
	if v == nil || len(body) == 0 {
		return dst, false
	}
	if body[0] == unifiedRowTrivial {
		if len(body) < 2 {
			return dst, false
		}
		return append(dst, body[1:]...), true
	}
	entry, ok := v.templateEntry(int(body[0]))
	if !ok {
		return dst, false
	}
	start := len(dst)
	cursor := 1
	segPrevious := uint32(0)
	for hole := 0; hole < entry.holes; hole++ {
		segEnd := binary.LittleEndian.Uint32(entry.ends[hole*4:])
		if segEnd < segPrevious || int(segEnd) > len(entry.static) {
			return dst[:start], false
		}
		dst = append(dst, entry.static[segPrevious:segEnd]...)
		segPrevious = segEnd
		if cursor >= len(body) {
			return dst[:start], false
		}
		tag := body[cursor]
		cursor++
		switch {
		case tag < unifiedTokenDictLimit:
			value, found := v.dictionaryEntry(int(tag))
			if !found {
				return dst[:start], false
			}
			dst = append(dst, value...)
		case tag >= unifiedTokenShortBase && tag < unifiedTokenShortBase+unifiedTokenShortMax:
			length := int(tag-unifiedTokenShortBase) + 1
			if length > len(body)-cursor {
				return dst[:start], false
			}
			dst = append(dst, body[cursor:cursor+length]...)
			cursor += length
		case tag == unifiedTokenLongLiteral:
			length, n, okLength := readUnifiedTokenUvarint(body[cursor:])
			if !okLength || uint64(length) > uint64(len(body)-cursor-n) {
				return dst[:start], false
			}
			cursor += n
			dst = append(dst, body[cursor:cursor+int(length)]...)
			cursor += int(length)
		case tag == unifiedTokenTrue:
			dst = append(dst, "true"...)
		case tag == unifiedTokenFalse:
			dst = append(dst, "false"...)
		case tag == unifiedTokenNull:
			dst = append(dst, "null"...)
		case tag == unifiedTokenInt:
			value, n := DecodeZigzagVarint(body[cursor:])
			if n == 0 {
				return dst[:start], false
			}
			cursor += n
			// Regeneration is byte-identical for every admitted spelling, as
			// pinned by the canonical-integer differential tests.
			dst = AppendCanonicalInt(dst, value)
		default:
			return dst[:start], false
		}
	}
	if cursor != len(body) {
		return dst[:start], false
	}
	return append(dst, entry.static[segPrevious:]...), true
}

// AppendAdmittedRowBody is the zero-allocation render path for a row body
// borrowed from this view's admitted envelope. PageCache admission has already
// performed AppendRowBody's complete structural walk: template ids, segment
// ends, dictionary ids, literal lengths, varints, and exact token-stream
// termination. A page lease keeps those immutable bytes stable for the call.
//
// Keeping the checked AppendRowBody entry is important for arbitrary-byte
// tests, verification, and defensive callers. Store read cursors use this
// admitted entry so whole-document reads do not repay the corruption proof for
// every row.
func (v *CommonPrimaryUnifiedLeafView) AppendAdmittedRowBody(dst, body []byte) []byte {
	if body[0] == unifiedRowTrivial {
		return append(dst, body[1:]...)
	}
	entry := v.admittedTemplateEntry(int(body[0]))
	cursor := 1
	segPrevious := 0
	for hole := 0; hole < entry.holes; hole++ {
		segEnd := int(binary.LittleEndian.Uint32(entry.ends[hole*4:]))
		dst = append(dst, entry.static[segPrevious:segEnd]...)
		segPrevious = segEnd
		tag := body[cursor]
		cursor++
		switch {
		case tag < unifiedTokenDictLimit:
			dst = append(dst, v.admittedDictionaryEntry(int(tag))...)
		case tag >= unifiedTokenShortBase && tag < unifiedTokenShortBase+unifiedTokenShortMax:
			length := int(tag-unifiedTokenShortBase) + 1
			dst = append(dst, body[cursor:cursor+length]...)
			cursor += length
		case tag == unifiedTokenLongLiteral:
			length, n, _ := readUnifiedTokenUvarint(body[cursor:])
			cursor += n
			dst = append(dst, body[cursor:cursor+int(length)]...)
			cursor += int(length)
		case tag == unifiedTokenTrue:
			dst = append(dst, "true"...)
		case tag == unifiedTokenFalse:
			dst = append(dst, "false"...)
		case tag == unifiedTokenNull:
			dst = append(dst, "null"...)
		case tag == unifiedTokenInt:
			value, n := DecodeZigzagVarint(body[cursor:])
			cursor += n
			dst = AppendCanonicalInt(dst, value)
		}
	}
	return append(dst, entry.static[segPrevious:]...)
}

// unifiedPrimaryRowRenderer is the sequential-scan resolver for one admitted
// unified leaf. Reset resolves the dictionary directory once per leaf, while
// Append retains the most recently used template. Lexically adjacent rows
// overwhelmingly share a template, so the scan loop avoids re-decoding both
// directories for every row and every token without allocating a side table.
type unifiedPrimaryRowRenderer struct {
	view             CommonPrimaryUnifiedLeafView
	template         unifiedTemplateView
	dictionaryData   []byte
	dictionaryBounds [unifiedTokenDictLimit]uint64
	templateID       uint8
	templateSet      bool
	threeHoles       bool
	threeStatic      [4][]byte
}

// CommonPrimaryUnifiedRowRenderer retains one admitted leaf's decoded
// dictionary directory and hottest template while a caller walks that leaf in
// lexical order. Its zero value is ready for Reset. It is single-consumer and
// borrows the admitted view until the next Reset.
//
// Use this instead of calling AppendAdmittedRowBody independently for every
// row: adjacent rows usually share a template, so the retained resolver avoids
// re-decoding the template and dictionary directories on the scan hot path.
type CommonPrimaryUnifiedRowRenderer struct {
	inner unifiedPrimaryRowRenderer
}

// Reset selects the admitted leaf and clears the prior template cache.
func (r *CommonPrimaryUnifiedRowRenderer) Reset(
	view CommonPrimaryUnifiedLeafView,
) {
	r.inner.Reset(view)
}

// Append reconstructs one admitted inline body into dst.
func (r *CommonPrimaryUnifiedRowRenderer) Append(dst, body []byte) []byte {
	return r.inner.Append(dst, body)
}

func (r *unifiedPrimaryRowRenderer) Reset(
	view CommonPrimaryUnifiedLeafView,
) {
	r.view = view
	r.templateSet = false
	r.dictionaryData =
		view.env.payload[view.dictionaryData:view.heapStart:view.heapStart]
	var previous uint32
	for id := 0; id < view.dictionaryCount; id++ {
		end := binary.LittleEndian.Uint32(
			view.env.payload[view.dictionaryDir+id*4:],
		)
		r.dictionaryBounds[id] =
			uint64(previous) | uint64(end)<<32
		previous = end
	}
}

// Append reconstructs one admitted inline row using Reset's leaf-local
// resolver state. Page admission has already proved every body and directory
// bound, so this is intentionally branch-light and fail-free.
func (r *unifiedPrimaryRowRenderer) Append(dst, body []byte) []byte {
	if body[0] == unifiedRowTrivial {
		return append(dst, body[1:]...)
	}
	templateID := body[0]
	if !r.templateSet || r.templateID != templateID {
		r.template = r.view.admittedTemplateEntry(int(templateID))
		r.templateID = templateID
		r.templateSet = true
		r.threeHoles = r.template.holes == 3
		if r.threeHoles {
			end0 := int(binary.LittleEndian.Uint32(r.template.ends))
			end1 := int(binary.LittleEndian.Uint32(r.template.ends[4:]))
			end2 := int(binary.LittleEndian.Uint32(r.template.ends[8:]))
			static := r.template.static
			r.threeStatic = [4][]byte{
				static[:end0:end0],
				static[end0:end1:end1],
				static[end1:end2:end2],
				static[end2:],
			}
		}
	}
	// Three scalar holes are the dominant document shape in ordered scans.
	// Its overwhelmingly common token signature is two canonical integers
	// followed by a short string. Recognize the admitted signature before
	// touching dst, then render it without the generic per-hole loop and
	// switch. Other signatures retain the complete generic path below.
	if r.threeHoles && body[1] == unifiedTokenInt {
		value0, n0 := DecodeZigzagVarint(body[2:])
		token1 := 2 + n0
		if body[token1] == unifiedTokenInt {
			value1, n1 := DecodeZigzagVarint(body[token1+1:])
			token2 := token1 + 1 + n1
			tag2 := body[token2]
			if tag2 >= unifiedTokenShortBase &&
				tag2 < unifiedTokenShortBase+unifiedTokenShortMax {
				length2 := int(tag2-unifiedTokenShortBase) + 1
				literal2 := token2 + 1
				if literal2+length2 == len(body) {
					dst = append(dst, r.threeStatic[0]...)
					dst = AppendCanonicalInt(dst, value0)
					dst = append(dst, r.threeStatic[1]...)
					dst = AppendCanonicalInt(dst, value1)
					dst = append(dst, r.threeStatic[2]...)
					dst = append(dst, body[literal2:]...)
					return append(dst, r.threeStatic[3]...)
				}
			}
		}
	}
	entry := r.template
	cursor := 1
	segPrevious := 0
	for hole := 0; hole < entry.holes; hole++ {
		segEnd := int(binary.LittleEndian.Uint32(entry.ends[hole*4:]))
		dst = append(dst, entry.static[segPrevious:segEnd]...)
		segPrevious = segEnd
		tag := body[cursor]
		cursor++
		switch {
		case tag < unifiedTokenDictLimit:
			bounds := r.dictionaryBounds[tag]
			start, end := uint32(bounds), uint32(bounds>>32)
			dst = append(dst, r.dictionaryData[start:end]...)
		case tag >= unifiedTokenShortBase &&
			tag < unifiedTokenShortBase+unifiedTokenShortMax:
			length := int(tag-unifiedTokenShortBase) + 1
			dst = append(dst, body[cursor:cursor+length]...)
			cursor += length
		case tag == unifiedTokenLongLiteral:
			length, n, _ := readUnifiedTokenUvarint(body[cursor:])
			cursor += n
			dst = append(dst, body[cursor:cursor+int(length)]...)
			cursor += int(length)
		case tag == unifiedTokenTrue:
			dst = append(dst, "true"...)
		case tag == unifiedTokenFalse:
			dst = append(dst, "false"...)
		case tag == unifiedTokenNull:
			dst = append(dst, "null"...)
		case tag == unifiedTokenInt:
			value, n := DecodeZigzagVarint(body[cursor:])
			cursor += n
			dst = AppendCanonicalInt(dst, value)
		}
	}
	return append(dst, entry.static[segPrevious:]...)
}

// AppendRawRank splices the canonical document at lexical rank onto dst. It
// reports false for overflow rows (whose canonical bytes live in the chain)
// and on structural corruption.
func (v *CommonPrimaryUnifiedLeafView) AppendRawRank(dst []byte, rank int) ([]byte, bool) {
	_, body, overflow, ok := v.RowRawAt(rank)
	if !ok || overflow {
		return dst, false
	}
	return v.AppendRowBody(dst, body)
}

// LookupBodySlotHashed resolves key to its stable slot and raw row body (or
// overflow descriptor) with the caller-supplied key hash. The body borrows the
// admitted page.
func (v *CommonPrimaryUnifiedLeafView) LookupBodySlotHashed(
	hash uint64, key []byte,
) (slot uint8, body []byte, overflow, ok bool) {
	if v == nil {
		return 0, nil, false, false
	}
	return v.env.LookupRawHashed(hash, key)
}

// LookupBodyHashed is the point-read hot path when the stable slot is not
// needed.
func (v *CommonPrimaryUnifiedLeafView) LookupBodyHashed(
	hash uint64, key []byte,
) (body []byte, overflow, ok bool) {
	_, body, overflow, ok = v.LookupBodySlotHashed(hash, key)
	return body, overflow, ok
}

// ChooseInsertSlotHashed returns the stable slot a new row may claim while
// preserving every slot already present in the durable envelope and every bit
// set in additional. additional represents overlay-native inserts that are not
// in the durable envelope yet.
func (v *CommonPrimaryUnifiedLeafView) ChooseInsertSlotHashed(
	hash uint64, additional [4]uint64,
) (uint8, bool) {
	if v == nil || v.env.class != CommonPrimaryLeafWide {
		return 0, false
	}
	occupied := func(slot uint8) bool {
		return additional[slot>>6]&(uint64(1)<<uint(slot&63)) != 0
	}
	first, second := commonPrimaryLeafGroups(hash)
	groups := [2]uint8{first, second}
	homes := [2]uint8{
		uint8(hash>>16) & (commonPrimaryLeafGroupSize - 1),
		uint8(hash>>20) & (commonPrimaryLeafGroupSize - 1),
	}
	for groupIndex := range 2 {
		base := groups[groupIndex] * commonPrimaryLeafGroupSize
		for ordinal := range uint8(commonPrimaryLeafGroupSize) {
			slot := base + (homes[groupIndex]+ordinal)&
				(commonPrimaryLeafGroupSize-1)
			if v.env.payload[v.env.layout.controlStart+int(slot)] == 0 &&
				!occupied(slot) {
				return slot, true
			}
		}
	}
	mask := v.env.stashMask()
	for stash := 0; stash < v.env.class.stashSlots(); stash++ {
		slot := uint8(CommonPrimaryLeafNormalSlots + stash)
		if mask&(uint64(1)<<uint(stash)) == 0 && !occupied(slot) {
			return slot, true
		}
	}
	return 0, false
}

// FirstRankFrom returns the first lexical rank whose key is >= lower (0 for
// an empty lower), for ordered scans.
func (v *CommonPrimaryUnifiedLeafView) FirstRankFrom(lower []byte) int {
	if v == nil || len(lower) == 0 {
		return 0
	}
	return v.env.LowerBound(lower)
}

// RenderRecords reconstructs every row as a raw CommonPrimaryLeafRecord for
// the structural mutation rewrite. Canonical renders
// are written contiguously into heap (grown as needed and returned); record
// values borrow that heap, record keys borrow the admitted page, and overflow
// rows keep their chain reference untouched.
func (v *CommonPrimaryUnifiedLeafView) RenderRecords(
	records []CommonPrimaryLeafRecord, heap []byte,
) ([]CommonPrimaryLeafRecord, []byte, error) {
	if v == nil {
		return records, heap, ErrCommonPrimaryLeafCorrupt
	}
	records = records[:0]
	heap = heap[:0]
	n := v.env.Len()
	slots, slotsOK := v.env.rankSlots()
	if !slotsOK {
		return records, heap, ErrCommonPrimaryLeafCorrupt
	}
	spans := make([][2]int, n)
	overflowRefs := make([]PageRef, n)
	for rank := 0; rank < n; rank++ {
		_, body, overflow, ok := v.RowRawAt(rank)
		if !ok {
			return records, heap, ErrCommonPrimaryLeafCorrupt
		}
		if overflow {
			overflowRefs[rank] = decodePageRef(body)
			spans[rank] = [2]int{-1, -1}
			continue
		}
		start := len(heap)
		out, rendered := v.AppendRowBody(heap, body)
		if !rendered {
			return records, heap, ErrCommonPrimaryLeafCorrupt
		}
		heap = out
		spans[rank] = [2]int{start, len(heap)}
	}
	for rank := 0; rank < n; rank++ {
		key, ok := v.RowAt(rank)
		if !ok {
			return records, heap, ErrCommonPrimaryLeafCorrupt
		}
		value := CommonPrimaryLeafValue{}
		if spans[rank][0] < 0 {
			value.Overflow = overflowRefs[rank]
		} else {
			value.Inline = heap[spans[rank][0]:spans[rank][1]:spans[rank][1]]
		}
		records = append(records, CommonPrimaryLeafRecord{
			Slot: slots[rank], Key: key, Value: value,
		})
	}
	return records, heap, nil
}

// renderRecordsInto is RenderRecords over the writer's fixed mutation
// workspace. It is deliberately separate from the public slice-returning
// helper: the scratch owns span/reference arrays as well as the output slices,
// removing every temporary allocation from a warmed cold-leaf conversion.
func (v *CommonPrimaryUnifiedLeafView) renderRecordsInto(
	s *PrimaryLeafMutationScratch,
) error {
	if v == nil || !s.reset(v.env.Len()) {
		return ErrCommonPrimaryLeafCorrupt
	}
	n := v.env.Len()
	slots, slotsOK := v.env.rankSlots()
	if !slotsOK {
		return ErrCommonPrimaryLeafCorrupt
	}
	spans := s.spans[:n]
	overflowRefs := s.overflowRefs[:n]
	for rank := 0; rank < n; rank++ {
		_, body, overflow, ok := v.RowRawAt(rank)
		if !ok {
			return ErrCommonPrimaryLeafCorrupt
		}
		if overflow {
			overflowRefs[rank] = decodePageRef(body)
			spans[rank] = [2]int{-1, -1}
			continue
		}
		start := len(s.heap)
		out, rendered := v.AppendAdmittedRowBody(s.heap, body), true
		if !rendered || len(out) > cap(s.heap) {
			return ErrCommonPrimaryLeafCorrupt
		}
		s.heap = out
		spans[rank] = [2]int{start, len(s.heap)}
	}
	for rank := 0; rank < n; rank++ {
		key, ok := v.RowAt(rank)
		if !ok {
			return ErrCommonPrimaryLeafCorrupt
		}
		value := CommonPrimaryLeafValue{}
		if spans[rank][0] < 0 {
			value.Overflow = overflowRefs[rank]
		} else {
			value.Inline = s.heap[spans[rank][0]:spans[rank][1]:spans[rank][1]]
		}
		s.records = append(s.records, CommonPrimaryLeafRecord{
			Slot: slots[rank], Key: key, Value: value,
		})
	}
	return nil
}

// RenderRecordsWithScratch reconstructs the leaf's canonical row set into one
// retained mutation workspace. The returned records borrow both the admitted
// page (keys) and scratch (values) until the next scratch use.
func (v *CommonPrimaryUnifiedLeafView) RenderRecordsWithScratch(
	scratch *PrimaryLeafMutationScratch,
) ([]CommonPrimaryLeafRecord, error) {
	if err := v.renderRecordsInto(scratch); err != nil {
		return nil, err
	}
	return scratch.records, nil
}
