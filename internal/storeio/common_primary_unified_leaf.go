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
// template rows inside the proven succinct ordered envelope (unified-leaf
// design §3.1). The envelope machinery — stable cuckoo hash slots, control
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
//	template section: cumulative u32 ends (relative to the entry data start),
//	  then per template the document_group entry layout (§3.3, reused):
//	  u16 holeCount | u16 zero | u32 staticBytes |
//	  (holeCount+1)×u32 cumulative static-segment ends | static bytes
//	dictionary section: cumulative u32 ends + raw canonical value spellings
//	record heap (physically lexical, succinct boundaries as today):
//	  per record: [key bytes][row body]
//	  row body = templateID u8 | token stream         (templated row)
//	           | 0xFF          | canonical JSON bytes (trivial row, §3.5)
//	           | 32-byte PageRef                      (overflow bitmap set)
//
// The stored logical value of every row is its vibejson canonical form
// (§3.2): the encoder canonicalizes each input document (already-canonical
// input is detected in the same walk and aliased, the §8 steady state), and
// every render reproduces exactly those canonical bytes.
const (
	// CommonPrimaryLeafUnified is the class-5 unified canonical template-row
	// leaf. Like classes 3/4 its payload is not the raw narrow/wide value heap,
	// so CommonPrimaryLeafClass.slots() reports zero for it and readers must
	// dispatch on the class byte; unlike classes 3/4 its posting-stable slot is
	// the envelope's stable cuckoo hash slot (§3.1: one slot discipline).
	CommonPrimaryLeafUnified CommonPrimaryLeafClass = 5

	// commonPrimaryUnifiedHeaderBytes frames the two unified sections. The
	// counts are duplicated from the section directories so an open can bound
	// every section arithmetic check before touching section bytes.
	commonPrimaryUnifiedHeaderBytes = 12

	// Token tags (§3.4). Dict ids and short-literal ranges are disjoint,
	// following the document-group scheme, extended with the typed tags whose
	// losslessness the design proves from the JSON grammar.
	unifiedTokenDictLimit   = 128  // dict ids occupy tags 0x00..0x7F
	unifiedTokenShortBase   = 0x80 // short literal lengths 1..120 → 0x80..0xF7
	unifiedTokenShortMax    = 120
	unifiedTokenLongLiteral = 0xF8 // uvarint length + spelling bytes
	unifiedTokenTrue        = 0xF9 // regeneration is identity by grammar (§3.4)
	unifiedTokenFalse       = 0xFA
	unifiedTokenNull        = 0xFB
	unifiedTokenInt         = 0xFC // canonical-int admission (§3.4), zigzag varint
	// Tags 0xFD/0xFE are unassigned and fail closed; 0xFF is never a token tag
	// because it is the row-level trivial escape, which keeps the row tag space
	// unambiguous when a decoder resynchronizes at a row boundary.

	// unifiedRowTrivial marks a row stored as its whole canonical spelling
	// (§3.5): the escape hatch inside the one grammar, not a class.
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
// without dictionary participation (the §3.5 predicate operand).
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
	// entryBytes is the §3.5 template-entry cost 8 + (holes+1)·4 + staticBytes,
	// exactly the document_group entry layout this class reuses.
	entryBytes int
}

// unifiedPrefixPlan is the deterministic derivation for one row prefix: which
// shapes template, in which id order, with which dictionary, and how many
// bytes each section and the heap take. It is a pure function of the prefix's
// rows (§7.4 fold purity), which is what makes bulk build and the later fold
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
	spans        []DocumentGroupSpan
	rows         []unifiedPrimaryLeafRow
	shapes       []unifiedPrimaryLeafShape
	records      []CommonPrimaryLeafRecord
	plan         unifiedPrefixPlan
	shapeRows    []int32
	shapeSavings []int64
	counts       map[string]int
	dictionary   []documentGroupDictionaryCandidate
	dictionaryID map[string]uint8
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
// document from its tape. The criterion is the document-group model the
// design reuses (§3.3): a non-key tape leaf with Next == 1, which places
// holes at scalar leaves of arbitrary depth and makes empty containers holes
// too — nesting never disqualifies (§6).
func appendHoleSpans(dst []DocumentGroupSpan, index vibejson.Index) []DocumentGroupSpan {
	for i := range index.Entries {
		e := &index.Entries[i]
		if e.Flags()&vibejson.TapeFlagKey != 0 || e.Next != 1 {
			continue
		}
		dst = append(dst, DocumentGroupSpan{Start: e.Start, End: e.End})
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
// dictionary participation: the typed tags of §3.4 where identity is provable
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
	return 1 + documentGroupUvarintLen(uint32(len(v))) + len(v)
}

// shapeEqual reports whether rows a and b share one skeleton: identical hole
// counts and identical static bytes between (and around) the holes. This is
// documentGroupStaticEqual restated over the builder's span storage.
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
// input skips the render (the §8 steady state: stored-and-rendered bytes are
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
			// chain itself holds the canonical spelling (§6) and the row takes
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
				// The §3.5 template-entry cost, verified against the reused
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
// first count extracted rows. Order of derivation follows §7.4: template
// census first, then dictionary selection, then token sizing. The §3.5
// amortization predicate is evaluated with dictionary-free typed token costs:
// the design's tokenStreamLen cannot include dictionary references without a
// circular dependency (which shapes template decides which rows feed the
// dictionary, per the §7.4 order), so the predicate uses the conservative
// dictionary-free cost — a shape that templates under it always saves at
// least what the predicate charged, and the choice is a pure function of the
// prefix's rows. This deviation is flagged in the stage report.
func (b *UnifiedPrimaryLeafBuilder) planPrefix(count int) (*unifiedPrefixPlan, error) {
	if count < 1 || count > len(b.rows) {
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
		// Per §3.5: each templated row saves canonicalLen − tokenStreamLen − 1,
		// the −1 charging the row its templateID byte.
		b.shapeSavings[row.shape] += int64(row.length) - int64(row.tokensNoDict) - 1
	}
	// Shapes are admitted in first-use lexical-rank order (§3.3), which is the
	// order b.shapes already holds. The template-id budget (≤ 255, the row tag
	// space minus the trivial escape) is enforced in the same order so the
	// choice stays a pure function of the row set; in practice the predicate
	// makes an overflow unreachable because every admitted shape must be
	// net-byte-positive across the ≤ 256 rows of one leaf.
	for s := range b.shapes {
		if b.shapeRows[s] == 0 || len(plan.templateShapes) >= unifiedMaxTemplates {
			continue
		}
		// Ties template (§3.5): equality is admission, fixed so that two
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
	if b.dictionaryID == nil {
		b.dictionaryID = make(map[string]uint8, unifiedTokenDictLimit)
	} else {
		clear(b.dictionaryID)
	}
	b.dictionary = b.dictionary[:0]
	for i := 0; i < count; i++ {
		row := &b.rows[i]
		if row.shape < 0 || plan.templated[row.shape] < 0 {
			continue
		}
		canonical := b.canonicalOf(i)
		for _, span := range b.spans[row.spanStart:row.spanEnd] {
			// The builder heap and the borrowed records are stable for the
			// whole plan cycle, so a read-only string view avoids copying
			// every scalar merely to count candidates.
			b.counts[byteview.String(canonical[span.Start:span.End])]++
		}
	}
	for value, n := range b.counts {
		cost := unifiedTypedTokenCost(byteview.Bytes(value))
		saving := n*cost - (n + 4 + len(value))
		if saving > 0 {
			b.dictionary = append(b.dictionary, documentGroupDictionaryCandidate{
				value: value, count: n, saving: saving,
			})
		}
	}
	slices.SortFunc(b.dictionary, func(x, y documentGroupDictionaryCandidate) int {
		if x.saving != y.saving {
			return y.saving - x.saving
		}
		return strings.Compare(x.value, y.value)
	})
	if len(b.dictionary) > unifiedTokenDictLimit {
		b.dictionary = b.dictionary[:unifiedTokenDictLimit]
	}
	for id, candidate := range b.dictionary {
		b.dictionaryID[candidate.value] = uint8(id)
		plan.dictionaryBytes += 4 + len(candidate.value)
	}

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
			n := putDocumentGroupUvarint(lengthBytes[:], uint32(len(value)))
			dst = append(dst, lengthBytes[:n]...)
		}
		dst = append(dst, value...)
	}
	return dst
}

// rawRenderCapacity returns the largest row prefix whose canonical renders
// fit one raw wide envelope of extentBytes. This cap is a U1-phase-only
// deviation from §3.6 (which deletes the de-templatability cap outright): in
// this phase mutations to a class-5 leaf still route through the existing
// structural rewrite, which renders every row into a raw wide envelope of at
// most 64 KiB before re-fitting, so a unified leaf must never hold more rows
// than that envelope can carry. The cap dies with that path in U2.
func (b *UnifiedPrimaryLeafBuilder) rawRenderCapacity(extentBytes int) int {
	capacity := extentBytes - PageHeaderSize - PageTrailerSize
	recordBytes := 0
	count := 0
	for count < CommonPrimaryLeafWideSlots && count < len(b.rows) {
		valueBytes := int(b.rows[count].length)
		if b.rows[count].shape < 0 {
			valueBytes = CommonPrimaryLeafOverflowBytes
		}
		grown := recordBytes + len(b.records[count].Key) + valueBytes
		if len(b.records[count].Key) >= commonPrimaryLeafEscapeLength {
			grown++
		}
		layout := commonPrimaryLeafLayoutFor(
			CommonPrimaryLeafWide, count+1, extentBytes,
		)
		if layout.heapStart+grown > capacity {
			break
		}
		recordBytes = grown
		count++
	}
	return count
}

// unifiedLeafExtents are the physical extents the §3.6 packing search tries,
// smallest first so an equal bytes-per-row tie keeps the smaller extent.
var unifiedLeafExtents = [...]int{
	CommonPrimaryLeafNarrowBytes, CommonPrimaryLeafWideBytes,
	16 << 10, 32 << 10, 64 << 10,
}

// planUnifiedLeaf runs the single packing planner of §3.6 over the front of
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
	// n−1 (§3.6), so the largest placeable prefix binary-searches.
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
	extent := int(header.PageSize)
	logicalID, logicalOK := CommonPrimaryLeafLogicalID(header.Bucket)
	if b == nil || !validPhysicalPageSize(header.PageSize) ||
		extent < CommonPrimaryLeafNarrowBytes ||
		extent > CommonPrimaryLeafMaxExtentBytes ||
		len(dst) < extent || header.StoreID == ([16]byte{}) ||
		header.Generation == 0 || header.Generation >= uint64(1)<<48 ||
		seed == ([16]byte{}) || len(records) == 0 ||
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
	if err := PlaceCommonPrimaryLeafRecords(
		CommonPrimaryLeafWide, seed, records,
	); err != nil {
		return nil, err
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
	payload[0] = byte(len(records) - 1)
	payload[2] = byte(CommonPrimaryLeafUnified)

	// Envelope tables: identical machinery and byte meaning to the wide raw
	// class (§3.1 — the slot, lookup, and boundary machinery is reused byte
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
	if templateCount > unifiedMaxTemplates ||
		dictionaryCount > unifiedTokenDictLimit ||
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
// a violation anywhere rejects the leaf (the U1 corruption gate).
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
		payload[2] != byte(CommonPrimaryLeafUnified) ||
		len(src) < int(pageHeader.PageSize) ||
		pageHeader.LogicalID != logicalID ||
		pageHeader.LogicalID != expected.LogicalID ||
		pageHeader.Generation != expected.Generation ||
		pageHeader.PageSize != expected.Length ||
		pageHeader.Kind != expected.Kind || pageHeader.Flags != 0 {
		return CommonPrimaryUnifiedLeafView{}, fmt.Errorf(
			"%w: common identity or unified header", ErrCommonPrimaryLeafCorrupt,
		)
	}
	// A unified leaf always holds at least one row: the empty creation leaf
	// stays a narrow raw leaf, and the planner never emits an empty plan, so
	// the class-5 empty spelling (0x80 flag) is rejected outright.
	count := int(payload[0]) + 1
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
		// the unified class shares the wide slot geometry exactly (§3.1).
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
	if payload[2] != byte(CommonPrimaryLeafUnified) {
		return CommonPrimaryUnifiedLeafView{}, false
	}
	count := int(payload[0]) + 1
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

// validateSections proves every unified section independently: the template
// directory and each entry's segment table, the dictionary directory, and —
// per row — the body classification and a complete bounds-checked token walk
// that must consume the body exactly. Fail-closed everywhere (U1 gate).
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
	for rank := 0; rank < v.env.Len(); rank++ {
		_, valueStart, end, ok := v.env.keyBounds(rank)
		if !ok {
			return corrupt("row bounds")
		}
		if v.env.rankOverflow(rank) {
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
			continue
		}
		entry, ok := v.templateEntry(int(body[0]))
		if !ok {
			return corrupt("row template id")
		}
		if !v.tokenStreamValid(body[1:], entry.holes) {
			return corrupt("row token stream")
		}
	}
	return nil
}

// tokenStreamValid walks holes tokens over body and reports whether the walk
// is in bounds, every tag is assigned, every dictionary reference resolves,
// every zigzag varint decodes, and the stream ends exactly at the body end.
func (v *CommonPrimaryUnifiedLeafView) tokenStreamValid(body []byte, holes int) bool {
	cursor := 0
	for range holes {
		if cursor >= len(body) {
			return false
		}
		tag := body[cursor]
		cursor++
		switch {
		case tag < unifiedTokenDictLimit:
			if int(tag) >= v.dictionaryCount {
				return false
			}
		case tag >= unifiedTokenShortBase && tag < unifiedTokenShortBase+unifiedTokenShortMax:
			length := int(tag-unifiedTokenShortBase) + 1
			if length > len(body)-cursor {
				return false
			}
			cursor += length
		case tag == unifiedTokenLongLiteral:
			length, n, ok := readDocumentGroupUvarint(body[cursor:])
			if !ok || length == 0 || uint64(length) > uint64(len(body)-cursor-n) {
				return false
			}
			cursor += n + int(length)
		case tag == unifiedTokenTrue, tag == unifiedTokenFalse, tag == unifiedTokenNull:
			// tag-only
		case tag == unifiedTokenInt:
			_, n := DecodeZigzagVarint(body[cursor:])
			if n == 0 {
				return false
			}
			cursor += n
		default:
			// 0xFD/0xFE unassigned; 0xFF never appears inside a token stream.
			return false
		}
	}
	return cursor == len(body)
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
// stored in trivial form (§3.5) — the census deliverable's trivial fraction.
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
	if !ok {
		return nil, nil, false, false
	}
	return key, v.env.payload[valueStart:end:end], v.env.rankOverflow(rank), true
}

// AppendRowBody splices the canonical document a templated or trivial row
// body encodes onto dst: trivial rows are one memcpy, templated rows
// interleave the template's static segments with the token renders (§5). It
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
			length, n, okLength := readDocumentGroupUvarint(body[cursor:])
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
			// Regeneration is byte-identical for every admitted spelling (§3.4
			// losslessness, pinned by the U0 differential tests).
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

// LookupBodyHashed is the point-read hot path: the envelope's stable-slot
// lookup resolves key to its raw row body (or overflow descriptor) with the
// caller-supplied key hash, borrowing the admitted page.
func (v *CommonPrimaryUnifiedLeafView) LookupBodyHashed(
	hash uint64, key []byte,
) (body []byte, overflow, ok bool) {
	if v == nil {
		return nil, false, false
	}
	_, body, overflow, ok = v.env.LookupRawHashed(hash, key)
	return body, overflow, ok
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
// the U1 mutation path (the existing structural rewrite). Canonical renders
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
		records = append(records, CommonPrimaryLeafRecord{Key: key, Value: value})
	}
	return records, heap, nil
}
