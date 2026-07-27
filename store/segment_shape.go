package store

import (
	"runtime"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// Shape-deduplicated tapes: the Segment storage mode behind Segment.ShapeTapes.
//
// A shape-clustered corpus stores the same ordered key sequence millions of
// times, and the classic tape stores it again per document: a flat object of
// N members costs 2N+1 entries, and N of them are key entries whose spelling
// the ShapeCache has already compiled once for the whole cluster. This mode
// stops paying for them. A conforming document — a flat root object whose
// key sequence byte-matches a compiled shape — keeps only its N value
// entries, in shape ordinal order, plus one compact header naming the shape
// and the root's source span:
//
//	classic  [ obj | key0 | val0 | key1 | val1 | ... ]   (2N+1) x 16 B
//	dedup    [ val0 | val1 | ... ]                        N x 16 B
//	         + shapeTapeRef{shape, root span}             one per document
//
// The keys live once, in the shape record; the per-document tape becomes a
// dense value array. That is both the space win (the tape roughly halves on
// object-heavy corpora) and the speed win: extracting a field from a
// conforming document is one shape-pointer compare and one array index — no
// header proof, no key verification, no suffix scan, and no source-byte
// touch, which on corpora larger than cache is the difference between
// streaming one tape line per document and faulting scattered source lines.
//
// Scope, stated plainly because the win above reads as more general than it
// is: conformance requires a *flat* root, and the gate is the identity
// root.next == 2*count+1. A root holding any nested object or array fails it,
// and one nested member is enough. On a corpus whose documents carry so much
// as a single sub-object this mode engages for zero documents, however
// shape-clustered that corpus otherwise is — the benchmark corpus under
// bench/competitive is exactly such a case, and none of its 100,000 documents
// conform. DocumentTemplate is the mechanism that covers nested shapes,
// because it matches a whole layout rather than requiring flatness, and it is
// what carries that corpus. Neither mechanism subsumes the other: this one is
// cheaper where it applies, and it applies to flat-root documents only. Do not
// quote a tape-footprint figure from a flat-root corpus as though it described
// nested data.
//
// Dual widths: the value array itself comes in two entry widths, chosen per
// document at ingest. The flatness identity the conformance gate already
// proves (root.next == 2*count+1) forces every member value to exactly one
// entry, so a dedup value entry never carries structure: its next is
// provably 1 (the root's span identity holds only if every member subtree is
// one entry, and a one-entry value's next is its subtree size) and its count
// bits are zero (scalars and empty containers alike). Position is ordinal.
// What remains is the span and the kind/flag bits, and for a document whose
// root ends within the first 64 KiB of its source those pack into eight
// bytes — shapeNarrowValue, half the classic width. Documents with a wider
// root span keep 16-byte value entries; the width is recorded per document
// in its header ref, and both widths reconstitute the identical classic
// entry on widening, so the choice is invisible to every read. Narrow
// arrays live in one segment-wide slab (Segment.narrow) rather than the pinned
// entry arena: no caller ever holds a pointer into them — hot paths copy
// eight bytes out, cold paths widen — so the slab may relocate as it grows.
//
// The trust boundary shifts, deliberately, from read time to ingest time.
// The classic shape paths never trust the fingerprint for reads: every
// positional read re-verifies key bytes because a fingerprint collision must
// never misread a field. Dropping key entries makes read-time verification
// impossible, so the proof moves to the single place the key bytes are still
// on hand: shapeTapeCompact byte-compares every key of the freshly built
// tape against the shape's compiled raw spellings before any key entry is
// dropped. A document is stored in dedup form only after that exact match,
// so the stored shape reference is proven, not fingerprint-routed, and read
// paths may index the value array with no residual collision deviation.
// Whatever fails the proof — a first-sighted layout under the cache's
// sighting economics, a non-flat or non-object root, an empty object, a
// fingerprint collision, or a shape with duplicate decoded names (dropping
// keys would erase which member a spelling names; see shapeRecord.dupKeys) —
// is stored classic, unchanged.
//
// Doc(i) keeps its one spelling and full contract in this mode: it always
// returns a fully functional classic Index. For a shape-taped document the
// classic tape no longer exists, so the first access synthesizes it — the
// header from the ref, each key's span recovered by a backward scan from its
// value (exact because ingest proved the spelling), each value copied
// verbatim, enrichment re-applied if the original build had it — and caches
// it for the segment's lifetime, so handles stay stable and repeat access is an
// ordinary map hit. The synthesized tape is entry-for-entry identical to
// what classic mode would have stored, which the differential tests pin.
// The costs are documented at Doc: a first access allocates the classic
// tape, and a caller that widens every document has re-bought the storage
// this mode dropped. Engines that want the space win extract through the
// fused batch primitives, which run natively on the value arrays.
//
// Everything here is safe Go: the compaction, the conformance proof, the
// widening scan, and the batch reads use ordinary slice indexing, bounded by
// the ingest invariant that a dedup document's entry count equals its
// shape's field count.

// A ShapeTapeRef is the per-document header of a shape-deduplicated tape:
// the proven shape, the root object's source span, the entry width with the
// narrow array's slab position, and whether the original build enriched key
// hashes (so widening reproduces the exact classic tape). The zero ref (nil
// rec) marks a classic document.
type ShapeTapeRef struct {
	Rec      *ShapeRecord
	Start    uint32 // root object's first source byte, per-document coordinates
	End      uint32 // one past the root's closing brace
	off      uint32 // Narrow only: first value entry in the set's Narrow slab
	Narrow   bool   // value entries are 8-byte shapeNarrowValues in the slab
	enriched bool   // the classic tape carried key-hash enrichment
}

// ShapeNarrowMaxEnd is the widest root span the narrow width can address:
// every stored offset is bounded by the root's end, so one comparison against
// it decides a document's entry width. The boundary is exact — a root ending
// at offset 65535 packs, one ending at 65536 does not.
const ShapeNarrowMaxEnd = 1<<16 - 1

// A ShapeNarrowValue is one 8-byte value entry of a narrow shape tape: the
// value's document-coordinate span packed into one word and the classic
// entry's info word verbatim. The info word is stored whole rather than just
// its live top six bits (kind and flags; the count bits are zero by the
// flatness proof) so kind tests written against classic entries — the typed
// extractors' masked integer probe — run on it unchanged, and widening is
// two shifts and a constant.
type ShapeNarrowValue struct {
	Span uint32 // start | end<<16, offsets within the document's source
	Info uint32 // the classic value entry's packed Info word, verbatim
}

// start and end unpack the span's document coordinates.
func (n ShapeNarrowValue) Start() uint32 { return n.Span & 0xFFFF }
func (n ShapeNarrowValue) End() uint32   { return n.Span >> 16 }

// widen reconstitutes the classic 16-byte entry this value was packed from,
// bit-identical by the dedup invariants: next is 1 for every single-entry
// value and the Info word was stored verbatim.
func (n ShapeNarrowValue) widen() vibejson.IndexEntry {
	return vibejson.IndexEntry{Start: n.Span & 0xFFFF, End: n.Span >> 16, Next: 1, Info: n.Info}
}

// ShapeTapeRefAt returns document i's dedup header, or the zero ref for a
// classic document and for sets where the mode never stored one. The refs
// slice is either empty or aligned with docs (commitDoc's invariant), so the
// single bounds test covers both.
func (s *Segment) ShapeTapeRefAt(i int) ShapeTapeRef {
	if s.mappedDocs != nil {
		index := s.mappedBase + uint64(i)
		var shapeID uint32
		var start, end uint32
		var kind uint8
		var enriched bool
		if s.mappedDocs.compactRefs != nil {
			r := &s.mappedDocs.compactRefs[index]
			shapeID = uint32(r.meta)
			kind, enriched = r.kind, r.flags&1 != 0
			if kind == persistDocClassic {
				shapeID = storeMappedNoShape
			}
		} else if s.mappedDocs.ownedRefs != nil {
			r := &s.mappedDocs.ownedRefs[index]
			shapeID, start, end = uint32(r.shapeID), uint32(r.start), uint32(r.end)
			kind, enriched = r.kind, r.flags&1 != 0
			if r.shapeID == storeOwnedNoShape {
				shapeID = storeMappedNoShape
			}
		} else {
			r := &s.mappedDocs.refs[index]
			shapeID, start, end = r.shapeID, r.start, r.end
			kind, enriched = r.kind, r.enriched
		}
		if shapeID == storeMappedNoShape || storeOwnedDocIsTemplate(kind) {
			runtime.KeepAlive(s.mappedDocs)
			return ShapeTapeRef{}
		}
		ref := ShapeTapeRef{
			Rec: s.mappedShapes[shapeID], Start: start, End: end,
			Narrow: kind == persistDocNarrow || storeOwnedDocIsNarrow(kind), enriched: enriched,
		}
		runtime.KeepAlive(s.mappedDocs)
		return ref
	}
	if uint(i) < uint(len(s.tapeRefs)) {
		return s.tapeRefs[i]
	}
	return ShapeTapeRef{}
}

// shapeTapeRootSpan supplies the cold root coordinates omitted by compact
// Builder row refs. Ordinary field scans never call it; empty-pointer
// gathers, classic-tape widening, and checkpoint expansion recover the exact
// trimmed parser span from validated source when needed.
func (s *Segment) shapeTapeRootSpan(doc vibejson.Index, ref ShapeTapeRef) (uint32, uint32) {
	if ref.End == 0 && s.mappedDocs != nil && s.mappedDocs.compactRefs != nil {
		return storeRootSpan(doc.Src)
	}
	return ref.Start, ref.End
}

// commitDoc appends one successfully built document and its dedup header,
// padding the refs slice with zero refs for any earlier classic-only prefix
// so the alignment invariant holds: tapeRefs is empty until the first dedup
// document and exactly docs-aligned after. It returns the new ordinal.
func (s *Segment) commitDoc(index vibejson.Index, ref ShapeTapeRef) int {
	if ref.Rec != nil || s.tapeRefs != nil {
		for len(s.tapeRefs) < len(s.docs) {
			s.tapeRefs = append(s.tapeRefs, ShapeTapeRef{})
		}
		s.tapeRefs = append(s.tapeRefs, ref)
	}
	s.docs = append(s.docs, index)
	ord := len(s.docs) - 1
	if s.Postings {
		// The document is committed and its ordinal live; a narrow shape tape's
		// values are already in s.narrow, so the postings read the stored form.
		s.indexPostings(ord, index, ref)
	}
	if s.ValueDict {
		// The value dictionary's ingest hook, alongside shape compaction and the
		// postings: it interns the just-committed document's repeated value spans
		// and records its splice header (segment_valuedict.go). It runs after the
		// document is stored so it can walk the document's classic tape —
		// synthesized transiently for a shape-taped document, so shape and value
		// dedup compose without either re-buying the other's storage. It records
		// separate splice entries and does not mutate the committed tape, so the
		// postings above read the same value content either way.
		s.valueDictAppend(ord, ref)
	}
	return ord
}

// shapeTapeConforms is the single exact-key proof for every route that turns a
// classic flat-object tape into a compact shape tape. Resolve's fingerprint is
// routing only; this byte comparison is the authorization boundary that makes
// positional compact reads collision-safe.
func shapeTapeConforms(index vibejson.Index, rec *ShapeRecord) bool {
	entries := index.Entries
	if rec == nil || len(entries) != 2*len(rec.Fields)+1 {
		return false
	}
	for m := range rec.Fields {
		key := &entries[2*m+1]
		if !vibejson.BytesEqualString(index.Src[key.Start+1:key.End-1], rec.Fields[m].raw) {
			return false
		}
	}
	return true
}

// appendNarrowShapeValues packs the value entries of one proven flat classic
// tape into s's narrow slab and returns their first offset.
func (s *Segment) appendNarrowShapeValues(entries []vibejson.IndexEntry, count int) uint32 {
	off := uint32(len(s.narrow))
	for m := 0; m < count; m++ {
		value := &entries[2*m+2]
		s.narrow = append(s.narrow, ShapeNarrowValue{
			Span: value.Start | value.End<<16,
			Info: value.Info,
		})
	}
	return off
}

// shapeTapeCompact converts a just-built, still-uncommitted classic tape to
// its shape-deduplicated form when the document qualifies, compacting the
// value entries to the front of index.entries in place and returning the
// shrunk index with its header ref. A document that does not qualify — the
// mode is off, the root is not a flat non-empty object, the layout is
// unresolved under the cache's sighting rules, the shape has duplicate
// decoded names, or any key byte-mismatches the compiled spelling — returns
// unchanged with the zero ref and is committed classic.
//
// The key comparison loop is the ingest-time conformance proof discussed in
// the file comment: it is the last moment the key bytes exist on a tape, and
// only an exact match lets them be dropped. Its cost is one short memcmp per
// member over source bytes the build just wrote, and the in-place value
// moves copy from strictly later entries (2m+2 > m), so the compaction needs
// no scratch.
func (s *Segment) shapeTapeCompact(index vibejson.Index) (vibejson.Index, ShapeTapeRef) {
	if !s.ShapeTapes {
		return index, ShapeTapeRef{}
	}
	entries := index.Entries
	root := &entries[0]
	count := int(root.Count())
	if root.Kind() != document.Object || count == 0 {
		return index, ShapeTapeRef{}
	}
	shape, ok := s.shapes.Resolve(vibejson.NodeFromEntries(index.Src, entries))
	if !ok {
		// Non-flat, too wide, or a layout the sighting gate has not yet
		// compiled; the second same-layout document promotes it.
		return index, ShapeTapeRef{}
	}
	rec := shape.rec
	if rec.dupKeys {
		return index, ShapeTapeRef{}
	}
	if !shapeTapeConforms(index, rec) {
		// A fingerprint collision routed a foreign layout here; the proof
		// fails closed to classic storage.
		return index, ShapeTapeRef{}
	}
	ref := ShapeTapeRef{
		Rec:      rec,
		Start:    root.Start,
		End:      root.End,
		enriched: root.KeysHashed(),
	}
	if root.End <= ShapeNarrowMaxEnd && !s.wideValueTapes &&
		uint64(len(s.narrow))+uint64(count) <= uint64(^uint32(0)) {
		// Narrow width: every member offset is bounded by root.end, so the
		// spans pack into 16 bits each. The entries move to the narrow slab
		// and the document keeps no entry-arena storage at all. The slab
		// bound keeps ref.off exact; a segment past four billion narrow entries
		// falls back to the wide form rather than overflowing it.
		ref.Narrow = true
		ref.off = s.appendNarrowShapeValues(entries, count)
		// No stored entry is needed at the narrow width. A nil slice also
		// releases the temporary classic entry arena when a collection carries this
		// document into a later immutable chunk version; a zero-capacity slice
		// could otherwise keep that whole build buffer live through its data
		// pointer.
		index.Entries = nil
		return index, ref
	}
	for m := 0; m < count; m++ {
		entries[m] = entries[2*m+2]
	}
	index.Entries = entries[:count:count]
	return index, ref
}

// widenShapeTape materializes document i's classic Index from its
// shape-deduplicated form, caching the synthesized tape for the segment's
// lifetime so repeated Doc calls return stable handles. The lock makes Doc
// safe for concurrent readers once appending stops, matching the classic
// contract; classic documents never take it.
func (s *Segment) widenShapeTape(i int, r ShapeTapeRef) vibejson.Index {
	s.widenMu.Lock()
	defer s.widenMu.Unlock()
	if entries, ok := s.widened[i]; ok {
		return vibejson.Index{Src: s.DocAt(i).Src, Entries: entries}
	}
	index := vibejson.Index{Src: s.DocAt(i).Src, Entries: s.synthShapeTape(i, r)}
	if r.enriched {
		vibejson.EnrichKeyHashes(&index)
	}
	if s.widened == nil {
		s.widened = make(map[int][]vibejson.IndexEntry)
	}
	s.widened[i] = index.Entries
	return index
}

// synthShapeTape rebuilds document i's classic entries from its value array
// and shape. Value entries copy verbatim — a narrow document's are widened
// first, which the packing's invariants make exact — and each key entry's
// span is recovered by scanning backward from its value's first byte across
// the colon and any whitespace to the closing quote: the spelling's length
// is compiled in the shape, and ingest proved the document's bytes match it,
// so the recovered span is exact, escapes included. The result is
// bit-identical to the tape classic mode would have stored (enrichment,
// applied by the caller, included), which the differential suite pins.
func (s *Segment) synthShapeTape(i int, r ShapeTapeRef) []vibejson.IndexEntry {
	doc := s.DocAt(i)
	r.Start, r.End = s.shapeTapeRootSpan(doc, r)
	values := doc.Entries
	src := doc.Src
	fields := r.Rec.Fields
	entries := make([]vibejson.IndexEntry, 2*len(fields)+1)
	entries[0] = vibejson.IndexEntry{
		Start: r.Start,
		End:   r.End,
		Next:  uint32(len(entries)),
		Info:  vibejson.PackInfo(uint32(len(fields)), document.Object, 0),
	}
	for m := range fields {
		if r.Narrow {
			entries[2*m+2] = s.NarrowAt(i, r, m).widen()
		} else {
			entries[2*m+2] = values[m]
		}
		j := int(entries[2*m+2].Start) - 1
		for vibejson.IsJSONWhitespace(src[j]) {
			j-- // back over any space between the colon and the value
		}
		j-- // src[j] was the colon
		for vibejson.IsJSONWhitespace(src[j]) {
			j-- // back over any space between the key and the colon
		}
		keyEnd := uint32(j) + 1 // src[j] is the key's closing quote
		f := &fields[m]
		entries[2*m+1] = vibejson.IndexEntry{
			Start: keyEnd - uint32(len(f.raw)) - 2,
			End:   keyEnd,
			Next:  1,
			Info:  f.info,
		}
	}
	return entries
}

// shapeTapeHintSlots sizes the inline ordinal cache below. Four slots cover
// the round-robin interleavings real batch loaders emit (the phase-0
// corpora cycle up to four shapes document by document, which a two-slot
// cache thrashes end to end), while the miss fallback — one table probe —
// keeps wider mixes merely cheap rather than cached.
const shapeTapeHintSlots = 4

// A shapeTapeHint memoizes one query's ordinal in the most recently seen
// shapes, the dedup counterpart of fieldScan's inline cache: a document
// whose shape is resident costs at most four pointer compares, and only a
// genuinely new shape pays the name table probe, evicting round-robin.
// Ordinal -1 records that the shape lacks the field — exact, not a
// heuristic, because dedup storage was proven at ingest. The zero hint
// matches nothing.
type shapeTapeHint struct {
	recs [shapeTapeHintSlots]*ShapeRecord
	ords [shapeTapeHintSlots]int32
	next uint8 // next slot to evict, advanced round-robin
}

// lookup returns the query's value ordinal in rec, or -1 when rec's shape
// lacks the field. key must carry the query's precomputed lookup hash.
func (h *shapeTapeHint) lookup(rec *ShapeRecord, key vibejson.CompiledKey) int32 {
	for i := range h.recs {
		if h.recs[i] == rec {
			return h.ords[i]
		}
	}
	ord := int32(-1)
	if o, ok := rec.fieldOrd(key.Key, key.Hash); ok {
		ord = int32(o)
	}
	i := h.next
	h.next = (i + 1) % shapeTapeHintSlots
	h.recs[i], h.ords[i] = rec, ord
	return ord
}

// SegmentStats reports the segment's tape storage composition, the accounting
// behind the space model: a classic document holds 2N+1 sixteen-byte tape
// entries for N members, a shape-taped one holds N value entries — sixteen
// bytes each in the wide form, eight in the narrow — plus one header ref,
// and each distinct shape stores its key spellings once. Stats never widens
// a document (unlike summing Doc(i).Len()), so measuring a segment's storage
// does not change it.
type SegmentStats struct {
	// Docs is the number of stored documents; ShapeTaped of them are held
	// in shape-deduplicated form, and NarrowTaped of those in the narrow
	// (8-byte-entry) width — documents whose root span fits 16-bit offsets.
	Docs        int
	ShapeTaped  int
	NarrowTaped int
	// TapeEntries counts the 16-byte entries of classic tapes, ValueEntries
	// those of wide shape-taped value arrays, and NarrowValueEntries the
	// 8-byte entries of narrow ones. Together they are the segment's entry
	// storage; the classic equivalent of a shape-taped document would have
	// cost 2N+1 sixteen-byte entries against its N of either width.
	TapeEntries        int64
	ValueEntries       int64
	NarrowValueEntries int64
	// Shapes is the number of layouts the segment's internal cache has
	// compiled. Widened counts documents whose classic tape Doc
	// re-materialized on demand.
	Shapes  int
	Widened int
	// Under ValueDict, the value dictionary's composition (segment_valuedict.go):
	// DictValues distinct value spans held once cost DictBytes in the shared
	// arena; DictSplices later occurrences reference them, standing in for
	// DictSplicedBytes of repeated source. DictSavedBytes is the modeled net
	// space a compacting store recovers — the spliced source it drops, less the
	// four-byte references it keeps and the arena it adds. The live segment retains
	// the source (every value stays directly addressable), so DictSavedBytes is
	// the at-rest space model the dictionary enables, not a live reduction.
	// Every field is zero when ValueDict is off.
	DictValues       int
	DictSplices      int64
	DictBytes        int64
	DictSplicedBytes int64
	DictSavedBytes   int64
}

// Stats summarizes the segment's tape storage. It costs one pass over the
// document table and reads nothing through Doc, so it is safe to call for
// accounting at any point between appends.
func (s *Segment) Stats() SegmentStats {
	st := SegmentStats{Docs: s.Len(), Shapes: len(s.shapes.shapes)}
	for i := 0; i < s.Len(); i++ {
		switch r := s.ShapeTapeRefAt(i); {
		case r.Rec == nil:
			st.TapeEntries += int64(len(s.DocAt(i).Entries))
		case r.Narrow:
			st.ShapeTaped++
			st.NarrowTaped++
			st.NarrowValueEntries += int64(len(r.Rec.Fields))
		default:
			st.ShapeTaped++
			st.ValueEntries += int64(len(s.DocAt(i).Entries))
		}
	}
	s.widenMu.Lock()
	st.Widened = len(s.widened)
	s.widenMu.Unlock()
	if s.ValueDict {
		s.fillValueDictStats(&st)
	}
	return st
}
