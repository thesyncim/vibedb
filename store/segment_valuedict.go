package store

import (
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
)

// Corpus-wide value dictionary: the Segment storage lever behind Segment.ValueDict.
//
// Real corpora repeat their values as hard as they repeat their keys: a
// ticketing feed names the same handful of venues, seat categories, and area
// sub-objects across every performance; a social feed repeats the same language
// tags, source strings, and boilerplate across every post. Shape-deduplicated
// tapes (segment_shape.go) stop paying for the repeated *keys* by moving them
// into a compiled shape; the value dictionary stops paying for the repeated
// *values*. A general-purpose compressor removes this byte-wise and pays
// whole-value decompression on every read. The dictionary removes it
// structurally, once, without surrendering random access: it interns each
// distinct value span into the segment-wide ValueInterner arena (value_dict.go) and
// records, per document, a compact reference in place of each repeated
// occurrence, from which a compacting store drops the repeated source bytes
// while every value still resolves to a stable arena view in O(1) — no
// decompression.
//
//	doc i: [ ... "PLEYEL_PLEYEL" ... {"areaId":205705999,"blockIds":[]} ... ]
//	doc j: [ ... "PLEYEL_PLEYEL" ... {"areaId":205705999,"blockIds":[]} ... ]
//	dictionary: id7 -> "PLEYEL_PLEYEL"   id9 -> {"areaId":205705999,"blockIds":[]}
//	doc i splices: (off,id7) (off,id9)   doc j splices: (off,id7) (off,id9)
//
// A space lever, and it composes. The dictionary indexes each document's
// classic tape at ingest and never changes its semantics: a document still
// reads through its Index exactly as before, the dictionary being the parallel
// structure a compacting store drops the repeated bytes into. That keeps it
// orthogonal to shape tapes — the shape removes key redundancy and the
// dictionary removes value redundancy over the same documents. When both modes
// are on, a shape-taped document's classic tape is synthesized transiently for
// the ingest walk (never cached, so the shape tape keeps its at-rest space
// win), so the two levers stack rather than trade.
//
// What is interned. A value span is any complete value the tape carries — a
// scalar or a whole container subtree — identified by its raw bytes. Interning
// by raw bytes keeps the value exact: a spliced value reads back byte-identical,
// number spellings included, and a container subtree reinstates verbatim. This
// is the KeyInterner discipline (intern.go) applied to value content, with one
// deliberate difference documented at ValueInterner: values intern by their raw
// span, verbatim, because RawValue.Bytes, NumberText, and every round trip
// return the original bytes and a number's spelling ("1e3" versus "1000") is
// significant. The lever that closes the real-corpus gap is the container span:
// one reference replaces a recurring sub-object's bytes and its whole entry
// subtree, where a scalar reference would replace only bytes comparable in size
// to the reference itself. The walk is therefore greedy and top-down — a
// repeated span is taken whole, its members never separately considered — and
// gates on a length floor so a span too short to out-save its reference stays
// inline.
//
// Sighting economics. Interning a value seen once costs a dictionary entry with
// no saving, so — as the shape cache gates compilation behind a repeat sighting
// — a span is interned only on its second appearance. The first occurrence stays
// inline; every later one is a reference. A corpus whose values never recur
// therefore pays only a hash-set probe per candidate, never a dictionary entry
// it cannot amortize.
//
// Read contract. A dictionary-backed value resolves — through Segment.DocValue
// for a node handle, Segment.AppendPointer for a batch column, or the RawValue a
// splice yields — to a view over the interned span in the shared arena, in O(1),
// with no decompression. The invariant that makes a dictionary read identical to
// a source read is ValueInterner's: Value(id) is byte-identical to the span
// interned for id, and stays so for the segment's lifetime because the arena never
// moves an interned span. A resolved node carries the source entry's info word
// verbatim (kind and flags), so every scalar accessor — a pure function of the
// bytes and that word — returns exactly what the source-backed node would; the
// bounded-exhaustive differential (segment_valuedict_test.go) checks this against
// classic reads across the whole small-scope domain. A resolved node is a
// whole-value materialization handle, not a navigable subtree: a container's
// members are not entries in the arena, so structural navigation stays on the
// source tape the node came from, which the dictionary never alters.
//
// Everything here is safe Go: the interning walk, the splice records, and the
// arena reads use ordinary slice indexing and the same tape accessors that read
// padding-free document slices, whose word kernels anchor every load inside the
// value's own bytes (number_digits.go), so an interned span needs no trailing
// padding.

// valueDictMinSpan is the default length floor for interning. It is a
// live-memory brake set well above the point where interning starts to pay at
// rest, and the distinction matters because the two costs pull opposite ways.
//
// At rest, a span of L bytes seen k times is interned once and spliced k-1
// times (the first sighting stays inline), so it recovers (k-1)(L-4) bytes of
// source and adds L bytes of arena: it pays as soon as (k-1)(L-4) > L. Only
// L <= 4 can never pay, the reference being no smaller than the value. An
// eight-byte enum spelling pays from its third occurrence, a six-byte one from
// its fourth — so sixteen is nowhere near the at-rest break-even, and the
// values it excludes are exactly the short, heavily repeated ones.
//
// What sixteen buys is live footprint, which the dictionary can only add to:
// a live segment keeps its source for zero-copy reads, so every splice record is
// eight resident bytes and every distinct span a sighting-set entry, none of
// it offset by a byte the segment stops holding. Measured on a corpus mixing short
// and long enums, dropping the floor to eight raised the modeled at-rest
// saving from 38.2 to 47.2 B per document (+23.6%) while raising live cost
// from 19.7 to 36.6 B per document (+86%). Sixteen takes the long spans, whose
// at-rest saving is large per record, and declines the short ones, whose live
// cost per byte saved is worst. A floor of four or less is never right at any
// setting: it admits spans that measurably lose at rest as well.
const valueDictMinSpan = 16

// valueDictRefBytes is the modeled at-rest cost of one dictionary reference: the
// four-byte identifier a compacting store keeps where it drops a spliced span.
// It is the reference charge in the SegmentStats space model; the live in-memory
// splice record (valueSplice) is wider because it also carries the offset that
// makes a read O(1) without reconstructing the document.
const valueDictRefBytes = 4

// A valueSplice records one dictionary-backed value occurrence in a document:
// the value's start offset in the document's source and the dictionary id whose
// bytes it stands for. The value's end is implicit — start + len(Value(id)) —
// because the dictionary holds the exact span, so the record needs no length.
// The invariant every read rests on is src[start : start+len(Value(id))] ==
// Value(id): the reference reads back the bytes it replaced.
type valueSplice struct {
	start uint32
	id    uint32
}

// A valueDictRef is one document's splice header: its records occupy
// Segment.valueSplices[off : off+n], in ascending start order. The zero ref
// (n == 0) marks a document with no dictionary-backed values — every value
// inline, first-sighted or below the length floor.
type valueDictRef struct {
	off uint32
	n   uint32
}

// valueDictAppend interns document i's repeated value spans and appends its
// splice header, aligning valueRefs with the documents they window. It is the
// ingest hook commitDoc calls under ValueDict, once the document is stored, so
// it walks the document's classic tape: a classic document's entries directly,
// a shape-taped document's synthesized transiently (uncached, so the shape
// tape's at-rest space win is preserved). Only the walk's source offsets are
// recorded, which are stable across both storage forms, so a later read
// resolves through them whichever form the document is held in.
func (s *Segment) valueDictAppend(i int, ref ShapeTapeRef) {
	if s.valueSeen == nil {
		s.valueSeen = make(map[uint64]struct{})
	}
	idx := s.DocAt(i)
	if ref.Rec != nil {
		// A shape-taped document's classic tape no longer exists; synthesize it
		// for the walk and drop it — never through Doc, which would cache it and
		// re-buy the storage the shape tape dropped.
		idx = vibejson.Index{Src: idx.Src, Entries: s.synthShapeTape(i, ref)}
	}
	vref := s.valueDictScan(idx)
	for len(s.valueRefs) < i {
		// A classic-only prefix before ValueDict first produced a ref keeps the
		// slice docs-aligned, exactly as commitDoc pads tapeRefs; under the
		// documented "set before the first Append" contract the padding never
		// binds, every document contributing a ref.
		s.valueRefs = append(s.valueRefs, valueDictRef{})
	}
	s.valueRefs = append(s.valueRefs, vref)
}

// valueDictScan is the greedy top-down interning walk over one classic tape. At
// each value a repeated span at or above the floor is taken whole and its
// subtree skipped, so a recurring sub-object is one reference rather than a
// reference per scalar inside it; only a value not taken is descended, and only
// into a container's member values, keys never — that redundancy belongs to the
// shape layer. The root is descended, never interned: whole-document repetition
// is an input property rather than a value-dictionary opportunity. It appends
// the document's splices to the segment-wide slab in ascending source order and
// returns the header windowing them.
func (s *Segment) valueDictScan(idx vibejson.Index) valueDictRef {
	off := uint32(len(s.valueSplices))
	ent := idx.Entries
	if len(ent) == 0 {
		return valueDictRef{off: off}
	}
	src := idx.Src
	floor := s.valueFloor
	if floor == 0 {
		floor = valueDictMinSpan
	}
	var walk func(i int, mayIntern bool) int
	walk = func(i int, mayIntern bool) int {
		e := &ent[i]
		if mayIntern && e.End-e.Start >= floor {
			span := byteview.SliceRange(&src[0], e.Start, e.End)
			h := valueDictHash(span)
			if _, sighted := s.valueSeen[h]; sighted {
				id := s.values.Intern(span)
				s.valueSplices = append(s.valueSplices, valueSplice{start: e.Start, id: id})
				return i + int(e.Next)
			}
			s.valueSeen[h] = struct{}{}
		}
		if k := e.Kind(); k != document.Object && k != document.Array {
			return i + 1
		}
		j := i + 1
		end := i + int(e.Next)
		for j < end {
			if ent[j].Flags()&vibejson.TapeFlagKey != 0 {
				j++ // step over the key to its value
				continue
			}
			j = walk(j, true)
		}
		return end
	}
	walk(0, false)
	return valueDictRef{off: off, n: uint32(len(s.valueSplices)) - off}
}

// valueDictHash is the 64-bit sighting hash: it gates the second-sighting
// interning decision only, so it never needs to be collision-resistant — a
// collision merely admits an unrelated singleton to interning, where the
// interner's own byte-verified Intern keeps it a distinct, correct entry. A
// 64-bit fold makes even that waste vanishingly rare. FNV-1a over the raw span.
func valueDictHash(b []byte) uint64 {
	h := uint64(1469598103934665603)
	for _, c := range b {
		h ^= uint64(c)
		h *= 1099511628211
	}
	return h
}

// valueSpliceAt returns the dictionary id for the value at source offset start
// in document doc, or false when that value is stored inline. It binary-searches
// the document's ascending splice records — the walk appends them in source
// order — so a value-dictionary read resolves a candidate in O(log splices).
func (s *Segment) valueSpliceAt(doc int, start uint32) (uint32, bool) {
	if uint(doc) >= uint(len(s.valueRefs)) {
		return 0, false
	}
	r := s.valueRefs[doc]
	lo, hi := r.off, r.off+r.n
	for lo < hi {
		mid := lo + (hi-lo)/2
		switch sp := s.valueSplices[mid]; {
		case sp.start == start:
			return sp.id, true
		case sp.start < start:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return 0, false
}

// valueNode returns a materialization handle over interned value id: a Node
// whose source addresses the value's bytes in the shared arena and whose single
// entry spans them entirely, [0, len), carrying the source entry's info word so
// the handle reports the same kind and flags as the value it stands for. The
// entry escapes to the heap, so the handle outlives this call; the arena bytes
// never move, so it stays valid for the segment's lifetime. Every scalar accessor
// and Raw read it directly — no dictionary knowledge on the read path — which is
// exactly what makes a dictionary read byte-identical to a source read.
func (s *Segment) valueNode(id uint32, info uint32) vibejson.Node {
	b := s.values.Value(id)
	return vibejson.Node{Src: &b[0], Entry: &vibejson.IndexEntry{Start: 0, End: uint32(len(b)), Next: 1, Info: info}}
}

// DocValue resolves the value at node v of document doc to a handle over its
// bytes. When v is a dictionary-backed occurrence — its span was interned under
// ValueDict — the returned Node addresses the value's interned bytes in the
// shared arena, reading them with no dictionary knowledge on the read path: same
// kind, same flags, byte-identical content, valid for the segment's lifetime. When v
// is stored inline, or ValueDict is off, v is returned unchanged. The result is
// invariant to the routing — the arena holds bytes identical to the source it
// stands in for — so DocValue changes where a value's bytes are read, never what
// they are; that is what lets a compacting store drop the backed span and still
// read it directly at tape speed.
//
// v must be a node obtained from Doc(doc). The handle is for value
// materialization — Raw and the scalar accessors (StringBytes, NumberBytes,
// Int64, Uint64, Float64, Bool, AppendText); structural navigation stays on the
// source tape v came from, because a container's members are not entries in the
// arena.
func (s *Segment) DocValue(doc int, v vibejson.Node) vibejson.Node {
	if s.ValueDict && v.Entry != nil {
		if id, ok := s.valueSpliceAt(doc, v.Entry.Start); ok {
			return s.valueNode(id, v.Entry.Info)
		}
	}
	return v
}

// valueRaw returns the RawValue for the dictionary-backed value at source offset
// start in document doc, or fallback when the value is stored inline. It is the
// columnar read path's dictionary hook: consulted only under ValueDict, it swaps
// a spliced occurrence's source slice for its interned arena span — byte-
// identical, borrowing the segment-lifetime arena — so Segment.AppendPointer reads
// dictionary-backed values transparently.
func (s *Segment) valueRaw(doc int, start uint32, fallback vibejson.RawValue) vibejson.RawValue {
	if id, ok := s.valueSpliceAt(doc, start); ok {
		return vibejson.RawValue{Src: s.values.Value(id)}
	}
	return fallback
}

// fillValueDictStats records the dictionary's storage composition into st. It
// costs one pass over the splice slab and materializes no document, so Stats
// stays safe for accounting at any point between appends.
func (s *Segment) fillValueDictStats(st *SegmentStats) {
	st.DictValues = s.values.Len()
	st.DictBytes = s.values.Bytes()
	st.DictSplices = int64(len(s.valueSplices))
	for _, sp := range s.valueSplices {
		st.DictSplicedBytes += int64(len(s.values.Value(sp.id)))
	}
	// The modeled at-rest saving of a compacting store: the spliced source it
	// drops, less the references it keeps and the arena it adds. The live segment
	// retains the source, so this is the space model the dictionary enables, not
	// a reduction already realized in memory.
	st.DictSavedBytes = st.DictSplicedBytes - st.DictSplices*valueDictRefBytes - st.DictBytes
}
