package store

import (
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
)

// Fused corpus extraction: the engine scan loop — for every document in a
// batch, extract field F — as one primitive over a [Segment] and a
// [ShapeCache]. The per-document composition
//
//	shape, _ := cache.Resolve(root) // fold + table probe, every document
//	ref, _ := shape.Field(name)     // memoized by the engine
//	v, _ := ref.In(root)            // verify + fixed-offset read
//
// re-pays the fingerprint fold, the cache probe, and the call boundaries on
// every document, even though real corpora arrive shape-clustered: runs of
// consecutive documents share one layout. The fused loop hoists everything
// loop-invariant into a two-slot inline cache — the compiled query, the last
// resolved shape and the one it displaced, the field's position in each —
// and extracts each run-extending document by a verified positional read
// that needs no shape resolution at all: byte-verify the key at the hinted
// position, then prove no later member can claim the spelling. Only a
// document rejecting both hints resolves through the cache, and only a
// document no compiled shape matches falls back to the exact per-document
// lookup, so heterogeneous batches degrade to the lookup ladder they would
// have used anyway.
//
// The positional read is exact, not probabilistic. Node.Get's answer is the
// last member whose key decodes to the query, so a hint hit is proven by two
// facts alone: the key at the hinted position byte-matches the compiled raw
// spelling, and no member after it can decode to the same spelling — on an
// enriched tape no later stored hash equals the query hash, on a plain tape
// no later raw span has the matching length, with escaped spellings treated
// as claimants either way. Earlier members never matter: even if the
// spelling recurs before the hint, the hinted member wins Get's
// last-duplicate rule. A document of a different layout that happens to pass
// both checks therefore still returns exactly Get's answer, which is why the
// hint needs no fingerprint routing and carries no residual collision
// deviation.

// AppendField resolves name against every document in s, in ordinal order,
// appending one RawValue per document to dst: the value of the last root
// member named name — the member Node.Get resolves — when the root is an
// object containing it, and the zero RawValue otherwise, the
// [Segment.AppendPointer] absence convention, so appended values stay aligned
// with document ordinals. It returns the extended slice. Appended values
// borrow the set's arenas under the usual RawValue lifetime rules, and the
// call allocates nothing beyond dst's growth.
//
// Extraction semantics per document are exactly root.Get(name)'s: names
// match by decoded spelling, the last duplicate wins, and non-object roots
// have no members. The shape machinery only accelerates that contract. A
// document extending the current run of one flat layout takes the verified
// positional read; a document whose layout rejects the hint re-resolves
// through c under [ShapeCache.Resolve]'s sighting rules; and every document
// the fast paths cannot prove — an unresolved layout, a non-flat or
// non-object root, a possible later duplicate of name, or a shape that does
// not contain name at all — takes the exact per-document lookup, so no
// routing accident can misread a field or turn a present member absent.
// Shapes that lack name skip resolution for their whole run and take the
// exact lookup directly.
//
// A shape-taped document (Segment.ShapeTapes) skips the routing machinery
// entirely: its shape was byte-proven at ingest, so extraction is one
// memoized ordinal lookup and one value-array index — no header proof, no
// key verification, no suffix scan — and absence in the shape is absence in
// the document, exactly.
//
// AppendField grows c and follows its concurrency rule: one cache per
// worker.
func (c *ShapeCache) AppendField(dst []vibejson.RawValue, s *Segment, name string) []vibejson.RawValue {
	fs := newFieldScan(name)
	var th shapeTapeHint
	var templateHint storeTemplateFieldHint
	for i := 0; i < s.Len(); i++ {
		if template, templateOK := s.TemplateAt(i); templateOK {
			if ord := templateHint.lookup(template, fs.key); ord >= 0 {
				span := s.TemplateSpan(i, template, ord)
				doc := s.DocAt(i)
				raw := vibejson.RawValue{Src: doc.Src[span&0xffff : span>>16]}
				if s.ValueDict {
					raw = s.valueRaw(i, span&0xffff, raw)
				}
				dst = append(dst, raw)
			} else {
				dst = append(dst, vibejson.RawValue{})
			}
			continue
		}
		if r := s.ShapeTapeRefAt(i); r.Rec != nil {
			doc := s.DocAt(i)
			if ord := th.lookup(r.Rec, fs.key); ord >= 0 {
				// Both widths are one array index; the narrow read unpacks
				// its two 16-bit offsets from half the memory traffic.
				var start, end uint32
				if r.Narrow {
					nv := s.NarrowAt(i, r, int(ord))
					start, end = nv.Span&0xFFFF, nv.Span>>16
				} else {
					v := &doc.Entries[ord]
					start, end = v.Start, v.End
				}
				raw := vibejson.RawValue{Src: doc.Src[start:end]}
				if s.ValueDict {
					raw = s.valueRaw(i, start, raw)
				}
				dst = append(dst, raw)
			} else {
				dst = append(dst, vibejson.RawValue{})
			}
			continue
		}
		root := s.DocAt(i).Root()
		if root.Entry == nil {
			dst = append(dst, vibejson.RawValue{})
			continue
		}
		if value := fs.next(c, root); value != nil {
			dst = append(dst, vibejson.RawValue{Src: byteview.SliceRange(root.Src, value.Start, value.End)})
			continue
		}
		dst = appendFieldGet(dst, root, fs.key)
	}
	return dst
}

// AppendFieldRows is the sparse-gather form of [ShapeCache.AppendField]. It
// resolves name only for the document ordinals in rows, in the order supplied,
// and appends one value per ordinal to dst. Duplicate ordinals produce
// duplicate values; an out-of-range ordinal panics like [Segment.Doc]. The
// value and lifetime semantics are otherwise exactly AppendField's, including
// last-duplicate-key wins and a zero RawValue for an absent field.
//
// This is the selection-pushdown primitive for engines that already have a
// selective posting list: work is O(len(rows)), not O(s.Len()). In particular,
// shape-taped documents are read directly from their narrow or wide value
// arrays and are never widened into classic tapes. Classic documents retain
// AppendField's two-shape routing cache, so sorted or shape-clustered row lists
// amortize lookup in the same way as a dense scan.
//
// AppendFieldRows grows c and follows its concurrency rule: one cache per
// worker.
func (c *ShapeCache) AppendFieldRows(dst []vibejson.RawValue, s *Segment, rows []int, name string) []vibejson.RawValue {
	fs := newFieldScan(name)
	var th shapeTapeHint
	var templateHint storeTemplateFieldHint
	for _, i := range rows {
		// Templated documents carry no classic tape entries, so they must be
		// resolved through their template exactly as AppendField does. Falling
		// through to the tape path instead reports every field of every
		// templated document absent — silently, because "absent" is a valid
		// answer — which is a dropped row for any predicate that reads it.
		if template, templateOK := s.TemplateAt(i); templateOK {
			if ord := templateHint.lookup(template, fs.key); ord >= 0 {
				span := s.TemplateSpan(i, template, ord)
				doc := s.DocAt(i)
				raw := vibejson.RawValue{Src: doc.Src[span&0xffff : span>>16]}
				if s.ValueDict {
					raw = s.valueRaw(i, span&0xffff, raw)
				}
				dst = append(dst, raw)
			} else {
				dst = append(dst, vibejson.RawValue{})
			}
			continue
		}
		if r := s.ShapeTapeRefAt(i); r.Rec != nil {
			doc := s.DocAt(i)
			if ord := th.lookup(r.Rec, fs.key); ord >= 0 {
				var start, end uint32
				if r.Narrow {
					nv := s.NarrowAt(i, r, int(ord))
					start, end = nv.Span&0xFFFF, nv.Span>>16
				} else {
					v := &doc.Entries[ord]
					start, end = v.Start, v.End
				}
				raw := vibejson.RawValue{Src: doc.Src[start:end]}
				if s.ValueDict {
					raw = s.valueRaw(i, start, raw)
				}
				dst = append(dst, raw)
			} else {
				dst = append(dst, vibejson.RawValue{})
			}
			continue
		}
		root := s.DocAt(i).Root()
		if root.Entry == nil {
			dst = append(dst, vibejson.RawValue{})
			continue
		}
		var raw vibejson.RawValue
		if value := fs.next(c, root); value != nil {
			raw = vibejson.RawValue{Src: byteview.SliceRange(root.Src, value.Start, value.End)}
			if s.ValueDict {
				raw = s.valueRaw(i, value.Start, raw)
			}
		} else if value, ok := root.GetCompiled(fs.key); ok {
			raw = value.Raw()
			if s.ValueDict {
				raw = s.valueRaw(i, value.Entry.Start, raw)
			}
		}
		dst = append(dst, raw)
	}
	return dst
}

// A fieldScan routes one query across a document batch: the state machine
// behind [ShapeCache.AppendField] and the typed drivers of
// shape_column_typed.go, holding the compiled query and the two hint slots
// across documents. Each driver owns one fieldScan for one pass and feeds it
// every document in ordinal order through next. The zero fieldScan is not
// ready; use newFieldScan.
type fieldScan struct {
	key     vibejson.CompiledKey
	rawLen  uint32
	h0, h1  fieldHint
	streak  int
	backoff int
	skip    int
}

// newFieldScan returns a scan state for one extraction pass over name.
func newFieldScan(name string) fieldScan {
	return fieldScan{key: vibejson.CompileKey(name), rawLen: uint32(len(name)) + 2}
}

// next routes one document, root, which must be a valid Node: it returns the
// value entry of the member Node.Get(fs.key) resolves when a verified
// positional read proves it, and nil when only the exact per-document lookup
// can answer — an absent-field shape, a possible later duplicate, a
// backed-off hunt, or an unresolved layout. next returns a route, never a
// verdict: nil demands the fallback lookup, and a non-nil entry is exactly
// the lookup's answer, so no routing accident can misread a field or turn a
// present member absent.
//
// Two hint slots, most recent first: run-extending documents pay only h0,
// and a corpus alternating two layouts ping-pongs between the slots instead
// of re-resolving every document. A hit on h1 promotes it. A document
// rejecting both hints resolves through c under [ShapeCache.Resolve]'s
// sighting rules and is extracted through the fresh hint, so a rare run
// boundary pays one resolution plus one positional read, never a full
// lookup. A corpus that keeps missing both hints — every layout distinct,
// every root non-flat, or many layouts alternating with the field's
// position — hunts under shapeHuntSkip's backoff and degrades to the exact
// lookup plus a vanishing resolution tax.
func (fs *fieldScan) next(c *ShapeCache, root vibejson.Node) *vibejson.IndexEntry {
	e := root.Entry
	if fs.h0.rec != nil && e.Info&(vibejson.InfoCountMask|vibejson.InfoKindMask) == fs.h0.info && e.Next == fs.h0.next {
		// The header words prove a flat object of the hinted shape's exact
		// width, bounding every entry offset the hint read touches inside
		// the document's 2*count+1 entry span — In's argument verbatim.
		if !fs.h0.has {
			// The run's shape lacks the field. Absence cannot be verified
			// positionally, so the exact lookup answers; it re-proves
			// absence per document, never trusting the hint for it.
			return nil
		}
		ke := vibejson.EntryAt(e, uintptr(2*fs.h0.ord)+1)
		if vibejson.BytesEqualString(byteview.SliceRange(root.Src, ke.Start+1, ke.End-1), fs.h0.raw) {
			fs.streak, fs.backoff, fs.skip = 0, 0, 0
			if tapeSuffixClaimsKey(root.Src, e, fs.h0.ord, fs.h0.count, fs.key.Key, fs.key.Hash, fs.rawLen) {
				// Only the last-duplicate rule is in doubt: the exact
				// lookup answers, but the run and its hint stand.
				return nil
			}
			return vibejson.EntryAt(ke, 1)
		}
		// The hinted position rejected: the layout changed under an
		// unchanged header. Try the displaced shape, then re-resolve.
	}
	if fs.h1.rec != nil && e.Info&(vibejson.InfoCountMask|vibejson.InfoKindMask) == fs.h1.info && e.Next == fs.h1.next {
		if fs.h1.has {
			ke := vibejson.EntryAt(e, uintptr(2*fs.h1.ord)+1)
			if vibejson.BytesEqualString(byteview.SliceRange(root.Src, ke.Start+1, ke.End-1), fs.h1.raw) {
				fs.h0, fs.h1 = fs.h1, fs.h0
				fs.streak, fs.backoff, fs.skip = 0, 0, 0
				if tapeSuffixClaimsKey(root.Src, e, fs.h0.ord, fs.h0.count, fs.key.Key, fs.key.Hash, fs.rawLen) {
					return nil
				}
				return vibejson.EntryAt(ke, 1)
			}
		} else if fs.h0.rec == nil || e.Info&(vibejson.InfoCountMask|vibejson.InfoKindMask) != fs.h0.info || e.Next != fs.h0.next {
			// The displaced shape lacks the field; promote its sticky
			// absent run unless h0 already claimed this header.
			fs.h0, fs.h1 = fs.h1, fs.h0
			return nil
		}
	}
	if fs.skip > 0 {
		fs.skip--
		return nil
	}
	if fs.streak >= shapeHuntStreak {
		fs.backoff = shapeHuntSkip(fs.backoff)
		fs.skip = fs.backoff
	}
	fs.streak++
	if shape, ok := c.Resolve(root); ok {
		rec := shape.rec
		fs.h1 = fs.h0
		fs.h0 = fieldHint{
			rec:   rec,
			info:  rec.info,
			next:  rec.next,
			count: len(rec.Fields),
		}
		var ref FieldRef
		ref, fs.h0.has = shape.Field(fs.key.Key)
		if fs.h0.has {
			fs.h0.ord = int(ref.ord)
			fs.h0.raw = rec.Fields[ref.ord].raw
			ke := vibejson.EntryAt(e, uintptr(2*fs.h0.ord)+1)
			if vibejson.BytesEqualString(byteview.SliceRange(root.Src, ke.Start+1, ke.End-1), fs.h0.raw) {
				if tapeSuffixClaimsKey(root.Src, e, fs.h0.ord, fs.h0.count, fs.key.Key, fs.key.Hash, fs.rawLen) {
					return nil
				}
				return vibejson.EntryAt(ke, 1)
			}
			// Unreachable short of an engineered fingerprint collision:
			// the shape was resolved from this very document.
		}
	}
	return nil
}

// A fieldHint is one slot of fieldScan's inline cache: a compiled shape,
// the queried field's position and raw spelling in it, and the shape's
// expected header words hoisted out of the per-document compares. The
// ShapeRecord is immutable and arena-pinned, so a hint held across Resolve
// calls stays valid. The zero fieldHint matches nothing.
type fieldHint struct {
	rec   *ShapeRecord
	raw   string
	ord   int
	count int
	info  uint32
	next  uint32
	has   bool
}

// shapeHuntStreak is the fast-path miss streak that starts the hunt backoff:
// misses at ordinary run boundaries are amortized by the runs they open, so
// only sustained missing backs off. One fast-path hit resets the streak, the
// backoff, and any pending skip.
const shapeHuntStreak = 8

// shapeHuntSkip advances the hunt backoff: after each streak crossing the
// next 2n+1 documents skip shape resolution and take the exact lookup,
// capped so the steady hunt rate on a corpus the fast path never serves —
// all-distinct layouts under the sighting gate, non-flat roots, or many
// layouts alternating with the field's position — is one declined resolution
// per 64 documents, while a corpus that resumes clustering is re-served
// within the cap.
func shapeHuntSkip(backoff int) int {
	return min(2*backoff+1, 63)
}

// tapeSuffixClaimsKey reports whether any member past ord of the flat object
// at header could decode to the queried key spelling — the sole condition
// under which the member at ord is not Node.Get's last-duplicate answer. A
// later member is a claimant when its key is escaped (its decoded spelling
// cannot be judged from the stored words, exactly as the lookup scans treat
// escaped keys) or, on a key-hash-enriched tape, when its stored hash equals
// the query hash; a plain tape substitutes the raw span length as the
// pre-filter and byte-compares the rare length matches, so same-length
// neighbours cost one comparison, not a false claim. False claims are safe —
// the caller falls back to the exact lookup — so the hash and length tests
// stay pure pre-filters, and claims are rare enough that the enriched scan
// tests four members per iteration, tapeScanFlatHash's stride.
//
// Bounds: callers established header as a flat object of count members
// spanning 2*count+1 entries, with 0 <= ord < count; the scan touches key
// entries 2m+1 for m in (ord, count), all inside that span.
func tapeSuffixClaimsKey(src *byte, header *vibejson.IndexEntry, ord, count int, name string, queryHash uint32, rawLen uint32) bool {
	const escapedInfo = uint32(vibejson.TapeFlagEscaped) << vibejson.InfoFlagsShift
	m := ord + 1
	if header.KeysHashed() {
		for ; m+3 < count; m += 4 {
			k0 := vibejson.EntryAt(header, uintptr(2*m)+1)
			k1 := vibejson.EntryAt(header, uintptr(2*m)+3)
			k2 := vibejson.EntryAt(header, uintptr(2*m)+5)
			k3 := vibejson.EntryAt(header, uintptr(2*m)+7)
			if k0.Next == queryHash || k1.Next == queryHash ||
				k2.Next == queryHash || k3.Next == queryHash ||
				(k0.Info|k1.Info|k2.Info|k3.Info)&escapedInfo != 0 {
				return true
			}
		}
		for ; m < count; m++ {
			k := vibejson.EntryAt(header, uintptr(2*m)+1)
			if k.Next == queryHash || k.Info&escapedInfo != 0 {
				return true
			}
		}
		return false
	}
	for ; m < count; m++ {
		k := vibejson.EntryAt(header, uintptr(2*m)+1)
		if k.Info&escapedInfo != 0 {
			return true
		}
		if k.End-k.Start == rawLen && vibejson.BytesEqualString(byteview.SliceRange(src, k.Start+1, k.End-1), name) {
			return true
		}
	}
	return false
}

// appendFieldGet appends one document's exact extraction: the compiled-key
// lookup on the document root, with absence and non-object roots appending
// the zero RawValue. It is the fallback all fused-loop misses share, and its
// semantics — root.Get with the query hashed once for the batch — are the
// contract the fast paths must match.
func appendFieldGet(dst []vibejson.RawValue, root vibejson.Node, key vibejson.CompiledKey) []vibejson.RawValue {
	if v, ok := root.GetCompiled(key); ok {
		return append(dst, v.Raw())
	}
	return append(dst, vibejson.RawValue{})
}

// AppendFields is [ShapeCache.AppendField] over several names in one pass,
// appending column-wise: for each names[j], one RawValue per document of s
// is appended to dst[j] in ordinal order under AppendField's exact
// per-document semantics, so every column grows by s.Len() values and row i
// of every column describes document i. Missing columns are appended to dst
// first (a nil dst works); columns beyond len(names) are untouched. It
// returns the extended dst.
//
// The columnar result is the engine layout: each field's values stay
// contiguous for filtering or aggregation, and callers reuse the columns'
// capacity across batches. Where AppendField proves each document
// positionally, the projection amortizes one shape resolution per document
// across all names — a fingerprint fold plus one verified fixed-offset read
// per name — which overtakes per-name suffix proofs as names grow; a
// document failing any per-name verification, or resolving to a shape
// lacking a name, takes the exact lookup for that name, under
// [FieldRef.In]'s contract and its documented residual deviation. Beyond
// dst's growth, the call allocates only its per-name state, independent of
// s.Len(). Shape-taped documents (Segment.ShapeTapes) bypass the fold: their
// proven shape resolves every name to a memoized ordinal and each column
// reads its value entry directly, under AppendField's exactness argument.
//
// AppendFields grows c and follows its concurrency rule: one cache per
// worker.
func (c *ShapeCache) AppendFields(dst [][]vibejson.RawValue, s *Segment, names ...string) [][]vibejson.RawValue {
	for len(dst) < len(names) {
		dst = append(dst, nil)
	}
	if len(names) == 0 {
		return dst
	}
	keys := make([]vibejson.CompiledKey, len(names))
	for j, name := range names {
		keys[j] = vibejson.CompileKey(name)
	}
	// A single-slot inline cache with each name's position and raw spelling
	// in the inline shape — the fold amortizes across all names, so a second
	// slot buys the projection little. Ordinal -1 marks a name the shape
	// lacks, and anyPresent gates the resolution-free run for shapes with
	// none.
	slots := make([]fieldColumn, len(names))
	var (
		rec        *ShapeRecord
		anyPresent bool
		hdrInfo    uint32
		hdrNext    uint32
		hdrCount   int
		streak     int
		backoff    int
		skip       int
		// The shape-taped inline cache: one proven ordinal per name for the
		// value-array documents, allocated on the first such document so
		// classic sets pay nothing.
		tapeRec  *ShapeRecord
		tapeOrds []int32
	)
	for i := 0; i < s.Len(); i++ {
		if r := s.ShapeTapeRefAt(i); r.Rec != nil {
			doc := s.DocAt(i)
			if r.Rec != tapeRec {
				tapeRec = r.Rec
				if tapeOrds == nil {
					tapeOrds = make([]int32, len(names))
				}
				for j := range keys {
					tapeOrds[j] = -1
					if o, ok := tapeRec.fieldOrd(keys[j].Key, keys[j].Hash); ok {
						tapeOrds[j] = int32(o)
					}
				}
			}
			for j, ord := range tapeOrds {
				if ord >= 0 {
					var start, end uint32
					if r.Narrow {
						nv := s.NarrowAt(i, r, int(ord))
						start, end = nv.Span&0xFFFF, nv.Span>>16
					} else {
						v := &doc.Entries[ord]
						start, end = v.Start, v.End
					}
					raw := vibejson.RawValue{Src: doc.Src[start:end]}
					if s.ValueDict {
						raw = s.valueRaw(i, start, raw)
					}
					dst[j] = append(dst[j], raw)
				} else {
					dst[j] = append(dst[j], vibejson.RawValue{})
				}
			}
			continue
		}
		root := s.DocAt(i).Root()
		var shape Shape
		var ok bool
		switch e := root.Entry; {
		case e == nil:
			for j := range keys {
				dst[j] = append(dst[j], vibejson.RawValue{})
			}
			continue
		case skip > 0:
			// Hunting is backed off. Unlike AppendField's always-cheap
			// positional probe, the projection's fast path opens with the
			// fold, so a backed-off run skips even that and re-proves every
			// document exactly.
			skip--
			dst = appendFieldsGet(dst, s, i, root, keys)
			continue
		case rec == nil || e.Info&(vibejson.InfoCountMask|vibejson.InfoKindMask) != hdrInfo || e.Next != hdrNext:
			// Header foreign to the inline shape: a new layout, a non-flat
			// or non-object root, or the first document. Resolve gates and
			// folds from scratch.
			if streak >= shapeHuntStreak {
				backoff = shapeHuntSkip(backoff)
				skip = backoff
			}
			streak++
			shape, ok = c.Resolve(root)
		case !anyPresent:
			// The run's shape contains none of the names: the exact lookups
			// are the answer, with no resolution until the header changes.
			dst = appendFieldsGet(dst, s, i, root, keys)
			continue
		default:
			fp := c.fingerprint(root, hdrCount)
			if fp == rec.fingerprint {
				// Inline hit: one fold routed the document for every name.
				streak, backoff = 0, 0
				dst = appendFieldsResolved(dst, s, i, root, slots, keys)
				continue
			}
			// Same header words, foreign key sequence: re-route through the
			// table on the fingerprint already folded.
			if streak >= shapeHuntStreak {
				backoff = shapeHuntSkip(backoff)
				skip = backoff
			}
			streak++
			shape, ok = c.resolveFingerprint(root, hdrCount, fp)
		}
		if !ok {
			// Unresolved: not flat, too wide, or sighting-gated.
			dst = appendFieldsGet(dst, s, i, root, keys)
			continue
		}
		// Retarget the inline cache at the newly resolved shape and extract
		// this document through it.
		rec = shape.rec
		hdrInfo = rec.info
		hdrNext = rec.next
		hdrCount = len(rec.Fields)
		anyPresent = false
		for j, name := range names {
			if ref, ok := shape.Field(name); ok {
				slots[j] = fieldColumn{ord: int32(ref.ord), raw: rec.Fields[ref.ord].raw}
				anyPresent = true
			} else {
				slots[j] = fieldColumn{ord: -1}
			}
		}
		dst = appendFieldsResolved(dst, s, i, root, slots, keys)
	}
	return dst
}

// A fieldColumn is one name's slot in the projection's inline cache: the
// member ordinal in the inline shape, or -1 when the shape lacks the name,
// and the compiled raw spelling verified before every positional read.
type fieldColumn struct {
	raw string
	ord int32
}

// appendFieldsGet appends one document's exact extraction to every column,
// the projection loop's shared fallback.
func appendFieldsGet(dst [][]vibejson.RawValue, s *Segment, doc int, root vibejson.Node, keys []vibejson.CompiledKey) [][]vibejson.RawValue {
	for j := range keys {
		if value, ok := root.GetCompiled(keys[j]); ok {
			raw := value.Raw()
			if s.ValueDict {
				raw = s.valueRaw(doc, value.Entry.Start, raw)
			}
			dst[j] = append(dst[j], raw)
		} else {
			dst[j] = append(dst[j], vibejson.RawValue{})
		}
	}
	return dst
}

// appendFieldsResolved appends one shape-routed document to every column:
// each present name reads at its compiled offset after the same key-byte
// verification FieldRef.In performs — the caller's header gate and
// fingerprint route stand in for In's header compares — and each name the
// shape lacks, or whose verification fails, takes the exact lookup.
// Bounds: the caller established root as a flat object of the routed
// shape's exact width, and every present ordinal is below it.
func appendFieldsResolved(dst [][]vibejson.RawValue, s *Segment, doc int, root vibejson.Node, slots []fieldColumn, keys []vibejson.CompiledKey) [][]vibejson.RawValue {
	for j := range slots {
		if ord := slots[j].ord; ord >= 0 {
			ke := vibejson.EntryAt(root.Entry, uintptr(2*ord)+1)
			if vibejson.BytesEqualString(byteview.SliceRange(root.Src, ke.Start+1, ke.End-1), slots[j].raw) {
				value := vibejson.EntryAt(ke, 1)
				raw := vibejson.RawValue{Src: byteview.SliceRange(root.Src, value.Start, value.End)}
				if s.ValueDict {
					raw = s.valueRaw(doc, value.Start, raw)
				}
				dst[j] = append(dst[j], raw)
				continue
			}
		}
		dst[j] = appendFieldGet(dst[j], root, keys[j])
	}
	return dst
}
