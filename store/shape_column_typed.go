package store

import (
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// Typed corpus extraction: the columnar bridge from indexed JSON to dense
// typed cells. An engine computing a sum, average, or filter over one field
// of a corpus composes today as AppendField followed by a parse loop — a
// []RawValue intermediate, a second pass over it, and a full revalidation
// per cell, because a raw span carries no tape classification. The typed
// variants parse each cell during the same fused scan instead, straight off
// the value entry the positional read proves: one info-word test recovers
// the tape's kind and integer classification, and the digit kernels run on
// coordinates the index already validated, so a dense []int64 column with a
// validity mask comes out at scan speed with no intermediate and no
// revalidation.
//
// Every driver shares fieldScan, AppendField's routing engine: the two-slot
// inline hint cache, the claimant proof, the hunt backoff, and the exact
// per-document fallback are identical, so the typed loops inherit the
// routing's exactness argument verbatim and differ from AppendField only in
// how a proven value entry is turned into a cell.
//
// The cell contract, shared by all three drivers: cell i is the value of
// the member [Node.Get] resolves in document i's root, under exactly the
// corresponding Node accessor's verdict — [Node.Int64], [Node.Float64], or
// [Node.Bool] — paired with validity true; every other document yields the
// zero cell paired with false. False therefore covers an absent field, a
// non-object root, a null, a value of another kind, and a number the
// accessor rejects — for Int64 fraction or exponent spellings and
// magnitudes outside int64, for Float64 magnitudes that overflow. A false
// cell is always exactly zero (never, say, the clamped or infinite value a
// failed parse produced), so an engine may aggregate the dense values
// unconditionally and apply the validity mask afterward. Presence with
// validity false is not distinguished from absence; callers needing the
// distinction extract the field with AppendField.

// A plain-integer number entry announces itself in one masked info-word
// compare: Number kind with the integer flag. Count bits are masked off —
// scalars leave them zero, but the mask keeps the test independent of that
// convention — and the escaped and key flags are zero on every number.
const (
	infoIntNumberMask = vibejson.InfoKindMask | uint32(vibejson.TapeFlagInt)<<vibejson.InfoFlagsShift
	infoIntNumberBits = uint32(document.Number)<<vibejson.InfoKindShift | uint32(vibejson.TapeFlagInt)<<vibejson.InfoFlagsShift
)

// AppendFieldInt64 resolves name against every document in s, in ordinal
// order, appending one int64 cell to dst and one validity flag to valid per
// document under the typed cell contract above with [Node.Int64] as the
// accessor, and returns both extended slices. dst and valid grow in
// lockstep by exactly s.Len(); prior contents are untouched, and the caller
// keeps the two aligned across batches. Beyond their growth the call
// allocates nothing, and the cells are owned — unlike a RawValue column
// they do not borrow the set's arenas.
//
// Extraction routes through the same fused scan as [ShapeCache.AppendField].
// On the proven positional read the common cell — a plain-integer number —
// parses by the tape digit kernel directly on the value entry's validated
// coordinates; rarer spellings and every fallback document go through the
// exact lookup and the Node accessor itself, so the verdicts are the
// accessor's by construction.
//
// AppendFieldInt64 grows c and follows its concurrency rule: one cache per
// worker.
func (c *ShapeCache) AppendFieldInt64(dst []int64, valid []bool, s *Segment, name string) ([]int64, []bool) {
	fs := newFieldScan(name)
	var th shapeTapeHint
	for i := 0; i < s.Len(); i++ {
		if r := s.ShapeTapeRefAt(i); r.Rec != nil {
			// A shape-taped document's proven value entry takes the same
			// kernel dispatch as a proven positional read below.
			var n int64
			var ok bool
			if ord := th.lookup(r.Rec, fs.key); ord >= 0 {
				doc := s.DocAt(i)
				if r.Narrow {
					// The verbatim info word keeps the integer probe
					// identical; only the span unpacks, and only the rare
					// non-integer spelling widens into a stack entry for
					// the accessor.
					nv := s.NarrowAt(i, r, int(ord))
					if nv.Info&infoIntNumberMask == infoIntNumberBits {
						n, ok = vibejson.TapeInt64(&doc.Src[0], nv.Span&0xFFFF, nv.Span>>16)
					} else {
						c.wide = nv.widen()
						n, ok = (vibejson.Node{Src: &doc.Src[0], Entry: &c.wide}).Int64()
					}
				} else if e := &doc.Entries[ord]; e.Info&infoIntNumberMask == infoIntNumberBits {
					n, ok = vibejson.TapeInt64(&doc.Src[0], e.Start, e.End)
				} else {
					n, ok = (vibejson.Node{Src: &doc.Src[0], Entry: e}).Int64()
				}
				if !ok {
					n = 0
				}
			}
			dst = append(dst, n)
			valid = append(valid, ok)
			continue
		}
		root := s.DocAt(i).Root()
		if root.Entry == nil {
			dst = append(dst, 0)
			valid = append(valid, false)
			continue
		}
		var n int64
		var ok bool
		if e := fs.next(c, root); e != nil {
			if e.Info&infoIntNumberMask == infoIntNumberBits {
				// The proven value entry is a plain integer: the digit
				// kernel reads it in place. Peeling the kernel call one
				// level further into this loop measures as a wash — the
				// cell's cost is the routing proof, not the parse — so the
				// shared kernel keeps the single spelling.
				n, ok = vibejson.TapeInt64(root.Src, e.Start, e.End)
			} else {
				// A fraction or exponent spelling, or another kind: the
				// accessor is the verdict, on the proven entry.
				n, ok = (vibejson.Node{Src: root.Src, Entry: e}).Int64()
			}
		} else if v, present := root.GetCompiled(fs.key); present {
			n, ok = v.Int64()
		}
		if !ok {
			n = 0 // pin the zero-cell contract against accessor drift
		}
		dst = append(dst, n)
		valid = append(valid, ok)
	}
	return dst, valid
}

// AppendFieldFloat64 is [ShapeCache.AppendFieldInt64] with [Node.Float64]
// as the accessor: cells round through the same kernels a per-document read
// would — the integer fast path, the exact-multiply envelope, Eisel-Lemire,
// and strconv only for the spellings those defer on — bit-for-bit,
// negative zero included. A magnitude Float64 rejects yields (0, false),
// never the overflowed infinity.
//
// AppendFieldFloat64 grows c and follows its concurrency rule: one cache
// per worker.
func (c *ShapeCache) AppendFieldFloat64(dst []float64, valid []bool, s *Segment, name string) ([]float64, []bool) {
	fs := newFieldScan(name)
	var th shapeTapeHint
	var templateHint storeTemplateFieldHint
	for i := 0; i < s.Len(); i++ {
		if template, templateOK := s.TemplateAt(i); templateOK {
			var f float64
			var ok bool
			if ord := templateHint.lookup(template, fs.key); ord >= 0 {
				span := s.TemplateSpan(i, template, ord)
				c.wide = template.Index.Entries[ord]
				c.wide.Start, c.wide.End = span&0xffff, span>>16
				doc := s.DocAt(i)
				if c.wide.Info&infoIntNumberMask == infoIntNumberBits {
					if n, intOK := vibejson.TapeInt64(&doc.Src[0], c.wide.Start, c.wide.End); intOK {
						f, ok = float64(n), true
					} else {
						f, ok = (vibejson.Node{Src: &doc.Src[0], Entry: &c.wide}).Float64()
					}
				} else {
					f, ok = (vibejson.Node{Src: &doc.Src[0], Entry: &c.wide}).Float64()
				}
				if !ok {
					f = 0
				}
			}
			dst = append(dst, f)
			valid = append(valid, ok)
			continue
		}
		if r := s.ShapeTapeRefAt(i); r.Rec != nil {
			var f float64
			var ok bool
			if ord := th.lookup(r.Rec, fs.key); ord >= 0 {
				doc := s.DocAt(i)
				if r.Narrow {
					nv := s.NarrowAt(i, r, int(ord))
					if nv.Info&infoIntNumberMask == infoIntNumberBits {
						if n, intOK := vibejson.TapeInt64(&doc.Src[0], nv.Span&0xffff, nv.Span>>16); intOK {
							f, ok = float64(n), true
						} else {
							c.wide = nv.widen()
							f, ok = (vibejson.Node{Src: &doc.Src[0], Entry: &c.wide}).Float64()
						}
					} else {
						c.wide = nv.widen()
						f, ok = (vibejson.Node{Src: &doc.Src[0], Entry: &c.wide}).Float64()
					}
				} else {
					f, ok = (vibejson.Node{Src: &doc.Src[0], Entry: &doc.Entries[ord]}).Float64()
				}
				if !ok {
					f = 0
				}
			}
			dst = append(dst, f)
			valid = append(valid, ok)
			continue
		}
		root := s.DocAt(i).Root()
		if root.Entry == nil {
			dst = append(dst, 0)
			valid = append(valid, false)
			continue
		}
		var f float64
		var ok bool
		if e := fs.next(c, root); e != nil {
			// Float64's own dispatch is already the fused kernel — its
			// integer fast path reads the same flag the int driver peels —
			// so the accessor runs directly on the proven entry.
			f, ok = (vibejson.Node{Src: root.Src, Entry: e}).Float64()
		} else if v, present := root.GetCompiled(fs.key); present {
			f, ok = v.Float64()
		}
		if !ok {
			f = 0 // an out-of-range parse yields an infinity; the cell is zero
		}
		dst = append(dst, f)
		valid = append(valid, ok)
	}
	return dst, valid
}

// Float64Aggregate is the result of one fused numeric field reduction.
// Count includes only cells for which Node.Float64 succeeds. Min and Max are
// meaningful when Count is non-zero.
type Float64Aggregate struct {
	Count int
	Sum   float64
	Min   float64
	Max   float64
}

func (a *Float64Aggregate) Add(value float64) {
	if a.Count == 0 {
		a.Min, a.Max = value, value
	} else {
		if value < a.Min {
			a.Min = value
		}
		if value > a.Max {
			a.Max = value
		}
	}
	a.Sum += value
	a.Count++
}

// ReduceFieldFloat64 fuses top-level field routing, numeric conversion, and
// SUM/AVG/MIN/MAX state into one pass with no intermediate column. The result
// has exactly the same Float64 acceptance semantics as AppendFieldFloat64.
// c follows the ordinary one-cache-per-worker rule.
func (c *ShapeCache) ReduceFieldFloat64(s *Segment, name string) Float64Aggregate {
	fs := newFieldScan(name)
	var th shapeTapeHint
	var templateHint storeTemplateFieldHint
	var aggregate Float64Aggregate
	for i := 0; i < s.Len(); i++ {
		if template, templateOK := s.TemplateAt(i); templateOK {
			if ord := templateHint.lookup(template, fs.key); ord >= 0 {
				span := s.TemplateSpan(i, template, ord)
				c.wide = template.Index.Entries[ord]
				c.wide.Start, c.wide.End = span&0xffff, span>>16
				doc := s.DocAt(i)
				if c.wide.Info&infoIntNumberMask == infoIntNumberBits {
					if n, ok := vibejson.TapeInt64(&doc.Src[0], c.wide.Start, c.wide.End); ok {
						aggregate.Add(float64(n))
					} else if f, ok := (vibejson.Node{Src: &doc.Src[0], Entry: &c.wide}).Float64(); ok {
						aggregate.Add(f)
					}
				} else if f, ok := (vibejson.Node{Src: &doc.Src[0], Entry: &c.wide}).Float64(); ok {
					aggregate.Add(f)
				}
			}
			continue
		}
		if r := s.ShapeTapeRefAt(i); r.Rec != nil {
			if ord := th.lookup(r.Rec, fs.key); ord >= 0 {
				doc := s.DocAt(i)
				var f float64
				var ok bool
				if r.Narrow {
					nv := s.NarrowAt(i, r, int(ord))
					if nv.Info&infoIntNumberMask == infoIntNumberBits {
						if n, intOK := vibejson.TapeInt64(&doc.Src[0], nv.Span&0xffff, nv.Span>>16); intOK {
							f, ok = float64(n), true
						} else {
							c.wide = nv.widen()
							f, ok = (vibejson.Node{Src: &doc.Src[0], Entry: &c.wide}).Float64()
						}
					} else {
						c.wide = nv.widen()
						f, ok = (vibejson.Node{Src: &doc.Src[0], Entry: &c.wide}).Float64()
					}
				} else {
					f, ok = (vibejson.Node{Src: &doc.Src[0], Entry: &doc.Entries[ord]}).Float64()
				}
				if ok {
					aggregate.Add(f)
				}
			}
			continue
		}
		root := s.DocAt(i).Root()
		if root.Entry == nil {
			continue
		}
		var f float64
		var ok bool
		if e := fs.next(c, root); e != nil {
			f, ok = (vibejson.Node{Src: root.Src, Entry: e}).Float64()
		} else if v, present := root.GetCompiled(fs.key); present {
			f, ok = v.Float64()
		}
		if ok {
			aggregate.Add(f)
		}
	}
	return aggregate
}

// AppendFieldBool is [ShapeCache.AppendFieldInt64] with [Node.Bool] as the
// accessor: a cell is true or false exactly for a JSON boolean value, and
// every other cell is (false, false).
//
// AppendFieldBool grows c and follows its concurrency rule: one cache per
// worker.
func (c *ShapeCache) AppendFieldBool(dst []bool, valid []bool, s *Segment, name string) ([]bool, []bool) {
	fs := newFieldScan(name)
	var th shapeTapeHint
	for i := 0; i < s.Len(); i++ {
		if r := s.ShapeTapeRefAt(i); r.Rec != nil {
			var b bool
			var ok bool
			if ord := th.lookup(r.Rec, fs.key); ord >= 0 {
				doc := s.DocAt(i)
				if r.Narrow {
					c.wide = s.NarrowAt(i, r, int(ord)).widen()
					b, ok = (vibejson.Node{Src: &doc.Src[0], Entry: &c.wide}).Bool()
				} else {
					b, ok = (vibejson.Node{Src: &doc.Src[0], Entry: &doc.Entries[ord]}).Bool()
				}
			}
			dst = append(dst, b)
			valid = append(valid, ok)
			continue
		}
		root := s.DocAt(i).Root()
		if root.Entry == nil {
			dst = append(dst, false)
			valid = append(valid, false)
			continue
		}
		var b bool
		var ok bool
		if e := fs.next(c, root); e != nil {
			b, ok = (vibejson.Node{Src: root.Src, Entry: e}).Bool()
		} else if v, present := root.GetCompiled(fs.key); present {
			b, ok = v.Bool()
		}
		dst = append(dst, b) // Bool's false verdict already zeroes the cell
		valid = append(valid, ok)
	}
	return dst, valid
}
