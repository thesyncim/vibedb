package storeio

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// The scan/filter lanes and the future SQL column probe consume rows without
// rendering them: a row is (templateID, token stream), a template resolves a
// field path to a hole ordinal once per (leaf, template), and a hole read is
// a walk of at most a dozen one-byte tags plus lengths. This is the
// same one read path exposing structure it already has — not a cache and not
// a second representation; every returned slice borrows the admitted page
// under the same lease/epoch rules as every other borrowed span (INVARIANT 8).

// UnifiedRowTokenKind classifies one decoded hole token.
type UnifiedRowTokenKind uint8

const (
	// UnifiedRowTokenLiteral is a canonical spelling carried by the token:
	// either an inline short/long literal or a dictionary reference (whose
	// entry stores the exact canonical spelling).
	UnifiedRowTokenLiteral UnifiedRowTokenKind = iota
	UnifiedRowTokenTrue
	UnifiedRowTokenFalse
	UnifiedRowTokenNull
	// UnifiedRowTokenInt is a canonical-integer token; regeneration through
	// AppendCanonicalInt is byte-identical for every admitted spelling.
	UnifiedRowTokenInt
)

// UnifiedRowToken is one resolved hole token. Spelling borrows the admitted
// page (dictionary entry or inline literal bytes) and is only set for
// UnifiedRowTokenLiteral; Int is only meaningful for UnifiedRowTokenInt; Dict
// is the dictionary id when the token was a dictionary reference, else -1.
type UnifiedRowToken struct {
	Spelling []byte
	Int      int64
	Dict     int16
	Kind     UnifiedRowTokenKind
}

// AppendUnifiedRowToken appends the canonical spelling a token encodes.
// Literal spellings copy; true/false/null are the grammar's unique spellings;
// integer tokens regenerate their unique minimal decimal spelling.
func AppendUnifiedRowToken(dst []byte, tok UnifiedRowToken) []byte {
	switch tok.Kind {
	case UnifiedRowTokenTrue:
		return append(dst, "true"...)
	case UnifiedRowTokenFalse:
		return append(dst, "false"...)
	case UnifiedRowTokenNull:
		return append(dst, "null"...)
	case UnifiedRowTokenInt:
		return AppendCanonicalInt(dst, tok.Int)
	default:
		return append(dst, tok.Spelling...)
	}
}

// RowTemplate classifies a raw row body: the template id for a templated row,
// -1 for a trivial row, and false for an empty (corrupt) body.
func (v *CommonPrimaryUnifiedLeafView) RowTemplate(body []byte) (int, bool) {
	if v == nil || len(body) == 0 {
		return 0, false
	}
	if body[0] == unifiedRowTrivial {
		return -1, true
	}
	return int(body[0]), true
}

// RowToken walks a templated row body to the given hole ordinal and decodes
// its token. The walk is bounds-checked at every step and fails closed on any
// structural violation (unassigned tag, truncated literal, unresolvable
// dictionary id, truncated varint, hole past the stream end) rather than
// reading outside the row body or the dictionary section.
func (v *CommonPrimaryUnifiedLeafView) RowToken(body []byte, hole int) (UnifiedRowToken, bool) {
	if v == nil || hole < 0 || len(body) < 2 || body[0] == unifiedRowTrivial {
		return UnifiedRowToken{}, false
	}
	cursor := 1
	for {
		if cursor >= len(body) {
			return UnifiedRowToken{}, false
		}
		tag := body[cursor]
		cursor++
		at := hole == 0
		hole--
		switch {
		case tag < unifiedTokenDictLimit:
			if at {
				value, found := v.dictionaryEntry(int(tag))
				if !found {
					return UnifiedRowToken{}, false
				}
				return UnifiedRowToken{
					Kind: UnifiedRowTokenLiteral, Spelling: value, Dict: int16(tag),
				}, true
			}
		case tag >= unifiedTokenShortBase && tag < unifiedTokenShortBase+unifiedTokenShortMax:
			length := int(tag-unifiedTokenShortBase) + 1
			if length > len(body)-cursor {
				return UnifiedRowToken{}, false
			}
			if at {
				return UnifiedRowToken{
					Kind:     UnifiedRowTokenLiteral,
					Spelling: body[cursor : cursor+length : cursor+length],
					Dict:     -1,
				}, true
			}
			cursor += length
		case tag == unifiedTokenLongLiteral:
			length, n, ok := readUnifiedTokenUvarint(body[cursor:])
			if !ok || length == 0 || uint64(length) > uint64(len(body)-cursor-n) {
				return UnifiedRowToken{}, false
			}
			cursor += n
			if at {
				return UnifiedRowToken{
					Kind:     UnifiedRowTokenLiteral,
					Spelling: body[cursor : cursor+int(length) : cursor+int(length)],
					Dict:     -1,
				}, true
			}
			cursor += int(length)
		case tag == unifiedTokenTrue:
			if at {
				return UnifiedRowToken{Kind: UnifiedRowTokenTrue, Dict: -1}, true
			}
		case tag == unifiedTokenFalse:
			if at {
				return UnifiedRowToken{Kind: UnifiedRowTokenFalse, Dict: -1}, true
			}
		case tag == unifiedTokenNull:
			if at {
				return UnifiedRowToken{Kind: UnifiedRowTokenNull, Dict: -1}, true
			}
		case tag == unifiedTokenInt:
			value, n := DecodeZigzagVarint(body[cursor:])
			if n == 0 {
				return UnifiedRowToken{}, false
			}
			cursor += n
			if at {
				return UnifiedRowToken{Kind: UnifiedRowTokenInt, Int: value, Dict: -1}, true
			}
		default:
			return UnifiedRowToken{}, false
		}
	}
}

// Hole resolution results below the valid ordinals. Resolution is per
// (template, path), cached by the caller, and amortized over the leaf's rows.
const (
	// UnifiedHoleAbsent: the path names no value in this template's shape.
	// Every row of the template shares the shape, so the field is absent from
	// every one of its rows — no per-row work is ever needed.
	UnifiedHoleAbsent = -1
	// UnifiedHoleContainer: the path resolves to a non-empty object or array,
	// which spans multiple holes and skeleton segments. Consumers take the
	// render path for these rows.
	UnifiedHoleContainer = -2
)

// UnifiedHoleResolver resolves a fixed field path against unified-leaf
// templates and against rendered canonical documents. It owns all scratch the
// resolutions need (skeleton fill buffer, tape storage, decoded-key scratch),
// so a warmed resolver allocates nothing. A resolver is single-consumer.
//
// Path syntax is a JSON-pointer-style "/a/b" chain of object member names;
// a segment of decimal digits additionally matches that array element index.
// Path segments use RFC 6901 decoding, including empty member names and ~0/~1
// escapes, so a storage-native filter addresses exactly the same fields as the
// query compiler's canonical pointer spelling.
type UnifiedHoleResolver struct {
	path       []byte
	segments   [][2]int32
	filled     []byte
	entries    []vibejson.IndexEntry
	keyScratch []byte
}

// SetPath parses and retains the target path. It must be called before any
// resolution and may be called again to retarget the resolver.
func (r *UnifiedHoleResolver) SetPath(path []byte) error {
	if len(path) < 2 || path[0] != '/' {
		return fmt.Errorf("%w: unified field path %q", ErrInvalidWrite, path)
	}
	r.path = r.path[:0]
	r.segments = r.segments[:0]
	for rawStart := 1; ; {
		rawEnd := rawStart
		for rawEnd < len(path) && path[rawEnd] != '/' {
			rawEnd++
		}
		start := len(r.path)
		for i := rawStart; i < rawEnd; i++ {
			if path[i] != '~' {
				r.path = append(r.path, path[i])
				continue
			}
			if i+1 >= rawEnd || (path[i+1] != '0' && path[i+1] != '1') {
				return fmt.Errorf("%w: invalid unified field path escape in %q", ErrInvalidWrite, path)
			}
			i++
			if path[i] == '0' {
				r.path = append(r.path, '~')
			} else {
				r.path = append(r.path, '/')
			}
		}
		r.segments = append(r.segments, [2]int32{int32(start), int32(len(r.path))})
		if rawEnd == len(path) {
			return nil
		}
		rawStart = rawEnd + 1
	}
}

func (r *UnifiedHoleResolver) segment(i int) []byte {
	s := r.segments[i]
	return r.path[s[0]:s[1]]
}

// segmentIndex parses segment i as a canonical decimal array index (no sign,
// no leading zero except "0" itself), returning -1 when it is not one.
func (r *UnifiedHoleResolver) segmentIndex(i int) int {
	seg := r.segment(i)
	if len(seg) == 0 {
		return -1
	}
	if len(seg) > 1 && seg[0] == '0' {
		return -1
	}
	n := 0
	for _, c := range seg {
		if c < '0' || c > '9' {
			return -1
		}
		n = n*10 + int(c-'0')
		// An index beyond any representable arity cannot match; the cap also
		// bounds the accumulator against overflow on absurd digit strings.
		if n > 1<<20 {
			return -1
		}
	}
	return n
}

// keyEquals compares a tape key entry's decoded spelling to target. Unescaped
// keys compare as source aliases; escaped keys decode once into the reusable
// scratch (the same discipline as the canonical-form checker).
func (r *UnifiedHoleResolver) keyEquals(src []byte, ke *vibejson.IndexEntry, target []byte) bool {
	if ke.Flags()&vibejson.TapeFlagEscaped == 0 {
		return bytes.Equal(src[ke.Start+1:ke.End-1], target)
	}
	r.keyScratch = r.keyScratch[:0]
	node := vibejson.Node{Src: &src[0], Entry: ke}
	r.keyScratch, _ = node.AppendText(r.keyScratch)
	return bytes.Equal(r.keyScratch, target)
}

// buildIndex builds a tape over src into the resolver's reusable entry
// storage, growing it on ErrIndexFull exactly as the leaf builder does.
func (r *UnifiedHoleResolver) buildIndex(src []byte) (vibejson.Index, error) {
	for {
		index, err := vibejson.BuildIndex(src, r.entries[:cap(r.entries)])
		if err == nil {
			r.entries = index.Entries
			return index, nil
		}
		if !errors.Is(err, document.ErrIndexFull) {
			return vibejson.Index{}, err
		}
		grown := cap(r.entries) * 2
		if grown < 64 {
			grown = 64
		}
		r.entries = make([]vibejson.IndexEntry, 0, grown)
	}
}

// Resolve maps the resolver's path to a hole ordinal of one template: a value
// >= 0 is the hole index, UnifiedHoleAbsent means no row of the template
// carries the field, UnifiedHoleContainer means the path names structure that
// spans holes (render-path rows). The resolution reconstructs the template's
// skeleton with "null" in every hole — legal by construction, because every
// scalar leaf of the canonical document is a hole — and walks its tape
// in document order, so hole ordinals match the encoder's span order exactly.
func (r *UnifiedHoleResolver) Resolve(v *CommonPrimaryUnifiedLeafView, templateID int) int {
	entry, ok := v.templateEntry(templateID)
	if !ok {
		// Fail closed: an unresolvable template routes its rows to the render
		// path, which performs its own structural validation.
		return UnifiedHoleContainer
	}
	return r.resolveTemplate(entry.holes, entry.ends, entry.static)
}

// resolveCompactTemplate applies the same path-to-hole proof to a compact
// stripe template. Both durable grammars use the identical static-segment
// representation; keeping one resolver prevents path semantics from drifting
// while the compact format replaces the row-token leaf.
func (r *UnifiedHoleResolver) resolveCompactTemplate(entry compactPrimaryTemplateView) int {
	return r.resolveTemplate(entry.holes, entry.ends, entry.static)
}

func (r *UnifiedHoleResolver) resolveTemplate(holes int, ends, static []byte) int {
	r.filled = r.filled[:0]
	previous := 0
	for segment := 0; segment <= holes; segment++ {
		end := int(readUint32(ends[segment*4:]))
		if end < previous || end > len(static) {
			return UnifiedHoleContainer
		}
		if segment > 0 {
			r.filled = append(r.filled, "null"...)
		}
		r.filled = append(r.filled, static[previous:end]...)
		previous = end
	}
	index, err := r.buildIndex(r.filled)
	if err != nil {
		return UnifiedHoleContainer
	}
	_, result, done := r.resolveWalk(r.filled, index.Entries, 0, 0, true, 0)
	if !done {
		return UnifiedHoleAbsent
	}
	return result
}

// resolveWalk walks the filled skeleton's tape in document order, counting
// holes (every entry with Next == 1 is a hole: the filler nulls stand exactly
// where the encoder extracted spans). onPath means the path to this
// value equals segments[:seg]. It returns the hole count after the subtree,
// plus the decided result once the target's fate is known.
func (r *UnifiedHoleResolver) resolveWalk(
	src []byte, entries []vibejson.IndexEntry, i, seg int, onPath bool, hole int,
) (nextHole, result int, done bool) {
	e := &entries[i]
	if e.Next == 1 {
		if onPath {
			if seg == len(r.segments) {
				return hole + 1, hole, true
			}
			// The path descends below a scalar leaf: absent in this shape.
			return hole + 1, UnifiedHoleAbsent, true
		}
		return hole + 1, 0, false
	}
	if onPath && seg == len(r.segments) {
		return hole, UnifiedHoleContainer, true
	}
	switch e.Kind() {
	case document.Array:
		count := int(e.Count())
		target := -1
		if onPath {
			target = r.segmentIndex(seg)
			if target < 0 || target >= count {
				return hole, UnifiedHoleAbsent, true
			}
		}
		child := i + 1
		for m := 0; m < count; m++ {
			childOnPath := onPath && m == target
			nextSeg := seg
			if childOnPath {
				nextSeg++
			}
			hole, result, done = r.resolveWalk(src, entries, child, nextSeg, childOnPath, hole)
			if done {
				return hole, result, true
			}
			child += int(entries[child].Next)
		}
		return hole, 0, false
	case document.Object:
		count := int(e.Count())
		matched := false
		key := i + 1
		for m := 0; m < count; m++ {
			ke := &entries[key]
			childOnPath := false
			if onPath && !matched && r.keyEquals(src, ke, r.segment(seg)) {
				// Duplicate keys are retained at rest: the first member in
				// canonical order wins, deterministically.
				childOnPath = true
				matched = true
			}
			value := key + 1
			nextSeg := seg
			if childOnPath {
				nextSeg++
			}
			hole, result, done = r.resolveWalk(src, entries, value, nextSeg, childOnPath, hole)
			if done {
				return hole, result, true
			}
			key = value + int(entries[value].Next)
		}
		if onPath {
			return hole, UnifiedHoleAbsent, true
		}
		return hole, 0, false
	default:
		// A non-container with Next != 1 cannot appear in a valid tape.
		return hole, UnifiedHoleAbsent, true
	}
}

// PathSpanOf resolves the resolver's path against an arbitrary rendered
// document (the render-path complement of Resolve: trivial rows, container
// targets, overflow chains, non-unified leaves) and returns the value's
// spelling span within doc. Missing paths report found == false; malformed
// documents report an error. The walk is direct navigation — no hole
// counting — using the same key/index matching rules as Resolve.
func (r *UnifiedHoleResolver) PathSpanOf(doc []byte) (start, end uint32, found bool, err error) {
	index, err := r.buildIndex(doc)
	if err != nil {
		return 0, 0, false, err
	}
	entries := index.Entries
	i := 0
	for seg := range r.segments {
		e := &entries[i]
		switch e.Kind() {
		case document.Object:
			count := int(e.Count())
			key := i + 1
			next := -1
			for m := 0; m < count; m++ {
				if r.keyEquals(doc, &entries[key], r.segment(seg)) {
					next = key + 1
					break
				}
				key += 1 + int(entries[key+1].Next)
			}
			if next < 0 {
				return 0, 0, false, nil
			}
			i = next
		case document.Array:
			target := r.segmentIndex(seg)
			if target < 0 || target >= int(e.Count()) {
				return 0, 0, false, nil
			}
			child := i + 1
			for m := 0; m < target; m++ {
				child += int(entries[child].Next)
			}
			i = child
		default:
			return 0, 0, false, nil
		}
	}
	return entries[i].Start, entries[i].End, true, nil
}

// readUint32 is a tiny local helper so the resolver need not import
// encoding/binary at every call site; the codec's directories are all
// little-endian u32 tables.
func readUint32(b []byte) uint32 {
	_ = b[3]
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}
