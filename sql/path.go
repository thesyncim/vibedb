package sql

import (
	"strconv"
	"strings"
)

// Rendering a parsed path to the spelling the engine's path compiler takes.
//
// query has exactly one path language already — a dotted name, or an RFC 6901
// JSON Pointer when the leading byte is '/' — and compilePath turns both into
// the same compiled pointer. This package therefore renders into that language
// rather than defining a second one, and renders deterministically: every
// clause that names the same path produces byte-identical output, which is what
// lets query's path registry extract a path once no matter how many clauses
// read it. Two spellings of one path would quietly double the extraction work
// and, worse, make a GROUP BY key fail to match the projection it groups.
//
// The choice between the two spellings is not cosmetic. A single clean field
// name must stay a bare name, because that is the form compilePath marks
// `single` and routes through the fused columnar fast path
// (ShapeCache.AppendField); rendering "price" as "/price" would still be
// correct and would silently cost every query the fast path.

// AppendSpec appends the engine's path spelling for p to dst and returns the
// extended buffer. It allocates only if dst has to grow, so a caller lowering
// many paths reuses one buffer and allocates nothing.
//
// A path with no segments appends nothing: the empty spec is how query names
// the whole document, which is what '*' and 'alias.*' mean.
func (p *PathExpr) AppendSpec(dst []byte) []byte {
	if p == nil || len(p.Segments) == 0 {
		return dst
	}
	if !needsPointer(p.Segments) {
		for i := range p.Segments {
			if i > 0 {
				dst = append(dst, '.')
			}
			dst = append(dst, p.Segments[i].Key...)
		}
		return dst
	}
	for i := range p.Segments {
		dst = append(dst, '/')
		if p.Segments[i].IsIndex {
			dst = strconv.AppendInt(dst, int64(p.Segments[i].Index), 10)
			continue
		}
		dst = appendPointerToken(dst, p.Segments[i].Key)
	}
	return dst
}

// Spec is AppendSpec into a fresh string, for diagnostics and tests. Lowering
// should use AppendSpec: this allocates once per call, which on a prepare path
// that renders a path per clause is exactly the allocation the arena exists to
// avoid.
func (p *PathExpr) Spec() string {
	if p == nil || len(p.Segments) == 0 {
		return ""
	}
	var b strings.Builder
	if !needsPointer(p.Segments) {
		for i := range p.Segments {
			if i > 0 {
				b.WriteByte('.')
			}
			b.WriteString(p.Segments[i].Key)
		}
		return b.String()
	}
	for i := range p.Segments {
		b.WriteByte('/')
		if p.Segments[i].IsIndex {
			b.WriteString(strconv.Itoa(p.Segments[i].Index))
			continue
		}
		b.Write(appendPointerToken(nil, p.Segments[i].Key))
	}
	return b.String()
}

// AppendPointer appends p's RFC 6901 JSON Pointer spelling to dst, whether or
// not the dotted form would also have served.
//
// It exists because two consumers of a path want different things and both are
// right. [PathExpr.AppendSpec] renders the engine's path language, where a
// clean single name deliberately stays a bare name so query's compiler routes
// it through the fused columnar fast path. A declared schema, on the other
// hand, takes an RFC 6901 pointer and nothing else — store.SchemaField.Path is
// handed straight to vibejson.CompilePointer, which refuses anything without a
// leading '/'. Rendering a schema path through AppendSpec would produce
// "name" where "/name" was required, and the failure would arrive as a schema
// compile error naming a path the author wrote correctly.
//
// A path with no segments appends nothing, which is the empty pointer: the
// whole document.
func (p *PathExpr) AppendPointer(dst []byte) []byte {
	if p == nil {
		return dst
	}
	for i := range p.Segments {
		dst = append(dst, '/')
		if p.Segments[i].IsIndex {
			dst = strconv.AppendInt(dst, int64(p.Segments[i].Index), 10)
			continue
		}
		dst = appendPointerToken(dst, p.Segments[i].Key)
	}
	return dst
}

// needsPointer reports whether segs can only be spelled as a JSON Pointer.
//
// An array index forces one because the dotted language has no subscript. A key
// containing '.' forces one because the dotted language would re-split it. A
// key containing '/' or '~' forces one because those are the pointer escapes,
// and a dotted spec is converted to a pointer by query's compilePath before
// anything else happens to it — so a dotted "a/b" would arrive at the pointer
// compiler already broken.
//
// "$key" also forces the pointer spelling. The programmatic query builder uses
// that bare string as its private physical-key join sentinel; SQL has no such
// pseudo-column, so rendering the quoted JSON field as "/$key" keeps it an
// ordinary field all the way through shared lowering.
//
// An empty key forces one for a subtler reason worth stating, since getting it
// wrong is silent: a lone empty key would render as the empty spec, which does
// not mean "the member named by the empty string" but "the whole document". The
// pointer form "/" says the former unambiguously.
func needsPointer(segs []Segment) bool {
	for i := range segs {
		if segs[i].IsIndex || segs[i].Key == "" ||
			segs[i].Key == "$key" ||
			strings.ContainsAny(segs[i].Key, "./~") {
			return true
		}
	}
	return false
}

// appendPointerToken appends key with RFC 6901's escapes applied: '~' becomes
// '~0' and '/' becomes '~1'.
func appendPointerToken(dst []byte, key string) []byte {
	if !strings.ContainsAny(key, "~/") {
		return append(dst, key...)
	}
	for i := 0; i < len(key); i++ {
		switch key[i] {
		case '~':
			dst = append(dst, '~', '0')
		case '/':
			dst = append(dst, '~', '1')
		default:
			dst = append(dst, key[i])
		}
	}
	return dst
}

// sameSpec reports whether two paths address the same value of the same range
// variable. It compares the parsed segments rather than the rendered specs so
// it needs no buffer, which matters because the GROUP BY, ORDER BY, and HAVING
// checks call it once per pair.
func sameSpec(a, b *PathExpr) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Source != b.Source || len(a.Segments) != len(b.Segments) {
		return false
	}
	for i := range a.Segments {
		if a.Segments[i] != b.Segments[i] {
			return false
		}
	}
	return true
}
