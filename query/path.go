package query

import (
	"strings"

	"github.com/thesyncim/vibejson"
)

// A compiledPath is a projection path compiled once for reuse across every
// document a query touches. A single dotted segment ("price") keeps the
// top-level field name so extraction can take the fused columnar fast path
// (ShapeCache.AppendField and the typed AppendField* variants); every other
// path — nested, or written in JSON Pointer syntax — resolves through a
// compiled RFC 6901 pointer (Segment.AppendPointer). Both spellings honor
// Node.Get semantics: names match by decoded content and duplicate keys
// resolve to the last occurrence.
type compiledPath struct {
	single   bool   // a single top-level object field
	topLevel bool   // either spelling names exactly one unescaped object field
	name     string // the field name when single or topLevel
	// spec is the front-end spelling this path was compiled from. It is kept
	// here so pathRegistry can dedupe by it without a second slice running
	// alongside the paths: the pointer spelling is not the spec (a dotted path
	// and its pointer form are different strings for the same path), so there
	// is nothing else to recover it from.
	spec    string
	key     vibejson.CompiledKey     // compiled name when single
	pointer vibejson.CompiledPointer // the full pointer otherwise

	// join is the slot of the join clause this path reads, or joinPathOuter for
	// a path in the driving collection. It is what lets one column space hold
	// both sides of a fan-out join: extraction routes on it, and everything
	// downstream — projection, ordering, grouping, aggregation — indexes the
	// resulting column by row without knowing which collection filled it.
	join int
}

// joinPathOuter marks a compiled path in the driving collection. It is the
// zero-value-hostile spelling on purpose: a path that never went through alias
// resolution must not silently read the first join's collection.
const joinPathOuter = -1

// compilePath compiles a path spec. A leading slash (or the empty string)
// selects JSON Pointer syntax; anything else is a dotted path whose segments
// are object keys. Invalid pointer syntax returns the core's PointerError.
func compilePath(spec string) (compiledPath, error) {
	if spec == "" || spec[0] == '/' {
		pointer, err := vibejson.CompilePointer(spec)
		if err != nil {
			return compiledPath{}, err
		}
		name, topLevel := rawTopLevelPointerName(spec)
		return compiledPath{
			topLevel: topLevel, name: name, spec: spec,
			pointer: pointer, join: joinPathOuter,
		}, nil
	}
	if strings.IndexByte(spec, '.') < 0 {
		pointer, err := vibejson.CompilePointer("/" + escapePointerSegment(spec))
		if err != nil {
			return compiledPath{}, err
		}
		return compiledPath{
			single:   true,
			topLevel: true,
			name:     spec,
			spec:     spec,
			key:      vibejson.CompileKey(spec),
			pointer:  pointer,
			join:     joinPathOuter,
		}, nil
	}
	pointer, err := vibejson.CompilePointer(pointerFromDotted(spec))
	if err != nil {
		return compiledPath{}, err
	}
	return compiledPath{spec: spec, pointer: pointer, join: joinPathOuter}, nil
}

func rawTopLevelPointerName(spec string) (string, bool) {
	if len(spec) < 2 || spec[0] != '/' {
		return "", false
	}
	for i := 1; i < len(spec); i++ {
		if spec[i] == '/' || spec[i] == '~' {
			return "", false
		}
	}
	return spec[1:], true
}

func (p compiledPath) pointerForStore() vibejson.CompiledPointer { return p.pointer }

// indexPath returns the canonical RFC 6901 spelling used by declared collection
// indexes. Query's dotted syntax is a front-end convenience; index matching is
// performed only on this exact compiled form.
func (p compiledPath) indexPath() string {
	return p.pointer.String()
}

// pointerFromDotted renders a dotted path as an RFC 6901 pointer, escaping each
// segment's tildes and slashes so a key containing them round-trips.
//
// The segments are walked in place rather than split out first: splitting
// allocated a []string per compiled path only to rejoin it, which is pure waste
// on a path this function already has to scan byte by byte to escape.
func pointerFromDotted(spec string) string {
	var b strings.Builder
	b.Grow(len(spec) + 1)
	for rest := spec; ; {
		b.WriteByte('/')
		dot := strings.IndexByte(rest, '.')
		if dot < 0 {
			b.WriteString(escapePointerSegment(rest))
			return b.String()
		}
		b.WriteString(escapePointerSegment(rest[:dot]))
		rest = rest[dot+1:]
	}
}

// escapePointerSegment applies RFC 6901's token escapes: ~ becomes ~0 and /
// becomes ~1.
func escapePointerSegment(seg string) string {
	if !strings.ContainsAny(seg, "~/") {
		return seg
	}
	var b strings.Builder
	for i := 0; i < len(seg); i++ {
		switch seg[i] {
		case '~':
			b.WriteString("~0")
		case '/':
			b.WriteString("~1")
		default:
			b.WriteByte(seg[i])
		}
	}
	return b.String()
}

// appendEscapedPointerSegment is escapePointerSegment into a caller-owned
// buffer, for the compiler paths that assemble a pointer into arena storage
// and must not mint an intermediate heap string per segment.
func appendEscapedPointerSegment(dst []byte, seg string) []byte {
	if !strings.ContainsAny(seg, "~/") {
		return append(dst, seg...)
	}
	for i := 0; i < len(seg); i++ {
		switch seg[i] {
		case '~':
			dst = append(dst, '~', '0')
		case '/':
			dst = append(dst, '~', '1')
		default:
			dst = append(dst, seg[i])
		}
	}
	return dst
}
