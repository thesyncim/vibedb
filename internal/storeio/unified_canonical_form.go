package storeio

import (
	"bytes"
	"fmt"
	"unsafe"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/scanner"
)

// The stored logical value of every document is its vibejson canonical form:
// object members stably sorted by the UTF-8 bytes of their *decoded* key,
// duplicate members retained in original relative order, arrays in order, no
// interstitial whitespace, string escapes normalized to the one spelling
// vibejson's Value.AppendJSON emits, number spellings preserved verbatim.
//
// Canonicalization lives at the storage boundary, not in the tape builder: the
// tape keeps reflecting the document as given, and the leaf encoder renders
// the canonical spelling from an already-validated tape. These primitives use
// the exported tape surface (Index, IndexEntry, Node) because vibejson is an
// external dependency. The escape normalizer below is a byte-exact mirror of vibejson's
// appendJSONStringBytes, and the differential tests in
// unified_canonical_form_test.go pin the two implementations against each other.
//
// Key collation is total because admission validation already rejects the
// only ambiguous inputs: vibejson's validator refuses unpaired surrogate
// escapes (vibejson/validate.go, validateUnicodeEscape: "missing low
// surrogate" / "unexpected low surrogate") and invalid raw UTF-8 ("invalid
// UTF-8 in string"), so every admitted key decodes to exactly one valid
// UTF-8 byte string and decoded-byte order is a total, stable order.

// CanonicalWorkspace holds the reusable scratch a canonical render or check
// needs so that steady-state calls allocate nothing: member references for
// per-object sorting, an auxiliary buffer for the stable merge, and one byte
// scratch for decoded key/string spellings. Scratch is segmented by
// recursion frame (each frame truncates back to its own base on return), so
// one workspace serves arbitrarily nested documents.
type CanonicalWorkspace struct {
	members []canonicalMember
	aux     []canonicalMember
	scratch []byte
}

// NewCanonicalWorkspace returns a workspace with fixed retained capacities.
// Callers that admit work through a bounded scratch lane can size it once and
// avoid input-dependent growth on the hot path.
func NewCanonicalWorkspace(memberCapacity, scratchCapacity int) CanonicalWorkspace {
	return CanonicalWorkspace{
		members: make([]canonicalMember, 0, memberCapacity),
		aux:     make([]canonicalMember, 0, memberCapacity),
		scratch: make([]byte, 0, scratchCapacity),
	}
}

// CanonicalWorkspaceCapacityBytes reports the exact retained backing bytes of
// a workspace constructed with the supplied capacities, without allocating it.
func CanonicalWorkspaceCapacityBytes(memberCapacity, scratchCapacity int) uint64 {
	return canonicalWorkspaceCapacityBytes(
		memberCapacity, memberCapacity, scratchCapacity,
	)
}

func canonicalWorkspaceCapacityBytes(
	memberCapacity, auxCapacity, scratchCapacity int,
) uint64 {
	return uint64(memberCapacity+auxCapacity)*
		uint64(unsafe.Sizeof(canonicalMember{})) + uint64(scratchCapacity)
}

// CapacityBytes reports the retained backing storage owned by the workspace.
func (w *CanonicalWorkspace) CapacityBytes() uint64 {
	if w == nil {
		return 0
	}
	return canonicalWorkspaceCapacityBytes(
		cap(w.members), cap(w.aux), cap(w.scratch),
	)
}

// canonicalMember is one object member reference during a canonical render:
// the key's tape entry index plus the location of its decoded spelling. A
// negative scratchOff means the key was unescaped and its decoded bytes
// alias the source between the key span's quotes; otherwise they live in the
// workspace scratch at [scratchOff, scratchOff+length). Offsets rather than
// slices survive scratch reallocation during nested decodes.
type canonicalMember struct {
	keyEntry   int32
	scratchOff int32
	length     int32
}

// AppendCanonicalIndexed appends the canonical spelling of the
// document behind index to dst and returns the extended slice. index must be
// a tape built by vibejson over validated input; dst's writable capacity
// must not overlap index.Src. The render is a deterministic pure function of
// the tape and allocates nothing once ws has warmed to the document's shape.
func AppendCanonicalIndexed(dst []byte, index vibejson.Index, ws *CanonicalWorkspace) ([]byte, error) {
	if len(index.Entries) == 0 || len(index.Src) == 0 {
		return dst, fmt.Errorf("%w: canonical render of empty tape", ErrInvalidWrite)
	}
	return appendCanonicalEntry(dst, index.Src, index.Entries, 0, ws, nil), nil
}

// CanonicalSpanIndex is an opaque, borrowed certificate tying one validated
// canonical document to its ordered scalar-hole spans. The value and span
// storage remain owned by the caller and must not change while the certificate
// is in use.
//
// Keeping the fields private matters: consumers may trust that a non-zero
// certificate came either from a canonical tape check or directly from the
// canonical renderer, rather than accepting independently supplied offsets.
type CanonicalSpanIndex struct {
	canonical []byte
	spans     []UnifiedTokenSpan
	valid     bool
}

// Bytes returns the canonical document certified by c. The returned bytes are
// borrowed from the source tape or render destination.
func (c CanonicalSpanIndex) Bytes() []byte {
	return c.canonical
}

// CanonicalSpanIndexOf returns a scalar-span certificate when index already
// names a canonical document. spanStorage is fixed caller scratch; capacity
// for every tape entry is a conservative allocation-free bound on the number
// of scalar holes. Insufficient scratch or non-canonical input returns false.
func CanonicalSpanIndexOf(
	index vibejson.Index,
	ws *CanonicalWorkspace,
	spanStorage []UnifiedTokenSpan,
) (CanonicalSpanIndex, bool) {
	if len(index.Entries) == 0 || len(index.Src) == 0 || ws == nil ||
		cap(spanStorage) < len(index.Entries) || !IndexIsCanonical(index, ws) {
		return CanonicalSpanIndex{}, false
	}
	spans := appendHoleSpans(spanStorage[:0], index)
	return CanonicalSpanIndex{
		canonical: index.Src,
		spans:     spans,
		valid:     true,
	}, true
}

type canonicalSpanTracker struct {
	base  int
	spans []UnifiedTokenSpan
}

// AppendCanonicalIndexedSpans renders index and simultaneously returns the
// scalar-hole certificate for the appended document. This avoids reparsing
// the canonical output. As with CanonicalSpanIndexOf, spanStorage needs room
// for len(index.Entries) entries; the conservative preflight keeps bounded
// callers allocation-free and fails closed on insufficient scratch.
func AppendCanonicalIndexedSpans(
	dst []byte,
	index vibejson.Index,
	ws *CanonicalWorkspace,
	spanStorage []UnifiedTokenSpan,
) ([]byte, CanonicalSpanIndex, error) {
	if len(index.Entries) == 0 || len(index.Src) == 0 || ws == nil {
		return dst, CanonicalSpanIndex{}, fmt.Errorf(
			"%w: canonical render of empty tape", ErrInvalidWrite,
		)
	}
	if cap(spanStorage) < len(index.Entries) {
		return dst, CanonicalSpanIndex{}, document.ErrIndexFull
	}
	base := len(dst)
	tracker := canonicalSpanTracker{
		base:  base,
		spans: spanStorage[:0],
	}
	dst = appendCanonicalEntry(
		dst, index.Src, index.Entries, 0, ws, &tracker,
	)
	return dst, CanonicalSpanIndex{
		canonical: dst[base:],
		spans:     tracker.spans,
		valid:     true,
	}, nil
}

// appendCanonicalEntry renders the value at tape entry i. Navigation follows
// the two tape rules (first child at header+1, next sibling at
// value+value.Next); a key's own Next word is never read because the
// key-hash enrichment may have repurposed it.
func appendCanonicalEntry(
	dst, src []byte,
	entries []vibejson.IndexEntry,
	i int,
	ws *CanonicalWorkspace,
	tracker *canonicalSpanTracker,
) []byte {
	e := &entries[i]
	start := len(dst)
	switch e.Kind() {
	case document.String:
		dst = appendCanonicalTapeString(dst, src, entries, i, ws)
	case document.Number, document.Bool, document.Null:
		// Number spellings are preserved verbatim; true/false/null
		// have exactly one grammatical spelling each, so the source span is
		// the canonical spelling by construction.
		dst = append(dst, src[e.Start:e.End]...)
	case document.Array:
		count := int(e.Count())
		if count == 0 {
			// Empty containers are scalar leaves with pinned spellings; the
			// source span may carry interior whitespace, so emit
			// the literal rather than the span.
			dst = append(dst, '[', ']')
			break
		}
		dst = append(dst, '[')
		child := i + 1
		for m := 0; m < count; m++ {
			if m > 0 {
				dst = append(dst, ',')
			}
			dst = appendCanonicalEntry(
				dst, src, entries, child, ws, tracker,
			)
			child += int(entries[child].Next)
		}
		dst = append(dst, ']')
	case document.Object:
		dst = appendCanonicalObject(dst, src, entries, i, ws, tracker)
	default:
		// Unreachable for a tape built over validated input; render nothing
		// rather than invent bytes.
		return dst
	}
	if tracker != nil && e.Flags()&vibejson.TapeFlagKey == 0 && e.Next == 1 {
		tracker.spans = append(tracker.spans, UnifiedTokenSpan{
			Start: uint32(start - tracker.base),
			End:   uint32(len(dst) - tracker.base),
		})
	}
	return dst
}

// appendCanonicalObject renders one object with members stably sorted by
// decoded key bytes. Member references are collected into the shared
// workspace slice segmented by a frame base, so nested objects reuse one
// backing array; the frame truncates its segment (and its scratch decodes)
// on return.
func appendCanonicalObject(
	dst, src []byte,
	entries []vibejson.IndexEntry,
	i int,
	ws *CanonicalWorkspace,
	tracker *canonicalSpanTracker,
) []byte {
	e := &entries[i]
	count := int(e.Count())
	if count == 0 {
		return append(dst, '{', '}')
	}
	memberBase := len(ws.members)
	scratchBase := len(ws.scratch)
	key := i + 1
	sorted := true
	for m := 0; m < count; m++ {
		ke := &entries[key]
		ref := canonicalMember{keyEntry: int32(key)}
		if ke.Flags()&vibejson.TapeFlagEscaped == 0 {
			// Unescaped key: the decoded spelling is the raw content between
			// the quotes, aliased from the source without copying.
			ref.scratchOff = -1
			ref.length = int32(ke.End - ke.Start - 2)
		} else {
			// Escaped key: decode once into workspace scratch; the sort and
			// any re-encode both read this one decoded spelling.
			off := len(ws.scratch)
			node := vibejson.Node{Src: &src[0], Entry: ke}
			ws.scratch, _ = node.AppendText(ws.scratch)
			ref.scratchOff = int32(off)
			ref.length = int32(len(ws.scratch) - off)
		}
		if sorted && m > 0 {
			prev := ws.members[len(ws.members)-1]
			if canonicalKeyCompare(src, entries, ws, prev, ref) > 0 {
				sorted = false
			}
		}
		ws.members = append(ws.members, ref)
		key += 1 + int(entries[key+1].Next)
	}
	if !sorted {
		// Stable sort so duplicate keys keep their original relative order:
		// the canonical form is a pure function of the member *sequence*,
		// and {"a":1,"a":2} and {"a":2,"a":1} stay distinct documents.
		// Already-sorted input skips this
		// entirely via the sortedness scan above.
		canonicalStableSortMembers(src, entries, ws, memberBase)
	}
	dst = append(dst, '{')
	for m := 0; m < count; m++ {
		if m > 0 {
			dst = append(dst, ',')
		}
		ref := ws.members[memberBase+m]
		dst = appendCanonicalKeyString(dst, src, entries, ws, ref)
		dst = append(dst, ':')
		dst = appendCanonicalEntry(
			dst, src, entries, int(ref.keyEntry)+1, ws, tracker,
		)
	}
	dst = append(dst, '}')
	ws.members = ws.members[:memberBase]
	ws.scratch = ws.scratch[:scratchBase]
	return dst
}

// canonicalKeyBytes resolves a member's decoded key spelling, either aliased
// from the source (unescaped keys) or from the workspace scratch (escaped
// keys, decoded at collection time).
func canonicalKeyBytes(src []byte, entries []vibejson.IndexEntry, ws *CanonicalWorkspace, m canonicalMember) []byte {
	if m.scratchOff < 0 {
		start := entries[m.keyEntry].Start + 1
		return src[start : start+uint32(m.length)]
	}
	return ws.scratch[m.scratchOff : m.scratchOff+m.length]
}

// canonicalKeyCompare orders two members by decoded key bytes. Equal keys
// compare 0; the stable sort preserves their insertion order, never this
// function.
func canonicalKeyCompare(src []byte, entries []vibejson.IndexEntry, ws *CanonicalWorkspace, a, b canonicalMember) int {
	return bytes.Compare(
		canonicalKeyBytes(src, entries, ws, a),
		canonicalKeyBytes(src, entries, ws, b),
	)
}

// canonicalStableSortMembers stably sorts ws.members[base:] by decoded key.
// Small segments use binary insertion sort; larger ones a bottom-up merge
// through the workspace aux buffer, so no call allocates at steady state.
// Both are stable because duplicate keys retain their insertion order.
func canonicalStableSortMembers(src []byte, entries []vibejson.IndexEntry, ws *CanonicalWorkspace, base int) {
	seg := ws.members[base:]
	if len(seg) <= 24 {
		insertionSortMembers(src, entries, ws, seg)
		return
	}
	// Bottom-up merge: sort runs of 24 by insertion, then merge doubling run
	// widths through aux. Stability holds because merges take from the left
	// run on ties.
	for lo := 0; lo < len(seg); lo += 24 {
		hi := lo + 24
		if hi > len(seg) {
			hi = len(seg)
		}
		insertionSortMembers(src, entries, ws, seg[lo:hi])
	}
	if cap(ws.aux) < len(seg) {
		ws.aux = make([]canonicalMember, len(seg))
	}
	aux := ws.aux[:len(seg)]
	for width := 24; width < len(seg); width *= 2 {
		for lo := 0; lo < len(seg)-width; lo += 2 * width {
			mid := lo + width
			hi := lo + 2*width
			if hi > len(seg) {
				hi = len(seg)
			}
			mergeMembers(src, entries, ws, seg[lo:hi], mid-lo, aux)
		}
	}
}

func insertionSortMembers(src []byte, entries []vibejson.IndexEntry, ws *CanonicalWorkspace, seg []canonicalMember) {
	for i := 1; i < len(seg); i++ {
		m := seg[i]
		j := i
		for j > 0 && canonicalKeyCompare(src, entries, ws, seg[j-1], m) > 0 {
			seg[j] = seg[j-1]
			j--
		}
		seg[j] = m
	}
}

func mergeMembers(src []byte, entries []vibejson.IndexEntry, ws *CanonicalWorkspace, seg []canonicalMember, mid int, aux []canonicalMember) {
	aux = aux[:copy(aux[:mid], seg[:mid])]
	i, j, k := 0, mid, 0
	for i < len(aux) && j < len(seg) {
		// <= keeps the left run's element on ties: stability.
		if canonicalKeyCompare(src, entries, ws, aux[i], seg[j]) <= 0 {
			seg[k] = aux[i]
			i++
		} else {
			seg[k] = seg[j]
			j++
		}
		k++
	}
	for i < len(aux) {
		seg[k] = aux[i]
		i++
		k++
	}
}

// appendCanonicalKeyString appends the canonical quoted spelling of a member
// key, reusing the decoded bytes the collection pass produced. If the raw
// spelling is already canonical it is copied verbatim;
// otherwise the decoded spelling is re-encoded.
func appendCanonicalKeyString(dst, src []byte, entries []vibejson.IndexEntry, ws *CanonicalWorkspace, m canonicalMember) []byte {
	ke := &entries[m.keyEntry]
	raw := src[ke.Start:ke.End]
	// An unescaped string is canonical unless it contains a raw U+2028 or
	// U+2029, which the encoder escapes for JSON/JavaScript compatibility.
	escaped := ke.Flags()&vibejson.TapeFlagEscaped != 0
	if !escaped && scanner.ValidUTF8NoLineSeparator(raw[1:len(raw)-1]) ||
		escaped && rawQuotedStringIsCanonical(raw) {
		return append(dst, raw...)
	}
	return appendCanonicalQuoted(dst, canonicalKeyBytes(src, entries, ws, m))
}

// appendCanonicalTapeString appends the canonical quoted spelling of a
// string value entry. Already-canonical raw spellings (the overwhelmingly
// common case in engine-generated input) are copied verbatim; everything
// else is decoded into frame scratch and re-encoded.
func appendCanonicalTapeString(dst, src []byte, entries []vibejson.IndexEntry, i int, ws *CanonicalWorkspace) []byte {
	e := &entries[i]
	raw := src[e.Start:e.End]
	// Same fast path as appendCanonicalKeyString: an unescaped string still
	// needs a line-separator check before it can be copied verbatim.
	escaped := e.Flags()&vibejson.TapeFlagEscaped != 0
	if !escaped && scanner.ValidUTF8NoLineSeparator(raw[1:len(raw)-1]) ||
		escaped && rawQuotedStringIsCanonical(raw) {
		return append(dst, raw...)
	}
	scratchBase := len(ws.scratch)
	node := vibejson.Node{Src: &src[0], Entry: e}
	ws.scratch, _ = node.AppendText(ws.scratch)
	dst = appendCanonicalQuoted(dst, ws.scratch[scratchBase:])
	ws.scratch = ws.scratch[:scratchBase]
	return dst
}

// appendCanonicalQuoted appends text as a quoted, canonically escaped JSON
// string. This is a byte-exact mirror of vibejson's appendJSONStringBytes:
// short escapes for the named controls, lowercase \u00xx for the rest of C0,
// quote and backslash escaped, solidus raw, and U+2028/U+2029 escaped for
// JSON/JavaScript compatibility. Text is valid UTF-8 by admission, so the
// replacement-rune branch
// of the original is unreachable here and deliberately omitted — the
// differential tests would catch any drift.
func appendCanonicalQuoted(dst, text []byte) []byte {
	const hex = "0123456789abcdef"
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(text); {
		c := text[i]
		if c >= 0x80 {
			if c == 0xe2 && i+2 < len(text) && text[i+1] == 0x80 &&
				(text[i+2] == 0xa8 || text[i+2] == 0xa9) {
				dst = append(dst, text[start:i]...)
				dst = append(dst, '\\', 'u', '2', '0', '2', hex[text[i+2]&0xf])
				i += 3
				start = i
				continue
			}
			// Every other valid multi-byte UTF-8 sequence passes through raw.
			i++
			continue
		}
		if c >= 0x20 && c != '"' && c != '\\' {
			i++
			continue
		}
		dst = append(dst, text[start:i]...)
		switch c {
		case '"', '\\':
			dst = append(dst, '\\', c)
		case '\b':
			dst = append(dst, '\\', 'b')
		case '\f':
			dst = append(dst, '\\', 'f')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		default:
			dst = append(dst, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xf])
		}
		i++
		start = i
	}
	dst = append(dst, text[start:]...)
	return append(dst, '"')
}

// rawQuotedStringIsCanonical reports whether a raw quoted source span (open
// quote through close quote) is already the canonical spelling of its
// decoded string, without decoding. It is the string half of the
// already-canonical fast path. raw is trusted validated JSON, so bounds
// inside escapes are guaranteed by the grammar.
func rawQuotedStringIsCanonical(raw []byte) bool {
	i := 1
	end := len(raw) - 1
	for i < end {
		c := raw[i]
		if c != '\\' {
			if c == 0xe2 && i+2 < end && raw[i+1] == 0x80 &&
				(raw[i+2] == 0xa8 || raw[i+2] == 0xa9) {
				return false
			}
			i++
			continue
		}
		switch raw[i+1] {
		case '"', '\\', 'b', 'f', 'n', 'r', 't':
			// Exactly the escapes the canonical encoder emits for these
			// characters.
			i += 2
		case 'u':
			// A \u escape is canonical for U+2028/U+2029 and for C0
			// controls without a short form, always with lowercase digits.
			v, lower := hex4Lower(raw[i+2:])
			if v < 0 || !lower {
				return false
			}
			if v == 0x2028 || v == 0x2029 {
				i += 6
				continue
			}
			if v >= 0x20 {
				return false
			}
			switch v {
			case '\b', '\f', '\n', '\r', '\t':
				// The encoder uses the short escape for these.
				return false
			}
			i += 6
		default:
			// \/ is legal JSON but the canonical encoder writes / raw.
			return false
		}
	}
	return true
}

// hex4Lower parses four hex digits, reporting the value and whether every
// alphabetic digit is lowercase (the canonical encoder emits lowercase hex).
// A short or non-hex input reports -1.
func hex4Lower(b []byte) (v int, lower bool) {
	if len(b) < 4 {
		return -1, false
	}
	lower = true
	for k := 0; k < 4; k++ {
		c := b[k]
		v <<= 4
		switch {
		case c >= '0' && c <= '9':
			v |= int(c - '0')
		case c >= 'a' && c <= 'f':
			v |= int(c-'a') + 10
		case c >= 'A' && c <= 'F':
			v |= int(c-'A') + 10
			lower = false
		default:
			return -1, false
		}
	}
	return v, lower
}

// IndexIsCanonical reports whether the document behind index is already in
// canonical form: no interstitial whitespace, every object's members
// in nondecreasing decoded-key order, every string canonically escaped. It
// This is the already-canonical fast check: one positional pass over the tape,
// no decoding except for escaped keys (needed for the order comparison), no
// output. A true result guarantees AppendCanonicalIndexed(index) equals
// index.Src byte for byte.
func IndexIsCanonical(index vibejson.Index, ws *CanonicalWorkspace) bool {
	if len(index.Entries) == 0 || len(index.Src) == 0 {
		return false
	}
	root := &index.Entries[0]
	// Leading or trailing whitespace around the root value is
	// non-canonical.
	if root.Start != 0 || root.End != uint32(len(index.Src)) {
		return false
	}
	_, ok := canonicalCheckEntry(index.Src, index.Entries, 0, root.Start, ws)
	return ok
}

// canonicalCheckEntry verifies the value at entry i begins exactly at pos
// (whitespace-freedom) and is canonically spelled, returning one past its
// last byte. The walk mirrors appendCanonicalEntry position for position.
func canonicalCheckEntry(src []byte, entries []vibejson.IndexEntry, i int, pos uint32, ws *CanonicalWorkspace) (uint32, bool) {
	e := &entries[i]
	if e.Start != pos {
		return 0, false
	}
	switch e.Kind() {
	case document.String:
		raw := src[e.Start:e.End]
		if e.Flags()&vibejson.TapeFlagEscaped == 0 {
			if !scanner.ValidUTF8NoLineSeparator(raw[1 : len(raw)-1]) {
				return 0, false
			}
		} else if !rawQuotedStringIsCanonical(raw) {
			return 0, false
		}
		return e.End, true
	case document.Number, document.Bool, document.Null:
		// Verbatim-preserved spellings are canonical by definition.
		return e.End, true
	case document.Array:
		count := int(e.Count())
		if count == 0 {
			// "[]" with no interior whitespace.
			return e.End, e.End == e.Start+2
		}
		child := i + 1
		p := e.Start + 1
		for m := 0; m < count; m++ {
			if m > 0 {
				p++ // ','
			}
			var ok bool
			p, ok = canonicalCheckEntry(src, entries, child, p, ws)
			if !ok {
				return 0, false
			}
			child += int(entries[child].Next)
		}
		// The close bracket must follow the last element immediately.
		return e.End, e.End == p+1
	case document.Object:
		return canonicalCheckObject(src, entries, i, ws)
	default:
		return 0, false
	}
}

// canonicalCheckObject verifies member spacing, canonical key spellings, and
// nondecreasing decoded-key order. Duplicate keys are equal and retain their
// sequence order. Escaped keys decode into frame scratch
// for the comparison; unescaped keys compare as source aliases.
func canonicalCheckObject(src []byte, entries []vibejson.IndexEntry, i int, ws *CanonicalWorkspace) (uint32, bool) {
	e := &entries[i]
	count := int(e.Count())
	if count == 0 {
		return e.End, e.End == e.Start+2
	}
	scratchBase := len(ws.scratch)
	defer func() { ws.scratch = ws.scratch[:scratchBase] }()
	var prev canonicalMember
	key := i + 1
	p := e.Start + 1
	for m := 0; m < count; m++ {
		if m > 0 {
			p++ // ','
		}
		ke := &entries[key]
		if ke.Start != p {
			return 0, false
		}
		if ke.Flags()&vibejson.TapeFlagEscaped != 0 &&
			!rawQuotedStringIsCanonical(src[ke.Start:ke.End]) {
			return 0, false
		}
		cur := canonicalMember{keyEntry: int32(key)}
		if ke.Flags()&vibejson.TapeFlagEscaped == 0 {
			cur.scratchOff = -1
			cur.length = int32(ke.End - ke.Start - 2)
		} else {
			// Frame scratch holds at most the previous and current decoded
			// keys; older decodes are dropped by rebasing below.
			off := len(ws.scratch)
			node := vibejson.Node{Src: &src[0], Entry: ke}
			ws.scratch, _ = node.AppendText(ws.scratch)
			cur.scratchOff = int32(off)
			cur.length = int32(len(ws.scratch) - off)
		}
		if m > 0 && canonicalKeyCompare(src, entries, ws, prev, cur) > 0 {
			return 0, false
		}
		// Only prev needs to survive to the next iteration. Keeping every
		// decoded key for the frame would also be correct; the scratch is
		// simply truncated to at most the surviving prev so a pathological
		// all-escaped-keys object cannot grow it past two keys per frame.
		if cur.scratchOff >= 0 && cur.scratchOff != int32(scratchBase) {
			n := copy(ws.scratch[scratchBase:], ws.scratch[cur.scratchOff:cur.scratchOff+cur.length])
			ws.scratch = ws.scratch[:scratchBase+n]
			cur.scratchOff = int32(scratchBase)
		}
		prev = cur
		p = ke.End + 1 // ':'
		value := key + 1
		var ok bool
		p, ok = canonicalCheckEntry(src, entries, value, p, ws)
		if !ok {
			return 0, false
		}
		key = value + int(entries[value].Next)
	}
	return e.End, e.End == p+1
}
