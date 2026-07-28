package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// Persistence is byte-for-byte reopen equivalence: an image WriteTo writes must
// Open into a Segment whose Len, every Doc(i)'s tape, and every accessor over it
// — Raw, StringBytes, the typed number and bool reads, Get, Index, and Pointer
// — are byte-identical to the segment that was written, and to a fresh standalone
// build of the same source. These tests hold the format to that bar three ways.
// The round-trip battery drives the adversarial corpora (nested, escaped,
// duplicate-key, wide, narrow, shape-taped, value-dictionary, mixed, empty, and
// a single oversize document) through WriteTo then Open under every mode. The
// bounded-exhaustive gate enumerates the small-document domain, composes it into
// sets, and reopens each, reporting the count as the strength of the evidence.
// The corruption battery feeds truncated and garbled images to Open and
// requires a rejected error, never a panic. The GOGC lifetime gate proves a
// reopened segment keeps its borrowed image alive after every external reference is
// dropped, under forced collection.

// persistModeVariants pairs a labeled segment configuration with the corpus it is
// exercised over. Each is round-tripped and every read compared.
type persistCase struct {
	name string
	seg  *Segment
	docs []string
}

// buildPersistCases assembles the segment/corpus pairs the round-trip battery
// covers: classic storage under both enrichment options, shape-taped storage in
// the narrow and (seam-forced) wide widths, the postings and value-dictionary
// accelerators layered on, the empty segment, and a single oversize document.
func buildPersistCases(t *testing.T) []persistCase {
	t.Helper()
	base := segmentTestCorpus()
	clustered := shapeTapeClusteredDocs(48, 3, 7)
	mixed := append(append([]string{}, clustered...), base...)

	// A repeat-heavy corpus with an oversize member so the value dictionary,
	// narrow, and wide widths all engage; the huge document also stands alone as
	// the single-oversize-document edge. Its root span clears the 64 KiB narrow
	// bound, so a recurring copy stores wide.
	huge := bigFlatObject(2000)
	repeated := []string{
		`{"lang":"en","src":"web","tag":"promoted"}`,
		`{"lang":"en","src":"web","tag":"promoted"}`,
		`{"lang":"fr","src":"web","tag":"promoted"}`,
		`{"lang":"en","src":"app","tag":"organic"}`,
		huge, huge,
	}

	var cases []persistCase
	add := func(name string, seg *Segment, docs []string) {
		for i, d := range docs {
			if _, err := seg.Append([]byte(d)); err != nil {
				t.Fatalf("%s: Append(%.40q) #%d: %v", name, d, i, err)
			}
		}
		cases = append(cases, persistCase{name, seg, docs})
	}

	add("classic", &Segment{}, base)
	add("classicHashKeys", &Segment{Options: document.IndexOptions{HashKeys: true}}, base)
	add("shapeNarrow", &Segment{ShapeTapes: true}, mixed)
	add("shapeNarrowHashKeys", &Segment{ShapeTapes: true, Options: document.IndexOptions{HashKeys: true}}, mixed)
	add("shapeWide", &Segment{ShapeTapes: true, wideValueTapes: true}, mixed)
	add("postings", &Segment{ShapeTapes: true, Postings: true}, mixed)
	add("valueDict", &Segment{ShapeTapes: true, ValueDict: true, valueFloor: 1}, mixed)
	add("allModes", &Segment{ShapeTapes: true, Postings: true, ValueDict: true, valueFloor: 1,
		Options: document.IndexOptions{HashKeys: true}}, repeated)
	add("empty", &Segment{ShapeTapes: true, Postings: true, ValueDict: true}, nil)
	add("singleHuge", &Segment{ShapeTapes: true}, []string{huge})
	return cases
}

// bigFlatObject builds a flat object of n members whose root span exceeds the
// 64 KiB narrow bound, so a recurring copy stores wide and a lone copy stores
// classic — the oversize edge in flat form.
func bigFlatObject(n int) string {
	var b strings.Builder
	b.WriteByte('{')
	for i := range n {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `"field_%05d":"value_%05d_with_padding"`, i, i)
	}
	b.WriteByte('}')
	return b.String()
}

// TestSegmentPersistRoundTrip is the reopen-equals-original gate over every mode
// and corpus: WriteTo then Open, then every read compared against a standalone
// build and against the original segment.
func TestSegmentPersistRoundTrip(t *testing.T) {
	for _, c := range buildPersistCases(t) {
		t.Run(c.name, func(t *testing.T) {
			reopened := persistRoundTrip(t, c.seg)
			checkPersistEquivalent(t, c.seg, reopened, c.docs)
		})
	}
}

// persistRoundTrip serializes set, reopens an independent copy of the image, and
// checks the byte count WriteTo reports matches what it wrote.
func persistRoundTrip(t *testing.T, seg *Segment) *Segment {
	t.Helper()
	var buf bytes.Buffer
	n, err := seg.WriteTo(&buf)
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if n != int64(buf.Len()) {
		t.Fatalf("WriteTo reported %d bytes, wrote %d", n, buf.Len())
	}
	// An independent copy models a fresh mapping and ensures the reopened segment
	// aliases only its own image, never the writer's buffer.
	image := append([]byte(nil), buf.Bytes()...)
	reopened, err := OpenSegment(image)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return reopened
}

// checkPersistEquivalent asserts a reopened segment reads identically to the
// original: same length, same per-document tape and accessors against a
// standalone reference, and the same segment-level Stats, batch pointer columns,
// and posting queries.
func checkPersistEquivalent(t *testing.T, orig, reopened *Segment, docs []string) {
	t.Helper()
	if reopened.Len() != orig.Len() || reopened.Len() != len(docs) {
		t.Fatalf("Len = %d, original %d, corpus %d", reopened.Len(), orig.Len(), len(docs))
	}
	// Stats is compared before any read, so neither segment has lazily widened a
	// shape-taped document: Widened is a runtime cache artifact, not persisted
	// state, and every other field pins the serialized composition (shape,
	// narrow/wide, and value-dictionary accounting) reopens exactly.
	if got, want := reopened.Stats(), orig.Stats(); got != want {
		t.Fatalf("Stats = %+v, original %+v", got, want)
	}
	for i, doc := range docs {
		checkPersistDoc(t, reopened, i, doc)
	}
	checkPersistBatch(t, orig, reopened, docs)
}

// checkPersistDoc holds one reopened document to the standalone reference: its
// source and tape are byte-identical, every accessor agrees, and every
// reachable JSON Pointer resolves to the same bytes.
func checkPersistDoc(t *testing.T, seg *Segment, i int, doc string) {
	t.Helper()
	ref, err := vibejson.BuildIndexOptions([]byte(doc), make([]vibejson.IndexEntry, len(doc)+2), seg.Options)
	if err != nil {
		t.Fatalf("standalone build of doc %d: %v", i, err)
	}
	got := seg.Doc(i)
	if string(got.Src) != doc {
		t.Fatalf("doc %d source = %.60q, want %.60q", i, got.Src, doc)
	}
	if len(got.Entries) != len(ref.Entries) {
		t.Fatalf("doc %d has %d entries, standalone %d", i, len(got.Entries), len(ref.Entries))
	}
	for j := range got.Entries {
		if got.Entries[j] != ref.Entries[j] {
			t.Fatalf("doc %d entry %d = %+v, standalone %+v", i, j, got.Entries[j], ref.Entries[j])
		}
	}
	assertReadsEqual(t, ref.Root(), got.Root(), fmt.Sprintf("doc%d", i))
	for _, p := range enumeratePointers(ref.Root(), "", nil) {
		rn, rok, rerr := ref.Pointer(p)
		gn, gok, gerr := got.Pointer(p)
		if rok != gok || (rerr == nil) != (gerr == nil) {
			t.Fatalf("doc %d Pointer(%q) = (%v,%v), standalone (%v,%v)", i, p, gok, gerr, rok, rerr)
		}
		if rok && !bytes.Equal(gn.Raw().Bytes(), rn.Raw().Bytes()) {
			t.Fatalf("doc %d Pointer(%q) raw %q != %q", i, p, gn.Raw().Bytes(), rn.Raw().Bytes())
		}
	}
}

// checkPersistBatch compares the segment-level reads that cross documents: the batch
// pointer column and, when built, the posting-backed existence and containment
// queries, each identical between the original and the reopened segment.
func checkPersistBatch(t *testing.T, orig, reopened *Segment, docs []string) {
	t.Helper()
	keys := persistProbeKeys(docs)
	for _, k := range keys {
		ptr, err := vibejson.CompilePointer("/" + escapePointerToken(k))
		if err != nil {
			continue
		}
		var a, b []vibejson.RawValue
		a, aerr := orig.AppendPointer(a, ptr)
		b, berr := reopened.AppendPointer(b, ptr)
		if (aerr == nil) != (berr == nil) || len(a) != len(b) {
			t.Fatalf("AppendPointer(%q): original (%d,%v), reopened (%d,%v)", k, len(a), aerr, len(b), berr)
		}
		for i := range a {
			if !bytes.Equal(a[i].Bytes(), b[i].Bytes()) {
				t.Fatalf("AppendPointer(%q)[%d] = %q, original %q", k, i, b[i].Bytes(), a[i].Bytes())
			}
		}
		if !intsEqual(orig.WhereExists(k), reopened.WhereExists(k)) {
			t.Fatalf("WhereExists(%q) diverged: original %v reopened %v", k, orig.WhereExists(k), reopened.WhereExists(k))
		}
		for _, needle := range []string{`"web"`, `1`, `true`, `null`, `"promoted"`} {
			oa, oerr := orig.WhereContains(k, []byte(needle))
			ra, rerr := reopened.WhereContains(k, []byte(needle))
			if (oerr == nil) != (rerr == nil) || !intsEqual(oa, ra) {
				t.Fatalf("WhereContains(%q,%s): original (%v,%v) reopened (%v,%v)", k, needle, oa, oerr, ra, rerr)
			}
		}
	}
}

// persistProbeKeys collects a bounded set of top-level object keys appearing in
// the corpus, the query surface the batch checks probe.
func persistProbeKeys(docs []string) []string {
	seen := map[string]struct{}{}
	var keys []string
	for _, d := range docs {
		idx, err := vibejson.BuildIndex([]byte(d), make([]vibejson.IndexEntry, len(d)+2))
		if err != nil {
			continue
		}
		it, ok := idx.Root().ObjectIter()
		if !ok {
			continue
		}
		for {
			k, _, ok := it.Next()
			if !ok {
				break
			}
			name, _ := k.AppendText(nil)
			s := string(name)
			if _, dup := seen[s]; !dup {
				seen[s] = struct{}{}
				keys = append(keys, s)
			}
		}
		if len(keys) >= 32 {
			break
		}
	}
	return keys
}

// assertReadsEqual compares two nodes through every read accessor and recurses
// through containers, so a mismatch anywhere in the tree is caught. ref is the
// standalone reference; got is the reopened segment's node.
func assertReadsEqual(t *testing.T, ref, got vibejson.Node, path string) {
	t.Helper()
	if ref.Kind() != got.Kind() {
		t.Fatalf("%s: kind %v != %v", path, got.Kind(), ref.Kind())
	}
	if !bytes.Equal(got.Raw().Bytes(), ref.Raw().Bytes()) {
		t.Fatalf("%s: Raw %q != %q", path, got.Raw().Bytes(), ref.Raw().Bytes())
	}
	switch ref.Kind() {
	case document.String:
		rb, rok := ref.StringBytes()
		gb, gok := got.StringBytes()
		if rok != gok || !bytes.Equal(rb, gb) {
			t.Fatalf("%s: StringBytes (%q,%v) != (%q,%v)", path, gb, gok, rb, rok)
		}
		ra, _ := ref.AppendText(nil)
		ga, _ := got.AppendText(nil)
		if !bytes.Equal(ra, ga) {
			t.Fatalf("%s: AppendText %q != %q", path, ga, ra)
		}
	case document.Number:
		if rb, rok := ref.NumberBytes(); true {
			gb, gok := got.NumberBytes()
			if rok != gok || !bytes.Equal(rb, gb) {
				t.Fatalf("%s: NumberBytes (%q,%v) != (%q,%v)", path, gb, gok, rb, rok)
			}
		}
		if ri, rok := ref.Int64(); true {
			gi, gok := got.Int64()
			if rok != gok || ri != gi {
				t.Fatalf("%s: Int64 (%d,%v) != (%d,%v)", path, gi, gok, ri, rok)
			}
		}
		if ru, rok := ref.Uint64(); true {
			gu, gok := got.Uint64()
			if rok != gok || ru != gu {
				t.Fatalf("%s: Uint64 (%d,%v) != (%d,%v)", path, gu, gok, ru, rok)
			}
		}
		if rf, rok := ref.Float64(); true {
			gf, gok := got.Float64()
			if rok != gok || rf != gf {
				t.Fatalf("%s: Float64 (%v,%v) != (%v,%v)", path, gf, gok, rf, rok)
			}
		}
		if ref.IsInteger() != got.IsInteger() {
			t.Fatalf("%s: IsInteger %v != %v", path, got.IsInteger(), ref.IsInteger())
		}
	case document.Bool:
		rb, _ := ref.Bool()
		gb, _ := got.Bool()
		if rb != gb {
			t.Fatalf("%s: Bool %v != %v", path, gb, rb)
		}
	case document.Null:
		if got.IsNull() != ref.IsNull() {
			t.Fatalf("%s: IsNull %v != %v", path, got.IsNull(), ref.IsNull())
		}
	case document.Object:
		rl, _ := ref.ObjectLen()
		gl, _ := got.ObjectLen()
		if rl != gl {
			t.Fatalf("%s: ObjectLen %d != %d", path, gl, rl)
		}
		rit, _ := ref.ObjectIter()
		git, _ := got.ObjectIter()
		for m := 0; ; m++ {
			rk, rv, rok := rit.Next()
			gk, gv, gok := git.Next()
			if rok != gok {
				t.Fatalf("%s: object member %d presence %v != %v", path, m, gok, rok)
			}
			if !rok {
				break
			}
			if !bytes.Equal(gk.Raw().Bytes(), rk.Raw().Bytes()) {
				t.Fatalf("%s: object key %d %q != %q", path, m, gk.Raw().Bytes(), rk.Raw().Bytes())
			}
			assertReadsEqual(t, rv, gv, fmt.Sprintf("%s/#%d", path, m))
		}
		// Get resolves by decoded name with the last duplicate winning; both
		// sides run the same rule, so they must agree key for key.
		rit2, _ := ref.ObjectIter()
		for {
			k, _, ok := rit2.Next()
			if !ok {
				break
			}
			name, _ := k.AppendText(nil)
			rg, rok := ref.Get(string(name))
			gg, gok := got.Get(string(name))
			if rok != gok || (rok && !bytes.Equal(gg.Raw().Bytes(), rg.Raw().Bytes())) {
				t.Fatalf("%s: Get(%q) diverged", path, name)
			}
		}
	case document.Array:
		rl, _ := ref.ArrayLen()
		gl, _ := got.ArrayLen()
		if rl != gl {
			t.Fatalf("%s: ArrayLen %d != %d", path, gl, rl)
		}
		rit, _ := ref.ArrayIter()
		git, _ := got.ArrayIter()
		for idx := 0; ; idx++ {
			re, rok := rit.Next()
			ge, gok := git.Next()
			if rok != gok {
				t.Fatalf("%s: array element %d presence %v != %v", path, idx, gok, rok)
			}
			if !rok {
				break
			}
			assertReadsEqual(t, re, ge, fmt.Sprintf("%s/%d", path, idx))
			ri, riok := ref.Index(idx)
			gi, giok := got.Index(idx)
			if riok != giok || (riok && !bytes.Equal(gi.Raw().Bytes(), ri.Raw().Bytes())) {
				t.Fatalf("%s: Index(%d) diverged", path, idx)
			}
		}
	}
}

// enumeratePointers collects every JSON Pointer reachable in n, so Pointer
// resolution can be compared at every position. Keys are decoded and escaped
// per RFC 6901.
func enumeratePointers(n vibejson.Node, prefix string, out []string) []string {
	out = append(out, prefix)
	switch n.Kind() {
	case document.Object:
		it, ok := n.ObjectIter()
		if !ok {
			return out
		}
		for {
			k, v, ok := it.Next()
			if !ok {
				break
			}
			name, _ := k.AppendText(nil)
			out = enumeratePointers(v, prefix+"/"+escapePointerToken(string(name)), out)
		}
	case document.Array:
		it, ok := n.ArrayIter()
		if !ok {
			return out
		}
		for idx := 0; ; idx++ {
			el, ok := it.Next()
			if !ok {
				break
			}
			out = enumeratePointers(el, prefix+"/"+strconv.Itoa(idx), out)
		}
	}
	return out
}

func intsEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestSegmentPersistExhaustive is the bounded-domain gate: it enumerates the
// small-document space, composes it into sets — one holding every document and
// one per ordered pair over a prefix — reopens each, and checks every read.
// The enumerated document count and reopened-segment count are logged as the
// evidence's strength.
func TestSegmentPersistExhaustive(t *testing.T) {
	depth, nodes, width := bexPairDepth, testIterations(bexPairNodes, 2), testIterations(bexPairWidth, 2)
	docs := exhaustiveGenerate(depth, nodes, width)
	if len(docs) > bexDomainCeiling {
		t.Fatalf("enumerated domain %d exceeds ceiling %d", len(docs), bexDomainCeiling)
	}

	// One segment over the whole domain, each document appended twice so every
	// conforming layout reaches its shape-taped (narrow or wide) storage.
	all := make([]string, 0, 2*len(docs))
	for _, d := range docs {
		all = append(all, string(d.json), string(d.json))
	}
	whole := &Segment{ShapeTapes: true, ValueDict: true, valueFloor: 1,
		Options: document.IndexOptions{HashKeys: true}}
	for i, d := range all {
		if _, err := whole.Append([]byte(d)); err != nil {
			t.Fatalf("whole Append(%.40q) #%d: %v", d, i, err)
		}
	}
	reopened := persistRoundTrip(t, whole)
	if reopened.Len() != len(all) {
		t.Fatalf("whole reopened Len %d != %d", reopened.Len(), len(all))
	}
	for i, d := range all {
		checkPersistDoc(t, reopened, i, d)
	}

	// Every ordered pair over a bounded prefix, each reopened as its own segment —
	// the small-Segment enumeration proper. The sequence repeats the pair so both
	// documents dedup, exercising composition across storage classes.
	prefix := docs
	if cap := testIterations(60, 12); len(prefix) > cap {
		prefix = prefix[:cap]
	}
	pairs := 0
	for _, a := range prefix {
		for _, b := range prefix {
			seg := &Segment{ShapeTapes: true, Postings: true}
			seq := []string{string(a.json), string(b.json), string(a.json), string(b.json)}
			for _, d := range seq {
				if _, err := seg.Append([]byte(d)); err != nil {
					t.Fatalf("pair Append(%.40q): %v", d, err)
				}
			}
			ro := persistRoundTrip(t, seg)
			for i, d := range seq {
				checkPersistDoc(t, ro, i, d)
			}
			pairs++
			if t.Failed() {
				return
			}
		}
	}

	t.Logf("bounded domain depth<=%d nodes<=%d width<=%d: %d documents enumerated", depth, nodes, width, len(docs))
	t.Logf("reopened sets: 1 whole-domain seg of %d documents, %d ordered-pair sets over the first %d", len(all), pairs, len(prefix))
}

// TestSegmentPersistCorruptInput requires Open to reject every malformed image
// with an error and never panic — the fail-closed contract on untrusted bytes.
func TestSegmentPersistCorruptInput(t *testing.T) {
	var seg Segment
	seg.ShapeTapes = true
	for _, d := range append(shapeTapeClusteredDocs(6, 2, 4), `{"a":1,"b":[2,3]}`, `"scalar"`) {
		if _, err := seg.Append([]byte(d)); err != nil {
			t.Fatal(err)
		}
	}
	var buf bytes.Buffer
	if _, err := seg.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}
	good := buf.Bytes()

	// A pristine copy must still open, so the mutations below are the only cause
	// of any rejection.
	if _, err := OpenSegment(append([]byte(nil), good...)); err != nil {
		t.Fatalf("OpenSegment(good): %v", err)
	}

	corrupt := []struct {
		name    string
		mutate  func([]byte) []byte
		wantErr error
	}{
		{"empty", func([]byte) []byte { return nil }, ErrPersistCorrupt},
		{"tiny", func([]byte) []byte { return make([]byte, 8) }, ErrPersistCorrupt},
		{"headerMagic", func(b []byte) []byte { b[0] ^= 0xFF; return b }, ErrPersistMagic},
		{"headerVersion", func(b []byte) []byte { b[8] ^= 0xFF; return b }, ErrPersistVersion},
		{"footerMagic", func(b []byte) []byte { b[len(b)-ImageFooterLen] ^= 0xFF; return b }, ErrPersistMagic},
		{"footerVersion", func(b []byte) []byte { b[len(b)-8] ^= 0xFF; return b }, ErrPersistVersion},
		{"manifestChecksum", func(b []byte) []byte { b[len(b)-ImageFooterLen-1] ^= 0xFF; return b }, ErrPersistCorrupt},
		{"truncatedFooter", func(b []byte) []byte { return b[:len(b)-4] }, nil},
		{"truncatedHalf", func(b []byte) []byte { return b[:len(b)/2] }, nil},
		{"manifestOffsetHuge", func(b []byte) []byte {
			for i := range 8 {
				b[len(b)-ImageFooterLen+8+i] = 0xFF
			}
			return b
		}, ErrPersistCorrupt},
	}
	for _, tc := range corrupt {
		t.Run(tc.name, func(t *testing.T) {
			img := tc.mutate(append([]byte(nil), good...))
			seg, err := OpenSegment(img)
			if err == nil {
				t.Fatalf("OpenSegment(%s) succeeded, want rejection", tc.name)
			}
			if seg != nil {
				t.Fatalf("OpenSegment(%s) returned a seg with an error", tc.name)
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("OpenSegment(%s) error %v, want %v", tc.name, err, tc.wantErr)
			}
		})
	}

	// Manifest fields resealed with a valid checksum must still be rejected by
	// the semantic bounds checks, not merely by the checksum: a corrupt count or
	// span that slipped a re-sealed image past the checksum would otherwise
	// drive an out-of-range read.
	reseal := func(mutate func(manifest []byte)) []byte {
		img := append([]byte(nil), good...)
		footer := img[len(img)-ImageFooterLen:]
		off := binary.LittleEndian.Uint64(footer[8:])
		length := binary.LittleEndian.Uint64(footer[16:])
		manifest := img[off : off+length]
		mutate(manifest)
		binary.LittleEndian.PutUint64(footer[24:], ManifestChecksum(manifest))
		return img
	}
	for _, tc := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"docCountHuge", func(m []byte) { binary.LittleEndian.PutUint32(m[32:], 1<<30) }},
		{"shapeOffsetHuge", func(m []byte) { binary.LittleEndian.PutUint64(m[40:], 1<<50) }},
		{"narrowTotalHuge", func(m []byte) { binary.LittleEndian.PutUint32(m[36:], 1<<30) }},
		{"manifestMagic", func(m []byte) { m[0] ^= 0xFF }},
	} {
		t.Run("resealed_"+tc.name, func(t *testing.T) {
			img := reseal(tc.mutate)
			seg, err := OpenSegment(img)
			if err == nil || seg != nil {
				t.Fatalf("OpenSegment(resealed %s) = (%v,%v), want rejection", tc.name, seg, err)
			}
			if !errors.Is(err, ErrPersistCorrupt) {
				t.Fatalf("OpenSegment(resealed %s) error %v, want ErrPersistCorrupt", tc.name, err)
			}
		})
	}

	// A sweep of single-byte flips must never panic; each either opens or is
	// rejected. Bytes that leave the checksum-guarded framing intact may open,
	// so only the no-panic invariant is asserted here.
	for i := 0; i < len(good); i += 7 {
		img := append([]byte(nil), good...)
		img[i] ^= 0xAA
		if s, err := OpenSegment(img); err == nil && s == nil {
			t.Fatalf("flip at %d: nil seg without error", i)
		}
	}

	// Random-length prefixes of arbitrary bytes: never a panic.
	garbage := bytes.Repeat([]byte("SJDOCSET\x01\x00\x00\x00garbage"), 64)
	for n := 0; n <= len(garbage); n += 3 {
		if _, err := OpenSegment(garbage[:n]); err == nil {
			t.Fatalf("OpenSegment(garbage[:%d]) unexpectedly succeeded", n)
		}
	}
}

// TestSegmentPersistReopenAppend proves a reopened segment is fully functional, not
// read-only: appending a document that matches an already-loaded shape dedups
// against the reconstructed shape (its fingerprint and name table rebuilt
// exactly), stores shape-taped, and reads back correctly, while the new bytes
// land in fresh arenas that never touch the borrowed image.
func TestSegmentPersistReopenAppend(t *testing.T) {
	docs := shapeTapeClusteredDocs(20, 2, 6)
	orig := &Segment{ShapeTapes: true}
	for _, d := range docs {
		if _, err := orig.Append([]byte(d)); err != nil {
			t.Fatal(err)
		}
	}
	reopened := persistRoundTrip(t, orig)
	before := reopened.Stats()

	// A document sharing an existing layout must dedup on its first post-reopen
	// sighting, since its shape was already compiled into the reconstructed cache.
	extra := docs[0]
	ord, err := reopened.Append([]byte(extra))
	if err != nil {
		t.Fatal(err)
	}
	if ord != len(docs) {
		t.Fatalf("Append ordinal = %d, want %d", ord, len(docs))
	}
	after := reopened.Stats()
	if after.ShapeTaped != before.ShapeTaped+1 {
		t.Fatalf("appended document did not dedup against a reopened shape: ShapeTaped %d -> %d",
			before.ShapeTaped, after.ShapeTaped)
	}
	if after.Shapes != before.Shapes {
		t.Fatalf("appended document compiled a new shape (%d -> %d), the reopened cache was incomplete",
			before.Shapes, after.Shapes)
	}
	checkPersistDoc(t, reopened, ord, extra)
}

// TestSegmentPersistZeroCopy proves the reopen is genuinely zero-copy on a
// little-endian host: a classic document's source and tape view straight into
// the mapped image rather than a fresh allocation.
func TestSegmentPersistZeroCopy(t *testing.T) {
	if !persistNativeLittleEndian {
		t.Skip("zero-copy views are taken only on little-endian hosts")
	}
	var seg Segment
	for _, d := range []string{`{"id":7,"name":"first","tags":[1,2,3]}`, `[true,false,null,42]`} {
		if _, err := seg.Append([]byte(d)); err != nil {
			t.Fatal(err)
		}
	}
	var buf bytes.Buffer
	if _, err := seg.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}
	image := append([]byte(nil), buf.Bytes()...)
	reopened, err := OpenSegment(image)
	if err != nil {
		t.Fatal(err)
	}
	lo := uintptr(unsafe.Pointer(&image[0]))
	hi := lo + uintptr(len(image))
	within := func(p uintptr) bool { return p >= lo && p < hi }
	for i := 0; i < reopened.Len(); i++ {
		doc := reopened.Doc(i)
		if !within(uintptr(unsafe.Pointer(unsafe.SliceData(doc.Src)))) {
			t.Fatalf("doc %d source is a copy, not a view into the image", i)
		}
		if len(doc.Entries) > 0 && !within(uintptr(unsafe.Pointer(&doc.Entries[0]))) {
			t.Fatalf("doc %d tape is a copy, not a view into the image", i)
		}
	}
}

// TestGCPersistBorrowedImage is the GOGC lifetime gate: a reopened segment must keep
// its borrowed image alive after every external reference to it is dropped. Run
// without -race (which masks the class) under GOGC=1:
//
//	GOGC=1 go test -run TestGCPersistBorrowedImage -count=5 ./
//
// A pinning regression surfaces as a bad heap pointer during a collection or as
// a read that no longer matches the standalone reference.
func TestGCPersistBorrowedImage(t *testing.T) {
	docs := append(shapeTapeClusteredDocs(40, 3, 6), segmentTestCorpus()...)
	build := func() *Segment {
		seg := &Segment{ShapeTapes: true, Postings: true, ValueDict: true, valueFloor: 1}
		for _, d := range docs {
			if _, err := seg.Append([]byte(d)); err != nil {
				t.Fatal(err)
			}
		}
		var buf bytes.Buffer
		if _, err := seg.WriteTo(&buf); err != nil {
			t.Fatal(err)
		}
		// The only surviving reference to these bytes is the one Open pins.
		image := append([]byte(nil), buf.Bytes()...)
		reopened, err := OpenSegment(image)
		if err != nil {
			t.Fatal(err)
		}
		return reopened
	}

	for iter := range 8 {
		reopened := build()
		// Churn the heap and force collections so any unpinned image would be
		// reclaimed before the reads below.
		runtime.GC()
		sink := 0
		for j := range 4096 {
			b := make([]byte, 512)
			sink += len(b)
			if j%512 == 0 {
				runtime.GC()
			}
		}
		_ = forceStackMovement(96, iter)
		runtime.GC()
		if reopened.Len() != len(docs) {
			t.Fatalf("iter %d: Len %d != %d", iter, reopened.Len(), len(docs))
		}
		for i, d := range docs {
			checkPersistDoc(t, reopened, i, d)
		}
		_ = sink
	}
}
