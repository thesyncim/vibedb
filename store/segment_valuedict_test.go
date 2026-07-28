package store

import (
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// Exhaustive differential testing of the corpus-wide value dictionary over a
// bounded input domain.
//
// TestExhaustiveValueDict enumerates the same small-scope document space the
// tape equivalence suite covers (verify_exhaustive_test.go), builds each
// document into a ValueDict Segment with the interning floor lowered to one — so
// every repeated span, however short, is dictionary-backed and the arena read
// path is exercised on every value shape — and asserts that every read routed
// through the dictionary is byte-identical to the same read on the classic
// source tape and to the from-scratch AST. It is exhaustive over the bounded
// domain, not a proof for all inputs: the enumerated domain size, logged by the
// test, is the evidence. It runs the whole domain twice more, once with
// ShapeTapes composed on and once corpus-wide across every document, so the two
// dedup levers are checked together and the cross-document interning path is
// covered.
//
// The read==source invariant it pins: a dictionary-backed value resolves to a
// handle over its interned arena span, which ValueInterner guarantees is
// byte-identical to the source span it replaced; the handle carries the source
// entry's info word, so every scalar accessor — a pure function of the bytes and
// that word — returns exactly what the source-backed node would. A disagreement
// on any enumerated input is a real bug.

// TestExhaustiveValueDict is the value dictionary's bounded-exhaustive
// equivalence gate. See the file comment for the domain and the invariant.
func TestExhaustiveValueDict(t *testing.T) {
	depth := bexMainDepth
	nodes := testIterations(bexMainNodes, 3)
	width := testIterations(bexMainWidth, 2)
	docs := exhaustiveGenerate(depth, nodes, width)
	if len(docs) > bexDomainCeiling {
		t.Fatalf("enumerated domain %d exceeds ceiling %d; raise bexDomainCeiling deliberately", len(docs), bexDomainCeiling)
	}

	var checkedDocs, backedReads, columnReads int
	for _, shapeTapes := range []bool{false, true} {
		for _, hashKeys := range []bool{false, true} {
			opts := document.IndexOptions{HashKeys: hashKeys}
			for _, doc := range docs {
				b, c := vdCheckDoubleAppend(t, doc, opts, shapeTapes)
				if !shapeTapes && !hashKeys { // count the domain once
					checkedDocs++
					backedReads += b
					columnReads += c
				}
				if t.Failed() {
					return
				}
			}
		}
	}

	// A corpus-wide pass: every document once, then every document again, into a
	// single segment. The second pass makes each document's spans repeats of the
	// first, so cross-document interning — the dictionary's whole point — backs
	// every value, over one large splice slab shared across the corpus.
	corpusBacked := vdCheckCorpus(t, docs)
	if t.Failed() {
		return
	}

	t.Logf("value-dict bound depth<=%d nodes<=%d width<=%d: %d documents enumerated (x4 for ShapeTapes/HashKeys)",
		depth, nodes, width, len(docs))
	t.Logf("dictionary read path exercised: %d dictionary-backed node reads, %d columnar reads over the per-document sets", backedReads, columnReads)
	t.Logf("corpus-wide pass: %d documents, %d dictionary-backed node reads across the shared splice slab", 2*len(docs), corpusBacked)
	if backedReads == 0 || corpusBacked == 0 {
		t.Fatalf("no dictionary-backed read was exercised; the differential proved nothing")
	}
}

// vdCheckDoubleAppend builds a ValueDict segment holding one document twice — so the
// second copy's every value is a repeat of the first and therefore dictionary-
// backed at floor one — and checks both stored copies against the classic tape
// and the AST through every read. It returns the number of dictionary-backed
// node reads and columnar reads it exercised.
func vdCheckDoubleAppend(t *testing.T, doc *exhaustiveValue, opts document.IndexOptions, shapeTapes bool) (backed, columns int) {
	t.Helper()
	src := doc.json

	seg := &Segment{Options: opts, ShapeTapes: shapeTapes, ValueDict: true, valueFloor: 1}
	if _, err := seg.Append(src); err != nil {
		t.Fatalf("Append(%s): %v", src, err)
	}
	if _, err := seg.Append(src); err != nil {
		t.Fatalf("Append(%s): %v", src, err)
	}
	for ord := 0; ord < seg.Len(); ord++ {
		backed += vdCheckNode(t, seg, ord, seg.Doc(ord).Root(), doc, string(src))
	}
	columns += vdCheckColumns(t, seg, doc, src)
	return backed, columns
}

// vdCheckCorpus builds every document into one segment twice over and verifies each
// stored ordinal reads back its AST through the dictionary, exercising the
// cross-document interning path and a splice slab spanning the whole corpus. It
// returns the number of dictionary-backed node reads.
func vdCheckCorpus(t *testing.T, docs []*exhaustiveValue) (backed int) {
	t.Helper()
	seg := &Segment{ValueDict: true, valueFloor: 1}
	for range 2 {
		for _, doc := range docs {
			if _, err := seg.Append(doc.json); err != nil {
				t.Fatalf("corpus Append(%s): %v", doc.json, err)
			}
		}
	}
	for ord := 0; ord < seg.Len(); ord++ {
		doc := docs[ord%len(docs)]
		backed += vdCheckNode(t, seg, ord, seg.Doc(ord).Root(), doc, string(doc.json))
		if t.Failed() {
			return backed
		}
	}
	return backed
}

// vdCheckNode asserts the value at node reads identically through the dictionary
// handle (Segment.DocValue), the classic source-tape node, and the AST, then
// recurses into a container's members over the source tape — which the
// dictionary never alters, so structural navigation stays classic. It returns
// the count of dictionary-backed reads in the subtree, and pins that a backed
// read rebases off the shared arena rather than the document source.
func vdCheckNode(t *testing.T, seg *Segment, ord int, node vibejson.Node, a *exhaustiveValue, path string) (backed int) {
	t.Helper()
	mv := seg.DocValue(ord, node)

	if _, ok := seg.valueSpliceAt(ord, node.Entry.Start); ok {
		backed = 1
		// A backed handle must read the interned arena span, not the document
		// source it stands in for; identical bytes, different backing.
		if unsafe.SliceData(mv.Raw().Bytes()) == unsafe.SliceData(node.Raw().Bytes()) {
			t.Fatalf("%s: dictionary-backed value did not rebase off the source arena", path)
		}
	}

	if mv.Kind() != a.kind || node.Kind() != a.kind {
		t.Fatalf("%s: Kind handle=%v node=%v, want %v", path, mv.Kind(), node.Kind(), a.kind)
	}
	if !bytes.Equal(mv.Raw().Bytes(), a.json) || !bytes.Equal(node.Raw().Bytes(), a.json) {
		t.Fatalf("%s: Raw handle=%q node=%q, want %q", path, mv.Raw().Bytes(), node.Raw().Bytes(), a.json)
	}

	switch a.kind {
	case document.Null:
		if !mv.IsNull() || !node.IsNull() {
			t.Fatalf("%s: IsNull handle=%v node=%v, want true", path, mv.IsNull(), node.IsNull())
		}
	case document.Bool:
		vdEqualBool(t, mv, node, a, path)
	case document.Number:
		vdEqualNumber(t, mv, node, a, path)
	case document.String:
		vdEqualString(t, mv, node, a, path)
	case document.Array:
		if l, ok := mv.ArrayLen(); !ok || l != len(a.elems) {
			t.Fatalf("%s: handle ArrayLen=(%d,%v), want %d", path, l, ok, len(a.elems))
		}
		for i, e := range a.elems {
			child, ok := node.Index(i)
			if !ok {
				t.Fatalf("%s: Index(%d) absent", path, i)
			}
			backed += vdCheckNode(t, seg, ord, child, e, fmt.Sprintf("%s[%d]", path, i))
		}
	case document.Object:
		if l, ok := mv.ObjectLen(); !ok || l != len(a.keys) {
			t.Fatalf("%s: handle ObjectLen=(%d,%v), want %d", path, l, ok, len(a.keys))
		}
		order, last := bexEffectiveMembers(a)
		for _, k := range order {
			child, ok := node.Get(k)
			if !ok {
				t.Fatalf("%s: Get(%q) absent", path, k)
			}
			backed += vdCheckNode(t, seg, ord, child, last[k], fmt.Sprintf("%s/%s", path, k))
		}
	}
	return backed
}

// vdEqualBool asserts the dictionary handle and the source node decode the same
// boolean, matching the AST.
func vdEqualBool(t *testing.T, mv, node vibejson.Node, a *exhaustiveValue, path string) {
	t.Helper()
	bm, okm := mv.Bool()
	bn, okn := node.Bool()
	if !okm || !okn || bm != a.boolVal || bn != a.boolVal {
		t.Fatalf("%s: Bool handle=(%v,%v) node=(%v,%v), want (%v,true)", path, bm, okm, bn, okn, a.boolVal)
	}
}

// vdEqualNumber asserts every numeric accessor agrees between the dictionary
// handle and the source node, and that the spelling round-trips exactly.
func vdEqualNumber(t *testing.T, mv, node vibejson.Node, a *exhaustiveValue, path string) {
	t.Helper()
	nbm, okm := mv.NumberBytes()
	nbn, okn := node.NumberBytes()
	if !okm || !okn || string(nbm) != a.numRaw || string(nbn) != a.numRaw {
		t.Fatalf("%s: NumberBytes handle=(%q,%v) node=(%q,%v), want %q", path, nbm, okm, nbn, okn, a.numRaw)
	}
	im, iokm := mv.Int64()
	in, iokn := node.Int64()
	if im != in || iokm != iokn {
		t.Fatalf("%s: Int64 handle=(%d,%v) node=(%d,%v)", path, im, iokm, in, iokn)
	}
	um, uokm := mv.Uint64()
	un, uokn := node.Uint64()
	if um != un || uokm != uokn {
		t.Fatalf("%s: Uint64 handle=(%d,%v) node=(%d,%v)", path, um, uokm, un, uokn)
	}
	fm, fokm := mv.Float64()
	fn, fokn := node.Float64()
	if fm != fn || fokm != fokn {
		t.Fatalf("%s: Float64 handle=(%v,%v) node=(%v,%v)", path, fm, fokm, fn, fokn)
	}
}

// vdEqualString asserts the string accessors agree between the dictionary handle
// and the source node — StringBytes declines identically on escaped spellings,
// AppendText decodes to the same content — matching the AST.
func vdEqualString(t *testing.T, mv, node vibejson.Node, a *exhaustiveValue, path string) {
	t.Helper()
	sbm, okm := mv.StringBytes()
	sbn, okn := node.StringBytes()
	if okm != okn || !bytes.Equal(sbm, sbn) {
		t.Fatalf("%s: StringBytes handle=(%q,%v) node=(%q,%v)", path, sbm, okm, sbn, okn)
	}
	if okm && string(sbm) != a.strDec {
		t.Fatalf("%s: StringBytes handle=%q, want %q", path, sbm, a.strDec)
	}
	dm, _ := mv.AppendText(nil)
	dn, _ := node.AppendText(nil)
	if !bytes.Equal(dm, dn) || string(dm) != a.strDec {
		t.Fatalf("%s: AppendText handle=%q node=%q, want %q", path, dm, dn, a.strDec)
	}
}

// vdCheckColumns exercises the columnar read path (Segment.AppendPointer), which
// routes dictionary-backed targets through the arena. Every reachable pointer
// must return the AST-specified bytes for both stored copies of the document. It
// returns the number of columnar values checked.
func vdCheckColumns(t *testing.T, seg *Segment, doc *exhaustiveValue, src []byte) (n int) {
	t.Helper()
	var walk func(a *exhaustiveValue, tokens []string)
	walk = func(a *exhaustiveValue, tokens []string) {
		ptr := bexPointerString(tokens)
		cp, err := vibejson.CompilePointer(ptr)
		if err != nil {
			t.Fatalf("%s: CompilePointer(%q): %v", src, ptr, err)
		}
		got, err := seg.AppendPointer(nil, cp)
		if err != nil {
			t.Fatalf("%s: AppendPointer(%q): %v", src, ptr, err)
		}
		if len(got) != seg.Len() {
			t.Fatalf("%s: AppendPointer(%q) returned %d values, want %d", src, ptr, len(got), seg.Len())
		}
		for i := range got {
			if g := got[i].Bytes(); !bytes.Equal(g, a.json) {
				t.Fatalf("%s: AppendPointer(%q)[%d] = %q, want %q", src, ptr, i, g, a.json)
			}
			n++
		}
		switch a.kind {
		case document.Array:
			for i, e := range a.elems {
				walk(e, append(tokens[:len(tokens):len(tokens)], fmt.Sprintf("%d", i)))
			}
		case document.Object:
			order, last := bexEffectiveMembers(a)
			for _, k := range order {
				walk(last[k], append(tokens[:len(tokens):len(tokens)], k))
			}
		}
	}
	walk(doc, nil)
	return n
}

// TestValueDictSightingEconomics pins the interning economics as a readable spec
// independent of the enumeration: a value seen once stays inline, its second
// sighting interns it, a span below the floor never interns however often it
// recurs, and the accounting reports what was deduplicated. It also confirms a
// value-dictionary read is byte-identical to a classic read of the same segment.
func TestValueDictSightingEconomics(t *testing.T) {
	// "venue" recurs across documents; "id" numbers are short and unique. With
	// the default floor the venue string interns on its second sighting and the
	// id numbers never do.
	docs := []string{
		`{"id":1,"venue":"PLEYEL_PLEYEL_HALL_A"}`,
		`{"id":2,"venue":"PLEYEL_PLEYEL_HALL_A"}`,
		`{"id":3,"venue":"PLEYEL_PLEYEL_HALL_A"}`,
	}

	var classic Segment
	seg := &Segment{ValueDict: true}
	for _, d := range docs {
		if _, err := classic.Append([]byte(d)); err != nil {
			t.Fatal(err)
		}
		if _, err := seg.Append([]byte(d)); err != nil {
			t.Fatal(err)
		}
	}

	st := seg.Stats()
	// One distinct interned value (the venue), spliced on the two later
	// documents; the first sighting stayed inline.
	if st.DictValues != 1 {
		t.Fatalf("DictValues = %d, want 1 (the venue string)", st.DictValues)
	}
	if st.DictSplices != 2 {
		t.Fatalf("DictSplices = %d, want 2 (the second and third sightings)", st.DictSplices)
	}
	venueBytes := int64(len(`"PLEYEL_PLEYEL_HALL_A"`))
	if st.DictBytes != venueBytes {
		t.Fatalf("DictBytes = %d, want %d", st.DictBytes, venueBytes)
	}
	if st.DictSplicedBytes != 2*venueBytes {
		t.Fatalf("DictSplicedBytes = %d, want %d", st.DictSplicedBytes, 2*venueBytes)
	}
	if st.DictSavedBytes != 2*venueBytes-2*valueDictRefBytes-venueBytes {
		t.Fatalf("DictSavedBytes = %d, want %d", st.DictSavedBytes, 2*venueBytes-2*valueDictRefBytes-venueBytes)
	}

	// Every venue reads back byte-identically to the classic segment, whether it was
	// spliced (documents two and three) or inline (document one).
	venue := vibejson.MustCompilePointer("/venue")
	classicCol, err := classic.AppendPointer(nil, venue)
	if err != nil {
		t.Fatal(err)
	}
	dictCol, err := seg.AppendPointer(nil, venue)
	if err != nil {
		t.Fatal(err)
	}
	for i := range classicCol {
		if !bytes.Equal(classicCol[i].Bytes(), dictCol[i].Bytes()) {
			t.Fatalf("venue[%d]: dict %q != classic %q", i, dictCol[i].Bytes(), classicCol[i].Bytes())
		}
	}
	// The spliced reads must alias the shared arena, proving they resolved
	// through the dictionary rather than the source.
	if unsafe.SliceData(dictCol[1].Bytes()) == unsafe.SliceData(classicCol[1].Bytes()) {
		t.Fatalf("spliced venue did not resolve through the arena")
	}
}

// TestValueDictFloorKeepsShortInline confirms the length floor: a short value
// that recurs endlessly is never interned, because its reference would not
// out-save its bytes.
func TestValueDictFloorKeepsShortInline(t *testing.T) {
	seg := &Segment{ValueDict: true} // default floor (valueDictMinSpan)
	for range 8 {
		if _, err := seg.Append([]byte(`{"a":1,"b":1,"c":1}`)); err != nil {
			t.Fatal(err)
		}
	}
	if st := seg.Stats(); st.DictValues != 0 || st.DictSplices != 0 {
		t.Fatalf("short values interned under the floor: DictValues=%d DictSplices=%d", st.DictValues, st.DictSplices)
	}
}

// TestValueDictComposesWithShapeTapes checks that the two dedup levers compose:
// a shape-taped segment with the dictionary on stores its documents in shape form
// (the space win preserved, no widening at ingest) while its repeated member
// values still intern and read back byte-identically.
func TestValueDictComposesWithShapeTapes(t *testing.T) {
	doc := []byte(`{"venue":"GRAND_AUDITORIUM_MAIN","status":"AVAILABLE_NOW"}`)
	seg := &Segment{ShapeTapes: true, ValueDict: true, valueFloor: 1}
	for range 4 {
		if _, err := seg.Append(doc); err != nil {
			t.Fatal(err)
		}
	}
	st := seg.Stats()
	if st.ShapeTaped == 0 {
		t.Fatalf("no document shape-taped; the compose case is not exercised")
	}
	if st.Widened != 0 {
		t.Fatalf("ingest widened %d documents; the shape space win was re-bought", st.Widened)
	}
	if st.DictSplices == 0 {
		t.Fatalf("no value interned under the composed modes")
	}
	// Every stored copy reads back the source document exactly.
	for i := 0; i < seg.Len(); i++ {
		for _, key := range []string{"venue", "status"} {
			want, _ := seg.Doc(i).Root().Get(key)
			got := seg.DocValue(i, want)
			if !bytes.Equal(got.Raw().Bytes(), want.Raw().Bytes()) {
				t.Fatalf("doc %d key %q: dict %q != source %q", i, key, got.Raw().Bytes(), want.Raw().Bytes())
			}
		}
	}
}

// TestGCCorruptionValueDict is the standing corruption gate for the value
// dictionary, whose reads rebase a Node onto the shared interner arena and parse
// its bytes through the tape kernels. Concurrent workers build dictionary-backed
// sets and materialize every value under forced stack movement and GC while
// retaining earlier sets, proving arena handles never dangle and dictionary
// reads stay byte-stable. The interned spans carry no trailing padding, so a
// kernel that over-read a value's end would corrupt here. Stress:
//
//	GOGC=1 GOEXPERIMENT=simd gotip test -run TestGCCorruptionValueDict -count=5 -cpu=1,4,8 ./
func TestGCCorruptionValueDict(t *testing.T) {
	docs := valueDictEnumCorpus(24)
	pointer := vibejson.MustCompilePointer("/area")

	// The single-threaded expectation every worker must reproduce: the arena
	// bytes of each document's interned sub-object, resolved through the
	// dictionary.
	want := valueDictReference(t, docs, pointer)

	workers := runtime.GOMAXPROCS(0) * 2
	const iters = 20
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			var retained []*Segment
			values := make([]vibejson.RawValue, 0, len(want))
			for it := range iters {
				forceStackMovement(51+id, it)
				seg := &Segment{ValueDict: true, valueFloor: 1}
				for _, doc := range docs {
					if _, err := seg.Append(doc); err != nil {
						errs <- fmt.Errorf("worker %d iter %d: Append: %v", id, it, err)
						return
					}
				}
				var err error
				values, err = seg.AppendPointer(values[:0], pointer)
				if err != nil {
					errs <- fmt.Errorf("worker %d iter %d: AppendPointer: %v", id, it, err)
					return
				}
				if len(values) != len(want) {
					errs <- fmt.Errorf("worker %d iter %d: %d values, want %d", id, it, len(values), len(want))
					return
				}
				for i := range values {
					if !bytes.Equal(values[i].Bytes(), want[i]) {
						errs <- fmt.Errorf("worker %d iter %d: value %d = %q, want %q", id, it, i, values[i].Bytes(), want[i])
						return
					}
				}
				// Retain the segment so its arena stays live across later GCs; a
				// dangling arena handle would surface as a later mismatch.
				retained = append(retained, seg)
			}
			runtime.KeepAlive(retained)
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// valueDictReference resolves pointer across a dictionary-backed segment built once,
// returning the byte contents each worker must reproduce. It also asserts the
// pointer actually resolved through the dictionary, so the corruption gate is
// reading arena bytes rather than source.
func valueDictReference(t *testing.T, docs [][]byte, pointer vibejson.CompiledPointer) [][]byte {
	t.Helper()
	seg := &Segment{ValueDict: true, valueFloor: 1}
	for _, doc := range docs {
		if _, err := seg.Append(doc); err != nil {
			t.Fatal(err)
		}
	}
	values, err := seg.AppendPointer(nil, pointer)
	if err != nil {
		t.Fatal(err)
	}
	if st := seg.Stats(); st.DictSplices == 0 {
		t.Fatal("reference seg interned nothing; the corruption gate would read only source")
	}
	want := make([][]byte, len(values))
	for i, v := range values {
		want[i] = append([]byte(nil), v.Bytes()...)
	}
	return want
}

// valueDictEnumCorpus builds an enum-heavy record corpus: each document repeats
// a venue, a category, a status, and an area sub-object drawn from small fixed
// sets, so those spans recur across documents and intern, while the id numbers
// stay unique and inline. It is the shape real repeat-heavy corpora take.
func valueDictEnumCorpus(n int) [][]byte {
	venues := []string{"PLEYEL_PLEYEL_HALL", "SALLE_GAVEAU_MAIN", "OLYMPIA_GRAND_HALL"}
	categories := []string{"CATEGORY_PREMIUM", "CATEGORY_STANDARD", "CATEGORY_ECONOMY"}
	statuses := []string{"AVAILABLE_FOR_SALE", "SOLD_OUT_NOW"}
	docs := make([][]byte, 0, n)
	for i := range n {
		doc := fmt.Sprintf(
			`{"id":%d,"venue":"%s","category":"%s","area":{"areaId":205705999,"blockIds":[]},"status":"%s"}`,
			i, venues[i%len(venues)], categories[(i/3)%len(categories)], statuses[i%len(statuses)],
		)
		docs = append(docs, []byte(doc))
	}
	return docs
}
