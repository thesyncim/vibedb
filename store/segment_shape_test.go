package store

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// Shape-deduplicated tapes change storage, never semantics: a ShapeTapes segment
// must be observationally identical to a classic segment of the same documents
// through every public read — the fused extractors, the batch pointer walk,
// and Doc — while actually storing conforming documents as bare value
// arrays. These tests hold the mode to that contract three ways. The twin
// gate compares every extractor against a classic twin segment without ever
// widening, so the value-array reads are proven self-sufficient. The
// reference gate reruns the shape_column differential battery — every
// routing transition it was engineered to force — on shape-taped sets, where
// the per-document reference itself runs on widened tapes. And the Doc gate
// requires each widened tape to be entry-for-entry identical to the classic
// twin's, with stable storage across calls and appends. Conformance gating
// is pinned by Stats: exactly the documents the sighting economics and the
// duplicate/flat/empty rules admit are shape-taped, and everything else
// remains classic. The standing GOGC gate stresses the value-array reads and
// the widening scan under forced stack movement and collection.

// buildShapeTapeSet indexes docs into one ShapeTapes Segment under the given
// enrichment.
func buildShapeTapeSet(docs []string, hashKeys bool) (*Segment, error) {
	seg := &Segment{
		Options:    document.IndexOptions{HashKeys: hashKeys},
		ShapeTapes: true,
	}
	for _, doc := range docs {
		if _, err := seg.Append([]byte(doc)); err != nil {
			return nil, fmt.Errorf("Append(%.60q): %v", doc, err)
		}
	}
	return seg, nil
}

// shapeTapeSegment is buildShapeTapeSet failing the test on error.
func shapeTapeSegment(t testing.TB, docs []string, hashKeys bool) *Segment {
	t.Helper()
	seg, err := buildShapeTapeSet(docs, hashKeys)
	if err != nil {
		t.Fatal(err)
	}
	return seg
}

// shapeTapeClusteredDocs returns a deterministic shape-clustered corpus:
// shapes flat layouts of width fields cycling round-robin over count
// documents, values spanning every kind that keeps a root flat — numbers
// plain and fractional, strings, booleans, null, and both empty containers.
func shapeTapeClusteredDocs(count, shapes, width int) []string {
	docs := make([]string, 0, count)
	var b strings.Builder
	for i := range count {
		b.Reset()
		prefix := fmt.Sprintf("s%02d", i%shapes)
		b.WriteByte('{')
		for f := range width {
			if f > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `"%s_f%02d":`, prefix, f)
			switch f % 7 {
			case 0:
				fmt.Fprintf(&b, "%d", i*31+f)
			case 1:
				fmt.Fprintf(&b, "%d.%02d", i, f)
			case 2:
				fmt.Fprintf(&b, `"v-%d-%d"`, i, f)
			case 3:
				fmt.Fprintf(&b, "%t", (i+f)%2 == 0)
			case 4:
				b.WriteString("null")
			case 5:
				b.WriteString("[]")
			default:
				b.WriteString("{}")
			}
		}
		b.WriteByte('}')
		docs = append(docs, b.String())
	}
	return docs
}

// shapeTapeCorpora extends the shape_column battery with the corpora this
// mode's ingest gate must judge: clustered conformers, escaped and unicode
// key spellings, whitespace-padded documents, empty objects, root
// duplicates, and conformers interleaved with every kind of non-conformer.
func shapeTapeCorpora() map[string][]string {
	corpora := shapeColumnCorpora()

	corpora["clustered"] = shapeTapeClusteredDocs(60, 3, 9)

	// One recurring layout whose spellings need decoding: escaped, unicode,
	// and pointer-metacharacter keys, so the compiled shape carries raw
	// spellings that differ from the queried names.
	var escaped []string
	for i := range 24 {
		escaped = append(escaped, fmt.Sprintf(
			`{"plain":%d,"tab\tkey":%d,"été":"%d","a/b":true,"":null}`, i, i*3, i))
	}
	corpora["escapedkeys"] = escaped

	// Conforming layouts wrapped in insignificant whitespace, inside and
	// around the object, so the widening scan must walk real gaps between
	// keys, colons, and values.
	var padded []string
	for i := range 16 {
		padded = append(padded, fmt.Sprintf(
			"  {\n\t\"a\" : %d ,\n\t\"b\"\t:\t\"x-%d\" ,\n\t\"c\" : [] \n}  ", i, i))
	}
	corpora["whitespace"] = padded

	// Empty objects between conformers: {} is flat but has nothing to
	// deduplicate and must stay classic without polluting the cache.
	var empties []string
	for i := range 18 {
		if i%3 == 0 {
			empties = append(empties, `{}`)
		} else {
			empties = append(empties, fmt.Sprintf(`{"a":%d,"b":%d}`, i, i*2))
		}
	}
	corpora["emptyobj"] = empties

	// Root duplicates recurring often enough to compile their shape: the
	// dupKeys gate must keep every one classic, and lookups keep the
	// last-duplicate rule.
	var dups []string
	for i := range 12 {
		dups = append(dups, fmt.Sprintf(`{"k":%d,"other":true,"k":%d}`, i, i+100))
	}
	corpora["rootdup"] = dups

	// One shape whose root span straddles the narrow bound exactly: the
	// same key sequence stored at both entry widths within one segment, with
	// adversarial spans at 65535 (the last narrow root) and 65536 (the
	// first wide one).
	var boundary []string
	for i := range 3 {
		for _, end := range []int{64, 1 << 10, ShapeNarrowMaxEnd - 1, ShapeNarrowMaxEnd,
			ShapeNarrowMaxEnd + 1, ShapeNarrowMaxEnd + 2} {
			boundary = append(boundary, shapeTapeBoundaryDoc(end, 0, i))
		}
	}
	corpora["boundary"] = boundary

	// A width-mixed stream of one recurring shape alternating small and
	// huge documents, with escaped string values and empty containers in
	// both widths, so narrow packing must preserve the escaped flag and the
	// empty-container kinds and the extractors must switch widths per
	// document.
	var widthmix []string
	for i := range 12 {
		pad := 40
		if i%2 == 1 {
			pad = 70_000
		}
		widthmix = append(widthmix, fmt.Sprintf(
			`{"esc":"a\tb-%02d","num":%d,"pad":"%s","mt":[],"obj":{}}`,
			i, i*7, strings.Repeat("x", pad)))
	}
	corpora["widthmix"] = widthmix

	return corpora
}

// shapeTapeBoundaryDoc returns a conforming two-member document whose root
// object ends exactly at source offset rootEnd, after lead spaces of leading
// whitespace: a filler string sized to hit the offset and a fixed-width
// integer (kept in [1000, 9999] so no spelling grows a leading zero), so one
// key sequence recurs at every width the boundary battery needs.
// rootEnd - lead must cover the 19 fixed syntax bytes.
func shapeTapeBoundaryDoc(rootEnd, lead, v int) string {
	const fixed = len(`{"pad":"","v":1000}`)
	return strings.Repeat(" ", lead) +
		fmt.Sprintf(`{"pad":"%s","v":%d}`, strings.Repeat("p", rootEnd-lead-fixed), 1000+v%9000)
}

// shapeTapePointers returns the compiled-pointer battery: the root pointer,
// present and absent members, escaped spellings in both the document and the
// pointer, numeric-key steps, and multi-token descents into flat values —
// empty containers, scalars, and the tokens that must error on an array
// target.
func shapeTapePointers() []string {
	return []string{
		"", "/a", "/b", "/c", "/absent", "/0", "/plain", "/a~1b", "/été",
		"/tab\tkey", "/s00_f00", "/s00_f05", "/s01_f06", "/id", "/ts", "/name",
		"/a/x", "/c/0", "/c/-", "/nested/deep", "/k", "/other",
	}
}

// checkShapeTapeTwin compares every extractor on a shape-taped segment against
// its classic twin by bytes, without ever calling Doc on the taped segment, so
// the value arrays are proven to answer alone. It returns with the taped
// segment still unwidened.
func checkShapeTapeTwin(t *testing.T, classic, taped *Segment, queries []string, label string) {
	t.Helper()
	var classicCache, tapedCache ShapeCache
	for _, q := range queries {
		want := classicCache.AppendField(nil, classic, q)
		got := tapedCache.AppendField(nil, taped, q)
		for i := range want {
			if !bytes.Equal(got[i].Bytes(), want[i].Bytes()) {
				t.Fatalf("%s: AppendField(%q)[%d] = %q, classic %q",
					label, q, i, got[i].Bytes(), want[i].Bytes())
			}
		}
		wi, wiOK := refInt64Cells(&classicCache, classic, q)
		gi, giOK := tapedCache.AppendFieldInt64(nil, nil, taped, q)
		for i := range wi {
			if gi[i] != wi[i] || giOK[i] != wiOK[i] {
				t.Fatalf("%s: AppendFieldInt64(%q)[%d] = (%d,%v), classic (%d,%v)",
					label, q, i, gi[i], giOK[i], wi[i], wiOK[i])
			}
		}
		wf, wfOK := classicCache.AppendFieldFloat64(nil, nil, classic, q)
		gf, gfOK := tapedCache.AppendFieldFloat64(nil, nil, taped, q)
		for i := range wf {
			if math.Float64bits(gf[i]) != math.Float64bits(wf[i]) || gfOK[i] != wfOK[i] {
				t.Fatalf("%s: AppendFieldFloat64(%q)[%d] = (%v,%v), classic (%v,%v)",
					label, q, i, gf[i], gfOK[i], wf[i], wfOK[i])
			}
		}
		wb, wbOK := classicCache.AppendFieldBool(nil, nil, classic, q)
		gb, gbOK := tapedCache.AppendFieldBool(nil, nil, taped, q)
		for i := range wb {
			if gb[i] != wb[i] || gbOK[i] != wbOK[i] {
				t.Fatalf("%s: AppendFieldBool(%q)[%d] = (%v,%v), classic (%v,%v)",
					label, q, i, gb[i], gbOK[i], wb[i], wbOK[i])
			}
		}
	}
	wantCols := classicCache.AppendFields(nil, classic, queries...)
	gotCols := tapedCache.AppendFields(nil, taped, queries...)
	for j := range wantCols {
		for i := range wantCols[j] {
			if !bytes.Equal(gotCols[j][i].Bytes(), wantCols[j][i].Bytes()) {
				t.Fatalf("%s: AppendFields[%q][%d] = %q, classic %q",
					label, queries[j], i, gotCols[j][i].Bytes(), wantCols[j][i].Bytes())
			}
		}
	}
	for _, p := range shapeTapePointers() {
		compiled, err := vibejson.CompilePointer(p)
		if err != nil {
			t.Fatalf("%s: CompilePointer(%q): %v", label, p, err)
		}
		want, wantErr := classic.AppendPointer(nil, compiled)
		got, gotErr := taped.AppendPointer(nil, compiled)
		if (wantErr == nil) != (gotErr == nil) {
			t.Fatalf("%s: AppendPointer(%q) error = %v, classic %v", label, p, gotErr, wantErr)
		}
		if len(got) != len(want) {
			t.Fatalf("%s: AppendPointer(%q) appended %d, classic %d", label, p, len(got), len(want))
		}
		for i := range want {
			if !bytes.Equal(got[i].Bytes(), want[i].Bytes()) {
				t.Fatalf("%s: AppendPointer(%q)[%d] = %q, classic %q",
					label, p, i, got[i].Bytes(), want[i].Bytes())
			}
		}
	}
}

// refInt64Cells is refFieldInt64 through the classic segment's own fused driver,
// so twin comparisons exercise both sets' drivers rather than one driver
// against Doc.
func refInt64Cells(cache *ShapeCache, s *Segment, name string) ([]int64, []bool) {
	return cache.AppendFieldInt64(nil, nil, s, name)
}

// TestSegmentShapeTapesTwinDifferential is the storage-independence gate:
// over every corpus and enrichment, each fused extractor and the batch
// pointer walk return byte-identical results on classic and shape-taped
// twins, with the taped segment never widened — proven by its Widened count
// staying zero through the whole battery.
func TestSegmentShapeTapesTwinDifferential(t *testing.T) {
	for label, docs := range shapeTapeCorpora() {
		for _, hashKeys := range []bool{false, true} {
			classic := shapeColumnSegment(t, docs, hashKeys)
			taped := shapeTapeSegment(t, docs, hashKeys)
			queries := shapeColumnQueries(classic)
			queries = append(queries, "absent", "")
			checkShapeTapeTwin(t, classic, taped, queries,
				fmt.Sprintf("%s hashKeys=%v", label, hashKeys))
			if w := taped.Stats().Widened; w != 0 {
				t.Fatalf("%s hashKeys=%v: extraction widened %d documents", label, hashKeys, w)
			}
		}
	}
}

// TestSegmentShapeTapesReferenceDifferential reruns the shape_column
// reference battery on shape-taped sets: cold and warm fused passes must
// equal the per-document root Get, whose documents come from widened tapes —
// so the two storage forms are checked against each other inside one segment.
func TestSegmentShapeTapesReferenceDifferential(t *testing.T) {
	for label, docs := range shapeTapeCorpora() {
		for _, hashKeys := range []bool{false, true} {
			seg := shapeTapeSegment(t, docs, hashKeys)
			var cache ShapeCache
			for _, q := range shapeColumnQueries(seg) {
				checkAppendField(t, &cache, seg, q,
					fmt.Sprintf("taped %s hashKeys=%v", label, hashKeys))
				checkTypedField(t, &cache, seg, q,
					fmt.Sprintf("taped %s hashKeys=%v", label, hashKeys))
			}
		}
	}
}

// TestSegmentShapeTapesDocContract pins Doc's widening: every widened tape is
// entry-for-entry and source-identical to the classic twin's tape, repeated
// calls return the same storage, and handles taken before later appends
// survive them.
func TestSegmentShapeTapesDocContract(t *testing.T) {
	docs := shapeTapeClusteredDocs(30, 3, 9)
	docs = append(docs, `  {"a":1, "esc\t":"x", "b":[]}  `, `{}`, `[1,2]`, `"scalar"`)
	for _, hashKeys := range []bool{false, true} {
		classic := shapeColumnSegment(t, docs, hashKeys)
		taped := shapeTapeSegment(t, docs, hashKeys)
		if taped.Stats().ShapeTaped == 0 {
			t.Fatal("corpus produced no shape-taped documents")
		}
		for i := 0; i < taped.Len(); i++ {
			want := classic.Doc(i)
			got := taped.Doc(i)
			if !bytes.Equal(got.Src, want.Src) {
				t.Fatalf("hashKeys=%v: Doc(%d) src %q, classic %q", hashKeys, i, got.Src, want.Src)
			}
			if !reflect.DeepEqual(got.Entries, want.Entries) {
				t.Fatalf("hashKeys=%v: Doc(%d) entries diverge from classic tape\n got %v\nwant %v",
					hashKeys, i, got.Entries, want.Entries)
			}
			again := taped.Doc(i)
			if &again.Entries[0] != &got.Entries[0] {
				t.Fatalf("hashKeys=%v: Doc(%d) returned fresh storage on the second call", hashKeys, i)
			}
		}

		// Handles survive later appends: widened tapes, extractor results,
		// and value nodes must be unaffected by segment growth.
		heldDoc := taped.Doc(2)
		var cache ShapeCache
		heldCol := cache.AppendField(nil, taped, "s02_f02")
		for i := range 40 {
			if _, err := taped.Append([]byte(docs[i%len(docs)])); err != nil {
				t.Fatal(err)
			}
		}
		after := taped.Doc(2)
		if &after.Entries[0] != &heldDoc.Entries[0] {
			t.Fatalf("hashKeys=%v: Doc(2) storage moved across appends", hashKeys)
		}
		afterCol := cache.AppendField(nil, taped, "s02_f02")
		for i := range heldCol {
			if !sameRawValue(heldCol[i], afterCol[i]) {
				t.Fatalf("hashKeys=%v: held column value %d diverged after appends", hashKeys, i)
			}
		}
	}
}

// TestSegmentShapeTapesStats pins the conformance gate's accounting: under
// the sighting economics exactly one document per recurring shape stays
// classic, small conformers take the narrow width, and the duplicate, empty,
// and non-flat rules keep their documents classic entirely.
func TestSegmentShapeTapesStats(t *testing.T) {
	const count, shapes, width = 60, 3, 9
	seg := shapeTapeSegment(t, shapeTapeClusteredDocs(count, shapes, width), true)
	st := seg.Stats()
	if st.Docs != count || st.Shapes != shapes {
		t.Fatalf("Stats = %+v, want %d docs over %d shapes", st, count, shapes)
	}
	if want := count - shapes; st.ShapeTaped != want {
		t.Fatalf("ShapeTaped = %d, want %d (one classic first sighting per shape)", st.ShapeTaped, want)
	}
	// Every clustered document is far under the narrow bound, so all dedup
	// storage is 8-byte entries and none is wide.
	if st.NarrowTaped != st.ShapeTaped || st.ValueEntries != 0 {
		t.Fatalf("Stats = %+v, want every shape-taped document narrow", st)
	}
	if want := int64(st.ShapeTaped) * int64(width); st.NarrowValueEntries != want {
		t.Fatalf("NarrowValueEntries = %d, want %d", st.NarrowValueEntries, want)
	}
	if want := int64(shapes) * int64(2*width+1); st.TapeEntries != want {
		t.Fatalf("TapeEntries = %d, want %d", st.TapeEntries, want)
	}
	if st.Widened != 0 {
		t.Fatalf("Widened = %d before any Doc call", st.Widened)
	}

	// The width seam forces the wide form on the same documents so differential
	// tests can pin its effect.
	wide := &Segment{
		Options:        document.IndexOptions{HashKeys: true},
		ShapeTapes:     true,
		wideValueTapes: true,
	}
	for _, doc := range shapeTapeClusteredDocs(count, shapes, width) {
		if _, err := wide.Append([]byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	if ws := wide.Stats(); ws.NarrowTaped != 0 || ws.NarrowValueEntries != 0 ||
		ws.ShapeTaped != st.ShapeTaped || ws.ValueEntries != st.NarrowValueEntries {
		t.Fatalf("wide-seam Stats = %+v, want the narrow accounting (%+v) moved to ValueEntries", ws, st)
	}

	for label, docs := range map[string][]string{
		"rootdup": {`{"k":1,"k":2}`, `{"k":3,"k":4}`, `{"k":5,"k":6}`},
		"empty":   {`{}`, `{}`, `{}`},
		"nonflat": {`{"a":{"b":1}}`, `{"a":{"b":2}}`, `{"a":[1]}`},
		"nonobj":  {`[1]`, `"s"`, `42`, `null`, `true`},
	} {
		seg := shapeTapeSegment(t, docs, true)
		if st := seg.Stats(); st.ShapeTaped != 0 {
			t.Fatalf("%s: ShapeTaped = %d, want 0", label, st.ShapeTaped)
		}
	}
}

// TestSegmentShapeTapesWidthBoundary pins the narrow-width gate byte by byte:
// the width follows the root object's end offset in document coordinates, so
// 65535 is the last narrow root and 65536 the first wide one, leading
// whitespace that shifts a small object past the bound forces wide, and
// trailing whitespace after the root changes nothing. Each classified
// document must still widen to the classic twin's exact tape.
func TestSegmentShapeTapesWidthBoundary(t *testing.T) {
	docs := []string{
		shapeTapeBoundaryDoc(64, 0, 0), // first sighting: stays classic
		shapeTapeBoundaryDoc(64, 0, 1), // second sighting compiles and dedups
		shapeTapeBoundaryDoc(ShapeNarrowMaxEnd, 0, 2),
		shapeTapeBoundaryDoc(ShapeNarrowMaxEnd+1, 0, 3),
		shapeTapeBoundaryDoc(ShapeNarrowMaxEnd+1, 40, 4),                           // lead pushes a fitting object wide
		shapeTapeBoundaryDoc(ShapeNarrowMaxEnd, 40, 5),                             // lead absorbed inside the bound
		shapeTapeBoundaryDoc(1<<10, 0, 6) + strings.Repeat(" ", ShapeNarrowMaxEnd), // trailing space is past the root
	}
	wantNarrow := []bool{false, true, true, false, false, true, true}
	for _, hashKeys := range []bool{false, true} {
		classic := shapeColumnSegment(t, docs, hashKeys)
		taped := shapeTapeSegment(t, docs, hashKeys)
		for i, want := range wantNarrow {
			r := taped.ShapeTapeRefAt(i)
			if i == 0 {
				if r.Rec != nil {
					t.Fatalf("hashKeys=%v: first sighting was shape-taped", hashKeys)
				}
				continue
			}
			if r.Rec == nil || r.Narrow != want {
				t.Fatalf("hashKeys=%v: doc %d (root end %d) narrow=%v, want %v",
					hashKeys, i, r.End, r.Narrow, want)
			}
		}
		checkShapeTapeTwin(t, classic, taped, []string{"pad", "v", "absent"},
			fmt.Sprintf("boundary hashKeys=%v", hashKeys))
		for i := range docs {
			want, got := classic.Doc(i), taped.Doc(i)
			if !reflect.DeepEqual(got.Entries, want.Entries) {
				t.Fatalf("hashKeys=%v: Doc(%d) diverges from the classic tape", hashKeys, i)
			}
		}
	}
}

// TestSegmentShapeTapesReadFrom holds the stream ingest to Append parity in
// this mode: an NDJSON ReadFrom — including documents large enough for the
// framing slow path and the spill copy — produces the same storage
// classification, the same widened tapes, and the same extraction results as
// Append of the same documents.
func TestSegmentShapeTapesReadFrom(t *testing.T) {
	docs := shapeTapeClusteredDocs(48, 3, 9)
	wide := keyHashWideDoc(1200, "")
	docs = append(docs, wide, wide, wide, `{"a":1,"a":2}`, `[7]`)
	// Wide-width conformers: past the narrow bound, large enough for the
	// framing slow path, and recurring so the stream must classify both
	// entry widths exactly as Append does.
	for i := range 3 {
		docs = append(docs, shapeTapeBoundaryDoc(ShapeNarrowMaxEnd+2, 0, i))
	}
	var stream strings.Builder
	for _, doc := range docs {
		stream.WriteString(doc)
		stream.WriteByte('\n')
	}
	for _, hashKeys := range []bool{false, true} {
		appended := shapeTapeSegment(t, docs, hashKeys)
		streamed := &Segment{
			Options:    document.IndexOptions{HashKeys: hashKeys},
			ShapeTapes: true,
		}
		if _, err := streamed.ReadFrom(strings.NewReader(stream.String())); err != nil {
			t.Fatal(err)
		}
		if streamed.Len() != appended.Len() {
			t.Fatalf("hashKeys=%v: ReadFrom ingested %d documents, Append %d",
				hashKeys, streamed.Len(), appended.Len())
		}
		sa, sb := appended.Stats(), streamed.Stats()
		sa.Widened, sb.Widened = 0, 0 // storage classification only
		if sa != sb {
			t.Fatalf("hashKeys=%v: stream stats %+v, append stats %+v", hashKeys, sb, sa)
		}
		for i := 0; i < appended.Len(); i++ {
			want, got := appended.Doc(i), streamed.Doc(i)
			if !bytes.Equal(got.Src, want.Src) || !reflect.DeepEqual(got.Entries, want.Entries) {
				t.Fatalf("hashKeys=%v: Doc(%d) diverges between ReadFrom and Append", hashKeys, i)
			}
		}
	}
}

// TestSegmentShapeTapesLateEnable pins the refs alignment when the mode flips
// mid-segment: documents appended before enabling stay classic and aligned, and
// documents after it dedup normally; disabling stops dedup without
// disturbing stored documents.
func TestSegmentShapeTapesLateEnable(t *testing.T) {
	doc := `{"a":1,"b":"x"}`
	var seg Segment
	for range 3 {
		if _, err := seg.Append([]byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	seg.ShapeTapes = true
	for range 4 {
		if _, err := seg.Append([]byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	seg.ShapeTapes = false
	if _, err := seg.Append([]byte(doc)); err != nil {
		t.Fatal(err)
	}
	st := seg.Stats()
	// The layout is first sighted at ordinal 3 and compiled at 4, so
	// ordinals 4-6 dedup; the classic-mode tail ordinal 7 does not.
	if st.Docs != 8 || st.ShapeTaped != 3 {
		t.Fatalf("Stats = %+v, want 8 docs with 3 shape-taped", st)
	}
	var cache ShapeCache
	col := cache.AppendField(nil, &seg, "b")
	for i := range col {
		if string(col[i].Bytes()) != `"x"` {
			t.Fatalf("AppendField[%d] = %q after mode flips", i, col[i].Bytes())
		}
	}
}

// TestSegmentShapeTapesConcurrentDoc drives concurrent first-access widening
// of every document once appending stops: all goroutines must observe
// identical, stable tapes. The race detector owns the locking proof.
func TestSegmentShapeTapesConcurrentDoc(t *testing.T) {
	docs := shapeTapeClusteredDocs(40, 2, 8)
	seg := shapeTapeSegment(t, docs, true)
	classic := shapeColumnSegment(t, docs, true)
	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := range workers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < seg.Len(); i++ {
				got := seg.Doc((i + w) % seg.Len())
				want := classic.Doc((i + w) % seg.Len())
				if !reflect.DeepEqual(got.Entries, want.Entries) {
					errs <- fmt.Errorf("worker %d: Doc(%d) diverges", w, (i+w)%seg.Len())
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// TestSegmentShapeTapesSteadyAllocs proves every fused pass over a
// shape-taped segment allocates nothing once dst has capacity. The corpus mixes
// both entry widths so the narrow paths' stack-widened entries are proven
// not to escape — the accessor fallbacks (a fractional number for the int
// driver, the float and bool accessors, the pointer descent) all take a
// pointer to the reconstituted entry.
func TestSegmentShapeTapesSteadyAllocs(t *testing.T) {
	docs := shapeTapeClusteredDocs(64, 2, 8)
	for i := range 3 {
		docs = append(docs, shapeTapeBoundaryDoc(ShapeNarrowMaxEnd+2, 0, i))
	}
	seg := shapeTapeSegment(t, docs, true)
	var cache ShapeCache
	pointer := vibejson.MustCompilePointer("/s00_f01")
	dst := cache.AppendField(nil, seg, "s00_f02")
	ints, iok := cache.AppendFieldInt64(nil, nil, seg, "s00_f01") // fractional: accessor path
	floats, fok := cache.AppendFieldFloat64(nil, nil, seg, "s00_f01")
	bools, bok := cache.AppendFieldBool(nil, nil, seg, "s00_f03")
	ptrs, err := seg.AppendPointer(nil, pointer)
	if err != nil {
		t.Fatal(err)
	}
	// AppendFields is absent by contract: it allocates its per-name state
	// (compiled keys and inline-cache slots) on every call, independent of
	// the storage form.
	for name, fn := range map[string]func(){
		"AppendField":        func() { dst = cache.AppendField(dst[:0], seg, "s00_f02") },
		"AppendFieldInt64":   func() { ints, iok = cache.AppendFieldInt64(ints[:0], iok[:0], seg, "s00_f01") },
		"AppendFieldFloat64": func() { floats, fok = cache.AppendFieldFloat64(floats[:0], fok[:0], seg, "s00_f01") },
		"AppendFieldBool":    func() { bools, bok = cache.AppendFieldBool(bools[:0], bok[:0], seg, "s00_f03") },
		"AppendPointer":      func() { ptrs, _ = seg.AppendPointer(ptrs[:0], pointer) },
	} {
		if allocs := testing.AllocsPerRun(32, fn); allocs != 0 {
			t.Fatalf("%s on a shape-taped seg allocates %v per run", name, allocs)
		}
	}
}

// TestGCCorruptionShapeTapes is the standing corruption gate for the mode's
// borrowed reads: value-array extraction at both entry widths and the
// widening scan walk arena-backed entries, the narrow slab, and source bytes
// while the collector may move stacks. Concurrent workers rebuild
// shape-taped sets whose entry arenas and narrow slabs end in
// sentinel-poisoned free tails, extract raw and typed columns, widen
// documents under forced stack movement and GC, verify everything against
// the classic reference, and prove the sentinels stay untouched. Stress:
//
//	GOGC=1 GOEXPERIMENT=simd gotip test -run TestGCCorruptionShapeTapes -count=5 -cpu=1,4,8 ./
func TestGCCorruptionShapeTapes(t *testing.T) {
	docs := shapeTapeClusteredDocs(36, 3, 9)
	for i := range 3 { // wide-width conformers among the narrow ones
		docs = append(docs, shapeTapeBoundaryDoc(ShapeNarrowMaxEnd+2, 0, i))
	}
	names := []string{"s00_f00", "s01_f01", "s02_f02", "s00_f03", "pad", "absent"}
	type wantCol struct {
		raws  [][]byte
		ints  []int64
		intOK []bool
	}
	reference := map[string]*wantCol{}
	var refTapes [][]vibejson.IndexEntry
	{
		seg := shapeColumnSegment(t, docs, true)
		var cache ShapeCache
		for _, name := range names {
			w := &wantCol{}
			for _, rv := range cache.AppendField(nil, seg, name) {
				w.raws = append(w.raws, append([]byte(nil), rv.Bytes()...))
			}
			w.ints, w.intOK = refFieldInt64(seg, name)
			reference[name] = w
		}
		for i := 0; i < seg.Len(); i++ {
			refTapes = append(refTapes, seg.Doc(i).Entries)
		}
	}

	sentinel := vibejson.IndexEntry{Start: ^uint32(0), End: ^uint32(0), Next: ^uint32(0), Info: ^uint32(0)}
	workers := runtime.GOMAXPROCS(0) * 2
	const iters = 24
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			var cache ShapeCache
			ints := make([]int64, 0, len(docs))
			valid := make([]bool, 0, len(docs))
			for it := range iters {
				forceStackMovement(48+id, it)
				seg, err := buildShapeTapeSet(docs, true)
				if err != nil {
					errs <- fmt.Errorf("worker %d iter %d: %v", id, it, err)
					return
				}
				tail := seg.entryChunk[len(seg.entryChunk):cap(seg.entryChunk)]
				for i := range tail {
					tail[i] = sentinel
				}
				narrowSentinel := ShapeNarrowValue{Span: ^uint32(0), Info: ^uint32(0)}
				narrowTail := seg.narrow[len(seg.narrow):cap(seg.narrow)]
				for i := range narrowTail {
					narrowTail[i] = narrowSentinel
				}
				for _, name := range names {
					want := reference[name]
					for i, rv := range cache.AppendField(nil, seg, name) {
						if !bytes.Equal(rv.Bytes(), want.raws[i]) {
							errs <- fmt.Errorf("worker %d iter %d: AppendField(%q)[%d] = %q, want %q",
								id, it, name, i, rv.Bytes(), want.raws[i])
							return
						}
					}
					ints, valid = cache.AppendFieldInt64(ints[:0], valid[:0], seg, name)
					for i := range ints {
						if ints[i] != want.ints[i] || valid[i] != want.intOK[i] {
							errs <- fmt.Errorf("worker %d iter %d: AppendFieldInt64(%q)[%d] = (%d,%v), want (%d,%v)",
								id, it, name, i, ints[i], valid[i], want.ints[i], want.intOK[i])
							return
						}
					}
				}
				// Widen a rotating subset under churn and hold the tapes
				// across a collection before verifying them.
				var widened []vibejson.Index
				for k := range 6 {
					widened = append(widened, seg.Doc((it*7+k*5)%seg.Len()))
				}
				runtime.GC()
				for k, tape := range widened {
					i := (it*7 + k*5) % seg.Len()
					if !reflect.DeepEqual(tape.Entries, refTapes[i]) {
						errs <- fmt.Errorf("worker %d iter %d: widened Doc(%d) diverges", id, it, i)
						return
					}
				}
				for i := range tail {
					if tail[i] != sentinel {
						errs <- fmt.Errorf("worker %d iter %d: sentinel %d overwritten", id, it, i)
						return
					}
				}
				for i := range narrowTail {
					if narrowTail[i] != narrowSentinel {
						errs <- fmt.Errorf("worker %d iter %d: narrow sentinel %d overwritten", id, it, i)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// TestStoreShapeTapeHeaderSlabAllocOnlyWhenConforming pins the rule that the
// per-document header slab is storage the corpus has to earn. Conformance
// needs a flat root object, so a corpus whose documents all carry a nested
// value can never produce a single header; reserving the slab anyway made
// ShapeTapes cost 26 B per document more than leaving it off, on exactly the
// corpus where it can return nothing. The flat half is the control: it proves
// the slab still appears the moment a document does conform, so the saving is
// not the mode quietly failing to store headers.
func TestStoreShapeTapeHeaderSlabAllocOnlyWhenConforming(t *testing.T) {
	refsPerChunk := func(t *testing.T, doc func(int) string) (refs, docs int) {
		t.Helper()
		collection := &Collection{Options: Options{ChunkDocuments: 8, ShapeTapes: true}}
		for i := range 64 {
			if _, err := collection.Put(fmt.Sprintf("k%02d", i), []byte(doc(i))); err != nil {
				t.Fatal(err)
			}
		}
		collection.state.Load().Chunks.Each(func(_ uint32, chunk *Chunk) bool {
			refs += len(chunk.Docs.tapeRefs)
			docs += len(chunk.Docs.docs)
			return true
		})
		return refs, docs
	}

	nested := func(i int) string {
		return fmt.Sprintf(`{"id":%d,"meta":{"tier":%d}}`, i, i%4)
	}
	if refs, docs := refsPerChunk(t, nested); refs != 0 || docs != 64 {
		t.Fatalf("nested corpus reserved %d headers over %d documents, want 0", refs, docs)
	}

	flat := func(i int) string {
		return fmt.Sprintf(`{"id":%d,"tier":%d}`, i, i%4)
	}
	refs, docs := refsPerChunk(t, flat)
	if docs != 64 {
		t.Fatalf("flat corpus stored %d documents, want 64", docs)
	}
	if refs == 0 {
		t.Fatal("flat corpus stored no shape-tape headers; the slab must still be reserved when documents conform")
	}
}
