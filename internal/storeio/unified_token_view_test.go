package storeio

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"
)

// unifiedCompetitiveShapeRecords builds n documents in the competitive
// corpus's canonical shape (bench/competitive/corpus.go: 12-14 scalar leaves,
// tags arity 2-4, ~3 skeletons per leaf) already spelled canonically, for the
// token-view and filter benchmarks whose gates were measured on that shape.
func unifiedCompetitiveShapeRecords(n int) []CommonPrimaryLeafRecord {
	countries := []string{"PT", "US", "DE", "FR", "BR", "JP", "IN", "GB"}
	tiers := []string{"free", "pro", "team"}
	notes := []string{
		"steady state, no anomalies observed in the last reporting window",
		"processed by the current pipeline during the maintenance window",
	}
	tags := []string{"alpha", "beta", "gamma", "delta"}
	records := make([]CommonPrimaryLeafRecord, n)
	for i := range n {
		var b bytes.Buffer
		fmt.Fprintf(&b,
			`{"active":%t,"country":"%s","id":%d,"name":"user-%d","note":"%s","profile":{"joined":"20%02d-01-02","region":"eu-west-1","tier":"%s"},"score":%d,"tags":[`,
			i%3 != 0, countries[i%len(countries)], i, i, notes[i%2],
			i%25, tiers[i%3], i%1000)
		for t := range 2 + i%3 {
			if t > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `"%s"`, tags[(i+t)%4])
		}
		b.WriteString(`]}`)
		records[i] = CommonPrimaryLeafRecord{
			Key:   fmt.Appendf(nil, "doc:%08d", i),
			Value: CommonPrimaryLeafValue{Inline: append([]byte(nil), b.Bytes()...)},
		}
	}
	return records
}

// TestUnifiedPathSpanOf pins the render-path resolver on handcrafted
// documents: nested objects, array indexes, escaped keys, duplicate keys,
// container targets, and absences.
func TestUnifiedPathSpanOf(t *testing.T) {
	doc := []byte(`{"a":{"b":[10,{"c":"x"},null]},"a":2,"d e":true,"n":{},"\ttab":7}`)
	cases := []struct {
		path  string
		want  string
		found bool
	}{
		{"/a", `{"b":[10,{"c":"x"},null]}`, true}, // first duplicate wins
		{"/a/b", `[10,{"c":"x"},null]`, true},
		{"/a/b/0", `10`, true},
		{"/a/b/1/c", `"x"`, true},
		{"/a/b/2", `null`, true},
		{"/a/b/3", "", false},
		{"/a/b/01", "", false}, // non-canonical index spelling
		{"/d e", `true`, true},
		{"/n", `{}`, true},
		{"/\ttab", `7`, true}, // decoded-key comparison across escapes
		{"/missing", "", false},
		{"/a/b/0/deep", "", false},
	}
	var r UnifiedHoleResolver
	for _, tc := range cases {
		if err := r.SetPath([]byte(tc.path)); err != nil {
			t.Fatalf("SetPath(%q): %v", tc.path, err)
		}
		start, end, found, err := r.PathSpanOf(doc)
		if err != nil {
			t.Fatalf("PathSpanOf(%q): %v", tc.path, err)
		}
		if found != tc.found {
			t.Fatalf("PathSpanOf(%q) found=%v want %v", tc.path, found, tc.found)
		}
		if found && string(doc[start:end]) != tc.want {
			t.Fatalf("PathSpanOf(%q) = %q want %q", tc.path, doc[start:end], tc.want)
		}
	}
	if err := r.SetPath([]byte("")); err == nil {
		t.Fatal("empty path must be rejected")
	}
	if err := r.SetPath([]byte("/a//b")); err == nil {
		t.Fatal("empty segment must be rejected")
	}
}

// unifiedProbeOracle extracts the path value from a rendered canonical
// document with an independently-seeded resolver, as the differential oracle
// for the token-view fast path.
func unifiedProbeOracle(t *testing.T, r *UnifiedHoleResolver, doc []byte) ([]byte, bool) {
	t.Helper()
	start, end, found, err := r.PathSpanOf(doc)
	if err != nil {
		t.Fatalf("oracle PathSpanOf: %v", err)
	}
	if !found {
		return nil, false
	}
	return doc[start:end], true
}

// TestUnifiedTokenViewDifferential proves the token view end to end against
// the render path: for every row of a mixed leaf (templated, trivial, typed
// tokens, dictionary refs) and every probe path, resolve → RowToken →
// AppendUnifiedRowToken must reproduce exactly the spelling a whole-document
// render plus path walk yields, including absences and container targets.
func TestUnifiedTokenViewDifferential(t *testing.T) {
	records, _ := unifiedTestCorpus(220)
	page, count := encodeUnifiedTestLeaf(t, records)
	view := openUnifiedTestLeaf(t, page)
	paths := []string{
		"/country", "/id", "/active", "/score", "/name", "/note",
		"/profile/tier", "/profile/joined", "/tags/0", "/tags/2",
		"/profile", "/tags", "/aa/1", "/mm", "/zz", "/f", "/absent",
	}
	var fast, oracle UnifiedHoleResolver
	rendered := make([]byte, 0, 1024)
	for _, path := range paths {
		if err := fast.SetPath([]byte(path)); err != nil {
			t.Fatal(err)
		}
		if err := oracle.SetPath([]byte(path)); err != nil {
			t.Fatal(err)
		}
		holeByTemplate := map[int]int{}
		for rank := 0; rank < count; rank++ {
			_, body, overflow, ok := view.RowRawAt(rank)
			if !ok || overflow {
				t.Fatalf("RowRawAt(%d)", rank)
			}
			rendered, ok = view.AppendRowBody(rendered[:0], body)
			if !ok {
				t.Fatalf("render rank %d", rank)
			}
			wantValue, wantFound := unifiedProbeOracle(t, &oracle, rendered)

			template, ok := view.RowTemplate(body)
			if !ok {
				t.Fatalf("RowTemplate rank %d", rank)
			}
			var gotValue []byte
			var gotFound bool
			if template < 0 {
				gotValue, gotFound = unifiedProbeOracle(t, &fast, body[1:])
			} else {
				hole, cached := holeByTemplate[template]
				if !cached {
					hole = fast.Resolve(&view, template)
					holeByTemplate[template] = hole
				}
				switch {
				case hole == UnifiedHoleAbsent:
					gotFound = false
				case hole == UnifiedHoleContainer:
					gotValue, gotFound = unifiedProbeOracle(t, &fast, rendered)
				default:
					tok, tokOK := view.RowToken(body, hole)
					if !tokOK {
						t.Fatalf("RowToken(rank %d, hole %d, path %q)", rank, hole, path)
					}
					gotValue = AppendUnifiedRowToken(nil, tok)
					gotFound = true
				}
			}
			if gotFound != wantFound || !bytes.Equal(gotValue, wantValue) {
				t.Fatalf("path %q rank %d: token view (%q,%v) != oracle (%q,%v)",
					path, rank, gotValue, gotFound, wantValue, wantFound)
			}
		}
	}
}

// TestUnifiedEqFilterLeafDifferential proves the token filter lane against a
// rendered-oracle count for needles that exercise every compare class:
// dictionary hit, non-dictionary literal, bool, canonical int, container
// comparand, and absent path.
func TestUnifiedEqFilterLeafDifferential(t *testing.T) {
	records, _ := unifiedTestCorpus(220)
	page, count := encodeUnifiedTestLeaf(t, records)
	view := openUnifiedTestLeaf(t, page)
	cases := []struct{ path, needle string }{
		{"/country", `"PT"`},
		{"/profile/tier", `"pro"`},
		{"/active", `true`},
		{"/active", `false`},
		{"/id", `7`},
		{"/score", `500`},
		{"/tags/0", `"alpha"`},
		{"/aa", `[true,false,null]`},
		{"/profile", `{"x":1}`},
		{"/absent", `"anything"`},
		{"/name", `"user-9"`},
	}
	rendered := make([]byte, 0, 1024)
	for _, tc := range cases {
		filter, err := NewUnifiedEqFilter([]byte(tc.path), []byte(tc.needle))
		if err != nil {
			t.Fatalf("NewUnifiedEqFilter(%q,%q): %v", tc.path, tc.needle, err)
		}
		filter.prepareLeaf(&view)
		gotMatched := 0
		wantMatched := 0
		var oracle UnifiedHoleResolver
		if err := oracle.SetPath([]byte(tc.path)); err != nil {
			t.Fatal(err)
		}
		for rank := 0; rank < count; rank++ {
			_, body, overflow, ok := view.RowRawAt(rank)
			if !ok || overflow {
				t.Fatalf("RowRawAt(%d)", rank)
			}
			rendered, ok = view.AppendRowBody(rendered[:0], body)
			if !ok {
				t.Fatalf("render rank %d", rank)
			}
			if value, found := unifiedProbeOracle(t, &oracle, rendered); found &&
				bytes.Equal(value, filter.Needle()) {
				wantMatched++
			}
			matched, needsRender, bodyOK := filter.matchBody(body)
			if !bodyOK {
				t.Fatalf("matchBody rank %d", rank)
			}
			if needsRender {
				doc := rendered
				if body[0] == unifiedRowTrivial {
					doc = body[1:]
				}
				matched, err = filter.EvalRendered(doc)
				if err != nil {
					t.Fatal(err)
				}
			}
			if matched {
				gotMatched++
			}
		}
		if gotMatched != wantMatched {
			t.Fatalf("filter (%q == %s): token lane %d, oracle %d",
				tc.path, tc.needle, gotMatched, wantMatched)
		}
	}
}

// TestUnifiedTokenViewZeroAlloc pins the steady-state hot calls at zero
// allocations: the hole read, the token compare, the warmed per-template
// resolution, and the warmed render-path evaluation.
func TestUnifiedTokenViewZeroAlloc(t *testing.T) {
	records := unifiedCompetitiveShapeRecords(200)
	page, _ := encodeUnifiedTestLeaf(t, records)
	view := openUnifiedTestLeaf(t, page)
	_, body, _, ok := view.RowRawAt(0)
	if !ok {
		t.Fatal("RowRawAt(0)")
	}
	template, _ := view.RowTemplate(body)
	if template < 0 {
		t.Fatal("competitive row must template")
	}
	var r UnifiedHoleResolver
	if err := r.SetPath([]byte("/country")); err != nil {
		t.Fatal(err)
	}
	hole := r.Resolve(&view, template)
	if hole < 0 {
		t.Fatalf("country hole = %d", hole)
	}
	if n := testing.AllocsPerRun(200, func() {
		if _, ok := view.RowToken(body, hole); !ok {
			t.Fatal("RowToken")
		}
	}); n != 0 {
		t.Fatalf("RowToken allocates %v/op", n)
	}
	filter, err := NewUnifiedEqFilter([]byte("/country"), []byte(`"PT"`))
	if err != nil {
		t.Fatal(err)
	}
	filter.prepareLeaf(&view)
	if n := testing.AllocsPerRun(200, func() {
		if _, _, ok := filter.matchBody(body); !ok {
			t.Fatal("matchBody")
		}
	}); n != 0 {
		t.Fatalf("matchBody allocates %v/op", n)
	}
	if n := testing.AllocsPerRun(50, func() {
		if r.Resolve(&view, template) != hole {
			t.Fatal("Resolve drift")
		}
	}); n != 0 {
		t.Fatalf("warmed Resolve allocates %v/op", n)
	}
	rendered, ok := view.AppendRowBody(make([]byte, 0, 1024), body)
	if !ok {
		t.Fatal("render")
	}
	if _, err := filter.EvalRendered(rendered); err != nil {
		t.Fatal(err)
	}
	if n := testing.AllocsPerRun(200, func() {
		if _, err := filter.EvalRendered(rendered); err != nil {
			t.Fatal(err)
		}
	}); n != 0 {
		t.Fatalf("warmed EvalRendered allocates %v/op", n)
	}
}

// TestUnifiedTokenViewAdversarial proves the token view fails closed on
// hostile row bodies and corrupted sections: no panic, no out-of-bounds
// read, no partial output — on truncated varints, unassigned tags,
// out-of-range dictionary ids, overrunning literals, random garbage bodies,
// and single-bit flips over an admitted (validation-bypassed) leaf image,
// mirroring resident-frame corruption after admission.
func TestUnifiedTokenViewAdversarial(t *testing.T) {
	records, _ := unifiedTestCorpus(220)
	page, count := encodeUnifiedTestLeaf(t, records)
	view := openUnifiedTestLeaf(t, page)

	hostile := [][]byte{
		{},                  // empty body
		{0x00},              // templated with no tokens where holes exist
		{0xFF},              // trivial with no bytes
		{0x00, 0xFC},        // int token, truncated varint
		{0x00, 0xFD},        // unassigned tag
		{0x00, 0xFE},        // unassigned tag
		{0x00, 0xF8},        // long literal, missing length
		{0x00, 0xF8, 0x7F},  // long literal, length overruns body
		{0x00, 0x80 | 0x40}, // short literal overruns body
		{0x00, 0x7F},        // dict id beyond dictionaryCount
		{0x00, 0xFC, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01}, // varint overflow
	}
	rng := rand.New(rand.NewPCG(7, 13))
	for range 500 {
		n := rng.IntN(40)
		body := make([]byte, n)
		for i := range body {
			body[i] = byte(rng.UintN(256))
		}
		hostile = append(hostile, body)
	}
	for _, body := range hostile {
		for hole := 0; hole < 16; hole++ {
			// Must never panic; a false return is the only acceptable failure.
			view.RowToken(body, hole)
		}
		view.RowTemplate(body)
		if out, ok := view.AppendRowBody(nil, body); ok && len(body) < 2 {
			t.Fatalf("hostile body %x rendered %q", body, out)
		}
	}

	// Single-bit flips over the unified sections and record heap of an
	// admitted view (validation deliberately bypassed): every probe/filter
	// call must complete without panic or out-of-bounds access.
	filter, err := NewUnifiedEqFilter([]byte("/country"), []byte(`"PT"`))
	if err != nil {
		t.Fatal(err)
	}
	var r UnifiedHoleResolver
	if err := r.SetPath([]byte("/profile/tier")); err != nil {
		t.Fatal(err)
	}
	for trial := 0; trial < 600; trial++ {
		corrupted := append([]byte(nil), page...)
		bit := rng.IntN(len(corrupted) * 8)
		corrupted[bit/8] ^= 1 << uint(bit%8)
		cv, ok := AdmittedCommonPrimaryUnifiedLeaf(
			corrupted, unifiedTestStoreID(), 0, unifiedTestBounds(),
		)
		if !ok {
			continue
		}
		filter.prepareLeaf(&cv)
		for rank := 0; rank < count && rank < cv.Len(); rank++ {
			_, body, overflow, rowOK := cv.RowRawAt(rank)
			if !rowOK || overflow {
				continue
			}
			cv.RowTemplate(body)
			cv.RowToken(body, 1)
			filter.matchBody(body)
			if template, tOK := cv.RowTemplate(body); tOK && template >= 0 {
				r.Resolve(&cv, template)
			}
		}
	}
}

// BenchmarkUnifiedHoleRead measures the scan-side hole read: tag walk to the
// country hole plus token decode (target: ≤ 25 ns, 0 allocs).
func BenchmarkUnifiedHoleRead(b *testing.B) {
	records := unifiedCompetitiveShapeRecords(200)
	page, count := encodeUnifiedTestBench(b, records)
	view, ok := AdmittedCommonPrimaryUnifiedLeaf(
		page, unifiedTestStoreID(), 0, unifiedTestBounds(),
	)
	if !ok {
		b.Fatal("admit")
	}
	bodies := make([][]byte, count)
	for rank := range bodies {
		_, body, _, rowOK := view.RowRawAt(rank)
		if !rowOK {
			b.Fatal("RowRawAt")
		}
		bodies[rank] = body
	}
	var r UnifiedHoleResolver
	if err := r.SetPath([]byte("/country")); err != nil {
		b.Fatal(err)
	}
	template, _ := view.RowTemplate(bodies[0])
	hole := r.Resolve(&view, template)
	if hole < 0 {
		b.Fatal("resolve")
	}
	b.ReportAllocs()
	b.ResetTimer()
	var sink int
	for i := 0; i < b.N; i++ {
		tok, ok := view.RowToken(bodies[i%count], hole)
		if !ok {
			b.Fatal("RowToken")
		}
		sink += len(tok.Spelling)
	}
	holeReadSink = sink
}

var holeReadSink int

// BenchmarkUnifiedLeafFilterToken measures the per-document token filter
// compare over one competitive-shape leaf: the token lane's core arithmetic
// (tag walk + dict-id compare), reported as ns/doc.
func BenchmarkUnifiedLeafFilterToken(b *testing.B) {
	records := unifiedCompetitiveShapeRecords(200)
	page, count := encodeUnifiedTestBench(b, records)
	view, ok := AdmittedCommonPrimaryUnifiedLeaf(
		page, unifiedTestStoreID(), 0, unifiedTestBounds(),
	)
	if !ok {
		b.Fatal("admit")
	}
	bodies := make([][]byte, count)
	for rank := range bodies {
		_, body, _, rowOK := view.RowRawAt(rank)
		if !rowOK {
			b.Fatal("RowRawAt")
		}
		bodies[rank] = body
	}
	filter, err := NewUnifiedEqFilter([]byte("/country"), []byte(`"PT"`))
	if err != nil {
		b.Fatal(err)
	}
	filter.prepareLeaf(&view)
	b.ReportAllocs()
	b.ResetTimer()
	matched := 0
	for i := 0; i < b.N; i++ {
		m, needsRender, ok := filter.matchBody(bodies[i%count])
		if !ok || needsRender {
			b.Fatal("token lane must decide competitive rows")
		}
		if m {
			matched++
		}
	}
	holeReadSink = matched
}

// BenchmarkUnifiedHoleResolve measures the per-(leaf, template) resolution
// cost the lanes amortize (design projection ~150 ns).
func BenchmarkUnifiedHoleResolve(b *testing.B) {
	records := unifiedCompetitiveShapeRecords(200)
	page, _ := encodeUnifiedTestBench(b, records)
	view, ok := AdmittedCommonPrimaryUnifiedLeaf(
		page, unifiedTestStoreID(), 0, unifiedTestBounds(),
	)
	if !ok {
		b.Fatal("admit")
	}
	var r UnifiedHoleResolver
	if err := r.SetPath([]byte("/country")); err != nil {
		b.Fatal(err)
	}
	_, body, _, _ := view.RowRawAt(0)
	template, _ := view.RowTemplate(body)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if r.Resolve(&view, template) < 0 {
			b.Fatal("resolve")
		}
	}
}

// encodeUnifiedTestBench is encodeUnifiedTestLeaf for benchmarks.
func encodeUnifiedTestBench(
	b *testing.B, records []CommonPrimaryLeafRecord,
) ([]byte, int) {
	b.Helper()
	storeID := unifiedTestStoreID()
	builder := NewUnifiedPrimaryLeafBuilder()
	count, extent, err := planUnifiedLeaf(builder, storeID, records)
	if err != nil {
		b.Fatal(err)
	}
	dst := make([]byte, extent)
	page, err := EncodeCommonPrimaryUnifiedLeaf(
		dst,
		CommonPrimaryLeafHeader{
			StoreID: storeID, Generation: 1, Bucket: 0, PageSize: uint32(extent),
		},
		storeID, records[:count], unifiedTestBounds(), builder,
	)
	if err != nil {
		b.Fatal(err)
	}
	return page, count
}
