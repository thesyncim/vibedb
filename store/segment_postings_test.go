package store

// Correctness gates for the inverted posting layer (segment_postings.go).
//
// The layer's contract is that acceleration is invisible: WhereExists and
// WhereContains return exactly what a full scan of the segment returns, on every
// input. Two harnesses pin that.
//
// TestSegmentPostingsDifferential runs an adversarial corpus — present and
// absent keys, nested and escaped and duplicate keys, repeated and shadowed
// values, non-conforming documents mixed among shape-clustered ones — under
// every storage combination (postings alone, postings with shape tapes, and
// postings enabled late) and asserts each posting answer equals an independent
// full-scan reference over the same documents.
//
// TestSegmentPostingsExhaustive enumerates every document in a bounded domain
// (the shared generator of verify_exhaustive_test.go), stores each three times
// so first-sighting classic storage, shape-taped storage, and multi-document
// shape lists are all exercised, and checks every existence and containment
// query in a battery against a from-scratch reference built on the enumerator's
// own abstract syntax tree — not the library's evaluator. The logged domain
// size is the strength of the evidence.

import (
	"fmt"
	"strings"
	"testing"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// refExists is the independent existence reference: the ordinals whose root
// object has a member named key, by a fresh standalone index per document.
func refExists(t *testing.T, docs []string, key string) []int {
	t.Helper()
	var res []int
	for i, doc := range docs {
		if _, ok := refIndex(t, doc).Root().Get(key); ok {
			res = append(res, i)
		}
	}
	return res
}

// postFullScanContains is the independent containment reference: the ordinals
// whose value at top-level key path contains needle, by fresh standalone
// indexes.
func postFullScanContains(t *testing.T, docs []string, path, needle string) []int {
	t.Helper()
	n := refIndex(t, needle).Root()
	var res []int
	for i, doc := range docs {
		if v, ok := refIndex(t, doc).Root().Get(path); ok && v.Contains(n) {
			res = append(res, i)
		}
	}
	return res
}

// refIndex builds a standalone exactly-sized index, the reference storage no
// posting or shape machinery touches.
func refIndex(t *testing.T, doc string) vibejson.Index {
	t.Helper()
	need, err := vibejson.RequiredIndexEntries([]byte(doc))
	if err != nil {
		t.Fatalf("RequiredIndexEntries(%s): %v", doc, err)
	}
	idx, err := vibejson.BuildIndex([]byte(doc), make([]vibejson.IndexEntry, need))
	if err != nil {
		t.Fatalf("BuildIndex(%s): %v", doc, err)
	}
	return idx
}

// postingsAdversarialCorpus is the differential battery: shape-clustered
// conforming objects repeated so shapes compile and dedup, interleaved with the
// non-conforming remainder (scalars, arrays, deep nests, wide objects) and the
// hard cases — duplicate keys, escaped keys, numbers spelled differently but
// equal, repeated values.
func postingsAdversarialCorpus() []string {
	var docs []string
	for i := 0; i < 6; i++ {
		// A recurring flat shape: same key sequence, varying scalar values,
		// with a repeated enum value across documents.
		docs = append(docs, fmt.Sprintf(`{"id":%d,"status":%q,"score":%d,"active":%t}`,
			i, []string{"open", "closed", "open"}[i%3], i*10, i%2 == 0))
	}
	return append(docs,
		`{"id":100,"status":"open","score":1e2,"active":true}`, // score equals 100
		`{"id":101,"status":"open","tags":[1,2,3],"active":false}`,
		`{"id":102,"tags":["open","x"],"note":"a\tb"}`,     // array of strings, escaped value
		`{"id":103,"a":1,"a":2}`,                           // duplicate key: last wins
		`{"id":104,"a":2,"a":1}`,                           // duplicate key, other order
		`{"abc":"v","status":null}`,                        // escaped key decoding to abc, null value
		`{"nested":{"status":"open"},"status":"deep"}`,     // nested object, top-level status
		`{"score":100,"status":"open"}`,                    // integer 100 vs 1e2
		`{"id":105,"status":"z","score":-0,"active":true}`, // negative-zero value
		`42`, `"open"`, `null`, `true`, `[1,2,3]`, `["open"]`, // non-object roots
		`[{"status":"open"}]`, // array root
	)
}

// TestSegmentPostingsDifferential asserts posting answers equal the full-scan
// reference over the adversarial corpus, under every storage combination and
// over a battery of existence and containment queries.
func TestSegmentPostingsDifferential(t *testing.T) {
	docs := postingsAdversarialCorpus()

	existKeys := []string{"id", "status", "score", "active", "tags", "note",
		"a", "abc", "nested", "missing", ""}
	type cq struct{ path, needle string }
	containQueries := []cq{
		{"status", `"open"`}, {"status", `"closed"`}, {"status", `null`},
		{"score", `100`}, {"score", `1e2`}, {"score", `1.0e2`}, {"score", `0`}, {"score", `-0`},
		{"active", `true`}, {"active", `false`},
		{"tags", `1`}, {"tags", `"open"`}, {"tags", `[1,2]`}, {"tags", `[1,9]`},
		{"a", `1`}, {"a", `2`}, {"note", `"a\tb"`}, {"abc", `"v"`},
		{"nested", `{"status":"open"}`}, {"status", `"open"`},
		{"missing", `1`}, {"status", `[]`},
	}

	variants := []struct {
		name  string
		build func() *Segment
	}{
		{"postings", func() *Segment {
			s := &Segment{Postings: true}
			appendAll(t, s, docs)
			return s
		}},
		{"postings+shapes", func() *Segment {
			s := &Segment{Postings: true, ShapeTapes: true}
			appendAll(t, s, docs)
			return s
		}},
		{"postings+shapes+hashkeys", func() *Segment {
			s := &Segment{Postings: true, ShapeTapes: true, Options: document.IndexOptions{HashKeys: true}}
			appendAll(t, s, docs)
			return s
		}},
		{"late-enable-fallback", func() *Segment {
			// Postings turned on after half the corpus: postingsReady is false,
			// so both queries must fall back to a correct full scan.
			s := &Segment{ShapeTapes: true}
			appendAll(t, s, docs[:len(docs)/2])
			s.Postings = true
			appendAll(t, s, docs[len(docs)/2:])
			return s
		}},
	}

	for _, v := range variants {
		s := v.build()
		for _, key := range existKeys {
			if got := s.WhereExists(key); !equalInts(got, refExists(t, docs, key)) {
				t.Fatalf("%s: WhereExists(%q) = %v, want %v", v.name, key, got, refExists(t, docs, key))
			}
		}
		for _, q := range containQueries {
			got, err := s.WhereContains(q.path, []byte(q.needle))
			if err != nil {
				t.Fatalf("%s: WhereContains(%q,%s): %v", v.name, q.path, q.needle, err)
			}
			if want := postFullScanContains(t, docs, q.path, q.needle); !equalInts(got, want) {
				t.Fatalf("%s: WhereContains(%q,%s) = %v, want %v", v.name, q.path, q.needle, got, want)
			}
		}
	}
}

func TestStoreBuilderShapeCompactionKeepsExistencePostingsExact(t *testing.T) {
	builder, err := NewBuilder(Options{
		ChunkDocuments: 2,
		ShapeTapes:     true,
		Postings:       true,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		key, json string
	}{
		{"first", `{"present":1,"other":true}`},
		{"second", `{"present":2,"other":false}`},
	} {
		if err := builder.Append(row.key, []byte(row.json)); err != nil {
			t.Fatal(err)
		}
	}

	// The first row was committed classic as the shape's sighting gate, then
	// retroactively compacted when the full unpublished chunk was flushed.
	chunk := builder.chunks.Get(0)
	if chunk == nil || chunk.Docs.ShapeTapeRefAt(0).Rec == nil {
		t.Fatal("first shape sighting was not compacted")
	}
	if got := chunk.Docs.WhereExists("present"); !equalInts(got, []int{0, 1}) {
		t.Fatalf("WhereExists after shape transition = %v, want [0 1]", got)
	}

	collection, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _ := collection.Snapshot()
	keys := make([]string, 0, snapshot.Len())
	keys = snapshot.AppendWhereExistsKeys(keys, "present")
	if len(keys) != 2 || keys[0] != "first" || keys[1] != "second" {
		t.Fatalf("AppendWhereExistsKeys after shape transition = %v, want [first second]", keys)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		keys = snapshot.AppendWhereExistsKeys(keys[:0], "present")
	}); allocs != 0 {
		t.Fatalf("AppendWhereExistsKeys allocated %.2f times, want 0", allocs)
	}
}

// TestSegmentPostingsInvalidNeedle checks an invalid needle surfaces the build
// error, matching RawContains.
func TestSegmentPostingsInvalidNeedle(t *testing.T) {
	s := &Segment{Postings: true}
	appendAll(t, s, []string{`{"a":1}`, `{"a":1}`})
	if _, err := s.WhereContains("a", []byte(`{`)); err == nil {
		t.Fatal("WhereContains with invalid needle: want error, got nil")
	}
}

func TestPostDecodedStringHashes(t *testing.T) {
	for n := 0; n <= 33; n++ {
		raw := []byte(`"` + strings.Repeat(`\u0061`, n) + `"`)
		decoded := strings.Repeat("a", n)
		if got, want := hashDecodedJSONString(raw), vibejson.HashKey(decoded); got != want {
			t.Fatalf("decoded key hash length %d = %#x, want %#x", n, got, want)
		}
		if got, want := postDecodedStringFNV(raw), postFNV([]byte(decoded)); got != want {
			t.Fatalf("decoded value hash length %d = %#x, want %#x", n, got, want)
		}
	}
	for _, test := range []struct {
		raw, decoded string
	}{
		{`"a\n\tb"`, "a\n\tb"},
		{`"\u00e9"`, "é"},
		{`"\uD83D\uDE00é"`, "😀é"},
	} {
		if got, want := hashDecodedJSONString([]byte(test.raw)), vibejson.HashKey(test.decoded); got != want {
			t.Errorf("decoded key hash %s = %#x, want %#x", test.raw, got, want)
		}
		if got, want := postDecodedStringFNV([]byte(test.raw)), postFNV([]byte(test.decoded)); got != want {
			t.Errorf("decoded value hash %s = %#x, want %#x", test.raw, got, want)
		}
	}
}

// TestSegmentPostingsAppendBuffers pins the caller-owned storage contract. The
// append forms must preserve an existing prefix, agree with the convenience
// APIs, and leave that prefix untouched when needle validation fails.
func TestSegmentPostingsAppendBuffers(t *testing.T) {
	docs := postingsAdversarialCorpus()
	variants := []struct {
		name string
		seg  *Segment
	}{
		{"postings", &Segment{Postings: true}},
		{"postings+shapes", &Segment{Postings: true, ShapeTapes: true}},
		{"late-enable-fallback", &Segment{ShapeTapes: true}},
	}
	for i := range variants {
		v := &variants[i]
		if v.name == "late-enable-fallback" {
			appendAll(t, v.seg, docs[:len(docs)/2])
			v.seg.Postings = true
			appendAll(t, v.seg, docs[len(docs)/2:])
		} else {
			appendAll(t, v.seg, docs)
		}

		prefix := []int{-2, -1}
		wantExists := append(append([]int(nil), prefix...), v.seg.WhereExists("status")...)
		if got := v.seg.AppendWhereExists(append([]int(nil), prefix...), "status"); !equalInts(got, wantExists) {
			t.Fatalf("%s: AppendWhereExists = %v, want %v", v.name, got, wantExists)
		}

		wantContains, err := v.seg.WhereContains("status", []byte(`"open"`))
		if err != nil {
			t.Fatalf("%s: WhereContains: %v", v.name, err)
		}
		wantContains = append(append([]int(nil), prefix...), wantContains...)
		gotContains, err := v.seg.AppendWhereContains(append([]int(nil), prefix...), "status", []byte(`"open"`))
		if err != nil {
			t.Fatalf("%s: AppendWhereContains: %v", v.name, err)
		}
		if !equalInts(gotContains, wantContains) {
			t.Fatalf("%s: AppendWhereContains = %v, want %v", v.name, gotContains, wantContains)
		}

		needle := refIndex(t, `"open"`)
		if got := v.seg.AppendWhereContainsIndex(append([]int(nil), prefix...), "status", needle); !equalInts(got, wantContains) {
			t.Fatalf("%s: AppendWhereContainsIndex = %v, want %v", v.name, got, wantContains)
		}

		gotInvalid, err := v.seg.AppendWhereContains(append([]int(nil), prefix...), "status", []byte(`{`))
		if err == nil {
			t.Fatalf("%s: AppendWhereContains invalid needle: want error", v.name)
		}
		if !equalInts(gotInvalid, prefix) {
			t.Fatalf("%s: AppendWhereContains invalid needle changed dst: got %v, want %v", v.name, gotInvalid, prefix)
		}
	}
}

// TestSegmentPostingsAppendSteadyAllocs proves that callers can remove the
// result allocation and repeated needle-index allocation from a hot lookup.
func TestSegmentPostingsAppendSteadyAllocs(t *testing.T) {
	s := &Segment{Postings: true, ShapeTapes: true, Options: document.IndexOptions{HashKeys: true}}
	docs := postingsAdversarialCorpus()
	longEscaped := `"` + strings.Repeat(`\u0061`, 96) + `"`
	longKey := `"` + strings.Repeat(`\u0062`, 96) + `"`
	for range 3 {
		docs = append(docs, `{"status":`+longEscaped+`,"empty_array":[],"empty_object":{}}`)
		docs = append(docs, `{"payload":{`+longKey+`:1}}`)
	}
	appendAll(t, s, docs)
	needle := refIndex(t, `"open"`)
	escapedNeedle := refIndex(t, longEscaped)
	emptyArrayNeedle := refIndex(t, `[]`)
	emptyObjectNeedle := refIndex(t, `{}`)
	objectNeedle := refIndex(t, `{`+longKey+`:1}`)

	exists := make([]int, 0, len(docs))
	contains := make([]int, 0, len(docs))
	escaped := make([]int, 0, len(docs))
	emptyArray := make([]int, 0, len(docs))
	emptyObject := make([]int, 0, len(docs))
	object := make([]int, 0, len(docs))
	exists = s.AppendWhereExists(exists, "status")
	contains = s.AppendWhereContainsIndex(contains, "status", needle)
	escaped = s.AppendWhereContainsIndex(escaped, "status", escapedNeedle)
	emptyArray = s.AppendWhereContainsIndex(emptyArray, "empty_array", emptyArrayNeedle)
	emptyObject = s.AppendWhereContainsIndex(emptyObject, "empty_object", emptyObjectNeedle)
	object = s.AppendWhereContainsIndex(object, "payload", objectNeedle)
	if len(exists) == 0 || len(contains) == 0 {
		t.Fatalf("warmup returned empty results: exists=%v contains=%v", exists, contains)
	}
	if len(escaped) != 3 || len(emptyArray) != 3 || len(emptyObject) != 3 || len(object) != 3 {
		t.Fatalf("warmup mismatch: escaped=%v array=%v empty-object=%v object=%v", escaped, emptyArray, emptyObject, object)
	}

	if n := testing.AllocsPerRun(100, func() {
		exists = s.AppendWhereExists(exists[:0], "status")
	}); n != 0 {
		t.Fatalf("AppendWhereExists allocated %.1f times per warmed lookup", n)
	}
	if n := testing.AllocsPerRun(100, func() {
		contains = s.AppendWhereContainsIndex(contains[:0], "status", needle)
	}); n != 0 {
		t.Fatalf("AppendWhereContainsIndex allocated %.1f times per warmed lookup", n)
	}
	if n := testing.AllocsPerRun(100, func() {
		escaped = s.AppendWhereContainsIndex(escaped[:0], "status", escapedNeedle)
		emptyArray = s.AppendWhereContainsIndex(emptyArray[:0], "empty_array", emptyArrayNeedle)
		emptyObject = s.AppendWhereContainsIndex(emptyObject[:0], "empty_object", emptyObjectNeedle)
		object = s.AppendWhereContainsIndex(object[:0], "payload", objectNeedle)
	}); n != 0 {
		t.Fatalf("AppendWhereContainsIndex allocated %.1f times across escaped scalar, empty-container, and object lookups", n)
	}

	// Enabling postings after ingest deliberately selects the exact scan
	// fallback; caller-owned storage must remove allocations there as well.
	fallback := &Segment{ShapeTapes: true, Options: document.IndexOptions{HashKeys: true}}
	appendAll(t, fallback, docs)
	fallback.Postings = true
	fallbackExists := make([]int, 0, len(docs))
	fallbackObject := make([]int, 0, len(docs))
	fallbackExists = fallback.AppendWhereExists(fallbackExists, "status")
	fallbackObject = fallback.AppendWhereContainsIndex(fallbackObject, "payload", objectNeedle)
	if n := testing.AllocsPerRun(100, func() {
		fallbackExists = fallback.AppendWhereExists(fallbackExists[:0], "status")
		fallbackObject = fallback.AppendWhereContainsIndex(fallbackObject[:0], "payload", objectNeedle)
	}); n != 0 {
		t.Fatalf("buffered posting scan fallbacks allocated %.1f times per warmed pair", n)
	}
}

// TestSegmentPostingsExhaustive enumerates a bounded document domain, stores each
// document three times so classic, shape-taped, and multi-document shape lists
// are all exercised, and checks every query in the battery against an
// independent reference over the enumerator's AST.
func TestSegmentPostingsExhaustive(t *testing.T) {
	depth, nodes, width := bexPairDepth, testIterations(bexPairNodes, 2), bexPairWidth
	universe := exhaustiveGenerate(depth, nodes, width)

	// The needle battery: every scalar terminal (so canonical numeric and
	// string bucketing is stressed, including equal-value spellings) plus the
	// empty and singleton containers that drive the full-scan needle path.
	needles := []*exhaustiveValue{
		bexNull(), bexBool(true), bexBool(false),
		bexNumber("0"), bexNumber("1"), bexNumber("1.0"), bexNumber("1e0"),
		bexNumber("10"), bexNumber("1e1"), bexNumber("1.5"), bexNumber("1e2"),
		bexString(`""`, "", false), bexString(`"a"`, "a", false),
		bexString(`"ab"`, "ab", false), bexString(`"a\n"`, "a\n", true),
		bexEmptyArray(), bexEmptyObject(),
		bexMakeArray([]*exhaustiveValue{bexNumber("1")}),
		bexMakeObject([]string{"a"}, []*exhaustiveValue{bexNumber("1")}),
	}
	// The existence battery: every key that can appear plus keys that never do.
	keys := []string{"a", "b", "", "missing"}

	// Store the universe three times: copy one is a first sighting (classic,
	// remainder), copies two and three are shape-taped when conforming, so a
	// shape's document list holds more than one ordinal.
	var docJSON []string
	s := &Segment{Postings: true, ShapeTapes: true}
	for rep := 0; rep < 3; rep++ {
		for _, d := range universe {
			docJSON = append(docJSON, string(d.json))
		}
	}
	appendAll(t, s, docJSON)

	// The AST-backed reference, aligned with the tripled ordinal order.
	asts := make([]*exhaustiveValue, 0, len(docJSON))
	for rep := 0; rep < 3; rep++ {
		asts = append(asts, universe...)
	}

	queries := 0
	for _, key := range keys {
		want := postingsRefExists(asts, key)
		if got := s.WhereExists(key); !equalInts(got, want) {
			t.Fatalf("WhereExists(%q): posting %v != reference %v", key, got, want)
		}
		queries++
		for _, needle := range needles {
			want := postingsRefContains(asts, key, needle)
			got, err := s.WhereContains(key, needle.json)
			if err != nil {
				t.Fatalf("WhereContains(%q,%s): %v", key, needle.json, err)
			}
			if !equalInts(got, want) {
				t.Fatalf("WhereContains(%q,%s): posting %v != reference %v", key, needle.json, got, want)
			}
			queries++
		}
	}

	t.Logf("pair bound depth<=%d nodes<=%d width<=%d: %d documents x3 stored, %d queries checked against the AST reference",
		depth, nodes, width, len(universe), queries)
}

// postingsRefExists is the AST-backed existence reference: the ordinals whose
// document is an object with an effective member named key.
func postingsRefExists(asts []*exhaustiveValue, key string) []int {
	var res []int
	for i, a := range asts {
		if a.kind != document.Object {
			continue
		}
		if _, last := bexEffectiveMembers(a); last[key] != nil {
			res = append(res, i)
		}
	}
	return res
}

// postingsRefContains is the AST-backed containment reference: the ordinals
// whose effective value at key contains needle, by the independent jsonb @>
// definition (exhaustiveContains, top-level).
func postingsRefContains(asts []*exhaustiveValue, key string, needle *exhaustiveValue) []int {
	var res []int
	for i, a := range asts {
		if a.kind != document.Object {
			continue
		}
		_, last := bexEffectiveMembers(a)
		if v := last[key]; v != nil && exhaustiveContains(v, needle, true) {
			res = append(res, i)
		}
	}
	return res
}

// appendAll appends every document to the segment, failing on any error.
func appendAll(t *testing.T, s *Segment, docs []string) {
	t.Helper()
	for _, doc := range docs {
		if _, err := s.Append([]byte(doc)); err != nil {
			t.Fatalf("Append(%s): %v", doc, err)
		}
	}
}

// equalInts reports whether two ordinal slices are equal, treating nil and
// empty as equal.
func equalInts(a, b []int) bool {
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
