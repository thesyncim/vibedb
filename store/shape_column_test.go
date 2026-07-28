package store

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// AppendField and AppendFields promise Node.Get semantics per document, with
// every fast path — the two-slot positional hint, the projection's
// fingerprint route, the sticky-absent run, and the hunt backoff — invisible
// in the results. These tests hold the fused loops to that contract by
// differential comparison against the per-document reference over corpora
// chosen to force each internal transition: homogeneous and clustered runs,
// strict alternation with the field's position moved between equal-width
// layouts, layouts whose suffix members duplicate the queried spelling in
// raw, escaped, and unicode forms, shapes lacking the field, all-distinct
// layouts deep enough to engage the backoff, non-flat and non-object roots,
// enriched and unenriched tapes, and empty sets. The standing GOGC gate
// covers the hint read's and suffix scan's unsafe entry walks.

// buildShapeColumnSet indexes docs into one Segment under the given
// enrichment.
func buildShapeColumnSet(docs []string, hashKeys bool) (*Segment, error) {
	var set Segment
	set.Options = document.IndexOptions{HashKeys: hashKeys}
	for _, doc := range docs {
		if _, err := set.Append([]byte(doc)); err != nil {
			return nil, fmt.Errorf("Append(%.60q): %v", doc, err)
		}
	}
	return &set, nil
}

// shapeColumnSegment is buildShapeColumnSet failing the test on error.
func shapeColumnSegment(t testing.TB, docs []string, hashKeys bool) *Segment {
	t.Helper()
	set, err := buildShapeColumnSet(docs, hashKeys)
	if err != nil {
		t.Fatal(err)
	}
	return set
}

// refAppendField is the exact per-document reference for AppendField: the
// root Get on every document in ordinal order, absence and non-object roots
// contributing the zero RawValue.
func refAppendField(s *Segment, name string) []vibejson.RawValue {
	dst := make([]vibejson.RawValue, 0, s.Len())
	for d := 0; d < s.Len(); d++ {
		if v, ok := s.Doc(d).Root().Get(name); ok {
			dst = append(dst, v.Raw())
			continue
		}
		dst = append(dst, vibejson.RawValue{})
	}
	return dst
}

// shapeColumnQueries returns the query battery for a set: the union of every
// distinct root layout's keyHashQuerySet — decoded keys plus absent
// neighbours shadowing their shapes.
func shapeColumnQueries(s *Segment) []string {
	seen := map[string]struct{}{}
	var queries []string
	for d := 0; d < s.Len(); d++ {
		for _, q := range keyHashQuerySet(s.Doc(d).Root()) {
			if _, ok := seen[q]; ok {
				continue
			}
			seen[q] = struct{}{}
			queries = append(queries, q)
		}
	}
	return queries
}

// checkAppendField runs one query against one set through a cold and a warm
// pass on one cache — the first exercising the sighting-gated fallbacks, the
// second the compiled fast paths — and requires alias-identical results to
// the reference on both.
func checkAppendField(t *testing.T, cache *ShapeCache, s *Segment, name, label string) {
	t.Helper()
	want := refAppendField(s, name)
	for pass := range 2 {
		got := cache.AppendField(nil, s, name)
		if len(got) != len(want) {
			t.Fatalf("%s pass %d: AppendField(%q) appended %d values for %d documents",
				label, pass, name, len(got), len(want))
		}
		for i := range got {
			if !sameRawValue(got[i], want[i]) {
				t.Fatalf("%s pass %d: AppendField(%q)[%d] = %q, Get %q",
					label, pass, name, i, got[i].Bytes(), want[i].Bytes())
			}
		}
	}
}

// shapeColumnCorpora returns the labeled document corpora the differential
// battery runs, each engineered for one internal transition of the fused
// loops.
func shapeColumnCorpora() map[string][]string {
	corpora := map[string][]string{}

	// The adversarial corpus in one heterogeneous set: duplicates, escaped
	// and unicode spellings, non-flat roots, pointer metacharacters, plus
	// non-object roots between them.
	hetero := append([]string{}, keyHashCorpus...)
	hetero = append(hetero, `[1,2,3]`, `"scalar"`, `42`, `null`, `true`,
		keyHashWideDoc(64, ""), shapeFlatDoc(64, "pad-"))
	corpora["hetero"] = hetero

	// One 8-field layout repeated: the pure run the hint serves.
	var homogeneous []string
	for i := range 48 {
		homogeneous = append(homogeneous, fmt.Sprintf(
			`{"id":%d,"name":"u-%03d","active":%t,"score":%d.%02d,"region":"eu-%d","tier":%d,"ts":%d,"flags":null}`,
			i, i, i%2 == 0, i%100, i%97, i%3, i%5, 1700000000+i))
	}
	corpora["homogeneous"] = homogeneous

	// Two equal-width layouts in strict alternation with the shared fields
	// at different positions, so every document rejects the leading hint and
	// must be served by the displaced slot.
	var shifted []string
	for i := range 48 {
		if i%2 == 0 {
			shifted = append(shifted, fmt.Sprintf(`{"ts":%d,"a0":0,"a1":1,"a2":2,"id":%d}`, i, i))
		} else {
			shifted = append(shifted, fmt.Sprintf(`{"b0":0,"b1":1,"ts":%d,"b2":2,"id":%d}`, i, i))
		}
	}
	corpora["shifted"] = shifted

	// Suffix claimants: layout A compiles "a" at member 0; the equal-width
	// impostors byte-match it there but repeat the spelling later — raw,
	// escaped, and unicode-escaped — so the positional read must yield to
	// the exact lookup's last-duplicate rule.
	corpora["suffixdup"] = []string{
		`{"a":1,"b":2,"c":3}`,
		`{"a":1,"b":2,"c":3}`,
		`{"a":4,"x":5,"a":6}`,
		`{"a":7,"a":8,"y":9}`,
		`{"a":10,"k":11,"a":12}`,
		`{"a":1,"b":2,"c":3}`,
		`{"a":13,"a":14,"a":15}`,
	}

	// A shape with the field against an equal-width shape without it, in
	// alternation: exercises the sticky-absent run, its promotion between
	// hint slots, and recovery.
	var absent []string
	for i := range 48 {
		if i%2 == 0 {
			absent = append(absent, fmt.Sprintf(`{"ts":%d,"a":1,"b":2}`, i))
		} else {
			absent = append(absent, fmt.Sprintf(`{"x":%d,"y":1,"z":2}`, i))
		}
	}
	corpora["absentalt"] = absent

	// All-distinct equal-width layouts, deep enough that the hunt backoff
	// engages and skips resolutions mid-corpus.
	var distinct []string
	for i := range 64 {
		distinct = append(distinct, fmt.Sprintf(
			`{"d%02d_a":1,"d%02d_b":2,"d%02d_c":3,"d%02d_d":4,"shared":%d}`, i, i, i, i, i))
	}
	corpora["distinct"] = distinct

	// Clustered runs whose boundaries interleave non-flat and non-object
	// documents, so runs are broken by unresolvable roots.
	var broken []string
	for i := range 48 {
		switch {
		case i%12 == 6:
			broken = append(broken, `{"nested":{"deep":1},"ts":2}`)
		case i%12 == 9:
			broken = append(broken, `[1,2]`)
		case i/12%2 == 0:
			broken = append(broken, fmt.Sprintf(`{"ts":%d,"u":1,"v":2}`, i))
		default:
			broken = append(broken, fmt.Sprintf(`{"w":1,"ts":%d,"u":2}`, i))
		}
	}
	corpora["broken"] = broken

	return corpora
}

// TestAppendFieldDifferential is the zero-regression gate for the fused
// single-field loop: over every corpus, enrichment, and query, cold and warm
// passes return alias-identical values to the per-document root Get.
func TestAppendFieldDifferential(t *testing.T) {
	for label, docs := range shapeColumnCorpora() {
		for _, hashKeys := range []bool{false, true} {
			set := shapeColumnSegment(t, docs, hashKeys)
			var cache ShapeCache
			for _, q := range shapeColumnQueries(set) {
				checkAppendField(t, &cache, set, q,
					fmt.Sprintf("%s hashKeys=%v", label, hashKeys))
			}
		}
	}
}

// TestAppendFieldMixedEnrichment drives one cache over enriched and
// unenriched builds of one layout in a single set: the hint must serve both
// through the same compiled shape, hashed and plain suffix scans included.
func TestAppendFieldMixedEnrichment(t *testing.T) {
	var set Segment
	for i := range 32 {
		set.Options = document.IndexOptions{HashKeys: i%2 == 0}
		doc := fmt.Sprintf(`{"a":%d,"dup":1,"b":%d,"dup":2,"ts":%d}`, i, i, i)
		if _, err := set.Append([]byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	var cache ShapeCache
	for _, q := range []string{"a", "dup", "b", "ts", "absent", ""} {
		checkAppendField(t, &cache, &set, q, "mixed enrichment")
	}
}

// TestAppendFieldsDifferential holds the projection to its two contracts:
// column j equals a single-field AppendField pass for names[j], and both
// equal the per-document reference — across corpora, enrichments, and name
// groups mixing present, absent, duplicate, and escaped spellings.
func TestAppendFieldsDifferential(t *testing.T) {
	groups := [][]string{
		{"ts"},
		{"ts", "id"},
		{"a", "b", "c", "absent"},
		{"ts", "u", "v", "w", "x", "y", "z", "shared"},
		{"dup", "k\n", "héllo", "😀", ""},
	}
	for label, docs := range shapeColumnCorpora() {
		for _, hashKeys := range []bool{false, true} {
			set := shapeColumnSegment(t, docs, hashKeys)
			for _, names := range groups {
				var cache ShapeCache
				for pass := range 2 {
					cols := cache.AppendFields(nil, set, names...)
					if len(cols) != len(names) {
						t.Fatalf("%s: AppendFields grew %d columns for %d names", label, len(cols), len(names))
					}
					var single ShapeCache
					for j, name := range names {
						want := refAppendField(set, name)
						one := single.AppendField(nil, set, name)
						if len(cols[j]) != len(want) || len(one) != len(want) {
							t.Fatalf("%s pass %d: column %q lengths %d/%d, want %d",
								label, pass, name, len(cols[j]), len(one), len(want))
						}
						for i := range want {
							if !sameRawValue(cols[j][i], want[i]) {
								t.Fatalf("%s pass %d: AppendFields[%q][%d] = %q, Get %q",
									label, pass, name, i, cols[j][i].Bytes(), want[i].Bytes())
							}
							if !sameRawValue(one[i], want[i]) {
								t.Fatalf("%s pass %d: AppendField(%q)[%d] = %q, Get %q",
									label, pass, name, i, one[i].Bytes(), want[i].Bytes())
							}
						}
					}
				}
			}
		}
	}
}

// TestAppendFieldsColumnContract pins the dst shape rules: nil dst grows one
// column per name, short dst is extended, surplus columns and prior contents
// are untouched, and no names returns dst unchanged.
func TestAppendFieldsColumnContract(t *testing.T) {
	set := shapeColumnSegment(t, []string{`{"a":1,"b":2}`, `{"a":3,"b":4}`}, true)
	var cache ShapeCache

	if cols := cache.AppendFields(nil, set); cols != nil {
		t.Fatalf("AppendFields with no names built columns: %v", cols)
	}
	sentinel := []vibejson.RawValue{{}, {}, {}}
	prior := vibejson.RawValue{Src: []byte(`"prior"`)}
	dst := [][]vibejson.RawValue{{prior}, nil, sentinel}
	cols := cache.AppendFields(dst, set, "a", "b")
	if len(cols) != 3 {
		t.Fatalf("AppendFields shrank dst to %d columns", len(cols))
	}
	if len(cols[0]) != 3 || !sameRawValue(cols[0][0], prior) {
		t.Fatal("AppendFields disturbed column 0's prior contents")
	}
	if len(cols[1]) != 2 {
		t.Fatalf("column 1 got %d values, want 2", len(cols[1]))
	}
	if len(cols[2]) != len(sentinel) {
		t.Fatal("AppendFields touched a column beyond the names")
	}
	for d := 0; d < set.Len(); d++ {
		wantA, _ := set.Doc(d).Root().Get("a")
		wantB, _ := set.Doc(d).Root().Get("b")
		if !sameRawValue(cols[0][d+1], wantA.Raw()) || !sameRawValue(cols[1][d], wantB.Raw()) {
			t.Fatalf("document %d misprojected", d)
		}
	}

	var empty Segment
	cols = cache.AppendFields(nil, &empty, "a")
	if len(cols) != 1 || cols[0] != nil {
		t.Fatalf("empty set: AppendFields = %v, want one empty column", cols)
	}
	if got := cache.AppendField(nil, &empty, "a"); got != nil {
		t.Fatalf("empty set: AppendField = %v, want nil", got)
	}
}

// TestAppendFieldSteadyAllocs proves the steady-state allocation contracts:
// a warm AppendField pass with dst capacity allocates nothing, and a warm
// AppendFields pass allocates only its per-name state, independent of the
// document count.
func TestAppendFieldSteadyAllocs(t *testing.T) {
	set := shapeColumnClusteredDocs(64, 8, t)
	var cache ShapeCache
	names := []string{"id", "ts", "name", "flags"}

	dst := make([]vibejson.RawValue, 0, set.Len())
	dst = cache.AppendField(dst[:0], set, "ts")
	if n := testing.AllocsPerRun(20, func() {
		dst = cache.AppendField(dst[:0], set, "ts")
	}); n != 0 {
		t.Fatalf("warm AppendField allocated %.1f times per pass", n)
	}

	cols := make([][]vibejson.RawValue, len(names))
	for j := range cols {
		cols[j] = make([]vibejson.RawValue, 0, set.Len())
	}
	reset := func() {
		for j := range cols {
			cols[j] = cols[j][:0]
		}
	}
	reset()
	cols = cache.AppendFields(cols, set, names...)
	if n := testing.AllocsPerRun(20, func() {
		reset()
		cols = cache.AppendFields(cols, set, names...)
	}); n > 2 {
		t.Fatalf("warm AppendFields allocated %.1f times per pass, want at most 2", n)
	}
}

// TestGCCorruptionShapeColumn is the standing corruption gate for the fused
// loops' unsafe tape walks: the positional hint read and the suffix claimant
// scan both step entries by offset arithmetic while the garbage collector
// may move stacks. Concurrent workers rebuild document sets whose entry
// arenas end in sentinel-poisoned free tails, extract single fields and
// projections under forced stack movement and GC, verify every value against
// the exact reference, and prove the sentinels — phantom key entries that
// byte-match the query just past the final document's tape — are never read
// into a result nor overwritten. Stress:
//
//	GOGC=1 GOEXPERIMENT=simd gotip test -run TestGCCorruptionShapeColumn -count=5 -cpu=1,4,8 ./
func TestGCCorruptionShapeColumn(t *testing.T) {
	// The corpus ends in the homogeneous run so the final document — the one
	// adjacent to the poisoned tail — is served by the hint fast path, whose
	// suffix scan runs to the tape's last key entry.
	var docs []string
	for i := range 8 {
		docs = append(docs, fmt.Sprintf(`{"pre%d":%d,"nested":{"x":1}}`, i, i))
	}
	for i := range 24 {
		docs = append(docs, fmt.Sprintf(`{"q":%d,"c0":%d,"c1":"v-%02d","c2":%d}`, i, i*3, i, i%7))
	}
	names := []string{"q", "c1", "c2", "absent"}
	reference := map[string][]string{}
	{
		set := shapeColumnSegment(t, docs, true)
		for _, name := range names {
			var vals []string
			for _, v := range refAppendField(set, name) {
				vals = append(vals, string(v.Bytes()))
			}
			reference[name] = vals
		}
	}

	sentinel := vibejson.IndexEntry{Start: ^uint32(0), End: ^uint32(0), Next: ^uint32(0), Info: ^uint32(0)}
	workers := runtime.GOMAXPROCS(0) * 2
	const iters = 32
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			var cache ShapeCache
			dst := make([]vibejson.RawValue, 0, len(docs))
			cols := make([][]vibejson.RawValue, len(names))
			var retained [][]vibejson.RawValue
			var retainedSets []*Segment
			for it := range iters {
				forceStackMovement(48+id, it)
				set, err := buildShapeColumnSet(docs, true)
				if err != nil {
					errs <- fmt.Errorf("worker %d iter %d: %v", id, it, err)
					return
				}
				// Poison the entry arena's free tail: sentinels behind the
				// final document, where an over-running suffix scan or hint
				// read would land.
				tail := set.entryChunk[len(set.entryChunk):cap(set.entryChunk)]
				for i := range tail {
					tail[i] = sentinel
				}
				for _, name := range names {
					dst = cache.AppendField(dst[:0], set, name)
					want := reference[name]
					if len(dst) != len(want) {
						errs <- fmt.Errorf("worker %d iter %d: AppendField(%q) length %d, want %d",
							id, it, name, len(dst), len(want))
						return
					}
					for i := range dst {
						if string(dst[i].Bytes()) != want[i] {
							errs <- fmt.Errorf("worker %d iter %d: AppendField(%q)[%d] = %q, want %q",
								id, it, name, i, dst[i].Bytes(), want[i])
							return
						}
					}
				}
				for j := range cols {
					cols[j] = cols[j][:0]
				}
				cols = cache.AppendFields(cols, set, names...)
				for j, name := range names {
					want := reference[name]
					for i := range cols[j] {
						if string(cols[j][i].Bytes()) != want[i] {
							errs <- fmt.Errorf("worker %d iter %d: AppendFields[%q][%d] = %q, want %q",
								id, it, name, i, cols[j][i].Bytes(), want[i])
							return
						}
					}
				}
				for i := range tail {
					if tail[i] != sentinel {
						errs <- fmt.Errorf("worker %d iter %d: sentinel %d overwritten", id, it, i)
						return
					}
				}
				// Retain a column across collections; its values alias the
				// set's arenas and must stay byte-stable.
				column := cache.AppendField(nil, set, "q")
				retained = append(retained, column)
				retainedSets = append(retainedSets, set)
				if len(retained) > 3 {
					retained = retained[1:]
					retainedSets = retainedSets[1:]
				}
				if it%8 == 0 {
					runtime.GC()
				}
				want := reference["q"]
				for _, r := range retained {
					for i := range r {
						if string(r[i].Bytes()) != want[i] {
							errs <- fmt.Errorf("worker %d iter %d: retained column value %d corrupted", id, it, i)
							return
						}
					}
				}
			}
			runtime.KeepAlive(retainedSets)
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// Given a Builder-built collection whose repeated document layout was admitted
// as a document template, when a sparse row gather resolves a top-level field,
// then it returns the same values a full column scan does.
//
// Before this test AppendFieldRows had no template branch at all: a templated
// document carries no classic tape entries, so every field of every such
// document resolved to the empty RawValue — which is the encoding of "absent",
// so nothing complained. The full scan (AppendField) and the typed scans have
// always had the branch; only the sparse gather was missing it, which made the
// bug reachable exactly when a planner narrowed to a row set, and invisible
// otherwise. Its symptom is a dropped row, never a wrong value.
func TestAppendFieldRowsResolvesTemplatedDocuments(t *testing.T) {
	builder, err := NewBuilder(Options{ChunkDocuments: 8})
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}
	const rows = 32
	for i := range rows {
		doc := fmt.Appendf(nil, `{"id":%d,"name":"user-%04d"}`, i, i)
		if err := builder.Append(fmt.Sprintf("k%04d", i), doc); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	collection, err := builder.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	snapshot, _ := collection.Snapshot()

	full := snapshot.AppendField(nil, "id", nil)
	if len(full) != rows {
		t.Fatalf("full scan length: got %d want %d", len(full), rows)
	}
	templated := false
	snapshot.state.Chunks.Each(func(_ uint32, chunk *Chunk) bool {
		if _, ok := chunk.Docs.TemplateAt(0); ok {
			templated = true
		}
		return false
	})
	if !templated {
		t.Skip("this Builder configuration did not admit document templates")
	}

	var locations []Location
	for i := range rows {
		locations = append(locations, Location{Chunk: uint32(i / 8), Slot: uint8(i % 8)})
	}
	sparse := snapshot.AppendFieldRows(nil, locations, "id", nil)
	if len(sparse) != rows {
		t.Fatalf("sparse gather length: got %d want %d", len(sparse), rows)
	}
	for i := range full {
		if string(sparse[i].Bytes()) != string(full[i].Bytes()) {
			t.Fatalf("row %d: sparse %q, full scan %q", i, sparse[i].Bytes(), full[i].Bytes())
		}
	}
}
