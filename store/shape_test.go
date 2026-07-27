package store

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// A ShapeCache promises Node.Get semantics for compiled field access on flat
// objects: names match by decoded spelling, the last duplicate wins, and a
// document that does not match a shape fails Resolve or In safely rather than
// misreading a field. These tests hold shapes to that contract from six
// directions: a differential against Get over the adversarial corpus on
// enriched and unenriched tapes, batch identity over homogeneous document
// sets, the sighting gate's decline-then-compile sequence, mismatched
// documents against a foreign shape, a constructed fingerprint collision
// pinning the documented residual deviation, and the standing GOGC
// corruption gate for the offset-derived entry pointers.

// shapeFlatDoc builds a flat object of width members — scalar values only, so
// the object stays shape-resolvable — whose keys mix lengths, escaped
// spellings, and duplicates like keyHashWideDoc's.
func shapeFlatDoc(width int, pad string) string {
	var sb strings.Builder
	sb.WriteString("{")
	for i := 0; i < width; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		switch {
		case i%13 == 12:
			fmt.Fprintf(&sb, `"member%04d":`, i-1) // duplicate of the previous key
		case i%5 == 4:
			fmt.Fprintf(&sb, `"m\tember%04d":`, i) // escaped spelling
		case i%3 == 2:
			fmt.Fprintf(&sb, `"very_long_member_key_%04d_with_tail":`, i)
		default:
			fmt.Fprintf(&sb, `"member%04d":`, i)
		}
		if pad != "" {
			fmt.Fprintf(&sb, `"%s%d"`, pad, i)
		} else {
			fmt.Fprintf(&sb, "%d", i)
		}
	}
	sb.WriteString("}")
	return sb.String()
}

// nodeIsFlatObject reports the shape-resolvability expectation for one node
// through the public surface alone: an object whose every member value spans
// a single index entry — a scalar or an empty container — within the width
// limit. That is exactly the layout whose member offsets are fixed.
func nodeIsFlatObject(v vibejson.Node) bool {
	iter, ok := v.ObjectIter()
	if !ok {
		return false
	}
	if n, _ := v.ObjectLen(); n > shapeMaxFields {
		return false
	}
	for {
		_, value, ok := iter.Next()
		if !ok {
			return true
		}
		if n, ok := value.ObjectLen(); ok && n > 0 {
			return false
		}
		if n, ok := value.ArrayLen(); ok && n > 0 {
			return false
		}
	}
}

// resolveCompiled resolves v through the sighting gate: Resolve declines the
// first sighting of a new layout and compiles on the second, so one retry
// yields the final verdict whether the layout was fresh or already cached.
func resolveCompiled(cache *ShapeCache, v vibejson.Node) (Shape, bool) {
	if shape, ok := cache.Resolve(v); ok {
		return shape, true
	}
	return cache.Resolve(v)
}

// checkShapeDifferential resolves one object node and drives Field and In
// against Node.Get over the full query battery: presence verdicts must agree
// and every present field must return the identical entry.
func checkShapeDifferential(t *testing.T, cache *ShapeCache, v vibejson.Node, label string) {
	t.Helper()
	shape, ok := resolveCompiled(cache, v)
	if want := nodeIsFlatObject(v); ok != want {
		t.Fatalf("%s: Resolve = %v, flat-object expectation %v", label, ok, want)
	}
	if !ok {
		return
	}
	if n, _ := v.ObjectLen(); shape.Len() != n {
		t.Fatalf("%s: Shape.Len = %d, ObjectLen %d", label, shape.Len(), n)
	}
	for _, q := range keyHashQuerySet(v) {
		ref, refOK := shape.Field(q)
		want, wantOK := v.Get(q)
		if refOK != wantOK {
			t.Fatalf("%s: Field(%q) = %v, Get %v", label, q, refOK, wantOK)
		}
		if !refOK {
			if _, ok := ref.In(v); ok {
				t.Fatalf("%s: zero FieldRef for %q resolved a value", label, q)
			}
			continue
		}
		got, gotOK := ref.In(v)
		if !gotOK || got.Entry != want.Entry {
			t.Fatalf("%s: Field(%q).In = (%p, %v), Get (%p, %v)",
				label, q, got.Entry, gotOK, want.Entry, wantOK)
		}
	}
}

// TestShapeResolveDifferential is the zero-regression gate for shape-compiled
// access: on every entry of every corpus document, enriched and unenriched,
// Resolve accepts exactly the flat objects, and each accepted shape's Field
// and In return entry-identical results to Node.Get across the query battery
// of every decoded key and absent neighbours. Duplicates, escaped and unicode
// spellings, and the empty key ride along from the corpus.
func TestShapeResolveDifferential(t *testing.T) {
	docs := append([]string{}, keyHashCorpus...)
	docs = append(docs,
		shapeFlatDoc(16, ""),
		shapeFlatDoc(64, "pad-value-"),
		shapeFlatDoc(512, ""),
		keyHashWideDoc(64, ""),        // nested values: exercises the non-flat reject
		`{"a":{},"b":[],"c":1}`,       // empty containers span one entry: still flat
		`{"a":{"x":1},"b":[2],"c":3}`, // the same keys with children: not flat
	)
	for _, hashKeys := range []bool{false, true} {
		for _, doc := range docs {
			src := []byte(doc)
			need, err := vibejson.RequiredIndexEntries(src)
			if err != nil {
				t.Fatalf("RequiredIndexEntries(%.60q): %v", doc, err)
			}
			tape, err := vibejson.BuildIndexOptions(src, make([]vibejson.IndexEntry, need), document.IndexOptions{HashKeys: hashKeys})
			if err != nil {
				t.Fatalf("BuildIndexOptions(%.60q): %v", doc, err)
			}
			var cache ShapeCache
			for i := range tape.Entries {
				if tape.Entries[i].Kind() != document.Object {
					continue
				}
				node := vibejson.Node{Src: unsafe.SliceData(tape.Src), Entry: &tape.Entries[i]}
				checkShapeDifferential(t, &cache, node, fmt.Sprintf("hashKeys=%v %.40q entry %d", hashKeys, doc, i))
			}
		}
	}
}

// TestShapeSameShapeBatch drives the intended engine loop over a homogeneous
// Segment: every document resolves to the identical Shape (pointer equality
// through ==), cached FieldRefs extract entry-identical values to Get on
// every document, and enriched and unenriched sets agree.
func TestShapeSameShapeBatch(t *testing.T) {
	fields := []string{"id", "name", "active", "score", "region", "tier", "ts", "flags"}
	for _, hashKeys := range []bool{false, true} {
		var set Segment
		set.Options = document.IndexOptions{HashKeys: hashKeys}
		for i := 0; i < 256; i++ {
			doc := fmt.Sprintf(
				`{"id":%d,"name":"user-%04d","active":%t,"score":%d.%02d,"region":"eu-west-%d","tier":%d,"ts":%d,"flags":null}`,
				i, i, i%2 == 0, i%100, i%97, i%3, i%5, 1700000000+i)
			if _, err := set.Append([]byte(doc)); err != nil {
				t.Fatal(err)
			}
		}
		var cache ShapeCache
		first, ok := resolveCompiled(&cache, set.Doc(0).Root())
		if !ok {
			t.Fatalf("hashKeys=%v: Resolve declined the batch shape", hashKeys)
		}
		refs := make([]FieldRef, len(fields))
		for i, name := range fields {
			ref, ok := first.Field(name)
			if !ok {
				t.Fatalf("hashKeys=%v: Field(%q) missing", hashKeys, name)
			}
			refs[i] = ref
		}
		for d := 0; d < set.Len(); d++ {
			root := set.Doc(d).Root()
			shape, ok := cache.Resolve(root)
			if !ok || shape != first {
				t.Fatalf("hashKeys=%v doc %d: Resolve = (%v, %v), want the batch shape", hashKeys, d, shape, ok)
			}
			for i, name := range fields {
				got, gotOK := refs[i].In(root)
				want, wantOK := root.Get(name)
				if gotOK != wantOK || got.Entry != want.Entry {
					t.Fatalf("hashKeys=%v doc %d: In(%q) = (%p, %v), Get (%p, %v)",
						hashKeys, d, name, got.Entry, gotOK, want.Entry, wantOK)
				}
			}
		}
	}
}

// TestShapeCrossEnrichment pins hash agreement between the fold's two
// sources: one document built enriched and unenriched must resolve to the
// same Shape in one cache, so mixed batches share compiled shapes.
func TestShapeCrossEnrichment(t *testing.T) {
	src := []byte(shapeFlatDoc(24, "v"))
	need, err := vibejson.RequiredIndexEntries(src)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := vibejson.BuildIndexOptions(src, make([]vibejson.IndexEntry, need), document.IndexOptions{})
	if err != nil {
		t.Fatal(err)
	}
	enriched, err := vibejson.BuildIndexOptions(append([]byte(nil), src...), make([]vibejson.IndexEntry, need), document.IndexOptions{HashKeys: true})
	if err != nil {
		t.Fatal(err)
	}
	var cache ShapeCache
	a, okA := resolveCompiled(&cache, plain.Root())
	// The enriched build must hit the compiled shape on its first sighting:
	// the stored-hash fold and the inline fold agree exactly.
	b, okB := cache.Resolve(enriched.Root())
	if !okA || !okB || a != b {
		t.Fatalf("enriched and unenriched builds resolve to distinct shapes: (%v,%v) (%v,%v)", a, okA, b, okB)
	}
}

// TestShapeSightingGate pins the compilation gate: a fresh layout declines
// its first sighting, compiles on its second, and afterwards resolves to the
// same record every time.
func TestShapeSightingGate(t *testing.T) {
	tape, err := vibejson.BuildIndexOptions([]byte(`{"a":1,"b":2}`), make([]vibejson.IndexEntry, 8), document.IndexOptions{HashKeys: true})
	if err != nil {
		t.Fatal(err)
	}
	var cache ShapeCache
	if _, ok := cache.Resolve(tape.Root()); ok {
		t.Fatal("first sighting compiled a shape")
	}
	shape, ok := cache.Resolve(tape.Root())
	if !ok {
		t.Fatal("second sighting declined")
	}
	for i := 0; i < 3; i++ {
		again, ok := cache.Resolve(tape.Root())
		if !ok || again != shape {
			t.Fatalf("sighting %d: Resolve = (%v, %v), want the compiled shape", i+3, again, ok)
		}
	}
}

// TestShapeMismatchSafety holds In to its self-checking contract against
// documents that do not match the compiled shape: a wrong width, a reordered
// or renamed key at the field's position, or a value kind that breaks the
// flat layout must report false, and whenever In does return a value it must
// be the entry Get returns for that name — never another field's value.
func TestShapeMismatchSafety(t *testing.T) {
	base := `{"alpha":1,"beta":2,"gamma":3,"delta":4}`
	variants := []string{
		`{"alpha":9,"beta":8,"gamma":7,"delta":6,"extra":5}`, // extra field
		`{"alpha":9,"beta":8,"gamma":7}`,                     // missing field
		`{"delta":9,"gamma":8,"beta":7,"alpha":6}`,           // fully reversed
		`{"alpha":9,"gamma":8,"beta":7,"delta":6}`,           // middle pair swapped
		`{"alpha":9,"beta":8,"gamma":{"x":7},"delta":6}`,     // value kind breaks flatness
		`{"alpha":9,"betaX":8,"gamma":7,"delta":6}`,          // renamed key, same width
		`{"alphaa":9,"beta":8,"gamma":7,"delta":6}`,          // same-length rename
		`[1,2,3]`, // wrong kind entirely
		`{}`,      // empty object
		`"alpha"`, // scalar
	}
	names := []string{"alpha", "beta", "gamma", "delta"}

	var cache ShapeCache
	baseTape, err := vibejson.BuildIndexOptions([]byte(base), make([]vibejson.IndexEntry, 16), document.IndexOptions{HashKeys: true})
	if err != nil {
		t.Fatal(err)
	}
	shape, ok := resolveCompiled(&cache, baseTape.Root())
	if !ok {
		t.Fatal("Resolve declined the base shape")
	}
	refs := make([]FieldRef, len(names))
	for i, name := range names {
		if refs[i], ok = shape.Field(name); !ok {
			t.Fatalf("Field(%q) missing", name)
		}
	}
	for _, variant := range variants {
		for _, hashKeys := range []bool{false, true} {
			src := []byte(variant)
			tape, err := vibejson.BuildIndexOptions(src, make([]vibejson.IndexEntry, len(src)+2), document.IndexOptions{HashKeys: hashKeys})
			if err != nil {
				t.Fatal(err)
			}
			root := tape.Root()
			for i, name := range names {
				got, gotOK := refs[i].In(root)
				if !gotOK {
					continue // safe failure; the caller falls back to Get
				}
				want, wantOK := root.Get(name)
				if !wantOK || got.Entry != want.Entry {
					t.Fatalf("hashKeys=%v variant %q: In(%q) returned (%p), Get (%p, %v)",
						hashKeys, variant, name, got.Entry, want.Entry, wantOK)
				}
			}
		}
	}
}

// findKeyHashCollision birthday-searches equal-length keys for a 32-bit
// content-hash collision, the exact ingredient a fingerprint collision needs.
// Around 2^16.5 draws are expected before the first collision, so the search
// is quick and deterministic.
func findKeyHashCollision(t *testing.T) (string, string) {
	t.Helper()
	seen := make(map[uint32]string, 1<<18)
	for i := 0; i < 1<<21; i++ {
		key := fmt.Sprintf("c%08x", i)
		h := vibejson.HashKey(key)
		if prev, ok := seen[h]; ok {
			return prev, key
		}
		seen[h] = key
	}
	t.Fatal("no 32-bit key-hash collision found in 2^21 draws")
	return "", ""
}

// TestShapeFingerprintCollision constructs the documented residual case: two
// distinct key sequences with identical fingerprints, via an equal-length
// 32-bit key-hash collision. The colliding document routes to the foreign
// shape, and the test pins the exact safety boundary — In never returns a
// value keyed by a different spelling, misrouted fields fail closed for the
// Get fallback, and the only reachable deviation is duplicate-ordinal
// selection of the queried field itself, as Resolve's trust analysis states.
func TestShapeFingerprintCollision(t *testing.T) {
	p, q := findKeyHashCollision(t)
	if len(p) != len(q) || p == q || vibejson.HashKey(p) != vibejson.HashKey(q) {
		t.Fatalf("collision search returned an invalid pair %q %q", p, q)
	}
	build := func(doc string) vibejson.Index {
		src := []byte(doc)
		tape, err := vibejson.BuildIndexOptions(src, make([]vibejson.IndexEntry, len(src)+2), document.IndexOptions{HashKeys: true})
		if err != nil {
			t.Fatal(err)
		}
		return tape
	}
	docA := build(fmt.Sprintf(`{"%s":1,"%s":2}`, p, q))
	docB := build(fmt.Sprintf(`{"%s":3,"%s":4}`, p, p)) // collides: q vs p at position 1

	var cache ShapeCache
	shapeA, ok := resolveCompiled(&cache, docA.Root())
	if !ok {
		t.Fatal("Resolve declined the base document")
	}
	// The colliding document routes to the compiled foreign shape on its
	// first sighting, exactly as a same-shape document would.
	shapeB, ok := cache.Resolve(docB.Root())
	if !ok || shapeB != shapeA {
		t.Fatalf("constructed sequences did not collide: %v %v", shapeA, shapeB)
	}

	refP, _ := shapeA.Field(p)
	refQ, _ := shapeA.Field(q)
	// Field q sits at ordinal 1; docB holds p there, so the byte verification
	// fails closed and the Get fallback answers correctly.
	if _, ok := refQ.In(docB.Root()); ok {
		t.Fatalf("In(%q) returned a value from the colliding document", q)
	}
	if v, ok := docB.Root().Get(q); ok {
		t.Fatalf("fallback Get(%q) = %v on a document without that key", q, v)
	}
	// Field p is the documented deviation: docB duplicates p, the compiled
	// ordinal (0, p's last position in the shape) byte-matches, and In
	// returns that ordinal's value while Get returns the later duplicate's.
	got, ok := refP.In(docB.Root())
	if !ok {
		t.Fatalf("In(%q) failed on a byte-matching position", p)
	}
	gotN, _ := got.Int64()
	want, _ := docB.Root().Get(p)
	wantN, _ := want.Int64()
	if gotN != 3 || wantN != 4 {
		t.Fatalf("collision deviation moved: In = %d (want 3), Get = %d (want 4)", gotN, wantN)
	}
}

// TestShapeZeroValues pins the zero-value contracts: the zero Shape and zero
// FieldRef resolve nothing, a zero cache accepts documents immediately, and
// the empty object compiles to a fieldless shape.
func TestShapeZeroValues(t *testing.T) {
	if _, ok := (Shape{}).Field("a"); ok {
		t.Fatal("zero Shape resolved a field")
	}
	if n := (Shape{}).Len(); n != 0 {
		t.Fatalf("zero Shape Len = %d", n)
	}
	var cache ShapeCache
	if _, ok := cache.Resolve(vibejson.Node{}); ok {
		t.Fatal("Resolve accepted the zero Node")
	}
	tape, err := vibejson.BuildIndex([]byte(`{}`), make([]vibejson.IndexEntry, 4))
	if err != nil {
		t.Fatal(err)
	}
	shape, ok := resolveCompiled(&cache, tape.Root())
	if !ok || shape.Len() != 0 {
		t.Fatalf("empty object: Resolve = (%v, %v)", shape, ok)
	}
	if _, ok := shape.Field(""); ok {
		t.Fatal("empty shape resolved a field")
	}
	var ref FieldRef
	if _, ok := ref.In(tape.Root()); ok {
		t.Fatal("zero FieldRef resolved a value")
	}
}

// TestShapeTapeEndBounds resolves and extracts from objects whose entries end
// exactly at the tape's last slot, across widths that place the verified key
// and value at the final entries, with exactly sized entry storage.
func TestShapeTapeEndBounds(t *testing.T) {
	for width := 1; width <= 24; width++ {
		doc := shapeFlatDoc(width, "")
		src := []byte(doc)
		need, err := vibejson.RequiredIndexEntries(src)
		if err != nil {
			t.Fatal(err)
		}
		for _, hashKeys := range []bool{false, true} {
			tape, err := vibejson.BuildIndexOptions(src, make([]vibejson.IndexEntry, need), document.IndexOptions{HashKeys: hashKeys})
			if err != nil {
				t.Fatal(err)
			}
			var cache ShapeCache
			node := tape.Root()
			checkShapeDifferential(t, &cache, node, fmt.Sprintf("width %d hashKeys=%v", width, hashKeys))
		}
	}
}

// TestShapeCacheGrowth forces fingerprint-table doubling and arena chunk
// turnover with hundreds of distinct shapes, then re-resolves each: every
// shape must keep its identity (the same record pointer) and its fields must
// still extract correctly, proving growth moves slots but never records or
// key bytes.
func TestShapeCacheGrowth(t *testing.T) {
	const shapes = 300
	var cache ShapeCache
	tapes := make([]vibejson.Index, shapes)
	first := make([]Shape, shapes)
	for i := 0; i < shapes; i++ {
		doc := fmt.Sprintf(`{"field_%d_a":1,"field_%d_b":2,"shared":%d}`, i, i, i)
		src := []byte(doc)
		tape, err := vibejson.BuildIndexOptions(src, make([]vibejson.IndexEntry, len(src)+2), document.IndexOptions{HashKeys: true})
		if err != nil {
			t.Fatal(err)
		}
		tapes[i] = tape
		shape, ok := resolveCompiled(&cache, tape.Root())
		if !ok {
			t.Fatalf("shape %d declined", i)
		}
		first[i] = shape
	}
	for i := 0; i < shapes; i++ {
		shape, ok := cache.Resolve(tapes[i].Root())
		if !ok || shape != first[i] {
			t.Fatalf("shape %d lost identity after growth", i)
		}
		ref, ok := shape.Field("shared")
		if !ok {
			t.Fatalf("shape %d lost its field table", i)
		}
		got, ok := ref.In(tapes[i].Root())
		n, _ := got.Int64()
		if !ok || n != int64(i) {
			t.Fatalf("shape %d: In(shared) = (%d, %v), want %d", i, n, ok, i)
		}
	}
}

// TestGCCorruptionShapeResolve is the standing corruption gate for the shape
// path, whose fold, compile, and In all read tape entries and source bytes
// through unsafe offset arithmetic. Concurrent per-worker caches resolving
// rebuilt tapes under forced stack movement and GC, with sentinel entries
// past the tape, prove the path never reads or writes outside the entry
// slice and that long-lived compiled shapes stay stable while arenas grow.
// Stress:
//
//	GOGC=1 GOEXPERIMENT=simd gotip test -run TestGCCorruptionShapeResolve -count=5 -cpu=1,4,8 ./
func TestGCCorruptionShapeResolve(t *testing.T) {
	src := []byte(shapeFlatDoc(64, "value-"))
	need, err := vibejson.RequiredIndexEntries(src)
	if err != nil {
		t.Fatal(err)
	}
	opts := document.IndexOptions{HashKeys: true}
	reference, err := vibejson.BuildIndexOptions(src, make([]vibejson.IndexEntry, need), opts)
	if err != nil {
		t.Fatal(err)
	}
	names := []string{"member0000", "member0011", "very_long_member_key_0032_with_tail", "m\tember0059"}
	want := make([]string, len(names))
	for i, name := range names {
		node, ok := reference.Root().Get(name)
		if !ok {
			t.Fatalf("reference missing %q", name)
		}
		want[i] = string(node.Raw().Bytes())
	}

	const slack = 8
	sentinel := vibejson.IndexEntry{Start: ^uint32(0), End: ^uint32(0), Next: ^uint32(0), Info: ^uint32(0)}
	workers := runtime.GOMAXPROCS(0) * 2
	const iters = 40
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			var cache ShapeCache
			var shape Shape
			refs := make([]FieldRef, len(names))
			storage := make([]vibejson.IndexEntry, need+slack)
			for it := 0; it < iters; it++ {
				forceStackMovement(48+id, it)
				for i := need; i < len(storage); i++ {
					storage[i] = sentinel
				}
				tape, err := vibejson.BuildIndexOptions(src, storage[:need], opts)
				if err != nil {
					errs <- fmt.Errorf("worker %d iter %d: %v", id, it, err)
					return
				}
				resolved, ok := resolveCompiled(&cache, tape.Root())
				if !ok || (it > 0 && resolved != shape) {
					errs <- fmt.Errorf("worker %d iter %d: Resolve = (%v, %v)", id, it, resolved, ok)
					return
				}
				if it == 0 {
					shape = resolved
					for i, name := range names {
						if refs[i], ok = shape.Field(name); !ok {
							errs <- fmt.Errorf("worker %d: Field(%q) missing", id, name)
							return
						}
					}
					// Grow the cache arenas past the compiled shape so later
					// iterations exercise a long-lived record amid turnover.
					for extra := 0; extra < 64; extra++ {
						doc := fmt.Sprintf(`{"w%d_x%d":1,"w%d_y%d":"padpadpadpadpadpad"}`, id, extra, id, extra)
						extraTape, err := vibejson.BuildIndexOptions([]byte(doc), make([]vibejson.IndexEntry, len(doc)+2), opts)
						if err != nil {
							errs <- err
							return
						}
						if _, ok := resolveCompiled(&cache, extraTape.Root()); !ok {
							errs <- fmt.Errorf("worker %d: filler shape %d declined", id, extra)
							return
						}
					}
				}
				for i := range names {
					node, ok := refs[i].In(tape.Root())
					if !ok || string(node.Raw().Bytes()) != want[i] {
						errs <- fmt.Errorf("worker %d iter %d: In(%q) = (%v, %q), want %q",
							id, it, names[i], ok, node.Raw().Bytes(), want[i])
						return
					}
				}
				for i := need; i < len(storage); i++ {
					if storage[i] != sentinel {
						errs <- fmt.Errorf("worker %d iter %d: sentinel %d overwritten", id, it, i)
						return
					}
				}
				if it%8 == 0 {
					runtime.GC()
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
