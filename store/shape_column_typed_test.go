package store

import (
	"fmt"
	"math"
	"runtime"
	"sync"
	"testing"

	"github.com/thesyncim/vibejson"
)

// The typed drivers promise one thing: every cell equals the per-document
// composition root.Get(name) followed by the Node accessor, with the zero
// cell pinned on every false verdict. These tests hold them to that
// contract differentially over the corpora that force each routing
// transition — the shape_column battery — plus a numeric corpus engineered
// for the accessors' own edges: int64 boundaries, 19- and 20-digit runs,
// fraction and exponent rejects, negative zero, overflowing magnitudes, and
// hard float spellings on both sides of the Eisel-Lemire path. Float cells
// compare bit-for-bit so a rounding or negative-zero divergence cannot
// hide. The standing GOGC gate covers the typed hint parse's unsafe entry
// reads.

// refFieldInt64 is the exact per-document reference for AppendFieldInt64:
// root Get then Node.Int64 on every document in ordinal order, with the
// cell zeroed on any false verdict.
func refFieldInt64(s *Segment, name string) ([]int64, []bool) {
	vals := make([]int64, 0, s.Len())
	oks := make([]bool, 0, s.Len())
	for d := 0; d < s.Len(); d++ {
		var n int64
		var ok bool
		if v, present := s.Doc(d).Root().Get(name); present {
			if n, ok = v.Int64(); !ok {
				n = 0
			}
		}
		vals = append(vals, n)
		oks = append(oks, ok)
	}
	return vals, oks
}

// refFieldFloat64 is the Float64 reference under refFieldInt64's rules.
func refFieldFloat64(s *Segment, name string) ([]float64, []bool) {
	vals := make([]float64, 0, s.Len())
	oks := make([]bool, 0, s.Len())
	for d := 0; d < s.Len(); d++ {
		var f float64
		var ok bool
		if v, present := s.Doc(d).Root().Get(name); present {
			if f, ok = v.Float64(); !ok {
				f = 0
			}
		}
		vals = append(vals, f)
		oks = append(oks, ok)
	}
	return vals, oks
}

// refFieldBool is the Bool reference under refFieldInt64's rules.
func refFieldBool(s *Segment, name string) ([]bool, []bool) {
	vals := make([]bool, 0, s.Len())
	oks := make([]bool, 0, s.Len())
	for d := 0; d < s.Len(); d++ {
		var b bool
		var ok bool
		if v, present := s.Doc(d).Root().Get(name); present {
			b, ok = v.Bool()
		}
		vals = append(vals, b)
		oks = append(oks, ok)
	}
	return vals, oks
}

// checkTypedField runs one query against one set through a cold and a warm
// pass on one cache — the first exercising the sighting-gated fallbacks,
// the second the compiled fast paths — and requires every typed cell to
// match its reference, floats bit-for-bit.
func checkTypedField(t *testing.T, cache *ShapeCache, s *Segment, name, label string) {
	t.Helper()
	wantInts, wantIntOK := refFieldInt64(s, name)
	wantFloats, wantFloatOK := refFieldFloat64(s, name)
	wantBools, wantBoolOK := refFieldBool(s, name)
	for pass := 0; pass < 2; pass++ {
		ints, intOK := cache.AppendFieldInt64(nil, nil, s, name)
		if len(ints) != s.Len() || len(intOK) != s.Len() {
			t.Fatalf("%s pass %d: AppendFieldInt64(%q) grew %d cells, %d flags for %d documents",
				label, pass, name, len(ints), len(intOK), s.Len())
		}
		floats, floatOK := cache.AppendFieldFloat64(nil, nil, s, name)
		bools, boolOK := cache.AppendFieldBool(nil, nil, s, name)
		for i := 0; i < s.Len(); i++ {
			if ints[i] != wantInts[i] || intOK[i] != wantIntOK[i] {
				t.Fatalf("%s pass %d: AppendFieldInt64(%q)[%d] = (%d, %v), Get+Int64 (%d, %v)",
					label, pass, name, i, ints[i], intOK[i], wantInts[i], wantIntOK[i])
			}
			if math.Float64bits(floats[i]) != math.Float64bits(wantFloats[i]) || floatOK[i] != wantFloatOK[i] {
				t.Fatalf("%s pass %d: AppendFieldFloat64(%q)[%d] = (%v/%#x, %v), Get+Float64 (%v/%#x, %v)",
					label, pass, name, i,
					floats[i], math.Float64bits(floats[i]), floatOK[i],
					wantFloats[i], math.Float64bits(wantFloats[i]), wantFloatOK[i])
			}
			if bools[i] != wantBools[i] || boolOK[i] != wantBoolOK[i] {
				t.Fatalf("%s pass %d: AppendFieldBool(%q)[%d] = (%v, %v), Get+Bool (%v, %v)",
					label, pass, name, i, bools[i], boolOK[i], wantBools[i], wantBoolOK[i])
			}
		}
	}
}

// typedColumnNumericSpellings is the accessor-edge battery: every spelling
// a cell can carry, valid JSON throughout. Each entry becomes the "n" field
// of one document of a single flat layout, so the hint path — not the
// fallback — parses them all on the warm pass.
var typedColumnNumericSpellings = []string{
	// int64 boundaries and near misses.
	`9223372036854775807`, `-9223372036854775808`,
	`9223372036854775808`, `-9223372036854775809`,
	// 19- and 20-digit runs: the digit kernel's width edges.
	`1234567890123456789`, `9999999999999999999`,
	`12345678901234567890`, `99999999999999999999`,
	`18446744073709551615`, `18446744073709551616`,
	// Small and signed, negative zero in integer and float spellings.
	`0`, `-0`, `-0.0`, `-0e7`, `7`, `-7`, `42`,
	// Integer-valued non-integer spellings: Int64 rejects, Float64 accepts.
	`1.0`, `-3.0`, `1e2`, `-2E3`, `5e-1`,
	// Fractions and exponents across the float ladder: exact-multiply
	// envelope, fixed-decimal shortcut, Eisel-Lemire, and strconv deferrals.
	`0.1`, `-2.5`, `3.141592653589793`, `48.858370123456789`,
	`-122.41941550000001`, `1.7976931348623157e308`, `2.2250738585072014e-308`,
	`5e-324`, `2.4703282292062327e-324`, `9007199254740993`,
	`1234567890.12345678901234567890`, `7.2057594037927933e16`,
	// Out of float64 range: Float64 rejects, the cell must be zero.
	`1e999`, `-1e999`, `1e309`, `-1.7976931348623159e308`,
	// Underflow to zero: in range, accepted.
	`1e-999`, `-1e-999`,
}

// typedColumnNumericSet builds one flat homogeneous layout whose "n" member
// cycles the numeric spellings and whose "mix" member cycles the value
// kinds — null, booleans, strings, containers — so one set exercises every
// verdict on both the positional and fallback paths.
func typedColumnNumericSet(t *testing.T, hashKeys bool) *Segment {
	t.Helper()
	mixes := []string{`null`, `true`, `false`, `"12"`, `""`, `[1,2]`, `{"x":1}`, `17`, `2.5`}
	docs := make([]string, 0, len(typedColumnNumericSpellings))
	for i, spelling := range typedColumnNumericSpellings {
		docs = append(docs, fmt.Sprintf(`{"n":%s,"mix":%s,"pad":%d}`,
			spelling, mixes[i%len(mixes)], i))
	}
	return shapeColumnSegment(t, docs, hashKeys)
}

// TestAppendFieldTypedNumericEdges is the accessor-edge gate: over the
// numeric corpus, both enrichments, and every member (plus an absent
// spelling), typed cells match the Get-plus-accessor reference exactly.
func TestAppendFieldTypedNumericEdges(t *testing.T) {
	for _, hashKeys := range []bool{false, true} {
		set := typedColumnNumericSet(t, hashKeys)
		var cache ShapeCache
		for _, name := range []string{"n", "mix", "pad", "absent", ""} {
			checkTypedField(t, &cache, set, name,
				fmt.Sprintf("numeric hashKeys=%v", hashKeys))
		}
	}
}

// TestAppendFieldTypedDifferential runs the typed drivers over the routing
// battery — the same corpora that gate AppendField — so every internal
// transition (hint runs, alternation, suffix duplicates, sticky absence,
// hunt backoff, non-flat and non-object roots) is crossed with the typed
// cell contract, under both enrichments.
func TestAppendFieldTypedDifferential(t *testing.T) {
	for label, docs := range shapeColumnCorpora() {
		for _, hashKeys := range []bool{false, true} {
			set := shapeColumnSegment(t, docs, hashKeys)
			var cache ShapeCache
			for _, q := range shapeColumnQueries(set) {
				checkTypedField(t, &cache, set, q,
					fmt.Sprintf("%s hashKeys=%v", label, hashKeys))
			}
		}
	}
}

// typedColumnMixedValiditySet is the engine's dirty corpus: one dominant
// layout whose field is a plain integer, interleaved with equal-width
// layouts where the field is missing, null, or a string — roughly 65%
// valid, 20% absent, 10% null, 5% string.
func typedColumnMixedValiditySet(t testing.TB, count int) *Segment {
	t.Helper()
	docs := make([]string, 0, count)
	for i := 0; i < count; i++ {
		switch {
		case i%20 >= 13 && i%20 <= 16: // absent: same width, no "v"
			docs = append(docs, fmt.Sprintf(`{"pad":%d,"a":1,"b":2}`, i))
		case i%20 == 17 || i%20 == 18: // null-valued, same layout as valid
			docs = append(docs, fmt.Sprintf(`{"v":null,"a":1,"b":%d}`, i))
		case i%20 == 19: // string-typed, same layout as valid
			docs = append(docs, fmt.Sprintf(`{"v":"x-%d","a":1,"b":%d}`, i, i))
		default:
			docs = append(docs, fmt.Sprintf(`{"v":%d,"a":1,"b":%d}`, i*3-1, i))
		}
	}
	return shapeColumnSegment(t, docs, true)
}

// TestAppendFieldTypedMixedValidity pins the validity handling on the mixed
// corpus and proves the dense-aggregation property the contract sells: the
// masked sum over (cells, validity) equals the reference sum, and the
// unmasked sum equals it too because false cells are zero.
func TestAppendFieldTypedMixedValidity(t *testing.T) {
	set := typedColumnMixedValiditySet(t, 240)
	var cache ShapeCache
	checkTypedField(t, &cache, set, "v", "mixed validity")

	cells, valid := cache.AppendFieldInt64(nil, nil, set, "v")
	var masked, unmasked, want int64
	var wantCount, gotCount int
	for i := range cells {
		unmasked += cells[i]
		if valid[i] {
			masked += cells[i]
			gotCount++
		}
	}
	for d := 0; d < set.Len(); d++ {
		if v, ok := set.Doc(d).Root().Get("v"); ok {
			if n, ok := v.Int64(); ok {
				want += n
				wantCount++
			}
		}
	}
	if masked != want || unmasked != want {
		t.Fatalf("sum over typed column = %d masked, %d unmasked, want %d", masked, unmasked, want)
	}
	if gotCount != wantCount {
		t.Fatalf("valid count = %d, want %d", gotCount, wantCount)
	}
}

// TestAppendFieldTypedGrowthContract pins the slice contract: cells and
// flags grow in lockstep by s.Len(), prior contents of both slices are
// untouched even at different starting lengths, and the empty set grows
// nothing.
func TestAppendFieldTypedGrowthContract(t *testing.T) {
	set := shapeColumnSegment(t, []string{`{"a":1}`, `{"a":2}`, `[3]`}, true)
	var cache ShapeCache

	dst := []int64{-7, -8}
	valid := []bool{true}
	dst, valid = cache.AppendFieldInt64(dst, valid, set, "a")
	if len(dst) != 2+set.Len() || len(valid) != 1+set.Len() {
		t.Fatalf("lockstep growth broken: %d cells, %d flags", len(dst), len(valid))
	}
	if dst[0] != -7 || dst[1] != -8 || valid[0] != true {
		t.Fatal("prior contents disturbed")
	}
	if got := dst[2:]; got[0] != 1 || got[1] != 2 || got[2] != 0 {
		t.Fatalf("appended cells = %v, want [1 2 0]", got)
	}
	if got := valid[1:]; !got[0] || !got[1] || got[2] {
		t.Fatalf("appended flags = %v, want [true true false]", got)
	}

	var empty Segment
	if dst, valid = cache.AppendFieldInt64(dst[:0], valid[:0], &empty, "a"); len(dst) != 0 || len(valid) != 0 {
		t.Fatalf("empty set grew cells: %d, %d", len(dst), len(valid))
	}
	if f, fv := cache.AppendFieldFloat64(nil, nil, &empty, "a"); f != nil || fv != nil {
		t.Fatalf("empty set: AppendFieldFloat64 = %v, %v, want nil, nil", f, fv)
	}
	if b, bv := cache.AppendFieldBool(nil, nil, &empty, "a"); b != nil || bv != nil {
		t.Fatalf("empty set: AppendFieldBool = %v, %v, want nil, nil", b, bv)
	}
}

// TestAppendFieldTypedSteadyAllocs proves the steady-state allocation
// contract: warm typed passes with dst and valid capacity in place allocate
// nothing.
func TestAppendFieldTypedSteadyAllocs(t *testing.T) {
	set := shapeColumnClusteredDocs(64, 8, t)
	var cache ShapeCache
	ints := make([]int64, 0, set.Len())
	floats := make([]float64, 0, set.Len())
	bools := make([]bool, 0, set.Len())
	valid := make([]bool, 0, set.Len())
	ints, valid = cache.AppendFieldInt64(ints[:0], valid[:0], set, "ts")
	if n := testing.AllocsPerRun(20, func() {
		ints, valid = cache.AppendFieldInt64(ints[:0], valid[:0], set, "ts")
		floats, valid = cache.AppendFieldFloat64(floats[:0], valid[:0], set, "ts")
		bools, valid = cache.AppendFieldBool(bools[:0], valid[:0], set, "flags")
	}); n != 0 {
		t.Fatalf("warm typed extraction allocated %.1f times per pass", n)
	}
}

// TestGCCorruptionShapeColumnTyped is the standing corruption gate for the
// typed drivers' unsafe tape reads: the routed value entry's info-word test
// and the digit kernels both read entry and source words by offset
// arithmetic while the garbage collector may move stacks. Concurrent
// workers rebuild document sets whose entry arenas end in sentinel-poisoned
// free tails, extract typed columns under forced stack movement and GC,
// verify every cell against the exact reference, and prove the sentinels
// are never read into a cell nor overwritten. Stress:
//
//	GOGC=1 GOEXPERIMENT=simd gotip test -run TestGCCorruptionShapeColumnTyped -count=5 -cpu=1,4,8 ./
func TestGCCorruptionShapeColumnTyped(t *testing.T) {
	// The corpus opens heterogeneous and ends in the homogeneous run so the
	// final documents — those adjacent to the poisoned tail — are served by
	// the hint fast path, whose suffix scan and cell parse run to the
	// tape's last entries. Cell values cover both digit-kernel widths and
	// the fallback verdicts.
	var docs []string
	for i := 0; i < 8; i++ {
		docs = append(docs, fmt.Sprintf(`{"pre%d":%d,"nested":{"x":1}}`, i, i))
	}
	for i := 0; i < 24; i++ {
		docs = append(docs, fmt.Sprintf(
			`{"q":%d,"f":%d.%02d,"flag":%t,"w":922337203685477580%d}`,
			i*7-3, i, i%97, i%3 == 0, i%10))
	}
	names := []string{"q", "f", "flag", "w", "absent"}
	type wantCol struct {
		ints    []int64
		intOK   []bool
		floats  []float64
		floatOK []bool
		bools   []bool
		boolOK  []bool
	}
	reference := map[string]*wantCol{}
	{
		set := shapeColumnSegment(t, docs, true)
		for _, name := range names {
			w := &wantCol{}
			w.ints, w.intOK = refFieldInt64(set, name)
			w.floats, w.floatOK = refFieldFloat64(set, name)
			w.bools, w.boolOK = refFieldBool(set, name)
			reference[name] = w
		}
	}

	sentinel := vibejson.IndexEntry{Start: ^uint32(0), End: ^uint32(0), Next: ^uint32(0), Info: ^uint32(0)}
	workers := runtime.GOMAXPROCS(0) * 2
	const iters = 32
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			var cache ShapeCache
			ints := make([]int64, 0, len(docs))
			floats := make([]float64, 0, len(docs))
			bools := make([]bool, 0, len(docs))
			valid := make([]bool, 0, len(docs))
			for it := 0; it < iters; it++ {
				forceStackMovement(48+id, it)
				set, err := buildShapeColumnSet(docs, true)
				if err != nil {
					errs <- fmt.Errorf("worker %d iter %d: %v", id, it, err)
					return
				}
				// Poison the entry arena's free tail: sentinels behind the
				// final document, where an over-running scan or cell parse
				// would land.
				tail := set.entryChunk[len(set.entryChunk):cap(set.entryChunk)]
				for i := range tail {
					tail[i] = sentinel
				}
				for _, name := range names {
					want := reference[name]
					ints, valid = cache.AppendFieldInt64(ints[:0], valid[:0], set, name)
					for i := range ints {
						if ints[i] != want.ints[i] || valid[i] != want.intOK[i] {
							errs <- fmt.Errorf("worker %d iter %d: AppendFieldInt64(%q)[%d] = (%d, %v), want (%d, %v)",
								id, it, name, i, ints[i], valid[i], want.ints[i], want.intOK[i])
							return
						}
					}
					floats, valid = cache.AppendFieldFloat64(floats[:0], valid[:0], set, name)
					for i := range floats {
						if math.Float64bits(floats[i]) != math.Float64bits(want.floats[i]) || valid[i] != want.floatOK[i] {
							errs <- fmt.Errorf("worker %d iter %d: AppendFieldFloat64(%q)[%d] = (%v, %v), want (%v, %v)",
								id, it, name, i, floats[i], valid[i], want.floats[i], want.floatOK[i])
							return
						}
					}
					bools, valid = cache.AppendFieldBool(bools[:0], valid[:0], set, name)
					for i := range bools {
						if bools[i] != want.bools[i] || valid[i] != want.boolOK[i] {
							errs <- fmt.Errorf("worker %d iter %d: AppendFieldBool(%q)[%d] = (%v, %v), want (%v, %v)",
								id, it, name, i, bools[i], valid[i], want.bools[i], want.boolOK[i])
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
