package query

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
)

// compilerDocs is the collection every compiler test answers over. It carries
// each shape the lowering has a distinct branch for — a present scalar, an
// explicit null, an absent path, a bool, a string, and a nested object — so a
// plan that borrows the wrong storage produces a visibly different answer
// rather than an accidentally equal one.
var compilerDocs = []string{
	`{"team":"red","score":1,"tier":"pro","live":true,"obj":{"x":1},"profile":{"country":"pt"}}`,
	`{"team":"blue","score":12,"tier":"free","live":false,"obj":{"x":2},"profile":{"country":"es"}}`,
	`{"team":"red","score":9007199254740993,"tier":"team","obj":{"x":1},"profile":{"country":"pt"}}`,
	`{"team":"blue","score":null,"tier":"pro","profile":{"country":"fr"}}`,
	`{"team":"green","score":4.5,"live":true,"obj":{"x":3}}`,
	`{"team":"red","score":-2,"tier":"pro","obj":{"x":1},"profile":{"country":"es"}}`,
}

// compilerQueries spans the document grammar: projections and aliases, every
// aggregate, sibling conjunction, the comparison and membership operators,
// containment, existence and null tests, boolean combinators, grouping,
// ordering in both directions, and a limit.
var compilerQueries = []struct {
	name string
	text string
}{
	{"projection", `{"select":["team","score"]}`},
	{"alias", `{"select":[{"who":"team"},{"total":{"$sum":"score"}}],"groupBy":"team"}`},
	{"aggregates", `{"select":[{"n":{"$count":true}},{"s":{"$sum":"score"}},{"a":{"$avg":"score"}},
	                 {"lo":{"$min":"score"}},{"hi":{"$max":"score"}}]}`},
	{"conjunction", `{"select":["team"],"where":{"tier":"pro","live":true}}`},
	{"comparisons", `{"select":["team"],"where":{"score":{"$gte":1,"$lt":100}}}`},
	{"membership", `{"select":["team","tier"],"where":{"tier":{"$in":["pro","team"]}}}`},
	{"membership-null", `{"select":["team"],"where":{"score":{"$in":[1,null,-2]}}}`},
	{"negated-membership", `{"select":["team"],"where":{"tier":{"$nin":["free"]}}}`},
	{"containment", `{"select":["team"],"where":{"obj":{"$contains":{"x":1}}}}`},
	{"nested-path", `{"select":["profile.country"],"where":{"profile.country":"pt"}}`},
	{"pointer-path", `{"select":["/profile/country"],"where":{"/obj/x":1}}`},
	{"existence", `{"select":["team"],"where":{"tier":{"$exists":false}}}`},
	{"null-test", `{"select":["team"],"where":{"score":null}}`},
	{"boolean", `{"select":["team"],"where":{"$or":[{"tier":"pro"},{"tier":"free"}]}}`},
	{"disjoint-equalities", `{"select":["team"],"where":{"$or":[{"team":"red"},{"team":"blue"},{"score":1}]}}`},
	{"negation", `{"select":["team"],"where":{"$not":{"tier":"pro"}}}`},
	{"grouped", `{"select":["team",{"n":{"$count":true}},{"s":{"$sum":"score"}}],
	              "groupBy":"team","orderBy":[{"team":"desc"}]}`},
	{"ordered-limited", `{"select":["team","score"],"orderBy":["score"],"limit":3}`},
	{"exact-integer", `{"select":["team"],"where":{"score":9007199254740993}}`},
	{"float", `{"select":["team"],"where":{"score":{"$gt":4.4}}}`},
	{"whole-document", `{"where":{"team":"red"}}`},
}

// Given the same query document, when one Query is compiled by a Compiler and
// another by the package-level Parse, then the two answer identically. This is
// the property that makes the reusable front end a storage change and not a
// semantics change.
func TestCompilerMatchesPackageParse(t *testing.T) {
	set := mustSegment(t, compilerDocs...)
	var c Compiler
	for _, query := range compilerQueries {
		t.Run(query.name, func(t *testing.T) {
			owned, err := Parse([]byte(query.text))
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			var borrowed Query
			if err := c.Parse(&borrowed, []byte(query.text)); err != nil {
				t.Fatalf("Compiler.Parse: %v", err)
			}
			want := resultKey(mustRun(t, owned, set))
			got := resultKey(mustRun(t, &borrowed, set))
			if got != want {
				t.Fatalf("compiler and package Parse disagree:\n got: %s\nwant: %s", got, want)
			}
		})
	}
}

// The same property for the Go-literal front end, which reaches the lowering
// through a different qvalue backing and never interns a string.
func TestCompilerNewMatchesPackageNew(t *testing.T) {
	set := buildSegment(t, docPool, storageModes[0])
	var c Compiler
	for _, twin := range jsonTwins() {
		t.Run(twin.name, func(t *testing.T) {
			owned, err := New(twin.doc)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			var borrowed Query
			if err := c.New(&borrowed, twin.doc); err != nil {
				t.Fatalf("Compiler.New: %v", err)
			}
			want := resultKey(mustRun(t, owned, set))
			got := resultKey(mustRun(t, &borrowed, set))
			if got != want {
				t.Fatalf("compiler and package New disagree:\n got: %s\nwant: %s", got, want)
			}
		})
	}
}

// Given a Compiler driven through every query in turn, when each Query is used
// before the next compile, then every answer is correct — the supported usage,
// and the one the borrowed-lifetime rule permits.
//
// The unsupported usage is holding two of a Compiler's Queries live at once.
// The design forbids it rather than making it work, and the assertion below
// says so concretely: the second compile hands back the very plan pointer the
// first Query is still holding, so the first Query has become the second one.
// A caller that needs two live Queries uses two Compilers, or the package-level
// Parse, which owns what it returns.
func TestCompilerReuseAcrossQueries(t *testing.T) {
	set := mustSegment(t, compilerDocs...)

	want := make([]string, len(compilerQueries))
	for i, query := range compilerQueries {
		q, err := Parse([]byte(query.text))
		if err != nil {
			t.Fatalf("Parse %s: %v", query.name, err)
		}
		want[i] = resultKey(mustRun(t, q, set))
	}

	// Two passes, so every compile after the first is a warm one refilling
	// arenas the previous query's plan was still pointing into.
	var c Compiler
	var q Query
	for pass := range 2 {
		for i, query := range compilerQueries {
			if err := c.Parse(&q, []byte(query.text)); err != nil {
				t.Fatalf("pass %d: Compiler.Parse %s: %v", pass, query.name, err)
			}
			if got := resultKey(mustRun(t, &q, set)); got != want[i] {
				t.Fatalf("pass %d: %s:\n got: %s\nwant: %s", pass, query.name, got, want[i])
			}
		}
	}

	// The forbidden aliasing, stated as a fact rather than left to chance.
	var first, second Query
	if err := c.Parse(&first, []byte(compilerQueries[0].text)); err != nil {
		t.Fatal(err)
	}
	firstPlan, err := first.compiled()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Parse(&second, []byte(compilerQueries[1].text)); err != nil {
		t.Fatal(err)
	}
	secondPlan, err := second.compiled()
	if err != nil {
		t.Fatal(err)
	}
	if firstPlan != secondPlan {
		t.Fatal("one Compiler handed out two distinct plans; the documented " +
			"invalidation rule no longer describes what it does")
	}
}

// Two Compilers are two independent lifetimes, which is the answer for a
// caller that needs several Queries live at once.
func TestCompilerPerQueryIsIndependent(t *testing.T) {
	set := mustSegment(t, compilerDocs...)
	var ca, cb Compiler
	var qa, qb Query
	if err := ca.Parse(&qa, []byte(compilerQueries[0].text)); err != nil {
		t.Fatal(err)
	}
	if err := cb.Parse(&qb, []byte(compilerQueries[5].text)); err != nil {
		t.Fatal(err)
	}
	gotA, gotB := resultKey(mustRun(t, &qa, set)), resultKey(mustRun(t, &qb, set))

	wantA, err := Parse([]byte(compilerQueries[0].text))
	if err != nil {
		t.Fatal(err)
	}
	wantB, err := Parse([]byte(compilerQueries[5].text))
	if err != nil {
		t.Fatal(err)
	}
	if want := resultKey(mustRun(t, wantA, set)); gotA != want {
		t.Fatalf("first compiler:\n got: %s\nwant: %s", gotA, want)
	}
	if want := resultKey(mustRun(t, wantB, set)); gotB != want {
		t.Fatalf("second compiler:\n got: %s\nwant: %s", gotB, want)
	}
}

// A Query a Compiler produced is still an ordinary compiled Query: immutable
// while it is valid, and therefore safe to execute from several goroutines
// with one Exec each.
func TestCompilerQueryRunsConcurrently(t *testing.T) {
	set := mustSegment(t, compilerDocs...)
	var c Compiler
	var q Query
	if err := c.Parse(&q, []byte(compilerQueries[15].text)); err != nil {
		t.Fatal(err)
	}
	want := resultKey(mustRun(t, &q, set))

	const workers = 8
	keys := make([]string, workers)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var e Exec
			for range 16 {
				if err := q.RunInto(&e, FromSegment(set)); err != nil {
					t.Error(err)
					return
				}
				keys[w] = resultKey(e.Result)
			}
		}()
	}
	wg.Wait()
	for w, got := range keys {
		if got != want {
			t.Fatalf("worker %d:\n got: %s\nwant: %s", w, got, want)
		}
	}
}

// A failed compile must poison dst rather than leave whichever clauses were
// lowered before the failure standing. A caller that ignores the returned
// error and runs the Query anyway has to see the same error, not a plan
// assembled from half of one query and half of the last one.
func TestCompilerFailureInvalidatesDestination(t *testing.T) {
	set := mustSegment(t, compilerDocs...)
	var c Compiler
	var q Query
	if err := c.Parse(&q, []byte(compilerQueries[0].text)); err != nil {
		t.Fatal(err)
	}
	bad := []string{
		`{"select":["team"],"where":{"score":{"$nope":1}}}`,
		`{"select":[]}`,
		`{"select":["team"],"limit":"soon"}`,
		`{"select":["team"],"groupBy":"team","orderBy":["score"]}`,
		`[1,2,3]`,
		`{"select":["team"]`,
	}
	for _, text := range bad {
		t.Run(text, func(t *testing.T) {
			err := c.Parse(&q, []byte(text))
			if err == nil {
				t.Fatalf("Compiler.Parse(%s) succeeded", text)
			}
			if _, runErr := q.Run(FromSegment(set)); runErr == nil {
				t.Fatal("running the failed destination succeeded; it kept a stale plan")
			}
			if prepErr := q.Prepare(); prepErr == nil {
				t.Fatal("Prepare on the failed destination succeeded")
			}
		})
	}

	// And the compiler still works afterwards.
	if err := c.Parse(&q, []byte(compilerQueries[0].text)); err != nil {
		t.Fatalf("compiler unusable after a failure: %v", err)
	}
	owned, err := Parse([]byte(compilerQueries[0].text))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := resultKey(mustRun(t, &q, set)), resultKey(mustRun(t, owned, set)); got != want {
		t.Fatalf("recovery:\n got: %s\nwant: %s", got, want)
	}
}

// Release must leave the Compiler usable, since its whole point is dropping an
// oversized working set without retiring the compiler that grew it.
func TestCompilerReleaseThenReuse(t *testing.T) {
	set := mustSegment(t, compilerDocs...)
	var c Compiler
	var q Query
	for range 3 {
		for _, query := range compilerQueries {
			if err := c.Parse(&q, []byte(query.text)); err != nil {
				t.Fatalf("%s: %v", query.name, err)
			}
			mustRun(t, &q, set)
		}
		c.Release()
	}
	if err := c.Parse(&q, []byte(compilerQueries[0].text)); err != nil {
		t.Fatal(err)
	}
	owned, err := Parse([]byte(compilerQueries[0].text))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := resultKey(mustRun(t, &q, set)), resultKey(mustRun(t, owned, set)); got != want {
		t.Fatalf("after Release:\n got: %s\nwant: %s", got, want)
	}
}

// TestCompilerParseSteadyAllocs is the whole point of the type: a warmed
// Compiler recompiling a query of the shape it was warmed on must refill its
// arenas rather than reallocate them. The first compile is excluded because a
// Query owns its paths, headers, literals, and needles, and that storage has
// to be allocated once.
func TestCompilerParseSteadyAllocs(t *testing.T) {
	var c Compiler
	var q Query
	for _, query := range compilerQueries {
		t.Run(query.name, func(t *testing.T) {
			src := []byte(query.text)
			// Two warm-up compiles: the first grows the arenas, the second
			// populates the compiled-path memo, which is deliberately shared
			// across compilations rather than rewound with them.
			for range 2 {
				if err := c.Parse(&q, src); err != nil {
					t.Fatal(err)
				}
			}
			allocs := testing.AllocsPerRun(50, func() {
				if err := c.Parse(&q, src); err != nil {
					panic(err)
				}
			})
			if allocs != 0 {
				t.Fatalf("warmed Compiler.Parse allocated %.2f times, want 0", allocs)
			}
		})
	}
}

// The Go-literal front end has the same contract, reached without interning a
// single string: a literal document's names and values are already owned by
// the caller.
func TestCompilerNewSteadyAllocs(t *testing.T) {
	var c Compiler
	var q Query
	for _, twin := range jsonTwins() {
		t.Run(twin.name, func(t *testing.T) {
			for range 2 {
				if err := c.New(&q, twin.doc); err != nil {
					t.Fatal(err)
				}
			}
			allocs := testing.AllocsPerRun(50, func() {
				if err := c.New(&q, twin.doc); err != nil {
					panic(err)
				}
			})
			if allocs != 0 {
				t.Fatalf("warmed Compiler.New allocated %.2f times, want 0", allocs)
			}
		})
	}
}

// A Compiler driven by an unbounded stream of distinct queries must not retain
// a compiled path per distinct path forever; the memo is a cache, not a leak.
func TestCompilerPathMemoIsBounded(t *testing.T) {
	var c Compiler
	var q Query
	for i := range pathCacheMax * 3 {
		src := fmt.Appendf(nil, `{"select":["f%d"],"where":{"g%d":%d}}`, i, i, i)
		if err := c.Parse(&q, src); err != nil {
			t.Fatal(err)
		}
	}
	if len(c.paths.entries) > pathCacheMax {
		t.Fatalf("path memo holds %d entries, want at most %d", len(c.paths.entries), pathCacheMax)
	}
}

// TestParseOneShotAllocs pins the package-level front end against what it cost
// before the Compiler existed.
//
// Routing Parse through a compiler is only worth doing if the default entry
// point — the one every example uses — does not pay for a reuse machinery it
// never reuses. Two mistakes would silently make it pay: sizing arena chunks
// for amortization it will not get, and returning a pointer into the Compiler
// value, which makes the whole kilobyte-wide struct escape to the heap. Both
// show up here as bytes rather than as a failing behaviour test, which is why
// this measures bytes at all.
//
// The bounds are the measured pre-Compiler numbers for this document. They are
// ceilings, not targets: tightening them when the front end gets cheaper is
// correct, raising them is the regression this exists to catch.
func TestParseOneShotAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector instruments allocation; the bounds below describe an uninstrumented build")
	}
	const (
		maxAllocs = 71   // pre-Compiler front end, same document
		maxBytes  = 5736 // likewise
	)
	allocs := testing.AllocsPerRun(200, func() {
		q, err := Parse(benchmarkQueryDoc)
		if err != nil {
			panic(err)
		}
		parseSink = q
	})
	if allocs > maxAllocs {
		t.Errorf("Parse allocated %.0f times, want at most %d", allocs, maxAllocs)
	}
	if bytes := bytesPerRun(200, func() {
		q, err := Parse(benchmarkQueryDoc)
		if err != nil {
			panic(err)
		}
		parseSink = q
	}); bytes > maxBytes {
		t.Errorf("Parse allocated %d bytes, want at most %d", bytes, maxBytes)
	}
}

// bytesPerRun is testing.AllocsPerRun for bytes, which it does not report.
// TotalAlloc is a cumulative count of bytes handed out, so the difference
// across a known number of runs is exact rather than sampled.
func bytesPerRun(runs int, fn func()) uint64 {
	fn() // fault in anything built lazily on the first call
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range runs {
		fn()
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / uint64(runs)
}

// parseSink keeps a compiled Query reachable so that neither the compiler nor
// the collector can discard the work being measured.
var parseSink *Query

// benchmarkQueryDoc is one ordinary analytics query: a nested projection, an
// alias over an aggregate, a conjunctive filter carrying a comparison and a
// membership, grouping, ordering, and a limit.
var benchmarkQueryDoc = []byte(`{
  "select":  ["profile.country", "team", {"total": {"$sum": "score"}}],
  "where":   {"tier": "pro", "score": {"$gte": 5}, "team": {"$in": ["red", "blue"]}},
  "groupBy": ["profile.country", "team"],
  "orderBy": [{"profile.country": "desc"}],
  "limit":   20
}`)

// BenchmarkCompilerParse reports the steady-state cost of recompiling one
// query document with a warmed Compiler. It is the number the type exists to
// produce, and it is zero allocations.
func BenchmarkCompilerParse(b *testing.B) {
	var c Compiler
	var q Query
	for range 2 {
		if err := c.Parse(&q, benchmarkQueryDoc); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := c.Parse(&q, benchmarkQueryDoc); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParse is the same query through the package-level front end, which
// owns everything it returns and therefore cannot reuse anything. It is here
// as the baseline BenchmarkCompilerParse is measured against.
func BenchmarkParse(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		q, err := Parse(benchmarkQueryDoc)
		if err != nil {
			b.Fatal(err)
		}
		parseSink = q
	}
}
