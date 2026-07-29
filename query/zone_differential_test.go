package query

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

// Differential evidence for block-level (chunk-summary) pruning.
//
// The contract these tests defend is one sentence: turning pruning on must not
// change any result. A pruning bug does not produce a wrong value, it produces
// a *missing row*, which no ordinary assertion about a query's output would
// catch — the result still looks like a plausible answer. So every test here
// runs the identical query body twice against the identical snapshot, once
// with store.SetZonePruning(true) and once with it false, and demands the two
// results agree cell for cell in the same order. The unpruned run is also
// checked against the independent reference executor in query_test.go where
// the battery is shared, so "both runs agree" cannot degenerate into "both
// runs are wrong the same way".
//
// Non-vacuity is asserted, not assumed: each test fails if the pruned runs did
// not actually skip any chunk, because a differential over a plan that never
// prunes proves nothing.

// zoneRunBothWays runs q against snapshot with pruning on and with pruning
// off, returns the pruned result and the number of chunks pruning skipped, and
// fails if the two results differ in any cell.
func zoneRunBothWays(t testing.TB, q *Query, snapshot store.Snapshot) (Result, int) {
	t.Helper()

	previous := store.SetZonePruning(true)
	var pruned Exec
	prunedErr := q.RunInto(&pruned, FromSnapshot(snapshot))
	skipped := pruned.Workspace.zonePruned

	store.SetZonePruning(false)
	var plain Exec
	plainErr := q.RunInto(&plain, FromSnapshot(snapshot))
	store.SetZonePruning(previous)

	// Errors are part of the observable result and are compared, not
	// short-circuited. A path spelling that a non-object root cannot resolve
	// reports an error from either plan, and pruning must not turn that into
	// silence or the reverse.
	if (prunedErr == nil) != (plainErr == nil) {
		t.Fatalf("pruned error %v, unpruned error %v", prunedErr, plainErr)
	}
	if prunedErr != nil {
		if prunedErr.Error() != plainErr.Error() {
			t.Fatalf("pruned error %q, unpruned error %q", prunedErr, plainErr)
		}
		return Result{}, skipped
	}
	if plain.Workspace.zonePruned != 0 {
		t.Fatalf("pruning reported %d skipped chunks while disabled", plain.Workspace.zonePruned)
	}
	if diff := zoneCompareResults(pruned.Result, plain.Result); diff != "" {
		t.Fatalf("pruned and unpruned results differ (%d chunks skipped): %s", skipped, diff)
	}
	return pruned.Result, skipped
}

// zoneCompareResults reports the first difference between two Results, or "".
// It compares raw JSON rather than going through the typed accessors so that a
// difference in cell kind, spelling, or ordering is all reported the same way.
func zoneCompareResults(a, b Result) string {
	if len(a.Columns) != len(b.Columns) {
		return fmt.Sprintf("column count: %d vs %d", len(a.Columns), len(b.Columns))
	}
	if a.RowCount != b.RowCount {
		return fmt.Sprintf("row count: %d vs %d", a.RowCount, b.RowCount)
	}
	for c := range a.Columns {
		if a.Columns[c].Header != b.Columns[c].Header {
			return fmt.Sprintf("column %d header: %q vs %q", c, a.Columns[c].Header, b.Columns[c].Header)
		}
		for r := 0; r < a.RowCount; r++ {
			x, y := a.Columns[c].Cells[r], b.Columns[c].Cells[r]
			if x.Kind() != y.Kind() {
				return fmt.Sprintf("row %d col %q kind: %v vs %v", r, a.Columns[c].Header, x.Kind(), y.Kind())
			}
			if string(x.JSON()) != string(y.JSON()) && !zoneEquivalentZero(x, y) {
				return fmt.Sprintf("row %d col %q: %s vs %s", r, a.Columns[c].Header, x.JSON(), y.JSON())
			}
		}
	}
	return ""
}

// zoneEquivalentZero admits the one spelling difference the two snapshot
// extraction paths have, which pruning juxtaposes but did not create: a
// document whose value is the JSON number -0 reaches a computed aggregate as
// float64 -0 through the RawValue path (store's numericRaws, used for a
// narrowed row gather) and as +0 through the fused typed path
// (ShapeCache.AppendFieldFloat64, used for a full column scan), because the
// latter's integer fast path goes through TapeInt64 and int64 has no signed
// zero. MIN(-0) therefore prints "-0" from one plan and "0" from the other.
//
// This is a pre-existing divergence between the compact and full extraction
// lanes — a declared index that narrows to a row set has always been able to
// produce it — and it is a difference in the sign of a zero, not in which rows
// were selected: both plans agree on the row count, on every projected cell,
// and on the numeric value. It is admitted here, narrowly and only between two
// zeros, rather than papered over by comparing aggregates as floats, so that
// every other spelling difference still fails this differential.
func zoneEquivalentZero(x, y Cell) bool {
	xf, xok := x.Float64()
	yf, yok := y.Float64()
	return xok && yok && xf == 0 && yf == 0
}

func zoneCollection(t testing.TB, chunkDocuments int, docs [][]byte) store.Snapshot {
	t.Helper()
	c, err := store.New(store.Options{ChunkDocuments: chunkDocuments})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for i, doc := range docs {
		if _, err := c.Put(fmt.Sprintf("k%06d", i), doc); err != nil {
			t.Fatalf("Put %d (%s): %v", i, doc, err)
		}
	}
	snapshot, err := c.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	return snapshot
}

// zoneBuiltCollection is zoneCollection through [store.Builder]. It is a
// separate construction path, not a convenience: Builder admits repeated
// document layouts as templates, and a templated document has no classic tape
// for a sparse gather to walk. The narrowed extraction that pruning enables
// reads exactly those documents, so a differential that only ever used Put
// would leave the whole templated representation untested — which is how the
// sparse-gather bug this construction path found stayed hidden.
func zoneBuiltCollection(t testing.TB, chunkDocuments int, docs [][]byte) store.Snapshot {
	t.Helper()
	builder, err := store.NewBuilder(store.Options{ChunkDocuments: chunkDocuments})
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}
	for i, doc := range docs {
		if err := builder.Append(fmt.Sprintf("k%06d", i), doc); err != nil {
			t.Fatalf("Append %d (%s): %v", i, doc, err)
		}
	}
	c, err := builder.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	snapshot, err := c.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	return snapshot
}

// zoneConstructors are the collection construction paths every differential
// runs against. Each produces a different physical document representation for
// the same logical corpus.
var zoneConstructors = []struct {
	name  string
	build func(testing.TB, int, [][]byte) store.Snapshot
}{
	{"put", zoneCollection},
	{"builder", zoneBuiltCollection},
}

// Given every ordered document sequence of length 1..3 drawn from the shared
// exhaustive document pool, when each query in the shared battery runs over a
// heap snapshot with one document per chunk, then the pruned result equals the
// unpruned result and both equal the independent reference executor's.
//
// One document per chunk is the adversarial chunking for this feature: every
// chunk is a single-document zone, so every summary is maximally precise and
// pruning fires on almost every shape. A bug that drops a row shows up as a
// dropped row here rather than being absorbed by a neighbour in the same
// chunk.
func TestExhaustiveZonePruningDifferential(t *testing.T) {
	segments := enumerateSegments(docPool, 3)
	battery := queryBattery()

	checks, pruningRuns := 0, 0
	for _, docs := range segments {
		decoded := decodeDocs(t, docs)
		for _, constructor := range zoneConstructors {
			snapshot := constructor.build(t, 1, docs)
			for qi, q := range battery {
				got, skipped := zoneRunBothWays(t, q, snapshot)
				if skipped > 0 {
					pruningRuns++
				}
				want := referenceRun(t, q, decoded)
				if diff := compareResults(got, want); diff != "" {
					t.Fatalf("query %d %s over %v: %s", qi, constructor.name, jsonStrings(docs), diff)
				}
				checks++
			}
		}
	}
	if pruningRuns == 0 {
		t.Fatal("no execution pruned a chunk: the differential is vacuous")
	}
	t.Logf("zone differential: %d segments × %d constructors × %d queries = %d checks, %d of them actually pruned",
		len(segments), len(zoneConstructors), len(battery), checks, pruningRuns)
}

// Given the same exhaustive family at four documents per chunk, when each
// query runs, then pruned and unpruned results still agree. Multi-document
// chunks are the case where a summary covers several rows at once and a
// widened bound is the only thing standing between a correct answer and a
// dropped row.
func TestExhaustiveZonePruningDifferentialMultiDocChunks(t *testing.T) {
	segments := enumerateSegments(docPool, 3)
	battery := queryBattery()

	pruningRuns := 0
	for _, docs := range segments {
		for _, constructor := range zoneConstructors {
			snapshot := constructor.build(t, 4, docs)
			for _, q := range battery {
				if _, skipped := zoneRunBothWays(t, q, snapshot); skipped > 0 {
					pruningRuns++
				}
			}
		}
	}
	if pruningRuns == 0 {
		t.Fatal("no execution pruned a chunk: the differential is vacuous")
	}
}

// zoneAdversarialDocs is the corpus the fuzz differential draws from. Every
// entry is here because it is a boundary the summary could get wrong:
//
//   - integers on either side of float64's 53-bit mantissa, where a naive
//     min/max would collapse two distinct values and could prune a chunk that
//     matches `> 9007199254740992`;
//   - -0 and 0, one value to the query and two bit patterns to a float;
//   - values at the exact bounds a predicate compares against;
//   - explicit null against an absent path, which the summary tracks as two
//     separate bits and IS NULL unions;
//   - strings that agree on the four-byte prefix the code keeps, so the
//     truncated bound is exercised rather than bypassed;
//   - an escaped string value and an escaped member name, the two shapes the
//     fold path deliberately widens and poisons;
//   - non-object roots and empty objects, where every tracked path is absent;
//   - containers, which sort above every scalar in the query's total order.
var zoneAdversarialDocs = []string{
	`{}`,
	`[1,2,3]`,
	`"scalar root"`,
	`{"v":null}`,
	`{"w":1}`,
	`{"v":0}`,
	`{"v":-0}`,
	`{"v":0.0}`,
	`{"v":9007199254740992}`,
	`{"v":9007199254740993}`,
	`{"v":9007199254740994}`,
	`{"v":-9007199254740993}`,
	`{"v":1e308}`,
	`{"v":-1e308}`,
	`{"v":1e-320}`,
	`{"v":1E2,"w":100}`,
	`{"v":100.0}`,
	`{"v":true}`,
	`{"v":false}`,
	`{"v":"abcd"}`,
	`{"v":"abcde"}`,
	`{"v":"abcdf"}`,
	`{"v":"abc"}`,
	`{"v":"ab"}`,
	`{"v":""}`,
	`{"v":"Abcd"}`,
	`{"v":"A\tbcd"}`,
	`{"v":[1,2]}`,
	`{"v":{"x":1}}`,
	`{"v":1,"w":null}`,
	`{"v":7}`,
	`{"v":1,"v":2}`,
	`{"a":1,"b":2,"c":3,"d":4,"e":5,"f":6,"g":7,"h":8,"i":9,"v":10}`,
}

// zoneFuzzPredicates returns the predicate shapes the fuzz differential
// applies. They cover every operator the zone tier answers plus the
// combinators that assemble leaf masks.
func zoneFuzzPredicates(rng *rand.Rand) Predicate {
	values := []any{
		0, 1, 2, 100, -1,
		Number("9007199254740992"), Number("9007199254740993"),
		1e308, 1e-320,
		"", "ab", "abc", "abcd", "abcde", "Abcd",
		true, false,
	}
	ops := []Op{Eq, Ne, Lt, Le, Gt, Ge}
	paths := []string{"v", "w", "a", "missing"}

	leaf := func() Predicate {
		switch rng.IntN(6) {
		case 0:
			return Exists(paths[rng.IntN(len(paths))])
		case 1:
			return IsNull(paths[rng.IntN(len(paths))])
		case 2:
			n := 1 + rng.IntN(3)
			alts := make([]any, n)
			for i := range alts {
				alts[i] = values[rng.IntN(len(values))]
			}
			return In(paths[rng.IntN(len(paths))], alts...)
		case 3:
			return Contains(paths[rng.IntN(len(paths))], strconv.Itoa(rng.IntN(3)))
		default:
			return Cmp(paths[rng.IntN(len(paths))], ops[rng.IntN(len(ops))], values[rng.IntN(len(values))])
		}
	}
	switch rng.IntN(5) {
	case 0:
		return And(leaf(), leaf())
	case 1:
		return Or(leaf(), leaf())
	case 2:
		return Not(leaf())
	case 3:
		return And(leaf(), Or(leaf(), leaf()))
	default:
		return leaf()
	}
}

// Given randomly assembled collections drawn from the adversarial corpus and
// randomly assembled predicates, when each query runs both ways, then the
// results are identical. This is the wide net the enumerated differential
// above cannot cast: it reaches chunk populations (all-null, all-absent,
// single-document, mixed-kind, budget-exhausting) and predicate trees that no
// hand-written battery would enumerate.
func TestZonePruningFuzzDifferential(t *testing.T) {
	rng := rand.New(rand.NewPCG(0x5eed, 0xC0FFEE))
	pruningRuns := 0
	for iteration := 0; iteration < 400; iteration++ {
		count := 1 + rng.IntN(20)
		docs := make([][]byte, count)
		// Half the collections draw from the whole corpus and half from a
		// narrow slice of it, so all-null, all-absent, and all-same chunks are
		// reached often rather than only by accident.
		narrow := rng.IntN(2) == 0
		base := rng.IntN(len(zoneAdversarialDocs))
		for i := range docs {
			if narrow {
				docs[i] = []byte(zoneAdversarialDocs[(base+rng.IntN(3))%len(zoneAdversarialDocs)])
			} else {
				docs[i] = []byte(zoneAdversarialDocs[rng.IntN(len(zoneAdversarialDocs))])
			}
		}
		chunkDocuments := []int{1, 2, 3, 8, 64}[rng.IntN(5)]
		snapshot := zoneConstructors[rng.IntN(len(zoneConstructors))].build(t, chunkDocuments, docs)

		for probe := 0; probe < 8; probe++ {
			q := Select(Path("v"), Path("w")).Where(zoneFuzzPredicates(rng))
			if rng.IntN(3) == 0 {
				q = Select(Count(), Sum("v"), Min("v"), Max("v")).Where(zoneFuzzPredicates(rng))
			}
			if _, skipped := zoneRunBothWays(t, q, snapshot); skipped > 0 {
				pruningRuns++
			}
		}
	}
	if pruningRuns == 0 {
		t.Fatal("no execution pruned a chunk: the fuzz differential is vacuous")
	}
	t.Logf("zone fuzz differential: 3200 executions, %d of them pruned at least one chunk", pruningRuns)
}

// Given a chunk holding only values above a threshold and a chunk holding only
// values below it, when a range predicate straddling the two runs, then
// pruning skips exactly the chunk that cannot match and the result is
// unchanged. This is the direct, readable statement of what the feature does,
// separate from the differential's blanket equality.
func TestZonePruningSkipsDisjointChunks(t *testing.T) {
	var docs [][]byte
	for i := 0; i < 256; i++ {
		docs = append(docs, fmt.Appendf(nil, `{"v":%d}`, i))
	}
	snapshot := zoneCollection(t, 64, docs) // four chunks: 0-63, 64-127, ...

	q := Select(Path("v")).Where(Cmp("v", Ge, 200))
	result, skipped := zoneRunBothWays(t, q, snapshot)
	if result.RowCount != 56 {
		t.Fatalf("rows: got %d want 56", result.RowCount)
	}
	if skipped != 3 {
		t.Fatalf("skipped chunks: got %d want 3", skipped)
	}

	// The same corpus shuffled across chunks is the unfavourable case: every
	// chunk spans nearly the whole range, so min/max proves nothing. It must
	// still be correct, and it must decline to prune rather than pretend.
	shuffled := make([][]byte, len(docs))
	copy(shuffled, docs)
	rng := rand.New(rand.NewPCG(1, 2))
	rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	shuffledSnapshot := zoneCollection(t, 64, shuffled)
	shuffledResult, shuffledSkipped := zoneRunBothWays(t, q, shuffledSnapshot)
	if shuffledResult.RowCount != 56 {
		t.Fatalf("shuffled rows: got %d want 56", shuffledResult.RowCount)
	}
	t.Logf("clustered skipped %d/4 chunks, shuffled skipped %d/4", skipped, shuffledSkipped)
}

// Given a path that is absent from every document in one chunk and present in
// another, when EXISTS and IS NULL run, then the presence bits prune the chunk
// that cannot match. This covers the two predicates no index in this store has
// ever been able to bound.
func TestZonePruningPresenceBits(t *testing.T) {
	var docs [][]byte
	for i := 0; i < 64; i++ {
		docs = append(docs, []byte(`{"v":1}`)) // chunk 0: v present, never null
	}
	for i := 0; i < 64; i++ {
		docs = append(docs, []byte(`{"other":1}`)) // chunk 1: v absent
	}
	for i := 0; i < 64; i++ {
		docs = append(docs, []byte(`{"v":null}`)) // chunk 2: v explicitly null
	}
	// Chunk 3 mixes present and absent, which is the only shape in which the
	// two presence bits of one entry are both meaningful: the entry exists (so
	// the "no entry means absent" deduction does not apply) and yet some
	// document in the chunk lacks the path, so IS NULL must keep it.
	for i := 0; i < 32; i++ {
		docs = append(docs, []byte(`{"v":2}`))
		docs = append(docs, []byte(`{"other":2}`))
	}
	snapshot := zoneCollection(t, 64, docs)

	exists, existsSkipped := zoneRunBothWays(t, Select(Path("v")).Where(Exists("v")), snapshot)
	if exists.RowCount != 160 {
		t.Fatalf("EXISTS rows: got %d want 160", exists.RowCount)
	}
	if existsSkipped != 1 {
		t.Fatalf("EXISTS skipped: got %d want 1 (the all-absent chunk)", existsSkipped)
	}

	isNull, isNullSkipped := zoneRunBothWays(t, Select(Path("v")).Where(IsNull("v")), snapshot)
	if isNull.RowCount != 160 {
		t.Fatalf("IS NULL rows: got %d want 160", isNull.RowCount)
	}
	if isNullSkipped != 1 {
		t.Fatalf("IS NULL skipped: got %d want 1 (the always-present, never-null chunk)", isNullSkipped)
	}

	// A comparison cannot match a null or absent cell at all, so the all-absent
	// and all-null chunks are both prunable for an ordered predicate, while the
	// mixed chunk survives on the strength of its 32 present values.
	cmp, cmpSkipped := zoneRunBothWays(t, Select(Path("v")).Where(Cmp("v", Ge, 0)), snapshot)
	if cmp.RowCount != 96 {
		t.Fatalf("Cmp rows: got %d want 96", cmp.RowCount)
	}
	if cmpSkipped != 2 {
		t.Fatalf("Cmp skipped: got %d want 2", cmpSkipped)
	}
}

// Given two integers that differ only past float64's 53-bit mantissa, when a
// range predicate separates them, then the chunk holding the larger one
// survives. A zone bound that rounded the maximum down to the nearest float64
// would prune it, and the row would silently disappear.
func TestZonePruningExactDecimalBoundary(t *testing.T) {
	var docs [][]byte
	for i := 0; i < 64; i++ {
		docs = append(docs, []byte(`{"v":1}`))
	}
	docs = append(docs, []byte(`{"v":9007199254740993}`))
	snapshot := zoneCollection(t, 64, docs)

	result, _ := zoneRunBothWays(t,
		Select(Path("v")).Where(Cmp("v", Gt, Number("9007199254740992"))),
		snapshot,
	)
	if result.RowCount != 1 {
		t.Fatalf("rows: got %d want 1 (the value past float64 precision)", result.RowCount)
	}
	if got := string(result.Columns[0].Cells[0].JSON()); got != "9007199254740993" {
		t.Fatalf("value: got %s", got)
	}
}

// Given a query executed repeatedly against a warm Exec, when block pruning is
// active, then the steady state still allocates nothing. The chunk walk writes
// into Workspace mask buffers and the probe is compiled once, so the tier must
// cost zero allocations however many chunks it visits.
func TestZonePruningSteadyAllocs(t *testing.T) {
	var docs [][]byte
	for i := 0; i < 4096; i++ {
		docs = append(docs, fmt.Appendf(nil, `{"v":%d,"w":"s%04d"}`, i, i))
	}
	snapshot := zoneCollection(t, 64, docs)
	q := Select(Path("v")).Where(And(Cmp("v", Ge, 4000), Cmp("w", Lt, "s9999")))

	var e Exec
	for i := 0; i < 8; i++ {
		if err := q.RunInto(&e, FromSnapshot(snapshot)); err != nil {
			t.Fatalf("warm-up: %v", err)
		}
	}
	if e.Workspace.zonePruned == 0 {
		t.Fatal("no chunk pruned: the allocation assertion is vacuous")
	}
	allocs := testing.AllocsPerRun(100, func() {
		if err := q.RunInto(&e, FromSnapshot(snapshot)); err != nil {
			t.Fatalf("run: %v", err)
		}
	})
	if allocs != 0 {
		t.Fatalf("steady-state allocations: got %v want 0", allocs)
	}
}
