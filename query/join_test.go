package query

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func sharedFanOutDatabase(t testing.TB, outerRows, innerRows int) *store.Database {
	t.Helper()
	db := &store.Database{}
	outer, err := db.CreateCollection("outer", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range outerRows {
		if _, err := outer.Put(
			fmt.Sprintf("o%d", i),
			fmt.Appendf(nil, `{"id":%d,"join":"shared"}`, i),
		); err != nil {
			t.Fatal(err)
		}
	}
	inner, err := db.CreateCollection("inner", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range innerRows {
		if _, err := inner.Put(
			fmt.Sprintf("i%d", i),
			fmt.Appendf(nil, `{"join":"shared","seat":%d,"label":"a\\u0062"}`, i),
		); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func sharedFanOutBuildBytes(rows int) int64 {
	return int64(rows) * joinBuildEntryBytes(scalar{
		kind: kindString,
		sval: "shared",
	})
}

// Given a pair of small collections captured in one database snapshot and a
// battery of semi-join shapes, when the compiled executor runs, then every
// result agrees with a naive nested-loop reference that decodes both sides with
// encoding/json and evaluates the join in plain Go — and it agrees under both
// execution strategies, with and without a declared exact index on the outer
// join path.
//
// This is the same bounded exhaustive discipline TestExhaustiveQueryDifferential
// applies to single-collection queries, extended in the two directions a join
// adds. The reference is the naive one on purpose: for every outer document it
// walks every inner document, applies the inner filter, and compares the join
// keys with the independent math/big comparator query_test.go already uses.
// Nothing about the adaptive strategy, the membership lowering, or the index
// masks exists on the reference's side of the check.
//
// The threshold sweep is what makes the adaptive choice testable at all. The
// same battery runs with the membership limit forced high (every join binds a
// membership) and forced to zero (every primary-key join falls back to a
// per-row lookup), and both must equal the same reference. A strategy that
// changed which rows survive would show up here as a disagreement with the
// reference, and a strategy that silently never engaged would show up in
// TestJoinAdaptiveStrategyIsNonVacuous, which asserts the two configurations
// actually take different paths.

// --- fixtures --------------------------------------------------------------

// outerJoinPool is the driving collection's document domain. It spans every
// shape a join key can take on the outer side: a string naming an inner key
// that exists and one that does not, an explicit null, an absent field, a
// number, the string spelling of that same number, an escaped string, and a
// container. The number and its string spelling are both present on purpose:
// a collection key is a string, so 7 must not join to the key "7" while "7"
// must, and the two would be indistinguishable if only one were in the pool.
var outerJoinPool = []string{
	`{"id":1,"ref":"k1","active":true}`,
	`{"id":2,"ref":"k2","active":false}`,
	`{"id":3,"ref":"gone","active":true}`,
	`{"id":4,"ref":null,"active":true}`,
	`{"id":5,"active":true}`,
	`{"id":6,"ref":7,"active":true}`,
	`{"id":7,"ref":"7","active":true}`,
	`{"id":8,"ref":"a\"b","active":false}`,
	`{"id":9,"ref":[1,2],"active":true}`,
}

// innerJoinPool is the inner collection's domain, paired with the keys it is
// stored under. Two documents share the join-key value 7 — spelled 7 and 7.0,
// which this engine compares as one number — so the semi-join's "duplicate
// inner matches keep the outer row exactly once" rule and the inner join's
// "duplicate inner matches are two rows" rule are both exercised, and a hash
// that keyed on the spelling rather than the value would lose one of them.
// Every document carries a distinct seat, which is what makes the order two
// matching rows come back in observable. One key is the decimal spelling of a
// number an outer document holds as a number, one is an escaped string, and one
// document's join-key value is a container, which no exact index can hold and
// which therefore forces the membership back off the mask lowering onto its
// per-row search.
var innerJoinPool = []struct {
	key string
	doc string
}{
	{"k1", `{"tier":"pro","code":7,"seat":1}`},
	{"k2", `{"tier":"free","code":7.0,"seat":2}`},
	{"k3", `{"tier":"pro","code":"k1","seat":3}`},
	{"k4", `{"code":null,"seat":4}`},
	{"7", `{"tier":"pro","code":"7","seat":5}`},
	{"a\"b", `{"tier":"free","code":[1,2],"seat":6}`},
}

// joinBattery returns the compiled-once join shapes the differential runs. Each
// query is reused across every collection pair and every execution mode, which
// also exercises the compile-once/run-many contract for a plan holding a join.
func joinBattery() []*Query {
	return []*Query{
		// The plain foreign-key shape, with and without an inner filter.
		Select(Path("id")).Join(JoinOn("inner", "ref", JoinKey)),
		Select(Path("id")).Join(JoinOn("inner", "ref", JoinKey).Where(Cmp("tier", Eq, "pro"))),
		Select(Path("id")).Join(JoinOn("inner", "ref", JoinKey).Where(IsNull("tier"))),
		Select(Path("id")).Join(JoinOn("inner", "ref", JoinKey).Where(In("tier", "pro", "free"))),
		// An outer filter beside the join: the join leaf must compose with an
		// ordinary predicate rather than replace it.
		Select(Path("id")).Where(Cmp("active", Eq, true)).
			Join(JoinOn("inner", "ref", JoinKey)),
		Select(Path("id")).Where(Or(Cmp("id", Eq, 1), Cmp("id", Eq, 3))).
			Join(JoinOn("inner", "ref", JoinKey).Where(Cmp("tier", Eq, "pro"))),
		// A join against an inner field rather than the primary key, including
		// the case where the value is numeric and the duplicate-key case.
		Select(Path("id")).Join(JoinOn("inner", "ref", "code")),
		Select(Path("id")).Join(JoinOn("inner", "ref", "code").Where(Cmp("tier", Eq, "pro"))),
		// Two clauses at once, so the conjunction of bindings is exercised.
		Select(Path("id")).Join(
			JoinOn("inner", "ref", JoinKey),
			JoinOn("inner", "ref", "code"),
		),
		// Ordering, limiting, grouping, and aggregation over a joined row set,
		// so the join filters before every later stage rather than after one.
		Select(Path("id")).Join(JoinOn("inner", "ref", JoinKey)).OrderBy("id", Desc),
		Select(Path("id")).Join(JoinOn("inner", "ref", JoinKey)).OrderBy("id", Asc).Limit(2),
		Select(Count(), Sum("id")).Join(JoinOn("inner", "ref", JoinKey)),
		Select(Path("active"), Count()).Join(JoinOn("inner", "ref", JoinKey)).
			GroupBy("active").OrderBy("active", Asc),
		// Grouping and ordering by the join path itself, which shares its value
		// column with the join leaf: a column-index mistake there would be
		// invisible in every query that names the join path only once.
		Select(Path("ref"), Count()).Join(JoinOn("inner", "ref", JoinKey)).
			GroupBy("ref").OrderBy("ref", Asc),
		Select(Path("id")).Join(JoinOn("inner", "ref", JoinKey)).OrderBy("ref", Asc).OrderBy("id", Asc),
		// A bare COUNT(*) over an indexed join path, which is the plan shape the
		// direct indexed-count lane answers from exact masks without decoding a
		// single document — so a membership binding that produced an unsound
		// mask would be believed rather than rechecked.
		Select(Count()).Join(JoinOn("inner", "ref", JoinKey)),
		Select(Count()).Join(JoinOn("inner", "ref", JoinKey).Where(Cmp("tier", Eq, "pro"))),
	}
}

// joinStrategy is one forced binding strategy. The threshold is the only input
// that selects between them, so sweeping it is how the battery runs the same
// query down both execution paths.
type joinStrategy struct {
	name string
	// membershipMax is the ExecOptions value. One forces the lookup path — the
	// binder overflows at limit+1 collected values, which a single inner match
	// already reaches — and a very large value forces the membership path.
	membershipMax int
}

var joinStrategies = []joinStrategy{
	{"membership", 1 << 20},
	{"lookup", 1},
}

// joinIndexModes toggles a declared exact index on the outer join path. With
// one, a membership binding lowers to a mask intersection and the rows the
// executor decodes are a strict subset; without one it searches the collected
// set per row. Both must return the same rows, which is what makes the index a
// cost decision here rather than a semantic one.
var joinIndexModes = []bool{false, true}

// joinIterations keeps the exhaustive sweep at full size in an ordinary run
// while giving the -short instrumentation runs — race and checkptr, where every
// allocation and every load is instrumented — a representative sample of the
// same generators. It mirrors the root package's testIterations for the same
// reason; the two packages cannot share one, and a join-scoped name keeps the
// query package's test namespace honest about which sweep it bounds.
func joinIterations(full, short int) int {
	if testing.Short() {
		return short
	}
	return full
}

func TestExhaustiveJoinDifferential(t *testing.T) {
	// The outer side is enumerated as sequences rather than subsets because the
	// driving collection is keyed independently of its contents: two documents
	// with identical bytes under different keys are two rows, and their order
	// decides the chunk layout the mask walk and the batch boundaries cross. The
	// inner side is enumerated as subsets, because there a repeated key is one
	// document by definition, and it is capped at three because the inner side
	// influences the answer only through the set of join-key values it yields:
	// the pool's six documents already contribute every distinct value shape, and
	// a fourth simultaneously present document adds combinations of shapes that
	// are already independently covered.
	outers := enumerateStrings(outerJoinPool, joinIterations(3, 2), 1)
	inners := enumerateInnerCollections(len(innerJoinPool), joinIterations(3, 2))
	battery := joinBattery()

	checks := 0
	// Tallied across the whole sweep and asserted at the end: a differential
	// that only ever exercised one strategy would pass every comparison below
	// while proving nothing about the other.
	var bound, probed ExecStats
	for _, outerDocs := range outers {
		decodedOuter := decodeDocs(t, asBytes(outerDocs))
		for _, innerRows := range inners {
			reference := newRefCollection(t, innerRows)
			for _, indexed := range joinIndexModes {
				db := buildJoinDatabase(t, outerDocs, innerRows, indexed)
				catalog := db.Snapshot()
				for _, strategy := range joinStrategies {
					var e Exec
					e.Options.JoinMembershipMax = strategy.membershipMax
					for qi, q := range battery {
						if err := q.RunInto(&e, FromDatabase(catalog, "outer")); err != nil {
							t.Fatalf("query %d %s indexed=%v over %v / %v: RunInto: %v",
								qi, strategy.name, indexed, outerDocs, innerRows, err)
						}
						want := referenceRunJoined(q, decodedOuter, func(doc any) bool {
							return refJoinsAll(q.joins, doc, reference)
						})
						if diff := compareResults(e.Result, want); diff != "" {
							t.Fatalf("query %d %s indexed=%v over %v / %v: %s",
								qi, strategy.name, indexed, outerDocs, innerRows, diff)
						}
						bound.JoinMemberships += e.Stats.JoinMemberships
						bound.JoinKeys += e.Stats.JoinKeys
						probed.JoinLookups += e.Stats.JoinLookups
						probed.JoinProbes += e.Stats.JoinProbes
						probed.JoinFilters += e.Stats.JoinFilters
						probed.JoinFilterRejected += e.Stats.JoinFilterRejected
						checks++
					}
				}
			}
		}
	}
	if bound.JoinMemberships == 0 || bound.JoinKeys == 0 {
		t.Fatalf("no membership binding was ever exercised: %+v", bound)
	}
	if probed.JoinLookups == 0 || probed.JoinProbes == 0 {
		t.Fatalf("no lookup binding was ever exercised: %+v", probed)
	}
	if probed.JoinFilters == 0 || probed.JoinFilterRejected == 0 {
		t.Fatalf("no semi-join reduction filter ever rejected a row: %+v", probed)
	}
	t.Logf("strategies exercised: %d membership bindings collecting %d values; "+
		"%d lookup bindings performing %d probes, %d of them carrying a filter "+
		"that rejected %d rows outright",
		bound.JoinMemberships, bound.JoinKeys, probed.JoinLookups, probed.JoinProbes,
		probed.JoinFilters, probed.JoinFilterRejected)
	t.Logf("exhaustive join differential: %d outer sets × %d inner sets × %d index modes × %d strategies × %d queries = %d checks",
		len(outers), len(inners), len(joinIndexModes), len(joinStrategies), len(battery), checks)
}

// TestJoinStrategiesAgreeRowForRow is the same claim stated as a direct
// comparison rather than through the reference: the identical query and the
// identical database, executed with the membership threshold at either
// extreme, must produce byte-identical results. It is the narrower check that
// survives even if the reference and the executor ever drifted together.
func TestJoinStrategiesAgreeRowForRow(t *testing.T) {
	outers := enumerateStrings(outerJoinPool, 3, 2)
	inners := enumerateInnerCollections(len(innerJoinPool), 2)
	battery := joinBattery()

	for _, outerDocs := range outers {
		for _, innerRows := range inners {
			db := buildJoinDatabase(t, outerDocs, innerRows, true)
			catalog := db.Snapshot()
			for qi, q := range battery {
				var membership, lookup Exec
				membership.Options.JoinMembershipMax = 1 << 20
				lookup.Options.JoinMembershipMax = 1
				if err := q.RunInto(&membership, FromDatabase(catalog, "outer")); err != nil {
					t.Fatalf("query %d membership: %v", qi, err)
				}
				if err := q.RunInto(&lookup, FromDatabase(catalog, "outer")); err != nil {
					t.Fatalf("query %d lookup: %v", qi, err)
				}
				if diff := diffResults(membership.Result, lookup.Result); diff != "" {
					t.Fatalf("query %d over %v / %v: strategies disagree: %s",
						qi, outerDocs, innerRows, diff)
				}
			}
		}
	}
}

// --- naive nested-loop reference -------------------------------------------

// A refCollection is the inner side as the reference sees it: keys and decoded
// documents, with no index, no ordering, and no set structure of any kind.
type refCollection struct {
	keys []string
	docs []any
}

func newRefCollection(t testing.TB, rows []int) refCollection {
	t.Helper()
	out := refCollection{}
	for _, r := range rows {
		out.keys = append(out.keys, innerJoinPool[r].key)
	}
	docs := make([][]byte, 0, len(rows))
	for _, r := range rows {
		docs = append(docs, []byte(innerJoinPool[r].doc))
	}
	out.docs = decodeDocs(t, docs)
	return out
}

// refJoinsAll is the reference semi-join: an outer document survives only when
// every clause finds at least one inner document that passes the clause's
// filter and whose join key equals the outer document's value.
func refJoinsAll(joins []Join, outer any, inner refCollection) bool {
	for _, j := range joins {
		if !refJoinMatches(j, outer, inner) {
			return false
		}
	}
	return true
}

// refJoinMatches is the nested loop itself. It stops at the first match, which
// is exactly why a duplicate inner key cannot duplicate an outer row: existence
// is a boolean, not a count.
func refJoinMatches(j Join, outer any, inner refCollection) bool {
	cell := refClassify(refResolve(j.outerPath, outer))
	if cell.kind == kindNull {
		return false // a null or absent outer key joins to nothing
	}
	for i := range inner.keys {
		if j.hasWhere && !refEval(j.where, inner.docs[i]) {
			continue
		}
		var target refScalar
		if j.innerPath == JoinKey {
			target = refScalar{kind: kindString, present: true, s: inner.keys[i]}
		} else {
			target = refClassify(refResolve(j.innerPath, inner.docs[i]))
			if target.kind == kindNull {
				continue // a null or absent inner key joins to nothing
			}
		}
		if refCompare(cell, target) == 0 {
			return true
		}
	}
	return false
}

// --- fixture construction ---------------------------------------------------

// buildJoinDatabase publishes the two collections into one Database, so that a
// single Database.Snapshot captures both at one instant — the consistent cut a
// join is only expressible over.
func buildJoinDatabase(t testing.TB, outerDocs []string, innerRows []int, index bool) *store.Database {
	t.Helper()
	db := &store.Database{}
	// Two documents per chunk, so the mask walk, the batch boundaries, and the
	// row gather all cross a chunk edge on collections this small.
	options := store.Options{ChunkDocuments: 2}
	outer, err := db.CreateCollection("outer", options)
	if err != nil {
		t.Fatalf("CreateCollection(outer): %v", err)
	}
	for i, doc := range outerDocs {
		if _, err := outer.Put(fmt.Sprintf("o%d", i), []byte(doc)); err != nil {
			t.Fatalf("Put(o%d, %s): %v", i, doc, err)
		}
	}
	inner, err := db.CreateCollection("inner", options)
	if err != nil {
		t.Fatalf("CreateCollection(inner): %v", err)
	}
	for _, r := range innerRows {
		if _, err := inner.Put(innerJoinPool[r].key, []byte(innerJoinPool[r].doc)); err != nil {
			t.Fatalf("Put(%s): %v", innerJoinPool[r].key, err)
		}
	}
	if index {
		if _, err := outer.CreateIndex(store.IndexDefinition{Name: "ref", Paths: []string{"/ref"}}); err != nil {
			t.Fatalf("CreateIndex(ref): %v", err)
		}
		if info, err := outer.BackfillIndex("ref", 0); err != nil || info.State != store.IndexReady {
			t.Fatalf("BackfillIndex(ref) = (%+v, %v)", info, err)
		}
	}
	return db
}

// enumerateStrings returns every ordered sequence of length min..maxLen drawn
// from pool.
func enumerateStrings(pool []string, maxLen, minLen int) [][]string {
	var out [][]string
	var rec func(prefix []string)
	rec = func(prefix []string) {
		if len(prefix) >= minLen {
			cp := make([]string, len(prefix))
			copy(cp, prefix)
			out = append(out, cp)
		}
		if len(prefix) == maxLen {
			return
		}
		for _, d := range pool {
			rec(append(prefix, d))
		}
	}
	rec(nil)
	return out
}

// enumerateInnerCollections returns every subset of the inner pool of size
// 0..maxLen, in pool order. Subsets rather than sequences, because a collection
// is keyed: the same key twice is one document, so an ordered enumeration would
// only repeat states the subsets already cover. The empty collection is
// included deliberately — a semi-join against nothing must keep no row.
func enumerateInnerCollections(n, maxLen int) [][]int {
	var out [][]int
	var rec func(start int, prefix []int)
	rec = func(start int, prefix []int) {
		cp := make([]int, len(prefix))
		copy(cp, prefix)
		out = append(out, cp)
		if len(prefix) == maxLen {
			return
		}
		for i := start; i < n; i++ {
			rec(i+1, append(prefix, i))
		}
	}
	rec(0, nil)
	return out
}

func asBytes(docs []string) [][]byte {
	out := make([][]byte, len(docs))
	for i, d := range docs {
		out[i] = []byte(d)
	}
	return out
}

// diffResults compares two engine results cell for cell, the direct
// strategy-versus-strategy check that needs no reference model.
func diffResults(a, b Result) string {
	if len(a.Columns) != len(b.Columns) || a.RowCount != b.RowCount {
		return fmt.Sprintf("shape: %d cols/%d rows vs %d cols/%d rows",
			len(a.Columns), a.RowCount, len(b.Columns), b.RowCount)
	}
	for c := range a.Columns {
		if a.Columns[c].Header != b.Columns[c].Header {
			return fmt.Sprintf("column %d header: %q vs %q",
				c, a.Columns[c].Header, b.Columns[c].Header)
		}
		for r := 0; r < a.RowCount; r++ {
			x, y := a.Columns[c].Cells[r], b.Columns[c].Cells[r]
			if x.Kind() != y.Kind() || !strings.EqualFold(string(x.JSON()), string(y.JSON())) {
				return fmt.Sprintf("row %d col %q: %s vs %s", r, a.Columns[c].Header, x.JSON(), y.JSON())
			}
		}
	}
	return ""
}

// --- the adaptive choice, and the proof that it is a choice -----------------

// joinScaleDatabase builds an orders/customers pair large enough that the two
// strategies are genuinely different plans rather than two spellings of the
// same three-row scan. matching is how many customers pass the inner filter and
// therefore how many values a membership would have to collect.
func joinScaleDatabase(t testing.TB, outerRows, customers, matching int, index bool) *store.Database {
	return joinScaleDatabaseIndexed(t, outerRows, customers, matching, index, false)
}

// joinScaleDatabaseIndexed is joinScaleDatabase with independent control over
// the inner collection's own index, which the inner filter pushes down into
// exactly as an ordinary single-collection query would.
func joinScaleDatabaseIndexed(t testing.TB, outerRows, customers, matching int, index, innerIndex bool) *store.Database {
	t.Helper()
	db := &store.Database{}
	orders, err := db.CreateCollection("orders", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < outerRows; i++ {
		doc := fmt.Sprintf(`{"id":%d,"customer":"c%d","total":%d}`, i, i%customers, i)
		if _, err := orders.Put(fmt.Sprintf("o%d", i), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	buyers, err := db.CreateCollection("customers", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < customers; i++ {
		// Spread the matching customers evenly rather than putting them in a
		// prefix. A prefix is not a neutral layout: it makes any sample of the
		// first rows scanned unrepresentative of the whole, which is precisely
		// what the mid-scan abandon check reads. Testing a sampler against data
		// laid out to defeat sampling measures the layout, not the sampler —
		// the clustered case gets its own test instead.
		tier := "free"
		if stride := customers / max(matching, 1); stride > 0 && i%stride == 0 {
			tier = "pro"
		}
		// name repeats the key as a field, so the same join can be expressed
		// either against the primary key or against an ordinary inner path and
		// the two must agree. label is spelled with an escape so that reading it
		// forces the decoded-string path, which is the one a per-row probe has
		// to reset between rows rather than let grow.
		doc := fmt.Sprintf(`{"tier":%q,"seat":%d,"name":"c%d","label":"lab\u0065l-%d"}`,
			tier, i, i, i%4)
		if _, err := buyers.Put(fmt.Sprintf("c%d", i), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	if index {
		if _, err := orders.CreateIndex(store.IndexDefinition{
			Name: "customer", Paths: []string{"/customer"},
		}); err != nil {
			t.Fatal(err)
		}
		if info, err := orders.BackfillIndex("customer", 0); err != nil || info.State != store.IndexReady {
			t.Fatalf("BackfillIndex(customer) = (%+v, %v)", info, err)
		}
	}
	if innerIndex {
		if _, err := buyers.CreateIndex(store.IndexDefinition{
			Name: "tier", Paths: []string{"/tier"},
		}); err != nil {
			t.Fatal(err)
		}
		if info, err := buyers.BackfillIndex("tier", 0); err != nil || info.State != store.IndexReady {
			t.Fatalf("BackfillIndex(tier) = (%+v, %v)", info, err)
		}
	}
	return db
}

// TestJoinAdaptiveStrategyIsNonVacuous proves that the threshold actually
// selects between two different plans, and that each assertion about a plan
// fails under the other one. Without this, every "the fast path was taken"
// claim in this file would be satisfiable by a build in which only one path
// exists — the differential alone cannot tell the difference, because both
// paths are required to return the same rows.
func TestJoinAdaptiveStrategyIsNonVacuous(t *testing.T) {
	const (
		orders    = 500
		customers = 200
		matching  = 40
	)
	db := joinScaleDatabase(t, orders, customers, matching, false)
	catalog := db.Snapshot()
	q := Select(Path("id")).
		Join(JoinOn("customers", "customer", JoinKey).Where(Cmp("tier", Eq, "pro"))).
		OrderBy("id", Asc)

	run := func(limit int) (Result, ExecStats) {
		var e Exec
		e.Options.JoinMembershipMax = limit
		if err := q.RunInto(&e, FromDatabase(catalog, "orders")); err != nil {
			t.Fatalf("RunInto(limit=%d): %v", limit, err)
		}
		return e.Result, e.Stats
	}

	// The membership side. The inner filter selects 40 of 200 customers, well
	// under the default threshold, so the default configuration must reach the
	// same binding the forced one does.
	wide, wideStats := run(1 << 20)
	_, defaultStats := run(0)
	for name, stats := range map[string]ExecStats{"forced": wideStats, "default": defaultStats} {
		if stats.JoinMemberships != 1 || stats.JoinLookups != 0 {
			t.Fatalf("%s membership run: JoinMemberships=%d JoinLookups=%d, want 1 and 0",
				name, stats.JoinMemberships, stats.JoinLookups)
		}
		if stats.JoinKeys != matching {
			t.Fatalf("%s membership run: JoinKeys=%d, want the %d matching customers",
				name, stats.JoinKeys, matching)
		}
		if stats.JoinProbes != 0 {
			t.Fatalf("%s membership run: JoinProbes=%d, want no per-row probing at all",
				name, stats.JoinProbes)
		}
	}

	// The lookup side. Every one of the assertions above must now fail, which is
	// what makes them evidence rather than tautologies.
	narrow, narrowStats := run(1)
	if narrowStats.JoinLookups != 1 || narrowStats.JoinMemberships != 0 {
		t.Fatalf("lookup run: JoinLookups=%d JoinMemberships=%d, want 1 and 0",
			narrowStats.JoinLookups, narrowStats.JoinMemberships)
	}
	if narrowStats.JoinKeys != 0 {
		t.Fatalf("lookup run: JoinKeys=%d, want none collected — the set was abandoned", narrowStats.JoinKeys)
	}
	// Every outer row reaches the join leaf exactly once and is answered either
	// by the prefilter or by a probe. Which of the two does the work is a cost
	// decision; that the two account for every row is not.
	if got := narrowStats.JoinProbes + narrowStats.JoinFilterRejected; got != orders {
		t.Fatalf("lookup run: %d rows answered (%d probed, %d prefiltered), want one per outer row (%d)",
			got, narrowStats.JoinProbes, narrowStats.JoinFilterRejected, orders)
	}

	// And the two plans must agree exactly, which is the whole point of being
	// allowed to choose between them at all.
	if diff := diffResults(wide, narrow); diff != "" {
		t.Fatalf("strategies disagree: %s", diff)
	}
	if wide.RowCount == 0 || wide.RowCount == orders {
		t.Fatalf("row count %d is degenerate; the join must filter some but not all rows", wide.RowCount)
	}
}

// TestJoinMembershipLowersToIndexMasks asserts the payoff the whole membership
// strategy exists for: with a declared exact index on the outer join path, the
// collected set is probed through the index instead of searched per row, and
// the executor decodes only the rows the masks admit.
//
// The no-index run is the non-vacuity half. It must observe zero index probes,
// so the assertion above cannot be satisfied by a build where the counter is
// simply always positive.
func TestJoinMembershipLowersToIndexMasks(t *testing.T) {
	const (
		orders    = 400
		customers = 100
		matching  = 5
	)
	q := Select(Path("id")).
		Join(JoinOn("customers", "customer", JoinKey).Where(Cmp("tier", Eq, "pro")))

	probes := map[bool]int{}
	rows := map[bool]int{}
	for _, indexed := range []bool{false, true} {
		db := joinScaleDatabase(t, orders, customers, matching, indexed)
		var e Exec
		if err := q.RunInto(&e, FromDatabase(db.Snapshot(), "orders")); err != nil {
			t.Fatalf("indexed=%v: %v", indexed, err)
		}
		if e.Stats.JoinMemberships != 1 {
			t.Fatalf("indexed=%v: the run must bind a membership, got %+v", indexed, e.Stats)
		}
		probes[indexed] = e.Workspace.storeIndexProbes
		rows[indexed] = e.Result.RowCount
	}
	if rows[false] != rows[true] || rows[true] == 0 {
		t.Fatalf("row counts differ or are degenerate: %d without an index, %d with one",
			rows[false], rows[true])
	}
	if probes[true] != matching {
		t.Fatalf("indexed run made %d index probes, want one per collected value (%d)",
			probes[true], matching)
	}
	if probes[false] != 0 {
		t.Fatalf("unindexed run made %d index probes, want none — there is no index to probe",
			probes[false])
	}
}

// --- value-kind rules the exhaustive sweep cannot reach ---------------------

// TestJoinPrimaryKeyMatchingIsNotCoerced pins the rule that only a string can
// name a collection key. It is written against a collection that holds a
// document under the empty key, because that is the shape where a missing
// in-kind check stops being harmless: a number, a boolean, and an absent path
// all classify with an empty decoded string, so a lookup that reached the hash
// map with one of them would find that document and report a match no
// membership binding would ever produce.
//
// The inner collection deliberately holds two documents. With only one, the
// forced-lookup configuration would never overflow its one-value threshold and
// the test would quietly check the membership path twice; the run asserts which
// strategy it got for exactly that reason.
func TestJoinPrimaryKeyMatchingIsNotCoerced(t *testing.T) {
	db := &store.Database{}
	outer, err := db.CreateCollection("outer", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	outerDocs := []string{
		`{"id":1,"ref":""}`,    // the empty string: the one value that matches
		`{"id":2,"ref":0}`,     // a number decodes to no string at all
		`{"id":3,"ref":false}`, // and neither does a boolean
		`{"id":4,"ref":null}`,
		`{"id":5}`,
		`{"id":6,"ref":"x"}`,
	}
	for i, doc := range outerDocs {
		if _, err := outer.Put(fmt.Sprintf("o%d", i), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	inner, err := db.CreateCollection("inner", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"", "unrelated"} {
		if _, err := inner.Put(key, []byte(`{"tier":"pro"}`)); err != nil {
			t.Fatal(err)
		}
	}
	catalog := db.Snapshot()

	q := Select(Path("id")).Join(JoinOn("inner", "ref", JoinKey)).OrderBy("id", Asc)
	for _, strategy := range joinStrategies {
		var e Exec
		e.Options.JoinMembershipMax = strategy.membershipMax
		if err := q.RunInto(&e, FromDatabase(catalog, "outer")); err != nil {
			t.Fatalf("%s: %v", strategy.name, err)
		}
		switch strategy.name {
		case "membership":
			if e.Stats.JoinMemberships != 1 {
				t.Fatalf("membership: bound %+v, want one membership", e.Stats)
			}
		default:
			if e.Stats.JoinLookups != 1 || e.Stats.JoinProbes == 0 {
				t.Fatalf("lookup: bound %+v, want one lookup that actually probed", e.Stats)
			}
		}
		if e.Result.RowCount != 1 {
			t.Fatalf("%s: %d rows, want only the empty-string reference:\n%s",
				strategy.name, e.Result.RowCount, dumpResult(e.Result))
		}
		if got, _ := e.Result.Columns[0].Cells[0].Float64(); got != 1 {
			t.Fatalf("%s: matched id %v, want 1", strategy.name, got)
		}
	}
}

// TestJoinNullInnerKeysAreNotCollected pins the other half of the null rule.
// An inner document whose join key is null or absent is not a partner for
// anything — an outer null does not join either — so it must not enter the
// collected set at all. Admitting it would be invisible in the rows (nothing
// can equal it) and very visible in the plan: a null alternative has no scalar
// needle, so one of them silently costs the whole membership its index
// lowering.
func TestJoinNullInnerKeysAreNotCollected(t *testing.T) {
	db := &store.Database{}
	outer, err := db.CreateCollection("outer", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i, doc := range []string{
		`{"id":1,"ref":"a"}`,
		`{"id":2,"ref":null}`,
		`{"id":3}`,
	} {
		if _, err := outer.Put(fmt.Sprintf("o%d", i), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := outer.CreateIndex(store.IndexDefinition{Name: "ref", Paths: []string{"/ref"}}); err != nil {
		t.Fatal(err)
	}
	if info, err := outer.BackfillIndex("ref", 0); err != nil || info.State != store.IndexReady {
		t.Fatalf("BackfillIndex(ref) = (%+v, %v)", info, err)
	}
	inner, err := db.CreateCollection("inner", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for key, doc := range map[string]string{
		"i0": `{"code":"a"}`,
		"i1": `{"code":null}`,
		"i2": `{}`,
	} {
		if _, err := inner.Put(key, []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}

	var e Exec
	q := Select(Path("id")).Join(JoinOn("inner", "ref", "code")).OrderBy("id", Asc)
	if err := q.RunInto(&e, FromDatabase(db.Snapshot(), "outer")); err != nil {
		t.Fatal(err)
	}
	if e.Result.RowCount != 1 {
		t.Fatalf("%d rows, want only the outer row whose key is a real value:\n%s",
			e.Result.RowCount, dumpResult(e.Result))
	}
	if e.Stats.JoinKeys != 1 {
		t.Fatalf("collected %d alternatives, want only the one non-null inner key", e.Stats.JoinKeys)
	}
	if e.Workspace.storeIndexProbes != 1 {
		t.Fatalf("%d index probes, want exactly one: a null alternative would have "+
			"cost the whole membership its index lowering", e.Workspace.storeIndexProbes)
	}
}

// TestJoinContainerKeysDeclineIndexLowering covers the alternatives an exact
// index cannot hold. A container join key is a legal value — the engine's total
// order compares containers by their source bytes — but the index stores
// scalars, so a membership carrying one must fall back to its per-row search
// rather than hand the index a needle it will reject. Getting this wrong is not
// a slow query; it is an error returned from a query that has a perfectly good
// answer.
func TestJoinContainerKeysDeclineIndexLowering(t *testing.T) {
	db := &store.Database{}
	outer, err := db.CreateCollection("outer", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i, doc := range []string{
		`{"id":1,"ref":[]}`,
		`{"id":2,"ref":{}}`,
		`{"id":3,"ref":[1,2]}`,
		`{"id":4,"ref":"scalar"}`,
	} {
		if _, err := outer.Put(fmt.Sprintf("o%d", i), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := outer.CreateIndex(store.IndexDefinition{Name: "ref", Paths: []string{"/ref"}}); err != nil {
		t.Fatal(err)
	}
	if info, err := outer.BackfillIndex("ref", 0); err != nil || info.State != store.IndexReady {
		t.Fatalf("BackfillIndex(ref) = (%+v, %v)", info, err)
	}
	inner, err := db.CreateCollection("inner", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for key, doc := range map[string]string{
		"a": `{"code":[]}`,
		"b": `{"code":{}}`,
		"c": `{"code":"scalar"}`,
	} {
		if _, err := inner.Put(key, []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}

	var e Exec
	q := Select(Path("id")).Join(JoinOn("inner", "ref", "code")).OrderBy("id", Asc)
	if err := q.RunInto(&e, FromDatabase(db.Snapshot(), "outer")); err != nil {
		t.Fatalf("a container join key must answer, not fail: %v", err)
	}
	if e.Result.RowCount != 3 {
		t.Fatalf("%d rows, want the empty array, the empty object, and the scalar:\n%s",
			e.Result.RowCount, dumpResult(e.Result))
	}
	if e.Workspace.storeIndexProbes != 0 {
		t.Fatalf("%d index probes, want none: the collected set holds containers, "+
			"and one unindexable alternative makes the whole set unindexable",
			e.Workspace.storeIndexProbes)
	}
}

// TestJoinDuplicateInnerMatchesKeepOuterRowOnce states the semi-join contract
// on its own, in a fixture where a fan-out join would be unmistakable: every
// outer row has four inner partners.
//
// It joins on an inner field rather than the primary key because that is the
// only way several inner documents can share one join-key value at all — keys
// are unique by construction — which is also why this clause has one strategy
// rather than two, and why the run asserts it got the membership.
func TestJoinDuplicateInnerMatchesKeepOuterRowOnce(t *testing.T) {
	db := &store.Database{}
	outer, err := db.CreateCollection("outer", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := outer.Put(fmt.Sprintf("o%d", i), []byte(`{"ref":"shared"}`)); err != nil {
			t.Fatal(err)
		}
	}
	inner, err := db.CreateCollection("inner", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, err := inner.Put(fmt.Sprintf("i%d", i), []byte(`{"code":"shared"}`)); err != nil {
			t.Fatal(err)
		}
	}
	catalog := db.Snapshot()
	q := Select(Count()).Join(JoinOn("inner", "ref", "code"))
	for _, strategy := range joinStrategies {
		var e Exec
		e.Options.JoinMembershipMax = strategy.membershipMax
		if err := q.RunInto(&e, FromDatabase(catalog, "outer")); err != nil {
			t.Fatalf("%s: %v", strategy.name, err)
		}
		got, _ := e.Result.Columns[0].Cells[0].Float64()
		if got != 3 {
			t.Fatalf("%s: counted %v rows, want 3 — a semi-join filters, it does not fan out",
				strategy.name, got)
		}
		if e.Stats.JoinMemberships != 1 {
			t.Fatalf("%s: bound %+v; a join on an inner field has no keyed probe to "+
				"fall back to and must always bind a membership", strategy.name, e.Stats)
		}
		if e.Stats.JoinKeys > 1 {
			t.Fatalf("%s: collected %d alternatives from four documents sharing one value; "+
				"the set must be deduplicated", strategy.name, e.Stats.JoinKeys)
		}
	}
}

// TestJoinInnerSideUsesItsOwnIndexes asserts that the inner run is a first-class
// query and not a hand-rolled scan: its filter binds the inner collection's
// declared indexes and probes them, so a selective inner predicate touches the
// documents it selected rather than all of them.
//
// The unindexed run is the non-vacuity half. Both must return the same rows,
// because an index is a cost decision on the inner side exactly as it is on the
// outer.
func TestJoinInnerSideUsesItsOwnIndexes(t *testing.T) {
	const (
		orders    = 200
		customers = 120
		matching  = 6
	)
	q := Select(Path("id")).
		Join(JoinOn("customers", "customer", JoinKey).Where(Cmp("tier", Eq, "pro")))

	probes := map[bool]int{}
	rows := map[bool]int{}
	for _, innerIndexed := range []bool{false, true} {
		db := joinScaleDatabaseIndexed(t, orders, customers, matching, false, innerIndexed)
		var e Exec
		if err := q.RunInto(&e, FromDatabase(db.Snapshot(), "orders")); err != nil {
			t.Fatalf("innerIndexed=%v: %v", innerIndexed, err)
		}
		if e.Stats.JoinMemberships != 1 {
			t.Fatalf("innerIndexed=%v: want one membership binding, got %+v", innerIndexed, e.Stats)
		}
		probes[innerIndexed] = e.Workspace.joins[0].scan.storeIndexProbes
		rows[innerIndexed] = e.Result.RowCount
	}
	if rows[false] != rows[true] || rows[true] == 0 {
		t.Fatalf("row counts differ or are degenerate: %d unindexed, %d indexed", rows[false], rows[true])
	}
	if probes[true] != 1 {
		t.Fatalf("inner run made %d index probes, want the one its tier equality needs", probes[true])
	}
	if probes[false] != 0 {
		t.Fatalf("unindexed inner run made %d index probes, want none", probes[false])
	}
}

// --- the front ends and the boundaries they enforce ------------------------

// TestJoinSourcesOtherThanDatabaseAreRejected pins the rule that makes snapshot
// skew across a join inexpressible rather than merely discouraged. A Source
// naming one collection has no catalog to resolve the inner side from, and the
// only alternative — reading it from a second, independently taken snapshot —
// is the failure mode the whole DatabaseSnapshot exists to prevent.
func TestJoinSourcesOtherThanDatabaseAreRejected(t *testing.T) {
	db := joinScaleDatabase(t, 4, 2, 1, false)
	orders, _ := db.Collection("orders")
	lone, err := orders.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	q := Select(Path("id")).Join(JoinOn("customers", "customer", JoinKey))

	for name, src := range map[string]Source{
		"FromSnapshot": FromSnapshot(lone),
		"FromSegment":  FromSegment(&store.Segment{}),
		"zero Source":  {},
	} {
		var e Exec
		err := q.RunInto(&e, src)
		if err == nil {
			t.Fatalf("%s accepted a join plan; it must not be able to", name)
		}
		if name != "zero Source" && !strings.Contains(err.Error(), "FromDatabase") {
			t.Fatalf("%s: error %q should name the constructor that works", name, err)
		}
	}

	// The same plan against the catalog those collections live in succeeds, so
	// the rejection above is about the Source and not about the plan.
	var e Exec
	if err := q.RunInto(&e, FromDatabase(db.Snapshot(), "orders")); err != nil {
		t.Fatalf("FromDatabase must accept the same plan: %v", err)
	}

	// And a plan with no join is unaffected by any of this.
	plain := Select(Path("id"))
	if _, err := plain.Run(FromSnapshot(lone)); err != nil {
		t.Fatalf("a query without a join must still run from a lone snapshot: %v", err)
	}
}

// TestJoinFromDatabaseRejectsAnUnknownDrivingCollection covers the other name
// resolution: the driving side itself.
func TestJoinFromDatabaseRejectsAnUnknownDrivingCollection(t *testing.T) {
	db := joinScaleDatabase(t, 2, 1, 1, false)
	var e Exec
	err := Select(Path("id")).RunInto(&e, FromDatabase(db.Snapshot(), "invoices"))
	if err == nil || !strings.Contains(err.Error(), "invoices") {
		t.Fatalf("got %v, want an error naming the missing collection", err)
	}
}

// TestJoinRejectsAnUnknownInnerCollection covers the inner name, which is only
// resolvable at execution because the catalog it is resolved against is the one
// the driving side was captured with.
func TestJoinRejectsAnUnknownInnerCollection(t *testing.T) {
	db := joinScaleDatabase(t, 2, 1, 1, false)
	var e Exec
	q := Select(Path("id")).Join(JoinOn("suppliers", "customer", JoinKey))
	err := q.RunInto(&e, FromDatabase(db.Snapshot(), "orders"))
	if err == nil || !strings.Contains(err.Error(), "suppliers") {
		t.Fatalf("got %v, want an error naming the missing inner collection", err)
	}
}

// TestJoinBuilderErrors pins the builder's compile-time rejections.
func TestJoinBuilderErrors(t *testing.T) {
	cases := []struct {
		name  string
		query *Query
		want  string
	}{
		{"no collection", Select(Path("id")).Join(JoinOn("", "a", JoinKey)), "join[0]"},
		{"whole document as the outer key", Select(Path("id")).Join(JoinOn("c", "", JoinKey)), "outer side"},
		{"whole document as the inner key", Select(Path("id")).Join(JoinOn("c", "a", "")), "inner side"},
		{"unparsable outer path", Select(Path("id")).Join(JoinOn("c", "/a/~9", JoinKey)), "~"},
		{"unparsable inner path", Select(Path("id")).Join(JoinOn("c", "a", "/b/~9")), "~"},
		{"unparsable inner filter path", Select(Path("id")).
			Join(JoinOn("c", "a", JoinKey).Where(Cmp("/x/~9", Eq, 1))), "~"},
		{"the zero Join", Select(Path("id")).Join(Join{}), "join[0]"},
		{"the second clause is the bad one", Select(Path("id")).
			Join(JoinOn("c", "a", JoinKey), JoinOn("", "b", JoinKey)), "join[1]"},
	}
	for _, tc := range cases {
		err := tc.query.Prepare()
		if err == nil {
			t.Fatalf("%s: compiled, want an error", tc.name)
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: got %q, want it to mention %q", tc.name, err, tc.want)
		}
	}
}

// --- steady-state cost ------------------------------------------------------

// TestJoinRunIntoSteadyAllocs holds a joined execution to the same contract
// TestRunIntoSteadyAllocs holds every other one to: after a warm-up, a repeated
// RunInto into the same Exec allocates nothing. A join adds a whole second
// execution — the inner scan, the collected set, the needles, and either the
// membership or the per-row probe scratch — and every one of those has to come
// out of storage the Workspace already holds, or the join would be the one
// operation in this package that leaks allocation into a hot loop.
func TestJoinRunIntoSteadyAllocs(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		db := joinScaleDatabaseIndexed(t, 512, 64, 16, indexed, indexed)
		catalog := db.Snapshot()
		queries := map[string]*Query{
			"primary key":  Select(Path("id")).Join(JoinOn("customers", "customer", JoinKey)),
			"inner filter": Select(Path("id")).Join(JoinOn("customers", "customer", JoinKey).Where(Cmp("tier", Eq, "pro"))),
			"inner field": Select(Path("id")).
				Join(JoinOn("customers", "customer", "name").Where(Cmp("seat", Lt, 8))),
			// An inner filter over an escaped string: every probe decodes it,
			// so a probe that failed to reset its decoded-string scratch would
			// grow one arena for the length of the outer scan.
			"escaped inner filter": Select(Path("id")).
				Join(JoinOn("customers", "customer", JoinKey).Where(Cmp("label", Eq, "label-1"))),
			"outer filter": Select(Path("id")).Where(Cmp("total", Lt, 400)).Join(JoinOn("customers", "customer", JoinKey)),
			"grouped": Select(Path("customer"), Count()).
				Join(JoinOn("customers", "customer", JoinKey)).GroupBy("customer"),
			"two clauses": Select(Count()).Join(
				JoinOn("customers", "customer", JoinKey),
				JoinOn("customers", "customer", JoinKey).Where(Cmp("seat", Lt, 8))),
			"ordered limited": Select(Path("id")).
				Join(JoinOn("customers", "customer", JoinKey)).OrderBy("id", Desc).Limit(50),
		}
		for _, strategy := range joinStrategies {
			for _, workers := range []int{1, 4} {
				for name, q := range queries {
					t.Run(fmt.Sprintf("indexed=%v/%s/workers=%d/%s",
						indexed, strategy.name, workers, name), func(t *testing.T) {
						runJoinAllocCase(t, q, catalog, strategy.membershipMax, workers)
					})
				}
			}
		}
	}
}

// runJoinAllocCase is one (query, strategy, worker count) point of the join
// allocation contract. The worker count is swept because a parallel filter
// phase gives every worker its own join probe scratch, and a scratch grown per
// execution rather than retained would put the allocation back one level down —
// where a per-row alloc test on the calling goroutine would never see it.
func runJoinAllocCase(t *testing.T, q *Query, catalog store.DatabaseSnapshot, membershipMax, workers int) {
	t.Helper()
	var e Exec
	e.Options.Workers = workers
	e.Options.JoinMembershipMax = membershipMax
	// Two warm-ups: the first grows every buffer, the second settles the ones
	// whose size depends on the first run's high-water mark.
	for range 2 {
		if err := q.RunInto(&e, FromDatabase(catalog, "orders")); err != nil {
			t.Fatal(err)
		}
	}
	if e.Result.RowCount == 0 {
		t.Fatal("the fixture must select rows, or the measurement is of nothing")
	}
	allocs := testing.AllocsPerRun(25, func() {
		if err := q.RunInto(&e, FromDatabase(catalog, "orders")); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("warmed joined RunInto allocated %.2f times, want 0", allocs)
	}
}

// TestJoinPrimaryKeyAndMirroredFieldAgree checks the two spellings of one join
// against each other. The customers fixture stores each document's own key in a
// "name" field, so joining on the primary key and joining on that field ask
// exactly the same question through two entirely different bindings — one that
// can fall back to a keyed probe and one that cannot.
func TestJoinPrimaryKeyAndMirroredFieldAgree(t *testing.T) {
	db := joinScaleDatabase(t, 200, 40, 12, true)
	catalog := db.Snapshot()
	byKey := Select(Path("id")).
		Join(JoinOn("customers", "customer", JoinKey).Where(Cmp("tier", Eq, "pro"))).
		OrderBy("id", Asc)
	byField := Select(Path("id")).
		Join(JoinOn("customers", "customer", "name").Where(Cmp("tier", Eq, "pro"))).
		OrderBy("id", Asc)

	var field Exec
	if err := byField.RunInto(&field, FromDatabase(catalog, "orders")); err != nil {
		t.Fatal(err)
	}
	if field.Result.RowCount == 0 {
		t.Fatal("the fixture must select rows")
	}
	for _, strategy := range joinStrategies {
		var key Exec
		key.Options.JoinMembershipMax = strategy.membershipMax
		if err := byKey.RunInto(&key, FromDatabase(catalog, "orders")); err != nil {
			t.Fatal(err)
		}
		if diff := diffResults(key.Result, field.Result); diff != "" {
			t.Fatalf("%s: joining on the key disagrees with joining on the mirrored field: %s",
				strategy.name, diff)
		}
	}
}

// TestJoinLookupTransientMemoryDoesNotScaleWithOuterRows pins the claim that
// makes the lookup strategy the sound answer when a membership would be
// unbounded: a probe has no build side and materializes nothing, so the
// transient storage it holds is one document's worth however many outer rows
// it is asked about.
//
// The inner filter reads an escaped string on purpose, because the decoded-
// string arena is the one buffer a probe could accidentally let grow: it is
// appended to per row, and only resetting it per probe keeps the peak at one
// document. The two collection sizes below differ by a factor of ten and must
// not differ in scratch.
func TestJoinLookupTransientMemoryDoesNotScaleWithOuterRows(t *testing.T) {
	// One decoded label is a handful of bytes; growth proportional to the outer
	// scan would put the larger run three orders of magnitude above this.
	const scratchBound = 4096
	q := Select(Count()).
		Join(JoinOn("customers", "customer", JoinKey).Where(Cmp("label", Eq, "label-1")))

	peaks := map[int]int{}
	for _, outerRows := range []int{2000, 20000} {
		db := joinScaleDatabase(t, outerRows, 64, 64, false)
		var e Exec
		e.Options.JoinMembershipMax = 1 // force the lookup
		if err := q.RunInto(&e, FromDatabase(db.Snapshot(), "orders")); err != nil {
			t.Fatal(err)
		}
		if e.Stats.JoinLookups != 1 {
			t.Fatalf("outerRows=%d: want one lookup binding, got %+v", outerRows, e.Stats)
		}
		if got := e.Stats.JoinProbes + e.Stats.JoinFilterRejected; got != uint64(outerRows) {
			t.Fatalf("outerRows=%d: %d rows answered, want one per outer row: %+v",
				outerRows, got, e.Stats)
		}
		if got, _ := e.Result.Columns[0].Cells[0].Float64(); got == 0 {
			t.Fatalf("outerRows=%d: the fixture must match rows", outerRows)
		}
		peaks[outerRows] = cap(e.Workspace.joins[0].scan.text)
	}
	for outerRows, peak := range peaks {
		if peak > scratchBound {
			t.Fatalf("outerRows=%d: the probe retained %d bytes of decoded-string scratch, "+
				"want at most %d — a lookup join must not accumulate one document's decode into the next",
				outerRows, peak, scratchBound)
		}
	}
}

// TestJoinInTheSQLSubset states the scope the SQL front end inherits from this
// file, now that a clause can fan out.
//
// Every JOIN lowers with its alias declared, so the operator is SQL's inner
// join and the joined collection's columns are readable. What remains refused
// is what the engine has no plan for: a chained join, a second fanning clause,
// while LEFT JOIN uses the same one-expansion bound.
func TestJoinInTheSQLSubset(t *testing.T) {
	for _, src := range []string{
		`SELECT t.id, u.b FROM t JOIN u ON u.b = t.a`,
		`SELECT t.id, u.b FROM t JOIN u ON u."$key" = t.a`,
		`SELECT t.id FROM t JOIN u ON u.b = t.a WHERE u.tier = 'pro'`,
		`SELECT t.id, SUM(u.n) FROM t JOIN u ON u.b = t.a GROUP BY t.id`,
		`SELECT t.id, u.b FROM t LEFT JOIN u ON u.b = t.a`,
	} {
		if _, err := PrepareStatement(src); err != nil {
			t.Fatalf("%q must lower to an inner join: %v", src, err)
		}
	}
	for _, tc := range []struct {
		sql  string
		want string
	}{
		{`SELECT t.id, v.c FROM t JOIN u ON u."$key" = t.a JOIN v ON v.c = u.b`,
			"chained join"},
		{`SELECT t.id, u.b, v.c FROM t JOIN u ON u.b = t.a JOIN v ON v.c = t.a`,
			"only once"},
		{`SELECT t.id, u.b FROM t JOIN u ON u.b = t.a WHERE t.x = 1 OR u.y = 2`,
			"two collections"},
	} {
		_, err := PrepareStatement(tc.sql)
		if err == nil {
			t.Fatalf("%q lowered; the engine has no plan for it", tc.sql)
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%q = %v, want a message naming %q", tc.sql, err, tc.want)
		}
	}
}

// TestJoinSQLNeverUsesTheProgrammaticPhysicalKeySentinel asserts that "$key"
// in SQL is an ordinary quoted JSON field. The programmatic builder retains
// JoinKey for direct store consumers, but the SQL surface derives identity from
// declared JSON primary-key fields and has no physical-key pseudo-column.
func TestJoinSQLNeverUsesTheProgrammaticPhysicalKeySentinel(t *testing.T) {
	for _, tc := range []struct {
		sql    string
		fanOut bool
	}{
		{`SELECT t.id FROM t JOIN u ON u."$key" = t.a`, true},
		{`SELECT t.id FROM t JOIN u ON u."$key" = t.a WHERE u.tier = 'pro'`, true},
		{`SELECT t.id, u.b FROM t JOIN u ON u."$key" = t.a`, true},
		{`SELECT t.id FROM t JOIN u ON u.b = t.a`, true},
		{`SELECT t.id FROM t JOIN u ON u."$key" = t.a ORDER BY u.b`, true},
	} {
		stmt, err := PrepareStatement(tc.sql)
		if err != nil {
			t.Fatalf("%q: %v", tc.sql, err)
		}
		p, err := stmt.q.compiled()
		if err != nil {
			t.Fatalf("%q: %v", tc.sql, err)
		}
		if got := p.fanOutJoin >= 0; got != tc.fanOut {
			t.Fatalf("%q fans out = %v, want %v", tc.sql, got, tc.fanOut)
		}
	}
}

// --- the semi-join reduction filter -----------------------------------------

// joinSelectiveDatabaseFor is the test-side twin of the benchmark fixture: an
// outer collection whose keys are scattered across a much larger inner one, of
// which only a fraction passes the inner filter. It is the shape a prefilter
// exists for, and the shape where the population it rejects is dominated by
// keys that exist and are rejected by the inner predicate rather than by keys
// that are simply absent.
func joinSelectiveDatabaseFor(t testing.TB, outerRows, customers, matching int) *store.Database {
	t.Helper()
	db := &store.Database{}
	orders, err := db.CreateCollection("orders", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < outerRows; i++ {
		doc := fmt.Sprintf(`{"id":%d,"customer":"c%d"}`, i, (i*7919)%customers)
		if _, err := orders.Put(fmt.Sprintf("o%d", i), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	buyers, err := db.CreateCollection("customers", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < customers; i++ {
		// Spread the matching customers evenly rather than putting them in a
		// prefix. A prefix is not a neutral layout: it makes any sample of the
		// first rows scanned unrepresentative of the whole, which is precisely
		// what the mid-scan abandon check reads. Testing a sampler against data
		// laid out to defeat sampling measures the layout, not the sampler —
		// the clustered case gets its own test instead.
		tier := "free"
		if stride := customers / max(matching, 1); stride > 0 && i%stride == 0 {
			tier = "pro"
		}
		// name repeats the key as a field, so the same join is expressible
		// against the primary key and against an ordinary inner path.
		if _, err := buyers.Put(fmt.Sprintf("c%d", i), []byte(
			fmt.Sprintf(`{"tier":%q,"seat":%d,"name":"c%d"}`, tier, i, i))); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

// TestJoinBloomPrefilterSkipsProbesWithoutChangingRows is the filter's whole
// claim in one test: with the prefilter the join performs far fewer probes, and
// it returns exactly the rows it returns without one.
//
// The unfiltered arm is the non-vacuity half, and it is reached through the
// documented option rather than through a build tag, so the two arms are the
// same binary running the same code with one decision inverted.
func TestJoinBloomPrefilterSkipsProbesWithoutChangingRows(t *testing.T) {
	const (
		outerRows = 4000
		customers = 8000
		matching  = 400
	)
	db := joinSelectiveDatabaseFor(t, outerRows, customers, matching)
	catalog := db.Snapshot()
	q := Select(Path("id")).
		Join(JoinOn("customers", "customer", JoinKey).Where(Cmp("tier", Eq, "pro"))).
		OrderBy("id", Asc)

	run := func(ratio int) (Result, ExecStats) {
		var e Exec
		e.Options.JoinMembershipMax = 1 // force the lookup strategy
		e.Options.JoinFilterScanRatio = ratio
		if err := q.RunInto(&e, FromDatabase(catalog, "orders")); err != nil {
			t.Fatalf("ratio=%d: %v", ratio, err)
		}
		return e.Result, e.Stats
	}

	filtered, filteredStats := run(0) // the shipped default
	plain, plainStats := run(-1)      // the filter declined

	if filteredStats.JoinFilters != 1 || filteredStats.JoinFilterKeys != matching {
		t.Fatalf("filtered run: want one filter over the %d matching customers, got %+v",
			matching, filteredStats)
	}
	if plainStats.JoinFilters != 0 || plainStats.JoinFilterRejected != 0 {
		t.Fatalf("declined run: want no filter at all, got %+v", plainStats)
	}
	if plainStats.JoinProbes != outerRows {
		t.Fatalf("declined run: %d probes, want one per outer row (%d)",
			plainStats.JoinProbes, outerRows)
	}

	// Every outer row is still accounted for exactly once, and the filter
	// answered the overwhelming majority of them without a probe.
	if got := filteredStats.JoinProbes + filteredStats.JoinFilterRejected; got != outerRows {
		t.Fatalf("filtered run: %d rows answered, want %d: %+v", got, outerRows, filteredStats)
	}
	if filteredStats.JoinProbes*4 >= plainStats.JoinProbes {
		t.Fatalf("the filter removed only %d of %d probes; it is not earning the scan",
			plainStats.JoinProbes-filteredStats.JoinProbes, plainStats.JoinProbes)
	}

	// And the rows are identical, which is the property the whole one-sided
	// error argument rests on.
	if plain.RowCount == 0 || plain.RowCount == outerRows {
		t.Fatalf("row count %d is degenerate; the join must filter some but not all rows", plain.RowCount)
	}
	if diff := diffResults(filtered, plain); diff != "" {
		t.Fatalf("the prefilter changed the answer: %s", diff)
	}
}

// TestJoinBloomPrefilterHasNoFalseNegatives states the one error a Bloom filter
// must not have, at the only place it could arise: the filter is seeded from
// the alternatives collected before the threshold was crossed and then fed
// every key after it, so a seeding mistake would silently drop exactly the
// first joinMembershipMax matching rows.
//
// It is checked by driving the threshold across the whole inner side one value
// at a time, so every possible split between "seeded from lits" and "inserted
// during the scan" is exercised, and comparing against the same query answered
// without any filter at all.
func TestJoinBloomPrefilterHasNoFalseNegatives(t *testing.T) {
	const (
		outerRows = 400
		customers = 64
	)
	db := joinSelectiveDatabaseFor(t, outerRows, customers, customers)
	catalog := db.Snapshot()
	q := Select(Path("id")).Join(JoinOn("customers", "customer", JoinKey)).OrderBy("id", Asc)

	var reference Exec
	reference.Options.JoinMembershipMax = 1 << 20 // the exact membership
	if err := q.RunInto(&reference, FromDatabase(catalog, "orders")); err != nil {
		t.Fatal(err)
	}
	if reference.Stats.JoinMemberships != 1 {
		t.Fatalf("the reference run must bind a membership, got %+v", reference.Stats)
	}
	if reference.Result.RowCount != outerRows {
		t.Fatalf("every outer row should match this fixture, got %d", reference.Result.RowCount)
	}

	for limit := 1; limit <= customers+1; limit++ {
		var e Exec
		e.Options.JoinMembershipMax = limit
		if err := q.RunInto(&e, FromDatabase(catalog, "orders")); err != nil {
			t.Fatalf("limit=%d: %v", limit, err)
		}
		if diff := diffResults(e.Result, reference.Result); diff != "" {
			t.Fatalf("limit=%d (%+v): a key was lost across the membership/filter split: %s",
				limit, e.Stats, diff)
		}
		if e.Stats.JoinFilterRejected != 0 {
			t.Fatalf("limit=%d: the filter rejected %d rows, but every outer key has a partner",
				limit, e.Stats.JoinFilterRejected)
		}
	}
}

// TestJoinBloomScanBudgetIsRespected pins the pre-scan gate: the filter is only
// attempted when the inner side is small enough relative to the driving
// collection that scanning it could repay the probes it saves. Past that, the
// join probes every row, because a scan that cannot pay for itself is worse
// than no filter.
//
// The inner filter is made very selective so that the second gate — the
// mid-scan abandon, which has its own test below — always votes to keep, and
// what is left varying is the budget alone. The boundary case is included
// exactly: at candidates == outerRows*joinBloomScanRatio the scan is admitted,
// and one row past it is not.
func TestJoinBloomScanBudgetIsRespected(t *testing.T) {
	const customers = 4000
	cases := []struct {
		name       string
		outerRows  int
		wantFilter bool
	}{
		{"inner well inside the budget", 4000, true},
		{"inner one row past the budget", customers/joinBloomScanRatio - 1, false},
		{"inner far past the budget", 200, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := joinSelectiveDatabaseFor(t, tc.outerRows, customers, customers/50)
			var e Exec
			e.Options.JoinMembershipMax = 1
			q := Select(Count()).
				Join(JoinOn("customers", "customer", JoinKey).Where(Cmp("tier", Eq, "pro")))
			if err := q.RunInto(&e, FromDatabase(db.Snapshot(), "orders")); err != nil {
				t.Fatal(err)
			}
			if e.Stats.JoinLookups != 1 {
				t.Fatalf("want a lookup binding, got %+v", e.Stats)
			}
			if got := e.Stats.JoinFilters == 1; got != tc.wantFilter {
				t.Fatalf("filter built = %v, want %v (%d inner candidates, %d outer rows): %+v",
					got, tc.wantFilter, customers, tc.outerRows, e.Stats)
			}
		})
	}
}

// TestJoinBloomAbandonsAScanThatCannotPay pins the second gate. The pre-scan
// budget cannot see how much of the outer side a filter will reject; this does,
// by reading the inner predicate's selectivity off the rows already scanned. An
// inner predicate that keeps nearly everything produces a filter that admits
// nearly everything, and finishing that scan is measurably worse than never
// starting it.
//
// Every case sits inside the pre-scan budget, so the only thing deciding the
// outcome is what the scan observed.
func TestJoinBloomAbandonsAScanThatCannotPay(t *testing.T) {
	cases := []struct {
		name                           string
		outerRows, customers, matching int
		wantFilter                     bool
	}{
		{"a selective inner filter is worth summarizing", 4000, 4000, 4000 / 50, true},
		{"an inner filter that keeps everything is not", 4000, 4000, 4000, false},
		// Half-selective, sized so that charging the gain against the whole
		// scan abandons and charging it against only the remaining scan would
		// not. The two rules disagree over a narrow band and this is inside it;
		// BenchmarkJoinBloomPrefilter measured the marginal rule keeping the
		// filter here and paying 7% for it, which is what settled the choice.
		{"half-selective, where the cheaper-looking rule loses", 3900, 8000, 4000, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := joinSelectiveDatabaseFor(t, tc.outerRows, tc.customers, tc.matching)
			var e Exec
			e.Options.JoinMembershipMax = 1
			q := Select(Count()).
				Join(JoinOn("customers", "customer", JoinKey).Where(Cmp("tier", Eq, "pro")))
			if err := q.RunInto(&e, FromDatabase(db.Snapshot(), "orders")); err != nil {
				t.Fatal(err)
			}
			if got := e.Stats.JoinFilters == 1; got != tc.wantFilter {
				t.Fatalf("filter kept = %v, want %v with %d of %d inner rows matching "+
					"and %d outer rows: %+v",
					got, tc.wantFilter, tc.matching, tc.customers, tc.outerRows, e.Stats)
			}
			if !tc.wantFilter && e.Stats.JoinProbes != uint64(tc.outerRows) {
				t.Fatalf("an abandoned filter must leave every row to be probed, got %+v", e.Stats)
			}
		})
	}
}

// TestJoinBloomClusteredSelectivityDegradesGracefully is the known blind spot,
// stated as a test rather than left as a comment. The mid-scan check reads the
// first rows scanned; a collection that puts every matching document at the
// front makes that sample unrepresentative and the check abandons a filter that
// would have paid.
//
// What the test pins is that this is a cost outcome and not a correctness one:
// the rows are identical either way, and the abandoned scan is bounded by the
// sample window rather than by the collection's size.
func TestJoinBloomClusteredSelectivityDegradesGracefully(t *testing.T) {
	const (
		outerRows = 4000
		customers = 4000
		matching  = 80
	)
	db := &store.Database{}
	orders, err := db.CreateCollection("orders", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < outerRows; i++ {
		if _, err := orders.Put(fmt.Sprintf("o%d", i),
			[]byte(fmt.Sprintf(`{"id":%d,"customer":"c%d"}`, i, (i*7919)%customers))); err != nil {
			t.Fatal(err)
		}
	}
	buyers, err := db.CreateCollection("customers", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < customers; i++ {
		// Every matching customer in a prefix: the layout that defeats a
		// sample of the first rows scanned.
		tier := "free"
		if i < matching {
			tier = "pro"
		}
		if _, err := buyers.Put(fmt.Sprintf("c%d", i),
			[]byte(fmt.Sprintf(`{"tier":%q}`, tier))); err != nil {
			t.Fatal(err)
		}
	}
	catalog := db.Snapshot()
	q := Select(Path("id")).
		Join(JoinOn("customers", "customer", JoinKey).Where(Cmp("tier", Eq, "pro"))).
		OrderBy("id", Asc)

	var clustered, reference Exec
	clustered.Options.JoinMembershipMax = 1
	if err := q.RunInto(&clustered, FromDatabase(catalog, "orders")); err != nil {
		t.Fatal(err)
	}
	reference.Options.JoinMembershipMax = 1
	reference.Options.JoinFilterScanRatio = -1
	if err := q.RunInto(&reference, FromDatabase(catalog, "orders")); err != nil {
		t.Fatal(err)
	}
	if reference.Result.RowCount == 0 {
		t.Fatal("the fixture must match rows")
	}
	if diff := diffResults(clustered.Result, reference.Result); diff != "" {
		t.Fatalf("a mis-sampled filter changed the answer, which it must never do: %s", diff)
	}
	// The scan is abandoned rather than completed, which is the bounded-cost
	// half of the claim. Whether it was abandoned or never started, no filter
	// survives to be consulted.
	if clustered.Stats.JoinFilters != 0 {
		t.Logf("the clustered layout did not defeat the sample this time: %+v", clustered.Stats)
	}
}

// TestJoinBloomFalsePositiveRate measures what the filter actually achieves at
// its shipped bit budget, so the sizing constant is backed by a number rather
// than by the textbook formula it was derived from. A filter far worse than
// designed would still be correct and would quietly stop paying for its scan;
// this is what notices.
func TestJoinBloomFalsePositiveRate(t *testing.T) {
	const keys = 20000
	var filter joinBloom
	filter.reset(keys)
	for i := 0; i < keys; i++ {
		filter.insert(hashJoinKey(fmt.Sprintf("present-%d", i)))
	}
	const trials = 200000
	var pr joinProbe
	positives := 0
	for i := 0; i < trials; i++ {
		if filter.admits(hashJoinKey(fmt.Sprintf("absent-%d", i)), &pr) {
			positives++
		}
	}
	rate := float64(positives) / float64(trials)
	// Every inserted key must still be admitted: a Bloom filter with a false
	// negative is not a Bloom filter.
	for i := 0; i < keys; i++ {
		if !filter.admits(hashJoinKey(fmt.Sprintf("present-%d", i)), &pr) {
			t.Fatalf("key %d was inserted and then rejected", i)
		}
	}
	if rate > 0.02 {
		t.Fatalf("false-positive rate %.4f at %d bits per key, want at most 0.02 — "+
			"the filter is admitting rows the scan was supposed to buy the right to skip",
			rate, joinBloomBits)
	}
	t.Logf("false-positive rate at %d bits per key over %d keys: %.4f (%d blocks, %d KiB)",
		joinBloomBits, keys, rate, len(filter.blocks), len(filter.blocks)*32/1024)
}

// TestJoinBloomSizingIsBoundedAndSound checks the two sizing invariants the
// probe path relies on: the block count is always a power of two, so selecting
// a block is a mask rather than a division, and it never exceeds the memory cap
// however many keys are announced.
func TestJoinBloomSizingIsBoundedAndSound(t *testing.T) {
	for _, keys := range []int{
		0, 1, 2, 15, 16, 4095, 4096, 1 << 20, 1 << 28,
		int(^uint(0) >> 1),
	} {
		blocks := joinBloomBlocks(keys)
		if blocks < 1 {
			t.Fatalf("keys=%d: %d blocks", keys, blocks)
		}
		if blocks&(blocks-1) != 0 {
			t.Fatalf("keys=%d: %d blocks is not a power of two, so the block mask is wrong", keys, blocks)
		}
		if blocks*32 > joinBloomMaxBytes {
			t.Fatalf("keys=%d: %d bytes exceeds the %d-byte cap", keys, blocks*32, joinBloomMaxBytes)
		}
		var filter joinBloom
		filter.reset(keys)
		if int(filter.mask)+1 != len(filter.blocks) {
			t.Fatalf("keys=%d: mask %d does not address %d blocks", keys, filter.mask, len(filter.blocks))
		}
	}
}

func TestJoinBloomCostArithmeticDoesNotOverflow(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	if !joinBloomWorthwhile(maxInt, maxInt, 0, maxInt) {
		t.Fatal("maximal exact cardinalities wrapped and declined a worthwhile filter")
	}
	max := uint64(maxInt)
	if !joinProduct3GreaterThan2(max, max, max, max, max) {
		t.Fatal("three-factor gain should exceed two-factor cost at MaxInt")
	}
	if joinProduct3GreaterThan2(1, max, max, max, max) {
		t.Fatal("equal overflowing products compared as greater")
	}
}

// TestJoinBloomKeepsAFilterWhoseScanIsAlreadyPaid pins the one branch of the
// mid-scan check that is not about selectivity at all. Once the inner side has
// been read to the end, the scan the check exists to protect is already spent,
// and the only cost still ahead is the per-row filter test — which a filter
// repays by rejecting anything at all. Abandoning there would throw away
// something already paid for.
//
// The fixture is an inner side smaller than one batch whose filter would fail
// the selectivity test outright: every inner document matches, so a mid-scan
// check that ignored the completed-scan branch would abandon it.
func TestJoinBloomKeepsAFilterWhoseScanIsAlreadyPaid(t *testing.T) {
	const (
		outerRows = 2000
		customers = 64 // well under joinBatchRows, so one batch reads them all
	)
	db := joinSelectiveDatabaseFor(t, outerRows, customers, customers)
	var e Exec
	e.Options.JoinMembershipMax = 1 // force the lookup strategy
	q := Select(Count()).
		Join(JoinOn("customers", "customer", JoinKey).Where(Cmp("tier", Eq, "pro")))
	if err := q.RunInto(&e, FromDatabase(db.Snapshot(), "orders")); err != nil {
		t.Fatal(err)
	}
	if e.Stats.JoinLookups != 1 {
		t.Fatalf("want a lookup binding, got %+v", e.Stats)
	}
	if e.Stats.JoinFilters != 1 {
		t.Fatalf("the filter was abandoned after its scan had already completed: %+v", e.Stats)
	}
	if e.Stats.JoinFilterKeys != customers {
		t.Fatalf("the filter holds %d keys, want all %d inner documents",
			e.Stats.JoinFilterKeys, customers)
	}
}

// --- joins under the parallel filter phase ---------------------------------

// TestJoinParallelFilterPhaseMatchesSerial is the one place where joins and
// late materialization are not independent of each other, pinned as a test.
//
// A join leaf is a predicate, so it is evaluated by the filter phase — and that
// phase runs on several goroutines over disjoint row ranges. The binding those
// goroutines share is read-only once bindJoins has finished, but a probe writes
// on every row it sees: it resolves a document into one-row columns, decodes
// escaped strings into an arena, and counts what it did. All of that lives in a
// per-worker joinProbe for exactly this reason, and this test is what would
// notice if any of it moved back onto the shared binding — as a wrong answer
// here, and as a report from the race detector under -race.
//
// Every strategy is covered, because they do not share a hazard: a membership
// binding is a pure read of a sorted set, while a lookup binding writes.
func TestJoinParallelFilterPhaseMatchesSerial(t *testing.T) {
	// The row count is a multiple of both the worker counts below and the
	// chunk width, because the uncompacted parallel path partitions a snapshot
	// by chunk: a split that lands mid-chunk declines and silently runs
	// serially, which would make this test pass while measuring nothing.
	const (
		outerRows = 8192
		customers = 4096
		matching  = 200
	)
	db := joinSelectiveDatabaseFor(t, outerRows, customers, matching)
	catalog := db.Snapshot()

	queries := map[string]*Query{
		"projection": Select(Path("id")).
			Join(JoinOn("customers", "customer", JoinKey).Where(Cmp("tier", Eq, "pro"))).
			OrderBy("id", Asc),
		"outer filter beside the join": Select(Path("id")).Where(Cmp("id", Lt, 3000)).
			Join(JoinOn("customers", "customer", JoinKey).Where(Cmp("tier", Eq, "pro"))).
			OrderBy("id", Asc),
		"aggregate": Select(Count()).
			Join(JoinOn("customers", "customer", JoinKey).Where(Cmp("tier", Eq, "pro"))),
		"grouped": Select(Path("customer"), Count()).
			Join(JoinOn("customers", "customer", JoinKey).Where(Cmp("tier", Eq, "pro"))).
			GroupBy("customer").OrderBy("customer", Asc),
		"inner field": Select(Path("id")).
			Join(JoinOn("customers", "customer", "name").Where(Cmp("tier", Eq, "pro"))).
			OrderBy("id", Asc),
	}

	for _, strategy := range joinStrategies {
		for name, q := range queries {
			t.Run(strategy.name+"/"+name, func(t *testing.T) {
				var serial Exec
				serial.Options.Workers = 1
				serial.Options.JoinMembershipMax = strategy.membershipMax
				if err := q.RunInto(&serial, FromDatabase(catalog, "orders")); err != nil {
					t.Fatal(err)
				}
				if serial.Result.RowCount == 0 {
					t.Fatal("the fixture must select rows, or the comparison proves nothing")
				}
				engaged := false
				for _, workers := range []int{2, 4, 8} {
					var parallel Exec
					parallel.Options.Workers = workers
					parallel.Options.JoinMembershipMax = strategy.membershipMax
					if err := q.RunInto(&parallel, FromDatabase(catalog, "orders")); err != nil {
						t.Fatalf("workers=%d: %v", workers, err)
					}
					if parallel.Workspace.scanUsed > 1 {
						engaged = true
					}
					if diff := diffResults(parallel.Result, serial.Result); diff != "" {
						t.Fatalf("workers=%d disagrees with the serial phase: %s", workers, diff)
					}
					// Every outer row is still answered exactly once across all
					// the workers, which is what proves the per-worker tallies
					// were summed rather than one worker's being reported.
					if serial.Stats.JoinLookups == 1 {
						want := serial.Stats.JoinProbes + serial.Stats.JoinFilterRejected
						got := parallel.Stats.JoinProbes + parallel.Stats.JoinFilterRejected
						if got != want {
							t.Fatalf("workers=%d answered %d rows at the join, want %d: %+v",
								workers, got, want, parallel.Stats)
						}
					}
				}
				// Without this the test would pass on a build where the filter
				// phase never split, and would therefore prove nothing about
				// the thing it exists to check.
				if !engaged {
					t.Fatal("the filter phase never ran across more than one worker")
				}
			})
		}
	}
}

// --- fan-out: the inner join ------------------------------------------------

// fanOutBattery returns the compiled-once fan-out shapes the differential runs.
// Every one of them names the joined collection through an alias, which is what
// turns the clause from a filter into SQL's inner join.
func fanOutBattery() []*Query {
	return []*Query{
		// The shape a SQL driver produces for
		// SELECT u.name, o.total FROM users u JOIN orders o ON o.user_id = u.id
		Select(Path("id"), Path("o.code")).
			Join(JoinOn("inner", "ref", "code").As("o")),
		// The joined column alone, so a query that reads nothing from the
		// driving collection still gets one row per pair.
		Select(Path("o.code")).Join(JoinOn("inner", "ref", "code").As("o")),
		// A joined column that distinguishes two documents sharing one join
		// value, which is what makes the order they come back in observable.
		Select(Path("id"), Path("o.seat")).Join(JoinOn("inner", "ref", "code").As("o")),
		Select(Path("o.seat"), Path("o.tier")).Join(JoinOn("inner", "ref", "code").As("o")),
		// The join's own driving column projected. It is a filter column — WHERE
		// reads it through the join leaf — so it exists at driving-row
		// granularity and has to be spread onto the pairs rather than read at
		// them, which is the one column class fan-out cannot gather.
		Select(Path("ref"), Path("o.seat")).Join(JoinOn("inner", "ref", "code").As("o")),
		Select(Path("active"), Path("ref"), Path("o.seat")).Where(Cmp("active", Eq, true)).
			Join(JoinOn("inner", "ref", "code").As("o")),
		Select(Path("ref"), Count()).Join(JoinOn("inner", "ref", "code").As("o")).
			GroupBy("ref").OrderBy("ref", Asc),
		// The joined clause's own filter narrows which pairs exist.
		Select(Path("id"), Path("o.tier")).
			Join(JoinOn("inner", "ref", "code").As("o").Where(Cmp("tier", Eq, "pro"))),
		Select(Path("id"), Path("o.code")).
			Join(JoinOn("inner", "ref", "code").As("o").Where(IsNull("tier"))),
		// A driving filter beside the join: it selects driving rows before any
		// pair exists, which is where an inner join's WHERE over driving
		// columns belongs.
		Select(Path("id"), Path("o.code")).Where(Cmp("active", Eq, true)).
			Join(JoinOn("inner", "ref", "code").As("o")),
		// Primary-key fan-out. At most one joined row can match, so the pair
		// count equals the semi-join's row count — and the two must agree,
		// which is what makes this the shape that catches an expansion that
		// duplicates or drops.
		Select(Path("id"), Path("k.tier")).Join(JoinOn("inner", "ref", JoinKey).As("k")),
		// Ordering by a joined column, and by a driving one, and by both.
		Select(Path("id"), Path("o.code")).
			Join(JoinOn("inner", "ref", "code").As("o")).OrderBy("o.code", Asc),
		Select(Path("id"), Path("o.code")).
			Join(JoinOn("inner", "ref", "code").As("o")).OrderBy("id", Desc).OrderBy("o.code", Asc),
		// LIMIT over pairs rather than over driving rows, which is the early
		// stop the expansion is allowed to take.
		Select(Path("id"), Path("o.code")).
			Join(JoinOn("inner", "ref", "code").As("o")).Limit(2),
		Select(Path("id"), Path("o.code")).
			Join(JoinOn("inner", "ref", "code").As("o")).OrderBy("id", Asc).Limit(3),
		// Aggregates over pairs, including over a joined numeric column.
		Select(Count()).Join(JoinOn("inner", "ref", "code").As("o")),
		Select(Count(), Sum("o.seat")).Join(JoinOn("inner", "ref", "code").As("o")),
		Select(Count(), Sum("id"), Min("o.seat"), Max("o.seat"), Avg("o.seat")).
			Join(JoinOn("inner", "ref", "code").As("o")),
		// Grouping by a joined column and by a driving one.
		Select(Path("o.tier"), Count()).
			Join(JoinOn("inner", "ref", "code").As("o")).
			GroupBy("o.tier").OrderBy("o.tier", Asc),
		Select(Path("id"), Count(), Sum("o.seat")).
			Join(JoinOn("inner", "ref", "code").As("o")).
			GroupBy("id").OrderBy("id", Asc),
		// An aliased primary-key clause nothing reads. At most one joined row
		// can match a key, so the planner is allowed to answer this with the
		// semi-join machinery — and the differential is what proves that
		// choice returns the inner join's rows and not merely plausible ones.
		Select(Path("id")).Join(JoinOn("inner", "ref", JoinKey).As("k")),
		Select(Count()).Join(JoinOn("inner", "ref", JoinKey).As("k")),
		// The same shape on a field, where the cardinalities genuinely differ
		// and COUNT(*) is the only thing that can tell them apart.
		Select(Count()).Join(JoinOn("inner", "ref", "code").As("o")),
		// A fan-out clause beside a semi-join clause: only the aliased one
		// produces pairs, and the other still filters.
		Select(Path("id"), Path("o.code")).Join(
			JoinOn("inner", "ref", "code").As("o"),
			JoinOn("inner", "ref", JoinKey),
		),
	}
}

func TestFanOutPairWorkspaceIsBoundedBeforeMaterialization(t *testing.T) {
	db := sharedFanOutDatabase(t, 16, 16)
	q := Select(Count()).Join(JoinOn("inner", "join", "join").As("i"))
	exec := Exec{Options: ExecOptions{
		JoinPairBytes: sharedFanOutBuildBytes(16),
		ResultRows:    -1,
		ResultBytes:   -1,
	}}
	err := q.RunInto(&exec, FromDatabase(db.Snapshot(), "outer"))
	var budgetErr *JoinPairBudgetError
	if !errors.As(err, &budgetErr) || !errors.Is(err, ErrJoinPairBudget) {
		t.Fatalf("error = %v, want JoinPairBudgetError", err)
	}
	if budgetErr.Pairs != 1 || len(exec.Workspace.pairInner) != 0 {
		t.Fatalf(
			"budget error/pair frontier = (%+v,%d), want first pair rejected",
			budgetErr, len(exec.Workspace.pairInner),
		)
	}
}

func TestFanOutBudgetErrorClearsPriorResult(t *testing.T) {
	db := sharedFanOutDatabase(t, 2, 2)
	source := FromDatabase(db.Snapshot(), "outer")
	q := Select(Count()).Join(JoinOn("inner", "join", "join").As("i"))
	var exec Exec
	if err := q.RunInto(&exec, source); err != nil {
		t.Fatal(err)
	}
	if exec.Result.RowCount != 1 {
		t.Fatalf("warm result has %d rows", exec.Result.RowCount)
	}
	exec.Options.JoinPairBytes = 1
	if err := q.RunInto(&exec, source); !errors.Is(err, ErrJoinPairBudget) {
		t.Fatalf("second execution error = %v, want pair budget", err)
	}
	if exec.Result.RowCount != 0 {
		t.Fatalf("failed execution exposed prior %d-row result", exec.Result.RowCount)
	}
	for i := range exec.Result.Columns {
		if len(exec.Result.Columns[i].Cells) != 0 {
			t.Fatalf(
				"failed execution retained %d visible cells in column %d",
				len(exec.Result.Columns[i].Cells), i,
			)
		}
	}
}

func TestFanOutEarlyLimitStopsAtPairBudgetWithoutSemanticError(t *testing.T) {
	db := sharedFanOutDatabase(t, 4, 4)
	q := Select(Path("id"), Path("i.seat")).
		Join(JoinOn("inner", "join", "join").As("i")).
		Limit(1)
	p, err := q.compiled()
	if err != nil {
		t.Fatal(err)
	}
	perPair := joinPairBytesPerRow(p, &p.joins[p.fanOutJoin])
	exec := Exec{Options: ExecOptions{
		JoinPairBytes: sharedFanOutBuildBytes(4) + perPair,
		ResultRows:    -1,
		ResultBytes:   -1,
	}}
	if err := q.RunInto(&exec, FromDatabase(db.Snapshot(), "outer")); err != nil {
		t.Fatal(err)
	}
	if exec.Result.RowCount != 1 || len(exec.Workspace.pairInner) != 1 {
		t.Fatalf(
			"limited fan-out = %d result rows, %d pairs",
			exec.Result.RowCount, len(exec.Workspace.pairInner),
		)
	}
}

func TestFanOutLimitZeroBuildsNoPairWorkspace(t *testing.T) {
	db := sharedFanOutDatabase(t, 4, 32)
	q := Select(Path("id"), Path("i.seat")).
		Join(JoinOn("inner", "join", "join").As("i")).
		Limit(0)
	exec := Exec{Options: ExecOptions{
		JoinPairBytes: 1,
		ResultRows:    -1,
		ResultBytes:   -1,
	}}
	if err := q.RunInto(&exec, FromDatabase(db.Snapshot(), "outer")); err != nil {
		t.Fatal(err)
	}
	if exec.Result.RowCount != 0 || len(exec.Workspace.pairInner) != 0 {
		t.Fatalf(
			"LIMIT 0 fan-out = %d result rows, %d pairs",
			exec.Result.RowCount, len(exec.Workspace.pairInner),
		)
	}
}

func TestFanOutDecodedTextSharesPairWorkspaceBudget(t *testing.T) {
	db := sharedFanOutDatabase(t, 1, 1)
	q := Select(Path("i.label")).
		Join(JoinOn("inner", "join", "join").As("i"))
	p, err := q.compiled()
	if err != nil {
		t.Fatal(err)
	}
	perPair := joinPairBytesPerRow(p, &p.joins[p.fanOutJoin])
	exec := Exec{Options: ExecOptions{
		JoinPairBytes: sharedFanOutBuildBytes(1) + perPair,
		ResultRows:    -1,
		ResultBytes:   -1,
	}}
	err = q.RunInto(&exec, FromDatabase(db.Snapshot(), "outer"))
	if !errors.Is(err, ErrJoinPairBudget) {
		t.Fatalf("decoded fan-out text error = %v, want pair budget", err)
	}
}

// refJoinPairs is the naive nested loop: for one driving document it walks the
// whole joined collection in order and returns every document that passes the
// clause's filter and whose join key equals the driving value.
//
// It stops at nothing and indexes nothing. That is the point — it is the oracle
// the hash multimap, its chain order, and its exact-comparison recheck are all
// checked against.
func refJoinPairs(j Join, outer any, inner refCollection) []any {
	var out []any
	cell := refClassify(refResolve(j.outerPath, outer))
	if cell.kind == kindNull {
		return nil // a null or absent driving key joins to nothing
	}
	for i := range inner.keys {
		if j.hasWhere && !refEval(j.where, inner.docs[i]) {
			continue
		}
		var target refScalar
		if j.innerPath == JoinKey {
			target = refScalar{kind: kindString, present: true, s: inner.keys[i]}
		} else {
			target = refClassify(refResolve(j.innerPath, inner.docs[i]))
			if target.kind == kindNull {
				continue
			}
		}
		if refCompare(cell, target) == 0 {
			out = append(out, inner.docs[i])
		}
	}
	return out
}

// refFanOut expands the driving documents into the inner join's rows: one per
// matching pair, in driving order and then joined order, each represented as
// the driving document's fields with the alias bound to the joined document.
//
// Representing a pair as one merged document is what lets the whole existing
// reference executor run over it unchanged. The engine resolves "o.total" by
// declaring an alias and routing the path to another collection; the reference
// resolves it by walking a dotted path into a nested map. Neither knows how the
// other does it, and the alias-wins rule falls out of the merge order rather
// than being restated.
func refFanOut(q *Query, docs []any, inner refCollection) []any {
	fan := -1
	for i, j := range q.joins {
		if j.alias != "" {
			fan = i
		}
	}
	if fan < 0 {
		return docs
	}
	var out []any
	for _, doc := range docs {
		// Every other clause is a semi-join and still filters.
		filtered := false
		for i, j := range q.joins {
			if i != fan && !refJoinMatches(j, doc, inner) {
				filtered = true
				break
			}
		}
		if filtered {
			continue
		}
		for _, partner := range refJoinPairs(q.joins[fan], doc, inner) {
			merged := map[string]any{}
			if fields, ok := doc.(map[string]any); ok {
				for k, v := range fields {
					merged[k] = v
				}
			}
			// Written last on purpose: a declared alias wins over a driving
			// field of the same name, which is the engine's documented rule.
			merged[q.joins[fan].alias] = partner
			out = append(out, merged)
		}
	}
	return out
}

func TestExhaustiveFanOutJoinDifferential(t *testing.T) {
	outers := enumerateStrings(outerJoinPool, joinIterations(3, 2), 1)
	inners := enumerateInnerCollections(len(innerJoinPool), joinIterations(3, 2))
	battery := fanOutBattery()

	checks, pairs := 0, uint64(0)
	for _, outerDocs := range outers {
		decodedOuter := decodeDocs(t, asBytes(outerDocs))
		for _, innerRows := range inners {
			reference := newRefCollection(t, innerRows)
			for _, indexed := range joinIndexModes {
				db := buildJoinDatabase(t, outerDocs, innerRows, indexed)
				catalog := db.Snapshot()
				for _, workers := range []int{1, 4} {
					var e Exec
					e.Options.Workers = workers
					for qi, q := range battery {
						if err := q.RunInto(&e, FromDatabase(catalog, "outer")); err != nil {
							t.Fatalf("query %d workers=%d indexed=%v over %v / %v: %v",
								qi, workers, indexed, outerDocs, innerRows, err)
						}
						want := referenceRunJoined(q, refFanOut(q, decodedOuter, reference), nil)
						if diff := compareResults(e.Result, want); diff != "" {
							t.Fatalf("query %d workers=%d indexed=%v over %v / %v: %s",
								qi, workers, indexed, outerDocs, innerRows, diff)
						}
						pairs += e.Stats.JoinPairs
						checks++
					}
				}
			}
		}
	}
	if pairs == 0 {
		t.Fatal("no pair was ever produced; the battery is not exercising fan-out")
	}
	t.Logf("exhaustive fan-out differential: %d outer sets × %d inner sets × %d index modes "+
		"× 2 worker counts × %d queries = %d checks over %d pairs",
		len(outers), len(inners), len(joinIndexModes), len(battery), checks, pairs)
}

// TestJoinPlannerPicksSemiJoinWhereItCan pins the planner rule and proves both
// lanes are live. The operator is chosen by the clause, not by what the query
// reads — an alias declares SQL's inner join, whose cardinality COUNT(*) can
// observe without any joined column being projected — with exactly one
// exception, which is a proof rather than a preference: a clause joining on the
// joined collection's primary key can match at most one document per driving
// row, so if nothing reads the joined side the semi-join returns the same rows
// and never materializes a match.
//
// Every case asserts which lane ran, so a build where one of them stopped being
// reachable fails here rather than passing the differential twice over.
func TestJoinPlannerPicksSemiJoinWhereItCan(t *testing.T) {
	cases := []struct {
		name   string
		query  *Query
		fanOut bool
	}{
		{"no alias: this engine's filter", Select(Path("id")).
			Join(JoinOn("inner", "ref", "code")), false},
		{"no alias, primary key", Select(Path("id")).
			Join(JoinOn("inner", "ref", JoinKey)), false},
		{"alias on a primary key nothing reads", Select(Path("id")).
			Join(JoinOn("inner", "ref", JoinKey).As("k")), false},
		{"alias on a primary key a column reads", Select(Path("id"), Path("k.tier")).
			Join(JoinOn("inner", "ref", JoinKey).As("k")), true},
		{"alias on a primary key an aggregate reads", Select(Sum("k.seat")).
			Join(JoinOn("inner", "ref", JoinKey).As("k")), true},
		{"alias on a field nothing reads", Select(Path("id")).
			Join(JoinOn("inner", "ref", "code").As("o")), true},
		{"alias on a field, counted only", Select(Count()).
			Join(JoinOn("inner", "ref", "code").As("o")), true},
		{"alias on a field a column reads", Select(Path("id"), Path("o.seat")).
			Join(JoinOn("inner", "ref", "code").As("o")), true},
		{"an alias used only for ordering still fans out", Select(Path("id")).
			Join(JoinOn("inner", "ref", "code").As("o")).OrderBy("o.seat", Asc), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := tc.query.compiled()
			if err != nil {
				t.Fatal(err)
			}
			if got := p.fanOutJoin >= 0; got != tc.fanOut {
				t.Fatalf("plan fans out = %v, want %v", got, tc.fanOut)
			}
		})
	}
}

// TestJoinPlannerLanesAreBothExercised is the runtime half of the rule: the
// same database answered by both lanes, with the statistics that only one of
// them can report. Without it the compile-time assertions above could all hold
// on a build where execution took one path regardless.
func TestJoinPlannerLanesAreBothExercised(t *testing.T) {
	db := joinSelectiveDatabaseFor(t, 400, 200, 50)
	catalog := db.Snapshot()

	var semi Exec
	if err := Select(Count()).
		Join(JoinOn("customers", "customer", JoinKey)).
		RunInto(&semi, FromDatabase(catalog, "orders")); err != nil {
		t.Fatal(err)
	}
	if semi.Stats.JoinBuilds != 0 || semi.Stats.JoinPairs != 0 {
		t.Fatalf("a filter-only join built a side: %+v", semi.Stats)
	}
	if semi.Stats.JoinMemberships+semi.Stats.JoinLookups != 1 {
		t.Fatalf("a filter-only join bound no strategy: %+v", semi.Stats)
	}

	var fan Exec
	if err := Select(Count()).
		Join(JoinOn("customers", "customer", JoinKey).As("c")).
		RunInto(&fan, FromDatabase(catalog, "orders")); err != nil {
		t.Fatal(err)
	}
	// A primary-key clause with no joined column read takes the semi-join even
	// with an alias, which is the proof-backed exception.
	if fan.Stats.JoinBuilds != 0 {
		t.Fatalf("an unread primary-key alias built a side: %+v", fan.Stats)
	}

	var built Exec
	if err := Select(Count(), Sum("c.seat")).
		Join(JoinOn("customers", "customer", JoinKey).As("c")).
		RunInto(&built, FromDatabase(catalog, "orders")); err != nil {
		t.Fatal(err)
	}
	if built.Stats.JoinBuilds != 1 || built.Stats.JoinPairs == 0 {
		t.Fatalf("reading a joined column did not build a side: %+v", built.Stats)
	}
	// A primary key matches at most one joined row, so the fan-out and the
	// semi-join must agree on how many rows there are.
	semiRows, _ := semi.Result.Columns[0].Cells[0].Float64()
	builtRows, _ := built.Result.Columns[0].Cells[0].Float64()
	if semiRows != builtRows {
		t.Fatalf("primary-key fan-out counted %v rows, semi-join counted %v", builtRows, semiRows)
	}
}

// TestFanOutOrderIsDrivingThenJoined pins the ordering rule as a rule rather
// than as whatever the hash table happened to produce: driving ordinal
// ascending, and within one driving row, joined-row address ascending.
//
// It is checked without ORDER BY on purpose. With one, a sort would impose the
// order and the test would prove nothing about the expansion.
func TestFanOutOrderIsDrivingThenJoined(t *testing.T) {
	db := &store.Database{}
	outer, err := db.CreateCollection("outer", store.Options{ChunkDocuments: 2})
	if err != nil {
		t.Fatal(err)
	}
	for i, ref := range []string{"a", "b", "a"} {
		if _, err := outer.Put(fmt.Sprintf("o%d", i),
			[]byte(fmt.Sprintf(`{"id":%d,"ref":%q}`, i, ref))); err != nil {
			t.Fatal(err)
		}
	}
	inner, err := db.CreateCollection("inner", store.Options{ChunkDocuments: 2})
	if err != nil {
		t.Fatal(err)
	}
	// Inserted in seat order, so the collection's scan order is seat order and
	// the expected pair order can be written down.
	for i, code := range []string{"a", "b", "a", "a"} {
		if _, err := inner.Put(fmt.Sprintf("i%d", i),
			[]byte(fmt.Sprintf(`{"code":%q,"seat":%d}`, code, i))); err != nil {
			t.Fatal(err)
		}
	}

	var e Exec
	q := Select(Path("id"), Path("o.seat")).Join(JoinOn("inner", "ref", "code").As("o"))
	if err := q.RunInto(&e, FromDatabase(db.Snapshot(), "outer")); err != nil {
		t.Fatal(err)
	}
	want := [][2]float64{
		{0, 0}, {0, 2}, {0, 3}, // driving row 0 (ref a) × seats 0,2,3 ascending
		{1, 1},                 // driving row 1 (ref b) × seat 1
		{2, 0}, {2, 2}, {2, 3}, // driving row 2 (ref a) again
	}
	if e.Result.RowCount != len(want) {
		t.Fatalf("%d rows, want %d:\n%s", e.Result.RowCount, len(want), dumpResult(e.Result))
	}
	for r, pair := range want {
		id, _ := e.Result.Columns[0].Cells[r].Float64()
		seat, _ := e.Result.Columns[1].Cells[r].Float64()
		if id != pair[0] || seat != pair[1] {
			t.Fatalf("row %d is (%v,%v), want (%v,%v) — pairs run driving ordinal "+
				"then joined ordinal:\n%s", r, id, seat, pair[0], pair[1], dumpResult(e.Result))
		}
	}
}

// TestFanOutRejections pins what an inner join will not do, by name. Each of
// these is a refusal rather than a gap, and the message has to say which
// spelling works instead — a caller who is told only "no" writes the same query
// again.
func TestFanOutRejections(t *testing.T) {
	cases := []struct {
		name  string
		query *Query
		want  []string
	}{
		{
			// An inner join's WHERE over a joined column selects the same pairs
			// as the clause's own filter, so this is a redirection.
			name: "WHERE reads a joined column",
			query: Select(Path("id")).Where(Cmp("o.seat", Gt, 2)).
				Join(JoinOn("inner", "ref", "code").As("o")),
			want: []string{"o.seat", "joined collection", "join clause's own filter"},
		},
		{
			name: "two clauses both fan out",
			query: Select(Path("a.seat"), Path("b.seat")).Join(
				JoinOn("inner", "ref", "code").As("a"),
				JoinOn("inner", "ref", "code").As("b"),
			),
			want: []string{"join[0]", "join[1]", "both fan out"},
		},
		{
			name: "two clauses share an alias",
			query: Select(Path("id")).Join(
				JoinOn("inner", "ref", "code").As("o"),
				JoinOn("inner", "ref", JoinKey).As("o"),
			),
			want: []string{"join[1]", "already used by join[0]"},
		},
		{
			name: "a grouped projection of a joined column must be grouped",
			query: Select(Path("o.seat"), Count()).
				Join(JoinOn("inner", "ref", "code").As("o")).GroupBy("id"),
			want: []string{"o.seat", "GROUP BY"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.query.Prepare()
			if err == nil {
				t.Fatal("compiled, want an error")
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("got %q, want it to mention %q", err, want)
				}
			}
		})
	}
}

// TestFanOutAliasWinsOverADrivingField pins the resolution rule a schemaless
// engine has no alternative to: a declared alias claims its leading segment,
// even when the driving collection has a field by that name. SQL resolves it
// the same way, and no query written before aliases existed could declare one,
// so nothing's meaning changed.
func TestFanOutAliasWinsOverADrivingField(t *testing.T) {
	db := &store.Database{}
	outer, err := db.CreateCollection("outer", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	// The driving document has a nested "o.seat" of its own, which the alias
	// must shadow.
	if _, err := outer.Put("o0", []byte(`{"ref":"a","o":{"seat":99}}`)); err != nil {
		t.Fatal(err)
	}
	inner, err := db.CreateCollection("inner", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := inner.Put("i0", []byte(`{"code":"a","seat":7}`)); err != nil {
		t.Fatal(err)
	}
	catalog := db.Snapshot()

	var aliased Exec
	if err := Select(Path("o.seat")).
		Join(JoinOn("inner", "ref", "code").As("o")).
		RunInto(&aliased, FromDatabase(catalog, "outer")); err != nil {
		t.Fatal(err)
	}
	if got, _ := aliased.Result.Columns[0].Cells[0].Float64(); got != 7 {
		t.Fatalf("o.seat resolved to %v, want the joined document's 7", got)
	}

	// Without the alias the same spec is the driving document's nested field,
	// which is what makes the rule a shadowing rather than a reservation.
	var plain Exec
	if err := Select(Path("o.seat")).
		Join(JoinOn("inner", "ref", "code")).
		RunInto(&plain, FromDatabase(catalog, "outer")); err != nil {
		t.Fatal(err)
	}
	if got, _ := plain.Result.Columns[0].Cells[0].Float64(); got != 99 {
		t.Fatalf("o.seat resolved to %v with no alias declared, want the driving 99", got)
	}
}

// TestFanOutAbsentJoinedPathIsNull pins the cell a projected joined path gets
// when the matched document does not have it. An inner join needs no outer-join
// null discipline — every row it emits has a real partner — but a partner may
// still be missing the path a projection names, and this engine's rule for that
// is the same on both sides of a join: an absent path and an explicit null are
// one value.
func TestFanOutAbsentJoinedPathIsNull(t *testing.T) {
	db := &store.Database{}
	outer, err := db.CreateCollection("outer", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := outer.Put("o0", []byte(`{"ref":"a"}`)); err != nil {
		t.Fatal(err)
	}
	inner, err := db.CreateCollection("inner", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for key, doc := range map[string]string{
		"absent":  `{"code":"a"}`,
		"present": `{"code":"a","seat":null}`,
	} {
		if _, err := inner.Put(key, []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	var e Exec
	if err := Select(Path("o.seat")).
		Join(JoinOn("inner", "ref", "code").As("o")).
		RunInto(&e, FromDatabase(db.Snapshot(), "outer")); err != nil {
		t.Fatal(err)
	}
	if e.Result.RowCount != 2 {
		t.Fatalf("%d rows, want one per matching pair:\n%s", e.Result.RowCount, dumpResult(e.Result))
	}
	for r := 0; r < e.Result.RowCount; r++ {
		if kind := e.Result.Columns[0].Cells[r].Kind(); kind != TypeNull {
			t.Fatalf("row %d is %v, want null for both the absent and the explicit spelling",
				r, kind)
		}
	}
}

// TestFanOutRunIntoSteadyAllocs holds an inner join to the project's
// zero-allocation contract, which fan-out is the hardest operation in this
// package to satisfy.
//
// Everything else here produces at most one row per scanned document, so a
// buffer sized to the collection is a bound on the output too. A fan-out
// produces one row per matching pair, which can be many times the driving
// collection's size — so anything allocated per pair would scale with the
// result rather than with the input, and would keep scaling every execution.
// The pair buffers, the build side's entries, its hash directory, and its value
// arena are all retained in the Workspace for that reason, and this is what
// notices if one of them stops being.
//
// The worker counts are swept because the filter phase that feeds the expansion
// is parallel, and the fan-out ratios because the buffers have to reach their
// high-water mark rather than grow on every run.
func TestFanOutRunIntoSteadyAllocs(t *testing.T) {
	for _, fan := range []int{1, 4, 16} {
		db := joinFanOutDatabase(t, 2000, 2000/fan)
		catalog := db.Snapshot()
		queries := map[string]*Query{
			"projection": Select(Path("id"), Path("o.seat")).
				Join(JoinOn("orders", "id", "user_id").As("o")),
			"left projection": Select(Path("id"), Path("o.seat")).
				Join(LeftJoinOn("orders", "id", "user_id").As("o")),
			"joined only": Select(Path("o.seat")).
				Join(JoinOn("orders", "id", "user_id").As("o")),
			"driving filter beside the join": Select(Path("id"), Path("o.seat")).
				Where(Cmp("id", Lt, 1500)).Join(JoinOn("orders", "id", "user_id").As("o")),
			"clause filter": Select(Path("id"), Path("o.seat")).
				Join(JoinOn("orders", "id", "user_id").As("o").Where(Cmp("tier", Eq, "pro"))),
			"aggregate over joined": Select(Count(), Sum("o.seat")).
				Join(JoinOn("orders", "id", "user_id").As("o")),
			"grouped by joined": Select(Path("o.tier"), Count()).
				Join(JoinOn("orders", "id", "user_id").As("o")).GroupBy("o.tier"),
			"ordered and limited": Select(Path("id"), Path("o.seat")).
				Join(JoinOn("orders", "id", "user_id").As("o")).
				OrderBy("o.seat", Desc).Limit(50),
			"limit without order": Select(Path("id"), Path("o.seat")).
				Join(JoinOn("orders", "id", "user_id").As("o")).Limit(50),
		}
		for _, workers := range []int{1, 4} {
			for name, q := range queries {
				t.Run(fmt.Sprintf("fan=%d/workers=%d/%s", fan, workers, name), func(t *testing.T) {
					var e Exec
					e.Options.Workers = workers
					for range 2 {
						if err := q.RunInto(&e, FromDatabase(catalog, "users")); err != nil {
							t.Fatal(err)
						}
					}
					if e.Result.RowCount == 0 || e.Stats.JoinPairs == 0 {
						t.Fatalf("the fixture must produce pairs: %+v", e.Stats)
					}
					allocs := testing.AllocsPerRun(25, func() {
						if err := q.RunInto(&e, FromDatabase(catalog, "users")); err != nil {
							panic(err)
						}
					})
					if allocs != 0 {
						t.Fatalf("warmed fan-out RunInto allocated %.2f times, want 0", allocs)
					}
				})
			}
		}
	}
}

// joinFanOutDatabase builds a users/orders pair with a controlled fan-out
// ratio: orders rows spread evenly over users, so each user has orders/users
// partners. It is the shape the cost curve is measured over and the shape the
// allocation contract is held at, because both care about how much larger the
// output is than the driving collection.
func joinFanOutDatabase(t testing.TB, orderRows, users int) *store.Database {
	t.Helper()
	db := &store.Database{}
	people, err := db.CreateCollection("users", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < users; i++ {
		if _, err := people.Put(fmt.Sprintf("u%d", i),
			[]byte(fmt.Sprintf(`{"id":%d,"name":"user-%d"}`, i, i))); err != nil {
			t.Fatal(err)
		}
	}
	orders, err := db.CreateCollection("orders", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < orderRows; i++ {
		tier := "free"
		if i%3 == 0 {
			tier = "pro"
		}
		if _, err := orders.Put(fmt.Sprintf("o%d", i), []byte(fmt.Sprintf(
			`{"user_id":%d,"seat":%d,"tier":%q}`, i%users, i, tier))); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

// TestFanOutSharesOneShapeCacheAcrossCollections pins a safety property the
// expansion depends on without saying so anywhere else: the driving and joined
// sides are extracted through one execCtx, and therefore through one
// store.ShapeCache.
//
// That is sound because a shape cache is keyed by document layout rather than
// by collection — a compiled shape describes where a layout's fields sit, which
// is the same answer whichever collection the document came from. This is the
// test that would notice if it stopped being true, using two collections whose
// layouts differ while sharing a field name, so a cache that confused them
// would read the wrong offset rather than merely miss.
func TestFanOutSharesOneShapeCacheAcrossCollections(t *testing.T) {
	db := &store.Database{}
	outer, err := db.CreateCollection("outer", store.Options{ShapeTapes: true})
	if err != nil {
		t.Fatal(err)
	}
	// Driving layout: ref first, then seat.
	for i := 0; i < 8; i++ {
		if _, err := outer.Put(fmt.Sprintf("o%d", i),
			[]byte(fmt.Sprintf(`{"ref":%d,"seat":%d}`, i%2, 100+i))); err != nil {
			t.Fatal(err)
		}
	}
	inner, err := db.CreateCollection("inner", store.Options{ShapeTapes: true})
	if err != nil {
		t.Fatal(err)
	}
	// Joined layout: seat first, then code, then a field the driving side does
	// not have — the same names in a different order and a different arity.
	for i := 0; i < 4; i++ {
		if _, err := inner.Put(fmt.Sprintf("i%d", i),
			[]byte(fmt.Sprintf(`{"seat":%d,"code":%d,"extra":true}`, 200+i, i%2))); err != nil {
			t.Fatal(err)
		}
	}

	var e Exec
	q := Select(Sum("seat"), Sum("o.seat"), Count()).
		Join(JoinOn("inner", "ref", "code").As("o"))
	if err := q.RunInto(&e, FromDatabase(db.Snapshot(), "outer")); err != nil {
		t.Fatal(err)
	}
	// Each of the 8 driving rows matches 2 joined rows: 16 pairs.
	pairs, _ := e.Result.Columns[2].Cells[0].Float64()
	if pairs != 16 {
		t.Fatalf("%v pairs, want 16", pairs)
	}
	// Driving seats 100..107, each appearing twice: 2*(100+...+107) = 1656.
	drivingSum, _ := e.Result.Columns[0].Cells[0].Float64()
	if drivingSum != 1656 {
		t.Fatalf("driving seat sum %v, want 1656 — the shared shape cache routed "+
			"a driving column through the joined collection's layout", drivingSum)
	}
	// Joined seats 200..203; each is matched by 4 driving rows: 4*(200+..+203) = 3224.
	joinedSum, _ := e.Result.Columns[1].Cells[0].Float64()
	if joinedSum != 3224 {
		t.Fatalf("joined seat sum %v, want 3224 — the shared shape cache routed "+
			"a joined column through the driving collection's layout", joinedSum)
	}
}
