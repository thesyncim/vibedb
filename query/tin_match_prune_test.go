package query

import (
	"math/bits"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

// ==> prunes heap snapshot scans with the slot's generation-pinned tin
// postings instead of testing every row. A lone ==> over an exact,
// compact mask skips the recheck outright (identity selection); every
// other shape still rechecks each candidate, so these tests prove two
// things separately: the query answers stay exactly the hand-derived
// expectations, and the candidate masks the planner produced are exactly
// the matching set (narrower than the full scan, empty for a no-hit
// query, declining to nil without a binding).
func TestSQLMatchPrunesHeapSnapshotScan(t *testing.T) {
	db := tinMatchDatabase(t, true)
	catalog := db.Snapshot()
	snapshot, ok := catalog.Collection("docs")
	if !ok {
		t.Fatal("catalog lost collection docs")
	}
	for _, tc := range []struct {
		name  string
		tinql string
		want  []string
		bits  int
	}{
		{name: "term", tinql: "luxury", want: []string{"d001", "d003", "d007"}, bits: 3},
		{name: "and", tinql: "luxury AND goods", want: []string{"d001", "d007"}, bits: 2},
		{name: "phrase", tinql: `"luxury goods"`, want: []string{"d001", "d007"}, bits: 2},
		{name: "prefix", tinql: "lux*", want: []string{"d001", "d003", "d007", "d008"}, bits: 4},
		{name: "nohit", tinql: "yachts", want: nil, bits: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			statement, err := PrepareStatement(
				`SELECT o.id FROM docs AS o WHERE o.body ==> '` + tc.tinql + `' ORDER BY o.id`,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Release()
			for _, workers := range []int{0, 4} {
				exec := Exec{Options: ExecOptions{Workers: workers}}
				got := tinMatchStatementIDs(
					t, statement, FromDatabase(catalog, "docs"), &exec,
				)
				if !slices.Equal(got, tc.want) {
					t.Fatalf("workers=%d ids=%v want=%v", workers, got, tc.want)
				}
				exec.Release()
			}
			q := Select(Path("id")).Where(Match("body", tc.tinql)).OrderBy("id", Asc)
			plan, err := q.compiled()
			if err != nil {
				t.Fatal(err)
			}
			var w Workspace
			if err := plan.bindMatches(&w, snapshot, catalog); err != nil {
				t.Fatal(err)
			}
			masks, err := plan.storeCandidateMasks(snapshot, &w)
			if err != nil {
				t.Fatal(err)
			}
			if tc.bits == 0 {
				if len(masks) != 0 {
					t.Fatalf("no-hit masks cover %d rows, want an empty scan", len(masks))
				}
				return
			}
			if masks == nil {
				t.Fatal("prunable ==> fell back to the full scan")
			}
			count := 0
			for _, mask := range masks {
				count += bits.OnesCount64(mask.Bits)
			}
			if count != tc.bits {
				t.Fatalf("masks cover %d rows, want %d", count, tc.bits)
			}
			if count > snapshot.Len()/2 {
				t.Fatalf("masks cover %d of %d rows, want a narrowed scan", count, snapshot.Len())
			}
		})
	}
}

// Boolean combinations prune through the shared combinators: one postable
// ==> conjunct narrows an AND, and an OR prunes when every disjunct is a
// bounded ==> leaf. NOT ==> prunes to the live complement and rechecks —
// the complement is a superset (non-string bodies fall out under
// three-valued logic), so it narrows but never answers directly — while
// still answering exactly.
func TestSQLMatchPrunesBooleanCombinations(t *testing.T) {
	db := tinMatchDatabase(t, true)
	catalog := db.Snapshot()
	snapshot, ok := catalog.Collection("docs")
	if !ok {
		t.Fatal("catalog lost collection docs")
	}
	for _, tc := range []struct {
		name   string
		q      *Query
		sql    string
		want   []string
		pruned bool
		bits   int
	}{
		{
			name:   "and-both-match",
			q:      Select(Path("id")).Where(And(Match("body", "luxury"), Match("body", "watches"))).OrderBy("id", Asc),
			sql:    `SELECT o.id FROM docs AS o WHERE o.body ==> 'luxury' AND o.body ==> 'watches' ORDER BY o.id`,
			want:   []string{"d003"},
			pruned: true,
		},
		{
			name:   "and-unpostable-conjunct",
			q:      Select(Path("id")).Where(And(Match("body", "luxury"), Cmp("body", Eq, "zzz"))).OrderBy("id", Asc),
			sql:    `SELECT o.id FROM docs AS o WHERE o.body ==> 'luxury' AND o.body = 'zzz' ORDER BY o.id`,
			want:   nil,
			pruned: true,
		},
		{
			name:   "or-both-match",
			q:      Select(Path("id")).Where(Or(Match("body", "luxury"), Match("body", "cheap"))).OrderBy("id", Asc),
			sql:    `SELECT o.id FROM docs AS o WHERE o.body ==> 'luxury' OR o.body ==> 'cheap' ORDER BY o.id`,
			want:   []string{"d001", "d002", "d003", "d007"},
			pruned: true,
		},
		{
			name:   "or-unbounded-disjunct",
			q:      Select(Path("id")).Where(Or(Match("body", "luxury"), Cmp("id", Eq, "d002"))).OrderBy("id", Asc),
			sql:    `SELECT o.id FROM docs AS o WHERE o.body ==> 'luxury' OR o.id = 'd002' ORDER BY o.id`,
			want:   []string{"d001", "d002", "d003", "d007"},
			pruned: false,
		},
		{
			// NOT prunes to the live complement of the exact child set
			// (d002, d004, d005, d006, d008) and rechecks: the numeric,
			// missing, and empty bodies fall out under three-valued
			// logic, leaving d002, d006, d008. The complement is a
			// superset prune, never a direct answer.
			name:   "not-prunes-complement",
			q:      Select(Path("id")).Where(Not(Match("body", "luxury"))).OrderBy("id", Asc),
			sql:    `SELECT o.id FROM docs AS o WHERE NOT o.body ==> 'luxury' ORDER BY o.id`,
			want:   []string{"d002", "d006", "d008"},
			pruned: true,
			bits:   5,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			statement, err := PrepareStatement(tc.sql)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Release()
			exec := Exec{}
			got := tinMatchStatementIDs(t, statement, FromDatabase(catalog, "docs"), &exec)
			exec.Release()
			if !slices.Equal(got, tc.want) {
				t.Fatalf("ids=%v want=%v", got, tc.want)
			}
			plan, err := tc.q.compiled()
			if err != nil {
				t.Fatal(err)
			}
			var w Workspace
			if err := plan.bindMatches(&w, snapshot, catalog); err != nil {
				t.Fatal(err)
			}
			masks, err := plan.storeCandidateMasks(snapshot, &w)
			if err != nil {
				t.Fatal(err)
			}
			if tc.pruned && masks == nil {
				t.Fatal("bounded combination fell back to the full scan")
			}
			if !tc.pruned && masks != nil {
				t.Fatalf("unbounded combination pruned to %d masks", len(masks))
			}
			if tc.bits > 0 {
				count := 0
				for _, mask := range masks {
					count += bits.OnesCount64(mask.Bits)
				}
				if count != tc.bits {
					t.Fatalf("masks cover %d rows, want %d", count, tc.bits)
				}
			}
		})
	}
}

// Without a binding the probe declines: EXPLAIN-time and catalog-only mask
// calls see empty slots and keep the full scan instead of erroring.
func TestSQLMatchPruningDeclinesUnbound(t *testing.T) {
	db := tinMatchDatabase(t, true)
	catalog := db.Snapshot()
	snapshot, ok := catalog.Collection("docs")
	if !ok {
		t.Fatal("catalog lost collection docs")
	}
	q := Select(Path("id")).Where(Match("body", "luxury")).OrderBy("id", Asc)
	plan, err := q.compiled()
	if err != nil {
		t.Fatal(err)
	}
	var w Workspace
	masks, err := plan.storeCandidateMasks(snapshot, &w)
	if err != nil {
		t.Fatal(err)
	}
	if masks != nil {
		t.Fatalf("unbound ==> pruned to %d masks, want the full scan", len(masks))
	}
}

// TestSQLMatchExactSkipCoversMutations proves the recheck skip stays exact
// when the snapshot moved under the index: a delete, an update, and
// non-string bodies share the corpus with live text rows. Every query is a
// lone ==> outside the top-K restriction (non-score orders, with and without
// LIMIT), so compact shapes take the identity selection while unselective
// ones keep the rechecked scan; both must answer the hand-derived sets.
func TestSQLMatchExactSkipCoversMutations(t *testing.T) {
	db := &store.Database{}
	coll, err := db.CreateCollection("docs", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	put := func(key, body string) {
		t.Helper()
		doc := `{"id":"` + key + `","body":` + body + `}`
		if _, err := coll.Put(key, []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	put("k1", `"alpha beta"`)
	put("k2", `"alpha gamma"`)
	put("k3", `"beta gamma delta"`)
	put("k4", `"alpha alpha beta"`)
	put("k5", `7`)
	put("k6", `"beta"`)
	put("k7", `"alpha zeta"`)
	put("k8", `"beta"`)
	put("k8", `"alpha epsilon"`)
	if _, err := coll.Delete("k7"); err != nil {
		t.Fatal(err)
	}
	if _, err := coll.Put("k9", []byte(`{"id":"k9"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := coll.CreateIndex(store.IndexDefinition{
		Name: "body_tin", Paths: []string{"/body"}, Kind: store.IndexTin,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := coll.BackfillIndex("body_tin", 0); err != nil {
		t.Fatal(err)
	}
	catalog := db.Snapshot()
	for _, tc := range []struct {
		name  string
		tinql string
		order string
		want  []string
	}{
		{name: "and", tinql: "alpha AND beta", order: "ASC", want: []string{"k1", "k4"}},
		{name: "term-selective", tinql: "epsilon", order: "ASC", want: []string{"k8"}},
		{name: "term-beta", tinql: "beta", order: "ASC", want: []string{"k1", "k3", "k4", "k6"}},
		{name: "term-beta-desc-limit", tinql: "beta", order: "DESC LIMIT 2", want: []string{"k6", "k4"}},
		{name: "phrase", tinql: `"alpha beta"`, order: "ASC", want: []string{"k1", "k4"}},
		{name: "phrase-slop", tinql: `"alpha beta"~1`, order: "ASC", want: []string{"k1", "k4"}},
		{name: "near", tinql: "alpha NEAR/1 beta", order: "ASC", want: []string{"k1", "k4"}},
		{name: "or", tinql: "epsilon OR delta", order: "ASC", want: []string{"k3", "k8"}},
		{name: "delta", tinql: "delta", order: "ASC", want: []string{"k3"}},
		{name: "deleted-term", tinql: "zeta", order: "ASC", want: nil},
		{name: "unselective-alpha", tinql: "alpha", order: "ASC", want: []string{"k1", "k2", "k4", "k8"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			statement, err := PrepareStatement(
				`SELECT o.id FROM docs AS o WHERE o.body ==> '` + tc.tinql + `' ORDER BY o.id ` + tc.order,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Release()
			for _, workers := range []int{0, 4} {
				exec := Exec{Options: ExecOptions{Workers: workers}}
				got := tinMatchStatementIDs(t, statement, FromDatabase(catalog, "docs"), &exec)
				exec.Release()
				if !slices.Equal(got, tc.want) {
					t.Fatalf("workers=%d ids=%v want=%v", workers, got, tc.want)
				}
			}
		})
	}
}

// TestSQLMatchParseReuseAcrossSnapshots pins the bound-parse cache: one
// statement executed repeatedly over one Exec reuses the parsed query
// while its snapshot's index stands, and re-parses the moment a new
// snapshot (with a new matching document) arrives. Same rows either way;
// the second snapshot must see the new document, the first must not.
func TestSQLMatchParseReuseAcrossSnapshots(t *testing.T) {
	db := tinMatchDatabase(t, true)
	coll, ok := db.Collection("docs")
	if !ok {
		t.Fatal("docs collection missing")
	}
	statement, err := PrepareStatement(
		`SELECT o.id FROM docs AS o WHERE o.body ==> 'luxury' ORDER BY o.id`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	exec := Exec{}
	defer exec.Release()
	run := func(catalog store.DatabaseSnapshot) []string {
		t.Helper()
		return tinMatchStatementIDs(t, statement, FromDatabase(catalog, "docs"), &exec)
	}
	snap1 := db.Snapshot()
	first := run(snap1)
	second := run(snap1)
	if !slices.Equal(first, second) {
		t.Fatalf("reuse changed rows: %v vs %v", first, second)
	}
	// The repeat execution reuses the bound parse and all warmed
	// staging, so the only allocation is the test's own ids slice:
	// steady-state production work is allocation-free.
	if n := testing.AllocsPerRun(20, func() { run(snap1) }); n > 1 {
		t.Fatalf("repeat execution allocates %.1f times, want <= 1", n)
	}
	if _, err := coll.Put("d009", []byte(`{"id":"d009","body":"luxury yachts"}`)); err != nil {
		t.Fatal(err)
	}
	snap2 := db.Snapshot()
	third := run(snap2)
	want := append(slices.Clone(first), "d009")
	slices.Sort(want)
	if !slices.Equal(third, want) {
		t.Fatalf("new snapshot rows=%v want=%v", third, want)
	}
	fourth := run(snap1)
	if !slices.Equal(fourth, first) {
		t.Fatalf("old snapshot rows=%v want=%v", fourth, first)
	}
}
