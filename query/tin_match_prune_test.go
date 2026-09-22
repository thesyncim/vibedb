package query

import (
	"math/bits"
	"slices"
	"testing"
)

// ==> prunes heap snapshot scans with the slot's generation-pinned tin
// postings instead of testing every row. The filter phase still rechecks
// every candidate, so these tests prove two things separately: the query
// answers stay exactly the hand-derived expectations, and the candidate
// masks the planner produced are exactly the matching set (narrower than
// the full scan, empty for a no-hit query, declining to nil without a
// binding).
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
// bounded ==> leaf. NOT ==> keeps the full scan — complementing a pruned
// set is not selective — while still answering exactly.
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
			name:   "not-stays-full",
			q:      Select(Path("id")).Where(Not(Match("body", "luxury"))).OrderBy("id", Asc),
			sql:    `SELECT o.id FROM docs AS o WHERE NOT o.body ==> 'luxury' ORDER BY o.id`,
			want:   []string{"d002", "d006", "d008"},
			pruned: false,
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
