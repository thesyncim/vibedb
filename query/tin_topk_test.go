package query

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

// tinTopKTieDatabase builds a tie-heavy multi-chunk corpus with holes: three
// identical-body groups (every document in a group scores exactly the same),
// a spread group, non-string and missing bodies that must never match, and a
// delete pattern punching holes across chunk boundaries. Any disagreement
// between index rank order and scan order shows up here as a LIMIT prefix
// mismatch.
func tinTopKTieDatabase(t *testing.T) *store.Database {
	t.Helper()
	db := &store.Database{}
	coll, err := db.CreateCollection("docs", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	put := func(key, body string) {
		t.Helper()
		doc := `{"id":` + strconv.Quote(key) + `,"body":` + body + `}`
		if _, err := coll.Put(key, []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 200 {
		key := fmt.Sprintf("t%04d", i)
		switch i % 4 {
		case 0:
			put(key, `"alpha beta gamma"`)
		case 1:
			put(key, `"alpha beta gamma"`)
		case 2:
			body := `"alpha`
			for k := 0; k < (i*13)%48; k++ {
				body += ` filler`
			}
			put(key, body+`"`)
		case 3:
			put(key, `"alpha beta"`)
		}
	}
	// Bodies the index must never see: numbers, nulls, and absent paths.
	for i := range 8 {
		key := fmt.Sprintf("n%04d", i)
		doc := `{"id":` + strconv.Quote(key) + `,"body":` + fmt.Sprintf("%d", i) + `}`
		if _, err := coll.Put(key, []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := coll.Put("m0000", []byte(`{"id":"m0000"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := coll.Put("l0000", []byte(`{"id":"l0000","body":null}`)); err != nil {
		t.Fatal(err)
	}
	// Holes across chunk boundaries: every 11th text row goes away.
	for i := 0; i < 200; i += 11 {
		if _, err := coll.Delete(fmt.Sprintf("t%04d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := coll.CreateIndex(store.IndexDefinition{
		Name: "body_tin", Paths: []string{"/body"}, Kind: store.IndexTin,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := coll.BackfillIndex("body_tin", 0); err != nil {
		t.Fatal(err)
	}
	return db
}

// tinTopKRun executes src and returns id/score rows plus whether the scan
// took the index-driven top-K restriction.
func tinTopKRun(t *testing.T, db *store.Database, src string, workers int) (rows [][2]any, engaged bool) {
	t.Helper()
	catalog := db.Snapshot()
	statement, err := PrepareStatement(src)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	exec := Exec{Options: ExecOptions{Workers: workers}}
	cursor, err := statement.RunInto(&exec, FromDatabase(catalog, "docs"), nil)
	if err != nil {
		t.Fatal(err)
	}
	for cursor.Next() {
		id, ok := cursor.Cell(0).Text()
		if !ok {
			t.Fatalf("id cell = %s, want string", cursor.Cell(0).JSON())
		}
		score, ok := cursor.Cell(1).Float64()
		if !ok {
			t.Fatalf("score cell = %s, want number", cursor.Cell(1).JSON())
		}
		rows = append(rows, [2]any{id, score})
	}
	engaged = exec.Workspace.tinTopKUsed
	exec.Release()
	return rows, engaged
}

// TestTinTopKMatchesFullSortPrefix proves the restriction returns exactly the
// full sort's prefix — ids and scores bit-identical — over a corpus built to
// break rank-order agreement: exact ties, deletes, both directions, offsets,
// past-the-end windows, and serial plus parallel execution. Every LIMIT shape
// here must engage the restriction; the unlimited full sorts must not.
func TestTinTopKMatchesFullSortPrefix(t *testing.T) {
	db := tinTopKTieDatabase(t)
	queries := []string{
		`'alpha'`,
		`'alpha AND beta'`,
		`'"alpha beta"'`,
	}
	limits := []string{
		` LIMIT 10`,
		` LIMIT 10 OFFSET 5`,
		` LIMIT 20000`,
		` LIMIT 0`,
		` LIMIT 7 OFFSET 600`,
	}
	for _, where := range queries {
		for _, dir := range []string{"DESC", "ASC"} {
			base := fmt.Sprintf(
				`SELECT o.id, SCORE() FROM docs AS o WHERE o.body ==> %s ORDER BY SCORE() %s`, where, dir)
			full, engaged := tinTopKRun(t, db, base, 0)
			if engaged {
				t.Fatalf("%s: unlimited sort engaged the restriction", base)
			}
			if len(full) < 100 {
				t.Fatalf("%s: %d rows, want a rank-heavy set", base, len(full))
			}
			for _, lim := range limits {
				for _, workers := range []int{0, 4} {
					src := base + lim
					got, engaged := tinTopKRun(t, db, src, workers)
					if !engaged {
						t.Fatalf("%s workers=%d: restriction did not engage", src, workers)
					}
					from, to := 0, len(full)
					if lim == ` LIMIT 10` {
						to = 10
					} else if lim == ` LIMIT 10 OFFSET 5` {
						from, to = 5, 15
					} else if lim == ` LIMIT 0` {
						to = 0
					} else if lim == ` LIMIT 7 OFFSET 600` {
						from, to = 600, 607
					}
					if to > len(full) {
						to = len(full)
					}
					if from > len(full) {
						from = len(full)
					}
					want := full[from:to]
					if len(got) != len(want) {
						t.Fatalf("%s workers=%d: %d rows, want %d", src, workers, len(got), len(want))
					}
					for i, row := range got {
						if row != want[i] {
							t.Fatalf("%s workers=%d row %d: %v != prefix %v",
								src, workers, i, row, want[i])
						}
					}
				}
			}
		}
	}
}

// TestTinTopKSelectivePaths proves prefix equality through the AND heap path
// and the fallback full-score path on the larger skewed corpus, and proves
// excluded shapes (multi-key orders, non-score orders) keep the ordinary scan
// while staying correct.
func TestTinTopKSelectivePaths(t *testing.T) {
	db := scoreBenchDatabase(t)
	full := scoreBenchRows(t, db,
		`SELECT o.id, SCORE() FROM docs AS o WHERE o.body ==> 'zipf AND common' ORDER BY SCORE() DESC`)
	if len(full) < 100 {
		t.Fatalf("%d rows, want a rank-heavy set", len(full))
	}
	got, engaged := tinTopKRun(t, db,
		`SELECT o.id, SCORE() FROM docs AS o WHERE o.body ==> 'zipf AND common' ORDER BY SCORE() DESC LIMIT 10`, 0)
	if !engaged {
		t.Fatal("selective AND restriction did not engage")
	}
	for i, row := range got {
		if row != full[i] {
			t.Fatalf("selective row %d: %v != prefix %v", i, row, full[i])
		}
	}
	phraseFull := scoreBenchRows(t, db,
		`SELECT o.id, SCORE() FROM docs AS o WHERE o.body ==> '"common filler"' ORDER BY SCORE() DESC`)
	got, engaged = tinTopKRun(t, db,
		`SELECT o.id, SCORE() FROM docs AS o WHERE o.body ==> '"common filler"' ORDER BY SCORE() DESC LIMIT 10`, 0)
	if !engaged {
		t.Fatal("phrase restriction did not engage")
	}
	if len(got) != 10 {
		t.Fatalf("phrase: %d rows, want 10", len(got))
	}
	for i, row := range got {
		if row != phraseFull[i] {
			t.Fatalf("phrase row %d: %v != prefix %v", i, row, phraseFull[i])
		}
	}
	// Excluded shapes stay on the ordinary scan but answer identically.
	multi := `SELECT o.id, SCORE() FROM docs AS o WHERE o.body ==> 'common' ORDER BY SCORE() DESC, o.id ASC`
	multiFull, engaged := tinTopKRun(t, db, multi, 0)
	if engaged {
		t.Fatal("multi-key order engaged the restriction")
	}
	multiGot, engaged := tinTopKRun(t, db, multi+` LIMIT 10`, 0)
	if engaged {
		t.Fatal("multi-key order with LIMIT engaged the restriction")
	}
	for i, row := range multiGot {
		if row != multiFull[i] {
			t.Fatalf("multi-key row %d: %v != prefix %v", i, row, multiFull[i])
		}
	}
	plain, engaged := tinTopKRun(t, db,
		`SELECT o.id, SCORE() FROM docs AS o WHERE o.body ==> 'zipf AND common' ORDER BY o.id DESC LIMIT 10`, 0)
	if engaged {
		t.Fatal("non-score order engaged the restriction")
	}
	if len(plain) != 10 {
		t.Fatalf("non-score order: %d rows, want 10", len(plain))
	}
}
