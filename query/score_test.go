package query

import (
	"math"
	"os"
	"slices"
	"strconv"
	"testing"

	"github.com/thesyncim/vibedb/internal/tin"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// scoreBodies is the string body per tinMatchDocs key. d004's body is a
// number and d005 has no body: both score 0 wherever they appear in output.
var scoreBodies = map[string]string{
	"d001": "luxury goods for sale",
	"d002": "cheap goods everyday",
	"d003": "luxury watches and clocks",
	"d006": "",
	"d007": "luxury luxury goods goods",
	"d008": "a luxuriously appointed suite",
}

// scoreOracle pins the BM25 statistics for tinql over scoreBodies and
// returns the index scorer's value per key: the same contract
// TestScoreSingleAgreesWithScore proves for the transient scorer, lifted to
// SQL. Keys the index does not rank expect exactly 0.
func scoreOracle(t testing.TB, tinql string) map[string]float64 {
	t.Helper()
	ix := tin.NewIndex()
	keys := make([]string, 0, len(scoreBodies))
	for key := range scoreBodies {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for i, key := range keys {
		ix.Add(tin.DocID(i+1), scoreBodies[key])
	}
	q, err := ix.ParseTINQL(tinql)
	if err != nil {
		t.Fatalf("ParseTINQL(%q): %v", tinql, err)
	}
	byDoc := map[tin.DocID]string{}
	for i, key := range keys {
		byDoc[tin.DocID(i+1)] = key
	}
	want := map[string]float64{}
	for _, s := range ix.Score(q, 0, nil) {
		want[byDoc[s.Doc]] = s.Score
	}
	return want
}

func scoreClose(got, want float64) bool {
	if math.IsNaN(got) || math.IsNaN(want) {
		return math.IsNaN(got) && math.IsNaN(want)
	}
	if got == want {
		return true
	}
	den := math.Abs(want)
	if den < 1e-300 {
		den = 1e-300
	}
	return math.Abs(got-want)/den < 1e-12
}

// scoreRows runs SELECT id, SCORE() over the heap corpus and returns the
// score per id in SELECT order.
func scoreRows(t testing.TB, src string, args ...any) [][2]any {
	t.Helper()
	db := tinMatchDatabase(t, true)
	catalog := db.Snapshot()
	statement, err := PrepareStatement(src)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	var out [][2]any
	for _, workers := range []int{0, 4} {
		exec := Exec{Options: ExecOptions{Workers: workers}}
		cursor, err := statement.RunInto(&exec, FromDatabase(catalog, "docs"), args)
		if err != nil {
			t.Fatal(err)
		}
		out = out[:0]
		for cursor.Next() {
			id, ok := cursor.Cell(0).Text()
			if !ok {
				t.Fatalf("id cell = %s, want string", cursor.Cell(0).JSON())
			}
			score, ok := cursor.Cell(1).Float64()
			if !ok {
				t.Fatalf("score cell = %s, want number", cursor.Cell(1).JSON())
			}
			out = append(out, [2]any{id, score})
		}
		exec.Release()
	}
	return out
}

// TestSQLScoreAgreesWithTinSearch proves SQL SCORE() mirrors the tin index
// scorer: for every query shape the (id, score) rows equal the index's own
// ranking within 1e-12 relative, the documented scalar/vector kernel gap.
func TestSQLScoreAgreesWithTinSearch(t *testing.T) {
	for _, tinql := range []string{
		`luxury`, `luxury AND goods`, `"luxury goods"`, `lux*`,
		`luxury OR cheap`, `goods`, `luxury AND NOT cheap`,
	} {
		t.Run(tinql, func(t *testing.T) {
			want := scoreOracle(t, tinql)
			got := scoreRows(t,
				`SELECT o.id, SCORE() FROM docs AS o WHERE o.body ==> '`+tinql+`' ORDER BY o.id`)
			if len(got) != len(want) {
				t.Fatalf("%q: %d rows, want %d", tinql, len(got), len(want))
			}
			for _, row := range got {
				id := row[0].(string)
				score := row[1].(float64)
				w, ok := want[id]
				if !ok {
					t.Fatalf("%q: unexpected id %q", tinql, id)
				}
				if !scoreClose(score, w) {
					t.Fatalf("%q id %q: SCORE() %v != index %v", tinql, id, score, w)
				}
			}
		})
	}
}

// TestSQLScoreOrdersByRelevance proves ORDER BY SCORE() DESC ranks by the
// index relevance: returned scores never increase, and the multiset of ids
// is exactly the index ranking.
func TestSQLScoreOrdersByRelevance(t *testing.T) {
	const tinql = `luxury OR cheap`
	want := scoreOracle(t, tinql)
	got := scoreRows(t,
		`SELECT o.id, SCORE() FROM docs AS o WHERE o.body ==> '`+tinql+`' ORDER BY SCORE() DESC`)
	if len(got) != len(want) {
		t.Fatalf("%d rows, want %d", len(got), len(want))
	}
	prev := math.Inf(1)
	for _, row := range got {
		score := row[1].(float64)
		if score > prev {
			t.Fatalf("scores increase: %v after %v", score, prev)
		}
		prev = score
		if _, ok := want[row[0].(string)]; !ok {
			t.Fatalf("unexpected id %q", row[0].(string))
		}
	}
}

// TestSQLScoreThreshold proves SCORE() filters in WHERE: only rows whose
// relevance clears the cutoff survive, with the same values the SELECT
// form reports.
func TestSQLScoreThreshold(t *testing.T) {
	const tinql = `luxury`
	want := scoreOracle(t, tinql)
	var max float64
	for _, w := range want {
		if w > max {
			max = w
		}
	}
	cut := max / 2
	var wantIDs []string
	for id, w := range want {
		if w > cut {
			wantIDs = append(wantIDs, id)
		}
	}
	slices.Sort(wantIDs)
	got := scoreRows(t, `SELECT o.id, SCORE() FROM docs AS o WHERE o.body ==> '`+tinql+
		`' AND SCORE() > `+strconv.FormatFloat(cut, 'f', -1, 64)+` ORDER BY o.id`)
	var gotIDs []string
	for _, row := range got {
		id := row[0].(string)
		gotIDs = append(gotIDs, id)
		score := row[1].(float64)
		if score <= cut {
			t.Fatalf("id %q: SCORE() %v <= cutoff %v", id, score, cut)
		}
		if !scoreClose(score, want[id]) {
			t.Fatalf("id %q: SCORE() %v != index %v", id, score, want[id])
		}
	}
	slices.Sort(gotIDs)
	if !slices.Equal(gotIDs, wantIDs) {
		t.Fatalf("ids %v, want %v", gotIDs, wantIDs)
	}
}

// TestSQLScoreZeroForNonMatch proves rows that reach output without matching
// the ==> query — a string non-match, a non-string body, an absent path, an
// empty body — score exactly 0 instead of erroring or leaking.
func TestSQLScoreZeroForNonMatch(t *testing.T) {
	got := scoreRows(t, `SELECT o.id, SCORE() FROM docs AS o `+
		`WHERE o.body ==> 'luxury' OR o.price > 5 OR o.id = 'd004' OR o.id = 'd006' ORDER BY o.id`)
	want := map[string]float64{}
	for id, w := range scoreOracle(t, `luxury`) {
		want[id] = w
	}
	want["d004"], want["d005"], want["d006"] = 0, 0, 0
	if len(got) != len(want) {
		t.Fatalf("%d rows %v, want %d", len(got), got, len(want))
	}
	for _, row := range got {
		id := row[0].(string)
		w, ok := want[id]
		if !ok {
			t.Fatalf("unexpected id %q", id)
		}
		if score := row[1].(float64); !scoreClose(score, w) {
			t.Fatalf("id %q: SCORE() %v, want %v", id, score, w)
		}
	}
}

// TestSQLScorePlaceholder proves a parameterized ==> rebinds statistics per
// execution: the same prepared statement scores two different bindings, so
// the second run cannot reuse the first run's idfs.
func TestSQLScorePlaceholder(t *testing.T) {
	db := tinMatchDatabase(t, true)
	catalog := db.Snapshot()
	statement, err := PrepareStatement(
		`SELECT o.id, SCORE() FROM docs AS o WHERE o.body ==> ? ORDER BY o.id`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	for _, tinql := range []string{`luxury AND goods`, `luxury`} {
		want := scoreOracle(t, tinql)
		exec := Exec{}
		cursor, err := statement.RunInto(&exec, FromDatabase(catalog, "docs"), []any{tinql})
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for cursor.Next() {
			id, _ := cursor.Cell(0).Text()
			score, _ := cursor.Cell(1).Float64()
			w, ok := want[id]
			if !ok {
				t.Fatalf("%q: unexpected id %q", tinql, id)
			}
			if !scoreClose(score, w) {
				t.Fatalf("%q id %q: SCORE() %v != index %v", tinql, id, score, w)
			}
			n++
		}
		if n != len(want) {
			t.Fatalf("%q: %d rows, want %d", tinql, n, len(want))
		}
		exec.Release()
	}
}

// TestSQLScoreOutputName proves SELECT SCORE() needs no alias: the output
// column is named score like a function call.
func TestSQLScoreOutputName(t *testing.T) {
	db := tinMatchDatabase(t, true)
	catalog := db.Snapshot()
	statement, err := PrepareStatement(
		`SELECT o.id, SCORE() FROM docs AS o WHERE o.body ==> 'luxury' ORDER BY o.id`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	exec := Exec{}
	defer exec.Release()
	if _, err := statement.RunInto(&exec, FromDatabase(catalog, "docs"), nil); err != nil {
		t.Fatal(err)
	}
	col, ok := exec.Result.Column("score")
	if !ok {
		t.Fatal("no score output column")
	}
	if len(col.Cells) != 3 {
		t.Fatalf("%d score cells, want 3", len(col.Cells))
	}
}

// TestSQLScoreErrors proves every ambiguous or unsupported SCORE() shape
// fails at prepare with a positioned error naming SCORE(), never a silent
// zero or a wrong-row success.
func TestSQLScoreErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want string
	}{
		{"no match", `SELECT o.id, SCORE() FROM docs AS o`, "SCORE() requires a ==>"},
		{"two matches", `SELECT o.id, SCORE() FROM docs AS o WHERE o.body ==> 'a' AND o.id ==> 'b'`, "exactly one ==>"},
		{"argument", `SELECT o.id, SCORE(o.body) FROM docs AS o WHERE o.body ==> 'a'`, "takes no arguments"},
		{"grouped", `SELECT o.id, SCORE() FROM docs AS o WHERE o.body ==> 'a' GROUP BY o.id`, "GROUP BY"},
		{"join on", `SELECT o.id FROM docs AS o JOIN docs AS p ON SCORE() > 1`, "not allowed in ON"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := PrepareStatement(tc.sql)
			if err == nil {
				t.Fatalf("%s: prepare succeeded, want SCORE() error", tc.sql)
			}
			if !contains(err.Error(), "SCORE()") || !contains(err.Error(), tc.want) {
				t.Fatalf("%s: error %q wants SCORE() and %q", tc.sql, err, tc.want)
			}
		})
	}
}

// TestSQLScoreFieldName proves a document field named score keeps its
// native projection and predicate plans: SCORE is a call only with (),
// so selecting or filtering a score field never engages the scalar
// sidecar. (The replicated driver suite covers the same for its own
// score column; this pins it at the query layer.)
func TestSQLScoreFieldName(t *testing.T) {
	db := &store.Database{}
	coll, err := db.CreateCollection("t", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, doc := range []struct{ key, body string }{
		{"s1", `{"id":"s1","score":42}`},
		{"s2", `{"id":"s2","score":"high"}`},
	} {
		if _, err := coll.Put(doc.key, []byte(doc.body)); err != nil {
			t.Fatal(err)
		}
	}
	catalog := db.Snapshot()
	statement, err := PrepareStatement(`SELECT o.score FROM t AS o ORDER BY o.id`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	exec := Exec{}
	defer exec.Release()
	cursor, err := statement.RunInto(&exec, FromDatabase(catalog, "t"), nil)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for cursor.Next() {
		got = append(got, string(cursor.Cell(0).JSON()))
	}
	if len(got) != 2 || got[0] != "42" || got[1] != `"high"` {
		t.Fatalf("score projection = %v, want [42 high]", got)
	}
	filtered, err := PrepareStatement(`SELECT o.id FROM t AS o WHERE o.score = 42`)
	if err != nil {
		t.Fatal(err)
	}
	defer filtered.Release()
	exec2 := Exec{}
	defer exec2.Release()
	cursor2, err := filtered.RunInto(&exec2, FromDatabase(catalog, "t"), nil)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for cursor2.Next() {
		id, ok := cursor2.Cell(0).Text()
		if !ok {
			t.Fatalf("id cell = %s, want string", cursor2.Cell(0).JSON())
		}
		ids = append(ids, id)
	}
	if len(ids) != 1 || ids[0] != "s1" {
		t.Fatalf("score filter ids = %v, want [s1]", ids)
	}
}

// TestSQLScoreAddsNoAllocations proves scoring is allocation-free by
// design: a warmed statement selecting SCORE() allocates no more per run
// than the same ==> query without it. The ==> reparse already spends per
// execution, so the test compares the two shapes instead of pinning zero.
func TestSQLScoreAddsNoAllocations(t *testing.T) {
	db := tinMatchDatabase(t, true)
	catalog := db.Snapshot()
	const where = ` WHERE o.body ==> 'luxury' ORDER BY o.id`
	plain, err := PrepareStatement(`SELECT o.id FROM docs AS o` + where)
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Release()
	scored, err := PrepareStatement(`SELECT o.id, SCORE() FROM docs AS o` + where)
	if err != nil {
		t.Fatal(err)
	}
	defer scored.Release()
	exec := Exec{}
	defer exec.Release()
	run := func(s *Statement) {
		t.Helper()
		if _, err := s.RunInto(&exec, FromDatabase(catalog, "docs"), nil); err != nil {
			t.Fatal(err)
		}
	}
	for range 5 {
		run(plain)
		run(scored)
	}
	base := testing.AllocsPerRun(200, func() { run(plain) })
	with := testing.AllocsPerRun(200, func() { run(scored) })
	if with > base {
		t.Fatalf("SCORE() allocated %.2f per run over %.2f without it", with, base)
	}
}

// TestSQLScoreFileAgreesWithTinSearch proves SCORE() over the durable path:
// the file scan's scores equal the generation-pinned TinSearch ranking, so
// the TinBuild statistics branch agrees with the heap branch.
func TestSQLScoreFileAgreesWithTinSearch(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "tin-score-file-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	collection, err := durable.Create(file, durable.Options{
		Indexes: []store.IndexDefinition{
			{Name: "body_tin", Paths: []string{"/body"}, Kind: store.IndexTin},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = collection.Close() })
	bodies := []string{
		"luxury vintage watches for sale",
		"cheap goods everyday low prices",
		"luxury goods and vintage clocks",
		"ordinary household items",
		"luxury luxury luxury watches",
	}
	for i, body := range bodies {
		key := "k" + strconv.Itoa(i)
		doc := `{"id":` + strconv.Quote(key) + `,"body":` + strconv.Quote(body) + `}`
		if _, err := collection.Put([]byte(key), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	for _, tinql := range []string{`luxury`, `luxury AND vintage`, `"vintage watches"`} {
		statement, err := PrepareStatement(
			`SELECT o.id, SCORE() FROM docs AS o WHERE o.body ==> '` + tinql + `' ORDER BY o.id`)
		if err != nil {
			t.Fatal(err)
		}
		exec := Exec{}
		cursor, err := statement.RunInto(&exec, FromFile(snapshot), nil)
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]float64{}
		for cursor.Next() {
			id, ok := cursor.Cell(0).Text()
			if !ok {
				t.Fatalf("id cell = %s, want string", cursor.Cell(0).JSON())
			}
			score, ok := cursor.Cell(1).Float64()
			if !ok {
				t.Fatalf("score cell = %s, want number", cursor.Cell(1).JSON())
			}
			got[id] = score
		}
		exec.Release()
		statement.Release()
		hits, err := collection.TinSearch(snapshot, "/body", tinql, 10000)
		if err != nil {
			t.Fatalf("%q: TinSearch: %v", tinql, err)
		}
		if len(got) != len(hits) {
			t.Fatalf("%q: %d rows, want %d hits", tinql, len(got), len(hits))
		}
		for _, hit := range hits {
			score, ok := got[hit.Key]
			if !ok {
				t.Fatalf("%q: missing key %q", tinql, hit.Key)
			}
			if !scoreClose(score, hit.Score) {
				t.Fatalf("%q key %q: SCORE() %v != TinSearch %v", tinql, hit.Key, score, hit.Score)
			}
		}
	}
}
