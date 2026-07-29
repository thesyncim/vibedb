package query

import (
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
)

// The filter-first execution contract: which columns a plan promises to
// classify for every scanned row, what a row the filter rejected may still do
// to the query, and that gathering the output columns for the survivors alone
// answers exactly what materializing all of them did.

// TestPlanPartitionsFilterAndLateColumns pins the partition the whole
// filter-first design rests on. Two failures it is here to catch: a predicate
// column classified as late, which would have execution evaluate WHERE against
// a column that does not exist yet; and a path named by both WHERE and the
// output being registered twice, which would extract the same column in both
// phases and quietly undo pathRegistry's dedupe.
func TestPlanPartitionsFilterAndLateColumns(t *testing.T) {
	cases := []struct {
		name   string
		query  *Query
		filter []string
		late   []string
	}{
		{
			name:   "disjoint",
			query:  Select(Path("a")).Where(Cmp("b", Eq, 1)),
			filter: []string{"b"},
			late:   []string{"a"},
		},
		{
			name:   "shared path is extracted once, in the filter phase",
			query:  Select(Path("a"), Path("b")).Where(Cmp("a", Eq, 1)),
			filter: []string{"a"},
			late:   []string{"b"},
		},
		{
			name:   "every operand of a boolean tree is a filter column",
			query:  Select(Path("a")).Where(Or(And(Cmp("c", Eq, 1), Exists("d")), Not(IsNull("e")))),
			filter: []string{"c", "d", "e"},
			late:   []string{"a"},
		},
		{
			// Containment lowers to a derived equality tree for index
			// planning. That tree names paths no column was registered for, so
			// counting it would put a nonexistent column in the filter phase.
			name:   "containment reads only its own haystack column",
			query:  Select(Path("a")).Where(Contains("obj", `{"x":1}`)),
			filter: []string{"obj"},
			late:   []string{"a"},
		},
		{
			name:   "membership",
			query:  Select(Path("a")).Where(In("b", 1, 2, 3)),
			filter: []string{"b"},
			late:   []string{"a"},
		},
		{
			name:   "grouping and ordering keys are late",
			query:  Select(Path("g"), Count()).GroupBy("g").OrderBy("g", Asc).Where(Cmp("s", Lt, 5)),
			filter: []string{"s"},
			late:   []string{"g"},
		},
		{
			name:   "an unfiltered plan has no filter phase",
			query:  Select(Path("a"), Path("b")),
			filter: nil,
			late:   []string{"a", "b"},
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			p, err := test.query.compiled()
			if err != nil {
				t.Fatal(err)
			}
			if got := columnSpecs(p, p.filterCols); !equalSpecs(got, test.filter) {
				t.Errorf("filter columns = %v, want %v", got, test.filter)
			}
			if got := columnSpecs(p, p.lateCols); !equalSpecs(got, test.late) {
				t.Errorf("late columns = %v, want %v", got, test.late)
			}
			if total := len(p.filterCols) + len(p.lateCols); total != len(p.valuePaths) {
				t.Errorf("partition covers %d of %d value paths", total, len(p.valuePaths))
			}
		})
	}
}

func columnSpecs(p *plan, cols []int) []string {
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		out = append(out, p.valuePaths[c].spec)
	}
	return out
}

func equalSpecs(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestFilteredScanIgnoresExcludedRowValueErrors states the consequence a
// filter-first scan has on error reporting: resolving a projected path is work
// done for the rows the filter kept, so a row the filter rejected can no longer
// fail the query with a pointer error its value would have raised.
//
// This is the rule an index-bounded scan has always followed — it gathers only
// candidate rows and never sees the others — extended to the unindexed scan, so
// the two now agree instead of differing by whether an index happened to exist.
func TestFilteredScanIgnoresExcludedRowValueErrors(t *testing.T) {
	docs := [][]byte{[]byte(`{"k":1,"a":{"x":"kept"}}`)}
	for i := 0; i < 7; i++ {
		// "x" is not an array index, so resolving /a/x against this row is a
		// pointer error rather than an absent value.
		docs = append(docs, fmt.Appendf(nil, `{"k":0,"a":[%d,%d]}`, i, i))
	}
	for _, mode := range storageModes {
		t.Run(mode.name, func(t *testing.T) {
			set := buildSegment(t, docs, mode)
			q := Select(Path("/a/x")).Where(Cmp("k", Eq, 1))
			got, err := q.Run(FromSegment(set))
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if got.RowCount != 1 {
				t.Fatalf("RowCount = %d, want 1", got.RowCount)
			}
			if text, ok := got.Columns[0].Cells[0].Text(); !ok || text != "kept" {
				t.Fatalf("cell = %q, %v; want %q", text, ok, "kept")
			}
			// The same path outside a filter still reports the error, so this
			// is a narrowing of when rows are read, not a swallowed error.
			if _, err := Select(Path("/a/x")).Run(FromSegment(set)); err == nil {
				t.Fatal("unfiltered projection of /a/x reported no error")
			}
		})
	}
}

// lateCorpus is a corpus wide enough to drive a filter through every
// selectivity the materialization choice turns on, with escaped strings in
// both a predicate column and a projected column so the two decoded-text
// arenas are exercised in the same query.
func lateCorpus(rows int) [][]byte {
	docs := make([][]byte, rows)
	for i := range docs {
		docs[i] = fmt.Appendf(nil,
			`{"id":%d,"sel":%d,"name":"n\u%04x-%d","tag":"t\u%04x","score":%d,"obj":{"x":%d}}`,
			i, i%8, 'a'+i%26, i, 'A'+i%13, i*3%37, i%3)
	}
	return docs
}

func lateQueries() []*Query {
	wide := Select(Path("id"), Path("name"), Path("tag"), Path("obj"))
	return []*Query{
		// Selectivity sweep across the gather threshold: one eighth, one half,
		// and everything.
		Select(Path("id"), Path("name")).Where(Cmp("sel", Eq, 3)),
		Select(Path("id"), Path("name")).Where(Cmp("sel", Lt, 4)),
		Select(Path("id"), Path("name")).Where(Cmp("sel", Lt, 8)),
		Select(Path("id"), Path("name")).Where(Cmp("sel", Lt, 0)),
		// A path read by both clauses must survive compaction intact.
		Select(Path("sel"), Path("name")).Where(Cmp("sel", Eq, 2)),
		// Escaped strings decoded in the filter phase, then compared and
		// grouped after the late phase grew its own arena.
		Select(Path("tag"), Count()).GroupBy("tag").Where(Cmp("name", Ge, "nb")).OrderBy("tag", Desc),
		// Ordering and limiting must see every survivor, not a prefix.
		Select(Path("id"), Path("name")).Where(Cmp("sel", Lt, 3)).OrderBy("name", Asc).OrderBy("id", Desc).Limit(5),
		// An unordered limit may truncate before materializing.
		Select(Path("id"), Path("name")).Where(Cmp("sel", Lt, 3)).Limit(5),
		// Aggregates read numeric columns, which are late in every plan.
		Select(Count(), Sum("score"), Avg("score"), Min("score"), Max("score")).Where(Cmp("sel", Lt, 2)),
		Select(Path("sel"), Count(), Sum("score")).GroupBy("sel").Where(Exists("score")).OrderBy("sel", Asc),
		wide.Where(Contains("obj", `{"x":1}`)),
		Select(Path("id")).Where(In("sel", 1, 5)),
		Select(Path("id")).Where(IsNull("absent")),
	}
}

// TestLateMaterializationMatchesReference checks the filter-first scan against
// the independent reference executor over a corpus large enough for the gather
// threshold to be crossed in both directions.
func TestLateMaterializationMatchesReference(t *testing.T) {
	docs := lateCorpus(96)
	decoded := decodeDocs(t, docs)
	queries := lateQueries()
	for _, mode := range storageModes {
		set := buildSegment(t, docs, mode)
		for qi, q := range queries {
			got, err := q.Run(FromSegment(set))
			if err != nil {
				t.Fatalf("query %d %s: Run: %v", qi, mode.name, err)
			}
			if diff := compareResults(got, referenceRun(t, q, decoded)); diff != "" {
				t.Fatalf("query %d %s: %s", qi, mode.name, diff)
			}
		}
	}
}

// TestLateMaterializationSnapshotSparseSlots is the snapshot half, over a
// collection whose live slots are deliberately sparse. A snapshot scan numbers
// its rows by position in live chunk/slot order, and the late gather has to
// translate those numbers back into addresses through the live masks; a
// collection with holes in it is what tells a correct translation from one that
// only works when slot equals ordinal.
func TestLateMaterializationSnapshotSparseSlots(t *testing.T) {
	collection, err := store.New(store.Options{ChunkDocuments: 8, ShapeTapes: true})
	if err != nil {
		t.Fatal(err)
	}
	docs := lateCorpus(96)
	for i, doc := range docs {
		if _, err := collection.Put(fmt.Sprintf("k%03d", i), doc); err != nil {
			t.Fatal(err)
		}
	}
	// Delete an irregular pattern so no chunk keeps a dense prefix of slots.
	for i := range docs {
		if i%3 == 1 || i%7 == 2 {
			if _, err := collection.Delete(fmt.Sprintf("k%03d", i)); err != nil {
				t.Fatal(err)
			}
		}
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	// The oracle is a Segment holding the surviving documents in the snapshot's
	// own live order, which Range defines. Comparing against it checks the
	// gathered values and their order at once.
	var survivors [][]byte
	snapshot.Range(func(_ string, value vibejson.RawValue) bool {
		survivors = append(survivors, append([]byte(nil), value.Bytes()...))
		return true
	})
	if len(survivors) != snapshot.Len() {
		t.Fatalf("Range visited %d rows, snapshot holds %d", len(survivors), snapshot.Len())
	}
	set := buildSegment(t, survivors, storageMode{"shaped", false, true})
	decoded := decodeDocs(t, survivors)

	for qi, q := range lateQueries() {
		got, err := q.Run(FromSnapshot(snapshot))
		if err != nil {
			t.Fatalf("query %d snapshot: Run: %v", qi, err)
		}
		want, err := q.Run(FromSegment(set))
		if err != nil {
			t.Fatalf("query %d segment: Run: %v", qi, err)
		}
		if gotKey, wantKey := resultKey(got), resultKey(want); gotKey != wantKey {
			t.Fatalf("query %d snapshot/segment mismatch:\n got: %s\nwant: %s", qi, gotKey, wantKey)
		}
		if diff := compareResults(got, referenceRun(t, q, decoded)); diff != "" {
			t.Fatalf("query %d snapshot vs reference: %s", qi, diff)
		}
	}
}
