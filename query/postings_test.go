package query

import (
	"testing"

	"github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibedb/store"
)

// Given a bounded family of Segments built with the inverted postings enabled,
// and a battery of WHERE predicates spanning the postable primitives (EXISTS,
// scalar @>, equality) and the unpostable ones (ranges, IS NULL, NOT, structured
// @>) in every And/Or/Not combination, when each query runs, then the
// postings-accelerated Result equals the same query's full-scan Result (postings
// off) and both equal the naive reference — for every (segment × postings config ×
// query). The seam only prunes candidates the compiled predicate re-verifies, so
// agreement is the contract; this differential is its exhaustive check over the
// bounded domain.

// postPool is the bounded document domain for the postings differential:
// objects with scalar fields (for equality and existence), array fields (for
// scalar containment), an object field (for structured containment), an explicit
// null, a duplicate key, and a non-object root. Every predicate path is a single
// top-level field, so the postable leaves exercise the shape and remainder
// posting paths and the value buckets.
var postPool = [][]byte{
	[]byte(`{}`),
	[]byte(`{"a":1,"b":"x"}`),
	[]byte(`{"a":2,"b":"y","c":true}`),
	[]byte(`{"a":1,"tags":["x","y"]}`),
	[]byte(`{"a":null,"b":"x"}`),
	[]byte(`{"b":"z","tags":["a"]}`),
	[]byte(`{"a":1,"a":2}`), // duplicate key: last wins
	[]byte(`[1,2,3]`),       // non-object root: every field absent
	[]byte(`{"m":{"k":1},"a":3}`),
	[]byte(`{"a":2,"tags":["x"]}`),
}

// postConfig is one Postings-on storage configuration; the differential runs
// each against the postings-off full scan.
type postConfig struct {
	name       string
	hashKeys   bool
	shapeTapes bool
}

// The three Postings-on configurations cover the classic-remainder posting path
// (no shape tapes) and the shape-index path (shape tapes), with and without key
// hashing.
var postConfigs = []postConfig{
	{"postings", false, false},
	{"postings+shaped", false, true},
	{"postings+hashed+shaped", true, true},
}

func buildPostSet(t testing.TB, docs [][]byte, cfg postConfig, postings bool) *store.Segment {
	t.Helper()
	set := &store.Segment{}
	set.Options = document.IndexOptions{HashKeys: cfg.hashKeys}
	set.ShapeTapes = cfg.shapeTapes
	set.Postings = postings
	for _, d := range docs {
		if _, err := set.Append(d); err != nil {
			t.Fatalf("Append(%s): %v", d, err)
		}
	}
	return set
}

// postingsPredBattery is the WHERE battery for the postings differential: each
// entry is a predicate; the differential wraps it in a projection and an
// aggregate. It spans postable leaves, unpostable leaves, and every combinator,
// including cases that must fall back to a full scan (an unpostable OR disjunct)
// and cases that prune to an empty candidate set.
func postingsPredBattery() []Predicate {
	return []Predicate{
		Exists("a"),
		Exists("tags"),
		Not(Exists("a")),
		Cmp("a", Eq, 1),
		Cmp("a", Eq, 2),
		Cmp("b", Eq, "x"),
		Cmp("c", Eq, true),
		Cmp("b", Eq, "nomatch"),       // postable, empty candidate set
		Cmp("a", Ne, 1),               // unpostable operator: full scan
		Cmp("a", Gt, 1),               // unpostable operator: full scan
		Contains("tags", `"x"`),       // scalar containment: WhereContains prunes
		Contains("", `{"a":1}`),       // one-member object: derived a=1 probe
		Contains("tags", `["x","y"]`), // structured needle: unpostable leaf
		Contains("m", `{"k":1}`),      // structured needle: unpostable leaf
		IsNull("a"),
		Not(IsNull("a")),
		And(Exists("a"), Cmp("b", Eq, "x")),
		And(Cmp("a", Eq, 1), Exists("tags")),
		And(Exists("a"), Cmp("a", Gt, 0)), // postable ∧ unpostable: prune by EXISTS
		And(Cmp("a", Eq, 1), Cmp("b", Eq, "x")),
		Or(Cmp("a", Eq, 1), Cmp("b", Eq, "z")), // both postable: union
		Or(Cmp("a", Eq, 1), Cmp("a", Gt, 5)),   // postable ∨ unpostable: full scan
		Or(Exists("a"), Contains("tags", `"x"`)),
		And(Or(Cmp("a", Eq, 1), Cmp("a", Eq, 2)), Exists("b")),
		Not(And(Exists("a"), Cmp("b", Eq, "x"))),
	}
}

func TestExhaustivePostingsDifferential(t *testing.T) {
	segments := enumerateSegments(postPool, 3)
	preds := postingsPredBattery()

	// Each predicate is exercised as a projection (filter visible directly) and
	// as an aggregate (filter feeds the reduction).
	queries := func(pred Predicate) []*Query {
		return []*Query{
			Select(Path("a"), Path("b")).Where(pred),
			Select(Count(), Sum("a")).Where(pred),
		}
	}

	checks := 0
	for _, docs := range segments {
		decoded := decodeDocs(t, docs)
		full := buildPostSet(t, docs, postConfig{"full", false, false}, false)
		accel := make([]*store.Segment, len(postConfigs))
		for i, cfg := range postConfigs {
			accel[i] = buildPostSet(t, docs, cfg, true)
		}
		for pi, pred := range preds {
			for _, q := range queries(pred) {
				want, err := q.Run(FromSegment(full))
				if err != nil {
					t.Fatalf("pred %d over %v: full-scan Run: %v", pi, jsonStrings(docs), err)
				}
				ref := referenceRun(t, q, decoded)
				if diff := compareResults(want, ref); diff != "" {
					t.Fatalf("pred %d over %v: full scan disagrees with reference: %s", pi, jsonStrings(docs), diff)
				}
				wantKey := resultKey(want)
				for i, set := range accel {
					got, err := q.Run(FromSegment(set))
					if err != nil {
						t.Fatalf("pred %d %s over %v: Run: %v", pi, postConfigs[i].name, jsonStrings(docs), err)
					}
					if resultKey(got) != wantKey {
						t.Fatalf("pred %d %s over %v: postings-accelerated disagrees with full scan:\n got: %s\nwant: %s",
							pi, postConfigs[i].name, jsonStrings(docs), resultKey(got), wantKey)
					}
					checks++
				}
			}
		}
	}
	t.Logf("exhaustive postings differential: %d segments × %d postings configs × %d predicates × 2 shapes = %d accelerated==full-scan checks",
		len(segments), len(postConfigs), len(preds), checks)
}

// TestPostingsSeamPrunes is a white-box check that the seam actually narrows:
// with Postings on, a selective equality yields a small non-nil candidate set,
// while with Postings off (and for an unpostable predicate) it yields nil, the
// full-scan sentinel.
func TestPostingsSeamPrunes(t *testing.T) {
	docs := make([][]byte, 0, 200)
	for i := 0; i < 200; i++ {
		docs = append(docs, []byte(`{"k":`+itoa(i%100)+`,"g":"`+itoa(i%7)+`"}`))
	}
	on := buildPostSet(t, docs, postConfig{"", false, true}, true)
	off := buildPostSet(t, docs, postConfig{"", false, true}, false)

	// A selective equality: k == 3 matches 2 of 200 rows.
	q := Select(Count()).Where(Cmp("k", Eq, 3))
	p, err := q.compiled()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	candidatesOf := func(set *store.Segment) []int {
		var w Workspace
		return p.candidateRows(set, &w)
	}

	got := candidatesOf(on)
	if got == nil {
		t.Fatalf("Postings on: candidateRows returned nil (full scan), want a pruned candidate set")
	}
	if len(got) != 2 {
		t.Fatalf("Postings on: candidateRows returned %d candidates, want 2", len(got))
	}
	if off := candidatesOf(off); off != nil {
		t.Fatalf("Postings off: candidateRows returned %v, want nil (full scan)", off)
	}

	// An unpostable predicate keeps the full scan even with Postings on.
	qr := Select(Count()).Where(Cmp("k", Gt, 50))
	pr, err := qr.compiled()
	if err != nil {
		t.Fatalf("compile range: %v", err)
	}
	var w Workspace
	if c := pr.candidateRows(on, &w); c != nil {
		t.Fatalf("unpostable range with Postings on: candidateRows = %v, want nil (full scan)", c)
	}
}

func TestSparseSelectionPolicy(t *testing.T) {
	for _, tc := range []struct {
		name       string
		candidates int
		total      int
		hasBound   bool
		want       bool
	}{
		{"no bound", 0, 100, false, false},
		{"empty bound", 0, 100, true, true},
		{"half", 50, 100, true, true},
		{"over half", 51, 100, true, false},
		{"all", 100, 100, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := preferSparseRows(tc.candidates, tc.total, tc.hasBound); got != tc.want {
				t.Fatalf("preferSparseRows(%d,%d,%v) = %v, want %v",
					tc.candidates, tc.total, tc.hasBound, got, tc.want)
			}
		})
	}
}

// TestPostingsQueryKeepsShapeTapesCompact guards the storage side of selection
// pushdown: probing and rechecking a selective equality must read narrow shape
// values directly, never cache the classic tapes Doc would synthesize.
func TestPostingsQueryKeepsShapeTapesCompact(t *testing.T) {
	docs := make([][]byte, 0, 256)
	for i := 0; i < cap(docs); i++ {
		docs = append(docs, []byte(`{"k":`+itoa(i%128)+`,"v":`+itoa(i)+`}`))
	}
	set := buildPostSet(t, docs, postConfig{"compact", true, true}, true)
	before := set.Stats()
	q := Select(Path("v")).Where(Cmp("k", Eq, 3))
	got, err := q.Run(FromSegment(set))
	if err != nil {
		t.Fatal(err)
	}
	if got.RowCount != 2 {
		t.Fatalf("RowCount = %d, want 2", got.RowCount)
	}
	after := set.Stats()
	if after.Widened != before.Widened {
		t.Fatalf("query widened %d compact tapes", after.Widened-before.Widened)
	}
}

// itoa is a tiny non-negative int formatter for the fixture, kept local so the
// test file does not pull strconv in for one use.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
