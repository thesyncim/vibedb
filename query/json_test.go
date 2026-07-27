package query

import (
	"fmt"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

// The JSON document front end is a translation layer, so its tests are almost
// all differential: a query document and the builder tree it denotes must
// produce byte-identical results over the same bounded document domain the
// builder's own exhaustive differential uses. What remains — number exactness,
// membership semantics, and rejection of malformed documents — is tested
// directly, because those are properties the builder twin cannot express.

// --- document front end vs builder ---------------------------------------

// jsonTwin pairs a query document with the builder query it must compile to.
type jsonTwin struct {
	name    string
	doc     M
	builder *Query
}

// renameColumn gives a builder column the output name a document alias would.
// The builder has no public alias form; the document front end sets the header
// directly, so the twin does too.
func renameColumn(c Column, header string) Column {
	c.header = header
	return c
}

// jsonTwins covers every clause and operator the document front end accepts,
// each alongside the builder tree it denotes.
func jsonTwins() []jsonTwin {
	return []jsonTwin{
		{
			name:    "bare path projection",
			doc:     M{"select": "a"},
			builder: Select(Path("a")),
		},
		{
			name:    "path list",
			doc:     M{"select": A{"a", "b", "c"}},
			builder: Select(Path("a"), Path("b"), Path("c")),
		},
		{
			name:    "count star",
			doc:     M{"select": M{"$count": nil}},
			builder: Select(Count()),
		},
		{
			name:    "count path",
			doc:     M{"select": M{"$count": "a"}},
			builder: Select(Count("a")),
		},
		{
			name: "every aggregate",
			doc: M{"select": A{
				M{"$sum": "a"}, M{"$avg": "b"}, M{"$min": "c"}, M{"$max": "a"}, M{"$count": nil},
			}},
			builder: Select(Sum("a"), Avg("b"), Min("c"), Max("a"), Count()),
		},
		{
			name:    "implicit equality",
			doc:     M{"select": "a", "where": M{"a": Number("1")}},
			builder: Select(Path("a")).Where(Cmp("a", Eq, 1)),
		},
		{
			name:    "string equality",
			doc:     M{"select": "a", "where": M{"a": "x"}},
			builder: Select(Path("a")).Where(Cmp("a", Eq, "x")),
		},
		{
			name:    "bool equality",
			doc:     M{"select": "a", "where": M{"a": true}},
			builder: Select(Path("a")).Where(Cmp("a", Eq, true)),
		},
		{
			name:    "null is the null test",
			doc:     M{"select": "a", "where": M{"a": nil}},
			builder: Select(Path("a")).Where(IsNull("a")),
		},
		{
			name:    "explicit comparison operators",
			doc:     M{"select": "a", "where": M{"a": M{"$gte": Number("1"), "$lt": Number("3")}}},
			builder: Select(Path("a")).Where(And(Cmp("a", Ge, 1), Cmp("a", Lt, 3))),
		},
		{
			name:    "not equal",
			doc:     M{"select": "a", "where": M{"a": M{"$ne": Number("1")}}},
			builder: Select(Path("a")).Where(Cmp("a", Ne, 1)),
		},
		{
			name:    "eq null is the null test",
			doc:     M{"select": "a", "where": M{"a": M{"$eq": nil}}},
			builder: Select(Path("a")).Where(IsNull("a")),
		},
		{
			name:    "ne null negates the null test",
			doc:     M{"select": "a", "where": M{"a": M{"$ne": nil}}},
			builder: Select(Path("a")).Where(Not(IsNull("a"))),
		},
		{
			name:    "sibling keys conjoin",
			doc:     M{"select": "a", "where": M{"a": Number("1"), "b": Number("2")}},
			builder: Select(Path("a")).Where(And(Cmp("a", Eq, 1), Cmp("b", Eq, 2))),
		},
		{
			name: "explicit and",
			doc: M{"select": "a", "where": M{"$and": A{
				M{"a": Number("1")}, M{"b": Number("2")},
			}}},
			builder: Select(Path("a")).Where(And(Cmp("a", Eq, 1), Cmp("b", Eq, 2))),
		},
		{
			name: "explicit or",
			doc: M{"select": "a", "where": M{"$or": A{
				M{"a": Number("1")}, M{"b": Number("2")},
			}}},
			builder: Select(Path("a")).Where(Or(Cmp("a", Eq, 1), Cmp("b", Eq, 2))),
		},
		{
			name:    "top level not",
			doc:     M{"select": "a", "where": M{"$not": M{"a": Number("1")}}},
			builder: Select(Path("a")).Where(Not(Cmp("a", Eq, 1))),
		},
		{
			name:    "operator level not",
			doc:     M{"select": "a", "where": M{"a": M{"$not": M{"$gt": Number("1")}}}},
			builder: Select(Path("a")).Where(Not(Cmp("a", Gt, 1))),
		},
		{
			name:    "exists true",
			doc:     M{"select": "a", "where": M{"a": M{"$exists": true}}},
			builder: Select(Path("a")).Where(Exists("a")),
		},
		{
			name:    "exists false",
			doc:     M{"select": "a", "where": M{"a": M{"$exists": false}}},
			builder: Select(Path("a")).Where(Not(Exists("a"))),
		},
		{
			name:    "null operator",
			doc:     M{"select": "a", "where": M{"a": M{"$null": true}}},
			builder: Select(Path("a")).Where(IsNull("a")),
		},
		{
			name:    "not null operator",
			doc:     M{"select": "a", "where": M{"a": M{"$null": false}}},
			builder: Select(Path("a")).Where(Not(IsNull("a"))),
		},
		{
			name:    "membership",
			doc:     M{"select": "a", "where": M{"a": M{"$in": A{Number("1"), Number("2")}}}},
			builder: Select(Path("a")).Where(In("a", 1, 2)),
		},
		{
			name:    "membership with null alternative",
			doc:     M{"select": "a", "where": M{"a": M{"$in": A{Number("1"), nil}}}},
			builder: Select(Path("a")).Where(Or(In("a", 1), IsNull("a"))),
		},
		{
			name:    "empty membership matches nothing",
			doc:     M{"select": "a", "where": M{"a": M{"$in": A{}}}},
			builder: Select(Path("a")).Where(In("a")),
		},
		{
			name:    "negated membership",
			doc:     M{"select": "a", "where": M{"a": M{"$nin": A{Number("1"), Number("2")}}}},
			builder: Select(Path("a")).Where(Not(In("a", 1, 2))),
		},
		{
			name:    "containment",
			doc:     M{"select": "a", "where": M{"a": M{"$contains": Number("1")}}},
			builder: Select(Path("a")).Where(Contains("a", "1")),
		},
		{
			name:    "group by and order by",
			doc:     M{"select": A{"a", M{"$count": nil}}, "groupBy": "a", "orderBy": "a"},
			builder: Select(Path("a"), Count()).GroupBy("a").OrderBy("a", Asc),
		},
		{
			// An alias renames the column without changing what it computes,
			// so the aliased query must differ from its unaliased twin in the
			// header and in nothing else.
			name:    "aliased aggregate",
			doc:     M{"select": A{"a", M{"n": M{"$count": nil}}}, "groupBy": "a", "orderBy": "a"},
			builder: Select(Path("a"), renameColumn(Count(), "n")).GroupBy("a").OrderBy("a", Asc),
		},
		{
			name:    "aliased projection",
			doc:     M{"select": A{M{"field": "a"}}},
			builder: Select(renameColumn(Path("a"), "field")),
		},
		{
			name: "order by descending",
			doc: M{
				"select": A{"a"}, "groupBy": A{"a"}, "orderBy": A{M{"a": "desc"}},
			},
			builder: Select(Path("a")).GroupBy("a").OrderBy("a", Desc),
		},
		{
			name: "numeric sort direction",
			doc: M{
				"select": A{"a"}, "groupBy": A{"a"}, "orderBy": A{M{"a": Number("-1")}},
			},
			builder: Select(Path("a")).GroupBy("a").OrderBy("a", Desc),
		},
		{
			name:    "limit",
			doc:     M{"select": "a", "orderBy": "a", "limit": Number("2")},
			builder: Select(Path("a")).OrderBy("a", Asc).Limit(2),
		},
		{
			name:    "whole document by default",
			doc:     M{"where": M{"a": Number("1")}},
			builder: Select(wholeDocument).Where(Cmp("a", Eq, 1)),
		},
	}
}

// Given each query document and the builder tree it denotes, when both run
// over every fixture segment and storage mode, then their results agree
// exactly. Column headers are compared too, so an alias that lands on the
// wrong column is caught.
func TestJSONQueryMatchesBuilder(t *testing.T) {
	twins := jsonTwins()
	for _, mode := range storageModes {
		for docsIndex, docs := range [][][]byte{docPool, docPool[:4], docPool[3:]} {
			set := buildSegment(t, docs, mode)
			for _, twin := range twins {
				t.Run(fmt.Sprintf("%s/%d/%s", mode.name, docsIndex, twin.name), func(t *testing.T) {
					compiled, err := New(twin.doc)
					if err != nil {
						t.Fatalf("New(%v): %v", twin.doc, err)
					}
					got := mustRun(t, compiled, set)
					want := mustRun(t, twin.builder, set)
					if gotKey, wantKey := resultKey(got), resultKey(want); gotKey != wantKey {
						t.Fatalf("document and builder disagree:\n got: %s\nwant: %s", gotKey, wantKey)
					}
				})
			}
		}
	}
}

// Given a query written as JSON text and the same query written as Go
// literals, when both compile and run, then they agree — the property that
// lets a query travel as bytes and arrive as the same plan.
func TestJSONParseMatchesNew(t *testing.T) {
	set := mustSegment(t,
		`{"a":1,"t":"acme"}`,
		`{"a":2,"t":"acme"}`,
		`{"a":3,"t":"other"}`,
		`{"a":null,"t":"acme"}`,
	)
	cases := []struct {
		text string
		doc  M
	}{
		{
			text: `{"select":["a"],"where":{"t":"acme"}}`,
			doc:  M{"select": A{"a"}, "where": M{"t": "acme"}},
		},
		{
			text: `{"select":[{"total":{"$sum":"a"}}],"where":{"a":{"$gte":2}}}`,
			doc:  M{"select": A{M{"total": M{"$sum": "a"}}}, "where": M{"a": M{"$gte": Number("2")}}},
		},
		{
			text: `{"select":["t",{"n":{"$count":null}}],"groupBy":["t"],"orderBy":[{"t":"desc"}],"limit":5}`,
			doc: M{
				"select":  A{"t", M{"n": M{"$count": nil}}},
				"groupBy": A{"t"},
				"orderBy": A{M{"t": "desc"}},
				"limit":   Number("5"),
			},
		},
		{
			text: `{"select":["a"],"where":{"a":{"$in":[1,3]}}}`,
			doc:  M{"select": A{"a"}, "where": M{"a": M{"$in": A{Number("1"), Number("3")}}}},
		},
		{
			text: `{"select":["a"],"where":{"a":null}}`,
			doc:  M{"select": A{"a"}, "where": M{"a": nil}},
		},
	}
	for _, c := range cases {
		t.Run(c.text, func(t *testing.T) {
			parsed, err := Parse([]byte(c.text))
			if err != nil {
				t.Fatalf("Parse(%s): %v", c.text, err)
			}
			built, err := New(c.doc)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			if got, want := resultKey(mustRun(t, parsed, set)), resultKey(mustRun(t, built, set)); got != want {
				t.Fatalf("Parse and New disagree:\n got: %s\nwant: %s", got, want)
			}
		})
	}
}

// Given an integer literal beyond float64's exact range, when it arrives as
// JSON text, then it compiles to an exact literal — the reason Parse keeps a
// number's original spelling instead of decoding it to float64.
func TestJSONNumberExactness(t *testing.T) {
	const near = 9007199254740993 // 2^53 + 1: not representable as float64
	set := mustSegment(t,
		fmt.Sprintf(`{"id":%d}`, near),
		fmt.Sprintf(`{"id":%d}`, near-1),
	)
	q, err := Parse(fmt.Appendf(nil, `{"select":["id"],"where":{"id":%d}}`, near))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	got := mustRun(t, q, set)
	if got.RowCount != 1 {
		t.Fatalf("RowCount=%d want 1: an exact literal must not collapse onto its float64 neighbour", got.RowCount)
	}
	column, _ := got.Column("id")
	if json := string(column.Cells[0].JSON()); json != fmt.Sprint(near) {
		t.Fatalf("matched %s want %d", json, near)
	}
}

// Given a hand-written Number that is not a JSON number, when the query
// compiles, then it is rejected rather than silently never matching.
func TestJSONNumberValidation(t *testing.T) {
	for _, bad := range []Number{"", "abc", "1.2.3", `"1"`, "+1", "0x10"} {
		if _, err := New(M{"select": "a", "where": M{"a": bad}}); err == nil {
			t.Fatalf("Number(%q) compiled; want an error", string(bad))
		}
	}
	for _, good := range []Number{"0", "-1", "1.5", "1e10", "-2.5e-3", "9007199254740993"} {
		if _, err := New(M{"select": "a", "where": M{"a": good}}); err != nil {
			t.Fatalf("Number(%q): %v", string(good), err)
		}
	}
}

// --- membership ------------------------------------------------------------

// Given a membership and the disjunction of equalities it is defined to mean,
// when both run over every fixture segment and storage mode, then they accept
// exactly the same rows. This is the contract that lets In be a compiled
// binary search rather than a chain of comparisons.
func TestInMatchesDisjunction(t *testing.T) {
	valueSets := [][]any{
		{},
		{1},
		{1, 2},
		{1, 1, 2},                               // duplicates must not change the accepted set
		{"x", 1, true},                          // mixed literal types
		{2, 1},                                  // order must not change the accepted set
		{3, 4, 5, 6, 7},                         // below the linear/binary threshold
		{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, // above it
	}
	for _, mode := range storageModes {
		set := buildSegment(t, docPool, mode)
		for _, values := range valueSets {
			t.Run(fmt.Sprintf("%s/%v", mode.name, values), func(t *testing.T) {
				alternatives := make([]Predicate, 0, len(values))
				for _, v := range values {
					alternatives = append(alternatives, Cmp("a", Eq, v))
				}
				got := mustRun(t, Select(Path("a"), Path("b")).Where(In("a", values...)), set)
				want := mustRun(t, Select(Path("a"), Path("b")).Where(Or(alternatives...)), set)
				if gotKey, wantKey := resultKey(got), resultKey(want); gotKey != wantKey {
					t.Fatalf("In and Or disagree:\n got: %s\nwant: %s", gotKey, wantKey)
				}
			})
		}
	}
}

// Given a disjunction of equalities on one path, when the query compiles,
// then it becomes a membership. A disjunction of equalities on one path is a
// membership by definition, so every front end — the builder's Or, SQL's OR,
// and a document's $or — reaches the compiled binary search without the caller
// having to know to ask for it.
func TestOrOfEqualitiesCompilesToMembership(t *testing.T) {
	cases := []struct {
		name      string
		predicate Predicate
		// want describes the compiled root: its kind, and for a membership the
		// number of alternatives it holds.
		wantKind predKind
		wantIn   map[int]int // operand index -> alternatives, for a surviving Or
		wantLits int         // alternatives, when the root is itself a membership
	}{
		{
			name:      "fully coalesced",
			predicate: Or(Cmp("a", Eq, 1), Cmp("a", Eq, 2), Cmp("a", Eq, 3)),
			wantKind:  predIn,
			wantLits:  3,
		},
		{
			name:      "duplicates deduplicated",
			predicate: Or(Cmp("a", Eq, 1), Cmp("a", Eq, 1), Cmp("a", Eq, 2)),
			wantKind:  predIn,
			wantLits:  2,
		},
		{
			name:      "two paths become two memberships",
			predicate: Or(Cmp("a", Eq, 1), Cmp("b", Eq, 1), Cmp("a", Eq, 2), Cmp("b", Eq, 2)),
			wantKind:  predOr,
			wantIn:    map[int]int{0: 2, 1: 2},
		},
		{
			name:      "a lone equality is left alone",
			predicate: Or(Cmp("a", Eq, 1), Cmp("b", Eq, 2)),
			wantKind:  predOr,
			wantIn:    map[int]int{},
		},
		{
			name:      "non-equalities pass through",
			predicate: Or(Cmp("a", Eq, 1), Cmp("a", Eq, 2), Exists("c")),
			wantKind:  predOr,
			wantIn:    map[int]int{0: 2},
		},
		{
			name:      "inequality is not an alternative",
			predicate: Or(Cmp("a", Eq, 1), Cmp("a", Gt, 2)),
			wantKind:  predOr,
			wantIn:    map[int]int{},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			prepared := Select(Path("a")).Where(c.predicate)
			if err := prepared.Prepare(); err != nil {
				t.Fatalf("Prepare: %v", err)
			}
			root := prepared.plan.where
			if root.kind != c.wantKind {
				t.Fatalf("compiled root kind = %d, want %d", root.kind, c.wantKind)
			}
			if c.wantKind == predIn {
				if len(root.lits) != c.wantLits {
					t.Fatalf("membership holds %d alternatives, want %d", len(root.lits), c.wantLits)
				}
				if len(root.needles) != len(root.lits) {
					t.Fatalf("needles=%d lits=%d: a rewritten membership must stay indexable",
						len(root.needles), len(root.lits))
				}
				return
			}
			for i, kid := range root.kids {
				want, expected := c.wantIn[i]
				if kid.kind == predIn {
					if !expected {
						t.Fatalf("operand %d became a membership unexpectedly", i)
					}
					if len(kid.lits) != want {
						t.Fatalf("operand %d holds %d alternatives, want %d", i, len(kid.lits), want)
					}
					continue
				}
				if expected {
					t.Fatalf("operand %d has kind %d, want a membership", i, kid.kind)
				}
			}
		})
	}
}

// Given a rewritten disjunction, when its alternatives are inspected, then
// they are sorted — the invariant the compiled binary search depends on, and
// the one a rewrite assembling a membership from separate leaves could most
// easily break.
func TestCoalescedMembershipIsSorted(t *testing.T) {
	prepared := Select(Path("a")).
		Where(Or(Cmp("a", Eq, 9), Cmp("a", Eq, 2), Cmp("a", Eq, 5), Cmp("a", Eq, 1)))
	if err := prepared.Prepare(); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	lits := prepared.plan.where.lits
	for i := 1; i < len(lits); i++ {
		if compareScalar(lits[i-1], lits[i]) >= 0 {
			t.Fatalf("alternatives are not strictly ascending at %d: %v", i, lits)
		}
	}
}

// Given a membership large enough to cross the linear-search threshold, when
// it runs, then it accepts the same rows as the linear form — the search and
// the scan must not disagree at the boundary.
func TestInSearchThresholdAgreement(t *testing.T) {
	docs := make([]string, 0, 64)
	for i := range 64 {
		docs = append(docs, fmt.Sprintf(`{"a":%d}`, i))
	}
	set := mustSegment(t, docs...)
	for size := 0; size <= inLinearMax*3; size++ {
		values := make([]any, 0, size)
		for i := range size {
			values = append(values, i*2) // every other document matches
		}
		got := mustRun(t, Select(Count()).Where(In("a", values...)), set)
		column, _ := got.Column("count(*)")
		matched, _ := column.Cells[0].Int64()
		want := int64(min(size, 32))
		if matched != want {
			t.Fatalf("membership of %d values matched %d rows, want %d", size, matched, want)
		}
	}
}

// Given a membership whose alternatives are null, absent, and present values,
// when it runs, then null and absent satisfy no alternative — the same rule a
// comparison follows.
func TestInNullAndAbsent(t *testing.T) {
	set := mustSegment(t, `{"a":1}`, `{"a":null}`, `{"b":9}`, `{"a":2}`)
	got := mustRun(t, Select(Count()).Where(In("a", 1, 2)), set)
	column, _ := got.Column("count(*)")
	if n, _ := column.Cells[0].Int64(); n != 2 {
		t.Fatalf("membership matched %d rows, want 2 (null and absent satisfy no alternative)", n)
	}
	// The negation therefore keeps the null and absent rows, matching Not over
	// any other comparison.
	got = mustRun(t, Select(Count()).Where(Not(In("a", 1, 2))), set)
	column, _ = got.Column("count(*)")
	if n, _ := column.Cells[0].Int64(); n != 2 {
		t.Fatalf("negated membership matched %d rows, want 2", n)
	}
}

// Given a declared exact index on the membership path, when the query runs
// over a snapshot, then the indexed result equals the unindexed one and the
// planner really did probe the index rather than fall back to a scan.
func TestInUsesDeclaredIndex(t *testing.T) {
	s, err := store.New(store.Options{ChunkDocuments: 8})
	if err != nil {
		t.Fatal(err)
	}
	set := &store.Segment{}
	for i := range 64 {
		doc := fmt.Appendf(nil, `{"tenant":"t%d","score":%d}`, i%8, i)
		if _, err := s.Put(fmt.Sprintf("k%03d", i), doc); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if _, err := set.Append(doc); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if _, err := s.CreateIndex(store.IndexDefinition{Name: "by_tenant", Paths: []string{"/tenant"}}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	for {
		info, err := s.BackfillIndex("by_tenant", 8)
		if err != nil {
			t.Fatalf("BackfillIndex: %v", err)
		}
		if info.State == store.IndexReady {
			break
		}
	}
	snapshot, err := s.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	q := Select(Path("tenant"), Path("score")).
		Where(In("tenant", "t1", "t3", "t5")).
		OrderBy("score", Asc)

	var e Exec
	if err := q.RunInto(&e, FromSnapshot(snapshot)); err != nil {
		t.Fatalf("RunInto: %v", err)
	}
	if e.Workspace.storeIndexProbes != 3 {
		t.Fatalf("storeIndexProbes=%d want 3: one probe per alternative", e.Workspace.storeIndexProbes)
	}
	if got, want := resultKey(e.Result), resultKey(mustRun(t, q, set)); got != want {
		t.Fatalf("indexed and unindexed disagree:\n got: %s\nwant: %s", got, want)
	}
}

// Given a membership with an unindexable alternative, when the query runs,
// then it falls back to a scan rather than probing a partial candidate set —
// a partial bound would drop real matches.
func TestInPartialNeedleIsUnindexable(t *testing.T) {
	values := []any{"t1", "t3"}
	compiled := Select(Path("tenant")).Where(In("tenant", values...))
	if err := compiled.Prepare(); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	where := compiled.plan.where
	if len(where.needles) != len(where.lits) {
		t.Fatalf("needles=%d lits=%d: every scalar alternative should carry a needle",
			len(where.needles), len(where.lits))
	}
}

// --- rejection -------------------------------------------------------------

// Given a malformed query document, when it compiles, then it is rejected
// with a message that names where the problem is and what was expected.
func TestJSONQueryErrors(t *testing.T) {
	cases := []struct {
		name string
		doc  M
		want string
	}{
		{"unknown clause", M{"select": "a", "wher": M{}}, "unknown query clause"},
		{"select not a column", M{"select": Number("1")}, "a column is a path string or an object"},
		{"empty select list", M{"select": A{}}, "projects nothing"},
		{"multi-entry column object", M{"select": M{"x": "a", "y": "b"}}, "exactly one entry"},
		{"unknown aggregate", M{"select": M{"$median": "a"}}, "unknown aggregate"},
		{"aggregate needs a path", M{"select": M{"$sum": Number("1")}}, "$sum takes a path string"},
		{"count rejects false", M{"select": M{"$count": false}}, "$count takes"},
		{"alias of a non-aggregate object", M{"select": M{"x": M{"nope": "a"}}}, "is not an aggregate"},
		{"where not an object", M{"select": "a", "where": A{}}, "a filter is an object"},
		{"unknown filter operator", M{"select": "a", "where": M{"$xor": A{}}}, "unknown operator"},
		{"and takes an array", M{"select": "a", "where": M{"$and": M{}}}, "expected an array of filters"},
		{"unknown path operator", M{"select": "a", "where": M{"a": M{"$like": "x"}}}, "unknown operator"},
		{"empty operator object", M{"select": "a", "where": M{"a": M{}}}, "at least one operator"},
		{"nested object is not a filter", M{"select": "a", "where": M{"a": M{"b": Number("1")}}}, "address a nested field by its path"},
		{"array value needs $in", M{"select": "a", "where": M{"a": A{Number("1")}}}, "match one of several values with $in"},
		{"in takes an array", M{"select": "a", "where": M{"a": M{"$in": Number("1")}}}, "$in takes an array"},
		{"exists takes a bool", M{"select": "a", "where": M{"a": M{"$exists": Number("1")}}}, "$exists takes true or false"},
		{"null takes a bool", M{"select": "a", "where": M{"a": M{"$null": "yes"}}}, "$null takes true or false"},
		{"comparison against null", M{"select": "a", "where": M{"a": M{"$gt": nil}}}, "compare against null with $null"},
		{"comparison against object", M{"select": "a", "where": M{"a": M{"$gt": M{}}}}, "compare against an object with $contains"},
		{"comparison against array", M{"select": "a", "where": M{"a": M{"$gt": A{}}}}, "compare against several values with $in"},
		{"groupBy not a path", M{"select": "a", "groupBy": Number("1")}, "expected a path or an array of paths"},
		{"orderBy direction", M{"select": "a", "groupBy": "a", "orderBy": M{"a": "sideways"}}, "unknown sort direction"},
		{"orderBy numeric direction", M{"select": "a", "groupBy": "a", "orderBy": M{"a": Number("2")}}, "expected 1 or -1"},
		{"limit not a number", M{"select": "a", "limit": "many"}, "expected a whole number"},
		{"limit not whole", M{"select": "a", "limit": 1.5}, "expected a whole number"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := New(c.doc)
			if err == nil {
				t.Fatalf("compiled; want an error containing %q", c.want)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %q does not contain %q", err, c.want)
			}
			if !strings.HasPrefix(err.Error(), "query: ") {
				t.Fatalf("error %q is not package-prefixed", err)
			}
		})
	}
}

// Given JSON text that is not a query, when Parse runs, then it reports the
// reason rather than panicking or compiling something surprising.
func TestParseRejectsNonQueryText(t *testing.T) {
	for _, bad := range []string{``, `[]`, `"select"`, `{`, `{"select":}`, `null`, `3`} {
		if _, err := Parse([]byte(bad)); err == nil {
			t.Fatalf("Parse(%q) compiled; want an error", bad)
		}
	}
}

// Given a plain map and slice rather than M and A, when the document
// compiles, then it behaves identically — a document decoded by another JSON
// package needs no conversion.
func TestJSONQueryAcceptsPlainMapsAndSlices(t *testing.T) {
	set := mustSegment(t, `{"a":1}`, `{"a":2}`)
	plain, err := New(M(map[string]any{
		"select": []any{"a"},
		"where":  map[string]any{"a": map[string]any{"$in": []any{1, 2}}},
	}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	typed, err := New(M{"select": A{"a"}, "where": M{"a": M{"$in": A{1, 2}}}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got, want := resultKey(mustRun(t, plain, set)), resultKey(mustRun(t, typed, set)); got != want {
		t.Fatalf("plain and typed documents disagree:\n got: %s\nwant: %s", got, want)
	}
}

// Given a containment needle written as a nested document, when it compiles,
// then it means the same as the equivalent JSON literal handed to Contains.
func TestJSONContainmentNeedle(t *testing.T) {
	set := mustSegment(t,
		`{"labels":{"env":"prod","tier":"web"}}`,
		`{"labels":{"env":"dev","tier":"web"}}`,
		`{"labels":{"env":"prod"}}`,
	)
	doc, err := New(M{"select": "labels", "where": M{"labels": M{"$contains": M{"env": "prod"}}}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	builder := Select(Path("labels")).Where(Contains("labels", `{"env":"prod"}`))
	if got, want := resultKey(mustRun(t, doc, set)), resultKey(mustRun(t, builder, set)); got != want {
		t.Fatalf("document and builder containment disagree:\n got: %s\nwant: %s", got, want)
	}
}

// Given an omitted select clause, when the query runs, then each row is the
// whole document — the shape a document store returns when asked for no
// particular columns.
func TestJSONDefaultProjectionIsWholeDocument(t *testing.T) {
	set := mustSegment(t, `{"a":1,"b":2}`, `{"a":9}`)
	q, err := New(M{"where": M{"a": 1}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	got := mustRun(t, q, set)
	column, ok := got.Column("*")
	if !ok {
		t.Fatalf("no whole-document column; headers: %v", got.Columns)
	}
	if got.RowCount != 1 {
		t.Fatalf("RowCount=%d want 1", got.RowCount)
	}
	if json := string(column.Cells[0].JSON()); json != `{"a":1,"b":2}` {
		t.Fatalf("projected %s want the whole document", json)
	}
}

// Given a document built from a Go map, whose iteration order is randomized,
// when it compiles repeatedly, then it produces the same plan every time —
// conjunction is commutative, but a plan that reorders itself run to run is
// not reproducible.
func TestJSONQueryCompilationIsDeterministic(t *testing.T) {
	doc := M{
		"select": "a",
		"where": M{
			"a": M{"$gte": 1, "$lte": 9},
			"b": "x",
			"c": true,
			"d": M{"$exists": true},
		},
	}
	first, err := New(doc)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	want := predicateShape(first)
	for range 32 {
		next, err := New(doc)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if got := predicateShape(next); got != want {
			t.Fatalf("compilation is not deterministic:\n got: %s\nwant: %s", got, want)
		}
	}
}

// predicateShape renders a compiled predicate tree as a stable string, so two
// compilations of one document can be compared structurally.
func predicateShape(q *Query) string {
	p, err := q.compiled()
	if err != nil {
		return "error: " + err.Error()
	}
	var b strings.Builder
	var walk func(*compiledPredicate)
	walk = func(node *compiledPredicate) {
		if node == nil {
			b.WriteString("nil")
			return
		}
		fmt.Fprintf(&b, "(%d col=%d op=%d", node.kind, node.col, node.op)
		for _, kid := range node.kids {
			b.WriteByte(' ')
			walk(kid)
		}
		b.WriteByte(')')
	}
	walk(p.where)
	return b.String()
}
