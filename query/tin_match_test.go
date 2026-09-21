package query

import (
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

// tinMatchDocs is the shared ==> corpus. Expectations below are hand-derived
// from TINQL semantics, not from the implementation: exact-term matching
// (luxuriously is not luxury), boolean AND, adjacency for phrases, prefix
// expansion through the index dictionary, and non-string/missing/empty rows
// matching nothing.
var tinMatchDocs = []struct {
	key  string
	body string
}{
	{"d001", `{"id":"d001","body":"luxury goods for sale"}`},
	{"d002", `{"id":"d002","body":"cheap goods everyday"}`},
	{"d003", `{"id":"d003","body":"luxury watches and clocks"}`},
	{"d004", `{"id":"d004","body":42}`},
	{"d005", `{"id":"d005","price":9}`},
	{"d006", `{"id":"d006","body":""}`},
	{"d007", `{"id":"d007","body":"luxury luxury goods goods"}`},
	{"d008", `{"id":"d008","body":"a luxuriously appointed suite"}`},
}

func tinMatchDatabase(t testing.TB, indexed bool) *store.Database {
	t.Helper()
	db := &store.Database{}
	coll, err := db.CreateCollection("docs", store.Options{ChunkDocuments: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, doc := range tinMatchDocs {
		if _, err := coll.Put(doc.key, []byte(doc.body)); err != nil {
			t.Fatal(err)
		}
	}
	if indexed {
		if _, err := coll.CreateIndex(store.IndexDefinition{
			Name: "body_tin", Paths: []string{"/body"}, Kind: store.IndexTin,
		}); err != nil {
			t.Fatal(err)
		}
		if info, err := coll.BackfillIndex("body_tin", 0); err != nil ||
			info.State != store.IndexReady {
			t.Fatalf("BackfillIndex = (%+v, %v)", info, err)
		}
	}
	return db
}

func tinMatchStatementIDs(
	t testing.TB, statement *Statement, source Source, exec *Exec, args ...any,
) []string {
	t.Helper()
	cursor, err := statement.RunInto(exec, source, args)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, exec.Result.RowCount)
	for cursor.Next() {
		id, ok := cursor.Cell(0).Text()
		if !ok {
			t.Fatalf("id cell = %s, want string", cursor.Cell(0).JSON())
		}
		ids = append(ids, id)
	}
	return ids
}

func TestSQLMatchFullText(t *testing.T) {
	db := tinMatchDatabase(t, true)
	catalog := db.Snapshot()
	for _, tc := range []struct {
		name  string
		tinql string
		want  []string
	}{
		{name: "term", tinql: "luxury", want: []string{"d001", "d003", "d007"}},
		{name: "and", tinql: "luxury AND goods", want: []string{"d001", "d007"}},
		{name: "phrase", tinql: `"luxury goods"`, want: []string{"d001", "d007"}},
		{name: "prefix", tinql: "lux*", want: []string{"d001", "d003", "d007", "d008"}},
		{name: "nohit", tinql: "yachts", want: nil},
		{name: "empty", tinql: "", want: nil},
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
		})
	}
}

func TestSQLMatchNotAndPlaceholder(t *testing.T) {
	db := tinMatchDatabase(t, true)
	catalog := db.Snapshot()
	statement, err := PrepareStatement(
		`SELECT o.id FROM docs AS o WHERE NOT o.body ==> 'luxury' ORDER BY o.id`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	exec := Exec{}
	defer exec.Release()
	got := tinMatchStatementIDs(t, statement, FromDatabase(catalog, "docs"), &exec)
	// The isString guard survives negation exactly like LIKE: only string
	// rows can satisfy or fail the match, so non-string and absent rows drop.
	want := []string{"d002", "d006", "d008"}
	if !slices.Equal(got, want) {
		t.Fatalf("NOT ids=%v want=%v", got, want)
	}

	param, err := PrepareStatement(
		`SELECT o.id FROM docs AS o WHERE o.body ==> ? ORDER BY o.id`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer param.Release()
	got = tinMatchStatementIDs(
		t, param, FromDatabase(catalog, "docs"), &exec, "luxury AND goods",
	)
	if want := []string{"d001", "d007"}; !slices.Equal(got, want) {
		t.Fatalf("placeholder ids=%v want=%v", got, want)
	}
}

func TestSQLMatchErrors(t *testing.T) {
	indexed := tinMatchDatabase(t, true)
	catalog := indexed.Snapshot()
	run := func(t *testing.T, src string, args ...any) error {
		t.Helper()
		statement, err := PrepareStatement(src)
		if err != nil {
			return err
		}
		defer statement.Release()
		exec := Exec{}
		defer exec.Release()
		_, err = statement.RunInto(&exec, FromDatabase(catalog, "docs"), args)
		return err
	}
	// A lone comma is bare punctuation without syntax: TIN's empty-input
	// rule, matching nothing rather than erroring.
	if err := run(t, `SELECT o.id FROM docs AS o WHERE o.body ==> '"unclosed'`); err == nil ||
		!strings.Contains(err.Error(), "invalid TINQL") {
		t.Fatalf("bad TINQL error = %v, want invalid TINQL", err)
	}
	if err := run(t, `SELECT o.id FROM docs AS o WHERE o.price ==> 'luxury'`); err == nil ||
		!strings.Contains(err.Error(), "requires a tin index") {
		t.Fatalf("wrong-path error = %v, want requires a tin index", err)
	}

	plain := tinMatchDatabase(t, false)
	uncataloged := plain.Snapshot()
	statement, err := PrepareStatement(`SELECT o.id FROM docs AS o WHERE o.body ==> 'luxury'`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	exec := Exec{}
	defer exec.Release()
	if _, err := statement.RunInto(&exec, FromDatabase(uncataloged, "docs"), nil); err == nil ||
		!strings.Contains(err.Error(), "requires a tin index") {
		t.Fatalf("unindexed error = %v, want requires a tin index", err)
	}

	// JOIN..ON with ==> stays a positioned error: the relational pair lane
	// evaluates ON per pair without an index catalog to parse against.
	joinStmt, err := PrepareStatement(
		`SELECT o.id FROM docs AS o JOIN notes AS n ON n.doc = o.id AND n.body ==> 'luxury'`,
	)
	if err != nil {
		if !strings.Contains(err.Error(), "unsupported ON expression") {
			t.Fatalf("join-on prepare error = %v, want unsupported ON expression", err)
		}
		return
	}
	defer joinStmt.Release()
	// The notes collection only exists in the notes database; reaching
	// execution here would be the surprise, and it must still refuse.
	db := tinMatchNotesDatabase(t)
	exec2 := Exec{}
	defer exec2.Release()
	if _, err := joinStmt.RunInto(&exec2, FromDatabase(db.Snapshot(), "docs"), nil); err == nil ||
		!strings.Contains(err.Error(), "unsupported ON expression") {
		t.Fatalf("join-on error = %v, want unsupported ON expression", err)
	}
}

func TestSQLMatchPreparedReuseSeesNewSnapshot(t *testing.T) {
	db := tinMatchDatabase(t, true)
	statement, err := PrepareStatement(
		`SELECT o.id FROM docs AS o WHERE o.body ==> 'luxury' ORDER BY o.id`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	exec := Exec{}
	defer exec.Release()
	first := tinMatchStatementIDs(t, statement, FromDatabase(db.Snapshot(), "docs"), &exec)
	if want := []string{"d001", "d003", "d007"}; !slices.Equal(first, want) {
		t.Fatalf("first ids=%v want=%v", first, want)
	}
	coll, ok := db.Collection("docs")
	if !ok {
		t.Fatal("docs collection missing")
	}
	if _, err := coll.Put("d009", []byte(`{"id":"d009","body":"luxury yachts"}`)); err != nil {
		t.Fatal(err)
	}
	second := tinMatchStatementIDs(t, statement, FromDatabase(db.Snapshot(), "docs"), &exec)
	if want := []string{"d001", "d003", "d007", "d009"}; !slices.Equal(second, want) {
		t.Fatalf("second ids=%v want=%v", second, want)
	}
}

func TestMatchBuilderAndSources(t *testing.T) {
	db := tinMatchDatabase(t, true)
	catalog := db.Snapshot()
	q := Select(Path("id")).Where(Match("body", "luxury AND goods")).OrderBy("id", Asc)
	exec := Exec{}
	defer exec.Release()
	if err := q.RunInto(&exec, FromDatabase(catalog, "docs")); err != nil {
		t.Fatal(err)
	}
	col, ok := exec.Result.Column("id")
	if !ok {
		t.Fatal("no id column")
	}
	var got []string
	for _, cell := range col.Cells {
		id, ok := cell.Text()
		if !ok {
			t.Fatalf("id cell = %s, want string", cell.JSON())
		}
		got = append(got, id)
	}
	if want := []string{"d001", "d007"}; !slices.Equal(got, want) {
		t.Fatalf("builder ids=%v want=%v", got, want)
	}

	// A bare segment carries no index catalog; durable will grow one later.
	// Both reject ==> loudly instead of answering an unbound non-match.
	raw := make([][]byte, 0, len(tinMatchDocs))
	for _, doc := range tinMatchDocs {
		raw = append(raw, []byte(doc.body))
	}
	segment := buildSegment(t, raw, storageModes[0])
	if err := q.RunInto(&exec, FromSegment(segment)); err == nil ||
		!strings.Contains(err.Error(), "==>") {
		t.Fatalf("segment error = %v, want ==> rejection", err)
	}
	if err := q.RunInto(&exec, FromFile(durableScanCorpus(t, 32))); err == nil ||
		!strings.Contains(err.Error(), "==>") {
		t.Fatalf("durable error = %v, want ==> rejection", err)
	}
}

func tinMatchNotesDatabase(t testing.TB) *store.Database {
	t.Helper()
	db := tinMatchDatabase(t, true)
	notes, err := db.CreateCollection("notes", store.Options{ChunkDocuments: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, doc := range []struct{ key, body string }{
		{"n1", `{"id":"n1","doc":"d001","body":"luxury suite"}`},
		{"n2", `{"id":"n2","doc":"d002","body":"plain room"}`},
	} {
		if _, err := notes.Put(doc.key, []byte(doc.body)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := notes.CreateIndex(store.IndexDefinition{
		Name: "notes_tin", Paths: []string{"/body"}, Kind: store.IndexTin,
	}); err != nil {
		t.Fatal(err)
	}
	if info, err := notes.BackfillIndex("notes_tin", 0); err != nil ||
		info.State != store.IndexReady {
		t.Fatalf("BackfillIndex = (%+v, %v)", info, err)
	}
	return db
}

func TestSQLMatchInsideSubqueryAndJoin(t *testing.T) {
	db := tinMatchNotesDatabase(t)
	catalog := db.Snapshot()
	all := []string{"d001", "d002", "d003", "d004", "d005", "d006", "d007", "d008"}
	for _, tc := range []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "exists",
			sql:  `SELECT o.id FROM docs AS o WHERE EXISTS (SELECT 1 FROM notes AS n WHERE n.body ==> 'luxury') ORDER BY o.id`,
			want: all,
		},
		{
			name: "exists-nohit",
			sql:  `SELECT o.id FROM docs AS o WHERE EXISTS (SELECT 1 FROM notes AS n WHERE n.body ==> 'zzz-nohit') ORDER BY o.id`,
			want: nil,
		},
		{
			name: "exists-correlated",
			sql:  `SELECT o.id FROM docs AS o WHERE EXISTS (SELECT 1 FROM notes AS n WHERE n.doc = o.id AND n.body ==> 'luxury') ORDER BY o.id`,
			want: []string{"d001"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			statement, err := PrepareStatement(tc.sql)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Release()
			exec := Exec{}
			defer exec.Release()
			got := tinMatchStatementIDs(t, statement, FromDatabase(catalog, "docs"), &exec)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("ids=%v want=%v", got, tc.want)
			}
		})
	}
}

func TestSQLMatchExplain(t *testing.T) {
	statement, err := PrepareStatement(`SELECT o.id FROM docs AS o WHERE o.body ==> 'luxury'`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	explained, err := statement.Explain()
	if err != nil {
		t.Fatal(err)
	}
	// The JSON encoding escapes >, so assert on the decoded operator.
	if !strings.Contains(explained, `"kind":"match"`) ||
		!strings.Contains(explained, `"operator":"==\u003e"`) {
		t.Fatalf("EXPLAIN = %s, want match kind with ==> operator", explained)
	}
}
