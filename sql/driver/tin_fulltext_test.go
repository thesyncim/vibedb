package driver

import (
	stdsql "database/sql"
	"slices"
	"testing"
)

func openTinTestDB(t *testing.T, path string) *stdsql.DB {
	t.Helper()
	db, err := stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

// TestTinFullTextEndToEnd drives the whole USING tin lane through the
// database/sql surface: DDL declares the index, DML writes documents, and
// ==> answers from the durable postings — on a catalog-only table, on a
// materialized one, and across a catalog reopen.
func TestTinFullTextEndToEnd(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE fts (id STRING PRIMARY KEY, body STRING)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(`CREATE INDEX fts_body_tin ON fts(body) USING tin`); err != nil {
		t.Fatalf("CREATE INDEX USING tin: %v", err)
	}
	seed := []string{
		`{"id":"one","body":"luxury goods and vintage watches"}`,
		`{"id":"two","body":"cheap goods everyday"}`,
		`{"id":"three","body":"luxury watches"}`,
	}
	for _, doc := range seed {
		if _, err := db.Exec(`INSERT INTO fts VALUES (?)`, doc); err != nil {
			t.Fatalf("INSERT %s: %v", doc, err)
		}
	}
	selectIDs := func(query string, args ...any) []string {
		t.Helper()
		rows, err := db.Query(query, args...)
		if err != nil {
			t.Fatalf("query %q: %v", query, err)
		}
		defer rows.Close()
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				t.Fatalf("scan %q: %v", query, err)
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows %q: %v", query, err)
		}
		return ids
	}
	if got := selectIDs(
		`SELECT id FROM fts WHERE body ==> 'luxury' ORDER BY id`,
	); !slices.Equal(got, []string{"one", "three"}) {
		t.Fatalf("==> luxury = %v, want [one three]", got)
	}
	if got := selectIDs(
		`SELECT id FROM fts WHERE body ==> 'luxury AND watches' ORDER BY id`,
	); !slices.Equal(got, []string{"one", "three"}) {
		t.Fatalf("==> luxury AND watches = %v, want [one three]", got)
	}
	if got := selectIDs(
		`SELECT id FROM fts WHERE body ==> 'vintage' ORDER BY id`,
	); !slices.Equal(got, []string{"one"}) {
		t.Fatalf("==> vintage = %v, want [one]", got)
	}
	if got := selectIDs(
		`SELECT id FROM fts WHERE body ==> 'yachts' ORDER BY id`,
	); len(got) != 0 {
		t.Fatalf("==> yachts = %v, want no rows", got)
	}
	// A parameterized query binds per execution against the same postings.
	if got := selectIDs(
		`SELECT id FROM fts WHERE body ==> ? ORDER BY id`, "cheap",
	); !slices.Equal(got, []string{"two"}) {
		t.Fatalf("==> ? = %v, want [two]", got)
	}
	// Writes after the declaration are searchable: the next generation
	// builds its own postings on first use.
	if _, err := db.Exec(`INSERT INTO fts VALUES (?)`,
		`{"id":"four","body":"luxury yachts"}`); err != nil {
		t.Fatalf("INSERT four: %v", err)
	}
	if got := selectIDs(
		`SELECT id FROM fts WHERE body ==> 'luxury' ORDER BY id`,
	); !slices.Equal(got, []string{"four", "one", "three"}) {
		t.Fatalf("==> luxury after write = %v, want [four one three]", got)
	}
	// Full-text DDL validation: UNIQUE, duplicates, and missing indexes
	// fail loudly instead of answering wrong.
	if _, err := db.Exec(
		`CREATE UNIQUE INDEX fts_body_unique ON fts(body) USING tin`,
	); err == nil {
		t.Fatal("CREATE UNIQUE INDEX USING tin = nil, want refusal")
	}
	if _, err := db.Exec(`CREATE INDEX fts_body_tin ON fts(body) USING tin`); err == nil {
		t.Fatal("duplicate CREATE INDEX USING tin = nil, want ErrIndexExists")
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS fts_body_tin ON fts(body) USING tin`); err != nil {
		t.Fatalf("IF NOT EXISTS over duplicate = %v, want success", err)
	}
	// ==> over a path with no tin index is a statement error naming it.
	if _, err := db.Query(
		`SELECT id FROM fts WHERE missing ==> 'luxury'`,
	); err == nil {
		t.Fatal("==> over unindexed path = nil, want statement error")
	}
	// Declaring tin over an absent path is legal; with no text there it
	// simply matches nothing.
	if _, err := db.Exec(`CREATE INDEX fts_missing_tin ON fts(missing) USING tin`); err != nil {
		t.Fatalf("CREATE INDEX over absent path: %v", err)
	}
	if got := selectIDs(
		`SELECT id FROM fts WHERE missing ==> 'luxury'`,
	); len(got) != 0 {
		t.Fatalf("==> over absent path = %v, want no rows", got)
	}
	// A second declaration over another path answers independently.
	if _, err := db.Exec(`CREATE INDEX fts_id_tin ON fts(id) USING tin`); err != nil {
		t.Fatalf("CREATE INDEX over id path: %v", err)
	}
	if got := selectIDs(
		`SELECT id FROM fts WHERE id ==> 'luxury'`,
	); len(got) != 0 {
		t.Fatalf("==> over id path = %v, want no rows", got)
	}
	if got := selectIDs(
		`SELECT id FROM fts WHERE id ==> 'one' ORDER BY id`,
	); !slices.Equal(got, []string{"one"}) {
		t.Fatalf("==> over id path = %v, want [one]", got)
	}
	// An invalid TINQL query is a statement error naming the path.
	rows, err := db.Query(`SELECT id FROM fts WHERE body ==> '"unclosed'`)
	if err == nil {
		rows.Close()
		t.Fatal("==> with invalid TINQL = nil, want statement error")
	}
	// TRUNCATE rebuilds storage from the SQL mirror: the declaration must
	// ride along or post-truncate queries go dark.
	if _, err := db.Exec(`TRUNCATE fts`); err != nil {
		t.Fatalf("TRUNCATE: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO fts VALUES (?)`,
		`{"id":"five","body":"luxury sedans"}`); err != nil {
		t.Fatalf("INSERT five: %v", err)
	}
	if got := selectIDs(
		`SELECT id FROM fts WHERE body ==> 'luxury' ORDER BY id`,
	); !slices.Equal(got, []string{"five"}) {
		t.Fatalf("==> luxury after TRUNCATE = %v, want [five]", got)
	}
	// DROP INDEX rebuilds without the declaration; ==> over the path is a
	// statement error again, while the table keeps serving.
	if _, err := db.Exec(`DROP INDEX fts_body_tin`); err != nil {
		t.Fatalf("DROP INDEX: %v", err)
	}
	if _, err := db.Query(
		`SELECT id FROM fts WHERE body ==> 'luxury'`,
	); err == nil {
		t.Fatal("==> after DROP INDEX = nil, want statement error")
	}
	if got := selectIDs(`SELECT id FROM fts ORDER BY id`); !slices.Equal(
		got, []string{"five"}) {
		t.Fatalf("table after DROP INDEX = %v, want [five]", got)
	}
}

// TestTinFullTextSurvivesReopen proves a USING tin declaration declared
// through SQL answers ==> after the catalog directory is closed and
// reopened: the declaration round-trips through both the SQL mirror and
// the durable page catalog.
func TestTinFullTextSurvivesReopen(t *testing.T) {
	path := t.TempDir() + "/catalog.vdb"
	first, err := stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Exec(`CREATE TABLE fts (id STRING PRIMARY KEY, body STRING)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := first.Exec(`CREATE INDEX fts_body_tin ON fts(body) USING tin`); err != nil {
		t.Fatalf("CREATE INDEX USING tin: %v", err)
	}
	if _, err := first.Exec(`INSERT INTO fts VALUES (?)`,
		`{"id":"one","body":"luxury goods"}`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	second := openTinTestDB(t, path)
	rows, err := second.Query(`SELECT id FROM fts WHERE body ==> 'luxury' ORDER BY id`)
	if err != nil {
		t.Fatalf("==> after reopen: %v", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(ids, []string{"one"}) {
		t.Fatalf("==> after reopen = %v, want [one]", ids)
	}
}
