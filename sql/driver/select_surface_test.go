package driver

import (
	"strings"
	"testing"

	"github.com/thesyncim/vibejson"
)

func TestExplainReturnsCompiledPlanWithoutOpeningTheTable(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	var plan string
	if err := db.QueryRow(
		`EXPLAIN SELECT id FROM docs WHERE id = ?`, "x",
	).Scan(&plan); err != nil {
		t.Fatal(err)
	}
	if !vibejson.Valid([]byte(plan)) {
		t.Fatalf("EXPLAIN returned invalid JSON: %s", plan)
	}
	for _, want := range []string{
		`"node":"scan"`,
		`"collection":"docs"`,
		`"access_path":"adaptive-posting-or-scan"`,
	} {
		if !strings.Contains(plan, want) {
			t.Errorf("EXPLAIN missing %s: %s", want, plan)
		}
	}
}

func TestSelectDelegatesFullSQLQuerySurface(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	for _, document := range []string{
		`{"id":"a","kind":"x","n":1}`,
		`{"id":"b","kind":"y","n":2}`,
		`{"id":"c","kind":"x","n":3}`,
		`{"id":"d","kind":"z","n":4}`,
	} {
		if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, document); err != nil {
			t.Fatal(err)
		}
	}

	rows, err := db.Query(`
		SELECT id
		FROM docs
		WHERE n >= ? AND kind <> ?
		ORDER BY n DESC
		LIMIT 2 OFFSET 1`, int64(1), "z")
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != "b" || ids[1] != "a" {
		t.Fatalf("full-scan ordered page = %v, want [b a]", ids)
	}

	var (
		kind                      string
		count, sum, avg, min, max int64
	)
	if err := db.QueryRow(`
		SELECT kind, COUNT(*), SUM(n), AVG(n), MIN(n), MAX(n)
		FROM docs
		GROUP BY kind
		HAVING SUM(n) > 3
		ORDER BY kind
		LIMIT 1`).Scan(&kind, &count, &sum, &avg, &min, &max); err != nil {
		t.Fatal(err)
	}
	if kind != "x" || count != 2 || sum != 4 || avg != 2 || min != 1 || max != 3 {
		t.Fatalf(
			"aggregate row = (%q,%d,%d,%d,%d,%d), want (x,2,4,2,1,3)",
			kind, count, sum, avg, min, max)
	}
}

func TestDeleteDelegatesGeneralPredicateToDMLExecutor(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	for _, document := range []string{
		`{"id":"a","kind":"x","n":1}`,
		`{"id":"b","kind":"y","n":2}`,
		`{"id":"c","kind":"x","n":3}`,
	} {
		if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, document); err != nil {
			t.Fatal(err)
		}
	}

	result, err := db.Exec(`DELETE FROM docs WHERE n >= 3 OR kind = 'y'`)
	if err != nil {
		t.Fatal(err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if affected != 2 {
		t.Fatalf("RowsAffected = %d, want 2", affected)
	}
	var count int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("remaining rows = %d, want 1", count)
	}
}
