package driver_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/thesyncim/vibedb/sql/driver"
)

func TestPGFetchFirstOffsetRowsIsUnknownAndLikeEscape(t *testing.T) {
	db, err := sql.Open("vibedb", filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, code STRING NOT NULL, flag BOOLEAN NULL)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, stmt := range []string{
		`INSERT INTO docs (id, code, flag) VALUES (1, 'a_1', TRUE)`,
		`INSERT INTO docs (id, code, flag) VALUES (2, 'ax1', FALSE)`,
		`INSERT INTO docs (id, code, flag) VALUES (3, 'a_2', NULL)`,
		`INSERT INTO docs (id, code, flag) VALUES (4, 'b_1', NULL)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("insert %q: %v", stmt, err)
		}
	}

	// OFFSET ... ROWS FETCH FIRST ... ROWS ONLY
	rows, err := db.Query(`SELECT id FROM docs ORDER BY id OFFSET 1 ROW FETCH NEXT 2 ROWS ONLY`)
	if err != nil {
		t.Fatalf("FETCH NEXT: %v", err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if len(ids) != 2 || ids[0] != 2 || ids[1] != 3 {
		t.Fatalf("FETCH NEXT ids = %v, want [2 3]", ids)
	}

	// LIKE ... ESCAPE '!'
	var matchCount int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs WHERE code LIKE 'a!_%' ESCAPE '!'`).Scan(&matchCount); err != nil {
		t.Fatalf("LIKE ESCAPE: %v", err)
	}
	if matchCount != 2 {
		t.Fatalf("LIKE ESCAPE count = %d, want 2", matchCount)
	}

	// IS UNKNOWN / IS NOT UNKNOWN
	var unkCount int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs WHERE flag IS UNKNOWN`).Scan(&unkCount); err != nil {
		t.Fatalf("IS UNKNOWN: %v", err)
	}
	if unkCount != 2 {
		t.Fatalf("IS UNKNOWN count = %d, want 2", unkCount)
	}

	// LIMIT ALL
	var allCount int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM (SELECT id FROM docs LIMIT ALL) d`).Scan(&allCount); err != nil {
		t.Fatalf("LIMIT ALL: %v", err)
	}
	if allCount != 4 {
		t.Fatalf("LIMIT ALL count = %d, want 4", allCount)
	}
}
