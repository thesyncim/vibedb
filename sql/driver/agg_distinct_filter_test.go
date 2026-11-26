package driver_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/thesyncim/vibedb/sql/driver"
)

func TestCountSumAvgDistinctAndAggregateFilter(t *testing.T) {
	db, err := sql.Open("vibedb", filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE events (id INTEGER PRIMARY KEY, team TEXT NOT NULL, score INTEGER NULL, active BOOL NOT NULL)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, stmt := range []string{
		`INSERT INTO events (id, team, score, active) VALUES (1, 'a', 10, TRUE)`,
		`INSERT INTO events (id, team, score, active) VALUES (2, 'a', 10, TRUE)`,
		`INSERT INTO events (id, team, score, active) VALUES (3, 'a', 20, FALSE)`,
		`INSERT INTO events (id, team, score, active) VALUES (4, 'b', 30, TRUE)`,
		`INSERT INTO events (id, team, score, active) VALUES (5, 'b', NULL, FALSE)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("insert %q: %v", stmt, err)
		}
	}

	var cd, sd, activeCnt, highActive int64
	err = db.QueryRow(`
		SELECT COUNT(DISTINCT score),
		       SUM(DISTINCT score),
		       COUNT(*) FILTER (WHERE active = TRUE),
		       COUNT(DISTINCT score) FILTER (WHERE active = TRUE)
		FROM events`).Scan(&cd, &sd, &activeCnt, &highActive)
	if err != nil {
		t.Fatalf("ungrouped query: %v", err)
	}
	if cd != 3 || sd != 60 || activeCnt != 3 || highActive != 2 {
		t.Fatalf("ungrouped got (%d, %d, %d, %d), want (3, 60, 3, 2)", cd, sd, activeCnt, highActive)
	}
}
