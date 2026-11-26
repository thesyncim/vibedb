package driver_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/thesyncim/vibedb/sql/driver"
)

func TestDistinctOnEndToEnd(t *testing.T) {
	db, err := sql.Open("vibedb", filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE msgs (id INTEGER PRIMARY KEY, channel TEXT NOT NULL, ts INTEGER NOT NULL)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, stmt := range []string{
		`INSERT INTO msgs (id, channel, ts) VALUES (1, 'general', 100)`,
		`INSERT INTO msgs (id, channel, ts) VALUES (2, 'general', 200)`,
		`INSERT INTO msgs (id, channel, ts) VALUES (3, 'random', 150)`,
		`INSERT INTO msgs (id, channel, ts) VALUES (4, 'random', 50)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	rows, err := db.Query(`SELECT DISTINCT ON (channel) channel, id, ts FROM msgs ORDER BY channel ASC, ts DESC`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	type row struct {
		ch string
		id int64
		ts int64
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.ch, &r.id, &r.ts); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if len(got) != 2 || got[0] != (row{"general", 2, 200}) || got[1] != (row{"random", 3, 150}) {
		t.Fatalf("got %+v, want [{general 2 200} {random 3 150}]", got)
	}
}
