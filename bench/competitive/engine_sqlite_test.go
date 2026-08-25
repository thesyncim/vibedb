package competitive

import "testing"

func TestSQLiteExactIndexCountIsPhysical(t *testing.T) {
	for _, count := range []uint8{0, 1, 3} {
		engine, err := newSQLite(Config{Dir: t.TempDir(), ExactIndexes: count})
		if err != nil {
			t.Fatal(err)
		}
		sqlite := engine.(*sqliteEngine)
		var physical int
		if err := sqlite.db.QueryRow(
			`SELECT count(*) FROM sqlite_master WHERE type = 'index' AND name LIKE 'idx_%'`,
		).Scan(&physical); err != nil {
			t.Fatal(err)
		}
		if physical != int(count) {
			t.Fatalf("exact-indexes=%d created %d SQLite indexes", count, physical)
		}
		if err := engine.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
