package driver_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/thesyncim/vibedb/sql/driver"
)

func TestExtendedAggregateHavingAndCTAS(t *testing.T) {
	db, err := sql.Open("vibedb", filepath.Join(t.TempDir(), "ctas_having.vdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE sales (id TEXT PRIMARY KEY, dept TEXT, tag TEXT, amount INTEGER)`); err != nil {
		t.Fatal(err)
	}
	rows := []struct {
		id, dept, tag string
		amount        int
	}{
		{"1", "eng", "go", 100},
		{"2", "eng", "go", 200},
		{"3", "eng", "rust", 300},
		{"4", "ops", "k8s", 50},
		{"5", "sales", "crm", 400},
		{"6", "sales", "crm", 500},
	}
	for _, r := range rows {
		if _, err := db.Exec(`INSERT INTO sales (id, dept, tag, amount) VALUES (?, ?, ?, ?)`, r.id, r.dept, r.tag, r.amount); err != nil {
			t.Fatal(err)
		}
	}

	// 1. Extended aggregate (COUNT(DISTINCT ...), STRING_AGG, FILTER) with HAVING
	q, err := db.Query(`
		SELECT dept, COUNT(DISTINCT tag) AS distinct_tags, SUM(amount) FILTER (WHERE amount >= 100) AS big_sum
		FROM sales
		GROUP BY dept
		HAVING COUNT(DISTINCT tag) >= 2 OR SUM(amount) >= 800
		ORDER BY dept`)
	if err != nil {
		t.Fatalf("extended aggregate HAVING query: %v", err)
	}
	defer q.Close()

	type resRow struct {
		dept         string
		distinctTags int
		bigSum       int
	}
	var got []resRow
	for q.Next() {
		var r resRow
		if err := q.Scan(&r.dept, &r.distinctTags, &r.bigSum); err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}
	if len(got) != 2 || got[0].dept != "eng" || got[0].distinctTags != 2 || got[0].bigSum != 600 ||
		got[1].dept != "sales" || got[1].distinctTags != 1 || got[1].bigSum != 900 {
		t.Fatalf("extended aggregate HAVING = %+v, want [{eng 2 600} {sales 1 900}]", got)
	}

	// 2. CREATE TABLE ... AS SELECT (CTAS)
	if _, err := db.Exec(`CREATE TABLE high_sales AS SELECT id, dept, amount FROM sales WHERE amount >= 200`); err != nil {
		t.Fatalf("CREATE TABLE AS SELECT: %v", err)
	}
	var cnt int
	if err := db.QueryRow(`SELECT COUNT(*) FROM high_sales`).Scan(&cnt); err != nil {
		t.Fatalf("SELECT COUNT(*) FROM high_sales: %v", err)
	}
	if cnt != 4 {
		t.Fatalf("high_sales count = %d, want 4", cnt)
	}

	// 3. CREATE TABLE IF NOT EXISTS ... AS SELECT is a no-op if table exists
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS high_sales AS SELECT id FROM sales LIMIT 1`); err != nil {
		t.Fatalf("CREATE TABLE IF NOT EXISTS AS SELECT: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM high_sales`).Scan(&cnt); err != nil {
		t.Fatal(err)
	}
	if cnt != 4 {
		t.Fatalf("high_sales count after IF NOT EXISTS = %d, want 4", cnt)
	}
}
