package driver_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/thesyncim/vibedb/sql/driver"
)

func TestGroupByOrdinalsAliasesComputedAndExtAggs(t *testing.T) {
	db, err := sql.Open("vibedb", filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE items (id TEXT PRIMARY KEY, grp TEXT, name TEXT, n INT, ok BOOL)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	for _, stmt := range []string{
		`INSERT INTO items (id, grp, name, n, ok) VALUES ('1', 'a', 'x', 10, true)`,
		`INSERT INTO items (id, grp, name, n, ok) VALUES ('2', 'a', 'y', 20, true)`,
		`INSERT INTO items (id, grp, name, n, ok) VALUES ('3', 'a', 'x', NULL, false)`,
		`INSERT INTO items (id, grp, name, n, ok) VALUES ('4', 'b', 'z', 10, true)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("INSERT %s: %v", stmt, err)
		}
	}

	// 1. GROUP BY ordinal (1)
	rows, err := db.Query(`SELECT grp, COUNT(*) AS c FROM items GROUP BY 1 ORDER BY grp`)
	if err != nil {
		t.Fatalf("GROUP BY 1: %v", err)
	}
	var gotGrps []string
	var gotCounts []int64
	for rows.Next() {
		var g string
		var c int64
		if err := rows.Scan(&g, &c); err != nil {
			t.Fatalf("scan: %v", err)
		}
		gotGrps = append(gotGrps, g)
		gotCounts = append(gotCounts, c)
	}
	rows.Close()
	if len(gotGrps) != 2 || gotGrps[0] != "a" || gotCounts[0] != 3 || gotGrps[1] != "b" || gotCounts[1] != 1 {
		t.Fatalf("GROUP BY 1 got (%v, %v), want ([a b], [3 1])", gotGrps, gotCounts)
	}

	// 2. Computed GROUP BY expression (COALESCE(n, 0)) + GROUP BY alias
	rows, err = db.Query(`SELECT COALESCE(n, 0) AS bucket, COUNT(*) FROM items GROUP BY COALESCE(n, 0) ORDER BY 1`)
	if err != nil {
		t.Fatalf("computed GROUP BY: %v", err)
	}
	var buckets []int64
	var bCounts []int64
	for rows.Next() {
		var b, c int64
		if err := rows.Scan(&b, &c); err != nil {
			t.Fatalf("scan: %v", err)
		}
		buckets = append(buckets, b)
		bCounts = append(bCounts, c)
	}
	rows.Close()
	if len(buckets) != 3 || buckets[0] != 0 || bCounts[0] != 1 || buckets[1] != 10 || bCounts[1] != 2 || buckets[2] != 20 || bCounts[2] != 1 {
		t.Fatalf("computed GROUP BY got (%v, %v), want ([0 10 20], [1 2 1])", buckets, bCounts)
	}

	rows, err = db.Query(`SELECT COALESCE(n, 0) AS bucket, COUNT(*) FROM items GROUP BY bucket ORDER BY bucket`)
	if err != nil {
		t.Fatalf("GROUP BY alias: %v", err)
	}
	buckets = buckets[:0]
	for rows.Next() {
		var b, c int64
		if err := rows.Scan(&b, &c); err != nil {
			t.Fatalf("scan alias: %v", err)
		}
		buckets = append(buckets, b)
	}
	rows.Close()
	if len(buckets) != 3 {
		t.Fatalf("GROUP BY alias got %v, want 3 buckets", buckets)
	}

	// 3. STRING_AGG, ARRAY_AGG, JSON_AGG, BOOL_AND, BOOL_OR + DISTINCT + FILTER
	rows, err = db.Query(`
		SELECT grp,
		       STRING_AGG(DISTINCT name, ','),
		       ARRAY_AGG(DISTINCT name),
		       JSON_AGG(n) FILTER (WHERE n IS NOT NULL),
		       BOOL_AND(ok),
		       BOOL_OR(ok)
		FROM items
		GROUP BY grp
		ORDER BY grp`)
	if err != nil {
		t.Fatalf("extended aggregates: %v", err)
	}
	defer rows.Close()
	type aggRow struct {
		grp     string
		strAgg  string
		arrAgg  string
		jsonAgg string
		bAnd    bool
		bOr     bool
	}
	var res []aggRow
	for rows.Next() {
		var r aggRow
		if err := rows.Scan(&r.grp, &r.strAgg, &r.arrAgg, &r.jsonAgg, &r.bAnd, &r.bOr); err != nil {
			t.Fatalf("scan ext agg: %v", err)
		}
		res = append(res, r)
	}
	if len(res) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(res))
	}
	if res[0].grp != "a" || res[0].bAnd != false || res[0].bOr != true {
		t.Fatalf("group a unexpected bools: %+v", res[0])
	}
	if res[0].strAgg != "x,y" && res[0].strAgg != "y,x" {
		t.Fatalf("group a STRING_AGG(DISTINCT) got %q, want x,y", res[0].strAgg)
	}
	if res[1].grp != "b" || res[1].strAgg != "z" || res[1].bAnd != true || res[1].bOr != true {
		t.Fatalf("group b unexpected: %+v", res[1])
	}
}
