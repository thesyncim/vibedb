package driver

import (
	stdsql "database/sql"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func scanPathComparisonIDs(t testing.TB, rows *stdsql.Rows, window bool) []string {
	t.Helper()
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if window {
			var ordinal int64
			if err := rows.Scan(&id, &ordinal); err != nil {
				t.Fatal(err)
			}
		} else if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return ids
}

func TestDatabaseSQLPathComparisonQueryCompositionAndDML(t *testing.T) {
	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE path_docs (id STRING PRIMARY KEY, a NUMBER, b NUMBER)`,
		`INSERT INTO path_docs VALUES ` +
			`('{"id":"equal","a":1,"b":1}'),` +
			`('{"id":"less","a":1,"b":2}'),` +
			`('{"id":"null-left","a":null,"b":1}'),` +
			`('{"id":"null-right","a":1,"b":null}')`,
		`CREATE TABLE path_right (id STRING PRIMARY KEY, owner STRING, b NUMBER)`,
		`INSERT INTO path_right VALUES ` +
			`('{"id":"r-equal","owner":"equal","b":1}'),` +
			`('{"id":"r-less","owner":"less","b":2}'),` +
			`('{"id":"r-null","owner":"null-left","b":1}')`,
		`CREATE TABLE path_copy (id STRING PRIMARY KEY, a NUMBER, b NUMBER)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	queries := []struct {
		name   string
		source string
		window bool
	}{
		{"direct", `SELECT id FROM path_docs WHERE a = b ORDER BY id`, false},
		{"derived", `SELECT d.id FROM (SELECT id, a, b FROM path_docs) d WHERE d.a = d.b ORDER BY d.id`, false},
		{"cte", `WITH d AS (SELECT id, a, b FROM path_docs) SELECT id FROM d WHERE a = b ORDER BY id`, false},
		{"window", `SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM path_docs WHERE a = b ORDER BY id`, true},
		{"searched CASE in WHERE", `SELECT id FROM path_docs ` +
			`WHERE CASE WHEN a = b THEN TRUE ELSE FALSE END = TRUE ORDER BY id`, false},
		{"mixed-source join", `SELECT d.id FROM path_docs d JOIN path_right r ON r.owner = d.id ` +
			`WHERE d.a = r.b ORDER BY d.id`, false},
	}
	for _, test := range queries {
		t.Run(test.name, func(t *testing.T) {
			rows, err := db.Query(test.source)
			if err != nil {
				t.Fatal(err)
			}
			if got := scanPathComparisonIDs(t, rows, test.window); !slices.Equal(got, []string{"equal"}) {
				t.Fatalf("rows = %v, want [equal]", got)
			}
		})
	}
	inserted, err := db.Exec(`INSERT INTO path_copy SELECT * FROM path_docs WHERE a = b`)
	if err != nil {
		t.Fatal(err)
	}
	if affected, _ := inserted.RowsAffected(); affected != 1 {
		t.Fatalf("INSERT SELECT affected %d rows, want 1", affected)
	}

	updated, err := db.Exec(`UPDATE path_docs SET "$doc" = ? WHERE a = b`,
		`{"id":"equal","a":9,"b":9}`)
	if err != nil {
		t.Fatal(err)
	}
	if affected, _ := updated.RowsAffected(); affected != 1 {
		t.Fatalf("UPDATE affected %d rows, want 1", affected)
	}
	deleted, err := db.Exec(`DELETE FROM path_docs WHERE a <> b`)
	if err != nil {
		t.Fatal(err)
	}
	if affected, _ := deleted.RowsAffected(); affected != 1 {
		t.Fatalf("DELETE affected %d rows, want 1", affected)
	}
	rows, err := db.Query(`SELECT id FROM path_docs ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := scanPathComparisonIDs(t, rows, false),
		[]string{"equal", "null-left", "null-right"}; !slices.Equal(got, want) {
		t.Fatalf("remaining rows = %v, want %v", got, want)
	}
}

func TestDatabaseSQLPathComparisonIncompatibleLiveKinds(t *testing.T) {
	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE path_dynamic (id STRING PRIMARY KEY, a ANY, b ANY)`,
		`INSERT INTO path_dynamic VALUES ('{"id":"bad","a":1,"b":"1"}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	source := `SELECT id FROM path_dynamic WHERE a <= b`
	rows, err := db.Query(source)
	if rows != nil {
		_ = rows.Close()
	}
	var undefined *sqlast.UndefinedOperatorError
	if !errors.As(err, &undefined) {
		t.Fatalf("error = %T %v", err, err)
	}
	if undefined.Left != "numeric" || undefined.Operator != "<=" ||
		undefined.Right != "text" || undefined.Pos != strings.Index(source, "<=") {
		t.Fatalf("error = %+v", undefined)
	}

	caseSource := `SELECT CASE WHEN a != b THEN 1 ELSE 0 END FROM path_dynamic`
	rows, err = db.Query(caseSource)
	if rows != nil {
		_ = rows.Close()
	}
	undefined = nil
	if !errors.As(err, &undefined) || undefined.Unpositioned ||
		undefined.Left != "numeric" || undefined.Operator != "<>" ||
		undefined.Right != "text" || undefined.Pos != strings.Index(caseSource, "!=") {
		t.Fatalf("CASE error = %T %+v", err, undefined)
	}
}

func TestDatabaseSQLPathComparisonRejectsDeclaredMismatchWithoutRows(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(
		`CREATE TABLE path_declared (id STRING PRIMARY KEY, a NUMBER, b STRING)`,
	); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{
		`SELECT id FROM path_declared WHERE a = b`,
		`SELECT id FROM path_declared WHERE a = b LIMIT 0`,
		`SELECT CASE WHEN a = b THEN 1 ELSE 0 END FROM path_declared LIMIT 0`,
		`SELECT l.id FROM path_declared l JOIN path_declared r ON l.a = r.b`,
		`DELETE FROM path_declared WHERE a = b`,
	} {
		var err error
		if strings.HasPrefix(source, "SELECT") {
			var rows *stdsql.Rows
			rows, err = db.Query(source)
			if rows != nil {
				_ = rows.Close()
			}
		} else {
			_, err = db.Exec(source)
		}
		var undefined *sqlast.UndefinedOperatorError
		if !errors.As(err, &undefined) || undefined.Left != "numeric" ||
			undefined.Operator != "=" || undefined.Right != "text" ||
			undefined.Pos != strings.Index(source, "=") {
			t.Fatalf("error = %T %+v", err, undefined)
		}
	}
}

func TestDatabaseSQLPathComparisonDMLFailureIsAtomic(t *testing.T) {
	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE path_atomic (id STRING PRIMARY KEY, a ANY, b ANY)`,
		`INSERT INTO path_atomic VALUES ` +
			`('{"id":"a","a":1,"b":1}'),` +
			`('{"id":"b","a":2,"b":"2"}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	assertRows := func() {
		t.Helper()
		rows, err := db.Query(`SELECT id FROM path_atomic ORDER BY id`)
		if err != nil {
			t.Fatal(err)
		}
		if got := scanPathComparisonIDs(t, rows, false); !slices.Equal(got, []string{"a", "b"}) {
			t.Fatalf("rows after failed DML = %v, want [a b]", got)
		}
	}
	update := `UPDATE path_atomic SET "$doc" = ? WHERE a = b`
	_, err := db.Exec(update, `{"id":"updated","a":1,"b":1}`)
	var undefined *sqlast.UndefinedOperatorError
	if !errors.As(err, &undefined) || undefined.Operator != "=" ||
		undefined.Pos != strings.LastIndex(update, "=") {
		t.Fatalf("UPDATE mismatch = %T %+v, want position %d",
			err, undefined, strings.LastIndex(update, "="))
	}
	assertRows()

	deleteSQL := `DELETE FROM path_atomic WHERE a = b`
	_, err = db.Exec(deleteSQL)
	undefined = nil
	if !errors.As(err, &undefined) || undefined.Operator != "=" ||
		undefined.Pos != strings.LastIndex(deleteSQL, "=") {
		t.Fatalf("DELETE mismatch = %T %+v", err, undefined)
	}
	assertRows()
}

func TestDatabaseSQLRelationalPathComparisonErrors(t *testing.T) {
	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE path_rel_left (id STRING PRIMARY KEY, k ANY, keep BOOL)`,
		`INSERT INTO path_rel_left VALUES ('{"id":"left","k":1,"keep":false}')`,
		`CREATE TABLE path_rel_right (id STRING PRIMARY KEY, owner STRING, k ANY, active BOOL)`,
		`INSERT INTO path_rel_right VALUES ('{"id":"right","owner":"left","k":"1","active":false}')`,
		`CREATE TABLE path_rel_json_left (id STRING PRIMARY KEY, k ANY)`,
		`INSERT INTO path_rel_json_left VALUES ('{"id":"left","k":{"a":1}}')`,
		`CREATE TABLE path_rel_json_right (id STRING PRIMARY KEY, k ANY)`,
		`INSERT INTO path_rel_json_right VALUES ('{"id":"right","k":{"a":1}}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	tests := []struct {
		name            string
		source          string
		marker          string
		left, op, right string
		unpositioned    bool
		firstMarker     bool
	}{
		{
			name:   "legacy pure key",
			source: `SELECT l.id FROM path_rel_left l JOIN path_rel_right r ON l.k = r.k`,
			marker: "=", left: "numeric", op: "=", right: "text",
			firstMarker: true,
		},
		{
			name: "legacy inner ON before false WHERE",
			source: `SELECT l.id FROM path_rel_left l JOIN path_rel_right r ` +
				`ON l.k = r.k WHERE l.keep = TRUE`,
			marker: "=", left: "numeric", op: "=", right: "text",
			firstMarker: true,
		},
		{
			name: "legacy left ON before false WHERE",
			source: `SELECT l.id FROM path_rel_left l LEFT JOIN path_rel_right r ` +
				`ON l.k = r.k WHERE l.keep = TRUE`,
			marker: "=", left: "numeric", op: "=", right: "text",
			firstMarker: true,
		},
		{
			name: "ON before joined-side WHERE",
			source: `SELECT l.id FROM path_rel_left l JOIN path_rel_right r ` +
				`ON l.k = r.k WHERE r.active = TRUE`,
			marker: "=", left: "numeric", op: "=", right: "text",
			firstMarker: true,
		},
		{
			name: "legacy inner ON before LIMIT zero",
			source: `SELECT l.id FROM path_rel_left l JOIN path_rel_right r ` +
				`ON l.k = r.k LIMIT 0`,
			marker: "=", left: "numeric", op: "=", right: "text",
		},
		{
			name: "legacy left ON before LIMIT zero",
			source: `SELECT l.id FROM path_rel_left l LEFT JOIN path_rel_right r ` +
				`ON l.k = r.k LIMIT 0`,
			marker: "=", left: "numeric", op: "=", right: "text",
		},
		{
			name: "generalized residual ON",
			source: `SELECT l.id FROM path_rel_left l JOIN path_rel_right r ` +
				`ON l.id = r.owner AND l.k < r.k`,
			marker: "<", left: "numeric", op: "<", right: "text",
		},
		{
			name: "generalized residual before false ON conjunct",
			source: `SELECT l.id FROM path_rel_left l JOIN path_rel_right r ` +
				`ON l.id = r.owner AND l.keep = TRUE AND l.k < r.k`,
			marker: "<", left: "numeric", op: "<", right: "text",
		},
		{
			name:   "USING synthesized",
			source: `SELECT l.id FROM path_rel_left l JOIN path_rel_right r USING (k)`,
			left:   "numeric", op: "=", right: "text", unpositioned: true,
		},
		{
			name: "LATERAL",
			source: `SELECT l.id FROM path_rel_left l CROSS JOIN LATERAL (` +
				`SELECT r.id FROM path_rel_right r WHERE r.k = l.k) d`,
			marker: "=", left: "text", op: "=", right: "numeric",
		},
		{
			name: "LATERAL ON before false conjunct",
			source: `SELECT l.id FROM path_rel_left l LEFT JOIN LATERAL (` +
				`SELECT r.k FROM path_rel_right r WHERE r.owner = l.id` +
				`) d ON FALSE AND l.k < d.k`,
			marker: "<", left: "numeric", op: "<", right: "text",
		},
		{
			name: "LATERAL WHERE before false local conjunct",
			source: `SELECT l.id FROM path_rel_left l CROSS JOIN LATERAL (` +
				`SELECT r.id FROM path_rel_right r ` +
				`WHERE r.active = TRUE AND r.k = l.k) d`,
			marker: "=", left: "text", op: "=", right: "numeric",
		},
		{
			name: "LATERAL WHERE before inner LIMIT zero",
			source: `SELECT l.id FROM path_rel_left l CROSS JOIN LATERAL (` +
				`SELECT r.id FROM path_rel_right r WHERE r.k = l.k LIMIT 0) d`,
			marker: "=", left: "text", op: "=", right: "numeric",
		},
		{
			name: "correlated EXISTS",
			source: `SELECT l.id FROM path_rel_left l WHERE EXISTS (` +
				`SELECT r.id FROM path_rel_right r WHERE r.k = l.k)`,
			marker: "=", left: "text", op: "=", right: "numeric",
		},
		{
			name: "container equality",
			source: `SELECT l.id FROM path_rel_json_left l JOIN path_rel_json_right r ` +
				`ON l.k = r.k`,
			marker: "=", left: "json", op: "=", right: "json",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows, err := db.Query(test.source)
			if rows != nil {
				_ = rows.Close()
			}
			var undefined *sqlast.UndefinedOperatorError
			if !errors.As(err, &undefined) || undefined.Left != test.left ||
				undefined.Operator != test.op || undefined.Right != test.right ||
				undefined.Unpositioned != test.unpositioned {
				t.Fatalf("error = %T %+v", err, undefined)
			}
			wantPos := strings.LastIndex(test.source, test.marker)
			if test.firstMarker {
				wantPos = strings.Index(test.source, test.marker)
			}
			if !test.unpositioned && undefined.Pos != wantPos {
				t.Fatalf("position = %d, want %d", undefined.Pos, wantPos)
			}
		})
	}
}

func TestDatabaseSQLPathComparisonDisablesPhysicalPruning(t *testing.T) {
	path := filepath.Join(t.TempDir(), "path-pruning.vdb")
	db, err := stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, statement := range []string{
		`CREATE TABLE path_pruning (id STRING PRIMARY KEY, a ANY, b ANY, keep BOOL)`,
		`INSERT INTO path_pruning VALUES ` +
			`('{"id":"bad","a":1,"b":"1","keep":false}'),` +
			`('{"id":"safe","a":1,"b":1,"keep":true}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	for _, source := range []string{
		`SELECT id FROM path_pruning WHERE id = 'safe' AND a = b`,
		`SELECT id FROM path_pruning WHERE id >= 'safe' AND a = b`,
		`SELECT id FROM path_pruning WHERE keep = TRUE AND a = b`,
		`SELECT id FROM path_pruning WHERE keep = FALSE OR a = b`,
		`SELECT id FROM path_pruning WHERE a = b LIMIT 0`,
	} {
		t.Run(source, func(t *testing.T) {
			rows, runErr := db.Query(source)
			if rows != nil {
				_ = rows.Close()
			}
			var undefined *sqlast.UndefinedOperatorError
			if !errors.As(runErr, &undefined) || undefined.Left != "numeric" ||
				undefined.Operator != "=" || undefined.Right != "text" ||
				undefined.Pos != strings.LastIndex(source, "=") {
				t.Fatalf("error = %T %+v", runErr, undefined)
			}
		})
	}
}

func TestDatabaseSQLDurableViewPathComparisonErrorHasNoOuterPosition(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	db, err := stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`CREATE TABLE view_path_dynamic (id STRING PRIMARY KEY, a ANY, b ANY)`,
		`INSERT INTO view_path_dynamic VALUES ('{"id":"bad","a":1,"b":"1"}')`,
		`CREATE TABLE view_path_outer (id STRING PRIMARY KEY, x ANY)`,
		`INSERT INTO view_path_outer VALUES ('{"id":"outer","x":1}')`,
		`CREATE TABLE view_path_inner (id STRING PRIMARY KEY, x ANY)`,
		`INSERT INTO view_path_inner VALUES ('{"id":"inner","x":"1"}')`,
		`CREATE VIEW view_path_bad AS ` +
			`SELECT id FROM view_path_dynamic WHERE a <= b`,
		`CREATE VIEW view_path_bad_cte AS WITH d AS (` +
			`SELECT id FROM view_path_dynamic WHERE a <= b) ` +
			`SELECT id FROM d`,
		`CREATE VIEW view_path_bad_set AS ` +
			`SELECT id FROM view_path_dynamic WHERE a <= b ` +
			`UNION ALL SELECT id FROM view_path_dynamic WHERE id = 'never'`,
		`CREATE VIEW view_path_bad_case AS ` +
			`SELECT CASE WHEN a <= b THEN id ELSE id END AS id FROM view_path_dynamic`,
		`CREATE VIEW view_path_bad_on AS ` +
			`SELECT o.id FROM view_path_outer o JOIN view_path_inner i ON o.x = i.x`,
		`CREATE VIEW view_path_bad_using AS ` +
			`SELECT o.id FROM view_path_outer o JOIN view_path_inner i USING (x)`,
		`CREATE VIEW view_path_bad_correlation AS ` +
			`SELECT o.id FROM view_path_outer o WHERE EXISTS (` +
			`SELECT i.id FROM view_path_inner i WHERE i.x = o.x)`,
		`CREATE VIEW view_path_bad_lateral AS ` +
			`SELECT o.id FROM view_path_outer o CROSS JOIN LATERAL (` +
			`SELECT i.id FROM view_path_inner i WHERE i.x = o.x) d`,
		`CREATE VIEW view_path_bad_nested AS SELECT id FROM view_path_bad_cte`,
	} {
		if _, err := db.Exec(statement); err != nil {
			_ = db.Close()
			t.Fatalf("%s: %v", statement, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, test := range []struct {
		view            string
		left, op, right string
	}{
		{"view_path_bad", "numeric", "<=", "text"},
		{"view_path_bad_cte", "numeric", "<=", "text"},
		{"view_path_bad_set", "numeric", "<=", "text"},
		{"view_path_bad_case", "numeric", "<=", "text"},
		{"view_path_bad_on", "numeric", "=", "text"},
		{"view_path_bad_using", "numeric", "=", "text"},
		{"view_path_bad_correlation", "text", "=", "numeric"},
		{"view_path_bad_lateral", "text", "=", "numeric"},
		{"view_path_bad_nested", "numeric", "<=", "text"},
	} {
		t.Run(test.view, func(t *testing.T) {
			rows, runErr := db.Query(`SELECT id FROM ` + test.view)
			if rows != nil {
				_ = rows.Close()
			}
			var undefined *sqlast.UndefinedOperatorError
			if !errors.As(runErr, &undefined) || !undefined.Unpositioned ||
				undefined.Left != test.left || undefined.Operator != test.op ||
				undefined.Right != test.right {
				t.Fatalf("durable-view error = %T %+v", runErr, undefined)
			}
			var positioned *sqlast.ParseError
			if errors.As(runErr, &positioned) {
				t.Fatalf("definition offset leaked as outer position: %+v", positioned)
			}
		})
	}
}

func TestDatabaseSQLPathComparisonPrimaryKeyStaysAFullScan(t *testing.T) {
	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE path_primary (id STRING PRIMARY KEY, other_id STRING)`,
		`INSERT INTO path_primary (id, other_id) VALUES ('a', 'a'), ('b', 'other')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	var id string
	if err := db.QueryRow(`SELECT id FROM path_primary WHERE id = other_id`).Scan(&id); err != nil || id != "a" {
		t.Fatalf("path primary equality = (%q, %v), want a", id, err)
	}
	var plan string
	if err := db.QueryRow(`EXPLAIN SELECT id FROM path_primary WHERE id = other_id`).Scan(&plan); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, `"access_path":"full-scan"`) ||
		strings.Contains(plan, `"access_path":"primary-key-point-or-scan"`) {
		t.Fatalf("path primary equality was point-routed: %s", plan)
	}
}

func TestPathComparisonCannotBindPrimaryPointKeys(t *testing.T) {
	parsed, err := sqlast.Parse(`SELECT id FROM docs WHERE id = other_id`)
	if err != nil {
		t.Fatal(err)
	}
	where := parsed.Where
	if _, candidate := primaryPredicateIdentity(where); candidate {
		t.Fatal("path comparison classified as primary point identity")
	}
	if point := compilePrimaryPointPredicate(where, "/id"); point != nil {
		t.Fatalf("compiled primary point = %+v", point)
	}
	conjunction, err := sqlast.Parse(
		`SELECT id FROM docs WHERE id = 'safe' AND a = b`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if point := compilePrimaryPointPredicate(conjunction.Where, "/id"); point != nil {
		t.Fatalf("path comparison conjunction compiled primary point = %+v", point)
	}
	if span := compilePrimaryRangeProgram(conjunction.Where, "/id"); span != nil {
		t.Fatalf("path comparison conjunction compiled primary range = %+v", span)
	}
	keys, bounded, err := primaryPredicateKeys(where, "/id", nil, nil, 1024)
	if err != nil || bounded || len(keys) != 0 {
		t.Fatalf("primaryPredicateKeys = (%v, %v, %v), want unbounded", keys, bounded, err)
	}
	if _, err := bindPrimaryPredicateKeys(where, nil, nil, 1024); err == nil {
		t.Fatal("owning primary binder accepted a RHS path")
	}
	connection := &conn{}
	if _, err := connection.bindPointPredicateKeys(where, nil, 1024); err == nil {
		t.Fatal("borrowed primary binder accepted a RHS path")
	}
}
