package driver

import (
	"context"
	stdsql "database/sql"
	"errors"
	"slices"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibejson"
)

func TestPrimaryPointConjunctClassification(t *testing.T) {
	connection := directTestConn(t)
	directExec(t, connection,
		`CREATE TABLE docs (id STRING PRIMARY KEY, active BOOLEAN)`, nil)
	for _, test := range []struct {
		name      string
		sql       string
		candidate bool
		kind      sqlast.ExprKind
		keys      int
	}{
		{
			name:      "root equality",
			sql:       `SELECT id FROM docs WHERE id = ?`,
			candidate: true, kind: sqlast.ExprCompare, keys: 1,
		},
		{
			name:      "equality under conjunction",
			sql:       `SELECT id FROM docs WHERE active = TRUE AND id = ?`,
			candidate: true, kind: sqlast.ExprCompare, keys: 1,
		},
		{
			name:      "membership under conjunction",
			sql:       `SELECT id FROM docs WHERE id IN (?, ?) AND active = TRUE`,
			candidate: true, kind: sqlast.ExprIn, keys: 2,
		},
		{
			name:      "equality preferred to membership",
			sql:       `SELECT id FROM docs WHERE id IN (?, ?) AND id = ?`,
			candidate: true, kind: sqlast.ExprCompare, keys: 1,
		},
		{
			name:      "equality wins equal cardinality tie",
			sql:       `SELECT id FROM docs WHERE id IN (?) AND id = ?`,
			candidate: true, kind: sqlast.ExprCompare, keys: 1,
		},
		{
			name:      "shortest membership preferred",
			sql:       `SELECT id FROM docs WHERE id IN (?, ?, ?) AND id IN (?)`,
			candidate: true, kind: sqlast.ExprIn, keys: 1,
		},
		{
			name: "or is not a bound",
			sql:  `SELECT id FROM docs WHERE (id = ? OR active = TRUE) AND active = TRUE`,
		},
		{
			name: "not is not a bound",
			sql:  `SELECT id FROM docs WHERE NOT (id = ?) AND active = TRUE`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			statement, err := connection.Prepare(test.sql)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Close()
			prepared := statement.(*stmt)
			if prepared.pointCandidate != test.candidate {
				t.Fatalf("point candidate = %v, want %v",
					prepared.pointCandidate, test.candidate)
			}
			if !test.candidate {
				if prepared.pointPredicate != nil || prepared.primaryPoint {
					t.Fatal("unsafe boolean subtree produced a primary point candidate")
				}
				return
			}
			if prepared.pointPredicate == nil {
				t.Fatal("point candidate has no binding predicate")
			}
			if prepared.pointPredicate.Kind != test.kind ||
				prepared.pointPath != "/id" || !prepared.primaryPoint {
				t.Fatalf("point classification = kind %v path %q primary %v",
					prepared.pointPredicate.Kind, prepared.pointPath, prepared.primaryPoint)
			}
			keys := 1
			if prepared.pointPredicate.Kind == sqlast.ExprIn {
				keys = len(prepared.pointPredicate.List)
			}
			if keys != test.keys {
				t.Fatalf("point source keys = %d, want %d", keys, test.keys)
			}
		})
	}
}

func TestPrimaryPointConjunctExecutesCompleteResidual(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(
		`CREATE TABLE docs (id STRING PRIMARY KEY, active BOOLEAN, value STRING)`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs (id, active, value) VALUES
		('a', TRUE, 'first'), ('b', FALSE, 'second'), ('c', TRUE, 'third')`); err != nil {
		t.Fatal(err)
	}

	var value string
	if err := db.QueryRow(
		`SELECT value FROM docs WHERE id = ? AND active = TRUE`, "a",
	).Scan(&value); err != nil || value != "first" {
		t.Fatalf("matching residual = (%q, %v), want first", value, err)
	}
	if err := db.QueryRow(
		`SELECT value FROM docs WHERE id = ? AND active = TRUE`, "b",
	).Scan(&value); !errors.Is(err, stdsql.ErrNoRows) {
		t.Fatalf("false residual = %v, want sql.ErrNoRows", err)
	}
	for _, test := range []struct {
		name string
		key  any
	}{
		{name: "null makes an empty point set"},
		{name: "oversized key makes an empty point set", key: strings.Repeat("x", 64<<10)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := db.QueryRow(
				`SELECT value FROM docs WHERE id = ? AND active = TRUE`, test.key,
			).Scan(&value); !errors.Is(err, stdsql.ErrNoRows) {
				t.Fatalf("unmatchable point operand = %v, want sql.ErrNoRows", err)
			}
		})
	}
	if err := db.QueryRow(`
		SELECT value FROM docs
		WHERE id IN (?, ?) AND active = TRUE`, nil, "a",
	).Scan(&value); err != nil || value != "first" {
		t.Fatalf("NULL IN alternative = (%q, %v), want first", value, err)
	}

	rows, err := db.Query(`
		SELECT id FROM docs
		WHERE active = TRUE AND id IN (?, ?)
		ORDER BY id`, "c", "a")
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
	if !slices.Equal(ids, []string{"a", "c"}) {
		t.Fatalf("membership residual ids = %v, want [a c]", ids)
	}

	if err := db.QueryRow(`
		SELECT id FROM docs
		WHERE id IN ('a', 'c') AND id = 'c' AND active = TRUE`,
	).Scan(&value); err != nil || value != "c" {
		t.Fatalf("intersected point predicates = (%q, %v), want c", value, err)
	}
}

func TestPrimaryPointConjunctExplainAnalyze(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(
		`CREATE TABLE docs (id STRING PRIMARY KEY, active BOOLEAN)`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO docs (id, active) VALUES ('a', TRUE), ('b', FALSE)`,
	); err != nil {
		t.Fatal(err)
	}
	var plan string
	if err := db.QueryRow(`
		EXPLAIN SELECT id FROM docs
		WHERE id = ? AND active = TRUE`, "a").Scan(&plan); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, `"access_path":"primary-key-point-or-scan"`) {
		t.Fatalf("EXPLAIN did not expose primary point candidate: %s", plan)
	}
	if err := db.QueryRow(`
		EXPLAIN ANALYZE SELECT id FROM docs
		WHERE id = ? AND active = TRUE`, "b").Scan(&plan); err != nil {
		t.Fatal(err)
	}
	if !vibejson.Valid([]byte(plan)) ||
		!strings.Contains(plan, `"actual_access_path":"primary-key-point"`) ||
		!strings.Contains(plan, `"rows":0`) {
		t.Fatalf("EXPLAIN ANALYZE did not report residual point execution: %s", plan)
	}
}

func TestPreparedPrimaryPointConjunctRevalidatesPrimaryKeyAfterRecreate(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(
		`CREATE TABLE docs (id STRING PRIMARY KEY, active BOOLEAN, value STRING)`,
	); err != nil {
		t.Fatal(err)
	}
	prepared, err := db.Prepare(
		`SELECT value FROM docs WHERE id = ? AND active = TRUE`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	for _, statement := range []string{
		`DROP TABLE docs`,
		`CREATE TABLE docs (code STRING PRIMARY KEY, id STRING, active BOOLEAN, value STRING)`,
		`INSERT INTO docs (code, id, active, value) VALUES ('storage-key', 'lookup', TRUE, 'found')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	var value string
	if err := prepared.QueryRow("lookup").Scan(&value); err != nil || value != "found" {
		t.Fatalf("recreated-table residual query = (%q, %v), want found", value, err)
	}
}

func TestSerializablePrimaryPointConjunctTracksCandidateKey(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(
		`CREATE TABLE docs (id STRING PRIMARY KEY, active BOOLEAN, value STRING)`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs (id, active, value) VALUES
		('left', TRUE, 'old'), ('right', TRUE, 'old')`); err != nil {
		t.Fatal(err)
	}

	reader, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
		Isolation: stdsql.LevelSerializable,
	})
	if err != nil {
		t.Fatal(err)
	}
	var value string
	if err := reader.QueryRow(`
		SELECT value FROM docs
		WHERE id = ? AND active = TRUE`, "left").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		UPDATE docs
		SET "$doc" = '{"id":"right","active":true,"value":"outside"}'
		WHERE id = 'right'`); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Exec(
		`INSERT INTO docs (id, active, value) VALUES ('guard', TRUE, 'tx')`,
	); err != nil {
		t.Fatal(err)
	}
	if err := reader.Commit(); err != nil {
		t.Fatalf("unrelated point write caused a false conflict: %v", err)
	}

	reader, err = db.BeginTx(context.Background(), &stdsql.TxOptions{
		Isolation: stdsql.LevelSerializable,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.QueryRow(`
		SELECT value FROM docs
		WHERE id = ? AND active = TRUE`, "left").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		UPDATE docs
		SET "$doc" = '{"id":"left","active":true,"value":"outside"}'
		WHERE id = 'left'`); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Exec(
		`UPDATE docs
		 SET "$doc" = '{"id":"guard","active":true,"value":"tx"}'
		 WHERE id = 'guard'`,
	); err != nil {
		t.Fatal(err)
	}
	if err := reader.Commit(); !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("candidate-key point read COMMIT = %v, want ErrTransactionConflict", err)
	}
}
