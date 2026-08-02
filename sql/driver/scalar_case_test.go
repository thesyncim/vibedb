package driver

import (
	"errors"
	"reflect"
	"testing"

	"github.com/thesyncim/vibedb/query"
)

func TestDatabaseSQLScalarCaseMetadataValuesPreparedErrorsAndRecovery(t *testing.T) {
	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE case_docs (id STRING PRIMARY KEY)`,
		`INSERT INTO case_docs VALUES
			('{"id":"a","flag":true,"n":2}'),
			('{"id":"b","flag":false,"n":5}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	rows, err := db.Query(`SELECT
		CASE WHEN flag THEN n + 1 ELSE 0 END AS score,
		CASE id WHEN 'a' THEN 'first' ELSE 'other' END AS label,
		CASE WHEN flag THEN TRUE ELSE FALSE END AS active
		FROM case_docs ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	wantNames := []string{"NUMERIC", "TEXT", "BOOLEAN"}
	wantScans := []reflect.Type{
		reflect.TypeFor[any](), reflect.TypeFor[[]byte](), reflect.TypeFor[bool](),
	}
	for i := range types {
		if types[i].DatabaseTypeName() != wantNames[i] || types[i].ScanType() != wantScans[i] {
			t.Fatalf("CASE type %d = %s/%v, want %s/%v", i,
				types[i].DatabaseTypeName(), types[i].ScanType(), wantNames[i], wantScans[i])
		}
	}
	want := []struct {
		score int64
		label string
		flag  bool
	}{{3, "first", true}, {0, "other", false}}
	for i := range want {
		if !rows.Next() {
			t.Fatalf("missing CASE row %d: %v", i, rows.Err())
		}
		var score int64
		var label []byte
		var active bool
		if err := rows.Scan(&score, &label, &active); err != nil {
			t.Fatal(err)
		}
		if score != want[i].score || string(label) != want[i].label || active != want[i].flag {
			t.Fatalf("CASE row %d = %d/%s/%v", i, score, label, active)
		}
	}
	if rows.Next() || rows.Err() != nil {
		t.Fatalf("unexpected CASE tail: %v", rows.Err())
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	prepared, err := db.Prepare(`SELECT CASE WHEN ? THEN CAST(? AS TEXT) ELSE CAST(? AS TEXT) END
		FROM case_docs WHERE id = 'a'`)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	var text []byte
	if err := prepared.QueryRow(true, "chosen", "dead").Scan(&text); err != nil || string(text) != "chosen" {
		t.Fatalf("prepared CASE true = %q/%v", text, err)
	}
	if err := prepared.QueryRow(false, "dead", "chosen").Scan(&text); err != nil || string(text) != "chosen" {
		t.Fatalf("prepared CASE false = %q/%v", text, err)
	}
	if err := prepared.QueryRow("not-bool", "x", "y").Scan(&text); !errors.Is(err, query.ErrScalarType) {
		t.Fatalf("prepared CASE type error = %T %v", err, err)
	}
	if err := prepared.QueryRow(true, "recovered", "dead").Scan(&text); err != nil || string(text) != "recovered" {
		t.Fatalf("prepared CASE recovery = %q/%v", text, err)
	}
}
