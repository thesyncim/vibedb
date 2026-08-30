package driver

import (
	"errors"
	"reflect"
	"testing"

	"github.com/thesyncim/vibedb/query"
)

func TestDatabaseSQLScalarCastMetadataValuesPreparedErrorsAndRecovery(t *testing.T) {
	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE cast_docs (id STRING PRIMARY KEY)`,
		`INSERT INTO cast_docs VALUES ('{"id":"a","n":"001.25e2","b":"yes","j":"{\"x\":1}"}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	rows, err := db.Query(`SELECT CAST(n AS NUMERIC), CAST(b AS BOOLEAN), CAST(j AS JSON), CAST(id AS TEXT) FROM cast_docs`)
	if err != nil {
		t.Fatal(err)
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	wantNames := []string{"NUMERIC", "BOOLEAN", "JSON", "TEXT"}
	wantScan := []reflect.Type{reflect.TypeFor[any](), reflect.TypeFor[bool](), reflect.TypeFor[any](), reflect.TypeFor[[]byte]()}
	for i := range wantNames {
		if types[i].DatabaseTypeName() != wantNames[i] || types[i].ScanType() != wantScan[i] {
			t.Fatalf("type %d = %s/%v, want %s/%v", i, types[i].DatabaseTypeName(), types[i].ScanType(), wantNames[i], wantScan[i])
		}
	}
	if !rows.Next() {
		t.Fatal("missing CAST row")
	}
	var number string
	var boolean bool
	var json, text []byte
	if err := rows.Scan(&number, &boolean, &json, &text); err != nil {
		t.Fatal(err)
	}
	if number != "1.25e2" || !boolean || string(json) != `{"x":1}` || string(text) != "a" {
		t.Fatalf("row = %q/%v/%s/%s", number, boolean, json, text)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{
		`SELECT CAST('not-numeric' AS NUMERIC), 1 / 0 FROM cast_docs WHERE 1 = 0`,
		`SELECT CAST('not-numeric' AS NUMERIC), 1 / 0 FROM cast_docs OFFSET 1`,
	} {
		lazy, err := db.Query(source)
		if err != nil {
			t.Fatalf("lazy projection %q: %v", source, err)
		}
		if lazy.Next() {
			t.Fatalf("lazy projection %q returned a row", source)
		}
		if err := lazy.Err(); err != nil {
			t.Fatalf("lazy projection %q raised %v", source, err)
		}
		if err := lazy.Close(); err != nil {
			t.Fatal(err)
		}
	}

	prepared, err := db.Prepare(`SELECT ?::BOOLEAN FROM cast_docs`)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if err := prepared.QueryRow("invalid").Scan(&boolean); !errors.Is(err, query.ErrScalarInvalidText) {
		t.Fatalf("invalid boolean error = %T %v", err, err)
	}
	if err := prepared.QueryRow("on").Scan(&boolean); err != nil || !boolean {
		t.Fatalf("prepared recovery = %v/%v", boolean, err)
	}
}
