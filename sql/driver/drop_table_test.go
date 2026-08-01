package driver

import (
	stdsql "database/sql"
	"errors"
	"path/filepath"
	"testing"
)

func TestDropTablePersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	db, err := stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"one"}`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE docs`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE IF EXISTS docs`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE IF EXISTS missing`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE docs`); !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("DROP TABLE after removal = %v, want ErrTableNotFound", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reusing the name proves the old durable table file is no longer visible
	// to materialization after the catalog has been reopened.
	db, err = stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"two"}`); err != nil {
		t.Fatal(err)
	}
}

func TestDropTableReusesNameWithIndependentActiveSnapshot(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(2)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"old"}`); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(`SELECT id FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE docs`); err != nil {
		_ = rows.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY)`); err != nil {
		_ = rows.Close()
		t.Fatalf("CREATE TABLE during retired snapshot: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"new"}`); err != nil {
		_ = rows.Close()
		t.Fatal(err)
	}
	if !rows.Next() {
		_ = rows.Close()
		t.Fatal("old snapshot lost its row after same-name recreation")
	}
	var old string
	if err := rows.Scan(&old); err != nil || old != "old" {
		_ = rows.Close()
		t.Fatalf("old snapshot row = %q, err %v", old, err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	var id string
	if err := db.QueryRow(`SELECT id FROM docs`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if id != "new" {
		t.Fatalf("recreated table row = %q, want new", id)
	}
}
