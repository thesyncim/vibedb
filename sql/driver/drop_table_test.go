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
