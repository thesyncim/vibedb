package driver

import (
	"context"
	stdsql "database/sql"
	"errors"
	"strings"
	"testing"
)

func TestDropViewTableIsWrongObjectTypeEvenWithIfExists(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{
		`DROP VIEW docs`,
		`DROP VIEW IF EXISTS docs`,
	} {
		_, err := db.Exec(source)
		if !errors.Is(err, ErrWrongObjectType) {
			t.Fatalf("%s = %v, want ErrWrongObjectType", source, err)
		}
		var positioned interface{ Position() int }
		if !errors.As(err, &positioned) ||
			positioned.Position() != strings.LastIndex(source, "docs") {
			t.Fatalf("%s position = %v, want table name", source, err)
		}
	}
}

func TestViewTargetsAreWrongObjectTypeAcrossTableOperations(t *testing.T) {
	db := openTestDB(t)
	for _, source := range []string{
		`CREATE TABLE base_docs (id STRING PRIMARY KEY)`,
		`INSERT INTO base_docs VALUES ('{"id":"base"}')`,
		`CREATE VIEW read_view AS SELECT id FROM base_docs`,
	} {
		if _, err := db.Exec(source); err != nil {
			t.Fatalf("%s: %v", source, err)
		}
	}
	tests := []struct {
		source string
		hint   string
	}{
		{`DROP TABLE read_view`, "DROP VIEW"},
		{`DROP TABLE IF EXISTS read_view`, "DROP VIEW"},
		{`TRUNCATE read_view`, "truncate a base table"},
		{`CREATE INDEX read_view_id ON read_view (id)`, "index on a base table"},
		{`DROP INDEX IF EXISTS missing_index ON read_view`, "index from its base table"},
		{`INSERT INTO read_view VALUES ('{"id":"write"}')`, "insert into a base table"},
		{`UPDATE read_view SET "$doc" = '{"id":"write"}' WHERE id = 'base'`, "update a base table"},
		{`DELETE FROM read_view WHERE id = 'base'`, "delete from a base table"},
	}
	for _, test := range tests {
		_, err := db.Exec(test.source)
		if !errors.Is(err, ErrWrongObjectType) {
			t.Fatalf("%s = %T %v, want ErrWrongObjectType", test.source, err, err)
		}
		var positioned interface{ Position() int }
		if !errors.As(err, &positioned) ||
			positioned.Position() != strings.LastIndex(test.source, "read_view") {
			t.Fatalf("%s position = %v, want view name", test.source, err)
		}
		var hinted interface{ SQLHint() string }
		if !errors.As(err, &hinted) || !strings.Contains(hinted.SQLHint(), test.hint) {
			t.Fatalf("%s hint = %v, want %q", test.source, err, test.hint)
		}
	}
	var id string
	if err := db.QueryRow(`SELECT id FROM read_view`).Scan(&id); err != nil {
		t.Fatalf("view after refused writes: %v", err)
	}
	if id != "base" {
		t.Fatalf("view row = %q, want base", id)
	}
}

func TestPreparedTableOperationsRevalidateViewObjectTypeAtExecution(t *testing.T) {
	tests := []struct {
		name        string
		source      string
		returnsRows bool
	}{
		{"drop table", `DROP TABLE race_view`, false},
		{"truncate", `TRUNCATE race_view`, false},
		{"create index", `CREATE INDEX race_view_id ON race_view (id)`, false},
		{"drop index", `DROP INDEX IF EXISTS missing_index ON race_view`, false},
		{"insert", `INSERT INTO race_view VALUES ('{"id":"write"}')`, false},
		{"update", `UPDATE race_view SET "$doc" = '{"id":"write"}' WHERE id = 'base'`, false},
		{"delete", `DELETE FROM race_view WHERE id = 'base'`, false},
		{"returning", `DELETE FROM race_view WHERE id = 'base' RETURNING id`, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := openTestDB(t)
			for _, source := range []string{
				`CREATE TABLE object_base (id STRING PRIMARY KEY)`,
				`CREATE TABLE race_view (id STRING PRIMARY KEY)`,
			} {
				if _, err := db.Exec(source); err != nil {
					t.Fatal(err)
				}
			}
			prepared, err := db.Prepare(test.source)
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Close()
			if _, err := db.Exec(`DROP TABLE race_view`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(
				`CREATE VIEW race_view AS SELECT id FROM object_base`,
			); err != nil {
				t.Fatal(err)
			}
			if test.returnsRows {
				rows, queryErr := prepared.Query()
				if rows != nil {
					rows.Close()
				}
				err = queryErr
			} else {
				_, err = prepared.Exec()
			}
			if !errors.Is(err, ErrWrongObjectType) {
				t.Fatalf("execute = %T %v, want ErrWrongObjectType", err, err)
			}
			var positioned interface{ Position() int }
			if !errors.As(err, &positioned) ||
				positioned.Position() != strings.LastIndex(test.source, "race_view") {
				t.Fatalf("position = %v, want authored target", err)
			}
		})
	}
}

func TestTransactionMutationRevalidatesCurrentViewObjectType(t *testing.T) {
	db := openTestDB(t)
	for _, source := range []string{
		`CREATE TABLE object_base (id STRING PRIMARY KEY)`,
		`CREATE TABLE race_view (id STRING PRIMARY KEY)`,
	} {
		if _, err := db.Exec(source); err != nil {
			t.Fatal(err)
		}
	}
	transaction, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
		Isolation: stdsql.LevelRepeatableRead,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer transaction.Rollback()
	const source = `DELETE FROM race_view WHERE id = 'base'`
	prepared, err := transaction.Prepare(source)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if _, err := db.Exec(`DROP TABLE race_view`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`CREATE VIEW race_view AS SELECT id FROM object_base`,
	); err != nil {
		t.Fatal(err)
	}
	_, err = prepared.Exec()
	if !errors.Is(err, ErrWrongObjectType) {
		t.Fatalf("transaction execute = %T %v, want ErrWrongObjectType", err, err)
	}
}

func TestPreparedDropViewRevalidatesTableObjectTypeAtExecution(t *testing.T) {
	for _, source := range []string{
		`DROP VIEW race_relation`,
		`DROP VIEW IF EXISTS race_relation`,
	} {
		t.Run(source, func(t *testing.T) {
			db := openTestDB(t)
			for _, setup := range []string{
				`CREATE TABLE object_base (id STRING PRIMARY KEY)`,
				`CREATE VIEW race_relation AS SELECT id FROM object_base`,
			} {
				if _, err := db.Exec(setup); err != nil {
					t.Fatal(err)
				}
			}
			prepared, err := db.Prepare(source)
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Close()
			if _, err := db.Exec(`DROP VIEW race_relation`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(
				`CREATE TABLE race_relation (id STRING PRIMARY KEY)`,
			); err != nil {
				t.Fatal(err)
			}
			_, err = prepared.Exec()
			if !errors.Is(err, ErrWrongObjectType) {
				t.Fatalf("execute = %T %v, want ErrWrongObjectType", err, err)
			}
		})
	}
}
