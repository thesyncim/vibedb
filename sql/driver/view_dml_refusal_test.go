package driver

import (
	"context"
	stdsql "database/sql"
	"errors"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestDMLViewSourcesArePositionedFeatureRefusals(t *testing.T) {
	db := openTestDB(t)
	for _, source := range []string{
		`CREATE TABLE docs (id STRING PRIMARY KEY)`,
		`CREATE TABLE target (id STRING PRIMARY KEY)`,
		`CREATE VIEW selected AS SELECT id FROM docs`,
	} {
		if _, err := db.Exec(source); err != nil {
			t.Fatal(err)
		}
	}
	for _, source := range []string{
		`UPDATE target SET "$doc" = ? WHERE id IN (SELECT id FROM selected)`,
		`DELETE FROM target WHERE EXISTS (SELECT id FROM selected)`,
	} {
		statement, err := db.Prepare(source)
		if statement != nil {
			statement.Close()
		}
		var unsupported *sqlast.FeatureNotSupportedError
		if !errors.As(err, &unsupported) {
			t.Fatalf("%s = %T %v, want FeatureNotSupportedError", source, err, err)
		}
		if unsupported.Pos != strings.LastIndex(source, "selected") {
			t.Fatalf("%s position = %d, want view reference", source, unsupported.Pos)
		}
	}
}

func TestUnexpandedDMLViewRefusalCoversReturningTrees(t *testing.T) {
	const source = `RETURNING (SELECT id FROM selected)`
	position := strings.LastIndex(source, "selected")
	connection := conn{tx: &tx{views: map[string]*viewMeta{
		"selected": {},
	}}}
	returning := &sqlast.SelectStmt{From: []sqlast.TableRef{{
		Name: "selected", Pos: position,
	}}}
	for _, statement := range []*sqlast.Statement{
		{Kind: sqlast.KindUpdate, Update: &sqlast.UpdateStmt{Returning: returning}},
		{Kind: sqlast.KindDelete, Delete: &sqlast.DeleteStmt{Returning: returning}},
	} {
		err := connection.validateViewStatementContext(
			context.Background(), source, statement,
		)
		var unsupported *sqlast.FeatureNotSupportedError
		if !errors.As(err, &unsupported) {
			t.Fatalf("kind %s = %T %v, want FeatureNotSupportedError",
				statement.Kind, err, err)
		}
		if unsupported.Pos != position {
			t.Fatalf("kind %s position = %d, want %d",
				statement.Kind, unsupported.Pos, position)
		}
	}
}

func TestPreparedDMLNestedTableReplacementByViewIsRefused(t *testing.T) {
	tests := []struct {
		name        string
		operation   string
		returning   bool
		transaction bool
	}{
		{name: "update exec", operation: "UPDATE"},
		{name: "delete exec", operation: "DELETE"},
		{name: "update returning", operation: "UPDATE", returning: true},
		{name: "delete returning", operation: "DELETE", returning: true},
		{name: "transaction update exec", operation: "UPDATE", transaction: true},
		{name: "transaction delete exec", operation: "DELETE", transaction: true},
		{name: "transaction update returning", operation: "UPDATE", returning: true, transaction: true},
		{name: "transaction delete returning", operation: "DELETE", returning: true, transaction: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := openTestDB(t)
			db.SetMaxOpenConns(4)
			for _, source := range []string{
				`CREATE TABLE mutation_target (id STRING PRIMARY KEY, state STRING NOT NULL)`,
				`CREATE TABLE nested_source (id STRING PRIMARY KEY)`,
				`CREATE TABLE replacement_base (id STRING PRIMARY KEY)`,
				`INSERT INTO mutation_target VALUES ('{"id":"target","state":"original"}')`,
				`INSERT INTO nested_source VALUES ('{"id":"target"}')`,
				`INSERT INTO replacement_base VALUES ('{"id":"target"}')`,
			} {
				if _, err := db.Exec(source); err != nil {
					t.Fatalf("setup %q: %v", source, err)
				}
			}

			var source string
			switch test.operation {
			case "UPDATE":
				source = `UPDATE mutation_target SET "$doc" = '{"id":"target","state":"mutated"}' WHERE id IN (SELECT id FROM nested_source)`
			case "DELETE":
				source = `DELETE FROM mutation_target WHERE id IN (SELECT id FROM nested_source)`
			default:
				t.Fatalf("unknown operation %q", test.operation)
			}
			if test.returning {
				source += ` RETURNING id`
			}

			var transaction interface {
				Prepare(string) (*stdsql.Stmt, error)
			}
			var tx *stdsql.Tx
			if test.transaction {
				var err error
				tx, err = db.Begin()
				if err != nil {
					t.Fatal(err)
				}
				transaction = tx
			} else {
				transaction = db
			}
			prepared, err := transaction.Prepare(source)
			if err != nil {
				if tx != nil {
					_ = tx.Rollback()
				}
				t.Fatalf("prepare: %v", err)
			}

			for _, replacement := range []string{
				`DROP TABLE nested_source`,
				`CREATE VIEW nested_source AS SELECT id FROM replacement_base`,
			} {
				if _, err := db.Exec(replacement); err != nil {
					prepared.Close()
					if tx != nil {
						_ = tx.Rollback()
					}
					t.Fatalf("replace relation %q: %v", replacement, err)
				}
			}

			if test.returning {
				rows, queryErr := prepared.Query()
				if rows != nil {
					rows.Close()
				}
				err = queryErr
			} else {
				_, err = prepared.Exec()
			}
			if closeErr := prepared.Close(); closeErr != nil {
				t.Errorf("close prepared statement: %v", closeErr)
			}

			var unsupported *sqlast.FeatureNotSupportedError
			if !errors.As(err, &unsupported) {
				t.Fatalf("execute = %T %v, want FeatureNotSupportedError", err, err)
			}
			if !errors.Is(err, ErrViewChanged) {
				t.Fatalf("execute = %v, want ErrViewChanged reprepare semantics", err)
			}
			position := strings.LastIndex(source, "nested_source")
			if unsupported.Pos != position {
				t.Fatalf("position = %d, want nested source at %d", unsupported.Pos, position)
			}
			if !strings.Contains(err.Error(), test.operation) {
				t.Fatalf("error = %q, want operation %s", err, test.operation)
			}
			var hinted interface{ SQLHint() string }
			if !errors.As(err, &hinted) ||
				!strings.Contains(hinted.SQLHint(), "prepare the "+test.operation+" statement again") {
				t.Fatalf("hint = %v, want operation-specific reprepare guidance", err)
			}

			if tx != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					t.Fatalf("rollback: %v", rollbackErr)
				}
			}
			var state string
			if err := db.QueryRow(
				`SELECT state FROM mutation_target WHERE id = 'target'`,
			).Scan(&state); err != nil {
				t.Fatalf("mutation reached execution: %v", err)
			}
			if state != "original" {
				t.Fatalf("target state = %q, want original", state)
			}
		})
	}
}

func TestDMLViewRebindValidationAbsentPathAllocations(t *testing.T) {
	statement, err := sqlast.ParseStatement(
		`UPDATE docs SET "$doc" = ? WHERE id = ?`,
	)
	if err != nil {
		t.Fatal(err)
	}
	views := map[string]*viewMeta{"unrelated": {}}
	allocations := testing.AllocsPerRun(1000, func() {
		if err := rejectReboundPreparedDMLViewReferences(statement, views); err != nil {
			t.Fatal(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("ordinary mutation validation = %.2f allocs/run, want 0", allocations)
	}
}

func TestInsertSelectViewSourceIsExpandedInsteadOfRefused(t *testing.T) {
	db := openTestDB(t)
	for _, source := range []string{
		`CREATE TABLE insert_view_source (id STRING PRIMARY KEY)`,
		`CREATE TABLE insert_view_target (id STRING PRIMARY KEY)`,
		`CREATE TABLE insert_view_tx_target (id STRING PRIMARY KEY)`,
		`INSERT INTO insert_view_source VALUES ('{"id":"outer","payload":{"id":"copied"}}')`,
		`CREATE VIEW insert_view (doc) AS SELECT payload FROM insert_view_source`,
	} {
		if _, err := db.Exec(source); err != nil {
			t.Fatalf("%s: %v", source, err)
		}
	}
	if _, err := db.Exec(
		`INSERT INTO insert_view_target SELECT doc FROM insert_view`,
	); err != nil {
		t.Fatalf("non-transactional view source: %v", err)
	}
	transaction, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.Exec(
		`INSERT INTO insert_view_tx_target SELECT doc FROM insert_view`,
	); err != nil {
		_ = transaction.Rollback()
		t.Fatalf("transactional view source: %v", err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"insert_view_target", "insert_view_tx_target"} {
		var id string
		if err := db.QueryRow(`SELECT id FROM ` + table).Scan(&id); err != nil {
			t.Fatalf("read %s: %v", table, err)
		}
		if id != "copied" {
			t.Fatalf("%s id = %q, want copied", table, id)
		}
	}
}
