package driver

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestPreparedMutationSettlesBeforeViewAndTargetRevalidation(t *testing.T) {
	for _, returnsRows := range []bool{false, true} {
		name := "exec"
		if returnsRows {
			name = "returning"
		}
		t.Run(name, func(t *testing.T) {
			database, err := Open(filepath.Join(t.TempDir(), "catalog.vdb"))
			if err != nil {
				t.Fatal(err)
			}
			session, err := database.NewSession(context.Background())
			if err != nil {
				_ = database.Close()
				t.Fatal(err)
			}
			defer func() {
				if err := session.Close(); err != nil {
					t.Error(err)
				}
				if err := database.Close(); err != nil {
					t.Error(err)
				}
			}()

			for _, source := range []string{
				`CREATE TABLE settlement_source (id STRING PRIMARY KEY)`,
				`CREATE TABLE settlement_target (id STRING PRIMARY KEY)`,
				`CREATE VIEW settlement_view (doc) AS SELECT payload FROM settlement_source`,
			} {
				statement, err := session.Prepare(context.Background(), source)
				if err != nil {
					t.Fatal(err)
				}
				_, execErr := statement.Exec(context.Background(), nil)
				closeErr := statement.Close()
				if execErr != nil || closeErr != nil {
					t.Fatalf("%s: exec=%v close=%v", source, execErr, closeErr)
				}
			}

			source := `INSERT INTO settlement_target SELECT doc FROM settlement_view`
			if returnsRows {
				source += ` RETURNING id`
			}
			prepared, err := session.Prepare(context.Background(), source)
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Close()
			dependencies := prepared.statement.views.dependencies
			if len(dependencies) != 1 || dependencies[0].name != "settlement_view" {
				t.Fatalf("prepared dependencies = %+v", dependencies)
			}

			core := session.conn.db
			core.mu.Lock()
			oldView := core.catalog.Views["settlement_view"]
			newView := *oldView
			targetTable := core.tables["settlement_target"]
			targetMeta := core.catalog.Tables["settlement_target"]
			settled := 0
			core.catalogFencePending = true
			core.syncDir = func(string) error {
				settled++
				// Model settlement adopting a newer authoritative generation and a
				// target-kind replacement in one catalog cut. Generation validation
				// must win over the later target-kind check in both execution paths.
				core.catalog.Views["settlement_view"] = &newView
				delete(core.tables, "settlement_target")
				delete(core.catalog.Tables, "settlement_target")
				core.catalog.Views["settlement_target"] = &newView
				return nil
			}
			core.mu.Unlock()

			if returnsRows {
				cursor, queryErr := prepared.Query(context.Background(), nil)
				if cursor != nil {
					_ = cursor.Close()
				}
				err = queryErr
			} else {
				_, err = prepared.Exec(context.Background(), nil)
			}

			core.mu.Lock()
			core.syncDir = nil
			delete(core.catalog.Views, "settlement_target")
			core.tables["settlement_target"] = targetTable
			core.catalog.Tables["settlement_target"] = targetMeta
			core.mu.Unlock()

			if settled != 1 {
				t.Fatalf("catalog settlement calls = %d, want 1", settled)
			}
			if !errors.Is(err, ErrViewChanged) {
				t.Fatalf("execution after settlement = %T %v, want ErrViewChanged", err, err)
			}
			if errors.Is(err, ErrWrongObjectType) {
				t.Fatalf("target-kind validation ran before generation validation: %v", err)
			}
		})
	}
}
