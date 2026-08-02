package driver

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestViewCatalogCrashPublicationMatrix(t *testing.T) {
	for _, operation := range [...]string{"create", "drop"} {
		for _, phase := range [...]string{"pre-rename", "post-rename"} {
			t.Run(operation+"/"+phase, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "catalog.vdb")
				database := openViewPublicationDatabase(t, path, operation == "drop")
				original := database.catalog.Views["selected"]
				closed := false
				defer func() {
					if closed {
						return
					}
					database.path = path
					database.syncDir = nil
					if err := database.closeTerminal(); err != nil {
						t.Errorf("cleanup close: %v", err)
					}
				}()

				fenceFailure := errors.New("injected view catalog fence failure")
				if phase == "pre-rename" {
					// CreateTemp fails before a replacement can become visible. This is
					// the earliest publication fault and must be exactly rollback-safe.
					database.path = filepath.Join(t.TempDir(), "missing", "catalog.vdb")
				} else {
					database.syncDir = func(string) error { return fenceFailure }
				}

				err := mutateTestView(database, operation)
				if err == nil {
					t.Fatal("faulted view DDL unexpectedly succeeded")
				}
				database.path = path

				if phase == "pre-rename" {
					if errors.Is(err, durable.ErrCommitOutcomeUnknown) {
						t.Fatalf("pre-rename failure = %v, must have a known rollback outcome", err)
					}
					assertTestViewGeneration(t, database, operation == "drop", original)
					if database.catalogWritePending || database.catalogFencePending {
						t.Fatalf("rollback retained publication state: write=%t fence=%t",
							database.catalogWritePending, database.catalogFencePending)
					}
				} else {
					if !errors.Is(err, durable.ErrCommitOutcomeUnknown) ||
						!errors.Is(err, fenceFailure) {
						t.Fatalf("post-rename failure = %v, want outcome unknown", err)
					}
					assertTestViewGeneration(t, database, operation == "create", nil)
					if !database.catalogFencePending {
						t.Fatal("published view mutation lost its pending namespace fence")
					}
					assertCatalogFileView(t, path, operation == "create")

					database.syncDir = nil
					database.mu.Lock()
					settleErr := database.settleCatalogLocked()
					database.mu.Unlock()
					if settleErr != nil {
						t.Fatalf("settle published view fence: %v", settleErr)
					}
					if database.catalogFencePending {
						t.Fatal("successful settlement retained catalog fence")
					}
				}

				if err := database.closeTerminal(); err != nil {
					t.Fatal(err)
				}
				closed = true
				reopened, err := openDatabase(path)
				if err != nil {
					t.Fatalf("reopen after %s %s: %v", operation, phase, err)
				}
				defer reopened.closeTerminal()
				wantView := (phase == "pre-rename" && operation == "drop") ||
					(phase == "post-rename" && operation == "create")
				assertTestViewGeneration(t, reopened, wantView, nil)
			})
		}
	}
}

func openViewPublicationDatabase(
	t *testing.T,
	path string,
	withView bool,
) *database {
	t.Helper()
	database, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	create, err := query.PrepareDML(`CREATE TABLE docs (PRIMARY KEY (id))`)
	if err != nil {
		database.closeTerminal()
		t.Fatal(err)
	}
	if _, err := database.createTableLocked(create); err != nil {
		create.Release()
		database.closeTerminal()
		t.Fatal(err)
	}
	create.Release()
	if withView {
		if err := mutateTestView(database, "create"); err != nil {
			database.closeTerminal()
			t.Fatal(err)
		}
	}
	return database
}

func mutateTestView(database *database, operation string) error {
	statement := `CREATE VIEW selected AS SELECT id FROM docs`
	tree, err := sqlast.ParseStatement(statement)
	if err != nil {
		return err
	}
	if operation == "create" {
		_, err := database.createViewLocked(
			context.Background(), nil, tree.CreateView,
		)
		return err
	}
	tree, err = sqlast.ParseStatement(`DROP VIEW selected`)
	if err != nil {
		return err
	}
	_, err = database.dropViewLocked(context.Background(), tree.DropView)
	return err
}

func assertTestViewGeneration(
	t *testing.T,
	database *database,
	want bool,
	wantGeneration *viewMeta,
) {
	t.Helper()
	generation, exists := database.catalog.Views["selected"]
	if exists != want {
		t.Fatalf("selected view exists = %t, want %t", exists, want)
	}
	if wantGeneration != nil && generation != wantGeneration {
		t.Fatalf("selected view generation = %p, want original %p", generation, wantGeneration)
	}
}

func assertCatalogFileView(t *testing.T, path string, want bool) {
	t.Helper()
	raw, err := readCatalogFileBytes(path)
	if err != nil {
		t.Fatal(err)
	}
	var catalog catalogFile
	if err := json.Unmarshal(raw, &catalog); err != nil {
		t.Fatal(err)
	}
	_, exists := catalog.Views["selected"]
	if exists != want {
		t.Fatalf("published catalog view exists = %t, want %t", exists, want)
	}
}

func readCatalogFileBytes(path string) ([]byte, error) {
	raw, exists, err := readCatalogFile(path)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.New("catalog file does not exist")
	}
	return raw, nil
}
