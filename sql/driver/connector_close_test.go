package driver

import (
	stdsql "database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/store/durable"
)

func openConnectorDB(t *testing.T, path string) (*stdsql.DB, *dbConnector) {
	t.Helper()
	raw, err := (Driver{}).OpenConnector(path)
	if err != nil {
		t.Fatal(err)
	}
	connector := raw.(*dbConnector)
	db := stdsql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	return db, connector
}

func TestConnectorTerminalCloseReleasesOwnedHandlesAfterFenceFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	db, connector := openConnectorDB(t, path)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?)`, `{"id":"kept"}`,
	); err != nil {
		t.Fatal(err)
	}

	database := connector.db
	database.mu.Lock()
	tableFile := database.tables["docs"].file
	lockFile := database.lockFile
	fenceFailure := errors.New("injected terminal catalog fence failure")
	database.catalogFencePending = true
	database.syncDir = func(string) error { return fenceFailure }
	database.mu.Unlock()

	if err := db.Close(); !errors.Is(err, fenceFailure) {
		t.Fatalf("DB.Close = %v, want injected fence failure", err)
	}
	if connector.db != nil {
		t.Fatal("terminal connector retained its closed database")
	}
	if !database.closeDone || database.lockFile != nil {
		t.Fatalf(
			"terminal database state = (closeDone=%t, lockFile=%p), want (true, nil)",
			database.closeDone, database.lockFile,
		)
	}
	if database.tables["docs"].file != nil ||
		database.tables["docs"].collection != nil {
		t.Fatal("terminal database retained a table handle")
	}
	if _, err := tableFile.Stat(); err == nil {
		t.Fatal("terminal close left the table descriptor open")
	}
	if _, err := lockFile.Stat(); err == nil {
		t.Fatal("terminal close left the catalog lock descriptor open")
	}

	reopened, err := stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	var id string
	if err := reopened.QueryRow(
		`SELECT id FROM docs WHERE id = ?`, "kept",
	).Scan(&id); err != nil || id != "kept" {
		t.Fatalf("reopened row = (%q, %v), want (kept, nil)", id, err)
	}
}

func TestConnectorTerminalCloseContinuesAfterCollectionCloseFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	db, connector := openConnectorDB(t, path)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO docs VALUES (?)`, `{"id":"kept"}`,
	); err != nil {
		t.Fatal(err)
	}

	database := connector.db
	database.mu.Lock()
	tableFile := database.tables["docs"].file
	lockFile := database.lockFile
	collectionFailure := errors.New("injected terminal collection close failure")
	database.closeCollection = func(collection *durable.Collection) error {
		return errors.Join(collection.Close(), collectionFailure)
	}
	database.mu.Unlock()

	if err := db.Close(); !errors.Is(err, collectionFailure) {
		t.Fatalf("DB.Close = %v, want injected collection close failure", err)
	}
	if connector.db != nil {
		t.Fatal("terminal connector retained its failed-close database")
	}
	if !database.closeDone || database.lockFile != nil {
		t.Fatalf(
			"terminal database state = (closeDone=%t, lockFile=%p), want (true, nil)",
			database.closeDone, database.lockFile,
		)
	}
	if database.tables["docs"].file != nil ||
		database.tables["docs"].collection != nil {
		t.Fatal("terminal database retained a table handle after close failure")
	}
	if _, err := tableFile.Stat(); err == nil {
		t.Fatal("collection close failure stranded the table descriptor")
	}
	if _, err := lockFile.Stat(); err == nil {
		t.Fatal("collection close failure stranded the catalog lock descriptor")
	}

	reopened, err := stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	var id string
	if err := reopened.QueryRow(
		`SELECT id FROM docs WHERE id = ?`, "kept",
	).Scan(&id); err != nil || id != "kept" {
		t.Fatalf("reopened row = (%q, %v), want (kept, nil)", id, err)
	}
}
