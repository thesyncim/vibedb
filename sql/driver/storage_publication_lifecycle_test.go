package driver

import (
	"context"
	sqldriver "database/sql/driver"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestStoragePublicationCrashReopenJournalFirstStoreSecond(t *testing.T) {
	tests := []struct {
		name         string
		fenceJournal bool
		publishStore bool
		fenceStore   bool
	}{
		{
			name: "journal visible before fence",
		},
		{
			name:         "journal fenced before store",
			fenceJournal: true,
		},
		{
			name:         "store visible before fence",
			fenceJournal: true,
			publishStore: true,
		},
		{
			name:         "store fenced before catalog",
			fenceJournal: true,
			publishStore: true,
			fenceStore:   true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			catalogPath := filepath.Join(t.TempDir(), "catalog.vdb")
			database, table, finalPath := prepareUnmaterializedPublicationTable(
				t, catalogPath,
			)

			tmpPath, key := buildClosedPublicationCandidate(t, database, table)
			if err := database.close(); err != nil {
				t.Fatal(err)
			}

			tmpJournal := durable.RecoveryJournalPath(tmpPath)
			finalJournal := durable.RecoveryJournalPath(finalPath)
			if err := publishNewPath(tmpJournal, finalJournal); err != nil {
				t.Fatalf("publish candidate journal: %v", err)
			}
			if test.fenceJournal {
				if err := syncDirectory(filepath.Dir(finalPath)); err != nil {
					t.Fatalf("fence candidate journal: %v", err)
				}
			}
			if test.publishStore {
				if err := publishNewPath(tmpPath, finalPath); err != nil {
					t.Fatalf("publish candidate store: %v", err)
				}
			}
			if test.fenceStore {
				if err := syncDirectory(filepath.Dir(finalPath)); err != nil {
					t.Fatalf("fence candidate store: %v", err)
				}
			}

			reopened, err := openDatabase(catalogPath)
			if err != nil {
				t.Fatalf("reopen publication boundary: %v", err)
			}
			t.Cleanup(func() { _ = reopened.close() })

			got := reopened.tables["docs"]
			if got == nil {
				t.Fatal("reopen lost the catalog-owned table")
			}
			if test.publishStore {
				assertAdoptedPublicationCandidate(t, got, key)
				repaired := readMaterializationRecoveryCatalog(t, catalogPath)
				if meta := repaired.Tables["docs"]; meta == nil || !meta.Materialized {
					t.Fatalf("recovered catalog metadata = %+v, want materialized", meta)
				}
				assertPathExists(t, finalPath)
				assertPathExists(t, finalJournal)
			} else {
				if got.collection != nil || got.file != nil || got.meta.Materialized {
					t.Fatalf(
						"journal-only reopen adopted incomplete storage: collection=%p file=%p materialized=%t",
						got.collection, got.file, got.meta.Materialized,
					)
				}
				assertPathAbsent(t, finalPath)
				assertPathAbsent(t, finalJournal)
			}
			assertPathAbsent(t, tmpPath)
			assertPathAbsent(t, tmpJournal)
		})
	}
}

func TestDiscardTableStorageRetainsOwnershipUntilCloseCompletes(t *testing.T) {
	directory := t.TempDir()
	meta := &tableMeta{
		PrimaryKey: "/id",
		Storage:    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	candidate := &table{meta: meta}
	database := &database{dataDir: directory}
	path := database.tablePathForMeta("docs", meta)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	collection, err := durable.Create(file, durableOptions(candidate))
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	candidate.file = file
	candidate.collection = collection
	t.Cleanup(func() {
		if !collection.CloseCompleted() {
			_ = collection.Close()
		}
		_ = file.Close()
		_ = os.Remove(path)
		_ = os.Remove(durable.RecoveryJournalPath(path))
	})
	if _, err := collection.Put([]byte("seed"), []byte(`{"id":"seed"}`)); err != nil {
		t.Fatal(err)
	}

	closeFailure := errors.New("injected incomplete collection close")
	closeCalls := 0
	database.closeCollection = func(got *durable.Collection) error {
		if got != collection {
			t.Fatalf("close candidate = %p, want %p", got, collection)
		}
		closeCalls++
		if closeCalls == 1 {
			return closeFailure
		}
		return got.Close()
	}

	err = database.discardTableStorageLocked("docs", candidate)
	if !errors.Is(err, closeFailure) {
		t.Fatalf("first discard = %v, want %v", err, closeFailure)
	}
	if collection.CloseCompleted() {
		t.Fatal("injected retryable close unexpectedly completed")
	}
	if candidate.collection != nil || candidate.file != nil {
		t.Fatalf(
			"candidate retained ownership after transfer: collection=%p file=%p",
			candidate.collection, candidate.file,
		)
	}
	if len(database.retired) != 1 ||
		database.retired[0].collection != collection ||
		database.retired[0].file != file {
		t.Fatalf("retry queue lost ownership: %+v", database.retired)
	}
	if _, err := file.Stat(); err != nil {
		t.Fatalf("incomplete close consumed owned descriptor: %v", err)
	}
	assertPathExists(t, path)
	assertPathExists(t, durable.RecoveryJournalPath(path))

	if err := database.settleDroppedTablesLocked(); err != nil {
		t.Fatalf("retry settlement: %v", err)
	}
	if closeCalls != 2 {
		t.Fatalf("collection close calls = %d, want 2", closeCalls)
	}
	if !collection.CloseCompleted() {
		t.Fatal("successful retry did not complete collection close")
	}
	if candidate.collection != nil || candidate.file != nil {
		t.Fatalf(
			"settled discard retained ownership: collection=%p file=%p",
			candidate.collection, candidate.file,
		)
	}
	if len(database.retired) != 0 {
		t.Fatalf("settled retry queue = %+v, want empty", database.retired)
	}
	assertPathAbsent(t, path)
	assertPathAbsent(t, durable.RecoveryJournalPath(path))
}

func TestRetiredConnectorTransfersIncompleteTerminalCloseToProcessOwner(t *testing.T) {
	catalogPath := filepath.Join(t.TempDir(), "catalog.vdb")
	connector, err := openConnector(catalogPath)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := connector.Connect(context.Background())
	if err != nil {
		_ = connector.Close()
		t.Fatal(err)
	}
	directExec(t, connection,
		`CREATE TABLE docs (id STRING PRIMARY KEY)`, nil)
	directExec(t, connection,
		`INSERT INTO docs VALUES (?)`, []sqldriver.NamedValue{{
			Ordinal: 1, Value: `{"id":"kept"}`,
		}})

	database := connector.db
	database.mu.RLock()
	collection := database.tables["docs"].collection
	database.mu.RUnlock()
	closeFailure := errors.New("injected retryable terminal close")
	var closeCalls atomic.Int32
	database.closeCollection = func(got *durable.Collection) error {
		if got != collection {
			t.Errorf("terminal close collection = %p, want %p", got, collection)
		}
		if closeCalls.Add(1) == 1 {
			return closeFailure
		}
		return got.Close()
	}

	// Retire the connector while its final connection is still live. The
	// connection release is database/sql's last guaranteed callback.
	if err := connector.Close(); err != nil {
		t.Fatal(err)
	}
	if err := connection.Close(); !errors.Is(err, closeFailure) {
		t.Fatalf("final connection close = %v, want %v", err, closeFailure)
	}

	deadline := time.Now().Add(5 * time.Second)
	for !database.closeCompleted() {
		if time.Now().After(deadline) {
			t.Fatal("process terminal owner did not finish retryable teardown")
		}
		time.Sleep(time.Millisecond)
	}
	if closeCalls.Load() < 2 {
		t.Fatalf("terminal close calls = %d, want at least 2", closeCalls.Load())
	}
	reopened, err := openDatabase(catalogPath)
	if err != nil {
		t.Fatalf("writer lock remained stranded after terminal retry: %v", err)
	}
	if err := reopened.close(); err != nil {
		t.Fatal(err)
	}
}

func TestTerminalCloseDrainsRetiredPathsAfterCompletedStickyError(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "retired.vdb")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	collection, err := durable.Create(file, durableOptions(&table{
		meta: &tableMeta{PrimaryKey: "/id"},
	}))
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if _, err := collection.Put(
		[]byte("seed"), []byte(`{"id":"seed"}`),
	); err != nil {
		_ = collection.Close()
		_ = file.Close()
		t.Fatal(err)
	}

	sticky := errors.New("injected completed close error")
	database := &database{
		dataDir: directory,
		retired: []retiredTable{{
			name: "docs", path: path,
			journal: durable.RecoveryJournalPath(path),
			file:    file, collection: collection,
		}},
	}
	database.closeCollection = func(got *durable.Collection) error {
		return errors.Join(got.Close(), sticky)
	}
	err = database.closeTerminal()
	if !errors.Is(err, sticky) {
		t.Fatalf("terminal close = %v, want sticky close error", err)
	}
	if !database.closeCompleted() {
		t.Fatal("completed sticky close did not finish terminal ownership")
	}
	assertPathAbsent(t, path)
	assertPathAbsent(t, durable.RecoveryJournalPath(path))
}

func prepareUnmaterializedPublicationTable(
	t *testing.T,
	catalogPath string,
) (*database, *table, string) {
	t.Helper()
	database, err := openDatabase(catalogPath)
	if err != nil {
		t.Fatal(err)
	}
	statement, err := query.PrepareDML(`CREATE TABLE docs (PRIMARY KEY (id))`)
	if err != nil {
		_ = database.close()
		t.Fatal(err)
	}
	database.mu.Lock()
	_, createErr := database.createTableLocked(statement)
	if createErr == nil {
		createErr = database.ensureDataDir()
	}
	table := database.tables["docs"]
	var finalPath string
	if table != nil {
		finalPath = database.tablePathForMeta("docs", table.meta)
	}
	database.mu.Unlock()
	statement.Release()
	if createErr != nil {
		_ = database.close()
		t.Fatal(createErr)
	}
	if table == nil || table.meta.Materialized || table.collection != nil || table.file != nil {
		_ = database.close()
		t.Fatalf("invalid unmaterialized table fixture: %+v", table)
	}
	return database, table, finalPath
}

func buildClosedPublicationCandidate(
	t *testing.T,
	database *database,
	table *table,
) (string, string) {
	t.Helper()
	finalPath := database.tablePathForMeta("docs", table.meta)
	file, err := createPublishableTableTemp(
		database.dataDir, "."+filepath.Base(finalPath)+".tmp-",
	)
	if err != nil {
		t.Fatal(err)
	}
	tmpPath := file.Name()
	collection, err := durable.Create(file, durableOptions(table))
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	key, err := primaryScalarKey("published")
	if err == nil {
		_, err = collection.Put(
			[]byte(key), []byte(`{"id":"published","value":1}`),
		)
	}
	if err != nil {
		_ = collection.Close()
		_ = file.Close()
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if !collection.CloseCompleted() {
		_ = file.Close()
		t.Fatal("candidate close returned success without completing teardown")
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	assertPathExists(t, tmpPath)
	assertPathExists(t, durable.RecoveryJournalPath(tmpPath))
	return tmpPath, key
}

func assertAdoptedPublicationCandidate(t *testing.T, table *table, key string) {
	t.Helper()
	if table.collection == nil || table.file == nil || !table.meta.Materialized {
		t.Fatalf(
			"complete publication was not adopted: collection=%p file=%p materialized=%t",
			table.collection, table.file, table.meta.Materialized,
		)
	}
	snapshot, err := table.collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	raw, found, readErr := snapshot.AppendRaw(nil, []byte(key))
	closeErr := snapshot.Close()
	if readErr != nil || closeErr != nil || !found || string(raw) != `{"id":"published","value":1}` {
		t.Fatalf(
			"adopted candidate row = (%s, %t, read %v, close %v)",
			raw, found, readErr, closeErr,
		)
	}
}

func assertPathExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected path %q: %v", path, err)
	}
}

func assertPathAbsent(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("path %q still exists (err %v)", path, err)
	}
}
