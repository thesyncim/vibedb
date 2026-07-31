package driver

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestCatalogFenceFailureKeepsPublishedDefinition(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	database, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}

	create, err := query.PrepareDML(`CREATE TABLE docs (PRIMARY KEY (id))`)
	if err != nil {
		t.Fatal(err)
	}
	defer create.Release()

	fenceFailure := errors.New("injected catalog directory fence failure")
	database.syncDir = func(string) error { return fenceFailure }
	database.mu.Lock()
	_, err = database.createTableLocked(create)
	database.mu.Unlock()
	if !errors.Is(err, durable.ErrCommitOutcomeUnknown) ||
		!errors.Is(err, fenceFailure) {
		t.Fatalf("CREATE TABLE fence failure = %v, want unknown outcome", err)
	}
	database.mu.RLock()
	_, tableRetained := database.tables["docs"]
	_, catalogRetained := database.catalog.Tables["docs"]
	database.mu.RUnlock()
	if !tableRetained || !catalogRetained {
		t.Fatal("published catalog definition was rolled back in memory")
	}
	if !database.catalogFencePending {
		t.Fatal("failed catalog namespace fence was not retained for retry")
	}

	database.syncDir = nil
	if err := database.close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	if _, exists := reopened.tables["docs"]; !exists {
		t.Fatal("renamed catalog did not contain the retained table")
	}
}

func TestIndexFenceFailureKeepsPublishedDefinition(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	database, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}

	create, err := query.PrepareDML(`CREATE TABLE docs (PRIMARY KEY (id))`)
	if err != nil {
		t.Fatal(err)
	}
	defer create.Release()
	database.mu.Lock()
	_, err = database.createTableLocked(create)
	database.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	index, err := query.PrepareDML(`CREATE INDEX by_kind ON docs (kind)`)
	if err != nil {
		t.Fatal(err)
	}
	defer index.Release()
	fenceFailure := errors.New("injected index directory fence failure")
	database.syncDir = func(string) error { return fenceFailure }
	database.mu.Lock()
	_, err = database.createIndexLocked(index)
	database.mu.Unlock()
	if !errors.Is(err, durable.ErrCommitOutcomeUnknown) ||
		!errors.Is(err, fenceFailure) {
		t.Fatalf("CREATE INDEX fence failure = %v, want unknown outcome", err)
	}
	if got := len(database.tables["docs"].meta.Indexes); got != 1 {
		t.Fatalf("published index count = %d, want 1", got)
	}

	database.syncDir = nil
	if err := database.close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	if got := len(reopened.tables["docs"].meta.Indexes); got != 1 ||
		reopened.tables["docs"].meta.Indexes[0].Name != "by_kind" {
		t.Fatalf("reopened indexes = %+v, want by_kind", reopened.tables["docs"].meta.Indexes)
	}
}

func TestTableFileFenceFailureKeepsCommittedFirstWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	database, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}

	create, err := query.PrepareDML(`CREATE TABLE docs (PRIMARY KEY (id))`)
	if err != nil {
		t.Fatal(err)
	}
	defer create.Release()
	database.mu.Lock()
	_, err = database.createTableLocked(create)
	database.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	key, err := primaryScalarKey("a")
	if err != nil {
		t.Fatal(err)
	}
	fenceFailure := errors.New("injected table directory fence failure")
	database.syncDir = func(path string) error {
		if path == database.dataDir {
			return fenceFailure
		}
		return syncDirectory(path)
	}
	database.mu.Lock()
	table, err := database.materializeLocked("docs", []seedDocument{{
		key: key, document: []byte(`{"id":"a","kind":"x"}`),
	}})
	database.mu.Unlock()
	if !errors.Is(err, durable.ErrCommitOutcomeUnknown) ||
		!errors.Is(err, fenceFailure) {
		t.Fatalf("first-write fence failure = %v, want unknown outcome", err)
	}
	if table == nil || table.collection == nil {
		t.Fatal("committed first write was discarded after namespace publication")
	}
	if _, statErr := os.Stat(database.tablePath("docs")); statErr != nil {
		t.Fatalf("published table file: %v", statErr)
	}
	if !database.tableDirFencePending {
		t.Fatal("failed table namespace fence was not retained for retry")
	}

	var retried []string
	database.syncDir = func(path string) error {
		retried = append(retried, path)
		return syncDirectory(path)
	}
	insert, err := query.PrepareDML(`INSERT INTO docs VALUES (?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer insert.Release()
	connection := &conn{db: database}
	if _, err := connection.execMutation(
		insert, []any{`{"id":"b","kind":"y"}`},
	); err != nil {
		t.Fatalf("write after retryable namespace failure: %v", err)
	}
	if len(retried) == 0 || retried[0] != database.dataDir {
		t.Fatalf("retried fences = %q, want table directory first", retried)
	}
	if database.tableDirFencePending {
		t.Fatal("successful retry did not clear table namespace fence")
	}

	database.syncDir = nil
	if err := database.close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	snapshot, err := reopened.tables["docs"].collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	raw, found, err := snapshot.AppendRaw(nil, []byte(key))
	if err != nil {
		t.Fatal(err)
	}
	if !found || string(raw) != `{"id":"a","kind":"x"}` {
		t.Fatalf("reopened first write = (%s, %t), want committed document", raw, found)
	}
	secondKey, err := primaryScalarKey("b")
	if err != nil {
		t.Fatal(err)
	}
	if raw, found, err = snapshot.AppendRaw(raw[:0], []byte(secondKey)); err != nil {
		t.Fatal(err)
	}
	if !found || string(raw) != `{"id":"b","kind":"y"}` {
		t.Fatalf("reopened second write = (%s, %t), want committed document", raw, found)
	}
}

func TestReopenRefusesToAdoptTableBeforeRecoveryFence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	database, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	create, err := query.PrepareDML(`CREATE TABLE docs (PRIMARY KEY (id))`)
	if err != nil {
		t.Fatal(err)
	}
	defer create.Release()
	database.mu.Lock()
	_, err = database.createTableLocked(create)
	database.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	key, err := primaryScalarKey("kept")
	if err != nil {
		t.Fatal(err)
	}
	database.mu.Lock()
	_, err = database.materializeLocked("docs", []seedDocument{{
		key: key, document: []byte(`{"id":"kept"}`),
	}})
	database.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := database.close(); err != nil {
		t.Fatal(err)
	}

	fenceFailure := errors.New("injected recovery fence failure")
	dataDir := database.dataDir
	failed, err := openDatabaseWithSync(path, func(candidate string) error {
		if candidate == dataDir {
			return fenceFailure
		}
		return syncDirectory(candidate)
	})
	if failed != nil {
		_ = failed.closeTerminal()
		t.Fatal("reopen returned a database after its recovery fence failed")
	}
	if !errors.Is(err, durable.ErrCommitOutcomeUnknown) ||
		!errors.Is(err, fenceFailure) {
		t.Fatalf("recovery fence failure = %v, want unknown outcome", err)
	}

	// The failed open must release its writer lease, and a later successful
	// recovery must see the still-intact document.
	reopened, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	raw, found, err := reopened.tables["docs"].collection.AppendRaw(nil, []byte(key))
	if err != nil {
		t.Fatal(err)
	}
	if !found || string(raw) != `{"id":"kept"}` {
		t.Fatalf("post-recovery row = (%s, %t), want committed document", raw, found)
	}
}

func TestFirstInsertRetriesPublishedCatalogFenceBeforeSuccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	database, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.close()

	database.mu.Lock()
	if err := database.ensureDataDir(); err != nil {
		database.mu.Unlock()
		t.Fatal(err)
	}
	database.mu.Unlock()

	create, err := query.PrepareDML(`CREATE TABLE docs (PRIMARY KEY (id))`)
	if err != nil {
		t.Fatal(err)
	}
	defer create.Release()
	fenceFailure := errors.New("injected catalog namespace fence failure")
	parent := filepath.Dir(database.path)
	failed := false
	database.syncDir = func(path string) error {
		if path == parent && !failed {
			failed = true
			return fenceFailure
		}
		return syncDirectory(path)
	}
	database.mu.Lock()
	_, err = database.createTableLocked(create)
	database.mu.Unlock()
	if !errors.Is(err, durable.ErrCommitOutcomeUnknown) ||
		!errors.Is(err, fenceFailure) {
		t.Fatalf("CREATE TABLE fence failure = %v, want unknown outcome", err)
	}

	var fences []string
	database.syncDir = func(path string) error {
		fences = append(fences, path)
		return syncDirectory(path)
	}
	insert, err := query.PrepareDML(`INSERT INTO docs VALUES (?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer insert.Release()
	if _, err := (&conn{db: database}).execMutation(
		insert, []any{`{"id":"a"}`},
	); err != nil {
		t.Fatalf("first INSERT after catalog fence retry: %v", err)
	}
	if len(fences) < 2 ||
		fences[0] != parent ||
		fences[1] != database.dataDir {
		t.Fatalf(
			"namespace fence order = %q, want catalog parent then table directory",
			fences,
		)
	}
	if database.catalogFencePending || database.tableDirFencePending {
		t.Fatalf(
			"successful INSERT left pending fences: catalog=%t table=%t",
			database.catalogFencePending, database.tableDirFencePending,
		)
	}
}

func TestFailedFirstWriteCleansTemporaryTableFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	database, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.close()

	create, err := query.PrepareDML(`CREATE TABLE docs (PRIMARY KEY (id))`)
	if err != nil {
		t.Fatal(err)
	}
	defer create.Release()
	database.mu.Lock()
	_, err = database.createTableLocked(create)
	database.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	key, err := primaryScalarKey("broken")
	if err != nil {
		t.Fatal(err)
	}
	fenceFailure := errors.New("injected rollback directory fence failure")
	database.syncDir = func(path string) error {
		if path == database.dataDir {
			return fenceFailure
		}
		return syncDirectory(path)
	}
	database.mu.Lock()
	table, err := database.materializeLocked("docs", []seedDocument{{
		key: key, document: []byte(`{"id":"broken"`),
	}})
	database.mu.Unlock()
	if table != nil {
		t.Fatal("failed first write returned a materialized table")
	}
	if errors.Is(err, durable.ErrCommitOutcomeUnknown) ||
		!errors.Is(err, fenceFailure) {
		t.Fatalf(
			"failed-write temporary cleanup fence = %v, want known failure",
			err,
		)
	}
	if database.tables["docs"].collection != nil {
		t.Fatal("failed first write installed a collection")
	}
	if _, statErr := os.Stat(database.tablePath("docs")); !os.IsNotExist(statErr) {
		t.Fatalf("failed first-write file = %v, want absent", statErr)
	}
}

func TestCatalogPathRequiresExistingParent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "catalog.vdb")
	if database, err := openDatabase(path); err == nil {
		_ = database.close()
		t.Fatal("openDatabase created missing DSN ancestors")
	}
	if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
		t.Fatalf("missing DSN parent = %v, want absent", err)
	}
}

func TestReopenRejectsMissingMaterializedTableFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	database, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	create, err := query.PrepareDML(`CREATE TABLE docs (PRIMARY KEY (id))`)
	if err != nil {
		t.Fatal(err)
	}
	database.mu.Lock()
	_, err = database.createTableLocked(create)
	database.mu.Unlock()
	create.Release()
	if err != nil {
		t.Fatal(err)
	}
	key, err := primaryScalarKey("a")
	if err != nil {
		t.Fatal(err)
	}
	database.mu.Lock()
	_, err = database.materializeLocked("docs", []seedDocument{{
		key: key, document: []byte(`{"id":"a"}`),
	}})
	tablePath := database.tablePath("docs")
	materialized := database.tables["docs"].meta.Materialized
	database.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if !materialized {
		t.Fatal("first committed write did not persist table identity")
	}
	if err := database.close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(tablePath); err != nil {
		t.Fatal(err)
	}
	reopened, err := openDatabase(path)
	if err == nil {
		_ = reopened.close()
		t.Fatal("reopen accepted a missing materialized table file")
	}
	if got := err.Error(); !strings.Contains(got, "materialized SQL table") {
		t.Fatalf("missing-file error = %q, want materialized-table integrity failure", got)
	}
}

func TestCatalogSymlinkUsesCanonicalLockAndTableDirectory(t *testing.T) {
	root := t.TempDir()
	realPath := filepath.Join(root, "real.vdb")
	database, err := openDatabase(realPath)
	if err != nil {
		t.Fatal(err)
	}
	canonicalRealPath := database.path
	create, err := query.PrepareDML(`CREATE TABLE docs (PRIMARY KEY (id))`)
	if err != nil {
		t.Fatal(err)
	}
	database.mu.Lock()
	_, err = database.createTableLocked(create)
	database.mu.Unlock()
	create.Release()
	if err != nil {
		t.Fatal(err)
	}
	key, err := primaryScalarKey("a")
	if err != nil {
		t.Fatal(err)
	}
	database.mu.Lock()
	_, err = database.materializeLocked("docs", []seedDocument{{
		key: key, document: []byte(`{"id":"a"}`),
	}})
	database.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := database.close(); err != nil {
		t.Fatal(err)
	}

	aliasPath := filepath.Join(root, "alias.vdb")
	if err := os.Symlink(realPath, aliasPath); err != nil {
		t.Skipf("catalog symlink unavailable: %v", err)
	}
	reopened, err := openDatabase(aliasPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	if reopened.path != canonicalRealPath ||
		reopened.dataDir != canonicalRealPath+".tables" {
		t.Fatalf(
			"canonical paths = (%q, %q), want (%q, %q)",
			reopened.path, reopened.dataDir,
			canonicalRealPath, canonicalRealPath+".tables",
		)
	}
	snapshot, err := reopened.tables["docs"].collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if _, found, err := snapshot.AppendRaw(nil, []byte(key)); err != nil || !found {
		t.Fatalf("read through catalog symlink = (found %t, err %v)", found, err)
	}
}
