package driver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store/durable"
)

func prepareIncarnationTable(
	t *testing.T,
	database *database,
	indexes ...string,
) []string {
	t.Helper()
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
	for _, definition := range indexes {
		statement, prepareErr := query.PrepareDML(definition)
		if prepareErr != nil {
			t.Fatal(prepareErr)
		}
		database.mu.Lock()
		_, createErr := database.createIndexLocked(statement)
		database.mu.Unlock()
		statement.Release()
		if createErr != nil {
			t.Fatal(createErr)
		}
	}
	documents := [][]byte{
		[]byte(`{"id":"a","kind":"x","zone":"west"}`),
		[]byte(`{"id":"b","kind":"y","zone":"east"}`),
	}
	keys := make([]string, len(documents))
	seeds := make([]seedDocument, len(documents))
	for i := range documents {
		keys[i], err = primaryScalarKey(string(rune('a' + i)))
		if err != nil {
			t.Fatal(err)
		}
		seeds[i] = seedDocument{key: keys[i], document: documents[i]}
	}
	database.mu.Lock()
	_, err = database.materializeLocked("docs", seeds)
	database.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

func snapshotDocumentCount(t *testing.T, snapshot *durable.Snapshot) int {
	t.Helper()
	count := 0
	if err := snapshot.RangeRaw(func(_, _ []byte) error {
		count++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return count
}

func snapshotIndexNames(snapshot *durable.Snapshot) []string {
	infos := snapshot.AppendIndexes(nil)
	names := make([]string, len(infos))
	for i := range infos {
		names[i] = infos[i].Name
	}
	return names
}

func TestTruncateStorageIncarnationPreservesIndexesAndSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	database, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	keys := prepareIncarnationTable(
		t, database, `CREATE INDEX by_kind ON docs (kind)`,
	)

	database.mu.RLock()
	oldTable := database.tables["docs"]
	oldPath := database.tablePath("docs")
	oldStorage := oldTable.meta.Storage
	oldSnapshot, err := oldTable.collection.Snapshot()
	database.mu.RUnlock()
	if err != nil {
		t.Fatal(err)
	}

	database.mu.Lock()
	err = database.truncateTableStorageLockedContext(context.Background(), "docs")
	newTable := database.tables["docs"]
	newPath := database.tablePath("docs")
	retired := len(database.retired)
	database.mu.Unlock()
	if err != nil {
		_ = oldSnapshot.Close()
		t.Fatal(err)
	}
	if newTable == oldTable || newTable.meta.Storage == oldStorage ||
		newTable.meta.Storage == "" || newPath == oldPath {
		t.Fatalf(
			"incarnation did not change: old storage %q path %q, new storage %q path %q",
			oldStorage, oldPath, newTable.meta.Storage, newPath,
		)
	}
	if retired != 1 {
		t.Fatalf("retired incarnations = %d, want 1 active snapshot", retired)
	}
	if snapshotDocumentCount(t, oldSnapshot) != 2 {
		t.Fatal("old snapshot changed across TRUNCATE cutover")
	}
	if raw, found, readErr := oldSnapshot.AppendRaw(nil, []byte(keys[0])); readErr != nil || !found || !strings.Contains(string(raw), `"id":"a"`) {
		t.Fatalf("old snapshot row = (%s, %t, %v)", raw, found, readErr)
	}
	newSnapshot, err := newTable.collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if got := snapshotDocumentCount(t, newSnapshot); got != 0 {
		t.Fatalf("truncated document count = %d, want 0", got)
	}
	if got := snapshotIndexNames(newSnapshot); len(got) != 1 || got[0] != "by_kind" {
		t.Fatalf("truncated indexes = %v, want [by_kind]", got)
	}
	if err := newSnapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := oldSnapshot.Close(); err != nil {
		t.Fatal(err)
	}
	database.mu.Lock()
	err = database.settleCatalogLocked()
	database.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if _, statErr := os.Stat(oldPath); !os.IsNotExist(statErr) {
		t.Fatalf("retired storage = %v, want removed", statErr)
	}
	if _, statErr := os.Stat(newPath); statErr != nil {
		t.Fatalf("new storage after retirement: %v", statErr)
	}
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
	if got := snapshotDocumentCount(t, snapshot); got != 0 {
		t.Fatalf("reopened truncated document count = %d", got)
	}
	if got := snapshotIndexNames(snapshot); len(got) != 1 || got[0] != "by_kind" {
		t.Fatalf("reopened truncated indexes = %v", got)
	}
}

func TestDropIndexStorageIncarnationCopiesRowsAndReopens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	database, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	keys := prepareIncarnationTable(t, database,
		`CREATE INDEX by_kind ON docs (kind)`,
		`CREATE INDEX by_zone ON docs (zone)`,
	)
	database.mu.RLock()
	oldTable := database.tables["docs"]
	oldPath := database.tablePath("docs")
	oldSnapshot, err := oldTable.collection.Snapshot()
	database.mu.RUnlock()
	if err != nil {
		t.Fatal(err)
	}

	database.mu.Lock()
	err = database.dropIndexStorageLockedContext(
		context.Background(), "docs", "by_kind",
	)
	newTable := database.tables["docs"]
	newPath := database.tablePath("docs")
	database.mu.Unlock()
	if err != nil {
		_ = oldSnapshot.Close()
		t.Fatal(err)
	}
	if newTable == oldTable || newPath == oldPath {
		t.Fatal("DROP INDEX did not publish a new table incarnation")
	}
	if got := snapshotIndexNames(oldSnapshot); len(got) != 2 {
		t.Fatalf("old snapshot indexes = %v, want both indexes", got)
	}
	newSnapshot, err := newTable.collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if got := snapshotDocumentCount(t, newSnapshot); got != 2 {
		t.Fatalf("rebuilt document count = %d, want 2", got)
	}
	if got := snapshotIndexNames(newSnapshot); len(got) != 1 || got[0] != "by_zone" {
		t.Fatalf("rebuilt indexes = %v, want [by_zone]", got)
	}
	for _, key := range keys {
		if _, found, readErr := newSnapshot.AppendRaw(nil, []byte(key)); readErr != nil || !found {
			t.Fatalf("copied row %q = (found %t, err %v)", key, found, readErr)
		}
	}
	if err := newSnapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := oldSnapshot.Close(); err != nil {
		t.Fatal(err)
	}
	database.mu.Lock()
	err = database.settleCatalogLocked()
	database.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
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
	if got := snapshotDocumentCount(t, snapshot); got != 2 {
		t.Fatalf("reopened rebuilt document count = %d", got)
	}
	if got := snapshotIndexNames(snapshot); len(got) != 1 || got[0] != "by_zone" {
		t.Fatalf("reopened rebuilt indexes = %v", got)
	}
}

func TestDropIndexRebuildCommitsBoundedBatchNotPerRow(t *testing.T) {
	database, err := openDatabase(filepath.Join(t.TempDir(), "catalog.vdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.close()
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
	index, err := query.PrepareDML(`CREATE INDEX by_kind ON docs (kind)`)
	if err != nil {
		t.Fatal(err)
	}
	database.mu.Lock()
	_, err = database.createIndexLocked(index)
	database.mu.Unlock()
	index.Release()
	if err != nil {
		t.Fatal(err)
	}
	const documents = 8
	seeds := make([]seedDocument, documents)
	for i := range seeds {
		id := fmt.Sprintf("id-%02d", i)
		key, keyErr := primaryScalarKey(id)
		if keyErr != nil {
			t.Fatal(keyErr)
		}
		seeds[i] = seedDocument{
			key: key,
			document: []byte(fmt.Sprintf(
				`{"id":%q,"kind":"same"}`, id,
			)),
		}
	}
	database.mu.Lock()
	_, err = database.materializeLocked("docs", seeds)
	if err == nil {
		err = database.dropIndexStorageLockedContext(
			context.Background(), "docs", "by_kind",
		)
	}
	rebuilt := database.tables["docs"].collection
	database.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	// Create starts at generation 1. Eight per-row synchronous Puts would end
	// at generation 9; one bounded WriteBatch advances exactly once.
	if got := rebuilt.Generation(); got != 2 {
		t.Fatalf("eight-row rebuild generation = %d, want one batch at generation 2", got)
	}
	if got := rebuilt.Stats().Durability; got != durable.DurabilitySync {
		t.Fatalf("rebuilt durability = %d, want DurabilitySync", got)
	}
}

func TestReplacementCatalogFenceFailureKeepsPublishedIncarnation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	database, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	prepareIncarnationTable(t, database)
	database.mu.RLock()
	oldTable := database.tables["docs"]
	oldStorage := oldTable.meta.Storage
	oldSnapshot, err := oldTable.collection.Snapshot()
	database.mu.RUnlock()
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("injected replacement catalog fence failure")
	database.syncDir = func(path string) error {
		if path == filepath.Dir(database.path) {
			return failure
		}
		return syncDirectory(path)
	}
	database.mu.Lock()
	err = database.truncateTableStorageLockedContext(context.Background(), "docs")
	newTable := database.tables["docs"]
	pending := database.catalogFencePending
	database.mu.Unlock()
	if !errors.Is(err, durable.ErrCommitOutcomeUnknown) || !errors.Is(err, failure) {
		_ = oldSnapshot.Close()
		t.Fatalf("replacement fence failure = %v, want unknown outcome", err)
	}
	if newTable == oldTable || newTable.meta.Storage == oldStorage || !pending {
		_ = oldSnapshot.Close()
		t.Fatal("published replacement was rolled back after catalog fence failure")
	}
	if snapshotDocumentCount(t, oldSnapshot) != 2 {
		t.Fatal("old leased snapshot changed after published replacement")
	}
	database.syncDir = nil
	if err := oldSnapshot.Close(); err != nil {
		t.Fatal(err)
	}
	database.mu.Lock()
	err = database.settleCatalogLocked()
	database.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := database.close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	if reopened.tables["docs"].meta.Storage != newTable.meta.Storage {
		t.Fatal("reopen did not retain the catalog-published incarnation")
	}
}

func TestReplacementFileFenceFailureLeavesOldCatalogAuthoritative(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	database, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.close()
	prepareIncarnationTable(t, database)
	database.mu.RLock()
	oldTable := database.tables["docs"]
	oldStorage := oldTable.meta.Storage
	database.mu.RUnlock()
	failure := errors.New("injected replacement table fence failure")
	database.syncDir = func(path string) error {
		if path == database.dataDir {
			return failure
		}
		return syncDirectory(path)
	}
	database.mu.Lock()
	err = database.truncateTableStorageLockedContext(context.Background(), "docs")
	current := database.tables["docs"]
	pending := database.tableDirFencePending
	database.mu.Unlock()
	if !errors.Is(err, durable.ErrCommitOutcomeUnknown) || !errors.Is(err, failure) {
		t.Fatalf("replacement table fence failure = %v, want unknown outcome", err)
	}
	if current != oldTable || current.meta.Storage != oldStorage || !pending {
		t.Fatal("unfenced replacement changed the authoritative catalog incarnation")
	}
	database.syncDir = nil
	database.mu.Lock()
	err = database.settleCatalogLocked()
	database.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := current.collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if got := snapshotDocumentCount(t, snapshot); got != 2 {
		t.Fatalf("old catalog data after failed replacement = %d rows", got)
	}
}

func TestLegacyCatalogPathReopensAndReplacementUpgradesIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	database, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	prepareIncarnationTable(t, database)
	uniquePath := database.tablePath("docs")
	legacyPath := database.legacyTablePath("docs")
	if err := database.close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var catalog catalogFile
	if err := json.Unmarshal(raw, &catalog); err != nil {
		t.Fatal(err)
	}
	catalog.Tables["docs"].Storage = ""
	raw, err = json.MarshalIndent(catalog, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(uniquePath, legacyPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(
		durable.RecoveryJournalPath(uniquePath),
		durable.RecoveryJournalPath(legacyPath),
	); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}

	database, err = openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	if database.tables["docs"].meta.Storage != "" || database.tablePath("docs") != legacyPath {
		t.Fatal("legacy deterministic table path was not preserved on reopen")
	}
	database.mu.Lock()
	err = database.truncateTableStorageLockedContext(context.Background(), "docs")
	upgraded := database.tables["docs"].meta.Storage
	database.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if upgraded == "" {
		t.Fatal("replacement did not upgrade the legacy storage identity")
	}
	if err := database.close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	if reopened.tables["docs"].meta.Storage != upgraded {
		t.Fatal("upgraded storage identity did not survive reopen")
	}
}

func TestOpenRemovesOnlyUnreferencedManagedStorage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	database, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	prepareIncarnationTable(t, database)
	livePath := database.tablePath("docs")
	if err := database.close(); err != nil {
		t.Fatal(err)
	}
	orphanID := strings.Repeat("a", storageIdentityBytes*2)
	if filepath.Base(livePath) == orphanID+".vjc" {
		orphanID = strings.Repeat("b", storageIdentityBytes*2)
	}
	orphans := []string{
		filepath.Join(path+".tables", orphanID+".vjc"),
		filepath.Join(path+".tables", orphanID+".vjc.rjournal"),
		filepath.Join(path+".tables", "."+orphanID+".vjc.tmp-crash"),
	}
	for _, orphan := range orphans {
		if err := os.WriteFile(orphan, []byte("orphan"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	unmanaged := filepath.Join(path+".tables", "README")
	if err := os.WriteFile(unmanaged, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	reopened, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	for _, orphan := range orphans {
		if _, statErr := os.Stat(orphan); !os.IsNotExist(statErr) {
			t.Fatalf("orphan %s = %v, want removed", filepath.Base(orphan), statErr)
		}
	}
	if _, statErr := os.Stat(livePath); statErr != nil {
		t.Fatalf("live catalog storage removed: %v", statErr)
	}
	if _, statErr := os.Stat(unmanaged); statErr != nil {
		t.Fatalf("unmanaged private-directory file removed: %v", statErr)
	}
}

func TestWriteTransactionConflictsWithStorageReplacement(t *testing.T) {
	database, transaction, _ := beginRawDocsTransaction(
		t, []byte(`{"id":"base","kind":"x"}`),
	)
	insert, err := query.PrepareDML(`INSERT INTO docs VALUES (?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer insert.Release()
	if _, err := transaction.execMutation(
		insert, []any{`{"id":"pending","kind":"y"}`},
	); err != nil {
		t.Fatal(err)
	}
	database.mu.Lock()
	err = database.truncateTableStorageLockedContext(context.Background(), "docs")
	database.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(); !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("pre-replacement transaction commit = %v, want conflict", err)
	}
	database.mu.RLock()
	current := database.tables["docs"]
	database.mu.RUnlock()
	snapshot, err := current.collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if got := snapshotDocumentCount(t, snapshot); got != 0 {
		t.Fatalf("conflicting transaction published %d rows", got)
	}
}

func TestStorageIdentityValidationAndRetirementBounds(t *testing.T) {
	for _, identity := range []string{"short", strings.Repeat("A", storageIdentityBytes*2)} {
		if err := validateStorageIdentity(identity); err == nil {
			t.Fatalf("invalid storage identity %q accepted", identity)
		}
	}
	if err := validateStorageIdentity(strings.Repeat("a", storageIdentityBytes*2)); err != nil {
		t.Fatalf("canonical storage identity: %v", err)
	}
	database := &database{retired: make([]retiredTable, maxRetiredTables)}
	if err := database.checkRetirementCapacityLocked(1); !errors.Is(err, ErrTooManyRetiredTables) {
		t.Fatalf("retirement overflow = %v, want ErrTooManyRetiredTables", err)
	}
}

func TestStorageReplacementRejectsRetirementOverflow(t *testing.T) {
	database, err := openDatabase(filepath.Join(t.TempDir(), "catalog.vdb"))
	if err != nil {
		t.Fatal(err)
	}
	prepareIncarnationTable(t, database)
	database.mu.Lock()
	oldTable := database.tables["docs"]
	for i := 0; i < maxRetiredTables; i++ {
		database.retired = append(database.retired, retiredTable{
			name: "leased", collection: new(durable.Collection),
		})
	}
	database.closeCollection = func(*durable.Collection) error {
		return storeio.ErrLeasesActive
	}
	err = database.truncateTableStorageLockedContext(context.Background(), "docs")
	current := database.tables["docs"]
	database.retired = nil
	database.closeCollection = nil
	database.mu.Unlock()
	if !errors.Is(err, ErrTooManyRetiredTables) {
		t.Fatalf("TRUNCATE retirement overflow = %v", err)
	}
	if current != oldTable {
		t.Fatal("retirement overflow published a replacement")
	}
	if err := database.close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenRejectsInvalidAndDuplicateStorageIdentities(t *testing.T) {
	validIdentity := strings.Repeat("a", storageIdentityBytes*2)
	tests := []struct {
		name string
		make func() catalogFile
		want string
	}{
		{
			name: "invalid identity",
			make: func() catalogFile {
				meta := boundedCatalogTableMeta(false)
				meta.Storage = "not-hex"
				return catalogFile{Version: catalogVersion,
					Tables: map[string]*tableMeta{"docs": meta}}
			},
			want: "storage identity",
		},
		{
			name: "duplicate identity",
			make: func() catalogFile {
				first := boundedCatalogTableMeta(false)
				second := boundedCatalogTableMeta(false)
				first.Storage, second.Storage = validIdentity, validIdentity
				return catalogFile{Version: catalogVersion,
					Tables: map[string]*tableMeta{"first": first, "second": second}}
			},
			want: "share storage identity",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "catalog.vdb")
			raw, err := json.Marshal(test.make())
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			database, err := openDatabase(path)
			if database != nil {
				_ = database.closeTerminal()
				t.Fatal("invalid storage catalog opened")
			}
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("open invalid storage catalog = %v, want %q", err, test.want)
			}
		})
	}
}
