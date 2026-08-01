package driver

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOpenRepairsLaggingMaterializationCatalogEntry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	var live *database
	t.Cleanup(func() {
		if live != nil {
			_ = live.close()
		}
	})

	database, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	live = database
	keys := prepareIncarnationTable(
		t, database, `CREATE INDEX by_kind ON docs (kind)`,
	)
	storage := database.tables["docs"].meta.Storage
	dataPath := database.tablePath("docs")
	if storage == "" {
		t.Fatal("materialized table did not receive a unique storage identity")
	}
	if _, err := os.Stat(dataPath); err != nil {
		t.Fatalf("catalog-owned durable storage before crash simulation: %v", err)
	}
	if err := database.close(); err != nil {
		t.Fatal(err)
	}
	live = nil

	catalog := readMaterializationRecoveryCatalog(t, path)
	if !catalog.Tables["docs"].Materialized {
		t.Fatal("fixture catalog was not materialized")
	}
	catalog.Tables["docs"].Materialized = false
	raw, err := json.MarshalIndent(catalog, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	recovered, err := openDatabase(path)
	if err != nil {
		t.Fatalf("reopen with valid storage and lagging catalog: %v", err)
	}
	live = recovered
	assertRecoveredMaterialization(t, recovered, storage, keys[0])

	repaired := readMaterializationRecoveryCatalog(t, path)
	assertRepairedMaterializationCatalog(t, repaired, storage)

	if err := recovered.close(); err != nil {
		t.Fatal(err)
	}
	live = nil

	reopened, err := openDatabase(path)
	if err != nil {
		t.Fatalf("second reopen after catalog repair: %v", err)
	}
	live = reopened
	assertRecoveredMaterialization(t, reopened, storage, keys[0])
	assertRepairedMaterializationCatalog(
		t, readMaterializationRecoveryCatalog(t, path), storage,
	)
}

func readMaterializationRecoveryCatalog(t *testing.T, path string) catalogFile {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var catalog catalogFile
	if err := json.Unmarshal(raw, &catalog); err != nil {
		t.Fatal(err)
	}
	return catalog
}

func assertRepairedMaterializationCatalog(
	t *testing.T,
	catalog catalogFile,
	storage string,
) {
	t.Helper()
	meta := catalog.Tables["docs"]
	if meta == nil {
		t.Fatal("repaired catalog lost docs table metadata")
	}
	if !meta.Materialized {
		t.Fatal("recovered catalog did not repair materialized=true")
	}
	if meta.Storage != storage {
		t.Fatalf("recovered catalog storage = %q, want %q", meta.Storage, storage)
	}
	if len(meta.Indexes) != 1 || meta.Indexes[0].Name != "by_kind" ||
		len(meta.Indexes[0].Paths) != 1 || meta.Indexes[0].Paths[0] != "/kind" {
		t.Fatalf("recovered catalog indexes = %+v, want by_kind(/kind)", meta.Indexes)
	}
}

func assertRecoveredMaterialization(
	t *testing.T,
	database *database,
	storage string,
	firstKey string,
) {
	t.Helper()
	table := database.tables["docs"]
	if table == nil || table.collection == nil {
		t.Fatal("reopen did not adopt the catalog-owned durable store")
	}
	if !table.meta.Materialized {
		t.Fatal("adopted table remains unmaterialized in memory")
	}
	if table.meta.Storage != storage {
		t.Fatalf("adopted storage = %q, want %q", table.meta.Storage, storage)
	}

	snapshot, err := table.collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if got := snapshotDocumentCount(t, snapshot); got != 2 {
		t.Fatalf("recovered document count = %d, want 2", got)
	}
	raw, found, err := snapshot.AppendRaw(nil, []byte(firstKey))
	if err != nil || !found || !strings.Contains(string(raw), `"id":"a"`) {
		t.Fatalf("recovered first row = (%s, %t, %v)", raw, found, err)
	}
	if got := snapshotIndexNames(snapshot); len(got) != 1 || got[0] != "by_kind" {
		t.Fatalf("recovered durable indexes = %v, want [by_kind]", got)
	}
}
