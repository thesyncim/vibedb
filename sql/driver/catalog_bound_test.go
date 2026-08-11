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

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store"
)

func TestOpenRejectsCatalogLargerThanMetadataBound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oversized.vdb")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(maxCatalogBytes + 1); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	database, err := openDatabase(path)
	if database != nil {
		_ = database.closeTerminal()
		t.Fatal("oversized catalog returned a database")
	}
	if !errors.Is(err, ErrCatalogTooLarge) {
		t.Fatalf("open oversized catalog = %v, want ErrCatalogTooLarge", err)
	}
}

func TestPersistRejectsProspectiveCatalogBeforeEncoding(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	database := &database{
		path: path,
		catalog: catalogFile{
			Version: catalogVersion,
			Tables: map[string]*tableMeta{
				"docs": {
					PrimaryKey: "/id",
					Indexes: []indexMeta{{
						Name: "oversized",
						Paths: []string{
							strings.Repeat("x", maxCatalogBytes),
						},
					}},
				},
			},
		},
	}

	published, err := database.persistCatalogLocked()
	if published {
		t.Fatal("oversized prospective catalog was published")
	}
	if !errors.Is(err, ErrCatalogTooLarge) {
		t.Fatalf("persist oversized catalog = %v, want ErrCatalogTooLarge", err)
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("rejected catalog file = %v, want absent", statErr)
	}
}

func TestCatalogSizePreflightCoversIndentedEncoding(t *testing.T) {
	catalog := catalogFile{
		Version: catalogVersion,
		Tables: map[string]*tableMeta{
			"quotes\"<&": {
				PrimaryKey: "/line\nid",
				Schema: &schemaMeta{
					Root: 1,
					Fields: []schemaFieldMeta{{
						Path: "/emoji/\u2028/😀", Types: 7, Required: true,
					}},
				},
				Indexes: []indexMeta{{
					Name: "by\\kind",
					Paths: []string{
						"/kind", "/nested/\tvalue",
					},
				}},
				Materialized: true,
			},
		},
	}
	bound, err := catalogSizeUpperBound(catalog)
	if err != nil {
		t.Fatalf("ordinary catalog preflight: %v", err)
	}
	raw, err := json.MarshalIndent(catalog, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > bound {
		t.Fatalf("indented encoding = %d bytes, preflight upper bound %d", len(raw), bound)
	}
}

func TestCatalogSizePreflightAccountsForV2PlacementShardKey(t *testing.T) {
	_, database, base := bindReplicatedApplyTestRoot(t, "catalog-v2-bound")
	claim, _, err := database.OpenReplicatedApply(
		base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = claim.Close()
		_ = database.Close()
	}()

	core := database.connector.db
	core.mu.RLock()
	raw, err := json.Marshal(core.catalog)
	core.mu.RUnlock()
	if err != nil {
		t.Fatal(err)
	}
	var v2 catalogFile
	if err := json.Unmarshal(raw, &v2); err != nil {
		t.Fatal(err)
	}
	primary := "/" + strings.Repeat("x", 4096)
	v2.ReplicatedShardStore.UserPrimaryKey = primary
	v2.Tables[v2.ReplicatedShardStore.UserTable].PrimaryKey = primary
	v2.ReplicatedApply.Placement.ShardKey = primary
	v2.ReplicatedApply.ValidationDigest = replicatedApplyProfileDigestV2(
		*v2.ReplicatedShardStore, v2.ReplicatedApply.Placement,
	)
	v2Bound, err := catalogSizeUpperBound(v2)
	if err != nil {
		t.Fatalf("v2 catalog bound: %v", err)
	}

	v2Raw, err := json.Marshal(v2)
	if err != nil {
		t.Fatal(err)
	}
	var v1 catalogFile
	if err := json.Unmarshal(v2Raw, &v1); err != nil {
		t.Fatal(err)
	}
	v1.ReplicatedApply.Format = ReplicatedApplyFormatV1
	v1.ReplicatedApply.ValidationProfile = uint8(replicatedstate.ValidationDeterministicMutationV1)
	v1.ReplicatedApply.Placement = ReplicatedPlacementProfile{}
	v1.ReplicatedApply.ValidationDigest = replicatedApplyProfileDigest(*v1.ReplicatedShardStore)
	v1Bound, err := catalogSizeUpperBound(v1)
	if err != nil {
		t.Fatalf("v1 catalog bound: %v", err)
	}
	if delta, want := v2Bound-v1Bound, encodedJSONStringBytes(primary); delta != want {
		t.Fatalf("v2 placement bound delta = %d, want exact encoded shard-key bytes %d", delta, want)
	}
}

func boundedCatalogTableMeta(materialized bool) *tableMeta {
	return &tableMeta{
		PrimaryKey: "/id",
		Schema: &schemaMeta{
			Root: uint16(store.SchemaObject),
			Fields: []schemaFieldMeta{{
				Path: "/id", Types: uint16(store.SchemaString), Required: true,
			}},
		},
		Materialized: materialized,
	}
}

func TestOpenRejectsTableCountBeforeOpeningMaterializedHandles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "too-many-tables.vdb")
	catalog := catalogFile{
		Version: catalogVersion,
		Tables:  make(map[string]*tableMeta, maxCatalogTables+1),
	}
	for i := 0; i <= maxCatalogTables; i++ {
		catalog.Tables[fmt.Sprintf("table_%03d", i)] =
			boundedCatalogTableMeta(true)
	}
	raw, err := json.Marshal(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > maxCatalogBytes {
		t.Fatalf("table-count fixture is %d bytes, larger than byte bound", len(raw))
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	database, err := openDatabase(path)
	if database != nil {
		_ = database.closeTerminal()
		t.Fatal("over-count catalog returned a database")
	}
	if !errors.Is(err, ErrTooManyTables) ||
		!strings.Contains(err.Error(), "maximum is") {
		t.Fatalf("open over-count catalog = %v, want table-count bound", err)
	}
	// Every entry claimed to be materialized, but no table file exists. Seeing
	// the count error rather than a missing-file error proves the catalog was
	// rejected before the eager handle-open loop began.
	if strings.Contains(err.Error(), "missing its data file") {
		t.Fatalf("catalog opened table handles before enforcing count: %v", err)
	}
}

func TestCreateTableRejectsProspectiveTableCount(t *testing.T) {
	database := &database{
		catalog: catalogFile{
			Version: catalogVersion,
			Tables:  make(map[string]*tableMeta, maxCatalogTables),
		},
		tables: make(map[string]*table, maxCatalogTables),
	}
	for i := 0; i < maxCatalogTables; i++ {
		name := fmt.Sprintf("table_%03d", i)
		meta := boundedCatalogTableMeta(false)
		database.catalog.Tables[name] = meta
		database.tables[name] = &table{meta: meta}
	}
	statement, err := query.PrepareDML(
		`CREATE TABLE overflow (id STRING PRIMARY KEY)`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()

	if _, err := database.createTableLockedContext(
		context.Background(), statement,
	); !errors.Is(err, ErrTooManyTables) {
		t.Fatalf("CREATE TABLE above catalog count = %v, want ErrTooManyTables", err)
	}
	if _, exists := database.tables["overflow"]; exists {
		t.Fatal("rejected CREATE TABLE mutated the live catalog")
	}
	if _, exists := database.catalog.Tables["overflow"]; exists {
		t.Fatal("rejected CREATE TABLE mutated persisted catalog metadata")
	}
}
