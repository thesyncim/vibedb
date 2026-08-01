package driver

import (
	stdsql "database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
)

// oneShardConfig builds a degenerate single-shard cluster configuration placing
// table on the given shard-key columns over the whole-keyspace manifest.
func oneShardConfig(t *testing.T, table string, columns ...string) distribution.ClusterConfig {
	t.Helper()
	return distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{
			{Name: "d", Arity: len(columns), MapperVersion: distribution.NativeMapperVersion},
		},
		Placements: []distribution.TablePlacement{
			{Table: table, Distribution: "d", Columns: columns},
		},
		Manifests: []*distribution.Manifest{fullManifest(t)},
	}
}

// openTestCluster opens a fresh single-shard cluster database at a temp path.
func openTestCluster(t *testing.T, cfg distribution.ClusterConfig) *stdsql.DB {
	t.Helper()
	db, err := OpenCluster(filepath.Join(t.TempDir(), "catalog.vdb"), cfg)
	if err != nil {
		t.Fatalf("OpenCluster: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

func TestOpenClusterSingleShardEndToEnd(t *testing.T) {
	db := openTestCluster(t, oneShardConfig(t, "docs", "/id"))
	if _, err := db.Exec(
		`CREATE TABLE docs (id STRING PRIMARY KEY, state STRING)`,
	); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// A single-row and a same-shard multi-row insert both route to the one
	// shard and are accepted.
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`,
		`{"id":"a","state":"old"}`); err != nil {
		t.Fatalf("INSERT single: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?), (?)`,
		`{"id":"b","state":"old"}`, `{"id":"c","state":"old"}`); err != nil {
		t.Fatalf("INSERT same-shard multi-row: %v", err)
	}

	// A shard-key-predicated update and delete resolve to the one shard.
	if _, err := db.Exec(`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		`{"id":"a","state":"new"}`, "a"); err != nil {
		t.Fatalf("UPDATE by shard key: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM docs WHERE id = ?`, "c"); err != nil {
		t.Fatalf("DELETE by shard key: %v", err)
	}

	var remaining int
	if err := db.QueryRow(`SELECT count(*) FROM docs`).Scan(&remaining); err != nil {
		t.Fatalf("SELECT count: %v", err)
	}
	if remaining != 2 {
		t.Fatalf("rows after mutations = %d, want 2", remaining)
	}
}

func TestOpenClusterTransactionRoutesThroughPreflight(t *testing.T) {
	db := openTestCluster(t, oneShardConfig(t, "docs", "/id"))
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO docs VALUES (?), (?)`,
		`{"id":"a"}`, `{"id":"b"}`); err != nil {
		t.Fatalf("tx INSERT: %v", err)
	}
	if _, err := tx.Exec(`DELETE FROM docs WHERE id = ?`, "a"); err != nil {
		t.Fatalf("tx DELETE by shard key: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	var remaining int
	if err := db.QueryRow(`SELECT count(*) FROM docs`).Scan(&remaining); err != nil {
		t.Fatalf("SELECT count: %v", err)
	}
	if remaining != 1 {
		t.Fatalf("rows after tx = %d, want 1", remaining)
	}
}

func TestOpenClusterRejectsScatterUpdateAndDelete(t *testing.T) {
	db := openTestCluster(t, oneShardConfig(t, "docs", "/id"))
	if _, err := db.Exec(
		`CREATE TABLE docs (id STRING PRIMARY KEY, state STRING)`,
	); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`,
		`{"id":"a","state":"old"}`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// A predicate that does not narrow the shard key cannot prove a single
	// shard, so the placed write is refused before dispatch even though the
	// facade physically has one shard.
	_, err := db.Exec(`DELETE FROM docs WHERE state = ?`, "old")
	if !errors.Is(err, ErrScatterWrite) {
		t.Fatalf("scatter DELETE error = %v, want ErrScatterWrite", err)
	}
	_, err = db.Exec(`UPDATE docs SET "$doc" = ? WHERE state = ?`,
		`{"id":"a","state":"new"}`, "old")
	if !errors.Is(err, ErrScatterWrite) {
		t.Fatalf("scatter UPDATE error = %v, want ErrScatterWrite", err)
	}

	// The refused writes changed nothing.
	var state string
	if err := db.QueryRow(`SELECT state FROM docs WHERE id = ?`, "a").
		Scan(&state); err != nil {
		t.Fatalf("SELECT state: %v", err)
	}
	if state != "old" {
		t.Fatalf("state after refused writes = %q, want old", state)
	}
}

func TestOpenClusterValidatesConfig(t *testing.T) {
	// An arity that disagrees with the placement column count is a metadata
	// error surfaced by ClusterConfig.Validate before any file is opened.
	cfg := distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{
			{Name: "d", Arity: 2, MapperVersion: distribution.NativeMapperVersion},
		},
		Placements: []distribution.TablePlacement{
			{Table: "docs", Distribution: "d", Columns: []string{"/id"}},
		},
		Manifests: []*distribution.Manifest{fullManifest(t)},
	}
	_, err := OpenCluster(filepath.Join(t.TempDir(), "catalog.vdb"), cfg)
	if !errors.Is(err, distribution.ErrInvalidPlacement) {
		t.Fatalf("OpenCluster invalid config = %v, want ErrInvalidPlacement", err)
	}
}

func TestOpenClusterRejectsNonLocalShardKeyAtCreate(t *testing.T) {
	db := openTestCluster(t, oneShardConfig(t, "docs", "/tenant"))
	// The primary key /id does not contain the shard-key column /tenant, so the
	// placement is refused when the table is created, before it is persisted.
	_, err := db.Exec(
		`CREATE TABLE docs (id STRING PRIMARY KEY, tenant STRING)`,
	)
	if !errors.Is(err, ErrShardKeyNotLocal) {
		t.Fatalf("CREATE non-local placement = %v, want ErrShardKeyNotLocal", err)
	}
	// The refused CREATE left no table behind.
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"a"}`); !errors.Is(
		err, ErrTableNotFound,
	) {
		t.Fatalf("INSERT after refused CREATE = %v, want ErrTableNotFound", err)
	}
}

func TestOpenClusterValidatesOpenedSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")

	// Create a table with the ordinary driver, then close it.
	plain, err := stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plain.Exec(
		`CREATE TABLE docs (id STRING PRIMARY KEY, tenant STRING)`,
	); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if err := plain.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopening as a cluster with a placement that violates locality against the
	// already-persisted schema is refused at open time.
	_, err = OpenCluster(path, oneShardConfig(t, "docs", "/tenant"))
	if !errors.Is(err, ErrShardKeyNotLocal) {
		t.Fatalf("OpenCluster non-local existing schema = %v, want ErrShardKeyNotLocal", err)
	}

	// The failed open released the writer lock, so a matching placement reopens.
	db, err := OpenCluster(path, oneShardConfig(t, "docs", "/id"))
	if err != nil {
		t.Fatalf("OpenCluster after released lock: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenClusterLeavesUnplacedTablesUnrouted(t *testing.T) {
	// The configuration places docs but not notes; writes to the unplaced table
	// keep the default non-distributed behavior, including scatter deletes.
	db := openTestCluster(t, oneShardConfig(t, "docs", "/id"))
	if _, err := db.Exec(
		`CREATE TABLE notes (id STRING PRIMARY KEY, body STRING)`,
	); err != nil {
		t.Fatalf("CREATE TABLE notes: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO notes VALUES (?)`,
		`{"id":"a","body":"x"}`); err != nil {
		t.Fatalf("INSERT notes: %v", err)
	}
	// A non-shard-key predicate would be a scatter for a placed table, but notes
	// is unplaced, so the delete runs unrouted.
	if _, err := db.Exec(`DELETE FROM notes WHERE body = ?`, "x"); err != nil {
		t.Fatalf("DELETE unplaced by non-key predicate: %v", err)
	}
	var remaining int
	if err := db.QueryRow(`SELECT count(*) FROM notes`).Scan(&remaining); err != nil {
		t.Fatalf("SELECT count: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("notes rows after delete = %d, want 0", remaining)
	}
}
