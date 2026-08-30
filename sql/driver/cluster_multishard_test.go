package driver

import (
	stdsql "database/sql"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
)

// twoShardClusterConfig builds a degenerate local cluster whose single embedded
// store is fronted by a synthetic two-shard manifest, so routing partitions the
// probe values across two shards while execution still lands in one store. The
// boundary is derived from the reference mapper's own routed points (via
// twoShardFixture), so same routes both values to one shard and diff routes them
// to distinct shards, deterministically.
func twoShardClusterConfig(t *testing.T, table string) (cfg distribution.ClusterConfig, same, diff [2]string) {
	t.Helper()
	binding, _, same, diff := twoShardFixture(t)
	cfg = distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{
			{Name: "d", Arity: 1, MapperVersion: distribution.NativeMapperVersion},
		},
		Placements: []distribution.TablePlacement{
			{Table: table, Distribution: "d", Columns: []string{"/tenant_id"}},
		},
		Manifests: []*distribution.Manifest{binding.manifest},
	}
	return cfg, same, diff
}

// openTwoShardDocs opens a fresh two-shard cluster whose docs table is keyed on
// the shard-key column, so every write routes through the full preflight against
// a real multi-shard manifest while persisting to the one embedded store.
func openTwoShardDocs(t *testing.T) (db *stdsql.DB, same, diff [2]string) {
	t.Helper()
	cfg, same, diff := twoShardClusterConfig(t, "docs")
	db = openTestCluster(t, cfg)
	if _, err := db.Exec(
		`CREATE TABLE docs (tenant_id STRING PRIMARY KEY, state STRING)`,
	); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	return db, same, diff
}

// tenantDoc renders a whole document for the docs table.
func tenantDoc(tenant, state string) string {
	return fmt.Sprintf(`{"tenant_id":%s,"state":%s}`, strconv.Quote(tenant), strconv.Quote(state))
}

func countDocs(t *testing.T, db *stdsql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM docs`).Scan(&n); err != nil {
		t.Fatalf("SELECT count: %v", err)
	}
	return n
}

// TestClusterMultiShardSameShardInsertAccepted proves a multi-row insert whose
// post-coercion rows all route to one physical shard is accepted end-to-end and
// both rows are persisted.
func TestClusterMultiShardSameShardInsertAccepted(t *testing.T) {
	db, same, _ := openTwoShardDocs(t)
	if _, err := db.Exec(`INSERT INTO docs VALUES (?), (?)`,
		tenantDoc(same[0], "old"), tenantDoc(same[1], "old")); err != nil {
		t.Fatalf("same-shard multi-row INSERT: %v", err)
	}
	if got := countDocs(t, db); got != 2 {
		t.Fatalf("rows after same-shard insert = %d, want 2", got)
	}
}

// TestClusterMultiShardCrossShardInsertRejectedBeforeExecution proves a multi-row
// insert whose rows route to different shards is refused with ErrCrossShardWrite
// and that no row is written: the preflight completes before any participant, so
// the batch never partially dispatches.
func TestClusterMultiShardCrossShardInsertRejectedBeforeExecution(t *testing.T) {
	db, _, diff := openTwoShardDocs(t)
	_, err := db.Exec(`INSERT INTO docs VALUES (?), (?)`,
		tenantDoc(diff[0], "old"), tenantDoc(diff[1], "old"))
	if !errors.Is(err, ErrCrossShardWrite) {
		t.Fatalf("cross-shard INSERT error = %v, want ErrCrossShardWrite", err)
	}
	if got := countDocs(t, db); got != 0 {
		t.Fatalf("rows after refused cross-shard insert = %d, want 0", got)
	}
}

// TestClusterMultiShardTransactionCrossShardInsertRejected proves the
// transactional insert path also routes through the preflight and rejects a
// cross-shard batch before anything is committed.
func TestClusterMultiShardTransactionCrossShardInsertRejected(t *testing.T) {
	db, _, diff := openTwoShardDocs(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	_, err = tx.Exec(`INSERT INTO docs VALUES (?), (?)`,
		tenantDoc(diff[0], "old"), tenantDoc(diff[1], "old"))
	if !errors.Is(err, ErrCrossShardWrite) {
		t.Fatalf("tx cross-shard INSERT error = %v, want ErrCrossShardWrite", err)
	}
	_ = tx.Rollback()
	if got := countDocs(t, db); got != 0 {
		t.Fatalf("rows after rolled-back cross-shard insert = %d, want 0", got)
	}
}

// TestClusterMultiShardShardKeyImmutable proves a whole-document UPDATE whose
// replacement would move the row to a different shard is refused with
// ErrShardKeyImmutable, ahead of the primary-key recompute, and leaves the row
// unchanged.
func TestClusterMultiShardShardKeyImmutable(t *testing.T) {
	db, _, diff := openTwoShardDocs(t)
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, tenantDoc(diff[0], "old")); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	// diff[0] and diff[1] route to distinct shards, so the replacement moves the
	// row across shards.
	_, err := db.Exec(`UPDATE docs SET "$doc" = ? WHERE tenant_id = ?`,
		tenantDoc(diff[1], "new"), diff[0])
	if !errors.Is(err, ErrShardKeyImmutable) {
		t.Fatalf("shard-key move UPDATE error = %v, want ErrShardKeyImmutable", err)
	}
	// The refused move left the original row untouched and created nothing new.
	var state string
	if err := db.QueryRow(`SELECT state FROM docs WHERE tenant_id = ?`, diff[0]).
		Scan(&state); err != nil {
		t.Fatalf("SELECT original row: %v", err)
	}
	if state != "old" {
		t.Fatalf("state after refused move = %q, want old", state)
	}
	var moved int
	if err := db.QueryRow(`SELECT count(*) FROM docs WHERE tenant_id = ?`, diff[1]).
		Scan(&moved); err != nil {
		t.Fatalf("SELECT moved key: %v", err)
	}
	if moved != 0 {
		t.Fatalf("moved-key rows = %d, want 0", moved)
	}

	_, err = db.Exec(`UPDATE docs SET tenant_id = ? WHERE tenant_id = ?`,
		diff[1], diff[0])
	if !errors.Is(err, ErrShardKeyImmutable) {
		t.Fatalf("column shard-key move error = %v, want ErrShardKeyImmutable", err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(`UPDATE docs SET tenant_id = ? WHERE tenant_id = ?`,
		diff[1], diff[0])
	if !errors.Is(err, ErrShardKeyImmutable) {
		_ = tx.Rollback()
		t.Fatalf("transaction column shard-key move error = %v, want ErrShardKeyImmutable", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

// TestClusterMultiShardRejectsMultiShardDeleteAndUpdate proves a placed UPDATE or
// DELETE whose predicate spans more than one physical shard is refused before
// dispatch and changes nothing.
func TestClusterMultiShardRejectsMultiShardDeleteAndUpdate(t *testing.T) {
	db, same, diff := openTwoShardDocs(t)
	if _, err := db.Exec(`INSERT INTO docs VALUES (?), (?)`,
		tenantDoc(same[0], "old"), tenantDoc(same[1], "old")); err != nil {
		t.Fatalf("seed INSERT: %v", err)
	}

	// An IN list spanning both shards cannot prove a single target shard.
	_, err := db.Exec(`DELETE FROM docs WHERE tenant_id IN (?, ?)`, diff[0], diff[1])
	if !errors.Is(err, ErrScatterWrite) {
		t.Fatalf("multi-shard DELETE error = %v, want ErrScatterWrite", err)
	}
	_, err = db.Exec(`UPDATE docs SET "$doc" = ? WHERE tenant_id IN (?, ?)`,
		tenantDoc(same[0], "new"), diff[0], diff[1])
	if !errors.Is(err, ErrScatterWrite) {
		t.Fatalf("multi-shard UPDATE error = %v, want ErrScatterWrite", err)
	}
	if got := countDocs(t, db); got != 2 {
		t.Fatalf("rows after refused multi-shard writes = %d, want 2", got)
	}
}

// TestClusterMultiShardSingleShardDeleteAndUpdate proves that when the predicate
// pins one shard, the placed UPDATE and DELETE run against the one store.
func TestClusterMultiShardSingleShardDeleteAndUpdate(t *testing.T) {
	db, _, diff := openTwoShardDocs(t)
	// diff[0] and diff[1] route to distinct shards, so each is seeded with its own
	// single-shard insert.
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, tenantDoc(diff[0], "old")); err != nil {
		t.Fatalf("seed INSERT diff[0]: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, tenantDoc(diff[1], "old")); err != nil {
		t.Fatalf("seed INSERT diff[1]: %v", err)
	}
	// A whole-document UPDATE that keeps the shard-key column resolves to the one
	// shard and is applied.
	if _, err := db.Exec(`UPDATE docs SET "$doc" = ? WHERE tenant_id = ?`,
		tenantDoc(diff[0], "new"), diff[0]); err != nil {
		t.Fatalf("single-shard UPDATE: %v", err)
	}
	var state string
	if err := db.QueryRow(`SELECT state FROM docs WHERE tenant_id = ?`, diff[0]).
		Scan(&state); err != nil {
		t.Fatalf("SELECT updated: %v", err)
	}
	if state != "new" {
		t.Fatalf("state after single-shard update = %q, want new", state)
	}
	if _, err := db.Exec(`DELETE FROM docs WHERE tenant_id = ?`, diff[1]); err != nil {
		t.Fatalf("single-shard DELETE: %v", err)
	}
	if got := countDocs(t, db); got != 1 {
		t.Fatalf("rows after single-shard delete = %d, want 1", got)
	}
}
