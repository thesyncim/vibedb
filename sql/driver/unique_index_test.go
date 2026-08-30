package driver

import (
	stdsql "database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func uniqueIndexCount(t *testing.T, db *stdsql.DB, table string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func uniqueIndexEmail(t *testing.T, db *stdsql.DB, id string) string {
	t.Helper()
	var email string
	if err := db.QueryRow(
		`SELECT email FROM docs WHERE id = ?`, id,
	).Scan(&email); err != nil {
		t.Fatal(err)
	}
	return email
}

func TestUniqueIndexBuildRejectsExistingDuplicatesAtomically(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(
		`CREATE TABLE docs (id STRING PRIMARY KEY, email STRING)`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?), (?)`,
		`{"id":"a","email":"same@example.com"}`,
		`{"id":"b","email":"same@example.com"}`,
	); err != nil {
		t.Fatal(err)
	}

	_, err := db.Exec(`CREATE UNIQUE INDEX by_email ON docs (email)`)
	if !errors.Is(err, ErrUniqueConstraint) {
		t.Fatalf("duplicate build = %v, want ErrUniqueConstraint", err)
	}
	if got := uniqueIndexCount(t, db, "docs"); got != 2 {
		t.Fatalf("failed build changed row count to %d, want 2", got)
	}
	if _, err := db.Exec(`DROP INDEX by_email ON docs`); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("failed build left catalog index: %v", err)
	}

	if _, err := db.Exec(`DELETE FROM docs WHERE id = 'b'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE UNIQUE INDEX by_email ON docs (email)`); err != nil {
		t.Fatalf("retry after duplicate removal: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`,
		`{"id":"c","email":"same@example.com"}`,
	); !errors.Is(err, ErrUniqueConstraint) {
		t.Fatalf("retry did not install uniqueness: %v", err)
	}
}

func TestUniqueIndexInsertBatchNullMissingAndCanonicalNumbers(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	// Creating the constraint before first materialization exercises the
	// catalog-only CREATE path and its later durable projection.
	if _, err := db.Exec(`CREATE UNIQUE INDEX by_email ON docs (email)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`,
		`{"id":"one","email":"one@example.com"}`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`,
		`{"id":"duplicate","email":"one@example.com"}`,
	); !errors.Is(err, ErrUniqueConstraint) {
		t.Fatalf("committed duplicate = %v, want ErrUniqueConstraint", err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?), (?)`,
		`{"id":"batch-a","email":"batch@example.com"}`,
		`{"id":"batch-b","email":"batch@example.com"}`,
	); !errors.Is(err, ErrUniqueConstraint) {
		t.Fatalf("same-batch duplicate = %v, want ErrUniqueConstraint", err)
	}
	if got := uniqueIndexCount(t, db, "docs"); got != 1 {
		t.Fatalf("failed inserts left %d rows, want 1", got)
	}

	// Default PostgreSQL UNIQUE semantics are NULLS DISTINCT. VibeDB also
	// treats an absent JSON path as non-participating.
	if _, err := db.Exec(`INSERT INTO docs VALUES (?), (?), (?), (?)`,
		`{"id":"null-a","email":null}`,
		`{"id":"null-b","email":null}`,
		`{"id":"missing-a"}`,
		`{"id":"missing-b"}`,
	); err != nil {
		t.Fatalf("NULL/missing values should be distinct: %v", err)
	}

	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`,
		`{"id":"number-a","email":1}`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`,
		`{"id":"number-b","email":1.0}`,
	); !errors.Is(err, ErrUniqueConstraint) {
		t.Fatalf("equivalent exact numbers = %v, want ErrUniqueConstraint", err)
	}
}

func TestUniqueIndexRejectsPresentContainersOnBuildAndMutation(t *testing.T) {
	t.Run("build", func(t *testing.T) {
		db := openTestDB(t)
		if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO docs VALUES (?)`,
			`{"id":"object","email":{"address":"x"}}`,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(
			`CREATE UNIQUE INDEX by_email ON docs (email)`,
		); !errors.Is(err, store.ErrIndexScalar) {
			t.Fatalf("container build = %v, want ErrIndexScalar", err)
		}
		if _, err := db.Exec(`DROP INDEX by_email ON docs`); !errors.Is(err, ErrIndexNotFound) {
			t.Fatalf("container build left catalog index: %v", err)
		}
	})

	t.Run("mutation", func(t *testing.T) {
		db := openTestDB(t)
		if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`CREATE UNIQUE INDEX by_email ON docs (email)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO docs VALUES (?)`,
			`{"id":"array","email":["x"]}`,
		); !errors.Is(err, store.ErrIndexScalar) {
			t.Fatalf("container mutation = %v, want ErrIndexScalar", err)
		}
		if got := uniqueIndexCount(t, db, "docs"); got != 0 {
			t.Fatalf("container mutation left %d rows, want 0", got)
		}
		if _, err := db.Exec(
			`CREATE UNIQUE INDEX by_pair ON docs (email, region)`,
		); err != nil {
			t.Fatal(err)
		}
		// NULL/missing exempts the tuple from conflicts, but it must not hide a
		// present container in a later component.
		if _, err := db.Exec(`INSERT INTO docs VALUES (?)`,
			`{"id":"later","region":[]}`,
		); !errors.Is(err, store.ErrIndexScalar) {
			t.Fatalf("missing then container mutation = %v, want ErrIndexScalar", err)
		}
	})
}

func TestUniqueIndexUpdateDeleteReplacementAndUpsertPostimages(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(
		`CREATE TABLE docs (id STRING PRIMARY KEY, email STRING)`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE UNIQUE INDEX by_email ON docs (email)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?), (?)`,
		`{"id":"a","email":"a@example.com"}`,
		`{"id":"b","email":"b@example.com"}`,
	); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(
		`UPDATE docs SET email = 'a@example.com' WHERE id = 'b'`,
	); !errors.Is(err, ErrUniqueConstraint) {
		t.Fatalf("UPDATE collision = %v, want ErrUniqueConstraint", err)
	}
	if got := uniqueIndexEmail(t, db, "b"); got != "b@example.com" {
		t.Fatalf("failed UPDATE stored %q, want b@example.com", got)
	}

	if _, err := db.Exec(`
		INSERT INTO docs VALUES (?)
		ON CONFLICT DO UPDATE SET email = EXCLUDED.email`,
		`{"id":"a","email":"b@example.com"}`,
	); !errors.Is(err, ErrUniqueConstraint) {
		t.Fatalf("upsert collision = %v, want ErrUniqueConstraint", err)
	}
	if got := uniqueIndexEmail(t, db, "a"); got != "a@example.com" {
		t.Fatalf("failed upsert stored %q, want a@example.com", got)
	}

	// The batch is validated by its final image. Both affected owners exchange
	// values atomically, so neither old posting is an unaffected conflict.
	if _, err := db.Exec(`
		INSERT INTO docs VALUES (?), (?)
		ON CONFLICT DO UPDATE SET email = EXCLUDED.email`,
		`{"id":"a","email":"b@example.com"}`,
		`{"id":"b","email":"a@example.com"}`,
	); err != nil {
		t.Fatalf("atomic final-image swap: %v", err)
	}
	if got := uniqueIndexEmail(t, db, "a"); got != "b@example.com" {
		t.Fatalf("swapped a email = %q", got)
	}
	if got := uniqueIndexEmail(t, db, "b"); got != "a@example.com" {
		t.Fatalf("swapped b email = %q", got)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`DELETE FROM docs WHERE id = 'a'`); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO docs VALUES (?)`,
		`{"id":"replacement","email":"b@example.com"}`,
	); err != nil {
		_ = tx.Rollback()
		t.Fatalf("transactional delete replacement: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestUniqueIndexTransactionOverlayAndConcurrentCommit(t *testing.T) {
	t.Run("overlay", func(t *testing.T) {
		db := openTestDB(t)
		if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, email STRING)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`CREATE UNIQUE INDEX by_email ON docs (email)`); err != nil {
			t.Fatal(err)
		}
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(`INSERT INTO docs VALUES (?)`,
			`{"id":"a","email":"same@example.com"}`,
		); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		if _, err := tx.Exec(`INSERT INTO docs VALUES (?)`,
			`{"id":"b","email":"same@example.com"}`,
		); !errors.Is(err, ErrUniqueConstraint) {
			_ = tx.Rollback()
			t.Fatalf("staged duplicate = %v, want ErrUniqueConstraint", err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if got := uniqueIndexCount(t, db, "docs"); got != 0 {
			t.Fatalf("rolled-back overlay left %d rows", got)
		}
	})

	t.Run("concurrent commits", func(t *testing.T) {
		db := openTestDB(t)
		db.SetMaxOpenConns(4)
		if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, email STRING)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`CREATE UNIQUE INDEX by_email ON docs (email)`); err != nil {
			t.Fatal(err)
		}
		first, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		second, err := db.Begin()
		if err != nil {
			_ = first.Rollback()
			t.Fatal(err)
		}
		if _, err := first.Exec(`INSERT INTO docs VALUES (?)`,
			`{"id":"first","email":"claimed@example.com"}`,
		); err != nil {
			_ = first.Rollback()
			_ = second.Rollback()
			t.Fatal(err)
		}
		if _, err := second.Exec(`INSERT INTO docs VALUES (?)`,
			`{"id":"second","email":"claimed@example.com"}`,
		); err != nil {
			_ = first.Rollback()
			_ = second.Rollback()
			t.Fatalf("second snapshot-local insert: %v", err)
		}
		if err := first.Commit(); err != nil {
			_ = second.Rollback()
			t.Fatal(err)
		}
		if err := second.Commit(); !errors.Is(err, ErrUniqueConstraint) {
			t.Fatalf("second concurrent commit = %v, want ErrUniqueConstraint", err)
		}
		if got := uniqueIndexCount(t, db, "docs"); got != 1 {
			t.Fatalf("concurrent commits stored %d rows, want 1", got)
		}
	})
}

func TestUniqueIndexReopenAliasAndDropSemantics(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	db, err := stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, email STRING)`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`,
		`{"id":"seed","email":"same@example.com"}`,
	); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE INDEX email_lookup ON docs (email)`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE UNIQUE INDEX email_unique ON docs (email)`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP INDEX email_lookup ON docs`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`,
		`{"id":"before-reopen","email":"same@example.com"}`,
	); !errors.Is(err, ErrUniqueConstraint) {
		_ = db.Close()
		t.Fatalf("dropping ordinary alias removed uniqueness: %v", err)
	}
	if _, err := db.Exec(`CREATE INDEX email_lookup ON docs (email)`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`,
		`{"id":"after-reopen","email":"same@example.com"}`,
	); !errors.Is(err, ErrUniqueConstraint) {
		t.Fatalf("reopened uniqueness = %v, want ErrUniqueConstraint", err)
	}
	if _, err := db.Exec(`DROP INDEX email_unique ON docs`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`,
		`{"id":"allowed","email":"same@example.com"}`,
	); err != nil {
		t.Fatalf("drop unique alias left constraint active: %v", err)
	}
	if _, err := db.Exec(`DROP INDEX email_lookup ON docs`); err != nil {
		t.Fatalf("drop surviving ordinary alias: %v", err)
	}
}

func TestUniqueIndexClusterRequiresShardKeyLocality(t *testing.T) {
	db := openTestCluster(t, oneShardConfig(t, "docs", "/tenant"))
	if _, err := db.Exec(`
		CREATE TABLE docs (
			tenant STRING PRIMARY KEY,
			email STRING
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`CREATE UNIQUE INDEX global_email ON docs (email)`,
	); !errors.Is(err, ErrShardKeyNotLocal) {
		t.Fatalf("non-local unique index = %v, want ErrShardKeyNotLocal", err)
	}
	if _, err := db.Exec(
		`CREATE UNIQUE INDEX tenant_email ON docs (tenant, email)`,
	); err != nil {
		t.Fatalf("local unique index: %v", err)
	}
	// Locality applies to unique keys only; an ordinary local lookup need not
	// contain the shard key.
	if _, err := db.Exec(`CREATE INDEX email_lookup ON docs (email)`); err != nil {
		t.Fatalf("ordinary non-local lookup: %v", err)
	}
}

func TestUniqueIndexClusterValidatesOpenedLocality(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	plain, err := stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plain.Exec(`
		CREATE TABLE docs (
			tenant STRING PRIMARY KEY,
			email STRING
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := plain.Exec(
		`CREATE UNIQUE INDEX global_email ON docs (email)`,
	); err != nil {
		t.Fatal(err)
	}
	if err := plain.Close(); err != nil {
		t.Fatal(err)
	}

	cluster, err := OpenCluster(path, oneShardConfig(t, "docs", "/tenant"))
	if cluster != nil {
		_ = cluster.Close()
	}
	if !errors.Is(err, ErrShardKeyNotLocal) {
		t.Fatalf("OpenCluster non-local unique index = %v, want ErrShardKeyNotLocal", err)
	}
}
