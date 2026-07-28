package driver

import (
	"context"
	stdsql "database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

func openTestDB(t *testing.T) *stdsql.DB {
	t.Helper()
	db, err := stdsql.Open("vibedb", filepath.Join(t.TempDir(), "catalog.vdb"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

func TestDatabaseSQLLifecycle(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	documents := []string{
		`{"id":"a","kind":"x","n":1}`,
		`{"id":"b","kind":"y","n":2}`,
		`{"id":"c","kind":"x","n":3}`,
	}
	for _, document := range documents {
		if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, []byte(document)); err != nil {
			t.Fatal(err)
		}
	}
	prepared, err := db.Prepare(`SELECT * FROM docs WHERE id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	var raw []byte
	if err := prepared.QueryRow("b").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if string(raw) != documents[1] {
		t.Fatalf("document = %s, want %s", raw, documents[1])
	}

	rows, err := db.Query(`SELECT id FROM docs ORDER BY id LIMIT 2`)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		got = append(got, id)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("limited scan = %v, want [a b]", got)
	}

	deleted, err := db.Exec(`DELETE FROM docs WHERE id = ?`, "b")
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := deleted.RowsAffected(); n != 1 {
		t.Fatalf("RowsAffected = %d, want 1", n)
	}
	if err := prepared.QueryRow("b").Scan(&raw); !errors.Is(err, stdsql.ErrNoRows) {
		t.Fatalf("deleted lookup = %v, want sql.ErrNoRows", err)
	}
}

func TestExactIndexCountAndMutationGate(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE INDEX ON docs(kind)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"seed","kind":"x"}`); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs WHERE kind = ?`, "x").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("COUNT(*) = %d, want 1", count)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"later","kind":"x"}`); !errors.Is(err, ErrIndexedTableReadOnly) {
		t.Fatalf("indexed INSERT error = %v, want ErrIndexedTableReadOnly", err)
	}
}

func TestFlatInsertAndConcurrentReaders(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(8)
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs (id, active, score) VALUES (?, ?, ?)`, "a", true, int64(7)); err != nil {
		t.Fatal(err)
	}
	const readers = 8
	var wg sync.WaitGroup
	errs := make(chan error, readers)
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var score int64
			errs <- db.QueryRow(`SELECT score FROM docs WHERE id = ?`, "a").Scan(&score)
			if score != 7 {
				errs <- errors.New("unexpected score")
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestTransactionsAreTypedFeatureGate(t *testing.T) {
	db := openTestDB(t)
	_, err := db.BeginTx(context.Background(), nil)
	if !errors.Is(err, ErrAutocommitOnly) {
		t.Fatalf("BeginTx error = %v, want ErrAutocommitOnly", err)
	}
}

func TestCatalogAndCollectionReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	db, err := stdsql.Open("vibedb", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE docs (PRIMARY KEY (id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, `{"id":"persisted","n":9}`); err != nil {
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
	var n int64
	if err := db.QueryRow(`SELECT n FROM docs WHERE id = ?`, "persisted").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 9 {
		t.Fatalf("n = %d, want 9", n)
	}
}
