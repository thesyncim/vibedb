package driver

import (
	stdsql "database/sql"
	"encoding/json"
	"path/filepath"
	"slices"
	"testing"
)

const selfCorrelatedDriverLeaf = `
	SELECT o.*
	FROM self_catalog_nodes AS o
	WHERE EXISTS (
		SELECT 1
		FROM self_catalog_nodes AS i
		WHERE i.parent = o.id AND i.active = TRUE
	)`

func openSelfCorrelatedCatalogDB(t *testing.T) *stdsql.DB {
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

func seedSelfCorrelatedCatalog(t *testing.T, db *stdsql.DB) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE self_catalog_nodes (` +
			`id STRING PRIMARY KEY, parent STRING, active BOOL NOT NULL)`,
		`CREATE TABLE self_catalog_empty (` +
			`id STRING PRIMARY KEY, parent STRING, active BOOL NOT NULL)`,
		`CREATE TABLE self_catalog_matches (` +
			`id STRING PRIMARY KEY, parent STRING, active BOOL NOT NULL)`,
		`CREATE TABLE self_catalog_tx_matches (` +
			`id STRING PRIMARY KEY, parent STRING, active BOOL NOT NULL)`,
		`INSERT INTO self_catalog_nodes VALUES ` +
			`('{"id":"p","parent":"none","active":true}'),` +
			`('{"id":"c","parent":"p","active":true}'),` +
			`('{"id":"off","parent":"p","active":false}'),` +
			`('{"id":"orphan","parent":"missing","active":true}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
}

func selfCorrelatedDriverIDs(t *testing.T, rows *stdsql.Rows) []string {
	t.Helper()
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var document []byte
		if err := rows.Scan(&document); err != nil {
			t.Fatal(err)
		}
		var decoded struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(document, &decoded); err != nil {
			t.Fatalf("decode document %s: %v", document, err)
		}
		if decoded.ID == "" {
			t.Fatalf("document has no id: %s", document)
		}
		ids = append(ids, decoded.ID)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return ids
}

func TestSelfCorrelatedCatalogAutocommitPreparedEmptyAndWrappers(t *testing.T) {
	db := openSelfCorrelatedCatalogDB(t)
	seedSelfCorrelatedCatalog(t, db)

	rows, err := db.Query(selfCorrelatedDriverLeaf)
	if err != nil {
		t.Fatal(err)
	}
	if got := selfCorrelatedDriverIDs(t, rows); !slices.Equal(got, []string{"p"}) {
		t.Fatalf("autocommit rows = %v, want [p]", got)
	}

	prepared, err := db.Prepare(selfCorrelatedDriverLeaf)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	for run := range 2 {
		rows, err = prepared.Query()
		if err != nil {
			t.Fatal(err)
		}
		if got := selfCorrelatedDriverIDs(t, rows); !slices.Equal(got, []string{"p"}) {
			t.Fatalf("prepared run %d rows = %v, want [p]", run, got)
		}
	}

	empty := `SELECT o.* FROM self_catalog_empty AS o WHERE EXISTS (` +
		`SELECT 1 FROM self_catalog_empty AS i ` +
		`WHERE i.parent = o.id AND i.active = TRUE)`
	rows, err = db.Query(empty)
	if err != nil {
		t.Fatal(err)
	}
	if got := selfCorrelatedDriverIDs(t, rows); len(got) != 0 {
		t.Fatalf("empty self-correlation rows = %v", got)
	}

	wrapperQueries := []string{
		`SELECT d.* FROM (` + selfCorrelatedDriverLeaf + `) AS d`,
		`WITH matched AS (` + selfCorrelatedDriverLeaf + `) SELECT * FROM matched`,
	}
	for _, statement := range wrapperQueries {
		rows, err = db.Query(statement)
		if err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
		if got := selfCorrelatedDriverIDs(t, rows); !slices.Equal(got, []string{"p"}) {
			t.Fatalf("wrapper rows = %v, want [p]: %s", got, statement)
		}
	}
	windowQuery := `SELECT o.*, ROW_NUMBER() OVER (ORDER BY o.id) AS ordinal ` +
		`FROM self_catalog_nodes AS o WHERE EXISTS (` +
		`SELECT 1 FROM self_catalog_nodes AS i ` +
		`WHERE i.parent = o.id AND i.active = TRUE)`
	rows, err = db.Query(windowQuery)
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("window wrapper returned no row")
	}
	var document []byte
	var ordinal int64
	if err := rows.Scan(&document, &ordinal); err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		t.Fatal("window wrapper returned more than one row")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	var windowDocument struct {
		ID string `json:"id"`
	}
	decodeErr := json.Unmarshal(document, &windowDocument)
	if decodeErr != nil ||
		windowDocument.ID != "p" || ordinal != 1 {
		t.Fatalf("window wrapper = document:%s id:%q ordinal:%d error:%v",
			document, windowDocument.ID, ordinal, decodeErr)
	}

	setQuery := selfCorrelatedDriverLeaf + ` UNION ALL ` + selfCorrelatedDriverLeaf
	rows, err = db.Query(setQuery)
	if err != nil {
		t.Fatal(err)
	}
	got := selfCorrelatedDriverIDs(t, rows)
	slices.Sort(got)
	if !slices.Equal(got, []string{"p", "p"}) {
		t.Fatalf("set wrapper rows = %v, want [p p]", got)
	}
}

func TestSelfCorrelatedCatalogTransactionsAndInsertSelect(t *testing.T) {
	db := openSelfCorrelatedCatalogDB(t)
	seedSelfCorrelatedCatalog(t, db)

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(selfCorrelatedDriverLeaf)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if got := selfCorrelatedDriverIDs(t, rows); !slices.Equal(got, []string{"p"}) {
		_ = tx.Rollback()
		t.Fatalf("no-pending transaction rows = %v, want [p]", got)
	}
	if _, err := tx.Exec(`INSERT INTO self_catalog_nodes VALUES ` +
		`('{"id":"q","parent":"none","active":true}'),` +
		`('{"id":"qc","parent":"q","active":true}')`); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	rows, err = tx.Query(selfCorrelatedDriverLeaf)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	got := selfCorrelatedDriverIDs(t, rows)
	slices.Sort(got)
	if !slices.Equal(got, []string{"p", "q"}) {
		_ = tx.Rollback()
		t.Fatalf("pending transaction rows = %v, want [p q]", got)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	result, err := tx.Exec(
		`INSERT INTO self_catalog_tx_matches ` + selfCorrelatedDriverLeaf,
	)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		_ = tx.Rollback()
		t.Fatalf("transaction INSERT SELECT affected = %d, want 1", affected)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	result, err = db.Exec(
		`INSERT INTO self_catalog_matches ` + selfCorrelatedDriverLeaf,
	)
	if err != nil {
		t.Fatal(err)
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		t.Fatalf("autocommit INSERT SELECT affected = %d, want 1", affected)
	}
	rows, err = db.Query(`SELECT * FROM self_catalog_matches`)
	if err != nil {
		t.Fatal(err)
	}
	if got := selfCorrelatedDriverIDs(t, rows); !slices.Equal(got, []string{"p"}) {
		t.Fatalf("autocommit INSERT SELECT rows = %v, want [p]", got)
	}
}
