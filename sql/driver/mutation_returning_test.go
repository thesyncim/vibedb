package driver

import (
	"encoding/json"
	"testing"
)

func TestUpdateAndDeleteReturning(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, state STRING, n INTEGER)`); err != nil {
		t.Fatal(err)
	}
	for _, document := range []string{
		`{"id":"a","state":"old","n":1}`,
		`{"id":"b","state":"old","n":2}`,
	} {
		if _, err := db.Exec(`INSERT INTO docs VALUES (?)`, document); err != nil {
			t.Fatal(err)
		}
	}

	rows, err := db.Query(`
		UPDATE docs SET "$doc" = ? WHERE id = 'a'
		RETURNING id, state, n`, `{"id":"a","state":"new","n":3}`)
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("UPDATE RETURNING returned no row")
	}
	var id, state string
	var n int64
	if err := rows.Scan(&id, &state, &n); err != nil {
		t.Fatal(err)
	}
	if id != "a" || state != "new" || n != 3 {
		t.Fatalf("UPDATE RETURNING = (%q, %q, %d), want new document", id, state, n)
	}
	if rows.Next() {
		t.Fatal("UPDATE RETURNING returned more than one row")
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	rows, err = db.Query(`DELETE FROM docs WHERE id = 'b' RETURNING *`)
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("DELETE RETURNING returned no row")
	}
	var document []byte
	if err := rows.Scan(&document); err != nil {
		t.Fatal(err)
	}
	var gotDocument, wantDocument map[string]any
	if err := json.Unmarshal(document, &gotDocument); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(`{"id":"b","state":"old","n":2}`), &wantDocument); err != nil {
		t.Fatal(err)
	}
	if len(gotDocument) != len(wantDocument) || gotDocument["id"] != wantDocument["id"] ||
		gotDocument["state"] != wantDocument["state"] || gotDocument["n"] != wantDocument["n"] {
		t.Fatalf("DELETE RETURNING document = %s, want equivalent document", document)
	}
	if rows.Next() {
		t.Fatal("DELETE RETURNING returned more than one row")
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	var count int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("remaining rows = %d, want 1", count)
	}
}

func TestUpdateAndDeleteReturningTransaction(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, state STRING)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?), (?)`,
		`{"id":"a","state":"old"}`,
		`{"id":"b","state":"old"}`,
	); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(`
		UPDATE docs SET "$doc" = ? WHERE id = 'a' RETURNING id, state`,
		`{"id":"a","state":"new"}`)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if !rows.Next() {
		_ = tx.Rollback()
		t.Fatal("transactional UPDATE RETURNING returned no row")
	}
	var id, state string
	if err := rows.Scan(&id, &state); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if id != "a" || state != "new" {
		t.Fatalf("transactional UPDATE RETURNING = (%q, %q), want (a, new)", id, state)
	}
	if rows.Next() {
		t.Fatal("transactional UPDATE RETURNING returned more than one row")
	}
	if err := rows.Close(); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}

	rows, err = tx.Query(`DELETE FROM docs WHERE id = 'b' RETURNING id, state`)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if !rows.Next() {
		_ = tx.Rollback()
		t.Fatal("transactional DELETE RETURNING returned no row")
	}
	if err := rows.Scan(&id, &state); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if id != "b" || state != "old" {
		t.Fatalf("transactional DELETE RETURNING = (%q, %q), want (b, old)", id, state)
	}
	if rows.Next() {
		t.Fatal("transactional DELETE RETURNING returned more than one row")
	}
	if err := rows.Close(); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("transactional RETURNING remaining rows = %d, want 1", count)
	}
}
