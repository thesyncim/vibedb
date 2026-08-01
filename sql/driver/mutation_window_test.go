package driver

import (
	"strings"
	"testing"
)

func TestMutationOrderByPrimaryKeyLimit(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, state STRING)`); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"d", "a", "c", "b"} {
		if _, err := db.Exec(`INSERT INTO docs VALUES (?)`,
			`{"id":"`+id+`","state":"old"}`); err != nil {
			t.Fatal(err)
		}
	}

	rows, err := db.Query(
		`DELETE FROM docs ORDER BY id DESC LIMIT ? RETURNING id`, int64(2))
	if err != nil {
		t.Fatal(err)
	}
	var deleted []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		deleted = append(deleted, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(deleted, ","), "d,c"; got != want {
		t.Fatalf("ordered DELETE RETURNING = %q, want %q", got, want)
	}

	rows, err = db.Query(
		`UPDATE docs SET "$doc" = ? WHERE state = 'old' ORDER BY id LIMIT 1 RETURNING id, state`,
		`{"id":"a","state":"new"}`)
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("ordered UPDATE RETURNING returned no row")
	}
	var id, state string
	if err := rows.Scan(&id, &state); err != nil {
		t.Fatal(err)
	}
	if id != "a" || state != "new" {
		t.Fatalf("ordered UPDATE RETURNING = (%q, %q), want (a, new)", id, state)
	}
	if rows.Next() {
		t.Fatal("ordered UPDATE RETURNING returned more than one row")
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`DELETE FROM docs ORDER BY state LIMIT 1`); err == nil ||
		!strings.Contains(err.Error(), "declared primary-key path") {
		t.Fatalf("unsupported mutation order error = %v", err)
	}
}

func TestMutationOrderByPrimaryKeyLimitTransaction(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE docs (id STRING PRIMARY KEY, state STRING)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?), (?), (?)`,
		`{"id":"a","state":"old"}`,
		`{"id":"b","state":"old"}`,
		`{"id":"c","state":"old"}`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(`DELETE FROM docs WHERE state = 'old' ORDER BY id DESC LIMIT 1 RETURNING id`)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if !rows.Next() {
		_ = tx.Rollback()
		t.Fatal("transactional ordered DELETE returned no row")
	}
	var id string
	if err := rows.Scan(&id); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if id != "c" {
		t.Fatalf("transactional ordered DELETE returned %q, want c", id)
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
	if count != 2 {
		t.Fatalf("remaining rows after transactional ordered DELETE = %d, want 2", count)
	}
}
