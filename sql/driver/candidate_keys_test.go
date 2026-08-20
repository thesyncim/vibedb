package driver

import (
	"context"
	"testing"
)

func TestPreparedCandidateKeysReplaceOnlyPhysicalScan(t *testing.T) {
	database, session := openRuntimeSession(t)
	t.Cleanup(func() {
		_ = session.Close()
		_ = database.Close()
	})
	ctx := context.Background()
	create := runtimePrepare(t, session, `
		CREATE TABLE docs (id STRING PRIMARY KEY, email STRING NOT NULL, n INTEGER NOT NULL)`)
	if _, err := create.Exec(ctx, nil); err != nil {
		t.Fatal(err)
	}
	insert := runtimePrepare(t, session,
		`INSERT INTO docs (id, email, n) VALUES (?, ?, ?)`)
	for _, row := range []struct {
		id    string
		email string
		n     int64
	}{
		{id: "a", email: "x@example.com", n: 1},
		{id: "b", email: "x@example.com", n: 2},
		{id: "c", email: "x@example.com", n: 3},
	} {
		if _, err := insert.Exec(ctx, []any{row.id, row.email, row.n}); err != nil {
			t.Fatal(err)
		}
	}

	query := runtimePrepare(t, session,
		`SELECT id FROM docs WHERE email = ? AND n >= ? ORDER BY id`)
	a, err := primaryScalarKey("a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := primaryScalarKey("b")
	if err != nil {
		t.Fatal(err)
	}
	var cursor Cursor
	if err := query.QueryCandidateKeysInto(ctx, []any{"x@example.com", int64(2)},
		[]byte("/id"), [][]byte{[]byte(a), []byte(b)}, &cursor); err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() {
		t.Fatal("candidate query returned no row")
	}
	if id, ok := cursor.Cell(0).Text(); !ok || id != "b" {
		t.Fatalf("candidate row = %q,%v; want b", id, ok)
	}
	if cursor.Next() {
		t.Fatal("candidate query scanned a non-candidate or ignored its predicate")
	}
	if got := session.conn.pointDocs.Len(); got != 2 {
		t.Fatalf("candidate source materialized %d documents, want exactly 2", got)
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if err := query.QueryCandidateKeysInto(ctx, []any{"x@example.com", int64(2)},
		[]byte("/tenant_id"), [][]byte{[]byte(b)}, &cursor); err == nil {
		t.Fatal("candidate query accepted a primary path that disagrees with the live table")
	}
}
