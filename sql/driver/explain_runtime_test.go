package driver

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibejson"
)

func TestTypedRuntimeExplainReturnsOnePlanRow(t *testing.T) {
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "catalog.vdb"))
	if err != nil {
		t.Fatal(err)
	}
	session, err := database.NewSession(ctx)
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(); _ = database.Close() })
	create := runtimePrepare(t, session, `CREATE TABLE docs (PRIMARY KEY (id))`)
	if _, err := create.Exec(ctx, nil); err != nil {
		t.Fatal(err)
	}

	prepared := runtimePrepare(t, session,
		`EXPLAIN SELECT id FROM docs WHERE id = ?`)
	if !prepared.ReturnsRows() {
		t.Fatal("EXPLAIN does not return rows")
	}
	if got := prepared.Columns(); len(got) != 1 || got[0] != "QUERY PLAN" {
		t.Fatalf("EXPLAIN columns = %v", got)
	}
	schema := prepared.AppendSchema(nil)
	if len(schema) != 1 || schema[0].Type != query.TypeString {
		t.Fatalf("EXPLAIN schema = %+v", schema)
	}
	cursor, err := prepared.Query(ctx, []any{"missing"})
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	if !cursor.Next() {
		t.Fatal("EXPLAIN returned no row")
	}
	plan, ok := cursor.Cell(0).Text()
	if !ok || !vibejson.Valid([]byte(plan)) {
		t.Fatalf("EXPLAIN cell = (%q, %v)", plan, ok)
	}
	if cursor.Next() {
		t.Fatal("EXPLAIN returned more than one row")
	}
}

func TestTypedRuntimeExplainAnalyzeReturnsMeasuredPlan(t *testing.T) {
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "catalog.vdb"))
	if err != nil {
		t.Fatal(err)
	}
	session, err := database.NewSession(ctx)
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(); _ = database.Close() })
	create := runtimePrepare(t, session, `CREATE TABLE docs (PRIMARY KEY (id))`)
	if _, err := create.Exec(ctx, nil); err != nil {
		t.Fatal(err)
	}
	insert := runtimePrepare(t, session, `INSERT INTO docs VALUES (?)`)
	if _, err := insert.Exec(ctx, []any{`{"id":"a"}`}); err != nil {
		t.Fatal(err)
	}
	prepared := runtimePrepare(t, session,
		`EXPLAIN ANALYZE SELECT id FROM docs WHERE id = ?`)
	cursor, err := prepared.Query(ctx, []any{"a"})
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	if !cursor.Next() {
		t.Fatal("EXPLAIN ANALYZE returned no row")
	}
	plan, ok := cursor.Cell(0).Text()
	if !ok || !vibejson.Valid([]byte(plan)) ||
		!strings.Contains(plan, `"analyze":{`) {
		t.Fatalf("EXPLAIN ANALYZE cell = (%q, %v)", plan, ok)
	}
}
