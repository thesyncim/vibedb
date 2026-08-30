package driver

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
)

func TestDeclaredColumnUpdateAutocommitPreparedReturning(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`
		CREATE TABLE employees (
			id STRING PRIMARY KEY,
			team STRING NOT NULL,
			score INTEGER NOT NULL,
			active BOOLEAN NOT NULL,
			note STRING
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO employees (id, team, score, active, note)
		VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)`,
		"a", "runtime", 1, true, "keep",
		"b", "storage", 2, false, "untouched",
	); err != nil {
		t.Fatal(err)
	}

	update, err := db.Prepare(`
		UPDATE employees SET team = ?, score = ?, note = ? WHERE id = ?
		RETURNING id, team, score, active, note`)
	if err != nil {
		t.Fatal(err)
	}
	defer update.Close()

	var id, team string
	var score int64
	var active bool
	var note []byte
	if err := update.QueryRow("platform", 7, nil, "a").Scan(
		&id, &team, &score, &active, &note,
	); err != nil {
		t.Fatal(err)
	}
	if id != "a" || team != "platform" || score != 7 || !active || note != nil {
		t.Fatalf(
			"UPDATE RETURNING = (%q, %q, %d, %v, %#v), want (a, platform, 7, true, nil)",
			id, team, score, active, note,
		)
	}

	if err := db.QueryRow(`
		SELECT team, score, active, note FROM employees WHERE id = 'b'`,
	).Scan(&team, &score, &active, &note); err != nil {
		t.Fatal(err)
	}
	if team != "storage" || score != 2 || active || string(note) != "untouched" {
		t.Fatalf(
			"unselected row = (%q, %d, %v, %#v), want (storage, 2, false, untouched)",
			team, score, active, note,
		)
	}

	// The declared-column path must not alter the existing explicit
	// whole-document replacement contract.
	if _, err := db.Exec(`UPDATE employees SET "$doc" = ? WHERE id = 'a'`,
		`{"id":"a","team":"whole","score":9,"active":false,"note":"replacement"}`,
	); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`
		SELECT team, score, active, note FROM employees WHERE id = 'a'`,
	).Scan(&team, &score, &active, &note); err != nil {
		t.Fatal(err)
	}
	if team != "whole" || score != 9 || active || string(note) != "replacement" {
		t.Fatalf(
			"whole-document row = (%q, %d, %v, %#v), want (whole, 9, false, replacement)",
			team, score, active, note,
		)
	}
}

func TestDeclaredColumnUpdateTransactionPreparedReturning(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`
		CREATE TABLE docs (
			id STRING PRIMARY KEY,
			state STRING NOT NULL,
			n INTEGER NOT NULL,
			keep BOOLEAN NOT NULL
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?)`,
		`{"id":"a","state":"old","n":1,"keep":true}`,
	); err != nil {
		t.Fatal(err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	update, err := tx.Prepare(`
		UPDATE docs SET state = ?, n = ? WHERE id = ?
		RETURNING id, state, n, keep`)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	var id, state string
	var n int64
	var keep bool
	if err := update.QueryRow("staged", 8, "a").Scan(&id, &state, &n, &keep); err != nil {
		_ = update.Close()
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := update.Close(); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if id != "a" || state != "staged" || n != 8 || !keep {
		_ = tx.Rollback()
		t.Fatalf(
			"transaction UPDATE RETURNING = (%q, %q, %d, %v), want (a, staged, 8, true)",
			id, state, n, keep,
		)
	}
	if _, err := tx.Exec(`UPDATE docs SET n = ? WHERE id = ?`, 9, "a"); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.QueryRow(`SELECT state, n, keep FROM docs WHERE id = 'a'`).Scan(
		&state, &n, &keep,
	); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if state != "staged" || n != 9 || !keep {
		_ = tx.Rollback()
		t.Fatalf("transaction view = (%q, %d, %v), want (staged, 9, true)", state, n, keep)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT state, n, keep FROM docs WHERE id = 'a'`).Scan(
		&state, &n, &keep,
	); err != nil {
		t.Fatal(err)
	}
	if state != "staged" || n != 9 || !keep {
		t.Fatalf("committed row = (%q, %d, %v), want (staged, 9, true)", state, n, keep)
	}

	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE docs SET state = ? WHERE id = ?`, "rolled-back", "a"); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT state FROM docs WHERE id = 'a'`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "staged" {
		t.Fatalf("row after rollback state = %q, want staged", state)
	}
}

func TestDeclaredColumnUpdateMultiRowPreservesRowsIndexesAndAtomicity(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`
		CREATE TABLE docs (
			id STRING PRIMARY KEY,
			grp STRING NOT NULL,
			state STRING NOT NULL,
			n INTEGER NOT NULL,
			keep STRING NOT NULL
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE INDEX by_state ON docs(state)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs VALUES (?), (?), (?)`,
		`{"id":"a","grp":"g","state":"old","n":1,"keep":"first","payload":{"tag":"A"}}`,
		`{"id":"b","grp":"g","state":"old","n":2,"keep":"second","payload":{"tag":"B"}}`,
		`{"id":"c","grp":"other","state":"old","n":3,"keep":"third","payload":{"tag":"C"}}`,
	); err != nil {
		t.Fatal(err)
	}

	result, err := db.Exec(`UPDATE docs SET state = 'new', n = 7 WHERE grp = 'g'`)
	if err != nil {
		t.Fatal(err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 2 {
		t.Fatalf("RowsAffected = %d, %v; want 2", affected, err)
	}
	rows, err := db.Query(`
		SELECT id, state, n, keep, payload.tag FROM docs ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	want := []struct {
		id, state, keep, tag string
		n                    int64
	}{
		{"a", "new", "first", "A", 7},
		{"b", "new", "second", "B", 7},
		{"c", "old", "third", "C", 3},
	}
	for i := range want {
		if !rows.Next() {
			t.Fatalf("rows ended at %d: %v", i, rows.Err())
		}
		var id, state, keep, tag string
		var n int64
		if err := rows.Scan(&id, &state, &n, &keep, &tag); err != nil {
			t.Fatal(err)
		}
		if id != want[i].id || state != want[i].state || n != want[i].n ||
			keep != want[i].keep || tag != want[i].tag {
			t.Fatalf(
				"row %d = (%q,%q,%d,%q,%q), want (%q,%q,%d,%q,%q)",
				i, id, state, n, keep, tag, want[i].id, want[i].state,
				want[i].n, want[i].keep, want[i].tag,
			)
		}
	}
	if rows.Next() || rows.Err() != nil {
		t.Fatalf("unexpected trailing rows or error: %v", rows.Err())
	}
	var indexed int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs WHERE state = 'new'`).Scan(&indexed); err != nil {
		t.Fatal(err)
	}
	if indexed != 2 {
		t.Fatalf("indexed new-state count = %d, want 2", indexed)
	}

	_, err = db.Exec(`
		UPDATE docs SET state = 'broken', id = 'a'
		WHERE grp = 'g' ORDER BY id LIMIT 2`)
	if !errors.Is(err, ErrUpdatePrimaryKey) {
		t.Fatalf("multi-row primary-key error = %v, want ErrUpdatePrimaryKey", err)
	}
	rows, err = db.Query(`SELECT id, state FROM docs WHERE grp = 'g' ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for _, id := range []string{"a", "b"} {
		if !rows.Next() {
			t.Fatalf("atomicity rows ended before %q: %v", id, rows.Err())
		}
		var gotID, state string
		if err := rows.Scan(&gotID, &state); err != nil {
			t.Fatal(err)
		}
		if gotID != id || state != "new" {
			t.Fatalf("row after failed multi-update = (%q,%q), want (%q,new)", gotID, state, id)
		}
	}
}

func TestDeclaredColumnUpdateMutationCapture(t *testing.T) {
	database, session := openRuntimeSession(t)
	t.Cleanup(func() {
		_ = session.Close()
		_ = database.Close()
	})
	ctx := context.Background()
	create := runtimePrepare(t, session,
		`CREATE TABLE docs (id STRING PRIMARY KEY, state STRING NOT NULL)`)
	if _, err := create.Exec(ctx, nil); err != nil {
		t.Fatal(err)
	}
	insert := runtimePrepare(t, session,
		`INSERT INTO docs (id, state) VALUES (?, ?)`)
	if _, err := insert.Exec(ctx, []any{"a", "old"}); err != nil {
		t.Fatal(err)
	}
	capture := runtimePrepare(t, session,
		`UPDATE docs SET state = ? WHERE id = ?`)
	var key, document []byte
	if err := capture.CaptureMutationInto(
		ctx, []any{"new", "a"}, func(k, doc []byte) error {
			key = append(key, k...)
			document = append(document, doc...)
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	if len(key) == 0 || string(document) != `{"id":"a","state":"old"}` {
		t.Fatalf("capture key=%x document=%s, want current row", key, document)
	}
	var scanned string
	_, complete, err := session.ScanDocumentsAfter(
		ctx, "docs", nil, 1, 1<<20,
		func(_ []byte, doc []byte) error {
			scanned = string(append([]byte(nil), doc...))
			return nil
		},
	)
	if err != nil || !complete || scanned != `{"id":"a","state":"old"}` {
		t.Fatalf("capture published a mutation: complete=%v document=%s err=%v", complete, scanned, err)
	}
	if _, err := insert.Exec(ctx, []any{"b", "old"}); err != nil {
		t.Fatal(err)
	}
	invalid := runtimePrepare(t, session,
		`UPDATE docs SET id = 'a' WHERE state = 'old' ORDER BY id LIMIT 2`)
	visited := 0
	err = invalid.CaptureMutationInto(ctx, nil, func(_, _ []byte) error {
		visited++
		return nil
	})
	if !errors.Is(err, ErrUpdatePrimaryKey) || visited != 0 {
		t.Fatalf("invalid multi-row capture = %v, visits=%d; want ErrUpdatePrimaryKey before callbacks", err, visited)
	}
}

func TestDeclaredColumnUpdatePlacedCapturePreservesOldDocument(t *testing.T) {
	connector, err := OpenClusterConnector(
		filepath.Join(t.TempDir(), "catalog.vdb"),
		oneShardConfig(t, "docs", "/id"),
	)
	if err != nil {
		t.Fatal(err)
	}
	database := &Database{connector: connector.(*dbConnector)}
	session, err := database.NewSession(context.Background())
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = session.Close()
		_ = database.Close()
	})
	ctx := context.Background()
	create := runtimePrepare(t, session,
		`CREATE TABLE docs (id STRING PRIMARY KEY, state STRING NOT NULL)`)
	if _, err := create.Exec(ctx, nil); err != nil {
		t.Fatal(err)
	}
	insert := runtimePrepare(t, session, `INSERT INTO docs VALUES (?)`)
	const oldDocument = `{"id":"a\nb","state":"old"}`
	if _, err := insert.Exec(ctx, []any{oldDocument}); err != nil {
		t.Fatal(err)
	}
	capture := runtimePrepare(t, session,
		`UPDATE docs SET state = ? WHERE id = ?`)
	var captured []byte
	if err := capture.CaptureMutationInto(
		ctx, []any{"new", "a\nb"}, func(_ []byte, document []byte) error {
			captured = append(captured, document...)
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	if string(captured) != oldDocument {
		t.Fatalf("captured document = %q, want %q", captured, oldDocument)
	}
}

func TestDeclaredColumnUpdateCaptureReportsShardMove(t *testing.T) {
	cfg, _, diff := twoShardClusterConfig(t, "docs")
	connector, err := OpenClusterConnector(
		filepath.Join(t.TempDir(), "catalog.vdb"), cfg,
	)
	if err != nil {
		t.Fatal(err)
	}
	database := &Database{connector: connector.(*dbConnector)}
	session, err := database.NewSession(context.Background())
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = session.Close()
		_ = database.Close()
	})
	ctx := context.Background()
	create := runtimePrepare(t, session,
		`CREATE TABLE docs (tenant_id STRING PRIMARY KEY, state STRING)`)
	if _, err := create.Exec(ctx, nil); err != nil {
		t.Fatal(err)
	}
	insert := runtimePrepare(t, session, `INSERT INTO docs VALUES (?)`)
	if _, err := insert.Exec(ctx, []any{tenantDoc(diff[0], "old")}); err != nil {
		t.Fatal(err)
	}
	capture := runtimePrepare(t, session,
		`UPDATE docs SET tenant_id = ? WHERE tenant_id = ?`)
	visited := false
	err = capture.CaptureMutationInto(
		ctx, []any{diff[1], diff[0]}, func(_, _ []byte) error {
			visited = true
			return nil
		},
	)
	if !errors.Is(err, ErrShardKeyImmutable) || visited {
		t.Fatalf("capture shard-key move = %v, visited=%v; want ErrShardKeyImmutable before visit", err, visited)
	}
}

func TestDeclaredColumnUpdatePreflight(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`
		CREATE TABLE docs (
			id STRING PRIMARY KEY,
			state STRING NOT NULL
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO docs (id, state) VALUES (?, ?)`, "a", "old"); err != nil {
		t.Fatal(err)
	}

	_, err := db.Prepare(`UPDATE docs SET missing = ? WHERE id = ?`)
	var columnErr *query.RelationColumnError
	if !errors.As(err, &columnErr) || !errors.Is(err, query.ErrUndefinedColumn) ||
		columnErr.Relation != "docs" || columnErr.Column != "missing" || columnErr.Pos <= 0 {
		t.Fatalf("unknown assignment prepare error = %#v / %v", columnErr, err)
	}
	if _, err := db.Exec(
		`UPDATE docs SET state = ? WHERE id = ?`, time.Unix(0, 0), "missing",
	); err == nil || !strings.Contains(err.Error(), "not a JSON scalar") {
		t.Fatalf("zero-row assignment bind error = %v", err)
	}
	if _, err := db.Exec(
		`UPDATE docs SET state = ? WHERE id = ?`, string([]byte{0xff}), "a",
	); err == nil || !strings.Contains(err.Error(), "valid UTF-8") {
		t.Fatalf("invalid UTF-8 assignment error = %v", err)
	}
	if _, err := db.Exec(`UPDATE docs SET id = ? WHERE id = ?`, "b", "a"); !errors.Is(err, ErrUpdatePrimaryKey) {
		t.Fatalf("primary-key assignment error = %v, want ErrUpdatePrimaryKey", err)
	}
	if _, err := db.Exec(`UPDATE docs SET state = 7 WHERE id = 'a'`); !errors.Is(err, store.ErrSchemaViolation) {
		t.Fatalf("wrong-type assignment error = %v, want ErrSchemaViolation", err)
	}
	var id, state string
	if err := db.QueryRow(`SELECT id, state FROM docs WHERE id = 'a'`).Scan(&id, &state); err != nil {
		t.Fatal(err)
	}
	if id != "a" || state != "old" {
		t.Fatalf("row after refused updates = (%q, %q), want (a, old)", id, state)
	}
}

func TestApplyColumnAssignmentsPreservesExactRawNumber(t *testing.T) {
	updated, err := ApplyColumnAssignments(
		[]byte(`{"id":"a","n":1}`),
		[]sqlast.UpdateAssignment{{
			Column: "n",
			Value:  sqlast.Operand{Kind: sqlast.OperandParam, Ordinal: 0},
		}},
		[]any{vibejson.RawValue{Src: []byte("9007199254740993")}},
		1<<20,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(updated), `{"id":"a","n":9007199254740993}`; got != want {
		t.Fatalf("updated document = %s, want %s", got, want)
	}
}

func TestApplyColumnAssignmentsPreservesDuplicateAndRawMembers(t *testing.T) {
	updated, err := ApplyColumnAssignments(
		[]byte(`{"id":"a","dup":1,"dup":2,"n":1e0,"obj":{"x": [1, 2]},"escaped\u006bey":"old"}`),
		[]sqlast.UpdateAssignment{
			{Column: "dup", Value: sqlast.Operand{Kind: sqlast.OperandNumber, Text: "3"}},
			{Column: "escapedkey", Value: sqlast.Operand{Kind: sqlast.OperandString, Text: "new"}},
			{Column: "added", Value: sqlast.Operand{Kind: sqlast.OperandNull}},
		},
		nil,
		1<<20,
	)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"id":"a","dup":1,"dup":3,"n":1e0,"obj":{"x": [1, 2]},"escaped\u006bey":"new","added":null}`
	if string(updated) != want {
		t.Fatalf("updated document = %s, want %s", updated, want)
	}
}
