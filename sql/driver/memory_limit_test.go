package driver

import (
	"context"
	"errors"
	"testing"
)

const testMinimumSessionMemoryBytes int64 = 64 << 10

func TestSessionMemoryLimitRejectsInvalidWithoutMutation(t *testing.T) {
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()

	if err := session.SetMemoryLimit(testMinimumSessionMemoryBytes); err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int64{-1, 1, testMinimumSessionMemoryBytes - 1} {
		if err := session.SetMemoryLimit(limit); err == nil {
			t.Fatalf("SetMemoryLimit(%d) succeeded", limit)
		}
		if got := session.conn.exec.Options.MemoryBytes; got != testMinimumSessionMemoryBytes {
			t.Fatalf("SetMemoryLimit(%d) changed limit to %d", limit, got)
		}
	}

	for _, limit := range []int64{
		0,
		testMinimumSessionMemoryBytes,
		2 * testMinimumSessionMemoryBytes,
	} {
		if err := session.SetMemoryLimit(limit); err != nil {
			t.Fatalf("SetMemoryLimit(%d): %v", limit, err)
		}
		if got := session.conn.exec.Options.MemoryBytes; got != limit {
			t.Fatalf("memory limit = %d, want %d", got, limit)
		}
	}
}

func TestSessionMemoryLimitLifecycleGuardsAreAtomic(t *testing.T) {
	var nilSession *Session
	if err := nilSession.SetMemoryLimit(testMinimumSessionMemoryBytes); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("nil Session SetMemoryLimit = %v", err)
	}
	var zero Session
	if err := zero.SetMemoryLimit(testMinimumSessionMemoryBytes); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("zero Session SetMemoryLimit = %v", err)
	}

	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()
	ctx := context.Background()
	const original = 128 << 10
	const replacement = 256 << 10
	if err := session.SetMemoryLimit(original); err != nil {
		t.Fatal(err)
	}

	create := runtimePrepare(t, session, `CREATE TABLE memory_docs (id STRING PRIMARY KEY)`)
	if _, err := create.Exec(ctx, nil); err != nil {
		t.Fatal(err)
	}
	selectAll := runtimePrepare(t, session, `SELECT id FROM memory_docs`)
	cursor, err := selectAll.Query(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.SetMemoryLimit(replacement); !errors.Is(err, ErrCursorOpen) {
		t.Fatalf("SetMemoryLimit with live cursor = %v", err)
	}
	if got := session.conn.exec.Options.MemoryBytes; got != original {
		t.Fatalf("live-cursor refusal changed limit to %d", got)
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}

	if err := session.Begin(ctx, TxOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := session.SetMemoryLimit(replacement); !errors.Is(err, ErrTransactionActive) {
		t.Fatalf("SetMemoryLimit in transaction = %v", err)
	}
	if got := session.conn.exec.Options.MemoryBytes; got != original {
		t.Fatalf("transaction refusal changed limit to %d", got)
	}
	session.MarkFailed()
	if err := session.SetMemoryLimit(replacement); !errors.Is(err, ErrTransactionActive) {
		t.Fatalf("SetMemoryLimit in failed transaction = %v", err)
	}
	if got := session.conn.exec.Options.MemoryBytes; got != original {
		t.Fatalf("failed-transaction refusal changed limit to %d", got)
	}
	if err := session.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := session.SetMemoryLimit(replacement); err != nil {
		t.Fatal(err)
	}

	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if err := session.SetMemoryLimit(original); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("closed Session SetMemoryLimit = %v", err)
	}
}

func TestSessionMemoryLimitExecutionAndReuse(t *testing.T) {
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()
	ctx := context.Background()

	create := runtimePrepare(t, session, `CREATE TABLE memory_reuse (id STRING PRIMARY KEY)`)
	if _, err := create.Exec(ctx, nil); err != nil {
		t.Fatal(err)
	}
	insert := runtimePrepare(t, session, `INSERT INTO memory_reuse VALUES (?)`)
	if _, err := insert.Exec(ctx, []any{[]byte(`{"id":"a"}`)}); err != nil {
		t.Fatal(err)
	}
	selectAll := runtimePrepare(t, session, `SELECT id FROM memory_reuse ORDER BY id`)

	if err := session.SetMemoryLimit(testMinimumSessionMemoryBytes); err != nil {
		t.Fatal(err)
	}
	queryAndClose := func() {
		cursor, err := selectAll.Query(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !cursor.Next() {
			t.Fatal("memory-limited query returned no row")
		}
		if id, ok := cursor.Cell(0).Text(); !ok || id != "a" {
			t.Fatalf("id = (%q, %t), want a", id, ok)
		}
		if err := cursor.Close(); err != nil {
			t.Fatal(err)
		}
	}
	queryAndClose()

	if err := session.SetMemoryLimit(1); err == nil {
		t.Fatal("sub-minimum memory limit succeeded")
	}
	if got := session.conn.exec.Options.MemoryBytes; got != testMinimumSessionMemoryBytes {
		t.Fatalf("invalid limit changed reusable session to %d", got)
	}
	queryAndClose()

	if err := session.SetMemoryLimit(0); err != nil {
		t.Fatal(err)
	}
	queryAndClose()
}
