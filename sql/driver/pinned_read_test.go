package driver

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
)

// TestBeginPinnedReadScopesSnapshotLeases proves the single-statement read
// pin leases only its executable closure: beginning and rolling back a
// pinned read over one of three tables must allocate strictly less than the
// equivalent full Repeatable Read begin, and the pinned transaction must
// still execute the point read it was scoped for.
func TestBeginPinnedReadScopesSnapshotLeases(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "shard.vdb")
	binding := ShardStoreBinding{
		Distribution: distribution.DistributionName("tenant_data"),
		Shard:        distribution.ShardID("-80"), AllocationGeneration: 1,
	}
	db, err := InitializeShardStore(path, binding)
	if err != nil {
		t.Fatalf("InitializeShardStore: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	session, err := db.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	for _, ddl := range []string{
		`CREATE TABLE docs (id STRING PRIMARY KEY, n INTEGER NOT NULL)`,
		`CREATE TABLE docs_b (id STRING PRIMARY KEY, n INTEGER NOT NULL)`,
		`CREATE TABLE docs_c (id STRING PRIMARY KEY, n INTEGER NOT NULL)`,
	} {
		create, err := session.Prepare(ctx, ddl)
		if err != nil {
			t.Fatalf("prepare %q: %v", ddl, err)
		}
		if _, err := create.Exec(ctx, nil); err != nil {
			t.Fatalf("exec %q: %v", ddl, err)
		}
		_ = create.Close()
	}
	insert, err := session.Prepare(ctx, `INSERT INTO docs (id, n) VALUES (?, ?)`)
	if err != nil {
		t.Fatalf("prepare INSERT: %v", err)
	}
	for _, id := range []string{"a", "b"} {
		if err := session.Begin(ctx, TxOptions{}); err != nil {
			t.Fatalf("Begin: %v", err)
		}
		if _, err := insert.Exec(ctx, []any{id, int64(1)}); err != nil {
			t.Fatalf("INSERT: %v", err)
		}
		if err := session.Commit(ctx); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}
	_ = insert.Close()

	read, err := session.Prepare(ctx, `SELECT n FROM docs WHERE id = ?`)
	if err != nil {
		t.Fatalf("prepare SELECT: %v", err)
	}
	defer func() { _ = read.Close() }()

	// The pinned transaction executes the read it was scoped for.
	if err := session.BeginPinnedRead(ctx, read); err != nil {
		t.Fatalf("BeginPinnedRead: %v", err)
	}
	if got := len(session.conn.tx.tables); got != 1 {
		t.Fatalf("pinned transaction tables = %d, want 1 scoped lease", got)
	}
	var cursor Cursor
	if err := read.QueryInto(ctx, []any{"a"}, &cursor); err != nil {
		t.Fatalf("QueryInto: %v", err)
	}
	rows := 0
	for cursor.Next() {
		rows++
	}
	_ = cursor.Close()
	if err := session.Rollback(ctx); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if rows != 1 {
		t.Fatalf("pinned read rows = %d, want 1", rows)
	}

	full := testing.AllocsPerRun(100, func() {
		if err := session.Begin(ctx, TxOptions{ReadOnly: true, Isolation: IsolationRepeatableRead}); err != nil {
			panic(err)
		}
		if err := session.Rollback(ctx); err != nil {
			panic(err)
		}
	})
	pinned := testing.AllocsPerRun(100, func() {
		if err := session.BeginPinnedRead(ctx, read); err != nil {
			panic(err)
		}
		if err := session.Rollback(ctx); err != nil {
			panic(err)
		}
	})
	t.Logf("begin/rollback allocs: full=%.1f pinned=%.1f", full, pinned)
	if pinned >= full {
		t.Fatalf("pinned begin allocs = %.1f, want strictly fewer than full %.1f", pinned, full)
	}
}
