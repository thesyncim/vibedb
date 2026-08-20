package driver

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func openGlobalIndexTestSession(t *testing.T) (*Database, *Session) {
	t.Helper()
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "global-index.vdb"))
	if err != nil {
		t.Fatal(err)
	}
	session, err := database.NewSession(ctx)
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	create, err := session.Prepare(ctx, `CREATE TABLE messages_by_email (PRIMARY KEY (id))`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := create.Exec(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := create.Close(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = session.Close()
		_ = database.Close()
	})
	return database, session
}

func TestGlobalIndexLookupPointPrefixFenceAndBounds(t *testing.T) {
	_, session := openGlobalIndexTestSession(t)
	ctx := context.Background()
	key := []byte{1, 5, 'a', '@', 'b'}
	uniqueValue := []byte(`["tenant-7",7]`)

	if err := session.Begin(ctx, TxOptions{Isolation: IsolationSerializable}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.ApplyGlobalIndexMutation(
		ctx, "messages_by_email", 17, 3, key, uniqueValue, 2, true, false,
	); err != nil {
		t.Fatal(err)
	}
	if err := session.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	var got [][]byte
	collect := func(locator []byte) error {
		got = append(got, append([]byte(nil), locator...))
		return nil
	}
	if err := session.LookupGlobalIndex(
		ctx, "messages_by_email", 17, 3, key, 2, true, 8, 1024, collect,
	); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !bytes.Equal(got[0], uniqueValue) {
		t.Fatalf("unique lookup = %q, want %q", got, uniqueValue)
	}
	got = got[:0]
	missing := append(append([]byte(nil), key...), 99)
	if err := session.LookupGlobalIndex(
		ctx, "messages_by_email", 17, 3, missing, 2, true, 8, 1024, collect,
	); err != nil || len(got) != 0 {
		t.Fatalf("missing unique lookup = %q,%v", got, err)
	}
	if err := session.LookupGlobalIndex(
		ctx, "messages_by_email", 17, 4, key, 2, true, 8, 1024, collect,
	); !errors.Is(err, ErrGlobalIndexFence) {
		t.Fatalf("stale lookup = %v, want incarnation fence", err)
	}

	prefix := []byte{1, 3, 'n', 'o', 'n'}
	values := [][]byte{[]byte(`["a",1]`), []byte(`["b",2]`), []byte(`["c",3]`)}
	if err := session.Begin(ctx, TxOptions{Isolation: IsolationSerializable}); err != nil {
		t.Fatal(err)
	}
	for i := range values {
		entry := append(append([]byte(nil), prefix...), byte(i+1))
		if _, err := session.ApplyGlobalIndexMutation(
			ctx, "messages_by_email", 17, 3, entry, values[i], 2, false, false,
		); err != nil {
			t.Fatal(err)
		}
	}
	if err := session.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	got = got[:0]
	if err := session.LookupGlobalIndex(
		ctx, "messages_by_email", 17, 3, prefix, 2, false, 8, 1024, collect,
	); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(values) {
		t.Fatalf("prefix lookup rows = %d, want %d", len(got), len(values))
	}
	for i := range values {
		if !bytes.Equal(got[i], values[i]) {
			t.Fatalf("prefix locator %d = %s, want %s", i, got[i], values[i])
		}
	}
	got = got[:0]
	if err := session.LookupGlobalIndex(
		ctx, "messages_by_email", 17, 3, prefix, 2, false, 2, 1024, collect,
	); !errors.Is(err, ErrGlobalIndexLookupTooLarge) {
		t.Fatalf("row bound = %v, want lookup-too-large", err)
	}
	if err := session.LookupGlobalIndex(
		ctx, "messages_by_email", 17, 3, prefix, 2, false, 8, 1, collect,
	); !errors.Is(err, ErrGlobalIndexLookupTooLarge) {
		t.Fatalf("byte bound = %v, want lookup-too-large", err)
	}
}

func globalIndexEntry(t *testing.T, session *Session, key []byte) ([]byte, bool) {
	t.Helper()
	if err := session.Begin(context.Background(), TxOptions{ReadOnly: true, Isolation: IsolationSnapshot}); err != nil {
		t.Fatal(err)
	}
	state := session.conn.tx.tables["messages_by_email"]
	raw, found, err := state.appendRaw(nil, string(key))
	if rollbackErr := session.Rollback(context.Background()); err == nil {
		err = rollbackErr
	}
	if err != nil {
		t.Fatal(err)
	}
	return raw, found
}

func TestGlobalIndexMutationFenceUniquenessAndDelete(t *testing.T) {
	_, session := openGlobalIndexTestSession(t)
	ctx := context.Background()
	key := []byte{1, 5, 'a', '@', 'b'}
	value := []byte(`["tenant\u002d7",7.00e0]`)

	if err := session.Begin(ctx, TxOptions{Isolation: IsolationSerializable}); err != nil {
		t.Fatal(err)
	}
	changed, err := session.ApplyGlobalIndexMutation(
		ctx, "messages_by_email", 17, 3, key, value, 2, true, false,
	)
	if err != nil || !changed {
		t.Fatalf("initial put = %v,%v", changed, err)
	}
	if err := session.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if got, found := globalIndexEntry(t, session, key); !found ||
		!globalIndexLocatorsEqual(got, value, 2) {
		t.Fatalf("entry = %s,%v", got, found)
	}

	// Equivalent JSON string/number spellings identify the same locator and
	// avoid a write-amplifying replacement.
	if err := session.Begin(ctx, TxOptions{Isolation: IsolationSerializable}); err != nil {
		t.Fatal(err)
	}
	changed, err = session.ApplyGlobalIndexMutation(
		ctx, "messages_by_email", 17, 3, key,
		[]byte(`["tenant-7",7]`), 2, true, false,
	)
	if err != nil || changed {
		t.Fatalf("idempotent put = %v,%v", changed, err)
	}
	if err := session.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	if err := session.Begin(ctx, TxOptions{Isolation: IsolationSerializable}); err != nil {
		t.Fatal(err)
	}
	_, err = session.ApplyGlobalIndexMutation(
		ctx, "messages_by_email", 17, 3, key,
		[]byte(`["other",7]`), 2, true, false,
	)
	if !errors.Is(err, ErrGlobalIndexUniqueConflict) ||
		!errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("unique conflict = %v", err)
	}
	if err := session.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	if err := session.Begin(ctx, TxOptions{Isolation: IsolationSerializable}); err != nil {
		t.Fatal(err)
	}
	_, err = session.ApplyGlobalIndexMutation(
		ctx, "messages_by_email", 17, 4, key, value, 2, true, false,
	)
	if !errors.Is(err, ErrGlobalIndexFence) {
		t.Fatalf("incarnation conflict = %v", err)
	}
	if err := session.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	if err := session.Begin(ctx, TxOptions{Isolation: IsolationSerializable}); err != nil {
		t.Fatal(err)
	}
	changed, err = session.ApplyGlobalIndexMutation(
		ctx, "messages_by_email", 17, 3, key,
		[]byte(`["tenant-7",7]`), 2, false, true,
	)
	if err != nil || !changed {
		t.Fatalf("delete = %v,%v", changed, err)
	}
	if err := session.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, found := globalIndexEntry(t, session, key); found {
		t.Fatal("deleted global index entry remains")
	}
}
