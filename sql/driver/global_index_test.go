package driver

import (
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
