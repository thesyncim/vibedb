package driver

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
)

func TestMutationCaptureAndSerializablePrimaryGuard(t *testing.T) {
	database, session := openRuntimeSession(t)
	other, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = other.Close()
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
		`INSERT INTO docs (id, state) VALUES (?, ?), (?, ?)`)
	if _, err := insert.Exec(ctx, []any{"a", "old", "b", "old"}); err != nil {
		t.Fatal(err)
	}
	capture := runtimePrepare(t, session, `DELETE FROM docs WHERE id = ?`)
	var key, document []byte
	if err := capture.CaptureMutationInto(ctx, []any{"a"}, func(k, doc []byte) error {
		key = append(key[:0], k...)
		document = append(document[:0], doc...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(key) == 0 || string(document) != `{"id":"a","state":"old"}` {
		t.Fatalf("capture key=%x document=%s", key, document)
	}

	concurrent := runtimePrepare(t, other,
		`UPDATE docs SET "$doc" = ? WHERE id = ?`)
	if _, err := concurrent.Exec(ctx, []any{`{"id":"a","state":"new"}`, "a"}); err != nil {
		t.Fatal(err)
	}
	oldDigest := [][sha256.Size]byte{sha256.Sum256(document)}
	if err := session.Begin(ctx, TxOptions{Isolation: IsolationSerializable}); err != nil {
		t.Fatal(err)
	}
	if err := session.ValidatePrimaryDocumentDigests(
		ctx, "docs", []byte("/id"), [][]byte{key}, oldDigest,
	); !errors.Is(err, ErrDistributedTransactionConflict) {
		t.Fatalf("stale digest validation = %v", err)
	}
	_ = session.Rollback(ctx)

	key = key[:0]
	document = document[:0]
	if err := capture.CaptureMutationInto(ctx, []any{"a"}, func(k, doc []byte) error {
		key = append(key, k...)
		document = append(document, doc...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	currentDigest := [][sha256.Size]byte{sha256.Sum256(document)}
	if err := session.Begin(ctx, TxOptions{Isolation: IsolationSerializable}); err != nil {
		t.Fatal(err)
	}
	if err := session.ValidatePrimaryDocumentDigests(
		ctx, "docs", []byte("/id"), [][]byte{key}, currentDigest,
	); err != nil {
		t.Fatal(err)
	}
	deleteAll := runtimePrepare(t, session, `DELETE FROM docs`)
	if _, err := deleteAll.Exec(ctx, nil); !errors.Is(err, ErrDistributedTransactionConflict) {
		t.Fatalf("guarded broader DELETE = %v", err)
	}
	_ = session.Rollback(ctx)

	insertOne := runtimePrepare(t, other, `INSERT INTO docs (id, state) VALUES (?, ?)`)
	if _, err := insertOne.Exec(ctx, []any{"c", "new"}); err != nil {
		t.Fatal(err)
	}
	if err := session.Begin(ctx, TxOptions{Isolation: IsolationSerializable}); err != nil {
		t.Fatal(err)
	}
	if err := session.ValidatePrimaryDocumentDigests(
		ctx, "docs", []byte("/id"), nil, nil,
	); err != nil {
		t.Fatal(err)
	}
	deleteC := runtimePrepare(t, session, `DELETE FROM docs WHERE id = ?`)
	if _, err := deleteC.Exec(ctx, []any{"c"}); !errors.Is(err, ErrDistributedTransactionConflict) {
		t.Fatalf("empty capture phantom guard = %v", err)
	}
	_ = session.Rollback(ctx)
}
