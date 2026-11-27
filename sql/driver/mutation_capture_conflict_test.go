package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"
)

func TestMutationImageCaptureInsertOnConflictDoNothingAndDoUpdate(t *testing.T) {
	ctx := context.Background()
	database, session := openRuntimeSession(t)
	t.Cleanup(func() {
		_ = session.Close()
		_ = database.Close()
	})

	for _, statement := range []string{
		`CREATE TABLE conflict_images (id STRING PRIMARY KEY, email STRING NOT NULL)`,
		`INSERT INTO conflict_images (id, email) VALUES ('existing', 'old@example.com')`,
	} {
		if err := testRuntimeExec(session, statement, nil); err != nil {
			t.Fatal(err)
		}
	}

	// 1. ON CONFLICT DO NOTHING with one existing row and one new row.
	doNothing := runtimePrepare(t, session, `
		INSERT INTO conflict_images (id, email)
		VALUES ('existing', 'ignored@example.com'), ('fresh', 'fresh@example.com')
		ON CONFLICT (id) DO NOTHING`)
	var nothingImages []capturedMutationImage
	if err := doNothing.CaptureMutationImagesInto(ctx, nil, func(key, before, after []byte) error {
		var beforeCopy []byte
		if before != nil {
			beforeCopy = append([]byte(nil), before...)
		}
		nothingImages = append(nothingImages, capturedMutationImage{
			key:    append([]byte(nil), key...),
			before: beforeCopy,
			after:  append([]byte(nil), after...),
		})
		return nil
	}); err != nil {
		t.Fatalf("CaptureMutationImagesInto(DO NOTHING): %v", err)
	}
	if len(nothingImages) != 2 {
		t.Fatalf("DO NOTHING captured %d rows, want 2", len(nothingImages))
	}
	// Existing row: before == after (exact no-op with before+after preconditions).
	if nothingImages[0].before == nil || !bytes.Equal(nothingImages[0].before, nothingImages[0].after) {
		t.Fatalf("DO NOTHING existing row before=%q after=%q, want non-nil equal", nothingImages[0].before, nothingImages[0].after)
	}
	// Fresh row: before == nil (absent precondition), after == canonical new doc.
	if nothingImages[1].before != nil || len(nothingImages[1].after) == 0 {
		t.Fatalf("DO NOTHING fresh row before=%q after=%q, want nil before and non-empty after", nothingImages[1].before, nothingImages[1].after)
	}

	// 2. ON CONFLICT DO UPDATE with one existing row (email changes) and one fresh row.
	doUpdate := runtimePrepare(t, session, `
		INSERT INTO conflict_images (id, email)
		VALUES ('existing', 'updated@example.com'), ('fresh2', 'fresh2@example.com')
		ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email`)
	var updateImages []capturedMutationImage
	if err := doUpdate.CaptureMutationImagesInto(ctx, nil, func(key, before, after []byte) error {
		var beforeCopy []byte
		if before != nil {
			beforeCopy = append([]byte(nil), before...)
		}
		updateImages = append(updateImages, capturedMutationImage{
			key:    append([]byte(nil), key...),
			before: beforeCopy,
			after:  append([]byte(nil), after...),
		})
		return nil
	}); err != nil {
		t.Fatalf("CaptureMutationImagesInto(DO UPDATE): %v", err)
	}
	if len(updateImages) != 2 {
		t.Fatalf("DO UPDATE captured %d rows, want 2", len(updateImages))
	}
	if updateImages[0].before == nil || !bytes.Contains(updateImages[0].before, []byte("old@example.com")) ||
		!bytes.Contains(updateImages[0].after, []byte("updated@example.com")) {
		t.Fatalf("DO UPDATE existing row before=%q after=%q", updateImages[0].before, updateImages[0].after)
	}
	if updateImages[1].before != nil || !bytes.Contains(updateImages[1].after, []byte("fresh2@example.com")) {
		t.Fatalf("DO UPDATE fresh row before=%q after=%q", updateImages[1].before, updateImages[1].after)
	}

	// 3. ValidatePrimaryDocumentDigests absent-key precondition ([32]byte{}):
	// succeeds for absent key, fails with ErrDistributedTransactionConflict for existing key.
	if err := session.Begin(ctx, TxOptions{Isolation: IsolationSerializable}); err != nil {
		t.Fatal(err)
	}
	if err := session.ValidatePrimaryDocumentDigests(
		ctx, "conflict_images", []byte("/id"),
		[][]byte{nothingImages[1].key}, [][sha256.Size]byte{{}},
	); err != nil {
		t.Fatalf("absent-key precondition rejected absent key: %v", err)
	}
	_ = session.Rollback(ctx)

	if err := session.Begin(ctx, TxOptions{Isolation: IsolationSerializable}); err != nil {
		t.Fatal(err)
	}
	err := session.ValidatePrimaryDocumentDigests(
		ctx, "conflict_images", []byte("/id"),
		[][]byte{nothingImages[0].key}, [][sha256.Size]byte{{}},
	)
	if !errors.Is(err, ErrDistributedTransactionConflict) {
		t.Fatalf("absent-key precondition on existing key = %v, want ErrDistributedTransactionConflict", err)
	}
	_ = session.Rollback(ctx)
}
