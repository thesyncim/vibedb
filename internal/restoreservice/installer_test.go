package restoreservice

import (
	"bytes"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
)

func TestArtifactSpoolAndCursorAreExactCrashReplayState(t *testing.T) {
	root := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	artifact := bytes.Repeat([]byte("authenticated-artifact"), 1024)
	digest := sha256.Sum256(artifact)
	path := filepath.Join(root, "artifact.snap")
	if err := materializeArtifact(path, bytes.NewReader(artifact), uint64(len(artifact)), digest); err != nil {
		t.Fatal(err)
	}
	// A crash replay consumes the already-synced spool, not a possibly partial
	// replacement reader supplied by a restarted controller.
	if err := materializeArtifact(path, bytes.NewReader(nil), uint64(len(artifact)), digest); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, artifact) {
		t.Fatalf("spool bytes=%d err=%v", len(got), err)
	}
	cursorPath := filepath.Join(root, "replica-1.cursor")
	wantCursor := bytes.Repeat([]byte{0xa5}, 4096)
	if err := replaceFile(cursorPath, wantCursor); err != nil {
		t.Fatal(err)
	}
	cursor, err := readCursor(cursorPath)
	if err != nil || !bytes.Equal(cursor, wantCursor) {
		t.Fatalf("cursor bytes=%d err=%v", len(cursor), err)
	}
	wantCursor[0] ^= 1
	if bytes.Equal(cursor, wantCursor) {
		t.Fatal("cursor aliases caller buffer")
	}
}

func TestArtifactSpoolRejectsWrongBoundAndExistingCorruption(t *testing.T) {
	root := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	raw := []byte("exact")
	digest := sha256.Sum256(raw)
	path := filepath.Join(root, "artifact.snap")
	if err := materializeArtifact(path, bytes.NewReader(raw), uint64(len(raw)-1), digest); err == nil {
		t.Fatal("accepted truncated artifact bound")
	}
	if err := os.WriteFile(path, []byte("forged"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := materializeArtifact(path, bytes.NewReader(raw), uint64(len(raw)), digest); err == nil {
		t.Fatal("accepted corrupt existing spool")
	}
}

func TestArtifactSpoolRecoversTornTemporaryAndRejectsUnsafeDirectory(t *testing.T) {
	root := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	raw := []byte("complete-authenticated-artifact")
	digest := sha256.Sum256(raw)
	path := filepath.Join(root, "artifact.snap")
	if err := os.WriteFile(path+".tmp", raw[:7], 0o600); err != nil {
		t.Fatal(err)
	}
	if err := materializeArtifact(path, bytes.NewReader(raw), uint64(len(raw)), digest); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, raw) {
		t.Fatalf("spool=%q err=%v", got, err)
	}
	if err := os.Chmod(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if privateRoot(root) {
		t.Fatal("world-readable restore directory accepted")
	}
}
