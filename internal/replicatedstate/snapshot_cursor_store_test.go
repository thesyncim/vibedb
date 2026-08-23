package replicatedstate

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func snapshotArtifactPersistedCursors(t testing.TB) (first, last []byte) {
	t.Helper()
	_, snapshot := snapshotArtifactFixture(t)
	artifact, _ := writeSnapshotArtifactFixture(t, snapshot)
	if _, err := VerifySnapshotArtifact(bytes.NewReader(artifact), SnapshotArtifactCallbacks{
		Chunk: func(_ SnapshotArtifactCheckpoint, next *SnapshotArtifactCursor) error {
			encoded, err := AppendSnapshotArtifactCursor(nil, next)
			if err != nil {
				return err
			}
			if first == nil {
				first = encoded
			}
			last = encoded
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	return first, last
}

func TestSnapshotCursorStoreDurableMonotoneReplacement(t *testing.T) {
	first, last := snapshotArtifactPersistedCursors(t)
	path := filepath.Join(t.TempDir(), "snapshot.cursor")
	store, err := OpenSnapshotCursorStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if raw, err := store.Load(); err != nil || raw != nil {
		t.Fatalf("initial Load = %x, %v", raw, err)
	}
	if err := store.Persist(first); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load()
	if err != nil || !bytes.Equal(loaded, first) {
		t.Fatalf("first Load = %x, %v", loaded, err)
	}
	loaded[0] ^= 1
	again, err := store.Load()
	if err != nil || !bytes.Equal(again, first) {
		t.Fatal("Load result aliases cursor store")
	}
	if err := store.Persist(first); err != nil {
		t.Fatalf("idempotent Persist: %v", err)
	}
	if err := store.Persist(last); err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(first); !errors.Is(err, ErrSnapshotStage) {
		t.Fatalf("cursor regression error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = OpenSnapshotCursorStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	loaded, err = store.Load()
	if err != nil || !bytes.Equal(loaded, last) {
		t.Fatalf("reopened Load = %x, %v", loaded, err)
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() != filepath.Base(path) && entry.Name() != filepath.Base(path)+".lock" {
			t.Fatalf("unexpected cursor temporary %q", entry.Name())
		}
	}
}

func TestSnapshotCursorStoreExclusiveWriterAndCorruption(t *testing.T) {
	first, _ := snapshotArtifactPersistedCursors(t)
	path := filepath.Join(t.TempDir(), "snapshot.cursor")
	store, err := OpenSnapshotCursorStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(first); err != nil {
		t.Fatal(err)
	}
	if second, err := OpenSnapshotCursorStore(path); second != nil ||
		(!errors.Is(err, ErrSnapshotStage) && !errors.Is(err, storeio.ErrWriterLocked)) {
		t.Fatalf("second writer = %+v, %v", second, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, 0); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if reopened, err := OpenSnapshotCursorStore(path); reopened != nil ||
		!errors.Is(err, ErrSnapshotArtifact) {
		t.Fatalf("corrupt reopen = %+v, %v", reopened, err)
	}
}

func TestSnapshotCursorStoreRejectsForeignArtifact(t *testing.T) {
	first, _ := snapshotArtifactPersistedCursors(t)
	path := filepath.Join(t.TempDir(), "snapshot.cursor")
	store, err := OpenSnapshotCursorStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Persist(first); err != nil {
		t.Fatal(err)
	}
	cursor, err := OpenSnapshotArtifactCursor(first)
	if err != nil {
		t.Fatal(err)
	}
	foreign := *cursor
	foreign.manifest = cloneSnapshotArtifactManifest(cursor.manifest)
	foreign.manifest.State.Binding.GroupID[0] ^= 1
	// Reconstruct a valid foreign state/header/cursor instead of merely
	// corrupting the cursor checksum.
	stateEnvelope, err := AppendState(nil, foreign.manifest.State)
	if err != nil {
		t.Fatal(err)
	}
	header, digest, err := makeSnapshotArtifactHeader(
		stateEnvelope, string(foreign.manifest.UserCollection),
		int(foreign.manifest.TargetChunkBytes),
	)
	if err != nil {
		t.Fatal(err)
	}
	foreign.manifest.HeaderDigest = digest
	foreign.manifest.LastChunkDigest = digest
	foreign.previousDigest = digest
	foreign.nextSequence = 0
	foreign.manifest.Chunks = 0
	foreign.manifest.SystemRows = 0
	foreign.manifest.UserRows = 0
	foreign.manifest.PayloadBytes = 0
	foreign.encodedBytes = uint64(len(header))
	foreign.previousKeyBytes = 0
	foreign.currentCollection = SnapshotArtifactSystem
	foreign.stateRowSeen = false
	foreign.expectedStateDocument = stateEnvelope
	foreignRaw, err := AppendSnapshotArtifactCursor(nil, &foreign)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(foreignRaw); !errors.Is(err, ErrSnapshotStage) {
		t.Fatalf("foreign cursor error = %v", err)
	}
}
