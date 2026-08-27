package rangesplit

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestChildStageCursorReceiptReopenRepairsDirectoryFence(t *testing.T) {
	stage, batch, _, _ := witnessedStageFixture(t)
	before, _ := stage.Cursor()
	path := filepath.Join(t.TempDir(), "receipt.cursor")
	store, err := OpenChildStageCursorStore(path)
	if err != nil {
		t.Fatal(err)
	}
	prior, err := AppendChildStageCursor(nil, &before)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Persist(prior); err != nil {
		t.Fatal(err)
	}
	receipt := before
	receipt.pendingBatchDigest = batch.Digest
	raw, err := AppendChildStageCursor(nil, &receipt)
	if err != nil {
		t.Fatal(err)
	}
	fault := errors.New("directory sync failed")
	store.syncRoot = func(*os.Root) error { return fault }
	if err = store.Persist(raw); !errors.Is(err, ErrChildStageOutcomeUnknown) {
		t.Fatal(err)
	}
	// The temporary file was actually synced and renamed, but its parent was
	// not. Reading those bytes alone must not authorize the first row write.
	visible, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(visible, raw) {
		t.Fatalf("renamed cursor missing: %v", err)
	}
	if _, _, err = store.Load(nil); !errors.Is(err, ErrChildStageOutcomeUnknown) {
		t.Fatal("stale cached cursor exposed")
	}
	if err = store.Persist(raw); !errors.Is(err, ErrChildStageOutcomeUnknown) {
		t.Fatal("uncertain exact retry bypassed fence")
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	fences := 0
	if recovered, err := openChildStageCursorStore(path, func(*os.Root) error { fences++; return fault }); !errors.Is(err, ErrChildStageOutcomeUnknown) || recovered != nil || fences != 1 {
		t.Fatalf("unfenced recovery store=%v error=%v fences=%d", recovered, err, fences)
	}
	recovered, err := openChildStageCursorStore(path, func(root *os.Root) error { fences++; return syncChildStageCursorRoot(root) })
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	loaded, ok, err := recovered.Load(nil)
	if err != nil || !ok || !bytes.Equal(loaded, raw) || fences != 2 {
		t.Fatalf("repaired load: present=%v error=%v fences=%d", ok, err, fences)
	}
	reopened, err := NewChildStage(stage.partitioner, stage.expected, stage.collection, loaded)
	if err != nil {
		t.Fatal(err)
	}
	if err = reopened.ApplyTailBatch(batch, recovered.Persist); err != nil {
		t.Fatal(err)
	}
	if stage.collection.Len() != 2 {
		t.Fatal("repaired receipt replay failed")
	}
}
