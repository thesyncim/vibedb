package durable

import (
	"fmt"
	"os"
	"testing"
)

func putSpaceRows(t *testing.T, collection *Collection, first, count int) {
	t.Helper()
	for i := range count {
		key := fmt.Sprintf("space-%03d", first+i)
		value := fmt.Appendf(nil, `{"key":%q,"value":%d}`, key, first+i)
		if created, err := collection.Put(key, value); err != nil || !created {
			t.Fatalf("Put(%q) = (%v,%v), want (true,nil)", key, created, err)
		}
	}
}

func fileKeyLocation(t *testing.T, collection *Collection, key string) (uint32, uint8) {
	t.Helper()
	state := collection.state.Load()
	match, found, err := collection.resolveFileFingerprint(state, []byte(key))
	if err != nil || !found {
		t.Fatalf("lookup %q = (%+v,%v,%v)", key, match.location, found, err)
	}
	defer match.Release()
	return match.location.Chunk, match.location.Slot
}

func TestFileStoreReusesDeletedSlotsAcrossReopen(t *testing.T) {
	options := testFileStoreOptions()
	file, err := os.CreateTemp(t.TempDir(), "file-store-space-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	putSpaceRows(t, collection, 0, 12)
	if root := collection.state.Load().root; root.ChunkHighWater != 3 ||
		root.LiveChunks != 3 || root.FreeChunkHint != 3 {
		t.Fatalf("dense root = %+v, want high-water/live/hint 3/3/3", root)
	}
	oldChunk, oldSlot := fileKeyLocation(t, collection, "space-001")
	if deleted, deleteErr := collection.Delete("space-001"); deleteErr != nil || !deleted {
		t.Fatalf("Delete = (%v,%v), want (true,nil)", deleted, deleteErr)
	}
	if got := collection.state.Load().root.FreeChunkHint; got != oldChunk {
		t.Fatalf("delete free-chunk hint = %d, want %d", got, oldChunk)
	}
	if stats := collection.Stats(); stats.ChunkSlots != 12 ||
		stats.VacantChunkSlots != 1 || stats.ChunkHighWater != 3 {
		t.Fatalf("deleted space stats = %+v", stats)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}

	collection, err = Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	if created, putErr := collection.Put("space-reused", []byte(`{"reused":true}`)); putErr != nil || !created {
		t.Fatalf("reusing Put = (%v,%v), want (true,nil)", created, putErr)
	}
	chunk, slot := fileKeyLocation(t, collection, "space-reused")
	if chunk != oldChunk || slot != oldSlot {
		t.Fatalf("reused location = %d:%d, want deleted %d:%d", chunk, slot, oldChunk, oldSlot)
	}
	root := collection.state.Load().root
	if root.ChunkHighWater != 3 || root.LiveChunks != 3 ||
		root.DocumentCount != 12 || root.FreeChunkHint > 1 {
		t.Fatalf("reused root = %+v, want dense 12 rows in three chunks", root)
	}
	if stats := collection.Stats(); stats.ChunkSlots != 12 ||
		stats.VacantChunkSlots != 0 || stats.ChunkHighWater != 3 {
		t.Fatalf("reused space stats = %+v", stats)
	}
}

func TestFileStoreBatchReusesPublishedHolesWithoutGrowingChunks(t *testing.T) {
	collection, _ := openBatchCollection(t, testBatchOptions(8))
	putSpaceRows(t, collection, 0, 16)
	if err := collection.Update(func(batch *WriteBatch) error {
		for _, key := range []string{"space-001", "space-005", "space-009", "space-013"} {
			if err := batch.Delete(key); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	deletedRoot := collection.state.Load().root
	if deletedRoot.ChunkHighWater != 2 || deletedRoot.FreeChunkHint != 0 ||
		deletedRoot.DocumentCount != 12 {
		t.Fatalf("deleted root = %+v", deletedRoot)
	}
	if stats := collection.Stats(); stats.ChunkSlots != 16 || stats.VacantChunkSlots != 4 {
		t.Fatalf("batch-deleted space stats = %+v", stats)
	}
	if err := collection.Update(func(batch *WriteBatch) error {
		for i := range 4 {
			key := fmt.Sprintf("batch-reused-%d", i)
			if err := batch.Put(key, fmt.Appendf(nil, `{"reused":%d}`, i)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	root := collection.state.Load().root
	if root.ChunkHighWater != 2 || root.LiveChunks != 2 ||
		root.DocumentCount != 16 || root.FreeChunkHint != 2 {
		t.Fatalf("batch-reused root = %+v, want dense 16 rows in two chunks", root)
	}
	if stats := collection.Stats(); stats.ChunkSlots != 16 || stats.VacantChunkSlots != 0 {
		t.Fatalf("batch-reused space stats = %+v", stats)
	}
	for i := range 4 {
		chunk, slot := fileKeyLocation(t, collection, fmt.Sprintf("batch-reused-%d", i))
		if slot != uint8(1+4*(i%2)) || chunk != uint32(i/2) {
			t.Fatalf("batch key %d location = %d:%d, want a published hole", i, chunk, slot)
		}
	}
}
