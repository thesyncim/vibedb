package durable

import (
	"fmt"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// fileStoreChunkTreeShape returns the height of the live chunk directory and
// the shift its root records. Height is what a copy-on-write Put pays: one
// rewritten page per level, every time.
func fileStoreChunkTreeShape(t *testing.T, s *Collection) (height int, rootShift uint8) {
	t.Helper()
	state := s.state.Load()
	bounds := storeio.ChunkTreeBounds{
		FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID,
	}
	ref := state.chunkRoot
	for ref != (storeio.PageRef{}) {
		lease, err := s.cache.Acquire(ref)
		if err != nil {
			t.Fatalf("acquire directory level %d: %v", height, err)
		}
		view, err := storeio.OpenChunkDirectoryPage(lease.Page(), bounds.FileEnd, bounds.NextLogicalID)
		if err != nil {
			lease.Release()
			t.Fatalf("open directory level %d: %v", height, err)
		}
		header := view.Header()
		if height == 0 {
			rootShift = header.Shift
		}
		height++
		child, ok := view.RefAt(0)
		lease.Release()
		if !ok {
			t.Fatalf("directory level %d has no child", height-1)
		}
		if header.Shift == 0 {
			break
		}
		ref = child
	}
	return height, rootShift
}

func newChunkHeightCollection(t *testing.T) *Collection {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "chunk-height-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	options := testFileStoreOptions()
	// One document per chunk makes the chunk id equal the document ordinal, so
	// the test can name the exact count at which the tree must grow a level.
	options.Collection.ChunkDocuments = 1
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = collection.Close() })
	return collection
}

// TestFileStoreChunkDirectoryHeightTracksChunkCount pins the shape the write
// path depends on: the radix directory is as tall as the live chunk ids
// require and no taller. A tree that always reached the uint32 ceiling would
// be six levels here, so every Put would rewrite six pages instead of one or
// two — correct, but a constant write-amplification surcharge no store size
// ever amortizes.
func TestFileStoreChunkDirectoryHeightTracksChunkCount(t *testing.T) {
	collection := newChunkHeightCollection(t)
	put := func(i int) {
		t.Helper()
		if _, err := collection.Put(fmt.Sprintf("key-%06d", i), []byte(fmt.Sprintf(`{"id":%d}`, i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	for i := range 64 {
		put(i)
	}
	// Chunks 0..63 share one 64-lane leaf, so the root is that leaf.
	if height, shift := fileStoreChunkTreeShape(t, collection); height != 1 || shift != 0 {
		t.Fatalf("64 chunks = height %d shift %d, want height 1 shift 0", height, shift)
	}
	// Chunk 64 falls outside the leaf's span and must raise the tree exactly
	// one level, not rebuild it at full depth.
	put(64)
	if height, shift := fileStoreChunkTreeShape(t, collection); height != 2 || shift != 6 {
		t.Fatalf("65 chunks = height %d shift %d, want height 2 shift 6", height, shift)
	}
	for i := 65; i < 200; i++ {
		put(i)
	}
	if height, shift := fileStoreChunkTreeShape(t, collection); height != 2 || shift != 6 {
		t.Fatalf("200 chunks = height %d shift %d, want height 2 shift 6", height, shift)
	}
	for i := range 200 {
		got, ok, err := collection.AppendRaw(nil, fmt.Sprintf("key-%06d", i))
		want := fmt.Sprintf(`{"id":%d}`, i)
		if err != nil || !ok || string(got) != want {
			t.Fatalf("key %d = (%q,%v,%v), want %q", i, got, ok, err, want)
		}
	}
}

// TestFileStoreChunkDirectoryGrowthPreservesLookups exercises the level the
// growth path inserts above an existing root. Every key written before the
// tree grew must still resolve through the new spine, and a chunk id outside
// the root's span must read as absent rather than as a corrupt directory.
func TestFileStoreChunkDirectoryGrowthPreservesLookups(t *testing.T) {
	collection := newChunkHeightCollection(t)
	const count = 130
	for i := range count {
		if _, err := collection.Put(fmt.Sprintf("key-%06d", i), []byte(fmt.Sprintf(`{"id":%d}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	state := collection.state.Load()
	bounds := storeio.ChunkTreeBounds{
		FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID,
	}
	for _, chunk := range []uint32{0, 63, 64, 129} {
		ref, ok, err := storeio.LookupChunkTree(collection.cache, state.chunkRoot, chunk, bounds)
		if err != nil || !ok || ref == (storeio.PageRef{}) {
			t.Fatalf("chunk %d = (%v,%v,%v), want a live mapping", chunk, ref, ok, err)
		}
	}
	// Far outside the root's covered span: absent, not corrupt.
	for _, chunk := range []uint32{4096, 1 << 20} {
		ref, ok, err := storeio.LookupChunkTree(collection.cache, state.chunkRoot, chunk, bounds)
		if err != nil || ok || ref != (storeio.PageRef{}) {
			t.Fatalf("uncovered chunk %d = (%v,%v,%v), want absent", chunk, ref, ok, err)
		}
	}
	// Deleting every key must unwind the tree back to no root at all.
	for i := range count {
		deleted, err := collection.Delete(fmt.Sprintf("key-%06d", i))
		if err != nil || !deleted {
			t.Fatalf("delete %d = (%v,%v)", i, deleted, err)
		}
	}
	if root := collection.state.Load().chunkRoot; root != (storeio.PageRef{}) {
		t.Fatalf("emptied chunk directory root = %v, want none", root)
	}
}

// TestFileStoreGroupedCommitCoversPublishedFileEnd guards the durability
// invariant that group-commit root suppression can break: a superblock names a
// FileEnd covering every extent its group allocated, and recovery treats a file
// shorter than its own FileEnd as truncated. Eliding the write that reaches
// highest therefore does not save a page, it makes the store unopenable.
func TestFileStoreGroupedCommitCoversPublishedFileEnd(t *testing.T) {
	// Whether the group's highest extent belongs to a suppressible root depends
	// on how the free set has recycled by that point, so the count is swept
	// rather than picked: 40 and 80 are counts observed to land there.
	for _, documents := range []int{40, 80, 120} {
		file, err := os.CreateTemp(t.TempDir(), "grouped-fileend-*")
		if err != nil {
			t.Fatal(err)
		}
		options := testFileStoreOptions()
		options.Durability = DurabilityAsyncVisible
		options.QueueSlots = 8
		options.GroupLimit = 8
		collection, err := Create(file, options)
		if err != nil {
			t.Fatal(err)
		}
		for i := range documents {
			if _, err := collection.Put(fmt.Sprintf("key-%04d", i), []byte(fmt.Sprintf(`{"id":%d}`, i))); err != nil {
				t.Fatal(err)
			}
		}
		if err := collection.Flush(); err != nil {
			t.Fatal(err)
		}
		published := collection.state.Load().super.FileEnd
		if err := collection.Close(); err != nil {
			t.Fatal(err)
		}
		info, err := file.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if uint64(info.Size()) < published {
			t.Fatalf("documents=%d: file is %d bytes, published FileEnd %d",
				documents, info.Size(), published)
		}
		reopened, err := Open(file, options)
		if err != nil {
			t.Fatalf("documents=%d: reopen: %v", documents, err)
		}
		for i := range documents {
			got, ok, readErr := reopened.AppendRaw(nil, fmt.Sprintf("key-%04d", i))
			want := fmt.Sprintf(`{"id":%d}`, i)
			if readErr != nil || !ok || string(got) != want {
				t.Fatalf("reopened key %d = (%q,%v,%v)", i, got, ok, readErr)
			}
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
		_ = file.Close()
	}
}
