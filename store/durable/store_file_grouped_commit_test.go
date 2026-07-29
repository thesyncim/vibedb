package durable

import (
	"fmt"
	"os"
	"testing"
)

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
			if _, err := collection.Put([]byte(fmt.Sprintf("key-%04d", i)), []byte(fmt.Sprintf(`{"id":%d}`, i))); err != nil {
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
			got, ok, readErr := reopened.AppendRaw(nil, []byte(fmt.Sprintf("key-%04d", i)))
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
