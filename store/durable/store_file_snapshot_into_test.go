package durable

import (
	"errors"
	"os"
	"testing"
)

func TestFileSnapshotIntoWarmedLoopDoesNotAllocate(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-fs-snapshot-into-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, testFileStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	if _, err := collection.Put([]byte("key"), []byte(`{"value":1}`)); err != nil {
		t.Fatal(err)
	}

	var snapshot Snapshot
	if err := collection.SnapshotInto(&snapshot); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	allocations := testing.AllocsPerRun(1_000, func() {
		if runErr := collection.SnapshotInto(&snapshot); runErr != nil {
			panic(runErr)
		}
		if snapshot.Len() != 1 {
			panic("snapshot lost its visible generation")
		}
		if runErr := snapshot.Close(); runErr != nil {
			panic(runErr)
		}
	})
	if allocations != 0 {
		t.Fatalf("warmed SnapshotInto/Close allocated %.2f times, want 0", allocations)
	}
}

func TestFileSnapshotIntoFailureReleasesPreviousBinding(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-fs-snapshot-rebind-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, testFileStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	var snapshot Snapshot
	if err := collection.SnapshotInto(&snapshot); err != nil {
		t.Fatal(err)
	}
	if active := collection.leases.Stats(collection.Generation()).Active; active != 1 {
		t.Fatalf("active leases before failed rebind = %d, want 1", active)
	}
	var unavailable *Collection
	if err := unavailable.SnapshotInto(&snapshot); !errors.Is(err, ErrClosed) {
		t.Fatalf("failed rebind error = %v, want %v", err, ErrClosed)
	}
	if active := collection.leases.Stats(collection.Generation()).Active; active != 0 {
		t.Fatalf("active leases after failed rebind = %d, want 0", active)
	}
	if snapshot.Len() != 0 {
		t.Fatalf("failed rebind left snapshot readable with length %d", snapshot.Len())
	}
	if err := snapshot.Close(); err != nil {
		t.Fatalf("Close after failed rebind: %v", err)
	}
}
