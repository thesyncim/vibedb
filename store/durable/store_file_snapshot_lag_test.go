package durable

import (
	"errors"
	"fmt"
	"os"
	"testing"
)

func TestFileStoreMaxSnapshotGenerationLagGuardrail(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "snapshot-lag-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	options := testFileStoreOptions()
	options.Durability = DurabilitySync
	options.MaxSnapshotGenerationLag = 3
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	if _, err := collection.Put([]byte("k0"), []byte(`{"v":0}`)); err != nil {
		t.Fatal(err)
	}
	pinned, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer pinned.Close()

	for i := 1; i <= 5; i++ {
		if _, err := collection.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf(`{"v":%d}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	if !collection.SnapshotLagExceeded() {
		t.Fatalf("expected SnapshotLagExceeded()=true, stats=%+v", collection.Stats())
	}
	if _, err := collection.Snapshot(); !errors.Is(err, ErrSnapshotLagExceeded) {
		t.Fatalf("Snapshot() err = %v, want ErrSnapshotLagExceeded", err)
	}
	if err := pinned.Close(); err != nil {
		t.Fatal(err)
	}
	if collection.SnapshotLagExceeded() {
		t.Fatal("expected SnapshotLagExceeded()=false after closing pinned snapshot")
	}
	fresh, err := collection.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() after releasing pinned snapshot: %v", err)
	}
	_ = fresh.Close()
}
