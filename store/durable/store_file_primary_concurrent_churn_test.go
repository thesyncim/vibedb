package durable

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestConcurrentPrimaryDeleteRestorePhysicalFoldReopens(t *testing.T) {
	options := concurrentPrimaryTestOptions()
	fixture := openConcurrentPrimaryTestFixture(t, 512, options)
	coll := fixture.collection
	at := len(fixture.keys) / 2
	key := []byte(fixture.keys[at])
	want := canonicalConcurrentPrimaryValue(t, fixture.values[at])
	baseGeneration := coll.Generation()
	baseCount := coll.Len()

	deleted, err := coll.Delete(key)
	if err != nil || !deleted {
		t.Fatalf("Delete = %v,%v", deleted, err)
	}
	deleteGeneration := baseGeneration + 1
	if err := flushPhysicalForTest(coll); err != nil {
		t.Fatal(err)
	}
	if got := coll.committer.DurableGeneration(); got != deleteGeneration {
		t.Fatalf("physical delete generation = %d, want %d", got, deleteGeneration)
	}
	if coll.primaryUnifiedOverlay.hasPending() ||
		coll.primaryUnifiedOverlay.count.Load() != 0 {
		t.Fatal("physical delete fold retained overlay tombstone")
	}
	if got, found, readErr := coll.AppendRaw(nil, key); readErr != nil || found {
		t.Fatalf("physically deleted read = %s,%v,%v", got, found, readErr)
	}
	var staged int
	previous := concurrentPrimaryReplaceStagedHook
	concurrentPrimaryReplaceStagedHook = func(storeio.BucketID) { staged++ }
	t.Cleanup(func() { concurrentPrimaryReplaceStagedHook = previous })
	created, err := coll.Put(key, fixture.values[at])
	if err != nil || !created {
		t.Fatalf("post-fold restore Put = %v,%v", created, err)
	}
	if staged != 1 {
		t.Fatalf("post-fold restore concurrent stages = %d, want 1", staged)
	}
	target := baseGeneration + 2
	if got := coll.Generation(); got != target {
		t.Fatalf("visible generation = %d, want %d", got, target)
	}
	if err := flushPhysicalForTest(coll); err != nil {
		t.Fatal(err)
	}
	if got := coll.committer.DurableGeneration(); got != target {
		t.Fatalf("physical generation = %d, want %d", got, target)
	}
	if coll.primaryUnifiedOverlay.hasPending() ||
		coll.primaryUnifiedOverlay.count.Load() != 0 {
		t.Fatal("physical fold retained delete/restore overlay records")
	}
	if err := coll.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(fixture.file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := reopened.Generation(); got != target {
		t.Fatalf("reopened generation = %d, want %d", got, target)
	}
	if got := reopened.Len(); got != baseCount {
		t.Fatalf("reopened count = %d, want %d", got, baseCount)
	}
	assertConcurrentPrimaryRaw(t, reopened, key, want)
}
