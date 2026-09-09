package durable

import (
	"bytes"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// TestFilePrimarySharedRouterSnapshotAfterSplitAndUntouchedLeafUpdate covers
// the subtle lifetime of a bounded persistent router. A structural split
// publishes a new router image, but unchanged routes share their coherent
// mutable handle cells with the retained image. A later COW of one such route
// may therefore make the old image observe a PageRef newer than its captured
// state even though the old router's own generation remains unchanged. Every
// snapshot read path must reject that future handle and resolve from its root.
func TestFilePrimarySharedRouterSnapshotAfterSplitAndUntouchedLeafUpdate(t *testing.T) {
	const rows = storeio.CompactPrimaryStripeMaxRows + 64
	built, keys, values := buildFilePrimaryCorpus(t, rows)
	options := journalTestOptions(CheckpointPowerSafe)
	options.RecoveryJournal = false
	options.Indexes = []store.IndexDefinition{{Name: "group", Paths: []string{"/group"}}}
	file := createPrimaryPointFile(t, built, options, "shared-router-snapshot.vibe")
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	oldSnapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer oldSnapshot.Close()
	oldRouter := oldSnapshot.primaryRouter
	if oldRouter == nil || oldRouter.Len() < 2 {
		t.Fatalf("initial router = %v leaves, want at least two", oldRouter)
	}
	oldGeneration := oldSnapshot.Generation()
	untouchedKey := []byte(keys[len(keys)-1])
	untouchedRoute, ok := oldRouter.Route(untouchedKey)
	if !ok {
		t.Fatalf("route untouched key %q", untouchedKey)
	}

	// The first bulk-built compact leaf is full. This key sorts inside it and
	// forces a local structural split, leaving the final leaf untouched.
	insertedKey := []byte("primary-key-000000000!")
	insertedValue := []byte(`{"id":9001,"group":9001,"name":"split row"}`)
	insertRoute, ok := oldRouter.Route(insertedKey)
	if !ok || insertRoute.Bucket == untouchedRoute.Bucket {
		t.Fatalf("fixture routes split=%+v untouched=%+v", insertRoute, untouchedRoute)
	}
	if created, putErr := collection.Put(insertedKey, insertedValue); putErr != nil || !created {
		t.Fatalf("split insert = created %v, err %v", created, putErr)
	}
	newRouter := collection.primaryRouter.Load()
	if newRouter == oldRouter || newRouter.Len() <= oldRouter.Len() {
		t.Fatalf("insert did not structurally replace router: old=%p/%d new=%p/%d",
			oldRouter, oldRouter.Len(), newRouter, newRouter.Len())
	}
	if got := oldRouter.Generation(); got != oldGeneration {
		t.Fatalf("retained router generation after split = %d, want %d", got, oldGeneration)
	}

	updatedValue := []byte(`{"id":99001,"group":99001,"name":"future row"}`)
	if created, putErr := collection.Put(untouchedKey, updatedValue); putErr != nil || created {
		t.Fatalf("untouched-leaf update = created %v, err %v", created, putErr)
	}
	sharedRoute, ok := oldRouter.Route(untouchedKey)
	if !ok {
		t.Fatalf("retained router lost untouched key %q", untouchedKey)
	}
	if sharedRoute.Ref.Generation <= oldGeneration {
		t.Fatalf("fixture did not expose shared future handle: route=%+v snapshot generation=%d",
			sharedRoute.Ref, oldGeneration)
	}
	if got := oldRouter.Generation(); got != oldGeneration {
		t.Fatalf("retained router generation after shared update = %d, want %d", got, oldGeneration)
	}

	raw, found, err := oldSnapshot.AppendRaw(nil, untouchedKey)
	if err != nil || !found || !bytes.Equal(raw, values[len(values)-1]) {
		t.Fatalf("old raw %q = %q,%v,%v, want %q",
			untouchedKey, raw, found, err, values[len(values)-1])
	}
	if found, err := oldSnapshot.ContainsKey(untouchedKey); err != nil || !found {
		t.Fatalf("old ContainsKey(existing) = %v,%v", found, err)
	}
	if found, err := oldSnapshot.ContainsKey(insertedKey); err != nil || found {
		t.Fatalf("old ContainsKey(post-split insert) = %v,%v", found, err)
	}

	probe, err := NewFieldProbe("/id")
	if err != nil {
		t.Fatal(err)
	}
	field, found, err := oldSnapshot.AppendField(nil, probe, untouchedKey)
	wantField := []byte("4159")
	if err != nil || !found || !bytes.Equal(field, wantField) {
		t.Fatalf("old projected field = %q,%v,%v, want %q", field, found, err, wantField)
	}

	oldNeedle := primaryExactTestNeedle(t, "171")
	gotKeys := primaryExactSnapshotKeys(t, oldSnapshot, "group", oldNeedle)
	wantKeys := make([]string, 0, 5)
	for row, key := range keys {
		if row%997 == 4159%997 {
			wantKeys = append(wantKeys, key)
		}
	}
	slices.Sort(wantKeys)
	if !slices.Equal(gotKeys, wantKeys) {
		t.Fatalf("old exact grouped scan = %v, want %v", gotKeys, wantKeys)
	}
	newNeedle := primaryExactTestNeedle(t, "99001")
	if got := primaryExactSnapshotKeys(t, oldSnapshot, "group", newNeedle); len(got) != 0 {
		t.Fatalf("old exact read exposed future value: %v", got)
	}
}
