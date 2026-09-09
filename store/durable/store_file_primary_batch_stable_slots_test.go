package durable

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
)

func stableBatchOptions(maxBatch int, unique bool) Options {
	return Options{
		Backend: BackendPortable, ResidentBytes: 512 << 20,
		Durability:        DurabilityBufferedVisible,
		MaxBatchDocuments: maxBatch,
		MaxBatchBytes:     64 << 20,
		MaxRetiredExtents: 1 << 17,
		Indexes: []store.IndexDefinition{
			{Name: "shared_a", Paths: []string{"/shared", "/a"}},
			{Name: "unique_id", Paths: []string{"/uid"}, Unique: unique},
		},
	}
}

func stableBatchDocument(row int, shared, a, note string) []byte {
	raw := fmt.Appendf(nil,
		`{"uid":"u%04d","shared":"%s","a":"%s","note":"%s"}`,
		row, shared, a, note,
	)
	canonical, err := vibejson.AppendCanonicalize(nil, raw)
	if err != nil {
		panic(err)
	}
	return canonical
}

func stableBatchFixture(t *testing.T, rows int, options Options) *Collection {
	t.Helper()
	docs := make(map[string][]byte, rows)
	for row := range rows {
		docs[fmt.Sprintf("k%04d", row)] = stableBatchDocument(
			row, fmt.Sprintf("s%03d", row%37), fmt.Sprintf("a%03d", row%53), "old",
		)
	}
	return buildIndexedPrimaryFile(t, t.TempDir(), "stable-batch-*", docs, options)
}

func stableBatchSlot(t *testing.T, collection *Collection, key string) uint8 {
	t.Helper()
	router := collection.primaryRouter.Load()
	route, ok := router.Route([]byte(key))
	if !ok {
		t.Fatalf("route %s", key)
	}
	lease, err := router.AcquireLeaf(collection.cache, route)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	stripe, ok := storeio.AdmittedCompactPrimaryStripe(
		lease.Page(), collection.storeID, route.Bucket,
	)
	if !ok {
		t.Fatalf("admit %s", key)
	}
	rank, found := stripe.FindKey([]byte(key))
	slot, slotOK := stripe.PostingSlot(rank)
	if !found || !slotOK {
		t.Fatalf("slot %s found=%v slot=%v", key, found, slotOK)
	}
	return slot
}

func stableBatchAssertExactUpdates(t *testing.T, collection *Collection) {
	t.Helper()
	check := func(shared, a string, want []string) {
		t.Helper()
		got := primaryExactTestKeys(t, collection, "shared_a",
			primaryExactTestNeedle(t, fmt.Sprintf("%q", shared)),
			primaryExactTestNeedle(t, fmt.Sprintf("%q", a)))
		if !slices.Equal(got, want) {
			t.Fatalf("exact (%q,%q)=%v want %v", shared, a, got, want)
		}
	}
	check("shared-changed", "a017", []string{"k0017"})
	check("s017", "a017", nil)
	check("s027", "indexed-a-changed", []string{"k0101"})
	check("s027", "a048", nil)
	check("s000", "a015", []string{"k0333"})
}

func TestPrimaryBatchAllExistingPreservesSlotsAndChangedExact(t *testing.T) {
	options := stableBatchOptions(64, false)
	collection := stableBatchFixture(t, 512, options)
	keys := []string{"k0017", "k0101", "k0333"}
	beforeSlots := make([]uint8, len(keys))
	for i := range keys {
		beforeSlots[i] = stableBatchSlot(t, collection, keys[i])
	}
	before, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	want := [][]byte{
		stableBatchDocument(17, "shared-changed", "a017", "old"),
		stableBatchDocument(101, "s027", "indexed-a-changed", "old"),
		stableBatchDocument(333, "s000", "a015", "unrelated-changed"),
	}
	if err := collection.Update(func(batch *WriteBatch) error {
		for i := range keys {
			if err := batch.Put([]byte(keys[i]), want[i]); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if collection.primaryEpoch.overlayEmpty() {
		t.Fatal("changed exact terms did not publish overlay deltas")
	}
	for i := range keys {
		if got := stableBatchSlot(t, collection, keys[i]); got != beforeSlots[i] {
			t.Fatalf("%s slot=%d want stable %d", keys[i], got, beforeSlots[i])
		}
		got, found, err := collection.AppendRaw(nil, []byte(keys[i]))
		if err != nil || !found || !bytes.Equal(got, want[i]) {
			t.Fatalf("%s current found=%v err=%v", keys[i], found, err)
		}
	}
	stableBatchAssertExactUpdates(t, collection)
	old, found, err := before.AppendRaw(nil, []byte(keys[0]))
	if err != nil || !found || bytes.Equal(old, want[0]) {
		t.Fatalf("old snapshot changed found=%v err=%v", found, err)
	}
	if err := before.Close(); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if !collection.primaryEpoch.overlayEmpty() {
		t.Fatal("checkpoint left overlay records")
	}
	path := collection.file.Name()
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	defer reopened.Close()
	for i := range keys {
		got, found, err := reopened.AppendRaw(nil, []byte(keys[i]))
		if err != nil || !found || !bytes.Equal(got, want[i]) {
			t.Fatalf("reopen %s found=%v err=%v", keys[i], found, err)
		}
	}
	stableBatchAssertExactUpdates(t, reopened)
}

func TestPrimaryBatchAllExistingUnchangedExactUsesStableOverlay(t *testing.T) {
	collection := stableBatchFixture(t, 512, stableBatchOptions(64, false))
	key := "k0333"
	beforeSlot := stableBatchSlot(t, collection, key)
	want := stableBatchDocument(333, "s000", "a015", "unrelated-changed")
	if err := collection.Update(func(batch *WriteBatch) error {
		return batch.Put([]byte(key), want)
	}); err != nil {
		t.Fatal(err)
	}
	if !collection.primaryEpoch.overlayEmpty() {
		t.Fatal("unchanged exact terms emitted exact overlay records")
	}
	if got := stableBatchSlot(t, collection, key); got != beforeSlot {
		t.Fatalf("slot=%d want stable %d", got, beforeSlot)
	}
	got, found, err := collection.AppendRaw(nil, []byte(key))
	if err != nil || !found || !bytes.Equal(got, want) {
		t.Fatalf("current found=%v err=%v", found, err)
	}
	if gotKeys := primaryExactTestKeys(t, collection, "shared_a",
		primaryExactTestNeedle(t, `"s000"`), primaryExactTestNeedle(t, `"a015"`)); !slices.Equal(gotKeys, []string{key}) {
		t.Fatalf("unchanged exact result=%v", gotKeys)
	}
}

func TestPrimaryBatchStableSlotsPreservesUniqueValidation(t *testing.T) {
	collection := stableBatchFixture(t, 64, stableBatchOptions(64, true))
	if err := collection.Update(func(batch *WriteBatch) error {
		if err := batch.Put([]byte("k0001"), []byte(`{"uid":"u0002","shared":"s001","a":"a001","note":"swap"}`)); err != nil {
			return err
		}
		return batch.Put([]byte("k0002"), []byte(`{"uid":"u0001","shared":"s002","a":"a002","note":"swap"}`))
	}); err != nil {
		t.Fatalf("unique swap: %v", err)
	}
	before, found, err := collection.AppendRaw(nil, []byte("k0003"))
	if err != nil || !found {
		t.Fatal(err)
	}
	err = collection.Update(func(batch *WriteBatch) error {
		if err := batch.Put([]byte("k0003"), []byte(`{"uid":"u0004","shared":"s003","a":"a003","note":"conflict"}`)); err != nil {
			return err
		}
		return batch.Put([]byte("k0005"), []byte(`{"uid":"u0004","shared":"s005","a":"a005","note":"conflict"}`))
	})
	if !errors.Is(err, store.ErrUniqueIndexViolation) {
		t.Fatalf("duplicate batch err=%v", err)
	}
	after, found, readErr := collection.AppendRaw(nil, []byte("k0003"))
	if readErr != nil || !found || !bytes.Equal(after, before) {
		t.Fatalf("duplicate rollback found=%v err=%v", found, readErr)
	}
}

func TestPrimaryBatchStablePressureFallsBackFromEmptyEpoch(t *testing.T) {
	const rows = 512
	options := stableBatchOptions(64, false)
	collection := stableBatchFixture(t, rows, options)
	oldEpoch := collection.primaryEpoch
	if !oldEpoch.overlayEmpty() {
		t.Fatal("fixture exact overlay is not empty")
	}
	// Leave fewer unpublished term-record slots than this all-existing batch
	// needs. The installed view is still empty and valid; this deterministic
	// seam exercises the bounded structural fallback from an empty epoch.
	oldEpoch.termRecordN = len(oldEpoch.termRecords) - 4
	keys := []int{17, 101, 333}
	if err := collection.Update(func(batch *WriteBatch) error {
		for _, row := range keys {
			if err := batch.Put(
				[]byte(fmt.Sprintf("k%04d", row)),
				stableBatchDocument(row, fmt.Sprintf("n%04d", row), fmt.Sprintf("z%04d", row), "pressure"),
			); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("batch larger than empty exact overlay: %v", err)
	}
	if collection.primaryEpoch == oldEpoch {
		t.Fatal("empty-epoch pressure did not install structural fallback")
	}
	if got := primaryExactTestKeys(t, collection, "shared_a",
		primaryExactTestNeedle(t, `"n0333"`), primaryExactTestNeedle(t, `"z0333"`)); !slices.Equal(got, []string{"k0333"}) {
		t.Fatalf("large fallback exact result=%v", got)
	}
}

func TestPrimaryBatchMixedTopologyKeepsStructuralFallback(t *testing.T) {
	collection := stableBatchFixture(t, 128, stableBatchOptions(64, false))
	if err := collection.Update(func(batch *WriteBatch) error {
		if err := batch.Put([]byte("k0001"), stableBatchDocument(1, "changed", "a001", "mixed")); err != nil {
			return err
		}
		if err := batch.Delete([]byte("k0002")); err != nil {
			return err
		}
		return batch.Put([]byte("new"), stableBatchDocument(9999, "new", "new", "mixed"))
	}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := collection.AppendRaw(nil, []byte("k0002")); err != nil || found {
		t.Fatalf("deleted key found=%v err=%v", found, err)
	}
	if _, found, err := collection.AppendRaw(nil, []byte("new")); err != nil || !found {
		t.Fatalf("inserted key found=%v err=%v", found, err)
	}
}
