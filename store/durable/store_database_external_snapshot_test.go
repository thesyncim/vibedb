package durable

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/store"
)

func TestSnapshotCollectionsRetainsCatalogedEmptyNames(t *testing.T) {
	db := newTestDatabase(t, "materialized")
	collection, _ := db.Collection("materialized")
	mustPut(t, collection, "k", `{"v":1}`)

	snapshot, err := SnapshotCollections([]NamedCollection{
		{Name: "unmaterialized"},
		{Name: "materialized", Collection: collection},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if snapshot.Len() != 2 {
		t.Fatalf("Len = %d, want 2", snapshot.Len())
	}
	empty, ok := snapshot.Collection("unmaterialized")
	if !ok || empty != nil {
		t.Fatalf("empty collection = (%p,%v), want (nil,true)", empty, ok)
	}
	view, ok := snapshot.Collection("materialized")
	if !ok || view == nil || view.Len() != 1 {
		t.Fatalf("materialized collection = (%p,%v), want one row", view, ok)
	}
}

func TestSnapshotCollectionsRejectsAmbiguousCatalog(t *testing.T) {
	db := newTestDatabase(t, "docs")
	collection, _ := db.Collection("docs")
	if _, err := SnapshotCollections([]NamedCollection{
		{Name: "docs", Collection: collection},
		{Name: "docs", Collection: collection},
	}); !errors.Is(err, ErrCollectionExists) {
		t.Fatalf("duplicate-name error = %v, want ErrCollectionExists", err)
	}
	if _, err := SnapshotCollections([]NamedCollection{
		{Name: "a", Collection: collection},
		{Name: "b", Collection: collection},
	}); err == nil {
		t.Fatal("one collection published under two names")
	}
}

func TestSnapshotCollectionsUsesHandleOrderAcrossCatalogNames(t *testing.T) {
	db := newTestDatabase(t, "a", "b")
	a, _ := db.Collection("a")
	b, _ := db.Collection("b")

	left := []*Collection{a, b}
	right := []*Collection{b, a}
	sortCollectionSnapshotOrder(left)
	sortCollectionSnapshotOrder(right)
	if !slices.Equal(left, right) {
		t.Fatalf("conflicting catalog names produced different gate orders")
	}
}

func TestSnapshotCollectionsHoldsEveryPublicationGate(t *testing.T) {
	db := newTestDatabase(t, "a", "b")
	a, _ := db.Collection("a")
	b, _ := db.Collection("b")
	mustPut(t, a, "k", `{"v":1}`)
	mustPut(t, b, "k", `{"v":1}`)

	b.snapshotGate.Lock()
	done := make(chan error, 1)
	go func() {
		snapshot, err := SnapshotCollections([]NamedCollection{
			{Name: "b", Collection: b},
			{Name: "a", Collection: a},
		})
		if closeErr := snapshot.Close(); err == nil {
			err = closeErr
		}
		done <- err
	}()

	held := false
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		if a.snapshotGate.TryLock() {
			a.snapshotGate.Unlock()
			time.Sleep(200 * time.Microsecond)
			continue
		}
		held = true
		break
	}
	b.snapshotGate.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !held {
		t.Fatal("capture reached b without retaining a's publication gate")
	}
}

func TestSnapshotCollectionsSealsDeferredPrimaryBeforeCapture(t *testing.T) {
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Append("k", []byte(`{"v":0}`)); err != nil {
		t.Fatal(err)
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "external-primary.vjc")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability:         DurabilityBufferedVisible,
		CheckpointStrength: CheckpointFilesystem,
	}
	if _, err := CreateFromPrimary(built, file, options); err != nil {
		t.Fatal(err)
	}
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	if _, err := collection.Put([]byte("k"), []byte(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}
	if len(collection.primaryPendingParents) == 0 {
		t.Fatal("test did not establish a deferred primary parent")
	}
	catalog, err := SnapshotCollections([]NamedCollection{{
		Name: "docs", Collection: collection,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()
	if len(collection.primaryPendingParents) != 0 {
		t.Fatal("external snapshot left deferred primary parents unsealed")
	}
	snapshot, ok := catalog.Collection("docs")
	if !ok || snapshot == nil || snapshot.Len() != 1 {
		t.Fatalf("captured collection = (%p, %t), want one-row snapshot", snapshot, ok)
	}
	point, found, err := snapshot.AppendRaw(nil, []byte("k"))
	if err != nil || !found {
		t.Fatalf("point read = (%q, %t, %v)", point, found, err)
	}
	var scanned []byte
	if _, err := snapshot.RangeRawBuffer(nil, func(key, value []byte) error {
		if string(key) == "k" {
			scanned = append(scanned[:0], value...)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if string(point) != `{"v":1}` || string(scanned) != string(point) {
		t.Fatalf("captured point/scan = (%s, %s), want v1 agreement", point, scanned)
	}
	if _, err := collection.Put([]byte("k"), []byte(`{"v":2}`)); err != nil {
		t.Fatal(err)
	}
	point, found, err = snapshot.AppendRaw(point[:0], []byte("k"))
	if err != nil || !found || string(point) != `{"v":1}` {
		t.Fatalf("old captured point after v2 = (%s, %t, %v)", point, found, err)
	}
}
