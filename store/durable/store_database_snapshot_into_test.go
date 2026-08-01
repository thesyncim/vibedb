package durable

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

type cancelAfterSnapshotLeaseContext struct {
	collection *Collection
}

func (*cancelAfterSnapshotLeaseContext) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (*cancelAfterSnapshotLeaseContext) Done() <-chan struct{} { return nil }

func (c *cancelAfterSnapshotLeaseContext) Err() error {
	if c.collection.leases.Stats(c.collection.Generation()).Active != 0 {
		return context.Canceled
	}
	return nil
}

func (*cancelAfterSnapshotLeaseContext) Value(any) any { return nil }

func TestSnapshotCollectionsIntoMatchesSnapshotCollections(t *testing.T) {
	db := newTestDatabase(t, "a", "b", "c")
	a, _ := db.Collection("a")
	b, _ := db.Collection("b")
	c, _ := db.Collection("c")
	mustPut(t, a, "a1", `{"v":"a"}`)
	mustPut(t, b, "b1", `{"v":"b"}`)
	mustPut(t, c, "c1", `{"v":"c"}`)

	for _, test := range []struct {
		name        string
		collections []NamedCollection
	}{
		{name: "empty"},
		{name: "one", collections: []NamedCollection{{Name: "b", Collection: b}}},
		{name: "many", collections: []NamedCollection{
			{Name: "empty"},
			{Name: "c", Collection: c},
			{Name: "a", Collection: a},
			{Name: "b", Collection: b},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			want, err := SnapshotCollections(test.collections)
			if err != nil {
				t.Fatal(err)
			}
			defer want.Close()

			var got DatabaseSnapshot
			if err := SnapshotCollectionsInto(&got, test.collections); err != nil {
				t.Fatal(err)
			}
			defer got.Close()

			wantNames := want.AppendNames(nil)
			gotNames := got.AppendNames(nil)
			if !slices.Equal(gotNames, wantNames) {
				t.Fatalf("names = %v, want %v", gotNames, wantNames)
			}
			for _, name := range wantNames {
				wantView, wantOK := want.Collection(name)
				gotView, gotOK := got.Collection(name)
				if gotOK != wantOK || (gotView == nil) != (wantView == nil) {
					t.Fatalf("Collection(%q) = (%p,%v), want (%p,%v)",
						name, gotView, gotOK, wantView, wantOK)
				}
				if gotView != nil && (gotView.Generation() != wantView.Generation() ||
					gotView.Len() != wantView.Len()) {
					t.Fatalf("Collection(%q) = generation %d len %d, want %d/%d",
						name, gotView.Generation(), gotView.Len(),
						wantView.Generation(), wantView.Len())
				}
			}

			// The returned catalog owns its names, just as SnapshotCollections does.
			if len(test.collections) != 0 {
				original := got.AppendNames(nil)
				test.collections[0].Name = "reused-input"
				if after := got.AppendNames(nil); !slices.Equal(after, original) {
					t.Fatalf("caller input mutation changed captured names: %v -> %v",
						original, after)
				}
			}
		})
	}
}

func TestSnapshotCollectionsIntoClonesCallerBackedNames(t *testing.T) {
	db := newTestDatabase(t, "docs")
	docs, _ := db.Collection("docs")
	bytes := []byte("docs")
	borrowed := unsafe.String(unsafe.SliceData(bytes), len(bytes))
	catalog := []NamedCollection{{Name: borrowed, Collection: docs}}

	var dst DatabaseSnapshot
	if err := SnapshotCollectionsInto(&dst, catalog); err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	bytes[0] = 'x'
	if names := dst.AppendNames(nil); len(names) != 1 || names[0] != "docs" {
		t.Fatalf("caller-backed name mutation changed capture: %v", names)
	}
	if _, ok := dst.Collection("docs"); !ok {
		t.Fatal("owned name no longer resolves after caller mutation")
	}
	if _, ok := dst.Collection("xocs"); ok {
		t.Fatal("capture retained caller-backed name storage")
	}
}

func TestSnapshotAndSnapshotIntoRemainDifferentiallyIdentical(t *testing.T) {
	db := newTestDatabase(t, "a", "b")
	a, _ := db.Collection("a")
	b, _ := db.Collection("b")
	mustPut(t, a, "a1", `{"v":1}`)
	mustPut(t, b, "b1", `{"v":2}`)

	allocated, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer allocated.Close()
	var reused DatabaseSnapshot
	if err := db.SnapshotInto(&reused); err != nil {
		t.Fatal(err)
	}
	defer reused.Close()
	if got, want := reused.AppendNames(nil), allocated.AppendNames(nil); !slices.Equal(got, want) {
		t.Fatalf("Database SnapshotInto names = %v, Snapshot names = %v", got, want)
	}
	for _, name := range allocated.AppendNames(nil) {
		left, _ := allocated.Collection(name)
		right, _ := reused.Collection(name)
		if left.Generation() != right.Generation() || left.Len() != right.Len() {
			t.Fatalf("database snapshot %q differs: %d/%d vs %d/%d",
				name, left.Generation(), left.Len(), right.Generation(), right.Len())
		}
	}

	plain, err := a.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	var into Snapshot
	if err := a.SnapshotInto(&into); err != nil {
		t.Fatal(err)
	}
	defer into.Close()
	if plain.Generation() != into.Generation() || plain.Len() != into.Len() {
		t.Fatalf("collection SnapshotInto = %d/%d, Snapshot = %d/%d",
			into.Generation(), into.Len(), plain.Generation(), plain.Len())
	}
}

func TestSnapshotCollectionsIntoErrorsReleaseOldAndPartialLeases(t *testing.T) {
	options := testDatabaseOptions()
	options.MaxSnapshotLeases = 1
	db, err := OpenDatabase(t.TempDir(), DatabaseOptions{Options: options})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a, err := db.CreateCollection("a", options)
	if err != nil {
		t.Fatal(err)
	}
	b, err := db.CreateCollection("b", options)
	if err != nil {
		t.Fatal(err)
	}

	var dst DatabaseSnapshot
	if err := SnapshotCollectionsInto(&dst, []NamedCollection{
		{Name: "a", Collection: a},
	}); err != nil {
		t.Fatal(err)
	}
	if active := a.leases.Stats(a.Generation()).Active; active != 1 {
		t.Fatalf("old capture active leases = %d, want 1", active)
	}
	if err := SnapshotCollectionsInto(&dst, []NamedCollection{
		{Name: "duplicate", Collection: a},
		{Name: "duplicate", Collection: a},
	}); !errors.Is(err, ErrCollectionExists) {
		t.Fatalf("duplicate name error = %v, want %v", err, ErrCollectionExists)
	}
	if dst.Len() != 0 {
		t.Fatalf("failed capture published %d entries", dst.Len())
	}
	if active := a.leases.Stats(a.Generation()).Active; active != 0 {
		t.Fatalf("old lease after failed rebind = %d, want 0", active)
	}

	if err := SnapshotCollectionsInto(&dst, []NamedCollection{
		{Name: "left", Collection: a},
		{Name: "right", Collection: a},
	}); err == nil {
		t.Fatal("one handle under two names was accepted")
	}
	if dst.Len() != 0 {
		t.Fatalf("ambiguous-handle capture published %d entries", dst.Len())
	}

	blocker, err := b.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	if err := SnapshotCollectionsInto(&dst, []NamedCollection{
		{Name: "a", Collection: a},
		{Name: "b", Collection: b},
	}); !errors.Is(err, storeio.ErrLeaseCapacity) {
		t.Fatalf("partial capture error = %v, want %v", err, storeio.ErrLeaseCapacity)
	}
	if dst.Len() != 0 {
		t.Fatalf("partial lease failure published %d entries", dst.Len())
	}
	if active := a.leases.Stats(a.Generation()).Active; active != 0 {
		t.Fatalf("partial capture leaked %d leases on first collection", active)
	}
	if active := b.leases.Stats(b.Generation()).Active; active != 1 {
		t.Fatalf("partial capture disturbed blocker: active=%d, want 1", active)
	}
	if err := blocker.Close(); err != nil {
		t.Fatal(err)
	}
	if err := SnapshotCollectionsInto(&dst, []NamedCollection{
		{Name: "a", Collection: a},
		{Name: "b", Collection: b},
	}); err != nil {
		t.Fatalf("capture after lease-acquire failure: %v", err)
	}
	if dst.Len() != 2 {
		t.Fatalf("recovered capture Len = %d, want 2", dst.Len())
	}
	if err := dst.Close(); err != nil {
		t.Fatal(err)
	}
	if err := dst.Close(); err != nil {
		t.Fatalf("idempotent Close: %v", err)
	}
}

func TestSnapshotCollectionsIntoCancellationPublishesNothing(t *testing.T) {
	db := newTestDatabase(t, "a", "b")
	a, _ := db.Collection("a")
	b, _ := db.Collection("b")
	collections := []NamedCollection{
		{Name: "a", Collection: a},
		{Name: "b", Collection: b},
	}

	var dst DatabaseSnapshot
	if err := SnapshotCollectionsInto(&dst, collections); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := SnapshotCollectionsIntoContext(ctx, &dst, collections); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled capture = %v, want %v", err, context.Canceled)
	}
	if dst.Len() != 0 {
		t.Fatalf("pre-canceled capture retained %d old entries", dst.Len())
	}
	if active := a.leases.Stats(a.Generation()).Active; active != 0 {
		t.Fatalf("pre-canceled rebind retained %d old leases", active)
	}

	order := []*Collection{a, b}
	sortCollectionSnapshotOrder(order)
	first, blocked := order[0], order[1]
	blocked.writer.Lock()
	ctx, cancel = context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- SnapshotCollectionsIntoContext(ctx, &dst, collections)
	}()

	held := false
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		if first.writer.TryLock() {
			first.writer.Unlock()
			time.Sleep(100 * time.Microsecond)
			continue
		}
		held = true
		break
	}
	if !held {
		blocked.writer.Unlock()
		cancel()
		t.Fatal("capture did not reach the blocked second writer")
	}
	cancel()
	blocked.writer.Unlock()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked cancellation = %v, want %v", err, context.Canceled)
	}
	if dst.Len() != 0 {
		t.Fatalf("blocked canceled capture published %d entries", dst.Len())
	}
	if !first.writer.TryLock() {
		t.Fatal("canceled capture retained the first writer lock")
	}
	first.writer.Unlock()

	partialContext := &cancelAfterSnapshotLeaseContext{collection: a}
	if err := SnapshotCollectionsIntoContext(partialContext, &dst, collections); !errors.Is(err, context.Canceled) {
		t.Fatalf("partial-lease cancellation = %v, want %v",
			err, context.Canceled)
	}
	if dst.Len() != 0 {
		t.Fatalf("partial-lease cancellation published %d entries", dst.Len())
	}
	if active := a.leases.Stats(a.Generation()).Active; active != 0 {
		t.Fatalf("partial-lease cancellation leaked %d leases", active)
	}
	if err := SnapshotCollectionsInto(&dst, collections); err != nil {
		t.Fatalf("capture after cancellation failure: %v", err)
	}
	if err := dst.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotCollectionsIntoPinsDroppedAndRecreatedIncarnations(t *testing.T) {
	db := newTestDatabase(t, "docs")
	old, _ := db.Collection("docs")
	mustPut(t, old, "old", `{"incarnation":1}`)

	var dst DatabaseSnapshot
	if err := SnapshotCollectionsInto(&dst, []NamedCollection{{
		Name: "docs", Collection: old,
	}}); err != nil {
		t.Fatal(err)
	}
	dropDone := make(chan error, 1)
	go func() { dropDone <- db.DropCollection("docs") }()
	for {
		select {
		case err := <-dropDone:
			if !errors.Is(err, storeio.ErrLeasesActive) {
				t.Fatalf("drop with captured incarnation = %v, want active leases", err)
			}
			goto dropped
		default:
			if _, found, err := oldViewRaw(dst, "docs", "old"); err != nil || !found {
				t.Fatalf("read racing drop = found %v, err %v", found, err)
			}
			runtime.Gosched()
		}
	}

dropped:
	oldView, ok := dst.Collection("docs")
	if !ok || oldView == nil {
		t.Fatal("old incarnation disappeared from captured catalog")
	}
	raw, found, err := oldView.AppendRaw(nil, []byte("old"))
	if err != nil || !found || string(raw) != `{"incarnation":1}` {
		t.Fatalf("old incarnation read = %s,%v,%v", raw, found, err)
	}
	if err := dst.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.DropCollection("docs"); err != nil {
		t.Fatalf("drop after release: %v", err)
	}
	if err := SnapshotCollectionsInto(&dst, []NamedCollection{{
		Name: "docs", Collection: old,
	}}); !errors.Is(err, ErrClosed) {
		t.Fatalf("capture of dropped handle = %v, want %v", err, ErrClosed)
	}
	if dst.Len() != 0 {
		t.Fatalf("capture of dropped handle published %d entries", dst.Len())
	}
	if err := SnapshotCollectionsInto(&dst, []NamedCollection{{
		Name: "docs", Collection: old,
	}}); !errors.Is(err, ErrClosed) {
		t.Fatalf("repeated closed-reader failure = %v, want %v", err, ErrClosed)
	}
	recreated, err := db.CreateCollection("docs", testDatabaseOptions())
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, recreated, "new", `{"incarnation":2}`)
	if err := SnapshotCollectionsInto(&dst, []NamedCollection{{
		Name: "docs", Collection: recreated,
	}}); err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	newView, ok := dst.Collection("docs")
	if !ok || newView == nil {
		t.Fatal("recreated incarnation is absent")
	}
	if _, found, err := newView.AppendRaw(nil, []byte("old")); err != nil || found {
		t.Fatalf("recreated incarnation exposed old row: found=%v err=%v", found, err)
	}
	raw, found, err = newView.AppendRaw(raw[:0], []byte("new"))
	if err != nil || !found || string(raw) != `{"incarnation":2}` {
		t.Fatalf("new incarnation read = %s,%v,%v", raw, found, err)
	}
}

func oldViewRaw(
	dst DatabaseSnapshot, relation, key string,
) ([]byte, bool, error) {
	view, ok := dst.Collection(relation)
	if !ok || view == nil {
		return nil, false, ErrClosed
	}
	return view.AppendRaw(nil, []byte(key))
}

func TestSnapshotCollectionsIntoPinsIndexCatalogGeneration(t *testing.T) {
	options := testDatabaseOptions()
	options.MaxBatchDocuments = 4
	db, err := OpenDatabase(t.TempDir(), DatabaseOptions{Options: options})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	docs, err := db.CreateCollection("docs", options)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, docs, "one", `{"kind":"ready"}`)

	var before DatabaseSnapshot
	if err := SnapshotCollectionsInto(&before, []NamedCollection{{
		Name: "docs", Collection: docs,
	}}); err != nil {
		t.Fatal(err)
	}
	defer before.Close()
	if _, err := docs.CreateIndex(store.IndexDefinition{
		Name: "by_kind", Paths: []string{"/kind"},
	}); err != nil {
		t.Fatal(err)
	}
	oldView, _ := before.Collection("docs")
	if indexes := oldView.AppendIndexes(nil); len(indexes) != 0 {
		t.Fatalf("old capture saw future index catalog: %+v", indexes)
	}

	var after DatabaseSnapshot
	if err := SnapshotCollectionsInto(&after, []NamedCollection{{
		Name: "docs", Collection: docs,
	}}); err != nil {
		t.Fatal(err)
	}
	defer after.Close()
	newView, _ := after.Collection("docs")
	indexes := newView.AppendIndexes(nil)
	if len(indexes) != 1 || indexes[0].Name != "by_kind" {
		t.Fatalf("new capture indexes = %+v", indexes)
	}
}

func TestSnapshotCollectionsIntoConcurrentIndexRootPublication(t *testing.T) {
	options := testDatabaseOptions()
	options.MaxBatchDocuments = 4
	db, err := OpenDatabase(t.TempDir(), DatabaseOptions{Options: options})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	docs, err := db.CreateCollection("docs", options)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 128 {
		mustPut(t, docs, fmt.Sprintf("k%03d", i),
			fmt.Sprintf(`{"kind":"g%02d"}`, i%8))
	}
	catalog := []NamedCollection{{Name: "docs", Collection: docs}}
	buildDone := make(chan error, 1)
	go func() {
		_, buildErr := docs.CreateIndex(store.IndexDefinition{
			Name: "by_kind", Paths: []string{"/kind"},
		})
		buildDone <- buildErr
	}()

	var capture DatabaseSnapshot
	captures := 0
	deadline := time.After(30 * time.Second)
	for {
		if err := SnapshotCollectionsInto(&capture, catalog); err != nil {
			t.Fatal(err)
		}
		view, _ := capture.Collection("docs")
		indexes := view.AppendIndexes(nil)
		if len(indexes) > 1 || (len(indexes) == 1 && indexes[0].Name != "by_kind") {
			t.Fatalf("torn index catalog = %+v", indexes)
		}
		captures++
		select {
		case err := <-buildDone:
			if err != nil {
				t.Fatal(err)
			}
			if err := capture.Close(); err != nil {
				t.Fatal(err)
			}
			if captures == 0 {
				t.Fatal("index publication raced no captures")
			}
			return
		case <-deadline:
			t.Fatal("index publication did not complete")
		default:
		}
	}
}

func TestSnapshotCollectionsIntoConcurrentMutationAndCapture(t *testing.T) {
	options := testDatabaseOptions()
	options.Durability = DurabilityBufferedVisible
	options.CheckpointStrength = CheckpointFilesystem
	db, err := OpenDatabase(t.TempDir(), DatabaseOptions{Options: options})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a, err := db.CreateCollection("a", options)
	if err != nil {
		t.Fatal(err)
	}
	b, err := db.CreateCollection("b", options)
	if err != nil {
		t.Fatal(err)
	}
	collections := []NamedCollection{
		{Name: "b", Collection: b},
		{Name: "a", Collection: a},
	}

	var writers sync.WaitGroup
	for worker := range 2 {
		writers.Go(func() {
			for i := range 40 {
				collection := a
				if (worker+i)&1 != 0 {
					collection = b
				}
				key := fmt.Appendf(nil, "w%d-%d", worker, i)
				value := fmt.Appendf(nil, `{"worker":%d,"row":%d}`, worker, i)
				if _, putErr := collection.Put(key, value); putErr != nil {
					t.Errorf("Put: %v", putErr)
					return
				}
			}
		})
	}

	var capture DatabaseSnapshot
	for range 40 {
		if err := SnapshotCollectionsInto(&capture, collections); err != nil {
			t.Fatal(err)
		}
		if capture.Len() != 2 {
			t.Fatalf("capture Len = %d, want 2", capture.Len())
		}
		capture.All(func(_ string, view *Snapshot) bool {
			_ = view.Len()
			return true
		})
	}
	writers.Wait()
	if err := capture.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotCollectionsIntoWarmedLoopDoesNotAllocate(t *testing.T) {
	db := newTestDatabase(t, "a", "b", "c")
	a, _ := db.Collection("a")
	b, _ := db.Collection("b")
	c, _ := db.Collection("c")
	collections := []NamedCollection{
		{Name: "empty"},
		{Name: "c", Collection: c},
		{Name: "a", Collection: a},
		{Name: "b", Collection: b},
	}

	var dst DatabaseSnapshot
	if err := SnapshotCollectionsInto(&dst, collections); err != nil {
		t.Fatal(err)
	}
	allocations := testing.AllocsPerRun(1_000, func() {
		if err := SnapshotCollectionsInto(&dst, collections); err != nil {
			panic(err)
		}
		if dst.Len() != len(collections) {
			panic("snapshot lost a catalog entry")
		}
	})
	if allocations != 0 {
		t.Fatalf("warmed SnapshotCollectionsInto allocated %.2f times, want 0",
			allocations)
	}
	if err := dst.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDurableDatabaseSnapshotIntoWarmedLoopDoesNotAllocate(t *testing.T) {
	db := newTestDatabase(t, "a", "b", "c")
	var dst DatabaseSnapshot
	if err := db.SnapshotInto(&dst); err != nil {
		t.Fatal(err)
	}
	allocations := testing.AllocsPerRun(1_000, func() {
		if err := db.SnapshotInto(&dst); err != nil {
			panic(err)
		}
		if dst.Len() != 3 {
			panic("database snapshot lost a catalog entry")
		}
	})
	if allocations != 0 {
		t.Fatalf("warmed Database.SnapshotInto allocated %.2f times, want 0",
			allocations)
	}
	if err := dst.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDatabaseSnapshotCloseRetainsAndReleaseDropsHighWaterStorage(t *testing.T) {
	db := newTestDatabase(t, "docs")
	docs, _ := db.Collection("docs")
	var dst DatabaseSnapshot
	if err := SnapshotCollectionsInto(&dst, []NamedCollection{{
		Name: "docs", Collection: docs,
	}}); err != nil {
		t.Fatal(err)
	}
	entry := &dst.storage[0]
	retained := entry.storage
	retained.overflowScanValue = make([]byte, 0, 64)
	retained.scanSpliceScratch = make([]byte, 0, 128)
	retained.maskGroups = make([]primarySnapshotMaskGroup, 0, 8)
	if err := dst.Close(); err != nil {
		t.Fatal(err)
	}
	if dst.Len() != 0 || len(dst.storage) != 1 ||
		dst.storage[0].name != "docs" || dst.storage[0].storage != retained {
		t.Fatalf("Close discarded reusable metadata: len=%d storage=%d name=%q",
			dst.Len(), len(dst.storage), dst.storage[0].name)
	}
	if cap(retained.overflowScanValue) != 64 ||
		cap(retained.scanSpliceScratch) != 128 || cap(retained.maskGroups) != 8 {
		t.Fatal("Close discarded reusable snapshot scan buffers")
	}
	if err := SnapshotCollectionsInto(&dst, []NamedCollection{{
		Name: "docs", Collection: docs,
	}}); err != nil {
		t.Fatal(err)
	}
	if dst.storage[0].storage != retained {
		t.Fatal("rebind replaced reusable Snapshot object")
	}
	if err := dst.Release(); err != nil {
		t.Fatal(err)
	}
	if dst.entries != nil || dst.storage != nil || dst.ordered != nil ||
		dst.gateOrder != nil {
		t.Fatal("Release retained destination-owned catalog storage")
	}
	if cap(retained.overflowScanValue) != 0 ||
		cap(retained.scanSpliceScratch) != 0 || cap(retained.maskGroups) != 0 {
		t.Fatal("Release retained Snapshot high-water buffers")
	}
	if err := dst.Release(); err != nil {
		t.Fatalf("idempotent Release: %v", err)
	}
	if err := SnapshotCollectionsInto(&dst, []NamedCollection{{
		Name: "docs", Collection: docs,
	}}); err != nil {
		t.Fatalf("capture after Release: %v", err)
	}
	if dst.Len() != 1 {
		t.Fatalf("capture after Release Len = %d, want 1", dst.Len())
	}
	if err := dst.Release(); err != nil {
		t.Fatalf("final Release: %v", err)
	}
}
