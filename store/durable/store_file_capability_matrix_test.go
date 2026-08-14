package durable

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/internal/collectionname"
	"github.com/thesyncim/vibedb/internal/conformance"
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
)

// TestNativeCapabilityMatrix consumes every native row in the public
// conformance manifest. Every listed operation expands into an independent
// subtest and fresh collection; indexed rows additionally compare both posting
// candidates and exact answers with an independent scan oracle before and
// after reopen. Multi-table rows drive durable.Database.Update across every
// DatabaseTxn lane (and refuse DatabaseTxnErrorLanes).
func TestNativeCapabilityMatrix(t *testing.T) {
	for _, capability := range conformance.CasesFor(conformance.Native) {
		capability := capability
		t.Run(capability.ID, func(t *testing.T) {
			for _, lane := range capability.Lanes {
				lane := lane
				t.Run(string(lane), func(t *testing.T) {
					for _, tables := range capability.Tables {
						tables := tables
						t.Run(string(tables), func(t *testing.T) {
							for _, keys := range capability.Keys {
								keys := keys
								t.Run(string(keys), func(t *testing.T) {
									for _, operation := range capability.Operations {
										operation := operation
										t.Run(string(operation), func(t *testing.T) {
											if tables == conformance.MultipleTables {
												runNativeDatabaseTxnCapability(
													t, capability, lane, keys, operation,
												)
												return
											}
											runNativeOneTableCapability(
												t, capability, lane, keys, operation,
											)
										})
									}
								})
							}
						})
					}
				})
			}
		})
	}
}

func runNativeOneTableCapability(
	t *testing.T, capability conformance.Case, lane conformance.Lane,
	keys conformance.Keys, operation conformance.Operation,
) {
	t.Helper()
	fixture := openNativeCapabilityFixture(
		t, lane, capability.Indexing == conformance.Indexed,
	)
	defer fixture.close(t)

	if capability.Transaction == conformance.Explicit {
		wantSupport := capability.Result == conformance.Success
		if got := fixture.collection.SupportsUpdate(); got != wantSupport {
			t.Fatalf("SupportsUpdate = %v, want %v", got, wantSupport)
		}
	}

	before := nativeCapabilityContent(t, fixture.collection)
	generation := fixture.collection.Generation()
	old, err := fixture.collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	callbackRan := false
	err = applyNativeCapability(
		fixture.collection, capability.Transaction, keys,
		operation, func() { callbackRan = true },
	)
	if capability.Result == conformance.DocumentedError {
		if !errors.Is(err, ErrPrimaryBatchUnsupportedLane) {
			t.Fatalf("Update error = %v, want ErrPrimaryBatchUnsupportedLane", err)
		}
		if callbackRan {
			t.Fatal("unsupported Update ran its callback")
		}
		if fixture.collection.Generation() != generation {
			t.Fatalf("refused Update advanced generation %d -> %d",
				generation, fixture.collection.Generation())
		}
		assertNativeCapabilityContent(t, fixture.collection, before)
		assertNativeCapabilityIndexes(t, fixture.collection)
		if fixture.collection.PersistenceError() != nil {
			t.Fatalf("capability refusal poisoned collection: %v",
				fixture.collection.PersistenceError())
		}
		if _, putErr := fixture.collection.Put(
			[]byte("after-refusal"), nativeCapabilityDoc("usable", 99),
		); putErr != nil {
			t.Fatalf("point write after capability refusal: %v", putErr)
		}
		_ = old.Close()
		return
	}
	if err != nil {
		t.Fatalf("capability execution: %v", err)
	}
	if capability.Transaction == conformance.Explicit && !callbackRan {
		t.Fatal("supported Update did not run its callback")
	}
	if fixture.collection.Generation() <= generation {
		t.Fatalf("successful capability did not publish: generation %d",
			fixture.collection.Generation())
	}
	if got := nativeCapabilitySnapshotContent(t, old); !nativeMapsEqual(got, before) {
		t.Fatalf("old snapshot crossed atomic cut: got=%v want=%v", got, before)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	assertNativeCapabilityIndexes(t, fixture.collection)

	if capability.Atomic && capability.Rollback {
		assertNativeCapabilityRollback(t, fixture.collection, capability.Transaction)
	}
	if nativeCapabilityReopenGate(capability, keys) {
		want := nativeCapabilityContent(t, fixture.collection)
		fixture.reopen(t)
		assertNativeCapabilityContent(t, fixture.collection, want)
		assertNativeCapabilityIndexes(t, fixture.collection)
	}
}

type nativeCapabilityFixture struct {
	path       string
	file       *os.File
	collection *Collection
	options    Options
}

func openNativeCapabilityFixture(
	t *testing.T, lane conformance.Lane, indexed bool,
) *nativeCapabilityFixture {
	t.Helper()
	path := filepath.Join(t.TempDir(), "capability.vibe")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	options := nativeCapabilityOptions(lane, indexed)
	createOptions := options
	if lane == conformance.SyncChainFence {
		createOptions.Durability = DurabilityAsyncVisible
	}
	builder, err := store.NewBuilder(store.Options{ChunkDocuments: 4})
	if err != nil {
		t.Fatal(err)
	}
	for i, key := range []string{"a", "b", "c", "d"} {
		if err := builder.Append(key, nativeCapabilityDoc("old", i)); err != nil {
			t.Fatal(err)
		}
	}
	source, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateFromPrimary(source, file, createOptions); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	collection, err := Open(file, options)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	return &nativeCapabilityFixture{
		path: path, file: file, collection: collection, options: options,
	}
}

// Reopen is the persistence gate, not a reason to multiply every semantic
// permutation by another filesystem lifecycle. Point rows qualify each lane
// once; batch rows qualify the wider multi-key shape, which subsumes one key.
func nativeCapabilityReopenGate(
	capability conformance.Case, keys conformance.Keys,
) bool {
	if capability.Result == conformance.DocumentedError {
		return false
	}
	return capability.Transaction == conformance.Autocommit ||
		keys == conformance.MultipleKeys
}

func nativeCapabilityOptions(lane conformance.Lane, indexed bool) Options {
	options := Options{
		Collection: store.Options{ChunkDocuments: 4},
		Backend:    BackendPortable, WriteMode: WriteBuffered,
		ResidentBytes: 16 << 20, MaxKeyBytes: 128,
		InlineValueBytes: 512, MaxDocumentBytes: 2048,
		MaxBatchDocuments: 16, MaxSnapshotLeases: 32,
		MaxRetiredExtents: 4096, GroupLimit: 1,
	}
	switch lane {
	case conformance.SyncJournal, conformance.SyncChainFence:
		options.Durability = DurabilitySync
	case conformance.AsyncCOW:
		options.Durability = DurabilityAsyncVisible
	case conformance.BufferedVolatilePowerSafe:
		options.Durability = DurabilityBufferedVisible
	case conformance.BufferedVolatileFilesystem:
		options.Durability = DurabilityBufferedVisible
		options.CheckpointStrength = CheckpointFilesystem
	case conformance.BufferedJournalPowerSafe:
		options.Durability = DurabilityBufferedVisible
		options.RecoveryJournal = true
	case conformance.BufferedJournalFilesystem:
		options.Durability = DurabilityBufferedVisible
		options.CheckpointStrength = CheckpointFilesystem
		options.RecoveryJournal = true
	default:
		panic("unknown native conformance lane " + lane)
	}
	if indexed {
		options.Indexes = []store.IndexDefinition{{
			Name: "by_group", Paths: []string{"/group"},
		}}
	}
	return options
}

func (f *nativeCapabilityFixture) reopen(t *testing.T) {
	t.Helper()
	want := nativeCapabilityContent(t, f.collection)
	if err := f.collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := f.collection.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(f.file, f.options)
	if err != nil {
		t.Fatal(err)
	}
	f.collection = reopened
	assertNativeCapabilityContent(t, f.collection, want)
}

func (f *nativeCapabilityFixture) close(t *testing.T) {
	t.Helper()
	if f.collection != nil {
		_ = f.collection.Close()
	}
	if f.file != nil {
		_ = f.file.Close()
	}
}

func applyNativeCapability(
	collection *Collection, transaction conformance.Transaction,
	keys conformance.Keys, operation conformance.Operation, callback func(),
) error {
	if transaction == conformance.Autocommit {
		callback()
		switch operation {
		case conformance.Insert:
			if keys == conformance.OneKey {
				_, err := collection.Put([]byte("insert-one"), nativeCapabilityDoc("insert", 10))
				return err
			}
			if _, err := collection.Put([]byte("insert-a"), nativeCapabilityDoc("insert", 20)); err != nil {
				return err
			}
			_, err := collection.Put([]byte("insert-b"), nativeCapabilityDoc("insert", 21))
			return err
		case conformance.Update:
			if _, err := collection.Put([]byte("a"), nativeCapabilityDoc("updated", 22)); err != nil {
				return err
			}
			if keys == conformance.OneKey {
				return nil
			}
			_, err := collection.Put([]byte("b"), nativeCapabilityDoc("updated", 23))
			return err
		case conformance.Delete:
			if _, err := collection.Delete([]byte("c")); err != nil {
				return err
			}
			if keys == conformance.OneKey {
				return nil
			}
			_, err := collection.Delete([]byte("d"))
			return err
		case conformance.Mixed:
			if keys == conformance.OneKey {
				if _, err := collection.Put([]byte("c"), nativeCapabilityDoc("mixed-before", 12)); err != nil {
					return err
				}
				if _, err := collection.Delete([]byte("c")); err != nil {
					return err
				}
				_, err := collection.Put([]byte("c"), nativeCapabilityDoc("mixed-final", 13))
				return err
			}
			if _, err := collection.Put([]byte("mixed-new"), nativeCapabilityDoc("mixed-insert", 24)); err != nil {
				return err
			}
			if _, err := collection.Put([]byte("a"), nativeCapabilityDoc("mixed-update", 25)); err != nil {
				return err
			}
			_, err := collection.Delete([]byte("c"))
			return err
		default:
			return fmt.Errorf("unknown native capability operation %q", operation)
		}
	}
	return collection.Update(func(batch *WriteBatch) error {
		callback()
		switch operation {
		case conformance.Insert:
			if err := batch.Put([]byte("insert-a"), nativeCapabilityDoc("insert", 40)); err != nil {
				return err
			}
			if keys == conformance.OneKey {
				return nil
			}
			return batch.Put([]byte("insert-b"), nativeCapabilityDoc("insert", 41))
		case conformance.Update:
			if err := batch.Put([]byte("a"), nativeCapabilityDoc("updated", 42)); err != nil {
				return err
			}
			if keys == conformance.OneKey {
				return nil
			}
			return batch.Put([]byte("b"), nativeCapabilityDoc("updated", 43))
		case conformance.Delete:
			if err := batch.Delete([]byte("c")); err != nil {
				return err
			}
			if keys == conformance.OneKey {
				return nil
			}
			return batch.Delete([]byte("d"))
		case conformance.Mixed:
			if keys == conformance.OneKey {
				if err := batch.Put([]byte("c"), nativeCapabilityDoc("mixed-before", 44)); err != nil {
					return err
				}
				if err := batch.Delete([]byte("c")); err != nil {
					return err
				}
				return batch.Put([]byte("c"), nativeCapabilityDoc("mixed-final", 45))
			}
			if err := batch.Put([]byte("mixed-new"), nativeCapabilityDoc("mixed-insert", 46)); err != nil {
				return err
			}
			if err := batch.Put([]byte("a"), nativeCapabilityDoc("mixed-update", 47)); err != nil {
				return err
			}
			return batch.Delete([]byte("c"))
		default:
			return fmt.Errorf("unknown native capability operation %q", operation)
		}
	})
}

func assertNativeCapabilityRollback(
	t *testing.T, collection *Collection, transaction conformance.Transaction,
) {
	t.Helper()
	want := nativeCapabilityContent(t, collection)
	generation := collection.Generation()
	if transaction == conformance.Explicit {
		err := collection.Update(func(batch *WriteBatch) error {
			if err := batch.Put([]byte("rollback-good"), nativeCapabilityDoc("rollback", 1)); err != nil {
				return err
			}
			return batch.Put([]byte("rollback-bad"), []byte(`{"group":`))
		})
		if err == nil {
			t.Fatal("malformed batch succeeded")
		}
	} else {
		if _, err := collection.Put([]byte("rollback-bad"), []byte(`{"group":`)); err == nil {
			t.Fatal("malformed point mutation succeeded")
		}
	}
	if collection.Generation() != generation {
		t.Fatalf("rejected mutation advanced generation %d -> %d",
			generation, collection.Generation())
	}
	assertNativeCapabilityContent(t, collection, want)
	assertNativeCapabilityIndexes(t, collection)
}

func runNativeDatabaseTxnCapability(
	t *testing.T, capability conformance.Case, lane conformance.Lane,
	keys conformance.Keys, operation conformance.Operation,
) {
	t.Helper()
	fixture := openNativeDatabaseTxnFixture(
		t, lane, capability.Indexing == conformance.Indexed,
	)
	defer fixture.close(t)

	before := map[string]map[string]string{}
	generations := map[string]uint64{}
	for name, collection := range fixture.collections {
		before[name] = nativeCapabilityContent(t, collection)
		generations[name] = collection.Generation()
	}
	old, err := fixture.db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	callbackRan := false
	err = fixture.db.Update(func(batch *DatabaseBatch) error {
		callbackRan = true
		for _, name := range fixture.names {
			wb, collErr := batch.Collection(name)
			if collErr != nil {
				return collErr
			}
			if applyErr := applyNativeDatabaseTxnBatch(wb, keys, operation); applyErr != nil {
				return applyErr
			}
		}
		return nil
	})
	if capability.Result == conformance.DocumentedError {
		if !errors.Is(err, ErrDatabaseTransactionUnsupportedLane) {
			t.Fatalf("Update error = %v, want ErrDatabaseTransactionUnsupportedLane", err)
		}
		for name, collection := range fixture.collections {
			if collection.Generation() != generations[name] {
				t.Fatalf("%s generation advanced on refusal %d -> %d",
					name, generations[name], collection.Generation())
			}
			assertNativeCapabilityContent(t, collection, before[name])
			assertNativeCapabilityIndexes(t, collection)
			if collection.PersistenceError() != nil {
				t.Fatalf("%s poisoned after refusal: %v", name, collection.PersistenceError())
			}
		}
		_ = old.Close()
		return
	}
	if err != nil {
		t.Fatalf("capability execution: %v", err)
	}
	if !callbackRan {
		t.Fatal("supported Database.Update did not run its callback")
	}
	for name, collection := range fixture.collections {
		if collection.Generation() <= generations[name] {
			t.Fatalf("%s did not publish: generation %d", name, collection.Generation())
		}
		snap, ok := old.Collection(name)
		if !ok {
			t.Fatalf("old snapshot missing %s", name)
		}
		if got := nativeCapabilitySnapshotContent(t, snap); !nativeMapsEqual(got, before[name]) {
			t.Fatalf("old snapshot crossed atomic cut on %s: got=%v want=%v",
				name, got, before[name])
		}
		assertNativeCapabilityIndexes(t, collection)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	if capability.Atomic && capability.Rollback {
		assertNativeDatabaseTxnRollback(t, fixture)
	}
	if keys == conformance.MultipleKeys {
		want := map[string]map[string]string{}
		for name, collection := range fixture.collections {
			want[name] = nativeCapabilityContent(t, collection)
		}
		fixture.reopen(t)
		for name, collection := range fixture.collections {
			assertNativeCapabilityContent(t, collection, want[name])
			assertNativeCapabilityIndexes(t, collection)
		}
	}
}

type nativeDatabaseTxnFixture struct {
	dir         string
	db          *Database
	names       []string
	collections map[string]*Collection
	options     Options
}

func openNativeDatabaseTxnFixture(
	t *testing.T, lane conformance.Lane, indexed bool,
) *nativeDatabaseTxnFixture {
	t.Helper()
	dir := t.TempDir()
	options := nativeCapabilityOptions(lane, indexed)
	names := []string{"alpha", "beta"}

	if lane == conformance.SyncChainFence {
		async := options
		async.Durability = DurabilityAsyncVisible
		for _, name := range names {
			filename, ok := collectionname.Encode(name)
			if !ok {
				t.Fatalf("encode %q", name)
			}
			path := filepath.Join(dir, filename)
			file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			coll, err := Create(file, async)
			if err != nil {
				_ = file.Close()
				t.Fatal(err)
			}
			if err := coll.Close(); err != nil {
				_ = file.Close()
				t.Fatal(err)
			}
			_ = file.Close()
		}
		db, err := OpenDatabase(dir, DatabaseOptions{Options: options})
		if err != nil {
			t.Fatal(err)
		}
		fixture := &nativeDatabaseTxnFixture{
			dir: dir, db: db, names: names,
			collections: make(map[string]*Collection, len(names)),
			options:     options,
		}
		for _, name := range names {
			collection, ok := db.Collection(name)
			if !ok {
				t.Fatalf("missing collection %q", name)
			}
			if !collection.chainFenceSync() {
				t.Fatalf("%s is not chain-fence after async→sync reopen", name)
			}
			fixture.collections[name] = collection
			seedNativeCapabilityCollection(t, collection)
		}
		return fixture
	}

	db, err := OpenDatabase(dir, DatabaseOptions{Options: options})
	if err != nil {
		t.Fatal(err)
	}
	fixture := &nativeDatabaseTxnFixture{
		dir: dir, db: db, names: names,
		collections: make(map[string]*Collection, len(names)),
		options:     options,
	}
	for _, name := range names {
		collection, err := db.CreateCollection(name, options)
		if err != nil {
			t.Fatal(err)
		}
		fixture.collections[name] = collection
		seedNativeCapabilityCollection(t, collection)
	}
	return fixture
}

func seedNativeCapabilityCollection(t *testing.T, collection *Collection) {
	t.Helper()
	for i, key := range []string{"a", "b", "c", "d"} {
		if _, err := collection.Put([]byte(key), nativeCapabilityDoc("old", i)); err != nil {
			t.Fatal(err)
		}
	}
	// Create mints the current journal grammar for DurabilitySync /
	// buffered-journal. Flush folds the seed window so the later
	// Database.Update prepare stages against an empty journal.
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
}

func (f *nativeDatabaseTxnFixture) reopen(t *testing.T) {
	t.Helper()
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := OpenDatabase(f.dir, DatabaseOptions{Options: f.options})
	if err != nil {
		t.Fatal(err)
	}
	f.db = db
	f.collections = make(map[string]*Collection, len(f.names))
	for _, name := range f.names {
		collection, ok := db.Collection(name)
		if !ok {
			t.Fatalf("reopen missing %q", name)
		}
		f.collections[name] = collection
	}
}

func (f *nativeDatabaseTxnFixture) close(t *testing.T) {
	t.Helper()
	if f.db != nil {
		_ = f.db.Close()
		f.db = nil
	}
}

func applyNativeDatabaseTxnBatch(
	batch *WriteBatch, keys conformance.Keys, operation conformance.Operation,
) error {
	switch operation {
	case conformance.Insert:
		if err := batch.Put([]byte("insert-a"), nativeCapabilityDoc("insert", 40)); err != nil {
			return err
		}
		if keys == conformance.OneKey {
			return nil
		}
		return batch.Put([]byte("insert-b"), nativeCapabilityDoc("insert", 41))
	case conformance.Update:
		if err := batch.Put([]byte("a"), nativeCapabilityDoc("updated", 42)); err != nil {
			return err
		}
		if keys == conformance.OneKey {
			return nil
		}
		return batch.Put([]byte("b"), nativeCapabilityDoc("updated", 43))
	case conformance.Delete:
		if err := batch.Delete([]byte("c")); err != nil {
			return err
		}
		if keys == conformance.OneKey {
			return nil
		}
		return batch.Delete([]byte("d"))
	case conformance.Mixed:
		if keys == conformance.OneKey {
			if err := batch.Put([]byte("c"), nativeCapabilityDoc("mixed-before", 44)); err != nil {
				return err
			}
			if err := batch.Delete([]byte("c")); err != nil {
				return err
			}
			return batch.Put([]byte("c"), nativeCapabilityDoc("mixed-final", 45))
		}
		if err := batch.Put([]byte("mixed-new"), nativeCapabilityDoc("mixed-insert", 46)); err != nil {
			return err
		}
		if err := batch.Put([]byte("a"), nativeCapabilityDoc("mixed-update", 47)); err != nil {
			return err
		}
		return batch.Delete([]byte("c"))
	default:
		return fmt.Errorf("unknown native database-txn operation %q", operation)
	}
}

func assertNativeDatabaseTxnRollback(t *testing.T, fixture *nativeDatabaseTxnFixture) {
	t.Helper()
	want := map[string]map[string]string{}
	generations := map[string]uint64{}
	for name, collection := range fixture.collections {
		want[name] = nativeCapabilityContent(t, collection)
		generations[name] = collection.Generation()
	}
	err := fixture.db.Update(func(batch *DatabaseBatch) error {
		for _, name := range fixture.names {
			wb, collErr := batch.Collection(name)
			if collErr != nil {
				return collErr
			}
			if err := wb.Put([]byte("rollback-good"), nativeCapabilityDoc("rollback", 1)); err != nil {
				return err
			}
		}
		last, err := batch.Collection(fixture.names[len(fixture.names)-1])
		if err != nil {
			return err
		}
		return last.Put([]byte("rollback-bad"), []byte(`{"group":`))
	})
	if err == nil {
		t.Fatal("malformed multi-collection batch succeeded")
	}
	for name, collection := range fixture.collections {
		if collection.Generation() != generations[name] {
			t.Fatalf("%s rejected sibling advanced generation %d -> %d",
				name, generations[name], collection.Generation())
		}
		assertNativeCapabilityContent(t, collection, want[name])
		assertNativeCapabilityIndexes(t, collection)
	}
}

func nativeCapabilityDoc(group string, n int) []byte {
	return fmt.Appendf(nil, `{"group":%q,"n":%d}`, group, n)
}

func nativeCapabilityContent(t *testing.T, collection *Collection) map[string]string {
	t.Helper()
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	return nativeCapabilitySnapshotContent(t, snapshot)
}

func nativeCapabilitySnapshotContent(t *testing.T, snapshot *Snapshot) map[string]string {
	t.Helper()
	out := map[string]string{}
	if err := snapshot.RangeRaw(func(key, value []byte) error {
		out[string(key)] = string(value)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func assertNativeCapabilityContent(
	t *testing.T, collection *Collection, want map[string]string,
) {
	t.Helper()
	if got := nativeCapabilityContent(t, collection); !nativeMapsEqual(got, want) {
		t.Fatalf("content = %v, want %v", got, want)
	}
}

func nativeMapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if b[key] != value {
			return false
		}
	}
	return true
}

func assertNativeCapabilityIndexes(t *testing.T, collection *Collection) {
	t.Helper()
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if len(snapshot.AppendIndexes(nil)) == 0 {
		return
	}
	want := map[string][]string{}
	if err := snapshot.RangeRaw(func(key, value []byte) error {
		var doc struct {
			Group string `json:"group"`
		}
		if err := json.Unmarshal(value, &doc); err != nil {
			return err
		}
		want[doc.Group] = append(want[doc.Group], string(key))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want["absent-sentinel"] = nil
	for group, keys := range want {
		slices.Sort(keys)
		needleRaw, _ := json.Marshal(group)
		need, err := vibejson.RequiredIndexEntries(needleRaw)
		if err != nil {
			t.Fatal(err)
		}
		needle, err := vibejson.BuildIndex(
			needleRaw, make([]vibejson.IndexEntry, need),
		)
		if err != nil {
			t.Fatal(err)
		}
		for _, candidates := range []bool{false, true} {
			var masks []store.Mask
			if candidates {
				masks, err = snapshot.AppendIndexCandidateMasks(
					nil, "by_group", needle,
				)
			} else {
				masks, err = snapshot.AppendIndexMasks(nil, "by_group", needle)
			}
			if err != nil {
				t.Fatalf("group=%q candidates=%v: %v", group, candidates, err)
			}
			var got []string
			if err := snapshot.RangeMasksRaw(masks, func(key, _ []byte) error {
				got = append(got, string(key))
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			slices.Sort(got)
			if !slices.Equal(got, keys) {
				t.Fatalf("group=%q candidates=%v keys=%v want=%v",
					group, candidates, got, keys)
			}
		}
	}
}
