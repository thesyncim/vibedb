package durable

import (
	"errors"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestFileBatchFingerprintPlannerResolvesForcedCollisionExactly(t *testing.T) {
	collection := &Collection{
		options: normalizedFileStoreOptions{Options: Options{
			MaxKeyBytes: 64, MaxDocumentBytes: 1024,
			MaxBatchDocuments: 8, MaxBatchBytes: 8192,
		}},
	}
	batch := &WriteBatch{
		collection: collection, position: make(map[string]int), active: true,
	}
	if err := batch.Put("update", []byte(`{"v":2}`)); err != nil {
		t.Fatal(err)
	}
	if err := batch.Delete("delete"); err != nil {
		t.Fatal(err)
	}
	if err := batch.Put("create", []byte(`{"v":3}`)); err != nil {
		t.Fatal(err)
	}
	if err := batch.Delete("missing"); err != nil {
		t.Fatal(err)
	}

	const forcedHash = uint64(77)
	state := &fileStoreState{root: storeio.StateRoot{
		StoreID:        [16]byte{1, 2, 3, 4},
		ChunkHighWater: 1, ChunkDocuments: 8, FreeChunkHint: 1,
	}}
	resolved := map[string]storeio.PageKeyLocation{
		"update": {
			Hash: forcedHash, Chunk: 0, Slot: 2,
		},
		"delete": {
			Hash: forcedHash, Chunk: 0, Slot: 1,
		},
	}
	seen := make(map[string]int)
	highWater, freeHint, err := collection.resolveFileBatchWith(
		state, batch,
		func([]byte) uint64 { return forcedHash },
		func(key []byte, hash uint64) (storeio.PageKeyLocation, bool, error) {
			if hash != forcedHash {
				t.Fatalf("resolver hash = %d, want forced collision %d", hash, forcedHash)
			}
			seen[string(key)]++
			location, found := resolved[string(key)]
			return location, found, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if highWater != 2 || freeHint != 0 {
		t.Fatalf("placement = (high-water %d, free-hint %d), want (2,0)", highWater, freeHint)
	}
	for _, key := range []string{"update", "delete", "create", "missing"} {
		if seen[key] != 1 {
			t.Fatalf("exact resolver calls for %q = %d, want 1", key, seen[key])
		}
	}
	if len(collection.batchMutations) != 3 {
		t.Fatalf("effective mutations = %d, want 3", len(collection.batchMutations))
	}

	edits := appendFileBatchKeyEdits(
		make([]storeio.PageKeyTreeEdit, 0, len(collection.batchMutations)),
		collection.batchMutations,
	)
	if len(edits) != 2 {
		t.Fatalf("fingerprint edits = %d, want delete + create", len(edits))
	}
	if edits[0].Operation != storeio.PageKeyTreeDelete ||
		edits[0].Location != resolved["delete"] {
		t.Fatalf("first collision edit = %+v, want exact delete %+v", edits[0], resolved["delete"])
	}
	if edits[1].Operation != storeio.PageKeyTreeInsert ||
		edits[1].Location.Hash != forcedHash ||
		edits[1].Location.Chunk != 1 || edits[1].Location.Slot != 0 {
		t.Fatalf("second collision edit = %+v, want allocated insert at {77,1,0}", edits[1])
	}
	if err := validateFileBatchKeyMutation(storeio.PageKeyTreeBatchMutation{
		Applied: len(edits), Changed: true,
	}, len(edits)); err != nil {
		t.Fatalf("matching applied count rejected: %v", err)
	}
	if err := validateFileBatchKeyMutation(storeio.PageKeyTreeBatchMutation{
		Applied: len(edits) - 1, Changed: true,
	}, len(edits)); !errors.Is(err, storeio.ErrKeyDirectoryCorrupt) {
		t.Fatalf("short applied count = %v, want key-directory corruption", err)
	}
	if err := validateFileBatchKeyMutation(storeio.PageKeyTreeBatchMutation{
		Applied: len(edits), Changed: false,
	}, len(edits)); !errors.Is(err, storeio.ErrKeyDirectoryCorrupt) {
		t.Fatalf("unchanged effective mutation = %v, want key-directory corruption", err)
	}
}

func TestCollectionBatchFingerprintMixedMutationReopens(t *testing.T) {
	options := testBatchOptions(8)
	file, err := os.CreateTemp(t.TempDir(), "batch-fingerprint-cutover-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := collection.Update(func(batch *WriteBatch) error {
		if err := batch.Put("update", []byte(`{"v":1}`)); err != nil {
			return err
		}
		if err := batch.Put("delete", []byte(`{"v":2}`)); err != nil {
			return err
		}
		return batch.Put("same", []byte(`{"v":3}`))
	}); err != nil {
		t.Fatal(err)
	}

	beforeUpdateRoot := collection.state.Load().keyRoot
	if beforeUpdateRoot.Kind != storeio.PageFingerprintDirectory {
		t.Fatalf("key root kind = %v, want fingerprint directory", beforeUpdateRoot.Kind)
	}
	if err := collection.Update(func(batch *WriteBatch) error {
		if err := batch.Put("update", []byte(`{"v":10}`)); err != nil {
			return err
		}
		if err := batch.Put("same", []byte(`{"v":3}`)); err != nil {
			return err
		}
		return batch.Delete("absent")
	}); err != nil {
		t.Fatal(err)
	}
	if after := collection.state.Load().keyRoot; after != beforeUpdateRoot {
		t.Fatalf("stable-location update rewrote fingerprint pages: before=%+v after=%+v", beforeUpdateRoot, after)
	}

	if err := collection.Update(func(batch *WriteBatch) error {
		if err := batch.Put("update", []byte(`{"v":11}`)); err != nil {
			return err
		}
		if err := batch.Delete("delete"); err != nil {
			return err
		}
		if err := batch.Put("create", []byte(`{"v":4}`)); err != nil {
			return err
		}
		return batch.Delete("still-absent")
	}); err != nil {
		t.Fatal(err)
	}
	assertBatchFingerprintValue(t, collection, "update", `{"v":11}`, true)
	assertBatchFingerprintValue(t, collection, "create", `{"v":4}`, true)
	assertBatchFingerprintValue(t, collection, "same", `{"v":3}`, true)
	assertBatchFingerprintValue(t, collection, "delete", "", false)
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if root := reopened.state.Load().keyRoot; root.Kind != storeio.PageFingerprintDirectory {
		t.Fatalf("reopened key root kind = %v, want fingerprint directory", root.Kind)
	}
	assertBatchFingerprintValue(t, reopened, "update", `{"v":11}`, true)
	assertBatchFingerprintValue(t, reopened, "create", `{"v":4}`, true)
	assertBatchFingerprintValue(t, reopened, "same", `{"v":3}`, true)
	assertBatchFingerprintValue(t, reopened, "delete", "", false)
}

func assertBatchFingerprintValue(
	t testing.TB, collection *Collection, key, want string, wantFound bool,
) {
	t.Helper()
	raw, found, err := collection.AppendRaw(nil, key)
	if err != nil || found != wantFound || found && string(raw) != want {
		t.Fatalf("%q = (%q,%v,%v), want (%q,%v,nil)", key, raw, found, err, want, wantFound)
	}
}
