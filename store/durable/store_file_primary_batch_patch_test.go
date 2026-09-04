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
)

func TestPrimaryBatchCompactReplacementFastPath(t *testing.T) {
	options := journalTestOptions(CheckpointPowerSafe)
	options.RecoveryJournal = false
	coll, file, _ := openPrimaryBatchStore(t, options)
	defer coll.Close()
	defer file.Close()

	keys := []string{"batch-a", "batch-b", "batch-c", "batch-d"}
	if err := coll.Update(func(batch *WriteBatch) error {
		for index, key := range keys {
			if err := batch.Put([]byte(key), []byte(fmt.Sprintf(`{"i":%d,"v":100}`, index))); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed batch: %v", err)
	}
	beforeStats := coll.Stats()
	beforeGeneration := coll.state.Load().root.Generation
	if err := coll.Update(func(batch *WriteBatch) error {
		for index, key := range keys {
			if err := batch.Put([]byte(key), []byte(fmt.Sprintf(`{"i":%d,"v":200}`, index))); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("replacement batch: %v", err)
	}
	afterStats := coll.Stats()
	if got := afterStats.PrimaryCompactColumnPatchAttempts - beforeStats.PrimaryCompactColumnPatchAttempts; got != 1 {
		t.Fatalf("compact patch attempts = %d, want 1", got)
	}
	if got := afterStats.PrimaryCompactColumnPatches - beforeStats.PrimaryCompactColumnPatches; got != 1 {
		t.Fatalf("compact patches = %d, want 1", got)
	}
	if got := coll.state.Load().root.Generation - beforeGeneration; got != 1 {
		t.Fatalf("replacement generation delta = %d, want 1", got)
	}
	for index, key := range keys {
		want := fmt.Sprintf(`{"i":%d,"v":200}`, index)
		value, found, err := coll.AppendRaw(nil, []byte(key))
		if err != nil || !found || string(value) != want {
			t.Fatalf("key %q = %q,%v,%v, want %q", key, value, found, err, want)
		}
	}

	route, ok := coll.primaryRouter.Load().Route([]byte(keys[0]))
	if !ok {
		t.Fatal("replacement route missing")
	}
	lease, err := coll.primaryRouter.Load().AcquireLeaf(coll.cache, route)
	if err != nil {
		t.Fatal(err)
	}
	view, admitted := storeio.AdmittedCompactPrimaryStripe(
		lease.Page(), coll.storeID, route.Bucket,
	)
	if !admitted {
		lease.Release()
		t.Fatal("replacement leaf is not admitted compact")
	}
	for index, key := range keys {
		rank, found := view.FindKey([]byte(key))
		if !found {
			lease.Release()
			t.Fatalf("replacement key %q missing from compact leaf", key)
		}
		if value, ok := view.AppendValue(nil, rank); !ok || string(value) != fmt.Sprintf(`{"i":%d,"v":200}`, index) {
			lease.Release()
			t.Fatalf("compact value %q = %q,%v", key, value, ok)
		}
		if _, ok := view.PostingSlot(rank); !ok {
			lease.Release()
			t.Fatalf("replacement key %q lost posting slot", key)
		}
	}
	lease.Release()
}

// Use SQL's default synchronous JSON geometry, and force only the comparison
// collection through the pre-existing full planner. Normalize store identity,
// generation, and placement before comparing encoded images: admitted slots
// and conservative summaries need not match a freshly placed full rebuild.
func TestPrimaryBatchCompactReplacementDefaultGeometryMatchesFullPlanner(t *testing.T) {
	options := Options{Indexes: []store.IndexDefinition{{Name: "u", Paths: []string{"/u"}, Unique: true}}}
	var collections [2]*Collection
	var files [2]*os.File
	for index := range collections {
		file, err := os.CreateTemp(t.TempDir(), "batch-compact-*")
		if err != nil {
			t.Fatal(err)
		}
		files[index] = file
		collection, err := Create(file, options)
		if err != nil {
			t.Fatal(err)
		}
		collections[index] = collection
		t.Cleanup(func() { _ = collections[index].Close(); _ = file.Close() })
		if collection.primaryUnifiedOverlay == nil ||
			cap(collection.primaryUnifiedReplacementScratch) != storeio.CommonPrimaryLeafWideSlots {
			t.Fatal("default synchronous JSON collection has no compact replacement workspace")
		}
		if err := collection.Update(func(batch *WriteBatch) error {
			for _, key := range []string{"a", "b", "c", "d", "e", "f"} {
				if err := batch.Put([]byte(key), []byte(fmt.Sprintf(`{"score":100,"u":%q}`, key))); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	collections[1].primaryUnifiedReplacementScratch = nil // Force the original full planner.
	for _, test := range []struct {
		name string
		ops  []batchOp
		hit  bool
	}{
		{"scalar and final duplicate", []batchOp{
			{key: "a", value: []byte(`{"score":200,"u":"a"}`)},
			{key: "a", value: []byte(`{"score":201,"u":"a"}`)},
			{key: "b", value: []byte(`{"score":201,"u":"b"}`)},
		}, true},
		{"unique swap", []batchOp{
			{key: "a", value: []byte(`{"score":201,"u":"b"}`)},
			{key: "b", value: []byte(`{"score":201,"u":"a"}`)},
		}, true},
		{"two holes decline", []batchOp{{key: "a", value: []byte(`{"score":301,"u":"z"}`)}}, false},
		{"partial qualification then insert", []batchOp{
			{key: "a", value: []byte(`{"score":302,"u":"z"}`)},
			{key: "g", value: []byte(`{"score":100,"u":"g"}`)},
		}, false},
		{"delete decline", []batchOp{{key: "b", remove: true}}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			beforeSlots := compactBatchPostingSlots(t, collections[0])
			beforePatches := collections[0].Stats().PrimaryCompactColumnPatches
			beforeGeneration := collections[0].Generation()
			before := normalizedCompactBatchImage(t, collections[0])
			snapshot, err := collections[0].Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			defer snapshot.Close()
			for _, collection := range collections {
				if err := collection.Update(func(batch *WriteBatch) error {
					for _, op := range test.ops {
						if op.remove {
							if err := batch.Delete([]byte(op.key)); err != nil {
								return err
							}
						} else if err := batch.Put([]byte(op.key), op.value); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			if got := collections[0].Stats().PrimaryCompactColumnPatches - beforePatches; (got == 1) != test.hit || got > 1 {
				t.Fatalf("patches=%d want hit=%v", got, test.hit)
			}
			if collections[0].Generation() != beforeGeneration+1 {
				t.Fatal("batch did not publish exactly one generation")
			}
			if !bytes.Equal(normalizedCompactBatchSnapshot(t, snapshot), before) {
				t.Fatal("replacement mutated the pinned preimage")
			}
			if !bytes.Equal(normalizedCompactBatchImage(t, collections[0]), normalizedCompactBatchImage(t, collections[1])) {
				t.Fatal("compact batch differs from the full planner")
			}
			if test.hit {
				for key, slot := range compactBatchPostingSlots(t, collections[0]) {
					if beforeSlots[key] != slot {
						t.Fatalf("key %q posting slot changed", key)
					}
				}
			}
			scratch := collections[0].primaryUnifiedReplacementScratch
			for _, replacement := range scratch[:cap(scratch)] {
				if replacement.Key != nil || replacement.Value != nil {
					t.Fatal("compact replacement retained borrowed batch bytes")
				}
			}
		})
	}
	for index, collection := range collections {
		before := normalizedCompactBatchImage(t, collection)
		if err := collection.Update(func(batch *WriteBatch) error {
			return batch.Put([]byte("a"), []byte(`{"score":302,"u":"c"}`))
		}); !errors.Is(err, store.ErrUniqueIndexViolation) {
			t.Fatalf("unique conflict err=%v", err)
		}
		if !bytes.Equal(before, normalizedCompactBatchImage(t, collection)) {
			t.Fatal("rejected unique replacement was published")
		}
		if err := collection.Close(); err != nil {
			t.Fatal(err)
		}
		reopened, err := Open(files[index], options)
		if err != nil {
			t.Fatal(err)
		}
		collections[index] = reopened
		if !bytes.Equal(before, normalizedCompactBatchImage(t, reopened)) {
			t.Fatal("reopened compact batch differs from its published image")
		}
	}
	for _, term := range []string{"a", "b", "c", "d", "e", "f", "g", "z"} {
		needle := primaryExactTestNeedle(t, fmt.Sprintf("%q", term))
		fast := primaryExactTestKeys(t, collections[0], "u", needle)
		full := primaryExactTestKeys(t, collections[1], "u", needle)
		slices.Sort(fast)
		slices.Sort(full)
		if !slices.Equal(fast, full) {
			t.Fatalf("reopened index %q: fast=%v full=%v", term, fast, full)
		}
	}
}

func normalizedCompactBatchImage(t *testing.T, collection *Collection) []byte {
	t.Helper()
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	return normalizedCompactBatchSnapshot(t, snapshot)
}

func normalizedCompactBatchSnapshot(t *testing.T, snapshot *Snapshot) []byte {
	t.Helper()
	var records []storeio.CommonPrimaryLeafRecord
	if err := snapshot.RangeRaw(func(key, value []byte) error {
		records = append(records, storeio.CommonPrimaryLeafRecord{
			Key: bytes.Clone(key), Value: storeio.CommonPrimaryLeafValue{Inline: bytes.Clone(value)},
		})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(records, func(a, b storeio.CommonPrimaryLeafRecord) int { return bytes.Compare(a.Key, b.Key) })
	storeID := [16]byte{1}
	if err := storeio.PlaceCommonPrimaryLeafRecords(storeio.CommonPrimaryLeafWide, storeID, records); err != nil {
		t.Fatal(err)
	}
	image, err := storeio.EncodeBestCompactPrimaryStripe(make([]byte, storeio.CommonPrimaryLeafMaxExtentBytes),
		storeio.CommonPrimaryLeafHeader{StoreID: storeID, Generation: 1}, storeID, records, storeio.NewUnifiedPrimaryLeafBuilder())
	if err != nil {
		t.Fatal(err)
	}
	return image
}

func compactBatchPostingSlots(t *testing.T, collection *Collection) map[string]uint8 {
	t.Helper()
	slots := make(map[string]uint8)
	for key := range primaryStoreContent(t, collection) {
		route, ok := collection.primaryRouter.Load().Route([]byte(key))
		if !ok {
			t.Fatalf("missing route for %q", key)
		}
		lease, err := collection.primaryRouter.Load().AcquireLeaf(collection.cache, route)
		if err != nil {
			t.Fatal(err)
		}
		view, admitted := storeio.AdmittedCompactPrimaryStripe(lease.Page(), collection.storeID, route.Bucket)
		if !admitted {
			lease.Release()
			t.Fatal("invalid compact leaf")
		}
		rank, found := view.FindKey([]byte(key))
		slot, valid := view.PostingSlot(rank)
		lease.Release()
		if !found || !valid {
			t.Fatalf("missing posting slot for %q", key)
		}
		slots[key] = slot
	}
	return slots
}
