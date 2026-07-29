package durable

import (
	"fmt"
	"math/bits"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
)

func TestFileStoreRandomizedHeapDifferentialAndReopen(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-store-differential-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.ResidentBytes = 8 << 20
	options.BufferCount = 1024
	options.MaxRetiredExtents = 2048
	options.Indexes = []store.IndexDefinition{{Name: "status", Paths: []string{"/status"}}}
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	heapCollection, err := store.New(options.Collection)
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(0x5eed))
	mirrored := 0

	for step := range 160 {
		key := fmt.Sprintf("key-%02d", rng.Intn(32))
		switch rng.Intn(3) {
		case 0, 1:
			status := []string{"active", "idle", "paused"}[rng.Intn(3)]
			doc := []byte(fmt.Sprintf(`{"step":%d,"status":%q,"value":%d,"padding":%q}`,
				step, status, rng.Int63(), strings.Repeat("x", rng.Intn(900))))
			heapCreated, heapErr := heapCollection.Put(key, doc)
			fileCreated, fileErr := collection.Put([]byte(key), doc)
			if heapErr != nil || fileErr != nil || heapCreated != fileCreated {
				t.Fatalf("step %d Put = heap(%v,%v) file(%v,%v)", step, heapCreated, heapErr, fileCreated, fileErr)
			}
		case 2:
			heapDeleted, _ := heapCollection.Delete(key)
			fileDeleted, fileErr := collection.Delete([]byte(key))
			if fileErr != nil || heapDeleted != fileDeleted {
				t.Fatalf("step %d Delete = heap %v file(%v,%v)", step, heapDeleted, fileDeleted, fileErr)
			}
		}

		// The mirror is checked after every step, not sampled. It is the
		// invariant whose violation corrupts, and a diff is only wrong for the
		// one commit that produced it: sampling would attribute the damage to
		// whichever later commit happened to be observed.
		if compared := assertFreeSetMirror(t, collection, fmt.Sprintf("step %d", step)); compared > 0 {
			mirrored++
		}
		if step%13 == 0 {
			assertFileCollectionMatchesHeap(t, collection, heapCollection, 32)
		}
		if step == 79 {
			heapSnapshot, _ := heapCollection.Snapshot()
			fileSnapshot, snapshotErr := collection.Snapshot()
			if snapshotErr != nil {
				t.Fatal(snapshotErr)
			}
			for i := range 16 {
				key := fmt.Sprintf("key-%02d", i)
				doc := []byte(fmt.Sprintf(`{"snapshot-churn":%d,"status":"new"}`, i))
				_, _ = heapCollection.Put(key, doc)
				if _, err := collection.Put([]byte(key), doc); err != nil {
					t.Fatal(err)
				}
			}
			assertFileSnapshotMatchesHeap(t, fileSnapshot, heapSnapshot, 32)
			if err := fileSnapshot.Close(); err != nil {
				t.Fatal(err)
			}
			if err := collection.Close(); err != nil {
				t.Fatal(err)
			}
			collection, err = Open(file, options)
			if err != nil {
				t.Fatal(err)
			}
			assertFileCollectionMatchesHeap(t, collection, heapCollection, 32)
		}
	}
	assertFileCollectionMatchesHeap(t, collection, heapCollection, 32)
	// The mirror check compares nothing when the durable free set is empty, so
	// the workload has to be shown to have produced one. A silently vacuous
	// assertion is worse than none: it reports the invariant as held.
	if mirrored < 50 {
		t.Fatalf("only %d of 160 steps compared a non-empty durable free set", mirrored)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
}

func assertFileCollectionMatchesHeap(t *testing.T, collection *Collection, heapCollection *store.Collection, keys int) {
	t.Helper()
	fileSnapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer fileSnapshot.Close()
	heapSnapshot, _ := heapCollection.Snapshot()
	assertFileSnapshotMatchesHeap(t, fileSnapshot, heapSnapshot, keys)
	if fileSnapshot.Len() != uint64(heapSnapshot.Len()) {
		t.Fatalf("snapshot lengths = file %d heap %d", fileSnapshot.Len(), heapSnapshot.Len())
	}
}

func assertFileSnapshotMatchesHeap(t *testing.T, fileSnapshot *Snapshot, heapSnapshot store.Snapshot, keys int) {
	t.Helper()
	for i := range keys {
		key := fmt.Sprintf("key-%02d", i)
		heapRaw, heapOK := heapSnapshot.GetRaw(key)
		fileRaw, fileOK, err := fileSnapshot.AppendRaw(nil, []byte(key))
		if err != nil || heapOK != fileOK || (heapOK && string(heapRaw.Bytes()) != string(fileRaw)) {
			t.Fatalf("GetRaw(%s) = heap(%q,%v) file(%q,%v,%v)", key, heapRaw.Bytes(), heapOK, fileRaw, fileOK, err)
		}
	}
}

func TestFileStoreCrashImagesRecoverWholeGeneration(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-store-crash-source-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	// The synchronous primary lane acknowledges through a synced recovery-journal
	// record and folds its root at the next checkpoint, so a per-Put store image
	// captures nothing and a torn copy references a journal it was never paired
	// with. This sweep tears the store's own commit and must recover from the
	// alternate superblock alone, so it runs on the journal-free async lane, whose
	// committer writes each generation's data pages and root to the device.
	options.Durability = DurabilityAsyncVisible
	options.ResidentBytes = 8 << 20
	options.BufferCount = 1024
	options.MaxRetiredExtents = 1024
	options.Indexes = []store.IndexDefinition{{Name: "status", Paths: []string{"/status"}}}
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 12 {
		doc := []byte(fmt.Sprintf(`{"id":%d,"status":"old","padding":%q}`, i, strings.Repeat("a", i*80)))
		if _, err := collection.Put([]byte(fmt.Sprintf("key-%02d", i)), doc); err != nil {
			t.Fatal(err)
		}
	}
	// Drain the background committer so the store bytes captured below reflect
	// every acknowledged generation.
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	oldGeneration := collection.Generation()
	oldValue, ok, err := collection.AppendRaw(nil, []byte("key-03"))
	if err != nil || !ok {
		t.Fatal(err)
	}
	before, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	newValue := []byte(fmt.Sprintf(`{"id":3,"status":"new","padding":%q}`, strings.Repeat("z", 7000)))
	if created, err := collection.Put([]byte("key-03"), newValue); err != nil || created {
		t.Fatalf("update = (%v,%v)", created, err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	newGeneration := collection.Generation()
	after, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}

	pageSize := options.PageSize
	dataStart := testMutableDataStart(pageSize)
	dataCuts := changedPageCrashCuts(before, after, dataStart, pageSize)
	for _, cut := range dataCuts {
		image := make([]byte, max(len(before), len(after)))
		copy(image, before)
		copy(image[dataStart:dataStart+cut], after[dataStart:dataStart+cut])
		assertCrashImage(t, image, options, oldGeneration, newGeneration, string(oldValue), string(newValue),
			fmt.Sprintf("data-cut-%d", cut))
	}

	rootOffset := int((newGeneration-1)&1) * pageSize
	for cut := 0; cut <= storeio.InlineSuperblockSize; cut++ {
		image := append([]byte(nil), after...)
		copy(image[rootOffset:rootOffset+pageSize], before[rootOffset:rootOffset+pageSize])
		copy(image[rootOffset:rootOffset+cut], after[rootOffset:rootOffset+cut])
		assertCrashImage(t, image, options, oldGeneration, newGeneration, string(oldValue), string(newValue),
			fmt.Sprintf("root-cut-%d", cut))
	}
}

// changedPageCrashCuts models the sorted positional-write phase used by both
// commit devices. Each returned prefix stops immediately before, inside, or
// after one changed physical page. Unchanged holes do not multiply the test
// matrix, and the final cut represents the completed data barrier.
func changedPageCrashCuts(before, after []byte, start, quantum int) []int {
	length := max(len(before), len(after)) - start
	cuts := make([]int, 0, 16)
	appendCut := func(cut int) {
		if cut < 0 || cut > length {
			return
		}
		if len(cuts) == 0 || cuts[len(cuts)-1] != cut {
			cuts = append(cuts, cut)
		}
	}
	appendCut(0)
	for blockStart := 0; blockStart < length; blockStart += quantum {
		blockEnd := min(blockStart+quantum, length)
		changed := false
		for offset := blockStart; offset < blockEnd; offset++ {
			absolute := start + offset
			var oldByte, newByte byte
			if absolute < len(before) {
				oldByte = before[absolute]
			}
			if absolute < len(after) {
				newByte = after[absolute]
			}
			if oldByte != newByte {
				changed = true
				break
			}
		}
		if changed {
			appendCut(blockStart)
			appendCut(blockStart + 1)
			appendCut(blockEnd - 1)
			appendCut(blockEnd)
		}
	}
	appendCut(length)
	return cuts
}

func assertCrashImage(t *testing.T, image []byte, options Options, oldGeneration, newGeneration uint64, oldValue, newValue string, name string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, image, 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	collection, err := Open(file, options)
	if err != nil {
		_ = file.Close()
		t.Fatalf("%s recovery: %v", name, err)
	}
	got, ok, getErr := collection.AppendRaw(nil, []byte("key-03"))
	if getErr != nil || !ok {
		t.Fatalf("%s GetRaw = (%q,%v,%v)", name, got, ok, getErr)
	}
	switch collection.Generation() {
	case oldGeneration:
		if string(got) != oldValue {
			t.Fatalf("%s recovered old generation with mixed value %q", name, got)
		}
		assertRecoveredIndexCounts(t, collection, name, 12, 0)
	case newGeneration:
		if string(got) != newValue {
			t.Fatalf("%s recovered new generation with mixed value %q", name, got)
		}
		assertRecoveredIndexCounts(t, collection, name, 11, 1)
	default:
		t.Fatalf("%s recovered generation %d, want %d or %d", name, collection.Generation(), oldGeneration, newGeneration)
	}
	// Recovery has to leave the free set usable, not merely readable. Whichever
	// generation this image landed on, the chain that generation's superblock
	// names must replay to a set that holds nothing the same generation can
	// reach — a torn commit that published a delta describing space its own root
	// still uses would be handing out live pages on the next write.
	if err := collection.refreshReusable(collection.state.Load()); err != nil {
		t.Fatalf("%s free-log replay after recovery: %v", name, err)
	}
	assertFreeSetMirror(t, collection, name+" after recovery")
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func assertRecoveredIndexCounts(t *testing.T, collection *Collection, name string, oldCount, newCount int) {
	t.Helper()
	needle := func(src []byte) vibejson.Index {
		needed, err := vibejson.RequiredIndexEntries(src)
		if err != nil {
			t.Fatal(err)
		}
		index, err := vibejson.BuildIndex(src, make([]vibejson.IndexEntry, needed))
		if err != nil {
			t.Fatal(err)
		}
		return index
	}
	count := func(value string) int {
		masks, err := collectionIndexMasks(collection, nil, "status", needle([]byte(value)))
		if err != nil {
			t.Fatalf("%s index probe %s: %v", name, value, err)
		}
		rows := 0
		for _, mask := range masks {
			rows += bits.OnesCount64(mask.Bits)
		}
		return rows
	}
	if got := count(`"old"`); got != oldCount {
		t.Fatalf("%s old index count = %d, want %d", name, got, oldCount)
	}
	if got := count(`"new"`); got != newCount {
		t.Fatalf("%s new index count = %d, want %d", name, got, newCount)
	}
}

func TestFileSnapshotInlineReadSteadyAllocations(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-store-alloc-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, testFileStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	value := []byte(`{"id":1,"status":"active"}`)
	if _, err := collection.Put([]byte("key"), value); err != nil {
		t.Fatal(err)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	dst := make([]byte, 0, len(value))
	// Convert the key once, outside the measured closures: the store borrows a
	// []byte key, so a per-call []byte("key") would be counted as a read-path
	// allocation the store never makes.
	krd := []byte("key")
	if _, ok, err := snapshot.AppendRaw(dst[:0], krd); err != nil || !ok {
		t.Fatalf("warm read = (%v,%v)", ok, err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		got, ok, err := snapshot.AppendRaw(dst[:0], krd)
		if err != nil || !ok || len(got) != len(value) {
			panic("file snapshot read failed")
		}
	})
	if allocs != 0 {
		t.Fatalf("inline cache-hit AppendRaw allocated %.2f times, want 0", allocs)
	}
	if _, ok, err := collection.AppendRaw(dst[:0], krd); err != nil || !ok {
		t.Fatalf("warm collection read = (%v,%v)", ok, err)
	}
	allocs = testing.AllocsPerRun(100, func() {
		got, ok, err := collection.AppendRaw(dst[:0], krd)
		if err != nil || !ok || len(got) != len(value) {
			panic("file collection read failed")
		}
	})
	if allocs != 0 {
		t.Fatalf("inline collection AppendRaw allocated %.2f times, want 0", allocs)
	}
}

func TestFileStoreAsyncPublicationFlushesDurably(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-store-async-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.Durability = DurabilityAsyncVisible
	options.QueueSlots = 8
	options.GroupLimit = 8
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 40 {
		if _, err := collection.Put([]byte(fmt.Sprintf("key-%02d", i)), []byte(fmt.Sprintf(`{"id":%d}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	if collection.Generation() != 41 || collection.DurableGeneration() > collection.Generation() {
		t.Fatalf("pre-flush generations = published %d durable %d", collection.Generation(), collection.DurableGeneration())
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if collection.DurableGeneration() != collection.Generation() {
		t.Fatalf("post-flush generations = published %d durable %d", collection.Generation(), collection.DurableGeneration())
	}
	stats := collection.Stats()
	if stats.PublishedGeneration != 41 || stats.DurableGeneration != 41 || stats.CommittedBatches != 41 || stats.DeviceCommits == 0 {
		t.Fatalf("async stats = %+v", stats)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for i := range 40 {
		got, ok, err := reopened.AppendRaw(nil, []byte(fmt.Sprintf("key-%02d", i)))
		if err != nil || !ok || string(got) != fmt.Sprintf(`{"id":%d}`, i) {
			t.Fatalf("reopened key %d = (%q,%v,%v)", i, got, ok, err)
		}
	}
}
